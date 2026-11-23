//! Recovery replay of the remote value WAL into the hybrid index.
//!
//! Native replay (`crate::wal_replay`) walks one local WAL and hands whole
//! frames to the index in offset order. Independent owner WALs have no shared
//! byte order, so this module supplies the other half of that driver: an
//! [`OwnerPages`] reader per owner stream, a k-way merge of the streams in
//! [`BatchVersion`] order (invariant I1), and the Version B acknowledgement
//! filter that decides which merged batches the index may see.
//!
//! Three rules carry over from native unchanged:
//!
//! * **Whole batches.** A batch is yielded as `BatchStart(count)` followed by
//!   its members, all with the batch's version. The index never sees a partial
//!   batch: an owner page holds only fully validated batches, and this source
//!   yields a batch's frames only once the whole batch is in hand.
//! * **Start at the region cut.** Replay begins at the replay-from position the
//!   control region recorded (paper 3.3 and 3.4). Each stream has a stored byte
//!   front (I3/I4), floored to a fragment, so a stream may re-read batches below
//!   the cut. Such a batch is already in the stored blobs, and applying it again
//!   is not harmless: a replayed entry sits in the cell overlay, and a read finds
//!   the overlay before the blob, so an old entry from one stream would hide a
//!   newer entry of the same key that the blob holds from another stream whose
//!   floor lies above it. The merge therefore never yields a batch below the
//!   cut (`ReplayOutcome::below_cut_skipped`), as native replay yields no frame
//!   below `replay_from`.
//! * **The acknowledgement filter is per batch, not one global cut.** Version B
//!   records a truncation per fencing generation: an older generation's
//!   unacknowledged tail stays in the retained owner WALs after the next open
//!   starts a newer generation, and must be dropped while the later epoch is
//!   replayed. Version order makes the tail sort before the newer epoch, so the
//!   merge keeps draining rejected batches instead of stopping at the first one.

use super::position::VersionPosition;
use super::region::StreamFront;
use super::table::HybridTable;
use crate::CellId;
use crate::db::WalEntry;
use crate::distributed::codec::{CompleteBatch, Mutation};
use crate::distributed::storage::{LocalDbError, aligned_replay_start};
use crate::distributed::tcp::{Client, RecoveryPage, TcpError};
use crate::distributed::types::{BatchVersion, PhysicalValueAddress, RecordDescriptor};
use crate::key_shape::KeySpace;
use crate::wal_replay::{ReplayFrame, ReplaySource};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

/// `[len u32][checksum u32]` in front of every member frame of a batch payload.
/// `RecordDescriptor::offset` points at it, so the member's own bytes start
/// `MEMBER_HEADER_LEN` later. Mirrors the codec's private constant of the same
/// name; the two must change together.
const MEMBER_HEADER_LEN: usize = 8;

/// Everything replay needs from the coordinator's owner connections.
///
/// `partitions[i]` is the partition served by `clients[i]`; `page_limit` bounds
/// the batches one owner page may carry; `authority_epoch` is the fencing
/// generation this open replays under.
pub(crate) struct ReplayTransport<'a> {
    pub(crate) clients: &'a mut [Client],
    pub(crate) partitions: &'a [u32],
    pub(crate) page_limit: usize,
    pub(crate) authority_epoch: u64,
    pub(crate) fragment_bytes: u64,
}

/// One owner's replayable WAL suffix.
pub(crate) trait OwnerPages {
    /// Whole validated batches from `from`, in stream order. `require_end` is
    /// the offset the owner's sealed WAL must still cover.
    fn read_page(
        &mut self,
        partition: u32,
        generation: u64,
        from: u64,
        limit: usize,
        require_end: u64,
    ) -> Result<RecoveryPage, ReplaySourceError>;
}

/// [`OwnerPages`] over the coordinator's live owner connections.
pub(crate) struct TransportPages<'a> {
    clients: &'a mut [Client],
    partitions: &'a [u32],
    authority_epoch: u64,
    page_limit: usize,
}

impl<'a> TransportPages<'a> {
    fn new(transport: &'a mut ReplayTransport<'_>) -> Self {
        Self {
            clients: &mut *transport.clients,
            partitions: transport.partitions,
            authority_epoch: transport.authority_epoch,
            page_limit: transport.page_limit,
        }
    }
}

impl OwnerPages for TransportPages<'_> {
    fn read_page(
        &mut self,
        partition: u32,
        generation: u64,
        from: u64,
        limit: usize,
        require_end: u64,
    ) -> Result<RecoveryPage, ReplaySourceError> {
        let index = self
            .partitions
            .iter()
            .position(|served| *served == partition)
            .ok_or(ReplaySourceError::UnknownPartition(partition))?;
        self.clients[index]
            .replay_live_wal_page(
                self.authority_epoch,
                generation,
                from,
                limit.min(self.page_limit),
                require_end,
            )
            .map_err(ReplaySourceError::Transport)
    }
}

/// Why a stream could not be read. The replay driver wraps these in
/// `ReplayError::Source`. A torn or absent tail is not an error: the owner
/// reports it as the end of the stream.
#[derive(Debug)]
pub(crate) enum ReplaySourceError {
    Transport(TcpError),
    /// A stream named by the control region has no connection.
    UnknownPartition(u32),
    /// A live generation with no stored front, so its suffix would be dropped
    /// silently. Fail closed.
    MissingFront {
        partition: u32,
        generation: u64,
    },
    /// A peer-provided frame described a position the encoding rejects. The
    /// typed cause is kept so a bad address is distinguishable from bad bytes.
    Position(LocalDbError),
    /// The owner returned a page that violates the recovery invariants: too
    /// many batches, no cursor progress, a foreign generation, a version that
    /// does not advance, or a batch whose descriptors do not match its members.
    InvalidPage(&'static str),
}

impl std::fmt::Display for ReplaySourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "owner replay page: {error}"),
            Self::UnknownPartition(partition) => {
                write!(f, "replay stream for unknown partition {partition}")
            }
            Self::MissingFront {
                partition,
                generation,
            } => write!(
                f,
                "live generation {generation} on partition {partition} has no stored replay front"
            ),
            Self::Position(error) => write!(f, "invalid replay position: {error}"),
            Self::InvalidPage(reason) => write!(f, "invalid owner replay page: {reason}"),
        }
    }
}

impl std::error::Error for ReplaySourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Position(error) => Some(error),
            Self::UnknownPartition(_) | Self::MissingFront { .. } | Self::InvalidPage(_) => None,
        }
    }
}

/// What one replay pass produced. `applied` is the highest batch the index
/// consumed whole; the caller serves at that cut.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReplayOutcome {
    pub(crate) applied: Option<BatchVersion>,
    /// Batches merged from owner pages.
    pub(crate) merged: u64,
    /// Merged batches the acknowledgement filter rejected; they stay in their
    /// owner WALs, unindexed.
    pub(crate) discarded: u64,
    /// Acknowledged batches below the region cut that the merge read again: a
    /// fragment-aligned stream front, a retained generation with no stored row
    /// (replayed from offset 0), the dropped floor at the extent-queue cap, or
    /// an older-generation row carried forward. The stored blobs already hold
    /// them, so the index never receives them (see the module comment).
    pub(crate) below_cut_skipped: u64,
    /// Stored fronts whose generation the manifest no longer retains: a
    /// trim-pending or already-trimmed generation, dead by construction.
    pub(crate) skipped_dead: u64,
    /// Encoded bytes of the batches the index received.
    pub(crate) tail_bytes: u64,
    /// Relocated-copy batches skipped: durable owner batches that consumed an
    /// allocator version but were never published, so replaying their members
    /// would let a stale copy override a newer user write of the same key.
    pub(crate) relocated_skipped: u64,
}

/// What the caller records about one recovery replay. Native's replay-tail
/// evidence (`RecoveryStats`) is re-sourced from here, so no hot-path counter is
/// added.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplayReport {
    /// Batches the index received.
    pub(crate) tail_batches: u64,
    /// Sum of `CompleteBatch::encoded().len()` over the indexed batches: the
    /// canonical batch payload bytes, not owner WAL bytes (which include
    /// framing, padding and retired extents).
    pub(crate) tail_bytes: u64,
    /// Time spent inside the replay driver.
    pub(crate) tail_replay_ns: u64,
    /// Batches Version B refused; still in their owner WALs.
    pub(crate) discarded: u64,
    /// Dead-generation fronts skipped.
    pub(crate) skipped_dead: u64,
    /// Relocated copies skipped (never indexed; they consumed an allocator
    /// version and are not part of the tail).
    pub(crate) relocated_skipped: u64,
    /// Batches below the region cut that were read again and not indexed; not
    /// part of the tail.
    pub(crate) below_cut_skipped: u64,
}

/// One owner stream's read state.
struct Stream {
    partition: u32,
    generation: u64,
    /// Next page cursor; `None` once the owner reported no further page.
    cursor: Option<usize>,
    /// Batches of the pages read so far, not yet merged.
    batches: VecDeque<(CompleteBatch, PhysicalValueAddress)>,
    /// Last version accepted from this stream. A non-increasing version is
    /// corruption: a batch lives on exactly one owner.
    last: Option<BatchVersion>,
    /// Stored start, also the offset the owner's sealed WAL must cover.
    replay_from: u64,
}

/// A batch whose frames are being handed to the replay driver.
struct BatchCursor {
    batch: CompleteBatch,
    version: BatchVersion,
    address: PhysicalValueAddress,
    descriptors: Vec<RecordDescriptor>,
    /// `Some` for the singleton `DropCells` member, which has no descriptor.
    range: Option<(KeySpace, CellId, CellId)>,
    /// The leading `BatchStart` (or the range member) has been yielded.
    started: bool,
    next_member: usize,
}

impl BatchCursor {
    fn new(batch: CompleteBatch, address: PhysicalValueAddress) -> Result<Self, ReplaySourceError> {
        let descriptors = batch.record_descriptors().to_vec();
        let range = batch
            .drop_cells()
            .map(|(keyspace, first, last)| (keyspace, first.clone(), last.clone()));
        if range.is_none() {
            if descriptors.len() != batch.mutations().len() {
                return Err(ReplaySourceError::InvalidPage(
                    "batch descriptors do not match its members",
                ));
            }
            // The batch came from an owner page, so the framing is peer data:
            // every member range must lie inside the payload before a position
            // is derived from it. A range outside would otherwise produce a
            // zero CRC that looks like the "not a member" sentinel.
            let payload = batch.encoded().len();
            for descriptor in &descriptors {
                let start = descriptor.offset as usize;
                let end = start.checked_add(descriptor.len as usize).ok_or(
                    ReplaySourceError::InvalidPage("member frame outside the batch payload"),
                )?;
                if (descriptor.len as usize) < MEMBER_HEADER_LEN || start > end || end > payload {
                    return Err(ReplaySourceError::InvalidPage(
                        "member frame outside the batch payload",
                    ));
                }
            }
        }
        Ok(Self {
            version: batch.version(),
            batch,
            address,
            descriptors,
            range,
            started: false,
            next_member: 0,
        })
    }

    /// The position that sorts at member 0 of this batch: before every later
    /// member and after the previous batch, so it is the anchor the driver's
    /// truncation cutoff compares to.
    fn batch_start(&self) -> VersionPosition {
        VersionPosition::batch_start(self.version, self.address)
    }

    fn next_frame(&mut self) -> Result<Option<ReplayFrame<VersionPosition>>, ReplaySourceError> {
        if let Some((keyspace, first, last)) = &self.range {
            if self.started {
                return Ok(None);
            }
            self.started = true;
            // A range clear is yielded as one lone `Entry`, never inside a
            // `BatchStart` group. The replay driver's `DropCells` arm applies it
            // by cell range, to both the replay buffer and the large table, and
            // never reads the frame's position or buffers it; inside a group the
            // driver would reject it outright. A range member has no
            // `RecordDescriptor`, so no real member position exists for it, and
            // none is needed: the position it carries is the batch anchor, a
            // cutoff value that is never buffered or stored.
            return Ok(Some(ReplayFrame::Entry {
                position: self.batch_start(),
                entry: WalEntry::DropCells(*keyspace, first.clone(), last.clone()),
            }));
        }
        if !self.started {
            self.started = true;
            return Ok(Some(ReplayFrame::BatchStart {
                position: self.batch_start(),
                count: u32::try_from(self.batch.mutations().len()).map_err(|_| {
                    ReplaySourceError::InvalidPage("batch member count exceeds the frame count")
                })?,
            }));
        }
        let index = self.next_member;
        // Every member was yielded: the batch is exhausted. (Asking past the
        // last member is the normal end of a batch, not a malformed page; the
        // descriptor count was validated against the mutation count at `new`.)
        if index >= self.descriptors.len() {
            return Ok(None);
        }
        let descriptor = self
            .descriptors
            .get(index)
            .ok_or(ReplaySourceError::InvalidPage(
                "member index outside the batch descriptors",
            ))?;
        let mutation = self
            .batch
            .mutations()
            .get(index)
            .ok_or(ReplaySourceError::InvalidPage(
                "member index outside the batch mutations",
            ))?;
        let payload_crc = member_payload_crc(&self.batch, descriptor)?;
        let operation_index = u32::try_from(index).map_err(|_| {
            ReplaySourceError::InvalidPage("member index exceeds the position encoding")
        })?;
        let entry = match mutation {
            Mutation::Put {
                keyspace,
                key,
                value,
            } => WalEntry::Record(*keyspace, key.clone(), value.clone(), false),
            Mutation::Delete { keyspace, key } => WalEntry::Remove(*keyspace, key.clone()),
            // A range clear is the singleton handled above.
            Mutation::DropCells { .. } => {
                return Err(ReplaySourceError::InvalidPage(
                    "range clear inside a point batch",
                ));
            }
        };
        let position = VersionPosition::member_checked(
            self.version,
            self.address,
            operation_index,
            *descriptor,
            payload_crc,
        )
        .map_err(ReplaySourceError::Position)?;
        self.next_member += 1;
        Ok(Some(ReplayFrame::Entry { position, entry }))
    }
}

/// CRC of the member's own bytes after its framing prefix: what a read
/// re-verifies before decoding the value. The 8 B `[len][checksum]` prefix and
/// the native entry header are not part of it.
fn member_payload_crc(
    batch: &CompleteBatch,
    descriptor: &RecordDescriptor,
) -> Result<u32, ReplaySourceError> {
    let start = descriptor.offset as usize + MEMBER_HEADER_LEN;
    let end = descriptor.offset as usize + descriptor.len as usize;
    batch
        .encoded()
        .get(start..end)
        .map(crc32fast::hash)
        .ok_or(ReplaySourceError::InvalidPage(
            "member frame outside the batch payload",
        ))
}

/// K-way merge of owner streams in [`BatchVersion`] order, filtered by Version
/// B's per-generation acknowledgement rule and started at the region cut.
pub(crate) struct HybridReplaySource<P: OwnerPages, K: FnMut(BatchVersion) -> bool> {
    pages: P,
    /// Decides whether the index may see a batch. Version B's recovery plan
    /// supplies it (`RecoveryPlan::keep`), so a recorded truncation of an older
    /// generation and the open cut of the current one both apply.
    keep: K,
    /// The cut of the control region this open loaded: every batch below it is
    /// in the stored blobs, so none is yielded. Fixed at construction.
    region_cut: BatchVersion,
    streams: Vec<Stream>,
    ready: BinaryHeap<Reverse<(BatchVersion, usize)>>,
    current: Option<BatchCursor>,
    /// Last merged version, for the cross-stream order check.
    last_merged: Option<BatchVersion>,
    page_limit: usize,
    outcome: ReplayOutcome,
}

impl<P: OwnerPages, K: FnMut(BatchVersion) -> bool> HybridReplaySource<P, K> {
    /// `fronts` are the stored per-stream starts (I3/I4); `live_streams` is the
    /// coordinator manifest's retained authority as `(partition, generation)`;
    /// `region_cut` is the cut of the loaded control region, below which no
    /// batch reaches the index; `page_limit` bounds retained batches;
    /// `fragment_bytes` floors an unaligned start.
    ///
    /// A stored front outside `live_streams` belongs to a trim-pending or
    /// already-absent generation: dead by construction, so it is skipped. A live
    /// generation with no stored front is refused, because replaying without it
    /// would silently drop that stream's suffix.
    pub(crate) fn new(
        pages: P,
        keep: K,
        fronts: &[StreamFront],
        live_streams: &[(u32, u64)],
        region_cut: BatchVersion,
        page_limit: usize,
        fragment_bytes: u64,
    ) -> Result<Self, ReplaySourceError> {
        if page_limit == 0 {
            return Err(ReplaySourceError::InvalidPage("empty page limit"));
        }
        for &(partition, generation) in live_streams {
            if !fronts
                .iter()
                .any(|front| front.partition == partition && front.generation == generation)
            {
                return Err(ReplaySourceError::MissingFront {
                    partition,
                    generation,
                });
            }
        }
        let mut skipped_dead = 0;
        let mut streams = Vec::with_capacity(fronts.len());
        for front in fronts {
            if !live_streams.contains(&(front.partition, front.generation)) {
                skipped_dead += 1;
                continue;
            }
            let start = aligned_replay_start(front.replay_from, fragment_bytes);
            streams.push(Stream {
                partition: front.partition,
                generation: front.generation,
                cursor: usize::try_from(start).map(Some).map_err(|_| {
                    ReplaySourceError::InvalidPage("start exceeds the address space")
                })?,
                batches: VecDeque::new(),
                last: None,
                replay_from: start,
            });
        }
        let mut source = Self {
            pages,
            keep,
            region_cut,
            streams,
            ready: BinaryHeap::new(),
            current: None,
            last_merged: None,
            page_limit,
            outcome: ReplayOutcome {
                skipped_dead,
                ..ReplayOutcome::default()
            },
        };
        // One page per stream bounds retained batches at `streams * page_limit`,
        // the same bound the recovery pages this replaces already enforced.
        for index in 0..source.streams.len() {
            source.fill(index)?;
            source.announce(index);
        }
        Ok(source)
    }

    /// Read pages until the stream has a batch or its owner reports no further
    /// page. A page may hold only retired or skipped groups, so a single read
    /// is not enough to prove the stream is exhausted.
    fn fill(&mut self, index: usize) -> Result<(), ReplaySourceError> {
        loop {
            if !self.streams[index].batches.is_empty() {
                return Ok(());
            }
            let Some(cursor) = self.streams[index].cursor else {
                return Ok(());
            };
            // Copy the request out before borrowing the page reader: `Stream`
            // owns a `VecDeque`, so it cannot be destructured through an index.
            let partition = self.streams[index].partition;
            let generation = self.streams[index].generation;
            let replay_from = self.streams[index].replay_from;
            let page = self.pages.read_page(
                partition,
                generation,
                cursor as u64,
                self.page_limit,
                replay_from,
            )?;
            if page.batches.len() > self.page_limit {
                return Err(ReplaySourceError::InvalidPage("page exceeds its limit"));
            }
            if page.next.is_some_and(|next| next <= cursor) {
                return Err(ReplaySourceError::InvalidPage(
                    "page cursor does not advance",
                ));
            }
            let stream = &mut self.streams[index];
            for (batch, durable) in page.batches {
                let version = batch.version();
                if version.epoch != generation {
                    return Err(ReplaySourceError::InvalidPage(
                        "batch belongs to another fencing generation",
                    ));
                }
                if stream.last.is_some_and(|last| last >= version) {
                    return Err(ReplaySourceError::InvalidPage(
                        "stream versions are not strictly ordered",
                    ));
                }
                stream.last = Some(version);
                stream
                    .batches
                    .push_back((batch, durable.appended().address()));
            }
            stream.cursor = page.next;
        }
    }

    fn announce(&mut self, index: usize) {
        if let Some((batch, _)) = self.streams[index].batches.front() {
            self.ready.push(Reverse((batch.version(), index)));
        }
    }

    /// Next acknowledged batch in version order, refilling a depleted stream on
    /// demand and draining rejected batches without stopping.
    fn pop_batch(&mut self) -> Result<Option<BatchCursor>, ReplaySourceError> {
        loop {
            let Some(Reverse((version, index))) = self.ready.pop() else {
                return Ok(None);
            };
            if self.last_merged.is_some_and(|last| last >= version) {
                return Err(ReplaySourceError::InvalidPage(
                    "replay streams are not strictly ordered",
                ));
            }
            self.last_merged = Some(version);
            let Some((batch, address)) = self.streams[index].batches.pop_front() else {
                return Err(ReplaySourceError::InvalidPage("queued batch disappeared"));
            };
            self.fill(index)?;
            self.announce(index);
            self.outcome.merged += 1;
            if !(self.keep)(version) {
                // Version B never acknowledged this batch, so the index must not
                // see it. A later epoch's batches sort above it and may well be
                // acknowledged, so draining continues.
                self.outcome.discarded += 1;
                continue;
            }
            if batch.is_relocated() {
                // A relocation copy is a durable owner batch that consumed an
                // allocator version and was never published. Its members are
                // older values copied to a new place, so inserting them would
                // let the copy at `vR` override a user write `vu < vR` of the
                // same key after a crash (native replay avoids this by skipping
                // relocated records). Nothing is inserted, but the version is a
                // processed no-op, so `applied` advances past it and the
                // consistent prefix is not stalled behind a batch no one reads.
                //
                // Skipping never leaves an index entry pointing at a retired
                // file: a re-point reaches disk only through a store that flushed
                // the re-pointed cells, and relocation adopts starts and retires
                // files only after such a store covers its scan cut. So when the
                // copy is the live location of a key, the stored blob already
                // names it, and when it is not, the original extent still exists.
                self.outcome.relocated_skipped += 1;
                self.outcome.applied = Some(version);
                continue;
            }
            if version < self.region_cut {
                // Read again for one of four reasons: the stream's front is
                // floored to a fragment; a retained generation with no stored
                // row replays from offset 0; the dropped floor at the queue cap
                // lies below it; or an older generation's row was carried
                // forward. The stored blobs hold this batch (the region cut is
                // the minimum frontier over its non-empty cells, and a blob holds
                // every entry below its cell's frontier). It is after `keep` and
                // the relocated check, so Version B accounting is unchanged, and
                // before the cursor, so the batch is not decoded.
                self.outcome.below_cut_skipped += 1;
                continue;
            }
            return Ok(Some(BatchCursor::new(batch, address)?));
        }
    }
}

impl<P: OwnerPages, K: FnMut(BatchVersion) -> bool> ReplaySource<VersionPosition>
    for HybridReplaySource<P, K>
{
    type Error = ReplaySourceError;
    type Output = ReplayOutcome;

    fn next(&mut self) -> Result<Option<ReplayFrame<VersionPosition>>, Self::Error> {
        loop {
            if let Some(cursor) = &mut self.current {
                if let Some(frame) = cursor.next_frame()? {
                    return Ok(Some(frame));
                }
                self.outcome.applied = Some(cursor.version);
                self.outcome.tail_bytes = self
                    .outcome
                    .tail_bytes
                    .saturating_add(cursor.batch.encoded().len() as u64);
                self.current = None;
                continue;
            }
            match self.pop_batch()? {
                Some(cursor) => self.current = Some(cursor),
                None => return Ok(None),
            }
        }
    }

    fn finish(self, cutoff: Option<VersionPosition>) -> Result<Self::Output, Self::Error> {
        // A batch's frames are yielded only once the whole batch is in hand, so
        // a cutoff would mean an invariant broke rather than a torn tail.
        if cutoff.is_some() {
            return Err(ReplaySourceError::InvalidPage(
                "replay ended inside a batch",
            ));
        }
        Ok(self.outcome)
    }
}

/// Replays every retained owner stream into `table` from the cut of the control
/// region the table opened with. Batches below that cut are read but not
/// applied (`ReplayOutcome::below_cut_skipped`); every batch at or above it is
/// applied in version order.
///
/// `keep` is Version B's recovery filter (`RecoveryPlan::keep` in the
/// coordinator). It is per batch because a truncation is recorded per fencing
/// generation: an older generation's unacknowledged tail is still in the
/// retained owner WALs and must be dropped while the newer epoch replays.
///
/// `live_streams` is the manifest's retained authority as
/// `(partition, generation)`: a stored front outside it is dead by construction
/// and skipped, while a live generation with no stored front fails closed.
pub(crate) fn replay_into(
    table: &HybridTable,
    transport: &mut ReplayTransport<'_>,
    keep: impl FnMut(BatchVersion) -> bool,
    fronts: &[StreamFront],
    live_streams: &[(u32, u64)],
) -> Result<ReplayReport, LocalDbError> {
    let page_limit = transport.page_limit;
    let fragment_bytes = transport.fragment_bytes;
    replay_pages(
        table,
        TransportPages::new(transport),
        keep,
        fronts,
        live_streams,
        page_limit,
        fragment_bytes,
    )
}

/// [`replay_into`] over any owner page reader, so a test can script the owners.
/// The replay starts at `table.opened_cut()`, the cut of the region loaded at
/// open, and never at a later store.
pub(crate) fn replay_pages<P: OwnerPages>(
    table: &HybridTable,
    pages: P,
    keep: impl FnMut(BatchVersion) -> bool,
    fronts: &[StreamFront],
    live_streams: &[(u32, u64)],
    page_limit: usize,
    fragment_bytes: u64,
) -> Result<ReplayReport, LocalDbError> {
    let source = HybridReplaySource::new(
        pages,
        keep,
        fronts,
        live_streams,
        table.opened_cut(),
        page_limit,
        fragment_bytes,
    )
    .map_err(LocalDbError::component)?;
    let replay_started = std::time::Instant::now();
    let outcome = crate::wal_replay::replay_wal(
        table.contexts(),
        table.large_table(),
        table.shape(),
        source,
        table.metrics(),
        table.loader(),
    )
    .map_err(LocalDbError::component)?;
    let tail_replay_ns = u64::try_from(replay_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    // `merged`, `discarded`, `skipped_dead` and `tail_bytes` are read here to
    // build the report, and the caller folds every field into `RecoveryStats`;
    // nothing is printed, because stdout is not an operator channel.
    Ok(ReplayReport {
        tail_batches: outcome
            .merged
            .saturating_sub(outcome.discarded)
            .saturating_sub(outcome.relocated_skipped)
            .saturating_sub(outcome.below_cut_skipped),
        tail_bytes: outcome.tail_bytes,
        tail_replay_ns,
        discarded: outcome.discarded,
        skipped_dead: outcome.skipped_dead,
        relocated_skipped: outcome.relocated_skipped,
        below_cut_skipped: outcome.below_cut_skipped,
    })
}
