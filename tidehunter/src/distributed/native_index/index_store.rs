//! Worker-private index-store segments (checkpoint option C spec §4.3, §10.1).
//!
//! - Each cell flusher owns one [`IndexStoreWriter`] and one stream id. The
//!   writer encodes captured cells and appends their unchanged frames to its
//!   open segment. A ready group holds at most [`GROUP_CELLS`] results and
//!   [`GROUP_BYTES`] frame bytes; one larger frame forms a group alone.
//! - [`IndexStoreWriter::seal`] flushes the fixed buffer, syncs the segment
//!   once, syncs the directory, and only then maps the segment read-only and
//!   returns one [`NewCellBase`] per appended cell. A sealed segment is never
//!   written, truncated or renamed again, which is the mapping contract of
//!   `snapshot::map_file`.
//! - Segment names are [`IndexStoreFileId::canonical_name`] of
//!   `(authority epoch, stream, sequence)`, created with `create_new`. The
//!   authority epoch is new for each open, so a full id is never reused.
//! - [`IndexStore::open`] lists canonical segments before it deletes anything,
//!   starts every stream at `max(sequence on disk, sequence in a root) + 1`,
//!   and deletes every segment that no retained root references.
//! - [`IndexStore::collect_garbage`] deletes a sealed segment only when no
//!   retained root, no in-memory pin and no live index base references it.
//!
//! Crash consistency (store before root before GC): a segment becomes
//! reachable only through a root that the caller adopts after `seal` returned.
//! A crash before or during `seal`, or before root adoption, leaves an orphan
//! that the next open deletes. A failed write or sync returns an error and no
//! result, and the writer refuses all later calls (fail-stop). The adopted
//! root file table is the only durable segment authority: there is no
//! separate catalog or journal.
//!
//! Lock: [`IndexStore`]'s catalog mutex is a leaf lock on cold paths (segment
//! creation, seal, mapping, GC). No I/O runs under it, and it is never taken
//! while the index writer, the publisher or an index shard is held.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "option C store; opened, given to the flusher pool and swept by the per-cell commit stage"
    )
)]

use super::cell_contract::{IndexStoreFileId, RootFile};
use super::cell_flush::{NewCellBase, PreparedCell};
use super::snapshot::{CellEncoder, CellFile, CellFrame, map_file};
use super::*;
use minibytes::BytesOwner;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

/// Most cell results in one ready group (spec §4.3).
pub(crate) const GROUP_CELLS: usize = 32;
/// Most frame bytes in one ready group. One larger frame forms a group alone.
pub(crate) const GROUP_BYTES: u64 = 8 << 20;
/// Fixed append buffer of an open segment, as in exact-mode persistence.
const BUFFER_BYTES: usize = 16 * 1024;
/// Most segments that one GC sweep deletes. The caller repeats the sweep
/// while [`GcSweep::more`] is set.
const GC_SWEEP_FILES: usize = 256;

/// In-memory reference to one segment. Every mapping of the segment owns a
/// clone, so live bases, flush results, root captures and readers that hold
/// any slice of the mapping keep the segment. The open writer holds one too.
/// Pins are only cloned from a live pin, or created by
/// [`IndexStore::map_sealed`] under the catalog lock for a segment that GC has
/// not selected.
struct FilePin;

/// Owner of a pinned mapping: the read-only map and the pin of its segment.
struct PinnedMap {
    map: Bytes,
    _pin: Arc<FilePin>,
}

impl AsRef<[u8]> for PinnedMap {
    fn as_ref(&self) -> &[u8] {
        &self.map
    }
}

impl BytesOwner for PinnedMap {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileState {
    /// Created by a writer and not sealed. GC never selects it; if the writer
    /// fails or is dropped first, it stays until the next open deletes it.
    Writing,
    Sealed,
    /// Selected by a GC sweep; no pin can be created again.
    Retiring,
}

struct FileEntry {
    state: FileState,
    sealed_len: u64,
    pin: Weak<FilePin>,
}

struct StreamState {
    /// Next segment sequence of this stream. Never decreases.
    next: u64,
    /// True while an [`IndexStoreWriter`] owns the stream.
    writer: bool,
}

#[derive(Default)]
struct Catalog {
    /// Known segments: sealed segments of retained roots and every segment
    /// created in this process until GC deletes it.
    files: BTreeMap<IndexStoreFileId, FileEntry>,
    streams: BTreeMap<u16, StreamState>,
}

/// The segments of one database directory and their in-memory references.
pub(crate) struct IndexStore {
    directory: PathBuf,
    authority_epoch: u64,
    /// First sequence of every stream: the largest sequence on disk, plus one.
    first_sequence: u64,
    catalog: Mutex<Catalog>,
}

/// Result of the open-time directory scan.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct OrphanScan {
    /// First segment sequence of every stream in this process.
    pub next_sequence: u64,
    /// Deleted canonical segments that no retained root references.
    pub deleted: Vec<IndexStoreFileId>,
}

/// Result of one GC sweep.
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct GcSweep {
    pub deleted: Vec<IndexStoreFileId>,
    /// More segments may be eligible: the sweep reached its limit.
    pub more: bool,
}

impl IndexStore {
    /// Scan `directory` and delete orphan segments before new output.
    ///
    /// `authority_epoch` must be the newly fenced epoch that the open protocol
    /// recorded durably. `retained_roots` are the authenticated file tables of
    /// the adopted root and of every older root that is still retained. Every
    /// segment they reference must exist with its sealed length. Legacy
    /// containers are not segments and are left unchanged. A crash during
    /// the deletion repeats the scan at the next open.
    pub(crate) fn open(
        directory: &Path,
        authority_epoch: u64,
        retained_roots: &[&[RootFile]],
    ) -> Result<(Arc<Self>, OrphanScan), NativeIndexError> {
        // List all canonical names first, so orphans count in the maximum.
        let mut on_disk = BTreeMap::new();
        let mut max_sequence = None;
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(IndexStoreFileId::parse_canonical)
            else {
                continue;
            };
            let IndexStoreFileId::Segment {
                authority_epoch: epoch,
                segment_seq,
                ..
            } = id
            else {
                continue;
            };
            if epoch > authority_epoch {
                return Err(NativeIndexError::Invalid(
                    "index segment from a newer authority epoch",
                ));
            }
            // `DirEntry::metadata` does not follow a symbolic link.
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                return Err(NativeIndexError::Invalid("index segment is not a file"));
            }
            on_disk.insert(id, metadata.len());
            max_sequence = max_sequence.max(Some(segment_seq));
        }
        let mut files = BTreeMap::new();
        for file in retained_roots.iter().flat_map(|root| root.iter()) {
            if !matches!(file.id, IndexStoreFileId::Segment { .. }) {
                continue;
            }
            match on_disk.get(&file.id) {
                Some(len) if *len == file.sealed_len => {}
                Some(_) => return Err(NativeIndexError::Invalid("root segment length")),
                None => return Err(NativeIndexError::Invalid("root segment missing")),
            }
            files.insert(
                file.id,
                FileEntry {
                    state: FileState::Sealed,
                    sealed_len: file.sealed_len,
                    pin: Weak::new(),
                },
            );
        }
        // Every root segment is on disk, so this maximum covers the roots.
        let next_sequence = match max_sequence {
            None => 0,
            Some(max) => max.checked_add(1).ok_or(NativeIndexError::Capacity)?,
        };
        let mut deleted = Vec::new();
        for id in on_disk.keys().filter(|id| !files.contains_key(*id)) {
            std::fs::remove_file(directory.join(id.canonical_name()))?;
            deleted.push(*id);
        }
        if !deleted.is_empty() {
            sync_directory(directory)?;
        }
        let store = Arc::new(Self {
            directory: directory.to_owned(),
            authority_epoch,
            first_sequence: next_sequence,
            catalog: Mutex::new(Catalog {
                files,
                streams: BTreeMap::new(),
            }),
        });
        Ok((
            store,
            OrphanScan {
                next_sequence,
                deleted,
            },
        ))
    }

    /// Length of a sealed segment, for the root file table.
    pub(crate) fn sealed_len(&self, id: IndexStoreFileId) -> Result<u64, NativeIndexError> {
        match self.catalog.lock().files.get(&id) {
            Some(entry) if entry.state == FileState::Sealed => Ok(entry.sealed_len),
            _ => Err(NativeIndexError::Invalid("index segment is not sealed")),
        }
    }

    /// Map a whole sealed segment read-only. The mapping pins the segment.
    /// Callers map only segments of a retained root or segments they already
    /// pin; a segment that GC selected is refused.
    pub(crate) fn map_sealed(&self, id: IndexStoreFileId) -> Result<Bytes, NativeIndexError> {
        let (pin, sealed_len) = {
            let mut catalog = self.catalog.lock();
            let entry = catalog
                .files
                .get_mut(&id)
                .filter(|entry| entry.state == FileState::Sealed)
                .ok_or(NativeIndexError::Invalid("index segment is not sealed"))?;
            // GC selects only entries without a strong pin, under this lock,
            // so a pin taken here is seen by every later sweep.
            let pin = entry.pin.upgrade().unwrap_or_else(|| {
                let pin = Arc::new(FilePin);
                entry.pin = Arc::downgrade(&pin);
                pin
            });
            (pin, entry.sealed_len)
        };
        let map = map_file(&File::open(self.directory.join(id.canonical_name()))?, None)?;
        if map.len() as u64 != sealed_len {
            return Err(NativeIndexError::Invalid("sealed index segment length"));
        }
        Ok(Bytes::from_owner(PinnedMap { map, _pin: pin }))
    }

    /// Delete sealed segments that no retained root, no in-memory pin and no
    /// live base of `index` references, then sync the directory.
    ///
    /// Call only from the metadata actor that serializes root capture and
    /// manifest replacement: after the newest root's manifest is
    /// directory-synced and its live apply is complete, never between a
    /// manifest rename and its directory sync, and never during a root
    /// capture. `retained_roots` are the file tables of the adopted root and of
    /// every older root that is still retained (spec §10.1).
    ///
    /// One catalog hold is the stable snapshot of the sweep: a segment that a
    /// writer creates later is `Writing` until it is sealed, so it is never
    /// selected. A selected entry is `Retiring`, so no pin can be created for
    /// it. A crash before an unlink leaks the file until the next open; a
    /// crash after it leaves every retained root complete. An unlink error
    /// returns every selected segment not yet unlinked to `Sealed`.
    pub(crate) fn collect_garbage(
        &self,
        retained_roots: &[&[RootFile]],
        index: &NativeIndex,
    ) -> Result<GcSweep, NativeIndexError> {
        let mut referenced: BTreeSet<IndexStoreFileId> = retained_roots
            .iter()
            .flat_map(|root| root.iter().map(|file| file.id))
            .collect();
        // A base that `IndexWriter::reload` copied to heap memory holds no
        // mapping pin but still names its segment. Excluding live bases here
        // keeps a later sweep from selecting the same reachable segments.
        referenced.extend(Self::live_segments(index));
        let mut selected = Vec::with_capacity(GC_SWEEP_FILES);
        let mut more = false;
        {
            let mut catalog = self.catalog.lock();
            for (id, entry) in &mut catalog.files {
                if entry.state != FileState::Sealed
                    || entry.pin.strong_count() != 0
                    || referenced.contains(id)
                {
                    continue;
                }
                if selected.len() == GC_SWEEP_FILES {
                    more = true;
                    break;
                }
                entry.state = FileState::Retiring;
                selected.push(*id);
            }
        }
        // Scan again after the zero-pin selection. A base installed after the
        // first scan came from a mapped, pinned block; if `reload` dropped that
        // pin before the selection, this scan still finds the base. No new base
        // of a selected segment can appear: it has no pin and is `Retiring`.
        let live = Self::live_segments(index);
        let mut kept = Vec::new();
        selected.retain(|id| {
            let reachable = live.contains(id);
            if reachable {
                kept.push(*id);
            }
            !reachable
        });
        let mut unlinked = 0;
        let mut failure = None;
        for id in &selected {
            match std::fs::remove_file(self.directory.join(id.canonical_name())) {
                // Already absent: the unlink goal holds.
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                    failure = Some(NativeIndexError::from(error));
                    break;
                }
                _ => unlinked += 1,
            }
        }
        if unlinked != 0
            && let Err(error) = sync_directory(&self.directory)
        {
            failure.get_or_insert(error.into());
        }
        {
            let mut catalog = self.catalog.lock();
            for id in &selected[..unlinked] {
                catalog.files.remove(id);
            }
            for id in kept.iter().chain(&selected[unlinked..]) {
                if let Some(entry) = catalog.files.get_mut(id) {
                    entry.state = FileState::Sealed;
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        selected.truncate(unlinked);
        Ok(GcSweep {
            deleted: selected,
            more,
        })
    }

    /// Segments named by the bases installed in `index`.
    fn live_segments(index: &NativeIndex) -> BTreeSet<IndexStoreFileId> {
        index
            .reachable_files()
            .iter()
            .filter_map(|name| IndexStoreFileId::parse_canonical(name))
            .filter(|id| matches!(id, IndexStoreFileId::Segment { .. }))
            .collect()
    }

    fn claim_stream(&self, stream_id: u16) -> Result<(), NativeIndexError> {
        let mut catalog = self.catalog.lock();
        let stream = catalog.streams.entry(stream_id).or_insert(StreamState {
            next: self.first_sequence,
            writer: false,
        });
        if stream.writer {
            return Err(NativeIndexError::Invalid(
                "index stream already has a writer",
            ));
        }
        stream.writer = true;
        Ok(())
    }

    fn release_stream(&self, stream_id: u16) {
        if let Some(stream) = self.catalog.lock().streams.get_mut(&stream_id) {
            stream.writer = false;
        }
    }

    /// Register and create the next segment of `stream_id`. The sequence is
    /// consumed before creation, so a failed creation never reuses its name.
    fn create_segment(&self, stream_id: u16) -> Result<OpenSegment, NativeIndexError> {
        let pin = Arc::new(FilePin);
        let id = {
            let mut catalog = self.catalog.lock();
            let catalog = &mut *catalog;
            let stream = catalog
                .streams
                .get_mut(&stream_id)
                .ok_or(NativeIndexError::Invalid("index stream has no writer"))?;
            let id = IndexStoreFileId::Segment {
                authority_epoch: self.authority_epoch,
                stream_id,
                segment_seq: stream.next,
            };
            stream.next = stream
                .next
                .checked_add(1)
                .ok_or(NativeIndexError::Capacity)?;
            if catalog.files.contains_key(&id) {
                return Err(NativeIndexError::Invalid("index segment name collision"));
            }
            catalog.files.insert(
                id,
                FileEntry {
                    state: FileState::Writing,
                    sealed_len: 0,
                    pin: Arc::downgrade(&pin),
                },
            );
            id
        };
        let path = self.directory.join(id.canonical_name());
        // Never overwrite: an existing canonical name is a collision.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    NativeIndexError::Invalid("index segment name collision")
                }
                _ => error.into(),
            })?;
        Ok(OpenSegment {
            id,
            path,
            file: SegmentFile {
                buffer: BufWriter::with_capacity(BUFFER_BYTES, file),
                written: 0,
            },
            pin,
        })
    }

    fn mark_sealed(&self, id: IndexStoreFileId, sealed_len: u64) -> Result<(), NativeIndexError> {
        let mut catalog = self.catalog.lock();
        let entry = catalog
            .files
            .get_mut(&id)
            .filter(|entry| entry.state == FileState::Writing)
            .ok_or(NativeIndexError::Invalid("sealed index segment state"))?;
        entry.state = FileState::Sealed;
        entry.sealed_len = sealed_len;
        Ok(())
    }
}

fn sync_directory(directory: &Path) -> std::io::Result<()> {
    File::open(directory)?.sync_all()
}

/// The fixed append buffer of an open segment and the count of bytes given
/// to it: the segment length once the buffer is flushed.
struct SegmentFile {
    buffer: BufWriter<File>,
    written: u64,
}

impl Write for SegmentFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.buffer.write(bytes)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.flush()
    }
}

struct OpenSegment {
    id: IndexStoreFileId,
    path: PathBuf,
    file: SegmentFile,
    pin: Arc<FilePin>,
}

enum Slot {
    /// A cell without a retained row: no frame.
    Empty,
    Frame {
        frame: CellFrame,
        cut: IndexReadCut,
        floor: IndexReadCut,
    },
}

/// The append side of one stream, separate from the encoder so that the
/// encoder can borrow it while it writes.
struct StreamOutput {
    store: Arc<IndexStore>,
    stream: u16,
    open: Option<OpenSegment>,
    /// Framed length promised to the encoder for the frame it writes now.
    promised: Option<u64>,
    /// Results of the open group, in append order. At most `GROUP_CELLS`.
    slots: Vec<Slot>,
    /// Results of a group sealed inside `append`, not yet returned. They
    /// precede `slots` in append order.
    ready: Vec<Option<SortedBlock>>,
    failed: bool,
}

impl StreamOutput {
    fn usable(&self) -> Result<(), NativeIndexError> {
        if self.failed {
            return Err(NativeIndexError::Invalid("index store writer failed"));
        }
        Ok(())
    }

    /// Discard everything not yet returned. The files stay as orphans.
    fn fail(&mut self) {
        self.failed = true;
        self.open = None;
        self.promised = None;
        self.slots.clear();
        self.ready.clear();
    }

    /// Writer and offset of a frame of `framed_len` bytes. Seals the open
    /// group first if the frame would exceed a group bound.
    fn reserve(&mut self, framed_len: u64) -> Result<(&mut SegmentFile, u64), NativeIndexError> {
        if self.slots.len() >= GROUP_CELLS
            || self
                .open
                .as_ref()
                .is_some_and(|open| open.file.written.saturating_add(framed_len) > GROUP_BYTES)
        {
            self.seal_group()?;
        }
        let open = match &mut self.open {
            Some(open) => open,
            empty => empty.insert(self.store.create_segment(self.stream)?),
        };
        self.promised = Some(framed_len);
        let offset = open.file.written;
        Ok((&mut open.file, offset))
    }

    /// Record the frame the encoder wrote after [`Self::reserve`].
    fn record(
        &mut self,
        frame: CellFrame,
        cut: IndexReadCut,
        floor: IndexReadCut,
    ) -> Result<(), NativeIndexError> {
        let open = self
            .open
            .as_ref()
            .ok_or(NativeIndexError::Invalid("index frame without a segment"))?;
        // The encoder wrote exactly the promised frame at the segment end, so
        // every recorded range lies within the sealed length.
        if self.promised.take() != Some(frame.bytes)
            || frame.offset.checked_add(frame.bytes) != Some(open.file.written)
        {
            return Err(NativeIndexError::Invalid("index frame length"));
        }
        self.slots.push(Slot::Frame { frame, cut, floor });
        Ok(())
    }

    fn push_empty(&mut self) -> Result<(), NativeIndexError> {
        if self.slots.len() >= GROUP_CELLS {
            self.seal_group()?;
        }
        self.slots.push(Slot::Empty);
        Ok(())
    }

    /// Make the open group durable and move its results to `ready`: flush,
    /// one file sync, one directory sync, then one read-only mapping.
    fn seal_group(&mut self) -> Result<(), NativeIndexError> {
        let Some(open) = self.open.take() else {
            // A group without a segment has no frame.
            for slot in self.slots.drain(..) {
                match slot {
                    Slot::Empty => self.ready.push(None),
                    Slot::Frame { .. } => {
                        return Err(NativeIndexError::Invalid("index frame without a segment"));
                    }
                }
            }
            return Ok(());
        };
        let OpenSegment {
            id,
            path,
            file,
            pin,
        } = open;
        let sealed_len = file.written;
        let file = file
            .buffer
            .into_inner()
            .map_err(|error| NativeIndexError::from(error.into_error()))?;
        file.sync_all()?;
        sync_directory(&self.store.directory)?;
        let map = map_file(&file, None)?;
        if map.len() as u64 != sealed_len {
            return Err(NativeIndexError::Invalid("sealed index segment length"));
        }
        self.store.mark_sealed(id, sealed_len)?;
        let map = Bytes::from_owner(PinnedMap { map, _pin: pin });
        for slot in self.slots.drain(..) {
            let Slot::Frame { frame, cut, floor } = slot else {
                self.ready.push(None);
                continue;
            };
            // `record` checked that the range ends within the sealed length,
            // and a frame holds its eight-byte header and a row count.
            let start = usize::try_from(frame.offset)
                .map_err(|_| NativeIndexError::Invalid("index frame offset"))?;
            let end = usize::try_from(frame.offset + frame.bytes)
                .map_err(|_| NativeIndexError::Invalid("index frame end"))?;
            self.ready.push(Some(SortedBlock {
                bytes: map.slice(start + 8..end),
                count: frame.count,
                mapped: true,
                cut,
                file: Some(CellFile {
                    path: path.clone(),
                    offset: frame.offset,
                    bytes: frame.bytes,
                    digest: frame.digest,
                    floor,
                }),
                retained_bytes: frame.retained_bytes,
                retained_versions: frame.retained_versions,
            }));
        }
        Ok(())
    }

    /// Seal the open group and emit every result not yet returned, in
    /// append order. On error nothing is emitted and the writer fails.
    fn seal_with(
        &mut self,
        mut emit: impl FnMut(Option<SortedBlock>),
    ) -> Result<(), NativeIndexError> {
        self.usable()?;
        if let Err(error) = self.seal_group() {
            self.fail();
            return Err(error);
        }
        for block in self.ready.drain(..) {
            emit(block);
        }
        Ok(())
    }
}

impl Drop for StreamOutput {
    fn drop(&mut self) {
        self.store.release_stream(self.stream);
    }
}

/// The private append stream of one cell flusher.
pub(crate) struct IndexStoreWriter {
    encoder: CellEncoder,
    output: StreamOutput,
}

impl IndexStoreWriter {
    /// Claim `stream_id` (one writer per stream at a time). `scratch_limit`
    /// is the encoder payload scratch bound `snapshot::cell_scratch_limit`.
    pub(crate) fn new(
        store: &Arc<IndexStore>,
        stream_id: u16,
        scratch_limit: usize,
    ) -> Result<Self, NativeIndexError> {
        store.claim_stream(stream_id)?;
        Ok(Self {
            encoder: CellEncoder::new(scratch_limit),
            output: StreamOutput {
                store: Arc::clone(store),
                stream: stream_id,
                open: None,
                promised: None,
                slots: Vec::with_capacity(GROUP_CELLS),
                ready: Vec::with_capacity(GROUP_CELLS),
                failed: false,
            },
        })
    }

    /// Encode `cell` once and buffer its frame in the open segment, creating
    /// it on the first frame. Nothing is synced. Returns the framed bytes
    /// written; `0` means no retained row and no frame. Any error is
    /// fail-stop.
    pub(crate) fn append(&mut self, cell: &PreparedCell) -> Result<u64, NativeIndexError> {
        self.output.usable()?;
        let source = cell.encode_source();
        let output = &mut self.output;
        let result = self
            .encoder
            .encode_source(&source, move |framed_len| output.reserve(framed_len))
            .and_then(|frame| match frame {
                None => self.output.push_empty().map(|()| 0),
                Some(frame) => self
                    .output
                    .record(frame, source.cut.clone(), source.floor.clone())
                    .map(|()| frame.bytes),
            });
        if result.is_err() {
            self.output.fail();
        }
        result
    }

    /// Make every appended frame durable (segment sync, then directory sync),
    /// map it read-only, and push one base per append since the last seal, in
    /// append order. Without appends: no file and no output. On error nothing
    /// is pushed and the writer fails.
    pub(crate) fn seal(&mut self, bases: &mut Vec<NewCellBase>) -> Result<(), NativeIndexError> {
        self.output.seal_with(|block| {
            bases.push(block.map_or_else(NewCellBase::empty, NewCellBase::frame));
        })
    }
}

#[cfg(test)]
#[path = "index_store_tests.rs"]
mod tests;
