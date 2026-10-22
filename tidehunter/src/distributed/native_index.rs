//! Serving experimental native cell index for sharded Value WAL references.
//!
//! This uses native PendingTable/Transaction and the ordered multiversion overlay
//! and sorted-search primitives shared with IndexTable. It deliberately has an
//! explicit experimental format: legacy LargeTable, IndexFormat and Db remain
//! unchanged. Values are never stored here. Tidehunter paper §§3.1–3.4 require
//! complete publication, logical checkpoint cuts and reconstructible indexes.
//!
//! Immutable shared cell containers retain pageable history independently of the bounded
//! resident dirty overlay. A snapshot is not authority to delete any Value WAL.

use super::codec::CompleteBatch;
use super::index_reference::LogicalValueId;
use super::native_publication::NativePendingSink;
use super::types::{
    BatchId, BatchVersion, MemberChecksum, MutationVersion, PayloadDigest, PhysicalValueAddress,
    RecordDescriptor, RecordKind, ValueRecordReference,
};
use crate::cell::CellId;
use crate::crc::{CrcFrame, IntoBytesFixed};
use crate::index::pending_table::PendingTable;
use crate::index::versioned_overlay::{Overlay, insert_unique_by, lower_bound_by, matching_prefix};
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace, KeySpaceDesc, KeyType};
use blake2::{Blake2b, Digest};
use bytes::BytesMut;
use minibytes::Bytes;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::ops::Bound;
use std::path::Path;
use typenum::U32;

mod retention;
mod snapshot;
pub use retention::IndexReadPin;
use retention::{CaptureLease, Retention};
use snapshot::FrozenCell;
pub(crate) use snapshot::{PersistedIndexImage, PreparedIndexImage};

const MAGIC: &[u8; 8] = b"THINDEX1";
const FORMAT: u16 = 4;
const REFERENCE_BYTES: usize = 134;
const MAX_IMAGE_BYTES: usize = 1 << 30;
const VERSION_CHARGE: usize = std::mem::size_of::<ValueRecordReference>() + 32;
const CELL_CHARGE: usize = std::mem::size_of::<CellIndex>() + 256;

fn cell_directory_charge(cell: &CellId) -> usize {
    CELL_CHARGE + cell.len()
}

/// `At` includes that exact mutation. `BeforeAll` is the empty checkpoint;
/// unlike an absent optional frontier, it can never accidentally mean latest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexReadCut {
    Latest,
    BeforeAll,
    At(MutationVersion),
}
impl IndexReadCut {
    fn contains(self, reference: &ValueRecordReference) -> bool {
        match self {
            Self::Latest => true,
            Self::BeforeAll => false,
            Self::At(version) => reference.mutation_version() <= version,
        }
    }
}

#[derive(Debug)]
pub enum NativeIndexError {
    Invalid(&'static str),
    Capacity,
    ConflictingVersion,
    PendingPublication,
    ExpiredCut,
    RetentionInProgress,
    CheckpointInProgress,
    Io(std::io::Error),
}
impl fmt::Display for NativeIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(reason) => write!(f, "invalid native reference index: {reason}"),
            Self::Capacity => f.write_str("native reference index metadata capacity exceeded"),
            Self::ConflictingVersion => {
                f.write_str("logical mutation has conflicting index references")
            }
            Self::PendingPublication => f.write_str("index has pending publication entries"),
            Self::ExpiredCut => f.write_str("read cut is below the persisted retention floor"),
            Self::RetentionInProgress => {
                f.write_str("read cut conflicts with retention in progress")
            }
            Self::CheckpointInProgress => f.write_str("an index checkpoint is already in progress"),
            Self::Io(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for NativeIndexError {}
impl From<std::io::Error> for NativeIndexError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NativeIndexStats {
    pub cells: usize,
    pub unloaded_cells: usize,
    pub retained_versions: usize,
    pub pending_operations: usize,
    /// Resident dirty-reference, key and cell-directory charge; not process RSS.
    pub charged_metadata_bytes: usize,
    /// Bounded frozen capture, separately limited by the configured budget.
    pub transient_metadata_bytes: usize,
}

#[derive(Default)]
struct CellIndex {
    pending: PendingTable<ValueRecordReference>,
    overlay: Overlay<ValueRecordReference>,
    base: Option<SortedBlock>,
}

/// One database's native-routed cells with typed logical versions and locations.
/// The owner serializes publication; get/scan use immutable references. This is a
/// real reference index, not a map storing a second copy of application values.
pub struct NativeIndex {
    shape: KeyShape,
    database: [u8; 16],
    max_metadata_bytes: usize,
    cells: HashMap<(KeySpace, CellId), CellIndex>,
    touched: HashSet<(KeySpace, CellId)>,
    retained_bytes: usize,
    retained_versions: usize,
    latest: Option<MutationVersion>,
    resident_overlay_bytes: usize,
    resident_directory_bytes: usize,
    resident_loaded_bytes: usize,
    retention: std::sync::Arc<parking_lot::Mutex<Retention>>,
}

/// Digest of the exact durable CRC-framed image, usable by its manifest owner.
#[derive(Clone, Copy)]
pub struct IndexImageMetadata {
    pub bytes: u64,
    pub digest: [u8; 32],
}

/// Scan-local merge state with one base/overlay frontier per native cell, never
/// a key/value image. A changed immutable base is re-seeked by the last key, so
/// publication and checkpoint installation may proceed between scan pages.
/// The caller retains its cut pin across pages; cached keys own their mappings.
pub(crate) struct NativeIndexScan {
    cells: Vec<CellScan>,
    heads: BinaryHeap<ScanHead>,
    reverse: bool,
    cut: IndexReadCut,
}

/// Retained-history traversal for native WAL-style relocation filters. Latest
/// rows cannot establish a safe cutoff: overwritten/deleted puts can still Stop.
/// A retained-floor pin protects all traversed history, not just latest heads.
/// The logical key/version frontier survives unloading and base replacement;
/// values are resolved one at a time without cloning retained history.
pub(crate) struct NativeIndexHistory {
    cells: Vec<(KeySpace, CellId)>,
    after: Option<(Bytes, MutationVersion)>,
    _pin: IndexReadPin,
    cut: IndexReadCut,
}

impl NativeIndexHistory {
    pub(crate) fn next(&mut self, index: &NativeIndex) -> Option<(Bytes, ValueRecordReference)> {
        loop {
            let identity = self.cells.last()?;
            let cell = &index.cells[identity];
            if let Some((key, reference)) = cell.next_history(self.after.as_ref(), self.cut) {
                self.after = Some((key.clone(), reference.mutation_version()));
                return Some((key, reference));
            }
            self.cells.pop();
            self.after = None;
        }
    }
}

impl NativeIndexScan {
    pub(crate) fn peek(&self) -> Option<(&Bytes, &ValueRecordReference)> {
        self.heads.peek().map(|head| (&head.key, &head.reference))
    }

    pub(crate) fn next(&mut self, index: &NativeIndex) -> Option<(Bytes, ValueRecordReference)> {
        let head = self.heads.pop()?;
        self.advance(index, head.cell);
        Some((head.key, head.reference))
    }

    fn advance(&mut self, index: &NativeIndex, cell: usize) {
        while let Some((key, reference)) = self.cells[cell].next(index, self.reverse, self.cut) {
            if reference.descriptor.kind == RecordKind::Put {
                self.heads.push(ScanHead {
                    key,
                    reference,
                    cell,
                    reverse: self.reverse,
                });
                return;
            }
        }
    }
}

struct CellScan {
    identity: (KeySpace, CellId),
    overlay: Option<(Bytes, ValueRecordReference)>,
    base: Option<(Bytes, ValueRecordReference)>,
    base_position: usize,
    base_owner: Option<Bytes>,
    after: Option<Bytes>,
}

impl CellScan {
    fn new(
        identity: (KeySpace, CellId),
        source: &CellIndex,
        after: Option<&Bytes>,
        reverse: bool,
        cut: IndexReadCut,
    ) -> Self {
        let base_position = Self::base_position(source, after.map(|key| key.as_ref()), reverse);
        let mut cursor = Self {
            identity,
            overlay: source.next_overlay(after.map(|key| key.as_ref()), reverse, cut),
            base: None,
            base_position,
            base_owner: source.base.as_ref().map(|block| block.bytes.clone()),
            after: after.cloned(),
        };
        cursor.advance_base(source, reverse, cut);
        cursor
    }

    fn base_position(source: &CellIndex, after: Option<&[u8]>, reverse: bool) -> usize {
        source.base.as_ref().map_or(0, |block| {
            if reverse {
                after.map_or(block.count, |key| {
                    lower_bound_by(block.count, |n| block.entry(n).0 < key)
                })
            } else {
                after.map_or(0, |key| {
                    lower_bound_by(block.count, |n| block.entry(n).0 <= key)
                })
            }
        })
    }

    fn advance_base(&mut self, source: &CellIndex, reverse: bool, cut: IndexReadCut) {
        self.base = None;
        let Some(block) = &source.base else { return };
        while if reverse {
            self.base_position != 0
        } else {
            self.base_position < block.count
        } {
            let position = if reverse {
                self.base_position -= 1;
                self.base_position
            } else {
                let position = self.base_position;
                self.base_position += 1;
                position
            };
            if let Some(reference) = block.selected(position, cut) {
                self.base = Some((
                    block.bytes.slice_to_bytes(block.entry(position).0),
                    reference,
                ));
                return;
            }
        }
    }

    fn next(
        &mut self,
        index: &NativeIndex,
        reverse: bool,
        cut: IndexReadCut,
    ) -> Option<(Bytes, ValueRecordReference)> {
        let source = &index.cells[&self.identity];
        let replaced = match (&self.base_owner, &source.base) {
            (None, None) => false,
            (Some(owner), Some(block)) => {
                owner.as_ptr() != block.bytes.as_ptr() || owner.len() != block.bytes.len()
            }
            _ => true,
        };
        if replaced {
            self.base_position = Self::base_position(source, self.after.as_deref(), reverse);
            self.base_owner = source.base.as_ref().map(|block| block.bytes.clone());
            self.overlay = source.next_overlay(self.after.as_deref(), reverse, cut);
            self.advance_base(source, reverse, cut);
        }
        let order = match (&self.overlay, &self.base) {
            (None, None) => return None,
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some((a, _)), Some((b, _))) => {
                if reverse {
                    b.cmp(a)
                } else {
                    a.cmp(b)
                }
            }
        };
        let mut result = if order != Ordering::Greater {
            let result = self.overlay.take().expect("overlay head");
            self.overlay = source.next_overlay(Some(&result.0), reverse, cut);
            result
        } else {
            self.base.take().expect("base head")
        };
        if order != Ordering::Less {
            if order == Ordering::Equal {
                result.1 = newest(
                    Some(result.1),
                    self.base.take().map(|(_, reference)| reference),
                )
                .expect("merged head");
            }
            self.advance_base(source, reverse, cut);
        }
        self.after = Some(result.0.clone());
        Some(result)
    }
}

impl NativeIndex {
    pub fn new(
        shape: &KeyShape,
        database: [u8; 16],
        max_metadata_bytes: usize,
    ) -> Result<Self, NativeIndexError> {
        if max_metadata_bytes == 0 || max_metadata_bytes > MAX_IMAGE_BYTES / 2 {
            return Err(NativeIndexError::Invalid(
                "metadata budget must be in 1..=512 MiB",
            ));
        }
        let shape = KeyShape::from_yaml(
            &shape
                .to_yaml()
                .map_err(|_| NativeIndexError::Invalid("schema serialization"))?,
        )
        .map_err(|_| NativeIndexError::Invalid("schema decoding"))?;
        Ok(Self {
            shape,
            database,
            max_metadata_bytes,
            cells: HashMap::new(),
            touched: HashSet::new(),
            retained_bytes: 0,
            retained_versions: 0,
            latest: None,
            resident_overlay_bytes: 0,
            resident_directory_bytes: 0,
            resident_loaded_bytes: 0,
            retention: Default::default(),
        })
    }

    /// Owner admission must include previously admitted outstanding reservations
    /// in `additional_bytes`, before emitting authoritative Value WAL records.
    pub fn check_capacity(&self, additional_bytes: usize) -> Result<(), NativeIndexError> {
        if self
            .resident_bytes()
            .checked_add(additional_bytes)
            .is_none_or(|total| total > self.max_metadata_bytes)
        {
            return Err(NativeIndexError::Capacity);
        }
        Ok(())
    }

    pub fn estimated_batch_bytes(&self, batch: &CompleteBatch) -> Result<usize, NativeIndexError> {
        if batch.id().database != self.database {
            return Err(NativeIndexError::Invalid("foreign database"));
        }
        batch
            .mutations()
            .iter()
            .try_fold(0usize, |total, mutation| {
                let (_, reduced, cell) = self.route(mutation.keyspace(), mutation.key())?;
                let cell_charge = if self
                    .cells
                    .contains_key(&(mutation.keyspace(), cell.clone()))
                {
                    0
                } else {
                    cell_directory_charge(&cell)
                };
                total
                    .checked_add(VERSION_CHARGE + reduced.len() + cell_charge)
                    .ok_or(NativeIndexError::Capacity)
            })
    }

    /// Capacity charge for locally validated native routing. Untrusted batches
    /// still use estimated_batch_bytes, including database/schema validation.
    pub(crate) fn estimated_routed_bytes(
        &self,
        operations: &[crate::batch::PendingOp],
    ) -> Result<usize, NativeIndexError> {
        operations.iter().try_fold(0usize, |total, operation| {
            total
                .checked_add(
                    VERSION_CHARGE
                        + operation.reduced_key().len()
                        + if self
                            .cells
                            .contains_key(&(operation.ks(), operation.cell_id().clone()))
                        {
                            0
                        } else {
                            cell_directory_charge(operation.cell_id())
                        },
                )
                .ok_or(NativeIndexError::Capacity)
        })
    }

    pub fn retained_metadata_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn resident_bytes(&self) -> usize {
        self.resident_overlay_bytes + self.resident_directory_bytes + self.resident_loaded_bytes
    }

    fn resolve_cut(&self, cut: IndexReadCut) -> IndexReadCut {
        if cut == IndexReadCut::Latest {
            self.latest_cut()
        } else {
            cut
        }
    }

    pub fn stats(&self) -> NativeIndexStats {
        NativeIndexStats {
            cells: self.cells.len(),
            unloaded_cells: self
                .cells
                .values()
                .filter(|cell| cell.base.as_ref().is_some_and(|block| block.mapped))
                .count(),
            retained_versions: self.retained_versions,
            pending_operations: self.cells.values().map(|cell| cell.pending.len()).sum(),
            charged_metadata_bytes: self.resident_bytes(),
            transient_metadata_bytes: self.transient_bytes(),
        }
    }

    pub fn latest_cut(&self) -> IndexReadCut {
        self.latest
            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
    }

    /// Promote committed native pending entries while retaining every logical
    /// version. Invoke inside the authority's atomic publication callback. Errors
    /// can follow partial promotion and therefore require authority recovery.
    pub fn promote_committed(&mut self) -> Result<usize, NativeIndexError> {
        let touched = std::mem::take(&mut self.touched);
        let mut promoted = 0;
        let mut resident = self.resident_bytes();
        for (keyspace, cell_id) in touched {
            let cell = self
                .cells
                .get_mut(&(keyspace, cell_id.clone()))
                .expect("touched cell exists");
            let (changes, _) = cell.pending.take_committed();
            if cell.pending.len() != 0 {
                self.touched.insert((keyspace, cell_id));
            }
            for change in changes {
                let reference = change.value;
                if reference.batch_id.database != self.database
                    || reference.descriptor.keyspace != keyspace.0
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
                let total = self
                    .retained_bytes
                    .checked_add(charge)
                    .ok_or(NativeIndexError::Capacity)?;
                resident = resident
                    .checked_add(charge)
                    .ok_or(NativeIndexError::Capacity)?;
                if resident > self.max_metadata_bytes {
                    return Err(NativeIndexError::Capacity);
                }
                let key = change.key.into_owned();
                let values = cell.overlay.entry(key).or_default();
                if let Some(old) = insert_unique_by(values, reference, |a, b| {
                    a.mutation_version().cmp(&b.mutation_version())
                }) && old != reference
                {
                    return Err(NativeIndexError::ConflictingVersion);
                }
                self.retained_bytes = total;
                self.resident_overlay_bytes += charge;
                self.retained_versions += 1;
                self.latest = Some(self.latest.map_or(reference.mutation_version(), |v| {
                    v.max(reference.mutation_version())
                }));
                promoted += 1;
            }
        }
        Ok(promoted)
    }

    pub fn get(
        &self,
        keyspace: KeySpace,
        full_key: &[u8],
        cut: IndexReadCut,
    ) -> Result<Option<ValueRecordReference>, NativeIndexError> {
        self.validate_cut(cut)?;
        let (_, reduced, cell) = self.route(keyspace, full_key)?;
        Ok(self
            .cells
            .get(&(keyspace, cell))
            .and_then(|cell| cell.get(&reduced, cut))
            .filter(|reference| reference.descriptor.kind == RecordKind::Put))
    }

    /// Ordered index references; cursors and ordering are reduced native index
    /// keys. The caller resolves values and performs required full-key checks.
    /// Native hash-index keyspaces do not promise application-key ordering.
    pub fn scan(
        &self,
        keyspace: KeySpace,
        after_reduced: Option<&[u8]>,
        reverse: bool,
        limit: usize,
        cut: IndexReadCut,
    ) -> Result<Vec<(Bytes, ValueRecordReference)>, NativeIndexError> {
        let mut cursor = self.scan_cursor(keyspace, after_reduced, reverse, cut)?;
        let mut output = Vec::with_capacity(limit.min(4096));
        while output.len() < limit {
            let Some(row) = cursor.next(self) else {
                break;
            };
            output.push(row);
        }
        Ok(output)
    }

    pub(crate) fn scan_cursor(
        &self,
        keyspace: KeySpace,
        after_reduced: Option<&[u8]>,
        reverse: bool,
        cut: IndexReadCut,
    ) -> Result<NativeIndexScan, NativeIndexError> {
        self.scan_cursor_impl(keyspace, after_reduced, reverse, cut, false)
    }

    /// Physical reduced-key enumeration for full-table adapters, including hash
    /// indexes. This does not promise application-key order for hashed keys.
    pub(crate) fn scan_reduced(
        &self,
        keyspace: KeySpace,
        after: Option<&[u8]>,
        reverse: bool,
        limit: usize,
        cut: IndexReadCut,
    ) -> Result<Vec<(Bytes, ValueRecordReference)>, NativeIndexError> {
        let mut cursor = self.scan_cursor_impl(keyspace, after, reverse, cut, true)?;
        let mut rows = Vec::with_capacity(limit.min(256));
        while rows.len() < limit.min(256) {
            match cursor.next(self) {
                Some(row) => rows.push(row),
                None => break,
            }
        }
        Ok(rows)
    }

    pub(crate) fn relocation_history(
        &self,
        keyspace: KeySpace,
        cut: IndexReadCut,
    ) -> Result<NativeIndexHistory, NativeIndexError> {
        self.descriptor(keyspace)?;
        self.validate_cut(cut)?;
        Ok(NativeIndexHistory {
            cells: self
                .cells
                .keys()
                .filter(|(ks, _)| *ks == keyspace)
                .cloned()
                .collect(),
            after: None,
            _pin: self.pin_cut(self.retention_floor())?,
            cut: if cut == IndexReadCut::Latest {
                self.latest_cut()
            } else {
                cut
            },
        })
    }

    fn scan_cursor_impl(
        &self,
        keyspace: KeySpace,
        after_reduced: Option<&[u8]>,
        reverse: bool,
        cut: IndexReadCut,
        allow_hash: bool,
    ) -> Result<NativeIndexScan, NativeIndexError> {
        let descriptor = self.descriptor(keyspace)?;
        self.validate_cut(cut)?;
        if !allow_hash && matches!(descriptor.key_indexing(), KeyIndexing::Hash) {
            return Err(NativeIndexError::Invalid(
                "ordered scan is unsupported for hashed keys",
            ));
        }
        let cut = if cut == IndexReadCut::Latest {
            self.latest_cut()
        } else {
            cut
        };
        let after_reduced = after_reduced.map(Bytes::copy_from_slice);
        let cells = if cut == IndexReadCut::BeforeAll {
            Vec::new()
        } else {
            self.cells
                .iter()
                .filter(|(identity, _)| identity.0 == keyspace)
                .map(|(identity, source)| {
                    CellScan::new(
                        identity.clone(),
                        source,
                        after_reduced.as_ref(),
                        reverse,
                        cut,
                    )
                })
                .collect()
        };
        let mut cursor = NativeIndexScan {
            cells,
            heads: BinaryHeap::new(),
            reverse,
            cut,
        };
        for cell in 0..cursor.cells.len() {
            cursor.advance(self, cell);
        }
        Ok(cursor)
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
        &self,
        cut: IndexReadCut,
    ) -> Result<PreparedIndexImage, NativeIndexError> {
        self.prepare_compacted_image(cut, self.retention_floor())
    }

    /// Reserve a supported-history floor, atomically clamped to the oldest pin.
    /// `floor` is the requested maximum; the installed durable floor can lag it.
    /// The authority must durably adopt the root before installing this image.
    pub(crate) fn prepare_compacted_image(
        &self,
        cut: IndexReadCut,
        floor: IndexReadCut,
    ) -> Result<PreparedIndexImage, NativeIndexError> {
        if self.cells.values().any(|cell| cell.pending.len() != 0) {
            return Err(NativeIndexError::PendingPublication);
        }
        let cut = self.resolve_cut(cut);
        let lease = self.capture_lease(cut, floor)?;
        let mut cells = Vec::with_capacity(self.cells.len());
        for (identity, cell) in &self.cells {
            let overlay = cell
                .overlay
                .iter()
                .filter_map(|(key, values)| {
                    let selected = matching_prefix(values, |reference| cut.contains(reference));
                    (!selected.is_empty())
                        .then(|| (key.clone(), selected.iter().copied().collect()))
                })
                .collect();
            cells.push(FrozenCell {
                identity: identity.clone(),
                base: cell.base.clone(),
                overlay,
            });
        }
        cells.sort_by(|a, b| {
            a.identity
                .0
                .0
                .cmp(&b.identity.0.0)
                .then_with(|| a.identity.1.cmp(&b.identity.1))
        });
        Ok(PreparedIndexImage::new(
            self.database,
            schema_digest(&self.shape)?,
            cells,
            cut,
            lease,
        ))
    }

    /// Native LargeTable's unmerge rule: remove exactly the captured references,
    /// never replace the current overlay with an earlier view of post-cut writes.
    pub(crate) fn install_image(&mut self, persisted: PersistedIndexImage) {
        for captured in persisted.cells {
            let Some(cell) = self.cells.get_mut(&captured.identity) else {
                continue;
            };
            // A caller may persist an older exact cut for export. Installing it
            // must not discard newer history that has already been unloaded.
            if cell.base.as_ref().is_some_and(|base| {
                retention::position(base.cut) > retention::position(persisted.cut)
            }) {
                continue;
            }
            cell.overlay.retain(|key, values| {
                if let Some(flushed) = captured.overlay.get(key) {
                    values.retain(|reference| {
                        let remove = flushed
                            .binary_search_by_key(&reference.mutation_version(), |value| {
                                value.mutation_version()
                            })
                            .is_ok_and(|at| flushed[at] == *reference);
                        if remove {
                            self.resident_overlay_bytes -= VERSION_CHARGE + key.len();
                        }
                        !remove
                    });
                }
                !values.is_empty()
            });
            cell.base = captured.base;
        }
        persisted.lease.install();
        // Compaction changes durable history, not just the residency of rows.
        self.recount_retained();
    }

    fn recount_retained(&mut self) {
        self.resident_directory_bytes = self
            .cells
            .keys()
            .map(|(_, cell)| cell_directory_charge(cell))
            .sum();
        self.resident_loaded_bytes = self
            .cells
            .values()
            .filter_map(|cell| cell.base.as_ref())
            .filter(|block| !block.mapped)
            .map(|block| block.bytes.len())
            .sum();
        self.retained_bytes = self.resident_overlay_bytes;
        self.retained_versions = self
            .cells
            .values()
            .map(|cell| {
                cell.overlay
                    .values()
                    .map(|values| values.len())
                    .sum::<usize>()
            })
            .sum();
        for block in self.cells.values().filter_map(|cell| cell.base.as_ref()) {
            self.retained_bytes += block.retained_bytes;
            self.retained_versions += block.retained_versions;
        }
    }

    /// Unique immutable containers reachable from the installed index. The
    /// authority must union these with every retained/protected checkpoint root.
    pub(crate) fn reachable_files(&self) -> Vec<String> {
        snapshot::container_names(
            self.cells
                .values()
                .filter_map(|cell| cell.base.as_ref())
                .filter_map(|block| block.file.as_ref()),
        )
    }

    /// Conservative payload reachability for one physical WAL file. Walk the
    /// actual cells, including destroyed keyspaces, and every retained version;
    /// latest-only heads are insufficient for old-cut readers and Stop filters.
    /// Tombstones need no payload once their derived index is durably adopted.
    pub(crate) fn references_wal_file(
        &self,
        partition: u32,
        generation: u64,
        start: u64,
        end: u64,
    ) -> bool {
        let references = |reference: &ValueRecordReference| {
            let address = reference.batch_address;
            reference.descriptor.kind == RecordKind::Put
                && address.partition == partition
                && address.segment_generation == generation
                && address.offset < end
                && address
                    .offset
                    .checked_add(u64::from(address.len))
                    .is_none_or(|record_end| record_end > start)
        };
        self.cells.values().any(|cell| {
            cell.overlay
                .values()
                .any(|values| values.iter().any(&references))
                || cell.base.as_ref().is_some_and(|block| {
                    (0..block.count).any(|n| {
                        block.entry(n).1.chunks_exact(REFERENCE_BYTES).any(|bytes| {
                            references(
                                &decode_reference(bytes).expect("validated immutable reference"),
                            )
                        })
                    })
                })
        })
    }

    pub fn open_snapshot(
        path: &Path,
        shape: &KeyShape,
        database: [u8; 16],
        max_metadata_bytes: usize,
    ) -> Result<Self, NativeIndexError> {
        snapshot::open(path, shape, database, max_metadata_bytes)
    }

    /// Materialize compact cell metadata in heap buffers. User values remain in
    /// their partition WAL; this does not expand blocks into per-key heap maps.
    pub fn reload(&mut self) -> Result<(), NativeIndexError> {
        let additional: usize = self
            .cells
            .values()
            .filter_map(|cell| cell.base.as_ref())
            .filter(|block| block.mapped)
            .map(|block| block.bytes.len())
            .sum();
        self.check_capacity(additional)?;
        for cell in self.cells.values_mut() {
            if let Some(block) = &mut cell.base
                && block.mapped
            {
                block.bytes = block.bytes.as_ref().to_vec().into();
                block.mapped = false;
            }
        }
        self.resident_loaded_bytes += additional;
        Ok(())
    }

    fn descriptor(&self, keyspace: KeySpace) -> Result<&KeySpaceDesc, NativeIndexError> {
        self.shape
            .iter_ks()
            .find(|descriptor| descriptor.id() == keyspace && !descriptor.destroyed())
            .ok_or(NativeIndexError::Invalid("unknown or destroyed keyspace"))
    }

    fn route<'key>(
        &self,
        keyspace: KeySpace,
        key: &'key [u8],
    ) -> Result<(&KeySpaceDesc, Cow<'key, [u8]>, CellId), NativeIndexError> {
        let descriptor = self.descriptor(keyspace)?;
        if key.len() > crate::db::MAX_KEY_LEN {
            return Err(NativeIndexError::Invalid("key too long"));
        }
        match descriptor.key_indexing() {
            KeyIndexing::Fixed(len) if key.len() != *len => {
                return Err(NativeIndexError::Invalid("key length"));
            }
            KeyIndexing::Reduction(len, range)
                if key.len() != *len || range.start > range.end || range.end > key.len() =>
            {
                return Err(NativeIndexError::Invalid("key reduction"));
            }
            _ => {}
        }
        // Fixed, variable and reduced keys borrow the caller's bytes; only hash
        // indexing allocates its native digest. No owned key escapes this read
        // routing path, and the validation above protects reduction slicing.
        let reduced = descriptor.reduce_key(key);
        validate_cell_routing(descriptor, &reduced)?;
        let cell = descriptor.cell_id(&reduced);
        Ok((descriptor, reduced, cell))
    }
}

/// A single ordered candidate per native cell. Merge costs O(log active cells)
/// per key; physical addresses never participate in application-key ordering.
struct ScanHead {
    key: Bytes,
    reference: ValueRecordReference,
    cell: usize,
    reverse: bool,
}
impl PartialEq for ScanHead {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.cell == other.cell && self.reverse == other.reverse
    }
}
impl Eq for ScanHead {}
impl PartialOrd for ScanHead {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScanHead {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.reverse != other.reverse {
            return self.reverse.cmp(&other.reverse);
        }
        let order = self
            .key
            .cmp(&other.key)
            .then_with(|| self.cell.cmp(&other.cell));
        if self.reverse { order } else { order.reverse() }
    }
}

impl NativePendingSink for NativeIndex {
    type Error = NativeIndexError;
    fn with_pending_table(
        &mut self,
        keyspace: KeySpace,
        cell: &CellId,
        apply: impl FnOnce(&mut PendingTable<ValueRecordReference>),
    ) -> Result<(), Self::Error> {
        let descriptor = self.descriptor(keyspace)?;
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
        if !self.cells.contains_key(&identity) {
            self.check_capacity(cell_directory_charge(&identity.1))?;
            self.resident_directory_bytes += cell_directory_charge(&identity.1);
        }
        self.touched.insert(identity.clone());
        apply(&mut self.cells.entry(identity).or_default().pending);
        Ok(())
    }
}

impl CellIndex {
    fn exact(&self, key: &[u8], version: MutationVersion) -> Option<ValueRecordReference> {
        let selected = self.get(key, IndexReadCut::At(version));
        selected.filter(|reference| reference.mutation_version() == version)
    }
    fn get(&self, key: &[u8], cut: IndexReadCut) -> Option<ValueRecordReference> {
        let overlay = self
            .overlay
            .get(key)
            .and_then(|values| matching_prefix(values, |value| cut.contains(value)).last())
            .copied();
        let base = self.base.as_ref().and_then(|block| block.get(key, cut));
        newest(overlay, base)
    }
    fn next_overlay(
        &self,
        previous: Option<&[u8]>,
        reverse: bool,
        cut: IndexReadCut,
    ) -> Option<(Bytes, ValueRecordReference)> {
        let start = previous.map_or(Bound::Unbounded, Bound::Excluded);
        if reverse {
            self.overlay
                .range::<[u8], _>((Bound::Unbounded, start))
                .rev()
                .find_map(|(key, values)| {
                    matching_prefix(values, |v| cut.contains(v))
                        .last()
                        .map(|reference| (key.clone(), *reference))
                })
        } else {
            self.overlay
                .range::<[u8], _>((start, Bound::Unbounded))
                .find_map(|(key, values)| {
                    matching_prefix(values, |v| cut.contains(v))
                        .last()
                        .map(|reference| (key.clone(), *reference))
                })
        }
    }

    fn next_history(
        &self,
        after: Option<&(Bytes, MutationVersion)>,
        cut: IndexReadCut,
    ) -> Option<(Bytes, ValueRecordReference)> {
        let base = self.base.as_ref().and_then(|block| {
            let first = after.map_or(0, |(key, _)| {
                lower_bound_by(block.count, |n| block.entry(n).0 < key.as_ref())
            });
            for n in first..block.count {
                let (key, versions) = block.entry(n);
                let previous = after
                    .filter(|(prior, _)| prior.as_ref() == key)
                    .map(|(_, version)| *version);
                let first = lower_bound_by(versions.len() / REFERENCE_BYTES, |n| {
                    let value =
                        decode_reference(&versions[n * REFERENCE_BYTES..(n + 1) * REFERENCE_BYTES])
                            .expect("validated immutable reference");
                    previous.is_some_and(|previous| value.mutation_version() <= previous)
                });
                for bytes in versions[first * REFERENCE_BYTES..].chunks_exact(REFERENCE_BYTES) {
                    let reference = decode_reference(bytes).expect("validated immutable reference");
                    if !cut.contains(&reference) {
                        break;
                    }
                    if reference.descriptor.kind == RecordKind::Put {
                        return Some((block.bytes.slice_to_bytes(key), reference));
                    }
                }
            }
            None
        });
        let lower = after.map_or(Bound::Unbounded, |(key, _)| Bound::Included(key.as_ref()));
        let overlay = self
            .overlay
            .range::<[u8], _>((lower, Bound::Unbounded))
            .find_map(|(key, versions)| {
                let previous = after
                    .filter(|(prior, _)| prior == key)
                    .map(|(_, version)| *version);
                let first = versions.partition_point(|value| {
                    previous.is_some_and(|previous| value.mutation_version() <= previous)
                });
                versions[first..]
                    .iter()
                    .take_while(|value| cut.contains(value))
                    .find(|value| value.descriptor.kind == RecordKind::Put)
                    .map(|reference| (key.clone(), *reference))
            });
        match (base, overlay) {
            (None, other) | (other, None) => other,
            (Some(a), Some(b)) => Some(
                if a.0
                    .cmp(&b.0)
                    .then_with(|| a.1.mutation_version().cmp(&b.1.mutation_version()))
                    != Ordering::Greater
                {
                    a
                } else {
                    b
                },
            ),
        }
    }
}

fn newest(
    a: Option<ValueRecordReference>,
    b: Option<ValueRecordReference>,
) -> Option<ValueRecordReference> {
    match (a, b) {
        (None, other) | (other, None) => other,
        (Some(a), Some(b)) => Some(if a.mutation_version() >= b.mutation_version() {
            a
        } else {
            b
        }),
    }
}

#[derive(Clone)]
struct SortedBlock {
    bytes: Bytes,
    count: usize,
    mapped: bool,
    cut: IndexReadCut,
    file: Option<snapshot::CellFile>,
    retained_bytes: usize,
    retained_versions: usize,
}
impl SortedBlock {
    fn validated(
        bytes: Bytes,
        mapped: bool,
        database: [u8; 16],
        keyspace: KeySpace,
        descriptor: &KeySpaceDesc,
        cell: &CellId,
        cut: IndexReadCut,
    ) -> Result<Self, NativeIndexError> {
        let mut cursor = Cursor::new(&bytes);
        let count = cursor.u32()? as usize;
        if count == 0 || count > cursor.remaining() / 4 {
            return Err(NativeIndexError::Invalid("block offset count"));
        }
        let offsets = cursor.take(count * 4)?;
        let mut expected = cursor.position;
        let mut prior_key: Option<&[u8]> = None;
        let mut retained_bytes = 0;
        let mut retained_versions = 0;
        for offset in offsets.chunks_exact(4) {
            let offset = u32::from_be_bytes(offset.try_into().unwrap()) as usize;
            if offset != expected {
                return Err(NativeIndexError::Invalid("noncanonical block offset"));
            }
            let length = cursor.u32()? as usize;
            let key = cursor.take(length)?;
            if prior_key.is_some_and(|prior| prior >= key) {
                return Err(NativeIndexError::Invalid("unsorted block keys"));
            }
            if descriptor
                .index_key_size()
                .is_some_and(|len| key.len() != len)
            {
                return Err(NativeIndexError::Invalid("index key width"));
            }
            validate_cell_routing(descriptor, key)?;
            if descriptor.cell_id(key) != *cell {
                return Err(NativeIndexError::Invalid("index cell key mismatch"));
            }
            let count = cursor.u32()? as usize;
            if count == 0 || count > cursor.remaining() / REFERENCE_BYTES {
                return Err(NativeIndexError::Invalid("version count"));
            }
            let mut prior = None;
            for _ in 0..count {
                let reference = decode_reference(cursor.take(REFERENCE_BYTES)?)?;
                if reference.batch_id.database != database
                    || reference.descriptor.keyspace != keyspace.0
                    || !cut.contains(&reference)
                    || prior.is_some_and(|version| version >= reference.mutation_version())
                {
                    return Err(NativeIndexError::Invalid(
                        "index reference domain, order or cut",
                    ));
                }
                prior = Some(reference.mutation_version());
                retained_bytes += VERSION_CHARGE + key.len();
                retained_versions += 1;
            }
            prior_key = Some(key);
            expected = cursor.position;
        }
        if cursor.remaining() != 0 {
            return Err(NativeIndexError::Invalid("trailing block bytes"));
        }
        Ok(Self {
            bytes,
            count,
            mapped,
            cut,
            file: None,
            retained_bytes,
            retained_versions,
        })
    }
    fn entry(&self, index: usize) -> (&[u8], &[u8]) {
        let offset =
            u32::from_be_bytes(self.bytes[4 + index * 4..8 + index * 4].try_into().unwrap())
                as usize;
        let len = u32::from_be_bytes(self.bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let key = &self.bytes[offset + 4..offset + 4 + len];
        let count_at = offset + 4 + len;
        let count =
            u32::from_be_bytes(self.bytes[count_at..count_at + 4].try_into().unwrap()) as usize;
        (
            key,
            &self.bytes[count_at + 4..count_at + 4 + count * REFERENCE_BYTES],
        )
    }
    fn selected(&self, index: usize, cut: IndexReadCut) -> Option<ValueRecordReference> {
        let (_, values) = self.entry(index);
        let count = lower_bound_by(values.len() / REFERENCE_BYTES, |n| {
            cut.contains(
                &decode_reference(&values[n * REFERENCE_BYTES..(n + 1) * REFERENCE_BYTES])
                    .expect("validated immutable reference"),
            )
        });
        (count > 0).then(|| {
            decode_reference(&values[(count - 1) * REFERENCE_BYTES..count * REFERENCE_BYTES])
                .expect("validated immutable reference")
        })
    }
    fn get(&self, key: &[u8], cut: IndexReadCut) -> Option<ValueRecordReference> {
        let at = lower_bound_by(self.count, |n| self.entry(n).0 < key);
        if at == self.count || self.entry(at).0 != key {
            return None;
        }
        self.selected(at, cut)
    }
}

fn validate_cell_routing(
    descriptor: &KeySpaceDesc,
    reduced: &[u8],
) -> Result<(), NativeIndexError> {
    match descriptor.key_type() {
        KeyType::Uniform(config) => {
            if config
                .cells_per_mutex()
                .checked_mul(descriptor.num_mutexes())
                .and_then(|count| u32::try_from(count).ok())
                .is_none_or(|count| count == 0)
            {
                return Err(NativeIndexError::Invalid("uniform cell count"));
            }
        }
        KeyType::PrefixedUniform(_) => {
            if let CellId::Bytes(prefix) = descriptor.first_cell()
                && reduced.len() < prefix.len()
            {
                return Err(NativeIndexError::Invalid("short cell prefix"));
            }
        }
    }
    Ok(())
}

fn schema_digest(shape: &KeyShape) -> Result<[u8; 32], NativeIndexError> {
    let encoded = shape
        .to_yaml()
        .map_err(|_| NativeIndexError::Invalid("schema serialization"))?;
    Ok(Blake2b::<U32>::digest(encoded.as_bytes()).into())
}

fn encode_reference(reference: &ValueRecordReference, output: &mut Vec<u8>) {
    let before = output.len();
    output.extend_from_slice(&reference.batch_id.database);
    output.extend_from_slice(&reference.batch_id.operation);
    output.extend_from_slice(&reference.batch_version.epoch.to_be_bytes());
    output.extend_from_slice(&reference.batch_version.sequence.to_be_bytes());
    output.extend_from_slice(&reference.batch_digest.0);
    let address = reference.batch_address;
    output.extend_from_slice(&address.partition.to_be_bytes());
    output.extend_from_slice(&address.segment_generation.to_be_bytes());
    output.extend_from_slice(&address.offset.to_be_bytes());
    output.extend_from_slice(&address.len.to_be_bytes());
    let d = reference.descriptor;
    for value in [d.operation_index, d.offset, d.len] {
        output.extend_from_slice(&value.to_be_bytes());
    }
    output.push(d.keyspace);
    output.push(if d.kind == RecordKind::Put { 0 } else { 1 });
    for value in [d.key_len, d.value_offset, d.value_len] {
        output.extend_from_slice(&value.to_be_bytes());
    }
    output.extend_from_slice(&d.checksum.0.to_be_bytes());
    debug_assert_eq!(output.len() - before, REFERENCE_BYTES);
}
fn decode_reference(bytes: &[u8]) -> Result<ValueRecordReference, NativeIndexError> {
    let mut c = Cursor::new(bytes);
    let id = BatchId {
        database: c.take(16)?.try_into().unwrap(),
        operation: c.take(16)?.try_into().unwrap(),
    };
    let version = BatchVersion {
        epoch: c.u64()?,
        sequence: c.u64()?,
    };
    let digest = PayloadDigest(c.take(32)?.try_into().unwrap());
    let address = PhysicalValueAddress {
        partition: c.u32()?,
        segment_generation: c.u64()?,
        offset: c.u64()?,
        len: c.u32()?,
    };
    let operation_index = c.u32()?;
    let offset = c.u32()?;
    let len = c.u32()?;
    let keyspace = c.u8()?;
    let kind = match c.u8()? {
        0 => RecordKind::Put,
        1 => RecordKind::Delete,
        _ => return Err(NativeIndexError::Invalid("reference kind")),
    };
    let descriptor = RecordDescriptor {
        operation_index,
        offset,
        len,
        keyspace,
        kind,
        key_len: c.u32()?,
        value_offset: c.u32()?,
        value_len: c.u32()?,
        checksum: MemberChecksum(c.u32()?),
    };
    if c.remaining() != 0
        || address.len < 8
        || u64::from(offset) + u64::from(len) > u64::from(address.len - 8)
        || u64::from(descriptor.value_offset) + u64::from(descriptor.value_len) > u64::from(len)
    {
        return Err(NativeIndexError::Invalid("reference bounds"));
    }
    Ok(ValueRecordReference {
        batch_id: id,
        batch_version: version,
        batch_digest: digest,
        batch_address: address,
        descriptor,
    })
}
fn encode_cut(cut: IndexReadCut, output: &mut Vec<u8>) {
    match cut {
        IndexReadCut::BeforeAll => {
            output.push(0);
            output.extend_from_slice(&[0; 20]);
        }
        IndexReadCut::At(version) => {
            output.push(1);
            output.extend_from_slice(&LogicalValueId::new(version).encode());
        }
        IndexReadCut::Latest => unreachable!("persisted cut must be concrete"),
    }
}
fn decode_cut(cursor: &mut Cursor<'_>) -> Result<IndexReadCut, NativeIndexError> {
    let tag = cursor.u8()?;
    let bytes = cursor.take(20)?;
    match tag {
        0 if bytes == [0; 20] => Ok(IndexReadCut::BeforeAll),
        1 => Ok(IndexReadCut::At(
            LogicalValueId::decode(1, bytes)
                .map_err(|_| NativeIndexError::Invalid("logical cut"))?
                .version(),
        )),
        _ => Err(NativeIndexError::Invalid("logical cut tag")),
    }
}
fn decode_cell(cursor: &mut Cursor<'_>) -> Result<CellId, NativeIndexError> {
    match cursor.u8()? {
        0 => Ok(CellId::Integer(
            usize::try_from(cursor.u64()?)
                .map_err(|_| NativeIndexError::Invalid("cell integer"))?,
        )),
        1 => {
            let len = cursor.u16()? as usize;
            Ok(CellId::Bytes(cursor.take(len)?.into()))
        }
        _ => Err(NativeIndexError::Invalid("cell tag")),
    }
}
struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], NativeIndexError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(NativeIndexError::Invalid("index length overflow"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(NativeIndexError::Invalid("truncated index image"))?;
        self.position = end;
        Ok(bytes)
    }
    fn u8(&mut self) -> Result<u8, NativeIndexError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, NativeIndexError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, NativeIndexError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, NativeIndexError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::codec::{BatchLimits, Mutation};
    use crate::distributed::group::{CompleteGroup, GroupLimits};
    use crate::distributed::native_publication::PreparedNativePublication;
    use crate::distributed::storage::{
        DurableBatch, PartitionConfig, PartitionLog, StaticInventory,
    };
    use crate::distributed::types::RequestAuthority;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
    use crate::metrics::Metrics;

    mod containers;
    mod cursors;
    mod lifecycle;
    mod owners;

    const DATABASE: [u8; 16] = [7; 16];
    const EPOCH: u64 = 11;
    const BUDGET: usize = 1024 * 1024;
    const A: &[u8] = &[0, 0, 0, 1];
    const B: &[u8] = &[128, 0, 0, 2];
    const C: &[u8] = &[255, 0, 0, 3];
    const REDUCED: &[u8] = &[9, 9, 9, 9, 128, 0, 0, 4];

    fn shape() -> KeyShape {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("plain", 4, 1, KeyType::uniform(2));
        builder.add_key_space_config_indexing(
            "reduced",
            KeyIndexing::key_reduction(8, 4..8),
            1,
            KeyType::uniform(2),
            KeySpaceConfig::new(),
        );
        builder.add_key_space_config_indexing(
            "prefix",
            KeyIndexing::variable_length(),
            1,
            KeyType::prefix_uniform(1, 0),
            KeySpaceConfig::new(),
        );
        builder.add_key_space_config_indexing(
            "hash",
            KeyIndexing::Hash,
            1,
            KeyType::uniform(2),
            KeySpaceConfig::new(),
        );
        builder.build()
    }

    fn config() -> PartitionConfig {
        PartitionConfig {
            inventory: StaticInventory {
                database: DATABASE,
                epoch: EPOCH,
                segment_generation: 3,
                partitions: vec![0],
            },
            partition: 0,
            batch_limits: BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: 4,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches: 16,
            max_reference_metadata_bytes: BUDGET,
        }
    }

    fn log(path: &Path) -> PartitionLog {
        PartitionLog::create(
            path,
            config(),
            &shape(),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap()
    }

    fn put(keyspace: u8, key: &[u8], value: &[u8]) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(keyspace),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        }
    }
    fn delete(key: &[u8]) -> Mutation {
        Mutation::Delete {
            keyspace: KeySpace(0),
            key: Bytes::copy_from_slice(key),
        }
    }
    fn batch(sequence: u64, mutations: Vec<Mutation>) -> CompleteBatch {
        CompleteBatch::new(
            &shape(),
            BatchId {
                database: DATABASE,
                operation: (sequence as u128).to_be_bytes(),
            },
            BatchVersion {
                epoch: EPOCH,
                sequence,
            },
            mutations,
            config().batch_limits,
        )
        .unwrap()
    }
    fn cut(sequence: u64, operation_index: u32) -> IndexReadCut {
        IndexReadCut::At(MutationVersion {
            batch: BatchVersion {
                epoch: EPOCH,
                sequence,
            },
            operation_index,
        })
    }
    fn durable(log: &PartitionLog, batches: &[CompleteBatch]) -> Vec<DurableBatch> {
        let group = CompleteGroup::new(
            &shape(),
            batches.to_vec(),
            config().batch_limits,
            config().group_limits,
        )
        .unwrap();
        log.append_group(RequestAuthority { epoch: EPOCH }, &group)
            .unwrap()
            .into_iter()
            .map(|appended| log.persist_through(appended).unwrap())
            .collect()
    }
    fn install(
        index: &mut NativeIndex,
        log: &PartitionLog,
        batch: &CompleteBatch,
        durable: DurableBatch,
    ) {
        PreparedNativePublication::prepare(log, batch, durable)
            .unwrap()
            .publish(index)
            .unwrap();
        index.promote_committed().unwrap();
    }
    fn value(
        index: &NativeIndex,
        log: &PartitionLog,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Option<Bytes> {
        index.get(KeySpace(0), key, cut).unwrap().map(|reference| {
            match log.read_record(&reference).unwrap() {
                Mutation::Put {
                    key: actual_key,
                    value,
                    ..
                } => {
                    assert_eq!(&*actual_key, key);
                    value
                }
                Mutation::Delete { .. } => panic!("get returned a tombstone"),
            }
        })
    }
    fn keys(index: &NativeIndex, reverse: bool, cut: IndexReadCut) -> Vec<Vec<u8>> {
        index
            .scan(KeySpace(0), None, reverse, 100, cut)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.to_vec())
            .collect()
    }

    #[test]
    fn real_wal_multicell_publication_history_and_ordered_scans() {
        let temp = tempdir::TempDir::new("native-reference-index").unwrap();
        let log = log(&temp.path().join("wal"));
        let first = batch(
            0,
            vec![
                put(0, A, b"first"),
                put(0, B, b"middle"),
                put(1, REDUCED, b"reduced"),
                put(2, b"z-last", b"z"),
                put(2, b"a-first", b"a"),
                delete(A),
                put(0, A, b"final"),
                put(3, b"arbitrary hash key", b"hashed"),
            ],
        );
        let second = batch(1, vec![delete(A), put(0, C, b"third")]);
        let receipts = durable(&log, &[first.clone(), second.clone()]);
        let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
        let prepared = PreparedNativePublication::prepare(&log, &first, receipts[0]).unwrap();
        prepared.publish(&mut index).unwrap();
        // Committed pending entries do not leak into the serving index before the
        // owning authority's single publication callback promotes every cell.
        assert_eq!(
            index.get(KeySpace(0), A, IndexReadCut::Latest).unwrap(),
            None
        );
        assert_eq!(index.promote_committed().unwrap(), 8);
        assert_eq!(index.promote_committed().unwrap(), 0);
        assert_eq!(
            value(&index, &log, A, cut(0, 0)).as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(value(&index, &log, A, cut(0, 5)), None);
        assert_eq!(
            value(&index, &log, A, cut(0, 6)).as_deref(),
            Some(b"final".as_slice())
        );
        assert_eq!(value(&index, &log, A, IndexReadCut::BeforeAll), None);
        assert_eq!(
            keys(&index, false, IndexReadCut::Latest),
            [A.to_vec(), B.to_vec()]
        );
        install(&mut index, &log, &second, receipts[1]);
        assert_eq!(
            keys(&index, false, IndexReadCut::Latest),
            [B.to_vec(), C.to_vec()]
        );
        assert_eq!(
            keys(&index, true, IndexReadCut::Latest),
            [C.to_vec(), B.to_vec()]
        );
        assert_eq!(
            keys(&index, false, cut(0, u32::MAX)),
            [A.to_vec(), B.to_vec()]
        );
        assert_eq!(
            index
                .scan(KeySpace(0), Some(B), false, 1, IndexReadCut::Latest)
                .unwrap()[0]
                .0
                .as_ref(),
            C
        );
        assert_eq!(
            index
                .scan(KeySpace(0), Some(C), true, 1, IndexReadCut::Latest)
                .unwrap()[0]
                .0
                .as_ref(),
            B
        );
        assert_eq!(
            index
                .scan(KeySpace(0), None, false, 0, IndexReadCut::Latest)
                .unwrap()
                .len(),
            0
        );
        let reduced = index
            .scan(KeySpace(1), None, false, 10, IndexReadCut::Latest)
            .unwrap();
        assert_eq!(reduced[0].0.as_ref(), &REDUCED[4..]);
        assert_eq!(
            index
                .get(KeySpace(1), REDUCED, IndexReadCut::Latest)
                .unwrap(),
            Some(reduced[0].1)
        );
        assert!(
            index
                .get(KeySpace(1), &REDUCED[..7], IndexReadCut::Latest)
                .is_err()
        );
        assert_eq!(
            log.read_record(&reduced[0].1).unwrap(),
            first.mutations()[2]
        );
        let prefix = index
            .scan(KeySpace(2), None, false, 10, IndexReadCut::Latest)
            .unwrap();
        assert_eq!(
            prefix
                .iter()
                .map(|(key, _)| key.as_ref())
                .collect::<Vec<_>>(),
            [b"a-first".as_slice(), b"z-last".as_slice()]
        );
        assert!(
            index
                .get(KeySpace(3), b"arbitrary hash key", IndexReadCut::Latest)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            index.get(KeySpace(3), b"", IndexReadCut::Latest).unwrap(),
            None
        );
        assert!(index.get(KeySpace(2), b"", IndexReadCut::Latest).is_err());
        assert!(
            index
                .scan(KeySpace(3), None, false, 10, IndexReadCut::Latest)
                .is_err()
        );
        assert_eq!(index.stats().retained_versions, 10);
        // Exact replay duplicates do not charge a second retained version.
        install(&mut index, &log, &first, receipts[0]);
        assert_eq!(index.stats().retained_versions, 10);
        assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
    }

    #[test]
    fn cut_snapshot_unload_reload_retains_tombstones_and_later_overlay() {
        let temp = tempdir::TempDir::new("native-reference-snapshot").unwrap();
        let log = log(&temp.path().join("wal"));
        let first = batch(
            0,
            vec![
                put(0, A, b"old"),
                put(0, B, b"middle"),
                put(1, REDUCED, b"reduced"),
                put(2, b"prefix", b"variable"),
            ],
        );
        let second = batch(1, vec![delete(A), put(0, C, b"new")]);
        let receipts = durable(&log, &[first.clone(), second.clone()]);
        let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
        // Publication arriving in reverse physical/completion order still obeys
        // the explicit logical version, never the address or insertion order.
        install(&mut index, &log, &second, receipts[1]);
        install(&mut index, &log, &first, receipts[0]);
        let old = temp.path().join("old.index");
        let expected_bytes = index.retained_metadata_bytes();
        let metadata = index.flush_and_unload(&old, cut(0, u32::MAX)).unwrap();
        let image = std::fs::read(&old).unwrap();
        assert_eq!(metadata.bytes, image.len() as u64);
        assert_eq!(
            metadata.digest,
            <[u8; 32]>::from(Blake2b::<U32>::digest(&image))
        );
        let repeated = temp.path().join("same-cut.index");
        let repeated_metadata = index.flush_and_unload(&repeated, cut(0, u32::MAX)).unwrap();
        assert_eq!(std::fs::read(repeated).unwrap(), image);
        assert_eq!(repeated_metadata.bytes, metadata.bytes);
        assert_eq!(repeated_metadata.digest, metadata.digest);
        assert!(index.stats().unloaded_cells > 0);
        assert_eq!(index.retained_metadata_bytes(), expected_bytes);
        assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
        assert_eq!(
            value(&index, &log, A, cut(0, u32::MAX)).as_deref(),
            Some(b"old".as_slice())
        );
        assert_eq!(
            keys(&index, false, IndexReadCut::Latest),
            [B.to_vec(), C.to_vec()]
        );
        let old_reopened = NativeIndex::open_snapshot(&old, &shape(), DATABASE, BUDGET).unwrap();
        assert_eq!(
            value(&old_reopened, &log, A, IndexReadCut::Latest).as_deref(),
            Some(b"old".as_slice())
        );
        assert_eq!(value(&old_reopened, &log, C, IndexReadCut::Latest), None);
        index.reload().unwrap();
        assert_eq!(index.stats().unloaded_cells, 0);
        assert_eq!(
            keys(&index, true, cut(0, u32::MAX)),
            [B.to_vec(), A.to_vec()]
        );
        let third = batch(2, vec![put(0, A, b"recreated")]);
        let receipt = durable(&log, std::slice::from_ref(&third))[0];
        install(&mut index, &log, &third, receipt);
        let latest = temp.path().join("latest.index");
        index
            .flush_and_unload(&latest, IndexReadCut::Latest)
            .unwrap();
        // Every cell is now a compact immutable base. A repeated image reuses
        // the exact same container ranges without rewriting their histories.
        let repeated_latest = temp.path().join("same-latest.index");
        index
            .flush_and_unload(&repeated_latest, IndexReadCut::Latest)
            .unwrap();
        assert_eq!(
            std::fs::read(&repeated_latest).unwrap(),
            std::fs::read(&latest).unwrap()
        );
        let mut reopened = NativeIndex::open_snapshot(&latest, &shape(), DATABASE, BUDGET).unwrap();
        assert_eq!(reopened.stats().retained_versions, 7);
        assert_eq!(
            value(&reopened, &log, A, IndexReadCut::Latest).as_deref(),
            Some(b"recreated".as_slice())
        );
        assert_eq!(value(&reopened, &log, A, cut(1, u32::MAX)), None);
        assert_eq!(
            value(&reopened, &log, A, cut(0, u32::MAX)).as_deref(),
            Some(b"old".as_slice())
        );
        assert_eq!(
            keys(&reopened, false, IndexReadCut::Latest),
            [A.to_vec(), B.to_vec(), C.to_vec()]
        );
        assert_eq!(
            keys(&reopened, true, cut(1, u32::MAX)),
            [C.to_vec(), B.to_vec()]
        );
        reopened.reload().unwrap();
        assert_eq!(
            value(&reopened, &log, A, IndexReadCut::Latest).as_deref(),
            Some(b"recreated".as_slice())
        );
        assert!(
            index
                .flush_and_unload(&latest, IndexReadCut::Latest)
                .is_err()
        );
        // Exporting BeforeAll stays empty without replacing a newer installed
        // base or granting a WAL-retention frontier.
        let empty = temp.path().join("empty.index");
        let unloaded_before_export = index.stats().unloaded_cells;
        index
            .flush_and_unload(&empty, IndexReadCut::BeforeAll)
            .unwrap();
        assert_eq!(index.stats().unloaded_cells, unloaded_before_export);
        assert_eq!(
            value(&index, &log, A, IndexReadCut::Latest).as_deref(),
            Some(b"recreated".as_slice())
        );
        let empty = NativeIndex::open_snapshot(&empty, &shape(), DATABASE, BUDGET).unwrap();
        assert_eq!(empty.latest_cut(), IndexReadCut::BeforeAll);
        assert_eq!(empty.stats().retained_versions, 0);
    }

    #[test]
    fn many_cell_heap_scan_matches_wal_history_across_overlay_and_unloaded_blocks() {
        let temp = tempdir::TempDir::new("native-index-heap-scan").unwrap();
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("many-cells", 4, 1, KeyType::uniform(64));
        let shape = builder.build();
        let mut config = config();
        config.batch_limits = BatchLimits {
            max_operations: 128,
            max_encoded_bytes: 16384,
        };
        config.group_limits = GroupLimits {
            max_batches: 4,
            max_encoded_bytes: 65528,
        };
        config.fragment_bytes = 65536;
        config.wal_file_bytes = 131072;
        let log = PartitionLog::create(
            &temp.path().join("wal"),
            config.clone(),
            &shape,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap();
        // The most significant byte distributes these keys across 64 real native
        // cells; updates and tombstones overlap keys retained in compact blocks.
        let key = |n: u32| (n << 24).to_be_bytes();
        let make = |sequence, mutations| {
            CompleteBatch::new(
                &shape,
                BatchId {
                    database: DATABASE,
                    operation: (sequence as u128).to_be_bytes(),
                },
                BatchVersion {
                    epoch: EPOCH,
                    sequence,
                },
                mutations,
                config.batch_limits,
            )
            .unwrap()
        };
        let batches = [
            make(0, (0..128).map(|n| put(0, &key(n), b"first")).collect()),
            make(
                1,
                (0..128)
                    .map(|n| {
                        if n % 2 == 0 {
                            put(0, &key(n), b"updated")
                        } else {
                            delete(&key(n))
                        }
                    })
                    .collect(),
            ),
            make(2, (128..256).map(|n| put(0, &key(n), b"later")).collect()),
        ];
        let group = CompleteGroup::new(
            &shape,
            batches.to_vec(),
            config.batch_limits,
            config.group_limits,
        )
        .unwrap();
        let receipts: Vec<_> = log
            .append_group(RequestAuthority { epoch: EPOCH }, &group)
            .unwrap()
            .into_iter()
            .map(|receipt| log.persist_through(receipt).unwrap())
            .collect();
        let mut index = NativeIndex::new(&shape, DATABASE, BUDGET).unwrap();
        install(&mut index, &log, &batches[0], receipts[0]);
        index
            .flush_and_unload(&temp.path().join("prefix.index"), cut(0, u32::MAX))
            .unwrap();
        install(&mut index, &log, &batches[1], receipts[1]);
        install(&mut index, &log, &batches[2], receipts[2]);
        assert_eq!(index.stats().cells, 64);
        for read_cut in [cut(0, u32::MAX), cut(1, u32::MAX), IndexReadCut::Latest] {
            for reverse in [false, true] {
                let mut expected: Vec<_> = (0..256)
                    .filter(|n| match read_cut {
                        IndexReadCut::At(v) if v.batch.sequence == 0 => *n < 128,
                        IndexReadCut::At(_) => *n < 128 && *n % 2 == 0,
                        _ => *n >= 128 || *n % 2 == 0,
                    })
                    .map(|n| key(n).to_vec())
                    .collect();
                if reverse {
                    expected.reverse();
                }
                let mut cursor = index
                    .scan_cursor(KeySpace(0), None, reverse, read_cut)
                    .unwrap();
                let mut after: Option<Bytes> = None;
                let heap_capacity = cursor.heads.capacity();
                assert_eq!(cursor.cells.len(), 64);
                let mut observed = Vec::new();
                loop {
                    let page: Vec<_> = (0..7).map_while(|_| cursor.next(&index)).collect();
                    assert_eq!(cursor.heads.capacity(), heap_capacity);
                    assert!(cursor.heads.len() <= cursor.cells.len());
                    assert_eq!(
                        page,
                        index
                            .scan(KeySpace(0), after.as_deref(), reverse, 7, read_cut)
                            .unwrap(),
                        "persistent cursor must match exclusive restart bounds",
                    );
                    if page.is_empty() {
                        break;
                    }
                    for (reduced, reference) in page {
                        match log.read_record(&reference).unwrap() {
                            Mutation::Put { key, .. } => assert_eq!(key, reduced),
                            _ => panic!("scan exposed tombstone"),
                        }
                        observed.push(reduced.to_vec());
                        after = Some(reduced);
                    }
                }
                assert_eq!(observed, expected, "cut {read_cut:?}, reverse {reverse}");
            }
        }
        index
            .flush_and_unload(&temp.path().join("all.index"), IndexReadCut::Latest)
            .unwrap();
        assert_eq!(index.stats().unloaded_cells, 64);
        assert_eq!(
            index
                .scan(KeySpace(0), None, false, 1000, IndexReadCut::Latest)
                .unwrap()
                .len(),
            192
        );
        assert_eq!(
            index
                .scan(KeySpace(0), None, true, 1000, cut(0, u32::MAX))
                .unwrap()
                .len(),
            128
        );
    }

    #[test]
    fn snapshot_rejects_corruption_wrong_domains_and_capacity() {
        let temp = tempdir::TempDir::new("native-index-validation").unwrap();
        let log = log(&temp.path().join("wal"));
        let batch = batch(0, vec![put(0, A, b"value kept only in WAL")]);
        let receipt = durable(&log, std::slice::from_ref(&batch))[0];
        let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
        let estimate = index.estimated_batch_bytes(&batch).unwrap();
        let small = NativeIndex::new(&shape(), DATABASE, estimate - 1).unwrap();
        assert!(matches!(
            small.check_capacity(estimate),
            Err(NativeIndexError::Capacity)
        ));
        install(&mut index, &log, &batch, receipt);
        assert_eq!(index.retained_metadata_bytes(), VERSION_CHARGE + A.len());
        let path = temp.path().join("valid.index");
        index.flush_and_unload(&path, IndexReadCut::Latest).unwrap();
        let image = std::fs::read(&path).unwrap();
        assert!(
            !image
                .windows(b"value kept only in WAL".len())
                .any(|window| window == b"value kept only in WAL")
        );
        assert!(NativeIndex::open_snapshot(&path, &shape(), [8; 16], BUDGET).is_err());
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("different", 4, 1, KeyType::uniform(2));
        assert!(NativeIndex::open_snapshot(&path, &builder.build(), DATABASE, BUDGET).is_err());
        assert!(matches!(
            NativeIndex::open_snapshot(&path, &shape(), DATABASE, CELL_CHARGE - 1),
            Err(NativeIndexError::Capacity)
        ));
        assert!(NativeIndex::open_snapshot(&path, &shape(), DATABASE, estimate - 1).is_ok());
        for (name, bytes) in [
            ("short", image[..image.len() - 1].to_vec()),
            ("trailing", [image.as_slice(), &[0]].concat()),
            ("bad-crc", {
                let mut bytes = image.clone();
                let last = bytes.len() - 1;
                bytes[last] ^= 1;
                bytes
            }),
        ] {
            let bad = temp.path().join(name);
            std::fs::write(&bad, bytes).unwrap();
            assert!(
                NativeIndex::open_snapshot(&bad, &shape(), DATABASE, BUDGET).is_err(),
                "{name}"
            );
        }
        // Repaired CRC cannot hide unsupported format or a malformed cell
        // retention floor in the root manifest.
        let payload = CrcFrame::read_from_bytes(&Bytes::from(image), 0).unwrap();
        for (name, bytes) in [
            ("format", {
                let mut bytes = payload.to_vec();
                bytes[9] = FORMAT as u8 + 1;
                bytes
            }),
            ("cell-floor", {
                let mut bytes = payload.to_vec();
                let at = bytes.len() - 21;
                bytes[at] = 7;
                bytes
            }),
        ] {
            let bad = temp.path().join(name);
            std::fs::write(&bad, CrcFrame::new(&bytes).as_ref()).unwrap();
            assert!(
                NativeIndex::open_snapshot(&bad, &shape(), DATABASE, BUDGET).is_err(),
                "{name}"
            );
        }
    }
}
