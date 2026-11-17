//! Native dirty/unloaded cells and exact captured-reference unmerge, with the
//! prepare/async-flush/install phases used by Tsavorite. Unlike a fuzzy snapshot,
//! every stored reference is at the authority's exact complete publication cut.
use super::*;
use crate::index::versioned_overlay::Versions as NativeVersions;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[derive(Clone)]
pub(super) struct CellFile {
    /// Owned immutable container path; several cells can share this backing.
    pub path: PathBuf,
    /// Start of this cell's CRC frame within the container.
    pub offset: u64,
    /// Exact CRC-framed range length, including the eight-byte frame header.
    pub bytes: u64,
    pub digest: [u8; 32],
    pub floor: IndexReadCut,
}

pub(super) fn cell_digest(bytes: &[u8]) -> [u8; 32] {
    // Cell frames use BLAKE2b-32, not truncated BLAKE2b-512.
    let hash = blake2b_simd::Params::new().hash_length(32).hash(bytes);
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.as_bytes());
    digest
}

pub(super) struct FrozenCell {
    pub identity: (KeySpace, CellId),
    pub base: Option<SortedBlock>,
    pub overlay: std::sync::Arc<Overlay<ValueRecordReference>>,
}

pub(crate) struct PreparedIndexImage {
    database: [u8; 16],
    schema: [u8; 32],
    cells: Vec<FrozenCell>,
    cut: IndexReadCut,
    lease: CaptureLease,
    /// Payload scratch limit of the one encoder that persists this image.
    scratch_limit: usize,
}

pub(crate) struct PersistedIndexImage {
    pub(super) cells: Vec<FrozenCell>,
    pub(super) cut: IndexReadCut,
    pub(super) lease: CaptureLease,
    pub(crate) metadata: IndexImageMetadata,
    // The checkpoint diagnostic stage exports these counters; until it reads
    // them, only tests do. The expectation fails once a reader exists.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "exported by the checkpoint metrics stage")
    )]
    pub(crate) encode_stats: CellEncodeStats,
}

impl PersistedIndexImage {
    pub(crate) fn files(&self) -> Vec<String> {
        container_names(
            self.cells
                .iter()
                .filter_map(|cell| cell.base.as_ref())
                .filter_map(|base| base.file.as_ref()),
        )
    }
}

impl PreparedIndexImage {
    pub(super) fn new(
        database: [u8; 16],
        schema: [u8; 32],
        cells: Vec<FrozenCell>,
        cut: IndexReadCut,
        lease: CaptureLease,
        scratch_limit: usize,
    ) -> Self {
        Self {
            database,
            schema,
            cells,
            cut,
            lease,
            scratch_limit,
        }
    }

    /// Reserve the generation-specific container even if persistence later
    /// reuses every existing base. The root is reserved by the authority.
    pub(crate) fn reserved_files(&self, path: &Path) -> Result<Vec<String>, NativeIndexError> {
        let (_, name) = output_parts(path)?;
        Ok(vec![format!("{name}.cells")])
    }

    fn needs_container(&self, parent: &Path) -> bool {
        self.cells.iter().any(|cell| {
            !reusable(cell, parent, &self.cut, &self.lease.floor)
                && has_rows(cell, &self.cut, &self.lease.floor)
        })
    }

    pub(crate) fn persist(self, path: &Path) -> Result<PersistedIndexImage, NativeIndexError> {
        self.persist_with_hook(path, || {})
    }

    // Test hook: after container creation but before appending any cell data,
    // without serving-index ownership. The root is created only after data sync.
    pub(super) fn persist_with_hook(
        mut self,
        path: &Path,
        after_create: impl FnOnce(),
    ) -> Result<PersistedIndexImage, NativeIndexError> {
        self.cells.sort_by(|a, b| {
            a.identity
                .0
                .0
                .cmp(&b.identity.0.0)
                .then_with(|| a.identity.1.cmp(&b.identity.1))
        });
        let (parent, root_name) = output_parts(path)?;
        match std::fs::symlink_metadata(path) {
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "index root already exists",
                )
                .into());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let container_path = parent.join(format!("{root_name}.cells"));
        let mut container = if self.needs_container(parent) {
            Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&container_path)?,
            )
        } else {
            None
        };
        after_create();
        let mut written = Vec::new();
        let mut reused_mappings = HashMap::new();
        // This thread's only payload scratch; it grows outside serving locks
        // to at most `scratch_limit` and is reused for every cell.
        let mut encoder = CellEncoder::new(self.scratch_limit);
        {
            // One fixed buffer combines tiny cells into shared writes. Headers
            // are computed before append, so no per-cell seek/flush is needed.
            let mut writer = container
                .as_mut()
                .map(|file| BufWriter::with_capacity(16 * 1024, file));
            let mut offset = 0u64;
            for (number, cell) in self.cells.iter_mut().enumerate() {
                if !reusable(cell, parent, &self.cut, &self.lease.floor) {
                    let Some(encoded) = encoder.append(
                        cell,
                        writer.as_mut(),
                        &container_path,
                        offset,
                        &self.cut,
                        &self.lease.floor,
                    )?
                    else {
                        cell.base = None;
                        continue;
                    };
                    offset = offset
                        .checked_add(encoded.file.bytes)
                        .ok_or(NativeIndexError::Capacity)?;
                    written.push((number, encoded));
                } else if let Some(base) = &mut cell.base
                    && !base.mapped
                {
                    let file = base.file.as_ref().expect("reusable durable cell");
                    let name = file
                        .path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or(NativeIndexError::Invalid("invalid cell container name"))?;
                    let frame =
                        open_frame(&mut reused_mappings, parent, name, file.offset, file.bytes)?;
                    if cell_digest(&frame) != file.digest {
                        return Err(NativeIndexError::Invalid("cell range digest"));
                    }
                    base.bytes = frame_payload(&frame)?;
                    base.mapped = true;
                }
            }
            if let Some(writer) = &mut writer {
                writer.flush()?;
            }
        }
        if let Some(container) = &container {
            // Exactly one data barrier and one mapping for all rewritten cells.
            container.sync_all()?;
            let mapped = map_file(container, None)?;
            for (number, encoded) in written {
                let frame = cell_frame(&mapped, encoded.file.offset, encoded.file.bytes)?;
                self.cells[number].base = Some(SortedBlock {
                    bytes: frame.slice(8..),
                    count: encoded.count,
                    mapped: true,
                    cut: self.cut.clone(),
                    file: Some(encoded.file),
                    retained_bytes: encoded.retained_bytes,
                    retained_versions: encoded.retained_versions,
                });
            }
        }
        // Small roots bind each cell to an immutable container and exact range.
        let mut payload = Vec::new();
        payload.extend_from_slice(MAGIC);
        payload.extend_from_slice(&FORMAT.to_be_bytes());
        payload.extend_from_slice(&0u16.to_be_bytes());
        payload.extend_from_slice(&self.database);
        payload.extend_from_slice(&self.schema);
        encode_cut(&self.cut, &mut payload);
        encode_cut(&self.lease.floor, &mut payload);
        let count = self.cells.iter().filter(|cell| cell.base.is_some()).count();
        payload.extend_from_slice(
            &u32::try_from(count)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        );
        let mut identity = BytesMut::new();
        for cell in &self.cells {
            let Some(base) = &cell.base else { continue };
            let file = base
                .file
                .as_ref()
                .ok_or(NativeIndexError::Invalid("cell has no durable file"))?;
            payload.push(cell.identity.0.0);
            identity.clear();
            cell.identity.1.write_into_bytes(&mut identity);
            payload.extend_from_slice(&identity);
            let name = file
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or(NativeIndexError::Invalid("invalid cell file name"))?
                .as_bytes();
            payload.extend_from_slice(
                &u16::try_from(name.len())
                    .map_err(|_| NativeIndexError::Capacity)?
                    .to_be_bytes(),
            );
            payload.extend_from_slice(name);
            payload.extend_from_slice(&file.offset.to_be_bytes());
            payload.extend_from_slice(&file.bytes.to_be_bytes());
            payload.extend_from_slice(&file.digest);
            encode_cut(&file.floor, &mut payload);
            if payload.len() > MAX_IMAGE_BYTES - 8 {
                return Err(NativeIndexError::Capacity);
            }
        }
        let frame = CrcFrame::new(&payload);
        let mut root_file = OpenOptions::new().write(true).create_new(true).open(path)?;
        root_file.write_all(frame.as_ref())?;
        root_file.sync_all()?;
        // All new cell frames share the already-synced container. The directory
        // barrier precedes authority-manifest adoption; a failed capture leaves
        // only staged orphans, never removes backing from an older durable root.
        File::open(parent)?.sync_all()?;
        let metadata = IndexImageMetadata {
            bytes: frame.len_with_header() as u64,
            digest: Blake2b::<U32>::digest(frame.as_ref()).into(),
        };
        Ok(PersistedIndexImage {
            cells: self.cells,
            cut: self.cut,
            lease: self.lease,
            metadata,
            encode_stats: encoder.stats,
        })
    }
}

fn reusable(cell: &FrozenCell, parent: &Path, cut: &IndexReadCut, floor: &IndexReadCut) -> bool {
    // Captures retain the complete sealed generation. A cell is still clean
    // for this prefix when all its overlay references are newer than the cut.
    cell.overlay.values().all(|values| {
        values
            .first()
            .is_none_or(|reference| !cut.contains(reference))
    }) && cell.base.as_ref().is_some_and(|base| {
        retention::position(&base.cut) <= retention::position(cut)
            && base.file.as_ref().is_some_and(|file| {
                file.path.parent() == Some(parent)
                    && retention::position(&file.floor) >= retention::position(floor)
            })
    })
}

pub(super) fn container_names<'a>(files: impl Iterator<Item = &'a CellFile>) -> Vec<String> {
    let unique: HashSet<&str> = files
        .filter_map(|file| file.path.file_name()?.to_str())
        .collect();
    let mut names: Vec<String> = unique.into_iter().map(str::to_owned).collect();
    names.sort_unstable();
    names
}

fn output_parts(path: &Path) -> Result<(&Path, &str), NativeIndexError> {
    let parent = path
        .parent()
        .ok_or(NativeIndexError::Invalid("image has no parent"))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(NativeIndexError::Invalid("invalid image file name"))?;
    if name.is_empty() || name == "." || name == ".." || name.contains('\\') || name.contains('\0')
    {
        return Err(NativeIndexError::Invalid("invalid image file name"));
    }
    Ok((parent, name))
}

/// The shared container may exceed one GiB; roots and individual frames may not.
pub(super) fn map_file(file: &File, maximum: Option<u64>) -> Result<Bytes, NativeIndexError> {
    let length = file.metadata()?.len();
    if length < 8 || length > isize::MAX as u64 || maximum.is_some_and(|maximum| length > maximum) {
        return Err(NativeIndexError::Invalid("image size"));
    }
    // SAFETY: containers/roots are create_new and fully written/synced before
    // mapping. They are never modified or truncated after mapping. Each cell
    // slice retains the shared map owner; unlink cannot invalidate its inode.
    Ok(Bytes::from_owner(unsafe {
        memmap2::MmapOptions::new().map(file)?
    }))
}

fn cell_frame(mapped: &Bytes, offset: u64, length: u64) -> Result<Bytes, NativeIndexError> {
    let end = offset
        .checked_add(length)
        .ok_or(NativeIndexError::Invalid("cell range overflow"))?;
    if !(12..=MAX_IMAGE_BYTES as u64).contains(&length) || end > mapped.len() as u64 {
        return Err(NativeIndexError::Invalid("cell range bounds"));
    }
    let start = usize::try_from(offset).map_err(|_| NativeIndexError::Invalid("cell offset"))?;
    let end = usize::try_from(end).map_err(|_| NativeIndexError::Invalid("cell range end"))?;
    Ok(mapped.slice(start..end))
}

struct Container {
    mapped: Bytes,
    ranges: BTreeMap<u64, u64>,
}

impl Container {
    fn add_range(&mut self, offset: u64, length: u64) -> Result<Bytes, NativeIndexError> {
        let frame = cell_frame(&self.mapped, offset, length)?;
        let end = offset + length; // cell_frame checked overflow and bounds.
        if self
            .ranges
            .range(..=offset)
            .next_back()
            .is_some_and(|(_, end)| *end > offset)
            || self
                .ranges
                .range(offset..)
                .next()
                .is_some_and(|(start, _)| *start < end)
        {
            return Err(NativeIndexError::Invalid("overlapping cell ranges"));
        }
        self.ranges.insert(offset, end);
        Ok(frame)
    }
}

/// Canonical relative names share one mapping. The range directory grows only
/// with the bounded number of cells and rejects aliases within one container.
fn open_frame(
    containers: &mut HashMap<String, Container>,
    parent: &Path,
    name: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, NativeIndexError> {
    if let Some(container) = containers.get_mut(name) {
        return container.add_range(offset, length);
    }
    let mut container = Container {
        mapped: map_file(&File::open(parent.join(name))?, None)?,
        ranges: BTreeMap::new(),
    };
    let frame = container.add_range(offset, length)?;
    containers.insert(name.to_owned(), container);
    Ok(frame)
}

pub(super) fn frame_payload(mapped: &Bytes) -> Result<Bytes, NativeIndexError> {
    let payload = CrcFrame::read_from_bytes(mapped, 0)
        .map_err(|_| NativeIndexError::Invalid("index image CRC"))?;
    if payload.len() + 8 != mapped.len() {
        return Err(NativeIndexError::Invalid("trailing image bytes"));
    }
    Ok(payload)
}

pub(super) fn open(
    path: &Path,
    shape: &KeyShape,
    database: [u8; 16],
    max_metadata_bytes: usize,
    shards: usize,
) -> Result<NativeIndex, NativeIndexError> {
    let mapped = map_file(&File::open(path)?, Some(MAX_IMAGE_BYTES as u64))?;
    let payload = frame_payload(&mapped)?;
    let mut cursor = Cursor::new(&payload);
    if cursor.take(8)? != MAGIC || cursor.u16()? != FORMAT || cursor.u16()? != 0 {
        return Err(NativeIndexError::Invalid("index image format"));
    }
    if cursor.take(16)? != database || cursor.take(32)? != schema_digest(shape)? {
        return Err(NativeIndexError::Invalid("index image database or schema"));
    }
    let cut = decode_cut(&mut cursor)?;
    let floor = decode_cut(&mut cursor)?;
    if cut == IndexReadCut::Latest
        || floor == IndexReadCut::Latest
        || retention::position(&floor) > retention::position(&cut)
    {
        return Err(NativeIndexError::Invalid(
            "persisted cut or retention floor",
        ));
    }
    let count = cursor.u32()? as usize;
    if count > cursor.remaining() / 53 || count > max_metadata_bytes / CELL_CHARGE {
        return Err(NativeIndexError::Capacity);
    }
    let parent = path
        .parent()
        .ok_or(NativeIndexError::Invalid("image has no parent"))?;
    let mut index = NativeIndex::with_shards(shape, database, max_metadata_bytes, shards)?;
    let mut containers: HashMap<String, Container> = HashMap::new();
    for _ in 0..count {
        let keyspace = KeySpace(cursor.u8()?);
        let cell_id = decode_cell(&mut cursor)?;
        index.check_capacity(cell_directory_charge(&cell_id))?;
        super::shards::add(&index.accounting.directory, cell_directory_charge(&cell_id));
        let name_length = cursor.u16()? as usize;
        let name = std::str::from_utf8(cursor.take(name_length)?)
            .map_err(|_| NativeIndexError::Invalid("cell file name encoding"))?;
        if name.is_empty()
            || name.contains('/')
            || name.contains('\\')
            || name == "."
            || name == ".."
        {
            return Err(NativeIndexError::Invalid("cell file name"));
        }
        let offset = cursor.u64()?;
        let length = cursor.u64()?;
        let digest: [u8; 32] = cursor.take(32)?.try_into().unwrap();
        let cell_floor = decode_cut(&mut cursor)?;
        if cell_floor == IndexReadCut::Latest
            || retention::position(&cell_floor) > retention::position(&floor)
        {
            return Err(NativeIndexError::Invalid("cell retention floor"));
        }
        if !name.ends_with(".cells") || name.len() <= ".cells".len() || name.contains('\0') {
            return Err(NativeIndexError::Invalid("cell container name"));
        }
        let cell_path = parent.join(name);
        let frame = open_frame(&mut containers, parent, name, offset, length)?;
        if cell_digest(&frame) != digest {
            return Err(NativeIndexError::Invalid("cell range digest"));
        }
        let mut block = SortedBlock::validated(
            frame_payload(&frame)?,
            true,
            database,
            keyspace,
            index.descriptor(keyspace)?,
            &cell_id,
            cut.clone(),
        )?;
        block.file = Some(CellFile {
            path: cell_path,
            offset,
            bytes: length,
            digest,
            floor: cell_floor,
        });
        for n in 0..block.count {
            let (_, versions) = block.entry(n);
            let reference = decode_reference(&versions[versions.len() - REFERENCE_BYTES..])?;
            let writer = index.writer.get_mut();
            writer.latest = Some(writer.latest.map_or(reference.mutation_version(), |old| {
                old.max(reference.mutation_version())
            }));
        }
        index
            .writer
            .get_mut()
            .cell_order
            .push((keyspace, cell_id.clone()));
        let identity = (keyspace, cell_id);
        let shard = index.shard_index(identity.0, &identity.1);
        if index.shards[shard]
            .get_mut()
            .insert(
                identity,
                CellIndex {
                    base: Some(block),
                    ..Default::default()
                },
            )
            .is_some()
        {
            return Err(NativeIndexError::Invalid("duplicate cell"));
        }
    }
    if cursor.remaining() != 0 {
        return Err(NativeIndexError::Invalid("trailing payload"));
    }
    index.retention.set_floor(floor);
    index.recount_retained();
    let latest = index.writer.get_mut().latest;
    index.published.store(std::sync::Arc::new(
        latest.map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
    ));
    Ok(index)
}

/// The immutable inputs of one cell encoding: an optional base and `N`
/// frozen overlays. Exact mode has its one frozen overlay (`N = 1`); option C
/// has the captured carry and the captured active overlay (`N = 2`, spec
/// §5.1); the new active overlay is never encoded. The overlay count is a
/// constant so the Exact path compiles to the single-overlay merge. Versions
/// are distinct across overlays (C2). A base version equal to an overlay
/// version is taken from the base, as the single-overlay encoder always did.
#[derive(Clone, Copy)]
struct CellSources<'a, const N: usize> {
    base: Option<&'a SortedBlock>,
    overlays: [Option<&'a Overlay<ValueRecordReference>>; N],
}

impl<'a> CellSources<'a, 1> {
    fn frozen(cell: &'a FrozenCell) -> Self {
        Self {
            base: cell.base.as_ref(),
            overlays: [Some(&*cell.overlay)],
        }
    }
}

impl<'a> CellSources<'a, 2> {
    fn captured(source: &CellEncodeSource<'a>) -> Self {
        let active: &'a std::sync::Arc<Overlay<ValueRecordReference>> = source.active;
        Self {
            base: source.base,
            overlays: [source.carry, Some(&**active)],
        }
    }
}

type OverlayRows<'a> = std::iter::Peekable<
    std::collections::btree_map::Iter<'a, Bytes, NativeVersions<ValueRecordReference>>,
>;

/// One merged row: its key, the encoded base versions and the versions of
/// each overlay source. A source without the key contributes nothing.
struct Row<'a, const N: usize> {
    key: &'a [u8],
    base: &'a [u8],
    overlays: [&'a [ValueRecordReference]; N],
}

/// Sorted merge of the base rows and the rows of every overlay source by key:
/// each key once, with the versions of every source that holds it.
struct Rows<'a, const N: usize> {
    base: Option<&'a SortedBlock>,
    position: usize,
    overlays: [Option<OverlayRows<'a>>; N],
}

impl<'a, const N: usize> Rows<'a, N> {
    fn new(sources: CellSources<'a, N>) -> Self {
        Self {
            base: sources.base,
            position: 0,
            overlays: sources
                .overlays
                .map(|overlay| overlay.map(|overlay| overlay.iter().peekable())),
        }
    }
}

impl<'a, const N: usize> Iterator for Rows<'a, N> {
    type Item = Row<'a, N>;
    fn next(&mut self) -> Option<Self::Item> {
        let base = self
            .base
            .filter(|base| self.position < base.count)
            .map(|base| base.entry(self.position));
        // One key comparison per source: track which sources hold the
        // smallest key while finding it.
        let mut key = base.map(|(key, _)| key);
        let mut in_base = base.is_some();
        let mut holders = [false; N];
        for (source, rows) in self.overlays.iter_mut().enumerate() {
            let Some(&(other, _)) = rows.as_mut().and_then(OverlayRows::peek) else {
                continue;
            };
            let other: &'a [u8] = &other[..];
            match key.map(|key| other.cmp(key)) {
                Some(Ordering::Greater) => {}
                Some(Ordering::Equal) => holders[source] = true,
                Some(Ordering::Less) | None => {
                    key = Some(other);
                    in_base = false;
                    holders = [false; N];
                    holders[source] = true;
                }
            }
        }
        let key = key?;
        let mut row = Row {
            key,
            base: &[],
            overlays: [&[]; N],
        };
        if in_base && let Some((_, versions)) = base {
            self.position += 1;
            row.base = versions;
        }
        for ((slot, rows), holds) in row.overlays.iter_mut().zip(&mut self.overlays).zip(holders) {
            if holds && let Some((_, values)) = rows.as_mut().and_then(Iterator::next) {
                *slot = values.as_slice();
            }
        }
        Some(row)
    }
}

/// Ascending merge of one row's retained versions from the base and every
/// overlay source. A base version equal to an overlay version is taken from
/// the base and both advance, as in the single-overlay encoder. An equal
/// version in two overlays breaks the C2 invariant that versions are distinct
/// across sources, so it fails the encoding instead of choosing one.
struct Versions<'a, const N: usize> {
    base: std::slice::ChunksExact<'a, u8>,
    overlays: [std::slice::Iter<'a, ValueRecordReference>; N],
    base_head: Option<ValueRecordReference>,
    heads: [Option<ValueRecordReference>; N],
}

impl<'a, const N: usize> Versions<'a, N> {
    fn new(base: &'a [u8], overlays: [&'a [ValueRecordReference]; N]) -> Self {
        let mut base = base.chunks_exact(REFERENCE_BYTES);
        let mut overlays = overlays.map(<[ValueRecordReference]>::iter);
        let base_head = base
            .next()
            .map(|bytes| decode_reference(bytes).expect("validated immutable reference"));
        let heads = std::array::from_fn(|n| overlays[n].next().copied());
        Self {
            base,
            overlays,
            base_head,
            heads,
        }
    }

    fn advance_base(&mut self) {
        self.base_head = self
            .base
            .next()
            .map(|bytes| decode_reference(bytes).expect("validated immutable reference"));
    }

    fn advance_overlay(&mut self, source: usize) {
        self.heads[source] = self.overlays[source].next().copied();
    }
}

impl<const N: usize> Iterator for Versions<'_, N> {
    type Item = Result<ValueRecordReference, NativeIndexError>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut lowest: Option<(usize, ValueRecordReference)> = None;
        for (source, head) in self.heads.iter().enumerate() {
            let Some(head) = *head else { continue };
            match lowest.map(|(_, low)| low.mutation_version().cmp(&head.mutation_version())) {
                Some(Ordering::Less) => {}
                Some(Ordering::Equal) => {
                    return Some(Err(NativeIndexError::Invalid(
                        "version repeated across overlay sources",
                    )));
                }
                Some(Ordering::Greater) | None => lowest = Some((source, head)),
            }
        }
        let result = match (self.base_head, lowest) {
            (None, None) => return None,
            (Some(base), None) => {
                self.advance_base();
                base
            }
            (Some(base), Some((source, overlay)))
                if base.mutation_version() <= overlay.mutation_version() =>
            {
                if base.mutation_version() == overlay.mutation_version() {
                    self.advance_overlay(source);
                }
                self.advance_base();
                base
            }
            (_, Some((source, overlay))) => {
                self.advance_overlay(source);
                overlay
            }
        };
        Some(Ok(result))
    }
}

fn retained<'a, const N: usize>(
    row: &Row<'a, N>,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
) -> Versions<'a, N> {
    // Preserve one anchor at/before the floor, including a tombstone, and every
    // later version. Binary-search old histories instead of scanning discarded
    // versions once per serialization pass. The anchor is the newest one of
    // every source, so the selection equals that of the merged history.
    let base = row.base;
    let count = base.len() / REFERENCE_BYTES;
    let reference = |n| {
        decode_reference(&base[n * REFERENCE_BYTES..(n + 1) * REFERENCE_BYTES])
            .expect("validated immutable reference")
    };
    let at_floor = lower_bound_by(count, |n| floor.contains(&reference(n)));
    let base_anchor = (at_floor > 0).then(|| reference(at_floor - 1));
    let anchor = row
        .overlays
        .iter()
        .fold(base_anchor, |anchor, overlay| {
            let overlay_anchor = matching_prefix(overlay, |value| floor.contains(value))
                .last()
                .copied();
            newest(anchor, overlay_anchor)
        })
        .map(|value| value.mutation_version());
    let base_start = lower_bound_by(count, |n| {
        anchor.is_some_and(|anchor| reference(n).mutation_version() < anchor)
    });
    let base_end = lower_bound_by(count, |n| cut.contains(&reference(n)));
    let overlays = row.overlays.map(|overlay| {
        let start = overlay.partition_point(|value| {
            anchor.is_some_and(|anchor| value.mutation_version() < anchor)
        });
        let end = overlay.partition_point(|value| cut.contains(value));
        &overlay[start..end]
    });
    Versions::new(
        &base[base_start * REFERENCE_BYTES..base_end * REFERENCE_BYTES],
        overlays,
    )
}

/// Retained version count of one row, or the duplicate-version error.
fn retained_len<const N: usize>(
    row: &Row<'_, N>,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
) -> Result<usize, NativeIndexError> {
    retained(row, cut, floor).try_fold(0usize, |n, reference| reference.map(|_| n + 1))
}

/// Rows with a retained version. A row whose first retained version is a
/// duplicate counts; the encoding passes then fail on it.
fn retained_rows<const N: usize>(
    sources: CellSources<'_, N>,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
) -> usize {
    Rows::new(sources)
        .filter(|row| retained(row, cut, floor).next().is_some())
        .count()
}

fn has_rows(cell: &FrozenCell, cut: &IndexReadCut, floor: &IndexReadCut) -> bool {
    *cut != IndexReadCut::BeforeAll && retained_rows(CellSources::frozen(cell), cut, floor) != 0
}

struct WrittenCell {
    file: CellFile,
    count: usize,
    retained_bytes: usize,
    retained_versions: usize,
}

/// One appended cell frame. `bytes` is the framed length, header included;
/// `digest` is BLAKE2b-32 of the header, then the payload. `count`,
/// `retained_bytes` and `retained_versions` equal what a validated
/// `SortedBlock` of the payload reports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CellFrame {
    pub offset: u64,
    pub bytes: u64,
    pub digest: [u8; 32],
    pub count: usize,
    pub retained_bytes: usize,
    pub retained_versions: usize,
}

#[derive(Debug, PartialEq)]
struct PayloadSummary {
    length: usize,
    retained_bytes: usize,
    retained_versions: usize,
}

/// Emit canonical sorted payload bytes without storing offsets or cold versions
/// in a heap table. The same traversal supplies the CRC before immutable append.
fn encode_cell_payload<const N: usize>(
    sources: CellSources<'_, N>,
    count: usize,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    reference_bytes: &mut Vec<u8>,
    mut emit: impl FnMut(&[u8]) -> Result<(), NativeIndexError>,
) -> Result<PayloadSummary, NativeIndexError> {
    let mut length = 0usize;
    let mut write = |bytes: &[u8]| -> Result<(), NativeIndexError> {
        length = length
            .checked_add(bytes.len())
            .ok_or(NativeIndexError::Capacity)?;
        if length > MAX_IMAGE_BYTES - 8 {
            return Err(NativeIndexError::Capacity);
        }
        emit(bytes)
    };
    write(
        &u32::try_from(count)
            .map_err(|_| NativeIndexError::Capacity)?
            .to_be_bytes(),
    )?;
    let mut offset = 4usize
        .checked_add(count.checked_mul(4).ok_or(NativeIndexError::Capacity)?)
        .ok_or(NativeIndexError::Capacity)?;
    for row in Rows::new(sources) {
        let n = retained_len(&row, cut, floor)?;
        if n == 0 {
            continue;
        }
        write(
            &u32::try_from(offset)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        )?;
        let row_bytes = n
            .checked_mul(REFERENCE_BYTES)
            .and_then(|bytes| bytes.checked_add(8 + row.key.len()))
            .ok_or(NativeIndexError::Capacity)?;
        offset = offset
            .checked_add(row_bytes)
            .ok_or(NativeIndexError::Capacity)?;
    }
    let mut retained_bytes = 0usize;
    let mut retained_versions = 0usize;
    for row in Rows::new(sources) {
        let n = retained_len(&row, cut, floor)?;
        if n == 0 {
            continue;
        }
        write(
            &u32::try_from(row.key.len())
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        )?;
        write(row.key)?;
        write(
            &u32::try_from(n)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        )?;
        for reference in retained(&row, cut, floor) {
            reference_bytes.clear();
            encode_reference(&reference?, reference_bytes);
            write(reference_bytes)?;
            retained_bytes += VERSION_CHARGE + row.key.len();
            retained_versions += 1;
        }
    }
    Ok(PayloadSummary {
        length,
        retained_bytes,
        retained_versions,
    })
}

/// Payload scratch ceiling of one encoder, independent of the index budget:
/// 8 MiB, which is below the payload limit of one frame.
const CELL_SCRATCH_CEILING: usize = 8 << 20;
const _: () = assert!(CELL_SCRATCH_CEILING <= MAX_IMAGE_BYTES - 8);

/// Payload scratch limit of one encoder (spec §3.1):
/// `S = min(8 MiB, M / (4 P), MAX_IMAGE_BYTES - 8)`, with `M` the resident
/// index budget and `P` the encoder count. All `P` scratches together stay at
/// or below `M / 4`. A cell above `S` streams; it is never rejected.
pub(super) fn cell_scratch_limit(max_metadata_bytes: usize, encoders: usize) -> usize {
    (max_metadata_bytes / encoders.max(1).saturating_mul(4)).min(CELL_SCRATCH_CEILING)
}

/// Counters of one image persistence. Buffered cells are encoded once into the
/// scratch; streamed cells use the exact three-pass fallback.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CellEncodeStats {
    pub buffered_cells: u64,
    pub streamed_cells: u64,
    /// Streamed cells whose final payload fits the scratch: their pre-encode
    /// size bound was above the scratch limit, the payload was not.
    pub conservative_fallbacks: u64,
    /// Rewritten cells without a retained row. They get no frame.
    pub empty_cells: u64,
    /// Canonical payload encodings: one per buffered and two per streamed cell.
    pub encode_passes: u64,
    /// Merged base/overlay row traversals, including the streaming count pass
    /// and both directory and row traversals of each streaming encoding.
    pub row_passes: u64,
    /// Reference encodings: once per retained version when buffered, twice
    /// when streamed.
    pub references_encoded: u64,
    /// Framed bytes, header included, appended by each path.
    pub buffered_bytes: u64,
    pub streamed_bytes: u64,
    /// Row-body bytes moved left over unused directory slots.
    pub moved_bytes: u64,
    /// Largest payload scratch capacity. Never above the scratch limit.
    pub scratch_capacity: u64,
}

/// One encoder's reused payload and reference scratch. A cell whose
/// conservative size bound fits `limit` is encoded once into `payload`; any
/// other cell streams, so the scratch never grows above `limit`. Each
/// persisting thread or cell flusher owns one encoder.
pub(super) struct CellEncoder {
    payload: Vec<u8>,
    reference: Vec<u8>,
    limit: usize,
    stats: CellEncodeStats,
}

impl CellEncoder {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            payload: Vec::new(),
            reference: Vec::with_capacity(REFERENCE_BYTES),
            limit: limit.min(CELL_SCRATCH_CEILING),
            stats: CellEncodeStats::default(),
        }
    }

    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "per-worker encoder counters, exported by the metrics stage"
        )
    )]
    pub(super) fn stats(&self) -> CellEncodeStats {
        self.stats
    }

    /// Append the canonical frame of an Exact-mode `cell` at `offset`.
    /// Returns `None`, and writes nothing, when the cell retains no row.
    fn append<W: Write>(
        &mut self,
        cell: &FrozenCell,
        writer: Option<&mut W>,
        path: &Path,
        offset: u64,
        cut: &IndexReadCut,
        floor: &IndexReadCut,
    ) -> Result<Option<WrittenCell>, NativeIndexError> {
        let frame = self.encode(CellSources::frozen(cell), cut, floor, |_| {
            let writer = writer.ok_or(NativeIndexError::Invalid("missing index container"))?;
            Ok((writer, offset))
        })?;
        Ok(frame.map(|frame| WrittenCell {
            file: CellFile {
                path: path.to_owned(),
                offset: frame.offset,
                bytes: frame.bytes,
                digest: frame.digest,
                floor: floor.clone(),
            },
            count: frame.count,
            retained_bytes: frame.retained_bytes,
            retained_versions: frame.retained_versions,
        }))
    }

    /// Encode one option C capture: base, captured carry and captured active
    /// in one merged traversal, selected at `source.cut` with the history
    /// floor `source.floor` (spec §3.2). The frame equals the one of a single
    /// overlay holding both captured overlays.
    ///
    /// `choose` receives the final framed length, header included, before any
    /// byte is written, and returns the writer and the frame offset. It is
    /// called once per frame and never for a cell without a retained row,
    /// which returns `None`. An error is fail-stop: `Invalid` for a version
    /// present in both captured overlays, `Capacity` for a size overflow.
    pub(super) fn encode_source<'w, W: Write + 'w>(
        &mut self,
        source: &CellEncodeSource<'_>,
        choose: impl FnOnce(u64) -> Result<(&'w mut W, u64), NativeIndexError>,
    ) -> Result<Option<CellFrame>, NativeIndexError> {
        self.encode(
            CellSources::captured(source),
            &source.cut,
            &source.floor,
            choose,
        )
    }

    /// The path is chosen before encoding: a buffered encode never overflows
    /// into a second encoding.
    fn encode<'w, W: Write + 'w, const N: usize>(
        &mut self,
        sources: CellSources<'_, N>,
        cut: &IndexReadCut,
        floor: &IndexReadCut,
        choose: impl FnOnce(u64) -> Result<(&'w mut W, u64), NativeIndexError>,
    ) -> Result<Option<CellFrame>, NativeIndexError> {
        self.stats.row_passes += 1;
        let (offset, count, bytes, digest, summary) = if let Some((rows, upper)) =
            buffered_bound(sources, self.limit)
        {
            let Some((count, summary)) = self.encode_buffered(sources, rows, upper, cut, floor)?
            else {
                self.stats.empty_cells += 1;
                return Ok(None);
            };
            let (writer, offset) = choose(framed_len(summary.length)?)?;
            let (bytes, digest) = write_frame(&self.payload, offset, writer)?;
            self.stats.buffered_cells += 1;
            self.stats.encode_passes += 1;
            self.stats.buffered_bytes += bytes;
            self.stats.references_encoded += summary.retained_versions as u64;
            (offset, count, bytes, digest, summary)
        } else {
            let count = retained_rows(sources, cut, floor);
            if count == 0 {
                self.stats.empty_cells += 1;
                return Ok(None);
            }
            let (offset, bytes, digest, summary) =
                stream_cell(sources, count, cut, floor, choose, &mut self.reference)?;
            self.stats.streamed_cells += 1;
            self.stats.encode_passes += 2;
            self.stats.row_passes += 4;
            self.stats.streamed_bytes += bytes;
            self.stats.references_encoded += 2 * summary.retained_versions as u64;
            if summary.length <= self.limit {
                self.stats.conservative_fallbacks += 1;
            }
            (offset, count, bytes, digest, summary)
        };
        Ok(Some(CellFrame {
            offset,
            bytes,
            digest,
            count,
            retained_bytes: summary.retained_bytes,
            retained_versions: summary.retained_versions,
        }))
    }

    /// One merged-row traversal that encodes each retained reference once.
    /// The payload is `[count][offsets][rows]`, and the offsets precede the
    /// rows, so the directory is reserved for every candidate row, filled per
    /// emitted row and compacted by at most one move of the row bodies.
    /// Returns the emitted row count, or `None` when no row is retained.
    fn encode_buffered<const N: usize>(
        &mut self,
        sources: CellSources<'_, N>,
        candidates: usize,
        upper: usize,
        cut: &IndexReadCut,
        floor: &IndexReadCut,
    ) -> Result<Option<(usize, PayloadSummary)>, NativeIndexError> {
        let payload = &mut self.payload;
        payload.clear();
        // `upper` bounds the provisional and the final length and is at most
        // `limit`, so this is the only growth and the capacity stays bounded.
        payload.reserve_exact(upper);
        // Recorded before encoding, so a cell without rows is counted too.
        self.stats.scratch_capacity = self.stats.scratch_capacity.max(payload.capacity() as u64);
        let body = 4 + 4 * candidates;
        payload.resize(body, 0);
        let mut rows = 0usize;
        let mut retained_bytes = 0usize;
        let mut retained_versions = 0usize;
        for row in Rows::new(sources) {
            let mut versions = retained(&row, cut, floor);
            let Some(first) = versions.next() else {
                continue;
            };
            let first = first?;
            let start = u32::try_from(payload.len()).map_err(|_| NativeIndexError::Capacity)?;
            payload[4 + 4 * rows..8 + 4 * rows].copy_from_slice(&start.to_be_bytes());
            payload.extend_from_slice(
                &u32::try_from(row.key.len())
                    .map_err(|_| NativeIndexError::Capacity)?
                    .to_be_bytes(),
            );
            payload.extend_from_slice(row.key);
            let count_at = payload.len();
            payload.extend_from_slice(&[0; 4]);
            let mut n = 0usize;
            for reference in std::iter::once(Ok(first)).chain(versions) {
                encode_reference(&reference?, payload);
                n += 1;
            }
            payload[count_at..count_at + 4].copy_from_slice(
                &u32::try_from(n)
                    .map_err(|_| NativeIndexError::Capacity)?
                    .to_be_bytes(),
            );
            retained_bytes += n * (VERSION_CHARGE + row.key.len());
            retained_versions += n;
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        let gap = 4 * (candidates - rows);
        if gap != 0 {
            // Every used offset points into the row bodies, past `body`, and
            // moves left by the same `gap`.
            payload.copy_within(body.., body - gap);
            payload.truncate(payload.len() - gap);
            let shift = u32::try_from(gap).map_err(|_| NativeIndexError::Capacity)?;
            for slot in payload[4..4 + 4 * rows].chunks_exact_mut(4) {
                let offset = u32::from_be_bytes([slot[0], slot[1], slot[2], slot[3]]) - shift;
                slot.copy_from_slice(&offset.to_be_bytes());
            }
            self.stats.moved_bytes += (payload.len() - (body - gap)) as u64;
        }
        payload[..4].copy_from_slice(
            &u32::try_from(rows)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        );
        debug_assert!(payload.len() <= upper, "cell payload above its bound");
        Ok(Some((
            rows,
            PayloadSummary {
                length: payload.len(),
                retained_bytes,
                retained_versions,
            },
        )))
    }
}

/// Conservative payload bound of `sources` as `(candidate rows, bound)`, or
/// `None` when the bound exceeds `limit` or overflows (spec §3.1 `U`). Every
/// base and overlay row counts its directory slot, row prefix, key and all
/// its versions. Equal keys, equal versions and discarded versions only reduce
/// the output, so the bound covers the reserved directory plus row bodies and
/// the final payload. The overlay walk stops at the limit and decodes no
/// reference.
fn buffered_bound<const N: usize>(
    sources: CellSources<'_, N>,
    limit: usize,
) -> Option<(usize, usize)> {
    // A base payload is `[count][offsets][rows]` with one offset per row.
    let mut upper = sources.base.map_or(4, |base| base.bytes.len());
    if upper > limit {
        return None;
    }
    let mut candidates = sources.base.map_or(0, |base| base.count);
    for overlay in sources.overlays.into_iter().flatten() {
        candidates = candidates.checked_add(overlay.len())?;
    }
    for (key, versions) in sources.overlays.into_iter().flatten().flatten() {
        upper = versions
            .len()
            .checked_mul(REFERENCE_BYTES)?
            .checked_add(12 + key.len())?
            .checked_add(upper)?;
        if upper > limit {
            return None;
        }
    }
    Some((candidates, upper))
}

/// Test-only base of one capture without a store: the production encoding of
/// `source` into a heap frame, validated as an unmapped base without a file.
/// No retained row gives the empty base.
#[cfg(test)]
pub(crate) fn encode_heap_base(
    index: &NativeIndex,
    source: &CellEncodeSource<'_>,
) -> Result<NewCellBase, NativeIndexError> {
    let mut frame = Vec::new();
    let encoded =
        CellEncoder::new(CELL_SCRATCH_CEILING).encode_source(source, |_| Ok((&mut frame, 0)))?;
    if encoded.is_none() {
        return Ok(NewCellBase::empty());
    }
    let (keyspace, cell) = source.identity;
    Ok(NewCellBase::frame(SortedBlock::validated(
        Bytes::from(frame).slice(8..),
        false,
        index.database,
        *keyspace,
        index.descriptor(*keyspace)?,
        cell,
        source.cut.clone(),
    )?))
}

/// Framed length of a payload: the eight-byte header, then the payload.
fn framed_len(payload: usize) -> Result<u64, NativeIndexError> {
    let length = u32::try_from(payload).map_err(|_| NativeIndexError::Capacity)?;
    Ok(u64::from(length) + 8)
}

/// Write the CRC frame of a final payload. CRC32 covers the payload; the
/// BLAKE2b-32 digest covers the final header, then the payload.
fn write_frame(
    payload: &[u8],
    offset: u64,
    writer: &mut impl Write,
) -> Result<(u64, [u8; 32]), NativeIndexError> {
    let length = u32::try_from(payload.len()).map_err(|_| NativeIndexError::Capacity)?;
    let bytes = u64::from(length) + 8;
    offset
        .checked_add(bytes)
        .ok_or(NativeIndexError::Capacity)?;
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4..].copy_from_slice(&crc32fast::hash(payload).to_be_bytes());
    let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
    hash.update(&header);
    hash.update(payload);
    writer.write_all(&header)?;
    writer.write_all(payload)?;
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.finalize().as_bytes());
    Ok((bytes, digest))
}

/// Exact streaming fallback for a cell with `count` retained rows: one
/// encoding computes the CRC, a second one hashes and writes. It holds no
/// payload in memory, so any accepted frame size is still possible.
fn stream_cell<'w, W: Write + 'w, const N: usize>(
    sources: CellSources<'_, N>,
    count: usize,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    choose: impl FnOnce(u64) -> Result<(&'w mut W, u64), NativeIndexError>,
    reference_bytes: &mut Vec<u8>,
) -> Result<(u64, u64, [u8; 32], PayloadSummary), NativeIndexError> {
    let mut crc = crc32fast::Hasher::new();
    let summary = encode_cell_payload(sources, count, cut, floor, reference_bytes, |bytes| {
        crc.update(bytes);
        Ok(())
    })?;
    let length = u32::try_from(summary.length).map_err(|_| NativeIndexError::Capacity)?;
    let bytes = u64::from(length) + 8;
    let (writer, offset) = choose(bytes)?;
    offset
        .checked_add(bytes)
        .ok_or(NativeIndexError::Capacity)?;
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4..].copy_from_slice(&crc.finalize().to_be_bytes());
    let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
    hash.update(&header);
    writer.write_all(&header)?;
    let emitted = encode_cell_payload(sources, count, cut, floor, reference_bytes, |bytes| {
        hash.update(bytes);
        writer.write_all(bytes)?;
        Ok(())
    })?;
    debug_assert_eq!(emitted, summary, "immutable captured cell serialization");
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.finalize().as_bytes());
    Ok((offset, bytes, digest, summary))
}

#[cfg(test)]
#[path = "cell_encode_tests.rs"]
mod cell_encode_tests;
