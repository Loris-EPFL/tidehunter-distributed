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

fn cell_digest(bytes: &[u8]) -> [u8; 32] {
    // Cell frames use BLAKE2b-32, not truncated BLAKE2b-512.
    let hash = blake2b_simd::Params::new().hash_length(32).hash(bytes);
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.as_bytes());
    digest
}

pub(super) struct FrozenCell {
    pub identity: (KeySpace, CellId),
    pub base: Option<SortedBlock>,
    pub overlay: Overlay<ValueRecordReference>,
}

pub(crate) struct PreparedIndexImage {
    database: [u8; 16],
    schema: [u8; 32],
    cells: Vec<FrozenCell>,
    cut: IndexReadCut,
    lease: CaptureLease,
}

pub(crate) struct PersistedIndexImage {
    pub(super) cells: Vec<FrozenCell>,
    pub(super) cut: IndexReadCut,
    pub(super) lease: CaptureLease,
    pub(crate) metadata: IndexImageMetadata,
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
    ) -> Self {
        Self {
            database,
            schema,
            cells,
            cut,
            lease,
        }
    }

    /// One candidate container for rewritten nonempty cells, never the root.
    /// Reservation precedes the background worker; no per-cell paths are owned.
    pub(crate) fn new_files(&self, path: &Path) -> Result<Vec<String>, NativeIndexError> {
        let (parent, name) = output_parts(path)?;
        Ok(if self.needs_container(parent) {
            vec![format!("{name}.cells")]
        } else {
            Vec::new()
        })
    }

    fn needs_container(&self, parent: &Path) -> bool {
        self.cells.iter().any(|cell| {
            !reusable(cell, parent, self.cut, self.lease.floor)
                && has_rows(cell, self.cut, self.lease.floor)
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
        {
            // One fixed buffer combines tiny cells into shared writes. Headers
            // are computed before append, so no per-cell seek/flush is needed.
            let mut writer = container
                .as_mut()
                .map(|file| BufWriter::with_capacity(16 * 1024, file));
            let mut offset = 0u64;
            let mut reference_bytes = if writer.is_some() {
                Vec::with_capacity(REFERENCE_BYTES)
            } else {
                Vec::new()
            };
            for (number, cell) in self.cells.iter_mut().enumerate() {
                if !reusable(cell, parent, self.cut, self.lease.floor) {
                    if !has_rows(cell, self.cut, self.lease.floor) {
                        cell.base = None;
                        continue;
                    }
                    let writer = writer
                        .as_mut()
                        .ok_or(NativeIndexError::Invalid("missing index container"))?;
                    let encoded = append_cell(
                        cell,
                        writer,
                        &container_path,
                        offset,
                        self.cut,
                        self.lease.floor,
                        &mut reference_bytes,
                    )?;
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
                    cut: self.cut,
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
        encode_cut(self.cut, &mut payload);
        encode_cut(self.lease.floor, &mut payload);
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
            encode_cut(file.floor, &mut payload);
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
        })
    }
}

fn reusable(cell: &FrozenCell, parent: &Path, cut: IndexReadCut, floor: IndexReadCut) -> bool {
    cell.overlay.is_empty()
        && cell.base.as_ref().is_some_and(|base| {
            retention::position(base.cut) <= retention::position(cut)
                && base.file.as_ref().is_some_and(|file| {
                    file.path.parent() == Some(parent)
                        && retention::position(file.floor) >= retention::position(floor)
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
fn map_file(file: &File, maximum: Option<u64>) -> Result<Bytes, NativeIndexError> {
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

fn frame_payload(mapped: &Bytes) -> Result<Bytes, NativeIndexError> {
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
        || retention::position(floor) > retention::position(cut)
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
    let mut index = NativeIndex::new(shape, database, max_metadata_bytes)?;
    let mut containers: HashMap<String, Container> = HashMap::new();
    for _ in 0..count {
        let keyspace = KeySpace(cursor.u8()?);
        let cell_id = decode_cell(&mut cursor)?;
        index.check_capacity(cell_directory_charge(&cell_id))?;
        index.resident_directory_bytes += cell_directory_charge(&cell_id);
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
            || retention::position(cell_floor) > retention::position(floor)
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
            cut,
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
            index.latest = Some(index.latest.map_or(reference.mutation_version(), |old| {
                old.max(reference.mutation_version())
            }));
        }
        if index
            .cells
            .insert(
                (keyspace, cell_id),
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
    index.retention.lock().floor = floor;
    index.recount_retained();
    Ok(index)
}

struct Rows<'a> {
    base: Option<&'a SortedBlock>,
    position: usize,
    overlay: std::iter::Peekable<
        std::collections::btree_map::Iter<'a, Bytes, NativeVersions<ValueRecordReference>>,
    >,
}

impl<'a> Rows<'a> {
    fn new(cell: &'a FrozenCell) -> Self {
        Self {
            base: cell.base.as_ref(),
            position: 0,
            overlay: cell.overlay.iter().peekable(),
        }
    }
}

impl<'a> Iterator for Rows<'a> {
    type Item = (&'a [u8], &'a [u8], &'a [ValueRecordReference]);
    fn next(&mut self) -> Option<Self::Item> {
        let base = self
            .base
            .filter(|base| self.position < base.count)
            .map(|base| base.entry(self.position));
        let overlay = self.overlay.peek();
        match (base, overlay) {
            (None, None) => None,
            (Some((key, versions)), None) => {
                self.position += 1;
                Some((key, versions, &[]))
            }
            (None, Some(_)) => self
                .overlay
                .next()
                .map(|(key, values)| (key.as_ref(), &[][..], values.as_slice())),
            (Some((key, versions)), Some((other, _))) => match key.cmp(other.as_ref()) {
                Ordering::Less => {
                    self.position += 1;
                    Some((key, versions, &[]))
                }
                Ordering::Greater => self
                    .overlay
                    .next()
                    .map(|(key, values)| (key.as_ref(), &[][..], values.as_slice())),
                Ordering::Equal => {
                    self.position += 1;
                    let (_, values) = self.overlay.next().unwrap();
                    Some((key, versions, values))
                }
            },
        }
    }
}

struct Versions<'a> {
    base: std::slice::ChunksExact<'a, u8>,
    overlay: std::slice::Iter<'a, ValueRecordReference>,
    a: Option<ValueRecordReference>,
    b: Option<ValueRecordReference>,
}

impl<'a> Versions<'a> {
    fn new(base: &'a [u8], overlay: &'a [ValueRecordReference]) -> Self {
        let mut result = Self {
            base: base.chunks_exact(REFERENCE_BYTES),
            overlay: overlay.iter(),
            a: None,
            b: None,
        };
        result.a = result
            .base
            .next()
            .map(|bytes| decode_reference(bytes).expect("validated immutable reference"));
        result.b = result.overlay.next().copied();
        result
    }
}

impl Iterator for Versions<'_> {
    type Item = ValueRecordReference;
    fn next(&mut self) -> Option<Self::Item> {
        let result = match (self.a, self.b) {
            (None, None) => return None,
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (Some(a), Some(b)) => {
                if a.mutation_version() <= b.mutation_version() {
                    a
                } else {
                    b
                }
            }
        };
        if self
            .a
            .is_some_and(|a| a.mutation_version() == result.mutation_version())
        {
            self.a = self
                .base
                .next()
                .map(|bytes| decode_reference(bytes).expect("validated immutable reference"));
        }
        if self
            .b
            .is_some_and(|b| b.mutation_version() == result.mutation_version())
        {
            self.b = self.overlay.next().copied();
        }
        Some(result)
    }
}

fn retained<'a>(
    base: &'a [u8],
    overlay: &'a [ValueRecordReference],
    cut: IndexReadCut,
    floor: IndexReadCut,
) -> Versions<'a> {
    // Preserve one anchor at/before the floor, including a tombstone, and every
    // later version. Binary-search old histories instead of scanning discarded
    // versions once per serialization pass.
    let count = base.len() / REFERENCE_BYTES;
    let reference = |n| {
        decode_reference(&base[n * REFERENCE_BYTES..(n + 1) * REFERENCE_BYTES])
            .expect("validated immutable reference")
    };
    let at_floor = lower_bound_by(count, |n| floor.contains(&reference(n)));
    let base_anchor = (at_floor > 0).then(|| reference(at_floor - 1));
    let overlay_anchor = matching_prefix(overlay, |value| floor.contains(value))
        .last()
        .copied();
    let anchor = newest(base_anchor, overlay_anchor).map(|value| value.mutation_version());
    let base_start = lower_bound_by(count, |n| {
        anchor.is_some_and(|anchor| reference(n).mutation_version() < anchor)
    });
    let base_end = lower_bound_by(count, |n| cut.contains(&reference(n)));
    let overlay_start = overlay
        .partition_point(|value| anchor.is_some_and(|anchor| value.mutation_version() < anchor));
    let overlay_end = overlay.partition_point(|value| cut.contains(value));
    Versions::new(
        &base[base_start * REFERENCE_BYTES..base_end * REFERENCE_BYTES],
        &overlay[overlay_start..overlay_end],
    )
}

fn has_rows(cell: &FrozenCell, cut: IndexReadCut, floor: IndexReadCut) -> bool {
    cut != IndexReadCut::BeforeAll
        && Rows::new(cell)
            .any(|(_, base, overlay)| retained(base, overlay, cut, floor).next().is_some())
}

struct WrittenCell {
    file: CellFile,
    count: usize,
    retained_bytes: usize,
    retained_versions: usize,
}

#[derive(Debug, PartialEq)]
struct PayloadSummary {
    length: usize,
    retained_bytes: usize,
    retained_versions: usize,
}

/// Emit canonical sorted payload bytes without storing offsets or cold versions
/// in a heap table. The same traversal supplies the CRC before immutable append.
fn encode_cell_payload(
    cell: &FrozenCell,
    count: usize,
    cut: IndexReadCut,
    floor: IndexReadCut,
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
    for (key, base, overlay) in Rows::new(cell) {
        let n = retained(base, overlay, cut, floor).count();
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
            .and_then(|bytes| bytes.checked_add(8 + key.len()))
            .ok_or(NativeIndexError::Capacity)?;
        offset = offset
            .checked_add(row_bytes)
            .ok_or(NativeIndexError::Capacity)?;
    }
    let mut retained_bytes = 0usize;
    let mut retained_versions = 0usize;
    for (key, base, overlay) in Rows::new(cell) {
        let n = retained(base, overlay, cut, floor).count();
        if n == 0 {
            continue;
        }
        write(
            &u32::try_from(key.len())
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        )?;
        write(key)?;
        write(
            &u32::try_from(n)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        )?;
        for reference in retained(base, overlay, cut, floor) {
            reference_bytes.clear();
            encode_reference(&reference, reference_bytes);
            write(reference_bytes)?;
            retained_bytes += VERSION_CHARGE + key.len();
            retained_versions += 1;
        }
    }
    Ok(PayloadSummary {
        length,
        retained_bytes,
        retained_versions,
    })
}

fn append_cell(
    cell: &FrozenCell,
    writer: &mut impl Write,
    path: &Path,
    offset: u64,
    cut: IndexReadCut,
    floor: IndexReadCut,
    reference_bytes: &mut Vec<u8>,
) -> Result<WrittenCell, NativeIndexError> {
    let count = Rows::new(cell)
        .filter(|(_, base, overlay)| retained(base, overlay, cut, floor).next().is_some())
        .count();
    if count == 0 {
        return Err(NativeIndexError::Invalid("empty cell append"));
    }
    let mut crc = crc32fast::Hasher::new();
    let summary = encode_cell_payload(cell, count, cut, floor, reference_bytes, |bytes| {
        crc.update(bytes);
        Ok(())
    })?;
    let length = u32::try_from(summary.length).map_err(|_| NativeIndexError::Capacity)?;
    let bytes = u64::from(length) + 8;
    offset
        .checked_add(bytes)
        .ok_or(NativeIndexError::Capacity)?;
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4..].copy_from_slice(&crc.finalize().to_be_bytes());
    let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
    hash.update(&header);
    writer.write_all(&header)?;
    let emitted = encode_cell_payload(cell, count, cut, floor, reference_bytes, |bytes| {
        hash.update(bytes);
        writer.write_all(bytes)?;
        Ok(())
    })?;
    debug_assert_eq!(emitted, summary, "immutable captured cell serialization");
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.finalize().as_bytes());
    Ok(WrittenCell {
        file: CellFile {
            path: path.to_owned(),
            offset,
            bytes,
            digest,
            floor,
        },
        count,
        retained_bytes: summary.retained_bytes,
        retained_versions: summary.retained_versions,
    })
}
