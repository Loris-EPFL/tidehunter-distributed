//! Option C metadata-only index root, format 5 (checkpoint option C spec
//! §7.1, §7.2, §8.1 steps 2-3, §10.2): binary codec, bounded capture from the
//! cell directory, byte-level persistence and the eager loader.
//!
//! # Format
//!
//! One CRC frame (`[payload_len:u32 BE][CRC32:u32 BE][payload]`, CRC32 over
//! the payload). The manifest binds the whole framed file by length and
//! BLAKE2b-32. All integers are big-endian and fixed width. Payload:
//!
//! - `THINDEX1`, format `u16 = 5`, flags `u16 = 0`, database `[16]`, schema
//!   digest `[32]`, authority epoch, snapshot generation and lifecycle
//!   generation (`u64` each);
//! - `T`, `F`, `U` as 17-byte [`PrefixCut`]s; `H` as a 21-byte mutation cut
//!   (tag 0 `BeforeAll` with zero body, tag 1 logical id). `Latest` and sparse
//!   tags do not exist on disk;
//! - file count and cell count (`u32` each);
//! - the sorted file table: file id (tag 0 legacy container: epoch,
//!   generation; tag 1 segment: epoch, stream `u16`, sequence), canonical
//!   name (`u16` length, UTF-8), sealed length `u64`;
//! - the sorted cell directory, cells without a frame included: keyspace
//!   `u8`, cell id (tag 0: `u64`; tag 1: `u16` length, bytes), coverage
//!   `C_i`, base flag `u8` (0 or 1) and, if 1: file id, offset, framed length,
//!   BLAKE2b-32 of the framed range, frame cut `B_i`, frame floor `H_i`.
//!
//! Row and version counts are not persisted: frame validation gives them.
//! Exact mode keeps format 4; each loader rejects the other format.
//!
//! # Crash consistency
//!
//! A root is an output artifact, never the commit: only the authority
//! manifest adopts it, and the manifest is written after [`PreparedCellRoot::
//! persist`] returns. The root is written to a staging name, synced, renamed
//! to its immutable generation name and the directory is synced; a crash at
//! any step leaves the previously adopted root and every file it references
//! untouched. The cell frames a root references are durable before capture
//! (a base is installed only after its segment and name are synced).
//!
//! # Locks
//!
//! Capture holds `writer` (and the caller's option-D publisher) only in
//! [`IndexWriter::begin_cell_root_capture`] and per slice of at most
//! [`ROOT_DESCRIPTOR_SLICE`] cells. Sorting, file-table construction,
//! encoding, hashing and all I/O run without any serving lock.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "option C root codec and loader, wired by the checkpoint and recovery stages"
    )
)]

use super::cell_contract::{
    CellRoot, IndexStoreFileId, PER_CELL_FORMAT, PREFIX_CUT_BYTES, RootCell, RootCellBase, RootFile,
};
use super::shards::{CellKey, add};
use super::snapshot::{CellFile, cell_digest, frame_payload, map_file};
use super::*;
use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::sync::Arc;

/// Encoded mutation cut: tag and 20-byte logical id.
const MUTATION_CUT_BYTES: usize = 21;
/// Longest encoded file id: a segment id.
const FILE_ID_BYTES: usize = 1 + 8 + 2 + 8;
/// Longest canonical file name: a segment with maximum epoch, stream and
/// sequence (`index-store-<20>-<5>-<20>.cells`).
const MAX_FILE_NAME_BYTES: usize = 65;
const ROOT_HEADER_BYTES: usize =
    8 + 2 + 2 + 16 + 32 + 3 * 8 + 3 * PREFIX_CUT_BYTES + MUTATION_CUT_BYTES + 4 + 4;
const ROOT_BASE_BYTES: usize = FILE_ID_BYTES + 8 + 8 + 32 + PREFIX_CUT_BYTES + MUTATION_CUT_BYTES;
const MIN_FILE_ENTRY_BYTES: usize = 1 + 8 + 8 + 2 + 8;
const MIN_CELL_ENTRY_BYTES: usize = 1 + 3 + PREFIX_CUT_BYTES + 1;

/// Fixed bytes of a root file: CRC header plus payload header.
pub(crate) const CELL_ROOT_FIXED_BYTES: usize = 8 + ROOT_HEADER_BYTES;
/// Largest file-table entry.
pub(crate) const CELL_ROOT_FILE_ENTRY_BYTES: usize = FILE_ID_BYTES + 2 + MAX_FILE_NAME_BYTES + 8;
/// Largest directory entry without its cell id; add the encoded cell id
/// length (`IntoBytesFixed::len`).
pub(crate) const CELL_ROOT_CELL_ENTRY_BYTES: usize = 1 + PREFIX_CUT_BYTES + 1 + ROOT_BASE_BYTES;

// Every cell contributes at most one directory entry, one file-table entry
// and its encoded id once, and is charged `CELL_CHARGE + 2 * id` in the
// resident directory. So a root is at most `CELL_ROOT_FIXED_BYTES` plus the
// directory charge, and the charge is at most the budget, at most half the
// image limit: a root of any admitted directory fits the 1 GiB format cap.
const _: () = assert!(CELL_ROOT_CELL_ENTRY_BYTES + CELL_ROOT_FILE_ENTRY_BYTES <= CELL_CHARGE);
const _: () = assert!(CELL_ROOT_FIXED_BYTES + MAX_IMAGE_BYTES / 2 <= MAX_IMAGE_BYTES);

/// Upper bound of the framed root bytes of `cells` directory entries whose
/// encoded cell ids total `cell_id_bytes`, referencing `files` files (at most
/// one per cell with a frame). `None` on overflow.
pub(crate) fn cell_root_bytes_bound(
    cells: usize,
    cell_id_bytes: usize,
    files: usize,
) -> Option<usize> {
    cells
        .checked_mul(CELL_ROOT_CELL_ENTRY_BYTES)?
        .checked_add(cell_id_bytes)?
        .checked_add(files.checked_mul(CELL_ROOT_FILE_ENTRY_BYTES)?)?
        .checked_add(CELL_ROOT_FIXED_BYTES)
}

/// True if a root of this shape is within `MAX_IMAGE_BYTES`, CRC header
/// included.
pub(crate) fn cell_root_fits(cells: usize, cell_id_bytes: usize, files: usize) -> bool {
    cell_root_bytes_bound(cells, cell_id_bytes, files).is_some_and(|bytes| bytes <= MAX_IMAGE_BYTES)
}

impl NativeIndex {
    /// Upper bound of the framed root of the current directory, from its
    /// resident charge (see the assertions above). At most
    /// `CELL_ROOT_FIXED_BYTES + max_metadata_bytes`, so always below
    /// `MAX_IMAGE_BYTES`; `checkpoint_metadata_fits` uses it for the root
    /// part. Relaxed: an independent counter that only grows under the
    /// writer; a caller that needs a stable value holds the writer.
    pub(crate) fn max_cell_root_bytes(&self) -> usize {
        CELL_ROOT_FIXED_BYTES
            + self
                .accounting
                .directory
                .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Immutable final name of a root generation.
pub(crate) fn cell_root_name(authority_epoch: u64, generation: u64) -> String {
    format!("index-root-{authority_epoch}-{generation}.cellroot")
}

/// Inverse of [`cell_root_name`]; non-canonical spellings are rejected.
pub(crate) fn parse_cell_root_name(name: &str) -> Option<(u64, u64)> {
    let rest = name
        .strip_prefix("index-root-")?
        .strip_suffix(".cellroot")?;
    let (epoch, generation) = rest.split_once('-')?;
    let parsed = (epoch.parse().ok()?, generation.parse().ok()?);
    (cell_root_name(parsed.0, parsed.1) == name).then_some(parsed)
}

/// Staging name of a root while it is written, the convention of
/// `storage::install_metadata`. A staging file is never adopted; restart
/// cleanup removes it.
pub(crate) fn cell_root_staging_name(name: &str) -> String {
    format!(".{name}.pending")
}

fn mutation_cut(version: Option<MutationVersion>) -> IndexReadCut {
    version.map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
}

fn decode_mutation_cut(
    cursor: &mut Cursor<'_>,
) -> Result<Option<MutationVersion>, NativeIndexError> {
    match decode_cut(cursor)? {
        IndexReadCut::BeforeAll => Ok(None),
        IndexReadCut::At(version) => Ok(Some(version)),
        IndexReadCut::Latest | IndexReadCut::Sparse(_) => {
            Err(NativeIndexError::Invalid("mutation cut tag"))
        }
    }
}

fn file_id_len(id: IndexStoreFileId) -> usize {
    match id {
        IndexStoreFileId::LegacyContainer { .. } => 1 + 8 + 8,
        IndexStoreFileId::Segment { .. } => FILE_ID_BYTES,
    }
}

fn encode_file_id(id: IndexStoreFileId, output: &mut Vec<u8>) {
    match id {
        IndexStoreFileId::LegacyContainer {
            authority_epoch,
            generation,
        } => {
            output.push(0);
            output.extend_from_slice(&authority_epoch.to_be_bytes());
            output.extend_from_slice(&generation.to_be_bytes());
        }
        IndexStoreFileId::Segment {
            authority_epoch,
            stream_id,
            segment_seq,
        } => {
            output.push(1);
            output.extend_from_slice(&authority_epoch.to_be_bytes());
            output.extend_from_slice(&stream_id.to_be_bytes());
            output.extend_from_slice(&segment_seq.to_be_bytes());
        }
    }
}

fn decode_file_id(cursor: &mut Cursor<'_>) -> Result<IndexStoreFileId, NativeIndexError> {
    match cursor.u8()? {
        0 => Ok(IndexStoreFileId::LegacyContainer {
            authority_epoch: cursor.u64()?,
            generation: cursor.u64()?,
        }),
        1 => Ok(IndexStoreFileId::Segment {
            authority_epoch: cursor.u64()?,
            stream_id: cursor.u16()?,
            segment_seq: cursor.u64()?,
        }),
        _ => Err(NativeIndexError::Invalid("file id tag")),
    }
}

fn encode_cell(cell: &CellId, output: &mut Vec<u8>) -> Result<(), NativeIndexError> {
    match cell {
        CellId::Integer(value) => {
            output.push(0);
            output.extend_from_slice(
                &u64::try_from(*value)
                    .map_err(|_| NativeIndexError::Invalid("cell integer"))?
                    .to_be_bytes(),
            );
        }
        CellId::Bytes(bytes) => {
            output.push(1);
            output.extend_from_slice(
                &u16::try_from(bytes.len())
                    .map_err(|_| NativeIndexError::Invalid("cell id length"))?
                    .to_be_bytes(),
            );
            output.extend_from_slice(bytes);
        }
    }
    Ok(())
}

/// Exact payload length of `root`; `Capacity` above the frame limit.
fn encoded_payload_len(root: &CellRoot) -> Result<usize, NativeIndexError> {
    let files = root.files.iter().try_fold(0usize, |total, file| {
        if file.name.len() > MAX_FILE_NAME_BYTES {
            return Err(NativeIndexError::Invalid("file table name"));
        }
        total
            .checked_add(file_id_len(file.id) + 2 + file.name.len() + 8)
            .ok_or(NativeIndexError::Capacity)
    })?;
    let cells = root.cells.iter().try_fold(0usize, |total, cell| {
        let base = cell.base.as_ref().map_or(0, |base| {
            file_id_len(base.file) + 8 + 8 + 32 + PREFIX_CUT_BYTES + MUTATION_CUT_BYTES
        });
        total
            .checked_add(1 + cell.cell.len() + PREFIX_CUT_BYTES + 1 + base)
            .ok_or(NativeIndexError::Capacity)
    })?;
    let length = ROOT_HEADER_BYTES
        .checked_add(files)
        .and_then(|length| length.checked_add(cells))
        .ok_or(NativeIndexError::Capacity)?;
    if length > MAX_IMAGE_BYTES - 8 {
        return Err(NativeIndexError::Capacity);
    }
    Ok(length)
}

/// The framed root file of `root`, encoded once into an exactly sized
/// buffer. Does not run [`CellRoot::check`]; persistence only accepts
/// checked roots.
fn encode_cell_root(root: &CellRoot) -> Result<Vec<u8>, NativeIndexError> {
    let length = encoded_payload_len(root)?;
    let mut output = Vec::with_capacity(8 + length);
    output.extend_from_slice(&[0; 8]);
    let header = &root.header;
    output.extend_from_slice(MAGIC);
    output.extend_from_slice(&header.format.to_be_bytes());
    output.extend_from_slice(&header.flags.to_be_bytes());
    output.extend_from_slice(&header.database);
    output.extend_from_slice(&header.schema);
    output.extend_from_slice(&header.authority_epoch.to_be_bytes());
    output.extend_from_slice(&header.snapshot_generation.to_be_bytes());
    output.extend_from_slice(&header.lifecycle_generation.to_be_bytes());
    header.directory_target.encode_into(&mut output);
    header.replay_floor.encode_into(&mut output);
    header.max_frame_cut.encode_into(&mut output);
    encode_cut(&mutation_cut(header.history_floor), &mut output);
    // Both counts fit: the payload is below 1 GiB and every entry is
    // larger than one byte.
    for count in [root.files.len(), root.cells.len()] {
        output.extend_from_slice(
            &u32::try_from(count)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        );
    }
    for file in &root.files {
        encode_file_id(file.id, &mut output);
        // `encoded_payload_len` bounded the name to MAX_FILE_NAME_BYTES.
        output.extend_from_slice(&(file.name.len() as u16).to_be_bytes());
        output.extend_from_slice(file.name.as_bytes());
        output.extend_from_slice(&file.sealed_len.to_be_bytes());
    }
    for cell in &root.cells {
        output.push(cell.keyspace.0);
        encode_cell(&cell.cell, &mut output)?;
        cell.coverage.encode_into(&mut output);
        match &cell.base {
            None => output.push(0),
            Some(base) => {
                output.push(1);
                encode_file_id(base.file, &mut output);
                output.extend_from_slice(&base.offset.to_be_bytes());
                output.extend_from_slice(&base.framed_len.to_be_bytes());
                output.extend_from_slice(&base.digest);
                base.frame_cut.encode_into(&mut output);
                encode_cut(&mutation_cut(base.frame_floor), &mut output);
            }
        }
    }
    debug_assert_eq!(output.len(), 8 + length);
    let payload_len = u32::try_from(length).map_err(|_| NativeIndexError::Capacity)?;
    let crc = crc32fast::hash(&output[8..]);
    output[..4].copy_from_slice(&payload_len.to_be_bytes());
    output[4..8].copy_from_slice(&crc.to_be_bytes());
    Ok(output)
}

/// Decode a root payload (after its CRC frame). `max_cells` (the directory
/// the budget can charge) and the remaining bytes bound every allocation
/// before it is made.
fn decode_cell_root(payload: &[u8], max_cells: usize) -> Result<CellRoot, NativeIndexError> {
    let mut cursor = Cursor::new(payload);
    if cursor.take(8)? != MAGIC {
        return Err(NativeIndexError::Invalid("cell root magic"));
    }
    // Format 4 has a different layout: reject it before reading further.
    let format = cursor.u16()?;
    if format != PER_CELL_FORMAT {
        return Err(NativeIndexError::Invalid("cell root format"));
    }
    let header = CellRootHeader {
        format,
        flags: cursor.u16()?,
        database: cursor.take(16)?.try_into().unwrap(),
        schema: cursor.take(32)?.try_into().unwrap(),
        authority_epoch: cursor.u64()?,
        snapshot_generation: cursor.u64()?,
        lifecycle_generation: cursor.u64()?,
        directory_target: PrefixCut::decode(&mut cursor)?,
        replay_floor: PrefixCut::decode(&mut cursor)?,
        max_frame_cut: PrefixCut::decode(&mut cursor)?,
        history_floor: decode_mutation_cut(&mut cursor)?,
    };
    let file_count = cursor.u32()? as usize;
    let cell_count = cursor.u32()? as usize;
    if cell_count > max_cells {
        return Err(NativeIndexError::Capacity);
    }
    if file_count
        .checked_mul(MIN_FILE_ENTRY_BYTES)
        .zip(cell_count.checked_mul(MIN_CELL_ENTRY_BYTES))
        .and_then(|(files, cells)| files.checked_add(cells))
        .is_none_or(|minimum| minimum > cursor.remaining())
    {
        return Err(NativeIndexError::Invalid("cell root counts"));
    }
    let mut files = Vec::with_capacity(file_count);
    for _ in 0..file_count {
        let id = decode_file_id(&mut cursor)?;
        let name_len = cursor.u16()? as usize;
        if name_len > MAX_FILE_NAME_BYTES {
            return Err(NativeIndexError::Invalid("file table name"));
        }
        let name = std::str::from_utf8(cursor.take(name_len)?)
            .map_err(|_| NativeIndexError::Invalid("file table name"))?
            .to_owned();
        files.push(RootFile {
            id,
            name,
            sealed_len: cursor.u64()?,
        });
    }
    let mut cells = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        let keyspace = KeySpace(cursor.u8()?);
        let cell = decode_cell(&mut cursor)?;
        let coverage = PrefixCut::decode(&mut cursor)?;
        let base = match cursor.u8()? {
            0 => None,
            1 => Some(RootCellBase {
                file: decode_file_id(&mut cursor)?,
                offset: cursor.u64()?,
                framed_len: cursor.u64()?,
                digest: cursor.take(32)?.try_into().unwrap(),
                frame_cut: PrefixCut::decode(&mut cursor)?,
                frame_floor: decode_mutation_cut(&mut cursor)?,
            }),
            _ => return Err(NativeIndexError::Invalid("cell base tag")),
        };
        cells.push(RootCell {
            keyspace,
            cell,
            coverage,
            base,
        });
    }
    if cursor.remaining() != 0 {
        return Err(NativeIndexError::Invalid("trailing cell root bytes"));
    }
    Ok(CellRoot {
        header,
        files,
        cells,
    })
}

/// Read, authenticate and decode the format-5 root at `path` (spec §8.1
/// step 2, root part). `expected` is the length and BLAKE2b-32 of the
/// framed file that the adopted manifest records. The returned root passed
/// [`CellRoot::check`]; no frame was opened yet.
pub(crate) fn read_cell_root(
    path: &Path,
    expected: IndexImageMetadata,
    shape: &KeyShape,
    database: [u8; 16],
    max_metadata_bytes: usize,
) -> Result<CellRoot, NativeIndexError> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length != expected.bytes || !(8..=MAX_IMAGE_BYTES as u64).contains(&length) {
        return Err(NativeIndexError::Invalid("cell root length"));
    }
    // `length` is at most 1 GiB, checked above.
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(length + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(NativeIndexError::Invalid("cell root length"));
    }
    if <[u8; 32]>::from(Blake2b::<U32>::digest(&bytes)) != expected.digest {
        return Err(NativeIndexError::Invalid("cell root digest"));
    }
    let payload = CrcFrame::read_from_slice(&bytes, 0)
        .map_err(|_| NativeIndexError::Invalid("cell root CRC"))?;
    if payload.len() + 8 != bytes.len() {
        return Err(NativeIndexError::Invalid("trailing cell root bytes"));
    }
    let root = decode_cell_root(payload, max_metadata_bytes / CELL_CHARGE)?;
    root.check(database, schema_digest(shape)?)?;
    Ok(root)
}

/// Map one file-table entry read-only from `directory`. For legacy
/// containers and tests; the index store maps its own segments and pins
/// them against GC.
pub(crate) fn map_root_file(directory: &Path, file: &RootFile) -> Result<Bytes, NativeIndexError> {
    // `CellRoot::check` made the name canonical: no separator or `..`.
    map_file(&File::open(directory.join(&file.name))?, None)
}

/// Same predicate as the shard writer's cell membership check: a directory
/// entry must be a cell its keyspace can route to, with or without a frame.
fn check_cell_identity(descriptor: &KeySpaceDesc, cell: &CellId) -> Result<(), NativeIndexError> {
    let valid = match (descriptor.key_type(), cell) {
        (KeyType::Uniform(config), CellId::Integer(id)) => config
            .cells_per_mutex()
            .checked_mul(descriptor.num_mutexes())
            .and_then(|count| u32::try_from(count).ok())
            .is_some_and(|count| (*id as u128) < u128::from(count)),
        (KeyType::PrefixedUniform(_), CellId::Bytes(prefix)) => {
            matches!(descriptor.first_cell(), CellId::Bytes(first) if first.len() == prefix.len())
                && descriptor.cell_id(prefix) == *cell
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(NativeIndexError::Invalid(
            "cell does not belong to keyspace",
        ))
    }
}

/// Authenticate and validate one frame (spec §8.1 step 2): BLAKE2b-32 of the
/// framed range, CRC and exact framing, canonical offsets and key order,
/// key routing, reference domain and order, every reference at or below
/// `B_i`, and at most one version per row at or below `H_i` (the anchor).
fn load_frame(
    index: &NativeIndex,
    mapped: &Bytes,
    directory: &Path,
    file: &RootFile,
    cell: &RootCell,
    base: &RootCellBase,
) -> Result<SortedBlock, NativeIndexError> {
    let start =
        usize::try_from(base.offset).map_err(|_| NativeIndexError::Invalid("cell offset"))?;
    let end = base
        .offset
        .checked_add(base.framed_len)
        .and_then(|end| usize::try_from(end).ok())
        .filter(|end| *end <= mapped.len())
        .ok_or(NativeIndexError::Invalid("cell range bounds"))?;
    let frame = mapped.slice(start..end);
    if cell_digest(&frame) != base.digest {
        return Err(NativeIndexError::Invalid("cell range digest"));
    }
    let mut block = SortedBlock::validated(
        frame_payload(&frame)?,
        true,
        index.database,
        cell.keyspace,
        index.descriptor(cell.keyspace)?,
        &cell.cell,
        base.frame_cut.index_cut(),
    )?;
    if let Some(floor) = base.frame_floor {
        let floor = IndexReadCut::At(floor);
        for n in 0..block.count {
            // Versions ascend: a second version within the floor means
            // more than one.
            let (_, versions) = block.entry(n);
            if let Some(second) = versions.get(REFERENCE_BYTES..2 * REFERENCE_BYTES)
                && floor.contains(&decode_reference(second)?)
            {
                return Err(NativeIndexError::Invalid("frame history floor"));
            }
        }
    }
    block.file = Some(CellFile {
        path: directory.join(&file.name),
        offset: base.offset,
        bytes: base.framed_len,
        digest: base.digest,
        floor: mutation_cut(base.frame_floor),
    });
    Ok(block)
}

impl NativeIndex {
    /// Build the recovery index of an authenticated root (spec §8.1 steps
    /// 2-3). `map` maps each file-table entry once (the index store for
    /// segments, [`map_root_file`] otherwise); its length must equal the
    /// sealed length. Every frame is authenticated and validated before it
    /// is installed.
    ///
    /// The result holds every root cell, cells without a frame included, as
    /// a mapped base with `cut = B_i` and coverage `D_i = C_i`. New cells get
    /// coverage `F`. The publication watermark and the writer's latest
    /// version are `F`, not the newest frame reference: `Latest` reads do
    /// not see the references above `F` that bases already hold until
    /// recovery replays their batches. The retention floor is `H`. Exact
    /// capture is refused: this database uses per-cell checkpoints.
    pub(crate) fn open_cell_root(
        root: &CellRoot,
        directory: &Path,
        shape: &KeyShape,
        database: [u8; 16],
        max_metadata_bytes: usize,
        shards: usize,
        mut map: impl FnMut(&RootFile) -> Result<Bytes, NativeIndexError>,
    ) -> Result<Self, NativeIndexError> {
        // The frame checks below rely on the structural rules (one file per
        // id, ranges within sealed lengths and disjoint, cuts ordered).
        root.check(database, schema_digest(shape)?)?;
        if root.cells.len() > max_metadata_bytes / CELL_CHARGE {
            return Err(NativeIndexError::Capacity);
        }
        let mut index = Self::with_shards(shape, database, max_metadata_bytes, shards)?;
        let mut mapped = Vec::with_capacity(root.files.len());
        for file in &root.files {
            let bytes = map(file)?;
            if bytes.len() as u64 != file.sealed_len {
                return Err(NativeIndexError::Invalid("cell file length"));
            }
            mapped.push(bytes);
        }
        for cell in &root.cells {
            check_cell_identity(index.descriptor(cell.keyspace)?, &cell.cell)?;
            let charge = cell_directory_charge(&cell.cell);
            index.check_capacity(charge)?;
            add(&index.accounting.directory, charge);
            let base = match &cell.base {
                None => None,
                Some(base) => {
                    let at = root
                        .files
                        .binary_search_by(|file| file.id.cmp(&base.file))
                        .map_err(|_| NativeIndexError::Invalid("cell file id"))?;
                    Some(load_frame(
                        &index,
                        &mapped[at],
                        directory,
                        &root.files[at],
                        cell,
                        base,
                    )?)
                }
            };
            let identity = (cell.keyspace, cell.cell.clone());
            index.writer.get_mut().cell_order.push(identity.clone());
            let shard = index.shard_index(identity.0, &identity.1);
            // The directory is strictly ordered (checked): no duplicate.
            index.shards[shard].get_mut().insert(
                identity,
                CellIndex {
                    base,
                    flush: CellFlushState::new(cell.coverage),
                    ..Default::default()
                },
            );
        }
        let floor = root.header.replay_floor;
        let writer = index.writer.get_mut();
        writer.cells.floor = floor;
        writer.cells.active = true;
        writer.latest = match floor {
            PrefixCut::BeforeAll => None,
            PrefixCut::Through(batch) => Some(MutationVersion {
                batch,
                operation_index: u32::MAX,
            }),
        };
        index
            .retention
            .set_floor(mutation_cut(root.header.history_floor));
        index.recount_retained();
        index.published.store(Arc::new(floor.index_cut()));
        Ok(index)
    }
}

/// Header fields of a root that the snapshot actor supplies.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CellRootParams {
    pub authority_epoch: u64,
    /// Nonzero.
    pub snapshot_generation: u64,
    pub lifecycle_generation: u64,
    /// `H`: at or below `F` and at or above every pinned frame floor.
    pub history_floor: Option<MutationVersion>,
}

/// A metadata root capture in progress (spec §7.2 steps 1-3).
///
/// 1. [`IndexWriter::begin_cell_root_capture`] under writer -> publisher:
///    `T = L` and the directory boundary. Allocates nothing.
/// 2. [`Self::reserve`] without serving locks: the descriptor buffer for the
///    whole boundary, so no slice allocates.
/// 3. [`Self::capture_slice`] under writer -> publisher, repeated: at most
///    [`ROOT_DESCRIPTOR_SLICE`] cells per hold.
/// 4. [`Self::finish`] without serving locks.
///
/// Cells inserted after the boundary have only mutations above `T` (every
/// batch through `L` was published, so its cells existed), so omitting them
/// cannot make `F` pass missing work.
pub(crate) struct CellRootCapture {
    retention: Arc<RetentionState>,
    target: PrefixCut,
    end: usize,
    descriptors: Vec<RootCellDescriptor>,
}

impl IndexWriter<'_> {
    /// Step 1 of a root capture. Call with the option-D publisher held so
    /// `T` is the dependency-closed published prefix.
    pub(crate) fn begin_cell_root_capture(&self) -> CellRootCapture {
        CellRootCapture {
            retention: Arc::clone(&self.index.retention),
            target: self.index.published_prefix(),
            end: self.state.cell_order.len(),
            descriptors: Vec::new(),
        }
    }
}

impl CellRootCapture {
    /// `T`, the directory-capture target.
    pub(crate) fn target(&self) -> PrefixCut {
        self.target
    }

    /// Reserve the descriptor buffer for the whole boundary. Call without
    /// serving locks, before the first slice.
    pub(crate) fn reserve(&mut self) -> Result<(), NativeIndexError> {
        self.descriptors
            .try_reserve_exact(self.end - self.descriptors.len())
            .map_err(|_| NativeIndexError::Capacity)
    }

    /// Capture the next slice under writer -> publisher. True when the
    /// boundary is complete. Fails if the buffer was not reserved.
    pub(crate) fn capture_slice(
        &mut self,
        writer: &mut IndexWriter<'_>,
    ) -> Result<bool, NativeIndexError> {
        if !Arc::ptr_eq(&self.retention, &writer.index.retention) {
            return Err(NativeIndexError::Invalid("foreign root capture"));
        }
        let start = self.descriptors.len();
        let end = start.saturating_add(ROOT_DESCRIPTOR_SLICE).min(self.end);
        writer.capture_cell_descriptors(start..end, self.target, &mut self.descriptors)?;
        Ok(self.descriptors.len() == self.end)
    }

    /// Build the checked root outside every serving lock (spec §7.1, §7.2
    /// steps 3-5). `F` is the minimum exported coverage, or `T` for an empty
    /// directory; `U = max(T, every B_i)` and must not exceed
    /// `complete_prefix`, a complete published prefix the caller observed
    /// after the last slice. `sealed_len` gives the sealed length of each
    /// referenced file (the index store's catalog). The descriptors stay
    /// pinned in the result, so every selected base mapping lives until the
    /// result is dropped after adoption.
    pub(crate) fn finish(
        self,
        index: &NativeIndex,
        directory: &Path,
        params: CellRootParams,
        complete_prefix: PrefixCut,
        mut sealed_len: impl FnMut(IndexStoreFileId) -> Result<u64, NativeIndexError>,
    ) -> Result<PreparedCellRoot, NativeIndexError> {
        if !Arc::ptr_eq(&self.retention, &index.retention) {
            return Err(NativeIndexError::Invalid("foreign root capture"));
        }
        if self.descriptors.len() != self.end {
            return Err(NativeIndexError::Invalid("root capture incomplete"));
        }
        if let PrefixCut::Through(batch) = complete_prefix {
            // The watermark compares batches: the operation is irrelevant.
            index.check_published(&IndexReadCut::At(MutationVersion {
                batch,
                operation_index: 0,
            }))?;
        }
        let target = self.target;
        let mut descriptors = self.descriptors;
        descriptors.sort_unstable_by(|a, b| directory_order(&a.identity, &b.identity));
        let mut ids = BTreeSet::new();
        let mut cells = Vec::with_capacity(descriptors.len());
        let mut floor = target;
        let mut upper = target;
        for descriptor in &descriptors {
            floor = floor.min(descriptor.coverage);
            let base = match &descriptor.base {
                None => None,
                Some(frame) => {
                    let file = frame
                        .block
                        .file
                        .as_ref()
                        .ok_or(NativeIndexError::Invalid("cell base has no durable file"))?;
                    let id = file_id(directory, file)?;
                    ids.insert(id);
                    upper = upper.max(frame.frame_cut);
                    Some(RootCellBase {
                        file: id,
                        offset: file.offset,
                        framed_len: file.bytes,
                        digest: file.digest,
                        frame_cut: frame.frame_cut,
                        frame_floor: frame.frame_floor,
                    })
                }
            };
            cells.push(RootCell {
                keyspace: descriptor.identity.0,
                cell: descriptor.identity.1.clone(),
                coverage: descriptor.coverage,
                base,
            });
        }
        if upper > complete_prefix {
            return Err(NativeIndexError::Invalid(
                "frame cut above the complete published prefix",
            ));
        }
        let mut files = Vec::with_capacity(ids.len());
        for id in ids {
            files.push(RootFile {
                id,
                name: id.canonical_name(),
                sealed_len: sealed_len(id)?,
            });
        }
        let schema = schema_digest(&index.shape)?;
        let root = CellRoot {
            header: CellRootHeader {
                format: PER_CELL_FORMAT,
                flags: 0,
                database: index.database,
                schema,
                authority_epoch: params.authority_epoch,
                snapshot_generation: params.snapshot_generation,
                lifecycle_generation: params.lifecycle_generation,
                directory_target: target,
                replay_floor: floor,
                max_frame_cut: upper,
                history_floor: params.history_floor,
            },
            files,
            cells,
        };
        root.check(index.database, schema)?;
        encoded_payload_len(&root)?;
        Ok(PreparedCellRoot {
            root,
            _pins: descriptors,
        })
    }
}

/// Cell directory order of [`CellRoot::check`]: keyspace, id kind, id.
fn directory_order(left: &CellKey, right: &CellKey) -> std::cmp::Ordering {
    let bytes = |cell: &CellId| matches!(cell, CellId::Bytes(_));
    left.0
        .0
        .cmp(&right.0.0)
        .then_with(|| bytes(&left.1).cmp(&bytes(&right.1)))
        .then_with(|| left.1.cmp(&right.1))
}

/// The file id of a base's backing file: canonical name, in `directory`.
fn file_id(directory: &Path, file: &CellFile) -> Result<IndexStoreFileId, NativeIndexError> {
    if file.path.parent() != Some(directory) {
        return Err(NativeIndexError::Invalid(
            "cell file outside the database directory",
        ));
    }
    file.path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(IndexStoreFileId::parse_canonical)
        .ok_or(NativeIndexError::Invalid("cell file name is not canonical"))
}

/// A checked root ready to persist. Holds the captured descriptors, which
/// pin every referenced base mapping.
pub(crate) struct PreparedCellRoot {
    root: CellRoot,
    _pins: Vec<RootCellDescriptor>,
}

impl PreparedCellRoot {
    pub(crate) fn root(&self) -> &CellRoot {
        &self.root
    }

    /// Write the root as [`cell_root_name`] of its epoch and generation in
    /// `directory` (spec §7.3 step 3) and return the length and BLAKE2b-32
    /// that the manifest must record. The name must not exist.
    pub(crate) fn persist(&self, directory: &Path) -> Result<IndexImageMetadata, NativeIndexError> {
        persist_root(directory, &self.root, |_| Ok(()))
    }
}

/// Persistence steps after which a crash can occur; test hooks only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootPersistStep {
    /// Staging file created (empty).
    Created,
    /// Frame written, not synced.
    Written,
    /// Staging file synced, not renamed.
    Synced,
    /// Renamed to the final name, directory not synced.
    Renamed,
}

fn persist_root(
    directory: &Path,
    root: &CellRoot,
    hook: impl FnMut(RootPersistStep) -> std::io::Result<()>,
) -> Result<IndexImageMetadata, NativeIndexError> {
    let frame = encode_cell_root(root)?;
    let name = cell_root_name(root.header.authority_epoch, root.header.snapshot_generation);
    write_root_file(directory, &name, &frame, hook)
}

/// Staging write, file sync, rename, directory sync. Never routed through the
/// YAML metadata codec. An error leaves the staging file for restart cleanup
/// (a failure is fail-stop; the previously adopted root is untouched).
fn write_root_file(
    directory: &Path,
    name: &str,
    frame: &[u8],
    mut hook: impl FnMut(RootPersistStep) -> std::io::Result<()>,
) -> Result<IndexImageMetadata, NativeIndexError> {
    let path = directory.join(name);
    // Generation names are immutable: never replace an existing root.
    match std::fs::symlink_metadata(&path) {
        Ok(_) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "cell root already exists",
            )
            .into());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let staging = directory.join(cell_root_staging_name(name));
    // One metadata actor owns the directory, so replacing a staging file an
    // interrupted attempt left is safe; nothing ever adopts a staging name.
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&staging)?;
    hook(RootPersistStep::Created)?;
    file.write_all(frame)?;
    hook(RootPersistStep::Written)?;
    file.sync_all()?;
    hook(RootPersistStep::Synced)?;
    drop(file);
    std::fs::rename(&staging, &path)?;
    hook(RootPersistStep::Renamed)?;
    // Name barrier before the manifest can reference the root.
    File::open(directory)?.sync_all()?;
    Ok(IndexImageMetadata {
        bytes: frame.len() as u64,
        digest: Blake2b::<U32>::digest(frame).into(),
    })
}

#[cfg(test)]
#[path = "cell_snapshot_tests.rs"]
mod tests;
