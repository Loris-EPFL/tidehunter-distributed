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
    /// Runtime birth of the captured materialization; installation checks it.
    pub birth: u64,
    pub base: Option<SortedBlock>,
    pub overlay: std::sync::Arc<Overlay<ValueRecordReference>>,
    pub masks: FrozenMasks,
    /// Adopted prune marks at capture; persistence replaces them with the
    /// next image's marks (adopted minus folded, plus new). Stored in the
    /// cell's frame.
    pub marks: Option<std::sync::Arc<[PruneMark]>>,
    /// True when persistence added marks to this cell, so its stored frame
    /// is stale and the cell is rewritten.
    pub marks_changed: bool,
    /// Policy progress and per-birth jitter of a callback-enabled cell;
    /// persistence replaces it with the next image's progress.
    pub policy: Option<(CellPolicy, u32)>,
    /// Sorted `(key, fold_through)`: versions this image omits with the
    /// marks it folds. Empty unless the floor reached a mark.
    pub fold: Vec<(Bytes, MutationVersion)>,
}

/// `DropCells` masks frozen at the capture boundary.
#[derive(Clone, Debug, Default)]
pub(super) struct FrozenMasks {
    /// Ascending masks at or below the capture cut.
    pub versions: Vec<MutationVersion>,
    /// The newest mask at or below the retention floor. Every reference at
    /// or below it is invisible to every valid cut, so the image omits them.
    pub absorbed: Option<MutationVersion>,
}

impl FrozenMasks {
    pub(super) fn capture(
        drop_versions: &[MutationVersion],
        cut: &IndexReadCut,
        floor: &IndexReadCut,
    ) -> Self {
        let versions: Vec<_> = drop_versions
            .iter()
            .copied()
            .filter(|version| cut.contains_mutation(*version))
            .collect();
        let absorbed = versions
            .iter()
            .rev()
            .copied()
            .find(|version| floor.contains_mutation(*version));
        Self { versions, absorbed }
    }

    /// Masks the root keeps for protected cuts between the floor and the cut.
    fn retained(&self) -> impl Iterator<Item = MutationVersion> + '_ {
        self.versions
            .iter()
            .copied()
            .filter(|version| self.absorbed.is_none_or(|absorbed| *version > absorbed))
    }
}

pub(crate) struct PreparedIndexImage {
    database: [u8; 16],
    schema: [u8; 32],
    cells: Vec<FrozenCell>,
    cut: IndexReadCut,
    lease: CaptureLease,
    /// Payload scratch limit of the one encoder that persists this image.
    scratch_limit: usize,
    /// Compactor inputs of this capture; `None` without callback keyspaces.
    policy: Option<PolicyCapture>,
    /// Root policy block of this image.
    policy_root: Option<PolicyRoot>,
    /// Install epoch of the marks this image adds.
    install_epoch: u64,
    /// Reservation of the resident charge of the marks this image adds.
    mark_budget: Box<dyn MarkBudget>,
    /// Removals planned on an earlier cut, checked on this image.
    prune_plan: PrunePlan,
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
    pub(super) policy_root: Option<PolicyRoot>,
    pub(super) install_epoch: u64,
    pub(crate) compactor_stats: CompactorStats,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "reported by R1 logical pruning")
    )]
    pub(crate) prune_stats: PruneStats,
    /// Holds the charge of the added marks until the caller ends it after
    /// live install, when the index itself charges them.
    pub(crate) mark_budget: Box<dyn MarkBudget>,
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
            policy: None,
            policy_root: None,
            install_epoch: 0,
            mark_budget: Box::new(UnboundedMarkBudget),
            prune_plan: PrunePlan::default(),
        }
    }

    /// Epoch that publishes the marks this image adds, after its last cell
    /// is installed.
    pub(super) fn with_install_epoch(mut self, install_epoch: u64) -> Self {
        self.install_epoch = install_epoch;
        self
    }

    /// Attach the compactor inputs frozen with the capture.
    pub(super) fn with_policy(
        mut self,
        policy: Option<PolicyCapture>,
        policy_root: Option<PolicyRoot>,
    ) -> Self {
        self.policy = policy;
        self.policy_root = policy_root;
        self
    }

    /// Reserve the resident charge of new marks through `budget` before they
    /// are created. Without it, reservations always succeed.
    pub(crate) fn with_mark_budget(mut self, budget: Box<dyn MarkBudget>) -> Self {
        self.mark_budget = budget;
        self
    }

    /// Check `plan` on this image: each candidate whose cell birth and exact
    /// head still match at the image's cut becomes a mark.
    pub(crate) fn with_prune_plan(mut self, plan: PrunePlan) -> Self {
        self.prune_plan = plan;
        self
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
        // Fold, apply the prune plan, then evaluate and select every cell once,
        // off-lock, before any encoding. The container decision below then
        // sees the final rows and marks.
        let mut prune_stats = PruneStats::default();
        let mut compactor_stats = CompactorStats::default();
        let candidates = std::mem::take(&mut self.prune_plan.candidates);
        let mut planned = candidates.as_slice();
        for cell in &mut self.cells {
            prune_marks::fold_cell(cell, &self.lease.floor, &mut prune_stats);
            // The plan and the cells share the `(keyspace, cell)` order; a
            // candidate of a cell this image does not hold is stale.
            let identity = (cell.identity.0.0, &cell.identity.1);
            let before = planned.partition_point(|c| (c.keyspace.0, &c.cell) < identity);
            prune_stats.plan_stale += before as u64;
            planned = &planned[before..];
            let run = planned.partition_point(|c| (c.keyspace.0, &c.cell) == identity);
            prune_marks::apply_plan(
                cell,
                &planned[..run],
                &self.cut,
                self.install_epoch,
                self.mark_budget.as_mut(),
                &mut prune_stats,
            )?;
            planned = &planned[run..];
            super::compactor::prepare_cell(
                cell,
                self.policy.as_ref(),
                self.install_epoch,
                &self.cut,
                self.scratch_limit,
                self.mark_budget.as_mut(),
                &mut compactor_stats,
            )?;
        }
        prune_stats.plan_stale += planned.len() as u64;
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
                    let payload = frame_payload(&frame)?;
                    // The block holds the rows only; the trailer stores the
                    // cell's marks, which are resident in `cell.marks`.
                    base.bytes = payload.slice(..SortedBlock::rows_len(&payload)?);
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
                    bytes: frame.slice(8..8 + encoded.rows),
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
        // Masks are kept only for cells that still store rows: an empty cell
        // has nothing a mask could hide, and the suffix replays above the cut.
        let masked = self
            .cells
            .iter()
            .filter(|cell| cell.base.is_some() && cell.masks.retained().next().is_some())
            .count();
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
        // The mask section is always present, so one writer format exists.
        encode_masks(&self.cells, masked, &mut payload)?;
        // A cell without stored rows keeps neither marks nor progress: nothing
        // at or below the cut remains for a mark to hide. Marks of stored
        // cells are in their frames.
        for cell in self.cells.iter_mut().filter(|cell| cell.base.is_none()) {
            cell.marks = None;
            if let Some((_, jitter)) = cell.policy {
                cell.policy = self
                    .policy_root
                    .as_ref()
                    .and_then(|root| root.baseline(cell.identity.0))
                    .map(|baseline| (baseline, jitter));
            }
        }
        // The policy block follows only when a callback keyspace is enabled:
        // a root without one ends at the mask section.
        super::compactor::encode_policy(self.policy_root.as_ref(), &self.cells, &mut payload)?;
        if payload.len() > MAX_IMAGE_BYTES - 8 {
            return Err(NativeIndexError::Capacity);
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
            policy_root: self.policy_root,
            install_epoch: self.install_epoch,
            compactor_stats,
            prune_stats,
            mark_budget: self.mark_budget,
        })
    }
}

fn reusable(cell: &FrozenCell, parent: &Path, cut: &IndexReadCut, floor: &IndexReadCut) -> bool {
    // Captures retain the complete sealed generation. A cell is still clean
    // for this prefix when all its overlay references are newer than the cut
    // and its marks are unchanged: no mark folds and none was added. A
    // keep-all evaluation changes only the root policy progress.
    cell.fold.is_empty()
        && !cell.marks_changed
        && cell.overlay.values().all(|values| {
            values
                .first()
                .is_none_or(|reference| !cut.contains(reference))
        })
        && cell.base.as_ref().is_some_and(|base| {
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

/// Mask section: `be_u32 masked_cell_count`, then per cell in directory
/// order `ks:u8, native CellId, be_u32 version_count` and ascending
/// `(be_u64 generation, be_u64 sequence, be_u32 ordinal = 0)` versions.
fn encode_masks(
    cells: &[FrozenCell],
    masked: usize,
    payload: &mut Vec<u8>,
) -> Result<(), NativeIndexError> {
    payload.extend_from_slice(
        &u32::try_from(masked)
            .map_err(|_| NativeIndexError::Capacity)?
            .to_be_bytes(),
    );
    let mut identity = BytesMut::new();
    for cell in cells.iter().filter(|cell| cell.base.is_some()) {
        let count = cell.masks.retained().count();
        if count == 0 {
            continue;
        }
        payload.push(cell.identity.0.0);
        identity.clear();
        cell.identity.1.write_into_bytes(&mut identity);
        payload.extend_from_slice(&identity);
        payload.extend_from_slice(
            &u32::try_from(count)
                .map_err(|_| NativeIndexError::Capacity)?
                .to_be_bytes(),
        );
        for version in cell.masks.retained() {
            payload.extend_from_slice(&version.batch.epoch.to_be_bytes());
            payload.extend_from_slice(&version.batch.sequence.to_be_bytes());
            payload.extend_from_slice(&version.operation_index.to_be_bytes());
        }
        if payload.len() > MAX_IMAGE_BYTES - 8 {
            return Err(NativeIndexError::Capacity);
        }
    }
    Ok(())
}

/// Bounded decoding of the mask section. Every masked cell has a stored
/// frame, appears once in ascending directory order, and has strictly
/// ascending ordinal-0 versions above the floor and within the cut. An empty
/// section (count zero) is the common case.
fn decode_masks(
    cursor: &mut Cursor<'_>,
    index: &mut NativeIndex,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    cells: usize,
) -> Result<(), NativeIndexError> {
    let masked = cursor.u32()? as usize;
    // Each entry needs at least a keyspace, a cell tag and one version.
    if masked > cells || masked > cursor.remaining() / 22 {
        return Err(NativeIndexError::Invalid("mask section count"));
    }
    let mut previous: Option<(u8, CellId)> = None;
    for _ in 0..masked {
        let keyspace = KeySpace(cursor.u8()?);
        let cell_id = decode_cell(cursor)?;
        let key = (keyspace.0, cell_id);
        let shard = index.shard_index(keyspace, &key.1);
        let identity = (keyspace, key.1.clone());
        if !index.shards[shard].get_mut().contains_key(&identity) {
            return Err(NativeIndexError::Invalid("mask names an absent cell"));
        }
        // The cell exists in `keyspace`, so `key` has its CellId variant and
        // the comparison with a same-keyspace predecessor cannot panic.
        if previous.as_ref().is_some_and(|prior| *prior >= key) {
            return Err(NativeIndexError::Invalid("unsorted mask cells"));
        }
        let count = cursor.u32()? as usize;
        if count == 0 || count > cursor.remaining() / 20 {
            return Err(NativeIndexError::Invalid("mask version count"));
        }
        let charge = count
            .checked_mul(MASK_CHARGE)
            .ok_or(NativeIndexError::Capacity)?;
        index.check_capacity(charge)?;
        super::shards::add(&index.accounting.directory, charge);
        let mut versions = Vec::with_capacity(count);
        for _ in 0..count {
            let version = MutationVersion {
                batch: BatchVersion {
                    epoch: cursor.u64()?,
                    sequence: cursor.u64()?,
                },
                operation_index: cursor.u32()?,
            };
            if version.operation_index != 0
                || !cut.contains_mutation(version)
                || floor.contains_mutation(version)
                || versions.last().is_some_and(|prior| *prior >= version)
            {
                return Err(NativeIndexError::Invalid("mask version domain or order"));
            }
            versions.push(version);
        }
        let cell = index.shards[shard]
            .get_mut()
            .get_mut(&identity)
            .ok_or(NativeIndexError::Invalid("mask names an absent cell"))?;
        cell.drop_versions = versions;
        previous = Some(key);
    }
    Ok(())
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
    if cursor.take(8)? != MAGIC {
        return Err(NativeIndexError::Invalid("index image format"));
    }
    let format = cursor.u16()?;
    if !matches!(format, FORMAT | PREVIOUS_FORMAT) || cursor.u16()? != 0 {
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
        let (mut block, trailer) = SortedBlock::validated(
            frame_payload(&frame)?,
            true,
            database,
            keyspace,
            index.descriptor(keyspace)?,
            &cell_id,
            cut.clone(),
        )?;
        // Only the current format stores prune marks after the rows.
        let prune_marks = if trailer.is_empty() {
            None
        } else if format == FORMAT {
            let (marks, charge) =
                prune_marks::decode_trailer(&trailer, index.descriptor(keyspace)?, &cell_id, &cut)?;
            index.check_capacity(charge)?;
            super::shards::add(&index.accounting.directory, charge);
            Some(std::sync::Arc::from(marks))
        } else {
            return Err(NativeIndexError::Invalid("trailing block bytes"));
        };
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
        let identity = (keyspace, cell_id);
        let birth = index.writer.get_mut().register_cell(&identity)?;
        let shard = index.shard_index(identity.0, &identity.1);
        if index.shards[shard]
            .get_mut()
            .insert(
                identity,
                CellIndex {
                    base: Some(block),
                    birth,
                    prune_marks,
                    ..Default::default()
                },
            )
            .is_some()
        {
            return Err(NativeIndexError::Invalid("duplicate cell"));
        }
    }
    let mut policy_root = None;
    if format == FORMAT {
        decode_masks(&mut cursor, &mut index, &cut, &floor, count)?;
        // Optional: present only when a callback keyspace was enabled.
        if cursor.remaining() != 0 {
            policy_root = Some(super::compactor::decode_policy(
                &mut cursor,
                &mut index,
                &cut,
            )?);
        }
    }
    if cursor.remaining() != 0 {
        return Err(NativeIndexError::Invalid("trailing payload"));
    }
    index.retention.set_floor(floor);
    index.recount_retained();
    // The root cut is published even when no stored reference has its
    // version (a `DropCells` batch absorbed into the image).
    let writer = index.writer.get_mut();
    writer.latest = writer.latest.max(retention::position(&cut));
    // Policy progress is restored, never re-evaluated: no callback runs at
    // open, validation or suffix replay.
    writer.policy_root = policy_root;
    writer.policy_cut = retention::position(&cut);
    let latest = writer.latest;
    index.published.store(std::sync::Arc::new(
        latest.map_or(IndexReadCut::BeforeAll, IndexReadCut::At),
    ));
    Ok(index)
}

pub(super) struct Rows<'a> {
    base: Option<&'a SortedBlock>,
    position: usize,
    overlay: std::iter::Peekable<
        std::collections::btree_map::Iter<'a, Bytes, NativeVersions<ValueRecordReference>>,
    >,
}

impl<'a> Rows<'a> {
    pub(super) fn new(cell: &'a FrozenCell) -> Self {
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
    cell: &FrozenCell,
    key: &[u8],
    base: &'a [u8],
    overlay: &'a [ValueRecordReference],
    cut: &IndexReadCut,
    floor: &IndexReadCut,
) -> Versions<'a> {
    let absorbed = cell.masks.absorbed;
    // A prune mark the floor reached folds with every version it covers.
    let fold = prune_marks::fold_through(&cell.fold, key);
    // Preserve one anchor at/before the floor, including a tombstone, and every
    // later version. Binary-search old histories instead of scanning discarded
    // versions once per serialization pass. A `DropCells` mask at or below the
    // floor hides every version at or below it from every valid cut: those
    // versions, the anchor included, are omitted (the mask is absorbed).
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
    let omitted = |version: MutationVersion| {
        anchor.is_some_and(|anchor| version < anchor)
            || absorbed.is_some_and(|absorbed| version <= absorbed)
            || fold.is_some_and(|fold| version <= fold)
    };
    let base_start = lower_bound_by(count, |n| omitted(reference(n).mutation_version()));
    let base_end = lower_bound_by(count, |n| cut.contains(&reference(n)));
    let overlay_start = overlay.partition_point(|value| omitted(value.mutation_version()));
    let overlay_end = overlay.partition_point(|value| cut.contains(value));
    // The absorbed mask and every fold bound are within the cut, so the starts
    // never pass the ends.
    Versions::new(
        &base[base_start.min(base_end) * REFERENCE_BYTES..base_end * REFERENCE_BYTES],
        &overlay[overlay_start.min(overlay_end)..overlay_end],
    )
}

fn has_rows(cell: &FrozenCell, cut: &IndexReadCut, floor: &IndexReadCut) -> bool {
    *cut != IndexReadCut::BeforeAll
        && Rows::new(cell).any(|(key, base, overlay)| {
            retained(cell, key, base, overlay, cut, floor)
                .next()
                .is_some()
        })
}

struct WrittenCell {
    file: CellFile,
    count: usize,
    /// Payload bytes of the rows; a mark trailer may follow them.
    rows: usize,
    retained_bytes: usize,
    retained_versions: usize,
}

#[derive(Debug, PartialEq)]
struct PayloadSummary {
    length: usize,
    /// Bytes of the rows; the mark trailer, if any, follows them.
    rows: usize,
    retained_bytes: usize,
    retained_versions: usize,
}

/// Emit canonical sorted payload bytes without storing offsets or cold versions
/// in a heap table. The same traversal supplies the CRC before immutable append.
fn encode_cell_payload(
    cell: &FrozenCell,
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
    for (key, base, overlay) in Rows::new(cell) {
        let n = retained(cell, key, base, overlay, cut, floor).count();
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
        let n = retained(cell, key, base, overlay, cut, floor).count();
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
        for reference in retained(cell, key, base, overlay, cut, floor) {
            reference_bytes.clear();
            encode_reference(&reference, reference_bytes);
            write(reference_bytes)?;
            retained_bytes += VERSION_CHARGE + key.len();
            retained_versions += 1;
        }
    }
    super::prune_marks::encode_trailer(cell.marks.as_ref(), &mut write)?;
    let trailer =
        super::prune_marks::trailer_len(cell.marks.as_ref()).ok_or(NativeIndexError::Capacity)?;
    Ok(PayloadSummary {
        length,
        rows: length - trailer,
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
/// other cell streams, so the scratch never grows above `limit`.
struct CellEncoder {
    payload: Vec<u8>,
    reference: Vec<u8>,
    limit: usize,
    stats: CellEncodeStats,
}

impl CellEncoder {
    fn new(limit: usize) -> Self {
        Self {
            payload: Vec::new(),
            reference: Vec::with_capacity(REFERENCE_BYTES),
            limit: limit.min(CELL_SCRATCH_CEILING),
            stats: CellEncodeStats::default(),
        }
    }

    /// Append the canonical frame of `cell` at `offset`. Returns `None`, and
    /// writes nothing, when the cell retains no row. The path is chosen before
    /// encoding: a buffered encode never overflows into a second encoding.
    fn append<W: Write>(
        &mut self,
        cell: &FrozenCell,
        writer: Option<&mut W>,
        path: &Path,
        offset: u64,
        cut: &IndexReadCut,
        floor: &IndexReadCut,
    ) -> Result<Option<WrittenCell>, NativeIndexError> {
        self.stats.row_passes += 1;
        let (count, bytes, digest, summary) =
            if let Some((rows, upper)) = buffered_bound(cell, self.limit) {
                let Some((count, summary)) = self.encode_buffered(cell, rows, upper, cut, floor)?
                else {
                    self.stats.empty_cells += 1;
                    return Ok(None);
                };
                let writer = writer.ok_or(NativeIndexError::Invalid("missing index container"))?;
                let (bytes, digest) = write_frame(&self.payload, offset, writer)?;
                self.stats.buffered_cells += 1;
                self.stats.encode_passes += 1;
                self.stats.buffered_bytes += bytes;
                self.stats.references_encoded += summary.retained_versions as u64;
                (count, bytes, digest, summary)
            } else {
                let count = Rows::new(cell)
                    .filter(|(key, base, overlay)| {
                        retained(cell, key, base, overlay, cut, floor)
                            .next()
                            .is_some()
                    })
                    .count();
                if count == 0 {
                    self.stats.empty_cells += 1;
                    return Ok(None);
                }
                let writer = writer.ok_or(NativeIndexError::Invalid("missing index container"))?;
                let (bytes, digest, summary) =
                    stream_cell(cell, count, cut, floor, offset, writer, &mut self.reference)?;
                self.stats.streamed_cells += 1;
                self.stats.encode_passes += 2;
                self.stats.row_passes += 4;
                self.stats.streamed_bytes += bytes;
                self.stats.references_encoded += 2 * summary.retained_versions as u64;
                if summary.length <= self.limit {
                    self.stats.conservative_fallbacks += 1;
                }
                (count, bytes, digest, summary)
            };
        Ok(Some(WrittenCell {
            file: CellFile {
                path: path.to_owned(),
                offset,
                bytes,
                digest,
                floor: floor.clone(),
            },
            count,
            rows: summary.rows,
            retained_bytes: summary.retained_bytes,
            retained_versions: summary.retained_versions,
        }))
    }

    /// One merged-row traversal that encodes each retained reference once.
    /// The payload is `[count][offsets][rows]`, and the offsets precede the
    /// rows, so the directory is reserved for every candidate row, filled per
    /// emitted row and compacted by at most one move of the row bodies.
    /// Returns the emitted row count, or `None` when no row is retained.
    fn encode_buffered(
        &mut self,
        cell: &FrozenCell,
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
        for (key, base, overlay) in Rows::new(cell) {
            let mut versions = retained(cell, key, base, overlay, cut, floor);
            let Some(first) = versions.next() else {
                continue;
            };
            let start = u32::try_from(payload.len()).map_err(|_| NativeIndexError::Capacity)?;
            payload[4 + 4 * rows..8 + 4 * rows].copy_from_slice(&start.to_be_bytes());
            payload.extend_from_slice(
                &u32::try_from(key.len())
                    .map_err(|_| NativeIndexError::Capacity)?
                    .to_be_bytes(),
            );
            payload.extend_from_slice(key);
            let count_at = payload.len();
            payload.extend_from_slice(&[0; 4]);
            let mut n = 0usize;
            for reference in std::iter::once(first).chain(versions) {
                encode_reference(&reference, payload);
                n += 1;
            }
            payload[count_at..count_at + 4].copy_from_slice(
                &u32::try_from(n)
                    .map_err(|_| NativeIndexError::Capacity)?
                    .to_be_bytes(),
            );
            retained_bytes += n * (VERSION_CHARGE + key.len());
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
        let row_bytes = payload.len();
        super::prune_marks::encode_trailer(cell.marks.as_ref(), &mut |bytes| {
            payload.extend_from_slice(bytes);
            Ok(())
        })?;
        debug_assert!(payload.len() <= upper, "cell payload above its bound");
        Ok(Some((
            rows,
            PayloadSummary {
                length: payload.len(),
                rows: row_bytes,
                retained_bytes,
                retained_versions,
            },
        )))
    }
}

/// Conservative payload bound of `cell` as `(candidate rows, bound)`, or
/// `None` when the bound exceeds `limit` or overflows. Every base and overlay
/// row counts its directory slot, row prefix, key and all its versions. Equal
/// keys, equal versions and discarded versions only reduce the output, so the
/// bound covers the reserved directory plus row bodies and the final payload.
/// The overlay walk stops at the limit and decodes no reference.
fn buffered_bound(cell: &FrozenCell, limit: usize) -> Option<(usize, usize)> {
    // A base payload is `[count][offsets][rows]` with one offset per row,
    // then the exact trailer of the marks this image stores.
    let mut upper = cell
        .base
        .as_ref()
        .map_or(4, |base| base.bytes.len())
        .checked_add(super::prune_marks::trailer_len(cell.marks.as_ref())?)?;
    if upper > limit {
        return None;
    }
    let candidates = cell
        .base
        .as_ref()
        .map_or(0, |base| base.count)
        .checked_add(cell.overlay.len())?;
    for (key, versions) in cell.overlay.iter() {
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
fn stream_cell(
    cell: &FrozenCell,
    count: usize,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    offset: u64,
    writer: &mut impl Write,
    reference_bytes: &mut Vec<u8>,
) -> Result<(u64, [u8; 32], PayloadSummary), NativeIndexError> {
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
    Ok((bytes, digest, summary))
}

#[cfg(test)]
#[path = "cell_encode_tests.rs"]
mod cell_encode_tests;
