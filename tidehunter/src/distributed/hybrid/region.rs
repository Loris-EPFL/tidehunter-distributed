//! Control-region V4 and the byte-cadence arithmetic of the hybrid index.
//!
//! Native Tidehunter stores a [`ControlRegion`] (sentinel, version, bincode)
//! whose cut is a local WAL offset. The hybrid control region stores the same
//! native snapshot container with a [`BatchVersion`] cut, plus two things the
//! multi-owner value WAL needs and native never did (plan invariants I3/I4 and
//! I2b):
//!
//! * `streams` — the per-owner replay table
//!   `[(partition, generation, replay_from)]`. A single cut cannot address N
//!   independent offset spaces, so recovery replays each stream from its own
//!   offset and merges by version.
//! * `cell_bytes` — each cell's `flushed_at`, a value-byte position. Native's
//!   cadence thresholds are byte distances (`snapshot_written_bytes`,
//!   `snapshot_unload_threshold`); a version cut cannot be compared to a byte
//!   distance, so the byte position is a separate scalar. It is advisory for
//!   pacing only and never decides what the index may flush or what is
//!   durable.
//!
//! The file uses the native layout — 8-byte sentinel `u64::MAX`, 4-byte
//! little-endian version, then the bincode payload — with version 4 from
//! [`IndexCut::CONTROL_REGION_VERSION`]. The native V3 reader refuses it and
//! this reader refuses V1/V2/V3 (plan invariant I9): a hybrid directory is
//! never silently reinterpreted as a native one, whose 12-byte positions would
//! be read as 48-byte positions.

use crate::cell::CellId;
use crate::container::LargeTableContainer;
use crate::control::ControlRegion;
use crate::db::CONTROL_REGION_FILE;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShape, KeyType};
use crate::large_table::SnapshotEntryData;
use crate::wal::position::IndexCut;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Size of the native sentinel + version header.
const HEADER_LEN: usize = 12;
/// Native sentinel ("this is a versioned control region, not V1 bincode").
const SENTINEL: u64 = u64::MAX;

/// Upper bound on a V4 control-region file. A native snapshot of Sui's
/// keyspaces is kilobytes; the bound only stops a corrupt length from driving a
/// large allocation before deserialization fails.
const MAX_CONTROL_REGION_BYTES: u64 = 64 * 1024 * 1024;

/// Upper bound on the owner streams one control region (and the coordinator's
/// position and callback-start tables) may name.
///
/// A stream is one `(owner partition, fencing generation)` pair the database
/// still references. There is no static cap on the owner count and the retained
/// generations are bounded only by what is still referenced, so this fixed
/// number is the failure boundary, not a derived quota: 4,096 is, for example,
/// 16 owners with 256 retained generations each (each retained generation
/// holds at least one unretired file on its owner). Past it an open refuses the
/// database, a store refuses the region and a publication fails closed; nothing
/// is ever truncated, because dropping a stream would drop its replay start.
pub(crate) const MAX_REGION_STREAMS: usize = 4096;

/// One owner stream's replay start, persisted with the snapshot.
///
/// `replay_from` is inclusive: replay batches with version `>= replay_from`'s
/// owner offset. It is the lowest offset of a published batch above the flush
/// target, or the stream tail when there is none (I4).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StreamFront {
    pub(crate) partition: u32,
    pub(crate) generation: u64,
    pub(crate) replay_from: u64,
}

/// Native control-region payload plus the hybrid-only fields.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HybridControlRegion {
    pub(crate) region: ControlRegion<BatchVersion>,
    pub(crate) streams: Vec<StreamFront>,
    pub(crate) cell_bytes: BTreeMap<(u8, CellId), u64>,
}

impl HybridControlRegion {
    pub(crate) fn new(
        snapshot: LargeTableContainer<SnapshotEntryData<BatchVersion>>,
        cut: BatchVersion,
        key_shape: &KeyShape,
        streams: Vec<StreamFront>,
        cell_bytes: BTreeMap<(u8, CellId), u64>,
    ) -> Self {
        Self {
            region: ControlRegion::new(snapshot, cut, key_shape),
            streams,
            cell_bytes,
        }
    }

    /// The cut this region represents (exclusive: every batch below it is in
    /// the stored index).
    pub(crate) fn last_position(&self) -> BatchVersion {
        self.region.last_position()
    }

    /// Verifies the stored snapshot against the canonical shape with the
    /// native verifier (same rules as a native V3 open; a mismatch is a
    /// configuration/disk disagreement and fails loudly, as native does).
    pub(crate) fn verify_shape(&self, key_shape: &KeyShape) {
        self.region.verify_shape(key_shape);
    }

    /// Open preparation: the snapshot extension for newly declared keyspaces,
    /// then the native shape check.
    ///
    /// Both steps are the native control-region methods (Amendment 20); a
    /// removed keyspace still fails the shape check, and a newly declared one
    /// gets an empty cell map so the table's `SnapshotEntryData` list matches
    /// the shape. The cut and the stored snapshot content are unchanged.
    pub(crate) fn prepare_for_open(&mut self, key_shape: &KeyShape) {
        self.region.extend_snapshot_if_needed(key_shape);
        self.region.verify_shape(key_shape);
    }

    /// Bound check against the fixed stream bound and the shape's cell count.
    ///
    /// Called after a read, before the region is trusted: a corrupt or foreign
    /// file cannot name more streams or cells than the bound allows. Partition
    /// membership is a separate check (`HybridTable::validate_partitions`),
    /// because partition ids are arbitrary owner ids, not indexes.
    pub(crate) fn validate_bounds(
        &self,
        max_streams: usize,
        max_cells: usize,
    ) -> Result<(), LocalDbError> {
        if self.streams.len() > max_streams {
            return Err(LocalDbError::new(format!(
                "control region names {} owner streams; the bound is {max_streams}",
                self.streams.len()
            )));
        }
        if self.cell_bytes.len() > max_cells {
            return Err(LocalDbError::new(format!(
                "control region names {} cells; the shape has {max_cells}",
                self.cell_bytes.len()
            )));
        }
        Ok(())
    }
}

/// The most cells `shape` can name in a control region: the exact count of
/// every uniform keyspace. A prefix-addressed keyspace has a data-dependent
/// cell set, bounded only by the region file limit, so it adds no static bound.
pub(crate) fn max_region_cells(shape: &KeyShape) -> usize {
    shape
        .iter_ks()
        .fold(0usize, |total, ks| match ks.key_type() {
            KeyType::Uniform(config) => total.saturating_add(config.num_cells(ks)),
            KeyType::PrefixedUniform(_) => usize::MAX,
        })
}

/// The V4 control-region file of one hybrid database.
pub(crate) struct HybridRegionStore {
    path: PathBuf,
    last_position: BatchVersion,
    streams: Vec<StreamFront>,
    /// Test hook run after the rename and before the directory sync, the same
    /// crash boundary the native store exposes.
    #[cfg(test)]
    after_rename_hook: Option<Box<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for HybridRegionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HybridRegionStore")
            .field("path", &self.path)
            .field("last_position", &self.last_position)
            .field("streams", &self.streams)
            .finish()
    }
}

impl HybridRegionStore {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            last_position: <BatchVersion as IndexCut>::NONE,
            streams: Vec::new(),
            #[cfg(test)]
            after_rename_hook: None,
        }
    }

    /// The control-region path inside a coordinator database directory.
    pub(crate) fn at(dir: &Path) -> Self {
        Self::new(dir.join(CONTROL_REGION_FILE))
    }

    /// Reads an existing region and prepares it for open: the native shape
    /// check plus the snapshot extension for newly declared keyspaces
    /// (Amendment 20). A fresh directory returns `None` without a check.
    ///
    /// The caller **MUST** also call [`HybridControlRegion::validate_bounds`]
    /// with the configured stream count and the shape's cell count before
    /// trusting the replay table or `cell_bytes`.
    pub(crate) fn open(
        path: PathBuf,
        key_shape: &KeyShape,
    ) -> Result<(Self, Option<HybridControlRegion>), LocalDbError> {
        let mut store = Self::new(path.clone());
        let region = match Self::read(&path)? {
            Some(mut region) => {
                region.prepare_for_open(key_shape);
                store.last_position = region.last_position();
                store.streams = region.streams.clone();
                Some(region)
            }
            None => None,
        };
        Ok((store, region))
    }

    /// Low-level read and header/format validation.
    ///
    /// The caller **MUST** call [`HybridControlRegion::prepare_for_open`]
    /// (native `verify_shape` plus the snapshot extension for newly declared
    /// keyspaces) before trusting the snapshot; [`Self::open`] does it.
    pub(crate) fn read(path: &Path) -> Result<Option<HybridControlRegion>, LocalDbError> {
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(LocalDbError::new(format!(
                    "failed to open control region {}: {error}",
                    path.display()
                )));
            }
        };
        let len = file
            .metadata()
            .map_err(|error| {
                LocalDbError::new(format!(
                    "failed to stat control region {}: {error}",
                    path.display()
                ))
            })?
            .len();
        if len > MAX_CONTROL_REGION_BYTES {
            return Err(LocalDbError::new(format!(
                "control region {} is {len} bytes, above the {MAX_CONTROL_REGION_BYTES}-byte limit",
                path.display()
            )));
        }
        let capacity = usize::try_from(len).map_err(|_| {
            LocalDbError::new(format!(
                "control region {} length {len} does not fit in memory",
                path.display()
            ))
        })?;
        let mut bytes = Vec::with_capacity(capacity);
        file.read_to_end(&mut bytes).map_err(|error| {
            LocalDbError::new(format!(
                "failed to read control region {}: {error}",
                path.display()
            ))
        })?;
        if bytes.len() < HEADER_LEN {
            return Err(LocalDbError::new(format!(
                "control region {} is {} bytes, shorter than the {HEADER_LEN}-byte header",
                path.display(),
                bytes.len(),
            )));
        }
        let sentinel = read_u64(&bytes, 0);
        if sentinel != SENTINEL {
            return Err(LocalDbError::new(format!(
                "control region {} is not a versioned Tidehunter control region; \
                 refusing legacy or foreign data (expected sentinel {SENTINEL:#x})",
                path.display(),
            )));
        }
        let found = read_u32(&bytes, 8);
        let expected = <BatchVersion as IndexCut>::CONTROL_REGION_VERSION;
        if found != expected {
            return Err(LocalDbError::new(format!(
                "control region {} has version {found}; the hybrid index requires \
                 version {expected} (a native version-3 or Exact directory is refused)",
                path.display(),
            )));
        }
        bincode::deserialize(&bytes[HEADER_LEN..])
            .map(Some)
            .map_err(|error| {
                LocalDbError::new(format!(
                    "control region {} is corrupt: {error}",
                    path.display()
                ))
            })
    }

    /// Durably replaces the control region. When this returns, a crash reopens
    /// this region, so the caller may then delete index files only the
    /// previous region referenced.
    pub(crate) fn store(&mut self, region: HybridControlRegion) -> Result<(), LocalDbError> {
        // Fail closed before anything is written: never truncate a stream, since
        // dropping one would drop its replay start.
        if region.streams.len() > MAX_REGION_STREAMS {
            return Err(LocalDbError::new(format!(
                "control region would name {} owner streams; the bound is {MAX_REGION_STREAMS}",
                region.streams.len()
            )));
        }
        let new_position = region.last_position();
        if new_position < self.last_position {
            return Err(LocalDbError::new(format!(
                "control region last_position regressed: new={new_position:?} < old={:?}",
                self.last_position
            )));
        }
        let streams = region.streams.clone();
        let mut serialized = Vec::with_capacity(HEADER_LEN + 64 * 1024);
        serialized.extend_from_slice(&SENTINEL.to_le_bytes());
        serialized
            .extend_from_slice(&<BatchVersion as IndexCut>::CONTROL_REGION_VERSION.to_le_bytes());
        bincode::serialize_into(&mut serialized, &region).map_err(|error| {
            LocalDbError::new(format!(
                "failed to serialize control region {}: {error}",
                self.path.display()
            ))
        })?;

        let temp = self.path.with_extension("v4.tmp");
        let write = || -> std::io::Result<()> {
            let mut file = fs::File::create(&temp)?;
            file.write_all(&serialized)?;
            file.sync_all()
        };
        write().map_err(|error| {
            LocalDbError::new(format!(
                "failed to write control region {}: {error}",
                temp.display()
            ))
        })?;
        fs::rename(&temp, &self.path).map_err(|error| {
            LocalDbError::new(format!(
                "failed to rename control region {} to {}: {error}",
                temp.display(),
                self.path.display()
            ))
        })?;
        #[cfg(test)]
        if let Some(hook) = &self.after_rename_hook {
            hook();
        }
        // Make the new name durable. Until then a crash can reopen the
        // previous control region, which can reference files the caller
        // deletes after this call.
        let directory = self.path.parent().ok_or_else(|| {
            LocalDbError::new(format!(
                "control region file {} has no parent directory",
                self.path.display()
            ))
        })?;
        fs::File::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                LocalDbError::new(format!(
                    "failed to sync control region directory {}: {error}",
                    directory.display()
                ))
            })?;
        self.last_position = new_position;
        self.streams = streams;
        Ok(())
    }

    pub(crate) fn last_position(&self) -> BatchVersion {
        self.last_position
    }

    pub(crate) fn stream_fronts(&self) -> &[StreamFront] {
        &self.streams
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(test)]
    pub(crate) fn set_after_rename_hook(&mut self, hook: Box<dyn Fn() + Send + Sync>) {
        self.after_rename_hook = Some(hook);
    }
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(raw)
}

fn read_u32(bytes: &[u8], at: usize) -> u32 {
    let mut raw = [0u8; 4];
    raw.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(raw)
}

/// `Σ` of the per-stream offsets persisted with the last snapshot.
///
/// This is the stored term of the byte-cadence root trigger. The persisted
/// offset is `replay_from`, which is at or below the snapshot tail (I4), so
/// `current - stored` can only overestimate the bytes since the snapshot and
/// can only make a snapshot fire earlier, never later.
pub(crate) fn stored_snapshot_offsets(streams: &[StreamFront]) -> u64 {
    streams
        .iter()
        .fold(0u64, |sum, stream| sum.saturating_add(stream.replay_from))
}

/// Native root trigger: `written > snapshot_written_bytes` (strictly greater),
/// expressed over the value-byte counter and the stored offsets.
pub(crate) fn snapshot_due(current: u64, stored: u64, snapshot_written_bytes: u64) -> bool {
    current.saturating_sub(stored) > snapshot_written_bytes
}

/// Native staleness threshold: `current - snapshot_unload_threshold`.
pub(crate) fn stale_threshold(current: u64, snapshot_unload_threshold: u64) -> u64 {
    current.saturating_sub(snapshot_unload_threshold)
}

/// Native unload predicate: a cell flushes when
/// `flushed_at <= current - snapshot_unload_threshold` (inclusive).
pub(crate) fn is_stale(flushed_at: u64, threshold: u64) -> bool {
    flushed_at <= threshold
}

/// Highest per-cell byte position, used to seed `current` on open so a cell
/// whose `flushed_at` is above the replayed tails can never wrap.
pub(crate) fn max_flushed_at(cell_bytes: &BTreeMap<(u8, CellId), u64>) -> u64 {
    cell_bytes.values().copied().max().unwrap_or(0)
}

/// Diagnostic for an owner byte counter that moved backwards.
///
/// A backwards per-stream tail means the owner restarted in a new generation
/// or lost data. The cadence must not wrap; the caller keeps the previous
/// counter and reports this error. The counter is advisory, so the message is
/// the observable contract, not a retry.
pub(crate) fn owner_counter_backwards(partition: u32, flushed_at: u64, sum: u64) -> LocalDbError {
    LocalDbError::with_kind(
        std::io::ErrorKind::InvalidData,
        format!(
            "owner byte counter went backwards: stream {partition}, \
             flushed_at {flushed_at}, sum {sum}"
        ),
    )
}

#[cfg(test)]
mod tests;
