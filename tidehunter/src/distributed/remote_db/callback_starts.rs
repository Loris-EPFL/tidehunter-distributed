//! Durable relocation callback starts.
//!
//! A callback start is the file-aligned offset below which a relocation pass has
//! already copied or dropped every live entry of one `(owner, fencing
//! generation)` stream, which is the native value watermark: files below it
//! may be retired, and the next pass resumes there. It only rises.
//!
//! The authority manifest is the single durable home. A pass adopts its starts
//! only after the snapshot that persisted its re-points is usable
//! (`RelocationReport` gate); a crash before this write repeats the pass and
//! never retires a file the start does not cover.

use super::relocation::merge_callback_starts;
use super::write_core::core::WriteCore;
use super::{CallbackStart, CoordinatorIoCategory, LocalDbError, persist_manifest};
use std::collections::BTreeMap;

impl WriteCore {
    /// Raises the adopted starts by `proposed` and persists them.
    ///
    /// The merge is validated against the retained streams and file alignment,
    /// the manifest is replaced once, and only then does the in-memory table
    /// change, so memory never runs ahead of the disk. The manifest writers
    /// that run while the database serves (this and retirement) are serialised by
    /// `retirement`; `fencing_generations::{commit_trim, finish_trim}` also write
    /// the manifest without it, but only in offline maintenance at open, before
    /// any relocation pass exists. The publisher lock is never held across the
    /// write.
    ///
    /// # Errors
    /// A proposal naming no retained stream or an unaligned offset, or the
    /// manifest write failing; nothing changes in either case.
    pub(in crate::distributed::remote_db) fn adopt_callback_starts(
        &self,
        proposed: &BTreeMap<(u32, u64), u64>,
    ) -> Result<(), LocalDbError> {
        let _writer = self.retirement.lock();
        let (manifest, merged) = {
            let publisher = self.publisher.lock();
            let merged = merge_callback_starts(
                &publisher.callback_starts,
                proposed,
                &publisher.retained_epochs,
                &self.partition_indexes,
                self.config.storage.wal_file_bytes,
            )?;
            if merged == publisher.callback_starts {
                return Ok(());
            }
            let mut manifest = publisher.manifest.clone();
            manifest.callback_starts = to_manifest(&merged);
            (manifest, merged)
        };
        persist_manifest(&self.path, &manifest, CoordinatorIoCategory::ManifestAdopt)?;
        let mut publisher = self.publisher.lock();
        publisher.manifest.callback_starts = manifest.callback_starts;
        publisher.callback_starts = merged;
        Ok(())
    }
}

fn to_manifest(starts: &BTreeMap<(u32, u64), u64>) -> Vec<CallbackStart> {
    starts
        .iter()
        .map(|(&(partition, fencing_generation), &from)| CallbackStart {
            partition,
            fencing_generation,
            from,
        })
        .collect()
}

/// The adopted starts the manifest names, validated: strictly ascending by
/// `(partition, generation)` and file-aligned. A start of a stream that is no
/// longer retained (trimmed after its last adoption) carries no meaning and is
/// dropped; the trim removed the stream itself.
///
/// `retained` lists, per owner in `partitions` order, the generations the open
/// found on that owner, and `epoch` is the generation this open serves.
///
/// # Errors
/// A malformed list: unsorted, duplicated or unaligned. The manifest is
/// authority state, so it is refused rather than repaired.
pub(super) fn load(
    entries: &[CallbackStart],
    wal_file_bytes: u64,
    partitions: &[u32],
    retained: &[Vec<u64>],
    epoch: u64,
) -> Result<BTreeMap<(u32, u64), u64>, LocalDbError> {
    let mut starts = BTreeMap::new();
    let mut previous: Option<(u32, u64)> = None;
    for entry in entries {
        let key = (entry.partition, entry.fencing_generation);
        if previous.is_some_and(|previous| previous >= key) || entry.from % wal_file_bytes != 0 {
            return Err(LocalDbError::new(
                "authority manifest relocation callback starts are not sorted and file-aligned",
            ));
        }
        previous = Some(key);
        let kept = partitions
            .iter()
            .position(|partition| *partition == entry.partition)
            .and_then(|node| retained.get(node))
            .is_some_and(|generations| {
                generations.contains(&entry.fencing_generation) || entry.fencing_generation == epoch
            });
        if kept {
            starts.insert(key, entry.from);
        }
    }
    Ok(starts)
}

#[cfg(test)]
mod tests;
