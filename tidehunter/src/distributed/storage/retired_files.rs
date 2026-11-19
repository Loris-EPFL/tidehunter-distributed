//! Compressed retirement history and per-file seals of one owner WAL stream.
//!
//! `WalRoot` format 2 stores retired files as one retired prefix plus sorted,
//! disjoint, nonadjacent half-open ranges, so its size grows with the live
//! holes between retired files, not with lifetime file turnover. A `FileSeal`
//! proves that one passed file is whole and lease-closed; it never proves that
//! the file is unreachable. A `PendingRetirement` names the exact inode whose
//! durable retirement still owes its unlink, directory sync or target
//! tombstone cleanup.
use super::StorageError;
use serde::{Deserialize, Serialize};

/// Bound of `WalRoot.cleanup_pending`. A batch that would exceed it is not
/// authorized; the caller retries after the pending entries finish.
pub(crate) const MAX_PENDING_RETIREMENTS: usize = 256;

/// Bound of both `WalRoot.sealed_files` and retired ranges. Each entry encodes
/// to at most 49 bytes, so a full root stays below 1 MiB. A stream that holds
/// this many sealed live files stops sealing (no new retirement candidates)
/// until retirement removes some; writes are never refused.
pub(crate) const MAX_ROOT_ENTRIES: usize = 16 * 1024;

/// Half-open retired byte interval `[start, end)`, aligned to whole files.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RetiredRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

/// Exactly `[0, prefix_end)` plus `ranges`. Canonical form: every bound is
/// file aligned, every range starts strictly above `prefix_end`, ranges are
/// sorted and neither overlap nor touch. A range that would touch the prefix
/// or a neighbour is merged into it, so contiguous turnover keeps one scalar.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RetiredFiles {
    prefix_end: u64,
    ranges: Vec<RetiredRange>,
}

impl RetiredFiles {
    /// Whether the file starting at `file_start` was durably retired.
    pub(crate) fn contains(&self, file_start: u64) -> bool {
        self.skip_end(file_start).is_some()
    }

    /// End of the retired interval that contains `offset`, if any. Scans and
    /// replay jump to it instead of visiting each retired file.
    pub(crate) fn skip_end(&self, offset: u64) -> Option<u64> {
        if offset < self.prefix_end {
            return Some(self.prefix_end);
        }
        let index = self.ranges.partition_point(|range| range.end <= offset);
        self.ranges
            .get(index)
            .filter(|range| range.start <= offset)
            .map(|range| range.end)
    }

    /// Highest retired end; zero when nothing is retired.
    pub(crate) fn end(&self) -> u64 {
        self.ranges
            .last()
            .map_or(self.prefix_end, |range| range.end)
    }

    /// Number of stored intervals; the prefix counts as one when nonempty.
    #[cfg(test)]
    pub(crate) fn interval_count(&self) -> usize {
        self.ranges.len() + usize::from(self.prefix_end != 0)
    }

    /// Reject a decoded value that is not in canonical form.
    pub(crate) fn validate(&self, file_bytes: u64) -> Result<(), StorageError> {
        let invalid = || StorageError::Invalid("noncanonical retired WAL ranges");
        if file_bytes == 0
            || !self.prefix_end.is_multiple_of(file_bytes)
            || self.ranges.len() > MAX_ROOT_ENTRIES
        {
            return Err(invalid());
        }
        let mut previous_end = self.prefix_end;
        for range in &self.ranges {
            if range.start <= previous_end
                || range.end <= range.start
                || !range.start.is_multiple_of(file_bytes)
                || !range.end.is_multiple_of(file_bytes)
            {
                return Err(invalid());
            }
            previous_end = range.end;
        }
        Ok(())
    }

    /// Add whole files, each newly authorized by one durable decision.
    /// `starts` must be sorted, unique, aligned and not yet retired. Merges
    /// only intervals that are both retired, so a live hole always survives.
    pub(crate) fn insert_authorized_batch(
        &mut self,
        starts: &[u64],
        file_bytes: u64,
    ) -> Result<(), StorageError> {
        if starts.windows(2).any(|pair| pair[0] >= pair[1])
            || starts
                .iter()
                .any(|start| !start.is_multiple_of(file_bytes) || self.contains(*start))
        {
            return Err(StorageError::Invalid(
                "retirement batch is unsorted, misaligned or already retired",
            ));
        }
        let mut merged = Vec::with_capacity(self.ranges.len() + starts.len());
        let mut added = starts.iter().map(|&start| {
            start
                .checked_add(file_bytes)
                .map(|end| RetiredRange { start, end })
                .ok_or(StorageError::Capacity)
        });
        let mut existing = self.ranges.iter().copied().peekable();
        let mut next_added = added.next().transpose()?;
        loop {
            let next = match (existing.peek().copied(), next_added) {
                (Some(old), Some(new)) if old.start < new.start => existing.next(),
                (Some(_), Some(new)) => {
                    next_added = added.next().transpose()?;
                    Some(new)
                }
                (Some(_), None) => existing.next(),
                (None, Some(new)) => {
                    next_added = added.next().transpose()?;
                    Some(new)
                }
                (None, None) => None,
            };
            let Some(range) = next else { break };
            match merged.last_mut() {
                Some(last) if last_touches(last, &range) => last.end = range.end,
                _ => merged.push(range),
            }
        }
        let absorbed = merged
            .iter()
            .take_while(|range| range.start == self.prefix_end)
            .count();
        if let Some(last) = merged[..absorbed].last() {
            self.prefix_end = last.end;
        }
        merged.drain(..absorbed);
        if merged.len() > MAX_ROOT_ENTRIES {
            return Err(StorageError::Capacity);
        }
        self.ranges = merged;
        Ok(())
    }
}

/// Ranges arrive sorted and disjoint, so only an exact touch can merge.
fn last_touches(last: &RetiredRange, next: &RetiredRange) -> bool {
    last.end == next.start
}

/// Durable proof that the whole passed file `[start, end)` is lease-closed:
/// the allocator passed it, no extent or reservation owns it, every byte is a
/// complete validated group or a validated skip marker plus zeros, and the
/// position root covers `end`. The writable target namespace stays installed
/// for live reads; the owner permanently refuses any new grant inside it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileSeal {
    pub start: u64,
    pub end: u64,
    pub device: u64,
    pub inode: u64,
    /// Root generation whose install made this seal durable.
    pub root_generation: u64,
    /// Largest batch sequence in the file; `None` for a skip-only file.
    pub last_batch_sequence: Option<u64>,
}

/// One durably authorized file whose cleanup is not finished. Recovery and
/// the next retirement repeat unlink, directory sync and tombstone cleanup
/// from this exact identity; a different inode at `start` is an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PendingRetirement {
    pub start: u64,
    pub device: u64,
    pub inode: u64,
    /// The seal that authorized an active-stream file; `None` for a file of
    /// an older sealed stream.
    pub seal_generation: Option<u64>,
}

/// Check the seal and pending sets of a decoded root against its other fields.
pub(super) fn validate_root_sets(
    retired: &RetiredFiles,
    sealed: &[FileSeal],
    pending: &[PendingRetirement],
    synced_end: u64,
    generation: u64,
    file_bytes: u64,
) -> Result<(), StorageError> {
    retired.validate(file_bytes)?;
    let invalid = || StorageError::Invalid("WAL root seal or cleanup set is inconsistent");
    if sealed.len() > MAX_ROOT_ENTRIES
        || pending.len() > MAX_PENDING_RETIREMENTS
        || sealed.windows(2).any(|pair| pair[0].start >= pair[1].start)
        || pending
            .windows(2)
            .any(|pair| pair[0].start >= pair[1].start)
    {
        return Err(invalid());
    }
    for seal in sealed {
        if !seal.start.is_multiple_of(file_bytes)
            || seal.start.checked_add(file_bytes) != Some(seal.end)
            || seal.end > synced_end
            || seal.root_generation == 0
            || seal.root_generation > generation
            || retired.contains(seal.start)
        {
            return Err(invalid());
        }
    }
    if pending
        .iter()
        .any(|entry| !entry.start.is_multiple_of(file_bytes) || !retired.contains(entry.start))
    {
        return Err(invalid());
    }
    Ok(())
}
