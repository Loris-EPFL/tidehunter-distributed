//! Shadow eligibility of one WAL file (design §2 blockers, §4 conjunction).
//!
//! Pure functions over facts collected by the observer, so the decision table
//! is tested without a cluster. Every missing proof is a blocker: an unknown
//! fact never yields an empty set.

use super::{Blocker, FileUseState, TargetAccess, WalFileObservation};
use std::collections::{BTreeMap, BTreeSet};

/// Adopted Exact `StreamPosition` of the file's stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PositionFacts {
    pub replay_from: u64,
    pub last_offset: Option<u64>,
    pub durable_end: u64,
}

/// Facts of one owner stream (partition, fencing generation) in one sample.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StreamFacts {
    /// The stream is the serving authority generation (active, may grow).
    pub same_generation: bool,
    /// Older stream whose completion the installed Exact root committed.
    pub complete_stream: bool,
    pub position: Option<PositionFacts>,
    pub owner_root_scan_from: Option<u64>,
    pub owner_root_synced_end: Option<u64>,
    /// Contiguous adopted end reported by the owner.
    pub processed_end: u64,
    pub page_stable: bool,
    /// Persisted Version B `next` when the durable cut tracks this stream.
    pub persisted_cut_next: Option<u64>,
}

/// Facts of one file in one sample.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FileInputs<'a> {
    pub file_start: u64,
    pub file_end: u64,
    /// `None` when the owner inventory did not list the file.
    pub observation: Option<&'a WalFileObservation>,
    pub stream: &'a StreamFacts,
    pub adopted_root: bool,
    /// A previous root is still selectable until its cleanup completes.
    pub obsolete_root_pending: bool,
    /// Retained puts of the measured Exact image in this file.
    pub retained_puts: u64,
    /// A live overlay, captured generation or pending publication references it.
    pub volatile_reference: bool,
    /// Exclusive upper bound of every batch sequence in the file.
    pub sequence_bound: Option<u64>,
    pub within_budget: bool,
    /// The installed image or owner state changed during the observation.
    pub changed: bool,
}

/// Sorted, nonexclusive blockers of one file. Empty means shadow eligible.
pub(crate) fn evaluate(input: &FileInputs<'_>) -> Vec<Blocker> {
    let mut blockers = BTreeSet::new();
    let stream = input.stream;
    let end = input.file_end;
    if input.changed || !stream.page_stable {
        blockers.insert(Blocker::ChangedDuringObservation);
    }
    match input.observation {
        None => {
            blockers.insert(Blocker::UnvalidatedInventory);
        }
        Some(row) => {
            match row.state {
                FileUseState::Unknown => {
                    blockers.insert(Blocker::UnvalidatedInventory);
                }
                FileUseState::Lookahead => {
                    blockers.insert(Blocker::Lookahead);
                }
                FileUseState::RetiredCleanupPending => {
                    blockers.insert(Blocker::CleanupPending);
                }
                FileUseState::Leased | FileUseState::Used => {}
            }
            if !row.allocator_past {
                blockers.insert(Blocker::AllocatorNotPast);
            }
            if row.write_lease || row.state == FileUseState::Leased {
                blockers.insert(Blocker::WriteLease);
            }
            if row.validated_through.is_none_or(|through| through < end) {
                blockers.insert(Blocker::UnvalidatedFrameOrSkip);
            }
            let read_pins = row.read_pins.is_none_or(|pins| pins != 0);
            let export_pins = row.export_pins.is_none_or(|pins| pins != 0);
            if read_pins {
                blockers.insert(Blocker::ReadPin);
            }
            if export_pins {
                blockers.insert(Blocker::ExportPin);
            }
            match row.target_access {
                // A writable export is revocable only with no other users.
                TargetAccess::Writable if read_pins || export_pins => {
                    blockers.insert(Blocker::UnsafeWritableExport);
                }
                TargetAccess::Unknown => {
                    blockers.insert(Blocker::TargetDrain);
                }
                TargetAccess::None | TargetAccess::ReadOnly | TargetAccess::Writable => {}
            }
        }
    }
    if !input.adopted_root {
        blockers.insert(Blocker::NoAdoptedRoot);
    }
    if input.retained_puts != 0 || input.obsolete_root_pending {
        blockers.insert(Blocker::SelectableRootRef);
    }
    if input.volatile_reference {
        blockers.insert(Blocker::LiveOrCapturedRef);
    }
    match stream.position {
        None => {
            blockers.insert(Blocker::CoordinatorReplayWindow);
        }
        Some(position) => {
            if stream.same_generation {
                if position.replay_from < end || position.durable_end < end {
                    blockers.insert(Blocker::CoordinatorReplayWindow);
                }
            } else if !stream.complete_stream {
                blockers.insert(Blocker::CoordinatorReplayWindow);
            }
            if position
                .last_offset
                .is_some_and(|offset| offset >= input.file_start && offset < end)
            {
                blockers.insert(Blocker::TailOrIdentityProof);
            }
        }
    }
    if stream.same_generation {
        if stream
            .owner_root_synced_end
            .is_none_or(|synced| synced < end)
        {
            blockers.insert(Blocker::OwnerRootBehind);
        }
        if stream.owner_root_scan_from.is_none_or(|scan| scan < end) {
            blockers.insert(Blocker::OwnerRecoveryWindow);
        }
        // The owner durable frontier tail inside the file is still a proof frame.
        if stream.processed_end > input.file_start && stream.processed_end < end {
            blockers.insert(Blocker::TailOrIdentityProof);
        }
        if let Some(next) = stream.persisted_cut_next
            && input.sequence_bound.is_none_or(|bound| bound > next)
        {
            blockers.insert(Blocker::VersionBCut);
        }
    }
    if !input.within_budget {
        blockers.insert(Blocker::SpanBudget);
    }
    blockers.into_iter().collect()
}

/// One owner stream span for the per-owner budget: oldest retained used file
/// start `lo` and contiguous processed frontier `hi`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OwnerSpan {
    pub generation: u64,
    pub lo: u64,
    pub hi: u64,
}

/// Exclusive budget end per stream, consuming older generations first:
/// `max(wal_file_bytes, floor(total * pct / 100))`, rounded down to whole
/// files. Offsets of different streams are never subtracted from each other.
pub(crate) fn span_budget_end(
    spans: &[OwnerSpan],
    wal_file_bytes: u64,
    pct: u8,
) -> BTreeMap<u64, u64> {
    let mut ordered = spans.to_vec();
    ordered.sort_unstable_by_key(|span| span.generation);
    let total: u128 = ordered
        .iter()
        .map(|span| u128::from(span.hi.saturating_sub(span.lo)))
        .sum();
    let file = u128::from(wal_file_bytes.max(1));
    let budget = (total * u128::from(pct.min(100)) / 100).max(file) / file * file;
    let mut remaining = budget;
    let mut ends = BTreeMap::new();
    for span in ordered {
        let length = u128::from(span.hi.saturating_sub(span.lo));
        let take = remaining.min(length);
        let end = u128::from(span.lo) + take;
        ends.insert(span.generation, u64::try_from(end).unwrap_or(u64::MAX));
        remaining -= take;
    }
    ends
}
