//! Read-only owner WAL file inventory and the per-owner span budget.
//!
//! The hybrid coordinator retires dead owner files from the native watermark
//! (`dead_wal_files`) and caps each relocation pass by
//! `relocation_max_reclaim_pct` (`relocation`). Both need the same two things:
//! the page types the owner's `FileInventoryPage` request carries, and the
//! exclusive budget end per owner stream. This module holds exactly those; the
//! R0 shadow accounting that used to sit beside them (blockers, ledger,
//! retained-put attribution, the observer thread) is gone with the Exact index.
//!
//! Nothing here decides eligibility: a file is retired only from the native
//! watermark, so no blocker set is evaluated.

use serde::{Deserialize, Serialize};

/// Largest owner file inventory page, as for the other owner WAL file pages.
pub(crate) const MAX_FILE_INVENTORY_PAGE: u16 = 256;

/// Owner-local use state of one WAL file. A file's nominal length or allocated
/// blocks never classify it; the owner derives it from allocator and lease state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FileUseState {
    /// Prepared ahead of the allocator; no lease or write reached it.
    Lookahead,
    /// An open, preleased or reserved write extent lies in the file.
    Leased,
    /// The allocator reached the file and no write lease remains in it.
    Used,
    /// The durable root retired the offset but its directory entry remains.
    RetiredCleanupPending,
    /// The owner could not prove one of the states above.
    Unknown,
}

/// Target namespace access the owner still exports for one file.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetAccess {
    None,
    ReadOnly,
    Writable,
    Unknown,
}

/// One owner WAL file, observed with `lstat` on the exact regular file. The
/// partition and fencing generation are those of the request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct WalFileObservation {
    pub file_start: u64,
    pub device: u64,
    pub inode: u64,
    /// `st_size`.
    pub logical_bytes: u64,
    /// `st_blocks * 512`.
    pub allocated_bytes: u64,
    pub state: FileUseState,
    /// The allocator permanently passed the file.
    pub allocator_past: bool,
    /// An open, preleased or reserved write extent lies in the file.
    pub write_lease: bool,
    /// Every byte below this offset was validated by adoption, extent close or
    /// stream seal. `None` when the owner has no such proof for the file.
    pub validated_through: Option<u64>,
    /// Owner record/page read pins in the file; `None` when not attributable.
    pub read_pins: Option<u64>,
    pub target_access: TargetAccess,
    /// Durable lease-logical `FileSeal` of this exact inode, if installed.
    pub seal: Option<crate::distributed::storage::FileSeal>,
}

/// Owner reply to `FileInventoryPage`. Rows are sorted by `file_start`, all
/// strictly after the request cursor, at most the request limit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FileInventoryReply {
    pub partition: u32,
    pub source_generation: u64,
    pub owner_incarnation: [u8; 16],
    /// `WalRoot.generation`; `None` without WAL-replay history.
    pub owner_root_generation: Option<u64>,
    pub wal_scan_from: Option<u64>,
    pub wal_synced_end: Option<u64>,
    /// Contiguous adopted end of the stream.
    pub processed_end: u64,
    /// False when allocator, lease or root state changed during enumeration.
    pub stable: bool,
    /// Value WAL files this owner process unlinked through retirement.
    pub value_wal_unlinks: u64,
    pub rows: Vec<WalFileObservation>,
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
) -> std::collections::BTreeMap<u64, u64> {
    use std::collections::BTreeMap;

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
