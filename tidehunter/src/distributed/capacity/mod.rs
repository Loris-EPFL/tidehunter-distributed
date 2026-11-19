//! R0 shadow capacity accounting for leased owner Value WAL files.
//!
//! R0 is read-only: it observes owner file inventories, attributes the
//! retained puts of the installed Exact image to WAL files, evaluates the
//! retirement blockers of each file and accumulates net allocated growth after
//! a *shadow* dead-only retirement. Nothing here unlinks, seals, revokes or
//! installs a root. The engine produces typed samples; the benchmark writes
//! them as `r0.json` (`checkpoint-work/research/design/CAPACITY-R0-R1.md` §2).

mod blockers;
mod ledger;
mod retained;

pub(crate) use blockers::{
    FileInputs, OwnerSpan, PositionFacts, StreamFacts, evaluate, span_budget_end,
};
pub(crate) use ledger::{GrowthLedger, LedgerFile};
pub(crate) use retained::{FileId, FileRetained, RetainedAccumulator};

use serde::{Deserialize, Serialize};

/// Largest owner file inventory page, as for the other owner WAL file pages.
pub const MAX_FILE_INVENTORY_PAGE: u16 = 256;

/// Owner-local use state of one WAL file. A file's nominal length or allocated
/// blocks never classify it; the owner derives it from allocator and lease state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileUseState {
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
pub enum TargetAccess {
    None,
    ReadOnly,
    Writable,
    Unknown,
}

/// One owner WAL file, observed with `lstat` on the exact regular file. The
/// partition and fencing generation are those of the request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalFileObservation {
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
    /// Owner connection (export) pins in the file; `None` when not attributable.
    pub export_pins: Option<u64>,
    pub target_access: TargetAccess,
}

/// Owner reply to `FileInventoryPage`. Rows are sorted by `file_start`, all
/// strictly after the request cursor, at most the request limit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileInventoryReply {
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

/// Retirement blockers, in the stable order of the design. A file is shadow
/// eligible only when its proven blocker set is empty.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Blocker {
    UnvalidatedInventory,
    Lookahead,
    AllocatorNotPast,
    WriteLease,
    UnvalidatedFrameOrSkip,
    OwnerRootBehind,
    UnsafeWritableExport,
    NoAdoptedRoot,
    SelectableRootRef,
    LiveOrCapturedRef,
    HistoricalView,
    OwnerRecoveryWindow,
    CoordinatorReplayWindow,
    VersionBCut,
    TailOrIdentityProof,
    ReadPin,
    ExportPin,
    TargetDrain,
    CleanupPending,
    SpanBudget,
    ChangedDuringObservation,
}

/// Identity of an adopted Exact root.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ExactKey {
    pub fencing_generation: u64,
    pub checkpoint_generation: u64,
    /// Hex of the 32-byte metadata digest.
    pub metadata_digest: String,
}

/// A logical index position.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct LogicalCut {
    pub fencing_generation: u64,
    pub sequence: u64,
    pub operation_index: u32,
}

/// Persisted Version B cut of the serving fencing generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DurableCutWitness {
    pub fencing_generation: u64,
    pub persisted_next: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleEvent {
    Begin,
    Prune,
    Roll,
    End,
}

/// One owner file crossing reported by the live writer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RolledFile {
    pub partition: u32,
    pub fencing_generation: u64,
    /// File the writer moved to; every lower file of the stream was passed.
    pub file_index: u64,
}

/// Coordinator-clock timing of one owner inventory RPC sequence. Owner clocks
/// are never compared with the coordinator clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct OwnerRpcTiming {
    pub partition: u32,
    pub fencing_generation: u64,
    pub observed_start_ns: u64,
    pub observed_end_ns: u64,
    pub pages: u64,
    pub rows: u64,
    pub stable: bool,
}

/// One file row of a sample. `null` means unknown, never zero.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct R0File {
    pub partition: u32,
    pub fencing_generation: u64,
    pub file_start: u64,
    pub file_end: u64,
    pub device: Option<u64>,
    pub inode: Option<u64>,
    pub logical_bytes: Option<u64>,
    pub allocated_bytes: Option<u64>,
    pub state: FileUseState,
    pub retained_put_members: u64,
    pub retained_member_bytes: u64,
    pub retained_value_bytes: u64,
    /// Distinct complete batches (by `batch_address`); `null` if dedup failed.
    pub retained_batches: Option<u64>,
    pub retained_batch_bytes: Option<u64>,
    pub affected_cells: u64,
    pub owner_root_generation: Option<u64>,
    pub wal_scan_from: Option<u64>,
    pub wal_synced_end: Option<u64>,
    pub replay_from: Option<u64>,
    pub last_offset: Option<u64>,
    pub durable_end: Option<u64>,
    pub complete_stream: Option<bool>,
    pub validated_through: Option<u64>,
    /// Exact last batch sequence; R0 has no seal record, so always `null`.
    pub last_batch_sequence: Option<u64>,
    /// Exclusive upper bound of every batch sequence in the file, when known.
    pub batch_sequence_bound: Option<u64>,
    pub read_pins: Option<u64>,
    pub export_pins: Option<u64>,
    pub blockers: Vec<Blocker>,
    pub shadow_eligible: bool,
    pub newly_shadow_reclaimed_bytes: u64,
}

/// One observation. Files are sorted by partition, fencing generation, start.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct R0Sample {
    pub sample_id: u64,
    pub event: SampleEvent,
    pub observed_start_ns: u64,
    pub observed_end_ns: u64,
    pub complete: bool,
    pub reason: Option<String>,
    pub rolled_files: Vec<RolledFile>,
    pub exact_key: Option<ExactKey>,
    pub cut: Option<LogicalCut>,
    /// `null` with `floor_before_all` false means unknown.
    pub floor: Option<LogicalCut>,
    pub floor_before_all: bool,
    pub durable_cut: Option<DurableCutWitness>,
    pub owner_rpcs: Vec<OwnerRpcTiming>,
    pub files: Vec<R0File>,
}

/// Per-owner growth over the run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OwnerGrowth {
    pub partition: u32,
    pub baseline_allocated_bytes: u64,
    pub end_allocated_bytes: u64,
    pub new_allocated_bytes: u64,
    pub newly_shadow_reclaimed_bytes: u64,
    pub net_allocated_growth_bytes_per_day: Option<i64>,
    pub reason: Option<String>,
}

/// Owner with the largest signed slope, ties to the lowest partition.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WorstOwner {
    pub partition: Option<u32>,
    pub net_allocated_growth_bytes_per_day: Option<i64>,
    pub reason: Option<String>,
}

/// Engine-known run geometry for the report header.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct R0Geometry {
    /// Hex of the 16-byte database ID.
    pub database: String,
    pub owners: Vec<u32>,
    pub wal_file_bytes: u64,
    pub fragment_bytes: u64,
    pub relocation_max_reclaim_pct: u8,
}

/// Run totals, known only after the last sample.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct R0Summary {
    pub elapsed_ns: u64,
    /// Number of streamed samples. `r0.json` keeps the sample array under
    /// `samples`, so the count has its own key.
    pub sample_count: u64,
    pub prune_events: u64,
    pub file_roll_events: u64,
    pub missed_events: u64,
    /// Sum over owners of the unlink counter delta; `null` if an owner was not
    /// observed at both begin and end.
    pub value_wal_unlinks: Option<u64>,
    /// Coordinator retirement passes the shadow mode skipped.
    pub suppressed_retirement_passes: u64,
    pub complete: bool,
    pub reasons: Vec<String>,
    pub per_owner: Vec<OwnerGrowth>,
    pub worst_owner: WorstOwner,
}

/// Receives each sample on the observer thread, in order. A sink error ends
/// observation and fails the run; it never affects user writes.
pub trait R0Sink: Send {
    fn sample(&mut self, sample: &R0Sample) -> std::io::Result<()>;
}

/// Observer configuration.
#[derive(Clone, Debug)]
pub struct CapacityObserverConfig {
    /// Native `relocation_max_reclaim_pct` used for the shadow span budget.
    pub relocation_max_reclaim_pct: u8,
    /// Distinct-batch records kept in memory before spilling a sorted run.
    pub max_batch_records: usize,
    /// Metadata-only spill directory under the run result directory.
    pub scratch_dir: Option<std::path::PathBuf>,
}

impl Default for CapacityObserverConfig {
    fn default() -> Self {
        Self {
            // Sui sets 100 (`typed-store/src/tidehunter_util.rs`).
            relocation_max_reclaim_pct: 100,
            max_batch_records: 1 << 20,
            scratch_dir: None,
        }
    }
}

/// Measurement failure. It never fails a user operation.
#[derive(Debug)]
pub struct R0Error(pub String);

impl std::fmt::Display for R0Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for R0Error {}

impl R0Error {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// Messages to the observer thread.
///
/// Channel owner: the observer (sole receiver). Capacity [`OBSERVER_QUEUE`].
/// Senders: the prune path and live writers use `try_send` and never wait: a
/// full queue counts a missed prune or a dropped crossing (never a sample from
/// a later cut). The benchmark sends `Begin`/`End` with a blocking send and
/// waits for the capacity-one reply. Shutdown: `End` is the last message; the
/// thread exits after replying.
pub(crate) enum ObserverEvent {
    /// One file crossing, captured coherently by the live writer at the roll.
    Roll(Crossing),
    Prune {
        exact: Option<ExactKey>,
    },
    Begin(std::sync::mpsc::SyncSender<Result<(), R0Error>>),
    End(std::sync::mpsc::SyncSender<Result<R0Summary, R0Error>>),
}

pub(crate) const OBSERVER_QUEUE: usize = 64;

/// One file crossing, sent by value so its file, bound and image belong to
/// the same roll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Crossing {
    /// Endpoint index of the owner.
    pub owner: usize,
    /// File the writer moved to; every lower file of the stream was passed.
    pub file: u64,
    /// Exclusive bound of every batch sequence submitted before the crossing.
    pub sequence_bound: u64,
    /// Installed-image seqlock value at the crossing: the Exact image
    /// installed when the file rolled.
    pub image: u64,
}

/// Per-owner live-writer file crossing reporter, installed only while an
/// observer runs. The writer sends one [`Crossing`] by value per roll with
/// `try_send`, without allocation or wait. Monotone atomics keep the latest
/// file and sequence bound for crossings whose event was dropped.
pub(crate) struct RollCursor {
    owner: usize,
    /// One above the latest file moved to; zero before a crossing.
    file: std::sync::atomic::AtomicU64,
    sequence_bound: std::sync::atomic::AtomicU64,
    /// Crossing events dropped on a full queue.
    dropped: std::sync::atomic::AtomicU64,
    install_seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
    wake: std::sync::mpsc::SyncSender<ObserverEvent>,
}

impl RollCursor {
    pub(crate) fn new(
        owner: usize,
        wake: std::sync::mpsc::SyncSender<ObserverEvent>,
        install_seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        Self {
            owner,
            file: std::sync::atomic::AtomicU64::new(0),
            sequence_bound: std::sync::atomic::AtomicU64::new(0),
            dropped: std::sync::atomic::AtomicU64::new(0),
            install_seq,
            wake,
        }
    }

    pub(crate) fn crossed(&self, file: u64, sequence_bound: u64) {
        use std::sync::atomic::Ordering;
        // Acquire pairs with the image installer's AcqRel increments.
        let image = self.install_seq.load(Ordering::Acquire);
        // Release, bound before file: a reader that acquires a file value sees
        // a bound at least as new, which is only larger and still an upper
        // bound. These atomics never qualify a sample; the event does.
        self.sequence_bound
            .fetch_max(sequence_bound, Ordering::Release);
        self.file
            .fetch_max(file.saturating_add(1), Ordering::Release);
        let crossing = Crossing {
            owner: self.owner,
            file,
            sequence_bound,
            image,
        };
        if self.wake.try_send(ObserverEvent::Roll(crossing)).is_err() {
            // Relaxed: independent counter, read by the observer only.
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `(latest file moved to, its sequence bound)`, for bounds only.
    pub(crate) fn latest(&self) -> Option<(u64, u64)> {
        use std::sync::atomic::Ordering;
        // Acquire pairs with the Release stores in `crossed`.
        let file = self.file.load(Ordering::Acquire);
        let sequence_bound = self.sequence_bound.load(Ordering::Acquire);
        file.checked_sub(1).map(|file| (file, sequence_bound))
    }

    pub(crate) fn dropped(&self) -> u64 {
        // Relaxed: see `crossed`.
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

#[cfg(test)]
mod tests;
