//! Exact checkpoint attribution (roadmap Phase 6 A0).
//!
//! Per-generation counters live in a worker-local [`CheckpointAccumulator`]
//! and travel in the existing bounded diagnostic ring as one row per cleaned or
//! failed generation. Coordinator file I/O is a process-wide cumulative counter
//! set (relaxed atomics) so a benchmark can assign writes to its windows by the
//! time they happened. Nothing here takes a lock: callers pass values that they
//! captured under the locks they already hold, in the existing lock order.

use super::write_core::core::WriteCore;
use super::*;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static DIAGNOSTICS_ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
static NEXT_OPEN_ID: AtomicU64 = AtomicU64::new(1);

/// Nanoseconds since the process diagnostics origin. Checkpoint rows and
/// benchmark window markers use this one monotonic clock, never owner clocks.
pub fn diagnostics_clock_ns() -> u64 {
    u64::try_from(DIAGNOSTICS_ORIGIN.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Process-unique identity of one database open.
pub(super) fn next_open_id() -> u64 {
    // Relaxed: the value only has to be unique; it orders no other memory.
    NEXT_OPEN_ID.fetch_add(1, Ordering::Relaxed)
}

/// The predicates and requests one checkpoint launch serves. A coalesced
/// request starts no generation; it is not recorded.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckpointTriggerFlags(u32);

impl CheckpointTriggerFlags {
    pub const GRANTED_BATCHES_SOFT: Self = Self(1);
    pub const GRANTED_BYTES_SOFT: Self = Self(1 << 1);
    pub const PUBLISHED_BATCHES_SOFT: Self = Self(1 << 2);
    pub const PUBLISHED_BYTES_SOFT: Self = Self(1 << 3);
    pub const GRANTED_IDENTITIES_SOFT: Self = Self(1 << 4);
    pub const KNOWN_IDENTITIES_SOFT: Self = Self(1 << 5);
    pub const INDEX_SOFT: Self = Self(1 << 6);
    pub const HARD_IDENTITY: Self = Self(1 << 7);
    pub const HARD_INDEX: Self = Self(1 << 8);
    pub const EXPLICIT: Self = Self(1 << 9);
    pub const RETENTION_FLOOR: Self = Self(1 << 10);
    pub const RECOVERY: Self = Self(1 << 11);
    pub const HARNESS_DRAIN: Self = Self(1 << 12);
    /// The capture took a plan from the asynchronous relocation worker.
    pub const RELOCATION: Self = Self(1 << 13);
    /// A named compactor force or at least one evaluated callback cell.
    pub const COMPACTOR: Self = Self(1 << 14);

    pub const NAMES: [(Self, &'static str); 15] = [
        (Self::GRANTED_BATCHES_SOFT, "granted_batches_soft"),
        (Self::GRANTED_BYTES_SOFT, "granted_bytes_soft"),
        (Self::PUBLISHED_BATCHES_SOFT, "published_batches_soft"),
        (Self::PUBLISHED_BYTES_SOFT, "published_bytes_soft"),
        (Self::GRANTED_IDENTITIES_SOFT, "granted_identities_soft"),
        (Self::KNOWN_IDENTITIES_SOFT, "known_identities_soft"),
        (Self::INDEX_SOFT, "index_soft"),
        (Self::HARD_IDENTITY, "hard_identity"),
        (Self::HARD_INDEX, "hard_index"),
        (Self::EXPLICIT, "explicit"),
        (Self::RETENTION_FLOOR, "retention_floor"),
        (Self::RECOVERY, "recovery"),
        (Self::HARNESS_DRAIN, "harness_drain"),
        (Self::RELOCATION, "relocation"),
        (Self::COMPACTOR, "compactor"),
    ];

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn names(self) -> impl Iterator<Item = &'static str> {
        Self::NAMES
            .into_iter()
            .filter(move |(flag, _)| self.contains(*flag))
            .map(|(_, name)| name)
    }

    /// Hard admission bit for the first blocking checkpoint dimension.
    /// Replay-tail reasons have no hard bit: WAL replay bypasses them.
    pub(super) fn hard(reason: &str) -> Self {
        match reason {
            "identity" => Self::HARD_IDENTITY,
            "index" => Self::HARD_INDEX,
            _ => Self::default(),
        }
    }
}

impl std::ops::BitOr for CheckpointTriggerFlags {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOrAssign for CheckpointTriggerFlags {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

/// The request a checkpoint worker serves, observed when it was launched.
#[derive(Clone, Copy, Debug)]
pub(super) struct CheckpointLaunch {
    pub trigger: CheckpointTriggerFlags,
    pub requested_at_ns: u64,
}

impl CheckpointLaunch {
    pub(super) fn new(trigger: CheckpointTriggerFlags) -> Self {
        Self {
            trigger,
            requested_at_ns: diagnostics_clock_ns(),
        }
    }
}

/// Completed file work: payload bytes, syncs and allocated blocks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileIo {
    /// Payload bytes issued by completed writes. A renamed temporary counts once.
    pub bytes: u64,
    pub file_syncs: u64,
    pub dir_syncs: u64,
    /// `st_blocks × 512` of each new synced inode; zero unless allocation
    /// accounting is on. Reused old container blocks are never counted.
    pub allocated_bytes: u64,
}

impl FileIo {
    pub fn add(&mut self, other: Self) {
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.file_syncs = self.file_syncs.saturating_add(other.file_syncs);
        self.dir_syncs = self.dir_syncs.saturating_add(other.dir_syncs);
        self.allocated_bytes = self.allocated_bytes.saturating_add(other.allocated_bytes);
    }

    /// One `install_metadata` call: temporary write, file sync, rename, and
    /// one parent directory sync.
    fn installed(bytes: usize, path: &Path) -> Self {
        Self {
            bytes: bytes as u64,
            file_syncs: 1,
            dir_syncs: 1,
            allocated_bytes: allocated_bytes(path),
        }
    }
}

/// Per-generation coordinator file work, by output.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointIo {
    /// Frame container: rewritten cell frames and one file sync if it exists.
    pub cells: FileIo,
    /// Index root, its file sync and the one shared root/container directory sync.
    pub root: FileIo,
    /// Checkpoint metadata wrapper.
    pub wrapper: FileIo,
    pub manifest_stage: FileIo,
    pub manifest_adopt: FileIo,
    pub manifest_cleanup: FileIo,
    /// Unlink directory sync of obsolete names; no payload bytes.
    pub cleanup_names: FileIo,
}

impl CheckpointIo {
    pub fn total(&self) -> FileIo {
        let mut total = FileIo::default();
        for io in [
            self.cells,
            self.root,
            self.wrapper,
            self.manifest_stage,
            self.manifest_adopt,
            self.manifest_cleanup,
            self.cleanup_names,
        ] {
            total.add(io);
        }
        total
    }
}

/// Coordinator files by category. Checkpoint categories also appear in the
/// per-generation rows; `Open` and `DurableCut` are not Exact output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CoordinatorIoCategory {
    Cells,
    Root,
    Wrapper,
    ManifestStage,
    ManifestAdopt,
    ManifestCleanup,
    CleanupNames,
    /// Authority manifest writes and cleanup during open.
    Open,
    /// `durable-cut.v1` record writes.
    DurableCut,
}

const CATEGORIES: usize = 9;

struct IoCounters {
    bytes: AtomicU64,
    file_syncs: AtomicU64,
    dir_syncs: AtomicU64,
    allocated_bytes: AtomicU64,
}

impl IoCounters {
    const fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            file_syncs: AtomicU64::new(0),
            dir_syncs: AtomicU64::new(0),
            allocated_bytes: AtomicU64::new(0),
        }
    }

    fn load(&self) -> FileIo {
        // Relaxed: independent statistics; a snapshot is not an atomic set.
        FileIo {
            bytes: self.bytes.load(Ordering::Relaxed),
            file_syncs: self.file_syncs.load(Ordering::Relaxed),
            dir_syncs: self.dir_syncs.load(Ordering::Relaxed),
            allocated_bytes: self.allocated_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Process-wide: the sum over every coordinator database of this process.
static COORDINATOR_IO: [IoCounters; CATEGORIES] = [const { IoCounters::new() }; CATEGORIES];
static ALLOCATION_ACCOUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATION_ERRORS: AtomicU64 = AtomicU64::new(0);

/// Enable `st_blocks` capture of new coordinator inodes (one `stat` per file).
/// Off by default; only device-attribution runs need it.
pub fn set_coordinator_allocation_accounting(enabled: bool) {
    // Relaxed: the flag only selects whether later writes add a stat call.
    ALLOCATION_ACCOUNTING.store(enabled, Ordering::Relaxed);
}

fn allocated_bytes(path: &Path) -> u64 {
    if !ALLOCATION_ACCOUNTING.load(Ordering::Relaxed) {
        return 0;
    }
    allocation_of(path).unwrap_or_else(|| {
        // A missing allocation makes device attribution ineligible.
        ALLOCATION_ERRORS.fetch_add(1, Ordering::Relaxed);
        0
    })
}

#[cfg(unix)]
fn allocation_of(path: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path)
        .ok()
        .map(|metadata| metadata.blocks().saturating_mul(512))
}

#[cfg(not(unix))]
fn allocation_of(_path: &Path) -> Option<u64> {
    None
}

pub(super) fn record_coordinator_io(category: CoordinatorIoCategory, io: FileIo) {
    let counters = &COORDINATOR_IO[category as usize];
    // Relaxed: independent cumulative statistics.
    counters.bytes.fetch_add(io.bytes, Ordering::Relaxed);
    counters
        .file_syncs
        .fetch_add(io.file_syncs, Ordering::Relaxed);
    counters
        .dir_syncs
        .fetch_add(io.dir_syncs, Ordering::Relaxed);
    counters
        .allocated_bytes
        .fetch_add(io.allocated_bytes, Ordering::Relaxed);
}

/// `install_metadata` plus its counted work, recorded under `category`.
pub(super) fn install_counted(
    path: &Path,
    name: &str,
    bytes: &[u8],
    category: CoordinatorIoCategory,
) -> std::io::Result<FileIo> {
    install_metadata(path, name, bytes)?;
    Ok(record_installed(path, name, bytes.len(), category))
}

/// Count one completed `install_metadata(path, name, ..)` of `len` bytes.
pub(super) fn record_installed(
    path: &Path,
    name: &str,
    len: usize,
    category: CoordinatorIoCategory,
) -> FileIo {
    let io = FileIo::installed(len, &path.join(name));
    record_coordinator_io(category, io);
    io
}

/// One directory sync after unlinking checkpoint names.
pub(super) fn record_unlink_sync(category: CoordinatorIoCategory) -> FileIo {
    let io = FileIo {
        dir_syncs: 1,
        ..FileIo::default()
    };
    record_coordinator_io(category, io);
    io
}

/// Cumulative coordinator file work of this process, by category.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinatorIoSnapshot {
    pub cells: FileIo,
    pub root: FileIo,
    pub wrapper: FileIo,
    pub manifest_stage: FileIo,
    pub manifest_adopt: FileIo,
    pub manifest_cleanup: FileIo,
    pub cleanup_names: FileIo,
    pub open: FileIo,
    pub durable_cut: FileIo,
    pub allocation_accounting: bool,
    /// New inodes whose allocation could not be read; nonzero makes the
    /// allocated totals incomplete.
    pub allocation_errors: u64,
}

impl CoordinatorIoSnapshot {
    pub fn total(&self) -> FileIo {
        let mut total = FileIo::default();
        for io in [
            self.cells,
            self.root,
            self.wrapper,
            self.manifest_stage,
            self.manifest_adopt,
            self.manifest_cleanup,
            self.cleanup_names,
            self.open,
            self.durable_cut,
        ] {
            total.add(io);
        }
        total
    }
}

pub fn coordinator_io_snapshot() -> CoordinatorIoSnapshot {
    let load = |category: CoordinatorIoCategory| COORDINATOR_IO[category as usize].load();
    CoordinatorIoSnapshot {
        cells: load(CoordinatorIoCategory::Cells),
        root: load(CoordinatorIoCategory::Root),
        wrapper: load(CoordinatorIoCategory::Wrapper),
        manifest_stage: load(CoordinatorIoCategory::ManifestStage),
        manifest_adopt: load(CoordinatorIoCategory::ManifestAdopt),
        manifest_cleanup: load(CoordinatorIoCategory::ManifestCleanup),
        cleanup_names: load(CoordinatorIoCategory::CleanupNames),
        open: load(CoordinatorIoCategory::Open),
        durable_cut: load(CoordinatorIoCategory::DurableCut),
        allocation_accounting: ALLOCATION_ACCOUNTING.load(Ordering::Relaxed),
        allocation_errors: ALLOCATION_ERRORS.load(Ordering::Relaxed),
    }
}

/// Batches and canonical encoded bytes of one suffix population.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TailCount {
    pub batches: u64,
    pub bytes: u64,
}

impl TailCount {
    pub(super) fn new(batches: usize, bytes: usize) -> Self {
        Self {
            batches: batches as u64,
            bytes: bytes as u64,
        }
    }
}

/// Admission credits returned by one generation. Index reservations are
/// returned at publication, never by checkpoint credit return.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReturnedCredits {
    pub batches: u64,
    pub bytes: u64,
    pub identities: u64,
}

/// Index image cells of one generation. `rewritten = buffered + streamed`
/// (new frames only), `captured = reused + rewritten + empty`,
/// `root = reused + rewritten`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CellPersistCounts {
    pub buffered_bytes: u64,
    pub streamed_bytes: u64,
    pub buffered_cells: u64,
    pub streamed_cells: u64,
    pub encode_passes: u64,
    pub empty_cells: u64,
    pub rewritten_cells: u64,
    pub reused_cells: u64,
    /// Bytes of reused old frames; already durable, never written again.
    pub reused_frame_bytes: u64,
    pub captured_cells: u64,
    pub root_cells: u64,
}

/// Boundaries of one generation, in order. A failed row names the last one
/// that completed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CheckpointStage {
    Launched,
    CaptureEnd,
    StagingManifest,
    Positions,
    IndexPersist,
    Wrapper,
    RootAdopted,
    LiveInstalled,
    CreditsReturned,
    Cleaned,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum GenerationStatus {
    #[default]
    Cleaned,
    Failed,
}

/// One Exact generation. All `*_at_ns` use [`diagnostics_clock_ns`]; `None`
/// means the boundary was not reached. Publisher and admission pairs are
/// different instants and populations: never add granted and published.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CheckpointAttribution {
    pub open_id: u64,
    pub database: [u8; 16],
    pub fencing_generation: u64,
    pub status: GenerationStatus,
    pub last_completed_stage: Option<CheckpointStage>,
    pub trigger_flags: CheckpointTriggerFlags,
    /// Scalar retention floor consumed by this capture, if any.
    pub retention_floor: Option<MutationVersion>,
    pub requested_at_ns: u64,
    pub worker_started_at_ns: Option<u64>,
    pub capture_start_at_ns: Option<u64>,
    pub capture_end_at_ns: Option<u64>,
    /// Published records beyond the captured cut that stay in the suffix.
    pub sparse_beyond_cut: Option<u64>,
    pub adopt_start_at_ns: Option<u64>,
    pub adopt_end_at_ns: Option<u64>,
    /// RAM install only: first `begin_install_image` through the last step.
    pub live_install_start_at_ns: Option<u64>,
    pub live_install_end_at_ns: Option<u64>,
    pub credits_returned_at_ns: Option<u64>,
    pub cleanup_end_at_ns: Option<u64>,
    pub published_before: Option<TailCount>,
    pub published_after: Option<TailCount>,
    pub granted_before: Option<TailCount>,
    pub granted_after: Option<TailCount>,
    pub returned: Option<ReturnedCredits>,
    pub charged_index_at_capture_bytes: Option<u64>,
    pub charged_index_at_capture_at_ns: Option<u64>,
    pub charged_index_at_credit_return_bytes: Option<u64>,
    pub charged_index_at_credit_return_at_ns: Option<u64>,
    pub cells: Option<CellPersistCounts>,
    pub io: CheckpointIo,
    /// Worker thread CPU time from launch through completion accounting.
    pub worker_cpu_ns: Option<u64>,
}

/// Run totals and rolling extrema of recorded generations, for the timing
/// visitor. Kept in the ring summary; the visitor is not a record transport.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttributionTotals {
    pub cleaned: u64,
    pub failed: u64,
    /// Coordinator file work of recorded generations, cleaned and failed.
    pub io: FileIo,
    pub rewritten_cells: u64,
    pub reused_cells: u64,
    pub empty_cells: u64,
    pub worker_cpu_ns: u64,
    /// Capture start to cleanup end of cleaned generations.
    pub min_capture_to_cleanup_ns: u64,
    pub max_capture_to_cleanup_ns: u64,
}

impl AttributionTotals {
    pub(super) fn record(&mut self, row: &CheckpointAttribution) {
        match row.status {
            GenerationStatus::Cleaned => self.cleaned += 1,
            GenerationStatus::Failed => self.failed += 1,
        }
        self.io.add(row.io.total());
        if let Some(cells) = row.cells {
            self.rewritten_cells += cells.rewritten_cells;
            self.reused_cells += cells.reused_cells;
            self.empty_cells += cells.empty_cells;
        }
        self.worker_cpu_ns = self
            .worker_cpu_ns
            .saturating_add(row.worker_cpu_ns.unwrap_or_default());
        if row.status == GenerationStatus::Cleaned
            && let (Some(start), Some(end)) = (row.capture_start_at_ns, row.cleanup_end_at_ns)
        {
            let span = end.saturating_sub(start);
            self.min_capture_to_cleanup_ns = if self.cleaned == 1 {
                span
            } else {
                self.min_capture_to_cleanup_ns.min(span)
            };
            self.max_capture_to_cleanup_ns = self.max_capture_to_cleanup_ns.max(span);
        }
    }
}

/// Worker-local accumulator; its row enters the ring once, at completion or
/// failure, under the existing publisher lock.
pub(super) struct CheckpointAccumulator {
    pub row: CheckpointAttribution,
    cpu_start: Option<u64>,
}

impl CheckpointAccumulator {
    /// Called on the worker thread, before any checkpoint work.
    pub(super) fn launched(core: &WriteCore, launch: CheckpointLaunch) -> Self {
        let cpu_start = thread_cpu_ns();
        Self {
            row: CheckpointAttribution {
                open_id: core.stats.lock().recovery.open_id,
                database: core.config.storage.database,
                fencing_generation: core.epoch,
                trigger_flags: launch.trigger,
                requested_at_ns: launch.requested_at_ns,
                worker_started_at_ns: Some(diagnostics_clock_ns()),
                last_completed_stage: Some(CheckpointStage::Launched),
                ..CheckpointAttribution::default()
            },
            cpu_start,
        }
    }

    pub(super) fn reached(&mut self, stage: CheckpointStage) {
        self.row.last_completed_stage = Some(stage);
    }

    pub(super) fn capture_started(&mut self, charged_index_bytes: usize) {
        let now = diagnostics_clock_ns();
        self.row.capture_start_at_ns = Some(now);
        self.row.charged_index_at_capture_bytes = Some(charged_index_bytes as u64);
        self.row.charged_index_at_capture_at_ns = Some(now);
    }

    /// Under publisher, at the freeze.
    pub(super) fn captured(
        &mut self,
        suffix: &mut Option<SuffixTracker>,
        floor: Option<&IndexReadCut>,
        sparse_beyond_cut: usize,
    ) {
        self.row.retention_floor = match floor {
            Some(IndexReadCut::At(version)) => Some(*version),
            _ => None,
        };
        self.row.sparse_beyond_cut = Some(sparse_beyond_cut as u64);
        if let Some(suffix) = suffix {
            suffix.on_capture(sparse_beyond_cut > 0);
        }
    }

    /// Under publisher, around the checked subtraction of the captured suffix.
    pub(super) fn tail_released(
        &mut self,
        suffix: &mut Option<SuffixTracker>,
        before: TailCount,
        after: TailCount,
        retained_records: bool,
    ) {
        self.row.published_before = Some(before);
        self.row.published_after = Some(after);
        if let Some(suffix) = suffix {
            let now = diagnostics_clock_ns();
            // Peak immediately before credit-driven removal.
            suffix.observe(now, before.batches, before.bytes);
            suffix.on_install(retained_records);
        }
    }

    pub(super) fn credits_returned(
        &mut self,
        returned: ReturnedCredits,
        granted: (TailCount, TailCount),
        charged_index_bytes: usize,
    ) {
        let now = diagnostics_clock_ns();
        self.row.returned = Some(returned);
        self.row.granted_before = Some(granted.0);
        self.row.granted_after = Some(granted.1);
        self.row.credits_returned_at_ns = Some(now);
        self.row.charged_index_at_credit_return_bytes = Some(charged_index_bytes as u64);
        self.row.charged_index_at_credit_return_at_ns = Some(now);
        self.reached(CheckpointStage::CreditsReturned);
    }

    /// Index image counters and the root/container file work of `persist`.
    pub(super) fn image_persisted(
        &mut self,
        encode: &super::super::native_index::CellEncodeStats,
        stats: &super::super::native_index::ImagePersistStats,
        root: &Path,
    ) {
        let rewritten = encode.buffered_cells + encode.streamed_cells;
        self.row.cells = Some(CellPersistCounts {
            buffered_bytes: encode.buffered_bytes,
            streamed_bytes: encode.streamed_bytes,
            buffered_cells: encode.buffered_cells,
            streamed_cells: encode.streamed_cells,
            encode_passes: encode.encode_passes,
            empty_cells: encode.empty_cells,
            rewritten_cells: rewritten,
            reused_cells: stats.reused_cells,
            reused_frame_bytes: stats.reused_frame_bytes,
            captured_cells: stats.captured_cells,
            root_cells: stats.root_cells,
        });
        let mut container = root.as_os_str().to_owned();
        container.push(".cells");
        let container = PathBuf::from(container);
        let cells = FileIo {
            bytes: encode.buffered_bytes + encode.streamed_bytes,
            file_syncs: u64::from(stats.container_created),
            dir_syncs: 0,
            allocated_bytes: if stats.container_created {
                allocated_bytes(&container)
            } else {
                0
            },
        };
        let root_io = FileIo {
            bytes: stats.root_bytes,
            file_syncs: 1,
            dir_syncs: 1,
            allocated_bytes: allocated_bytes(root),
        };
        record_coordinator_io(CoordinatorIoCategory::Cells, cells);
        record_coordinator_io(CoordinatorIoCategory::Root, root_io);
        self.row.io.cells = cells;
        self.row.io.root = root_io;
        self.reached(CheckpointStage::IndexPersist);
    }

    /// Final row: thread CPU through completion accounting and its status.
    pub(super) fn finish(&mut self, status: GenerationStatus) -> CheckpointAttribution {
        self.row.status = status;
        self.row.worker_cpu_ns = self
            .cpu_start
            .zip(thread_cpu_ns())
            .map(|(start, end)| end.saturating_sub(start));
        self.row
    }
}

/// On-CPU time of the calling thread (`/proc/thread-self/schedstat`, first
/// field, nanoseconds). `None` when the kernel does not expose it.
fn thread_cpu_ns() -> Option<u64> {
    let text = fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    text.split_ascii_whitespace().next()?.parse().ok()
}

/// How the exported oldest-record age relates to the true age.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum AgeQuality {
    #[default]
    Exact,
    /// Records retained from before arming or recovery: the true age is larger.
    LowerBound,
    /// Sparse records carried over a capture: the true age is not larger.
    UpperBound,
    /// A carried bound over a lower bound: no relation is known.
    Indeterminate,
}

/// Published uncheckpointed suffix at one instant.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SuffixSample {
    pub at_ns: u64,
    pub batches: u64,
    pub bytes: u64,
    /// Wall time since the oldest retained record first published.
    pub oldest_age_ns: u64,
    pub age_quality: AgeQuality,
}

/// Independent run peaks, each with the contemporaneous other values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SuffixPeaks {
    pub batches: SuffixSample,
    pub bytes: SuffixSample,
    pub oldest_age: SuffixSample,
    /// Charged index bytes sampled at each publication while armed.
    pub charged_index_bytes: u64,
}

/// Publisher-owned suffix debt tracker. O(1) per publication; no tail queue.
/// After a capture, the retained records are those published after it plus
/// the sparse records beyond the cut, so the oldest retained publication is
/// known exactly unless a sparse record was carried.
pub(super) struct SuffixTracker {
    oldest_published_ns: Option<u64>,
    quality: AgeQuality,
    first_after_capture_ns: Option<u64>,
    sparse_at_capture: bool,
    peaks: SuffixPeaks,
}

impl SuffixTracker {
    /// `retained_records`: published records already exist (recovered suffix
    /// or pre-arm publication); their original publication time is unknown.
    pub(super) fn armed(now: u64, retained_records: bool) -> Self {
        Self {
            oldest_published_ns: retained_records.then_some(now),
            quality: if retained_records {
                AgeQuality::LowerBound
            } else {
                AgeQuality::Exact
            },
            first_after_capture_ns: None,
            sparse_at_capture: false,
            peaks: SuffixPeaks::default(),
        }
    }

    pub(super) fn sample(&self, now: u64, batches: u64, bytes: u64) -> SuffixSample {
        SuffixSample {
            at_ns: now,
            batches,
            bytes,
            oldest_age_ns: self
                .oldest_published_ns
                .map_or(0, |published| now.saturating_sub(published)),
            age_quality: self.quality,
        }
    }

    pub(super) fn observe(&mut self, now: u64, batches: u64, bytes: u64) -> SuffixSample {
        let sample = self.sample(now, batches, bytes);
        if sample.batches > self.peaks.batches.batches {
            self.peaks.batches = sample;
        }
        if sample.bytes > self.peaks.bytes.bytes {
            self.peaks.bytes = sample;
        }
        if sample.oldest_age_ns > self.peaks.oldest_age.oldest_age_ns {
            self.peaks.oldest_age = sample;
        }
        sample
    }

    pub(super) fn on_publish(&mut self, now: u64, tail: TailCount, charged_index_bytes: usize) {
        if self.oldest_published_ns.is_none() {
            self.oldest_published_ns = Some(now);
            self.quality = AgeQuality::Exact;
        }
        if self.first_after_capture_ns.is_none() {
            self.first_after_capture_ns = Some(now);
        }
        self.peaks.charged_index_bytes = self
            .peaks
            .charged_index_bytes
            .max(charged_index_bytes as u64);
        self.observe(now, tail.batches, tail.bytes);
    }

    fn on_capture(&mut self, sparse_beyond_cut: bool) {
        self.sparse_at_capture = sparse_beyond_cut;
        self.first_after_capture_ns = None;
    }

    fn on_install(&mut self, retained_records: bool) {
        if !retained_records {
            self.oldest_published_ns = None;
            self.quality = AgeQuality::Exact;
        } else if self.sparse_at_capture {
            // Keep the previous oldest time: it does not follow the carried records.
            self.quality = match self.quality {
                AgeQuality::Exact | AgeQuality::UpperBound => AgeQuality::UpperBound,
                AgeQuality::LowerBound | AgeQuality::Indeterminate => AgeQuality::Indeterminate,
            };
        } else if let Some(first) = self.first_after_capture_ns {
            self.oldest_published_ns = Some(first);
            self.quality = AgeQuality::Exact;
        }
    }

    pub(super) fn peaks(&self) -> SuffixPeaks {
        self.peaks
    }
}

/// Live state for a benchmark collector, read without I/O.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct IndexCheckpointSnapshot {
    pub at_ns: u64,
    pub armed: bool,
    pub open_id: u64,
    pub database: [u8; 16],
    pub fencing_generation: u64,
    /// Last launched checkpoint generation of this open.
    pub snapshot_generation: u64,
    pub active_worker: bool,
    pub ring_next_cursor: u64,
    pub ring_dropped_records: u64,
    pub suffix: SuffixSample,
    pub suffix_peaks: SuffixPeaks,
    /// Admission checkpoint charge (granted, includes the published suffix).
    pub granted: TailCount,
    pub granted_identities: u64,
    pub granted_peak: TailCount,
    pub charged_index_bytes: u64,
    pub hard_wait: super::concurrent::HardWaitSnapshot,
    pub coordinator_io: CoordinatorIoSnapshot,
}

impl WriteCore {
    /// Arm the bounded checkpoint ring and the suffix tracker independently of
    /// the request profile and timeline. Idempotent; never clears rows.
    pub(in crate::distributed::remote_db) fn arm_index_checkpoint_diagnostics(&self) {
        let now = diagnostics_clock_ns();
        let mut publisher = self.publisher.lock();
        if publisher.checkpoint_diagnostics.is_none() {
            publisher.checkpoint_diagnostics = Some(VecDeque::with_capacity(128));
        }
        publisher.checkpoint_diagnostic_summary.available = true;
        if publisher.index_suffix.is_none() {
            let retained = !publisher.known.is_empty();
            publisher.index_suffix = Some(SuffixTracker::armed(now, retained));
        }
    }

    pub(in crate::distributed::remote_db) fn index_checkpoint_snapshot(
        &self,
    ) -> IndexCheckpointSnapshot {
        // Separate short critical sections in the existing order; Admission is
        // never taken under publisher.
        let (granted, granted_peak) = self.admission.checkpoint_charge_and_peak();
        let hard_wait = self.admission.hard_wait_snapshot();
        let open_id = self.stats.lock().recovery.open_id;
        let charged_index_bytes = self.index.charged_metadata_bytes() as u64;
        let now = diagnostics_clock_ns();
        let mut publisher = self.publisher.lock();
        let snapshot_generation = publisher.snapshot_generation;
        let active_worker = publisher.background_checkpoint.is_some();
        let ring_next_cursor = publisher.next_checkpoint_diagnostic;
        let ring_dropped_records = publisher.dropped_checkpoint_diagnostics;
        let tail = TailCount::new(
            publisher.checkpoint_tail_batches,
            publisher.checkpoint_tail_bytes,
        );
        let (armed, suffix, suffix_peaks) = match publisher.index_suffix.as_mut() {
            Some(tracker) => {
                let sample = tracker.observe(now, tail.batches, tail.bytes);
                (true, sample, tracker.peaks())
            }
            None => (
                false,
                SuffixSample {
                    at_ns: now,
                    batches: tail.batches,
                    bytes: tail.bytes,
                    ..SuffixSample::default()
                },
                SuffixPeaks::default(),
            ),
        };
        drop(publisher);
        IndexCheckpointSnapshot {
            at_ns: now,
            armed,
            open_id,
            database: self.config.storage.database,
            fencing_generation: self.epoch,
            snapshot_generation,
            active_worker,
            ring_next_cursor,
            ring_dropped_records,
            suffix,
            suffix_peaks,
            granted: TailCount::new(granted.replay_batches, granted.replay_bytes),
            granted_identities: granted.identities as u64,
            granted_peak,
            charged_index_bytes,
            hard_wait,
            coordinator_io: coordinator_io_snapshot(),
        }
    }

    /// Wait for natural checkpoint work; with `force`, also adopt one more root
    /// marked `harness_drain` so it is never charged to steady state.
    pub(in crate::distributed::remote_db) fn drain_index_checkpoints(
        self: &Arc<Self>,
        force: bool,
    ) -> Result<(), LocalDbError> {
        self.progress_checkpoint(true)?;
        if force {
            self.commit_checkpoint_for(CheckpointTriggerFlags::HARNESS_DRAIN)?;
        }
        Ok(())
    }
}

impl RemoteDatabase {
    pub fn arm_index_checkpoint_diagnostics(&self) {
        self.core.arm_index_checkpoint_diagnostics();
    }

    pub fn index_checkpoint_snapshot(&self) -> IndexCheckpointSnapshot {
        self.core.index_checkpoint_snapshot()
    }

    pub fn drain_index_checkpoints(&self, force: bool) -> Result<(), LocalDbError> {
        self.core.drain_index_checkpoints(force)
    }
}

#[cfg(test)]
mod tests;
