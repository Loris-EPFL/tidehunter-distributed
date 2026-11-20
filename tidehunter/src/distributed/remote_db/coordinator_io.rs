//! Coordinator file I/O accounting and the diagnostics clock.
//!
//! Coordinator file I/O is a process-wide cumulative counter set (relaxed
//! atomics), so a benchmark can assign writes to its windows by the time they
//! happened. Nothing here takes a lock: callers pass values that they captured
//! under the locks they already hold, in the existing lock order.

use super::install_metadata;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

static DIAGNOSTICS_ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Nanoseconds since the process diagnostics origin. Window markers and
/// admission wait rows use this one monotonic clock, never owner clocks.
pub fn diagnostics_clock_ns() -> u64 {
    u64::try_from(DIAGNOSTICS_ORIGIN.elapsed().as_nanos()).unwrap_or(u64::MAX)
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

/// Coordinator files by category: the authority manifest (callback-start
/// adoption and open), the durable-cut record. The index WAL and control
/// region are the native stack's own files and are not counted here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CoordinatorIoCategory {
    /// Callback-start adoption: one manifest replacement.
    ManifestAdopt,
    /// Authority manifest writes during open.
    Open,
    /// `durable-cut.v1` record writes.
    DurableCut,
}

const CATEGORIES: usize = 3;

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

/// Cumulative coordinator file work of this process, by category.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinatorIoSnapshot {
    pub manifest_adopt: FileIo,
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
        for io in [self.manifest_adopt, self.open, self.durable_cut] {
            total.add(io);
        }
        total
    }
}

pub fn coordinator_io_snapshot() -> CoordinatorIoSnapshot {
    let load = |category: CoordinatorIoCategory| COORDINATOR_IO[category as usize].load();
    CoordinatorIoSnapshot {
        manifest_adopt: load(CoordinatorIoCategory::ManifestAdopt),
        open: load(CoordinatorIoCategory::Open),
        durable_cut: load(CoordinatorIoCategory::DurableCut),
        allocation_accounting: ALLOCATION_ACCOUNTING.load(Ordering::Relaxed),
        allocation_errors: ALLOCATION_ERRORS.load(Ordering::Relaxed),
    }
}
