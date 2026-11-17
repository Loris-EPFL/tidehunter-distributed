//! Option C aggregate counters (checkpoint option C spec §11.4).
//!
//! Low-cost, always-on counters of the cell flusher pool, the snapshot
//! (metadata) actor, Exact checkpoints, Admission stalls and mixed-cut
//! recovery, exported per flusher worker, per shard range and as totals.
//! Recording is a few relaxed atomic adds per cell, group, root or blocked
//! grant, plus two thread-CPU clock reads per cell append; nothing is
//! recorded per write. `/proc` is read only at snapshots and when a
//! maintenance thread ends.
//!
//! Thread CPU: a thread measures itself with `CLOCK_THREAD_CPUTIME_ID`
//! (exact). A snapshot reads other threads from `/proc/.../schedstat`, which
//! for a thread running on another CPU lags by at most one scheduler tick,
//! so window CPU carries at most one tick of error per running maintenance
//! thread at each boundary.
//!
//! # Ownership (share nothing)
//!
//! - [`WorkerCounters`] and [`StoreCounters`] of worker `w`: written only by
//!   flusher `w`.
//! - [`RangeCounters`] of range `w`, the scheduler counters and the pass
//!   gauges: written only by the scheduler actor.
//! - Root counters: written only by the snapshot/metadata actor.
//! - Exact, Admission and recovery counters: any thread, by atomic add.
//!
//! Every block is cache-line aligned so that two writers never share a line.
//! Every atomic uses `Relaxed`: these are independent statistics, nothing is
//! published through them, and a snapshot is not atomic across fields.
//!
//! # Export
//!
//! [`CellMetricsSnapshot::visit`] emits stable `&'static str` names:
//! - `cell_<field>` totals and `cell_w<N>_<field>` / `cell_r<N>_<field>` per
//!   worker or range: cumulative counters, monotone for the lifetime of one
//!   [`CellMetrics`], so a measurement window is the end minus the start.
//!   Names holding `_max` or `peak` are lifetime maxima, not deltas.
//! - `cell_gauge_<name>`: instantaneous values at the snapshot. A gauge
//!   whose source is unavailable is not emitted.
#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "option C counters, recorded by the flusher pool and read through the per-cell commit stage"
    )
)]

use super::cell_recovery::CellReplayStats;
pub(crate) use crate::distributed::native_index::thread_cpu_ns;
use crate::distributed::native_index::{
    CELL_FLUSH_STATE_BYTES, CellEncodeStats, IndexStoreSpace, IndexStoreWriterStats,
    MAX_CELL_FLUSHERS, PrefixCut,
};
use crate::distributed::types::BatchVersion;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-index name tables below have one row per possible flusher.
const INDEXED: usize = 8;
const _: () = assert!(MAX_CELL_FLUSHERS == INDEXED);

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn add(counter: &AtomicU64, value: u64) {
    counter.fetch_add(value, Ordering::Relaxed);
}

fn raise(counter: &AtomicU64, value: u64) {
    counter.fetch_max(value, Ordering::Relaxed);
}

macro_rules! merge {
    (sum, $a:expr, $b:expr) => {
        $a.saturating_add($b)
    };
    (max, $a:expr, $b:expr) => {
        $a.max($b)
    };
}

/// One block of counters: an atomic recording struct, a plain snapshot, and
/// the name table of its totals (`$total` prefix). The `indexed` form adds
/// one name row per worker or range.
macro_rules! counter_block {
    (
        $(#[$meta:meta])*
        $atomic:ident / $snapshot:ident;
        total $total:literal;
        $( $(#[$fmeta:meta])* $field:ident : $agg:ident ),+ $(,)?
    ) => {
        $(#[$meta])*
        #[derive(Default)]
        #[repr(align(64))]
        pub(crate) struct $atomic {
            $( $field: AtomicU64, )+
        }

        #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
        pub(crate) struct $snapshot {
            $( $(#[$fmeta])* pub $field: u64, )+
        }

        impl $atomic {
            fn load(&self) -> $snapshot {
                $snapshot { $( $field: self.$field.load(Ordering::Relaxed), )+ }
            }
        }

        impl $snapshot {
            const FIELDS: usize = [$( stringify!($field) ),+].len();
            const TOTAL_NAMES: [&'static str; $snapshot::FIELDS] =
                [$( concat!($total, stringify!($field)) ),+];

            fn values(&self) -> [u64; $snapshot::FIELDS] {
                [$( self.$field ),+]
            }
        }
    };
    (
        $(#[$meta:meta])*
        $atomic:ident / $snapshot:ident;
        total $total:literal;
        indexed [$p0:literal, $p1:literal, $p2:literal, $p3:literal,
                 $p4:literal, $p5:literal, $p6:literal, $p7:literal];
        $( $(#[$fmeta:meta])* $field:ident : $agg:ident ),+ $(,)?
    ) => {
        counter_block! {
            $(#[$meta])*
            $atomic / $snapshot;
            total $total;
            $( $(#[$fmeta])* $field : $agg ),+
        }

        impl $snapshot {
            const INDEXED_NAMES: [[&'static str; $snapshot::FIELDS]; INDEXED] = [
                [$( concat!($p0, stringify!($field)) ),+],
                [$( concat!($p1, stringify!($field)) ),+],
                [$( concat!($p2, stringify!($field)) ),+],
                [$( concat!($p3, stringify!($field)) ),+],
                [$( concat!($p4, stringify!($field)) ),+],
                [$( concat!($p5, stringify!($field)) ),+],
                [$( concat!($p6, stringify!($field)) ),+],
                [$( concat!($p7, stringify!($field)) ),+],
            ];

            /// Aggregate another worker or range: sums add, maxima take the
            /// larger value.
            fn merge(&mut self, other: &Self) {
                $( self.$field = merge!($agg, self.$field, other.$field); )+
            }
        }
    };
}

counter_block! {
    /// Events of one flusher worker. Written only by that worker.
    WorkerCounters / WorkerSnapshot;
    total "cell_";
    indexed ["cell_w0_", "cell_w1_", "cell_w2_", "cell_w3_",
             "cell_w4_", "cell_w5_", "cell_w6_", "cell_w7_"];
    /// Cells taken from the job queue, and their enqueue-to-dequeue time.
    dequeued: sum,
    queue_wait_ns: sum,
    queue_wait_max_ns: max,
    /// Captures, with the captured overlay versions and rows (the overlay
    /// references examined by the merge) and the versions above the target
    /// carried into the residual.
    captured: sum,
    captured_versions: sum,
    captured_rows: sum,
    residual_versions: sum,
    /// Clean advances (no I/O) and cells returned for lack of allowance.
    clean: sum,
    deferred: sum,
    /// Install outcomes: flush results installed, stale results, restores.
    installed: sum,
    stale: sum,
    restored: sum,
    /// Writer -> publisher holds of captures and group installs: time to
    /// acquire both guards, and time with the publisher held.
    capture_holds: sum,
    capture_wait_ns: sum,
    capture_hold_ns: sum,
    capture_hold_max_ns: max,
    install_holds: sum,
    install_wait_ns: sum,
    install_hold_ns: sum,
    install_hold_max_ns: max,
    /// Store appends (encode, CRC, BLAKE2b and buffered write of one cell)
    /// and their wall time.
    appends: sum,
    append_ns: sum,
    append_cpu_ns: sum,
    /// Group seals and their wall time (buffer flush, file and directory
    /// sync, mapping).
    seals: sum,
    seal_ns: sum,
}

counter_block! {
    /// Cumulative counters of one worker's index store and encoder, offset
    /// by the values present when the worker started. Written only by that
    /// worker.
    StoreCounters / StoreSnapshot;
    total "cell_";
    indexed ["cell_w0_", "cell_w1_", "cell_w2_", "cell_w3_",
             "cell_w4_", "cell_w5_", "cell_w6_", "cell_w7_"];
    /// Frames appended, framed bytes (headers included), emitted rows and
    /// retained versions.
    frames: sum,
    frame_bytes: sum,
    emitted_rows: sum,
    emitted_versions: sum,
    /// Encoder: cells without a retained row, buffered (encoded once) and
    /// streamed (legacy three-pass) cells, streamed cells whose payload fit
    /// the scratch (conservative fallbacks), canonical encoder invocations,
    /// merged row traversals, reference encodings, framed bytes per path,
    /// body-move bytes, payload scratch capacity.
    empty_cells: sum,
    buffered_cells: sum,
    streamed_cells: sum,
    conservative_fallbacks: sum,
    encoder_invocations: sum,
    row_passes: sum,
    references_encoded: sum,
    buffered_bytes: sum,
    streamed_bytes: sum,
    moved_bytes: sum,
    scratch_capacity_peak: max,
    /// Segment files: creations, sealed groups and bytes, syncs, mappings.
    segments_created: sum,
    groups_sealed: sum,
    sealed_bytes: sum,
    file_syncs: sum,
    directory_syncs: sum,
    sync_ns: sum,
    mappings: sum,
    /// Profile-on encoder phases ([`CellMetrics::set_profiling`]): cells
    /// timed and thread CPU per phase (see `CellEncodeStats`).
    profiled_cells: sum,
    encode_cpu_ns: sum,
    crc_cpu_ns: sum,
    blake2b_cpu_ns: sum,
    write_cpu_ns: sum,
    streamed_crc_pass_cpu_ns: sum,
    streamed_hash_pass_cpu_ns: sum,
}

counter_block! {
    /// Scheduler events of one shard range. Written only by the scheduler.
    RangeCounters / RangeSnapshot;
    total "cell_";
    indexed ["cell_r0_", "cell_r1_", "cell_r2_", "cell_r3_",
             "cell_r4_", "cell_r5_", "cell_r6_", "cell_r7_"];
    /// Cells sent to the worker, by selection reason: inside the fixed
    /// pressure (or requested) target, lagging the published prefix, aged,
    /// over a size threshold.
    selected_pressure: sum,
    selected_lagging: sum,
    selected_aged: sum,
    selected_size: sum,
    /// Sends that found the worker queue full.
    queue_full: sum,
    /// Cells read by scheduler passes, and those with an uncovered mutation.
    scanned_cells: sum,
    dirty_cells: sum,
}

counter_block! {
    /// Scheduler passes and fixed targets. Written only by the scheduler.
    SchedulerCounters / SchedulerSnapshot;
    total "cell_scheduler_";
    passes: sum,
    targets_started: sum,
    targets_covered: sum,
}

counter_block! {
    /// Metadata-only roots. Written only by the snapshot actor.
    RootCounters / RootSnapshot;
    total "cell_root_";
    captures: sum,
    captured_cells: sum,
    captured_files: sum,
    capture_ns: sum,
    commits: sum,
    root_bytes: sum,
    wrapper_bytes: sum,
    manifest_bytes: sum,
    owner_proofs: sum,
    proof_wait_ns: sum,
    commit_ns: sum,
    applies: sum,
    apply_ns: sum,
    /// Applies that advanced the replay floor F.
    f_advances: sum,
    returned_batches: sum,
    returned_encoded_bytes: sum,
    returned_identities: sum,
    /// Roots skipped because nothing changed.
    skipped_unchanged: sum,
    /// Cell frames encoded on the root path. Must stay zero: a root is
    /// metadata only.
    payload_cell_encodes: sum,
}

counter_block! {
    /// Exact-mode checkpoint image persistence, for the Exact and
    /// encode-only Exact arms.
    ExactCounters / ExactSnapshot;
    total "cell_exact_";
    checkpoints: sum,
    persist_ns: sum,
    /// CPU of the checkpoint threads at their end.
    thread_cpu_ns: sum,
    empty_cells: sum,
    buffered_cells: sum,
    streamed_cells: sum,
    conservative_fallbacks: sum,
    encoder_invocations: sum,
    row_passes: sum,
    references_encoded: sum,
    buffered_bytes: sum,
    streamed_bytes: sum,
    moved_bytes: sum,
    scratch_capacity_peak: max,
    /// Profile-on encoder phases of the persist encoder, as for the
    /// flushers' store counters.
    profiled_cells: sum,
    encode_cpu_ns: sum,
    crc_cpu_ns: sum,
    blake2b_cpu_ns: sum,
    write_cpu_ns: sum,
    streamed_crc_pass_cpu_ns: sum,
    streamed_hash_pass_cpu_ns: sum,
}

counter_block! {
    /// Admission grants blocked by a checkpoint-bound dimension, and the
    /// time each waited.
    AdmissionCounters / AdmissionSnapshot;
    total "cell_admission_";
    blocked_replay_tail: sum,
    blocked_replay_tail_ns: sum,
    blocked_index_bytes: sum,
    blocked_index_bytes_ns: sum,
    blocked_identities: sum,
    blocked_identities_ns: sum,
    blocked_checkpoint_metadata: sum,
    blocked_checkpoint_metadata_ns: sum,
    blocked_other: sum,
    blocked_other_ns: sum,
}

counter_block! {
    /// Mixed-cut recoveries.
    RecoveryCounters / RecoverySnapshot;
    total "cell_recovery_";
    runs: sum,
    batches: sum,
    /// Encoded bytes of the authenticated whole batches above F.
    encoded_bytes: sum,
    fully_covered_batches: sum,
    skipped_batches: sum,
    applied_members: sum,
    covered_members: sum,
    offline_flush_passes: sum,
    offline_flushed_cells: sum,
    peak_resident_bytes: max,
    elapsed_ns: sum,
}

/// Why the scheduler sent a cell (spec §6.1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SelectionReason {
    /// The oldest uncovered batch is inside the fixed pressure or requested
    /// target.
    Pressure,
    /// The oldest uncovered batch lags the published prefix by a quarter of
    /// a replay-tail bound.
    Lagging,
    Aged,
    Size,
}

/// The Admission dimension that blocked a grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BlockedDimension {
    ReplayTail,
    IndexBytes,
    Identities,
    CheckpointMetadata,
    Other,
}

impl RangeCounters {
    pub(crate) fn scanned(&self, cells: usize, dirty: usize) {
        add(&self.scanned_cells, cells as u64);
        add(&self.dirty_cells, dirty as u64);
    }

    pub(crate) fn selected(&self, reason: SelectionReason) {
        add(
            match reason {
                SelectionReason::Pressure => &self.selected_pressure,
                SelectionReason::Lagging => &self.selected_lagging,
                SelectionReason::Aged => &self.selected_aged,
                SelectionReason::Size => &self.selected_size,
            },
            1,
        );
    }

    pub(crate) fn queue_full(&self) {
        add(&self.queue_full, 1);
    }
}

impl RangeSnapshot {
    /// Cells sent to the worker queue: the sum of the selection reasons.
    pub(crate) fn dispatched(&self) -> u64 {
        self.selected_pressure
            .saturating_add(self.selected_lagging)
            .saturating_add(self.selected_aged)
            .saturating_add(self.selected_size)
    }
}

impl WorkerCounters {
    pub(crate) fn dequeued(&self, wait: Duration) {
        let wait = nanos(wait);
        add(&self.dequeued, 1);
        add(&self.queue_wait_ns, wait);
        raise(&self.queue_wait_max_ns, wait);
    }

    pub(crate) fn captured(&self, versions: usize, rows: usize, residual_versions: usize) {
        add(&self.captured, 1);
        add(&self.captured_versions, versions as u64);
        add(&self.captured_rows, rows as u64);
        add(&self.residual_versions, residual_versions as u64);
    }

    pub(crate) fn clean(&self) {
        add(&self.clean, 1);
    }

    pub(crate) fn deferred(&self) {
        add(&self.deferred, 1);
    }

    pub(crate) fn capture_hold(&self, wait: Duration, hold: Duration) {
        let hold = nanos(hold);
        add(&self.capture_holds, 1);
        add(&self.capture_wait_ns, nanos(wait));
        add(&self.capture_hold_ns, hold);
        raise(&self.capture_hold_max_ns, hold);
    }

    /// One group install hold with its outcomes.
    pub(crate) fn installed(
        &self,
        installed: usize,
        stale: usize,
        restored: usize,
        wait: Duration,
        hold: Duration,
    ) {
        let hold = nanos(hold);
        add(&self.installed, installed as u64);
        add(&self.stale, stale as u64);
        add(&self.restored, restored as u64);
        add(&self.install_holds, 1);
        add(&self.install_wait_ns, nanos(wait));
        add(&self.install_hold_ns, hold);
        raise(&self.install_hold_max_ns, hold);
    }

    /// One store append: wall time and this thread's CPU time.
    pub(crate) fn appended(&self, wall: Duration, cpu_ns: u64) {
        add(&self.appends, 1);
        add(&self.append_ns, nanos(wall));
        add(&self.append_cpu_ns, cpu_ns);
    }

    pub(crate) fn sealed(&self, wall: Duration) {
        add(&self.seals, 1);
        add(&self.seal_ns, nanos(wall));
    }
}

impl StoreSnapshot {
    fn from_store(stats: &IndexStoreWriterStats) -> Self {
        let encode = &stats.encode;
        Self {
            frames: stats.frames,
            frame_bytes: stats.frame_bytes,
            emitted_rows: stats.rows,
            emitted_versions: stats.versions,
            empty_cells: encode.empty_cells,
            buffered_cells: encode.buffered_cells,
            streamed_cells: encode.streamed_cells,
            conservative_fallbacks: encode.conservative_fallbacks,
            encoder_invocations: encode.encode_passes,
            row_passes: encode.row_passes,
            references_encoded: encode.references_encoded,
            buffered_bytes: encode.buffered_bytes,
            streamed_bytes: encode.streamed_bytes,
            moved_bytes: encode.moved_bytes,
            scratch_capacity_peak: encode.scratch_capacity,
            segments_created: stats.segments_created,
            groups_sealed: stats.groups_sealed,
            sealed_bytes: stats.sealed_bytes,
            file_syncs: stats.file_syncs,
            directory_syncs: stats.directory_syncs,
            sync_ns: stats.sync_ns,
            mappings: stats.mappings,
            profiled_cells: encode.profiled_cells,
            encode_cpu_ns: encode.encode_cpu_ns,
            crc_cpu_ns: encode.crc_cpu_ns,
            blake2b_cpu_ns: encode.blake2b_cpu_ns,
            write_cpu_ns: encode.write_cpu_ns,
            streamed_crc_pass_cpu_ns: encode.streamed_crc_pass_cpu_ns,
            streamed_hash_pass_cpu_ns: encode.streamed_hash_pass_cpu_ns,
        }
    }

    /// Framed bytes produced by canonical encodings: once per buffered
    /// frame, twice per streamed frame.
    fn encode_bytes(&self) -> u64 {
        self.buffered_bytes
            .saturating_add(self.streamed_bytes.saturating_mul(2))
    }

    /// CRC32 covers each payload once (frame minus its 8-byte header).
    fn crc_bytes(&self) -> u64 {
        self.frame_bytes
            .saturating_sub(self.frames.saturating_mul(8))
    }
}

impl StoreCounters {
    /// Value of the store counters when a worker starts; the worker adds its
    /// writer's cumulative counters to it.
    pub(crate) fn base(&self) -> StoreSnapshot {
        self.load()
    }

    /// Publish `base` plus the writer's cumulative counters. One writer per
    /// block, so plain stores keep the values monotone.
    pub(crate) fn publish(&self, base: &StoreSnapshot, stats: &IndexStoreWriterStats) {
        let mut value = *base;
        value.merge(&StoreSnapshot::from_store(stats));
        let StoreSnapshot {
            frames,
            frame_bytes,
            emitted_rows,
            emitted_versions,
            empty_cells,
            buffered_cells,
            streamed_cells,
            conservative_fallbacks,
            encoder_invocations,
            row_passes,
            references_encoded,
            buffered_bytes,
            streamed_bytes,
            moved_bytes,
            scratch_capacity_peak,
            segments_created,
            groups_sealed,
            sealed_bytes,
            file_syncs,
            directory_syncs,
            sync_ns,
            mappings,
            profiled_cells,
            encode_cpu_ns,
            crc_cpu_ns,
            blake2b_cpu_ns,
            write_cpu_ns,
            streamed_crc_pass_cpu_ns,
            streamed_hash_pass_cpu_ns,
        } = value;
        for (counter, value) in [
            (&self.frames, frames),
            (&self.frame_bytes, frame_bytes),
            (&self.emitted_rows, emitted_rows),
            (&self.emitted_versions, emitted_versions),
            (&self.empty_cells, empty_cells),
            (&self.buffered_cells, buffered_cells),
            (&self.streamed_cells, streamed_cells),
            (&self.conservative_fallbacks, conservative_fallbacks),
            (&self.encoder_invocations, encoder_invocations),
            (&self.row_passes, row_passes),
            (&self.references_encoded, references_encoded),
            (&self.buffered_bytes, buffered_bytes),
            (&self.streamed_bytes, streamed_bytes),
            (&self.moved_bytes, moved_bytes),
            (&self.scratch_capacity_peak, scratch_capacity_peak),
            (&self.segments_created, segments_created),
            (&self.groups_sealed, groups_sealed),
            (&self.sealed_bytes, sealed_bytes),
            (&self.file_syncs, file_syncs),
            (&self.directory_syncs, directory_syncs),
            (&self.sync_ns, sync_ns),
            (&self.mappings, mappings),
            (&self.profiled_cells, profiled_cells),
            (&self.encode_cpu_ns, encode_cpu_ns),
            (&self.crc_cpu_ns, crc_cpu_ns),
            (&self.blake2b_cpu_ns, blake2b_cpu_ns),
            (&self.write_cpu_ns, write_cpu_ns),
            (&self.streamed_crc_pass_cpu_ns, streamed_crc_pass_cpu_ns),
            (&self.streamed_hash_pass_cpu_ns, streamed_hash_pass_cpu_ns),
        ] {
            counter.store(value, Ordering::Relaxed);
        }
    }
}

impl ExactSnapshot {
    fn encode_bytes(&self) -> u64 {
        self.buffered_bytes
            .saturating_add(self.streamed_bytes.saturating_mul(2))
    }

    fn frame_bytes(&self) -> u64 {
        self.buffered_bytes.saturating_add(self.streamed_bytes)
    }

    /// Frames are the non-empty buffered and streamed cells.
    fn crc_bytes(&self) -> u64 {
        let frames = self.buffered_cells.saturating_add(self.streamed_cells);
        self.frame_bytes().saturating_sub(frames.saturating_mul(8))
    }
}

/// CPU time and page faults of one thread (`/proc`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ThreadUsage {
    pub cpu_ns: u64,
    pub minor_faults: u64,
    pub major_faults: u64,
}

impl ThreadUsage {
    fn plus(self, other: Self) -> Self {
        Self {
            cpu_ns: self.cpu_ns.saturating_add(other.cpu_ns),
            minor_faults: self.minor_faults.saturating_add(other.minor_faults),
            major_faults: self.major_faults.saturating_add(other.major_faults),
        }
    }

    fn max(self, other: Self) -> Self {
        Self {
            cpu_ns: self.cpu_ns.max(other.cpu_ns),
            minor_faults: self.minor_faults.max(other.minor_faults),
            major_faults: self.major_faults.max(other.major_faults),
        }
    }
}

/// `/proc/self/task/<tid>/schedstat` runtime in nanoseconds.
fn parse_schedstat(text: &str) -> Option<u64> {
    text.split_ascii_whitespace().next()?.parse().ok()
}

/// Minor and major faults from `/proc/.../stat` (fields 10 and 12). The
/// command name can hold spaces and parentheses, so fields are counted
/// after its last `)`.
fn parse_stat_faults(text: &str) -> Option<(u64, u64)> {
    let mut fields = text.get(text.rfind(')')? + 1..)?.split_ascii_whitespace();
    // The remainder starts at field 3 (state).
    let minor = fields.nth(7)?.parse().ok()?;
    let major = fields.nth(1)?.parse().ok()?;
    Some((minor, major))
}

fn read_usage(directory: &str) -> Option<ThreadUsage> {
    let cpu_ns = parse_schedstat(&std::fs::read_to_string(format!("{directory}/schedstat")).ok()?)?;
    let (minor_faults, major_faults) =
        parse_stat_faults(&std::fs::read_to_string(format!("{directory}/stat")).ok()?)?;
    Some(ThreadUsage {
        cpu_ns,
        minor_faults,
        major_faults,
    })
}

/// Usage of the calling thread: exact CPU time, faults from `/proc`.
pub(crate) fn current_thread_usage() -> Option<ThreadUsage> {
    let cpu_ns = thread_cpu_ns()?;
    let (minor_faults, major_faults) =
        parse_stat_faults(&std::fs::read_to_string("/proc/thread-self/stat").ok()?)?;
    Some(ThreadUsage {
        cpu_ns,
        minor_faults,
        major_faults,
    })
}

/// Whether a snapshot can read other threads' CPU time.
fn thread_cpu_available() -> bool {
    std::fs::metadata("/proc/thread-self/schedstat").is_ok()
}

/// Kernel thread id of the calling thread: `/proc/thread-self` links to
/// `<pid>/task/<tid>`.
fn current_tid() -> Option<u64> {
    std::fs::read_link("/proc/thread-self")
        .ok()?
        .file_name()?
        .to_str()?
        .parse()
        .ok()
}

#[derive(Default)]
struct SlotState {
    /// Running registered thread, if any.
    tid: Option<u64>,
    /// Usage of the registered threads that finished.
    finished: ThreadUsage,
}

/// CPU accounting of one maintenance role (a flusher, the scheduler, the
/// snapshot actor). The role's thread registers itself at start and
/// finishes before it exits. The leaf mutex only copies or updates scalars:
/// `/proc` is read outside it.
#[derive(Default)]
struct ThreadSlot {
    state: Mutex<SlotState>,
    /// Largest value reported, so a read that races a thread's exit never
    /// moves a reported value backwards.
    reported: Mutex<ThreadUsage>,
}

impl ThreadSlot {
    fn register(&self) {
        self.state.lock().tid = current_tid();
    }

    fn finish(&self) {
        let usage = current_thread_usage().unwrap_or_default();
        let mut state = self.state.lock();
        state.finished = state.finished.plus(usage);
        state.tid = None;
    }

    fn read(&self) -> ThreadUsage {
        let (tid, finished) = {
            let state = self.state.lock();
            (state.tid, state.finished)
        };
        // A thread that exits between the copy and this read reports only
        // its finished predecessors here; `reported` keeps the value
        // monotone, and the next read includes its final usage.
        let live = tid
            .and_then(|tid| read_usage(&format!("/proc/self/task/{tid}")))
            .unwrap_or_default();
        let mut reported = self.reported.lock();
        *reported = reported.max(finished.plus(live));
        *reported
    }
}

#[derive(Clone, Copy, Debug)]
enum Role {
    Worker(usize),
    Scheduler,
    Snapshot,
}

/// Accounts the current thread's CPU to its maintenance role until dropped.
/// Declare it at the top of the thread body.
pub(crate) struct RoleGuard {
    metrics: Arc<CellMetrics>,
    role: Role,
}

impl Drop for RoleGuard {
    fn drop(&mut self) {
        self.metrics.slot(self.role).finish();
    }
}

/// Scheduler state of its last pass. Written only by the scheduler.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PassGauges {
    /// The fixed pressure or requested target being drained, if any.
    pub target: Option<PrefixCut>,
    /// Newest target with no cell missing a mutation at or below it.
    pub covered: Option<PrefixCut>,
    /// Oldest uncovered batch over every cell, and when the oldest
    /// unrepresented reference arrived.
    pub oldest_uncovered: Option<BatchVersion>,
    pub oldest_dirty_since: Option<Instant>,
    /// Cells with an uncovered mutation.
    pub dirty_cells: u64,
}

/// Cuts of the adopted root (`AdoptedCuts` of the commit stage).
#[derive(Clone, Copy, Debug)]
pub(crate) struct AdoptedGauges {
    pub generation: u64,
    /// Directory-capture target `T`.
    pub target: PrefixCut,
    /// Replay floor `F`.
    pub floor: PrefixCut,
    /// Maximum referenced frame cut `U`.
    pub max_frame_cut: PrefixCut,
    /// History floor `H`, at batch granularity.
    pub history_floor: PrefixCut,
}

/// Gauges the caller samples at the snapshot from state it owns.
#[derive(Clone, Debug, Default)]
pub(crate) struct CellGauges {
    /// Published scalar prefix `L` (`NativeIndex::published_prefix`).
    pub published: Option<PrefixCut>,
    pub adopted: Option<AdoptedGauges>,
    /// Resident index charge (`NativeIndex::charged_metadata_bytes`).
    pub resident_index_bytes: Option<u64>,
    /// Admitted unpublished index reservations (Admission).
    pub admitted_index_bytes: Option<u64>,
    /// Transient index charge (captures, residuals, views).
    pub transient_index_bytes: Option<u64>,
    /// Sealed-segment space (`IndexStore::space`).
    pub segments: Option<IndexStoreSpace>,
}

/// All option C counters of one database open.
pub(crate) struct CellMetrics {
    workers: usize,
    worker: [WorkerCounters; INDEXED],
    store: [StoreCounters; INDEXED],
    range: [RangeCounters; INDEXED],
    worker_threads: [ThreadSlot; INDEXED],
    scheduler_thread: ThreadSlot,
    snapshot_thread: ThreadSlot,
    scheduler: SchedulerCounters,
    pass: Mutex<PassGauges>,
    root: RootCounters,
    exact: ExactCounters,
    admission: AdmissionCounters,
    recovery: RecoveryCounters,
    /// Profile-on encoder phase timing. Relaxed: workers pick a change up
    /// at their next append; nothing else is ordered by it.
    profiling: AtomicBool,
}

impl CellMetrics {
    /// Counters for `workers` flushers (clamped to `1..=MAX_CELL_FLUSHERS`).
    pub(crate) fn new(workers: usize) -> Arc<Self> {
        Arc::new(Self {
            workers: workers.clamp(1, INDEXED),
            worker: Default::default(),
            store: Default::default(),
            range: Default::default(),
            worker_threads: Default::default(),
            scheduler_thread: ThreadSlot::default(),
            snapshot_thread: ThreadSlot::default(),
            scheduler: SchedulerCounters::default(),
            pass: Mutex::new(PassGauges::default()),
            root: RootCounters::default(),
            exact: ExactCounters::default(),
            admission: AdmissionCounters::default(),
            recovery: RecoveryCounters::default(),
            profiling: AtomicBool::new(false),
        })
    }

    pub(crate) fn workers(&self) -> usize {
        self.workers
    }

    /// Worker `w`'s counters. `w` is below the pool size, which is at most
    /// `MAX_CELL_FLUSHERS`; a larger index shares the last row.
    pub(crate) fn worker(&self, worker: usize) -> &WorkerCounters {
        &self.worker[worker.min(INDEXED - 1)]
    }

    pub(crate) fn store(&self, worker: usize) -> &StoreCounters {
        &self.store[worker.min(INDEXED - 1)]
    }

    pub(crate) fn range(&self, worker: usize) -> &RangeCounters {
        &self.range[worker.min(INDEXED - 1)]
    }

    /// Worker event totals, without reading thread usage.
    pub(crate) fn worker_totals(&self) -> WorkerSnapshot {
        let mut total = WorkerSnapshot::default();
        self.worker[..self.workers]
            .iter()
            .for_each(|worker| total.merge(&worker.load()));
        total
    }

    /// Scheduler range totals.
    pub(crate) fn range_totals(&self) -> RangeSnapshot {
        let mut total = RangeSnapshot::default();
        self.range[..self.workers]
            .iter()
            .for_each(|range| total.merge(&range.load()));
        total
    }

    /// Account the calling thread as flusher `worker` until the guard drops
    /// (also on unwind).
    pub(crate) fn enter_worker(self: &Arc<Self>, worker: usize) -> RoleGuard {
        self.enter(Role::Worker(worker.min(INDEXED - 1)))
    }

    pub(crate) fn enter_scheduler(self: &Arc<Self>) -> RoleGuard {
        self.enter(Role::Scheduler)
    }

    /// For the snapshot (metadata) actor thread.
    pub(crate) fn enter_snapshot(self: &Arc<Self>) -> RoleGuard {
        self.enter(Role::Snapshot)
    }

    fn enter(self: &Arc<Self>, role: Role) -> RoleGuard {
        self.slot(role).register();
        RoleGuard {
            metrics: Arc::clone(self),
            role,
        }
    }

    fn slot(&self, role: Role) -> &ThreadSlot {
        match role {
            Role::Worker(worker) => &self.worker_threads[worker],
            Role::Scheduler => &self.scheduler_thread,
            Role::Snapshot => &self.snapshot_thread,
        }
    }

    /// Arm or disarm per-phase encoder timing (measurement profiles only;
    /// off costs one branch per encoder phase).
    pub(crate) fn set_profiling(&self, on: bool) {
        self.profiling.store(on, Ordering::Relaxed);
    }

    pub(crate) fn profiling(&self) -> bool {
        self.profiling.load(Ordering::Relaxed)
    }

    /// End of one scheduler pass. `target_started`/`target_covered` count a
    /// fixed target chosen or proved covered in this pass.
    pub(crate) fn scheduler_pass(
        &self,
        gauges: PassGauges,
        target_started: bool,
        target_covered: bool,
    ) {
        add(&self.scheduler.passes, 1);
        add(&self.scheduler.targets_started, u64::from(target_started));
        add(&self.scheduler.targets_covered, u64::from(target_covered));
        *self.pass.lock() = gauges;
    }

    pub(crate) fn root_captured(&self, cells: u64, files: u64, capture_ns: u64) {
        add(&self.root.captures, 1);
        add(&self.root.captured_cells, cells);
        add(&self.root.captured_files, files);
        add(&self.root.capture_ns, capture_ns);
    }

    pub(crate) fn root_committed(
        &self,
        root_bytes: u64,
        wrapper_bytes: u64,
        manifest_bytes: u64,
        owner_proofs: u64,
        proof_wait_ns: u64,
        commit_ns: u64,
    ) {
        add(&self.root.commits, 1);
        add(&self.root.root_bytes, root_bytes);
        add(&self.root.wrapper_bytes, wrapper_bytes);
        add(&self.root.manifest_bytes, manifest_bytes);
        add(&self.root.owner_proofs, owner_proofs);
        add(&self.root.proof_wait_ns, proof_wait_ns);
        add(&self.root.commit_ns, commit_ns);
    }

    pub(crate) fn root_applied(
        &self,
        apply_ns: u64,
        returned_batches: u64,
        returned_encoded_bytes: u64,
        returned_identities: u64,
        f_advanced: bool,
    ) {
        add(&self.root.applies, 1);
        add(&self.root.apply_ns, apply_ns);
        add(&self.root.returned_batches, returned_batches);
        add(&self.root.returned_encoded_bytes, returned_encoded_bytes);
        add(&self.root.returned_identities, returned_identities);
        add(&self.root.f_advances, u64::from(f_advanced));
    }

    pub(crate) fn root_skipped_unchanged(&self) {
        add(&self.root.skipped_unchanged, 1);
    }

    /// Cell frames encoded while building or committing a root.
    pub(crate) fn root_payload_cell_encodes(&self, frames: u64) {
        add(&self.root.payload_cell_encodes, frames);
    }

    /// One persisted Exact checkpoint image (`PersistedIndexImage`) and its
    /// persistence wall time.
    pub(crate) fn exact_checkpoint(&self, encode: &CellEncodeStats, persist: Duration) {
        let exact = &self.exact;
        add(&exact.checkpoints, 1);
        add(&exact.persist_ns, nanos(persist));
        add(&exact.empty_cells, encode.empty_cells);
        add(&exact.buffered_cells, encode.buffered_cells);
        add(&exact.streamed_cells, encode.streamed_cells);
        add(&exact.conservative_fallbacks, encode.conservative_fallbacks);
        add(&exact.encoder_invocations, encode.encode_passes);
        add(&exact.row_passes, encode.row_passes);
        add(&exact.references_encoded, encode.references_encoded);
        add(&exact.buffered_bytes, encode.buffered_bytes);
        add(&exact.streamed_bytes, encode.streamed_bytes);
        add(&exact.moved_bytes, encode.moved_bytes);
        raise(&exact.scratch_capacity_peak, encode.scratch_capacity);
        add(&exact.profiled_cells, encode.profiled_cells);
        add(&exact.encode_cpu_ns, encode.encode_cpu_ns);
        add(&exact.crc_cpu_ns, encode.crc_cpu_ns);
        add(&exact.blake2b_cpu_ns, encode.blake2b_cpu_ns);
        add(&exact.write_cpu_ns, encode.write_cpu_ns);
        add(
            &exact.streamed_crc_pass_cpu_ns,
            encode.streamed_crc_pass_cpu_ns,
        );
        add(
            &exact.streamed_hash_pass_cpu_ns,
            encode.streamed_hash_pass_cpu_ns,
        );
    }

    /// Called last on an Exact checkpoint thread (one thread per checkpoint):
    /// its whole CPU time is that checkpoint's maintenance CPU.
    pub(crate) fn exact_checkpoint_thread_exiting(&self) {
        if let Some(cpu) = thread_cpu_ns() {
            self.exact_thread_cpu(cpu);
        }
    }

    fn exact_thread_cpu(&self, cpu_ns: u64) {
        add(&self.exact.thread_cpu_ns, cpu_ns);
    }

    /// One Admission grant that waited on `dimension`. Call after the wait,
    /// outside the Admission mutex.
    pub(crate) fn admission_blocked(&self, dimension: BlockedDimension, wait: Duration) {
        let admission = &self.admission;
        let (count, total) = match dimension {
            BlockedDimension::ReplayTail => (
                &admission.blocked_replay_tail,
                &admission.blocked_replay_tail_ns,
            ),
            BlockedDimension::IndexBytes => (
                &admission.blocked_index_bytes,
                &admission.blocked_index_bytes_ns,
            ),
            BlockedDimension::Identities => (
                &admission.blocked_identities,
                &admission.blocked_identities_ns,
            ),
            BlockedDimension::CheckpointMetadata => (
                &admission.blocked_checkpoint_metadata,
                &admission.blocked_checkpoint_metadata_ns,
            ),
            BlockedDimension::Other => (&admission.blocked_other, &admission.blocked_other_ns),
        };
        add(count, 1);
        add(total, nanos(wait));
    }

    /// One completed mixed-cut recovery.
    pub(crate) fn recovery(&self, stats: &CellReplayStats, elapsed: Duration) {
        let recovery = &self.recovery;
        add(&recovery.runs, 1);
        add(&recovery.batches, stats.batches);
        add(&recovery.encoded_bytes, stats.encoded_bytes);
        add(&recovery.fully_covered_batches, stats.fully_covered_batches);
        add(&recovery.skipped_batches, stats.skipped_batches);
        add(&recovery.applied_members, stats.applied_members);
        add(&recovery.covered_members, stats.covered_members);
        add(&recovery.offline_flush_passes, stats.offline_flush_passes);
        add(&recovery.offline_flushed_cells, stats.offline_flushed_cells);
        raise(
            &recovery.peak_resident_bytes,
            stats.peak_resident_bytes as u64,
        );
        add(&recovery.elapsed_ns, nanos(elapsed));
    }

    /// Copy every counter and read the maintenance threads' usage. Not
    /// atomic across fields. Reads `/proc` for each registered thread: call
    /// at measurement boundaries, not per request.
    pub(crate) fn snapshot(&self, gauges: CellGauges) -> CellMetricsSnapshot {
        let workers = self.workers;
        let mut snapshot = CellMetricsSnapshot {
            workers,
            worker: [WorkerSnapshot::default(); INDEXED],
            store: [StoreSnapshot::default(); INDEXED],
            range: [RangeSnapshot::default(); INDEXED],
            worker_threads: [ThreadUsage::default(); INDEXED],
            scheduler_thread: self.scheduler_thread.read(),
            snapshot_thread: self.snapshot_thread.read(),
            scheduler: self.scheduler.load(),
            pass: *self.pass.lock(),
            root: self.root.load(),
            exact: self.exact.load(),
            admission: self.admission.load(),
            recovery: self.recovery.load(),
            thread_cpu_available: thread_cpu_available(),
            taken: Instant::now(),
            gauges,
        };
        for worker in 0..workers {
            self.load_row(worker, &mut snapshot);
        }
        snapshot
    }

    fn load_row(&self, worker: usize, snapshot: &mut CellMetricsSnapshot) {
        snapshot.worker[worker] = self.worker[worker].load();
        snapshot.store[worker] = self.store[worker].load();
        snapshot.range[worker] = self.range[worker].load();
        snapshot.worker_threads[worker] = self.worker_threads[worker].read();
    }
}

/// A copy of every counter plus the caller's gauges.
#[derive(Clone, Debug)]
pub(crate) struct CellMetricsSnapshot {
    pub workers: usize,
    pub worker: [WorkerSnapshot; INDEXED],
    pub store: [StoreSnapshot; INDEXED],
    pub range: [RangeSnapshot; INDEXED],
    pub worker_threads: [ThreadUsage; INDEXED],
    pub scheduler_thread: ThreadUsage,
    pub snapshot_thread: ThreadUsage,
    pub scheduler: SchedulerSnapshot,
    pub pass: PassGauges,
    pub root: RootSnapshot,
    pub exact: ExactSnapshot,
    pub admission: AdmissionSnapshot,
    pub recovery: RecoverySnapshot,
    pub thread_cpu_available: bool,
    pub taken: Instant,
    pub gauges: CellGauges,
}

const THREAD_NAMES: [[&str; 3]; INDEXED] = [
    [
        "cell_w0_thread_cpu_ns",
        "cell_w0_thread_minor_faults",
        "cell_w0_thread_major_faults",
    ],
    [
        "cell_w1_thread_cpu_ns",
        "cell_w1_thread_minor_faults",
        "cell_w1_thread_major_faults",
    ],
    [
        "cell_w2_thread_cpu_ns",
        "cell_w2_thread_minor_faults",
        "cell_w2_thread_major_faults",
    ],
    [
        "cell_w3_thread_cpu_ns",
        "cell_w3_thread_minor_faults",
        "cell_w3_thread_major_faults",
    ],
    [
        "cell_w4_thread_cpu_ns",
        "cell_w4_thread_minor_faults",
        "cell_w4_thread_major_faults",
    ],
    [
        "cell_w5_thread_cpu_ns",
        "cell_w5_thread_minor_faults",
        "cell_w5_thread_major_faults",
    ],
    [
        "cell_w6_thread_cpu_ns",
        "cell_w6_thread_minor_faults",
        "cell_w6_thread_major_faults",
    ],
    [
        "cell_w7_thread_cpu_ns",
        "cell_w7_thread_minor_faults",
        "cell_w7_thread_major_faults",
    ],
];
const WORKER_THREAD_NAMES: [&str; 3] = [
    "cell_worker_thread_cpu_ns",
    "cell_worker_thread_minor_faults",
    "cell_worker_thread_major_faults",
];
const SCHEDULER_THREAD_NAMES: [&str; 3] = [
    "cell_scheduler_thread_cpu_ns",
    "cell_scheduler_thread_minor_faults",
    "cell_scheduler_thread_major_faults",
];
const SNAPSHOT_THREAD_NAMES: [&str; 3] = [
    "cell_snapshot_thread_cpu_ns",
    "cell_snapshot_thread_minor_faults",
    "cell_snapshot_thread_major_faults",
];
const QUEUE_DEPTH_NAMES: [&str; INDEXED] = [
    "cell_gauge_r0_queue_depth",
    "cell_gauge_r1_queue_depth",
    "cell_gauge_r2_queue_depth",
    "cell_gauge_r3_queue_depth",
    "cell_gauge_r4_queue_depth",
    "cell_gauge_r5_queue_depth",
    "cell_gauge_r6_queue_depth",
    "cell_gauge_r7_queue_depth",
];
const IN_FLIGHT_NAMES: [&str; INDEXED] = [
    "cell_gauge_w0_in_flight",
    "cell_gauge_w1_in_flight",
    "cell_gauge_w2_in_flight",
    "cell_gauge_w3_in_flight",
    "cell_gauge_w4_in_flight",
    "cell_gauge_w5_in_flight",
    "cell_gauge_w6_in_flight",
    "cell_gauge_w7_in_flight",
];

macro_rules! cut_names {
    ($name:literal) => {
        [
            concat!("cell_gauge_", $name, "_present"),
            concat!("cell_gauge_", $name, "_epoch"),
            concat!("cell_gauge_", $name, "_sequence"),
        ]
    };
}

fn emit_usage(
    set: &mut impl FnMut(&'static str, u64),
    names: &[&'static str; 3],
    usage: ThreadUsage,
) {
    set(names[0], usage.cpu_ns);
    set(names[1], usage.minor_faults);
    set(names[2], usage.major_faults);
}

fn emit_cut(set: &mut impl FnMut(&'static str, u64), names: [&'static str; 3], cut: PrefixCut) {
    match cut {
        PrefixCut::BeforeAll => {
            set(names[0], 0);
            set(names[1], 0);
            set(names[2], 0);
        }
        PrefixCut::Through(version) => {
            set(names[0], 1);
            set(names[1], version.epoch);
            set(names[2], version.sequence);
        }
    }
}

/// Batch-sequence distance `newer - older` within one epoch. `None` across
/// epochs or when a cut is `BeforeAll`. Sequence gaps of aborted slots count.
fn lag(newer: PrefixCut, older: PrefixCut) -> Option<u64> {
    match (newer, older) {
        (PrefixCut::Through(newer), PrefixCut::Through(older)) if newer.epoch == older.epoch => {
            Some(newer.sequence.saturating_sub(older.sequence))
        }
        _ => None,
    }
}

impl CellMetricsSnapshot {
    pub(crate) fn worker_totals(&self) -> WorkerSnapshot {
        let mut total = WorkerSnapshot::default();
        self.worker[..self.workers]
            .iter()
            .for_each(|worker| total.merge(worker));
        total
    }

    pub(crate) fn store_totals(&self) -> StoreSnapshot {
        let mut total = StoreSnapshot::default();
        self.store[..self.workers]
            .iter()
            .for_each(|store| total.merge(store));
        total
    }

    pub(crate) fn range_totals(&self) -> RangeSnapshot {
        let mut total = RangeSnapshot::default();
        self.range[..self.workers]
            .iter()
            .for_each(|range| total.merge(range));
        total
    }

    pub(crate) fn worker_thread_totals(&self) -> ThreadUsage {
        self.worker_threads[..self.workers]
            .iter()
            .fold(ThreadUsage::default(), |total, usage| total.plus(*usage))
    }

    /// Flushers, scheduler, snapshot actor and Exact checkpoint threads.
    pub(crate) fn maintenance_cpu_ns(&self) -> u64 {
        self.worker_thread_totals()
            .cpu_ns
            .saturating_add(self.scheduler_thread.cpu_ns)
            .saturating_add(self.snapshot_thread.cpu_ns)
            .saturating_add(self.exact.thread_cpu_ns)
    }

    /// Coordinator index bytes written: worker segments, root, wrapper and
    /// manifest bytes, and Exact checkpoint frames.
    pub(crate) fn logical_index_write_bytes(&self) -> u64 {
        self.store_totals()
            .sealed_bytes
            .saturating_add(self.root.root_bytes)
            .saturating_add(self.root.wrapper_bytes)
            .saturating_add(self.root.manifest_bytes)
            .saturating_add(self.exact.frame_bytes())
    }

    fn queue_depth(&self, worker: usize) -> u64 {
        self.range[worker]
            .dispatched()
            .saturating_sub(self.worker[worker].dequeued)
    }

    /// Captured cells whose result is not installed or restored yet.
    fn in_flight(&self, worker: usize) -> u64 {
        let counters = &self.worker[worker];
        counters
            .captured
            .saturating_sub(counters.installed)
            .saturating_sub(counters.restored)
    }

    /// Emit every counter and every available gauge (see the module docs
    /// for the naming rules).
    pub(crate) fn visit(&self, mut set: impl FnMut(&'static str, u64)) {
        let workers = self.workers;
        let worker_totals = self.worker_totals();
        let store_totals = self.store_totals();
        let range_totals = self.range_totals();
        for (name, value) in WorkerSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(worker_totals.values())
        {
            set(name, value);
        }
        for (name, value) in StoreSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(store_totals.values())
        {
            set(name, value);
        }
        for (name, value) in RangeSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(range_totals.values())
        {
            set(name, value);
        }
        for worker in 0..workers {
            self.visit_row(worker, &mut set);
        }
        for (name, value) in SchedulerSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(self.scheduler.values())
        {
            set(name, value);
        }
        for (name, value) in RootSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(self.root.values())
        {
            set(name, value);
        }
        for (name, value) in ExactSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(self.exact.values())
        {
            set(name, value);
        }
        for (name, value) in AdmissionSnapshot::TOTAL_NAMES
            .into_iter()
            .zip(self.admission.values())
        {
            set(name, value);
        }
        for (name, value) in RecoverySnapshot::TOTAL_NAMES
            .into_iter()
            .zip(self.recovery.values())
        {
            set(name, value);
        }
        emit_usage(&mut set, &WORKER_THREAD_NAMES, self.worker_thread_totals());
        emit_usage(&mut set, &SCHEDULER_THREAD_NAMES, self.scheduler_thread);
        emit_usage(&mut set, &SNAPSHOT_THREAD_NAMES, self.snapshot_thread);
        // Derived cumulative counters.
        set("cell_dispatched", range_totals.dispatched());
        set("cell_encode_bytes", store_totals.encode_bytes());
        set("cell_crc_bytes", store_totals.crc_bytes());
        set("cell_blake2b_bytes", store_totals.frame_bytes);
        set("cell_exact_encode_bytes", self.exact.encode_bytes());
        set("cell_exact_crc_bytes", self.exact.crc_bytes());
        set("cell_exact_blake2b_bytes", self.exact.frame_bytes());
        set("cell_maintenance_cpu_ns", self.maintenance_cpu_ns());
        set(
            "cell_logical_index_write_bytes",
            self.logical_index_write_bytes(),
        );
        self.visit_gauges(&mut set);
    }

    /// Counters of worker and range `worker`.
    fn visit_row(&self, worker: usize, set: &mut impl FnMut(&'static str, u64)) {
        for (name, value) in WorkerSnapshot::INDEXED_NAMES[worker]
            .into_iter()
            .zip(self.worker[worker].values())
        {
            set(name, value);
        }
        for (name, value) in StoreSnapshot::INDEXED_NAMES[worker]
            .into_iter()
            .zip(self.store[worker].values())
        {
            set(name, value);
        }
        for (name, value) in RangeSnapshot::INDEXED_NAMES[worker]
            .into_iter()
            .zip(self.range[worker].values())
        {
            set(name, value);
        }
        emit_usage(set, &THREAD_NAMES[worker], self.worker_threads[worker]);
    }

    fn visit_gauges(&self, set: &mut impl FnMut(&'static str, u64)) {
        let workers = self.workers;
        set("cell_gauge_workers", workers as u64);
        set(
            "cell_gauge_flush_state_bytes",
            CELL_FLUSH_STATE_BYTES as u64,
        );
        set(
            "cell_gauge_thread_cpu_available",
            u64::from(self.thread_cpu_available),
        );
        let mut depth = 0u64;
        let mut in_flight = 0u64;
        let mut active = 0u64;
        for worker in 0..workers {
            let queued = self.queue_depth(worker);
            let flying = self.in_flight(worker);
            set(QUEUE_DEPTH_NAMES[worker], queued);
            set(IN_FLIGHT_NAMES[worker], flying);
            depth = depth.saturating_add(queued);
            in_flight = in_flight.saturating_add(flying);
            active += u64::from(queued != 0 || flying != 0);
        }
        set("cell_gauge_queue_depth", depth);
        set("cell_gauge_in_flight", in_flight);
        set("cell_gauge_active_workers", active);
        let pass = &self.pass;
        set("cell_gauge_dirty_cells", pass.dirty_cells);
        if let Some(target) = pass.target {
            emit_cut(set, cut_names!("pressure_target"), target);
        }
        if let Some(covered) = pass.covered {
            emit_cut(set, cut_names!("covered"), covered);
        }
        if let Some(oldest) = pass.oldest_uncovered {
            emit_cut(
                set,
                cut_names!("oldest_uncovered"),
                PrefixCut::Through(oldest),
            );
        }
        if let Some(since) = pass.oldest_dirty_since {
            set(
                "cell_gauge_oldest_uncovered_age_ms",
                u64::try_from(self.taken.saturating_duration_since(since).as_millis())
                    .unwrap_or(u64::MAX),
            );
        }
        let gauges = &self.gauges;
        if let Some(published) = gauges.published {
            emit_cut(set, cut_names!("published"), published);
            if let Some(oldest) = pass.oldest_uncovered
                && let Some(lag) = lag(published, PrefixCut::Through(oldest))
            {
                set("cell_gauge_lag_l_oldest_batches", lag);
            }
        }
        if let Some(adopted) = gauges.adopted {
            set("cell_gauge_root_generation", adopted.generation);
            emit_cut(set, cut_names!("root_target"), adopted.target);
            emit_cut(set, cut_names!("replay_floor"), adopted.floor);
            emit_cut(set, cut_names!("max_frame_cut"), adopted.max_frame_cut);
            emit_cut(set, cut_names!("history_floor"), adopted.history_floor);
            let mut cross_epoch = false;
            if let Some(published) = gauges.published {
                match lag(published, adopted.floor) {
                    Some(lag) => set("cell_gauge_lag_l_f_batches", lag),
                    None => cross_epoch |= published != PrefixCut::BeforeAll,
                }
            }
            match lag(adopted.floor, adopted.history_floor) {
                Some(lag) => set("cell_gauge_lag_f_h_batches", lag),
                None => cross_epoch |= adopted.floor != PrefixCut::BeforeAll,
            }
            set("cell_gauge_lag_cross_epoch", u64::from(cross_epoch));
        }
        for (name, value) in [
            (
                "cell_gauge_resident_index_bytes",
                gauges.resident_index_bytes,
            ),
            (
                "cell_gauge_admitted_index_bytes",
                gauges.admitted_index_bytes,
            ),
            (
                "cell_gauge_transient_index_bytes",
                gauges.transient_index_bytes,
            ),
        ] {
            if let Some(value) = value {
                set(name, value);
            }
        }
        if let Some(space) = gauges.segments {
            set("cell_gauge_segments", space.segments);
            set("cell_gauge_segment_sealed_bytes", space.sealed_bytes);
            set("cell_gauge_segment_live_bytes", space.live_bytes);
            set(
                "cell_gauge_segment_dead_bytes",
                space.sealed_bytes.saturating_sub(space.live_bytes),
            );
            set(
                "cell_gauge_segment_gc_pending_bytes",
                space.gc_pending_bytes,
            );
            set("cell_gauge_segments_writing", space.writing_segments);
        }
    }
}

#[cfg(test)]
#[path = "cell_metrics_tests.rs"]
mod tests;
