//! Option C crash-cut event model (checkpoint option C spec §8.3, §11.3).
//!
//! A deterministic, single-threaded model of the per-cell protocol. Actors:
//! eight callers and the owner workers of option D (allocation, append, owner
//! sync, dependency-ordered publication, acknowledgement in either tier),
//! index-shard-range flushers with private segment streams, the metadata actor
//! (root capture in slices, owner proofs, receipt history, root, wrapper and
//! manifest persistence, live apply, retirement and GC), and the restart path
//! (epoch reservation, orphan scan, mixed-cut replay, offline flush). Every
//! transition of spec §11.3 is a crash cut ([`Transition`]). A seed fixes the
//! workload and the interleaving; [`run_schedule`] returns the first
//! divergence with its seed and first divergent event.
//!
//! Model filesystem: one database directory. File data survives a process
//! crash. A power loss keeps only fsynced bytes and a prefix of the directory
//! operations since the last directory fsync, so a rename before its directory
//! fsync recovers as the old or the new entry; every crash checks the process
//! outcome and every power-loss prefix. Owner WALs survive a coordinator crash
//! (owner page cache). Owner power loss (each owner keeps only its synced
//! prefix) is the supported Native-tier loss model; it is checked and counted
//! separately and is not evidence about real hardware.
//!
//! Real code in the loop: `CellRoot::check` validates every root before it is
//! persisted and at recovery, `CellRoot::replay_plan` classifies replayed
//! members, `RetirementLedger::apply` returns credits exactly once,
//! `IndexStoreFileId` names segments, and `IndexStore::open` cross-checks the
//! restart orphan scan on a materialized directory. The oracle uses only the
//! issued batch log and the surviving owner WAL contents, never recovery
//! bookkeeping.
use super::super::checkpoint_contract::{RetirementLedger, RetirementToken};
use crate::cell::CellId;
use crate::distributed::native_index::{
    CellRoot, CellRootHeader, IndexStore, IndexStoreFileId, MemberReplay, PrefixCut, RootCell,
    RootCellBase, RootFile,
};
use crate::distributed::types::{BatchVersion, MutationVersion};
use crate::key_shape::KeySpace;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

pub(super) const KEYS: usize = 64;
const CELLS: usize = 16;
const KEYS_PER_CELL: usize = KEYS / CELLS;
const SHARDS: usize = 8;
pub(super) const CALLERS: usize = 8;
const OPS_PER_CALLER: usize = 6;
/// Hard replay-tail bound in batches: granted and not retired.
const TAIL_BATCHES: usize = 24;
/// Resident overlay references recovery may hold before an offline flush.
const RESIDENT_REFS: usize = 96;
const ROOT_SLICE: usize = 4;
const GROUP_CELLS: usize = 4;
const IDENTITY_SHARDS: usize = 4;
/// Offline recovery stream id: live flushers use `0..P`, `P <= 8`.
const OFFLINE_STREAM: u16 = 8;
const MAX_STEPS: usize = 20_000;
/// The same recent-window bound as the format-4 wrapper.
const RECENT_WINDOW: usize = 4096;
const DATABASE: [u8; 16] = *b"cell-model-db-01";
const SCHEMA: [u8; 32] = [7; 32];
const MANIFEST: &str = "authority.manifest";
const HISTORY_LOG: &str = "receipt-history.log";
const HISTORY_RECORD: u64 = 32;
const OBJECT_BYTES: u64 = 64;

/// Crash cuts. Names match the `write_model` failpoints of the real path.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) enum Transition {
    Append,
    OwnerSync,
    Publication,
    SegmentCreate,
    FrameWrite,
    FramePartial,
    GroupFlush,
    SegmentFsync,
    SegmentDirsync,
    Install,
    CleanAdvance,
    RootSlice,
    OwnerProof,
    HistoryAppend,
    HistorySync,
    HistoryRoot,
    RootTemp,
    RootFsync,
    RootRename,
    RootDirsync,
    WrapperTemp,
    WrapperFsync,
    WrapperRename,
    WrapperDirsync,
    ManifestTemp,
    ManifestFsync,
    ManifestRename,
    ManifestDirsync,
    ApplyHistory,
    ApplyAuthority,
    ApplyLedger,
    IdentityRemove,
    CreditReturn,
    GcCheck,
    GcUnlink,
    GcDirsync,
    EpochReserve,
    OrphanScan,
    OrphanDelete,
    OrphanDirsync,
    OfflineFlush,
}

impl Transition {
    /// Cuts while serving.
    pub(super) const RUN: &'static [Self] = &[
        Self::Append,
        Self::OwnerSync,
        Self::Publication,
        Self::SegmentCreate,
        Self::FrameWrite,
        Self::FramePartial,
        Self::GroupFlush,
        Self::SegmentFsync,
        Self::SegmentDirsync,
        Self::Install,
        Self::CleanAdvance,
        Self::RootSlice,
        Self::OwnerProof,
        Self::HistoryAppend,
        Self::HistorySync,
        Self::HistoryRoot,
        Self::RootTemp,
        Self::RootFsync,
        Self::RootRename,
        Self::RootDirsync,
        Self::WrapperTemp,
        Self::WrapperFsync,
        Self::WrapperRename,
        Self::WrapperDirsync,
        Self::ManifestTemp,
        Self::ManifestFsync,
        Self::ManifestRename,
        Self::ManifestDirsync,
        Self::ApplyHistory,
        Self::ApplyAuthority,
        Self::ApplyLedger,
        Self::IdentityRemove,
        Self::CreditReturn,
        Self::GcCheck,
        Self::GcUnlink,
        Self::GcDirsync,
    ];
    /// Cuts during reopen; the reopen after them runs from that state.
    pub(super) const RESTART: &'static [Self] = &[
        Self::EpochReserve,
        Self::OrphanScan,
        Self::OrphanDelete,
        Self::OrphanDirsync,
        Self::OfflineFlush,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Append => "append_after",
            Self::OwnerSync => "file_sync_after",
            Self::Publication => "publication_after",
            Self::SegmentCreate => "cell_segment_create_after",
            Self::FrameWrite => "cell_frame_write_after",
            Self::FramePartial => "cell_frame_partial_after",
            Self::GroupFlush => "cell_group_flush_after",
            Self::SegmentFsync => "cell_segment_fsync_after",
            Self::SegmentDirsync => "cell_segment_dirsync_after",
            Self::Install => "cell_install_after",
            Self::CleanAdvance => "cell_clean_advance_after",
            Self::RootSlice => "cell_root_slice_after",
            Self::OwnerProof => "cell_owner_proof_after",
            Self::HistoryAppend => "cell_history_append_after",
            Self::HistorySync => "cell_history_sync_after",
            Self::HistoryRoot => "cell_history_root_after",
            Self::RootTemp => "cell_root_temp_written",
            Self::RootFsync => "cell_root_fsync_after",
            Self::RootRename => "cell_root_rename_after",
            Self::RootDirsync => "cell_root_dirsync_after",
            Self::WrapperTemp => "cell_wrapper_temp_written",
            Self::WrapperFsync => "cell_wrapper_fsync_after",
            Self::WrapperRename => "cell_wrapper_rename_after",
            Self::WrapperDirsync => "cell_wrapper_dirsync_after",
            Self::ManifestTemp => "cell_manifest_temp_written",
            Self::ManifestFsync => "cell_manifest_fsync_after",
            Self::ManifestRename => "cell_manifest_rename_after",
            Self::ManifestDirsync => "cell_manifest_dirsync_after",
            Self::ApplyHistory => "cell_apply_history_after",
            Self::ApplyAuthority => "cell_apply_authority_after",
            Self::ApplyLedger => "cell_apply_ledger_after",
            Self::IdentityRemove => "cell_identity_remove_after",
            Self::CreditReturn => "cell_credit_return_after",
            Self::GcCheck => "cell_gc_check_after",
            Self::GcUnlink => "cell_gc_unlink_after",
            Self::GcDirsync => "cell_gc_dirsync_after",
            Self::EpochReserve => "cell_epoch_reserve_after",
            Self::OrphanScan => "cell_orphan_scan_after",
            Self::OrphanDelete => "cell_orphan_delete_after",
            Self::OrphanDirsync => "cell_orphan_dirsync_after",
            Self::OfflineFlush => "cell_offline_flush_after",
        }
    }

    pub(super) fn parse(name: &str) -> Option<Self> {
        Self::RUN
            .iter()
            .chain(Self::RESTART)
            .copied()
            .find(|transition| transition.name() == name)
    }

    /// Receipt-history steps exist only in that history mode.
    pub(super) fn applies(self, on_disk_history: bool) -> bool {
        on_disk_history
            || !matches!(
                self,
                Self::HistoryAppend | Self::HistorySync | Self::HistoryRoot
            )
    }

    fn restart(self) -> bool {
        Self::RESTART.contains(&self)
    }
}

/// Crash at the `occurrence`-th firing (1-based) of `at`. A restart cut
/// counts within the first reopen; `again` also crashes the next reopen at
/// its orphan scan (a crash immediately on a second restart).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CrashCut {
    pub at: Transition,
    pub occurrence: usize,
    pub again: bool,
}

/// Protocol defects. Each must be rejected by the checker; they prove the
/// oracle is not vacuous. Never enabled outside the rejection test.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Bug {
    /// Install and root-reference a segment before file and directory fsync.
    SkipSegmentSync,
    /// Export `F` as the maximum instead of the minimum cell coverage.
    MaxCoverageFloor,
    /// Recovery skips a whole batch when one member is covered.
    SkipCoveredBatch,
    /// Remove identities before installing the retry authority.
    IdentityBeforeAuthority,
    /// Sweep obsolete files right after the manifest rename, before its
    /// directory fsync and live apply.
    GcBeforeManifestSync,
    /// Return replay-tail credit when the root is written, before adoption.
    CreditBeforeAdoption,
    /// Reopen deletes no orphan.
    NoOrphanCleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ModelConfig {
    pub seed: u64,
    pub owners: usize,
    pub zipf: bool,
    pub native: bool,
    pub on_disk_history: bool,
    pub cut: Option<CrashCut>,
    /// Cross-check the orphan scan of each continued reopen with the real
    /// `IndexStore::open` on a materialized directory.
    pub real_store: bool,
    pub bug: Option<Bug>,
}

impl ModelConfig {
    pub(super) fn new(seed: u64, owners: usize, zipf: bool, native: bool, history: bool) -> Self {
        Self {
            seed,
            owners,
            zipf,
            native,
            on_disk_history: history,
            cut: None,
            real_store: false,
            bug: None,
        }
    }

    fn flushers(&self) -> usize {
        1 + (self.seed as usize >> 3) % 4
    }

    fn bug(&self, bug: Bug) -> bool {
        self.bug == Some(bug)
    }
}

/// Counters of one schedule. Supported crash outcomes and owner power loss
/// are kept apart (spec §11.3).
#[derive(Clone, Debug, Default)]
pub(super) struct Report {
    pub crashes: usize,
    pub cut_reached: bool,
    pub process_outcomes: usize,
    pub power_outcomes: usize,
    /// Power-loss outcomes that recovered a pending rename's old entry.
    pub rename_old: usize,
    /// Power-loss outcomes that recovered a pending rename's new entry.
    pub rename_new: usize,
    pub owner_power_loss: usize,
    pub owner_power_lost_acks: usize,
    pub fired: BTreeMap<&'static str, usize>,
    pub roots_adopted: usize,
    pub coverage_behind_frame: usize,
    pub split_batches: usize,
    pub fully_covered: usize,
    pub multi_epoch_split: usize,
    pub offline_flushes: usize,
    pub orphans_deleted: usize,
    pub restart_crashes: usize,
    /// Root captures rejected before any output because F < adopted F.
    pub abandoned_roots: usize,
    pub real_store_checks: usize,
    pub acknowledged: usize,
    pub retries: usize,
    pub aborted: usize,
}

impl Report {
    pub(super) fn merge(&mut self, other: &Self) {
        self.crashes += other.crashes;
        self.cut_reached |= other.cut_reached;
        self.process_outcomes += other.process_outcomes;
        self.power_outcomes += other.power_outcomes;
        self.rename_old += other.rename_old;
        self.rename_new += other.rename_new;
        self.owner_power_loss += other.owner_power_loss;
        self.owner_power_lost_acks += other.owner_power_lost_acks;
        for (name, count) in &other.fired {
            *self.fired.entry(name).or_default() += count;
        }
        self.roots_adopted += other.roots_adopted;
        self.coverage_behind_frame += other.coverage_behind_frame;
        self.split_batches += other.split_batches;
        self.fully_covered += other.fully_covered;
        self.multi_epoch_split += other.multi_epoch_split;
        self.offline_flushes += other.offline_flushes;
        self.orphans_deleted += other.orphans_deleted;
        self.restart_crashes += other.restart_crashes;
        self.abandoned_roots += other.abandoned_roots;
        self.real_store_checks += other.real_store_checks;
        self.acknowledged += other.acknowledged;
        self.retries += other.retries;
        self.aborted += other.aborted;
    }
}

/// A protocol or oracle violation, with what it concerns.
#[derive(Clone, Debug)]
pub(super) struct Divergence {
    pub kind: &'static str,
    pub detail: String,
    versions: Vec<BatchVersion>,
    ops: Vec<u64>,
    names: Vec<String>,
}

fn diverge(kind: &'static str, detail: impl Into<String>) -> Divergence {
    Divergence {
        kind,
        detail: detail.into(),
        versions: Vec::new(),
        ops: Vec::new(),
        names: Vec::new(),
    }
}

impl Divergence {
    fn version(mut self, version: BatchVersion) -> Self {
        self.versions.push(version);
        self
    }
    fn op(mut self, op: u64) -> Self {
        self.ops.push(op);
        self
    }
    fn name(mut self, name: impl Into<String>) -> Self {
        self.names.push(name.into());
        self
    }
}

#[derive(Clone, Debug)]
pub(super) struct TraceEvent {
    pub index: usize,
    pub name: &'static str,
    version: Option<BatchVersion>,
    op: Option<u64>,
    file: Option<String>,
}

/// A failed schedule: everything needed to replay and locate it.
#[derive(Debug)]
pub(super) struct Failure {
    pub config: ModelConfig,
    pub outcome: String,
    pub divergence: Divergence,
    pub first_event: Option<TraceEvent>,
    pub events: usize,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cell model divergence seed={} owners={} zipf={} native={} on_disk_history={} cut={} outcome={} kind={} detail={} first_divergent_event={} events={} (replay: TH_MODEL_SEED={} TH_MODEL_OWNERS={} TH_MODEL_HOT={} TH_MODEL_NATIVE={} TH_MODEL_ON_DISK_HISTORY={}{})",
            self.config.seed,
            self.config.owners,
            self.config.zipf,
            self.config.native,
            self.config.on_disk_history,
            self.config.cut.map_or("none".to_owned(), |cut| format!(
                "{}#{}{}",
                cut.at.name(),
                cut.occurrence,
                if cut.again { "+again" } else { "" }
            )),
            self.outcome,
            self.divergence.kind,
            self.divergence.detail,
            self.first_event
                .as_ref()
                .map_or("none".to_owned(), |event| format!(
                    "#{} {} version={:?} op={:?} file={:?}",
                    event.index, event.name, event.version, event.op, event.file
                )),
            self.events,
            self.config.seed,
            self.config.owners,
            u8::from(self.config.zipf),
            u8::from(self.config.native),
            u8::from(self.config.on_disk_history),
            self.config.cut.map_or(String::new(), |cut| format!(
                " TH_MODEL_CUT={} TH_MODEL_OCCURRENCE={}",
                cut.at.name(),
                cut.occurrence
            )),
        )
    }
}

enum Halt {
    Crash,
    Diverge(Divergence),
}

impl From<Divergence> for Halt {
    fn from(divergence: Divergence) -> Self {
        Self::Diverge(divergence)
    }
}

// ---------------------------------------------------------------- filesystem

#[derive(Clone, Debug)]
enum Content {
    Segment { frames: Vec<Frame> },
    Root(Arc<CellRoot>),
    Wrapper(Arc<Wrapper>),
    Manifest(Arc<Manifest>),
    HistoryLog(Vec<(u64, BatchVersion)>),
    HistoryRoot { generation: u64, records: usize },
}

#[derive(Clone, Debug)]
struct Inode {
    content: Content,
    /// Bytes in the file (page cache).
    len: u64,
    /// Bytes covered by the last fsync.
    synced: u64,
    /// Length of a complete object file; segments and logs grow instead.
    full: u64,
}

#[derive(Clone, Debug)]
enum DirOp {
    Link(String, usize),
    Unlink(String),
    Rename(String, String),
}

/// One directory. `durable` is the namespace at the last directory fsync;
/// `pending` the operations since, in order.
#[derive(Clone, Debug, Default)]
struct Fs {
    inodes: Vec<Inode>,
    live: BTreeMap<String, usize>,
    durable: BTreeMap<String, usize>,
    pending: Vec<DirOp>,
}

/// `applied`: the directory operations since the last directory fsync that
/// survive, as a prefix. `unlinks`: every later unlink survives as well; no
/// ordering between an unlink and an earlier rename is assumed without a
/// directory fsync.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FsOutcome {
    Process,
    PowerLoss { applied: usize, unlinks: bool },
}

impl Fs {
    fn create_new(&mut self, name: &str, content: Content, full: u64) -> Result<usize, Divergence> {
        if self.live.contains_key(name) {
            return Err(diverge("create_new name collision", name).name(name));
        }
        Ok(self.link(name, content, full))
    }

    /// Create or truncate (staging files of one exclusive writer).
    fn create(&mut self, name: &str, content: Content, full: u64) -> usize {
        if self.live.contains_key(name) {
            self.unlink(name);
        }
        self.link(name, content, full)
    }

    fn link(&mut self, name: &str, content: Content, full: u64) -> usize {
        let inode = self.inodes.len();
        self.inodes.push(Inode {
            content,
            len: 0,
            synced: 0,
            full,
        });
        self.live.insert(name.to_owned(), inode);
        self.pending.push(DirOp::Link(name.to_owned(), inode));
        inode
    }

    fn unlink(&mut self, name: &str) {
        if self.live.remove(name).is_some() {
            self.pending.push(DirOp::Unlink(name.to_owned()));
        }
    }

    fn rename(&mut self, from: &str, to: &str) -> Result<(), Divergence> {
        let inode = self
            .live
            .remove(from)
            .ok_or_else(|| diverge("rename of a missing name", from).name(from))?;
        self.live.insert(to.to_owned(), inode);
        self.pending
            .push(DirOp::Rename(from.to_owned(), to.to_owned()));
        Ok(())
    }

    fn fsync(&mut self, inode: usize) {
        let inode = &mut self.inodes[inode];
        inode.synced = inode.len;
    }

    fn dirsync(&mut self) {
        self.durable = self.live.clone();
        self.pending.clear();
    }

    fn inode(&self, name: &str) -> Option<&Inode> {
        self.live.get(name).map(|inode| &self.inodes[*inode])
    }

    /// A complete object file, or `None` if absent or torn.
    fn object(&self, name: &str) -> Option<&Content> {
        self.inode(name)
            .filter(|inode| inode.len == inode.full)
            .map(|inode| &inode.content)
    }

    /// The state the next open sees after a crash.
    fn crash(&self, outcome: FsOutcome) -> Self {
        match outcome {
            FsOutcome::Process => {
                let mut fs = self.clone();
                fs.durable = fs.live.clone();
                fs.pending.clear();
                fs
            }
            FsOutcome::PowerLoss { applied, unlinks } => {
                let mut live = self.durable.clone();
                for (index, op) in self.pending.iter().enumerate() {
                    match op {
                        DirOp::Link(name, inode) if index < applied => {
                            live.insert(name.clone(), *inode);
                        }
                        DirOp::Unlink(name) if index < applied || unlinks => {
                            live.remove(name);
                        }
                        DirOp::Rename(from, to) if index < applied => {
                            if let Some(inode) = live.remove(from) {
                                live.insert(to.clone(), inode);
                            }
                        }
                        _ => {}
                    }
                }
                let mut inodes = self.inodes.clone();
                for inode in &mut inodes {
                    inode.len = inode.synced;
                    // Unsynced log records are gone, not merely hidden.
                    if let Content::HistoryLog(log) = &mut inode.content {
                        log.truncate((inode.synced / HISTORY_RECORD) as usize);
                    }
                }
                Self {
                    inodes,
                    durable: live.clone(),
                    live,
                    pending: Vec::new(),
                }
            }
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Edit {
    key: u8,
    value: Option<u64>,
}

/// One retained reference. Ordered by key, then version, as a frame row.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Ref {
    key: u8,
    version: MutationVersion,
    value: Option<u64>,
}

#[derive(Clone, Debug)]
struct Frame {
    offset: u64,
    len: u64,
    cell: usize,
    cut: PrefixCut,
    floor: Option<MutationVersion>,
    refs: Arc<Vec<Ref>>,
    digest: [u8; 32],
}

#[derive(Clone, Debug)]
struct Wrapper {
    generation: u64,
    root_name: String,
    floor: PrefixCut,
    history_floor: Option<MutationVersion>,
    /// Per `(owner, owner epoch)`: index of the last entry through `F`.
    streams: BTreeMap<(usize, u64), Option<usize>>,
    recent: Vec<(u64, BatchVersion)>,
    history_root: Option<(String, u64)>,
    token: RetirementToken,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Commit {
    generation: u64,
    root_name: String,
    wrapper_name: String,
}

#[derive(Clone, Debug)]
struct Manifest {
    epoch: u64,
    commit: Option<Commit>,
}

#[derive(Clone, Debug)]
struct WalEntry {
    version: BatchVersion,
    op: u64,
    edits: Arc<Vec<Edit>>,
}

#[derive(Clone, Debug, Default)]
struct Stream {
    entries: Vec<WalEntry>,
    synced: usize,
}

/// Owner WAL streams by owner epoch (the coordinator epoch that wrote them).
type OwnerStore = BTreeMap<u64, Stream>;

#[derive(Clone, Debug)]
struct Truth {
    op: u64,
    edits: Arc<Vec<Edit>>,
    owner: usize,
    aborted: bool,
    /// Reached its owner's WAL (page cache) at least once.
    appended: bool,
}

#[derive(Clone, Debug)]
struct PlanOp {
    op: u64,
    edits: Arc<Vec<Edit>>,
}

#[derive(Clone, Debug)]
enum CallerState {
    Idle,
    Waiting { op: u64, version: BatchVersion },
}

#[derive(Clone, Debug)]
struct Caller {
    plan: Vec<PlanOp>,
    next: usize,
    state: CallerState,
}

// ---------------------------------------------------------------- RAM state

#[derive(Clone, Debug)]
struct Base {
    file: IndexStoreFileId,
    offset: u64,
    len: u64,
    digest: [u8; 32],
    cut: PrefixCut,
    floor: Option<MutationVersion>,
    refs: Arc<Vec<Ref>>,
}

#[derive(Clone, Debug)]
struct Cell {
    base: Option<Base>,
    coverage: PrefixCut,
    /// Published references not represented by the base (active, carry and
    /// captured sources together; all of them stay readable).
    overlay: BTreeSet<Ref>,
    flight: Option<u64>,
    queued: bool,
    token: u64,
}

impl Cell {
    fn new(coverage: PrefixCut) -> Self {
        Self {
            base: None,
            coverage,
            overlay: BTreeSet::new(),
            flight: None,
            queued: false,
            token: 0,
        }
    }

    fn missing_through(&self, target: PrefixCut) -> bool {
        self.overlay
            .iter()
            .any(|reference| target.contains(reference.version.batch))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchState {
    Queued,
    Appended(usize),
    Published,
    Aborted,
}

#[derive(Clone, Debug)]
struct Captured {
    cell: usize,
    flight: u64,
    token: u64,
    target: PrefixCut,
    captured: Vec<Ref>,
    refs: Arc<Vec<Ref>>,
    floor: Option<MutationVersion>,
    frame: Option<Frame>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FlushPhase {
    Capturing,
    Create,
    Writing(usize),
    Partial,
    Flush,
    Fsync,
    Dirsync,
    Install,
}

#[derive(Clone, Debug)]
struct Flusher {
    stream: u16,
    next_seq: u64,
    queue: VecDeque<usize>,
    phase: FlushPhase,
    group: Vec<Captured>,
    segment: Option<(IndexStoreFileId, usize, u64)>,
}

#[derive(Clone, Debug)]
enum SnapStep {
    Slice,
    Proof(usize),
    HistoryAppend,
    HistorySync,
    HistoryRoot,
    RootTemp,
    RootFsync,
    RootRename,
    RootDirsync,
    WrapperTemp,
    WrapperFsync,
    WrapperRename,
    WrapperDirsync,
    ManifestTemp,
    ManifestFsync,
    ManifestRename,
    ManifestDirsync,
    ApplyHistory,
    ApplyAuthority,
    ApplyLedger,
    IdentityRemove(usize),
    CreditReturn,
    GcCheck,
    GcUnlink(String),
    GcDirsync,
}

#[derive(Clone, Debug)]
struct Desc {
    cell: usize,
    coverage: PrefixCut,
    base: Option<Base>,
}

#[derive(Clone, Debug)]
struct Capture {
    generation: u64,
    target: PrefixCut,
    boundary: Vec<usize>,
    next: usize,
    descs: Vec<Desc>,
    floor: PrefixCut,
    max_cut: PrefixCut,
    history: Option<MutationVersion>,
    root: Option<Arc<CellRoot>>,
    token: Option<RetirementToken>,
    history_root: Option<(String, u64)>,
    credited: bool,
    steps: VecDeque<SnapStep>,
}

#[derive(Clone, Debug)]
enum Snap {
    Idle,
    Draining(PrefixCut),
    Running(Box<Capture>),
}

#[derive(Clone, Debug)]
struct Adopted {
    generation: u64,
    floor: PrefixCut,
    history: Option<MutationVersion>,
    max_cut: PrefixCut,
    files: Vec<RootFile>,
    commit: Option<Commit>,
}

/// Coordinator RAM: lost at every crash.
#[derive(Clone, Debug)]
struct Ram {
    epoch: u64,
    next_seq: u64,
    batches: BTreeMap<BatchVersion, BatchState>,
    preds: BTreeMap<BatchVersion, Vec<BatchVersion>>,
    key_last: Vec<Option<BatchVersion>>,
    queues: Vec<VecDeque<BatchVersion>>,
    identities: Vec<BTreeMap<u64, BatchVersion>>,
    authority: BTreeMap<u64, BatchVersion>,
    /// Batches visible to readers: recovered ones and those published since.
    present: BTreeSet<BatchVersion>,
    published: PrefixCut,
    cells: Vec<Option<Cell>>,
    flushers: Vec<Flusher>,
    flight_ids: u64,
    snap: Snap,
    adopted: Adopted,
    ledger: RetirementLedger,
    tail: usize,
    sealed: BTreeMap<IndexStoreFileId, u64>,
}

impl Ram {
    fn lookup(&self, op: u64) -> Option<BatchVersion> {
        self.identities[identity_shard(op)]
            .get(&op)
            .or_else(|| self.authority.get(&op))
            .copied()
    }

    fn resolved(&self, version: BatchVersion) -> bool {
        version.epoch < self.epoch
            || matches!(
                self.batches.get(&version),
                Some(BatchState::Published | BatchState::Aborted)
            )
    }

    fn cell(&mut self, cell: usize) -> &mut Cell {
        let floor = self.adopted.floor;
        self.cells[cell].get_or_insert_with(|| Cell::new(floor))
    }

    fn history(&self, key: usize) -> Vec<Ref> {
        let Some(cell) = &self.cells[cell_of(key)] else {
            return Vec::new();
        };
        let mut refs: Vec<Ref> = cell
            .base
            .iter()
            .flat_map(|base| base.refs.iter())
            .chain(&cell.overlay)
            .filter(|reference| usize::from(reference.key) == key)
            .copied()
            .collect();
        refs.sort();
        refs
    }
}

// ---------------------------------------------------------------- helpers

fn cell_of(key: usize) -> usize {
    key / KEYS_PER_CELL
}

fn identity_shard(op: u64) -> usize {
    (op % IDENTITY_SHARDS as u64) as usize
}

fn owner_of(op: u64, owners: usize) -> usize {
    ((op.wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32) % owners as u64) as usize
}

fn encoded_bytes(edits: &[Edit]) -> u64 {
    16 + 12 * edits.len() as u64
}

fn refs_of(version: BatchVersion, edits: &[Edit]) -> impl Iterator<Item = Ref> + '_ {
    edits.iter().enumerate().map(move |(index, edit)| Ref {
        key: edit.key,
        version: MutationVersion {
            batch: version,
            operation_index: index as u32,
        },
        value: edit.value,
    })
}

fn root_name(epoch: u64, generation: u64) -> String {
    format!("index-root-{epoch}-{generation}.cellroot")
}

fn wrapper_name(epoch: u64, generation: u64) -> String {
    format!("checkpoint-{epoch}-{generation}.metadata")
}

fn history_root_name(generation: u64) -> String {
    format!("history-root-{generation}")
}

fn staging(name: &str) -> String {
    format!(".{name}.pending")
}

fn metadata_output(name: &str) -> bool {
    name.starts_with('.')
        || name.starts_with("index-root-")
        || name.starts_with("checkpoint-")
        || name.starts_with("history-root-")
}

fn floor_of(cut: PrefixCut) -> Option<MutationVersion> {
    match cut {
        PrefixCut::BeforeAll => None,
        PrefixCut::Through(batch) => Some(MutationVersion {
            batch,
            operation_index: u32::MAX,
        }),
    }
}

/// Keep every version above `floor` and the newest at or below it, per key
/// (including a tombstone): the retention rule of a frame.
fn compact(refs: impl IntoIterator<Item = Ref>, floor: Option<MutationVersion>) -> Vec<Ref> {
    let mut sorted: Vec<Ref> = refs.into_iter().collect();
    sorted.sort();
    sorted.dedup();
    let mut out = Vec::with_capacity(sorted.len());
    for (index, reference) in sorted.iter().enumerate() {
        let below = floor.is_some_and(|floor| reference.version <= floor);
        let superseded = sorted.get(index + 1).is_some_and(|next| {
            next.key == reference.key && floor.is_some_and(|floor| next.version <= floor)
        });
        if !(below && superseded) {
            out.push(*reference);
        }
    }
    out
}

fn digest(refs: &[Ref], cut: PrefixCut) -> [u8; 32] {
    let mut out = [0; 32];
    for (lane, chunk) in out.chunks_mut(8).enumerate() {
        let mut hasher = DefaultHasher::new();
        lane.hash(&mut hasher);
        refs.hash(&mut hasher);
        cut.hash(&mut hasher);
        chunk.copy_from_slice(&hasher.finish().to_le_bytes());
    }
    out
}

fn frame_len(refs: &[Ref]) -> u64 {
    12 + 24 * refs.len() as u64
}

fn members(edits: &[Edit]) -> Vec<(KeySpace, CellId)> {
    edits
        .iter()
        .map(|edit| (KeySpace(0), CellId::Integer(cell_of(edit.key.into()))))
        .collect()
}

fn plans(config: &ModelConfig, rng: &mut StdRng) -> Vec<Vec<PlanOp>> {
    let weights: Vec<f64> = (1..=KEYS).map(|rank| (rank as f64).powf(-0.99)).collect();
    let total: f64 = weights.iter().sum();
    let mut issued: Vec<PlanOp> = Vec::new();
    let mut plans: Vec<Vec<PlanOp>> = (0..CALLERS)
        .map(|_| Vec::with_capacity(OPS_PER_CALLER))
        .collect();
    for round in 0..OPS_PER_CALLER {
        for plan in &mut plans {
            if !issued.is_empty() && round > 0 && rng.gen_ratio(1, 5) {
                // A duplicate identity with the identical payload: a retry,
                // possibly of another caller's operation, in flight or done.
                plan.push(issued[rng.gen_range(0..issued.len())].clone());
                continue;
            }
            let op = issued.len() as u64;
            let size = [1, 8, 32][op as usize % 3];
            let mut keys = Vec::with_capacity(size);
            while keys.len() < size {
                let key = if config.zipf {
                    let draw = rng.gen_range(0.0..total);
                    let mut sum = 0.0;
                    weights
                        .iter()
                        .position(|weight| {
                            sum += weight;
                            draw < sum
                        })
                        .unwrap_or(KEYS - 1)
                } else {
                    rng.gen_range(0..KEYS)
                };
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
            let mut edits: Vec<Edit> = keys
                .iter()
                .enumerate()
                .map(|(member, key)| Edit {
                    key: *key as u8,
                    value: (op == 0 || rng.gen_ratio(3, 4)).then_some(op * 64 + member as u64 + 1),
                })
                .collect();
            if size > 1 && rng.gen_ratio(1, 8) {
                // The same key twice in one batch, at two operation indexes.
                edits.push(Edit {
                    key: keys[0] as u8,
                    value: edits[0].value.xor(Some(op * 64 + 63)),
                });
            }
            let planned = PlanOp {
                op,
                edits: Arc::new(edits),
            };
            issued.push(planned.clone());
            plan.push(planned);
        }
    }
    plans
}

// ---------------------------------------------------------------- world

/// Everything that survives a coordinator crash, plus the observation log.
struct World {
    config: ModelConfig,
    rng: StdRng,
    fs: Fs,
    owners: Vec<OwnerStore>,
    truth: BTreeMap<BatchVersion, Truth>,
    acked: BTreeMap<u64, BatchVersion>,
    callers: Vec<Caller>,
    ever_created: BTreeSet<IndexStoreFileId>,
    trace: Vec<TraceEvent>,
    fired: BTreeMap<Transition, usize>,
    report: Report,
    cut_armed: bool,
    epochs: BTreeSet<u64>,
}

impl World {
    fn record(
        &mut self,
        name: &'static str,
        version: Option<BatchVersion>,
        op: Option<u64>,
        file: Option<String>,
    ) {
        self.trace.push(TraceEvent {
            index: self.trace.len(),
            name,
            version,
            op,
            file,
        });
    }

    /// Record a transition; crash here if it is the configured cut.
    fn fire(
        &mut self,
        transition: Transition,
        version: Option<BatchVersion>,
        file: Option<String>,
    ) -> Result<(), Halt> {
        self.record(transition.name(), version, None, file);
        let count = self.fired.entry(transition).or_default();
        *count += 1;
        *self.report.fired.entry(transition.name()).or_default() += 1;
        if self.cut_armed
            && let Some(cut) = self.config.cut
            && !cut.at.restart()
            && cut.at == transition
            && *count == cut.occurrence
        {
            self.cut_armed = false;
            self.report.cut_reached = true;
            return Err(Halt::Crash);
        }
        Ok(())
    }
}

struct Sim {
    w: World,
    r: Ram,
}

#[derive(Clone, Copy, Debug)]
enum Action {
    Caller(usize),
    Ack(usize),
    Append(usize),
    Sync(usize),
    Publish(BatchVersion),
    Queue(usize),
    Flusher(usize),
    Snapshot,
}

impl Sim {
    fn new(config: ModelConfig) -> Self {
        let mut rng = StdRng::seed_from_u64(config.seed);
        let plans = plans(&config, &mut rng);
        let mut fs = Fs::default();
        // Database creation: a durable empty manifest at authority epoch 1.
        let manifest = fs.link(
            MANIFEST,
            Content::Manifest(Arc::new(Manifest {
                epoch: 1,
                commit: None,
            })),
            OBJECT_BYTES,
        );
        fs.inodes[manifest].len = OBJECT_BYTES;
        fs.fsync(manifest);
        fs.dirsync();
        let w = World {
            config,
            rng,
            fs,
            owners: vec![OwnerStore::new(); config.owners],
            truth: BTreeMap::new(),
            acked: BTreeMap::new(),
            callers: plans
                .into_iter()
                .map(|plan| Caller {
                    plan,
                    next: 0,
                    state: CallerState::Idle,
                })
                .collect(),
            ever_created: BTreeSet::new(),
            trace: Vec::new(),
            fired: BTreeMap::new(),
            report: Report::default(),
            cut_armed: config.cut.is_some(),
            epochs: BTreeSet::from([1]),
        };
        let r = Ram::fresh(&config, 1, 0);
        Self { w, r }
    }

    fn finished(&self) -> bool {
        self.w.callers.iter().all(|caller| {
            caller.next == caller.plan.len() && matches!(caller.state, CallerState::Idle)
        }) && self.r.queues.iter().all(VecDeque::is_empty)
    }

    fn enabled(&self, actions: &mut Vec<Action>) {
        actions.clear();
        for (index, caller) in self.w.callers.iter().enumerate() {
            match &caller.state {
                CallerState::Idle if caller.next < caller.plan.len() => {
                    let op = caller.plan[caller.next].op;
                    if self.r.lookup(op).is_some() || self.r.tail < TAIL_BATCHES {
                        actions.push(Action::Caller(index));
                    }
                }
                CallerState::Idle => {}
                CallerState::Waiting { version, .. } => {
                    if self.r.resolved(*version) {
                        actions.push(Action::Ack(index));
                    }
                }
            }
        }
        for owner in 0..self.w.config.owners {
            if !self.r.queues[owner].is_empty() {
                actions.push(Action::Append(owner));
            }
            if self.w.owners[owner]
                .get(&self.r.epoch)
                .is_some_and(|stream| stream.synced < stream.entries.len())
            {
                actions.push(Action::Sync(owner));
            }
        }
        for (version, state) in &self.r.batches {
            if let BatchState::Appended(index) = state
                && self.publishable(*version, *index)
            {
                actions.push(Action::Publish(*version));
                break;
            }
        }
        let (target, draining) = match &self.r.snap {
            Snap::Draining(target) => (*target, true),
            _ => (self.r.published, false),
        };
        // Dirty cells, and under a pressure drain also lagging clean cells:
        // they advance coverage without encoding (spec §6.1).
        if let Some(cell) = self.r.cells.iter().position(|cell| {
            cell.as_ref().is_some_and(|cell| {
                !cell.queued
                    && cell.flight.is_none()
                    && (cell.missing_through(target) || draining && cell.coverage < target)
            })
        }) {
            actions.push(Action::Queue(cell));
        }
        for (index, flusher) in self.r.flushers.iter().enumerate() {
            if flusher.phase != FlushPhase::Capturing
                || !flusher.queue.is_empty()
                || !flusher.group.is_empty()
            {
                actions.push(Action::Flusher(index));
            }
        }
        let snapshot = match &self.r.snap {
            // Start a root whenever there is a published suffix to retire.
            Snap::Idle => self.r.published > self.r.adopted.floor,
            Snap::Draining(target) => !self
                .r
                .cells
                .iter()
                .flatten()
                .any(|cell| cell.missing_through(*target)),
            Snap::Running(_) => true,
        };
        if snapshot {
            actions.push(Action::Snapshot);
        }
    }

    fn publishable(&self, version: BatchVersion, index: usize) -> bool {
        let owner = self.w.truth[&version].owner;
        let durable = self.w.config.native
            || self.w.owners[owner]
                .get(&version.epoch)
                .is_some_and(|stream| stream.synced > index);
        durable
            && self.r.preds[&version]
                .iter()
                .all(|pred| self.r.resolved(*pred))
    }

    /// One scheduler step. `Ok(true)` when the workload is finished.
    fn step(&mut self, actions: &mut Vec<Action>) -> Result<bool, Halt> {
        if self.finished() {
            return Ok(true);
        }
        self.enabled(actions);
        if actions.is_empty() {
            let unresolved: Vec<_> = self
                .r
                .batches
                .iter()
                .filter(|(_, state)| !matches!(state, BatchState::Published | BatchState::Aborted))
                .map(|(version, state)| {
                    let owner = self.w.truth[version].owner;
                    (
                        version.sequence,
                        *state,
                        self.w.owners[owner]
                            .get(&version.epoch)
                            .map(|s| (s.synced, s.entries.len())),
                        self.r.preds[version]
                            .iter()
                            .map(|pred| (pred.sequence, self.r.batches.get(pred).copied()))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect();
            return Err(diverge(
                "no progress",
                format!(
                    "no enabled action before the workload finished: tail={} published={:?} floor={:?} snap={:?} unresolved={unresolved:?} callers={:?} cells_missing={:?}",
                    self.r.tail,
                    self.r.published,
                    self.r.adopted.floor,
                    std::mem::discriminant(&self.r.snap),
                    self.w
                        .callers
                        .iter()
                        .map(|caller| (caller.next, caller.plan.len(), caller.state.clone()))
                        .collect::<Vec<_>>(),
                    self.r
                        .cells
                        .iter()
                        .enumerate()
                        .filter_map(|(index, cell)| cell
                            .as_ref()
                            .filter(|cell| cell.missing_through(self.r.published))
                            .map(|cell| (index, cell.queued, cell.flight)))
                        .collect::<Vec<_>>(),
                ),
            )
            .into());
        }
        // Snapshot starts compete with all other work at a low rate, except
        // under replay-tail pressure, so writes interleave with every phase.
        let pick = actions[self.w.rng.gen_range(0..actions.len())];
        match pick {
            Action::Caller(caller) => self.caller(caller),
            Action::Ack(caller) => self.ack(caller),
            Action::Append(owner) => self.append(owner),
            Action::Sync(owner) => self.sync(owner),
            Action::Publish(version) => self.publish(version),
            Action::Queue(cell) => {
                let worker = self.worker_of(cell);
                let cell_state = self.r.cell(cell);
                cell_state.queued = true;
                self.r.flushers[worker].queue.push_back(cell);
                Ok(())
            }
            Action::Flusher(worker) => self.flusher(worker),
            Action::Snapshot => self.snapshot(),
        }?;
        Ok(false)
    }

    fn worker_of(&self, cell: usize) -> usize {
        let shard = cell % SHARDS;
        let workers = self.r.flushers.len();
        // Worker w owns shards [floor(wN/P), floor((w+1)N/P)).
        (0..workers)
            .find(|worker| shard < (worker + 1) * SHARDS / workers)
            .unwrap_or(workers - 1)
    }

    // ------------------------------------------------------------ option D

    fn caller(&mut self, index: usize) -> Result<(), Halt> {
        let planned = self.w.callers[index].plan[self.w.callers[index].next].clone();
        let op = planned.op;
        if let Some(version) = self.r.lookup(op) {
            self.w.report.retries += 1;
            self.w.record("retry", Some(version), Some(op), None);
            self.w.callers[index].state = CallerState::Waiting { op, version };
            return Ok(());
        }
        if let Some(acked) = self.w.acked.get(&op) {
            return Err(diverge(
                "identity-free retry gap",
                format!("acknowledged operation {op} at {acked:?} has no identity"),
            )
            .op(op)
            .version(*acked)
            .into());
        }
        let version = BatchVersion {
            epoch: self.r.epoch,
            sequence: self.r.next_seq,
        };
        self.r.next_seq += 1;
        self.r.tail += 1;
        let owner = owner_of(op, self.w.config.owners);
        let mut preds = Vec::new();
        for edit in planned.edits.iter() {
            if let Some(pred) = self.r.key_last[usize::from(edit.key)].replace(version)
                && pred != version
                && !preds.contains(&pred)
            {
                preds.push(pred);
            }
        }
        self.r.preds.insert(version, preds);
        self.w.truth.insert(
            version,
            Truth {
                op,
                edits: planned.edits.clone(),
                owner,
                aborted: false,
                appended: false,
            },
        );
        self.w.record("allocate", Some(version), Some(op), None);
        if self.w.rng.gen_ratio(1, 16) {
            // Pre-READY abort: a resolved sequence gap with no WAL batch. It
            // returns the replay-tail credit it took; no identity remains.
            self.r.batches.insert(version, BatchState::Aborted);
            self.r.tail -= 1;
            if let Some(truth) = self.w.truth.get_mut(&version) {
                truth.aborted = true;
            }
            self.w.report.aborted += 1;
            self.w.record("abort", Some(version), Some(op), None);
            self.w.callers[index].next += 1;
            self.advance_published();
            return Ok(());
        }
        self.r.identities[identity_shard(op)].insert(op, version);
        self.r.batches.insert(version, BatchState::Queued);
        self.r.queues[owner].push_back(version);
        self.w.callers[index].state = CallerState::Waiting { op, version };
        Ok(())
    }

    fn ack(&mut self, index: usize) -> Result<(), Halt> {
        let CallerState::Waiting { op, version } = self.w.callers[index].state else {
            return Ok(());
        };
        let caller = &mut self.w.callers[index];
        caller.state = CallerState::Idle;
        caller.next += 1;
        let aborted = version.epoch == self.r.epoch
            && self.r.batches.get(&version) == Some(&BatchState::Aborted);
        if aborted {
            return Ok(());
        }
        if let Some(previous) = self.w.acked.insert(op, version)
            && previous != version
        {
            return Err(diverge(
                "retry changed version",
                format!("operation {op} acknowledged at {previous:?} and {version:?}"),
            )
            .op(op)
            .version(previous)
            .version(version)
            .into());
        }
        self.w.report.acknowledged += 1;
        self.w.record("ack", Some(version), Some(op), None);
        Ok(())
    }

    fn append(&mut self, owner: usize) -> Result<(), Halt> {
        let Some(version) = self.r.queues[owner].pop_front() else {
            return Ok(());
        };
        let Some(truth) = self.w.truth.get_mut(&version) else {
            return Err(diverge("append of an unknown batch", format!("{version:?}")).into());
        };
        truth.appended = true;
        let entry = WalEntry {
            version,
            op: truth.op,
            edits: truth.edits.clone(),
        };
        let stream = self.w.owners[owner].entry(self.r.epoch).or_default();
        stream.entries.push(entry);
        let index = stream.entries.len() - 1;
        self.r.batches.insert(version, BatchState::Appended(index));
        self.w.fire(Transition::Append, Some(version), None)
    }

    fn sync(&mut self, owner: usize) -> Result<(), Halt> {
        if let Some(stream) = self.w.owners[owner].get_mut(&self.r.epoch) {
            stream.synced = stream.entries.len();
        }
        self.w.fire(Transition::OwnerSync, None, None)
    }

    fn publish(&mut self, version: BatchVersion) -> Result<(), Halt> {
        let edits = self.w.truth[&version].edits.clone();
        for reference in refs_of(version, &edits) {
            self.r
                .cell(cell_of(reference.key.into()))
                .overlay
                .insert(reference);
        }
        self.r.batches.insert(version, BatchState::Published);
        self.r.present.insert(version);
        self.advance_published();
        self.w.fire(Transition::Publication, Some(version), None)
    }

    fn advance_published(&mut self) {
        loop {
            let next = BatchVersion {
                epoch: self.r.epoch,
                sequence: match self.r.published {
                    PrefixCut::Through(last) if last.epoch == self.r.epoch => last.sequence + 1,
                    _ => 1,
                },
            };
            if matches!(
                self.r.batches.get(&next),
                Some(BatchState::Published | BatchState::Aborted)
            ) {
                self.r.published = PrefixCut::Through(next);
            } else {
                break;
            }
        }
    }

    // ------------------------------------------------------------ flushers

    fn flusher(&mut self, worker: usize) -> Result<(), Halt> {
        let phase = self.r.flushers[worker].phase;
        match phase {
            FlushPhase::Capturing => {
                if self.r.flushers[worker].group.len() < GROUP_CELLS
                    && let Some(cell) = self.r.flushers[worker].queue.pop_front()
                {
                    return self.capture(worker, cell);
                }
                self.r.flushers[worker].phase = FlushPhase::Create;
                Ok(())
            }
            FlushPhase::Create => {
                let epoch = self.r.epoch;
                let flusher = &mut self.r.flushers[worker];
                let id = IndexStoreFileId::Segment {
                    authority_epoch: epoch,
                    stream_id: flusher.stream,
                    segment_seq: flusher.next_seq,
                };
                flusher.next_seq += 1;
                let name = id.canonical_name();
                if !self.w.ever_created.insert(id) {
                    return Err(diverge("file id alias", format!("{id:?} created twice"))
                        .name(&name)
                        .into());
                }
                let inode =
                    self.w
                        .fs
                        .create_new(&name, Content::Segment { frames: Vec::new() }, 0)?;
                self.r.flushers[worker].segment = Some((id, inode, 0));
                self.r.flushers[worker].phase = FlushPhase::Writing(0);
                self.w.fire(Transition::SegmentCreate, None, Some(name))
            }
            FlushPhase::Writing(index) => {
                let flusher = &mut self.r.flushers[worker];
                let Some((id, inode, buffered)) = flusher.segment else {
                    return Err(diverge("flusher state", "write without a segment").into());
                };
                let captured = &mut flusher.group[index];
                let len = frame_len(&captured.refs);
                let frame = Frame {
                    offset: buffered,
                    len,
                    cell: captured.cell,
                    cut: captured.target,
                    floor: captured.floor,
                    refs: captured.refs.clone(),
                    digest: digest(&captured.refs, captured.target),
                };
                captured.frame = Some(frame.clone());
                flusher.segment = Some((id, inode, buffered + len));
                if let Content::Segment { frames, .. } = &mut self.w.fs.inodes[inode].content {
                    frames.push(frame);
                }
                flusher.phase = if index + 1 == flusher.group.len() {
                    FlushPhase::Partial
                } else {
                    FlushPhase::Writing(index + 1)
                };
                self.w
                    .fire(Transition::FrameWrite, None, Some(id.canonical_name()))
            }
            FlushPhase::Partial => {
                // The buffered writer's first write reaches the file: a torn
                // frame survives a process crash at this cut.
                let (id, inode, buffered) = self.segment(worker)?;
                self.w.fs.inodes[inode].len = buffered / 2;
                self.r.flushers[worker].phase = FlushPhase::Flush;
                self.w
                    .fire(Transition::FramePartial, None, Some(id.canonical_name()))
            }
            FlushPhase::Flush => {
                let (id, inode, buffered) = self.segment(worker)?;
                self.w.fs.inodes[inode].len = buffered;
                self.r.flushers[worker].phase = if self.w.config.bug(Bug::SkipSegmentSync) {
                    FlushPhase::Install
                } else {
                    FlushPhase::Fsync
                };
                self.w
                    .fire(Transition::GroupFlush, None, Some(id.canonical_name()))
            }
            FlushPhase::Fsync => {
                let (id, inode, _) = self.segment(worker)?;
                self.w.fs.fsync(inode);
                self.r.flushers[worker].phase = FlushPhase::Dirsync;
                self.w
                    .fire(Transition::SegmentFsync, None, Some(id.canonical_name()))
            }
            FlushPhase::Dirsync => {
                let (id, _, _) = self.segment(worker)?;
                self.w.fs.dirsync();
                self.r.flushers[worker].phase = FlushPhase::Install;
                self.w
                    .fire(Transition::SegmentDirsync, None, Some(id.canonical_name()))
            }
            FlushPhase::Install => self.install(worker),
        }
    }

    fn segment(&self, worker: usize) -> Result<(IndexStoreFileId, usize, u64), Halt> {
        self.r.flushers[worker]
            .segment
            .ok_or_else(|| diverge("flusher state", "segment step without a segment").into())
    }

    fn capture(&mut self, worker: usize, cell: usize) -> Result<(), Halt> {
        let target = self.r.published;
        let history = self.r.adopted.history;
        let flight = self.r.flight_ids;
        let state = self.r.cell(cell);
        state.queued = false;
        if state.flight.is_some() {
            return Err(diverge("cell captured twice", format!("cell {cell}")).into());
        }
        if !state.missing_through(target) {
            state.coverage = state.coverage.max(target);
            return self.w.fire(Transition::CleanAdvance, None, None);
        }
        if let Some(base) = &state.base
            && base.cut > target
        {
            return Err(diverge("capture below base cut", format!("cell {cell}")).into());
        }
        let captured: Vec<Ref> = state
            .overlay
            .iter()
            .filter(|reference| target.contains(reference.version.batch))
            .copied()
            .collect();
        let refs = compact(
            state
                .base
                .iter()
                .flat_map(|base| base.refs.iter().copied())
                .chain(captured.iter().copied()),
            history,
        );
        state.flight = Some(flight);
        let token = state.token;
        self.r.flight_ids += 1;
        self.r.flushers[worker].group.push(Captured {
            cell,
            flight,
            token,
            target,
            captured,
            refs: Arc::new(refs),
            floor: history,
            frame: None,
        });
        self.w.record("cell_capture", None, None, None);
        Ok(())
    }

    fn install(&mut self, worker: usize) -> Result<(), Halt> {
        let (id, _, len) = self.segment(worker)?;
        let group = std::mem::take(&mut self.r.flushers[worker].group);
        self.r.sealed.insert(id, len);
        for captured in group {
            let Some(frame) = captured.frame else {
                return Err(diverge("flusher state", "install without a frame").into());
            };
            let state = self.r.cell(captured.cell);
            if state.flight != Some(captured.flight) || state.token != captured.token {
                return Err(
                    diverge("install token mismatch", format!("cell {}", captured.cell)).into(),
                );
            }
            for reference in &captured.captured {
                state.overlay.remove(reference);
            }
            state.base = Some(Base {
                file: id,
                offset: frame.offset,
                len: frame.len,
                digest: frame.digest,
                cut: captured.target,
                floor: captured.floor,
                refs: captured.refs,
            });
            state.coverage = state.coverage.max(captured.target);
            state.flight = None;
            state.token += 1;
        }
        let flusher = &mut self.r.flushers[worker];
        flusher.segment = None;
        flusher.phase = FlushPhase::Capturing;
        check_reads(&self.w, &self.r)?;
        self.w
            .fire(Transition::Install, None, Some(id.canonical_name()))
    }

    // ------------------------------------------------------------ metadata actor

    fn snapshot(&mut self) -> Result<(), Halt> {
        match std::mem::replace(&mut self.r.snap, Snap::Idle) {
            Snap::Idle => {
                // Pressure drain (spec §6.1): fix T and cover every cell first.
                if self.r.tail >= TAIL_BATCHES || self.w.rng.gen_ratio(1, 2) {
                    self.r.snap = Snap::Draining(self.r.published);
                } else {
                    self.begin(self.r.published);
                }
                Ok(())
            }
            Snap::Draining(target) => {
                self.begin(target);
                Ok(())
            }
            Snap::Running(mut capture) => {
                let result = self.snap_step(&mut capture);
                if !capture.steps.is_empty() || capture.next < capture.boundary.len() {
                    self.r.snap = Snap::Running(capture);
                }
                result
            }
        }
    }

    fn begin(&mut self, target: PrefixCut) {
        let boundary: Vec<usize> = (0..CELLS)
            .filter(|cell| self.r.cells[*cell].is_some())
            .collect();
        self.w.record("cell_root_capture", None, None, None);
        self.r.snap = Snap::Running(Box::new(Capture {
            generation: self.r.adopted.generation + 1,
            target,
            boundary,
            next: 0,
            descs: Vec::new(),
            floor: target,
            max_cut: target,
            history: self.r.adopted.history,
            root: None,
            token: None,
            history_root: None,
            credited: false,
            steps: VecDeque::from([SnapStep::Slice]),
        }));
    }

    fn snap_step(&mut self, capture: &mut Capture) -> Result<(), Halt> {
        let Some(step) = capture.steps.pop_front() else {
            return Ok(());
        };
        let epoch = self.r.epoch;
        let generation = capture.generation;
        match step {
            SnapStep::Slice => {
                let end = (capture.next + ROOT_SLICE).min(capture.boundary.len());
                for cell in capture.boundary[capture.next..end].iter().copied() {
                    let target = capture.target;
                    let state = self.r.cell(cell);
                    if !state.missing_through(target) {
                        state.coverage = state.coverage.max(target);
                    }
                    let desc = Desc {
                        cell,
                        coverage: state.coverage.min(target),
                        base: state.base.clone(),
                    };
                    capture.descs.push(desc);
                }
                capture.next = end;
                if capture.next < capture.boundary.len() {
                    capture.steps.push_front(SnapStep::Slice);
                } else {
                    self.finish_capture(capture)?;
                }
                self.w.fire(Transition::RootSlice, None, None)
            }
            SnapStep::Proof(owner) => {
                // The owner durable-prefix proof covers every address through
                // U, in every retained stream of that owner.
                for stream in self.w.owners[owner].values_mut() {
                    if stream
                        .entries
                        .iter()
                        .any(|entry| capture.max_cut.contains(entry.version))
                    {
                        stream.synced = stream.entries.len();
                    }
                }
                self.w.fire(Transition::OwnerProof, None, None)
            }
            SnapStep::HistoryAppend => {
                let records: Vec<(u64, BatchVersion)> = self
                    .retired(capture.floor)
                    .map(|(v, t)| (t.op, v))
                    .collect();
                if self.w.fs.inode(HISTORY_LOG).is_none() {
                    self.w
                        .fs
                        .create_new(HISTORY_LOG, Content::HistoryLog(Vec::new()), 0)?;
                }
                let inode = self.w.fs.live[HISTORY_LOG];
                let node = &mut self.w.fs.inodes[inode];
                if let Content::HistoryLog(log) = &mut node.content {
                    // Records already logged by an earlier unadopted attempt
                    // are not repeated: the log is keyed by version.
                    for record in records {
                        if !log.contains(&record) {
                            log.push(record);
                        }
                    }
                    node.len = log.len() as u64 * HISTORY_RECORD;
                }
                self.w
                    .fire(Transition::HistoryAppend, None, Some(HISTORY_LOG.into()))
            }
            SnapStep::HistorySync => {
                let inode = self.w.fs.live[HISTORY_LOG];
                self.w.fs.fsync(inode);
                self.w
                    .fire(Transition::HistorySync, None, Some(HISTORY_LOG.into()))
            }
            SnapStep::HistoryRoot => {
                let records = match &self.w.fs.inode(HISTORY_LOG).map(|inode| &inode.content) {
                    Some(Content::HistoryLog(log)) => log.len(),
                    _ => 0,
                };
                let name = history_root_name(generation);
                let temp = staging(&name);
                let inode = self.w.fs.create(
                    &temp,
                    Content::HistoryRoot {
                        generation,
                        records,
                    },
                    OBJECT_BYTES,
                );
                self.w.fs.inodes[inode].len = OBJECT_BYTES;
                self.w.fs.fsync(inode);
                if self.w.fs.inode(&name).is_some() {
                    return Err(diverge("metadata generation name collision", &name)
                        .name(&name)
                        .into());
                }
                self.w.fs.rename(&temp, &name)?;
                capture.history_root = Some((name.clone(), generation));
                // Its directory entry is made durable by the root's directory
                // fsync in the same directory.
                self.w.fire(Transition::HistoryRoot, None, Some(name))
            }
            SnapStep::RootTemp => {
                let root = self.build_root(capture)?;
                let name = root_name(epoch, generation);
                let inode =
                    self.w
                        .fs
                        .create(&staging(&name), Content::Root(root.clone()), OBJECT_BYTES);
                self.w.fs.inodes[inode].len = OBJECT_BYTES;
                capture.root = Some(root);
                if self.w.config.bug(Bug::CreditBeforeAdoption) {
                    self.return_credits(capture)?;
                }
                self.w.fire(Transition::RootTemp, None, Some(name))
            }
            SnapStep::RootFsync => {
                let name = root_name(epoch, generation);
                let inode = self.w.fs.live[&staging(&name)];
                self.w.fs.fsync(inode);
                self.w.fire(Transition::RootFsync, None, Some(name))
            }
            SnapStep::RootRename => {
                let name = root_name(epoch, generation);
                if self.w.fs.inode(&name).is_some() {
                    return Err(diverge("metadata generation name collision", &name)
                        .name(&name)
                        .into());
                }
                self.w.fs.rename(&staging(&name), &name)?;
                self.w.fire(Transition::RootRename, None, Some(name))
            }
            SnapStep::RootDirsync => {
                self.w.fs.dirsync();
                self.w.fire(
                    Transition::RootDirsync,
                    None,
                    Some(root_name(epoch, generation)),
                )
            }
            SnapStep::WrapperTemp => {
                let wrapper = self.build_wrapper(capture)?;
                let name = wrapper_name(epoch, generation);
                let inode =
                    self.w
                        .fs
                        .create(&staging(&name), Content::Wrapper(wrapper), OBJECT_BYTES);
                self.w.fs.inodes[inode].len = OBJECT_BYTES;
                self.w.fire(Transition::WrapperTemp, None, Some(name))
            }
            SnapStep::WrapperFsync => {
                let name = wrapper_name(epoch, generation);
                let inode = self.w.fs.live[&staging(&name)];
                self.w.fs.fsync(inode);
                self.w.fire(Transition::WrapperFsync, None, Some(name))
            }
            SnapStep::WrapperRename => {
                let name = wrapper_name(epoch, generation);
                if self.w.fs.inode(&name).is_some() {
                    return Err(diverge("metadata generation name collision", &name)
                        .name(&name)
                        .into());
                }
                self.w.fs.rename(&staging(&name), &name)?;
                self.w.fire(Transition::WrapperRename, None, Some(name))
            }
            SnapStep::WrapperDirsync => {
                self.w.fs.dirsync();
                self.w.fire(
                    Transition::WrapperDirsync,
                    None,
                    Some(wrapper_name(epoch, generation)),
                )
            }
            SnapStep::ManifestTemp => {
                // Revalidate: one actor, no lifecycle change; F and H only grow.
                if capture.floor < self.r.adopted.floor || capture.history < self.r.adopted.history
                {
                    return Err(diverge("non-monotonic root", "F or H moved backwards").into());
                }
                let manifest = Manifest {
                    epoch,
                    commit: Some(Commit {
                        generation,
                        root_name: root_name(epoch, generation),
                        wrapper_name: wrapper_name(epoch, generation),
                    }),
                };
                let inode = self.w.fs.create(
                    &staging(MANIFEST),
                    Content::Manifest(Arc::new(manifest)),
                    OBJECT_BYTES,
                );
                self.w.fs.inodes[inode].len = OBJECT_BYTES;
                self.w
                    .fire(Transition::ManifestTemp, None, Some(MANIFEST.into()))
            }
            SnapStep::ManifestFsync => {
                let inode = self.w.fs.live[&staging(MANIFEST)];
                self.w.fs.fsync(inode);
                self.w
                    .fire(Transition::ManifestFsync, None, Some(MANIFEST.into()))
            }
            SnapStep::ManifestRename => {
                self.w.fs.rename(&staging(MANIFEST), MANIFEST)?;
                if self.w.config.bug(Bug::GcBeforeManifestSync) {
                    let mut steps = VecDeque::from([SnapStep::GcCheck]);
                    steps.extend(capture.steps.drain(..));
                    capture.steps = steps;
                    // The defect also sweeps as if the new root were applied.
                    self.apply_history(capture);
                }
                self.w
                    .fire(Transition::ManifestRename, None, Some(MANIFEST.into()))
            }
            SnapStep::ManifestDirsync => {
                self.w.fs.dirsync();
                self.w.report.roots_adopted += 1;
                self.w
                    .fire(Transition::ManifestDirsync, None, Some(MANIFEST.into()))
            }
            SnapStep::ApplyHistory => {
                self.apply_history(capture);
                check_reads(&self.w, &self.r)?;
                self.w.fire(Transition::ApplyHistory, None, None)
            }
            SnapStep::ApplyAuthority => {
                // Publish the through-F retry authority before any identity
                // removal (spec §9.2 step 2).
                let retired: Vec<(u64, BatchVersion)> = self
                    .w
                    .truth
                    .iter()
                    .filter(|(version, truth)| {
                        !truth.aborted
                            && capture.floor.contains(**version)
                            && self.r.present.contains(version)
                    })
                    .map(|(version, truth)| (truth.op, *version))
                    .collect();
                for (op, version) in retired {
                    self.r.authority.insert(op, version);
                }
                check_identities(&self.w, &self.r)?;
                self.w.fire(Transition::ApplyAuthority, None, None)
            }
            SnapStep::ApplyLedger => self.w.fire(Transition::ApplyLedger, None, None),
            SnapStep::IdentityRemove(shard) => {
                let floor = capture.floor;
                self.r.identities[shard].retain(|_, version| !floor.contains(*version));
                check_identities(&self.w, &self.r)?;
                self.w.fire(Transition::IdentityRemove, None, None)
            }
            SnapStep::CreditReturn => {
                if !capture.credited {
                    self.return_credits(capture)?;
                }
                self.w.fire(Transition::CreditReturn, None, None)
            }
            SnapStep::GcCheck => {
                let retained: BTreeSet<IndexStoreFileId> = self
                    .r
                    .adopted
                    .files
                    .iter()
                    .map(|file| file.id)
                    .chain(
                        self.r
                            .cells
                            .iter()
                            .flatten()
                            .filter_map(|cell| cell.base.as_ref().map(|base| base.file)),
                    )
                    .chain(
                        self.r
                            .flushers
                            .iter()
                            .filter_map(|f| f.segment.map(|s| s.0)),
                    )
                    .collect();
                let adopted: Vec<String> = self
                    .r
                    .adopted
                    .commit
                    .iter()
                    .flat_map(|commit| [commit.root_name.clone(), commit.wrapper_name.clone()])
                    .chain(capture.history_root.iter().map(|(name, _)| name.clone()))
                    .collect();
                let mut unlinks = Vec::new();
                for name in self.w.fs.live.keys() {
                    let obsolete = match IndexStoreFileId::parse_canonical(name) {
                        Some(id) => !retained.contains(&id),
                        None => {
                            metadata_output(name)
                                && !name.starts_with('.')
                                && !adopted.contains(name)
                        }
                    };
                    if obsolete {
                        unlinks.push(SnapStep::GcUnlink(name.clone()));
                    }
                }
                let rest: Vec<SnapStep> = capture.steps.drain(..).collect();
                capture.steps.extend(unlinks);
                capture.steps.push_back(SnapStep::GcDirsync);
                capture.steps.extend(rest);
                self.w.fire(Transition::GcCheck, None, None)
            }
            SnapStep::GcUnlink(name) => {
                self.w.fs.unlink(&name);
                self.w.fire(Transition::GcUnlink, None, Some(name))
            }
            SnapStep::GcDirsync => {
                self.w.fs.dirsync();
                self.w.fire(Transition::GcDirsync, None, None)
            }
        }
    }

    fn retired(&self, floor: PrefixCut) -> impl Iterator<Item = (BatchVersion, &Truth)> + '_ {
        let old = self.r.adopted.floor;
        self.w.truth.iter().filter_map(move |(version, truth)| {
            (!truth.aborted
                && floor.contains(*version)
                && !old.contains(*version)
                && self.r.present.contains(version))
            .then_some((*version, truth))
        })
    }

    fn finish_capture(&mut self, capture: &mut Capture) -> Result<(), Halt> {
        let coverages = capture.descs.iter().map(|desc| desc.coverage);
        capture.floor = if self.w.config.bug(Bug::MaxCoverageFloor) {
            coverages.max().unwrap_or(capture.target)
        } else {
            coverages.min().unwrap_or(capture.target)
        };
        capture.max_cut = capture
            .descs
            .iter()
            .filter_map(|desc| desc.base.as_ref().map(|base| base.cut))
            .fold(capture.target, PrefixCut::max);
        if capture.floor < self.r.adopted.floor {
            // Spec §7.3 step 5 (C5a `run_checkpoint`): a capture whose F is
            // below the adopted F is rejected before any output; nothing is
            // adopted. A cell created after the previous root's boundary
            // keeps the old floor until a drained root covers it.
            capture.steps.clear();
            self.w.report.abandoned_roots += 1;
            self.w.record("cell_root_abandoned", None, None, None);
            return Ok(());
        }
        if self.w.rng.gen_ratio(1, 2) {
            capture.history = capture.history.max(floor_of(capture.floor));
        }
        if capture.descs.iter().any(|desc| {
            desc.base
                .as_ref()
                .is_some_and(|base| base.cut > desc.coverage)
        }) {
            self.w.report.coverage_behind_frame += 1;
        }
        let mut steps: VecDeque<SnapStep> =
            (0..self.w.config.owners).map(SnapStep::Proof).collect();
        if self.w.config.on_disk_history {
            steps.extend([
                SnapStep::HistoryAppend,
                SnapStep::HistorySync,
                SnapStep::HistoryRoot,
            ]);
        }
        steps.extend([
            SnapStep::RootTemp,
            SnapStep::RootFsync,
            SnapStep::RootRename,
            SnapStep::RootDirsync,
            SnapStep::WrapperTemp,
            SnapStep::WrapperFsync,
            SnapStep::WrapperRename,
            SnapStep::WrapperDirsync,
            SnapStep::ManifestTemp,
            SnapStep::ManifestFsync,
            SnapStep::ManifestRename,
            SnapStep::ManifestDirsync,
            SnapStep::ApplyHistory,
        ]);
        let identity = (0..IDENTITY_SHARDS).map(SnapStep::IdentityRemove);
        if self.w.config.bug(Bug::IdentityBeforeAuthority) {
            steps.extend(identity);
            steps.extend([SnapStep::ApplyAuthority, SnapStep::ApplyLedger]);
        } else {
            steps.extend([SnapStep::ApplyAuthority, SnapStep::ApplyLedger]);
            steps.extend(identity);
        }
        steps.extend([SnapStep::CreditReturn, SnapStep::GcCheck]);
        capture.steps = steps;
        Ok(())
    }

    fn build_root(&mut self, capture: &Capture) -> Result<Arc<CellRoot>, Halt> {
        let mut files = BTreeMap::new();
        let mut cells = Vec::with_capacity(capture.descs.len());
        let mut descs = capture.descs.clone();
        descs.sort_by_key(|desc| desc.cell);
        for desc in &descs {
            let base = match &desc.base {
                None => None,
                Some(base) => {
                    let sealed = *self.r.sealed.get(&base.file).ok_or_else(|| {
                        diverge(
                            "root references an unsealed file",
                            format!("{:?}", base.file),
                        )
                    })?;
                    files.insert(base.file, sealed);
                    Some(RootCellBase {
                        file: base.file,
                        offset: base.offset,
                        framed_len: base.len,
                        digest: base.digest,
                        frame_cut: base.cut,
                        frame_floor: base.floor,
                    })
                }
            };
            cells.push(RootCell {
                keyspace: KeySpace(0),
                cell: CellId::Integer(desc.cell),
                coverage: desc.coverage,
                base,
            });
        }
        let root = CellRoot {
            header: CellRootHeader {
                format: 5,
                flags: 0,
                database: DATABASE,
                schema: SCHEMA,
                authority_epoch: self.r.epoch,
                snapshot_generation: capture.generation,
                lifecycle_generation: 0,
                directory_target: capture.target,
                replay_floor: capture.floor,
                max_frame_cut: capture.max_cut,
                history_floor: capture.history,
            },
            files: files
                .into_iter()
                .map(|(id, sealed_len)| RootFile {
                    id,
                    name: id.canonical_name(),
                    sealed_len,
                })
                .collect(),
            cells,
        };
        // The real structural oracle of spec §7.1 (C0) on every root.
        root.check(DATABASE, SCHEMA).map_err(|error| {
            diverge(
                "CellRoot::check rejected a built root",
                format!("{error:?}"),
            )
        })?;
        Ok(Arc::new(root))
    }

    fn build_wrapper(&mut self, capture: &mut Capture) -> Result<Arc<Wrapper>, Halt> {
        let floor = capture.floor;
        let mut streams = BTreeMap::new();
        for (owner, store) in self.w.owners.iter().enumerate() {
            for (owner_epoch, stream) in store {
                let through = stream
                    .entries
                    .iter()
                    .take_while(|entry| floor.contains(entry.version))
                    .count();
                if stream.entries[through..]
                    .iter()
                    .any(|entry| floor.contains(entry.version))
                {
                    return Err(diverge("owner stream order", format!("owner {owner}")).into());
                }
                streams.insert((owner, *owner_epoch), through.checked_sub(1));
            }
        }
        let recent: Vec<(u64, BatchVersion)> = if self.w.config.on_disk_history {
            Vec::new()
        } else {
            self.w
                .truth
                .iter()
                .filter(|(version, truth)| {
                    !truth.aborted && floor.contains(**version) && self.r.present.contains(version)
                })
                .map(|(version, truth)| (truth.op, *version))
                .collect()
        };
        if recent.len() > RECENT_WINDOW {
            return Err(diverge("recent window overflow", recent.len().to_string()).into());
        }
        let (batches, bytes) = self
            .retired(floor)
            .fold((0, 0), |(batches, bytes), (_, truth)| {
                (batches + 1, bytes + encoded_bytes(&truth.edits))
            });
        let token = RetirementToken {
            generation: capture.generation,
            from: self.r.adopted.floor,
            through: floor,
            batches,
            encoded_bytes: bytes,
            identities: batches,
        };
        capture.token = Some(token);
        Ok(Arc::new(Wrapper {
            generation: capture.generation,
            root_name: root_name(self.r.epoch, capture.generation),
            floor,
            history_floor: capture.history,
            streams,
            recent,
            history_root: capture.history_root.clone(),
            token,
        }))
    }

    fn apply_history(&mut self, capture: &Capture) {
        let Some(root) = &capture.root else { return };
        self.r.adopted = Adopted {
            generation: capture.generation,
            floor: capture.floor,
            history: capture.history,
            max_cut: capture.max_cut,
            files: root.files.clone(),
            commit: Some(Commit {
                generation: capture.generation,
                root_name: root_name(self.r.epoch, capture.generation),
                wrapper_name: wrapper_name(self.r.epoch, capture.generation),
            }),
        };
    }

    fn return_credits(&mut self, capture: &mut Capture) -> Result<(), Halt> {
        let token = match capture.token {
            Some(token) => token,
            None => {
                let (batches, bytes) =
                    self.retired(capture.floor)
                        .fold((0, 0), |(batches, bytes), (_, truth)| {
                            (batches + 1, bytes + encoded_bytes(&truth.edits))
                        });
                RetirementToken {
                    generation: capture.generation,
                    from: self.r.adopted.floor,
                    through: capture.floor,
                    batches,
                    encoded_bytes: bytes,
                    identities: batches,
                }
            }
        };
        let credits = self
            .r
            .ledger
            .apply(&token)
            .map_err(|error| diverge("retirement ledger rejected a token", error.to_string()))?
            .ok_or_else(|| diverge("credit returned twice", format!("{token:?}")))?;
        // A second apply of the same generation returns nothing.
        if self
            .r
            .ledger
            .apply(&token)
            .map_err(|error| diverge("retirement ledger re-apply", error.to_string()))?
            .is_some()
        {
            return Err(diverge("credit returned twice", format!("{token:?}")).into());
        }
        self.r.tail = self
            .r
            .tail
            .checked_sub(credits.replay_batches)
            .ok_or_else(|| diverge("replay-tail credit underflow", format!("{token:?}")))?;
        capture.credited = true;
        Ok(())
    }
}

impl Ram {
    fn fresh(config: &ModelConfig, epoch: u64, first_sequence: u64) -> Self {
        Self {
            epoch,
            next_seq: 1,
            batches: BTreeMap::new(),
            preds: BTreeMap::new(),
            key_last: vec![None; KEYS],
            queues: vec![VecDeque::new(); config.owners],
            identities: vec![BTreeMap::new(); IDENTITY_SHARDS],
            authority: BTreeMap::new(),
            present: BTreeSet::new(),
            published: PrefixCut::BeforeAll,
            cells: vec![None; CELLS],
            flushers: (0..config.flushers())
                .map(|stream| Flusher {
                    stream: stream as u16,
                    next_seq: first_sequence,
                    queue: VecDeque::new(),
                    phase: FlushPhase::Capturing,
                    group: Vec::new(),
                    segment: None,
                })
                .collect(),
            flight_ids: 0,
            snap: Snap::Idle,
            adopted: Adopted {
                generation: 0,
                floor: PrefixCut::BeforeAll,
                history: None,
                max_cut: PrefixCut::BeforeAll,
                files: Vec::new(),
                commit: None,
            },
            ledger: RetirementLedger {
                generation: 0,
                floor: PrefixCut::BeforeAll,
            },
            tail: 0,
            sealed: BTreeMap::new(),
        }
    }
}

// ---------------------------------------------------------------- live invariants

/// Reads through base plus every overlay source equal the published log,
/// at the committed history floor (spec §11.2: no partial batch, no lost or
/// resurrected row during freeze, encoding and install).
fn check_reads(w: &World, r: &Ram) -> Result<(), Divergence> {
    for key in 0..KEYS {
        let expected = compact(
            r.present
                .iter()
                .flat_map(|version| refs_of(*version, &w.truth[version].edits))
                .filter(|reference| usize::from(reference.key) == key),
            r.adopted.history,
        );
        let actual = compact(r.history(key), r.adopted.history);
        if expected != actual {
            return Err(diverge(
                "live read differs from published log",
                format!("key {key} expected {expected:?} read {actual:?}"),
            ));
        }
    }
    Ok(())
}

/// Every acknowledged identity stays findable (spec §9.2 step 4).
fn check_identities(w: &World, r: &Ram) -> Result<(), Divergence> {
    for (op, version) in &w.acked {
        if r.lookup(*op) != Some(*version) {
            return Err(diverge(
                "identity-free retry gap",
                format!("acknowledged operation {op} at {version:?} is not findable"),
            )
            .op(*op)
            .version(*version));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- recovery

struct Recovery<'a> {
    config: &'a ModelConfig,
    fs: Fs,
    owners: &'a [OwnerStore],
    ever: &'a BTreeSet<IndexStoreFileId>,
    created: Vec<IndexStoreFileId>,
    trace: Vec<TraceEvent>,
    trace_base: usize,
    cut: Option<CrashCut>,
    counts: BTreeMap<Transition, usize>,
    report: Report,
    real_store: bool,
    /// Version → owner of every issued batch, for split-batch counters.
    multi_epoch: bool,
}

impl Recovery<'_> {
    fn fire(&mut self, transition: Transition, file: Option<String>) -> Result<(), Halt> {
        self.trace.push(TraceEvent {
            index: self.trace_base + self.trace.len(),
            name: transition.name(),
            version: None,
            op: None,
            file,
        });
        *self.report.fired.entry(transition.name()).or_default() += 1;
        let count = self.counts.entry(transition).or_default();
        *count += 1;
        if let Some(cut) = self.cut
            && cut.at == transition
            && *count == cut.occurrence
        {
            return Err(Halt::Crash);
        }
        Ok(())
    }

    fn manifest(&self) -> Result<Arc<Manifest>, Divergence> {
        match self.fs.object(MANIFEST) {
            Some(Content::Manifest(manifest)) => Ok(manifest.clone()),
            _ => Err(diverge("authority manifest missing or torn", MANIFEST).name(MANIFEST)),
        }
    }

    fn run(&mut self) -> Result<(Ram, PrefixCut), Halt> {
        let manifest = self.manifest()?;
        let mut root: Option<Arc<CellRoot>> = None;
        let mut wrapper: Option<Arc<Wrapper>> = None;
        let mut authority = BTreeMap::new();
        if let Some(commit) = &manifest.commit {
            match self.fs.object(&commit.root_name) {
                Some(Content::Root(found))
                    if found.header.snapshot_generation == commit.generation =>
                {
                    root = Some(found.clone());
                }
                _ => {
                    return Err(diverge("adopted root missing or torn", &commit.root_name)
                        .name(&commit.root_name)
                        .into());
                }
            }
            match self.fs.object(&commit.wrapper_name) {
                Some(Content::Wrapper(found))
                    if found.generation == commit.generation
                        && found.root_name == commit.root_name =>
                {
                    wrapper = Some(found.clone());
                }
                _ => {
                    return Err(
                        diverge("adopted wrapper missing or torn", &commit.wrapper_name)
                            .name(&commit.wrapper_name)
                            .into(),
                    );
                }
            }
        }
        if let Some(root) = &root {
            root.check(DATABASE, SCHEMA).map_err(|error| {
                diverge(
                    "CellRoot::check rejected the adopted root",
                    format!("{error:?}"),
                )
            })?;
        }
        let floor = root
            .as_ref()
            .map_or(PrefixCut::BeforeAll, |root| root.header.replay_floor);
        let history = root.as_ref().and_then(|root| root.header.history_floor);
        if let Some(wrapper) = &wrapper {
            // The retirement token is bound to the adopted generation and F.
            if wrapper.floor != floor
                || wrapper.history_floor != history
                || wrapper.token.generation != wrapper.generation
                || wrapper.token.through != floor
            {
                return Err(diverge(
                    "wrapper and root cuts differ",
                    format!("{:?}", wrapper.floor),
                )
                .into());
            }
            for (op, version) in &wrapper.recent {
                authority.insert(*op, *version);
            }
            if let Some((name, generation)) = &wrapper.history_root {
                let records = match self.fs.object(name) {
                    Some(Content::HistoryRoot {
                        generation: found,
                        records,
                    }) if found == generation => *records,
                    _ => {
                        return Err(diverge("receipt history root missing or torn", name)
                            .name(name)
                            .into());
                    }
                };
                let log = self.fs.inode(HISTORY_LOG);
                let complete = log.map_or(0, |log| (log.len / HISTORY_RECORD) as usize);
                let Some(Content::HistoryLog(entries)) = log.map(|log| &log.content) else {
                    return Err(diverge("receipt history log missing", HISTORY_LOG)
                        .name(HISTORY_LOG)
                        .into());
                };
                if complete < records {
                    return Err(diverge(
                        "receipt history truncated below its root",
                        format!("{complete} < {records}"),
                    )
                    .name(HISTORY_LOG)
                    .into());
                }
                // Only records through F seed retry authority; a log can
                // hold later records of an unadopted attempt.
                for (op, version) in &entries[..records] {
                    if floor.contains(*version) {
                        authority.insert(*op, *version);
                    }
                }
            }
        }
        // Reserve the new authority epoch durably before any new output.
        let epoch = manifest.epoch + 1;
        let staged = staging(MANIFEST);
        let inode = self.fs.create(
            &staged,
            Content::Manifest(Arc::new(Manifest {
                epoch,
                commit: manifest.commit.clone(),
            })),
            OBJECT_BYTES,
        );
        self.fs.inodes[inode].len = OBJECT_BYTES;
        self.fs.fsync(inode);
        self.fs.rename(&staged, MANIFEST)?;
        self.fs.dirsync();
        self.fire(Transition::EpochReserve, Some(MANIFEST.into()))?;
        // Orphan scan: list every canonical name before deleting anything.
        let files: Vec<RootFile> = root.as_ref().map_or(Vec::new(), |root| root.files.clone());
        for file in &files {
            match self.fs.inode(&file.name) {
                Some(inode) if inode.len == file.sealed_len => {}
                _ => {
                    return Err(diverge("root segment missing or short", &file.name)
                        .name(&file.name)
                        .into());
                }
            }
        }
        let mut next_sequence = None;
        let mut orphans = Vec::new();
        let mut orphan_segments = Vec::new();
        let adopted_names: Vec<String> = manifest
            .commit
            .iter()
            .flat_map(|commit| [commit.root_name.clone(), commit.wrapper_name.clone()])
            .chain(
                wrapper
                    .as_ref()
                    .and_then(|wrapper| wrapper.history_root.as_ref())
                    .map(|(name, _)| name.clone()),
            )
            .collect();
        for name in self.fs.live.keys() {
            if let Some(id) = IndexStoreFileId::parse_canonical(name) {
                if let IndexStoreFileId::Segment { segment_seq, .. } = id {
                    next_sequence = next_sequence.max(Some(segment_seq));
                }
                if !files.iter().any(|file| file.id == id) {
                    orphans.push(name.clone());
                    orphan_segments.push(id);
                }
            } else if metadata_output(name) && !adopted_names.contains(name) {
                orphans.push(name.clone());
            }
        }
        let next_sequence = next_sequence.map_or(0, |max| max + 1);
        if self.real_store {
            self.real_orphan_scan(epoch, &files, &orphan_segments, next_sequence)?;
        }
        self.fire(Transition::OrphanScan, None)?;
        if !self.config.bug(Bug::NoOrphanCleanup) && !orphans.is_empty() {
            for name in &orphans {
                self.fs.unlink(name);
                self.report.orphans_deleted += 1;
                self.fire(Transition::OrphanDelete, Some(name.clone()))?;
            }
            self.fs.dirsync();
            self.fire(Transition::OrphanDirsync, None)?;
        }
        // Load bases: every referenced range must authenticate.
        let mut ram = Ram::fresh(self.config, epoch, next_sequence);
        ram.adopted = Adopted {
            generation: manifest
                .commit
                .as_ref()
                .map_or(0, |commit| commit.generation),
            floor,
            history,
            max_cut: root
                .as_ref()
                .map_or(PrefixCut::BeforeAll, |root| root.header.max_frame_cut),
            files: files.clone(),
            commit: manifest.commit.clone(),
        };
        ram.ledger = RetirementLedger {
            generation: ram.adopted.generation,
            floor,
        };
        ram.authority = authority;
        ram.published = floor;
        for file in &files {
            ram.sealed.insert(file.id, file.sealed_len);
        }
        if let Some(root) = &root {
            for entry in &root.cells {
                let CellId::Integer(cell) = entry.cell else {
                    return Err(diverge("cell id kind", format!("{:?}", entry.cell)).into());
                };
                let mut state = Cell::new(entry.coverage);
                if let Some(base) = &entry.base {
                    state.base = Some(self.load_frame(base, cell)?);
                }
                ram.cells[cell] = Some(state);
            }
        }
        // Mixed-cut replay of whole authenticated batches above F.
        let mut entries: Vec<(BatchVersion, &WalEntry)> = Vec::new();
        let owners = self.owners;
        for (owner, store) in owners.iter().enumerate() {
            for (owner_epoch, stream) in store {
                let skip = wrapper
                    .as_ref()
                    .and_then(|wrapper| wrapper.streams.get(&(owner, *owner_epoch)).copied())
                    .flatten()
                    .map_or(0, |last| last + 1);
                for entry in stream.entries.iter().skip(skip) {
                    entries.push((entry.version, entry));
                }
            }
        }
        entries.sort_by_key(|(version, _)| *version);
        if let Some(pair) = entries.windows(2).find(|pair| pair[0].0 == pair[1].0) {
            return Err(diverge(
                "duplicate version across streams",
                format!("{:?}", pair[0].0),
            )
            .version(pair[0].0)
            .into());
        }
        let mut matched: BTreeSet<(usize, Ref)> = BTreeSet::new();
        let mut known: BTreeMap<u64, BatchVersion> = BTreeMap::new();
        let mut replayed = 0;
        let mut last = floor;
        for (version, entry) in entries {
            if floor.contains(version) {
                continue;
            }
            if let Some(other) = ram
                .authority
                .get(&entry.op)
                .or_else(|| known.get(&entry.op))
            {
                return Err(diverge(
                    "duplicate batch identity",
                    format!("operation {} at {other:?} and {version:?}", entry.op),
                )
                .op(entry.op)
                .version(version)
                .version(*other)
                .into());
            }
            let plan = match &root {
                Some(root) => root.replay_plan(version, &members(&entry.edits)),
                None => Some(vec![MemberReplay::Apply; entry.edits.len()]),
            }
            .ok_or_else(|| {
                diverge(
                    "replay plan skipped a batch above F",
                    format!("{version:?}"),
                )
            })?;
            let covered = plan
                .iter()
                .filter(|m| **m == MemberReplay::CompareBase)
                .count();
            if covered == plan.len() {
                self.report.fully_covered += 1;
            } else if covered > 0 {
                self.report.split_batches += 1;
                if self.multi_epoch {
                    self.report.multi_epoch_split += 1;
                }
            }
            if covered > 0 && self.config.bug(Bug::SkipCoveredBatch) {
                continue;
            }
            for (reference, action) in refs_of(version, &entry.edits).zip(&plan) {
                let cell = cell_of(reference.key.into());
                match action {
                    MemberReplay::CompareBase => {
                        let present = ram.cells[cell]
                            .as_ref()
                            .and_then(|state| state.base.as_ref())
                            .is_some_and(|base| base.refs.contains(&reference));
                        let compacted = history.is_some_and(|floor| reference.version <= floor);
                        if !present && !compacted {
                            return Err(diverge(
                                "covered member absent from its base",
                                format!("{reference:?} in cell {cell}"),
                            )
                            .version(version)
                            .into());
                        }
                        matched.insert((cell, reference));
                    }
                    MemberReplay::Apply => {
                        ram.cell(cell).overlay.insert(reference);
                    }
                }
            }
            known.insert(entry.op, version);
            ram.present.insert(version);
            replayed += 1;
            last = PrefixCut::Through(version);
            let resident: usize = ram
                .cells
                .iter()
                .flatten()
                .map(|cell| cell.overlay.len())
                .sum();
            if resident > RESIDENT_REFS {
                self.offline_flush(&mut ram, epoch, last)?;
            }
        }
        // End witness: every base reference above F had its whole batch.
        for (cell, state) in ram.cells.iter().enumerate() {
            let Some(state) = state else { continue };
            let Some(base) = &state.base else { continue };
            for reference in base.refs.iter() {
                let witnessed = floor.contains(reference.version.batch)
                    || matched.contains(&(cell, *reference))
                    || is_offline(base.file)
                        && known.values().any(|v| *v == reference.version.batch);
                if !witnessed {
                    return Err(diverge(
                        "base reference above F without its suffix batch",
                        format!("{reference:?} in cell {cell}"),
                    )
                    .version(reference.version.batch)
                    .into());
                }
            }
        }
        if replayed > TAIL_BATCHES {
            return Err(diverge(
                "replay tail above bound",
                format!("{replayed} batches above F, bound {TAIL_BATCHES}"),
            )
            .into());
        }
        for (op, version) in known {
            ram.identities[identity_shard(op)].insert(op, version);
        }
        ram.tail = replayed;
        ram.published = last;
        // Batches at or below F stay visible through bases and the authority.
        let present_floor: Vec<BatchVersion> = ram.authority.values().copied().collect();
        ram.present.extend(present_floor);
        // Store before root before GC (spec §4.3): after a reopen only the
        // adopted metadata, its referenced segments and this reopen's own
        // offline outputs remain; every older unadopted output is gone.
        for name in self.fs.live.keys() {
            let kept = name == MANIFEST
                || name == HISTORY_LOG
                || adopted_names.contains(name)
                || IndexStoreFileId::parse_canonical(name).is_some_and(|id| {
                    files.iter().any(|file| file.id == id) || self.created.contains(&id)
                });
            if !kept {
                return Err(diverge("orphan survived reopen", name).name(name).into());
            }
        }
        Ok((ram, floor))
    }

    fn load_frame(&self, base: &RootCellBase, cell: usize) -> Result<Base, Divergence> {
        let name = base.file.canonical_name();
        let inode = self
            .fs
            .inode(&name)
            .ok_or_else(|| diverge("adopted frame missing", &name).name(&name))?;
        let Content::Segment { frames, .. } = &inode.content else {
            return Err(diverge("adopted frame missing", &name).name(&name));
        };
        frames
            .iter()
            .find(|frame| {
                frame.offset == base.offset
                    && frame.len == base.framed_len
                    && frame.offset + frame.len <= inode.len
                    && frame.digest == base.digest
                    && frame.cell == cell
                    && digest(&frame.refs, frame.cut) == base.digest
                    && frame.cut == base.frame_cut
                    && frame.floor == base.frame_floor
            })
            .map(|frame| Base {
                file: base.file,
                offset: frame.offset,
                len: frame.len,
                digest: frame.digest,
                cut: frame.cut,
                floor: frame.floor,
                refs: frame.refs.clone(),
            })
            .ok_or_else(|| diverge("adopted frame missing", &name).name(&name))
    }

    /// Spec §8.1 step 7: flush every dirty cell at the replayed prefix into a
    /// new unadopted segment; the original suffix stays the authority.
    fn offline_flush(&mut self, ram: &mut Ram, epoch: u64, prefix: PrefixCut) -> Result<(), Halt> {
        let sequence = ram.flushers[0].next_seq + self.created.len() as u64;
        let id = IndexStoreFileId::Segment {
            authority_epoch: epoch,
            stream_id: OFFLINE_STREAM,
            segment_seq: sequence,
        };
        let name = id.canonical_name();
        if self.ever.contains(&id) || self.created.contains(&id) {
            return Err(diverge("file id alias", format!("{id:?} created twice"))
                .name(&name)
                .into());
        }
        self.created.push(id);
        let inode = self
            .fs
            .create_new(&name, Content::Segment { frames: Vec::new() }, 0)?;
        let mut offset = 0;
        let mut frames = Vec::new();
        for (cell, state) in ram.cells.iter_mut().enumerate() {
            let Some(state) = state else { continue };
            if state.overlay.is_empty() {
                continue;
            }
            let refs = Arc::new(compact(
                state
                    .base
                    .iter()
                    .flat_map(|base| base.refs.iter().copied())
                    .chain(state.overlay.iter().copied()),
                ram.adopted.history,
            ));
            let frame = Frame {
                offset,
                len: frame_len(&refs),
                cell,
                cut: prefix,
                floor: ram.adopted.history,
                digest: digest(&refs, prefix),
                refs: refs.clone(),
            };
            offset += frame.len;
            state.base = Some(Base {
                file: id,
                offset: frame.offset,
                len: frame.len,
                digest: frame.digest,
                cut: prefix,
                floor: frame.floor,
                refs,
            });
            state.overlay.clear();
            state.coverage = state.coverage.max(prefix);
            frames.push(frame);
        }
        let node = &mut self.fs.inodes[inode];
        node.content = Content::Segment { frames };
        node.len = offset;
        self.fs.fsync(inode);
        self.fs.dirsync();
        ram.sealed.insert(id, offset);
        self.report.offline_flushes += 1;
        self.fire(Transition::OfflineFlush, Some(name))
    }

    /// Run the real `IndexStore::open` on a directory holding the same
    /// canonical names and lengths; its deletions and next sequence must
    /// equal the model's scan.
    fn real_orphan_scan(
        &mut self,
        epoch: u64,
        files: &[RootFile],
        orphans: &[IndexStoreFileId],
        next_sequence: u64,
    ) -> Result<(), Divergence> {
        let directory = tempdir::TempDir::new("cell-model-store")
            .map_err(|error| diverge("tempdir", error.to_string()))?;
        for (name, inode) in &self.fs.live {
            if IndexStoreFileId::parse_canonical(name).is_some() {
                let file = std::fs::File::create(directory.path().join(name))
                    .map_err(|error| diverge("materialize", error.to_string()))?;
                file.set_len(self.fs.inodes[*inode].len)
                    .map_err(|error| diverge("materialize", error.to_string()))?;
            }
        }
        let (_, scan) = IndexStore::open(directory.path(), epoch, &[files])
            .map_err(|error| diverge("real IndexStore::open failed", format!("{error:?}")))?;
        let mut expected = orphans.to_vec();
        expected.sort();
        let mut deleted = scan.deleted.clone();
        deleted.sort();
        if deleted != expected || scan.next_sequence != next_sequence {
            return Err(diverge(
                "real orphan scan differs from the model",
                format!(
                    "real deleted {deleted:?} next {} model deleted {expected:?} next {next_sequence}",
                    scan.next_sequence
                ),
            ));
        }
        self.report.real_store_checks += 1;
        Ok(())
    }
}

/// Offline recovery output, built from replayed whole batches.
fn is_offline(id: IndexStoreFileId) -> bool {
    matches!(
        id,
        IndexStoreFileId::Segment {
            stream_id: OFFLINE_STREAM,
            ..
        }
    )
}

// ---------------------------------------------------------------- oracle

/// The sequential per-key and per-batch oracle (option D rules): recovered
/// state is the version-ordered application of a set R of whole issued
/// batches; R contains every acknowledged batch; every batch in R has a
/// surviving owner WAL record; identities map to their one applied version.
/// Only `truth`, `acked` and the surviving owner WALs are consulted.
fn check_recovered(
    w: &World,
    owners: &[OwnerStore],
    ram: &Ram,
    owner_power_loss: bool,
) -> Result<usize, Divergence> {
    let history = ram.adopted.history;
    let survives = |version: &BatchVersion| {
        w.truth.get(version).is_some_and(|truth| {
            !truth.aborted
                && owners[truth.owner]
                    .get(&version.epoch)
                    .is_some_and(|stream| {
                        stream.entries.iter().any(|entry| entry.version == *version)
                    })
        })
    };
    let mut present = BTreeSet::new();
    let mut histories = Vec::with_capacity(KEYS);
    for key in 0..KEYS {
        let refs = compact(ram.history(key), history);
        present.extend(refs.iter().map(|reference| reference.version.batch));
        histories.push(refs);
    }
    for version in &present {
        if !survives(version) {
            return Err(diverge(
                "recovered batch without a surviving WAL record",
                format!("{version:?}"),
            )
            .version(*version));
        }
    }
    let mut ops = BTreeMap::new();
    for version in &present {
        let op = w.truth[version].op;
        if let Some(other) = ops.insert(op, *version) {
            return Err(diverge(
                "operation applied twice",
                format!("operation {op} at {other:?} and {version:?}"),
            )
            .op(op)
            .version(*version)
            .version(other));
        }
    }
    for (key, actual) in histories.iter().enumerate() {
        let expected = compact(
            present
                .iter()
                .flat_map(|version| refs_of(*version, &w.truth[version].edits))
                .filter(|reference| usize::from(reference.key) == key),
            history,
        );
        if &expected != actual {
            let versions: BTreeSet<BatchVersion> = expected
                .iter()
                .chain(actual)
                .map(|reference| reference.version.batch)
                .filter(|version| {
                    expected.iter().any(|r| r.version.batch == *version)
                        != actual.iter().any(|r| r.version.batch == *version)
                })
                .collect();
            let mut divergence = diverge(
                "per-key history differs (partial batch or lost member)",
                format!("key {key} expected {expected:?} recovered {actual:?}"),
            );
            for version in versions {
                divergence = divergence.version(version);
            }
            return Err(divergence);
        }
    }
    let mut lost_acks = 0;
    for (op, version) in &w.acked {
        if present.contains(version) {
            continue;
        }
        let edits = &w.truth[version].edits;
        let compacted = history.is_some_and(|floor| version <= &floor.batch)
            && edits.iter().all(|edit| {
                histories[usize::from(edit.key)]
                    .iter()
                    .any(|reference| reference.version.batch > *version)
            });
        if compacted {
            continue;
        }
        if owner_power_loss && w.config.native && !survives(version) {
            // The Native contract permits losing an unsynced owner suffix.
            lost_acks += 1;
            continue;
        }
        return Err(diverge(
            "lost acknowledged write",
            format!(
                "operation {op} at {version:?} (WAL record survives: {}, adopted F {:?}, H {:?})",
                survives(version),
                ram.adopted.floor,
                ram.adopted.history
            ),
        )
        .op(*op)
        .version(*version));
    }
    if owner_power_loss {
        for (version, truth) in &w.truth {
            if truth.appended
                && !truth.aborted
                && ram.adopted.max_cut.contains(*version)
                && !survives(version)
            {
                return Err(diverge(
                    "proof-covered reference lost",
                    format!("{version:?} at or below the adopted U"),
                )
                .version(*version));
            }
        }
    }
    for (op, version) in &w.acked {
        if present.contains(version) && ram.lookup(*op) != Some(*version) {
            return Err(diverge(
                "recovered identity differs",
                format!(
                    "operation {op} expected {version:?} found {:?}",
                    ram.lookup(*op)
                ),
            )
            .op(*op)
            .version(*version));
        }
    }
    for version in &present {
        let op = w.truth[version].op;
        if ram.lookup(op) != Some(*version) {
            return Err(diverge(
                "recovered batch without its identity",
                format!("operation {op} at {version:?} found {:?}", ram.lookup(op)),
            )
            .op(op)
            .version(*version));
        }
    }
    Ok(lost_acks)
}

// ---------------------------------------------------------------- driver

fn outcome_name(outcome: FsOutcome, owner_loss: bool) -> String {
    format!(
        "{outcome:?}{}",
        if owner_loss { "+owner_power_loss" } else { "" }
    )
}

fn owner_power_loss(owners: &[OwnerStore]) -> Vec<OwnerStore> {
    owners
        .iter()
        .map(|store| {
            store
                .iter()
                .map(|(epoch, stream)| {
                    (
                        *epoch,
                        Stream {
                            entries: stream.entries[..stream.synced].to_vec(),
                            synced: stream.synced,
                        },
                    )
                })
                .collect()
        })
        .collect()
}

impl Sim {
    fn failure(&self, outcome: String, divergence: Divergence) -> Box<Failure> {
        let first_event = self
            .w
            .trace
            .iter()
            .find(|event| {
                event
                    .version
                    .is_some_and(|v| divergence.versions.contains(&v))
                    || event.op.is_some_and(|op| divergence.ops.contains(&op))
                    || event
                        .file
                        .as_ref()
                        .is_some_and(|file| divergence.names.contains(file))
            })
            .or_else(|| self.w.trace.last())
            .cloned();
        Box::new(Failure {
            config: self.w.config,
            outcome,
            divergence,
            first_event,
            events: self.w.trace.len(),
        })
    }

    /// Recover `fs` and check the oracle; returns the recovery and the
    /// number of acknowledgements the Native owner-loss model dropped.
    fn evaluate<'s>(
        &'s self,
        fs: Fs,
        owners: &'s [OwnerStore],
        cut: Option<CrashCut>,
        owner_loss: bool,
        real_store: bool,
    ) -> (Recovery<'s>, Result<(Ram, usize), Halt>) {
        let config = &self.w.config;
        let mut recovery = Recovery {
            config,
            fs,
            owners,
            ever: &self.w.ever_created,
            created: Vec::new(),
            trace: Vec::new(),
            trace_base: self.w.trace.len(),
            cut,
            counts: BTreeMap::new(),
            report: Report::default(),
            real_store,
            multi_epoch: self.w.epochs.len() > 1 && config.owners > 1,
        };
        let result = recovery.run().and_then(|(ram, _)| {
            let lost = check_recovered(&self.w, owners, &ram, owner_loss)?;
            Ok((ram, lost))
        });
        (recovery, result)
    }

    fn outcomes(fs: &Fs) -> Vec<FsOutcome> {
        let mut outcomes = vec![FsOutcome::Process];
        for applied in 0..=fs.pending.len() {
            outcomes.push(FsOutcome::PowerLoss {
                applied,
                unlinks: false,
            });
            if fs.pending[applied..]
                .iter()
                .any(|op| matches!(op, DirOp::Unlink(_)))
            {
                outcomes.push(FsOutcome::PowerLoss {
                    applied,
                    unlinks: true,
                });
            }
        }
        outcomes
    }

    fn count_renames(&mut self, fs: &Fs, applied: usize) {
        for (index, op) in fs.pending.iter().enumerate() {
            if matches!(op, DirOp::Rename(..)) {
                if applied > index {
                    self.w.report.rename_new += 1;
                } else {
                    self.w.report.rename_old += 1;
                }
            }
        }
    }

    /// Check every crash outcome, then continue from one seeded outcome.
    fn crash(&mut self, mut restart_cut: Option<CrashCut>) -> Result<(), Box<Failure>> {
        self.w.report.crashes += 1;
        for caller in &mut self.w.callers {
            if matches!(caller.state, CallerState::Waiting { .. }) {
                // The call is abandoned: pending, never acknowledged.
                caller.state = CallerState::Idle;
                caller.next += 1;
            }
        }
        let mut fs = self.w.fs.clone();
        for _ in 0..4 {
            let outcomes = Self::outcomes(&fs);
            let chosen = if self.w.rng.gen_ratio(1, 2) {
                0
            } else {
                self.w.rng.gen_range(0..outcomes.len())
            };
            for (index, outcome) in outcomes.iter().enumerate() {
                let crashed = fs.crash(*outcome);
                match outcome {
                    FsOutcome::Process => self.w.report.process_outcomes += 1,
                    FsOutcome::PowerLoss { applied, .. } => {
                        self.w.report.power_outcomes += 1;
                        self.count_renames(&fs, *applied);
                    }
                }
                if index != chosen {
                    let (_, result) = self.evaluate(crashed, &self.w.owners, None, false, false);
                    if let Err(Halt::Diverge(divergence)) = result {
                        return Err(self.failure(outcome_name(*outcome, false), divergence));
                    }
                }
            }
            // Owner power loss, counted apart from supported crash outcomes.
            for outcome in [
                FsOutcome::Process,
                FsOutcome::PowerLoss {
                    applied: 0,
                    unlinks: false,
                },
            ] {
                let owners = owner_power_loss(&self.w.owners);
                let (_, result) = self.evaluate(fs.crash(outcome), &owners, None, true, false);
                self.w.report.owner_power_loss += 1;
                match result {
                    Ok((_, lost)) => self.w.report.owner_power_lost_acks += lost,
                    Err(Halt::Diverge(divergence)) => {
                        return Err(self.failure(outcome_name(outcome, true), divergence));
                    }
                    Err(Halt::Crash) => {}
                }
            }
            let outcome = outcomes[chosen];
            // The reopen fences, drains and seals every retained owner stream
            // before replay (the existing owner open protocol): replayed
            // records are durable on their owners before any of them can be
            // read or returned to a retry. Owner power loss above was checked
            // on the unsealed state.
            for store in &mut self.w.owners {
                for stream in store.values_mut() {
                    stream.synced = stream.entries.len();
                }
            }
            let real_store = self.w.config.real_store;
            let (recovery, result) = self.evaluate(
                fs.crash(outcome),
                &self.w.owners,
                restart_cut,
                false,
                real_store,
            );
            let Recovery {
                fs: recovered_fs,
                created,
                trace,
                report,
                ..
            } = recovery;
            self.w.trace.extend(trace);
            self.w.ever_created.extend(created);
            self.w.report.merge(&report);
            match result {
                Ok((ram, _)) => {
                    self.w.fs = recovered_fs;
                    self.w.epochs.insert(ram.epoch);
                    self.r = ram;
                    return Ok(());
                }
                Err(Halt::Diverge(divergence)) => {
                    return Err(self.failure(outcome_name(outcome, false), divergence));
                }
                Err(Halt::Crash) => {
                    // A crash during reopen: the next reopen starts from it.
                    self.w.report.restart_crashes += 1;
                    self.w.report.cut_reached = true;
                    self.w.report.crashes += 1;
                    fs = recovered_fs;
                    restart_cut = restart_cut.filter(|cut| cut.again).map(|_| CrashCut {
                        at: Transition::OrphanScan,
                        occurrence: 1,
                        again: false,
                    });
                }
            }
        }
        Err(self.failure(
            "restart".into(),
            diverge(
                "restart crash loop",
                "more than four consecutive reopen crashes",
            ),
        ))
    }
}

/// Run one seeded schedule to completion, checking every crash outcome.
pub(super) fn run_schedule(config: ModelConfig) -> Result<Report, Box<Failure>> {
    let mut sim = Sim::new(config);
    let restart = config.cut.filter(|cut| cut.at.restart());
    // A restart cut needs a first crash; other schedules add one at random.
    let mut extra = if restart.is_some() || sim.w.rng.gen_ratio(1, 2) {
        Some(sim.w.rng.gen_range(40..400))
    } else {
        None
    };
    let mut actions = Vec::new();
    let mut steps = 0;
    loop {
        steps += 1;
        if steps > MAX_STEPS {
            return Err(sim.failure(
                "serving".into(),
                diverge("step budget exhausted", MAX_STEPS.to_string()),
            ));
        }
        match sim.step(&mut actions) {
            Ok(false) => {
                if extra == Some(steps) {
                    extra = None;
                    sim.crash(restart)?;
                }
            }
            Ok(true) => {
                if extra.is_some() {
                    // Finished before the random crash: crash now instead.
                    sim.crash(restart)?;
                }
                // Final stop is a crash too: there is no shutdown checkpoint.
                sim.crash(None)?;
                break;
            }
            Err(Halt::Crash) => sim.crash(None)?,
            Err(Halt::Diverge(divergence)) => {
                return Err(sim.failure("serving".into(), divergence));
            }
        }
    }
    Ok(sim.w.report)
}

#[cfg(test)]
#[path = "cell_model_tests.rs"]
mod tests;
