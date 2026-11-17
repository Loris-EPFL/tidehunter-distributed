//! Per-cell checkpoints: metadata-only roots, their commit, retirement and
//! recovery (checkpoint option C spec §§6.3, 7, 8.1, 9.2, 10.1). Active only
//! under an explicit `CheckpointMode::PerCell`; `Exact` never reaches this
//! module.
//!
//! # Roles
//!
//! - The cell flusher pool (`cell_flush`) encodes and installs cell bases in
//!   worker-private index-store segments. A flush returns no replay-tail or
//!   identity credit and authorizes no deletion.
//! - The existing checkpoint actor (`value-wal-checkpoint`, one generation at a
//!   time) runs [`run_checkpoint`]: it drains the cells to the published prefix,
//!   captures a metadata-only root in bounded slices, obtains owner proofs,
//!   commits root, wrapper and manifest, applies the adopted state, returns
//!   credits once and collects garbage. It is the only writer of the authority
//!   manifest while serving.
//!
//! # Cuts
//!
//! `F` (replay floor) is the minimum exported cell coverage; replay applies
//! batches strictly above it. `U` (maximum frame cut) bounds every frame the
//! root references. Wrapper streams skip through `F` (`last_offset`) and carry
//! an owner durability proof through `U` (`durable_end`): a proof end through
//! `U` is never the skip position.
//!
//! # Crash consistency (store before root before GC)
//!
//! 1. Segments are sealed (file and directory sync) before a base referencing
//!    them is installed (flusher pool).
//! 2. A staging reservation names the root output before it is written, so a
//!    crash at any later step leaves only names the next open deletes.
//! 3. Root, then wrapper, are written by temp, fsync, rename, directory fsync.
//!    Neither is the commit.
//! 4. The format-5 manifest rename (then directory fsync) is the only adoption.
//! 5. Only after the manifest is durable: RAM apply, history before identity
//!    retirement, exactly-once credit return, then deletion of the obsolete
//!    root and wrapper and of unreachable segments.
//!
//! A crash before step 4 recovers from the previous root and the retained WAL
//! suffix, which no credit return or deletion has touched; a crash after it
//! recovers from the new root. Recovery rebuilds counters from the adopted root
//! and the replayed suffix, never from a partly applied retirement.
//!
//! # Locks
//!
//! Serving guards are `index.writer()` -> option-D publisher, taken only for
//! short captures and installs. `CellRuntime::pool` (read guards for wakes and
//! coverage waits, a write guard only at stop) and `CellRuntime::state` (a
//! leaf) are never taken while a serving guard is held, and no file I/O, owner
//! control, condvar wait or Admission call runs under a serving guard.

use super::cell_flush::{
    CellFlushPool, FlushError, FlushHooks, FlushPolicy, ReplayPressure, StopMode, WakeReason,
    flush_prepared,
};
use super::cell_metrics::{AdoptedGauges, CellGauges, CellMetricsSnapshot};
use super::cell_recovery::{
    CellReplayConfig, CoveragePlan, OfflineCellFlusher, StreamCursor, replay_mixed_cuts,
};
use super::checkpoint_contract::{
    CellCheckpointCommit, CellCheckpointStream, CheckpointImageV4, PER_CELL_CHECKPOINT_FORMAT,
    PER_CELL_MANIFEST_FORMAT, RecoveryKind, RetirementLedger, RetirementToken,
};
use super::write_core::core::WriteCore;
use super::*;
use crate::distributed::native_index::{
    CellFlushResult, CellRoot, CellRootHeader, CellRootParams, IndexImageMetadata, IndexStore,
    IndexStoreFileId, IndexStoreSpace, IndexStoreWriter, IndexWriter, MAX_CELL_FLUSHERS,
    NativeIndexError, PrefixCut, PreparedCell, PreparedCellRoot, RootFile, RootFloorLease,
    cell_flushers, cell_root_name, cell_root_staging_name, cell_scratch_limit, map_root_file,
    parse_cell_root_name, read_cell_root,
};
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Weak;

/// Most garbage sweeps per checkpoint. One sweep deletes at most 256
/// segments, so this bounds one pass at about one million deletions; the
/// remainder is collected by the next checkpoint.
const GC_SWEEPS_PER_CHECKPOINT: usize = 4096;

/// Crash-cut boundaries of the per-cell checkpoint. Each fires the model
/// failpoint of the same name and, in tests, can inject an interruption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CellStep {
    /// After every cell covered the published prefix, before the root capture.
    Drained,
    /// After the bounded root capture and the ledger capture, before any I/O.
    Capture,
    /// After every owner proof through `U` was obtained.
    OwnerProof,
    /// After the staging reservation manifest is durable.
    Staging,
    /// After the root is durable under its final name.
    Root,
    /// After the wrapper is durable under its final name.
    Wrapper,
    /// After the format-5 manifest adopting the root is durable.
    Manifest,
    /// After the adopted coverage floor `F` is installed in RAM.
    ApplyHistory,
    /// After the manifest, stream state and retry window are installed.
    ApplyAuthority,
    /// After the retired suffix receipts and tail counters are removed.
    ApplyLedger,
    /// After identity shards dropped the entries the token certifies.
    IdentityRemove,
    /// After the exact credits returned to Admission.
    CreditReturn,
    /// After the obsolete root and wrapper were unlinked and the manifest
    /// stopped naming them.
    GcUnlink,
    /// After the unreachable segment sweep.
    GcSweep,
}

impl CellStep {
    fn failpoint(self) -> &'static str {
        match self {
            Self::Drained => "cell_drain_after",
            Self::Capture => "cell_capture_after",
            Self::OwnerProof => "cell_owner_proof_after",
            Self::Staging => "cell_staging_after",
            Self::Root => "cell_root_dirsync_after",
            Self::Wrapper => "cell_wrapper_dirsync_after",
            Self::Manifest => "cell_manifest_dirsync_after",
            Self::ApplyHistory => "cell_apply_history_after",
            Self::ApplyAuthority => "cell_apply_authority_after",
            Self::ApplyLedger => "cell_apply_ledger_after",
            Self::IdentityRemove => "cell_identity_remove_after",
            Self::CreditReturn => "cell_credit_return_after",
            Self::GcUnlink => "cell_gc_dirsync_after",
            Self::GcSweep => "cell_gc_sweep_after",
        }
    }
}

/// Model crash failpoint. A no-op outside tests.
#[inline]
fn boundary(_name: &'static str) {
    #[cfg(test)]
    crate::failpoints::write_model::boundary(_name);
}

/// Steps of [`commit_cell_root`], reported to its hook after each one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum CommitStep {
    StagingReserved,
    RootPersisted,
    WrapperPersisted,
    ManifestPersisted,
}

impl CommitStep {
    fn cell_step(self) -> CellStep {
        match self {
            Self::StagingReserved => CellStep::Staging,
            Self::RootPersisted => CellStep::Root,
            Self::WrapperPersisted => CellStep::Wrapper,
            Self::ManifestPersisted => CellStep::Manifest,
        }
    }
}

/// Cuts of the adopted root. Updated once per apply under the leaf state
/// lock; readers never take a serving guard to see them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AdoptedCuts {
    /// Generation in the current authority epoch; 0 before the first root of
    /// this process (the recovered root belongs to an older epoch).
    pub generation: u64,
    pub target: PrefixCut,
    pub floor: PrefixCut,
    pub max_frame_cut: PrefixCut,
    pub history_floor: Option<MutationVersion>,
}

impl AdoptedCuts {
    fn of(header: &CellRootHeader, generation: u64) -> Self {
        Self {
            generation,
            target: header.directory_target,
            floor: header.replay_floor,
            max_frame_cut: header.max_frame_cut,
            history_floor: header.history_floor,
        }
    }

    fn empty() -> Self {
        Self {
            generation: 0,
            target: PrefixCut::BeforeAll,
            floor: PrefixCut::BeforeAll,
            max_frame_cut: PrefixCut::BeforeAll,
            history_floor: None,
        }
    }
}

/// Facts of one root capture (metrics wiring point).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CellRootCaptured {
    pub cells: usize,
    pub files: usize,
    pub capture_ns: u64,
}

/// Facts of one durable root commit (metrics wiring point).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CellRootCommitted {
    pub root_bytes: u64,
    pub wrapper_bytes: u64,
    pub manifest_bytes: u64,
    pub owner_proofs: usize,
    pub proof_wait_ns: u64,
    pub commit_ns: u64,
}

/// Facts of one adopted-root apply (metrics wiring point).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CellRootApplied {
    pub apply_ns: u64,
    pub returned_batches: usize,
    pub returned_encoded_bytes: usize,
    pub returned_identities: usize,
    pub f_advanced: bool,
}

/// Actor-owned state. Leaf lock: taken only by the checkpoint actor (one
/// generation at a time) and by observers, never with a serving guard.
struct CellState {
    /// Exactly-once credit return per generation of this process. Starts at
    /// generation 0 and the recovered `F` (generations restart per authority
    /// epoch; counters were rebuilt from the root and the replayed suffix).
    ledger: RetirementLedger,
    adopted: AdoptedCuts,
    /// File table of the adopted root: the retained root of segment GC.
    root_files: Vec<RootFile>,
    /// Old-epoch streams fully read at recovery. Such a sealed stream is
    /// complete once no retained receipt of it is above `F`.
    scanned: BTreeSet<(u32, u64)>,
    captured: CellRootCaptured,
    committed: CellRootCommitted,
    applied: CellRootApplied,
    skipped_unchanged: u64,
    /// Cell lifecycle generation of the index (`advance_cell_lifecycle`):
    /// bumped at the PerCell open (store epoch change and fence) and at the
    /// shutdown seal. Recorded in every root; stale flushes are rejected.
    lifecycle: u64,
}

/// Per-cell checkpoint state of one open database, held by the write core.
pub(in crate::distributed::remote_db) struct CellRuntime {
    store: Arc<IndexStore>,
    /// The flusher pool. Read guards: publication wakes (`try_read`, never
    /// blocking a publisher), target requests and coverage waits. The write
    /// guard is taken once, by [`CellRuntime::stop`], after a stop request has
    /// released every coverage waiter.
    pool: RwLock<Option<CellFlushPool<CoreHooks>>>,
    state: Mutex<CellState>,
}

impl CellRuntime {
    /// Coalesced publication or Admission hint. Publication uses `try_read`:
    /// a missed wake is covered by the index dirty signal, which the
    /// scheduler reads on every pass, and by its one-second deadline.
    pub(in crate::distributed::remote_db) fn wake(&self, reason: WakeReason) {
        let pool = match reason {
            WakeReason::Dirty => self.pool.try_read(),
            WakeReason::AdmissionBlocked => Some(self.pool.read()),
        };
        if let Some(pool) = pool
            && let Some(pool) = pool.as_ref()
        {
            pool.wake(reason);
        }
    }

    /// Stop the scheduler and the flushers and join them. Captured cells are
    /// restored (nothing adopts them after the final root); an open segment
    /// stays as an orphan for the next open.
    pub(in crate::distributed::remote_db) fn stop(&self) -> Result<(), LocalDbError> {
        // Release every coverage waiter first, so the write guard below never
        // waits for a deadline.
        if let Some(pool) = self.pool.read().as_ref() {
            pool.request_stop(StopMode::Cancel);
        }
        let pool = self.pool.write().take();
        match pool {
            Some(pool) => pool
                .join()
                .map_err(|error| LocalDbError::new(error.to_string())),
            None => Ok(()),
        }
    }

    pub(in crate::distributed::remote_db) fn adopted(&self) -> AdoptedCuts {
        self.state.lock().adopted
    }

    /// Sealed-segment space against the live bases and the adopted root's
    /// file table. O(cells) shard read locks: measurement boundaries only.
    fn space(&self, index: &NativeIndex) -> IndexStoreSpace {
        let root_files = self.state.lock().root_files.clone();
        self.store.space(&[&root_files], index)
    }
}

/// Option C counters, plus (with `gauges`) the cuts, charges and segment
/// space sampled now. Gauges take the Admission mutex, the leaf state lock
/// and index shard read locks one at a time, never with a serving guard; the
/// non-blocking metrics path passes `false`.
pub(in crate::distributed::remote_db) fn metrics_snapshot(
    core: &WriteCore,
    gauges: bool,
) -> CellMetricsSnapshot {
    if !gauges {
        return core.cell_metrics.snapshot(CellGauges::default());
    }
    let runtime = core.cell.get();
    let gauges = CellGauges {
        published: Some(core.index.published_prefix()),
        adopted: runtime.map(|runtime| {
            let adopted = runtime.adopted();
            AdoptedGauges {
                generation: adopted.generation,
                target: adopted.target,
                floor: adopted.floor,
                max_frame_cut: adopted.max_frame_cut,
                history_floor: adopted.history_floor.map_or(PrefixCut::BeforeAll, |floor| {
                    PrefixCut::Through(floor.batch)
                }),
            }
        }),
        resident_index_bytes: Some(core.index.charged_metadata_bytes() as u64),
        admitted_index_bytes: Some(core.admission.admitted_index_bytes() as u64),
        transient_index_bytes: Some(core.index.transient_metadata_bytes() as u64),
        segments: runtime.map(|runtime| runtime.space(&core.index)),
    };
    core.cell_metrics.snapshot(gauges)
}

impl CellRuntime {
    /// Advance the index cell lifecycle (a store epoch change, a fence or a
    /// seal). In-flight flushes captured before it install as stale and are
    /// restored. Takes writer -> publisher briefly; no other guard held.
    pub(in crate::distributed::remote_db) fn advance_lifecycle(&self, core: &WriteCore) {
        let lifecycle = {
            let mut writer = core.index.writer();
            let _publisher = core.publisher.lock();
            writer.advance_cell_lifecycle()
        };
        self.state.lock().lifecycle = lifecycle;
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn last_root(
        &self,
    ) -> (CellRootCaptured, CellRootCommitted, CellRootApplied, u64) {
        let state = self.state.lock();
        (
            state.captured,
            state.committed,
            state.applied,
            state.skipped_unchanged,
        )
    }

    #[cfg(test)]
    pub(in crate::distributed::remote_db) fn ledger(&self) -> RetirementLedger {
        self.state.lock().ledger
    }

    /// Drain every cell to `target` (a complete published prefix). Waits on
    /// the pool's progress condvar without any serving guard.
    fn drain_to(&self, target: PrefixCut, timeout: Duration) -> Result<(), LocalDbError> {
        let pool = self.pool.read();
        let pool = pool
            .as_ref()
            .ok_or_else(|| LocalDbError::new("cell flusher pool stopped"))?;
        pool.request_target(target);
        pool.wait_covered(target, timeout)
            .map_err(|error| LocalDbError::new(format!("cell drain to {target:?}: {error}")))
    }
}

/// Recovered per-cell state handed to [`start`] once the core exists.
pub(in crate::distributed::remote_db) struct CellOpen {
    store: Arc<IndexStore>,
    adopted: AdoptedCuts,
    root_files: Vec<RootFile>,
    scanned: BTreeSet<(u32, u64)>,
    /// Mixed-cut recovery counters and elapsed time of this open, recorded
    /// into the core's metrics by [`start`] (the core does not exist yet
    /// while recovery runs).
    recovery: Option<(super::cell_recovery::CellReplayStats, Duration)>,
}

impl CellOpen {
    /// A runtime start without a recovered per-cell root (the exact to
    /// per-cell migration installs its adopted root with
    /// [`install_adopted`] before the open returns).
    pub(in crate::distributed::remote_db) fn without_root(store: Arc<IndexStore>) -> Self {
        Self {
            store,
            adopted: AdoptedCuts::empty(),
            root_files: Vec::new(),
            scanned: BTreeSet::new(),
            recovery: None,
        }
    }
}

/// Flusher hooks into the write core. A `Weak` reference: the core owns the
/// pool, and the pool is joined by shutdown while the database still holds a
/// strong reference, so `upgrade` cannot fail while a pool thread runs.
struct CoreHooks {
    core: Weak<WriteCore>,
}

impl FlushHooks for CoreHooks {
    fn with_publisher<'w, R>(
        &self,
        writer: &mut IndexWriter<'w>,
        f: impl FnOnce(&mut IndexWriter<'w>) -> R,
    ) -> R {
        match self.core.upgrade() {
            Some(core) => {
                let _publisher = core.publisher.lock();
                f(writer)
            }
            // No core means no publisher can run: nothing to serialize with.
            None => f(writer),
        }
    }

    fn replay_pressure(&self) -> ReplayPressure {
        let Some(core) = self.core.upgrade() else {
            return ReplayPressure::default();
        };
        let limits = core.config.replay_limits;
        let granted = core.admission.checkpoint_pressure();
        let drain = granted.replay_batches.saturating_mul(2) >= limits.max_batches
            || granted.replay_bytes.saturating_mul(2) >= limits.max_encoded_bytes;
        // Sequences of the current epoch are dense (aborted gaps included), so
        // the batch `quarter` below `L` bounds the suffix count from below.
        // Older-epoch suffixes are served by the age and drain rules.
        let quarter = (limits.max_batches / 4).max(1) as u64;
        let lagging = match core.index.published_prefix() {
            PrefixCut::Through(batch) if batch.epoch == core.epoch && batch.sequence >= quarter => {
                PrefixCut::Through(BatchVersion {
                    epoch: batch.epoch,
                    sequence: batch.sequence - quarter,
                })
            }
            _ => PrefixCut::BeforeAll,
        };
        ReplayPressure { lagging, drain }
    }

    fn regrant(&self) {
        if let Some(core) = self.core.upgrade() {
            core.admission.regrant();
        }
    }

    fn fail(&self, error: &FlushError) {
        if let Some(core) = self.core.upgrade() {
            core.fail(LocalDbError::new(error.to_string()));
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only flusher policy of the next `start` on this thread (the
    /// opening thread), so a test can keep selected cells dirty.
    pub(super) static TEST_FLUSH_POLICY: std::cell::Cell<Option<FlushPolicy>> =
        const { std::cell::Cell::new(None) };
}

/// The scheduler thresholds: the spec §6.1 defaults (tests may override).
fn flush_policy() -> FlushPolicy {
    #[cfg(test)]
    if let Some(policy) = TEST_FLUSH_POLICY.with(std::cell::Cell::get) {
        return policy;
    }
    FlushPolicy::default()
}

/// Start the flusher pool for an opened per-cell database. Called once, after
/// the core exists and before the open returns a serving handle.
pub(in crate::distributed::remote_db) fn start(
    core: &Arc<WriteCore>,
    open: CellOpen,
) -> Result<(), LocalDbError> {
    let flushers = cell_flushers(core.index.shard_count());
    let scratch = cell_scratch_limit(core.config.max_index_bytes, flushers);
    let store = Arc::clone(&open.store);
    if let Some((stats, elapsed)) = &open.recovery {
        core.cell_metrics.recovery(stats, *elapsed);
    }
    let pool = CellFlushPool::start(
        Arc::clone(&core.index),
        CoreHooks {
            core: Arc::downgrade(core),
        },
        flush_policy(),
        Arc::clone(&core.cell_metrics),
        |worker| {
            let stream = u16::try_from(worker).map_err(|_| NativeIndexError::Capacity)?;
            IndexStoreWriter::new(&store, stream, scratch)
        },
    )
    .map_err(|error| LocalDbError::new(error.to_string()))?;
    let floor = open.adopted.floor;
    let runtime = CellRuntime {
        store: open.store,
        pool: RwLock::new(Some(pool)),
        state: Mutex::new(CellState {
            ledger: RetirementLedger {
                generation: 0,
                floor,
            },
            adopted: open.adopted,
            root_files: open.root_files,
            scanned: open.scanned,
            captured: CellRootCaptured::default(),
            committed: CellRootCommitted::default(),
            applied: CellRootApplied::default(),
            skipped_unchanged: 0,
            lifecycle: 0,
        }),
    };
    if let Err(runtime) = core.cell.set(runtime) {
        // Join the pool just started before reporting the misuse.
        let _ = runtime.stop();
        return Err(LocalDbError::new("per-cell runtime already started"));
    }
    // This open fenced every owner to a new authority (store) epoch.
    if let Some(runtime) = core.cell.get() {
        runtime.advance_lifecycle(core);
    }
    Ok(())
}

/// Install the state of a root another component adopted (the exact to
/// per-cell migration), so the next generation retires from it.
pub(in crate::distributed::remote_db) fn install_adopted(
    core: &WriteCore,
    commit: &CellCheckpointCommit,
    prepared: &PreparedCellRoot,
    ledger: RetirementLedger,
) -> Result<(), LocalDbError> {
    let runtime = core
        .cell
        .get()
        .ok_or_else(|| LocalDbError::new("per-cell runtime is not started"))?;
    let header = &prepared.root().header;
    if (header.authority_epoch, header.snapshot_generation) != (commit.epoch, commit.generation)
        || ledger.floor != header.replay_floor
    {
        return Err(LocalDbError::new(
            "adopted root differs from its commit or retirement ledger",
        ));
    }
    let mut state = runtime.state.lock();
    state.ledger = ledger;
    state.adopted = AdoptedCuts::of(header, commit.generation);
    state.root_files = prepared.root().files.clone();
    Ok(())
}

/// Wrapper inputs of one root commit, captured through `F` (streams, retry
/// state) and through `U` (owner proofs).
pub(super) struct CellCommitInputs<'a> {
    /// The checked root; it pins every referenced base until dropped.
    pub prepared: &'a PreparedCellRoot,
    /// Next sequence of the authority epoch after `F` (0 if `F` is older).
    pub published_next: u64,
    /// Every retained `(partition, owner epoch)` stream, sorted, unique.
    pub streams: Vec<CellCheckpointStream>,
    /// WAL-replay mode only: the bounded recent retry window ending at `F`.
    pub recent: Vec<RecentReceipt>,
    /// Receipt-history mode only: durable exact-retry history through `F`.
    pub history_root: Option<HistoryPointer>,
    pub replay_limits: ReplayLimits,
    /// Retirement of `(old F, F]` owned by this generation.
    pub retirement: RetirementToken,
}

/// Commit one metadata-only root (spec §7.3 steps 3-5): staging reservation,
/// root, format-4 wrapper, then the format-5 manifest that adopts them. Each
/// file is durable (temp, fsync, rename, directory fsync) before the next
/// step. `manifest` is a copy of the live manifest; on success it is the
/// adopted manifest, which the caller installs in RAM. `checkpoint_obsolete`
/// (migration only) names the last exact commit, retained until GC. An error
/// from `hook` stops before the next step; nothing is rolled back, because
/// every earlier output is unadopted and named for cleanup.
pub(super) fn commit_cell_root(
    path: &Path,
    config: &RemoteDatabaseConfig,
    manifest: &mut AuthorityManifest,
    inputs: CellCommitInputs<'_>,
    checkpoint_obsolete: Option<CheckpointCommit>,
    hook: &mut dyn FnMut(CommitStep) -> Result<(), LocalDbError>,
) -> Result<CellCheckpointCommit, LocalDbError> {
    let header = &inputs.prepared.root().header;
    let (epoch, generation) = (header.authority_epoch, header.snapshot_generation);
    if manifest.checkpoint_staging.is_some() || manifest.cell_checkpoint_obsolete.is_some() {
        return Err(LocalDbError::new(
            "checkpoint cleanup must complete before another installation",
        ));
    }
    let root_name = cell_root_name(epoch, generation);
    // Store before root: the reservation names every root/wrapper output, so
    // a crash after this point leaves only names that open cleanup removes.
    let mut staging = manifest.clone();
    staging.checkpoint_staging = Some(CheckpointFiles {
        epoch,
        generation,
        index_files: vec![root_name.clone(), cell_root_staging_name(&root_name)],
    });
    boundary("cell_staging_before");
    persist_manifest(path, &staging)?;
    hook(CommitStep::StagingReserved)?;
    boundary("cell_root_persist_before");
    let root = inputs
        .prepared
        .persist(path)
        .map_err(LocalDbError::component)?;
    hook(CommitStep::RootPersisted)?;
    let mut commit = CellCheckpointCommit {
        epoch,
        generation,
        metadata_bytes: 0,
        metadata_digest: [0; 32],
        root_bytes: root.bytes,
        root_digest: root.digest,
    };
    let image = CheckpointImageV4 {
        format: PER_CELL_CHECKPOINT_FORMAT,
        recovery_kind: RecoveryKind::PerCellCoverageV1,
        history_persistence: config.history_persistence,
        database: config.storage.database,
        shape_yaml: manifest.shape_yaml.clone(),
        authority_epoch: epoch,
        generation,
        root_name,
        root_bytes: root.bytes,
        root_digest: root.digest,
        directory_target: header.directory_target,
        replay_floor: header.replay_floor,
        max_frame_cut: header.max_frame_cut,
        history_floor: header.history_floor,
        published_next: inputs.published_next,
        streams: inputs.streams,
        recent: inputs.recent,
        history_root: inputs.history_root,
        replay_limits: inputs.replay_limits,
        retirement: inputs.retirement,
    };
    // The loader applies the same rules; never persist a wrapper it rejects.
    image.check(&commit, header, config, &manifest.shape_yaml)?;
    let encoded = encode_checkpoint(&image)?;
    commit.metadata_bytes = encoded.len() as u64;
    commit.metadata_digest = Blake2b::<U32>::digest(&encoded).into();
    boundary("cell_wrapper_persist_before");
    install_metadata(path, &commit.metadata_name(), &encoded).map_err(LocalDbError::component)?;
    hook(CommitStep::WrapperPersisted)?;
    let mut adopted = staging;
    adopted.format = PER_CELL_MANIFEST_FORMAT;
    adopted.checkpoint_mode = CheckpointMode::PerCell;
    if let Some(obsolete) = checkpoint_obsolete {
        adopted.checkpoint_obsolete = Some(obsolete);
    }
    adopted.checkpoint = None;
    adopted.cell_checkpoint_obsolete = adopted.cell_checkpoint.replace(commit.clone());
    adopted.checkpoint_staging = None;
    manifest_checkpoint_mode(&adopted)?;
    // The only adoption: every dependency above is already durable.
    boundary("cell_manifest_persist_before");
    persist_manifest(path, &adopted)?;
    *manifest = adopted;
    hook(CommitStep::ManifestPersisted)?;
    Ok(commit)
}

/// Exact `(old F, F]` totals and stream summaries over the retained receipt
/// ledger (spec §6.3). `known` holds every published batch above the adopted
/// `F` until retirement, keyed by id: it is independent of recycled sequencer
/// slots, so totals stay exact after ring wrap. Aborted gaps hold no receipt.
struct ReplayLedger {
    token: RetirementToken,
    /// Per stream, the last complete batch at or below `F`.
    through_floor: BTreeMap<(u32, u64), (u64, u32)>,
    /// Per stream, the end of the last batch at or below `U`: the owner proof
    /// must cover it, because a referenced frame may hold such a batch.
    through_frames: BTreeMap<(u32, u64), u64>,
    /// Streams that keep a batch above `F`.
    above_floor: BTreeSet<(u32, u64)>,
    /// Receipts at or below `F`, for the WAL-replay retry window.
    retired: Vec<RecentReceipt>,
    /// Receipt-history mode: per stream, the receipts in `(F, U]` in version
    /// order. Owners serve no WAL position proofs in this mode, so these are
    /// proved by an owner receipt prefix extending the through-`F` prefix.
    frame_receipts: BTreeMap<(u32, u64), Vec<CheckpointReceipt>>,
}

impl ReplayLedger {
    fn capture(
        known: &HashMap<BatchId, AppendedBatch>,
        from: PrefixCut,
        floor: PrefixCut,
        frames: PrefixCut,
        generation: u64,
        window: bool,
    ) -> Result<Self, LocalDbError> {
        let mut ledger = Self {
            token: RetirementToken {
                generation,
                from,
                through: floor,
                batches: 0,
                encoded_bytes: 0,
                identities: 0,
            },
            through_floor: BTreeMap::new(),
            through_frames: BTreeMap::new(),
            above_floor: BTreeSet::new(),
            retired: Vec::new(),
            frame_receipts: BTreeMap::new(),
        };
        for receipt in known.values() {
            let version = receipt.version();
            if from.contains(version) {
                return Err(LocalDbError::new(
                    "retained receipt is at or below the adopted replay floor",
                ));
            }
            let address = receipt.address();
            let stream = (address.partition, version.epoch);
            let end = address
                .offset
                .checked_add(u64::from(address.len))
                .ok_or_else(|| LocalDbError::new("receipt address overflow"))?;
            if frames.contains(version) {
                let last = ledger.through_frames.entry(stream).or_insert(end);
                *last = (*last).max(end);
            }
            if !floor.contains(version) {
                ledger.above_floor.insert(stream);
                if !window && frames.contains(version) {
                    ledger
                        .frame_receipts
                        .entry(stream)
                        .or_default()
                        .push((*receipt).into());
                }
                continue;
            }
            let encoded = address.len.checked_sub(8).ok_or_else(|| {
                LocalDbError::new("checkpoint receipt frame is shorter than its header")
            })?;
            let token = &mut ledger.token;
            token.batches = token
                .batches
                .checked_add(1)
                .ok_or_else(|| LocalDbError::new("retirement batch count overflow"))?;
            token.encoded_bytes = token
                .encoded_bytes
                .checked_add(u64::from(encoded))
                .ok_or_else(|| LocalDbError::new("retirement byte count overflow"))?;
            ledger
                .through_floor
                .entry(stream)
                .and_modify(|last| {
                    if address.offset > last.0 {
                        *last = (address.offset, address.len);
                    }
                })
                .or_insert((address.offset, address.len));
            if window {
                ledger.retired.push(RecentReceipt {
                    id: receipt.id(),
                    version,
                    digest: receipt.digest(),
                });
            }
        }
        // Every retained receipt owns exactly one charged identity (Active or
        // Suffix); a retry of an identical id allocates none.
        ledger.token.identities = ledger.token.batches;
        ledger
            .retired
            .sort_unstable_by_key(|receipt| receipt.version);
        for receipts in ledger.frame_receipts.values_mut() {
            receipts.sort_unstable_by_key(|receipt| receipt.version);
        }
        Ok(ledger)
    }
}

/// Everything one generation captured under the serving guards.
struct Captured {
    ledger: ReplayLedger,
    manifest: AuthorityManifest,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: VecDeque<RecentReceipt>,
    jobs: super::write_core::core::CheckpointJobs,
    /// Receipt-history mode: the `(old F, F]` receipts, the live history at
    /// the old `F` and owner controls for its prefix proofs.
    receipts: Option<(
        Vec<CheckpointReceipt>,
        ReceiptHistory,
        super::write_core::core::CheckpointJobs,
    )>,
}

/// Test injection and the model failpoint of one step.
fn step(core: &WriteCore, step: CellStep) -> Result<(), LocalDbError> {
    boundary(step.failpoint());
    #[cfg(test)]
    {
        let (interrupt, pause) = {
            let mut hooks = core.test_hooks.lock();
            let pause = if hooks
                .cell_pause
                .as_ref()
                .is_some_and(|(at, _, _)| *at == step)
            {
                hooks.cell_pause.take()
            } else {
                None
            };
            (hooks.cell_interruption == Some(step), pause)
        };
        if let Some((_, entered, resume)) = pause {
            let _ = entered.send(());
            resume
                .recv()
                .map_err(|_| LocalDbError::new("per-cell test pause disconnected"))?;
        }
        if interrupt {
            return Err(LocalDbError::new(format!(
                "injected per-cell interruption after {step:?}"
            )));
        }
    }
    #[cfg(not(test))]
    let _ = core;
    Ok(())
}

fn mutation_floor(floor: PrefixCut) -> Option<MutationVersion> {
    match floor {
        PrefixCut::BeforeAll => None,
        PrefixCut::Through(batch) => Some(MutationVersion {
            batch,
            operation_index: u32::MAX,
        }),
    }
}

/// One per-cell checkpoint generation on the checkpoint actor thread.
pub(super) fn run_checkpoint(core: &Arc<WriteCore>, generation: u64) -> Result<(), LocalDbError> {
    let started = Instant::now();
    let runtime = core
        .cell
        .get()
        .ok_or_else(|| LocalDbError::new("per-cell runtime is not started"))?;
    core.stats.lock().write.checkpoint_count += 1;
    let requested = core
        .publisher
        .lock()
        .background_checkpoint
        .as_mut()
        .and_then(|background| background.requested_floor.take());
    // Spec §7.2 step 4: without a requested floor the root keeps H (the
    // lease then reserves the committed floor and installs it unchanged).
    let requested = requested.unwrap_or_else(|| core.index.retention_floor());
    // Spec §7.2: cover the published prefix in every cell first; the root then
    // exports F at or above it. Writers keep publishing meanwhile. Required
    // for F monotonicity: a cell created after an earlier root's directory
    // boundary keeps the older new-cell floor until it is captured, and only
    // this drain plus the root's clean advance lifts it to F. The backward-F
    // check below is fail-stop (the actor's error fails the core).
    runtime.drain_to(core.index.published_prefix(), core.config.timeout)?;
    step(core, CellStep::Drained)?;
    let (adopted, scanned, lifecycle) = {
        let state = runtime.state.lock();
        (state.adopted, state.scanned.clone(), state.lifecycle)
    };
    let mut capture = {
        let writer = core.index.writer();
        let _publisher = core.publisher.lock();
        writer.begin_cell_root_capture()
    };
    capture.reserve().map_err(LocalDbError::component)?;
    loop {
        let complete = {
            let mut writer = core.index.writer();
            let _publisher = core.publisher.lock();
            capture
                .capture_slice(&mut writer)
                .map_err(LocalDbError::component)?
        };
        boundary("cell_root_slice_after");
        if complete {
            break;
        }
    }
    // Spec §7.2 step 4: H_new = max(H, min(requested, F, oldest pin/view)),
    // reserved from here until apply installs it (dropped = abandoned root,
    // committed H unchanged). No serving guard is held.
    let history = core
        .index
        .begin_root_floor(
            requested,
            capture.replay_floor().map_err(LocalDbError::component)?,
        )
        .map_err(LocalDbError::component)?;
    // A complete prefix observed after the last slice: U cannot exceed it.
    let complete = core.index.published_prefix();
    let prepared = capture
        .finish(
            &core.index,
            &core.path,
            CellRootParams {
                authority_epoch: core.epoch,
                snapshot_generation: generation,
                lifecycle_generation: lifecycle,
                history_floor: history.floor(),
            },
            complete,
            |id| match id {
                IndexStoreFileId::Segment { .. } => runtime.store.sealed_len(id),
                // Migrated roots keep referencing legacy exact containers.
                IndexStoreFileId::LegacyContainer { .. } => {
                    cell_migration::legacy_sealed_len(&core.path, id)
                }
            },
        )
        .map_err(LocalDbError::component)?;
    let header = &prepared.root().header;
    let floor = header.replay_floor;
    if floor < adopted.floor {
        return Err(LocalDbError::new("per-cell replay floor regressed"));
    }
    #[cfg(test)]
    let forced = std::mem::take(&mut core.test_hooks.lock().force_cell_root);
    #[cfg(not(test))]
    let forced = false;
    if !forced && floor == adopted.floor && !history.advances() {
        // Nothing retires and no history floor moves: an adoption would only
        // rewrite metadata. The previous root stays sufficient.
        runtime.state.lock().skipped_unchanged += 1;
        core.cell_metrics.root_skipped_unchanged();
        return Ok(());
    }
    let captured = {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let ledger = ReplayLedger::capture(
            state.known,
            adopted.floor,
            floor,
            header.max_frame_cut,
            generation,
            core.config.history_persistence == HistoryPersistence::WalReplay,
        )?;
        let receipts = match state.receipt_history.as_ref() {
            None => None,
            Some(history) => Some((
                cell_migration::through_floor_receipts(state.known, floor),
                history.clone(),
                core.checkpoint_jobs(state.retained_epochs)?,
            )),
        };
        Captured {
            ledger,
            manifest: state.manifest.clone(),
            positions: state.positions.clone(),
            recent: state.recent.clone(),
            jobs: core.checkpoint_jobs(state.retained_epochs)?,
            receipts,
        }
    };
    let capture_ns = elapsed(started);
    step(core, CellStep::Capture)?;
    let Captured {
        ledger,
        mut manifest,
        positions,
        mut recent,
        jobs,
        receipts,
    } = captured;
    let proof_started = Instant::now();
    boundary("cell_owner_proof_before");
    let (positions, mut owner_proofs) = prove_streams(core, jobs, &ledger, positions, &scanned)?;
    // Receipt history: the through-F prefixes become durable history (the
    // next recovery's receipt cursors and retry authority) before the root;
    // then the receipts in (F, U] are proved by extending those prefixes.
    let mut receipt_commit = match receipts {
        None => None,
        Some((receipts, history, jobs)) => {
            let frame_jobs = jobs.clone();
            let commit = cell_migration::commit_receipt_history(core, history, receipts, jobs)?;
            owner_proofs += prove_receipt_frames(core, &ledger, &commit.history, frame_jobs)?;
            Some(commit)
        }
    };
    let proof_wait_ns = elapsed(proof_started);
    step(core, CellStep::OwnerProof)?;
    for receipt in &ledger.retired {
        recent.push_back(*receipt);
        if recent.len() > WAL_REPLAY_WINDOW_BATCHES {
            recent.pop_front();
        }
    }
    let commit_started = Instant::now();
    let commit = commit_cell_root(
        &core.path,
        &core.config,
        &mut manifest,
        CellCommitInputs {
            prepared: &prepared,
            published_next: match floor {
                PrefixCut::Through(batch) if batch.epoch == core.epoch => {
                    batch.sequence.saturating_add(1)
                }
                _ => 0,
            },
            streams: positions
                .values()
                .map(|position| {
                    let key = (position.partition, position.epoch);
                    Ok(CellCheckpointStream {
                        partition: position.partition,
                        epoch: position.epoch,
                        replay_from: position.replay_from,
                        last_offset: position.last_offset,
                        durable_end: position.durable_end,
                        complete: position.complete,
                        receipt_prefix: match receipt_commit.as_mut() {
                            None => None,
                            Some(commit) => {
                                Some(commit.prefixes.remove(&key).ok_or_else(|| {
                                    LocalDbError::new("retained stream has no receipt prefix proof")
                                })?)
                            }
                        },
                    })
                })
                .collect::<Result<_, LocalDbError>>()?,
            recent: recent.iter().copied().collect(),
            history_root: receipt_commit
                .as_ref()
                .and_then(|commit| commit.history.root),
            replay_limits: core.config.replay_limits,
            retirement: ledger.token,
        },
        None,
        &mut |done| step(core, done.cell_step()),
    )?;
    let committed = CellRootCommitted {
        root_bytes: commit.root_bytes,
        wrapper_bytes: commit.metadata_bytes,
        manifest_bytes: encode_metadata(&manifest).map_or(0, |bytes| bytes.len() as u64),
        owner_proofs,
        proof_wait_ns,
        commit_ns: elapsed(commit_started),
    };
    let captured = CellRootCaptured {
        cells: prepared.root().cells.len(),
        files: prepared.root().files.len(),
        capture_ns,
    };
    let metrics = &core.cell_metrics;
    metrics.root_captured(captured.cells as u64, captured.files as u64, capture_ns);
    metrics.root_committed(
        committed.root_bytes,
        committed.wrapper_bytes,
        committed.manifest_bytes,
        committed.owner_proofs as u64,
        committed.proof_wait_ns,
        committed.commit_ns,
    );
    {
        let mut state = runtime.state.lock();
        state.captured = captured;
        state.committed = committed;
    }
    let applied = apply(
        core,
        runtime,
        Adoption {
            manifest,
            commit,
            prepared: &prepared,
            token: ledger.token,
            positions,
            recent,
            receipt_history: receipt_commit.map(|commit| commit.history),
            history,
        },
    )?;
    metrics.root_applied(
        applied.apply_ns,
        applied.returned_batches as u64,
        applied.returned_encoded_bytes as u64,
        applied.returned_identities as u64,
        applied.f_advanced,
    );
    runtime.state.lock().applied = applied;
    collect_garbage(core, runtime)?;
    // The root's base pins drop only after GC selected against its table.
    drop(prepared);
    let mut stats = core.stats.lock();
    stats.write.checkpoint_ns += capture_ns;
    stats.write.background_checkpoint_ns += elapsed(started);
    Ok(())
}

/// WAL-replay stream positions keyed by `(partition, owner epoch)`.
type StreamPositions = BTreeMap<(u32, u64), StreamPosition>;

/// Owner durability proofs through `U` and stream positions through `F`, and
/// the number of owner proofs requested. Owner controls run in parallel per
/// owner, with no serving guard held.
fn prove_streams(
    core: &WriteCore,
    mut jobs: super::write_core::core::CheckpointJobs,
    ledger: &ReplayLedger,
    previous: StreamPositions,
    scanned: &BTreeSet<(u32, u64)>,
) -> Result<(StreamPositions, usize), LocalDbError> {
    let database = core.config.storage.database;
    let fragment = core.config.storage.fragment_bytes;
    let authority_epoch = core.epoch;
    let wal_positions = core.config.history_persistence == HistoryPersistence::WalReplay;
    let output = endpoint_map(&mut jobs, |_, (sender, partition, epochs)| {
        let mut output = Vec::with_capacity(epochs.len());
        for &epoch in epochs.iter() {
            let key = (*partition, epoch);
            let mut position = previous.get(&key).cloned().unwrap_or(StreamPosition {
                partition: *partition,
                epoch,
                replay_from: 0,
                last_offset: None,
                durable_end: 0,
                complete: false,
            });
            // Skip position: the last complete batch at or below F only.
            if let Some(&(offset, _)) = ledger.through_floor.get(&key)
                && position.last_offset.is_none_or(|last| offset > last)
            {
                position.last_offset = Some(offset);
                position.replay_from = offset - offset % fragment;
            }
            // Proof position: every address through U, which can exceed F.
            // WAL replay proves it with an owner position proof. Receipt
            // history owners serve no position proofs (on-disk history), so
            // `prove_receipt_frames` proves the same receipts by a longer
            // receipt prefix before anything is committed.
            let mut proved = false;
            if let Some(&through) = ledger.through_frames.get(&key)
                && through > position.durable_end
            {
                if wal_positions {
                    let (reply, pending) = mpsc::sync_channel(1);
                    sender.send(Command::CheckpointPosition(
                        authority_epoch,
                        epoch,
                        through,
                        reply,
                    ))?;
                    let proof = receive(pending)?;
                    if proof.database != database
                        || proof.partition != *partition
                        || proof.epoch != epoch
                        || proof.durable_end < through
                    {
                        return Err(LocalDbError::new("checkpoint position proof differs"));
                    }
                    position.durable_end = proof.durable_end;
                    proved = true;
                } else {
                    position.durable_end = through;
                }
            }
            // A sealed old stream is complete only when it holds no batch above
            // F, not merely because every byte was synced.
            position.complete |= epoch < authority_epoch
                && scanned.contains(&key)
                && !ledger.above_floor.contains(&key);
            output.push((position, proved));
        }
        Ok(output)
    })?;
    let mut proofs = 0;
    let mut positions = BTreeMap::new();
    for (position, proved) in output.into_iter().flatten() {
        proofs += usize::from(proved);
        positions.insert((position.partition, position.epoch), position);
    }
    Ok((positions, proofs))
}

/// Receipt-history proof through `U` (spec §7.3 step 1, main's decision):
/// never a WAL position proof (an on-disk-history owner rejects it by
/// design). For each stream with receipts in `(F, U]`, ask the fenced owner
/// for its receipt prefix through them and verify the digest by extending the
/// synced through-`F` chain state of `history`. A forged, reordered, missing
/// or extra receipt fails before anything is committed. Returns the number of
/// owner proofs. Owner controls run in parallel per owner, no guard held.
fn prove_receipt_frames(
    core: &WriteCore,
    ledger: &ReplayLedger,
    history: &ReceiptHistory,
    mut jobs: super::write_core::core::CheckpointJobs,
) -> Result<usize, LocalDbError> {
    let database = core.config.storage.database;
    let authority_epoch = core.epoch;
    #[cfg(test)]
    let forge = core.test_hooks.lock().corrupt_checkpoint_proof;
    let proofs = endpoint_map(&mut jobs, |_, (sender, partition, epochs)| {
        let mut proofs = 0usize;
        for &epoch in epochs.iter() {
            let Some(extension) = ledger.frame_receipts.get(&(*partition, epoch)) else {
                continue;
            };
            let from_count = history
                .counts
                .get(&(*partition, epoch))
                .copied()
                .unwrap_or(0);
            let from_state = history.state(*partition, epoch)?;
            let count = from_count
                .checked_add(extension.len())
                .ok_or_else(|| LocalDbError::new("receipt prefix count overflow"))?;
            let (reply, pending) = mpsc::sync_channel(1);
            sender.send(Command::Checkpoint(authority_epoch, epoch, count, reply))?;
            #[cfg_attr(not(test), expect(unused_mut, reason = "forged only in tests"))]
            let mut proof = receive(pending)?;
            #[cfg(test)]
            if forge {
                proof.digest[0] ^= 1;
            }
            cell_migration::verify_prefix_extension(
                database, *partition, epoch, from_count, from_state, extension, &proof,
            )?;
            proofs += 1;
        }
        Ok(proofs)
    })?;
    Ok(proofs.into_iter().sum())
}

/// The durably adopted generation to apply in RAM.
struct Adoption<'a> {
    manifest: AuthorityManifest,
    commit: CellCheckpointCommit,
    prepared: &'a PreparedCellRoot,
    /// The root's history floor `H`, reserved since capture; installed as
    /// the committed retention floor in apply step 1.
    history: RootFloorLease,
    token: RetirementToken,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: VecDeque<RecentReceipt>,
    /// Receipt-history mode: the history through `F` (its root is the
    /// wrapper's), installed before any identity retires.
    receipt_history: Option<ReceiptHistory>,
}

/// Spec §9.2: history and retry authority before identity retirement, then
/// exact credits once. Every step after the manifest is RAM only: a crash
/// recovers from the adopted root, and a failure keeps conservative state
/// (identities and credits retained) under fail-stop.
fn apply(
    core: &WriteCore,
    runtime: &CellRuntime,
    adoption: Adoption<'_>,
) -> Result<CellRootApplied, LocalDbError> {
    let started = Instant::now();
    let Adoption {
        manifest,
        commit,
        prepared,
        token,
        positions,
        recent,
        receipt_history,
        history,
    } = adoption;
    let header = &prepared.root().header;
    let floor = header.replay_floor;
    let previous = runtime.adopted();
    if commit.epoch != core.epoch
        || commit.generation <= previous.generation
        || floor < previous.floor
        || header.history_floor < previous.history_floor
        || history.floor() != header.history_floor
    {
        return Err(LocalDbError::new("adopted root is not monotonic"));
    }
    core.ensure_healthy()?;
    // 1. History floor H (spec §9.2 step 1): the root-floor lease becomes the
    //    committed retention floor; cell captures from now on compact at it,
    //    older bases and in-flight captures over-retain. Then the coverage
    //    floor of new cells: a cell created from now on starts at F.
    {
        let mut writer = core.index.writer();
        let _publisher = core.publisher.lock();
        core.index
            .install_root_floor(history)
            .map_err(LocalDbError::component)?;
        writer
            .set_cell_floor(floor)
            .map_err(LocalDbError::component)?;
    }
    step(core, CellStep::ApplyHistory)?;
    // 2. Retry authority through F before any identity leaves its shard.
    {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        *state.manifest = manifest;
        *state.committed_complete = positions
            .iter()
            .filter_map(|(key, position)| position.complete.then_some(*key))
            .collect();
        *state.positions = positions;
        *state.recent = recent.clone();
        if let Some(history) = receipt_history {
            // A retry that misses the identity table after its entry retires
            // snapshots this root, which holds every receipt through F.
            *state.receipt_history = Some(history);
        }
    }
    step(core, CellStep::ApplyAuthority)?;
    // 3. The retired ledger prefix. Receipts above F, fully represented ones
    //    included, stay; live counters are reduced, never replaced.
    {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let batches = usize::try_from(token.batches)
            .ok()
            .and_then(|batches| state.checkpoint_tail_batches.checked_sub(batches))
            .ok_or_else(|| LocalDbError::new("retired batch count exceeds the tail"))?;
        let bytes = usize::try_from(token.encoded_bytes)
            .ok()
            .and_then(|bytes| state.checkpoint_tail_bytes.checked_sub(bytes))
            .ok_or_else(|| LocalDbError::new("retired byte count exceeds the tail"))?;
        let before = state.known.len();
        state
            .known
            .retain(|_, receipt| !floor.contains(receipt.version()));
        if (before - state.known.len()) as u64 != token.batches {
            return Err(LocalDbError::new("retired receipts differ from the token"));
        }
        *state.checkpoint_tail_batches = batches;
        *state.checkpoint_tail_bytes = bytes;
    }
    {
        let mut state = runtime.state.lock();
        state.adopted = AdoptedCuts::of(header, commit.generation);
        state.root_files = prepared.root().files.clone();
    }
    step(core, CellStep::ApplyLedger)?;
    // 4. Identities, one shard at a time, after every serving guard dropped.
    //    The recent window installed above keeps covered retries answerable.
    let removed = core.retire_checkpoint_identities(mutation_floor(floor), &recent);
    if removed as u64 != token.identities {
        return Err(LocalDbError::new(format!(
            "identity retirement removed {removed}, token certifies {}",
            token.identities
        )));
    }
    step(core, CellStep::IdentityRemove)?;
    // 5. Exact credits once per generation; an unchanged F returns zero.
    let credits = runtime.state.lock().ledger.apply(&token)?;
    let applied = CellRootApplied {
        apply_ns: 0,
        returned_batches: credits.as_ref().map_or(0, |credits| credits.replay_batches),
        returned_encoded_bytes: credits.as_ref().map_or(0, |credits| credits.replay_bytes),
        returned_identities: credits.as_ref().map_or(0, |credits| credits.identities),
        f_advanced: floor > previous.floor,
    };
    if let Some(credits) = credits {
        core.release_checkpoint_credits(credits)?;
    }
    step(core, CellStep::CreditReturn)?;
    Ok(CellRootApplied {
        apply_ns: elapsed(started),
        ..applied
    })
}

/// Spec §10.1, after the manifest is durable and the apply completed: the
/// obsolete root and wrapper, then segments no retained root, live base or
/// pin reaches. Runs only on the checkpoint actor, never during a capture.
fn collect_garbage(core: &WriteCore, runtime: &CellRuntime) -> Result<(), LocalDbError> {
    let roots = runtime.state.lock().root_files.clone();
    let mut manifest = core.publisher.lock().manifest.clone();
    if manifest.checkpoint_obsolete.is_some() {
        // Migration: the last exact commit, retained until a per-cell root
        // was adopted and applied.
        cell_migration::retire_obsolete_exact(&core.path, &mut manifest, &roots)?;
        let _writer = core.index.writer();
        core.publisher.lock().manifest.checkpoint_obsolete = None;
    }
    if let Some(obsolete) = manifest.cell_checkpoint_obsolete.take() {
        for name in [obsolete.root_name(), obsolete.metadata_name()] {
            remove_if_present(&core.path.join(name))?;
            boundary("cell_gc_unlink_after");
        }
        sync_directory(&core.path)?;
        persist_manifest(&core.path, &manifest)?;
        let _writer = core.index.writer();
        core.publisher.lock().manifest.cell_checkpoint_obsolete = None;
    }
    step(core, CellStep::GcUnlink)?;
    for _ in 0..GC_SWEEPS_PER_CHECKPOINT {
        boundary("cell_gc_sweep_before");
        let sweep = runtime
            .store
            .collect_garbage(&[roots.as_slice()], &core.index)
            .map_err(LocalDbError::component)?;
        if !sweep.more {
            break;
        }
    }
    // Legacy exact containers no retained root or live base references.
    cell_migration::collect_legacy_containers(&core.path, &[roots.as_slice()], &core.index)?;
    step(core, CellStep::GcSweep)
}

fn remove_if_present(path: &Path) -> Result<(), LocalDbError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalDbError::component(error)),
    }
}

fn sync_directory(path: &Path) -> Result<(), LocalDbError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(LocalDbError::component)
}

/// Delete unadopted per-cell metadata before any new output (spec §4.3),
/// next to the index-store orphan scan: the staging reservation's names, the
/// obsolete root and wrapper, and any canonical root or staging name that the
/// adopted commit does not name. A crash during this repeats it at the next
/// open.
fn cleanup_metadata(path: &Path, manifest: &mut AuthorityManifest) -> Result<(), LocalDbError> {
    // The obsolete exact commit of a migration is retired against the
    // adopted root's file table first (`cell_migration::retire_obsolete_exact`).
    if manifest.checkpoint_obsolete.is_some() {
        return Err(LocalDbError::new(
            "obsolete exact commit must be retired before per-cell cleanup",
        ));
    }
    let adopted = manifest.cell_checkpoint.clone();
    let adopted_root = adopted.as_ref().map(CellCheckpointCommit::root_name);
    let adopted_wrapper = adopted.as_ref().map(CellCheckpointCommit::metadata_name);
    let mut names = BTreeSet::new();
    if let Some(staging) = &manifest.checkpoint_staging {
        if adopted.as_ref().is_some_and(|commit| {
            (commit.epoch, commit.generation) == (staging.epoch, staging.generation)
        }) {
            return Err(LocalDbError::new(
                "checkpoint staging names the adopted generation",
            ));
        }
        names.extend(staging.names());
    }
    if let Some(obsolete) = &manifest.cell_checkpoint_obsolete {
        names.insert(obsolete.root_name());
        names.insert(obsolete.metadata_name());
    }
    for entry in fs::read_dir(path).map_err(LocalDbError::component)? {
        let entry = entry.map_err(LocalDbError::component)?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let root_staging = name
            .strip_prefix('.')
            .and_then(|name| name.strip_suffix(".pending"))
            .is_some_and(|name| parse_cell_root_name(name).is_some());
        if root_staging || parse_cell_root_name(&name).is_some() {
            names.insert(name);
        }
    }
    let mut deleted = false;
    for name in &names {
        if Some(name) == adopted_root.as_ref() || Some(name) == adopted_wrapper.as_ref() {
            continue;
        }
        if Path::new(name).components().count() != 1 {
            return Err(LocalDbError::new(
                "checkpoint cleanup filename is not relative",
            ));
        }
        let target = path.join(name);
        match fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_file() => {
                remove_if_present(&target)?;
                deleted = true;
                boundary("cell_orphan_delete_after");
            }
            Ok(_) => {
                return Err(LocalDbError::new(
                    "checkpoint cleanup target is not a regular file",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(LocalDbError::component(error)),
        }
    }
    if deleted {
        sync_directory(path)?;
        boundary("cell_orphan_dirsync_after");
    }
    if manifest.checkpoint_staging.is_some() || manifest.cell_checkpoint_obsolete.is_some() {
        manifest.checkpoint_staging = None;
        manifest.cell_checkpoint_obsolete = None;
        persist_manifest(path, manifest)?;
    }
    Ok(())
}

/// Offline recovery flushes into a stream no live flusher uses.
struct OfflineStore {
    writer: IndexStoreWriter,
}

impl OfflineCellFlusher for OfflineStore {
    fn flush(&mut self, prepared: PreparedCell) -> Result<CellFlushResult, LocalDbError> {
        let result = flush_prepared(&mut self.writer, prepared).map_err(LocalDbError::component)?;
        boundary("cell_offline_flush_after");
        Ok(result)
    }
}

/// State recovered from the adopted per-cell root and the retained suffix.
pub(in crate::distributed::remote_db) struct CellRecovered {
    pub index: NativeIndex,
    pub known: HashMap<BatchId, AppendedBatch>,
    pub latest: Option<MutationVersion>,
    pub positions: BTreeMap<(u32, u64), StreamPosition>,
    pub recent: VecDeque<RecentReceipt>,
    /// Receipt-history mode: the history at the adopted wrapper's root with
    /// its through-`F` prefixes; `None` in WAL-replay mode.
    pub receipt_history: Option<ReceiptHistory>,
    /// Streams the adopted wrapper records complete. WAL-file retirement uses
    /// only these: a completeness first observed by this recovery is not
    /// durable until a later root records it.
    pub committed_complete: BTreeSet<(u32, u64)>,
    pub open: CellOpen,
}

/// Recover a per-cell database after every owner was fenced (spec §8.1):
/// authenticate the adopted manifest, wrapper and root; delete unadopted
/// metadata and orphan segments; load the root's bases; replay every whole
/// batch above `F` with per-cell covered-member checks. No serving handle
/// exists until this returns.
#[allow(clippy::too_many_arguments)] // The same fenced state exact recovery uses.
pub(in crate::distributed::remote_db) fn recover(
    path: &Path,
    manifest: &mut AuthorityManifest,
    config: &RemoteDatabaseConfig,
    shape: &KeyShape,
    clients: &mut [Client],
    histories: &[Vec<u64>],
    authority_epoch: u64,
    stats: &mut RecoveryStats,
) -> Result<CellRecovered, LocalDbError> {
    let started = Instant::now();
    let database = config.storage.database;
    let mut positions = BTreeMap::new();
    let mut root = None;
    if let Some(commit) = manifest.cell_checkpoint.clone() {
        if commit.generation == 0
            || commit.epoch >= authority_epoch
            || !manifest
                .epochs
                .iter()
                .any(|epoch| epoch.epoch == commit.epoch && epoch.active)
        {
            return Err(LocalDbError::new(
                "per-cell checkpoint has no committed authority generation",
            ));
        }
        let wrapper_path = path.join(commit.metadata_name());
        if commit.metadata_bytes > MAX_CHECKPOINT_METADATA_BYTES
            || checkpoint_file_digest(&wrapper_path)?
                != (commit.metadata_bytes, commit.metadata_digest)
        {
            return Err(LocalDbError::new(
                "committed checkpoint metadata digest differs",
            ));
        }
        let image: CheckpointImageV4 = read_checkpoint(&wrapper_path)?;
        let loaded = read_cell_root(
            &path.join(commit.root_name()),
            IndexImageMetadata {
                bytes: commit.root_bytes,
                digest: commit.root_digest,
            },
            shape,
            database,
            config.max_index_bytes,
        )
        .map_err(LocalDbError::component)?;
        image.check(&commit, &loaded.header, config, &manifest.shape_yaml)?;
        for stream in &image.streams {
            let node = config
                .endpoints
                .iter()
                .position(|endpoint| endpoint.partition == stream.partition)
                .ok_or_else(|| LocalDbError::new("wrapper stream names an unknown partition"))?;
            if !histories[node].contains(&stream.epoch) || stream.epoch > commit.epoch {
                return Err(LocalDbError::new(
                    "wrapper stream is outside the fenced inventory",
                ));
            }
            StreamCursor::from_wrapper(stream, config.history_persistence)?;
            positions.insert(
                (stream.partition, stream.epoch),
                StreamPosition {
                    partition: stream.partition,
                    epoch: stream.epoch,
                    replay_from: stream.replay_from,
                    last_offset: stream.last_offset,
                    durable_end: stream.durable_end,
                    complete: stream.complete,
                },
            );
        }
        // Every retained stream of an epoch the root covers must be named.
        for (node, epochs) in histories.iter().enumerate() {
            let partition = config.endpoints[node].partition;
            if epochs.iter().any(|epoch| {
                *epoch <= commit.epoch && !positions.contains_key(&(partition, *epoch))
            }) {
                return Err(LocalDbError::new(
                    "wrapper is missing a retained owner stream",
                ));
            }
        }
        root = Some((commit, loaded, image));
    }
    // The through-`F` retry authority of the adopted wrapper (recent window
    // or receipt history); every suffix batch must be absent from it.
    let authority = match &root {
        Some((_, _, image)) => cell_migration::RetryAuthority::from_wrapper(path, config, image)?,
        None => cell_migration::RetryAuthority::empty(path, config)?,
    };
    // A migration's last exact commit is retired against the adopted root's
    // file table (repeated here after a crash), before per-cell cleanup.
    if let Some((_, loaded, _)) = &root {
        cell_migration::retire_obsolete_exact(path, manifest, &loaded.files)?;
    }
    let committed_complete = positions
        .iter()
        .filter_map(|(key, position): (&(u32, u64), &StreamPosition)| {
            position.complete.then_some(*key)
        })
        .collect();
    // Unadopted metadata, then orphan segments, before any new output.
    cleanup_metadata(path, manifest)?;
    let retained: Vec<&[RootFile]> = root
        .iter()
        .map(|(_, root, _)| root.files.as_slice())
        .collect();
    boundary("cell_orphan_scan_before");
    let (store, _orphans) =
        IndexStore::open(path, authority_epoch, &retained).map_err(LocalDbError::component)?;
    boundary("cell_orphan_scan_after");
    let (index, plan, adopted, root_files) = match &root {
        Some((commit, root, _)) => {
            let plan = CoveragePlan::new(root);
            let index = NativeIndex::open_cell_root(
                root,
                path,
                shape,
                database,
                config.max_index_bytes,
                config.index_shards,
                |file| match file.id {
                    IndexStoreFileId::Segment { .. } => store.map_sealed(file.id),
                    IndexStoreFileId::LegacyContainer { .. } => map_root_file(path, file),
                },
            )
            .map_err(LocalDbError::component)?;
            // Legacy exact containers the adopted root no longer references.
            cell_migration::collect_legacy_containers(path, &[root.files.as_slice()], &index)?;
            (
                index,
                plan,
                AdoptedCuts {
                    generation: 0,
                    ..AdoptedCuts::of(&root.header, commit.generation)
                },
                root.files.clone(),
            )
        }
        None => {
            let index = NativeIndex::with_mode(
                shape,
                database,
                config.max_index_bytes,
                config.index_shards,
                CheckpointMode::PerCell,
            )
            .map_err(LocalDbError::component)?;
            (
                index,
                empty_plan(database),
                AdoptedCuts::empty(),
                Vec::new(),
            )
        }
    };
    let floor = adopted.floor;
    stats.checkpoint_loaded = root.is_some();
    stats.checkpoint_load_ns = elapsed(started);
    let replay_started = Instant::now();
    let page_limit = cell_migration::suffix_page_limit(config)?;
    let clients: Vec<RefCell<&mut Client>> = clients.iter_mut().map(RefCell::new).collect();
    let mut streams = cell_migration::open_suffix_streams(
        &clients,
        config,
        histories,
        authority_epoch,
        root.as_ref().map(|(_, _, image)| image),
    )?;
    let flushers = cell_flushers(index.shard_count());
    let mut offline = OfflineStore {
        writer: IndexStoreWriter::new(
            &store,
            MAX_CELL_FLUSHERS as u16,
            cell_scratch_limit(config.max_index_bytes, flushers),
        )
        .map_err(LocalDbError::component)?,
    };
    let replayed = replay_mixed_cuts(
        index,
        plan,
        &mut streams,
        &CellReplayConfig {
            shape,
            limits: config.storage.batch_limits,
            page_limit,
            // Receipt history bounds the tail like exact replay; WAL replay
            // only counts it.
            tail_bound: (config.history_persistence != HistoryPersistence::WalReplay)
                .then_some(manifest.replay_limits),
        },
        &mut offline,
        |id| authority.known(id),
    )?;
    // Release the offline stream before live flushers claim theirs.
    drop(offline);
    // Positions stay through F (the next root's skip input); only the proved
    // end grows. Every read stream is a fully scanned sealed epoch.
    let mut above_floor = BTreeSet::new();
    for receipt in replayed.known.values() {
        above_floor.insert((receipt.address().partition, receipt.version().epoch));
    }
    let mut scanned = BTreeSet::new();
    for stream in streams.iter().filter(|stream| stream.scanned()) {
        let key = stream.key();
        let position = positions.entry(key).or_insert(StreamPosition {
            partition: key.0,
            epoch: key.1,
            replay_from: 0,
            last_offset: None,
            durable_end: 0,
            complete: false,
        });
        position.durable_end = position.durable_end.max(stream.replayed_end());
        position.complete = !above_floor.contains(&key);
        scanned.insert(key);
    }
    drop(streams);
    let latest = match replayed.index.latest_cut() {
        IndexReadCut::At(version) => Some(version),
        _ => None,
    };
    if latest.is_none() && floor != PrefixCut::BeforeAll {
        return Err(LocalDbError::new(
            "recovered watermark is below the replay floor",
        ));
    }
    stats.tail_batches = replayed.stats.batches;
    stats.tail_bytes = replayed.stats.encoded_bytes;
    stats.tail_replay_ns = elapsed(replay_started);
    let (receipt_history, recent) = authority.into_publisher_parts();
    Ok(CellRecovered {
        index: replayed.index,
        known: replayed.known,
        latest,
        positions,
        recent,
        receipt_history,
        committed_complete,
        open: CellOpen {
            store,
            adopted,
            root_files,
            scanned,
            recovery: Some((replayed.stats, started.elapsed())),
        },
    })
}

/// Coverage plan of a database without an adopted root: `F = BeforeAll` and
/// no cell base, so every retained batch is replayed in full.
fn empty_plan(database: [u8; 16]) -> CoveragePlan {
    CoveragePlan::new(&CellRoot {
        // Only the replay floor and the (empty) cell list are read.
        header: CellRootHeader {
            format: 5,
            flags: 0,
            database,
            schema: [0; 32],
            authority_epoch: 0,
            snapshot_generation: 0,
            lifecycle_generation: 0,
            directory_target: PrefixCut::BeforeAll,
            replay_floor: PrefixCut::BeforeAll,
            max_frame_cut: PrefixCut::BeforeAll,
            history_floor: None,
        },
        files: Vec::new(),
        cells: Vec::new(),
    })
}

#[cfg(test)]
#[path = "cell_checkpoint_tests.rs"]
mod tests;
