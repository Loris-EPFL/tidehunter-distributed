//! Option C one-way migration and history-mode suffix authority
//! (checkpoint option C spec §7.1, §8.1 steps 4-6, §9.2, §10.1-§10.3).
//!
//! # Exact -> per-cell adoption ([`adopt_exact`], spec §10.3 steps 1-5)
//!
//! Open recovers an `Exact` database (authority manifest format 2 to 4) with
//! the exact loader and replay, then calls [`adopt_exact`] before any serving
//! handle exists. It:
//!
//! 1. re-reads the adopted exact wrapper (format 2 or 3), bound by the
//!    manifest's length and BLAKE2b-32, and checks it against the recovered
//!    state;
//! 2. treats the old global checkpoint cut as complete cell coverage: a root
//!    capture at `T = old cut` clean-advances every cell, because every
//!    reference through `T` is in a legacy base and every replayed suffix
//!    reference is above `T`. Legacy frames are referenced unchanged under
//!    their `LegacyContainer` file ids; no frame is rewritten or renamed;
//! 3. builds the through-`T` stream cursors (WAL replay: the wrapper-3
//!    positions; receipt history: fresh fenced owner prefix proofs of the
//!    through-`T` receipt counts), empty cursors for every later retained
//!    stream, and keeps the history-mode retry authority (recent window or
//!    history root) unchanged;
//! 4. commits the first format-5 root and format-4 wrapper and adopts
//!    authority manifest format 5 through the per-cell commit protocol
//!    (`cell_checkpoint::commit_cell_root`); the exact commit becomes the
//!    manifest's obsolete commit;
//! 5. installs the adoption in RAM, then retires the obsolete exact root and
//!    wrapper ([`retire_obsolete_exact`]). Legacy containers stay while a
//!    retained root or a live base references them
//!    ([`collect_legacy_containers`]).
//!
//! `F = T`: the retained suffix is exactly the one the exact checkpoint
//! already required, so the recorded replay bound is unchanged and the
//! retirement token returns no credit.
//!
//! # Crash consistency
//!
//! Before the format-5 manifest rename the exact manifest, root, wrapper and
//! suffix stay authoritative. Every per-cell output is named by the durable
//! staging reservation, so the existing exact cleanup removes it at the next
//! open, and a retried migration runs in a new authority epoch, hence with
//! new names. After the rename, every dependency of the new root (legacy
//! containers, receipt history, the owner suffix) was durable before it, and
//! the old exact root and wrapper are deleted only after the manifest's
//! directory sync. There is no downgrade: a format-5 manifest never opens in
//! `Exact` mode, and old binaries accept only formats 2 to 4.
//!
//! # Locks
//!
//! Capture slices take `writer` -> `publisher`, at most
//! `ROOT_DESCRIPTOR_SLICE` cells per hold, like every per-cell root capture.
//! Owner proofs, file reads, writes and syncs run without a serving lock.

use super::cell_checkpoint::{CellCommitInputs, CommitStep, commit_cell_root};
use super::checkpoint_contract::{CellCheckpointStream, RetirementLedger, RetirementToken};
use super::write_core::core::WriteCore;
use super::*;
use crate::distributed::native_index::{
    CellRootParams, IndexStoreFileId, NativeIndexError, PrefixCut, PreparedCellRoot, RootFile,
};
use std::collections::BTreeSet;

mod history;
mod suffix;
pub(super) use history::{commit_receipt_history, through_floor_receipts, verify_prefix_extension};
pub(super) use suffix::{RetryAuthority, open_suffix_streams, suffix_page_limit};

/// Migration boundaries after which a crash can occur; tests inject them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MigrationStep {
    /// Root descriptors captured; nothing written.
    Captured,
    /// A step of the per-cell commit protocol completed.
    Commit(CommitStep),
    /// Adoption installed in RAM; the obsolete exact files still exist.
    Applied,
}

/// The exact state at the adopted exact cut, re-read from its wrapper.
struct ExactCut {
    commit: Option<CheckpointCommit>,
    /// `T`: the exact checkpoint's complete published cut.
    cut: PrefixCut,
    /// WAL-replay mode: positions through `T` and the recent window.
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: Vec<RecentReceipt>,
    /// Receipt-history mode: the durable retry history root through `T`.
    history_root: Option<HistoryPointer>,
}

fn prefix_of(cut: Option<MutationVersion>) -> PrefixCut {
    cut.map_or(PrefixCut::BeforeAll, |cut| PrefixCut::Through(cut.batch))
}

/// Next sequence of `authority_epoch` after `floor` (the wrapper-4 rule).
fn published_next_at(floor: PrefixCut, authority_epoch: u64) -> u64 {
    match floor {
        PrefixCut::Through(batch) if batch.epoch == authority_epoch => {
            batch.sequence.saturating_add(1)
        }
        _ => 0,
    }
}

/// Re-read the adopted exact wrapper. The manifest binds it by length and
/// BLAKE2b-32, so it is the image the exact loader validated at this open.
fn read_exact_cut(
    path: &Path,
    manifest: &AuthorityManifest,
    config: &RemoteDatabaseConfig,
) -> Result<ExactCut, LocalDbError> {
    let Some(commit) = manifest.checkpoint.clone() else {
        return Ok(ExactCut {
            commit: None,
            cut: PrefixCut::BeforeAll,
            positions: BTreeMap::new(),
            recent: Vec::new(),
            history_root: None,
        });
    };
    let metadata = path.join(commit.metadata_name());
    if commit.metadata_bytes > MAX_CHECKPOINT_METADATA_BYTES
        || checkpoint_file_digest(&metadata)? != (commit.metadata_bytes, commit.metadata_digest)
    {
        return Err(LocalDbError::new(
            "committed checkpoint metadata digest differs",
        ));
    }
    let replay = config.history_persistence == HistoryPersistence::WalReplay;
    let (domain, cut) = if replay {
        let image: CheckpointImageV3 = read_checkpoint(&metadata)?;
        let mut positions = BTreeMap::new();
        for position in image.positions {
            if positions
                .insert((position.partition, position.epoch), position)
                .is_some()
            {
                return Err(LocalDbError::new("duplicate checkpoint WAL position"));
            }
        }
        (
            (
                image.format,
                image.database,
                image.shape_yaml,
                image.authority_epoch,
                image.generation,
                image.cut,
                image.published_next,
            ),
            ExactCut {
                commit: None,
                cut: prefix_of(image.cut),
                positions,
                recent: image.recent,
                history_root: None,
            },
        )
    } else {
        let image: CheckpointImage = read_checkpoint(&metadata)?;
        (
            (
                image.format,
                image.database,
                image.shape_yaml,
                image.authority_epoch,
                image.generation,
                image.cut,
                image.published_next,
            ),
            ExactCut {
                commit: None,
                cut: prefix_of(image.cut),
                positions: BTreeMap::new(),
                recent: Vec::new(),
                history_root: image.history_root,
            },
        )
    };
    let (format, database, shape_yaml, epoch, generation, exact_cut, published_next) = domain;
    if format != if replay { 3 } else { 2 }
        || database != config.storage.database
        || shape_yaml != manifest.shape_yaml
        || epoch != commit.epoch
        || generation != commit.generation
        || published_next
            != exact_cut
                .filter(|cut| cut.batch.epoch == commit.epoch)
                .map_or(0, |cut| cut.batch.sequence.saturating_add(1))
    {
        return Err(LocalDbError::new(
            "checkpoint domain, generation or publication cut differs",
        ));
    }
    Ok(ExactCut {
        commit: Some(commit),
        ..cut
    })
}

/// Sealed length of a legacy exact container: committed containers are
/// immutable, and the loader checks every referenced range and digest. A
/// per-cell root keeps referencing legacy containers after migration, so
/// every later root capture uses this for `LegacyContainer` ids.
pub(super) fn legacy_sealed_len(
    path: &Path,
    id: IndexStoreFileId,
) -> Result<u64, NativeIndexError> {
    if !matches!(id, IndexStoreFileId::LegacyContainer { .. }) {
        return Err(NativeIndexError::Invalid(
            "an exact database has no index-store segment",
        ));
    }
    let metadata = fs::symlink_metadata(path.join(id.canonical_name()))?;
    if !metadata.file_type().is_file() {
        return Err(NativeIndexError::Invalid(
            "legacy index container is not a regular file",
        ));
    }
    Ok(metadata.len())
}

/// Through-`T` cursors of every retained `(partition, owner epoch)` stream,
/// the current authority epoch included (writes after adoption land there),
/// sorted by `(partition, epoch)` (spec §7.1, §10.3 step 3).
fn exact_cut_streams(
    core: &WriteCore,
    exact: &ExactCut,
    retained_epochs: &[Vec<u64>],
    history: Option<&ReceiptHistory>,
) -> Result<Vec<CellCheckpointStream>, LocalDbError> {
    let config = &core.config;
    let fragment = config.storage.fragment_bytes;
    // `(partition, epoch) -> owner node`, sorted as the wrapper requires.
    let mut keys = BTreeMap::new();
    for (node, epochs) in retained_epochs.iter().enumerate() {
        let partition = config
            .endpoints
            .get(node)
            .ok_or_else(|| LocalDbError::new("migration owner inventory differs"))?
            .partition;
        for &epoch in epochs {
            if epoch > core.epoch || keys.insert((partition, epoch), node).is_some() {
                return Err(LocalDbError::new("migration retained stream inventory"));
            }
        }
    }
    let Some(history) = history else {
        // WAL replay: the wrapper-3 positions are exactly through `T`; a
        // stream the exact checkpoint did not know has no batch at or below
        // `T` and is replayed from its start.
        if exact.positions.keys().any(|key| !keys.contains_key(key)) {
            return Err(LocalDbError::new(
                "checkpoint WAL position outside retained inventory",
            ));
        }
        return Ok(keys
            .into_iter()
            .map(
                |((partition, epoch), _)| match exact.positions.get(&(partition, epoch)) {
                    Some(position) => CellCheckpointStream {
                        partition,
                        epoch,
                        replay_from: position.replay_from,
                        last_offset: position.last_offset,
                        durable_end: position.durable_end,
                        complete: position.complete,
                        receipt_prefix: None,
                    },
                    None => CellCheckpointStream {
                        partition,
                        epoch,
                        replay_from: 0,
                        last_offset: None,
                        durable_end: 0,
                        complete: false,
                        receipt_prefix: None,
                    },
                },
            )
            .collect());
    };
    // Receipt history: the exact replay never appends to the history, so its
    // counts are still the through-`T` prefixes of the exact wrapper. Each
    // owner proves that prefix again in the current authority epoch.
    if history.counts.keys().any(|key| !keys.contains_key(key)) {
        return Err(LocalDbError::new(
            "checkpoint receipt prefix outside retained inventory",
        ));
    }
    let mut streams = Vec::with_capacity(keys.len());
    for ((partition, epoch), node) in keys {
        let count = history
            .counts
            .get(&(partition, epoch))
            .copied()
            .unwrap_or(0);
        let authority_epoch = core.epoch;
        let proof = core.control_owner(node, move |client| {
            client.checkpoint(authority_epoch, epoch, count)
        })?;
        if proof.database != config.storage.database
            || proof.partition != partition
            || proof.epoch != epoch
            || proof.prefix != count
            || proof.retained < count
            || proof.digest != history.prefix(partition, epoch, count)?
        {
            return Err(LocalDbError::new(
                "fenced owner prefix differs from the exact checkpoint",
            ));
        }
        let (replay_from, last_offset, durable_end) = match count.checked_sub(1) {
            None => (0, None, 0),
            Some(last) => {
                let receipt = history.receipt(partition, epoch, last)?;
                if !exact.cut.contains(receipt.version) {
                    return Err(LocalDbError::new(
                        "checkpoint receipt prefix extends beyond the exact cut",
                    ));
                }
                let offset = receipt.address.offset;
                let end = offset
                    .checked_add(u64::from(receipt.address.len))
                    .ok_or_else(|| LocalDbError::new("checkpoint receipt bounds overflow"))?;
                (offset - offset % fragment, Some(offset), end)
            }
        };
        streams.push(CellCheckpointStream {
            partition,
            epoch,
            replay_from,
            last_offset,
            durable_end,
            // A sealed stream whose retained history is exactly the
            // through-`T` prefix holds no batch above `T`.
            complete: epoch < core.epoch && proof.retained == count,
            receipt_prefix: Some(CheckpointStream {
                state: history.state(partition, epoch)?,
                proof,
            }),
        });
    }
    Ok(streams)
}

/// The first adopted per-cell generation of a migration. The per-cell
/// runtime seeds its adopted cuts, retirement ledger and root file table from
/// it (`cell_checkpoint::install_adopted`) before its first job, so the next
/// generation retires from `F = T`; `root` pins the legacy bases until then.
pub(super) struct ExactAdoption {
    pub commit: CellCheckpointCommit,
    pub root: PreparedCellRoot,
    pub ledger: RetirementLedger,
}

/// One-way `Exact` -> `PerCell` adoption at open (spec §10.3 steps 1-5).
/// Call once, after exact recovery built the write core and before any
/// serving handle exists or any per-cell flush runs (a flushed base could
/// exceed `T`), when the configuration is `PerCell` and the manifest is
/// format 2 to 4. A failure fails the core: the database is dropped and the
/// next open recovers from whichever manifest is durable.
pub(super) fn adopt_exact(core: &Arc<WriteCore>) -> Result<ExactAdoption, LocalDbError> {
    #[cfg(test)]
    if let Some(mut hook) = MIGRATION_HOOK.with(|slot| slot.borrow_mut().take()) {
        return adopt_exact_with(core, &mut *hook);
    }
    adopt_exact_with(core, &mut |_| Ok(()))
}

/// Test-only crash injection for the next [`adopt_exact`] on this thread:
/// open runs adoption on the opening thread, so a thread-local reaches it
/// through the real `RemoteDatabase::open` path. Taken by one adoption.
#[cfg(test)]
type MigrationHook = Box<dyn FnMut(MigrationStep) -> Result<(), LocalDbError>>;
#[cfg(test)]
thread_local! {
    pub(super) static MIGRATION_HOOK: std::cell::RefCell<Option<MigrationHook>> =
        const { std::cell::RefCell::new(None) };
}

fn adopt_exact_with(
    core: &Arc<WriteCore>,
    hook: &mut dyn FnMut(MigrationStep) -> Result<(), LocalDbError>,
) -> Result<ExactAdoption, LocalDbError> {
    let result = adopt(core, hook);
    if let Err(error) = &result {
        // Fail-stop: no later checkpoint or shutdown step may replace the
        // manifest; the next open recovers whichever manifest is durable.
        core.fail(LocalDbError::new(error.message()));
    }
    result
}

fn adopt(
    core: &Arc<WriteCore>,
    hook: &mut dyn FnMut(MigrationStep) -> Result<(), LocalDbError>,
) -> Result<ExactAdoption, LocalDbError> {
    core.ensure_healthy()?;
    // Step 1: the recovered exact state. Nothing has been admitted yet: the
    // database handle does not exist, so the published state is recovery's.
    let (manifest, retained_epochs, history, generation) = {
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let manifest = state.manifest.clone();
        if !matches!(manifest.format, 2..=4)
            || manifest_checkpoint_mode(&manifest)? != CheckpointMode::Exact
            || manifest.checkpoint_staging.is_some()
            || manifest.checkpoint_obsolete.is_some()
            || state.background_checkpoint.is_some()
        {
            return Err(LocalDbError::new(
                "per-cell migration needs a settled exact authority manifest",
            ));
        }
        let generation = state
            .snapshot_generation
            .checked_add(1)
            .ok_or_else(|| LocalDbError::new("snapshot counter exhausted"))?;
        *state.snapshot_generation = generation;
        (
            manifest,
            state.retained_epochs.clone(),
            state.receipt_history.clone(),
            generation,
        )
    };
    let exact = read_exact_cut(&core.path, &manifest, &core.config)?;
    let target = exact.cut;
    let floor = match core.index.retention_floor() {
        IndexReadCut::BeforeAll => None,
        IndexReadCut::At(floor) => Some(floor),
        IndexReadCut::Latest | IndexReadCut::Sparse(_) => {
            return Err(LocalDbError::new("retention floor is not a scalar cut"));
        }
    };
    if !target.holds_floor(floor) {
        return Err(LocalDbError::new(
            "exact retention floor is above the exact cut",
        ));
    }
    if let Some(history) = &history
        && history.root != exact.history_root
    {
        return Err(LocalDbError::new(
            "receipt history root differs from the exact checkpoint",
        ));
    }
    // Step 2: descriptors at `T`, one slice per writer -> publisher hold.
    let mut capture = {
        let writer = core.index.writer();
        let _publisher = core.publisher.lock();
        writer
            .begin_cell_root_capture_at(target)
            .map_err(LocalDbError::component)?
    };
    capture.reserve().map_err(LocalDbError::component)?;
    loop {
        let mut writer = core.index.writer();
        let _publisher = core.publisher.lock();
        if capture
            .capture_slice(&mut writer)
            .map_err(LocalDbError::component)?
        {
            break;
        }
    }
    let complete_prefix = core.index.published_prefix();
    let prepared = capture
        .finish(
            &core.index,
            &core.path,
            CellRootParams {
                authority_epoch: core.epoch,
                snapshot_generation: generation,
                // No lifecycle event has happened in this index yet.
                lifecycle_generation: 0,
                history_floor: floor,
            },
            complete_prefix,
            |id| legacy_sealed_len(&core.path, id),
        )
        .map_err(LocalDbError::component)?;
    let header = &prepared.root().header;
    // Every cell must clean-advance to `T`, and no legacy frame can be
    // newer than the exact cut that wrote it.
    if header.replay_floor != target || header.max_frame_cut != target {
        return Err(LocalDbError::new(
            "exact checkpoint cut is not complete cell coverage",
        ));
    }
    hook(MigrationStep::Captured)?;
    // Step 3: through-`T` cursors and retry authority.
    let streams = exact_cut_streams(core, &exact, &retained_epochs, history.as_ref())?;
    let mut adopted = manifest.clone();
    let inputs = CellCommitInputs {
        prepared: &prepared,
        published_next: published_next_at(target, core.epoch),
        streams,
        recent: exact.recent.clone(),
        history_root: exact.history_root,
        replay_limits: manifest.replay_limits,
        retirement: RetirementToken {
            generation,
            from: target,
            through: target,
            batches: 0,
            encoded_bytes: 0,
            identities: 0,
        },
    };
    let positions = baseline_positions(&inputs.streams, history.is_none());
    // Step 4: the only adoption point is the format-5 manifest rename.
    let commit = commit_cell_root(
        &core.path,
        &core.config,
        &mut adopted,
        inputs,
        exact.commit.clone(),
        &mut |step| hook(MigrationStep::Commit(step)),
    )?;
    // Step 5: RAM apply. The suffix above `T` stays charged: no credit.
    {
        let mut writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        writer
            .set_cell_floor(target)
            .map_err(LocalDbError::component)?;
        writer.adopt_per_cell().map_err(LocalDbError::component)?;
        let state = publisher.checkpoint_state();
        *state.manifest = adopted.clone();
        if let Some(positions) = positions {
            // Through-`T` positions are the baseline of the next per-cell
            // root: the post-replay stream ends are not a through-F state
            // once F lags the replayed suffix.
            *state.committed_complete = positions
                .iter()
                .filter_map(|(key, position)| position.complete.then_some(*key))
                .collect();
            *state.positions = positions;
        }
    }
    hook(MigrationStep::Applied)?;
    retire_obsolete_exact(&core.path, &mut adopted, &prepared.root().files)?;
    {
        let _writer = core.index.writer();
        core.publisher
            .lock()
            .checkpoint_state()
            .manifest
            .clone_from(&adopted);
    }
    Ok(ExactAdoption {
        commit,
        root: prepared,
        ledger: RetirementLedger {
            generation,
            floor: target,
        },
    })
}

/// WAL-replay positions of `streams`; `None` in receipt-history mode.
fn baseline_positions(
    streams: &[CellCheckpointStream],
    replay: bool,
) -> Option<BTreeMap<(u32, u64), StreamPosition>> {
    replay.then(|| {
        streams
            .iter()
            .map(|stream| {
                (
                    (stream.partition, stream.epoch),
                    StreamPosition {
                        partition: stream.partition,
                        epoch: stream.epoch,
                        replay_from: stream.replay_from,
                        last_offset: stream.last_offset,
                        durable_end: stream.durable_end,
                        complete: stream.complete,
                    },
                )
            })
            .collect()
    })
}

/// Validate a cleanup name: one relative component, a regular file or absent.
fn check_cleanup_target(path: &Path, name: &str) -> Result<(), LocalDbError> {
    if Path::new(name).components().count() != 1 {
        return Err(LocalDbError::new(
            "checkpoint cleanup filename is not relative",
        ));
    }
    match fs::symlink_metadata(path.join(name)) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalDbError::component(error)),
        Ok(_) => Err(LocalDbError::new(
            "checkpoint cleanup target is not a regular file",
        )),
    }
}

fn remove_if_present(path: &Path, name: &str) -> Result<(), LocalDbError> {
    match fs::remove_file(path.join(name)) {
        Ok(()) => Ok(()),
        // An interrupted earlier cleanup may already have unlinked it.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalDbError::component(error)),
    }
}

/// Delete the obsolete exact commit of a format-5 manifest (spec §10.3 step
/// 5): its format-4 root, its format-2/3 wrapper and every container it
/// listed that `retained_root_files` (the adopted and every still-retained
/// per-cell root) does not reference. Then sync the directory, clear
/// `checkpoint_obsolete` and persist the manifest. Call only after the
/// format-5 manifest's directory sync. Idempotent: a crash repeats it.
pub(super) fn retire_obsolete_exact(
    path: &Path,
    manifest: &mut AuthorityManifest,
    retained_root_files: &[RootFile],
) -> Result<(), LocalDbError> {
    let Some(obsolete) = manifest.checkpoint_obsolete.clone() else {
        return Ok(());
    };
    let adopted = manifest
        .cell_checkpoint
        .as_ref()
        .map(|commit| (commit.epoch, commit.generation));
    if manifest_checkpoint_mode(manifest)? != CheckpointMode::PerCell
        || adopted.is_none_or(|adopted| (obsolete.epoch, obsolete.generation) >= adopted)
        || obsolete.generation == 0
        || !manifest
            .epochs
            .iter()
            .any(|epoch| epoch.epoch == obsolete.epoch && epoch.active)
    {
        return Err(LocalDbError::new(
            "obsolete exact commit conflicts with the per-cell authority",
        ));
    }
    let referenced: BTreeSet<&str> = retained_root_files
        .iter()
        .map(|file| file.name.as_str())
        .collect();
    let mut names = CheckpointFiles {
        epoch: obsolete.epoch,
        generation: obsolete.generation,
        index_files: obsolete.index_files.clone(),
    }
    .names();
    names.retain(|name| !referenced.contains(name.as_str()));
    // Validate every target before deleting anything.
    for name in &names {
        check_cleanup_target(path, name)?;
    }
    for name in &names {
        remove_if_present(path, name)?;
    }
    // Unlink never modifies mapped immutable bytes; names are never reused.
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(LocalDbError::component)?;
    manifest.checkpoint_obsolete = None;
    persist_manifest(path, manifest)
}

/// Delete legacy exact containers that no retained root references and no
/// live base names (spec §10.1, §10.3 step 5). The per-cell protocol never
/// creates a legacy container, so no concurrent writer can race this scan.
///
/// Precondition (as `IndexStore::collect_garbage`): call only from the
/// metadata actor, after the adopted manifest's directory sync and live
/// apply, never between a manifest rename and its directory sync and never
/// during a root capture; `retained_roots` lists every still-retained root.
/// Mappings stay valid after the unlink; the live check covers bases a reload
/// moved to heap memory, which keep the file name but no mapping.
pub(super) fn collect_legacy_containers(
    path: &Path,
    retained_roots: &[&[RootFile]],
    index: &NativeIndex,
) -> Result<Vec<IndexStoreFileId>, LocalDbError> {
    let mut keep: BTreeSet<String> = retained_roots
        .iter()
        .flat_map(|root| root.iter().map(|file| file.name.clone()))
        .collect();
    // `reachable_files` lists container file names of live bases.
    keep.extend(index.reachable_files());
    let mut deleted = Vec::new();
    for entry in fs::read_dir(path).map_err(LocalDbError::component)? {
        let entry = entry.map_err(LocalDbError::component)?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(id @ IndexStoreFileId::LegacyContainer { .. }) =
            IndexStoreFileId::parse_canonical(&name)
        else {
            continue;
        };
        if keep.contains(&name) {
            continue;
        }
        check_cleanup_target(path, &name)?;
        remove_if_present(path, &name)?;
        deleted.push(id);
    }
    if !deleted.is_empty() {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(LocalDbError::component)?;
    }
    Ok(deleted)
}

#[cfg(test)]
#[path = "cell_migration_tests.rs"]
mod tests;
