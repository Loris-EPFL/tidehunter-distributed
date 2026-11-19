//! Monotone, never-reused fencing generations with bounded retained state
//! (roadmap Phase 5 L1).
//!
//! The authority manifest allocates generations from `next_generation` and
//! keeps only generations whose streams may still be needed: the last served
//! one, every older one still referenced, and aborted reservations until their
//! zero-data proof trims them. Durable order of one open:
//!
//! 1. one manifest replacement turns a previous `Reserved` into `Aborted`,
//!    appends `Reserved(g)` and advances `next_generation`; only then is any
//!    owner fenced to `g`;
//! 2. after every fence, a frozen page sweep of every owner ledger, checkpoint
//!    load and replay of the retained union;
//! 3. the open-time durable-cut record, then `Served(g)`;
//! 4. offline trim of each eligible older generation `s`, one at a time:
//!    owner readiness from every owner, then ONE manifest replacement that
//!    excludes `s`, sets `trim_pending = Some(s)` and, when the current Exact
//!    root names `s`, adopts a replacement root produced by `g`; owner trims;
//!    the cut truncation of `s` is forgotten; `trim_pending` is cleared.
//!
//! Step 4 runs on the constructed core before the durable-cut actor starts and
//! before the handle is returned, so no foreground write, read or durability
//! poll can observe or race it.

use super::durable_cut::{self, CutRecord};
use super::write_core::core::WriteCore;
use super::*;
use crate::distributed::tcp::{
    InventoryGeneration, NodeStatus, OwnerGenerationState, TrimReadiness,
};

/// Authority manifest format with generation states, `next_generation` and
/// `trim_pending` (number assigned at merge). Format 5 is the deleted per-cell
/// mode; formats 2 to 4 are pre-cutover Exact manifests.
pub(super) const AUTHORITY_FORMAT: u32 = 6;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum GenerationState {
    /// Allocated and possibly fenced; no handle has been returned.
    Reserved,
    /// May have served: a handle could have been returned. Not "writable now".
    Served,
    /// A reservation replaced by a later one; it never served.
    Aborted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuthorityGeneration {
    pub generation: u64,
    pub state: GenerationState,
}

#[derive(Deserialize)]
struct FormatProbe {
    format: u32,
    #[serde(flatten)]
    mode: AuthorityMode,
}

/// Read the authority manifest; another format fails with its number before
/// any current field is interpreted, and the store is never rewritten.
pub(super) fn read_authority_manifest(path: &Path) -> Result<AuthorityManifest, LocalDbError> {
    let probe: FormatProbe = read_metadata(path).map_err(LocalDbError::component)?;
    probe.mode.reject_legacy_mode(probe.format)?;
    match probe.format {
        AUTHORITY_FORMAT => read_metadata(path).map_err(LocalDbError::component),
        format => Err(LocalDbError::new(format!(
            "unsupported distributed authority manifest format {format} (expected {AUTHORITY_FORMAT})"
        ))),
    }
}

/// Manifest invariants: nonzero strictly ascending generations below
/// `next_generation` (holes allowed); at most one `Reserved`, and only as the
/// highest allocated generation; `trim_pending` allocated, older than the
/// newest retained generation and not retained.
pub(super) fn validate(manifest: &AuthorityManifest) -> Result<(), LocalDbError> {
    let next = manifest.next_generation;
    let generations = &manifest.generations;
    let reserved = generations
        .iter()
        .filter(|entry| entry.state == GenerationState::Reserved)
        .count();
    let invalid = next == 0
        || generations
            .iter()
            .any(|entry| entry.generation == 0 || entry.generation >= next)
        || generations
            .windows(2)
            .any(|pair| pair[0].generation >= pair[1].generation)
        || reserved > 1
        || (reserved == 1
            && generations.last().is_none_or(|last| {
                last.state != GenerationState::Reserved
                    || last.generation.checked_add(1) != Some(next)
            }))
        || manifest.trim_pending.is_some_and(|pending| {
            pending == 0
                || pending >= next
                || generations.iter().any(|entry| entry.generation == pending)
                || generations
                    .last()
                    .is_none_or(|last| last.generation < pending)
        });
    if invalid {
        return Err(LocalDbError::new(
            "authority fencing generation history is inconsistent",
        ));
    }
    Ok(())
}

/// Allocate the next generation. One durable replacement (by the caller) holds
/// the folded `Aborted` predecessor, `Reserved(g)` and the advanced counter, so
/// no durable manifest can hold two reservations. Overflow mutates nothing.
pub(super) fn reserve(
    manifest: &mut AuthorityManifest,
    database: [u8; 16],
) -> Result<u64, LocalDbError> {
    let generation = manifest.next_generation;
    let next = generation.checked_add(1).ok_or_else(|| {
        LocalDbError::new(format!(
            "fencing generation exhausted for database {}",
            hex(&database)
        ))
    })?;
    for entry in &mut manifest.generations {
        if entry.state == GenerationState::Reserved {
            entry.state = GenerationState::Aborted;
        }
    }
    manifest.generations.push(AuthorityGeneration {
        generation,
        state: GenerationState::Reserved,
    });
    manifest.next_generation = next;
    Ok(generation)
}

/// Record that `generation` may serve. Persisted after the open-time cut
/// record and before any `generation`-owned Exact adoption or trim.
pub(super) fn mark_served(
    manifest: &mut AuthorityManifest,
    generation: u64,
) -> Result<(), LocalDbError> {
    match manifest.generations.last_mut() {
        Some(last) if last.generation == generation && last.state == GenerationState::Reserved => {
            last.state = GenerationState::Served;
            Ok(())
        }
        _ => Err(LocalDbError::new(
            "authority reservation changed before activation",
        )),
    }
}

pub(super) fn is_served(manifest: &AuthorityManifest, generation: u64) -> bool {
    manifest
        .generations
        .iter()
        .any(|entry| entry.generation == generation && entry.state == GenerationState::Served)
}

/// Retained generations below `current`, ascending, with `true` for an aborted
/// reservation (whose stream must hold no accepted data).
pub(super) fn retained_below(manifest: &AuthorityManifest, current: u64) -> Vec<(u64, bool)> {
    manifest
        .generations
        .iter()
        .filter(|entry| entry.generation < current && entry.state != GenerationState::Reserved)
        .map(|entry| (entry.generation, entry.state == GenerationState::Aborted))
        .collect()
}

/// Pre-reservation owner check: no owner can have accepted a generation this
/// authority never allocated.
pub(super) fn check_owner_status(
    partition: u32,
    status: &NodeStatus,
    manifest: &AuthorityManifest,
) -> Result<(), LocalDbError> {
    let allocated = manifest.next_generation.saturating_sub(1);
    if status.highest_fenced_generation > allocated {
        return Err(disagrees(
            partition,
            format_args!(
                "owner high-water {} exceeds the allocated generation {allocated}",
                status.highest_fenced_generation
            ),
        ));
    }
    Ok(())
}

/// Validate one owner's complete fenced ledger against the authority and
/// return the owner history used for checkpoint load and replay: retained
/// authority generations below `current` that the owner holds. A generation
/// named by authority `trim_pending` is the only allowed unknown entry, and it
/// never enters the history (no default WAL position is planned for it).
pub(super) fn owner_history(
    partition: u32,
    entries: &[InventoryGeneration],
    manifest: &AuthorityManifest,
    current: u64,
) -> Result<Vec<u64>, LocalDbError> {
    let Some((last, older)) = entries.split_last() else {
        return Err(disagrees(partition, "the fenced generation is absent"));
    };
    if last.generation != current || last.state != OwnerGenerationState::Active {
        return Err(disagrees(
            partition,
            format_args!("generation {current} is not the newest active entry"),
        ));
    }
    let mut history = Vec::with_capacity(older.len());
    for entry in older {
        if !matches!(
            entry.state,
            OwnerGenerationState::Sealed | OwnerGenerationState::Abandoned
        ) {
            return Err(disagrees(
                partition,
                format_args!("older generation {} is not sealed", entry.generation),
            ));
        }
        let retained = manifest.generations.iter().any(|authority| {
            authority.generation == entry.generation && authority.state != GenerationState::Reserved
        });
        if retained {
            history.push(entry.generation);
        } else if manifest.trim_pending != Some(entry.generation) {
            return Err(disagrees(
                partition,
                format_args!("unknown generation {}", entry.generation),
            ));
        }
    }
    for served in manifest
        .generations
        .iter()
        .filter(|entry| entry.generation < current && entry.state == GenerationState::Served)
    {
        if history.binary_search(&served.generation).is_err() {
            return Err(disagrees(
                partition,
                format_args!("served generation {} is absent", served.generation),
            ));
        }
    }
    Ok(history)
}

/// Charge the actual retained streams, not a configured lifetime quota, against
/// the checkpoint metadata budget before the replayed root can be built.
pub(super) fn check_stream_budget(
    config: &RemoteDatabaseConfig,
    histories: &[Vec<u64>],
) -> Result<(), LocalDbError> {
    let streams = histories
        .iter()
        .try_fold(config.endpoints.len(), |total, history| {
            total.checked_add(history.len())
        });
    let receipts = config
        .replay_limits
        .max_batches
        .min(config.max_admitted_batches);
    if streams.is_none_or(|streams| !checkpoint_metadata_fits(streams, receipts)) {
        return Err(LocalDbError::new(format!(
            "checkpoint retained-stream metadata exceeds {MAX_CHECKPOINT_METADATA_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Apply a trim prepared for this checkpoint at its root adoption, in the same
/// manifest replacement that adopts the root. `adopt_trim` is never persisted,
/// so the staging install before the image keeps the stream retained.
pub(super) fn apply_adoption_trim(manifest: &mut AuthorityManifest) {
    if let Some(generation) = manifest.adopt_trim.take() {
        manifest
            .generations
            .retain(|entry| entry.generation != generation);
        manifest.trim_pending = Some(generation);
    }
}

/// Test-only coordinator crash points inside the trim transaction.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TrimCrash {
    /// Every owner reported readiness; nothing was committed.
    BeforeCommit,
    /// The manifest excludes the generation and names it in `trim_pending`.
    AfterCommit,
    /// This many owners acknowledged the trim.
    AfterOwnerAcks(usize),
    /// The cut truncation was forgotten; `trim_pending` is still set.
    BeforeClear,
}

/// Offline maintenance of step 4. `record` is the open-time durable-cut record
/// that the actor will own once this returns.
pub(super) fn maintain(
    core: &Arc<WriteCore>,
    record: &mut Option<CutRecord>,
    #[cfg(test)] crash: Option<TrimCrash>,
) -> Result<(), LocalDbError> {
    let pending = core.publisher.lock().manifest.trim_pending;
    if let Some(generation) = pending {
        finish_trim(
            core,
            record,
            generation,
            #[cfg(test)]
            crash,
        )?;
    }
    let candidates: Vec<u64> = {
        let publisher = core.publisher.lock();
        retained_below(&publisher.manifest, core.epoch)
            .into_iter()
            .map(|(generation, _)| generation)
            .collect()
    };
    for generation in candidates {
        if !coordinator_eligible(core, record.as_ref(), generation) {
            continue;
        }
        if !owners_ready(core, generation)? {
            continue;
        }
        #[cfg(test)]
        if crash == Some(TrimCrash::BeforeCommit) {
            return Err(injected(core, "before trim commit"));
        }
        commit_trim(core, generation)?;
        finish_trim(
            core,
            record,
            generation,
            #[cfg(test)]
            crash,
        )?;
    }
    Ok(())
}

/// Coordinator-side proof that no selectable state needs bytes, positions or
/// proofs of `generation`: no index value, tail receipt, retry identity or
/// incomplete replay position names it, and the tracked cut does not.
fn coordinator_eligible(core: &WriteCore, record: Option<&CutRecord>, generation: u64) -> bool {
    if record.is_some_and(|record| record.live_generation() == Some(generation)) {
        return false;
    }
    {
        let publisher = core.publisher.lock();
        if publisher
            .recent
            .iter()
            .any(|receipt| receipt.version.epoch == generation)
            || publisher
                .known
                .values()
                .any(|receipt| receipt.version().epoch == generation)
            || publisher
                .positions
                .iter()
                .any(|((_, stream), position)| *stream == generation && !position.complete)
        {
            return false;
        }
    }
    !core.config.endpoints.iter().any(|endpoint| {
        core.index
            .references_wal_file(endpoint.partition, generation, 0, u64::MAX)
    })
}

/// Every owner must accept the candidate; `Blocked` defers it. An unreachable
/// RF1 owner is an error, never absence.
fn owners_ready(core: &WriteCore, generation: u64) -> Result<bool, LocalDbError> {
    let authority = core.epoch;
    for node in 0..core.config.endpoints.len() {
        let readiness = core.control_owner(node, move |client| {
            client.check_trim_generation(authority, generation)
        })?;
        if readiness == TrimReadiness::Blocked {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The atomic authority commit: exclude `generation`, record `trim_pending`
/// and, when the current root names it, adopt a replacement root produced by
/// the already served current generation, all in one manifest replacement.
fn commit_trim(core: &Arc<WriteCore>, generation: u64) -> Result<(), LocalDbError> {
    let names_generation = {
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        // The committed root, not the in-memory positions: a stream that only
        // appeared after the root (an empty generation after a crash without
        // checkpoint) needs no replacement root.
        let names = state
            .manifest
            .checkpoint
            .as_ref()
            .is_some_and(|commit| commit.epoch == generation)
            || state
                .committed_streams
                .iter()
                .any(|(_, stream)| *stream == generation);
        // Checkpoint inputs and in-memory histories drop the stream now; a
        // replacement capture therefore has complete coverage without it.
        for streams in state.retained_epochs.iter_mut() {
            streams.retain(|stream| *stream != generation);
        }
        state
            .positions
            .retain(|(_, stream), _| *stream != generation);
        state
            .committed_complete
            .retain(|(_, stream)| *stream != generation);
        state
            .committed_streams
            .retain(|(_, stream)| *stream != generation);
        state
            .stream_last
            .retain(|(_, stream), _| *stream != generation);
        if names {
            state.manifest.adopt_trim = Some(generation);
        } else {
            state
                .manifest
                .generations
                .retain(|entry| entry.generation != generation);
            state.manifest.trim_pending = Some(generation);
        }
        names
    };
    if names_generation {
        // Obsolete-root cleanup completes inside the commit, before any Trim.
        let result = core.commit_checkpoint();
        let mut publisher = core.publisher.lock();
        publisher.manifest.adopt_trim = None;
        result?;
        if publisher.manifest.trim_pending != Some(generation) {
            return Err(LocalDbError::new(
                "replacement checkpoint did not commit the generation trim",
            ));
        }
        Ok(())
    } else {
        let manifest = core.publisher.lock().manifest.clone();
        persist_manifest(&core.path, &manifest)
    }
}

/// Owner trims, then the cut truncation, then the authority intent. Repeatable
/// after any crash: owners treat an already-trimmed generation as success only
/// with the same absent-state checks.
fn finish_trim(
    core: &WriteCore,
    record: &mut Option<CutRecord>,
    generation: u64,
    #[cfg(test)] crash: Option<TrimCrash>,
) -> Result<(), LocalDbError> {
    #[cfg(test)]
    if crash == Some(TrimCrash::AfterCommit) {
        return Err(injected(core, "after trim commit"));
    }
    let authority = core.epoch;
    for node in 0..core.config.endpoints.len() {
        core.control_owner(node, move |client| {
            client.trim_generation(authority, generation)
        })?;
        #[cfg(test)]
        if crash == Some(TrimCrash::AfterOwnerAcks(node + 1)) {
            return Err(injected(core, "after owner trim acknowledgements"));
        }
    }
    if let Some(record) = record.as_mut()
        && record.forget_retired_generation(generation)?
    {
        durable_cut::persist_open_record(&core.path, record)?;
    }
    #[cfg(test)]
    if crash == Some(TrimCrash::BeforeClear) {
        return Err(injected(core, "before trim intent clear"));
    }
    let manifest = {
        let mut publisher = core.publisher.lock();
        if publisher.manifest.trim_pending != Some(generation) {
            return Err(LocalDbError::new("authority trim intent changed"));
        }
        publisher.manifest.trim_pending = None;
        publisher.manifest.clone()
    };
    persist_manifest(&core.path, &manifest)
}

/// A simulated power cut: every earlier step was installed with a synced
/// rename, and the graceful shutdown checkpoint is suppressed so the next open
/// sees exactly the durable state at this boundary.
#[cfg(test)]
fn injected(core: &WriteCore, point: &str) -> LocalDbError {
    core.test_hooks.lock().skip_shutdown_checkpoint = true;
    LocalDbError::new(format!("injected coordinator crash {point}"))
}

fn disagrees(partition: u32, reason: impl std::fmt::Display) -> LocalDbError {
    LocalDbError::new(format!(
        "owner {partition} fencing inventory disagrees: {reason}"
    ))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
#[path = "fencing_generations_tests.rs"]
mod tests;
