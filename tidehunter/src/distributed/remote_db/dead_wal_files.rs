//! Coordinator pass: retire dead owner WAL files of every retained stream,
//! keyed to the native value watermark (CAPACITY-R0-R1.md §4 on the native
//! index). No Exact root and no reference walk decide a victim.
//!
//! One pass, under the cold `WriteCore::retirement` gate (the caller holds
//! it; foreground writes, reads and snapshots never take it):
//! 1. Freeze, per retained stream `(partition, fencing generation)`, the
//!    native two-term watermark `min(adopted relocation callback start,
//!    durable control-region replay_from)`. Bytes below `replay_from` are
//!    represented by the stored index, so recovery never replays them; bytes
//!    below the callback start hold no live value, because every survivor of
//!    an unfiltered keyspace was copied to the serving generation and
//!    re-pointed before the start moved past it, and a filtered keyspace
//!    dropped the entries below the start.
//! 2. Serving stream: page the owner inventory over a dedicated authenticated
//!    session, never the owner worker's control lane. A victim needs an exact
//!    durable seal of the observed inode, its file end at or below the
//!    watermark, every batch below the persisted Version B cut, and a whole
//!    file inside the per-owner span budget. The owner rechecks every bound
//!    it can compare, drains and disables the exact namespace, and answers
//!    `Busy` without a durable decision when a pin, an unprepared successor
//!    or the pending bound stands in the way.
//! 3. Older retained generations: page the owner's retirement list and push
//!    the same watermark as a bare proof (`retire_files_if_idle`); the owner
//!    runs its sealed-stream path. Readiness to trim a generation is a
//!    separate check (see `fencing_generations`).
//!
//! A held checkpoint does not keep owner WAL files, exactly as native
//! `Db::checkpoint` does not (its docs: "does not itself hold WAL files from
//! being reclaimed, so it is intended for short-lived reads"). The watermark
//! is the two stored terms alone. A read through a checkpoint whose frame a
//! pass reclaimed answers as native answers for an unreachable position:
//! absent (`hybrid::read` maps the retired member to `None` on a latched view).
//!
//! Nothing here is fatal for the authority: a failed or refused request made
//! no coordinator state change, and the next pass re-pages the owner.

use super::file_inventory::{
    FileInventoryReply, FileUseState, MAX_FILE_INVENTORY_PAGE, OwnerSpan, WalFileObservation,
    span_budget_end,
};
use super::write_core::core::WriteCore;
use super::{Client, LocalDbError};
use crate::distributed::storage::{RetireProof, RetirementWitness, WalInode};
use crate::distributed::tcp::checkpoint::RetirementResult;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// Owner inventory pages per pass and owner; a stream has far fewer files.
const MAX_PAGES_PER_STREAM: usize = 1 << 16;
/// Proofs per request; equal to the owner's bounded pending set.
const MAX_PROOFS_PER_REQUEST: usize = 256;
/// Sealed-stream files per owner page and retirement request.
const MAX_FILES_PER_PAGE: usize = 256;

/// One stream's frozen watermark and the terms it was taken from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StreamWatermark {
    /// Exclusive end of the dead prefix: `min(callback_from, replay_from)`.
    end: u64,
    /// Durable control-region `replay_from` of this stream.
    replay_from: u64,
    /// Adopted relocation callback start; `0` when no pass has adopted one,
    /// whatever the shape. An unfiltered shape still copies every record it
    /// reclaims, so this term is never vacuous.
    callback_from: u64,
}

/// Frozen per-pass evidence: one watermark per retained stream plus the
/// Version B cut of the serving generation.
struct Snapshot {
    watermark: BTreeMap<(u32, u64), StreamWatermark>,
    /// Contiguous durable end per stream, from the publisher positions. It
    /// bounds the per-owner span budget, never a victim's coverage.
    durable_end: BTreeMap<(u32, u64), u64>,
    /// Persisted `CutRecord.live.next` of the serving generation.
    ///
    /// `None` means one of exactly two states, never "unknown": Version B is
    /// disabled at the native tier, where every acknowledged write is already
    /// durable by the tier's own contract, so the B term is redundant; or the
    /// record belongs to another generation, which defers the pass entirely.
    /// A future config that disables Version B at the native tier must keep
    /// that invariant or make this `None` impossible for a serving stream.
    persisted_cut_next: Option<u64>,
    /// Retained generations per owner, read once with the watermark.
    retained: Vec<Vec<u64>>,
}

/// Retire every dead sealed file of every retained stream. The caller holds
/// `core.retirement` (as `gate`); no other coordinator lock is held across a
/// page, an RPC or an unlink. Every early `Ok(())` below is a deferral that made
/// no decision; the next pass retries.
pub(in crate::distributed::remote_db) fn core_retire_dead_wal_files(
    core: &WriteCore,
) -> Result<(), LocalDbError> {
    let snapshot = take_snapshot(core);
    let tls_config = core
        .config
        .tls
        .client_config_with_identity()
        .map_err(LocalDbError::component)?;
    for node in 0..core.config.endpoints.len() {
        let endpoint = &core.config.endpoints[node];
        let mut tls = core.config.tls.clone();
        tls.server_name = endpoint.server_name.clone();
        let mut client = Client::connect_shared(
            &endpoint.address,
            &tls,
            tls_config.clone(),
            core.config.timeout,
            core.config.partition_config(1, endpoint.partition),
            &core.shape,
        )
        .map_err(|error| {
            LocalDbError::new(format!(
                "retirement session to partition {} failed: {error}",
                endpoint.partition
            ))
        })?;
        // The retirement request first disconnects the coordinator's own idle
        // read controllers of the victim files (`evict_retirement_files`). A
        // session without the pool skips that step and the owner then finds
        // the lanes' cached connection pins on the victims.
        if let Some(pool) = &core.live_pool {
            client.set_live_pool(pool.clone());
        }
        let Some(generations) = snapshot.retained.get(node) else {
            continue;
        };
        for &generation in generations {
            let key = (endpoint.partition, generation);
            let (Some(&watermark), Some(&stream_end)) =
                (snapshot.watermark.get(&key), snapshot.durable_end.get(&key))
            else {
                continue;
            };
            if generation == core.epoch {
                retire_serving_stream(
                    core,
                    &mut client,
                    node,
                    endpoint.partition,
                    watermark,
                    stream_end,
                    snapshot.persisted_cut_next,
                )?;
            } else {
                retire_sealed_stream(
                    core,
                    &mut client,
                    node,
                    endpoint.partition,
                    generation,
                    watermark,
                    stream_end,
                )?;
            }
        }
    }
    Ok(())
}

fn take_snapshot(core: &WriteCore) -> Snapshot {
    // Two guards of the deleted Exact design are replaced, not lost:
    // `install_seq`/the installed-cut bound existed to freeze one image while
    // walking its references, and `referenced_files` existed because a file
    // could hold a live value the index still named. The watermark now supplies
    // both: the control region is stored under Amendment 11's stored-target
    // rule (never ahead of an unapplied batch), and the relocation term below
    // guarantees every live value in a reclaimed file was copied first. Reads
    // hold their cut latches (I6), which clamps the frontier they need.
    let (starts, durable_end, retained) = {
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let mut starts = BTreeMap::new();
        let mut durable_end = BTreeMap::new();
        let retained = state.retained_epochs.clone();
        for (node, generations) in state.retained_epochs.iter().enumerate() {
            let partition = core.config.endpoints[node].partition;
            for &generation in generations {
                let key = (partition, generation);
                // A stream without a position has no proven end: nothing of it
                // can be proven dead, so it is skipped, never read as empty.
                let Some(position) = state.positions.get(&key) else {
                    continue;
                };
                // No adopted start means no relocation pass reached this
                // stream yet: no byte of it can be proven dead, with or
                // without a filter. An unfiltered shape still copies every
                // record it reclaims, so the term is never vacuous.
                let callback_from = state.callback_starts.get(&key).copied().unwrap_or(0);
                starts.insert(key, callback_from);
                durable_end.insert(key, position.durable_end);
            }
        }
        (starts, durable_end, retained)
    };
    // Per stream, never one global term: independent owner offset spaces.
    //
    // Read order matters: `starts` above came first, the stored rows come now.
    // A store between the two reads can only add rows (the retained set changes
    // at open only), so the subset check below cannot trip on that race.
    let mut fronts: BTreeMap<(u32, u64), u64> = BTreeMap::new();
    for front in &core.table.stored_fronts() {
        fronts.insert((front.partition, front.generation), front.replay_from);
    }
    // Native's rule: `min(adopted callback start, stored replay_from)` per
    // stream, with no latch term. A held checkpoint therefore never keeps a
    // file, and a read through it whose frame a pass reclaimed answers absent,
    // as native's unreachable-position read does.
    let mut watermark = BTreeMap::new();
    for (&key, &callback_from) in &starts {
        // A stream the durable control region never covered has no replay
        // floor yet: no byte of it can be proven dead.
        let Some(&replay_from) = fronts.get(&key) else {
            continue;
        };
        watermark.insert(
            key,
            StreamWatermark {
                end: callback_from.min(replay_from),
                replay_from,
                callback_from,
            },
        );
    }
    let persisted_cut_next = match core.durable_cut.get() {
        None => None,
        Some(cut) => {
            let stats = cut.stats();
            // Disabled, or owned by another generation: no B term.
            (stats.enabled && stats.epoch == core.epoch).then_some(stats.persisted_next)
        }
    };
    // A start without a control-region row is legitimate until THIS process
    // stores a region: a generation that wrote and closed before any store has
    // a start seeded at open and no row, even when an older region was loaded
    // (the first snapshot is cadence-driven, 8 GiB of values); and the serving
    // generation can publish before its first store. The loop above skips such
    // a stream (no byte can be proven dead without a replay floor).
    //
    // From the first store of this process on, every start stream has a row:
    // the store names the serving generation and one row per retained older
    // generation of the manifest (`HybridTable::snapshot_with`). The serving
    // generation is new at every open, so a loaded region can never name it:
    // its row is the proof that a store happened since this open. A start
    // without a row after that is a broken invariant. Release builds keep the
    // skip above (nothing retires without a floor) and count it.
    let stored_by_this_process = fronts
        .keys()
        .any(|&(_, generation)| generation == core.epoch);
    if stored_by_this_process {
        let missing: Vec<(u32, u64)> = starts
            .keys()
            .filter(|key| !fronts.contains_key(key))
            .copied()
            .collect();
        if !missing.is_empty() {
            // Relaxed: a diagnostic count; nothing is ordered against it.
            core.retirement_missing_rows.fetch_add(
                u64::try_from(missing.len()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        debug_assert!(
            missing.is_empty(),
            "every seeded start stream (serving or retained older) must have a row in the \
             region this process stored; missing (partition, generation): {missing:?}"
        );
    }
    Snapshot {
        watermark,
        durable_end,
        persisted_cut_next,
        retained,
    }
}

/// Serving stream: retire durably sealed whole files at or below the
/// watermark, in owner page order, with the watermark as the witness.
fn retire_serving_stream(
    core: &WriteCore,
    client: &mut Client,
    node: usize,
    partition: u32,
    watermark: StreamWatermark,
    durable_end: u64,
    persisted_cut_next: Option<u64>,
) -> Result<(), LocalDbError> {
    // An owner whose allocator, lease or root moved during paging yields no
    // trustworthy proof: defer it to the next pass.
    let Some(rows) = page_serving_stream(client, core.epoch)? else {
        return Ok(());
    };
    let file_bytes = core.config.partition_config(1, partition).wal_file_bytes;
    let proofs = victims(core, &rows, watermark, durable_end, persisted_cut_next)?;
    let witness = RetirementWitness {
        replay_from: watermark.replay_from,
        callback_from: watermark.callback_from,
        persisted_cut_next,
    };
    for chunk in proofs.chunks(MAX_PROOFS_PER_REQUEST) {
        let files: Vec<u64> = chunk.iter().map(|proof| proof.file_start).collect();
        if !core.reads.evict_retirement_grants(node, core.epoch, &files) {
            core.retirement_busy_replies.fetch_add(1, Ordering::Relaxed);
            break;
        }
        let claim = core
            .r1_events
            .claimed(core, partition, file_bytes, chunk, &witness);
        let outcome = client
            .retire_sealed_files(core.epoch, chunk.to_vec(), witness)
            .map_err(|error| {
                LocalDbError::new(format!(
                    "retirement request to partition {partition} failed: {error}"
                ))
            })?;
        if let Some((attempt, after)) = claim {
            let files: Vec<u64> = chunk.iter().map(|proof| proof.file_start).collect();
            core.r1_events
                .replied(partition, core.epoch, attempt, &after, &files, outcome);
        }
        // `Busy`: no durable decision; retry on the next pass, never in a loop.
        if outcome == RetirementResult::Busy {
            // Relaxed: a diagnostic count; nothing is ordered against it.
            core.retirement_busy_replies.fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
    Ok(())
}

/// Older retained generation: push the same watermark as a bare proof. The
/// owner's sealed-stream path rechecks its own pins and read admission.
fn retire_sealed_stream(
    core: &WriteCore,
    client: &mut Client,
    node: usize,
    partition: u32,
    generation: u64,
    watermark: StreamWatermark,
    stream_end: u64,
) -> Result<(), LocalDbError> {
    let mut after = None;
    loop {
        let files = client
            .retirement_files(core.epoch, generation, after, MAX_FILES_PER_PAGE)
            .map_err(|error| {
                LocalDbError::new(format!(
                    "retirement list of partition {partition} generation {generation} failed: {error}"
                ))
            })?;
        if files.is_empty() {
            return Ok(());
        }
        after = files.last().copied();
        // A sealed stream can never append, so its last file is dead once the
        // watermark covers the whole stream: bytes after the stream end are the
        // seal scan's zero or skip suffix. Without that full coverage a file is
        // only dead when its *end* is at or below the watermark, exactly like
        // the serving stream: a mid-file target leaves live bytes above it.
        let size = core.config.partition_config(1, partition).wal_file_bytes;
        let dead: Vec<u64> = files
            .into_iter()
            .filter(|&start| sealed_file_is_dead(start, size, watermark.end, stream_end))
            .collect();
        if dead.is_empty() {
            return Ok(());
        }
        if !core.reads.evict_retirement_grants(node, generation, &dead) {
            core.retirement_busy_replies.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if client
            .retire_files_if_idle(core.epoch, generation, dead)
            .map_err(|error| {
                LocalDbError::new(format!(
                    "sealed retirement of partition {partition} generation {generation} failed: {error}"
                ))
            })?
            == RetirementResult::Busy
        {
            // Relaxed: a diagnostic count; nothing is ordered against it.
            core.retirement_busy_replies.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
    }
}

/// Whether the file starting at `start` of a sealed stream ending at
/// `stream_end` is dead under a watermark ending at `watermark_end`. Full
/// coverage (`watermark_end >= stream_end`) kills every file that starts below
/// the watermark, including the last one whose tail past the stream end is the
/// seal scan's zero or skip suffix; otherwise the file's own end must be at or
/// below the watermark.
fn sealed_file_is_dead(start: u64, size: u64, watermark_end: u64, stream_end: u64) -> bool {
    if watermark_end >= stream_end {
        start < watermark_end
    } else {
        start
            .checked_add(size)
            .is_some_and(|end| end <= watermark_end)
    }
}

/// Page the serving stream to its end. Rows are ordered by `file_start`;
/// `None` when the owner identity or allocator moved during pagination.
fn page_serving_stream(
    client: &mut Client,
    authority: u64,
) -> Result<Option<Vec<WalFileObservation>>, LocalDbError> {
    let mut after = None;
    let mut rows = Vec::new();
    let mut header: Option<FileInventoryReply> = None;
    for _ in 0..MAX_PAGES_PER_STREAM {
        let mut reply = client
            .file_inventory_page(authority, authority, after, MAX_FILE_INVENTORY_PAGE)
            .map_err(|error| {
                LocalDbError::new(format!("retirement inventory page failed: {error}"))
            })?;
        if let Some(first) = &header
            && (first.owner_incarnation != reply.owner_incarnation
                || first.owner_root_generation != reply.owner_root_generation)
        {
            return Ok(None);
        }
        if !reply.stable {
            return Ok(None);
        }
        let full = reply.rows.len() == usize::from(MAX_FILE_INVENTORY_PAGE);
        after = reply.rows.last().map(|row| row.file_start);
        rows.append(&mut reply.rows);
        header = Some(reply);
        if !full {
            return Ok(Some(rows));
        }
    }
    Err(LocalDbError::new(
        "retirement owner inventory exceeds its page bound",
    ))
}

/// Proofs for the durably sealed files of one owner at or below the watermark.
fn victims(
    core: &WriteCore,
    rows: &[WalFileObservation],
    watermark: StreamWatermark,
    processed_end: u64,
    persisted_cut_next: Option<u64>,
) -> Result<Vec<RetireProof>, LocalDbError> {
    let size = core.config.storage.wal_file_bytes;
    let used = rows
        .iter()
        .filter(|row| matches!(row.state, FileUseState::Used | FileUseState::Leased))
        .map(|row| row.file_start)
        .min();
    let budget_end = used.and_then(|lo| {
        span_budget_end(
            &[OwnerSpan {
                generation: core.epoch,
                lo,
                hi: processed_end,
            }],
            size,
            core.config.index.relocation_max_reclaim_pct,
        )
        .get(&core.epoch)
        .copied()
    });
    let mut proofs = Vec::new();
    for row in rows {
        let file_end = row
            .file_start
            .checked_add(size)
            .ok_or_else(|| LocalDbError::new("retirement file bound overflow"))?;
        // Only an exact durable seal of the observed inode is a candidate.
        let Some(seal) = row.seal.filter(|seal| {
            seal.start == row.file_start
                && seal.end == file_end
                && seal.device == row.device
                && seal.inode == row.inode
        }) else {
            continue;
        };
        if file_end > watermark.end {
            continue;
        }
        if seal
            .last_batch_sequence
            .is_some_and(|last| persisted_cut_next.is_some_and(|next| last >= next))
        {
            continue;
        }
        if budget_end.is_none_or(|end| file_end > end) {
            continue;
        }
        proofs.push(RetireProof {
            file_start: row.file_start,
            inode: Some(WalInode {
                device: row.device,
                inode: row.inode,
            }),
            seal_generation: Some(seal.root_generation),
        });
    }
    Ok(proofs)
}

#[cfg(test)]
mod tests;
