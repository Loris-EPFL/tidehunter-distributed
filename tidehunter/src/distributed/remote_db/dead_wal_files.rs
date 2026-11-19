//! R1 coordinator pass: retire exact-dead, durably sealed owner WAL files of
//! the serving fencing generation (CAPACITY-R0-R1.md §4), without an
//! authority restart and without any foreground wait.
//!
//! One pass, under the cold `WriteCore::retirement` gate (the caller holds
//! it; foreground writes, reads and checkpoints never take it):
//! 1. Skip databases whose shape has a relocation filter. Until the §5
//!    callback cursor (`CallbackStart`) lands, WAL history in those databases
//!    is a callback obligation with no adopted bound, so only the older
//!    sealed-generation path runs there (today's behavior).
//! 2. Between two equal even values of the R0 install seqlock, copy the
//!    adopted Exact identity, the adopted stream positions and the installed
//!    cut, and walk every retained reference of the installed image plus the
//!    volatile overlay and captured layers. The walk decodes immutable cell
//!    blocks off-lock (shard read locks only, per shard).
//! 3. Page the serving stream of each owner over a dedicated authenticated
//!    session, never the owner worker's control lane (a worker control
//!    settles the open leased extent). Evaluate each durably sealed file
//!    with the R0 blocker table plus the Exact coverage bound.
//! 4. Send inode- and seal-bound proofs with the Exact witness. The owner
//!    rechecks every bound it can compare, drains and disables the exact
//!    namespace, and answers `Busy` without any durable decision when a pin,
//!    an unprepared successor or the pending bound stands in the way.
//!
//! In-flight reads need no extra file guard here: every remote read flight
//! owns its `IndexReadPin` until its terminal completion (reactor lanes
//! complete a request only when all of its flights returned their CQE;
//! inline reads return only after the terminal CQE or abort the process), and
//! a registered pin keeps the retention floor at or below its cut, so every
//! reference such a read can still use stays in the installed image and
//! blocks its file in step 2.
//!
//! Nothing here is fatal for the authority: a failed or refused request made
//! no coordinator state change, and the next pass re-pages the owner.

use super::write_core::core::WriteCore;
use super::{Client, LocalDbError};
use crate::distributed::capacity::{
    FileInputs, FileInventoryReply, MAX_FILE_INVENTORY_PAGE, OwnerSpan, PositionFacts, StreamFacts,
    WalFileObservation, evaluate, span_budget_end,
};
use crate::distributed::storage::{RetireProof, RetirementWitness, WalInode};
use crate::distributed::tcp::checkpoint::RetirementResult;
use crate::distributed::types::{RecordKind, ValueRecordReference};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

/// Owner inventory pages per pass and owner; a stream has far fewer files.
const MAX_PAGES_PER_STREAM: usize = 1 << 16;
/// Bounded wait (1 ms each) for an in-progress image installation.
const INSTALL_WAIT_TRIES: u32 = 1000;
/// Physical reclaim span per pass, in percent of the owner's span. Sui sets
/// native `relocation_max_reclaim_pct` to 100; the coordinator option that
/// carries Sui's value is Phase 3.3 work, so the pass uses Sui's value.
const RECLAIM_PCT: u8 = 100;
/// Proofs per request; equal to the owner's bounded pending set.
const MAX_PROOFS_PER_REQUEST: usize = 256;

/// Publisher state copied once, inside the stable seqlock window.
struct Snapshot {
    checkpoint_generation: u64,
    metadata_digest: [u8; 32],
    obsolete_root: bool,
    positions: BTreeMap<u32, PositionFacts>,
}

/// Retire every exact-dead sealed file of the serving generation. The caller
/// holds `core.retirement`; no other coordinator lock is held across a walk
/// step or an RPC. Every early `Ok(())` below is a deferral that made no
/// decision; the next pass (after a later root, roll or unpin) retries.
pub(in crate::distributed::remote_db) fn core_retire_dead_wal_files(
    core: &WriteCore,
) -> Result<(), LocalDbError> {
    // Decision 1 (until §5 lands): any relocation filter keeps today's
    // behavior. Destroyed keyspaces count too: their filter is not called,
    // but the conservative skip never deletes a file a decision might need.
    if core
        .shape
        .iter_ks()
        .any(|keyspace| keyspace.relocation_filter().is_some())
    {
        return Ok(());
    }
    // An installation that keeps running defers the pass.
    let Some(seq) = stable_install_seq(core) else {
        return Ok(());
    };
    // No image installed by this process yet: the covered cut is unknown.
    let Some(installed) = core.capacity.installed_cut() else {
        return Ok(());
    };
    // Every batch strictly below this sequence is covered by the installed
    // image. The cut's own batch is excluded: a file ending in it waits for
    // the next checkpoint rather than relying on member-level coverage.
    let covered_next = match installed {
        Some(cut) if cut.batch.epoch == core.epoch => cut.batch.sequence,
        Some(cut) if cut.batch.epoch > core.epoch => {
            return Err(LocalDbError::new(
                "installed Exact cut names a future fencing generation",
            ));
        }
        _ => 0,
    };
    // No root adopted by this generation, or a previous root still
    // selectable until its cleanup completes.
    let Some(snapshot) = take_snapshot(core).filter(|snapshot| !snapshot.obsolete_root) else {
        return Ok(());
    };
    let persisted_cut_next = match core.durable_cut.get() {
        None => None,
        Some(cut) => {
            let stats = cut.stats();
            // The durable cut tracks another generation or is disabled.
            if !stats.enabled || stats.epoch != core.epoch {
                return Ok(());
            }
            Some(stats.persisted_next)
        }
    };
    let referenced = referenced_files(core).map_err(|error| {
        LocalDbError::new(format!(
            "R1 retirement walk of the installed image failed: {error}"
        ))
    })?;
    // An image installed during the walk: its result is discarded.
    if core.capacity.install_seq() != seq {
        return Ok(());
    }
    let tls_config = core
        .config
        .tls
        .client_config_with_identity()
        .map_err(LocalDbError::component)?;
    for endpoint in &core.config.endpoints {
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
                "R1 retirement session to partition {} failed: {error}",
                endpoint.partition
            ))
        })?;
        let (header, rows, stable) = page_serving_stream(&mut client, core.epoch)?;
        // The owner's allocator, leases or root moved during paging.
        if !stable {
            continue;
        }
        let facts = StreamFacts {
            same_generation: true,
            complete_stream: false,
            position: snapshot.positions.get(&endpoint.partition).copied(),
            owner_root_scan_from: header.wal_scan_from,
            owner_root_synced_end: header.wal_synced_end,
            processed_end: header.processed_end,
            page_stable: stable,
            persisted_cut_next,
        };
        let proofs = victims(
            core,
            endpoint.partition,
            &rows,
            &facts,
            &referenced,
            covered_next,
        )?;
        let Some(position) = facts.position else {
            continue;
        };
        let witness = RetirementWitness {
            checkpoint_generation: snapshot.checkpoint_generation,
            metadata_digest: snapshot.metadata_digest,
            replay_from: position.replay_from,
            durable_end: position.durable_end,
            // Decision 1: no keyspace has a relocation filter, so no WAL
            // callback can ever scan this history again.
            callback_from: u64::MAX,
            covered_next,
            persisted_cut_next,
        };
        for chunk in proofs.chunks(MAX_PROOFS_PER_REQUEST) {
            // Revalidate the walked image identity at submission.
            if core.capacity.install_seq() != seq {
                return Ok(());
            }
            let outcome = client
                .retire_sealed_files(core.epoch, chunk.to_vec(), witness)
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "R1 retirement request to partition {} failed: {error}",
                        endpoint.partition
                    ))
                })?;
            // `Busy`: no durable decision; retry after a later root, roll or
            // unpin (the next pass), never in a loop here.
            if outcome == RetirementResult::Busy {
                break;
            }
        }
    }
    Ok(())
}

/// The current seqlock value once it is even, or `None` if an install keeps
/// running for the whole bounded wait.
fn stable_install_seq(core: &WriteCore) -> Option<u64> {
    for _ in 0..INSTALL_WAIT_TRIES {
        let seq = core.capacity.install_seq();
        if seq.is_multiple_of(2) {
            return Some(seq);
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    None
}

fn take_snapshot(core: &WriteCore) -> Option<Snapshot> {
    let mut publisher = core.publisher.lock();
    let state = publisher.checkpoint_state();
    let commit = state
        .manifest
        .checkpoint
        .as_ref()
        .filter(|commit| commit.epoch == core.epoch)?;
    Some(Snapshot {
        checkpoint_generation: commit.generation,
        metadata_digest: commit.metadata_digest,
        // A previous root stays selectable until its cleanup completes; its
        // references are not walked. A staged, unadopted image is not
        // selectable, and it can only hold references published at or
        // before its capture, which the walk already covers.
        obsolete_root: state.manifest.checkpoint_obsolete.is_some(),
        positions: state
            .positions
            .iter()
            .filter(|((_, epoch), _)| *epoch == core.epoch)
            .map(|((partition, _), position)| {
                (
                    *partition,
                    PositionFacts {
                        replay_from: position.replay_from,
                        last_offset: position.last_offset,
                        durable_end: position.durable_end,
                    },
                )
            })
            .collect(),
    })
}

/// `(partition, file_start)` of every serving-generation file that a retained
/// put of the installed image, the live overlay or a captured layer names.
fn referenced_files(
    core: &WriteCore,
) -> Result<BTreeSet<(u32, u64)>, crate::distributed::native_index::NativeIndexError> {
    let size = core.config.storage.wal_file_bytes;
    let epoch = core.epoch;
    let mut files = BTreeSet::new();
    let mut mark = |reference: &ValueRecordReference| {
        let address = reference.batch_address;
        if reference.descriptor.kind != RecordKind::Put || address.segment_generation != epoch {
            return;
        }
        let end = address.offset.saturating_add(u64::from(address.len));
        let mut start = address.offset - address.offset % size;
        while start < end {
            files.insert((address.partition, start));
            let Some(next) = start.checked_add(size) else {
                break;
            };
            start = next;
        }
    };
    core.index.visit_installed_references(|reference| {
        if let Some(reference) = reference {
            mark(reference);
        }
    })?;
    core.index.visit_volatile_references(mark);
    Ok(files)
}

/// Page the serving stream to its end. `stable` is false when the owner
/// reported a change during enumeration or its identity moved between pages.
fn page_serving_stream(
    client: &mut Client,
    authority: u64,
) -> Result<(FileInventoryReply, Vec<WalFileObservation>, bool), LocalDbError> {
    let mut after = None;
    let mut rows = Vec::new();
    let mut header: Option<FileInventoryReply> = None;
    let mut stable = true;
    for _ in 0..MAX_PAGES_PER_STREAM {
        let mut reply = client
            .file_inventory_page(authority, authority, after, MAX_FILE_INVENTORY_PAGE)
            .map_err(|error| {
                LocalDbError::new(format!("R1 retirement inventory page failed: {error}"))
            })?;
        stable &= reply.stable;
        if let Some(first) = &header
            && (first.owner_incarnation != reply.owner_incarnation
                || first.owner_root_generation != reply.owner_root_generation)
        {
            stable = false;
        }
        let full = reply.rows.len() == usize::from(MAX_FILE_INVENTORY_PAGE);
        after = reply.rows.last().map(|row| row.file_start);
        rows.append(&mut reply.rows);
        header = Some(reply);
        if !full {
            let header = header.ok_or_else(|| LocalDbError::new("inventory page missing"))?;
            return Ok((header, rows, stable));
        }
    }
    Err(LocalDbError::new(
        "R1 retirement owner inventory exceeds its page bound",
    ))
}

/// Proofs for the durably sealed files of one owner whose proven blocker set
/// is empty and whose batches the installed image covers. The caller already
/// deferred the pass while a previous root is still selectable.
fn victims(
    core: &WriteCore,
    partition: u32,
    rows: &[WalFileObservation],
    facts: &StreamFacts,
    referenced: &BTreeSet<(u32, u64)>,
    covered_next: u64,
) -> Result<Vec<RetireProof>, LocalDbError> {
    let size = core.config.storage.wal_file_bytes;
    let used = rows
        .iter()
        .filter(|row| {
            matches!(
                row.state,
                crate::distributed::capacity::FileUseState::Used
                    | crate::distributed::capacity::FileUseState::Leased
            )
        })
        .map(|row| row.file_start)
        .min();
    let budget_end = used.and_then(|lo| {
        span_budget_end(
            &[OwnerSpan {
                generation: core.epoch,
                lo,
                hi: facts.processed_end,
            }],
            size,
            RECLAIM_PCT,
        )
        .get(&core.epoch)
        .copied()
    });
    let mut proofs = Vec::new();
    for row in rows {
        let file_end = row
            .file_start
            .checked_add(size)
            .ok_or_else(|| LocalDbError::new("R1 retirement file bound overflow"))?;
        // Only an exact durable seal of the observed inode is a candidate.
        let Some(seal) = row.seal.filter(|seal| {
            seal.start == row.file_start
                && seal.end == file_end
                && seal.device == row.device
                && seal.inode == row.inode
        }) else {
            continue;
        };
        if seal
            .last_batch_sequence
            .is_some_and(|last| last >= covered_next)
        {
            continue;
        }
        let sequence_bound = match seal.last_batch_sequence {
            None => Some(0),
            Some(last) => last.checked_add(1),
        };
        let blockers = evaluate(&FileInputs {
            file_start: row.file_start,
            file_end,
            observation: Some(row),
            stream: facts,
            adopted_root: true,
            obsolete_root_pending: false,
            retained_puts: u64::from(referenced.contains(&(partition, row.file_start))),
            volatile_reference: false,
            sequence_bound,
            within_budget: budget_end.is_some_and(|end| file_end <= end),
            changed: false,
        });
        if blockers.is_empty() {
            proofs.push(RetireProof {
                file_start: row.file_start,
                inode: Some(WalInode {
                    device: row.device,
                    inode: row.inode,
                }),
                seal_generation: Some(seal.root_generation),
            });
        }
    }
    Ok(proofs)
}
