//! Receipt-history authority root of one per-cell generation (spec §7.1,
//! §7.3 step 2, §9.2 step 2): the through-`F` receipt prefixes appended to
//! the durable exact-retry history, and each retained stream's fenced owner
//! proof of that prefix. The format-4 wrapper records the prefixes (the
//! receipt suffix cursors of the next recovery) and the history root.
//!
//! # Crash consistency
//!
//! The appended pages and stream slots are synced before the wrapper or the
//! manifest can reference them, and nothing references them until the
//! manifest adopts the wrapper: a crash before adoption leaves them
//! unreachable from the adopted history root, as in the exact protocol. The
//! live publisher keeps the previous root until the adopted generation is
//! applied; its identities are retired only after the new root is installed.
//!
//! # Locks
//!
//! Runs on the checkpoint actor with no serving guard: receipts are captured
//! under writer -> publisher by the caller, and the history clone, file I/O,
//! syncs and owner controls run here without any lock.

use super::super::write_core::core::{CheckpointJobs, WriteCore};
use super::super::*;
use crate::distributed::native_index::PrefixCut;

/// Durable receipt-history state of one generation, ready to commit.
pub(in crate::distributed::remote_db) struct ReceiptCommit {
    /// The history with the through-`F` prefixes appended and synced. Its
    /// root goes into the wrapper; the publisher installs it at apply,
    /// before any identity retires.
    pub history: ReceiptHistory,
    /// Per retained `(partition, owner epoch)` stream: the fenced owner proof
    /// of the through-`F` receipt prefix and its chain state.
    pub prefixes: BTreeMap<(u32, u64), CheckpointStream>,
}

/// Receipts of `known` at or below `floor`, the newly adopted `F`. Call
/// under writer -> publisher with the publisher's `known` (every entry is
/// above the previously adopted `F`), so the set is the exact `(old F, F]`
/// prefix the retirement token counts.
pub(in crate::distributed::remote_db) fn through_floor_receipts(
    known: &HashMap<BatchId, AppendedBatch>,
    floor: PrefixCut,
) -> Vec<CheckpointReceipt> {
    let mut receipts: Vec<CheckpointReceipt> = known
        .values()
        .filter(|receipt| floor.contains(receipt.version()))
        .map(|receipt| (*receipt).into())
        .collect();
    receipts.sort_unstable_by_key(|receipt| (receipt.address.partition, receipt.version));
    receipts
}

/// Append `receipts` (from [`through_floor_receipts`]) to `history`, a
/// clone of the live history at the previously adopted `F`, sync it, then
/// obtain every retained stream's fenced owner proof of its through-`F`
/// prefix (`jobs` from `WriteCore::checkpoint_jobs`). Owner controls run in
/// parallel per owner, as in the exact checkpoint.
pub(in crate::distributed::remote_db) fn commit_receipt_history(
    core: &WriteCore,
    mut history: ReceiptHistory,
    receipts: Vec<CheckpointReceipt>,
    mut jobs: CheckpointJobs,
) -> Result<ReceiptCommit, LocalDbError> {
    history.reset_io_stats();
    let mut start = 0;
    while start < receipts.len() {
        let stream = (
            receipts[start].address.partition,
            receipts[start].version.epoch,
        );
        let end = receipts[start..]
            .iter()
            .position(|receipt| (receipt.address.partition, receipt.version.epoch) != stream)
            .map_or(receipts.len(), |length| start + length);
        // One stream at a time, in version order: the history's ordinal
        // slots and chain states extend the previous prefix contiguously.
        history.append_stream(&receipts[start..end])?;
        start = end;
    }
    history.sync()?;
    let database = core.config.storage.database;
    let authority_epoch = core.epoch;
    let synced = &history;
    let proved = endpoint_map(&mut jobs, |_, (sender, partition, epochs)| {
        let mut output = Vec::with_capacity(epochs.len());
        for &epoch in epochs.iter() {
            let count = synced
                .counts
                .get(&(*partition, epoch))
                .copied()
                .unwrap_or(0);
            let (reply, pending) = mpsc::sync_channel(1);
            sender.send(Command::Checkpoint(authority_epoch, epoch, count, reply))?;
            let proof = receive(pending)?;
            // Later durable appends do not change this exact prefix.
            if proof.database != database
                || proof.partition != *partition
                || proof.epoch != epoch
                || proof.prefix != count
                || proof.retained < count
                || proof.digest != synced.prefix(*partition, epoch, count)?
            {
                return Err(LocalDbError::new(
                    "checkpoint prefix differs from published receipts",
                ));
            }
            output.push((
                (*partition, epoch),
                CheckpointStream {
                    state: synced.state(*partition, epoch)?,
                    proof,
                },
            ));
        }
        Ok(output)
    })?;
    let prefixes = proved.into_iter().flatten().collect();
    Ok(ReceiptCommit { history, prefixes })
}

/// Receipt-history proof through `U` (spec §7.3 step 1): owners do not serve
/// WAL position proofs in this mode, so the frame-covered receipts in
/// `(F, U]` are proved by an owner receipt prefix longer than the history.
/// `from_count`/`from_state` are the stream's through-`F` prefix (the
/// history's count and chain state after [`commit_receipt_history`]);
/// `extension` holds the stream's receipts above `F` through `U`, in
/// version order, so the owner prefix is `from_count + extension.len()`.
/// Verify `proof` (from `client.checkpoint(authority, epoch, that count)`)
/// by extending the chain from `from_state`; a forged, reordered, missing or
/// extra receipt changes the digest. Returns the extended chain state.
pub(in crate::distributed::remote_db) fn verify_prefix_extension(
    database: [u8; 16],
    partition: u32,
    epoch: u64,
    from_count: usize,
    from_state: [u8; 32],
    extension: &[CheckpointReceipt],
    proof: &CheckpointProof,
) -> Result<[u8; 32], LocalDbError> {
    let count = from_count
        .checked_add(extension.len())
        .ok_or_else(|| LocalDbError::new("receipt prefix count overflow"))?;
    if proof.database != database
        || proof.partition != partition
        || proof.epoch != epoch
        || proof.prefix != count
        || proof.retained < count
        || extension
            .windows(2)
            .any(|pair| pair[0].version >= pair[1].version)
        || extension.iter().any(|receipt| {
            receipt.id.database != database
                || receipt.address.partition != partition
                || receipt.version.epoch != epoch
        })
    {
        return Err(LocalDbError::new(
            "receipt prefix extension differs from published receipts",
        ));
    }
    let mut digest =
        crate::distributed::tcp::checkpoint::PrefixDigest::from_state(from_count, from_state);
    for receipt in extension {
        digest.push(*receipt);
    }
    let state = digest.state();
    if digest.finish() != proof.digest {
        return Err(LocalDbError::new(
            "receipt prefix extension digest differs from the owner proof",
        ));
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::tcp::checkpoint::{PrefixDigest, prefix_digest};
    use crate::distributed::types::{PayloadDigest, PhysicalValueAddress};

    fn receipt(sequence: u64) -> CheckpointReceipt {
        CheckpointReceipt {
            id: BatchId {
                database: [3; 16],
                operation: [sequence as u8; 16],
            },
            version: BatchVersion { epoch: 2, sequence },
            digest: PayloadDigest([sequence as u8; 32]),
            address: PhysicalValueAddress {
                partition: 1,
                segment_generation: 2,
                offset: sequence * 4096,
                len: 128,
            },
        }
    }

    fn proof(prefix: usize, digest: [u8; 32]) -> CheckpointProof {
        CheckpointProof {
            database: [3; 16],
            partition: 1,
            epoch: 2,
            prefix,
            retained: prefix + 1,
            digest,
        }
    }

    #[test]
    fn prefix_extension_matches_owner_digest_and_rejects_forgeries() {
        let all: Vec<_> = (0..5).map(receipt).collect();
        // Through-F prefix: 2 receipts in the history; (F, U]: 3 more.
        let mut through_floor = PrefixDigest::new([3; 16], 1, 2, 0);
        for receipt in &all[..2] {
            through_floor.push(*receipt);
        }
        let from_state = through_floor.state();
        let owner = prefix_digest([3; 16], 1, 2, all.iter().copied());
        let state =
            verify_prefix_extension([3; 16], 1, 2, 2, from_state, &all[2..], &proof(5, owner))
                .unwrap();
        let mut full = PrefixDigest::new([3; 16], 1, 2, 0);
        for receipt in &all {
            full.push(*receipt);
        }
        assert_eq!(state, full.state());
        // Empty extension proves exactly the through-F prefix.
        let at_floor = prefix_digest([3; 16], 1, 2, all[..2].iter().copied());
        verify_prefix_extension([3; 16], 1, 2, 2, from_state, &[], &proof(2, at_floor)).unwrap();
        // Forged member, reordered, missing, extra, wrong count, foreign
        // stream or a retained end below the prefix: all rejected.
        let mut forged = all[2..].to_vec();
        forged[1].digest = PayloadDigest([0xEE; 32]);
        let mut reordered = all[2..].to_vec();
        reordered.swap(0, 1);
        let mut foreign = all[2..].to_vec();
        foreign[0].address.partition = 9;
        let short_retained = CheckpointProof {
            retained: 4,
            ..proof(5, owner)
        };
        for (extension, proof) in [
            (forged, proof(5, owner)),
            (reordered, proof(5, owner)),
            (all[2..4].to_vec(), proof(4, owner)),
            (all[2..4].to_vec(), proof(5, owner)),
            (all[1..].to_vec(), proof(6, owner)),
            (foreign, proof(5, owner)),
            (all[2..].to_vec(), short_retained),
        ] {
            assert!(
                verify_prefix_extension([3; 16], 1, 2, 2, from_state, &extension, &proof).is_err()
            );
        }
    }
}
