//! The coordinator's per-stream positions: what the relocation scan, dead-file
//! retirement and generation trim know about each `(owner partition, fencing
//! generation)` stream.
//!
//! Every reader treats a stream **without an entry as unproven**, never as
//! empty: a missing entry skips scan and retirement and blocks trim. So every
//! retained stream gets an entry, either from the owner's report at open
//! ([`seed_positions`]) or from the first publication on it
//! ([`admit_stream`] then [`record_extent`]).
//!
//! The table is bounded by [`MAX_REGION_STREAMS`] at both writers; a stream
//! past the bound is an error, never a dropped entry.

use super::StreamPosition;
use crate::distributed::hybrid::region::MAX_REGION_STREAMS;
use crate::distributed::storage::LocalDbError;
use std::collections::{BTreeMap, HashMap};

/// What an owner reported in the fence sweep about one retained older
/// generation it holds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct OwnerTail {
    pub partition: u32,
    pub generation: u64,
    /// End of the highest durable frame of the generation's log, as the owner
    /// computed it from that log.
    pub durable_end: u64,
    /// The owner's generation is final: no byte can be appended any more.
    pub complete: bool,
}

/// The stream positions an open starts with: one entry per owner partition and
/// retained older generation.
///
/// `retained` is the manifest's retained generations below the serving one,
/// each with `true` for an aborted reservation. An owner that reports a tail
/// gets exactly that tail; only a final (sealed or abandoned) generation is
/// `complete`, and the relocation scan and trim eligibility treat an incomplete
/// one as blocking. An owner that does not list an *aborted* generation holds no
/// log for it (its complete fenced ledger was swept, so a reservation that
/// never reached the owner has nothing there): that is a recorded empty,
/// complete stream, not an absent entry. Any other generation an owner does not
/// report has no proof of its tail and fails the open.
///
/// The tail is never derived from replay: replay stops at the stored front's
/// suffix, so its end can be below the real tail, which would make the
/// generation look covered sooner and end the relocation scan before live tail
/// members.
///
/// # Errors
/// More streams than [`MAX_REGION_STREAMS`], or a non-aborted retained
/// generation an owner did not report.
pub(super) fn seed_positions(
    partitions: &[u32],
    retained: &[(u64, bool)],
    tails: &[OwnerTail],
) -> Result<BTreeMap<(u32, u64), StreamPosition>, LocalDbError> {
    // One stream per owner for each retained generation plus the serving one,
    // which is what the control region stores.
    if partitions
        .len()
        .saturating_mul(retained.len().saturating_add(1))
        > MAX_REGION_STREAMS
    {
        return Err(LocalDbError::new(format!(
            "{} owners with {} retained generations exceed the {MAX_REGION_STREAMS}-stream bound",
            partitions.len(),
            retained.len()
        )));
    }
    let reported: BTreeMap<(u32, u64), &OwnerTail> = tails
        .iter()
        .map(|tail| ((tail.partition, tail.generation), tail))
        .collect();
    let mut positions = BTreeMap::new();
    for &partition in partitions {
        for &(generation, aborted) in retained {
            let (durable_end, complete) = match reported.get(&(partition, generation)) {
                Some(tail) => (tail.durable_end, tail.complete),
                None if aborted => (0, true),
                None => {
                    return Err(LocalDbError::new(format!(
                        "owner {partition} did not report retained generation {generation}"
                    )));
                }
            };
            positions.insert(
                (partition, generation),
                StreamPosition {
                    partition,
                    epoch: generation,
                    replay_from: 0,
                    last_offset: None,
                    durable_end,
                    complete,
                },
            );
        }
    }
    Ok(positions)
}

/// Checks, before a batch is published, that the stream its address names is
/// one the scan can read: a retained fencing generation of that owner, within
/// the stream bound. Publication keys positions by the address; an address
/// whose segment generation is not a retained fencing generation would create an
/// entry nothing reads, and the stream's file could then be retired unseen.
///
/// # Errors
/// An address naming an unretained stream, or a new stream past the bound.
/// Either is an invariant break; the caller fails the database.
pub(super) fn admit_stream(
    positions: &BTreeMap<(u32, u64), StreamPosition>,
    retained_epochs: &[Vec<u64>],
    partition_indexes: &HashMap<u32, usize>,
    partition: u32,
    generation: u64,
) -> Result<(), LocalDbError> {
    let retained = partition_indexes
        .get(&partition)
        .and_then(|node| retained_epochs.get(*node))
        .is_some_and(|generations| generations.contains(&generation));
    if !retained {
        return Err(LocalDbError::new(format!(
            "owner address names partition {partition} segment generation {generation}, which \
             is not a retained fencing generation of that owner"
        )));
    }
    if positions.len() >= MAX_REGION_STREAMS && !positions.contains_key(&(partition, generation)) {
        return Err(LocalDbError::new(format!(
            "stream ({partition}, {generation}) would exceed the {MAX_REGION_STREAMS}-stream bound"
        )));
    }
    Ok(())
}

/// Records one published extent. The caller ran [`admit_stream`] first. A
/// stream's durable end only grows.
pub(super) fn record_extent(
    positions: &mut BTreeMap<(u32, u64), StreamPosition>,
    partition: u32,
    generation: u64,
    offset: u64,
    len: u32,
) {
    let position = positions
        .entry((partition, generation))
        .or_insert(StreamPosition {
            partition,
            epoch: generation,
            replay_from: 0,
            last_offset: None,
            durable_end: 0,
            complete: true,
        });
    position.durable_end = position
        .durable_end
        .max(offset.saturating_add(u64::from(len)));
    position.last_offset = Some(position.last_offset.map_or(offset, |last| last.max(offset)));
    position.complete = true;
}

#[cfg(test)]
mod tests;
