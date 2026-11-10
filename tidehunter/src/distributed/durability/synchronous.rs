//! Synchronous tier: acknowledge only after the covering fsync.
//!
//! Matches the native `value_wal_barrier` cohort and Valkey `appendfsync always`.
//!
//! * **Acknowledgement condition:** the owner's *synced* frontier covers the batch. The
//!   wait raises the syncer's demand, so the fsync starts at once. Every batch appended
//!   while an fsync runs is covered by the next one: group commit, not one fsync per batch.
//! * **Flush:** equal to the Native flush; after a Synchronous acknowledgement nothing
//!   acknowledged is unsynced.
//! * Same frontier, same syncer, same failure rule as Native. A failed fsync fails every
//!   waiter; no receipt is returned for data that may be lost.

use super::ReceiptDurability;
use super::frontier::{Frontier, FrontierError};
use std::time::Instant;

/// The caller waits for the sync after its append; the owner must not reply at append time.
#[must_use]
pub const fn ack_after_append() -> bool {
    false
}

/// Block until the synced frontier covers `end`, then report a `Synced` receipt.
pub fn acknowledge(
    frontier: &Frontier,
    end: u64,
    deadline: Option<Instant>,
) -> Result<ReceiptDurability, FrontierError> {
    frontier.wait_synced(end, deadline)?;
    Ok(ReceiptDurability::Synced)
}
