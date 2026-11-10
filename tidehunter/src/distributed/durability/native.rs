//! Native tier: the acknowledgement contract of native Tidehunter.
//!
//! Native Tidehunter (paper §3.1/§5) completes a write after the page-cache (mmap WAL)
//! copy and the index update, without waiting for fsync. The tracker advances the highest
//! WAL offset below which all entries are processed; a background syncer batches fsyncs;
//! durability against an application crash is immediate, against a kernel crash only after
//! the fsync; applications that need more call an explicit flush.
//!
//! Distributed meaning:
//!
//! * **Acknowledgement condition:** the owner's *appended* frontier covers the batch. The
//!   batch is in the owner's page cache and every earlier position is too. No fsync wait.
//! * **Sync cadence:** as in native Tidehunter, the shared syncer runs when a WAL fragment
//!   is finalized ([`SyncPolicy::fragment_bytes`]), and at once when anything waits for a
//!   sync. There is no age bound by default; [`SyncPolicy::max_unsynced_age_ms`] is an
//!   opt-in extra trigger, stronger than native. Both are sync triggers, not hard lag bounds: a
//!   stalled fsync cannot be interrupted and nothing applies backpressure to writers.
//! * **Explicit flush:** [`flush`] waits until the owner's synced frontier covers
//!   everything appended when the flush began. [`super::frontier::flush_all`] does this for
//!   every owner in parallel.
//! * **Not covered:** a Native acknowledgement is not kernel-crash durable. The coordinator
//!   checkpoint still references only synced batches (the recovery invariant).

use super::ReceiptDurability;
use super::frontier::{FlushTarget, Frontier, FrontierError, SyncPolicy};
use std::time::Instant;

/// The Native default cadence for an owner WAL with fragments of `fragment_bytes`.
#[must_use]
pub const fn sync_policy(fragment_bytes: u64) -> SyncPolicy {
    SyncPolicy::native(fragment_bytes)
}

/// Callers that already published `end` through [`Frontier::publish_appended`] reply
/// immediately; this is then a lock-free check.
#[must_use]
pub const fn ack_after_append() -> bool {
    true
}

/// Block until the appended frontier covers `end`, then report an `Appended` receipt.
/// Never waits for an fsync.
pub fn acknowledge(
    frontier: &Frontier,
    end: u64,
    deadline: Option<Instant>,
) -> Result<ReceiptDurability, FrontierError> {
    frontier.wait_appended(end, deadline)?;
    Ok(ReceiptDurability::Appended)
}

/// Explicit flush of one owner: wait until the synced frontier covers everything that was
/// appended when the call began. Returns the synced frontier.
pub fn flush(frontier: &Frontier, deadline: Option<Instant>) -> Result<u64, FrontierError> {
    let ticket = frontier.begin_flush()?;
    frontier.finish_flush(ticket, deadline)
}
