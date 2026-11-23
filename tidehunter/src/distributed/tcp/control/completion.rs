//! Completion of one admitted control: retire its admission, then publish its
//! reply.
//!
//! Moved from `control.rs` to stay under that file's length baseline; the
//! delivery call is now a sibling module path. The reply is the last effect of
//! an admitted command, so the admission is retired first.

use super::*;

// Keep completion fields inline; a wrapper would only move this argument list.
#[allow(clippy::too_many_arguments)]
pub(super) fn complete(
    shared: &Shared,
    id: u64,
    charge: usize,
    completed: mpsc::SyncSender<reactor::WorkCompletion>,
    response: Result<Response, TcpError>,
    profile: ConnectionProfile,
    measured: bool,
    began: Option<Instant>,
) {
    // Retire the admission before publishing the reply. The reply is the last
    // effect of an admitted command, so a thread that holds it (or is ordered
    // after its receiver, e.g. by a session join) must see the owner quiescent:
    // `finish_live_target_shutdown` refuses teardown while `requests != 0`, and
    // a session retrying a full `try_submit` after this reply must find the
    // slot free. The queue mutex release is sequenced before `try_send`, and the
    // channel orders it before the receiver's next queue lock. Crash
    // consistency is unchanged: every durable effect of the command (the WAL
    // write, and the covering sync for Synchronous) precedes this call; only
    // delivery of the already computed reply follows the release. Releasing
    // first admits at most one extra in-transit completion per worker and the
    // flusher beyond `MAX_COMMANDS`, so admission stays bounded. Parked
    // durable-progress polls add at most the progress hub's waiter bound.
    release(shared, charge);
    super::deliver(id, completed, response, profile, measured, began);
}

/// Retire one admitted command's request and byte credits.
pub(super) fn release(shared: &Shared, charge: usize) {
    let mut queue = shared.queue.lock();
    queue.requests -= 1;
    queue.bytes -= charge;
}
