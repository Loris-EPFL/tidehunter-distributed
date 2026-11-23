//! Deadlines and owner-side stall diagnosis of the recovery replay page.
//!
//! A `ReplayLiveWal` page is an exclusive control on the owner: an unlocked
//! sealed page starts without waiting for active stream work, then takes the
//! flush barrier of the control worker and runs its handler (page read and
//! live-lease publication). The coordinator's
//! control deadline is stamped before the owner's, so a page that waits too
//! long always ends as a bare coordinator expiry. This module keeps the owner
//! stages of each accepted page, so a peer that abandons one makes the owner
//! name where the page stopped: not passed on by the session, admitted but
//! unanswered, reply ready but not queued, or reply queued and not delivered.
//!
//! The table holds one entry per accepted page (at most the session's request
//! window) and is cleared on the page's completion or with its session.

use super::*;

/// Owner progress of one accepted replay-page control.
struct ReplayStall {
    received: Instant,
    dispatched: Option<Instant>,
}

/// Accepted replay pages of one owner session, by envelope ID.
pub(super) struct ReplayStages(HashMap<u64, ReplayStall>);

impl ReplayStages {
    pub(super) fn new() -> Self {
        Self(HashMap::new())
    }

    /// The session accepted a replay page from the wire.
    pub(super) fn accept(&mut self, id: u64) {
        self.0.insert(
            id,
            ReplayStall {
                received: Instant::now(),
                dispatched: None,
            },
        );
    }

    /// The session passed the page to the control owner.
    pub(super) fn dispatch(&mut self, id: u64) {
        if let Some(stall) = self.0.get_mut(&id) {
            stall.dispatched = Some(Instant::now());
        }
    }

    /// The page's reply left the owner; the entry is no longer needed.
    pub(super) fn complete(&mut self, id: u64) {
        // Called for every retired reply; keep the empty shared case free of a
        // hash.
        if self.0.is_empty() {
            return;
        }
        self.0.remove(&id);
    }

    /// Where page `id` stopped, or an empty string for another request kind.
    /// `request` is its session entry: the reply stages live there.
    pub(super) fn describe(&self, id: u64, request: &ServerPending, now: Instant) -> String {
        let Some(stall) = self.0.get(&id) else {
            return String::new();
        };
        let waited = now.saturating_duration_since(stall.received);
        if request.response_queued {
            format!("; its reply was queued and is still in transit {waited:?} after receipt")
        } else if request.ready_response.is_some() {
            format!("; its reply was ready {waited:?} after receipt and was not queued")
        } else if let Some(admitted) = stall.dispatched {
            format!(
                "; the control owner admitted it {:?} after receipt and it produced no reply for \
                 {:?} (waiting for an exclusive control, the flush barrier, or its handler)",
                admitted.saturating_duration_since(stall.received),
                now.saturating_duration_since(admitted)
            )
        } else {
            format!(
                "; the session did not pass it to the control owner within {waited:?} of receipt"
            )
        }
    }

    /// Detail of an abandoned page for a closed peer: its request and kind and
    /// where it stopped.
    pub(super) fn closed_session_note(&self, pending: &HashMap<u64, ServerPending>) -> String {
        let now = Instant::now();
        let Some((id, request)) = pending.iter().find(|(id, _)| self.0.contains_key(id)) else {
            return String::new();
        };
        format!(
            ": request {id} (kind {}){}",
            request.kind,
            self.describe(*id, request, now)
        )
    }
}

/// Test-only note of the owner's own queue: more than one accepted control
/// means the page waits behind another control, one means the control owner
/// already owns it (flush barrier or handler). Empty outside a test.
pub(super) fn test_queue_note(node: &StorageNode) -> String {
    #[cfg(test)]
    {
        format!(
            "; test owner queue: {} accepted, stream work ahead of the front control: {}",
            node.control.pending_requests_for_test(),
            node.control.exclusive_waits_for_stream()
        )
    }
    #[cfg(not(test))]
    {
        let _ = node;
        String::new()
    }
}

#[cfg(test)]
#[path = "reactor_replay_page_tests.rs"]
mod tests;
