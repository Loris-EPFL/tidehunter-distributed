//! Fencing-generation inventory pages and generation trim (roadmap Phase 5 L1).
//!
//! `NodeStatus` carries only the owner high-water and an incarnation-local
//! inventory revision. The complete retained ledger is read in bounded pages
//! frozen at one `(incarnation, revision)`; a page served after any ledger
//! install fails instead of mixing two ledgers. Trim is a cold lifecycle
//! request: readiness first (`CheckTrimGeneration`), then the fenced deletion
//! (`TrimGeneration`), both on the control owner under the lifecycle gate.
//! TCP and OFI dispatch the same `Request`/`Response` variants.

use super::*;

/// Upper bound of entries in one page, before the negotiated frame bound.
pub(crate) const MAX_INVENTORY_PAGE: usize = 256;
/// Fixed-width bincode bytes per entry (u64 + u8 + u64), rounded up with headroom.
const PAGE_ENTRY_BYTES: usize = 24;
/// Envelope, enum tags, incarnation, revision and cursor fields.
const PAGE_FIXED_BYTES: usize = 128;

/// Owner ledger state of one fencing generation. Explicit discriminants: the
/// numeric values are part of the wire contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OwnerGenerationState {
    Preparing = 0,
    Active = 1,
    Sealed = 2,
    Abandoned = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryGeneration {
    pub generation: u64,
    pub state: OwnerGenerationState,
    /// End of the highest durable frame of this generation's log, computed by
    /// the owner from the log (zero when it holds no log for the generation).
    /// The coordinator seeds a retained older stream's position from it: replay
    /// cannot know a tail it did not read.
    pub durable_end: u64,
}

/// Owner answer to a trim readiness check. `Blocked` defers the candidate; it is
/// never a deletion proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum TrimReadiness {
    /// No ledger entry, directory, pin or target obligation; high-water above it.
    Absent = 0,
    /// The stream never accepted a group (including prewrite-only storage).
    EmptySealed = 1,
    /// Every WAL file was retired and unlinked; no file or proof obligation.
    Retired = 2,
    Blocked = 3,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct InventoryPageRequest {
    pub incarnation: [u8; 16],
    pub revision: u64,
    pub after: Option<u64>,
    pub limit: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct InventoryPageReply {
    pub incarnation: [u8; 16],
    pub revision: u64,
    pub entries: Vec<InventoryGeneration>,
    pub next: Option<u64>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct CheckTrimRequest {
    pub authority_generation: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct CheckTrimReply {
    pub generation: u64,
    pub result: TrimReadiness,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct TrimRequest {
    pub authority_generation: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(super) struct TrimReply {
    pub generation: u64,
}

/// Page entries one frame of `frame_limit` bytes can carry, at most 256.
pub(super) fn page_capacity(frame_limit: usize) -> usize {
    (frame_limit.saturating_sub(PAGE_FIXED_BYTES) / PAGE_ENTRY_BYTES).min(MAX_INVENTORY_PAGE)
}

/// Check one page against the request that produced it. Shared by the client
/// and the tests so that the owner and coordinator agree on the page grammar.
pub(super) fn check_page(
    request: &InventoryPageRequest,
    reply: &InventoryPageReply,
) -> Result<(), &'static str> {
    if reply.incarnation != request.incarnation || reply.revision != request.revision {
        return Err("inventory changed during pagination");
    }
    if reply.entries.len() > usize::from(request.limit) {
        return Err("page exceeds the requested limit");
    }
    let mut previous = request.after;
    for entry in &reply.entries {
        if entry.generation == 0 || previous.is_some_and(|previous| entry.generation <= previous) {
            return Err("page entries are not strictly ascending after the cursor");
        }
        previous = Some(entry.generation);
    }
    match reply.next {
        None => Ok(()),
        Some(_) if reply.entries.is_empty() => Err("empty page names a continuation"),
        Some(next) if Some(next) != reply.entries.last().map(|entry| entry.generation) => {
            Err("continuation is not the last page entry")
        }
        Some(_) => Ok(()),
    }
}

impl Client {
    /// Read the owner's complete retained ledger frozen at `(incarnation,
    /// revision)` from a `Status` or `Fence` reply. A partial sweep is never
    /// returned: any changed revision or malformed page fails the sweep.
    pub fn inventory_sweep(
        &mut self,
        incarnation: [u8; 16],
        revision: u64,
    ) -> Result<Vec<InventoryGeneration>, TcpError> {
        let limit = u16::try_from(page_capacity(self.frame_limit))
            .map_err(|_| TcpError::new("inventory page capacity overflow"))?;
        if limit == 0 {
            return Err(TcpError::new(
                "negotiated frame cannot carry an inventory page",
            ));
        }
        let mut entries: Vec<InventoryGeneration> = Vec::new();
        let mut after = None;
        loop {
            let request = InventoryPageRequest {
                incarnation,
                revision,
                after,
                limit,
            };
            let reply = match self.request(Request::InventoryPage(request))? {
                Response::InventoryPage(reply) => reply,
                response => {
                    return self.wrong_variant("InventoryPage", &response, "InventoryPage");
                }
            };
            if let Err(detail) = check_page(&request, &reply) {
                let detail = if detail == "inventory changed during pagination" {
                    format!(
                        "owner {} inventory changed during pagination",
                        self.expected.partition
                    )
                } else {
                    detail.to_owned()
                };
                return self.wrong_response("InventoryPage", detail);
            }
            entries.extend_from_slice(&reply.entries);
            match reply.next {
                Some(next) => after = Some(next),
                None => return Ok(entries),
            }
        }
    }

    pub fn check_trim_generation(
        &mut self,
        authority_generation: u64,
        generation: u64,
    ) -> Result<TrimReadiness, TcpError> {
        if generation == 0 || generation >= authority_generation {
            return Err(TcpError::new(
                "trim check requires an older nonzero generation",
            ));
        }
        match self.request(Request::CheckTrimGeneration(CheckTrimRequest {
            authority_generation,
            generation,
        }))? {
            Response::CheckTrimGeneration(reply) if reply.generation == generation => {
                Ok(reply.result)
            }
            response => self.wrong_variant(
                "CheckTrimGeneration",
                &response,
                "CheckTrimGeneration for the requested generation",
            ),
        }
    }

    pub fn trim_generation(
        &mut self,
        authority_generation: u64,
        generation: u64,
    ) -> Result<(), TcpError> {
        if generation == 0 || generation >= authority_generation {
            return Err(TcpError::new("trim requires an older nonzero generation"));
        }
        match self.request(Request::TrimGeneration(TrimRequest {
            authority_generation,
            generation,
        }))? {
            Response::TrimGeneration(reply) if reply.generation == generation => Ok(()),
            response => self.wrong_variant(
                "TrimGeneration",
                &response,
                "TrimGeneration for the requested generation",
            ),
        }
    }
}

/// Owner crash points inside `TrimGeneration`, after the named durable step.
/// Test-only: the owner reports an error at the point and the test restarts it
/// from disk, which is exactly the durable state at that boundary.
#[cfg(test)]
#[allow(
    clippy::enum_variant_names,
    reason = "each variant names the durable trim step the owner has just completed"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OwnerTrimCrash {
    /// A zero-end seal for an abandoned generation was installed.
    AfterEmptySeal,
    /// Ledger without the generation and with `trim_pending` is installed.
    AfterLedger,
    /// The lookahead marker was removed but its directory was not synced.
    AfterLookaheadMarkerUnlink,
    /// Lookahead marker release and marker directory sync completed.
    AfterLookaheadMarker,
    /// The lookahead WAL was unlinked but its parent was not synced.
    AfterLookaheadFileUnlink,
    /// Lookahead WAL unlink and its parent directory sync completed.
    AfterLookaheadUnlink,
    /// The generation directory is deleted and the root synced.
    AfterUnlink,
}

#[cfg(test)]
impl StorageNode {
    pub(crate) fn fail_next_generation_trim(&self, point: OwnerTrimCrash) {
        self.inventory.lock().fail_next_trim(point);
    }
}
