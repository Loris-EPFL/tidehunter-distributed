//! Bounded resident value grants layered over the existing WAL read authority.
//!
//! The envelope is immutable and repeats the full logical/physical reference;
//! neither its checksum nor a remote region key proves WAL membership or current
//! authority. Callers must retain the read cut and validate the grant identity.
#[cfg(feature = "experimental-fabric")]
use super::PROTOCOL;
use super::TcpError;
use crate::distributed::types::ValueRecordReference;
#[cfg(feature = "experimental-fabric")]
use bincode::Options;
use serde::{Deserialize, Serialize};
#[cfg(feature = "experimental-fabric")]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(feature = "experimental-fabric")]
const ENVELOPE_MAGIC: [u8; 8] = *b"THRMA001";

/// Explicit opt-in limits for one-sided resident reads. Zero source bytes disables
/// promotion; zero destination slots disables local RMA issue without changing RPC.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResidentReadConfig {
    /// Process-wide maximum resident source bytes, including filling/revoking data.
    #[serde(default)]
    pub source_bytes: usize,
    /// Process-wide maximum independently registered immutable members.
    #[serde(default)]
    pub source_regions: usize,
    /// Process-wide maximum provider-owned plus caller-owned read destinations.
    #[serde(default)]
    pub destination_bytes: usize,
    #[serde(default)]
    pub destination_slots: usize,
    /// Initial implementation does not split members across multiple operations.
    #[serde(default)]
    pub max_read_bytes: usize,
    /// Maximum reusable remote grants retained by one client connection.
    #[serde(default)]
    pub max_handles: usize,
    /// Reject selected software/emulated provider reads.
    #[serde(default)]
    pub require_device_read: bool,
}

impl ResidentReadConfig {
    pub(crate) fn source_enabled(self) -> bool {
        self.source_bytes != 0 && self.source_regions != 0
    }

    pub(crate) fn enabled(self) -> bool {
        self.source_enabled() || self.destination_slots != 0
    }

    pub(crate) fn validate(self) -> Result<(), TcpError> {
        if (self.source_bytes == 0) != (self.source_regions == 0)
            || (self.destination_bytes == 0) != (self.destination_slots == 0)
            || (self.destination_bytes == 0) != (self.max_handles == 0)
            || self.enabled() != (self.max_read_bytes != 0)
            || (self.require_device_read && !self.enabled())
            || (self.destination_bytes != 0 && self.max_read_bytes > self.destination_bytes)
        {
            return Err(TcpError::new("invalid resident-read limits"));
        }
        Ok(())
    }

    // Libfabric precharges source-region metadata per endpoint. Partition only
    // that fixed table across admitted connections; source bytes remain a
    // node-wide budget enforced by GrantRegistry.
    pub(crate) fn source_per_connection(self, connections: usize) -> Self {
        if connections == 0 || !self.source_enabled() {
            return Self::default();
        }
        let source_regions = self.source_regions.div_ceil(connections).max(1);
        Self {
            source_bytes: self.source_bytes,
            source_regions,
            destination_bytes: 0,
            destination_slots: 0,
            max_read_bytes: self.max_read_bytes,
            max_handles: 0,
            require_device_read: false,
        }
    }

    pub(crate) fn destination_per_connection(self, connections: usize) -> Result<Self, TcpError> {
        if connections == 0 || self.destination_slots == 0 {
            return Ok(Self::default());
        }
        let destination_bytes = self.destination_bytes / connections;
        let destination_slots = self.destination_slots / connections;
        let max_handles = self.max_handles / connections;
        if destination_bytes == 0 || destination_slots == 0 || max_handles == 0 {
            if self.require_device_read {
                return Err(TcpError::new(
                    "resident-read destination budget is too small for all lanes",
                ));
            }
            return Ok(Self::default());
        }
        let max_read_bytes = self
            .max_read_bytes
            .min(destination_bytes / destination_slots);
        if max_read_bytes == 0 {
            return Ok(Self::default());
        }
        Ok(Self {
            source_bytes: 0,
            source_regions: 0,
            destination_bytes,
            destination_slots,
            max_read_bytes,
            max_handles,
            require_device_read: self.require_device_read,
        })
    }
}
/// Wire-safe remote descriptor; it contains no process-local MR pointer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct RemoteRegion {
    pub(crate) region_id: u64,
    pub(crate) generation: u64,
    pub(crate) remote_address: u64,
    pub(crate) key: u64,
    pub(crate) len: u64,
}

/// Session-scoped capability attached opportunistically to a successful RPC.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ResidentGrant {
    pub(crate) reference: ValueRecordReference,
    pub(crate) request_epoch: u64,
    pub(crate) reader_generation: u64,
    pub(crate) owner_boot: [u8; 16],
    pub(crate) session_id: u64,
    pub(crate) grant_id: u64,
    pub(crate) grant_generation: u64,
    pub(crate) region: RemoteRegion,
    pub(crate) source_region_id: u64,
    pub(crate) source_region_generation: u64,
}

#[cfg(feature = "experimental-fabric")]
#[derive(Serialize, Deserialize)]
struct EnvelopeHeader {
    magic: [u8; 8],
    protocol: u32,
    reference: ValueRecordReference,
    request_epoch: u64,
    reader_generation: u64,
    owner_boot: [u8; 16],
    session_id: u64,
    grant_id: u64,
    grant_generation: u64,
    region_id: u64,
    region_generation: u64,
    member_len: u32,
}

#[cfg(feature = "experimental-fabric")]
/// Construct the immutable source bytes once, before publishing a grant.
pub(crate) fn encode_envelope(grant: ResidentGrant, member: &[u8]) -> Result<Vec<u8>, TcpError> {
    let member_len = u32::try_from(member.len())
        .map_err(|_| TcpError::new("resident member exceeds envelope limit"))?;
    if member_len != grant.reference.descriptor.len {
        return Err(TcpError::new("resident member length/reference mismatch"));
    }
    let header = EnvelopeHeader {
        magic: ENVELOPE_MAGIC,
        protocol: PROTOCOL,
        reference: grant.reference,
        request_epoch: grant.request_epoch,
        reader_generation: grant.reader_generation,
        owner_boot: grant.owner_boot,
        session_id: grant.session_id,
        grant_id: grant.grant_id,
        grant_generation: grant.grant_generation,
        region_id: grant.source_region_id,
        region_generation: grant.source_region_generation,
        member_len,
    };
    let header = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .serialize(&header)
        .map_err(TcpError::component)?;
    let checksum = crc32fast::hash(&header);
    let total = 4usize
        .checked_add(header.len())
        .and_then(|len| len.checked_add(4))
        .and_then(|len| len.checked_add(member.len()))
        .ok_or_else(|| TcpError::new("resident envelope length overflow"))?;
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&(header.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(member);
    Ok(bytes)
}

/// Validate envelope identity and checksum before returning the encoded member.
#[cfg(feature = "experimental-fabric")]
pub(crate) fn checked_member(grant: ResidentGrant, bytes: &[u8]) -> Result<&[u8], TcpError> {
    if bytes.len() < 8 {
        return Err(TcpError::new("truncated resident envelope"));
    }
    let header_len =
        u32::from_le_bytes(bytes[..4].try_into().map_err(TcpError::component)?) as usize;
    let header_end = 4usize
        .checked_add(header_len)
        .ok_or_else(|| TcpError::new("resident envelope header overflow"))?;
    let checksum_end = header_end
        .checked_add(4)
        .ok_or_else(|| TcpError::new("resident envelope checksum overflow"))?;
    if checksum_end > bytes.len() {
        return Err(TcpError::new("truncated resident envelope header"));
    }
    let header_bytes = &bytes[4..header_end];
    let expected_checksum = u32::from_le_bytes(
        bytes[header_end..checksum_end]
            .try_into()
            .map_err(TcpError::component)?,
    );
    if crc32fast::hash(header_bytes) != expected_checksum {
        return Err(TcpError::new("resident envelope checksum mismatch"));
    }
    let header: EnvelopeHeader = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(header_bytes)
        .map_err(TcpError::component)?;
    if header.magic != ENVELOPE_MAGIC
        || header.protocol != PROTOCOL
        || header.reference != grant.reference
        || header.request_epoch != grant.request_epoch
        || header.reader_generation != grant.reader_generation
        || header.owner_boot != grant.owner_boot
        || header.session_id != grant.session_id
        || header.grant_id != grant.grant_id
        || header.grant_generation != grant.grant_generation
        || header.region_id != grant.source_region_id
        || header.region_generation != grant.source_region_generation
        || header.member_len != grant.reference.descriptor.len
    {
        return Err(TcpError::new("resident envelope identity mismatch"));
    }
    let member_end = checksum_end
        .checked_add(header.member_len as usize)
        .ok_or_else(|| TcpError::new("resident member length overflow"))?;
    if member_end != bytes.len() {
        return Err(TcpError::new("resident envelope length mismatch"));
    }
    Ok(&bytes[checksum_end..member_end])
}

/// Cumulative process snapshot. Occupancy includes revoking and quarantined owners.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResidentReadStats {
    pub message_read_requests: u64,
    pub grants_issued: u64,
    pub grant_acquisitions: u64,
    pub rma_completions: u64,
    pub eligible_reads: u64,
    pub rma_posts: u64,
    pub rma_hits: u64,
    pub rma_errors: u64,
    pub fallback_missing: u64,
    pub fallback_oversize: u64,
    pub fallback_capacity: u64,
    pub fallback_unsupported: u64,
    pub fallback_stale: u64,
    pub source_bytes_live: u64,
    pub source_regions_live: u64,
    pub source_bytes_quarantined: u64,
    pub destination_slots_live: u64,
    pub handles_live: u64,
    pub grants_live: u64,
}

impl ResidentReadStats {
    pub(crate) fn add(&mut self, other: Self) {
        self.message_read_requests += other.message_read_requests;
        self.grants_issued += other.grants_issued;
        self.grant_acquisitions += other.grant_acquisitions;
        self.rma_completions += other.rma_completions;
        self.eligible_reads += other.eligible_reads;
        self.rma_posts += other.rma_posts;
        self.rma_hits += other.rma_hits;
        self.rma_errors += other.rma_errors;
        self.fallback_missing += other.fallback_missing;
        self.fallback_oversize += other.fallback_oversize;
        self.fallback_capacity += other.fallback_capacity;
        self.fallback_unsupported += other.fallback_unsupported;
        self.fallback_stale += other.fallback_stale;
        self.source_bytes_live += other.source_bytes_live;
        self.source_regions_live += other.source_regions_live;
        self.source_bytes_quarantined += other.source_bytes_quarantined;
        self.destination_slots_live += other.destination_slots_live;
        self.handles_live += other.handles_live;
        self.grants_live += other.grants_live;
    }
}

#[cfg(feature = "experimental-fabric")]
struct PendingGrant {
    _owner: Arc<super::super::storage::ResidentReadOwner>,
    bytes: usize,
}

#[cfg(feature = "experimental-fabric")]
struct GrantOwner {
    _owner: Arc<super::super::storage::ResidentReadOwner>,
    bytes: usize,
    quarantined: bool,
}

struct GrantRegistryState {
    accepting: bool,
    poison_requested: bool,
    #[cfg(feature = "experimental-fabric")]
    poison_notified: std::collections::HashSet<u64>,
    #[cfg(feature = "experimental-fabric")]
    pending_bytes: usize,
    #[cfg(feature = "experimental-fabric")]
    pending: std::collections::HashMap<(u64, u64), PendingGrant>,
    #[cfg(feature = "experimental-fabric")]
    live_bytes: usize,
    #[cfg(feature = "experimental-fabric")]
    quarantined_bytes: usize,
    #[cfg(feature = "experimental-fabric")]
    source_bytes_limit: usize,
    #[cfg(feature = "experimental-fabric")]
    source_regions_limit: usize,
    #[cfg(feature = "experimental-fabric")]
    owners: std::collections::HashMap<(u64, u64), GrantOwner>,
}

pub(crate) struct GrantRegistry {
    state: parking_lot::Mutex<GrantRegistryState>,
    poison_requested: AtomicBool,
}

impl GrantRegistry {
    pub(crate) fn new(source_bytes: usize, source_regions: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new(GrantRegistryState {
                accepting: source_bytes != 0 && source_regions != 0,
                poison_requested: false,
                #[cfg(feature = "experimental-fabric")]
                poison_notified: std::collections::HashSet::new(),
                #[cfg(feature = "experimental-fabric")]
                pending_bytes: 0,
                #[cfg(feature = "experimental-fabric")]
                pending: std::collections::HashMap::new(),
                #[cfg(feature = "experimental-fabric")]
                live_bytes: 0,
                #[cfg(feature = "experimental-fabric")]
                quarantined_bytes: 0,
                #[cfg(feature = "experimental-fabric")]
                source_bytes_limit: source_bytes,
                #[cfg(feature = "experimental-fabric")]
                source_regions_limit: source_regions,
                #[cfg(feature = "experimental-fabric")]
                owners: std::collections::HashMap::new(),
            }),
            poison_requested: AtomicBool::new(false),
        }
    }

    #[cfg(feature = "experimental-fabric")]
    pub(crate) fn try_reserve(
        &self,
        session_id: u64,
        generation: u64,
        bytes: usize,
        owner: super::super::storage::ResidentReadOwner,
    ) -> Option<Arc<super::super::storage::ResidentReadOwner>> {
        let mut state = self.state.lock();
        let key = (session_id, generation);
        if !state.accepting
            || bytes == 0
            || state.pending.contains_key(&key)
            || state.owners.contains_key(&key)
            || state
                .pending
                .len()
                .checked_add(state.owners.len())
                .is_none_or(|used| used >= state.source_regions_limit)
            || state
                .live_bytes
                .checked_add(state.pending_bytes)
                .and_then(|used| used.checked_add(bytes))
                .is_none_or(|used| used > state.source_bytes_limit)
        {
            return None;
        }
        let owner = Arc::new(owner);
        state.pending_bytes += bytes;
        state.pending.insert(
            key,
            PendingGrant {
                _owner: Arc::clone(&owner),
                bytes,
            },
        );
        Some(owner)
    }

    #[cfg(feature = "experimental-fabric")]
    pub(crate) fn cancel_reservation(&self, session_id: u64, generation: u64) {
        let mut state = self.state.lock();
        if let Some(pending) = state.pending.remove(&(session_id, generation)) {
            state.pending_bytes -= pending.bytes;
        }
    }

    #[cfg(feature = "experimental-fabric")]
    pub(crate) fn shrink_reservation(
        &self,
        session_id: u64,
        generation: u64,
        actual: usize,
    ) -> bool {
        let mut state = self.state.lock();
        let released = {
            let Some(pending) = state.pending.get_mut(&(session_id, generation)) else {
                return false;
            };
            if actual == 0 || actual > pending.bytes {
                return false;
            }
            let released = pending.bytes - actual;
            pending.bytes = actual;
            released
        };
        state.pending_bytes -= released;
        true
    }

    #[cfg(feature = "experimental-fabric")]
    pub(crate) fn publish(&self, session_id: u64, generation: u64, bytes: usize) -> bool {
        let mut state = self.state.lock();
        let key = (session_id, generation);
        if state.poison_requested
            || state.owners.contains_key(&key)
            || state
                .pending
                .get(&key)
                .is_none_or(|pending| pending.bytes != bytes)
        {
            return false;
        }
        let Some(pending) = state.pending.remove(&key) else {
            return false;
        };
        state.pending_bytes -= bytes;
        state.owners.insert(
            key,
            GrantOwner {
                _owner: pending._owner,
                bytes,
                quarantined: false,
            },
        );
        state.live_bytes += bytes;
        true
    }

    #[cfg(feature = "experimental-fabric")]
    pub(crate) fn release(&self, session_id: u64, generations: &[u64]) -> bool {
        let mut state = self.state.lock();
        if generations
            .iter()
            .any(|generation| !state.owners.contains_key(&(session_id, *generation)))
        {
            return false;
        }
        for generation in generations {
            if let Some(owner) = state.owners.remove(&(session_id, *generation)) {
                state.live_bytes -= owner.bytes;
                if owner.quarantined {
                    state.quarantined_bytes -= owner.bytes;
                }
            }
        }
        true
    }

    #[cfg(feature = "experimental-fabric")]
    pub(crate) fn quarantine_session(&self, session_id: u64) {
        let mut state = self.state.lock();
        let mut added = 0usize;
        for ((owner_session, _), owner) in &mut state.owners {
            if *owner_session == session_id && !owner.quarantined {
                owner.quarantined = true;
                added += owner.bytes;
            }
        }
        state.quarantined_bytes += added;
    }

    pub(crate) fn request_poison(&self) {
        let mut state = self.state.lock();
        state.accepting = false;
        state.poison_requested = true;
        self.poison_requested.store(true, Ordering::Release);
    }

    pub(crate) fn poison_requested(&self) -> bool {
        self.poison_requested.load(Ordering::Acquire)
    }

    pub(crate) fn poison_quiescent(&self) -> bool {
        if !self.poison_requested() {
            return false;
        }
        #[cfg(feature = "experimental-fabric")]
        {
            let state = self.state.lock();
            state.pending.is_empty() && state.owners.is_empty()
        }
        #[cfg(not(feature = "experimental-fabric"))]
        {
            true
        }
    }

    #[cfg(feature = "experimental-fabric")]
    /// Each exact session is notified once. Its owner thread sends the notice
    /// only after all earlier response TX owners have retired.
    pub(crate) fn take_poison_notice(&self, session_id: u64) -> Option<Vec<u64>> {
        if !self.poison_requested() {
            return None;
        }
        let mut state = self.state.lock();
        if state.poison_notified.contains(&session_id) {
            return None;
        }
        let mut generations: Vec<_> = state
            .owners
            .keys()
            .filter_map(|(owner_session, generation)| {
                (*owner_session == session_id).then_some(*generation)
            })
            .collect();
        if generations.is_empty() {
            return None;
        }
        generations.sort_unstable();
        state.poison_notified.insert(session_id);
        Some(generations)
    }

    pub(crate) fn close_admission(&self) -> bool {
        let mut state = self.state.lock();
        state.accepting = false;
        #[cfg(feature = "experimental-fabric")]
        {
            state.pending.is_empty() && state.owners.is_empty()
        }
        #[cfg(not(feature = "experimental-fabric"))]
        {
            true
        }
    }

    pub(crate) fn reopen_admission(&self) {
        let mut state = self.state.lock();
        if !state.poison_requested {
            state.accepting = true;
        }
    }

    #[cfg(all(test, feature = "experimental-fabric"))]
    pub(crate) fn live_stats(&self) -> (usize, usize, usize) {
        let state = self.state.lock();
        (
            state.live_bytes,
            state.owners.len(),
            state.quarantined_bytes,
        )
    }
}
