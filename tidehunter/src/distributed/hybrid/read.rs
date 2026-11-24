//! Reading a value by native index position over the leased value WAL.
//!
//! The native index resolves a key to a [`VersionPosition`]; the position
//! carries the exact member frame on its owner and the CRC-32 of that member's
//! native entry. This module fetches precisely those bytes through the existing
//! asynchronous read lanes, verifies the CRC the coordinator computed when it
//! published the member, decodes the native [`WalEntry`] with the native helper,
//! and caches only the value bytes.
//!
//! There is no reference descriptor, no batch digest, no index pin and no
//! retained cut here. A read at an older cut is resolved by the native index
//! (`LargeTable::get_at`) and by the native checkpoint latch; the value path is
//! the same for every cut. Integrity is the owner-accepted weakening recorded in
//! the hybrid design: the per-member CRC-32 replaces the reference digests.
//!
//! [`WalEntry`]: crate::db::WalEntry

use crate::db::decode_record_frame;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::hybrid::table::ResolvedValue;
use crate::distributed::remote_db::pending_read::{
    ReadBatch, ReadRequest, ReadService, admission_error,
};
use crate::distributed::remote_db::read_cache::ReadCache;
use crate::distributed::storage::LocalDbError;
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShape, KeySpace, KeySpaceDesc};
use minibytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Bytes of the per-member frame header: entry length and identity checksum.
/// The hybrid read does not use the checksum; it verifies the position's CRC.
const MEMBER_HEADER_LEN: usize = 8;

/// Rows one `read_many` call may hold as fetched frames, the same page size the
/// exact scan builder used.
const MAX_PAGE_RECORDS: usize = 256;

/// A failure the value path may retry once.
///
/// The gate is `NotFound` and nothing else: an owner that no longer names the
/// member's address reports ENOENT, and that is the only failure a re-resolve can
/// repair. Every other kind fails fast with its own error, so corruption
/// (`InvalidData`), a poisoned authority, a lane deadline and a timed-out owner
/// RPC are never retried and never relabelled as an eviction.
///
/// The kind survives the wire: the owner's error carries an `io::ErrorKind` code
/// and [`crate::distributed::remote_db::pending_read::transport_error`] keeps it
/// when it converts the transport error, so this is a real classification rather
/// than a guess about the owner's wording.
fn retryable_read_error(error: &LocalDbError) -> bool {
    crate::distributed::remote_db::pending_read::member_read_is_retired(error)
}

/// Run `guard` and then `again` exactly once, if `first` failed retryably.
///
/// The whole retry policy of the value path: one re-resolve after a failure that
/// the index can repair, then a typed error. `again` runs at most once, so a
/// permanently failing address cannot spin.
///
/// Three outcomes are returned unchanged, never wrapped:
///
/// * a local member rejection (`InvalidData`: length, payload CRC, key binding,
///   the invalid sentinel) — corruption is never retried;
/// * `guard`'s error — a poisoned or closed engine carries its own cause, and
///   the fail-stop contract forbids masking it, so the guard runs *before* the
///   second attempt and its error wins;
/// * a second attempt that itself hits a local member rejection — the
///   re-resolved address holds the wrong bytes, which is a decode failure, not
///   an eviction.
///
/// Only a second *transport* failure becomes `evicted_during_read`.
pub(crate) fn retry_once<T>(
    first: Result<T, LocalDbError>,
    version: BatchVersion,
    guard: impl FnOnce() -> Result<(), LocalDbError>,
    again: impl FnOnce() -> Result<T, LocalDbError>,
) -> Result<T, LocalDbError> {
    match first {
        Ok(value) => Ok(value),
        Err(first_error) => {
            if !retryable_read_error(&first_error) {
                return Err(first_error);
            }
            guard()?;
            again().map_err(|second| {
                if second.kind() == io::ErrorKind::InvalidData {
                    second
                } else {
                    LocalDbError::evicted_during_read(version, &first_error, &second)
                }
            })
        }
    }
}

/// Apply the latched-view rule for an unreachable member: absent, exactly as
/// native answers for a reclaimed position. A live read returns the typed
/// eviction unchanged.
fn latched_absent<T>(
    result: Result<Option<T>, LocalDbError>,
    latched: bool,
) -> Result<Option<T>, LocalDbError> {
    match result {
        Err(error) if latched && error.is_evicted_during_read() => Ok(None),
        other => other,
    }
}

/// Reads values by position. One instance per open database.
///
/// `lanes` owns the bounded async read service and its owner connections;
/// `cache` is the process-wide value cache keyed by the immutable logical
/// identity. Both are shared, never cloned per request.
///
/// **Retention:** a value returned by `read*` is a refcounted slice of a lent
/// lane read block (up to 256 members share one block). The lane's byte budget
/// is released when a block is lent, so it does NOT bound memory held by
/// callers: total memory is the lane budget plus every block still referenced
/// by a retained result, and a small retained value pins its whole block. The
/// lane itself never stalls on it (it allocates a fresh block). Callers must not
/// hold results long-term. The value cache is unaffected: it copies on fill.
/// Whether `read` should detach small values (a cheap copy) is an open decision.
pub(crate) struct ValueReader {
    lanes: Arc<ReadService>,
    cache: Arc<ReadCache>,
    shape: Arc<KeyShape>,
    /// Registered partition to owner lane index, fixed for the life of the open.
    partitions: Arc<HashMap<u32, usize>>,
    /// Bound on parking for admission in one read, the same value the base
    /// scalar path used for its deadline. An owner stall ends the wait with a
    /// typed timeout instead of parking the calling thread forever; the live
    /// stream is selected from the position's address, so no open epoch is kept.
    read_timeout: Duration,
    hits: AtomicU64,
    misses: AtomicU64,
    decode_failures: AtomicU64,
    /// Test-only failure of every batched fetch: 0 none, 1 `NotFound`, 2 any
    /// other kind. Relaxed: a knob set by the test thread between operations.
    #[cfg(test)]
    read_many_fault: AtomicU8,
}

impl ValueReader {
    pub(crate) fn new(
        lanes: Arc<ReadService>,
        cache: Arc<ReadCache>,
        shape: Arc<KeyShape>,
        partitions: Arc<HashMap<u32, usize>>,
        read_timeout: Duration,
    ) -> Self {
        Self {
            lanes,
            cache,
            shape,
            partitions,
            read_timeout,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            decode_failures: AtomicU64::new(0),
            #[cfg(test)]
            read_many_fault: AtomicU8::new(0),
        }
    }

    /// Arm or clear a test-only failure of every [`Self::read_many`] call.
    /// `NotFound` is the retired-member kind the per-slot batch re-reads; any
    /// other kind is terminal. Tests only.
    #[cfg(test)]
    pub(crate) fn set_read_many_fault_for_test(&self, kind: Option<io::ErrorKind>) {
        let code = match kind {
            None => 0,
            Some(io::ErrorKind::NotFound) => 1,
            Some(_) => 2,
        };
        self.read_many_fault.store(code, Ordering::Relaxed);
    }

    /// Resolve the value stored for `position`.
    ///
    /// `Ok(None)` means the position holds a tombstone: the newest write for the
    /// key is a delete, so the value path has nothing to return. A CRC mismatch
    /// or a failed fetch is an error, never a value.
    ///
    /// There is no re-resolve here: a caller that can re-run the lookup uses
    /// [`Self::read_with_resolve`], and this one fails closed rather than
    /// retrying an address it cannot re-derive.
    ///
    /// `indexed_key` is the index's own (already reduced) key, not the caller's
    /// full key: for `Hash` and `Reduction` keyspaces the member's full key is
    /// reduced and compared with it, so a full key here rejects every member.
    #[cfg(test)]
    pub(crate) fn read(
        &self,
        ks: KeySpace,
        indexed_key: &[u8],
        position: VersionPosition,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.read_position(ks, indexed_key, position)
    }

    /// Read with exactly one re-resolve.
    ///
    /// The index resolved `position` at some cut, and the owner may retire the
    /// file that holds it between that lookup and this fetch (release is an
    /// unlink plus a directory sync, with no reader pin in this design). The
    /// caller's `resolve` re-runs the same index lookup at the same cut once:
    /// the entry either moved, in which case its new address is read, or it is
    /// gone, in which case the read is not-found. A second fetch failure is the
    /// typed `value evicted during read` error — never a retry loop.
    ///
    /// `latched` marks a read at a held checkpoint cut. Such a read answers a
    /// frame a pass reclaimed as absent, which is native's storage-layer result
    /// for an unreachable position (`Db::resolve_record_frame` returns `None`,
    /// and `DbCheckpoint` reads through it); native's checkpoint docs say the
    /// view does not hold WAL files. A live read keeps the typed eviction,
    /// because only a held cut can name a frame retirement has passed.
    ///
    /// The re-resolve may instead find the key resident in the native per-cell
    /// value LRU (a newer write of the same key fills it at publication), and
    /// that value is the answer: the `get_checkpoint` guard that yields it is
    /// exactly the guarantee that the key is unchanged since the requested cut.
    pub(crate) fn read_with_resolve(
        &self,
        ks: KeySpace,
        indexed_key: &[u8],
        position: VersionPosition,
        resolve: impl FnOnce() -> Result<Option<ResolvedValue>, LocalDbError>,
        latched: bool,
    ) -> Result<Option<Bytes>, LocalDbError> {
        let result = self.resolve_retrying(
            position,
            resolve,
            |at| self.read_position(ks, indexed_key, at),
            |_full_key, value| Ok(Some(value)),
        );
        latched_absent(result, latched)
    }

    /// Like [`Self::read_with_resolve`], but returns the member's **full key** with
    /// its value.
    ///
    /// A native iterator yields the full key, while the index holds only the
    /// reduced one (`Reduction`/`Hash` keyspaces), so the scan needs the key the
    /// member itself carries; a resident value carries the key it was stored
    /// under. The same CRC and key-binding checks, retry policy and
    /// decode-failure counter apply; `Ok(None)` means a tombstone or an entry the
    /// re-resolve found gone.
    pub(crate) fn read_entry_with_resolve(
        &self,
        ks: KeySpace,
        indexed_key: &[u8],
        position: VersionPosition,
        resolve: impl FnOnce() -> Result<Option<ResolvedValue>, LocalDbError>,
        latched: bool,
    ) -> Result<Option<(Bytes, Bytes)>, LocalDbError> {
        let result = self.resolve_retrying(
            position,
            resolve,
            |at| self.read_entry_position(ks, indexed_key, at),
            |full_key, value| Ok(Some((full_key, value))),
        );
        latched_absent(result, latched)
    }

    /// The one retry policy of the value path: read, and on a retired-member
    /// failure re-resolve exactly once and read again.
    ///
    /// `resident` converts a re-resolve that the native value LRU answered: the
    /// value is already in hand, so no owner is contacted. Both readers keep
    /// their own return shape (value only, or key and value).
    fn resolve_retrying<T>(
        &self,
        position: VersionPosition,
        resolve: impl FnOnce() -> Result<Option<ResolvedValue>, LocalDbError>,
        read: impl Fn(VersionPosition) -> Result<Option<T>, LocalDbError>,
        resident: impl FnOnce(Bytes, Bytes) -> Result<Option<T>, LocalDbError>,
    ) -> Result<Option<T>, LocalDbError> {
        let first = read(position);
        retry_once(
            first,
            position.batch,
            // Poison carries its own cause and must not be masked by a retry.
            || self.lanes.ensure_healthy(),
            || match resolve()? {
                Some(ResolvedValue::Resident { full_key, value }) => resident(full_key, value),
                // The index returned the same physical member, so the fetch would
                // fail again for the same reason: report the eviction instead of
                // spending a second round trip.
                Some(ResolvedValue::Position(resolved)) if resolved.same_physical(&position) => {
                    Err(LocalDbError::new(
                        "re-resolve returned the same physical member",
                    ))
                }
                Some(ResolvedValue::Position(resolved)) => read(resolved),
                None => Ok(None),
            },
        )
    }

    /// A position that is not valid at all (`VersionPosition::INVALID`,
    /// `batch == NONE`) is a caller bug and is reported as such instead of being
    /// answered with `None`.
    fn check_position(position: VersionPosition) -> Result<(), LocalDbError> {
        if position.is_valid() {
            return Ok(());
        }
        Err(LocalDbError::with_kind(
            io::ErrorKind::InvalidData,
            format!("value read was handed the invalid position sentinel: {position:?}"),
        ))
    }

    /// Cache lookup, fetch and decode for one position.
    ///
    /// Only `Put` positions reach this layer: the index returns a
    /// `TaggedVersionPosition`, and a delete is filtered there, so a value read
    /// never costs a round trip for a tombstone.
    fn read_position(
        &self,
        ks: KeySpace,
        indexed_key: &[u8],
        position: VersionPosition,
    ) -> Result<Option<Bytes>, LocalDbError> {
        Self::check_position(position)?;
        let identity = position.identity();
        if let Some(value) = self.cache.get(identity, ks.0) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(value));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let frame = self.fetch(position, indexed_key.len())?;
        let value = self.decode(ks, indexed_key, position, &frame)?;
        if let Some(value) = &value {
            let prepared = self.cache.prepare(identity, ks.0, value);
            self.cache.insert_prepared(prepared);
        }
        Ok(value)
    }

    /// Fetch and decode one position, returning the member's full key and value
    /// without consulting or filling the value cache.
    fn read_entry_position(
        &self,
        ks: KeySpace,
        indexed_key: &[u8],
        position: VersionPosition,
    ) -> Result<Option<(Bytes, Bytes)>, LocalDbError> {
        self.read_indexed(self.shape.ks(ks), indexed_key, position)
    }

    /// One verified read of a fixed position, returning the member's full key and
    /// value. No cache, no re-resolve.
    ///
    /// Relocation reads a survivor under its own cut latch and protocol: a
    /// failure is a rejection it counts and blocks the stream on, not something to
    /// retry or re-resolve here. The same CRC and key-binding checks and
    /// decode-failure counter apply as for every other read.
    pub(crate) fn read_indexed(
        &self,
        desc: &KeySpaceDesc,
        indexed_key: &[u8],
        position: VersionPosition,
    ) -> Result<Option<(Bytes, Bytes)>, LocalDbError> {
        Self::check_position(position)?;
        let frame = self.fetch(position, indexed_key.len())?;
        decode_entry_at(desc, indexed_key, position, &frame).inspect_err(|_| {
            self.decode_failures.fetch_add(1, Ordering::Relaxed);
        })
    }

    /// Fetch one member frame, preferring the caller-owned inline lane.
    ///
    /// The inline path borrows an idle lane's cached grant and skips the reactor
    /// hop, exactly as the scalar get did; it falls back to the admitted lane
    /// path when no lane is idle. Admission pressure (`BACKPRESSURE`,
    /// `BYTE_BUDGET`) is load, not failure: the caller parks on admission
    /// progress and retries the same position.
    fn fetch(&self, position: VersionPosition, extra_bytes: usize) -> Result<Bytes, LocalDbError> {
        let node = self.node(position)?;
        let deadline = Instant::now() + self.read_timeout;
        loop {
            let generation = self.lanes.admission_generation();
            if let Some(mut frames) =
                self.lanes
                    .read_inline_before(node, &position, extra_bytes, deadline)?
            {
                return frames
                    .pop()
                    .ok_or_else(|| LocalDbError::new("inline member read returned no value"));
            }
            match self.lanes.submit_member_before(node, position, deadline) {
                Ok(request) => return request.finish_member(),
                Err(error) if admission_error(&error) => {
                    self.lanes.wait_admission(generation, Some(deadline))?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Resolve several values of one keyspace, preserving input order.
    ///
    /// Values already resident are removed from the transport work; the rest
    /// are grouped by owner and fetched as bounded read batches through the same
    /// lanes as scalar reads. A missing or mismatched member fails the whole
    /// call, exactly like a scalar read.
    pub(crate) fn read_many(
        &self,
        ks: KeySpace,
        rows: &[(Bytes, VersionPosition)],
    ) -> Result<Vec<Option<Bytes>>, LocalDbError> {
        // One call keeps every fetched frame resident, so it is bounded like the
        // page builder it replaced: an iterator page asks for at most this many
        // rows, and a larger caller pages itself. This bounds value memory per
        // call instead of letting a scan pin an unbounded frame set.
        if rows.len() > MAX_PAGE_RECORDS {
            return Err(LocalDbError::new(
                "value page exceeds the bounded read page size",
            ));
        }
        #[cfg(test)]
        match self.read_many_fault.load(Ordering::Relaxed) {
            0 => {}
            1 => {
                return Err(LocalDbError::with_kind(
                    io::ErrorKind::NotFound,
                    "test-injected batched fetch failure",
                ));
            }
            _ => {
                return Err(LocalDbError::with_kind(
                    io::ErrorKind::PermissionDenied,
                    "test-injected batched fetch failure",
                ));
            }
        }
        let mut values: Vec<Option<Bytes>> = vec![None; rows.len()];
        let mut remote: Vec<Option<VersionPosition>> = vec![None; rows.len()];
        let mut groups: Vec<Vec<(usize, VersionPosition)>> =
            vec![Vec::new(); self.lanes.owner_count()];
        for (slot, (_, position)) in rows.iter().enumerate() {
            Self::check_position(*position)?;
            let identity = position.identity();
            if let Some(value) = self.cache.get(identity, ks.0) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                values[slot] = Some(value);
                continue;
            }
            self.misses.fetch_add(1, Ordering::Relaxed);
            remote[slot] = Some(*position);
            groups[self.node(*position)?].push((slot, *position));
        }
        let mut batch = ReadBatch::new(
            &self.lanes,
            values,
            groups,
            Instant::now() + self.read_timeout,
        )?;
        self.drain(&mut batch)?;
        for (slot, key_and_position) in rows.iter().enumerate() {
            let Some(position) = remote[slot] else {
                continue;
            };
            let frame = batch.values[slot]
                .take()
                .ok_or_else(|| LocalDbError::new("missing remote read result"))?;
            let value = self.decode(ks, &key_and_position.0, position, &frame)?;
            if let Some(value) = &value {
                let prepared = self.cache.prepare(position.identity(), ks.0, value);
                self.cache.insert_prepared(prepared);
            }
            batch.values[slot] = value;
        }
        Ok(batch.values)
    }

    /// Verify and decode one member frame.
    ///
    /// Every rejection counts as a decode failure; corruption and a key
    /// mismatch are reported by their own messages, so the receipt can tell
    /// them apart.
    fn decode(
        &self,
        ks: KeySpace,
        indexed_key: &[u8],
        position: VersionPosition,
        frame: &Bytes,
    ) -> Result<Option<Bytes>, LocalDbError> {
        decode_member(&self.shape, ks, indexed_key, position, frame).inspect_err(|_| {
            self.decode_failures.fetch_add(1, Ordering::Relaxed);
        })
    }

    fn node(&self, position: VersionPosition) -> Result<usize, LocalDbError> {
        self.partitions
            .get(&position.address.partition)
            .copied()
            .ok_or_else(|| LocalDbError::new("position names an unregistered partition"))
    }

    /// Run a read batch to completion, retrying while admission is full.
    ///
    /// The batch owns its admitted requests; a full read service is load, not a
    /// failure, so the caller parks on admission progress instead of spinning.
    fn drain(&self, batch: &mut ReadBatch) -> Result<(), LocalDbError> {
        // The batch owns the single absolute deadline of this call, so a
        // dispatch wave, an admission park and a member retry all share it.
        let deadline = batch.deadline();
        while !batch.complete() {
            let generation = self.lanes.admission_generation();
            match batch.dispatch(&self.lanes) {
                Ok(()) => {
                    if batch.is_idle() {
                        self.lanes.wait_admission(generation, Some(deadline))?;
                    } else {
                        batch.finish_one()?;
                    }
                }
                Err(error) if admission_error(&error) => {
                    self.lanes.wait_admission(generation, Some(deadline))?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub(crate) fn cache_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub(crate) fn cache_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub(crate) fn decode_failure_count(&self) -> u64 {
        self.decode_failures.load(Ordering::Relaxed)
    }
}

/// Verify and decode one member frame.
///
/// The frame is `[entry length u32][identity checksum u32][native entry]`. The
/// identity checksum is a write-side binding the hybrid does not keep; the
/// coordinator's CRC over the entry is the integrity check (I5). Any different
/// bytes at the address — corruption or a substituted member — change that CRC,
/// so the check is what makes a position trustworthy.
///
/// The decoded entry must still name the key the index asked for. The native
/// decoder only enforces that for a compressed batch, so the comparison is made
/// here for every member: a position whose CRC was itself corrupted must not
/// serve another key's value.
fn decode_member(
    shape: &KeyShape,
    ks: KeySpace,
    indexed_key: &[u8],
    position: VersionPosition,
    frame: &Bytes,
) -> Result<Option<Bytes>, LocalDbError> {
    decode_member_entry(shape, ks, indexed_key, position, frame)
        .map(|entry| entry.map(|(_, value)| value))
}

/// [`decode_member`] returning the member's full key with its value.
///
/// A native iterator yields the full key and the index holds the reduced one, so
/// a scan needs the key the member carries; every check is the same as for a
/// point read. `Ok(None)` is a tombstone.
fn decode_member_entry(
    shape: &KeyShape,
    ks: KeySpace,
    indexed_key: &[u8],
    position: VersionPosition,
    frame: &Bytes,
) -> Result<Option<(Bytes, Bytes)>, LocalDbError> {
    decode_entry_at(shape.ks(ks), indexed_key, position, frame)
}

/// The checks of [`decode_member_entry`] against an already-resolved keyspace
/// descriptor, for callers (relocation) that hold the descriptor, not the shape.
/// The frame is a `Bytes` so the entry handed to the native decoder is a slice
/// of it: the returned key and value share the fetched allocation and no payload
/// byte is copied on a read.
///
/// Where the frame comes from, and what keeping a slice of it costs, is
/// documented once on [`ReadRequest::finish_member`].
fn decode_entry_at(
    desc: &KeySpaceDesc,
    indexed_key: &[u8],
    position: VersionPosition,
    frame: &Bytes,
) -> Result<Option<(Bytes, Bytes)>, LocalDbError> {
    let header = frame
        .get(..MEMBER_HEADER_LEN)
        .ok_or_else(|| member_error(position, "member frame shorter than its header"))?;
    let declared = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let payload = frame.slice(MEMBER_HEADER_LEN..);
    if declared != payload.len() {
        return Err(member_error(
            position,
            "member frame length disagrees with its header",
        ));
    }
    if crc32fast::hash(&payload) != position.payload_crc {
        return Err(member_error(position, "member payload CRC mismatch"));
    }
    // A decode failure is a content failure, whatever the native decoder's own
    // error kind: mark it `InvalidData` so the value path never retries it.
    let Some((key, value)) = decode_record_frame(desc, indexed_key, payload).map_err(|error| {
        LocalDbError::with_kind(io::ErrorKind::InvalidData, format!("{error:?}"))
    })?
    else {
        return Ok(None);
    };
    // `indexed_key` is the index's key and is already reduced, so only the
    // member's full key is reduced here — never both sides. `reduce_key` is the
    // identity for fixed and variable-length keys, a slice for `Reduction` and a
    // hash for `Hash`, so one comparison is right for every indexing. Native's
    // `need_check_index_key()` is not used: it is false for `Hash`, which would
    // compare the full key with the hashed index key and reject every member.
    let names_requested_key = desc.reduce_key(&key).as_ref() == indexed_key;
    if !names_requested_key {
        return Err(member_error(
            position,
            "member entry names another key than the index asked for",
        ));
    }
    Ok(Some((key, value)))
}

/// Error for a member frame that does not match the position that names it.
///
/// The suffix names the check, because a length disagreement, a payload CRC
/// mismatch and a key mismatch are different findings.
fn member_error(position: VersionPosition, what: &str) -> LocalDbError {
    // `InvalidData` is the value path's marker for "the local member check
    // rejected these bytes": `retryable_read_error` uses it so a corrupted or
    // substituted member is never retried.
    LocalDbError::with_kind(
        io::ErrorKind::InvalidData,
        format!(
            "{what}: version {}:{} member {} (member frame check)",
            position.batch.epoch, position.batch.sequence, position.operation_index,
        ),
    )
}

/// A single-member read kept for callers that already own one position.
impl ReadService {
    #[cfg(test)]
    pub(crate) fn submit_member(
        &self,
        node: usize,
        position: VersionPosition,
    ) -> Result<ReadRequest, LocalDbError> {
        self.submit_positions(node, std::slice::from_ref(&position), None)
    }
    /// Submit one member under the caller's absolute deadline.
    pub(crate) fn submit_member_before(
        &self,
        node: usize,
        position: VersionPosition,
        deadline: Instant,
    ) -> Result<ReadRequest, LocalDbError> {
        self.submit_positions_before(node, std::slice::from_ref(&position), None, deadline)
    }
}

impl ReadRequest {
    /// Frame ownership, the one statement of it (the decode docs point here).
    ///
    /// Every `Bytes` a read hands back is a refcounted slice of a lane read block
    /// that the lane **lent** with `Bytes::from_owner(MemberBuffer)`
    /// (`nvmeof/live_io.rs`, the read completion). It is not a separately
    /// allocated copy and it is not a window the lane may refill: the block goes
    /// back to the lane's pool only when the last slice of it drops
    /// (`MemberBuffer::drop`), so the bytes of a frame, and of the key and value
    /// decoded as slices of it, never change while a handle exists, whatever else
    /// the lane reads meanwhile. The inline path returns the lane's `Bytes` as
    /// is, the lane path stores it in the request slot and `finish` clones the
    /// handle (a refcount bump); no step copies the payload before decode.
    ///
    /// Lending releases the block's charge from the lane's byte budget, so the
    /// lane never stalls on retained results and allocates a fresh block for the
    /// next read. The cost is that the budget does not bound memory held by
    /// callers: it is the lane budget plus every block still referenced by a
    /// retained result, and a small retained value pins its whole read block. The
    /// value cache copies on fill and never pins one. Tests:
    /// `live_io::tests::a_retained_read_is_never_overwritten_and_its_block_returns_after_the_drop`
    /// (the lane) and `a_decoded_member_shares_the_fetched_allocation` (decode).
    pub(crate) fn finish_member(self) -> Result<Bytes, LocalDbError> {
        let mut values = self.finish()?;
        if values.len() != 1 {
            return Err(LocalDbError::new(
                "single member read returned the wrong count",
            ));
        }
        values
            .pop()
            .ok_or_else(|| LocalDbError::new("single member read returned no value"))
    }
}

#[cfg(test)]
mod tests;
