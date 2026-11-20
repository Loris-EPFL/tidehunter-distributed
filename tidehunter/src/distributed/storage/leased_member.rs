//! Member-address reads over the owner's leased value WAL.
//!
//! A hybrid index position names the member frame itself, so these paths need no
//! reference directory or descriptor: they resolve an address to an owner-pinned
//! file range, plan a whole-frame read, and classify a retired address as
//! `NotFound`. They live in a child module of `storage` to hold `storage.rs`
//! under its length ratchet; everything stays inherent on `PartitionLog` and the
//! two items other modules name are re-exported from `storage`.
//!
//! Trust scope. Before the hybrid, the owner served only members that the
//! coordinator had registered as batch members: a read named a reference, and
//! the owner resolved it through the reference directory and the batch
//! descriptor, so it could not be asked for any other bytes. Now a live member
//! resolve is keyed by address alone: the owner grants any in-WAL range the
//! authenticated, epoch-fenced coordinator names. Partition, segment
//! generation and in-file bounds are checked, and the request's
//! `authority_epoch` must be the owner's active epoch (a fenced coordinator is
//! refused); the content of the bytes is not checked here. This is safe because
//! the authenticated session is the coordinator's authority over its own
//! partition's WAL (it already writes it), the epoch fence excludes a deposed
//! coordinator, and the content check is the member CRC-32 carried by the index
//! position, verified before decoding (`hybrid::read`). That CRC-for-digest
//! exchange is the owner-accepted weakening of the hybrid design.

use super::*;

#[derive(Clone)]
pub(crate) struct RecordReadPlan {
    pub(crate) member: crate::wal::WalMemberReadPlan,
    // Descriptor authority survives cache eviction through completion retirement.
    // `None` when the plan was built from a hybrid member address: the index
    // carries the position and the payload CRC, so there is no descriptor
    // authority to keep alive.
    pub(super) _authority: Option<Arc<RegisteredBatch>>,
    // Holds both immutable segment ownership and the directory authority lock.
    pub(crate) _log: Arc<PartitionLog>,
}

impl RecordReadPlan {
    #[cfg(test)]
    pub(crate) fn authority_bytes(&self) -> usize {
        self._authority
            .as_ref()
            .map_or(0, |authority| authority.authority_bytes())
    }
}

impl StorageError {
    /// The `io::ErrorKind` this failure must reach a coordinator with.
    ///
    /// A retired member address is `NotFound` (the owner writes it as such), and
    /// the value path retries exactly that kind once because a re-resolve can
    /// name a live address. Everything else — validation (`InvalidData`),
    /// poison, timeouts, capacity — must fail fast. The owner's control handler
    /// carries this kind onto the wire with `TcpError::with_kind` instead of
    /// flattening it through `TcpError::component`.
    pub(crate) fn wire_kind(&self) -> io::ErrorKind {
        match self {
            Self::Io(error) => error.kind(),
            Self::Invalid(_) => io::ErrorKind::InvalidData,
            _ => io::ErrorKind::Other,
        }
    }
}

impl PartitionLog {
    pub(super) fn record_extent(
        state: &State,
        address: PhysicalValueAddress,
    ) -> Option<&OpenLiveExtent> {
        let end = address.offset.checked_add(u64::from(address.len))?;
        state
            .open_live_extent
            .iter()
            .chain(state.next_live_extent.iter())
            .find(|open| address.offset >= open.extent.start && end <= open.extent.end)
    }

    /// A live member read is served only from this stream's active generation.
    ///
    /// The hybrid position carries the address, not the batch version, so the
    /// live-stream constraint is the address's generation, not a batch epoch:
    /// a relocated position keeps its logical epoch but names the generation
    /// that holds its bytes.
    pub(super) fn check_cold_read_state(
        &self,
        state: &State,
        address: PhysicalValueAddress,
    ) -> Result<(), StorageError> {
        if state.poison.is_some() {
            return Err(StorageError::Poisoned);
        }
        if state.is_sealing() {
            return Err(StorageError::Invalid("live stream is sealing"));
        }
        if address.partition != self.config().partition
            || address.segment_generation != self.config().inventory.segment_generation
        {
            return Err(StorageError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "foreign record address",
            )));
        }
        Ok(())
    }

    pub(super) fn check_cold_read_extent(
        &self,
        state: &State,
        address: PhysicalValueAddress,
        extent: &crate::distributed::nvmeof::live_storage::LiveWriteExtent,
    ) -> Result<bool, StorageError> {
        self.check_cold_read_state(state, address)?;
        if state.sealed {
            return Err(StorageError::Sealed);
        }
        match Self::record_extent(state, address) {
            Some(open) if open.extent == *extent => Ok(true),
            Some(_) => Err(StorageError::Invalid("live record extent changed")),
            // Healthy close is the only non-lifecycle removal of an extent.
            // Its batches are now adopted. Discard this scoped proof/pin and
            // let the normal serving path obtain authoritative coverage; a
            // successful close must not cause a spurious value-read failure.
            None => Ok(false),
        }
    }

    /// Resolve a member address to an owner-pinned file range.
    ///
    /// The live stream takes the leased path first, which pins the exact extent
    /// that owns the bytes. Anything else (a sealed file of this log, or a
    /// member whose extent has already closed) is planned through this log's
    /// native WAL file set: the address is self-describing, so no reference
    /// directory or descriptor lookup is involved.
    pub fn resolve_live_member(
        self: &Arc<Self>,
        address: PhysicalValueAddress,
    ) -> Result<crate::distributed::nvmeof::live_storage::LiveRecordLease, StorageError> {
        if self.live_stream
            && let Some(lease) = self.resolve_leased_member(address, None)?
        {
            return Ok(lease);
        }
        let _admission = self.read_admission()?;
        let plan = self.plan_member_read(address)?;
        self.live_lease(plan, address.offset)
    }

    /// Plan a member read directly from its address.
    ///
    /// `address.len` is the member frame length, so the read is the whole frame
    /// inside the owner WAL file: `[length][crc][entry]`, exactly what the
    /// reader verifies and decodes. The address is validated before any file is
    /// planned: an empty or file-crossing range is rejected, never wrapped or
    /// truncated.
    fn plan_member_read(
        self: &Arc<Self>,
        address: PhysicalValueAddress,
    ) -> Result<RecordReadPlan, StorageError> {
        let file_bytes = self.config().wal_file_bytes;
        if (address.len as usize) < crate::crc::CrcFrame::CRC_HEADER_LENGTH
            || (address.offset % file_bytes)
                .checked_add(u64::from(address.len))
                .is_none_or(|end| end > file_bytes)
        {
            return Err(StorageError::Invalid("member address crosses a WAL file"));
        }
        let member = self
            .wal
            .plan_frame(WalPosition::new(address.offset, address.len))
            .map_err(StorageError::Wal)?
            .ok_or(StorageError::Io(io::Error::new(
                io::ErrorKind::NotFound,
                "member address is not in this WAL log",
            )))?;
        Ok(RecordReadPlan {
            member,
            _authority: None,
            _log: self.clone(),
        })
    }
}

/// The member frame address a validated reference descriptor names.
///
/// `reference.batch_address` is the batch frame (its offset includes the batch's
/// own native CRC header); the batch payload starts 8 bytes later, and
/// `descriptor.offset`/`len` are relative to that payload and include the
/// member's own 8-byte header. The result is therefore the exact bytes a hybrid
/// position names, so both read paths transfer the same range.
pub(crate) fn member_address_of_reference(
    reference: &ValueRecordReference,
) -> Result<PhysicalValueAddress, StorageError> {
    // The native frame header is 8 bytes; the batch and descriptor lengths are
    // `u32`, so the constant is narrowed once instead of at every use.
    let header = crate::crc::CrcFrame::CRC_HEADER_LENGTH as u32;
    let payload_len =
        reference
            .batch_address
            .len
            .checked_sub(header)
            .ok_or(StorageError::Invalid(
                "batch address is shorter than a frame header",
            ))?;
    let end = reference
        .descriptor
        .offset
        .checked_add(reference.descriptor.len)
        .ok_or(StorageError::Invalid("member bounds overflow"))?;
    let offset = reference
        .batch_address
        .offset
        .checked_add(u64::from(header))
        .and_then(|start| start.checked_add(u64::from(reference.descriptor.offset)))
        .ok_or(StorageError::Invalid("member address overflow"))?;
    if reference.descriptor.len < header || end > payload_len {
        return Err(StorageError::Invalid(
            "member lies outside its batch payload",
        ));
    }
    Ok(PhysicalValueAddress {
        partition: reference.batch_address.partition,
        segment_generation: reference.batch_address.segment_generation,
        offset,
        len: reference.descriptor.len,
    })
}
