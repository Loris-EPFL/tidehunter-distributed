//! Bridge from an authoritative durable batch to native index publication.
//!
//! Publication inserts one entry per mutation into the coordinator's native
//! `LargeTable` at a version-carrying [`VersionPosition`], in batch order, so a
//! complete application batch becomes visible together and no value byte is
//! copied into a second storage engine. The position orders by
//! `(batch, operation_index)` and addresses the member frame in its owner's
//! value WAL, which the remote read path fetches.
//!
//! The Exact-index bridge that this design replaced is deleted.
//!
//! Call inside the enclosing engine's ordered publication critical section. Any
//! failure after durable authority requires recovery, even when the index
//! transaction drops every staged entry; it cannot abort that durable batch.

use super::codec::{CodecError, CompleteBatch, Mutation, ValidatedMutations};
use super::hybrid::position::VersionPosition;
use super::hybrid::table::{HybridTable, PublishedMutation};
use super::storage::{AppendedBatch, DurableBatch, LocalDbError};
use super::types::{BatchId, BatchVersion, PayloadDigest, PhysicalValueAddress, RecordDescriptor};
use crate::cell::CellId;
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace};
use minibytes::Bytes;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::fmt;

/// Version-free routing for an immutable validated input. Borrowing the exact
/// mutation slice prevents caller changes between preflight and encoding.
pub(crate) struct UnboundNativePublicationRouting<'a> {
    shape: &'a KeyShape,
    mutations: &'a [Mutation],
    routes: Vec<MemberRoute>,
    drop_cells: bool,
}

/// One point member's routing: its keyspace, its native reduced key, and the
/// CRC-32 of its entry payload computed once at encode time (never under the
/// publisher lock).
struct MemberRoute {
    keyspace: KeySpace,
    reduced_key: Bytes,
    payload_crc: u32,
}

impl<'a> UnboundNativePublicationRouting<'a> {
    pub(crate) fn prepare(
        shape: &'a KeyShape,
        validated: &ValidatedMutations<'a>,
    ) -> Result<Self, NativePublicationError> {
        Self::from_mutations(shape, validated.mutations())
    }

    fn from_mutations(
        shape: &'a KeyShape,
        mutations: &'a [Mutation],
    ) -> Result<Self, NativePublicationError> {
        // A DropCells singleton has no point member; codec validation already
        // rejected it in a mixed batch.
        if matches!(mutations, [Mutation::DropCells { .. }]) {
            return Ok(Self {
                shape,
                mutations,
                routes: Vec::new(),
                drop_cells: true,
            });
        }
        let mut routes = Vec::with_capacity(mutations.len());
        for mutation in mutations {
            routes.push(route_mutation(shape, mutation)?);
        }
        Ok(Self {
            shape,
            mutations,
            routes,
            drop_cells: false,
        })
    }

    /// The reduced keys of a point batch, in batch order, for the dependency
    /// registry. Empty for a `DropCells` member.
    pub(crate) fn keys(&self) -> impl Iterator<Item = (KeySpace, &[u8])> {
        self.routes
            .iter()
            .map(|route| (route.keyspace, route.reduced_key.as_ref()))
    }

    pub(crate) fn owned_bytes(&self, shape: &KeyShape) -> Option<usize> {
        routing_owned_bytes(&self.routes, self.routes.capacity(), shape)
    }

    pub(crate) fn bind(
        mut self,
        batch: &CompleteBatch,
    ) -> Result<NativePublicationRouting, NativePublicationError> {
        // Count and ordered mutation equality bind the preallocation token to
        // the actual canonical batch. A reordered, substituted or truncated
        // batch can never reuse trusted routing merely because its size fits.
        if !std::ptr::eq(self.mutations, batch.mutations()) && self.mutations != batch.mutations() {
            return Err(NativePublicationError::ReceiptMismatch);
        }
        if !std::ptr::eq(self.mutations, batch.mutations()) {
            // Caller slices may be tiny views of arbitrarily large allocations.
            // Rebase only borrowed reduced keys onto the encoded batch and
            // retain already computed hashes. No values are copied, and neither
            // routing hashes nor canonical checksums are recomputed.
            for (route, mutation) in self.routes.iter_mut().zip(batch.mutations()) {
                let shape = self.shape.ks(route.keyspace);
                if !matches!(shape.key_indexing(), KeyIndexing::Hash)
                    && let Some(key) = mutation.point_key()
                {
                    route.reduced_key = shape.reduced_key_bytes(key.clone());
                }
            }
        }
        // One CRC pass per member runs here, in the caller's encode phase, so
        // `publish` does no O(value bytes) work under the publisher mutex.
        for (index, route) in self.routes.iter_mut().enumerate() {
            let reference = batch
                .record_reference(batch_address_of(batch), index)
                .map_err(NativePublicationError::Codec)?;
            route.payload_crc = member_payload_crc(batch, &reference.descriptor)?;
        }
        Ok(NativePublicationRouting {
            id: batch.id(),
            version: batch.version(),
            digest: batch.digest(),
            routes: self.routes,
            drop_cells: self.drop_cells,
        })
    }
}

fn routing_owned_bytes(routes: &[MemberRoute], capacity: usize, shape: &KeyShape) -> Option<usize> {
    routes.iter().try_fold(
        capacity.checked_mul(std::mem::size_of::<MemberRoute>())?,
        |bytes, route| {
            // Only a hash-indexed keyspace owns a newly allocated reduced key;
            // a fixed/reduced key is a zero-copy slice of the batch's key.
            if matches!(shape.ks(route.keyspace).key_indexing(), KeyIndexing::Hash) {
                bytes
                    .checked_add(route.reduced_key.len())?
                    .checked_add(super::codec::ROUTING_HASH_OWNER_BYTES)
            } else {
                Some(bytes)
            }
        },
    )
}

/// Routing retained from local preflight through durable publication. Values
/// still belong to the canonical batch; this token never establishes durability.
pub(crate) struct NativePublicationRouting {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    routes: Vec<MemberRoute>,
    drop_cells: bool,
}

impl NativePublicationRouting {
    /// Only authority-local batches already constructed/decoded with this shape.
    /// Untrusted bytes must pass CompleteBatch::decode before reaching here.
    #[cfg(test)]
    pub(crate) fn prepare(
        shape: &KeyShape,
        batch: &CompleteBatch,
    ) -> Result<Self, NativePublicationError> {
        UnboundNativePublicationRouting::from_mutations(shape, batch.mutations())?.bind(batch)
    }

    #[cfg(test)]
    pub(crate) fn owned_bytes(&self, shape: &KeyShape) -> Option<usize> {
        routing_owned_bytes(&self.routes, self.routes.capacity(), shape)
    }

    #[cfg(test)]
    pub(crate) fn key_count(&self) -> usize {
        self.routes.len()
    }

    /// Publish one complete batch into the coordinator's native index: insert
    /// every member at its version-carrying position, or apply a `DropCells`
    /// range at this version. One call per batch, so no reader observes a
    /// partially inserted batch. The per-member payload CRCs were computed at
    /// encode time, so this path does no O(value) work under the lock.
    pub(crate) fn publish(
        self,
        batch: &CompleteBatch,
        durable: DurableBatch,
        table: &HybridTable,
    ) -> Result<(), NativePublicationError<LocalDbError>> {
        let receipt = durable.appended();
        if !self.matches(batch, receipt) {
            return Err(NativePublicationError::ReceiptMismatch);
        }
        if let Some((keyspace, first, last)) = batch.drop_cells() {
            return table
                .drop_cells(keyspace, first.clone(), last.clone(), self.version)
                .map_err(NativePublicationError::Install);
        }
        if batch.is_relocated() {
            // A copy is never indexed at its own version: that would override a
            // later write of the key with the stale value (native replay skips
            // relocated records the same way). Its bytes still count toward the
            // snapshot cadence, as native counts them in its WAL position.
            return table
                .insert_published(&[], u64::from(receipt.address().len))
                .map_err(NativePublicationError::Install);
        }
        let members = published_members(&self.routes, batch, self.version, receipt.address())?;
        table
            .insert_published(&members, u64::from(receipt.address().len))
            .map_err(NativePublicationError::Install)
    }

    /// The durable receipt, the canonical batch and this routing must name one
    /// batch, one version and one member set. Anything else is a substitution.
    fn matches(&self, batch: &CompleteBatch, receipt: AppendedBatch) -> bool {
        receipt.id() == self.id
            && receipt.version() == self.version
            && receipt.digest() == self.digest
            && batch.id() == self.id
            && batch.version() == self.version
            && batch.digest() == self.digest
            && self.drop_cells == batch.drop_cells().is_some()
            && (self.drop_cells || self.routes.len() == batch.mutations().len())
    }
}

/// The index rows one complete batch inserts, in batch order. Every position
/// is derived from the trusted durable receipt: the member frame the read path
/// fetches (`batch offset + batch CRC header + descriptor offset`) and the
/// CRC-32 of its payload computed at encode time, which a reader verifies
/// without the `BatchId` that the member's identity checksum needs.
fn published_members<'a, E>(
    routes: &[MemberRoute],
    batch: &'a CompleteBatch,
    version: BatchVersion,
    address: PhysicalValueAddress,
) -> Result<SmallVec<[PublishedMutation<'a>; 16]>, NativePublicationError<E>> {
    let mut members = SmallVec::with_capacity(batch.mutations().len());
    for (index, mutation) in batch.mutations().iter().enumerate() {
        let route = routes
            .get(index)
            .ok_or(NativePublicationError::ReceiptMismatch)?;
        let reference = batch
            .record_reference(address, index)
            .map_err(NativePublicationError::Codec)?;
        let descriptor = reference.descriptor;
        let key = mutation
            .point_key()
            .ok_or(NativePublicationError::InvalidRouting)?;
        members.push(PublishedMutation {
            keyspace: mutation.keyspace(),
            key: key.as_ref(),
            kind: descriptor.kind,
            position: VersionPosition::member(
                version,
                address,
                descriptor.operation_index,
                descriptor,
                route.payload_crc,
            ),
            value: match mutation {
                Mutation::Put { value, .. } => Some(value),
                Mutation::Delete { .. } | Mutation::DropCells { .. } => None,
            },
        });
    }
    Ok(members)
}

/// The index positions the members of `batch` take when it is stored at
/// `address`: the version-carrying member frames a reader fetches. A relocation
/// pass uses them to re-point an entry at the copy it wrote.
///
/// # Errors
/// A codec error when `address` does not describe `batch`.
pub(crate) fn member_positions(
    batch: &CompleteBatch,
    address: PhysicalValueAddress,
) -> Result<Vec<VersionPosition>, NativePublicationError> {
    let version = batch.version();
    let mut positions = Vec::with_capacity(batch.mutations().len());
    for index in 0..batch.mutations().len() {
        let descriptor = batch
            .record_reference(address, index)
            .map_err(NativePublicationError::Codec)?
            .descriptor;
        let crc = member_payload_crc(batch, &descriptor)?;
        positions.push(VersionPosition::member(
            version,
            address,
            descriptor.operation_index,
            descriptor,
            crc,
        ));
    }
    Ok(positions)
}

/// A synthetic address that only satisfies `record_reference`'s length check.
/// Member offsets are batch-relative, so a member's payload CRC does not depend
/// on where the frame lives in its owner's WAL.
fn batch_address_of(batch: &CompleteBatch) -> PhysicalValueAddress {
    let len = batch
        .encoded()
        .len()
        .saturating_add(crate::crc::CrcFrame::CRC_HEADER_LENGTH);
    PhysicalValueAddress {
        partition: 0,
        segment_generation: 0,
        offset: 0,
        len: u32::try_from(len).unwrap_or(u32::MAX),
    }
}

/// CRC-32 over one member's native entry bytes, after its member header.
fn member_payload_crc(
    batch: &CompleteBatch,
    descriptor: &RecordDescriptor,
) -> Result<u32, NativePublicationError> {
    // `[len u32][identity checksum u32]`, codec member header.
    const MEMBER_HEADER_LEN: usize = 4 + 4;
    let start = descriptor.offset as usize + MEMBER_HEADER_LEN;
    let end = descriptor.offset as usize + descriptor.len as usize;
    let payload = batch
        .encoded()
        .get(start..end)
        .ok_or(NativePublicationError::InvalidRouting)?;
    let mut crc = crc32fast::Hasher::new();
    crc.update(payload);
    Ok(crc.finalize())
}

fn route_mutation(
    shape: &KeyShape,
    mutation: &Mutation,
) -> Result<MemberRoute, NativePublicationError> {
    let keyspace = mutation.keyspace();
    let descriptor = shape.ks(keyspace);
    let Some(key) = mutation.point_key() else {
        return Err(NativePublicationError::InvalidRouting);
    };
    if let KeyIndexing::Reduction(_, range) = descriptor.key_indexing()
        && (range.start > range.end || range.end > key.len())
    {
        return Err(NativePublicationError::InvalidRouting);
    }
    let reduced_key = descriptor.reduced_key_bytes(key.clone());
    if let CellId::Bytes(prefix) = descriptor.first_cell()
        && reduced_key.len() < prefix.len()
    {
        return Err(NativePublicationError::InvalidRouting);
    }
    Ok(MemberRoute {
        keyspace,
        reduced_key,
        // Filled by `bind` from the encoded batch, where no lock is held.
        payload_crc: 0,
    })
}

#[derive(Debug)]
pub enum NativePublicationError<E = Infallible> {
    Codec(CodecError),
    ReceiptMismatch,
    InvalidRouting,
    Install(E),
}

impl<E: fmt::Display> fmt::Display for NativePublicationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(f, "native publication batch validation: {error}"),
            Self::ReceiptMismatch => f.write_str("durable receipt does not match complete batch"),
            Self::InvalidRouting => f.write_str("mutation cannot use the native cell routing"),
            Self::Install(error) => write!(f, "native index installation failed: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for NativePublicationError<E> {}

#[cfg(test)]
mod tests;
