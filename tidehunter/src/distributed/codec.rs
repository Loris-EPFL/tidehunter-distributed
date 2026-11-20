//! Complete authority record using native `WalEntry` record/tombstone encoding.
//!
//! Tidehunter's atomic-batch write flow requires all-or-none replay; unlike native BatchStart's
//! following frames, this experimental path wraps the whole batch in one native
//! CRC frame. The October 6 design §7.1 requires semantic validation *before*
//! that frame can exist. CORFU's multi-object entry motivates the authority
//! boundary, but does not supply Tidehunter's index or relocation semantics.
//!
//! Version 3 frames each native member with its length and canonical IEEE CRC-32.
//! Whole-container validation still establishes completeness at append/replay;
//! point reads use trusted membership metadata and verify only the selected
//! member. This keeps the paper's direct index-to-Value-WAL read path (§3)
//! without reading sibling values or creating another permanent value representation.
//!
//! Version 4 is a singleton batch whose one member is a native `DropCells`
//! entry (tag 6, see `drop_cells`). It has no value and no record descriptor;
//! it is never mixed with point members.

mod drop_cells;
mod relocated;

pub(crate) use super::types::{
    BatchId, BatchVersion, MemberChecksum, MutationVersion, PayloadDigest, PhysicalValueAddress,
    RecordDescriptor, RecordKind, ValueRecordReference,
};
use crate::cell::CellId;
use crate::crc::IntoBytesFixed;
use crate::db::{MAX_KEY_LEN, WalEntry};
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace};
use bytes::{BufMut, BytesMut};
use minibytes::Bytes;
use std::fmt;

const MAGIC: &[u8; 8] = b"THBATCH3";
pub const FORMAT_VERSION: u16 = 3;
/// Format of a singleton `DropCells` batch. Point batches keep version 3.
pub(crate) const DROP_CELLS_FORMAT_VERSION: u16 = 4;
// magic, format, reserved, stable identity, original version, count, payload digest.
const DIGEST_OFFSET: usize = 8 + 2 + 2 + 32 + 16 + 4;
pub(crate) const HEADER_LEN: usize = DIGEST_OFFSET + 32;
pub(crate) const MEMBER_HEADER_LEN: usize = 4 + 4;
const MIN_MEMBER_LEN: usize = MEMBER_HEADER_LEN + 2;
const NATIVE_FRAME_MAX_EXCLUSIVE: usize = 1 << 30;
const NATIVE_CRC_HEADER_LEN: usize = 8;
const NATIVE_BATCH_MAX_EXCLUSIVE: u32 = 1_000_000;
const MEMBER_CHECKSUM_DOMAIN: &[u8] = b"tidehunter-batch-member-v3\0";
/// Explicit admission bounds for the experimental record API only.
/// Native `Db` and its accepted batch sizes are unaffected.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BatchLimits {
    pub max_operations: u32,
    /// Complete encoded payload bytes, excluding the outer native CRC header.
    pub max_encoded_bytes: usize,
}

impl BatchLimits {
    pub fn validate(self) -> Result<(), CodecError> {
        if self.max_operations == 0
            || self.max_operations >= NATIVE_BATCH_MAX_EXCLUSIVE
            || self.max_encoded_bytes < HEADER_LEN + MIN_MEMBER_LEN
            || self.max_encoded_bytes >= NATIVE_FRAME_MAX_EXCLUSIVE - NATIVE_CRC_HEADER_LEN
        {
            return Err(CodecError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    Put {
        keyspace: KeySpace,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        keyspace: KeySpace,
        key: Bytes,
    },
    /// Clear every key of the inclusive reduced-cell range at this version.
    /// Valid only as the sole member of a batch; it has no key or value.
    DropCells {
        keyspace: KeySpace,
        first_cell: CellId,
        last_cell: CellId,
    },
}

impl Mutation {
    pub fn keyspace(&self) -> KeySpace {
        match self {
            Self::Put { keyspace, .. }
            | Self::Delete { keyspace, .. }
            | Self::DropCells { keyspace, .. } => *keyspace,
        }
    }

    /// The key of a point member; `None` for `DropCells`.
    pub fn point_key(&self) -> Option<&Bytes> {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => Some(key),
            Self::DropCells { .. } => None,
        }
    }

    fn native_entry(&self) -> WalEntry {
        match self {
            Self::Put {
                keyspace,
                key,
                value,
            } => WalEntry::Record(*keyspace, key.clone(), value.clone(), false),
            Self::Delete { keyspace, key } => WalEntry::Remove(*keyspace, key.clone()),
            Self::DropCells {
                keyspace,
                first_cell,
                last_cell,
            } => WalEntry::DropCells(*keyspace, first_cell.clone(), last_cell.clone()),
        }
    }
}

/// Framing of the one `DropCells` member: offset and length inside the
/// payload and its domain-separated CRC. Not a value record reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RangeMemberDescriptor {
    pub offset: u32,
    pub len: u32,
    pub checksum: MemberChecksum,
}

/// Point batches carry one record descriptor per member; a `DropCells`
/// batch carries one member and zero record descriptors.
#[derive(Clone, Debug)]
enum BatchDescriptors {
    Points(Vec<RecordDescriptor>),
    DropCells(
        // The validated framing of the one member. Readers need only the
        // decoded cells; the codec tests check the framing.
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "member framing is checked by codec tests only")
        )]
        RangeMemberDescriptor,
    ),
}

/// A validated, immutable canonical record. Construction does not append or
/// establish authority: storage must still authenticate, fence and admit it.
#[derive(Clone, Debug)]
pub struct CompleteBatch {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    mutations: Vec<Mutation>,
    descriptors: BatchDescriptors,
    encoded: Bytes,
    // All constructors own encoded bytes before validation. `new` additionally
    // needs to detach caller mutation owners before retaining a queued job.
    canonical_owned: bool,
}

// Conservative charge for minibytes' separately allocated Arc<Vec<u8>> owner.
pub(crate) const ROUTING_HASH_OWNER_BYTES: usize = 64;

/// An immutable borrowed window member, already validated against its shape and
/// limits. Private fields prevent swapping input after preflight.
pub(crate) struct ValidatedMutations<'a> {
    mutations: &'a [Mutation],
    encoded_len: usize,
    routing_heap_bound: usize,
    /// Header format the batch encodes with: `FORMAT_VERSION`, or
    /// `RELOCATED_FORMAT_VERSION` for a batch of relocated copies.
    format: u16,
}

impl<'a> ValidatedMutations<'a> {
    pub(crate) fn mutations(&self) -> &'a [Mutation] {
        self.mutations
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.encoded_len
    }
    pub(crate) fn routing_heap_bound(&self) -> usize {
        self.routing_heap_bound
    }

    pub(crate) fn encode(
        &self,
        id: BatchId,
        version: BatchVersion,
    ) -> Result<CompleteBatch, CodecError> {
        CompleteBatch::encode_validated(
            id,
            version,
            self.mutations.to_vec(),
            self.encoded_len,
            self.format,
        )
    }
}

impl CompleteBatch {
    /// Revalidates an immutable, already integrity-checked batch against another
    /// storage schema and admission policy without hashing its values again.
    /// This is crate-private: disk/network bytes must still pass `decode` first.
    pub(crate) fn validate_for(
        &self,
        shape: &KeyShape,
        limits: BatchLimits,
    ) -> Result<(), CodecError> {
        limits.validate()?;
        check_count(self.mutations.len(), limits)?;
        check_drop_cells_singleton(&self.mutations)?;
        if self.encoded.len() > limits.max_encoded_bytes {
            return Err(CodecError::TooLarge);
        }
        for mutation in &self.mutations {
            validate_mutation(shape, mutation)?;
        }
        Ok(())
    }

    // The validated borrowed token below keeps input immutable through encoding.
    /// Validate and size caller-owned mutations without allocating or hashing.
    /// Used to reserve bounded parallel preparation before workers build records.
    pub(crate) fn encoded_len_for_mutations(
        shape: &KeyShape,
        mutations: &[Mutation],
        limits: BatchLimits,
    ) -> Result<usize, CodecError> {
        Ok(Self::validated_mutations(shape, mutations, limits)?.encoded_len())
    }

    pub(crate) fn validated_mutations<'a>(
        shape: &KeyShape,
        mutations: &'a [Mutation],
        limits: BatchLimits,
    ) -> Result<ValidatedMutations<'a>, CodecError> {
        limits.validate()?;
        check_count(mutations.len(), limits)?;
        check_drop_cells_singleton(mutations)?;
        let mut size = HEADER_LEN;
        let mut routing_heap_bound = 0usize;
        // One walk validates encoding and bounds retained routing allocations.
        // Cell prefixes fit inline through 16 reduced bytes. Hash keys also
        // own a separate byte buffer and Arc<Vec<u8>>, even for empty inputs.
        for mutation in mutations {
            validate_mutation(shape, mutation)?;
            size = size
                .checked_add(MEMBER_HEADER_LEN)
                .and_then(|n| n.checked_add(mutation.native_entry().len()))
                .filter(|size| *size <= limits.max_encoded_bytes)
                .ok_or(CodecError::TooLarge)?;
            let key = match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key, .. } => key,
                Mutation::DropCells {
                    first_cell,
                    last_cell,
                    ..
                } => {
                    // The slot copy and the routing copy of each spilled prefix.
                    routing_heap_bound = drop_cells::spilled_bytes(first_cell, last_cell)
                        .checked_mul(2)
                        .and_then(|bytes| routing_heap_bound.checked_add(bytes))
                        .ok_or(CodecError::TooLarge)?;
                    continue;
                }
            };
            let descriptor = shape.ks(mutation.keyspace());
            let indexing = descriptor.key_indexing();
            let reduced_len = indexing.index_key_size().unwrap_or(key.len());
            if reduced_len > 16 {
                routing_heap_bound = reduced_len
                    .checked_mul(2)
                    .and_then(|bytes| routing_heap_bound.checked_add(bytes))
                    .ok_or(CodecError::TooLarge)?;
            }
            if matches!(indexing, KeyIndexing::Hash) {
                routing_heap_bound = routing_heap_bound
                    .checked_add(reduced_len)
                    .and_then(|bytes| bytes.checked_add(ROUTING_HASH_OWNER_BYTES))
                    .ok_or(CodecError::TooLarge)?;
            }
        }
        Ok(ValidatedMutations {
            mutations,
            encoded_len: size,
            routing_heap_bound,
            format: FORMAT_VERSION,
        })
    }

    pub fn new(
        shape: &KeyShape,
        id: BatchId,
        version: BatchVersion,
        mutations: Vec<Mutation>,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        let size = Self::encoded_len_for_mutations(shape, &mutations, limits)?;
        Self::encode_validated(id, version, mutations, size, FORMAT_VERSION)
    }

    fn encode_validated(
        id: BatchId,
        version: BatchVersion,
        mutations: Vec<Mutation>,
        size: usize,
        format: u16,
    ) -> Result<Self, CodecError> {
        let range = match mutations.as_slice() {
            [
                Mutation::DropCells {
                    keyspace,
                    first_cell,
                    last_cell,
                },
            ] => Some((*keyspace, first_cell, last_cell)),
            _ => None,
        };
        let mut encoded = BytesMut::with_capacity(size);
        encoded.put_slice(MAGIC);
        encoded.put_u16(if range.is_some() {
            DROP_CELLS_FORMAT_VERSION
        } else {
            format
        });
        encoded.put_u16(0); // reserved, must remain zero for canonical encoding
        encoded.put_slice(&id.database);
        encoded.put_slice(&id.operation);
        encoded.put_u64(version.epoch);
        encoded.put_u64(version.sequence);
        encoded.put_u32(mutations.len() as u32);
        encoded.put_slice(&[0; 32]);
        if let Some((keyspace, first_cell, last_cell)) = range {
            let descriptor = drop_cells::encode_member(
                id,
                version,
                keyspace,
                first_cell,
                last_cell,
                &mut encoded,
            )?;
            let digest = record_digest(&encoded);
            encoded[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
            return Ok(Self {
                id,
                version,
                digest,
                mutations,
                descriptors: BatchDescriptors::DropCells(descriptor),
                encoded: encoded.freeze().into(),
                canonical_owned: false,
            });
        }
        let mut descriptors = Vec::with_capacity(mutations.len());
        for (operation_index, mutation) in mutations.iter().enumerate() {
            let entry = mutation.native_entry();
            let offset = encoded.len();
            encoded.put_u32(entry.len() as u32);
            encoded.put_u32(0);
            entry.write_into_bytes(&mut encoded);
            descriptors.push(
                describe_mutation(operation_index as u32, offset, mutation)
                    .ok_or(CodecError::DropCellsNotSingleton)?,
            );
        }
        for descriptor in &mut descriptors {
            let checksum =
                member_checksum(id, version, descriptor, native_member(&encoded, descriptor));
            descriptor.checksum = checksum;
            let offset = descriptor.offset as usize;
            encoded[offset + 4..offset + MEMBER_HEADER_LEN]
                .copy_from_slice(&checksum.0.to_be_bytes());
        }
        let digest = record_digest(&encoded);
        encoded[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
        Ok(Self {
            id,
            version,
            digest,
            mutations,
            descriptors: BatchDescriptors::Points(descriptors),
            encoded: encoded.freeze().into(),
            canonical_owned: false,
        })
    }

    /// Fallible, bounded parser for arbitrary disk/network backing. Detaches
    /// before integrity validation: a read-only mmap can still be modified by
    /// another file writer. Mutations share the resulting verified owned buffer.
    /// Use `decode_owned` to transfer a network Vec without that boundary copy.
    pub fn decode(
        shape: &KeyShape,
        encoded: Bytes,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        check_encoded_len(encoded.len(), limits)?;
        let owned = encoded.as_ref().to_vec().into_boxed_slice();
        Self::decode_canonical(shape, owned.into(), limits)
    }

    /// Transfer an exclusively owned input buffer before parsing/hashing. Excess
    /// Vec capacity is released, so a short wire record cannot pin an oversized
    /// allocation. No second payload copy or cryptographic pass is required.
    pub fn decode_owned(
        shape: &KeyShape,
        encoded: Vec<u8>,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        check_encoded_len(encoded.len(), limits)?;
        Self::decode_canonical(shape, encoded.into_boxed_slice().into(), limits)
    }

    // Only owned immutable payloads enter this parser. It does not call the
    // native trusted-local parser, whose unchecked input can panic.
    fn decode_canonical(
        shape: &KeyShape,
        encoded: Bytes,
        limits: BatchLimits,
    ) -> Result<Self, CodecError> {
        limits.validate()?;
        if encoded.len() > limits.max_encoded_bytes {
            return Err(CodecError::TooLarge);
        }
        if encoded.len() < HEADER_LEN {
            return Err(CodecError::Truncated);
        }
        let mut cursor = Cursor::new(&encoded);
        if cursor.take(8)? != MAGIC {
            return Err(CodecError::InvalidFormat);
        }
        let format = cursor.u16()?;
        if !relocated::known_format(format) || cursor.u16()? != 0 {
            return Err(CodecError::InvalidFormat);
        }
        let id = BatchId {
            database: cursor.take_array::<16>()?,
            operation: cursor.take_array::<16>()?,
        };
        let version = BatchVersion {
            epoch: cursor.u64()?,
            sequence: cursor.u64()?,
        };
        let count = cursor.u32()? as usize;
        check_count(count, limits)?;
        // Every member needs bounds, checksum and at least a native tag/keyspace.
        // Check this before allocating a count-sized vector supplied by a peer.
        if count > (encoded.len() - HEADER_LEN) / MIN_MEMBER_LEN {
            return Err(CodecError::Truncated);
        }
        let digest = PayloadDigest(cursor.take_array::<32>()?);
        if digest != record_digest(&encoded) {
            return Err(CodecError::DigestMismatch);
        }
        if format == DROP_CELLS_FORMAT_VERSION {
            if count != 1 {
                return Err(CodecError::DropCellsNotSingleton);
            }
            let offset = u32::try_from(cursor.position).map_err(|_| CodecError::TooLarge)?;
            let len = cursor.u32()? as usize;
            let checksum = MemberChecksum(cursor.u32()?);
            let native = cursor.take(len)?;
            if cursor.position != encoded.len() {
                return Err(CodecError::TrailingBytes);
            }
            // Version 4 accepts only tag 6; version 3 rejects it below.
            let mutation = drop_cells::decode_native(shape, native)?;
            let descriptor = RangeMemberDescriptor {
                offset,
                len: u32::try_from(MEMBER_HEADER_LEN + len).map_err(|_| CodecError::TooLarge)?,
                checksum,
            };
            if checksum
                != drop_cells::member_checksum(
                    id,
                    version,
                    &descriptor,
                    mutation.keyspace(),
                    native,
                )
            {
                return Err(CodecError::ChecksumMismatch);
            }
            return Ok(Self {
                id,
                version,
                digest,
                mutations: vec![mutation],
                descriptors: BatchDescriptors::DropCells(descriptor),
                encoded,
                canonical_owned: true,
            });
        }
        let mut mutations = Vec::with_capacity(count);
        let mut descriptors = Vec::with_capacity(count);
        for operation_index in 0..count {
            let offset = cursor.position;
            let len = cursor.u32()? as usize;
            let checksum = MemberChecksum(cursor.u32()?);
            let start = cursor.position;
            cursor.take(len)?;
            let mutation = decode_native(shape, &encoded.slice(start..start + len))?;
            let mut descriptor = describe_mutation(operation_index as u32, offset, &mutation)
                .ok_or(CodecError::InvalidMutation)?;
            descriptor.checksum = checksum;
            mutations.push(mutation);
            descriptors.push(descriptor);
        }
        relocated::check_members(format, &mutations)?;
        if cursor.position != encoded.len() {
            return Err(CodecError::TrailingBytes);
        }
        for descriptor in &descriptors {
            if descriptor.checksum
                != member_checksum(id, version, descriptor, native_member(&encoded, descriptor))
            {
                return Err(CodecError::ChecksumMismatch);
            }
        }
        Ok(Self {
            id,
            version,
            digest,
            mutations,
            descriptors: BatchDescriptors::Points(descriptors),
            encoded,
            canonical_owned: true,
        })
    }

    /// Keep only a bounded canonical payload plus exact-length metadata vectors.
    /// Construction/decode already checked integrity and descriptors, so rebasing
    /// slices needs neither native re-parsing nor another cryptographic pass.
    /// This establishes memory ownership, not authority or storage-schema validity.
    pub(crate) fn into_canonical_owned(mut self) -> Self {
        if self.canonical_owned {
            return self;
        }
        // A DropCells member owns its cell identities; nothing borrows input.
        let BatchDescriptors::Points(descriptors) = self.descriptors else {
            self.canonical_owned = true;
            return self;
        };
        let mutations: Vec<_> = descriptors
            .iter()
            .map(|descriptor| {
                let value_start = descriptor.offset as usize + descriptor.value_offset as usize;
                let key_start = value_start - descriptor.key_len as usize;
                let key = self.encoded.slice(key_start..value_start);
                let keyspace = KeySpace(descriptor.keyspace);
                match descriptor.kind {
                    RecordKind::Put => Mutation::Put {
                        keyspace,
                        key,
                        value: self
                            .encoded
                            .slice(value_start..value_start + descriptor.value_len as usize),
                    },
                    RecordKind::Delete => Mutation::Delete { keyspace, key },
                }
            })
            .collect();
        self.mutations = mutations.into_boxed_slice().into_vec();
        self.descriptors = BatchDescriptors::Points(descriptors.into_boxed_slice().into_vec());
        self.canonical_owned = true;
        self
    }

    pub fn id(&self) -> BatchId {
        self.id
    }
    pub fn version(&self) -> BatchVersion {
        self.version
    }
    pub fn digest(&self) -> PayloadDigest {
        self.digest
    }
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }
    pub fn encoded(&self) -> &Bytes {
        &self.encoded
    }

    /// Point record descriptors; empty for a `DropCells` batch, which has one
    /// member but no value record.
    pub fn record_descriptors(&self) -> &[RecordDescriptor] {
        match &self.descriptors {
            BatchDescriptors::Points(descriptors) => descriptors,
            BatchDescriptors::DropCells(_) => &[],
        }
    }

    /// Framing of the `DropCells` member, for codec tests.
    #[cfg(test)]
    pub(crate) fn range_member(&self) -> Option<RangeMemberDescriptor> {
        match &self.descriptors {
            BatchDescriptors::DropCells(member) => Some(*member),
            BatchDescriptors::Points(_) => None,
        }
    }

    /// The validated cell range of a singleton `DropCells` batch.
    pub(crate) fn drop_cells(&self) -> Option<(KeySpace, &CellId, &CellId)> {
        match self.mutations.as_slice() {
            [
                Mutation::DropCells {
                    keyspace,
                    first_cell,
                    last_cell,
                },
            ] => Some((*keyspace, first_cell, last_cell)),
            _ => None,
        }
    }

    /// Create membership metadata only after the complete batch was validated.
    /// The caller must establish storage ownership/durability separately.
    pub fn record_reference(
        &self,
        address: PhysicalValueAddress,
        operation_index: usize,
    ) -> Result<ValueRecordReference, CodecError> {
        let BatchDescriptors::Points(descriptors) = &self.descriptors else {
            return Err(CodecError::DropCellsReference);
        };
        if address.len as usize != self.encoded.len() + NATIVE_CRC_HEADER_LEN {
            return Err(CodecError::InvalidReference);
        }
        let descriptor = *descriptors
            .get(operation_index)
            .ok_or(CodecError::InvalidReference)?;
        let reference = ValueRecordReference {
            batch_id: self.id,
            batch_version: self.version,
            batch_digest: self.digest,
            batch_address: address,
            descriptor,
        };
        validate_reference_bounds(&reference)?;
        Ok(reference)
    }

    /// Verify one member using trusted metadata established by append/replay.
    ///
    /// Storage must first match the complete reference against its authoritative
    /// descriptor. The returned key/value share `encoded_member`; this path never
    /// reads, hashes or copies sibling values. It does not establish authority.
    pub fn decode_record(
        shape: &KeyShape,
        reference: &ValueRecordReference,
        encoded_member: Bytes,
    ) -> Result<Mutation, CodecError> {
        decode_record_mutation(shape, reference, &encoded_member)
    }

    /// Verify each independently addressed member. Every member receives the
    /// same checks as `decode_record`; results retain order and duplicates.
    /// No partial result escapes on any failure.
    ///
    /// The caller must authenticate membership exactly as for `decode_record`.
    /// Verification borrows input buffers; decoded keys and values retain them.
    /// No sibling bytes are read or copied.
    pub fn decode_records(
        shape: &KeyShape,
        records: &[(ValueRecordReference, Bytes)],
    ) -> Result<Vec<Mutation>, CodecError> {
        let mut mutations = Vec::with_capacity(records.len());
        for (reference, bytes) in records {
            mutations.push(decode_record_mutation(shape, reference, bytes)?);
        }
        Ok(mutations)
    }

    pub fn mutation_version(&self, operation_index: usize) -> Option<MutationVersion> {
        (operation_index < self.mutations.len()).then_some(MutationVersion {
            batch: self.version,
            operation_index: operation_index as u32,
        })
    }
}

impl IntoBytesFixed for CompleteBatch {
    fn len(&self) -> usize {
        self.encoded.len()
    }
    fn write_into_bytes(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.encoded);
    }
}

/// `None` for `DropCells`, which has no record descriptor.
fn describe_mutation(
    operation_index: u32,
    offset: usize,
    mutation: &Mutation,
) -> Option<RecordDescriptor> {
    let (kind, key, value_len, native_header_len) = match mutation {
        Mutation::Put { key, value, .. } => (RecordKind::Put, key, value.len(), 4),
        Mutation::Delete { key, .. } => (RecordKind::Delete, key, 0, 2),
        Mutation::DropCells { .. } => return None,
    };
    // Callers validated total frame size (< 1 GiB) and the native key limit.
    let value_offset = MEMBER_HEADER_LEN + native_header_len + key.len();
    Some(RecordDescriptor {
        operation_index,
        offset: offset as u32,
        len: (value_offset + value_len) as u32,
        keyspace: mutation.keyspace().0,
        kind,
        key_len: key.len() as u32,
        value_offset: value_offset as u32,
        value_len: value_len as u32,
        checksum: MemberChecksum(0),
    })
}

/// Validate untrusted address arithmetic before any storage I/O or allocation.
/// Membership, generation and lifetime still require the owning storage node.
pub fn validate_reference_bounds(reference: &ValueRecordReference) -> Result<(), CodecError> {
    let descriptor = &reference.descriptor;
    let batch_len = reference.batch_address.len as usize;
    let payload_len = batch_len
        .checked_sub(NATIVE_CRC_HEADER_LEN)
        .ok_or(CodecError::InvalidReference)?;
    if !(HEADER_LEN + MIN_MEMBER_LEN..NATIVE_FRAME_MAX_EXCLUSIVE - NATIVE_CRC_HEADER_LEN)
        .contains(&payload_len)
        || descriptor.operation_index >= NATIVE_BATCH_MAX_EXCLUSIVE
        || descriptor.offset < HEADER_LEN as u32
        || descriptor.len < MIN_MEMBER_LEN as u32
        || descriptor.key_len as usize > MAX_KEY_LEN
        || (descriptor.offset as usize)
            .checked_add(descriptor.len as usize)
            .is_none_or(|end| end > payload_len)
        || reference
            .batch_address
            .offset
            .checked_add(batch_len as u64)
            .is_none()
    {
        return Err(CodecError::InvalidReference);
    }
    let native_header_len = match descriptor.kind {
        RecordKind::Put => 4,
        RecordKind::Delete => 2,
    };
    let expected_value_offset = (MEMBER_HEADER_LEN as u32)
        .checked_add(native_header_len)
        .and_then(|len| len.checked_add(descriptor.key_len));
    if expected_value_offset != Some(descriptor.value_offset)
        || descriptor.value_offset.checked_add(descriptor.value_len) != Some(descriptor.len)
        || (descriptor.kind == RecordKind::Delete && descriptor.value_len != 0)
    {
        return Err(CodecError::InvalidReference);
    }
    Ok(())
}

fn decode_native(shape: &KeyShape, encoded: &Bytes) -> Result<Mutation, CodecError> {
    let mut native = Cursor::new(encoded);
    let tag = native.u8()?;
    let keyspace = KeySpace(native.u8()?);
    let mutation = match tag {
        1 => {
            let key_len = native.u16()? as usize;
            native.take(key_len)?;
            Mutation::Put {
                keyspace,
                key: encoded.slice(4..4 + key_len),
                value: encoded.slice(4 + key_len..),
            }
        }
        3 => Mutation::Delete {
            keyspace,
            key: encoded.slice(2..),
        },
        // No nested batches, relocations, control records or index entries.
        _ => return Err(CodecError::InvalidMutation),
    };
    validate_mutation(shape, &mutation)?;
    Ok(mutation)
}

fn checked_record_native<'a>(
    reference: &ValueRecordReference,
    encoded_member: &'a [u8],
) -> Result<&'a [u8], CodecError> {
    validate_reference_bounds(reference)?;
    if encoded_member.len() != reference.descriptor.len as usize {
        return Err(CodecError::InvalidReference);
    }
    let mut cursor = Cursor::new(encoded_member);
    let native_len = cursor.u32()? as usize;
    let checksum = MemberChecksum(cursor.u32()?);
    let native = cursor.take(native_len)?;
    if cursor.position != encoded_member.len() {
        return Err(CodecError::TrailingBytes);
    }
    if checksum != reference.descriptor.checksum {
        return Err(CodecError::ChecksumMismatch);
    }
    Ok(native)
}

fn decode_record_mutation(
    shape: &KeyShape,
    reference: &ValueRecordReference,
    encoded_member: &Bytes,
) -> Result<Mutation, CodecError> {
    let native = checked_record_native(reference, encoded_member)?;
    if reference.descriptor.checksum
        != member_checksum(
            reference.batch_id,
            reference.batch_version,
            &reference.descriptor,
            native,
        )
    {
        return Err(CodecError::ChecksumMismatch);
    }
    let descriptor = &reference.descriptor;
    let mutation = decode_native(shape, &encoded_member.slice(MEMBER_HEADER_LEN..))?;
    let mut expected = describe_mutation(
        descriptor.operation_index,
        descriptor.offset as usize,
        &mutation,
    )
    .ok_or(CodecError::DropCellsReference)?;
    expected.checksum = descriptor.checksum;
    if *descriptor != expected {
        return Err(CodecError::InvalidReference);
    }
    Ok(mutation)
}

// Only private, validated descriptors reach this helper. Network references use
// checked_record_native so they cannot index a buffer using unchecked bounds.
fn native_member<'a>(encoded: &'a [u8], descriptor: &RecordDescriptor) -> &'a [u8] {
    &encoded[descriptor.offset as usize + MEMBER_HEADER_LEN
        ..descriptor.offset as usize + descriptor.len as usize]
}

/// BLAKE2b-256 complete-batch hasher used for root and retry identity.
struct PayloadHasher(blake2b_simd::State);

impl PayloadHasher {
    fn new() -> Self {
        Self(blake2b_simd::Params::new().hash_length(32).to_state())
    }

    fn update(&mut self, bytes: impl AsRef<[u8]>) {
        self.0.update(bytes.as_ref());
    }

    #[allow(
        clippy::expect_used,
        reason = "blake2b_simd is configured for a 32-byte digest, so the conversion cannot fail"
    )]
    fn finalize(self) -> [u8; 32] {
        self.0
            .finalize()
            .as_bytes()
            .try_into()
            .expect("configured 32-byte BLAKE2b output")
    }
}

fn member_checksum(
    id: BatchId,
    version: BatchVersion,
    descriptor: &RecordDescriptor,
    native: &[u8],
) -> MemberChecksum {
    let mut crc = crc32fast::Hasher::new();
    crc.update(MEMBER_CHECKSUM_DOMAIN);
    crc.update(&id.database);
    crc.update(&id.operation);
    crc.update(&version.epoch.to_be_bytes());
    crc.update(&version.sequence.to_be_bytes());
    crc.update(&descriptor.operation_index.to_be_bytes());
    crc.update(&descriptor.offset.to_be_bytes());
    crc.update(&descriptor.len.to_be_bytes());
    crc.update(&[descriptor.keyspace]);
    crc.update(&[match descriptor.kind {
        RecordKind::Put => 1,
        RecordKind::Delete => 3,
    }]);
    crc.update(&descriptor.key_len.to_be_bytes());
    crc.update(&descriptor.value_offset.to_be_bytes());
    crc.update(&descriptor.value_len.to_be_bytes());
    crc.update(native);
    MemberChecksum(crc.finalize())
}

fn record_digest(encoded: &[u8]) -> PayloadDigest {
    let mut hash = PayloadHasher::new();
    hash.update(b"tidehunter-complete-batch-v2\0");
    hash.update(&encoded[..DIGEST_OFFSET]);
    hash.update(&encoded[HEADER_LEN..]);
    PayloadDigest(hash.finalize())
}

fn check_count(count: usize, limits: BatchLimits) -> Result<(), CodecError> {
    if count == 0 || count > limits.max_operations as usize {
        Err(CodecError::InvalidOperationCount)
    } else {
        Ok(())
    }
}

fn check_encoded_len(len: usize, limits: BatchLimits) -> Result<(), CodecError> {
    limits.validate()?;
    if len > limits.max_encoded_bytes {
        Err(CodecError::TooLarge)
    } else if len < HEADER_LEN {
        Err(CodecError::Truncated)
    } else {
        Ok(())
    }
}

/// A `DropCells` member is valid only as the sole member of a batch.
fn check_drop_cells_singleton(mutations: &[Mutation]) -> Result<(), CodecError> {
    if mutations.len() > 1
        && mutations
            .iter()
            .any(|mutation| matches!(mutation, Mutation::DropCells { .. }))
    {
        return Err(CodecError::DropCellsNotSingleton);
    }
    Ok(())
}

fn validate_mutation(shape: &KeyShape, mutation: &Mutation) -> Result<(), CodecError> {
    let key = match mutation {
        Mutation::Put { key, .. } | Mutation::Delete { key, .. } => key,
        Mutation::DropCells {
            keyspace,
            first_cell,
            last_cell,
        } => return drop_cells::validate(shape, *keyspace, first_cell, last_cell),
    };
    let keyspace = mutation.keyspace();
    let Some(descriptor) = shape
        .iter_ks()
        .find(|descriptor| descriptor.id() == keyspace)
    else {
        return Err(CodecError::UnknownKeySpace(keyspace.0));
    };
    if descriptor.destroyed() {
        return Err(CodecError::DestroyedKeySpace(keyspace.0));
    }
    let len = key.len();
    let valid = len <= MAX_KEY_LEN
        && match descriptor.key_indexing() {
            KeyIndexing::Fixed(expected) | KeyIndexing::Reduction(expected, _) => len == *expected,
            KeyIndexing::Hash | KeyIndexing::VariableLength => true,
        };
    if !valid {
        return Err(CodecError::InvalidKeyLength {
            keyspace: keyspace.0,
            actual: len,
        });
    }
    // Native cell routing slices reduced keys for prefix cells. Validate this
    // before an authority frame can be emitted, rather than discovering an
    // unpublishable mutation after its WAL batch has become irrevocable.
    let reduced_len = match descriptor.key_indexing() {
        KeyIndexing::Reduction(_, range) => {
            if range.start > range.end || range.end > len {
                return Err(CodecError::InvalidRouting {
                    keyspace: keyspace.0,
                });
            }
            range.end - range.start
        }
        // Includes native HASH_SIZE through index_key_size, not the full key's
        // length: an empty original key can still have a valid hashed prefix.
        _ => descriptor.index_key_size().unwrap_or(len),
    };
    if let crate::key_shape::KeyType::Uniform(config) = descriptor.key_type() {
        // Native cell_id narrows this product to u32 before downscale_u32.
        // Reject zero, arithmetic overflow and narrowing overflow before any
        // complete authoritative frame can make the mutation irrevocable.
        let valid_cells = config
            .cells_per_mutex()
            .checked_mul(descriptor.num_mutexes())
            .and_then(|cells| u32::try_from(cells).ok())
            .is_some_and(|cells| cells != 0);
        if !valid_cells {
            return Err(CodecError::InvalidRouting {
                keyspace: keyspace.0,
            });
        }
    }
    if let crate::cell::CellId::Bytes(prefix) = descriptor.first_cell()
        && reduced_len < prefix.len()
    {
        return Err(CodecError::InvalidRouting {
            keyspace: keyspace.0,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    InvalidLimits,
    InvalidOperationCount,
    TooLarge,
    Truncated,
    InvalidFormat,
    InvalidMutation,
    InvalidReference,
    /// Whole-batch BLAKE2b payload digest mismatch.
    DigestMismatch,
    /// Per-member canonical CRC-32 mismatch.
    ChecksumMismatch,
    TrailingBytes,
    UnknownKeySpace(u8),
    DestroyedKeySpace(u8),
    InvalidKeyLength {
        keyspace: u8,
        actual: usize,
    },
    InvalidRouting {
        keyspace: u8,
    },
    /// A `DropCells` member shares its batch with another member.
    DropCellsNotSingleton,
    /// Malformed, noncanonical or reversed `DropCells` cells for the shape.
    InvalidDropCells {
        keyspace: u8,
    },
    /// A value reference was requested for the `DropCells` member.
    DropCellsReference,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DropCellsNotSingleton => {
                f.write_str("DropCells must be the only member in a batch")
            }
            Self::InvalidDropCells { keyspace } => {
                write!(f, "invalid DropCells cell encoding for keyspace {keyspace}")
            }
            Self::DropCellsReference => f.write_str("DropCells does not have a value reference"),
            _ => write!(f, "invalid complete batch: {self:?}"),
        }
    }
}
impl std::error::Error for CodecError {}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(CodecError::Truncated)?;
        let result = self
            .bytes
            .get(self.position..end)
            .ok_or(CodecError::Truncated)?;
        self.position = end;
        Ok(result)
    }
    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        // `take` returns exactly `N` bytes or errors; the conversion only
        // narrows the type and cannot fail.
        self.take(N)?.try_into().map_err(|_| CodecError::Truncated)
    }
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(self.take_array::<2>()?))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.take_array::<4>()?))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(self.take_array::<8>()?))
    }
}

#[cfg(test)]
mod tests;
