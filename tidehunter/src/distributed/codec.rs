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

use super::types::{
    BatchId, BatchVersion, MemberChecksum, MutationVersion, PayloadDigest, PhysicalValueAddress,
    RecordDescriptor, RecordKind, ValueRecordReference,
};
use crate::crc::IntoBytesFixed;
use crate::db::{MAX_KEY_LEN, WalEntry};
use crate::key_shape::{KeyIndexing, KeyShape, KeySpace};
use bytes::{BufMut, BytesMut};
use minibytes::Bytes;
use std::fmt;

const MAGIC: &[u8; 8] = b"THBATCH3";
pub const FORMAT_VERSION: u16 = 3;
// magic, format, reserved, stable identity, original version, count, payload digest.
const DIGEST_OFFSET: usize = 8 + 2 + 2 + 32 + 16 + 4;
pub(crate) const HEADER_LEN: usize = DIGEST_OFFSET + 32;
const MEMBER_HEADER_LEN: usize = 4 + 4;
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
}

impl Mutation {
    pub fn keyspace(&self) -> KeySpace {
        match self {
            Self::Put { keyspace, .. } | Self::Delete { keyspace, .. } => *keyspace,
        }
    }

    pub fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
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
        }
    }
}

/// A validated, immutable canonical record. Construction does not append or
/// establish authority: storage must still authenticate, fence and admit it.
#[derive(Clone, Debug)]
pub struct CompleteBatch {
    id: BatchId,
    version: BatchVersion,
    digest: PayloadDigest,
    mutations: Vec<Mutation>,
    descriptors: Vec<RecordDescriptor>,
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
}

impl ValidatedMutations<'_> {
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
        CompleteBatch::encode_validated(id, version, self.mutations.to_vec(), self.encoded_len)
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
            let descriptor = shape.ks(mutation.keyspace());
            let indexing = descriptor.key_indexing();
            let reduced_len = indexing.index_key_size().unwrap_or(mutation.key().len());
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
        Self::encode_validated(id, version, mutations, size)
    }

    fn encode_validated(
        id: BatchId,
        version: BatchVersion,
        mutations: Vec<Mutation>,
        size: usize,
    ) -> Result<Self, CodecError> {
        let mut encoded = BytesMut::with_capacity(size);
        encoded.put_slice(MAGIC);
        encoded.put_u16(FORMAT_VERSION);
        encoded.put_u16(0); // reserved, must remain zero for canonical encoding
        encoded.put_slice(&id.database);
        encoded.put_slice(&id.operation);
        encoded.put_u64(version.epoch);
        encoded.put_u64(version.sequence);
        encoded.put_u32(mutations.len() as u32);
        encoded.put_slice(&[0; 32]);
        let mut descriptors = Vec::with_capacity(mutations.len());
        for (operation_index, mutation) in mutations.iter().enumerate() {
            let entry = mutation.native_entry();
            let offset = encoded.len();
            encoded.put_u32(entry.len() as u32);
            encoded.put_u32(0);
            entry.write_into_bytes(&mut encoded);
            descriptors.push(describe_mutation(operation_index as u32, offset, mutation));
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
            descriptors,
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
        if cursor.u16()? != FORMAT_VERSION || cursor.u16()? != 0 {
            return Err(CodecError::InvalidFormat);
        }
        let id = BatchId {
            database: cursor.take(16)?.try_into().expect("checked 16-byte slice"),
            operation: cursor.take(16)?.try_into().expect("checked 16-byte slice"),
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
        let digest = PayloadDigest(cursor.take(32)?.try_into().expect("checked 32-byte slice"));
        if digest != record_digest(&encoded) {
            return Err(CodecError::DigestMismatch);
        }
        let mut mutations = Vec::with_capacity(count);
        let mut descriptors = Vec::with_capacity(count);
        for operation_index in 0..count {
            let offset = cursor.position;
            let len = cursor.u32()? as usize;
            let checksum = MemberChecksum(cursor.u32()?);
            let start = cursor.position;
            cursor.take(len)?;
            let mutation = decode_native(shape, encoded.slice(start..start + len))?;
            let mut descriptor = describe_mutation(operation_index as u32, offset, &mutation);
            descriptor.checksum = checksum;
            mutations.push(mutation);
            descriptors.push(descriptor);
        }
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
            descriptors,
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
        let mutations: Vec<_> = self
            .descriptors
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
        self.descriptors = self.descriptors.into_boxed_slice().into_vec();
        self.canonical_owned = true;
        self
    }

    /// Bound used after `into_canonical_owned`; excludes the enclosing batch
    /// struct and allocator bookkeeping. Caller-owned oversized buffers do not
    /// survive canonicalization. Private constructor backing permits 128 slack.
    pub(crate) fn canonical_heap_bytes(&self) -> Option<usize> {
        self.mutations
            .len()
            .checked_mul(std::mem::size_of::<Mutation>() + std::mem::size_of::<RecordDescriptor>())
            .and_then(|metadata| metadata.checked_add(self.encoded.len()))
            .and_then(|bytes| bytes.checked_add(128))
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

    pub fn record_descriptors(&self) -> &[RecordDescriptor] {
        &self.descriptors
    }

    /// Create membership metadata only after the complete batch was validated.
    /// The caller must establish storage ownership/durability separately.
    pub fn record_reference(
        &self,
        address: PhysicalValueAddress,
        operation_index: usize,
    ) -> Result<ValueRecordReference, CodecError> {
        if address.len as usize != self.encoded.len() + NATIVE_CRC_HEADER_LEN {
            return Err(CodecError::InvalidReference);
        }
        let descriptor = *self
            .descriptors
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
        let native = checked_record_native(reference, &encoded_member)?;
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
        decode_record_mutation(shape, reference, &encoded_member)
    }

    /// Verify each independently addressed member. Every member receives the
    /// same checks as `decode_record`; results retain order and duplicates.
    /// No partial result escapes on any failure.
    ///
    /// The caller must authenticate membership exactly as for `decode_record`.
    /// Values share their input buffers; no sibling bytes are read or copied.
    pub fn decode_records(
        shape: &KeyShape,
        records: &[(ValueRecordReference, Bytes)],
    ) -> Result<Vec<Mutation>, CodecError> {
        let mut mutations = Vec::with_capacity(records.len());
        for (reference, bytes) in records {
            mutations.push(Self::decode_record(shape, reference, bytes.clone())?);
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

fn describe_mutation(operation_index: u32, offset: usize, mutation: &Mutation) -> RecordDescriptor {
    let (kind, value_len, native_header_len) = match mutation {
        Mutation::Put { value, .. } => (RecordKind::Put, value.len(), 4),
        Mutation::Delete { .. } => (RecordKind::Delete, 0, 2),
    };
    // Callers validated total frame size (< 1 GiB) and the native key limit.
    let value_offset = MEMBER_HEADER_LEN + native_header_len + mutation.key().len();
    RecordDescriptor {
        operation_index,
        offset: offset as u32,
        len: (value_offset + value_len) as u32,
        keyspace: mutation.keyspace().0,
        kind,
        key_len: mutation.key().len() as u32,
        value_offset: value_offset as u32,
        value_len: value_len as u32,
        checksum: MemberChecksum(0),
    }
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

fn decode_native(shape: &KeyShape, encoded: Bytes) -> Result<Mutation, CodecError> {
    let mut native = Cursor::new(&encoded);
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
    let descriptor = &reference.descriptor;
    let mutation = decode_native(shape, encoded_member.slice(MEMBER_HEADER_LEN..))?;
    let mut expected = describe_mutation(
        descriptor.operation_index,
        descriptor.offset as usize,
        &mutation,
    );
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

fn validate_mutation(shape: &KeyShape, mutation: &Mutation) -> Result<(), CodecError> {
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
    let len = mutation.key().len();
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
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid complete batch: {self:?}")
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
    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, CodecError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().expect("checked slice"),
        ))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("checked slice"),
        ))
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("checked slice"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crc::CrcFrame;
    use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};

    fn shape() -> KeyShape {
        KeyShape::new_single(4, 1, KeyType::uniform(1))
    }

    fn limits() -> BatchLimits {
        BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 4096,
        }
    }

    fn identity() -> BatchId {
        BatchId {
            database: [1; 16],
            operation: [2; 16],
        }
    }

    fn version() -> BatchVersion {
        BatchVersion {
            epoch: 7,
            sequence: 11,
        }
    }

    fn put(value: &'static [u8]) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key1"[..].into(),
            value: value.into(),
        }
    }

    fn batch(mutations: Vec<Mutation>) -> CompleteBatch {
        CompleteBatch::new(&shape(), identity(), version(), mutations, limits()).unwrap()
    }

    #[test]
    fn validated_borrowed_layout_preserves_canonical_identity_and_untrusted_checks() {
        let mutations = vec![
            put(b"value"),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
            },
        ];
        let validated = CompleteBatch::validated_mutations(&shape(), &mutations, limits()).unwrap();
        let prepared = validated.encode(identity(), version()).unwrap();
        let ordinary = batch(mutations.clone());
        let mut legacy = prepared.encoded().to_vec();
        legacy[..8].copy_from_slice(b"THBATCH2");
        legacy[8..10].copy_from_slice(&2u16.to_be_bytes());
        assert_eq!(
            CompleteBatch::decode(&shape(), with_valid_digest(legacy), limits()).unwrap_err(),
            CodecError::InvalidFormat
        );
        assert_eq!(prepared.digest(), ordinary.digest());
        assert_eq!(validated.encoded_len(), prepared.encoded().len());
        let mut corrupt = prepared.encoded().to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(CompleteBatch::decode(&shape(), corrupt.into(), limits()).is_err());
        let invalid = [Mutation::Put {
            keyspace: KeySpace(0),
            key: b"short"[..].into(),
            value: b"v"[..].into(),
        }];
        assert!(CompleteBatch::validated_mutations(&shape(), &invalid, limits()).is_err());
    }

    fn with_valid_digest(mut bytes: Vec<u8>) -> Bytes {
        let digest = record_digest(&bytes);
        bytes[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
        bytes.into()
    }

    #[test]
    fn canonical_ownership_drops_oversized_input_owners_without_changing_records() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Tracked {
            bytes: Vec<u8>,
            dropped: Arc<AtomicBool>,
        }
        impl AsRef<[u8]> for Tracked {
            fn as_ref(&self) -> &[u8] {
                &self.bytes
            }
        }
        impl minibytes::BytesOwner for Tracked {}
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = Bytes::from_owner(Tracked {
            bytes: vec![b'x'; 16384],
            dropped: dropped.clone(),
        });
        let mut input = Vec::with_capacity(1024);
        input.push(Mutation::Put {
            keyspace: KeySpace(0),
            key: owner.slice(0..4),
            value: owner.slice(4..20),
        });
        input.push(Mutation::Delete {
            keyspace: KeySpace(0),
            key: owner.slice(0..4),
        });
        drop(owner);
        let original = batch(input);
        let encoded = original.encoded().clone();
        let digest = original.digest();
        assert!(!dropped.load(Ordering::SeqCst));
        let canonical = original.into_canonical_owned();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(canonical.encoded(), &encoded);
        assert_eq!(canonical.digest(), digest);
        assert_eq!(canonical.mutations.capacity(), canonical.mutations.len());
        assert_eq!(
            canonical.descriptors.capacity(),
            canonical.descriptors.len()
        );
        let decoded = CompleteBatch::decode(&shape(), encoded.clone(), limits()).unwrap();
        assert_eq!(canonical.mutations(), decoded.mutations());
        assert_eq!(canonical.record_descriptors(), decoded.record_descriptors());

        // Visible owner length can be small while retained Vec capacity is huge.
        // External bytes always detach, even when into_owned would keep them.
        let dropped = Arc::new(AtomicBool::new(false));
        let mut backing = Vec::with_capacity(1024 * 1024);
        backing.extend_from_slice(&encoded);
        let input = Bytes::from_owner(Tracked {
            bytes: backing,
            dropped: dropped.clone(),
        });
        let decoded = CompleteBatch::decode(&shape(), input, limits()).unwrap();
        let decoded = decoded.into_canonical_owned();
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(decoded.encoded(), canonical.encoded());
        assert_eq!(decoded.mutations(), canonical.mutations());
        // Reusing an already canonical batch doesn't duplicate its payload.
        let pointer = decoded.encoded().as_ptr();
        assert_eq!(decoded.into_canonical_owned().encoded().as_ptr(), pointer);
    }

    #[test]
    fn owned_decode_reuses_exact_buffer_and_arbitrary_decode_detaches_mmap() {
        use std::io::{Seek, SeekFrom, Write};
        let original = batch(vec![put(b"owned value")]);
        let encoded = original.encoded().as_ref().to_vec();
        assert_eq!(encoded.len(), encoded.capacity());
        let pointer = encoded.as_ptr();
        let decoded = CompleteBatch::decode_owned(&shape(), encoded, limits()).unwrap();
        assert_eq!(decoded.encoded().as_ptr(), pointer);
        assert_eq!(decoded.mutations(), original.mutations());
        let invalid = vec![0; limits().max_encoded_bytes + 1];
        assert!(matches!(
            CompleteBatch::decode_owned(&shape(), invalid, limits()),
            Err(CodecError::TooLarge)
        ));

        let temp = tempdir::TempDir::new("codec-owned-decode").unwrap();
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(temp.path().join("input"))
            .unwrap();
        file.write_all(original.encoded()).unwrap();
        file.sync_all().unwrap();
        // SAFETY: the test keeps the file allocated at its original length and
        // uses only a same-length write after decode; no mapping is truncated.
        let map = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
        let mapped = Bytes::from_owner(map);
        let decoded = CompleteBatch::decode(&shape(), mapped.clone(), limits()).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        assert_eq!(mapped[0], b'X');
        assert_eq!(decoded.encoded(), original.encoded());
        assert_eq!(decoded.mutations(), original.mutations());
        assert!(CompleteBatch::decode(&shape(), mapped, limits()).is_err());
    }

    #[test]
    fn payload_hasher_matches_blake2b256_across_update_boundaries() {
        use blake2::{Blake2b, Digest, digest::consts::U32};

        for len in [0, 1, 31, 32, 63, 64, 127, 128, 129, 255, 256, 1024, 8192] {
            let input: Vec<u8> = (0..len).map(|i| (i * 73 + 19) as u8).collect();
            let expected: [u8; 32] = Blake2b::<U32>::digest(&input).into();
            for chunk_size in [1, 3, 127, 128, 129, 4096] {
                let mut actual = PayloadHasher::new();
                for chunk in input.chunks(chunk_size) {
                    actual.update(chunk);
                }
                assert_eq!(actual.finalize(), expected, "len={len}, chunk={chunk_size}");
            }
        }
    }

    #[test]
    fn codec_v3_batches_preserve_the_complete_payload_digest() {
        use blake2::{Blake2b, Digest, digest::consts::U32};

        for len in [0, 1, 127, 128, 129, 1024, 8192] {
            for count in [1, 2, 3, 4, 5, 32] {
                let mutations: Vec<_> = (0..count)
                    .map(|operation| {
                        if operation % 5 == 3 {
                            Mutation::Delete {
                                keyspace: KeySpace(0),
                                key: (operation as u32).to_be_bytes().to_vec().into(),
                            }
                        } else {
                            Mutation::Put {
                                keyspace: KeySpace(0),
                                key: (operation as u32).to_be_bytes().to_vec().into(),
                                value: (0..len)
                                    .map(|i| (i * 73 + 19 + operation) as u8)
                                    .collect::<Vec<_>>()
                                    .into(),
                            }
                        }
                    })
                    .collect();
                let bounds = BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 512 * 1024,
                };
                let batch =
                    CompleteBatch::new(&shape(), identity(), version(), mutations, bounds).unwrap();

                let mut old_batch = Blake2b::<U32>::new();
                old_batch.update(b"tidehunter-complete-batch-v2\0");
                old_batch.update(&batch.encoded()[..DIGEST_OFFSET]);
                old_batch.update(&batch.encoded()[HEADER_LEN..]);
                let old_digest: [u8; 32] = old_batch.finalize().into();
                assert_eq!(batch.digest().0, old_digest);
                assert_eq!(
                    CompleteBatch::decode(&shape(), batch.encoded().clone(), bounds)
                        .unwrap()
                        .digest(),
                    batch.digest()
                );
            }
        }
    }

    #[test]
    fn invalid_native_routing_is_rejected_before_complete_authority_exists() {
        let original = batch(vec![put(b"value")]);
        let prefix_shape = KeyShape::new_single_config_indexing(
            KeyIndexing::VariableLength,
            1,
            KeyType::prefix_uniform(8, 0),
            KeySpaceConfig::new(),
        );
        for target in [
            prefix_shape,
            KeyShape::new_single_config_indexing(
                KeyIndexing::Reduction(4, 1..8),
                1,
                KeyType::uniform(1),
                KeySpaceConfig::new(),
            ),
            KeyShape::new_single_config_indexing(
                KeyIndexing::Reduction(4, std::ops::Range { start: 3, end: 2 }),
                1,
                KeyType::uniform(1),
                KeySpaceConfig::new(),
            ),
        ] {
            let expected = CodecError::InvalidRouting { keyspace: 0 };
            assert_eq!(
                CompleteBatch::new(
                    &target,
                    identity(),
                    version(),
                    original.mutations().to_vec(),
                    limits()
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(
                CompleteBatch::decode(&target, original.encoded().clone(), limits()).unwrap_err(),
                expected
            );
            assert_eq!(
                original.validate_for(&target, limits()).unwrap_err(),
                expected
            );
        }

        // Prefix routing uses the reduced hash length, not the original key's
        // length. Preserve native support for a short/empty key under hashing.
        let hashed = KeyShape::new_single_config_indexing(
            KeyIndexing::Hash,
            1,
            KeyType::prefix_uniform(8, 0),
            KeySpaceConfig::new(),
        );
        let empty_key = Mutation::Put {
            keyspace: KeySpace(0),
            key: Vec::<u8>::new().into(),
            value: b"value"[..].into(),
        };
        assert!(
            CompleteBatch::new(&hashed, identity(), version(), vec![empty_key], limits()).is_ok()
        );
    }

    #[test]
    fn uniform_cell_arithmetic_is_checked_before_authority() {
        let original = batch(vec![put(b"value")]);
        let overflow = KeyShape::new_single(4, 2, KeyType::uniform(1usize << (usize::BITS - 1)));
        // This largest power-of-two cell count representable in u32 remains a
        // valid routing configuration. No cell table is allocated by this test.
        let boundary = KeyShape::new_single(4, 1, KeyType::uniform(1usize << 31));
        let accepted = CompleteBatch::new(
            &boundary,
            identity(),
            version(),
            original.mutations().to_vec(),
            limits(),
        )
        .unwrap();
        let _ = boundary
            .ks(KeySpace(0))
            .cell_id(accepted.mutations()[0].key());

        // Deserialization can encounter a zero count even though the ordinary
        // KeyType constructor rejects it. It must fail the same admission gate.
        let zero_yaml = shape()
            .to_yaml()
            .unwrap()
            .replace("cells_per_mutex: 1", "cells_per_mutex: 0");
        let zero = KeyShape::from_yaml(&zero_yaml).unwrap();
        let mut invalid_shapes = vec![overflow, zero];
        #[cfg(target_pointer_width = "64")]
        invalid_shapes.push(KeyShape::new_single(4, 1, KeyType::uniform(1usize << 32)));
        for target in invalid_shapes {
            let expected = CodecError::InvalidRouting { keyspace: 0 };
            assert_eq!(
                CompleteBatch::new(
                    &target,
                    identity(),
                    version(),
                    original.mutations().to_vec(),
                    limits(),
                )
                .unwrap_err(),
                expected
            );
            assert_eq!(
                CompleteBatch::decode(&target, original.encoded().clone(), limits()).unwrap_err(),
                expected
            );
            assert_eq!(
                original.validate_for(&target, limits()).unwrap_err(),
                expected
            );
        }
    }

    #[test]
    fn native_frame_roundtrip_preserves_complete_batch_and_duplicate_key_order() {
        let original = batch(vec![
            put(b"first"),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
            },
            put(b"last"),
        ]);
        let framed = CrcFrame::new(&original);
        let payload = CrcFrame::read_from_slice(framed.as_ref(), 0).unwrap();
        let decoded = CompleteBatch::decode(&shape(), payload.to_vec().into(), limits()).unwrap();
        assert_eq!(original.mutations(), decoded.mutations());
        assert_eq!(original.encoded(), decoded.encoded());
        assert_eq!(original.digest(), decoded.digest());
        assert!(decoded.mutation_version(0) < decoded.mutation_version(2));
        assert_eq!(decoded.mutation_version(3), None);
        // The envelope retains native record/tombstone bytes, rather than a
        // second serialization with subtly different key/value behavior.
        let mut cursor = Cursor::new(&payload[HEADER_LEN..]);
        for mutation in original.mutations() {
            let len = cursor.u32().unwrap() as usize;
            cursor.take(4).unwrap();
            let native = WalEntry::from_bytes(cursor.take(len).unwrap().to_vec().into());
            match (native, mutation) {
                (
                    WalEntry::Record(ks, key, value, false),
                    Mutation::Put {
                        keyspace,
                        key: k,
                        value: v,
                    },
                ) => {
                    assert_eq!((ks, key, value), (*keyspace, k.clone(), v.clone()));
                }
                (WalEntry::Remove(ks, key), Mutation::Delete { keyspace, key: k }) => {
                    assert_eq!((ks, key), (*keyspace, k.clone()));
                }
                other => panic!("unexpected native record: {other:?}"),
            }
        }
    }

    #[test]
    fn retry_digest_binds_identity_original_version_and_order() {
        let first = batch(vec![put(b"a"), put(b"b")]);
        let retry = batch(vec![put(b"a"), put(b"b")]);
        assert_eq!(first.digest(), retry.digest());
        let mut id = identity();
        id.operation[0] ^= 1;
        let other_id = CompleteBatch::new(
            &shape(),
            id,
            version(),
            vec![put(b"a"), put(b"b")],
            limits(),
        )
        .unwrap();
        assert_ne!(first.digest(), other_id.digest());
        let other_version = CompleteBatch::new(
            &shape(),
            identity(),
            BatchVersion {
                epoch: 8,
                sequence: 11,
            },
            vec![put(b"a"), put(b"b")],
            limits(),
        )
        .unwrap();
        assert_ne!(first.digest(), other_version.digest());
        assert_ne!(first.digest(), batch(vec![put(b"b"), put(b"a")]).digest());
    }

    #[test]
    fn malformed_or_oversized_input_never_reaches_native_trusted_parser() {
        let original = batch(vec![put(b"some value")]);
        for len in 0..original.encoded().len() {
            assert!(
                CompleteBatch::decode(&shape(), original.encoded().slice(..len), limits()).is_err()
            );
        }
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN..HEADER_LEN + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::Truncated)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes[DIGEST_OFFSET - 4..DIGEST_OFFSET].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), bytes.into(), limits()),
            Err(CodecError::InvalidOperationCount)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN + MEMBER_HEADER_LEN] = 5; // No user relocation entry.
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::InvalidMutation)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN + MEMBER_HEADER_LEN + 2..HEADER_LEN + MEMBER_HEADER_LEN + 4]
            .copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::Truncated)
        ));
        let mut bytes = original.encoded().to_vec();
        bytes.push(0);
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::TrailingBytes)
        ));
    }

    #[test]
    fn semantics_are_validated_even_when_digest_and_native_crc_are_valid() {
        let original = batch(vec![put(b"value")]);
        let mut bytes = original.encoded().to_vec();
        bytes[HEADER_LEN + MEMBER_HEADER_LEN + 1] = 255;
        assert!(matches!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()),
            Err(CodecError::UnknownKeySpace(255))
        ));
        let incompatible_shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        assert!(matches!(
            CompleteBatch::decode(&incompatible_shape, original.encoded().clone(), limits()),
            Err(CodecError::InvalidKeyLength { .. })
        ));
        let destroyed_shape = shape();
        destroyed_shape.iter_ks().next().unwrap().mark_destroyed();
        assert!(matches!(
            CompleteBatch::decode(&destroyed_shape, original.encoded().clone(), limits()),
            Err(CodecError::DestroyedKeySpace(0))
        ));
    }

    #[test]
    fn variable_keys_and_empty_values_preserve_native_semantics() {
        let variable_shape = KeyShape::new_single_config_indexing(
            KeyIndexing::VariableLength,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::default(),
        );
        let mutations = vec![Mutation::Put {
            keyspace: KeySpace(0),
            key: Bytes::new(),
            value: Bytes::new(),
        }];
        let batch = CompleteBatch::new(
            &variable_shape,
            identity(),
            version(),
            mutations.clone(),
            limits(),
        )
        .unwrap();
        assert_eq!(
            CompleteBatch::decode(&variable_shape, batch.encoded().clone(), limits())
                .unwrap()
                .mutations(),
            &mutations
        );
    }

    #[test]
    fn limits_reject_before_encoded_record_can_exist() {
        assert!(matches!(
            CompleteBatch::new(&shape(), identity(), version(), vec![], limits()),
            Err(CodecError::InvalidOperationCount)
        ));
        let mut limit = limits();
        limit.max_operations = 1;
        assert!(matches!(
            CompleteBatch::new(
                &shape(),
                identity(),
                version(),
                vec![put(b"a"), put(b"b")],
                limit
            ),
            Err(CodecError::InvalidOperationCount)
        ));
        limit = limits();
        limit.max_encoded_bytes = HEADER_LEN + MIN_MEMBER_LEN;
        assert!(matches!(
            CompleteBatch::new(&shape(), identity(), version(), vec![put(b"a")], limit),
            Err(CodecError::TooLarge)
        ));
        let invalid = Mutation::Put {
            keyspace: KeySpace(0),
            key: b"wrong length"[..].into(),
            value: Bytes::new(),
        };
        assert!(matches!(
            CompleteBatch::new(&shape(), identity(), version(), vec![invalid], limits()),
            Err(CodecError::InvalidKeyLength { .. })
        ));
    }

    fn address(batch: &CompleteBatch) -> PhysicalValueAddress {
        PhysicalValueAddress {
            partition: 3,
            segment_generation: 5,
            offset: 1024,
            len: (batch.encoded().len() + NATIVE_CRC_HEADER_LEN) as u32,
        }
    }

    fn member_bytes(batch: &CompleteBatch, descriptor: RecordDescriptor) -> Bytes {
        batch
            .encoded()
            .slice(descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize)
    }

    #[test]
    fn scalar_reads_verify_only_one_member_and_keep_zero_copy_values() {
        let large = Mutation::Put {
            keyspace: KeySpace(0),
            key: b"key2"[..].into(),
            value: vec![42; 1024 * 1024].into(),
        };
        let original = CompleteBatch::new(
            &shape(),
            identity(),
            version(),
            vec![large, put(b"tiny")],
            BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 2 * 1024 * 1024,
            },
        )
        .unwrap();
        let reference = original.record_reference(address(&original), 1).unwrap();
        let member = member_bytes(&original, reference.descriptor);
        assert_eq!(member.len(), MEMBER_HEADER_LEN + 4 + 4 + 4);
        assert!(original.encoded().len() > 1024 * 1024);
        let Mutation::Put { value, .. } =
            CompleteBatch::decode_record(&shape(), &reference, member.clone()).unwrap()
        else {
            panic!("expected put");
        };
        assert_eq!(&value[..], b"tiny");
        assert_eq!(
            value.as_ptr(),
            member[reference.descriptor.value_offset as usize..].as_ptr()
        );
        assert_eq!(
            reference.mutation_version(),
            original.mutation_version(1).unwrap()
        );
        // Corrupt a sibling after the container was validated. A point read is
        // independent; later whole-batch recovery must still reject corruption.
        let mut corrupt = original.encoded().to_vec();
        corrupt[original.record_descriptors()[0].offset as usize + MEMBER_HEADER_LEN + 8] ^= 1;
        let corrupt: Bytes = corrupt.into();
        assert!(
            CompleteBatch::decode(
                &shape(),
                corrupt.clone(),
                BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 2 * 1024 * 1024,
                }
            )
            .is_err()
        );
        let descriptor = reference.descriptor;
        assert!(
            CompleteBatch::decode_record(
                &shape(),
                &reference,
                corrupt.slice(
                    descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize,
                )
            )
            .is_ok()
        );
    }

    #[test]
    fn grouped_record_verification_preserves_order_ownership_and_every_member_check() {
        let bounds = BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 512 * 1024,
        };
        let mut records = Vec::new();
        for sequence in 1..=2 {
            let mutations = (0..17)
                .map(|i| {
                    let key = (i as u32).to_be_bytes().to_vec().into();
                    if i % 7 == 0 {
                        Mutation::Delete {
                            keyspace: KeySpace(0),
                            key,
                        }
                    } else {
                        Mutation::Put {
                            keyspace: KeySpace(0),
                            key,
                            value: vec![
                                (sequence * 17 + i) as u8;
                                [0, 1, 127, 128, 129, 1024, 8192][i % 7]
                            ]
                            .into(),
                        }
                    }
                })
                .collect();
            let batch = CompleteBatch::new(
                &shape(),
                BatchId {
                    database: [1; 16],
                    operation: [sequence as u8; 16],
                },
                BatchVersion {
                    epoch: 3,
                    sequence: sequence as u64,
                },
                mutations,
                bounds,
            )
            .unwrap();
            for operation in (0..17).rev() {
                let reference = batch.record_reference(address(&batch), operation).unwrap();
                records.push((reference, member_bytes(&batch, reference.descriptor)));
            }
        }
        records.push(records[0].clone());
        let scalar: Vec<_> = records
            .iter()
            .map(|(reference, bytes)| {
                CompleteBatch::decode_record(&shape(), reference, bytes.clone()).unwrap()
            })
            .collect();
        let grouped = CompleteBatch::decode_records(&shape(), &records).unwrap();
        assert_eq!(grouped, scalar);
        assert!(
            CompleteBatch::decode_records(&shape(), &[])
                .unwrap()
                .is_empty()
        );
        for (mutation, (reference, bytes)) in grouped.iter().zip(&records) {
            if let Mutation::Put { value, .. } = mutation {
                assert_eq!(
                    value.as_ptr(),
                    bytes[reference.descriptor.value_offset as usize..].as_ptr()
                );
            }
        }
        // No partial Vec may escape when a later member fails.
        for bad in 0..records.len() {
            let mut input = records.clone();
            let mut bytes = input[bad].1.to_vec();
            *bytes.last_mut().unwrap() ^= 1;
            input[bad].1 = bytes.into();
            assert!(CompleteBatch::decode_records(&shape(), &input).is_err());
            input[bad] = records[bad].clone();
            input[bad].0.batch_version.sequence += 1;
            assert!(CompleteBatch::decode_records(&shape(), &input).is_err());
            input[bad] = records[bad].clone();
            input[bad].1 = input[bad].1.slice(..input[bad].1.len() - 1);
            assert!(CompleteBatch::decode_records(&shape(), &input).is_err());
        }
        let wrong_shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
        assert!(CompleteBatch::decode_records(&wrong_shape, &records).is_err());
    }

    #[test]
    fn scalar_reference_rejects_corruption_truncation_wrong_member_and_forged_metadata() {
        let original = batch(vec![put(b"same"), put(b"same")]);
        let reference = original.record_reference(address(&original), 0).unwrap();
        let member = member_bytes(&original, reference.descriptor);
        for end in 0..member.len() {
            assert!(
                CompleteBatch::decode_record(&shape(), &reference, member.slice(..end)).is_err()
            );
        }
        let mut corrupt = member.to_vec();
        corrupt[MEMBER_HEADER_LEN] ^= 1;
        assert_eq!(
            CompleteBatch::decode_record(&shape(), &reference, corrupt.into()).unwrap_err(),
            CodecError::ChecksumMismatch
        );
        for byte in 0..member.len() {
            let mut corrupt = member.to_vec();
            corrupt[byte] ^= 1;
            assert!(CompleteBatch::decode_record(&shape(), &reference, corrupt.into()).is_err());
        }
        // Same-key, same-value records are still distinct logical operations.
        assert_ne!(
            original.record_descriptors()[0].checksum,
            original.record_descriptors()[1].checksum
        );
        assert!(
            CompleteBatch::decode_record(
                &shape(),
                &reference,
                member_bytes(&original, original.record_descriptors()[1])
            )
            .is_err()
        );
        let mut forgeries = Vec::new();
        let mut forged = reference;
        forged.batch_id.operation[0] ^= 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.batch_version.sequence += 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.operation_index += 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.offset += 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.offset = u32::MAX;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.len = u32::MAX;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.keyspace = 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.kind = RecordKind::Delete;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.key_len += 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.value_offset += 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.descriptor.value_len += 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.batch_address.len = 1;
        forgeries.push(forged);
        let mut forged = reference;
        forged.batch_address.offset = u64::MAX;
        forgeries.push(forged);
        for forged in forgeries {
            assert!(
                CompleteBatch::decode_record(&shape(), &forged, member.clone()).is_err(),
                "accepted {forged:?}"
            );
        }
        // Physical partition/generation and complete-batch digest are checked by
        // storage membership lookup, not this content-integrity-only parser.
        let mut moved = reference;
        moved.batch_address.partition += 1;
        moved.batch_address.segment_generation += 1;
        moved.batch_digest.0[0] ^= 1;
        assert!(CompleteBatch::decode_record(&shape(), &moved, member).is_ok());
    }

    #[test]
    fn mixed_keyspaces_and_repeated_mutations_keep_canonical_membership() {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("one", 4, 1, KeyType::uniform(1));
        builder.add_key_space_config_indexing(
            "two",
            KeyIndexing::VariableLength,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::default(),
        );
        let shape = builder.build();
        let mutations = vec![
            put(b"first"),
            Mutation::Put {
                keyspace: KeySpace(1),
                key: Bytes::new(),
                value: Bytes::new(),
            },
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: b"key1"[..].into(),
            },
            put(b"last"),
        ];
        let original =
            CompleteBatch::new(&shape, identity(), version(), mutations.clone(), limits()).unwrap();
        let decoded = CompleteBatch::decode(&shape, original.encoded().clone(), limits()).unwrap();
        assert_eq!(original.record_descriptors(), decoded.record_descriptors());
        for (i, mutation) in mutations.into_iter().enumerate() {
            let reference = decoded.record_reference(address(&decoded), i).unwrap();
            assert_eq!(
                CompleteBatch::decode_record(
                    &shape,
                    &reference,
                    member_bytes(&decoded, reference.descriptor)
                )
                .unwrap(),
                mutation
            );
        }
        assert!(original.record_reference(address(&original), 4).is_err());
        let mut wrong_address = address(&original);
        wrong_address.len += 8;
        assert!(original.record_reference(wrong_address, 0).is_err());
    }

    #[test]
    fn old_format_is_refused_explicitly() {
        let original = batch(vec![put(b"value")]);
        let mut bytes = original.encoded().to_vec();
        bytes[..8].copy_from_slice(b"THBATCH1");
        bytes[8..10].copy_from_slice(&1u16.to_be_bytes());
        assert!(matches!(
            CompleteBatch::decode(&shape(), bytes.into(), limits()),
            Err(CodecError::InvalidFormat)
        ));
    }
}
