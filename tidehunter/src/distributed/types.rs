//! Distinct identity, authorization, ordering and location domains.
//!
//! See the October 6 architecture decision, §7.2. Native `WalPosition` is
//! deliberately unchanged: its offsets are not comparable across partitions.

use serde::{Deserialize, Serialize};

/// Stable original operation identity, retained across retries and failover.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BatchId {
    pub database: [u8; 16],
    pub operation: [u8; 16],
}

/// Current fenced writer authority. This is not the original mutation version.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestAuthority {
    pub epoch: u64,
}

/// Original logical order within one database; unrelated databases are unordered.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct BatchVersion {
    pub epoch: u64,
    pub sequence: u64,
}

/// Logical last-write order, including repeated keys in the same batch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MutationVersion {
    pub batch: BatchVersion,
    pub operation_index: u32,
}

/// Location of a complete batch frame, never a user mutation version.
///
/// `offset` is relative to the named segment generation; `len` includes the
/// native CRC frame header. Reusing storage requires a new generation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct PhysicalValueAddress {
    pub partition: u32,
    pub segment_generation: u64,
    pub offset: u64,
    pub len: u32,
}

/// BLAKE2b-256 of the complete canonical authority record, excluding this field.
/// This binds stable identity, original order and mutations. It authenticates
/// neither a caller nor its current fencing authority.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct PayloadDigest(pub [u8; 32]);

/// IEEE CRC-32 over a canonical batch-member identity and its native entry.
/// This detects accidental corruption; it is not authentication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemberChecksum(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum RecordKind {
    Put,
    Delete,
}

/// Bounded metadata derived from a fully validated complete batch.
///
/// `offset` and `len` address the member (native length, checksum and entry)
/// inside the batch payload, excluding its outer native CRC header. `value_offset`
/// is relative to that member. A delete has a zero-length value at the member end.
/// The checksum binds identity, original logical version, operation index, bounds
/// and native entry bytes. It is an integrity check, not authentication.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RecordDescriptor {
    pub operation_index: u32,
    pub offset: u32,
    pub len: u32,
    pub keyspace: u8,
    pub kind: RecordKind,
    pub key_len: u32,
    pub value_offset: u32,
    pub value_len: u32,
    pub checksum: MemberChecksum,
}

/// A scalar-read reference backed by a complete authoritative batch.
///
/// Accepting self-consistent bytes alone cannot prove batch membership. Storage
/// must compare this reference to metadata retained from validated append/replay
/// before serving it, including address generation and complete-batch digest.
/// Possessing a reference does not prove durability, publication or a GC lease.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ValueRecordReference {
    pub batch_id: BatchId,
    pub batch_version: BatchVersion,
    pub batch_digest: PayloadDigest,
    pub batch_address: PhysicalValueAddress,
    pub descriptor: RecordDescriptor,
}

impl ValueRecordReference {
    pub fn mutation_version(self) -> MutationVersion {
        MutationVersion {
            batch: self.batch_version,
            operation_index: self.descriptor.operation_index,
        }
    }
}
