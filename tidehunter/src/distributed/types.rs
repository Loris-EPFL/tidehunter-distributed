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
