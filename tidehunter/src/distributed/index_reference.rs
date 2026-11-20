//! Compact logical identities for native index publication groundwork.
//!
//! Paper §§3.1–3.4 couples an index reference to mutation order because native
//! values share one physical WAL. Across partitions those domains must separate.
//! This type identifies a mutation; it is neither a byte address nor evidence
//! that its batch is durable, published or retained. Resolution requires the
//! validated authority-record directory for the same database.
//!
//! The codec is a building block, not a complete persisted native index format.
//! A future index container must bind database identity, this format version,
//! integrity, publication cut and the recoverable reference-map dependencies.
//! Existing native `WalPosition` and on-disk index formats remain unchanged.

use super::types::{BatchVersion, MutationVersion};
use std::fmt;

#[cfg(test)]
mod tests;

/// Explicit container-level discriminator; never infer a format from its length.
pub const LOGICAL_VALUE_ID_FORMAT_VERSION: u16 = 1;

/// Original mutation identity and order, scoped to one logical database.
///
/// Relocation preserves this identity while replacing a separately validated
/// physical binding. Database identity is intentionally held by the enclosing
/// index/directory, so comparing IDs from different databases is invalid. Every
/// component uses its full range; no partition or physical offset is packed here.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct LogicalValueId(MutationVersion);

impl LogicalValueId {
    /// Packed payload width; the format version is stored by the container.
    pub const ENCODED_LEN: usize = 20;

    pub const fn new(version: MutationVersion) -> Self {
        Self(version)
    }

    pub const fn version(self) -> MutationVersion {
        self.0
    }

    /// Fixed-width, big-endian encoding whose lexicographic order agrees with
    /// the original mutation order within one database domain.
    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0; Self::ENCODED_LEN];
        bytes[..8].copy_from_slice(&self.0.batch.epoch.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.0.batch.sequence.to_be_bytes());
        bytes[16..].copy_from_slice(&self.0.operation_index.to_be_bytes());
        bytes
    }

    /// Decode only after the enclosing index container has validated database,
    /// integrity and version metadata. Decoding alone proves no authority.
    pub fn decode(format_version: u16, bytes: &[u8]) -> Result<Self, ReferenceCodecError> {
        if format_version != LOGICAL_VALUE_ID_FORMAT_VERSION {
            return Err(ReferenceCodecError::UnsupportedFormat(format_version));
        }
        if bytes.len() != Self::ENCODED_LEN {
            return Err(ReferenceCodecError::InvalidLength(bytes.len()));
        }
        // The exact length check above makes each fixed-width conversion safe.
        Ok(Self(MutationVersion {
            batch: BatchVersion {
                epoch: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
                sequence: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            },
            operation_index: u32::from_be_bytes(bytes[16..].try_into().unwrap()),
        }))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReferenceCodecError {
    UnsupportedFormat(u16),
    InvalidLength(usize),
}

impl fmt::Display for ReferenceCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormat(version) => {
                write!(f, "unsupported logical value reference format {version}")
            }
            Self::InvalidLength(actual) => write!(
                f,
                "logical value reference has {actual} bytes, expected {}",
                LogicalValueId::ENCODED_LEN
            ),
        }
    }
}

impl std::error::Error for ReferenceCodecError {}
