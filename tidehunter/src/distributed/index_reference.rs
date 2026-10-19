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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::pending_table::{PendingTable, Transaction};
    use minibytes::Bytes;

    fn id(epoch: u64, sequence: u64, operation_index: u32) -> LogicalValueId {
        LogicalValueId::new(MutationVersion {
            batch: BatchVersion { epoch, sequence },
            operation_index,
        })
    }

    #[test]
    fn codec_preserves_full_logical_order_and_rejects_other_formats() {
        let ordered = [
            id(0, 0, 0),
            id(0, 0, u32::MAX),
            id(0, 1, 0),
            id(0, u64::MAX, u32::MAX),
            id(1, 0, 0),
            id(u64::MAX, u64::MAX, u32::MAX),
        ];
        for pair in ordered.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].encode() < pair[1].encode());
        }
        for value in ordered {
            assert_eq!(
                LogicalValueId::decode(LOGICAL_VALUE_ID_FORMAT_VERSION, &value.encode()),
                Ok(value)
            );
        }
        let bytes = id(1, 2, 3).encode();
        assert_eq!(
            LogicalValueId::decode(0, &bytes),
            Err(ReferenceCodecError::UnsupportedFormat(0))
        );
        assert_eq!(
            LogicalValueId::decode(2, &bytes),
            Err(ReferenceCodecError::UnsupportedFormat(2))
        );
        for len in 0..LogicalValueId::ENCODED_LEN {
            assert_eq!(
                LogicalValueId::decode(LOGICAL_VALUE_ID_FORMAT_VERSION, &bytes[..len]),
                Err(ReferenceCodecError::InvalidLength(len))
            );
        }
        assert_eq!(
            LogicalValueId::decode(LOGICAL_VALUE_ID_FORMAT_VERSION, &[0; 21]),
            Err(ReferenceCodecError::InvalidLength(21))
        );
    }

    #[test]
    fn logical_references_use_native_atomic_pending_publication() {
        let mut first = PendingTable::<LogicalValueId>::default();
        let mut second = PendingTable::<LogicalValueId>::default();
        let transaction = Transaction::default();
        let key = Bytes::from(vec![3]);
        first.insert(key.clone(), id(8, 12, 0), None, &transaction);
        first.remove(key.clone(), id(8, 12, 1), &transaction);
        second.insert(key.clone(), id(8, 12, 2), None, &transaction);
        assert!(first.take_committed().0.is_empty());
        assert!(second.take_committed().0.is_empty());
        transaction.commit();
        let (updates, removed) = first.take_committed_for(&key);
        assert_eq!(removed, 2);
        assert_eq!(updates[0].value, id(8, 12, 0));
        assert_eq!(updates[1].value, id(8, 12, 1));
        assert!(!updates[1].is_modified);
        assert_eq!(second.take_committed().0[0].value, id(8, 12, 2));
        // Payload width is the disk-format cost; Rust layout has target padding.
        assert_eq!(LogicalValueId::ENCODED_LEN, 20);
        assert_eq!(
            std::mem::size_of::<LogicalValueId>(),
            std::mem::size_of::<MutationVersion>()
        );
    }
}
