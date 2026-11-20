//! Format 5 (relocated copies): round trip, byte identity of members, and the
//! members a relocated batch refuses.

use super::super::{DIGEST_OFFSET, HEADER_LEN, MEMBER_HEADER_LEN, record_digest};
use super::*;
use crate::cell::CellId;
use crate::key_shape::{KeySpace, KeyType};
use minibytes::Bytes;

fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

fn limits() -> BatchLimits {
    BatchLimits {
        max_operations: 32,
        max_encoded_bytes: 4096,
    }
}

fn id() -> BatchId {
    BatchId {
        database: [1; 16],
        operation: [2; 16],
    }
}

fn put(key: &'static [u8; 4], value: &'static [u8]) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key[..].into(),
        value: value.into(),
    }
}

fn with_valid_digest(mut bytes: Vec<u8>) -> Bytes {
    let digest = record_digest(&bytes);
    bytes[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
    bytes.into()
}

/// The member payload after its 8-byte header: what `payload_crc` covers.
fn payload(batch: &CompleteBatch, operation: usize) -> Vec<u8> {
    let descriptor = batch.record_descriptors()[operation];
    let start = descriptor.offset as usize + MEMBER_HEADER_LEN;
    batch.encoded()[start..descriptor.offset as usize + descriptor.len as usize].to_vec()
}

#[test]
fn the_format_values_are_frozen() {
    assert_eq!(FORMAT_VERSION, 3);
    assert_eq!(DROP_CELLS_FORMAT_VERSION, 4);
    assert_eq!(RELOCATED_FORMAT_VERSION, 5);
    let version = BatchVersion {
        epoch: 4,
        sequence: 9,
    };
    let copy =
        CompleteBatch::new_relocated(&shape(), id(), version, vec![put(b"key1", b"v")], limits())
            .unwrap();
    assert_eq!(&copy.encoded()[8..10], &5u16.to_be_bytes());
    let plain =
        CompleteBatch::new(&shape(), id(), version, vec![put(b"key1", b"v")], limits()).unwrap();
    assert_eq!(&plain.encoded()[8..10], &3u16.to_be_bytes());
    assert!(!known_format(0) && !known_format(2) && !known_format(6));
}

#[test]
fn the_flag_survives_encode_decode_and_canonical_ownership() {
    let version = BatchVersion {
        epoch: 4,
        sequence: 9,
    };
    let mutations = vec![put(b"key1", b"first"), put(b"key2", b"second")];
    let copy =
        CompleteBatch::new_relocated(&shape(), id(), version, mutations.clone(), limits()).unwrap();
    assert!(copy.is_relocated());
    let decoded = CompleteBatch::decode(&shape(), copy.encoded().clone(), limits()).unwrap();
    assert!(decoded.is_relocated());
    assert_eq!(decoded.mutations(), mutations.as_slice());
    assert_eq!(decoded.digest(), copy.digest());
    assert!(decoded.into_canonical_owned().is_relocated());
    let owned = CompleteBatch::decode_owned(&shape(), copy.encoded().to_vec(), limits()).unwrap();
    assert!(owned.is_relocated());
    // The marker is part of the digest: the same members as a point batch differ.
    let plain = CompleteBatch::new(&shape(), id(), version, mutations, limits()).unwrap();
    assert!(!plain.is_relocated());
    assert_ne!(plain.digest(), copy.digest());
    assert!(
        !CompleteBatch::decode(&shape(), plain.encoded().clone(), limits())
            .unwrap()
            .is_relocated()
    );
}

#[test]
fn a_copy_member_payload_is_byte_identical_to_its_source_member() {
    // The source member lives in an ordinary batch at one version, its copy in a
    // relocated batch at a later one: the identity checksum in the member header
    // differs (it binds the batch version), the payload the index CRC covers does not.
    let source = CompleteBatch::new(
        &shape(),
        id(),
        BatchVersion {
            epoch: 4,
            sequence: 9,
        },
        vec![put(b"key1", b"unused"), put(b"key2", b"the value")],
        limits(),
    )
    .unwrap();
    let copy = CompleteBatch::new_relocated(
        &shape(),
        BatchId {
            database: [1; 16],
            operation: [7; 16],
        },
        BatchVersion {
            epoch: 4,
            sequence: 40,
        },
        vec![put(b"key2", b"the value")],
        limits(),
    )
    .unwrap();
    assert_eq!(payload(&source, 1), payload(&copy, 0));
    assert_eq!(
        crc32fast::hash(&payload(&source, 1)),
        crc32fast::hash(&payload(&copy, 0))
    );
    assert_ne!(
        source.record_descriptors()[1].checksum,
        copy.record_descriptors()[0].checksum
    );
}

#[test]
fn only_put_members_are_accepted() {
    let version = BatchVersion {
        epoch: 4,
        sequence: 9,
    };
    let delete = Mutation::Delete {
        keyspace: KeySpace(0),
        key: b"key1"[..].into(),
    };
    assert_eq!(
        CompleteBatch::new_relocated(
            &shape(),
            id(),
            version,
            vec![put(b"key2", b"v"), delete.clone()],
            limits()
        )
        .unwrap_err(),
        CodecError::InvalidMutation
    );
    let drop = Mutation::DropCells {
        keyspace: KeySpace(0),
        first_cell: CellId::Integer(0),
        last_cell: CellId::Integer(0),
    };
    assert_eq!(
        CompleteBatch::new_relocated(&shape(), id(), version, vec![drop], limits()).unwrap_err(),
        CodecError::InvalidMutation
    );
    assert_eq!(
        CompleteBatch::new_relocated(&shape(), id(), version, vec![], limits()).unwrap_err(),
        CodecError::InvalidOperationCount
    );
    // Forged: a point batch with a tombstone whose header says "relocated" and
    // whose digest is valid is refused at decode, not skipped.
    let plain = CompleteBatch::new(&shape(), id(), version, vec![delete], limits()).unwrap();
    let mut bytes = plain.encoded().to_vec();
    bytes[8..10].copy_from_slice(&RELOCATED_FORMAT_VERSION.to_be_bytes());
    assert_eq!(
        CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()).unwrap_err(),
        CodecError::InvalidMutation
    );
}

#[test]
fn an_unknown_format_is_refused() {
    let version = BatchVersion {
        epoch: 4,
        sequence: 9,
    };
    let plain =
        CompleteBatch::new(&shape(), id(), version, vec![put(b"key1", b"v")], limits()).unwrap();
    for format in [0u16, 1, 2, 6, u16::MAX] {
        let mut bytes = plain.encoded().to_vec();
        bytes[8..10].copy_from_slice(&format.to_be_bytes());
        assert_eq!(
            CompleteBatch::decode(&shape(), with_valid_digest(bytes), limits()).unwrap_err(),
            CodecError::InvalidFormat,
            "format {format}"
        );
    }
}
