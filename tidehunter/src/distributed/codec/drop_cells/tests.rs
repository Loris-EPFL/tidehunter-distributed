//! `DropCells` member encoding and checked decoding: round trips, size
//! independence, singleton rule, canonical-cell validation, forged bytes and
//! exhaustive corruption. No input below may panic the decoder.

use super::super::{
    BatchLimits, CodecError, CompleteBatch, DIGEST_OFFSET, DROP_CELLS_FORMAT_VERSION,
    FORMAT_VERSION, HEADER_LEN, MEMBER_HEADER_LEN, Mutation, record_digest,
};
use crate::cell::CellId;
use crate::distributed::types::{BatchId, BatchVersion, PhysicalValueAddress};
use crate::key_shape::{KeyIndexing, KeyShape, KeyShapeBuilder, KeySpace, KeyType};
use minibytes::Bytes;
use smallvec::SmallVec;

const UNIFORM: KeySpace = KeySpace(0);
const PREFIXED: KeySpace = KeySpace(1);
const VARIABLE: KeySpace = KeySpace(2);
const SHORT: KeySpace = KeySpace(3);

/// Uniform 8-byte keys with 4 × 8 cells; 4-byte prefixes with 4 cluster bits
/// (non-byte-aligned cells); a variable-length keyspace; 2-byte uniform keys
/// shorter than the cell prefix.
fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("uniform", 8, 4, KeyType::uniform(8));
    builder.add_key_space("prefixed", 8, 4, KeyType::prefix_uniform(4, 4));
    builder.add_key_space_config_indexing(
        "variable",
        KeyIndexing::VariableLength,
        4,
        KeyType::uniform(8),
        Default::default(),
    );
    builder.add_key_space("short", 2, 1, KeyType::uniform(1));
    builder.build()
}

fn limits() -> BatchLimits {
    BatchLimits {
        max_operations: 8,
        max_encoded_bytes: 4096,
    }
}

fn id() -> BatchId {
    BatchId {
        database: [3; 16],
        operation: [4; 16],
    }
}

fn version() -> BatchVersion {
    BatchVersion {
        epoch: 5,
        sequence: 9,
    }
}

fn bytes_cell(prefix: &[u8]) -> CellId {
    CellId::Bytes(SmallVec::from_slice(prefix))
}

fn drop_member(keyspace: KeySpace, first: CellId, last: CellId) -> Mutation {
    Mutation::DropCells {
        keyspace,
        first_cell: first,
        last_cell: last,
    }
}

fn encode(mutations: Vec<Mutation>) -> Result<CompleteBatch, CodecError> {
    CompleteBatch::new(&shape(), id(), version(), mutations, limits())
}

fn decode(bytes: Vec<u8>) -> Result<CompleteBatch, CodecError> {
    CompleteBatch::decode(&shape(), Bytes::from(bytes), limits())
}

fn integer_drop(first: usize, last: usize) -> CompleteBatch {
    encode(vec![drop_member(
        UNIFORM,
        CellId::Integer(first),
        CellId::Integer(last),
    )])
    .unwrap()
}

/// Reseal the whole-batch digest after a forgery, so decoding reaches the
/// structural and semantic checks behind it.
fn redigest(bytes: &mut [u8]) {
    let digest = record_digest(bytes);
    bytes[DIGEST_OFFSET..HEADER_LEN].copy_from_slice(&digest.0);
}

/// Replace the native member bytes, fix the member length and reseal.
fn forge_native(batch: &CompleteBatch, native: &[u8]) -> Vec<u8> {
    let mut bytes = batch.encoded()[..HEADER_LEN].to_vec();
    bytes.extend_from_slice(&(native.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&batch.encoded()[HEADER_LEN + 4..HEADER_LEN + MEMBER_HEADER_LEN]);
    bytes.extend_from_slice(native);
    redigest(&mut bytes);
    bytes
}

fn native(batch: &CompleteBatch) -> Vec<u8> {
    batch.encoded()[HEADER_LEN + MEMBER_HEADER_LEN..].to_vec()
}

#[test]
fn uniform_and_prefixed_members_round_trip_without_descriptors() {
    for mutation in [
        drop_member(UNIFORM, CellId::Integer(0), CellId::Integer(31)),
        drop_member(UNIFORM, CellId::Integer(7), CellId::Integer(7)),
        drop_member(
            PREFIXED,
            bytes_cell(&[0, 0, 0, 0]),
            bytes_cell(&[9, 9, 9, 0xf0]),
        ),
    ] {
        let batch = encode(vec![mutation.clone()]).unwrap();
        assert_eq!(
            batch.encoded()[8..10],
            DROP_CELLS_FORMAT_VERSION.to_be_bytes()
        );
        assert!(batch.record_descriptors().is_empty());
        assert_eq!(batch.mutations(), std::slice::from_ref(&mutation));
        assert!(batch.drop_cells().is_some());
        let member = batch.range_member().unwrap();
        assert_eq!(member.offset as usize, HEADER_LEN);
        assert_eq!(member.len as usize, batch.encoded().len() - HEADER_LEN);
        assert_eq!(mutation.point_key(), None);
        let decoded = decode(batch.encoded().to_vec()).unwrap();
        assert_eq!(decoded.mutations(), batch.mutations());
        assert_eq!(decoded.digest(), batch.digest());
        assert!(decoded.record_descriptors().is_empty());
        let address = PhysicalValueAddress {
            partition: 0,
            segment_generation: version().epoch,
            offset: 0,
            len: (batch.encoded().len() + 8) as u32,
        };
        let error = batch.record_reference(address, 0).unwrap_err();
        assert!(matches!(error, CodecError::DropCellsReference));
        assert_eq!(
            error.to_string(),
            "DropCells does not have a value reference"
        );
    }
}

#[test]
fn encoded_size_depends_on_the_endpoints_only() {
    // One cell or every cell: same bytes, however many keys the cells hold.
    let narrow = integer_drop(3, 3);
    let wide = integer_drop(0, 31);
    assert_eq!(narrow.encoded().len(), wide.encoded().len());
    assert_eq!(
        narrow.encoded().len(),
        HEADER_LEN + MEMBER_HEADER_LEN + 1 + 1 + 9 + 9
    );
    let point = encode(vec![Mutation::Put {
        keyspace: UNIFORM,
        key: vec![0; 8].into(),
        value: vec![1].into(),
    }])
    .unwrap();
    assert_eq!(point.encoded()[8..10], FORMAT_VERSION.to_be_bytes());
}

#[test]
fn drop_cells_is_rejected_beside_any_other_member() {
    let range = drop_member(UNIFORM, CellId::Integer(0), CellId::Integer(1));
    let put = Mutation::Put {
        keyspace: UNIFORM,
        key: vec![0; 8].into(),
        value: vec![1].into(),
    };
    for mutations in [
        vec![range.clone(), put.clone()],
        vec![put, range.clone()],
        vec![range.clone(), range],
    ] {
        let error = encode(mutations).unwrap_err();
        assert!(matches!(error, CodecError::DropCellsNotSingleton));
        assert_eq!(
            error.to_string(),
            "DropCells must be the only member in a batch"
        );
    }
}

#[test]
fn noncanonical_or_foreign_cells_are_rejected_before_encoding() {
    let invalid = |keyspace: KeySpace, first: CellId, last: CellId| {
        let error = encode(vec![drop_member(keyspace, first, last)]).unwrap_err();
        assert!(
            matches!(error, CodecError::InvalidDropCells { keyspace: ks } if ks == keyspace.0),
            "{error:?}"
        );
        assert_eq!(
            error.to_string(),
            format!(
                "invalid DropCells cell encoding for keyspace {}",
                keyspace.0
            )
        );
    };
    // Integer beyond the 32 cells, reversed, and a byte cell in a uniform space.
    invalid(UNIFORM, CellId::Integer(0), CellId::Integer(32));
    invalid(UNIFORM, CellId::Integer(5), CellId::Integer(4));
    invalid(UNIFORM, bytes_cell(&[0; 4]), bytes_cell(&[0; 4]));
    // Wrong prefix length, cluster bits set, reversed, integer in a prefixed space.
    invalid(PREFIXED, bytes_cell(&[0; 3]), bytes_cell(&[0; 4]));
    invalid(
        PREFIXED,
        bytes_cell(&[0, 0, 0, 1]),
        bytes_cell(&[0, 0, 0, 0x10]),
    );
    invalid(
        PREFIXED,
        bytes_cell(&[0, 0, 1, 0]),
        bytes_cell(&[0, 0, 0, 0]),
    );
    invalid(PREFIXED, CellId::Integer(0), CellId::Integer(0));
    // Variable-length keys and keys shorter than the cell prefix.
    invalid(VARIABLE, CellId::Integer(0), CellId::Integer(0));
    invalid(SHORT, CellId::Integer(0), CellId::Integer(0));
    assert!(matches!(
        encode(vec![drop_member(
            KeySpace(9),
            CellId::Integer(0),
            CellId::Integer(0)
        )]),
        Err(CodecError::UnknownKeySpace(9))
    ));
}

#[test]
fn forged_native_members_fail_closed() {
    let batch = integer_drop(1, 2);
    let good = native(&batch);
    assert_eq!(good.len(), 20);
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    let mut tag = good.clone();
    tag[0] = 7;
    cases.push(("native tag", tag));
    let mut point_tag = good.clone();
    point_tag[0] = 1;
    cases.push(("point tag in range format", point_tag));
    let mut keyspace = good.clone();
    keyspace[1] = 9;
    cases.push(("unknown keyspace", keyspace));
    let mut cell_tag = good.clone();
    cell_tag[2] = 2;
    cases.push(("cell tag", cell_tag));
    let mut huge = good.clone();
    huge[3..11].copy_from_slice(&u64::MAX.to_be_bytes());
    cases.push(("u64 cell bound", huge));
    let mut beyond = good.clone();
    beyond[12..20].copy_from_slice(&32u64.to_be_bytes());
    cases.push(("cell past the keyspace", beyond));
    let mut reversed = good.clone();
    reversed[3..11].copy_from_slice(&3u64.to_be_bytes());
    cases.push(("reversed", reversed));
    let mut long_prefix = good.clone();
    long_prefix[2] = 1;
    long_prefix[3..5].copy_from_slice(&u16::MAX.to_be_bytes());
    cases.push(("byte cell length past the member", long_prefix));
    cases.push(("truncated", good[..good.len() - 1].to_vec()));
    cases.push(("extra native byte", [good.as_slice(), &[0]].concat()));
    cases.push(("empty", Vec::new()));
    for (name, forged) in cases {
        assert!(decode(forge_native(&batch, &forged)).is_err(), "{name}");
    }
    // Bytes after the member, outside its declared length.
    let mut trailing = batch.encoded().to_vec();
    trailing.push(0);
    redigest(&mut trailing);
    assert!(matches!(decode(trailing), Err(CodecError::TrailingBytes)));
    // A second declared member.
    let mut count = batch.encoded().to_vec();
    count[DIGEST_OFFSET - 4..DIGEST_OFFSET].copy_from_slice(&2u32.to_be_bytes());
    redigest(&mut count);
    assert!(decode(count).is_err());
}

#[test]
fn checksum_digest_and_version_mismatches_are_rejected() {
    let batch = integer_drop(0, 3);
    let mut crc = batch.encoded().to_vec();
    crc[HEADER_LEN + 4] ^= 1;
    redigest(&mut crc);
    assert!(matches!(decode(crc), Err(CodecError::ChecksumMismatch)));
    let mut digest = batch.encoded().to_vec();
    digest[DIGEST_OFFSET] ^= 1;
    assert!(matches!(decode(digest), Err(CodecError::DigestMismatch)));
    // The member CRC binds the batch version: a resealed relabel fails.
    let mut relabel = batch.encoded().to_vec();
    relabel[DIGEST_OFFSET - 5] ^= 1;
    redigest(&mut relabel);
    assert!(matches!(decode(relabel), Err(CodecError::ChecksumMismatch)));
    // Point format 3 never accepts tag 6; range format 4 never a point.
    let mut as_point = batch.encoded().to_vec();
    as_point[8..10].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
    redigest(&mut as_point);
    assert!(decode(as_point).is_err());
    let point = encode(vec![Mutation::Delete {
        keyspace: UNIFORM,
        key: vec![0; 8].into(),
    }])
    .unwrap();
    let mut as_range = point.encoded().to_vec();
    as_range[8..10].copy_from_slice(&DROP_CELLS_FORMAT_VERSION.to_be_bytes());
    redigest(&mut as_range);
    assert!(decode(as_range).is_err());
    // Format 5 is the relocated-copy format (`RELOCATED_FORMAT_VERSION`) and
    // decodes through its own checks, so the unknown versions are 2 and 6+.
    for unknown in [2u16, 6, u16::MAX] {
        let mut format = batch.encoded().to_vec();
        format[8..10].copy_from_slice(&unknown.to_be_bytes());
        redigest(&mut format);
        assert!(matches!(decode(format), Err(CodecError::InvalidFormat)));
    }
}

#[test]
fn every_single_byte_corruption_and_truncation_decodes_without_panic() {
    for batch in [
        integer_drop(0, 31),
        encode(vec![drop_member(
            PREFIXED,
            bytes_cell(&[1, 2, 3, 0x40]),
            bytes_cell(&[1, 2, 3, 0x80]),
        )])
        .unwrap(),
    ] {
        let encoded = batch.encoded().to_vec();
        for at in 0..encoded.len() {
            for flip in [0x01u8, 0x80, 0xff] {
                let mut forged = encoded.clone();
                forged[at] ^= flip;
                assert!(decode(forged.clone()).is_err(), "unsealed byte {at}");
                // Resealed forgeries reach the member parser; outside the
                // digest field they may only fail closed or decode canonically.
                if !(DIGEST_OFFSET..HEADER_LEN).contains(&at) {
                    redigest(&mut forged);
                    if let Ok(decoded) = decode(forged.clone()) {
                        assert_eq!(decoded.encoded().as_ref(), forged.as_slice());
                    }
                }
            }
        }
        for len in 0..encoded.len() {
            let mut truncated = encoded[..len].to_vec();
            if len >= HEADER_LEN {
                redigest(&mut truncated);
            }
            assert!(decode(truncated).is_err(), "truncated to {len}");
        }
    }
}
