//! Unit tests of the complete-batch codec.
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
    let BatchDescriptors::Points(descriptors) = &canonical.descriptors else {
        panic!("point batch has point descriptors");
    };
    assert_eq!(descriptors.capacity(), descriptors.len());
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
    assert!(CompleteBatch::new(&hashed, identity(), version(), vec![empty_key], limits()).is_ok());
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
        .cell_id(accepted.mutations()[0].point_key().unwrap());

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
            corrupt
                .slice(descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize,)
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
        assert!(CompleteBatch::decode_record(&shape(), &reference, member.slice(..end)).is_err());
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
