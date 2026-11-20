use super::*;
use crate::distributed::codec::Mutation;
use crate::distributed::types::{BatchId, BatchVersion};
use crate::key_shape::KeyType;

#[test]
fn live_in_place_encoder_matches_canonical_frames_for_group_shapes() {
    let sizes = [0, 1, 7, 8, 1024, 4088, 4089, 65536];
    let bl = BatchLimits {
        max_operations: 16,
        max_encoded_bytes: 128 * 1024,
    };
    let gl = GroupLimits {
        max_batches: 31,
        max_encoded_bytes: 4 * 1024 * 1024,
    };
    let shape = shape();
    let keyspace = shape.iter_ks().next().unwrap().id();
    for count in [1, 2, 31] {
        // Each uniform size plus one mixed-size group.
        for variant in 0..=sizes.len() {
            let batches: Vec<_> = (0..count)
                .map(|index| {
                    let size = sizes[if variant == sizes.len() {
                        index % sizes.len()
                    } else {
                        variant
                    }];
                    CompleteBatch::new(
                        &shape,
                        BatchId {
                            database: [1; 16],
                            operation: [index as u8; 16],
                        },
                        BatchVersion {
                            epoch: 2,
                            sequence: index as u64,
                        },
                        vec![Mutation::Put {
                            keyspace,
                            key: Bytes::from(vec![index as u8; 32]),
                            value: Bytes::from(vec![index as u8; size]),
                        }],
                        bl,
                    )
                    .unwrap()
                })
                .collect();
            let canonical = CompleteGroup::new_live(&shape, batches.clone(), bl, gl).unwrap();
            let frame = LiveFrame::new(&shape, batches, bl, gl).unwrap();
            let mut encoded = vec![0xa5; frame.len()];
            frame.encode_into(&mut encoded).unwrap();
            assert_eq!(
                encoded,
                canonical.live_frame().unwrap(),
                "count={count} variant={variant}"
            );
            assert_eq!(
                frame.batch_offsets().collect::<Vec<_>>(),
                canonical.batch_offsets()
            );
            let payload = CrcFrame::read_from_slice(&encoded, 0).unwrap();
            let decoded = CompleteGroup::decode_slice(&shape, payload, bl, gl).unwrap();
            assert_eq!(decoded.batch_offsets(), canonical.batch_offsets());
            assert_eq!(decoded.batches().len(), count);
        }
    }
}

#[test]
fn live_in_place_validates_without_taking_an_encoding_buffer() {
    let (bl, gl) = limits();
    let shape = shape();
    let a = batch(1, 1);
    assert!(matches!(
        LiveFrame::new(&shape, vec![a.clone(), a.clone()], bl, gl),
        Err(GroupError::DuplicateIdentity)
    ));
    assert!(matches!(
        LiveFrame::new(&shape, vec![a.clone(), batch(2, 1)], bl, gl),
        Err(GroupError::DuplicateVersion)
    ));
    assert!(matches!(
        LiveFrame::new(
            &shape,
            vec![a.clone()],
            bl,
            GroupLimits {
                max_encoded_bytes: GROUP_HEADER_LEN + 16,
                ..gl
            }
        ),
        Err(GroupError::TooLarge)
    ));
    let frame = LiveFrame::new(&shape, vec![a], bl, gl).unwrap();
    assert_eq!(frame.encode_into(&mut []), Err(GroupError::InvalidFormat));
}

fn shape() -> KeyShape {
    KeyShape::new_single(32, 1, KeyType::uniform(1))
}

fn limits() -> (BatchLimits, GroupLimits) {
    (
        BatchLimits {
            max_operations: 16,
            max_encoded_bytes: 4096,
        },
        GroupLimits {
            max_batches: 8,
            max_encoded_bytes: 32768,
        },
    )
}

fn batch(id: u8, sequence: u64) -> CompleteBatch {
    let shape = shape();
    let keyspace = shape.iter_ks().next().unwrap().id();
    CompleteBatch::new(
        &shape,
        BatchId {
            database: [1; 16],
            operation: [id; 16],
        },
        BatchVersion { epoch: 2, sequence },
        vec![
            Mutation::Put {
                keyspace,
                key: Bytes::from(vec![id; 32]),
                value: Bytes::from(vec![id; 17]),
            },
            Mutation::Delete {
                keyspace,
                key: Bytes::from(vec![id.wrapping_add(1); 32]),
            },
        ],
        limits().0,
    )
    .unwrap()
}

#[test]
fn live_group_is_canonical_and_crc_covers_padding() {
    let (bl, gl) = limits();
    let batches = vec![batch(3, 2), batch(4, 1)];
    let ordinary = CompleteGroup::new(&shape(), batches.clone(), bl, gl).unwrap();
    let live = CompleteGroup::new_live(&shape(), batches, bl, gl).unwrap();
    assert_eq!(ordinary.batch_offsets(), live.batch_offsets());
    let frame = live.live_frame().unwrap();
    assert_eq!(frame.len() % LIVE_BLOCK_SIZE, 0);
    let payload = CrcFrame::read_from_slice(&frame, 0).unwrap();
    let recovered = CompleteGroup::decode(&shape(), payload.to_vec().into(), bl, gl).unwrap();
    assert_eq!(recovered.batch_offsets(), live.batch_offsets());
    let mut bad = payload.to_vec();
    *bad.last_mut().unwrap() = 1;
    assert_eq!(
        CompleteGroup::decode(&shape(), bad.into(), bl, gl).unwrap_err(),
        GroupError::InvalidPadding
    );
    let mut bad = payload.to_vec();
    bad[11] = 13;
    assert_eq!(
        CompleteGroup::decode(&shape(), bad.into(), bl, gl).unwrap_err(),
        GroupError::InvalidFormat
    );
    let mut bad = payload.to_vec();
    bad.extend_from_slice(&[0; LIVE_BLOCK_SIZE]);
    assert_eq!(
        CompleteGroup::decode(&shape(), bad.into(), bl, gl).unwrap_err(),
        GroupError::TrailingBytes
    );
    let mut torn = frame;
    *torn.last_mut().unwrap() = 1;
    assert!(CrcFrame::read_from_slice(&torn, 0).is_err());
    assert!(ordinary.live_frame().is_err());
    let small = GroupLimits {
        max_encoded_bytes: 1024,
        ..gl
    };
    let small_live = CompleteGroup::new_live(&shape(), vec![batch(3, 2)], bl, small).unwrap();
    assert!(small_live.encoded().len() > small.max_encoded_bytes);
    CompleteGroup::decode(&shape(), small_live.encoded().clone(), bl, small).unwrap();
}

#[test]
fn complete_group_roundtrips_and_exposes_native_inner_frames() {
    let (batch_limits, group_limits) = limits();
    // Physical grouping does not reorder logical versions or duplicate keys.
    let batches = vec![batch(3, 2), batch(4, 1)];
    let expected_size = CompleteGroup::encoded_len_for_batches(&batches).unwrap();
    let group = CompleteGroup::new(&shape(), batches, batch_limits, group_limits).unwrap();
    assert_eq!(group.encoded().len(), expected_size);
    let outer = CrcFrame::new(&group);
    let payload = CrcFrame::read_from_slice(outer.as_ref(), 0).unwrap();
    let recovered = CompleteGroup::decode(
        &shape(),
        Bytes::from(payload.to_vec()),
        batch_limits,
        group_limits,
    )
    .unwrap();
    for (i, offset) in recovered.batch_offsets().iter().enumerate() {
        assert_eq!(offset % 8, 0);
        let inner =
            CrcFrame::read_from_slice(outer.as_ref(), *offset as usize + FRAME_HEADER_LEN).unwrap();
        assert_eq!(inner, recovered.batches()[i].encoded().as_ref());
        assert_eq!(recovered.batches()[i].digest(), group.batches()[i].digest());
        assert_eq!(
            recovered.batches()[i].mutations(),
            group.batches()[i].mutations()
        );
    }
}

#[test]
fn direct_member_encoding_is_byte_identical_to_native_crc_frames() {
    let (batch_limits, group_limits) = limits();
    let keyspace = shape().iter_ks().next().unwrap().id();
    let batches: Vec<_> = [0, 1, 7, 8, 127, 128, 1024]
        .into_iter()
        .enumerate()
        .map(|(n, len)| {
            CompleteBatch::new(
                &shape(),
                BatchId {
                    database: [1; 16],
                    operation: [n as u8; 16],
                },
                BatchVersion {
                    epoch: 2,
                    sequence: n as u64,
                },
                vec![Mutation::Put {
                    keyspace,
                    key: vec![n as u8; 32].into(),
                    value: vec![n as u8; len].into(),
                }],
                batch_limits,
            )
            .unwrap()
        })
        .collect();
    let group = CompleteGroup::new(&shape(), batches, batch_limits, group_limits).unwrap();
    let mut expected = BytesMut::new();
    expected.put_slice(MAGIC);
    expected.put_u16(FORMAT_VERSION);
    expected.put_u16(0);
    expected.put_u32(group.batches().len() as u32);
    for batch in group.batches() {
        expected.put_slice(CrcFrame::new(batch).as_ref());
        expected.resize(align(expected.len()).unwrap(), 0);
    }
    assert_eq!(group.encoded().as_ref(), expected.as_ref());
    let recovered = CompleteGroup::decode(
        &shape(),
        group.encoded().clone(),
        batch_limits,
        group_limits,
    )
    .unwrap();
    for (actual, expected) in recovered.batches().iter().zip(group.batches()) {
        assert_eq!(actual.mutations(), expected.mutations());
        assert_eq!(actual.digest(), expected.digest());
    }
}

#[test]
fn decoded_group_detaches_arbitrary_mapped_backing_before_validation() {
    use std::io::{Seek, SeekFrom, Write};
    let (bl, gl) = limits();
    let original = CompleteGroup::new(&shape(), vec![batch(3, 1)], bl, gl).unwrap();
    let temp = tempdir::TempDir::new("group-owned-decode").unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(temp.path().join("input"))
        .unwrap();
    file.write_all(original.encoded()).unwrap();
    file.sync_all().unwrap();
    // SAFETY: the mapping's backing length is fixed for this whole test.
    let map = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };
    let mapped = Bytes::from_owner(map);
    let decoded = CompleteGroup::decode(&shape(), mapped.clone(), bl, gl).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    assert_eq!(mapped[0], b'X');
    assert_eq!(decoded.encoded(), original.encoded());
    assert_eq!(
        decoded.batches()[0].mutations(),
        original.batches()[0].mutations()
    );
    assert!(CompleteGroup::decode(&shape(), mapped, bl, gl).is_err());
}

#[test]
fn every_outer_and_inner_truncation_rejects_partial_group() {
    let (bl, gl) = limits();
    let group = CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap();
    let mut old_group = group.encoded().to_vec();
    old_group[..8].copy_from_slice(b"THGROUP1");
    assert_eq!(
        CompleteGroup::decode(&shape(), old_group.into(), bl, gl).unwrap_err(),
        GroupError::InvalidFormat
    );
    let outer = CrcFrame::new(&group);
    for cut in 0..outer.as_ref().len() {
        assert!(
            CrcFrame::read_from_slice(&outer.as_ref()[..cut], 0).is_err(),
            "outer cut {cut}"
        );
    }
    for cut in 0..group.encoded().len() {
        assert!(
            CompleteGroup::decode(&shape(), group.encoded().slice(..cut), bl, gl).is_err(),
            "inner cut {cut}"
        );
    }
}

#[test]
fn identity_version_epoch_and_capacity_validation_happens_before_emit() {
    let (bl, gl) = limits();
    let b = batch(3, 1);
    assert_eq!(
        CompleteGroup::new(&shape(), vec![b.clone(), b.clone()], bl, gl).unwrap_err(),
        GroupError::DuplicateIdentity
    );
    assert_eq!(
        CompleteGroup::new(&shape(), vec![b.clone(), batch(4, 1)], bl, gl).unwrap_err(),
        GroupError::DuplicateVersion
    );
    let tiny = GroupLimits {
        max_encoded_bytes: CompleteGroup::encoded_len_for_batches(std::slice::from_ref(&b))
            .unwrap()
            - 1,
        ..gl
    };
    assert_eq!(
        CompleteGroup::new(&shape(), vec![b.clone()], bl, tiny).unwrap_err(),
        GroupError::TooLarge
    );
    assert_eq!(
        CompleteGroup::new(&shape(), vec![], bl, gl).unwrap_err(),
        GroupError::InvalidBatchCount
    );
    assert_eq!(
        CompleteGroup::new(
            &shape(),
            vec![b.clone(), batch(4, 2)],
            bl,
            GroupLimits {
                max_batches: 1,
                ..gl
            }
        )
        .unwrap_err(),
        GroupError::InvalidBatchCount
    );
    let wrong = CompleteBatch::new(
        &shape(),
        BatchId {
            database: [2; 16],
            operation: [4; 16],
        },
        BatchVersion {
            epoch: 2,
            sequence: 2,
        },
        b.mutations().to_vec(),
        bl,
    )
    .unwrap();
    assert_eq!(
        CompleteGroup::new(&shape(), vec![b, wrong], bl, gl).unwrap_err(),
        GroupError::MixedAuthority
    );
    assert!(CompleteGroup::encoded_member_len(usize::MAX).is_err());
}

#[test]
fn immutable_members_are_revalidated_against_storage_schema_and_limits() {
    let (bl, gl) = limits();
    let b = batch(3, 1);
    let wrong_shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    assert!(matches!(
        CompleteGroup::new(&wrong_shape, vec![b.clone()], bl, gl),
        Err(GroupError::Batch(CodecError::InvalidKeyLength { .. }))
    ));
    let fewer = BatchLimits {
        max_operations: 1,
        ..bl
    };
    assert_eq!(
        CompleteGroup::new(&shape(), vec![b.clone()], fewer, gl).unwrap_err(),
        GroupError::Batch(CodecError::InvalidOperationCount)
    );
    let smaller = BatchLimits {
        max_encoded_bytes: b.encoded().len() - 1,
        ..bl
    };
    assert_eq!(
        CompleteGroup::new(&shape(), vec![b], smaller, gl).unwrap_err(),
        GroupError::Batch(CodecError::TooLarge)
    );

    let group = CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap();
    group.validate_for(&shape(), bl, gl).unwrap();
    assert!(matches!(
        group.validate_for(&wrong_shape, bl, gl),
        Err(GroupError::Batch(CodecError::InvalidKeyLength { .. }))
    ));
    assert_eq!(
        group.validate_for(
            &shape(),
            bl,
            GroupLimits {
                max_batches: 1,
                ..gl
            }
        ),
        Err(GroupError::InvalidBatchCount)
    );
    assert_eq!(
        group.validate_for(
            &shape(),
            bl,
            GroupLimits {
                max_encoded_bytes: group.encoded().len() - 1,
                ..gl
            }
        ),
        Err(GroupError::TooLarge)
    );
    assert_eq!(
        group.validate_for(&shape(), fewer, gl),
        Err(GroupError::Batch(CodecError::InvalidOperationCount))
    );
}

#[test]
fn corruption_rejected_even_with_recomputed_outer_crc() {
    let (bl, gl) = limits();
    let group = CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap();
    // Corruption of every byte must fail either group structure, member CRC,
    // digest or canonical padding even if a caller recomputes the outer CRC.
    for index in 0..group.encoded().len() {
        let mut bytes = group.encoded().to_vec();
        bytes[index] ^= 0x80;
        assert!(
            CompleteGroup::decode(&shape(), Bytes::from(bytes), bl, gl).is_err(),
            "byte {index}"
        );
    }
    let mut trailing = group.encoded().to_vec();
    trailing.extend_from_slice(&[0; 8]);
    assert_eq!(
        CompleteGroup::decode(&shape(), Bytes::from(trailing), bl, gl).unwrap_err(),
        GroupError::TrailingBytes
    );
}

#[test]
fn borrowed_and_owned_group_decoders_enforce_identical_validation() {
    let (bl, gl) = limits();
    let group = CompleteGroup::new_live(&shape(), vec![batch(1, 1)], bl, gl).unwrap();
    let valid = group.encoded().to_vec();
    let mut bad_crc = valid.clone();
    bad_crc[GROUP_HEADER_LEN + 8] ^= 1;
    let mut bad_padding = valid.clone();
    *bad_padding.last_mut().unwrap() = 1;
    let mut trailing = valid.clone();
    trailing.push(0);
    for (bytes, error) in [
        (valid, None),
        (bad_crc, Some(GroupError::InvalidMemberFrame)),
        (bad_padding, Some(GroupError::InvalidPadding)),
        (trailing, Some(GroupError::TrailingBytes)),
    ] {
        let borrowed = CompleteGroup::decode_slice(&shape(), &bytes, bl, gl);
        let owned = CompleteGroup::decode(&shape(), Bytes::from(bytes), bl, gl);
        match error {
            Some(error) => {
                assert_eq!(borrowed.unwrap_err(), error);
                assert_eq!(owned.unwrap_err(), error);
            }
            None => assert_eq!(borrowed.unwrap().encoded(), owned.unwrap().encoded()),
        }
    }
}

#[test]
fn immutable_groups_can_be_decoded_concurrently() {
    let (bl, gl) = limits();
    let group = std::sync::Arc::new(
        CompleteGroup::new(&shape(), vec![batch(3, 1), batch(4, 2)], bl, gl).unwrap(),
    );
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let group = group.clone();
            scope.spawn(move || {
                for _ in 0..16 {
                    let decoded =
                        CompleteGroup::decode(&shape(), group.encoded().clone(), bl, gl).unwrap();
                    assert_eq!(decoded.batches()[1].digest(), group.batches()[1].digest());
                }
            });
        }
    });
}
