//! Tests for the hybrid value path: bounded retry, identity-keyed cache and
//! member integrity.
//!
//! These are the S8 acceptance tests that need no engine: the retry policy, the
//! cache rekey (a relocated position keeps its cache entry), the CRC check over
//! the fetched member and the native key binding behind it. The crash-cut test
//! for a held latch and the end-to-end count of re-resolves belong to the
//! snapshot and golden suites (S12), which own a real engine and a crash hook.

use super::*;
use crate::crc::CrcFrame;
use crate::db::WalEntry;
use crate::distributed::remote_db::read_cache::ReadCache;
use crate::distributed::types::{BatchVersion, PhysicalValueAddress};
use crate::key_shape::{KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType};
use crate::wal::position::IndexPosition;
use std::io;

/// A one-keyspace shape; `ordinary_application_data` is the uniform keyspace
/// the Sui crosswalk uses for user data.
fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("ordinary_application_data", 1, 1, KeyType::uniform(1));
    builder.build()
}

fn keyspace(shape: &KeyShape) -> KeySpace {
    shape
        .iter_ks()
        .next()
        .expect("the shape has one keyspace")
        .id()
}

/// A position at `batch`/`operation_index` naming `address` with `crc`.
fn position(epoch: u64, sequence: u64, address: u64, crc: u32) -> VersionPosition {
    VersionPosition::from_member(
        BatchVersion { epoch, sequence },
        0,
        PhysicalValueAddress {
            partition: 1,
            segment_generation: epoch,
            offset: address,
            len: 64,
        },
        crc,
    )
}

/// One member frame `[length][crc][entry]` holding a put of `key`/`value`.
fn member_frame(shape: &KeyShape, key: &[u8], value: &[u8]) -> Bytes {
    let entry = WalEntry::Record(
        keyspace(shape),
        Bytes::copy_from_slice(key),
        Bytes::copy_from_slice(value),
        false,
    );
    Bytes::copy_from_slice(CrcFrame::new(&entry).as_ref())
}

/// One member frame `[length][crc][entry]` holding a delete of `key`.
fn tombstone_frame(shape: &KeyShape, key: &[u8]) -> Bytes {
    let entry = WalEntry::Remove(keyspace(shape), Bytes::copy_from_slice(key));
    Bytes::copy_from_slice(CrcFrame::new(&entry).as_ref())
}

#[test]
fn a_member_delete_of_the_requested_key_is_a_tombstone() {
    let shape = shape();
    let ks = keyspace(&shape);
    let frame = tombstone_frame(&shape, b"key-a");
    let position = position(1, 1, 4096, payload_crc(&frame));
    assert_eq!(
        decode_member_entry(&shape, ks, b"key-a", position, &frame).unwrap(),
        None
    );
}

#[test]
fn a_member_delete_of_another_key_is_rejected_not_reported_as_a_tombstone() {
    // Native maps a `Remove` to `None` without looking at its key. A CRC-valid
    // delete reached through a stale or mis-pointed position must not read as a
    // tombstone for this key: relocation skips tombstones and would drop the
    // live entry from the index.
    let shape = shape();
    let ks = keyspace(&shape);
    let frame = tombstone_frame(&shape, b"key-z");
    let position = position(1, 1, 4096, payload_crc(&frame));
    let error = decode_member_entry(&shape, ks, b"key-a", position, &frame).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .message()
            .contains("another key than the index asked for")
    );
}

/// The one failure a re-resolve can repair: the owner reports ENOENT for a
/// member whose file is gone.
fn retired() -> LocalDbError {
    LocalDbError::with_kind(io::ErrorKind::NotFound, "member file is gone")
}

/// The CRC a publisher stores in the position for `frame`: the native member
/// entry, which is everything after the member's own 8-byte header.
fn payload_crc(frame: &[u8]) -> u32 {
    crc32fast::hash(&frame[MEMBER_HEADER_LEN..])
}

#[test]
fn retry_does_not_run_the_second_attempt_after_a_success() {
    let mut attempts = 0;
    let value = retry_once(
        Ok::<_, LocalDbError>(7u32),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || Ok(()),
        || {
            attempts += 1;
            Ok(9)
        },
    )
    .unwrap();
    assert_eq!(value, 7);
    assert_eq!(attempts, 0, "a successful read never re-resolves");
}

#[test]
fn retry_runs_the_second_attempt_once_for_a_retired_address() {
    let mut attempts = 0;
    let value = retry_once(
        Err(retired()),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || Ok(()),
        || {
            attempts += 1;
            Ok(9u32)
        },
    )
    .unwrap();
    assert_eq!(value, 9);
    assert_eq!(attempts, 1, "one re-resolve, never a loop");
}

#[test]
fn retry_after_a_second_failure_is_the_typed_evicted_error() {
    let mut attempts = 0;
    let error = retry_once(
        Err::<u32, _>(retired()),
        BatchVersion {
            epoch: 1,
            sequence: 7,
        },
        || Ok(()),
        || {
            attempts += 1;
            Err(LocalDbError::new("generation is not served"))
        },
    )
    .unwrap_err();
    assert_eq!(attempts, 1);
    assert!(
        error.is_evicted_during_read(),
        "unexpected message: {}",
        error.message()
    );
    // Both causes stay in the message; the kind is fixed so callers do not have
    // to match prose. The first cause is the one `retired()` builds: the retry
    // classifies by kind (`member_read_is_retired`), so the wording is the
    // fixture's own, not the owner's.
    assert!(error.message().contains("member file is gone"));
    assert!(error.message().contains("generation is not served"));
    // `Other`, not `NotFound`: `NotFound` is the value path's retry marker
    // (`member_read_is_retired`), and a typed eviction is terminal
    // (`LocalDbError::evicted_during_read`).
    assert_eq!(error.kind(), io::ErrorKind::Other);
}

#[test]
fn a_corruption_failure_is_never_retried() {
    // The real constructor, not a literal: the marker is the kind `member_error`
    // sets, so this test fails if a member check ever loses it.
    let position = position(1, 1, 4096, 17);
    let mut attempts = 0;
    let error = retry_once(
        Err::<u32, _>(super::member_error(position, "member payload CRC mismatch")),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || Ok(()),
        || {
            attempts += 1;
            Ok(9u32)
        },
    )
    .unwrap_err();
    assert_eq!(attempts, 0, "corruption propagates without a re-resolve");
    assert!(!error.is_evicted_during_read());
    assert!(error.message().contains("CRC mismatch"));
}

#[test]
fn the_invalid_position_sentinel_is_not_retried() {
    // Handing the reader a sentinel is a caller bug; it is a local rejection
    // (`InvalidData`), so the path fails closed instead of re-resolving.
    let sentinel = VersionPosition::INVALID;
    let mut attempts = 0;
    let error = retry_once(
        Err::<u32, _>(super::member_error(sentinel, "invalid position sentinel")),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || Ok(()),
        || {
            attempts += 1;
            Ok(9u32)
        },
    )
    .unwrap_err();
    assert_eq!(attempts, 0);
    assert!(!error.is_evicted_during_read());
}

#[test]
fn a_deadline_or_poison_failure_is_not_retried() {
    // Timed out, poisoned and transport failures fail fast with their own error:
    // only ENOENT is repaired by a re-resolve, so none of these can be labelled
    // an eviction or cost a second `read_timeout`.
    for kind in [
        io::ErrorKind::TimedOut,
        io::ErrorKind::ConnectionReset,
        io::ErrorKind::Other,
    ] {
        let mut attempts = 0;
        let error = retry_once(
            Err::<u32, _>(LocalDbError::with_kind(
                kind,
                "read authority is poisoned or closed",
            )),
            BatchVersion {
                epoch: 1,
                sequence: 1,
            },
            || Ok(()),
            || {
                attempts += 1;
                Ok(9u32)
            },
        )
        .unwrap_err();
        assert_eq!(attempts, 0, "{kind:?} must not be retried");
        assert_eq!(error.kind(), kind);
        assert!(!error.is_evicted_during_read());
    }
}

#[test]
fn a_poisoned_engine_fails_closed_without_a_second_attempt() {
    // The guard runs before the re-resolve: poison carries its own cause and the
    // fail-stop contract forbids masking it.
    let mut attempts = 0;
    let error = retry_once(
        Err::<u32, _>(retired()),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || {
            Err(LocalDbError::with_kind(
                io::ErrorKind::Other,
                "writer poisoned: EIO",
            ))
        },
        || {
            attempts += 1;
            Ok(9u32)
        },
    )
    .unwrap_err();
    assert_eq!(attempts, 0, "the guard stops the second attempt");
    assert!(error.message().contains("writer poisoned: EIO"));
    assert!(!error.is_evicted_during_read());
}

#[test]
fn a_second_attempt_that_fails_a_member_check_is_not_an_eviction() {
    // The re-resolved address holds the wrong bytes: that is corruption at the
    // new address, not an eviction, so it is reported as a member failure.
    let position = position(1, 2, 8192, 23);
    let mut attempts = 0;
    let error = retry_once(
        Err::<u32, _>(retired()),
        BatchVersion {
            epoch: 1,
            sequence: 2,
        },
        || Ok(()),
        || {
            attempts += 1;
            Err(super::member_error(position, "member payload CRC mismatch"))
        },
    )
    .unwrap_err();
    assert_eq!(attempts, 1);
    assert!(!error.is_evicted_during_read());
    assert!(error.message().contains("CRC mismatch"));
}

#[test]
fn a_retired_member_is_classified_recoverable_and_does_not_poison() {
    // The inline path consults this predicate before poisoning the read
    // service: a retired address is recoverable by one re-resolve, corruption
    // and transport failures are not.
    assert!(crate::distributed::remote_db::pending_read::member_read_is_retired(&retired()));
    for kind in [
        io::ErrorKind::InvalidData,
        io::ErrorKind::TimedOut,
        io::ErrorKind::Other,
    ] {
        assert!(
            !crate::distributed::remote_db::pending_read::member_read_is_retired(
                &LocalDbError::with_kind(kind, "read authority is poisoned")
            ),
            "{kind:?} must not be classified as recoverable"
        );
    }
    // And a retired first error does take exactly one re-resolve.
    let mut attempts = 0;
    let value = retry_once(
        Err::<u32, _>(retired()),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || Ok(()),
        || {
            attempts += 1;
            Ok(9u32)
        },
    )
    .unwrap();
    assert_eq!((value, attempts), (9, 1));
}

#[test]
fn a_dropped_entry_re_resolves_to_not_found() {
    // A delete landed between the lookup and the fetch: the index re-resolve
    // finds no position, and that is an absent value, not an eviction error.
    let value = retry_once(
        Err::<Option<u32>, _>(retired()),
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        || Ok(()),
        || Ok(None),
    )
    .unwrap();
    assert_eq!(value, None);
}

#[test]
fn a_relocated_position_hits_the_same_cache_entry() {
    let shape = shape();
    let ks = keyspace(&shape);
    let original = position(1, 4, 4096, 17);
    let relocated = original.moved_to(PhysicalValueAddress {
        partition: 1,
        segment_generation: 2,
        offset: 8192,
        len: 64,
    });
    assert!(
        !original.same_physical(&relocated),
        "the fixture must really move the bytes"
    );
    let cache = ReadCache::new(1024 * 1024);
    let value = Bytes::from_static(b"cached value");
    let prepared = cache.prepare(original.identity(), ks.0, &value);
    assert!(cache.insert_prepared(prepared));
    // The re-pointed position resolves to the same logical write, so the cache
    // serves it without another owner fetch.
    assert_eq!(relocated.identity(), original.identity());
    assert_eq!(cache.get(relocated.identity(), ks.0), Some(value));
}

#[test]
fn a_valid_member_decodes_to_its_value() {
    let shape = shape();
    let ks = keyspace(&shape);
    let key = b"key-b";
    let frame = member_frame(&shape, key, b"value-b");
    let position = position(1, 1, 4096, payload_crc(&frame));
    assert_eq!(
        decode_member(&shape, ks, key, position, &frame).unwrap(),
        Some(Bytes::from_static(b"value-b"))
    );
}

#[test]
fn the_entry_decoder_returns_the_members_full_key_with_its_value() {
    // A scan yields the full key while the index holds only the reduced one, so
    // the entry decoder must return the key the member carries, not drop it.
    let shape = shape();
    let ks = keyspace(&shape);
    let key = b"key-b";
    let frame = member_frame(&shape, key, b"value-b");
    let position = position(1, 1, 4096, payload_crc(&frame));
    assert_eq!(
        decode_member_entry(&shape, ks, key, position, &frame).unwrap(),
        Some((Bytes::copy_from_slice(key), Bytes::from_static(b"value-b")))
    );
}

/// A keyspace whose index holds only the first four bytes of an eight-byte key
/// (`Reduction`), the shape Sui's reduced and hash keyspaces take: the index key
/// is already reduced, so it differs from the member's full key.
fn reduced_shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config_indexing(
        "reduced",
        KeyIndexing::key_reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default(),
    );
    builder.build()
}

#[test]
fn a_reduced_keyspace_member_binds_by_the_reduced_key_and_returns_the_full_key() {
    // The index hands the reader the reduced key `abcd`; the member carries the
    // full key. Reducing both sides would reduce an already-reduced key, which
    // for a hash keyspace is a hash of a hash and rejects every member.
    let shape = reduced_shape();
    let ks = keyspace(&shape);
    let full = b"abcdWXYZ";
    let frame = member_frame(&shape, full, b"value");
    let position = position(1, 1, 4096, payload_crc(&frame));
    assert_eq!(
        decode_member_entry(&shape, ks, b"abcd", position, &frame).unwrap(),
        Some((Bytes::copy_from_slice(full), Bytes::from_static(b"value")))
    );
}

#[test]
fn a_reduced_keyspace_member_with_another_reduced_key_is_rejected() {
    let shape = reduced_shape();
    let ks = keyspace(&shape);
    let frame = member_frame(&shape, b"zzzzWXYZ", b"value");
    let position = position(1, 1, 4096, payload_crc(&frame));
    let error = decode_member_entry(&shape, ks, b"abcd", position, &frame).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .message()
            .contains("another key than the index asked for"),
        "unexpected message: {}",
        error.message()
    );
}

/// A keyspace indexed by a hash of the key. `Reduction(8, 0..4)` is idempotent
/// (`reduce(reduce(k)) == reduce(k)`), so it cannot tell a double reduction from
/// a single one; only a hash can. It is also the keyspace for which native's
/// `need_check_index_key()` is false, so it distinguishes that rule too.
fn hashed_shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config_indexing(
        "hashed",
        KeyIndexing::hash(),
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default(),
    );
    builder.build()
}

#[test]
fn a_hashed_keyspace_member_binds_by_the_hash_and_returns_the_full_key() {
    // The index holds the hash of the key. Reducing the index key again would
    // hash the hash; comparing the member's full key with the hash directly (the
    // `need_check_index_key() == false` branch) would never match. Either would
    // reject every member of a hashed keyspace.
    let shape = hashed_shape();
    let ks = keyspace(&shape);
    let full = b"abcdWXYZ";
    let indexed = shape.ks(ks).reduce_key(full).into_owned();
    assert_ne!(
        indexed.as_slice(),
        full.as_slice(),
        "the index key is a hash"
    );
    let frame = member_frame(&shape, full, b"value");
    let position = position(1, 1, 4096, payload_crc(&frame));
    assert_eq!(
        decode_member_entry(&shape, ks, &indexed, position, &frame).unwrap(),
        Some((Bytes::copy_from_slice(full), Bytes::from_static(b"value")))
    );
}

#[test]
fn a_hashed_keyspace_member_whose_hash_differs_is_rejected_not_reported_absent() {
    // A key binding failure is an error: `Ok(None)` means only "tombstone", and a
    // caller that skips a tombstone (relocation drops it from the index) must
    // never receive one for a mismatch.
    let shape = hashed_shape();
    let ks = keyspace(&shape);
    let other = shape.ks(ks).reduce_key(b"another!").into_owned();
    let frame = member_frame(&shape, b"abcdWXYZ", b"value");
    let position = position(1, 1, 4096, payload_crc(&frame));
    let error = decode_member_entry(&shape, ks, &other, position, &frame).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .message()
            .contains("another key than the index asked for")
    );
}

#[test]
fn a_crc_flip_in_a_member_fails_the_read() {
    let shape = shape();
    let ks = keyspace(&shape);
    let key = b"key-b";
    let frame = member_frame(&shape, key, b"value-b");
    let position = position(1, 1, 4096, payload_crc(&frame));
    let mut flipped = frame.to_vec();
    *flipped.last_mut().unwrap() ^= 1;
    let error =
        decode_member(&shape, ks, key, position, &Bytes::copy_from_slice(&flipped)).unwrap_err();
    assert!(
        error.message().contains("member payload CRC mismatch"),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn a_swapped_member_fails_even_when_its_crc_is_self_consistent() {
    let shape = shape();
    let ks = keyspace(&shape);
    let key = b"key-b";
    // The owner now holds another member at this address, with a valid frame.
    let other = member_frame(&shape, b"key-c", b"value-c");
    // Even a position whose CRC matches those bytes must fail: the native
    // decoder binds the entry's key to the key the index asked for, so a
    // substituted member cannot be served as this key's value.
    let position = position(1, 1, 4096, payload_crc(&other));
    let error = decode_member(&shape, ks, key, position, &other).unwrap_err();
    assert!(
        !error.message().is_empty(),
        "the swapped member must be rejected with a reason"
    );
}

#[test]
fn a_member_whose_length_header_disagrees_is_rejected_before_decoding() {
    let shape = shape();
    let ks = keyspace(&shape);
    let key = b"key-b";
    let mut frame = member_frame(&shape, key, b"value-b").to_vec();
    // Declare one more payload byte than the frame carries.
    let declared = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) + 1;
    frame[0..4].copy_from_slice(&declared.to_be_bytes());
    let position = position(1, 1, 4096, crc32fast::hash(&frame[MEMBER_HEADER_LEN..]));
    let error =
        decode_member(&shape, ks, key, position, &Bytes::copy_from_slice(&frame)).unwrap_err();
    assert!(
        error
            .message()
            .contains("member frame length disagrees with its header"),
        "unexpected message: {}",
        error.message()
    );
}

#[test]
fn a_truncated_member_is_rejected_before_decoding() {
    let shape = shape();
    let ks = keyspace(&shape);
    let position = position(1, 1, 4096, 0);
    let error =
        decode_member(&shape, ks, b"key-b", position, &Bytes::from_static(&[0; 4])).unwrap_err();
    assert!(
        error.message().contains("shorter than its header"),
        "unexpected message: {}",
        error.message()
    );
}

/// A member frame around raw entry bytes, with the length header and the payload
/// CRC a coordinator would have indexed for them.
fn raw_member_frame(payload: &[u8]) -> Bytes {
    let mut frame = Vec::with_capacity(MEMBER_HEADER_LEN + payload.len());
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes());
    frame.extend_from_slice(payload);
    Bytes::copy_from_slice(&frame)
}

#[test]
fn a_crc_valid_entry_that_is_not_a_put_or_delete_is_invalid_data() {
    // Only a put or a delete is a member. Every other entry — including shapes
    // the native decoder would `panic!` on — must be a typed content failure that
    // the value path never retries, through the reader's decode and through the
    // native wrapper itself.
    let shape = shape();
    let ks = keyspace(&shape);
    let desc = shape.ks(ks);
    let encoded = |entry: &WalEntry| {
        Bytes::copy_from_slice(&CrcFrame::new(entry).as_ref()[MEMBER_HEADER_LEN..])
    };
    let cases: Vec<(&str, Bytes, &str)> = vec![
        (
            "index entry",
            encoded(&WalEntry::Index(ks, Bytes::from_static(b"index-body"))),
            "non-record entry",
        ),
        (
            "batch start",
            encoded(&WalEntry::BatchStart(3)),
            "non-record entry",
        ),
        (
            "drop cells",
            encoded(&WalEntry::DropCells(
                ks,
                crate::cell::CellId::Integer(0),
                crate::cell::CellId::Integer(1),
            )),
            "non-record entry",
        ),
        (
            "compressed batch",
            encoded(&WalEntry::CompressedBatch(
                crate::compressed_batch::BatchCodec::Lz4,
                4,
                Bytes::from_static(b"body"),
            )),
            "non-record entry",
        ),
        (
            "unknown entry type",
            Bytes::from_static(&[0xEE, 1, 2, 3]),
            "non-record entry",
        ),
        ("empty entry", Bytes::new(), "holds no entry"),
        (
            "record shorter than its header",
            Bytes::from_static(&[1, 0, 0]),
            "truncated record entry",
        ),
        (
            "record whose key overruns the entry",
            Bytes::from_static(&[1, 0, 0xFF, 0xFF, b'k']),
            "truncated record entry",
        ),
        (
            "delete without a keyspace",
            Bytes::from_static(&[3]),
            "truncated delete entry",
        ),
    ];
    for (name, payload, expected) in cases {
        let frame = raw_member_frame(&payload);
        let position = position(1, 1, 4096, crc32fast::hash(&payload));
        let error = decode_member_entry(&shape, ks, b"key-a", position, &frame).expect_err(name);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{name}");
        assert!(
            error.message().contains(expected),
            "{name}: unexpected message: {}",
            error.message()
        );
        let native = crate::db::decode_record_frame(desc, b"key-a", payload).expect_err(name);
        assert!(
            matches!(&native, crate::db::DbError::Io(inner) if inner.kind() == io::ErrorKind::InvalidData),
            "{name}: native wrapper returned {native:?}"
        );
    }
}

#[test]
fn a_decoded_member_shares_the_fetched_allocation() {
    // The value handed to the caller is a slice of the fetched frame: no payload
    // byte is copied between the owner's reply and the caller.
    let shape = shape();
    let ks = keyspace(&shape);
    let frame = member_frame(&shape, b"key-b", b"value-b");
    let position = position(1, 1, 4096, payload_crc(&frame));
    let (_, value) = decode_member_entry(&shape, ks, b"key-b", position, &frame)
        .unwrap()
        .unwrap();
    let frame_range = frame.as_ptr() as usize..frame.as_ptr() as usize + frame.len();
    let value_start = value.as_ptr() as usize;
    assert!(
        frame_range.contains(&value_start) && value_start + value.len() <= frame_range.end,
        "the value was copied out of the frame"
    );
}

/// The latched-view rule: a read whose member the owner no longer holds, and
/// whose re-resolve names the same physical member, answers absent. That is
/// native's result for an unreachable position (`Db::resolve_record_frame`
/// returns `None`, and `DbCheckpoint` reads through it), and it is why a held
/// checkpoint does not need retirement to keep its files (`dead_wal_files`).
/// The live read of the same state keeps the typed eviction, which
/// [`retry_after_a_second_failure_is_the_typed_evicted_error`] pins.
#[test]
fn a_latched_read_of_a_reclaimed_member_answers_absent() {
    let evicted = || {
        retry_once(
            Err::<Option<u32>, _>(retired()),
            BatchVersion {
                epoch: 1,
                sequence: 7,
            },
            || Ok(()),
            || {
                Err(LocalDbError::new(
                    "re-resolve returned the same physical member",
                ))
            },
        )
    };
    assert_eq!(
        latched_absent(evicted(), true).expect("a latched read never fails on eviction"),
        None,
        "a reclaimed frame reads as absent on a held cut"
    );
    assert!(evicted().is_err(), "the live read keeps the typed eviction");
    assert!(
        latched_absent(Err::<Option<u32>, _>(retired()), true).is_err(),
        "only a terminal eviction maps to absent; a first transport failure still propagates"
    );
}
