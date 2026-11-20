//! `decode_record_entry`: the one entry decode that the native read path's
//! batch search and the distributed coordinator's member decode share.

use super::*;
use crate::compressed_batch::{BatchCodec, CompressedBatch};
use crate::key_shape::{KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType};

fn shape(indexing: KeyIndexing) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config_indexing(
        "k",
        indexing,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default(),
    );
    builder.build()
}

fn record(key: &[u8], value: &[u8]) -> WalEntry {
    WalEntry::Record(
        KeySpace::first(),
        Bytes::from(key.to_vec()),
        Bytes::from(value.to_vec()),
        false,
    )
}

fn compressed(entries: &[WalEntry]) -> WalEntry {
    let batch = CompressedBatch::encode(entries, BatchCodec::Lz4);
    WalEntry::CompressedBatch(batch.codec, batch.uncompressed_len, batch.body)
}

fn decode(shape: &KeyShape, indexed_key: &[u8], entry: WalEntry) -> Option<(Bytes, Bytes)> {
    decode_record_entry(shape.ks(KeySpace::first()), indexed_key, entry)
        .expect("a well-formed record entry decodes")
}

#[test]
fn plain_record_is_returned_as_is() {
    let shape = shape(KeyIndexing::fixed(4));
    let (key, value) = decode(&shape, b"abcd", record(b"abcd", b"value")).unwrap();
    assert_eq!(key.as_ref(), b"abcd");
    assert_eq!(value.as_ref(), b"value");
}

#[test]
fn single_record_frame_is_not_key_checked() {
    // The position addresses this exact frame and the caller has verified its
    // integrity, so the indexed key is not compared against the stored key.
    let shape = shape(KeyIndexing::fixed(4));
    let (key, _) = decode(&shape, b"zzzz", record(b"abcd", b"v")).unwrap();
    assert_eq!(key.as_ref(), b"abcd");
}

#[test]
fn empty_value_record_is_returned() {
    let shape = shape(KeyIndexing::fixed(4));
    let (key, value) = decode(&shape, b"abcd", record(b"abcd", b"")).unwrap();
    assert_eq!(key.as_ref(), b"abcd");
    assert!(value.is_empty());
}

#[test]
fn tombstone_has_no_value() {
    let shape = shape(KeyIndexing::fixed(4));
    let entry = WalEntry::Remove(KeySpace::first(), Bytes::from(b"abcd".to_vec()));
    assert!(decode(&shape, b"abcd", entry).is_none());
}

#[test]
fn compressed_batch_member_is_found_by_index_key() {
    let shape = shape(KeyIndexing::fixed(4));
    let batch = compressed(&[
        record(b"aaaa", b"1"),
        record(b"bbbb", b"2"),
        record(b"cccc", b"3"),
    ]);
    let (key, value) = decode(&shape, b"bbbb", batch).unwrap();
    assert_eq!(key.as_ref(), b"bbbb");
    assert_eq!(value.as_ref(), b"2");
}

#[test]
fn compressed_batch_without_the_key_yields_none() {
    let shape = shape(KeyIndexing::fixed(4));
    let batch = compressed(&[record(b"aaaa", b"1"), record(b"bbbb", b"2")]);
    assert!(decode(&shape, b"zzzz", batch).is_none());
}

#[test]
fn compressed_batch_ignores_tombstones_and_other_keyspaces() {
    let shape = shape(KeyIndexing::fixed(4));
    let other_keyspace = WalEntry::Record(
        KeySpace(5),
        Bytes::from(b"bbbb".to_vec()),
        Bytes::from(b"other".to_vec()),
        false,
    );
    let batch = compressed(&[
        WalEntry::Remove(KeySpace::first(), Bytes::from(b"bbbb".to_vec())),
        other_keyspace,
        record(b"bbbb", b"mine"),
    ]);
    let (_, value) = decode(&shape, b"bbbb", batch).unwrap();
    assert_eq!(value.as_ref(), b"mine");
}

/// With key reduction the index stores the reduced key, so a batch member is
/// matched on its reduced form, and the full stored key is what comes back.
#[test]
fn compressed_batch_member_is_matched_on_the_reduced_key() {
    let shape = shape(KeyIndexing::key_reduction(8, 0..4));
    let batch = compressed(&[record(b"aaaa0001", b"1"), record(b"bbbb0002", b"2")]);
    let (key, value) = decode(&shape, b"bbbb", batch).unwrap();
    assert_eq!(key.as_ref(), b"bbbb0002");
    assert_eq!(value.as_ref(), b"2");
}

#[test]
#[should_panic(expected = "Unexpected wal entry where expected record/batch")]
fn index_entry_is_not_a_record() {
    let shape = shape(KeyIndexing::fixed(4));
    let _ = decode_record_entry(
        shape.ks(KeySpace::first()),
        b"abcd",
        WalEntry::Index(KeySpace::first(), Bytes::new()),
    );
}
