use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;
use crate::distributed::types::{
    BatchId, BatchVersion, MemberChecksum, PayloadDigest, PhysicalValueAddress, RecordDescriptor,
    RecordKind,
};
use crate::key_shape::{KeyShape, KeyType};
use std::collections::BTreeMap;

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

fn reference(sequence: u64, len: u32) -> (Bytes, ValueRecordReference) {
    (
        (sequence as u32).to_be_bytes().to_vec().into(),
        ValueRecordReference {
            batch_id: BatchId {
                database: [1; 16],
                operation: [sequence as u8; 16],
            },
            batch_version: BatchVersion { epoch: 1, sequence },
            batch_digest: PayloadDigest([1; 32]),
            batch_address: PhysicalValueAddress {
                partition: 0,
                segment_generation: 1,
                offset: sequence * 4096,
                len,
            },
            descriptor: RecordDescriptor {
                operation_index: 0,
                offset: 0,
                len,
                keyspace: 0,
                kind: RecordKind::Put,
                key_len: 4,
                value_offset: 8,
                value_len: len.saturating_sub(8),
                checksum: MemberChecksum(1),
            },
        },
    )
}

fn bounded_page(rows: Vec<(Bytes, ValueRecordReference)>) -> Result<ScanPage, LocalDbError> {
    let mut page = ScanPage {
        rows: Vec::new(),
        charged_bytes: 0,
    };
    for (key, reference) in rows {
        let Some(charge) = page.next_charge(&key, &reference)? else {
            break;
        };
        page.charged_bytes = charge;
        page.rows.push((key, reference));
        if page.oversized() {
            break;
        }
    }
    Ok(page)
}

#[test]
fn byte_budget_bounds_page_and_oversized_record_is_alone() {
    // Metadata only: no large allocation is necessary to exercise the bounds.
    let rows = (0..256).map(|i| reference(i, 128 * 1024)).collect();
    let page = bounded_page(rows).unwrap();
    assert!(page.rows.len() < 32);
    assert!(page.charged_bytes <= PAGE_BYTES);
    let oversized = bounded_page(vec![reference(0, PAGE_BYTES as u32), reference(1, 128)]).unwrap();
    assert_eq!(oversized.rows.len(), 1);
    assert!(oversized.oversized());
    let small_then_large =
        bounded_page(vec![reference(0, 128), reference(1, PAGE_BYTES as u32)]).unwrap();
    assert_eq!(small_then_large.rows.len(), 1);
    assert!(!small_then_large.oversized());
    assert!(
        bounded_page(vec![reference(
            0,
            crate::distributed::tcp::HARD_FRAME_LIMIT as u32
        )])
        .is_err()
    );
}

#[test]
fn exact_ordered_row_rejects_mismatched_key_keyspace_and_tombstone() {
    let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    let key = 3u32.to_be_bytes();
    assert!(checked_row(&shape, KeySpace(0), &key, put(3, 9)).is_ok());
    assert!(checked_row(&shape, KeySpace(0), &key, put(4, 9)).is_err());
    let wrong_space = Mutation::Put {
        keyspace: KeySpace(1),
        key: key.to_vec().into(),
        value: Bytes::new(),
    };
    assert!(checked_row(&shape, KeySpace(0), &key, wrong_space).is_err());
    let tombstone = Mutation::Delete {
        keyspace: KeySpace(0),
        key: key.to_vec().into(),
    };
    assert!(checked_row(&shape, KeySpace(0), &key, tombstone).is_err());
}

#[test]
fn multi_page_scan_matches_serial_cut_updates_deletes_and_reopen() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let mut expected = BTreeMap::new();
    for batch in 0..80u32 {
        let mutations = (batch * 4..batch * 4 + 4)
            .map(|key| {
                expected.insert(key.to_be_bytes().to_vec(), vec![key as u8; 32]);
                put(key, key as u8)
            })
            .collect();
        db.submit(fixture.operation(batch % 5, batch as u64), mutations)
            .unwrap();
        if batch % 32 == 31 {
            db.drain().unwrap();
        }
    }
    db.drain().unwrap();
    let checkpoint = db.checkpoint().unwrap();
    let pin = {
        let index = db.index.read();
        index.pin_cut(index.latest_cut()).unwrap()
    };
    let captured_rows: Vec<_> = expected
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    db.write_batch(
        fixture.operation(2, 100),
        vec![
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: 17u32.to_be_bytes().to_vec().into(),
            },
            put(35, 211),
        ],
    )
    .unwrap();
    db.write_batch(
        fixture.operation(1, 102),
        vec![
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: 255u32.to_be_bytes().to_vec().into(),
            },
            put(256, 212),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: 257u32.to_be_bytes().to_vec().into(),
            },
        ],
    )
    .unwrap();
    expected.remove(255u32.to_be_bytes().as_slice());
    expected.insert(256u32.to_be_bytes().to_vec(), vec![212; 32]);
    expected.remove(257u32.to_be_bytes().as_slice());
    expected.remove(17u32.to_be_bytes().as_slice());
    expected.insert(35u32.to_be_bytes().to_vec(), vec![211; 32]);
    db.clear_read_cache();
    let expected_rows: Vec<_> = expected
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let as_rows = |rows: Vec<(Bytes, Bytes)>| {
        rows.into_iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>()
    };
    let mut cursor = db
        .index
        .read()
        .scan_cursor(KeySpace(0), None, false, pin.cut())
        .unwrap();
    let first = db.next_scan_page(&mut cursor).unwrap().unwrap();
    assert_eq!(first.rows.len(), PAGE_RECORDS);
    assert_eq!(
        as_rows(
            db.scan_prefetched(KeySpace(0), &mut cursor, Some(first), pin)
                .unwrap()
        ),
        captured_rows,
        "captured cut must span page boundaries despite later updates/deletes"
    );
    assert_eq!(as_rows(db.scan(KeySpace(0)).unwrap()), expected_rows);
    assert_eq!(
        db.read_cache_stats().inserts,
        0,
        "scan must not fill point cache"
    );
    assert_eq!(
        db.get_at(KeySpace(0), &35u32.to_be_bytes(), &checkpoint)
            .unwrap()
            .unwrap()
            .as_ref(),
        &[35; 32]
    );
    db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let inserts = db.read_cache_stats().inserts;

    // This admitted write is outside the captured published cut. A scan must
    // neither publish it nor implicitly change acknowledgement semantics.
    let prefix = db.publication.published_prefix();
    db.submit(fixture.operation(3, 101), vec![put(400, 77)])
        .unwrap();
    assert_eq!(as_rows(db.scan(KeySpace(0)).unwrap()), expected_rows);
    assert_eq!(db.publication.published_prefix(), prefix);
    assert_eq!(db.read_cache_stats().inserts, inserts);
    db.config.scan_prefetch_pages = 1;
    assert_eq!(as_rows(db.scan(KeySpace(0)).unwrap()), expected_rows);
    db.config.scan_prefetch_pages = 2;
    db.flush_index().unwrap();
    expected.insert(400u32.to_be_bytes().to_vec(), vec![77; 32]);
    let expected_rows: Vec<_> = expected.into_iter().collect();
    assert_eq!(as_rows(db.scan(KeySpace(0)).unwrap()), expected_rows);
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(as_rows(reopened.scan(KeySpace(0)).unwrap()), expected_rows);
    reopened.shutdown().unwrap();
}

#[test]
fn later_page_node_completes_while_first_node_is_explicitly_stalled() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut db = fixture.open();
    let rows = db
        .index
        .read()
        .scan(KeySpace(0), None, false, 2, IndexReadCut::Latest)
        .unwrap();
    let first_node = db.partition_indexes[&rows[0].1.batch_address.partition];
    let second_node = db.partition_indexes[&rows[1].1.batch_address.partition];
    assert_ne!(first_node, second_node);
    let pin = db.index.read().pin_cut(IndexReadCut::Latest).unwrap();
    let connection = db.reads.lane_client(first_node, 0);
    let held = connection.lock();
    let first = db
        .dispatch_scan_page(bounded_page(vec![rows[0].clone()]).unwrap(), pin.clone())
        .unwrap();
    let mut second = db
        .dispatch_scan_page(bounded_page(vec![rows[1].clone()]).unwrap(), pin)
        .unwrap();
    let (slots, receiver) = second.batch.reads.pop_back().unwrap();
    let (value, _) = receiver.finish().unwrap();
    assert_eq!(value, vec![put(1, 8)]);
    assert!(first.batch.reads[0].1.try_result().unwrap().is_none());
    for (slot, value) in slots.into_iter().zip(value) {
        second.batch.values[slot] = Some(value);
    }
    drop(held);
    let mut output = Vec::new();
    db.finish_scan_page(KeySpace(0), first, &mut output)
        .unwrap();
    db.finish_scan_page(KeySpace(0), second, &mut output)
        .unwrap();
    assert_eq!(
        output
            .iter()
            .map(|(key, _)| key.to_vec())
            .collect::<Vec<_>>(),
        vec![0u32.to_be_bytes().to_vec(), 1u32.to_be_bytes().to_vec()]
    );
    db.shutdown().unwrap();
}

#[test]
fn observed_scan_fence_failure_blocks_later_cached_reads() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut db = fixture.open();
    db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let next_epoch = db.epoch() + 1;
    let reference = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    db.clients[node].client.lock().fence_to(next_epoch).unwrap();
    assert!(db.scan(KeySpace(0)).is_err());
    assert!(!db.healthy);
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
}
