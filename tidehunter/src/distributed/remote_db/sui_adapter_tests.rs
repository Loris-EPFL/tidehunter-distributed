//! Sui adapter tests over the five-owner leased fixture: paging, bounds,
//! snapshots, grouped scans, key reduction, cell pruning and reopen. Moved out
//! of `tests.rs` to keep that file under the length cap; the test names are
//! unchanged.
use super::pipeline_tests::Fixture;
use super::tests::recovery_metrics;
use super::*;
use crate::distributed::hybrid::position::VersionPosition;
use crate::index::index_format::IndexIterCaches;

/// Every index entry of `ks` at `cut` in walk order: `(reduced key, position)`,
/// the index level of the Sui iterator without any value fetch.
fn index_entries(
    db: &ConcurrentRemoteDatabase,
    ks: KeySpace,
    cut: BatchVersion,
) -> Vec<(Bytes, VersionPosition)> {
    let table = db.hybrid_table();
    let mut caches = IndexIterCaches::new();
    let mut cell = table.ks_desc(ks).first_cell();
    let mut previous = None;
    let mut entries = Vec::new();
    while let Some(entry) = table
        .next_entry(ks, cell, previous, &None, false, &mut caches, Some(cut))
        .unwrap()
    {
        cell = entry
            .cell
            .expect("an index entry names the cell it was found in");
        previous = Some(entry.key.clone());
        let ResolvedValue::Position(position) = entry.value else {
            panic!("a latched walk answered from the value cache");
        };
        entries.push((entry.key, position));
    }
    entries
}

#[test]
fn sui_adapter_pages_reverse_bounds_snapshot_and_reopen() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let ks = db.ks("root");
    for first in (0..260u32).step_by(4) {
        let mut batch = db.write_batch();
        for key in first..first + 4 {
            batch.write(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32]);
        }
        batch.commit().unwrap();
    }
    let snapshot = db.checkpoint();
    db.remove(ks, 255u32.to_be_bytes().to_vec()).unwrap();
    db.insert(ks, 256u32.to_be_bytes().to_vec(), vec![99; 32])
        .unwrap();
    let mut forward = db.iterator(ks);
    forward.set_lower_bound(253u32.to_be_bytes().to_vec());
    forward.set_upper_bound(259u32.to_be_bytes().to_vec());
    let rows = forward.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|(k, _)| u32::from_be_bytes(k.as_ref().try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![253, 254, 256, 257, 258]
    );
    let mut reverse = snapshot.iterator(ks);
    reverse.set_lower_bound(253u32.to_be_bytes().to_vec());
    reverse.set_upper_bound(259u32.to_be_bytes().to_vec());
    reverse.reverse();
    let rows = reverse.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        rows.iter()
            .map(|(k, _)| u32::from_be_bytes(k.as_ref().try_into().unwrap()))
            .collect::<Vec<_>>(),
        vec![258, 257, 256, 255, 254, 253]
    );
    assert_eq!(rows[2].1.as_ref(), &[0; 32]);
    let before_scan_rpcs = db.remote_counters().unwrap().2;
    let full_rows = db.iterator(ks).collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(full_rows.len(), 259);
    let scan_rpcs = db.remote_counters().unwrap().2 - before_scan_rpcs;
    assert_eq!(
        scan_rpcs, 259,
        "the hybrid iterator fetches exactly one live value per row: the \
         pipelined scan pages that grouped several references per request are gone"
    );
    assert_eq!(db.remote_counters().unwrap().0.inserts, 0);
    let before_short_scan = db.remote_counters().unwrap().2;
    let mut short = db.iterator(ks);
    assert!(short.next().unwrap().is_ok());
    drop(short);
    let short_scan_rpcs = db.remote_counters().unwrap().2 - before_short_scan;
    assert_eq!(
        short_scan_rpcs, 1,
        "one yielded row costs one live value fetch; there is no grouped lookahead"
    );
    let mut timing = std::collections::BTreeMap::new();
    db.visit_timing_metrics(|name, value| {
        timing.insert(name, value);
    });
    assert!(timing.contains_key("write_durable_receipt_publication_blocked_batches"));
    assert!(timing.contains_key("write_durable_receipt_publication_waiting_batches"));
    assert!(timing.contains_key("write_durable_receipt_publication_waiting_peak"));
    // The Exact checkpoint cleanup metric and the foreground/concurrent
    // checkpoint family are gone with the Exact index (S11f): the native
    // checkpoint pair is what the hybrid still exports.
    assert!(timing.contains_key("write_checkpoint_count"));
    assert!(timing.contains_key("write_checkpoint_ns"));
    assert!(timing.contains_key("coordinator_io_durable_cut_bytes"));
    assert!(timing.contains_key("cache_inserts"));
    assert!(timing.contains_key("value_cache_hits"));
    assert!(timing.contains_key("write_profile_expected_connections"));
    assert!(timing.contains_key("write_profile_captured_connections"));
    assert!(timing.contains_key("write_profile_sampled_requests"));
    db.flush().unwrap();

    drop(db);
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    assert!(db.get(ks, &255u32.to_be_bytes()).unwrap().is_none());
    assert_eq!(
        db.get(ks, &256u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[99; 32]
    );
}

#[test]
fn sui_iterator_rechecks_authority_health_before_yielding_prefetched_rows() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in 0..4u32 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32])
            .unwrap();
    }

    let mut iterator = db.iterator(ks);
    assert!(iterator.next().unwrap().is_ok());
    db.remote_for_test().reads.poison();
    assert!(iterator.next().unwrap().is_err());
    assert!(iterator.next().is_none());
}

#[test]
fn sui_scan_filters_reduced_boundary_candidates_by_verified_full_key() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(4, 0..2),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let mut fixture = Fixture::new_with_shape(128, shape);
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    db.insert(ks, vec![0x12, 0x34, 0x50, 0], vec![0x50; 32])
        .unwrap();
    db.insert(ks, vec![0x12, 0x35, 0, 0], vec![0x35; 32])
        .unwrap();

    let mut forward = db.iterator(ks);
    forward.set_lower_bound(vec![0x12, 0x34, 0x60, 0]);
    let rows = forward.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0.as_ref(), &[0x12, 0x35, 0, 0]);

    let mut reverse = db.iterator(ks);
    reverse.set_upper_bound(vec![0x12, 0x34, 0x40, 0]);
    reverse.reverse();
    assert!(reverse.next().is_none());
}
#[test]
fn sui_grouped_scan_keeps_order_while_one_storage_lane_is_stalled() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = Some(1);
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    let epoch = db.remote_for_test().epoch;
    let partition_for_sequence = |sequence: u64| {
        let mut operation = [0u8; 16];
        operation[..8].copy_from_slice(&epoch.to_le_bytes());
        operation[8..].copy_from_slice(&sequence.to_le_bytes());
        fixture
            .config
            .inventory(epoch)
            .partition_for(BatchId {
                database: fixture.config.storage.database,
                operation,
            })
            .unwrap()
    };
    db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![0; 32])
        .unwrap();
    let first_partition = partition_for_sequence(0);
    let other_sequence = (1..32)
        .find(|sequence| partition_for_sequence(*sequence) != first_partition)
        .unwrap();
    for sequence in 1..=other_sequence {
        let key = if sequence == other_sequence {
            1
        } else {
            100 + sequence as u32
        };
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32])
            .unwrap();
    }
    for key in [2u32, 3] {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8; 32])
            .unwrap();
    }
    db.flush().unwrap();
    db.wait_for_background_threads_to_finish().unwrap();
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let cut = db.remote_for_test().published_cut().unwrap();
    let first_page = index_entries(db.remote_for_test(), ks, cut);
    assert!(first_page.len() >= 4);
    let stalled_partition = first_page[0].1.address.partition;
    let stalled_node = db.remote_for_test().write_core().partition_indexes[&stalled_partition];
    assert!(
        first_page[..4]
            .iter()
            .any(|(_, position)| position.address.partition != stalled_partition)
    );
    let lane = db.remote_for_test().reads.lane_client(stalled_node, 0);
    let held = lane.lock();
    let (started, start_rx) = std::sync::mpsc::sync_channel(1);
    let (finished, finish_rx) = std::sync::mpsc::sync_channel(1);
    let reader = db.clone();
    let scan_rpcs_before = db.remote_for_test().live_read_submission_count();
    let scan = std::thread::spawn(move || {
        started.send(()).unwrap();
        let rows = reader.iterator(ks).collect::<Result<Vec<_>, _>>();
        finished.send(rows).unwrap();
    });
    start_rx.recv().unwrap();
    let dispatch_deadline = Instant::now() + Duration::from_secs(5);
    while db.remote_for_test().live_read_submission_count() == scan_rpcs_before
        && Instant::now() < dispatch_deadline
    {
        std::thread::yield_now();
    }
    assert!(db.remote_for_test().live_read_submission_count() > scan_rpcs_before);
    let independent = first_page[..4]
        .iter()
        .find(|(_, position)| position.address.partition != stalled_partition)
        .unwrap()
        .0
        .clone();
    assert_eq!(
        db.get(ks, &independent).unwrap().unwrap().as_ref(),
        &[independent[3]; 32]
    );
    assert!(matches!(
        finish_rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    drop(held);
    let rows = finish_rx
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    scan.join().unwrap();
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
}

#[test]
fn sui_adapter_concurrent_reads_with_one_credit_and_atomic_batch_failure() {
    use crate::distributed::sui::Db;
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    let mut batch = db.write_batch();
    batch.write(ks, 1u32.to_be_bytes().to_vec(), vec![7; 32]);
    batch.write(ks, 2u32.to_be_bytes().to_vec(), vec![8; 32]);
    batch.commit().unwrap();
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let db = &db;
            scope.spawn(move || {
                assert_eq!(
                    db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().as_ref(),
                    &[7; 32]
                );
                assert_eq!(
                    db.get(ks, &2u32.to_be_bytes()).unwrap().unwrap().as_ref(),
                    &[8; 32]
                );
            });
        }
    });
    let mut oversized = db.write_batch();
    for key in 1..=5u32 {
        oversized.write(ks, key.to_be_bytes().to_vec(), vec![9; 32]);
    }
    assert!(oversized.commit().is_err());
    assert_eq!(
        db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[7; 32]
    );
    assert!(db.get(ks, &5u32.to_be_bytes()).unwrap().is_none());
}

#[test]
fn sui_adapter_hash_enumeration_and_reduced_key_collisions() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    for indexing in [KeyIndexing::Hash, KeyIndexing::key_reduction(4, 0..2)] {
        let shape = KeyShape::new_single_config_indexing(
            indexing.clone(),
            1,
            KeyType::uniform(1),
            Default::default(),
        );
        let fixture = Fixture::new_with_shape(128, shape);
        let db = Db::open_remote_local_fixture(
            &fixture.temp.path().join("authority"),
            fixture.shape.clone(),
            fixture.config.clone(),
        )
        .unwrap();
        let ks = db.ks("root");
        let a = vec![1, 2, 3, 4];
        let b = vec![1, 2, 9, 9];
        db.insert(ks, a.clone(), vec![7]).unwrap();
        db.insert(ks, b.clone(), vec![8]).unwrap();
        let rows = db.iterator(ks).collect::<Result<Vec<_>, _>>().unwrap();
        if matches!(indexing, KeyIndexing::Hash) {
            assert_eq!(rows.len(), 2);
            assert_eq!(db.get(ks, &a).unwrap().unwrap().as_ref(), &[7]);
        } else {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0.as_ref(), b.as_slice());
            assert!(db.get(ks, &a).unwrap().is_none());
            assert!(!db.exists(ks, &a).unwrap());
        }
        assert_eq!(db.get(ks, &b).unwrap().unwrap().as_ref(), &[8]);
        db.remove(ks, b.clone()).unwrap();
        assert!(!db.exists(ks, &b).unwrap());
    }
}

#[test]
fn sui_adapter_variable_prefix_bounds_and_bulk_order() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::VariableLength,
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let mut fixture = Fixture::new_with_shape(128, shape);
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in [vec![], vec![1], vec![1, 0], vec![1, 255], vec![2]] {
        db.insert(ks, key.clone(), key).unwrap();
    }
    let mut iter = db.iterator(ks);
    iter.set_lower_bound(vec![1]);
    iter.set_upper_bound(vec![2]);
    iter.reverse();
    let keys = iter.map(|row| row.unwrap().0.to_vec()).collect::<Vec<_>>();
    assert_eq!(keys, vec![vec![1, 255], vec![1, 0], vec![1]]);
    let queries = vec![vec![2], vec![3], vec![1, 0], vec![], vec![2]];
    let values = db
        .multi_get(ks, &queries)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        values
            .into_iter()
            .map(|v| v.map(|v| v.to_vec()))
            .collect::<Vec<_>>(),
        vec![
            Some(vec![2]),
            None,
            Some(vec![1, 0]),
            Some(vec![]),
            Some(vec![2])
        ]
    );
}

#[test]
fn sui_cell_pruning_drops_more_keys_than_one_batch_admits() {
    // One DropCells batch replaces the bounded enumerate-and-delete batch, so
    // five keys drop although a batch admits four operations.
    use crate::distributed::sui::Db;
    let fixture = Fixture::new();
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let ks = db.ks("root");
    for key in 0..5u32 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![7]).unwrap();
    }
    assert!(5 > fixture.config.storage.batch_limits.max_operations);
    db.drop_cells_in_range(ks, &[0; 4], &[255; 4]).unwrap();
    assert!(db.iterator(ks).next().is_none());
    for key in 0..5u32 {
        assert!(!db.exists(ks, &key.to_be_bytes()).unwrap());
    }
}

#[test]
fn sui_cell_pruning_uses_reduced_full_cell_range_and_survives_reopen() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let fixture = Fixture::new_with_shape(128, shape);
    let path = fixture.temp.path().join("authority");
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    let ks = db.ks("root");
    db.insert(ks, vec![0; 8], vec![7]).unwrap();
    db.drop_cells_in_range(ks, &[0, 0, 0, 0, 255, 255, 255, 255], &[255; 8])
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    drop(db);
    let db = Db::open_remote_local_fixture(&path, fixture.shape.clone(), fixture.config.clone())
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
}

#[test]
fn sui_native_cell_pruning_propagates_durability_barrier_failure() {
    use crate::distributed::sui::Db;
    let temp = tempdir::TempDir::new("sui-native-drop-barrier").unwrap();
    let path = temp.path().join("db");
    std::fs::create_dir(&path).unwrap();
    let moved = temp.path().join("moved");
    let mut config = crate::config::Config::small();
    config.snapshot_written_bytes = u64::MAX;
    let db = Db::open_native(
        &path,
        KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
        Arc::new(config),
        recovery_metrics(),
    )
    .unwrap();
    db.set_native_durable(true);
    let ks = db.ks("root");
    db.insert(ks, vec![0; 4], vec![7]).unwrap();
    // Registered WAL handles remain writable, but syncing their original
    // namespace must fail. A deletion may not acknowledge that failed barrier.
    std::fs::rename(&path, &moved).unwrap();
    let result = db.drop_cells_in_range(ks, &[0; 4], &[255; 4]);
    std::fs::rename(&moved, &path).unwrap();
    assert!(
        result.is_err(),
        "durable deletion must propagate the WAL namespace barrier failure"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn sui_native_cell_pruning_durable_deletion_survives_reopen() {
    use crate::distributed::sui::Db;
    use crate::key_shape::{KeyIndexing, KeyType};
    let temp = tempdir::TempDir::new("sui-native-drop-reopen").unwrap();
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        Default::default(),
    );
    let mut config = crate::config::Config::small();
    config.snapshot_written_bytes = u64::MAX;
    let config = Arc::new(config);
    let db = Db::open_native(
        temp.path(),
        shape.clone(),
        config.clone(),
        recovery_metrics(),
    )
    .unwrap();
    db.set_native_durable(true);
    let ks = db.ks("root");
    db.insert(ks, vec![0; 8], vec![7]).unwrap();
    db.drop_cells_in_range(ks, &[0, 0, 0, 0, 255, 255, 255, 255], &[255; 8])
        .unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    db.wait_for_background_threads_to_finish().unwrap();
    let db = Db::open_native(temp.path(), shape, config, recovery_metrics()).unwrap();
    assert!(!db.exists(ks, &[0; 8]).unwrap());
    db.wait_for_background_threads_to_finish().unwrap();
}
