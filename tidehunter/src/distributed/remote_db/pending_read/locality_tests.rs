use super::tests::{node_for, put, reopen_cold, value};
use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;

#[test]
fn identical_misses_share_one_physical_credit_and_survive_leader_drop() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    fixture.config.max_pending_reads = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(7, 9)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.reads.lane_client(node_for(&db, 7), 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &7u32.to_be_bytes()).unwrap();
    let first_bytes = db.reads.state.retained.lock().1;
    let second = db.submit_get(KeySpace(0), &7u32.to_be_bytes()).unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 1);
    assert!(db.reads.state.retained.lock().1 > first_bytes);
    assert_eq!(db.reads.state.coalesced_members.load(Ordering::Relaxed), 1);
    drop(first);
    assert_eq!(db.reads.state.retained.lock().0, 1);
    drop(held);
    assert_eq!(db.finish_get(second).unwrap(), value(9));
    assert_eq!(db.read_rpc_count(), 1);
    wait_read_retirement(&db.reads);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn overlapping_groups_share_members_and_preserve_each_callers_slot_order() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(2, 9)],
    )
    .unwrap();
    let pin = db.index.read().pin_cut(IndexReadCut::Latest).unwrap();
    let references: Vec<_> = (0u32..3)
        .map(|key| {
            db.index_reference(KeySpace(0), &key.to_be_bytes(), pin.cut())
                .unwrap()
                .unwrap()
        })
        .collect();
    let node = db.partition_indexes[&references[0].batch_address.partition];
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();
    let first = db
        .reads
        .submit(node, references[..2].to_vec(), 0, false, Some(pin.clone()))
        .unwrap();
    let second = db
        .reads
        .submit_until(
            node,
            vec![references[1], references[2], references[1]],
            0,
            false,
            ReadContext {
                deadline: Instant::now() + db.config.timeout,
                class: ReadClass::ScanNoFill,
                pin: Some(pin),
            },
        )
        .unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 2);
    assert_eq!(db.reads.state.coalesced_members.load(Ordering::Relaxed), 2);
    drop(held);
    let (second_values, _) = second.finish().unwrap();
    let (first_values, _) = first.finish().unwrap();
    assert_eq!(first_values, vec![put(0, 7), put(1, 8)]);
    assert_eq!(second_values, vec![put(1, 8), put(2, 9), put(1, 8)]);
    assert_eq!(db.read_rpc_count(), 2);
    wait_read_retirement(&db.reads);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn old_and_current_cut_misses_never_merge_versions() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let cut = db.published_cut().unwrap();
    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let old = db
        .submit_get_cut(KeySpace(0), &0u32.to_be_bytes(), cut.clone())
        .unwrap();
    db.write_batch(fixture.operation(0, 1), vec![put(0, 8)])
        .unwrap();
    let current = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let old_again = db
        .submit_get_cut(KeySpace(0), &0u32.to_be_bytes(), cut)
        .unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 2);
    drop(held);
    assert_eq!(db.finish_get(current).unwrap(), value(8));
    assert_eq!(db.finish_get(old_again).unwrap(), value(7));
    assert_eq!(db.finish_get(old).unwrap(), value(7));
    assert_eq!(db.read_rpc_count(), 2);
    wait_read_retirement(&db.reads);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn abandoned_shared_failure_poison_and_all_owned_credits_retire() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let node = node_for(&db, 0);
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let second = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(first);
    db.clients[node]
        .client
        .lock()
        .fence_to(db.epoch + 1)
        .unwrap();
    drop(held);
    let completion = second.wait();
    assert!(db.finish_ready_get(completion).is_err());
    db.reads.stop();
    assert!(db.reads.failed());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.shutdown().is_err());
}

fn open_sui_scan_fixture(fixture: &Fixture) -> Arc<crate::distributed::sui::Db> {
    let mut original = fixture.open();
    for (batch, partition) in [(0u32, 0u32), (1, 1), (2, 1)] {
        original
            .write_batch(
                fixture.operation(partition, u64::from(batch)),
                (batch * 4..batch * 4 + 4)
                    .map(|key| put(key, key as u8))
                    .collect(),
            )
            .unwrap();
    }
    original.shutdown().unwrap();
    drop(original);
    crate::distributed::sui::Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap()
}

fn wait_read_retirement(reads: &ReadService) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let generation = reads.admission_generation();
        if reads.ensure_idle().is_ok() {
            return;
        }
        reads.wait_admission(generation, deadline).unwrap();
    }
}

#[test]
fn actual_sui_iterator_dispatches_second_page_while_first_page_is_blocked() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let db = open_sui_scan_fixture(&fixture);
    let ks = db.ks("root");
    let remote = db.remote_for_test();
    let first = remote
        .index_reference(ks, &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = remote.partition_indexes[&first.batch_address.partition];
    let lane = remote.reads.lane_client(node, 0);
    let held = lane.lock();
    let reader = db.clone();
    let scan = thread::spawn(move || {
        reader
            .iterator(ks)
            .collect::<crate::distributed::sui::Result<Vec<_>>>()
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while remote.read_rpc_count() == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    let second_page_dispatched = remote.read_rpc_count() != 0;
    drop(held); // Always release before assertions or joining the scan.
    let rows = scan.join().unwrap().unwrap();
    assert!(
        second_page_dispatched,
        "second-page I/O must not wait for first-page results"
    );
    assert_eq!(rows.len(), 12);
    assert!(rows.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(remote.read_cache_stats().inserts, 0);
    wait_read_retirement(&remote.reads);
    assert_eq!(*remote.reads.state.retained.lock(), (0, 0));
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn dropping_sui_iterator_cancels_delivery_but_keeps_pending_page_ownership() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let db = open_sui_scan_fixture(&fixture);
    let ks = db.ks("root");
    let remote = db.remote_for_test();
    let second = remote
        .index_reference(ks, &4u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = remote.partition_indexes[&second.batch_address.partition];
    let lane = remote.reads.lane_client(node, 0);
    let held = lane.lock();
    let mut iterator = db.iterator(ks);
    assert_eq!(
        iterator.next().unwrap().unwrap().0.as_ref(),
        &0u32.to_be_bytes()
    );
    drop(iterator);
    assert_ne!(remote.reads.state.retained.lock().0, 0);
    drop(held);
    wait_read_retirement(&remote.reads);
    assert_eq!(*remote.reads.state.retained.lock(), (0, 0));
    assert_eq!(remote.read_cache_stats().inserts, 0);
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn sui_reseek_and_checkpoint_pins_preserve_old_cut_until_final_drop() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let db = open_sui_scan_fixture(&fixture);
    let ks = db.ks("root");
    let old_cut = db.remote_for_test().published_cut().unwrap();
    let checkpoint = db.checkpoint();
    let mut iterator = db.iterator(ks);
    assert_eq!(
        iterator.next().unwrap().unwrap().0.as_ref(),
        &0u32.to_be_bytes()
    );
    db.insert(ks, 1u32.to_be_bytes().to_vec(), vec![88; 32])
        .unwrap();
    db.remove(ks, 2u32.to_be_bytes().to_vec()).unwrap();
    db.start_relocation().unwrap();
    assert!(db.remote_for_test().pin_cut(old_cut.clone()).is_ok());
    let snapshot = checkpoint
        .iterator(ks)
        .collect::<crate::distributed::sui::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(snapshot.len(), 12);
    assert_eq!(snapshot[1].1.as_ref(), &[1; 32]);
    assert_eq!(snapshot[2].1.as_ref(), &[2; 32]);
    // Reseek cancels old lookahead, but neither reacquires Latest nor loses the pin.
    iterator.set_lower_bound(1u32.to_be_bytes().to_vec());
    iterator.set_upper_bound(4u32.to_be_bytes().to_vec());
    iterator.reverse();
    let rows = iterator
        .collect::<crate::distributed::sui::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.0.to_vec()).collect::<Vec<_>>(),
        vec![
            3u32.to_be_bytes().to_vec(),
            2u32.to_be_bytes().to_vec(),
            1u32.to_be_bytes().to_vec()
        ]
    );
    assert_eq!(rows[2].1.as_ref(), &[1; 32]);
    wait_read_retirement(&db.remote_for_test().reads);
    db.start_relocation().unwrap();
    assert!(db.remote_for_test().pin_cut(old_cut).is_err());
    assert_eq!(
        db.get(ks, &1u32.to_be_bytes()).unwrap().unwrap().as_ref(),
        &[88; 32]
    );
    assert!(db.get(ks, &2u32.to_be_bytes()).unwrap().is_none());
    db.wait_for_background_threads_to_finish().unwrap();
}
