use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;

/// Observable-progress guard. No test waits on a clock premise: the condition
/// is a counter or flag another thread sets, and this bound only turns a hang
/// into a named failure.
const WAIT: std::time::Duration = std::time::Duration::from_secs(30);

fn wait_for(what: &str, condition: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + WAIT;
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::yield_now();
    }
}

#[test]
fn begin_failure_fences_cached_and_completed_values_before_notifications() {
    use crate::distributed::codec::Mutation;
    use crate::key_shape::KeySpace;

    let fixture = Fixture::new();
    let mut database = fixture.open();
    let key = 17u32.to_be_bytes();
    database
        .write_batch(
            fixture.operation(0, 17),
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key.to_vec().into(),
                value: vec![7; 32].into(),
            }],
        )
        .unwrap();
    // The first read fills the read cache from the owner; the second is a
    // cache hit that no owner round trip would gate.
    assert_eq!(
        database.get(KeySpace(0), &key).unwrap().as_deref(),
        Some(&[7u8; 32][..])
    );
    let hits = database.read_cache_stats().hits;
    assert!(database.get(KeySpace(0), &key).unwrap().is_some());
    assert_eq!(
        database.read_cache_stats().hits,
        hits + 1,
        "the value is served from the cache before the failure"
    );
    let core = Arc::clone(&database.core);
    // A transport read that finished before the failure: the owner read is
    // submitted for the member the index names, with the cache emptied so
    // the bytes come from the owner, and waited until its flight is ready.
    let cut = core.table.current();
    let position = core
        .table
        .get(KeySpace(0), &key, cut)
        .unwrap()
        .expect("the written key is indexed");
    let node = core
        .config
        .endpoints
        .iter()
        .position(|endpoint| endpoint.partition == position.address.partition)
        .expect("the position names a served partition");
    database.clear_read_cache();
    let completed = core.reads.submit_member(node, position).unwrap();
    let deadline = std::time::Instant::now() + WAIT;
    while completed.try_result().unwrap().is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the owner read did not complete"
        );
        std::thread::yield_now();
    }
    {
        // No index-writer lock exists in the hybrid: the coordinator's
        // index lives in `table`, whose flusher threads the read gates never
        // touch. The publication lock is the one this test holds.
        let _publisher = core.publisher.lock();
        core.begin_failure(&LocalDbError::new("injected publication failure"));
        // begin_failure is atomic-only: even while the publication lock
        // stays held, a transport value that completed before the failure
        // must fail its final serving gate (the `try_result`/`finish`
        // Acquire on `failed`), and every later read fails at its entry
        // gate. None of these gates takes the index writer or publisher.
        assert!(completed.try_result().is_err());
        assert!(completed.finish_member().is_err());
        assert!(core.ensure_healthy().is_err());
        assert!(core.reads.failed());
        assert!(database.validate_cached_read().is_err());
        assert!(database.get(KeySpace(0), &key).is_err());
        assert!(database.exists(KeySpace(0), &key).is_err());
        assert!(database.get_many(KeySpace(0), &[key.to_vec()]).is_err());
        assert!(database.checkpoint().is_err());
    }
    core.finish_failure();
    core.finish_failure();
    assert!(core.shutdown().is_err());
    assert!(
        core.first_failure
            .get()
            .unwrap()
            .contains("injected publication failure")
    );
}

#[test]
fn finish_failure_releases_every_waiter_without_reusing_unknowns() {
    use crate::distributed::types::BatchVersion;
    use std::time::Instant;

    let fixture = Fixture::new();
    let database = fixture.open();
    let core = Arc::clone(&database.core);
    let sequence = core.sequencer.allocate(crate::distributed::types::BatchId {
        database: fixture.config.storage.database,
        operation: fixture.operation(0, 18),
    });
    let version = BatchVersion {
        epoch: core.epoch,
        sequence,
    };
    let credit = core
        .admission
        .reserve(core.config.max_pending_bytes, || core.ensure_healthy())
        .unwrap();
    let read_generation = core.reads.admission_generation();
    let (started, starting) = mpsc::sync_channel(4);
    let (finished, finishing) = mpsc::sync_channel(4);
    std::thread::scope(|scope| {
        let slot_started = started.clone();
        let slot_finished = finished.clone();
        let core = &core;
        scope.spawn(move || {
            slot_started.send(()).unwrap();
            let result = core.sequencer.wait(version, Instant::now() + WAIT);
            slot_finished.send(result.map(|_| ())).unwrap();
        });
        let prefix_started = started.clone();
        let prefix_finished = finished.clone();
        scope.spawn(move || {
            prefix_started.send(()).unwrap();
            prefix_finished
                .send(core.wait_published_before(sequence + 1))
                .unwrap();
        });
        let read_started = started.clone();
        let read_finished = finished.clone();
        scope.spawn(move || {
            read_started.send(()).unwrap();
            read_finished
                .send(
                    core.reads
                        .wait_admission(read_generation, Some(Instant::now() + WAIT)),
                )
                .unwrap();
        });
        scope.spawn(move || {
            started.send(()).unwrap();
            finished
                .send(
                    core.admission
                        .reserve(1, || core.ensure_healthy())
                        .map(drop),
                )
                .unwrap();
        });
        for _ in 0..4 {
            starting.recv_timeout(WAIT).unwrap();
        }
        wait_for("admission waiter queued", || {
            core.admission.queued_waiters() == 1
        });
        core.begin_failure(&LocalDbError::new("injected waiter failure"));
        core.finish_failure();
        for _ in 0..4 {
            let result = finishing.recv_timeout(WAIT).unwrap();
            assert!(
                result.is_err(),
                "a waiter outlived the injected failure: {result:?}"
            );
        }
    });
    assert_eq!(
        core.sequencer.resolution(sequence),
        Some(Resolution::Unknown)
    );
    core.release_sequence(sequence).unwrap();
    assert_eq!(core.sequencer.reuse_next(), sequence);
    assert_eq!(
        core.sequencer
            .slot(sequence)
            .data
            .lock()
            .outcome
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .uncertain_version,
        Some(version)
    );
    drop(credit);
    assert!(core.shutdown().is_err());
}

/// Replaces the deleted PublicationTracker sparse test on the live
/// publisher: an independent later batch publishes past an unresolved
/// floor, the same-key chain waits as dependents, the installed view
/// excludes exactly the unpublished generations in [floor, upper] (an
/// ABORTED generation included), and no slot returns to the ring until the
/// floor passes every PUBLISHED and ABORTED generation.
#[test]
fn sparse_view_excludes_exactly_unpublished_and_floor_passes_resolved() {
    use crate::distributed::codec::Mutation;
    use crate::distributed::types::BatchVersion;
    use crate::key_shape::KeySpace;
    use std::time::Instant;

    let put = |key: u32, value: u8| Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    };
    let fixture = Fixture::new();
    let mut database = fixture.open();
    let core = Arc::clone(&database.core);
    let version = |sequence| BatchVersion {
        epoch: core.epoch,
        sequence,
    };
    let held_client = Arc::clone(&core.clients[0].client);
    let held = held_client.lock();
    let gap = database
        .submit(fixture.operation(0, 1), vec![put(0, 1)])
        .unwrap();
    let chained = database
        .submit(fixture.operation(1, 2), vec![put(0, 2), put(1, 2)])
        .unwrap();
    let successor = database
        .submit(fixture.operation(1, 3), vec![put(1, 3)])
        .unwrap();
    let failed = fixture.operation(3, 4);
    core.test_hooks.lock().fail_prepared_operation = Some(failed);
    assert!(database.submit(failed, vec![put(4, 4)]).is_err());
    let independent = database
        .submit(fixture.operation(2, 5), vec![put(3, 5)])
        .unwrap();
    assert_eq!(
        [gap, chained, successor, independent].map(|version| version.sequence),
        [0, 1, 2, 4]
    );
    let deadline = Instant::now() + WAIT;
    for sequence in [1, 2, 4] {
        while !core.publication_settled(sequence) {
            core.ensure_healthy().unwrap();
            assert!(
                Instant::now() < deadline,
                "owner receipts were not processed"
            );
            std::thread::yield_now();
        }
    }
    // Owner 0 is held: the gap is READY in its queue, never durable.
    assert_eq!(core.sequencer.resolution(0), Some(Resolution::Ready));
    assert_eq!(core.sequencer.resolution(1), Some(Resolution::Durable));
    assert_eq!(core.sequencer.resolution(2), Some(Resolution::Durable));
    assert_eq!(core.sequencer.resolution(3), Some(Resolution::Aborted));
    assert_eq!(core.sequencer.resolution(4), Some(Resolution::Published));
    {
        let publisher = core.publisher.lock();
        assert_eq!(publisher.published_next, 0);
        assert_eq!(publisher.dependents.get(&0), Some(&vec![1]));
        assert_eq!(publisher.dependents.get(&1), Some(&vec![2]));
    }
    assert_eq!(
        core.sequencer.reuse_next(),
        0,
        "no slot passes the unresolved floor"
    );
    assert_eq!(core.retained_slots(), 5);
    // Deviation (M) from native (PARITY.md, "Read visibility past an unresolved
    // prefix"): reads resolve at the publication cut, which stops at the floor,
    // so key 3 (sequence 4, published beyond it) is invisible until the floor
    // passes. The loop after `drain` below is the counter-control.
    assert!(
        database
            .get(KeySpace(0), &3u32.to_be_bytes())
            .unwrap()
            .is_none()
    );
    assert!(
        database
            .get(KeySpace(0), &0u32.to_be_bytes())
            .unwrap()
            .is_none()
    );
    assert!(
        database
            .get(KeySpace(0), &1u32.to_be_bytes())
            .unwrap()
            .is_none()
    );

    drop(held);
    database.drain().unwrap();
    assert_eq!(core.published_next(), 5);
    for sequence in [0, 1, 2, 4] {
        assert!(core.is_published(version(sequence)));
    }
    core.wait_settled().unwrap();
    assert_eq!(core.sequencer.reuse_next(), 5);
    for (key, expected) in [(0u32, Some(2u8)), (1, Some(3)), (3, Some(5)), (4, None)] {
        assert_eq!(
            database
                .get(KeySpace(0), &key.to_be_bytes())
                .unwrap()
                .map(|value| value[0]),
            expected
        );
    }
    drop(core);
    database.shutdown().unwrap();
}
