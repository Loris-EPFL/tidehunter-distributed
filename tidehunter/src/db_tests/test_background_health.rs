use super::*;
use crate::failpoints::FailPoint;
use crate::flusher::RelocationFlush;
use crate::key_shape::{KeyShapeBuilder, KeyType};
use std::sync::Barrier;

fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("health", 4, 1, KeyType::uniform(1));
    builder.build()
}

fn assert_failure<T: std::fmt::Debug>(result: DbResult<T>, expected: &Arc<BackgroundFailure>) {
    match result.unwrap_err() {
        DbError::BackgroundFailure(actual) => assert!(Arc::ptr_eq(&actual, expected)),
        other => panic!("expected background failure, got {other:?}"),
    }
}

#[test]
fn healthy_database_reports_healthy() {
    let dir = tempdir::TempDir::new("healthy-db").unwrap();
    let db = Db::open(
        dir.path(),
        shape(),
        Arc::new(Config::small()),
        Metrics::new(),
    )
    .unwrap();
    db.check_background_health().unwrap();
    db.insert(db.single_ks(), vec![0; 4], vec![1]).unwrap();
    let mut batch = db.write_batch();
    batch.write(db.single_ks(), vec![1; 4], vec![2]);
    batch.commit().unwrap();
    db.force_rebuild_control_region().unwrap();
    db.check_background_health().unwrap();
    db.wait_for_background_threads_to_finish();
}

fn worker_panic_fails_closed(role: BackgroundThread) {
    let dir = tempdir::TempDir::new("failed-background-worker").unwrap();
    let (panic_now, panic_requested) = mpsc::sync_channel(1);
    let panic_requested = Mutex::new(panic_requested);
    let (recorded, failure_recorded) = mpsc::sync_channel(1);
    let health = Arc::new(BackgroundHealth::with_start_hook(
        role,
        FailPoint::from_fn(move || {
            panic_requested
                .lock()
                .recv_timeout(Duration::from_secs(20))
                .unwrap();
            panic!("injected {role:?} failure");
        }),
        recorded,
    ));
    let mut config = Config::small();
    config.num_pending_promotion_threads = 1;
    let config = Arc::new(config);
    let db = Db::open_with_background_health(
        dir.path(),
        shape(),
        config.clone(),
        Metrics::new(),
        false,
        health,
    )
    .unwrap();
    let ks = db.single_ks();
    db.insert(ks, vec![0; 4], vec![7]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    db.wal.fsync().unwrap();
    let mut batch = db.write_batch();
    batch.write(ks, vec![1; 4], vec![8]);
    db.check_background_health().unwrap();
    if role == BackgroundThread::Snapshot {
        db.start_periodic_snapshot();
    }
    panic_now.send(()).unwrap();
    failure_recorded
        .recv_timeout(Duration::from_secs(20))
        .unwrap();
    let DbError::BackgroundFailure(failure) = db.check_background_health().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(failure.role, role);
    assert_eq!(
        failure.reason,
        BackgroundFailureReason::Panic(format!("injected {role:?} failure"))
    );
    let wal_position = db.wal_writer.position();
    assert_failure(db.insert(ks, vec![1; 4], vec![9]), &failure);
    assert_failure(db.remove(ks, vec![0; 4]), &failure);
    assert_failure(batch.commit(), &failure);
    assert_failure(db.write_batch().commit(), &failure);
    assert_failure(db.drop_cells_in_range(ks, &[0; 4], &[255; 4]), &failure);
    assert_failure(db.destroy_key_space("health"), &failure);
    assert_failure(db.rebuild_control_region(), &failure);
    assert_failure(db.force_rebuild_control_region(), &failure);
    assert_failure(
        db.create_state_snapshot(dir.path().join("snapshot")),
        &failure,
    );
    #[cfg(feature = "experimental-distributed")]
    {
        let mut profile = NativeWriteStages::default();
        assert_failure(
            db.insert_profiled_for_benchmark(ks, vec![1; 4], vec![9], &mut profile),
            &failure,
        );
        assert_failure(
            db.remove_profiled_for_benchmark(ks, vec![0; 4], &mut profile),
            &failure,
        );
        assert_eq!(profile.selected_operations, 0);
        assert_failure(db.persist_value_wal_for_benchmark(), &failure);
        assert_failure(
            db.persist_value_wal_profiled_for_benchmark(&mut ValueWalPersistenceProfile::default()),
            &failure,
        );
    }
    assert_eq!(db.wal_writer.position(), wal_position);
    assert_eq!(db.get(ks, &[0; 4]).unwrap(), Some(vec![7].into()));
    assert_eq!(db.get(ks, &[1; 4]).unwrap(), None);
    drop(failure);
    db.wait_for_background_threads_to_finish();
    let reopened = Db::open(dir.path(), shape(), config, Metrics::new()).unwrap();
    reopened.check_background_health().unwrap();
    assert_eq!(reopened.get(ks, &[0; 4]).unwrap(), Some(vec![7].into()));
    assert_eq!(reopened.get(ks, &[1; 4]).unwrap(), None);
    reopened.wait_for_background_threads_to_finish();
}

#[test]
fn flusher_panic_fails_closed() {
    worker_panic_fails_closed(BackgroundThread::Flusher);
}

#[test]
fn relocator_panic_fails_closed() {
    worker_panic_fails_closed(BackgroundThread::Relocator);
}

#[test]
fn pending_promotion_panic_fails_closed() {
    worker_panic_fails_closed(BackgroundThread::PendingPromotion);
}

#[test]
fn flat_promotion_panic_fails_closed() {
    worker_panic_fails_closed(BackgroundThread::FlatPromotion);
}

#[test]
fn snapshot_panic_fails_closed() {
    worker_panic_fails_closed(BackgroundThread::Snapshot);
}

#[test]
fn promotion_errors_preserve_first_failure_and_count_later_failures() {
    let dir = tempdir::TempDir::new("background-error").unwrap();
    let db = Db::open(
        dir.path(),
        shape(),
        Arc::new(Config::small()),
        Metrics::new(),
    )
    .unwrap();
    db.background_health
        .record_error(BackgroundThread::PendingPromotion, &DbError::CrCorrupted);
    db.background_health.record_error(
        BackgroundThread::FlatPromotion,
        &DbError::Io(io::Error::other("injected I/O error")),
    );

    let DbError::BackgroundFailure(failure) = db.check_background_health().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(failure.role, BackgroundThread::PendingPromotion);
    assert_eq!(
        failure.reason,
        BackgroundFailureReason::Error("CrCorrupted".to_owned())
    );
    assert_eq!(db.background_health.additional_failures(), 1);
    assert_failure(db.insert(db.single_ks(), vec![0; 4], vec![1]), &failure);
    db.wait_for_background_threads_to_finish();
}

#[test]
fn concurrent_failures_keep_one_reason_and_count_every_other_failure() {
    let health = Arc::new(BackgroundHealth::default());
    let ready = Arc::new(Barrier::new(8));
    thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                ready.wait();
                health.record_error(BackgroundThread::Relocator, &DbError::CrCorrupted);
                assert!(matches!(health.check(), Err(DbError::BackgroundFailure(_))));
            });
        }
    });
    assert_eq!(health.additional_failures(), 7);
    let DbError::BackgroundFailure(first) = health.check().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(first.role, BackgroundThread::Relocator);
    assert_eq!(
        first.reason,
        BackgroundFailureReason::Error("CrCorrupted".to_owned())
    );
}

#[test]
fn wrapper_resumes_original_non_string_panic() {
    let health = Arc::new(BackgroundHealth::default());
    let worker_health = health.clone();
    let worker = thread::spawn(move || {
        worker_health.run(BackgroundThread::Flusher, || std::panic::panic_any(42u32))
    });
    assert_eq!(*worker.join().unwrap_err().downcast::<u32>().unwrap(), 42);
    let DbError::BackgroundFailure(failure) = health.check().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(
        failure.reason,
        BackgroundFailureReason::Panic("non-string panic payload".to_owned())
    );
}
#[test]
fn wal_tracker_panic_fails_closed_without_affecting_another_database() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempdir::TempDir::new("wal-tracker-failure").unwrap();
    let other_dir = tempdir::TempDir::new("independent-db-health").unwrap();
    let config = Arc::new(Config::small());
    let other = Db::open(other_dir.path(), shape(), config.clone(), Metrics::new()).unwrap();
    let (panic_now, panic_requested) = mpsc::sync_channel(1);
    let panic_requested = Mutex::new(panic_requested);
    let first = AtomicBool::new(true);
    let (recorded, failure_recorded) = mpsc::sync_channel(1);
    let health = Arc::new(BackgroundHealth::with_start_hook(
        BackgroundThread::WalTracker,
        FailPoint::from_fn(move || {
            // Relaxed: only one of the two WAL trackers receives the injection.
            if first.swap(false, Ordering::Relaxed) {
                panic_requested
                    .lock()
                    .recv_timeout(Duration::from_secs(20))
                    .unwrap();
                panic!("injected WAL tracker failure");
            }
        }),
        recorded,
    ));
    let db = Db::open_with_background_health(
        dir.path(),
        shape(),
        config.clone(),
        Metrics::new(),
        false,
        health,
    )
    .unwrap();
    let ks = db.single_ks();
    db.insert(ks, vec![0; 4], vec![7]).unwrap();
    db.wal.fsync().unwrap();
    panic_now.send(()).unwrap();
    failure_recorded
        .recv_timeout(Duration::from_secs(20))
        .unwrap();
    let DbError::BackgroundFailure(failure) = db.check_background_health().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(failure.role, BackgroundThread::WalTracker);
    assert_eq!(
        failure.reason,
        BackgroundFailureReason::Panic("injected WAL tracker failure".to_owned())
    );
    assert_failure(db.insert(ks, vec![1; 4], vec![8]), &failure);
    assert_failure(db.write_batch().commit(), &failure);
    assert_failure(db.force_rebuild_control_region(), &failure);
    other.check_background_health().unwrap();
    other
        .insert(other.single_ks(), vec![0; 4], vec![9])
        .unwrap();
    other.force_rebuild_control_region().unwrap();
    other.wait_for_background_threads_to_finish();
    assert_eq!(db.get(ks, &[0; 4]).unwrap(), Some(vec![7].into()));
    // WAL worker destruction still joins and propagates the original panic.
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.wait_for_background_threads_to_finish();
        }))
        .is_err()
    );
    let recovered = Db::open(dir.path(), shape(), config, Metrics::new()).unwrap();
    recovered.check_background_health().unwrap();
    assert_eq!(recovered.get(ks, &[0; 4]).unwrap(), Some(vec![7].into()));
    assert_eq!(recovered.get(ks, &[1; 4]).unwrap(), None);
    recovered.wait_for_background_threads_to_finish();
}

#[test]
fn mid_flush_panic_preserves_wal_and_rejects_next_write() {
    let dir = tempdir::TempDir::new("mid-flush-health-failure").unwrap();
    let config = Arc::new(Config::small());
    let (recorded, failure_recorded) = mpsc::sync_channel(1);
    let health = Arc::new(BackgroundHealth::with_start_hook(
        BackgroundThread::Flusher,
        FailPoint::default(),
        recorded,
    ));
    let db = Db::open_with_background_health(
        dir.path(),
        shape(),
        config.clone(),
        Metrics::new(),
        false,
        health,
    )
    .unwrap();
    let ks = db.single_ks();
    db.insert(ks, vec![0; 4], vec![7]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    db.wal.fsync().unwrap();
    db.large_table.fp.0.write().fp_flush_before_completion =
        FailPoint::from_fn(|| panic!("injected mid-flush failure"));
    let layout = db.config.wal_layout(WalKind::Index);
    db.large_table
        .flush_and_accumulate(db.as_ref(), &layout, u64::MAX);
    failure_recorded
        .recv_timeout(Duration::from_secs(20))
        .unwrap();
    let DbError::BackgroundFailure(failure) = db.check_background_health().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(failure.role, BackgroundThread::Flusher);
    assert_eq!(
        failure.reason,
        BackgroundFailureReason::Panic("injected mid-flush failure".to_owned())
    );
    let wal_position = db.wal_writer.position();
    assert_failure(db.insert(ks, vec![1; 4], vec![9]), &failure);
    assert_eq!(db.wal_writer.position(), wal_position);
    assert_eq!(db.get(ks, &[0; 4]).unwrap(), Some(vec![7].into()));
    db.wait_for_background_threads_to_finish();
    let recovered = Db::open(dir.path(), shape(), config, Metrics::new()).unwrap();
    recovered.check_background_health().unwrap();
    assert_eq!(recovered.get(ks, &[0; 4]).unwrap(), Some(vec![7].into()));
    assert_eq!(recovered.get(ks, &[1; 4]).unwrap(), None);
    recovered.wait_for_background_threads_to_finish();
}

#[derive(Clone, Copy, Debug)]
enum FlushWaitMutation {
    RangeDelete,
    DestroyKeySpace,
    Relocate,
}

fn admitted_flush_wait_returns_background_failure(mutation: FlushWaitMutation) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempdir::TempDir::new("failed-flush-wait").unwrap();
    let config = Arc::new(Config::small());
    let make_shape = |include_health| {
        let mut builder = KeyShapeBuilder::new();
        if include_health {
            builder.add_key_space("health", 4, 1, KeyType::uniform(1));
        }
        builder.add_key_space("keeper", 4, 1, KeyType::uniform(1));
        builder.build()
    };
    let initial = Db::open(dir.path(), make_shape(true), config.clone(), Metrics::new()).unwrap();
    let ks = initial.ks("health");
    initial.insert(ks, vec![0; 4], vec![7]).unwrap();
    initial.wal_writer.wal_tracker_barrier();
    initial.wal.fsync().unwrap();
    initial.wait_for_background_threads_to_finish();

    // Replay leaves this retained keyspace dirty, so a real flush can overlap
    // destruction even though no application handle is exposed for it.
    let (recorded, failure_recorded) = mpsc::sync_channel(1);
    let health = Arc::new(BackgroundHealth::with_start_hook(
        BackgroundThread::Flusher,
        FailPoint::default(),
        recorded,
    ));
    let db = Db::open_with_background_health(
        dir.path(),
        make_shape(false),
        config.clone(),
        Metrics::new(),
        false,
        health,
    )
    .unwrap();
    assert!(db.try_ks("health").is_none());
    let (flushing, flush_started) = mpsc::sync_channel(1);
    let (panic_now, panic_requested) = mpsc::sync_channel(1);
    let panic_requested = Mutex::new(panic_requested);
    let (waiting, wait_started) = mpsc::sync_channel(1);
    let first_wait = AtomicBool::new(true);
    {
        let mut hooks = db.large_table.fp.0.write();
        hooks.fp_flush_before_completion = FailPoint::from_fn(move || {
            flushing.send(()).unwrap();
            panic_requested
                .lock()
                .recv_timeout(Duration::from_secs(20))
                .unwrap();
            panic!("injected failure while foreground waits");
        });
        hooks.fp_waiting_for_flush = FailPoint::from_fn(move || {
            // Relaxed: this flag only limits the test notification to one.
            if first_wait.swap(false, Ordering::Relaxed) {
                waiting.send(()).unwrap();
            }
        });
    }
    db.large_table.flush_and_accumulate(
        db.as_ref(),
        &db.config.wal_layout(WalKind::Index),
        u64::MAX,
    );
    flush_started.recv_timeout(Duration::from_secs(20)).unwrap();
    let waiter_db = db.clone();
    let (finished, wait_finished) = mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        let result = match mutation {
            FlushWaitMutation::RangeDelete => waiter_db.drop_cells_in_range(ks, &[0; 4], &[255; 4]),
            FlushWaitMutation::DestroyKeySpace => waiter_db.destroy_key_space("health"),
            FlushWaitMutation::Relocate => {
                let context = waiter_db.ks_context(ks);
                let cell = context.ks_config.cell_id(&[0; 4]);
                waiter_db
                    .large_table
                    .sync_flush_for_relocation(
                        context,
                        &cell,
                        waiter_db.as_ref(),
                        RelocationFlush::None,
                    )
                    .map(|_| ())
            }
        };
        finished.send(result).unwrap();
    });
    wait_started.recv_timeout(Duration::from_secs(20)).unwrap();
    db.check_background_health().unwrap();
    panic_now.send(()).unwrap();
    failure_recorded
        .recv_timeout(Duration::from_secs(20))
        .unwrap();
    let result = wait_finished.recv_timeout(Duration::from_secs(20)).unwrap();
    waiter.join().unwrap();
    let DbError::BackgroundFailure(failure) = db.check_background_health().unwrap_err() else {
        panic!("expected typed background error");
    };
    assert_eq!(failure.role, BackgroundThread::Flusher);
    assert_eq!(
        failure.reason,
        BackgroundFailureReason::Panic("injected failure while foreground waits".to_owned())
    );
    assert_failure(result, &failure);
    assert_failure(db.insert(db.ks("keeper"), vec![0; 4], vec![1]), &failure);
    db.wait_for_background_threads_to_finish();
    let recovered = Db::open(dir.path(), make_shape(true), config, Metrics::new()).unwrap();
    recovered.check_background_health().unwrap();
    // Deletion may already have committed its WAL intent or registry tombstone
    // before waiting. An error cannot roll back those durable effects.
    let expected = match mutation {
        FlushWaitMutation::Relocate => Some(vec![7].into()),
        FlushWaitMutation::RangeDelete | FlushWaitMutation::DestroyKeySpace => None,
    };
    assert_eq!(
        recovered.get(recovered.ks("health"), &[0; 4]).unwrap(),
        expected,
        "{mutation:?}"
    );
    recovered.wait_for_background_threads_to_finish();
}

#[test]
fn admitted_range_delete_returns_mid_flush_failure() {
    admitted_flush_wait_returns_background_failure(FlushWaitMutation::RangeDelete);
}

#[test]
fn admitted_keyspace_destruction_returns_mid_flush_failure() {
    admitted_flush_wait_returns_background_failure(FlushWaitMutation::DestroyKeySpace);
}

#[test]
fn admitted_sync_relocation_returns_mid_flush_failure() {
    admitted_flush_wait_returns_background_failure(FlushWaitMutation::Relocate);
}
