use super::*;

#[test]
fn acquisition_span_completes_before_guard_is_released() {
    let stage = Stage::default();
    let mutex = parking_lot::Mutex::new(());
    let guard = stage.measure(|| mutex.lock());
    assert_eq!(stage.calls.load(Ordering::Relaxed), 1);
    assert_eq!(stage.errors.load(Ordering::Relaxed), 0);
    assert!(mutex.try_lock().is_none());
    drop(guard);
    assert_eq!(stage.calls.load(Ordering::Relaxed), 1);
}

#[test]
fn failed_and_unwinding_stage_spans_are_not_lost() {
    let stage = Stage::default();
    assert_eq!(stage.run(|| Err::<(), _>("rejected")), Err("rejected"));
    assert_eq!(stage.calls.load(Ordering::Relaxed), 1);
    assert_eq!(stage.errors.load(Ordering::Relaxed), 1);
    let unwind = std::panic::catch_unwind(|| stage.measure(|| panic!("failed operation")));
    assert!(unwind.is_err());
    assert_eq!(stage.calls.load(Ordering::Relaxed), 2);
    assert_eq!(stage.errors.load(Ordering::Relaxed), 2);
    stage.run(|| Ok::<_, ()>(())).unwrap();
    assert_eq!(stage.calls.load(Ordering::Relaxed), 3);
    assert_eq!(stage.errors.load(Ordering::Relaxed), 2);
}

#[test]
fn native_profile_selection_is_bounded_across_callers_and_clean_at_zero() {
    let profile = NativeWriteProfile::default();
    assert!(!profile.select());
    assert_eq!(profile.barrier.calls.load(Ordering::Relaxed), 0);
    profile.remaining.store(7, Ordering::Relaxed);
    let selected = AtomicU64::new(0);
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let (profile, selected) = (&profile, &selected);
            scope.spawn(move || {
                if profile.select() {
                    selected.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });
    assert_eq!(selected.load(Ordering::Relaxed), 7);
    assert_eq!(profile.remaining.load(Ordering::Relaxed), 0);
}

#[test]
fn native_foreground_profile_excludes_maintenance_and_stops_at_limit() {
    let temp = tempdir::TempDir::new("sui-native-profile").unwrap();
    let shape = KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1));
    let mut config = Config::small();
    config.snapshot_written_bytes = u64::MAX;
    let db = Db::open_native(
        temp.path(),
        shape,
        Arc::new(config),
        Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
    .unwrap();
    db.set_native_durable(true);
    let ks = db.ks("root");
    db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![1]).unwrap();
    assert_eq!(
        db.native_write_profile
            .barrier
            .calls
            .load(Ordering::Relaxed),
        0
    );
    db.set_native_write_profile_limit(2);
    db.native_barrier(false).unwrap();
    assert_eq!(db.native_write_profile.remaining.load(Ordering::Relaxed), 2);
    for key in 1u32..4 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8])
            .unwrap();
    }
    let mut counters = std::collections::BTreeMap::new();
    db.visit_timing_metrics(|name, value| {
        counters.insert(name, value);
    });
    assert_eq!(counters["native_profile_limit"], 2);
    assert_eq!(counters["native_profile_available"], 1);
    assert_eq!(counters["native_profile_barrier_calls"], 2);
    assert_eq!(counters["native_profile_barrier_errors"], 0);
    assert!(counters["native_profile_file_calls"] >= 2);
    assert_eq!(
        counters["native_profile_file_calls"],
        counters["native_profile_file_successes"]
    );
    db.wait_for_background_threads_to_finish().unwrap();
}
