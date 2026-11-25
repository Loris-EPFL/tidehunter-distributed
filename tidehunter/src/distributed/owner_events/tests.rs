use super::*;

#[test]
fn disabled_owner_events_do_not_initialize_or_allocate_a_log() {
    let log = OnceLock::new();
    for _ in 0..100 {
        record_into(&log, OwnerEventKind::ProofStart, 0, 0, 0);
    }
    assert!(log.get().is_none());
}

#[test]
fn owner_events_capacity_overflow_counts_drops() {
    let log = OwnerEventLog::new(2);
    for _ in 0..5 {
        log.record(OwnerEventKind::ProofStart, 3, 7, 0);
    }
    assert_eq!(log.buffer.lock().events.len(), 2);
    assert_eq!(log.buffer.lock().events.capacity(), 2);
    assert_eq!(log.dropped.load(Ordering::Relaxed), 3);
}

#[test]
fn owner_events_json_round_trip() {
    let log = OwnerEventLog::new(2);
    log.record(OwnerEventKind::PrepareEnsureStart, 3, 7, 0);
    let dir = tempdir::TempDir::new("owner-events").unwrap();
    let path = dir.path().join("events.json");
    log.write_json(&path).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(json["schema"], "th-owner-events-v1");
    assert_eq!(
        json["origin_monotonic_ns"],
        log.buffer.lock().origin_monotonic_ns
    );
    assert_eq!(json["capacity"], 2);
    assert_eq!(json["dropped"], 0);
    let events: Vec<OwnerEvent> = serde_json::from_value(json["events"].clone()).unwrap();
    assert_eq!(events[0].kind, OwnerEventKind::PrepareEnsureStart);
    assert_eq!((events[0].partition, events[0].a, events[0].b), (3, 7, 0));
}

#[test]
fn owner_event_times_are_monotonic_per_thread() {
    let log = OwnerEventLog::new(400);
    std::thread::scope(|scope| {
        for partition in 0..4 {
            let log = &log;
            scope.spawn(move || {
                for a in 0..100 {
                    log.record(OwnerEventKind::ProofStart, partition, a, 0);
                }
            });
        }
    });
    let buffer = log.buffer.lock();
    for partition in 0..4 {
        let times: Vec<_> = buffer
            .events
            .iter()
            .filter(|e| e.partition == partition)
            .map(|e| e.t_ns)
            .collect();
        assert_eq!(times.len(), 100);
        assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
    }
}

#[test]
fn owner_events_drain_restarts_without_prior_events_or_drops() {
    let log = OwnerEventLog::new(1);
    log.record(OwnerEventKind::ControlReceived, 3, 1, 18);
    log.record(OwnerEventKind::ProofStart, 3, 4096, 0);
    let old_origin = log.buffer.lock().origin_monotonic_ns;
    let dir = tempdir::TempDir::new("owner-events-drain").unwrap();
    let first = dir.path().join("first.json");
    log.drain_json(&first).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(first).unwrap()).unwrap();
    assert_eq!(json["events"].as_array().unwrap().len(), 1);
    assert_eq!(json["events"][0]["a"], 1);
    assert_eq!(json["dropped"], 1);
    assert_eq!(json["origin_monotonic_ns"], old_origin);
    assert!(log.buffer.lock().origin_monotonic_ns >= old_origin);
    assert!(log.buffer.lock().events.is_empty());
    assert_eq!(log.dropped.load(Ordering::Relaxed), 0);
    log.record(OwnerEventKind::ControlReceived, 3, 1, 33);
    let second = dir.path().join("second.json");
    log.drain_json(&second).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(second).unwrap()).unwrap();
    assert_eq!(json["events"].as_array().unwrap().len(), 1);
    assert_eq!(json["events"][0]["b"], 33);
    assert_eq!(json["dropped"], 0);
}
