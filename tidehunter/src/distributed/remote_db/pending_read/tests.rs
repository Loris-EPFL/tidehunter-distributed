use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;

pub(super) fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

pub(super) fn reopen_cold(fixture: &Fixture, mut db: RemoteDatabase) -> RemoteDatabase {
    db.shutdown().unwrap();
    drop(db);
    fixture.open()
}

pub(super) fn node_for(db: &RemoteDatabase, key: u32) -> usize {
    let reference = db
        .index_reference(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    db.partition_indexes[&reference.batch_address.partition]
}
pub(super) fn value(byte: u8) -> Option<Bytes> {
    Some(vec![byte; 32].into())
}

#[test]
fn nonconcurrent_bulk_records_pin_lock_separately() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.set_read_profile_limit(1);
    let monitor = db.read_profile_monitor();
    let values = db
        .get_many(KeySpace(0), &[0u32.to_be_bytes().to_vec()])
        .unwrap();
    assert_eq!(values[0], value(7));
    let snapshot = monitor.coordinator_resolution_snapshot();
    // One acquisition for cut capture, one for member lookup, one for pinning.
    assert_eq!(snapshot.index_lock_wait.count, 3);
    assert_eq!(snapshot.index_lookup.count, 1);
    assert_eq!(snapshot.cut_capture.count, 1);
    assert_eq!(snapshot.pin_preparation.count, 1);
    db.shutdown().unwrap();
}

#[test]
fn owner_grant_stage_aggregation_retains_counts_and_maxima() {
    let diagnostics = ReadDiagnostics::default();
    let mut profile = super::super::super::tcp::TcpReadProfile::default();
    profile.server_grant.registration.ns = 12;
    profile.server_grant.registration.count = 1;
    profile.server_grant.registration.max_ns = 12;
    profile.server_grant.registration.errors = 1;
    diagnostics.add_grant_stages(&profile);
    profile.server_grant.registration.ns = 7;
    profile.server_grant.registration.max_ns = 7;
    profile.server_grant.registration.errors = 0;
    diagnostics.add_grant_stages(&profile);
    assert_eq!(
        diagnostics
            .server_grant_registration_ns
            .load(Ordering::Relaxed),
        19
    );
    assert_eq!(
        diagnostics
            .server_grant_registration_count
            .load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        diagnostics
            .server_grant_registration_max_ns
            .load(Ordering::Relaxed),
        12
    );
    assert_eq!(
        diagnostics
            .server_grant_registration_errors
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn failed_client_only_profiles_do_not_claim_owner_timing() {
    let diagnostics = ReadDiagnostics::default();
    let mut profile = super::super::super::tcp::TcpReadProfile {
        server_handler_not_applicable: true,
        client_fi_read: super::super::super::tcp::ReadStageProfile {
            count: 1,
            errors: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    diagnostics.inline_complete(0, Some(profile), true);
    diagnostics.complete(&Err(LocalDbError::new("failed read")), Some(profile));
    assert_eq!(diagnostics.client_fi_read_count.load(Ordering::Relaxed), 2);
    assert_eq!(diagnostics.client_fi_read_errors.load(Ordering::Relaxed), 2);
    assert_eq!(
        diagnostics
            .failed_owner_profile_retained
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        diagnostics
            .failed_owner_profile_not_applicable
            .load(Ordering::Relaxed),
        2
    );
    profile.server_handler_not_applicable = false;
    diagnostics.inline_complete(0, Some(profile), true);
    assert_eq!(
        diagnostics
            .failed_owner_profile_unavailable
            .load(Ordering::Relaxed),
        1
    );
    profile.server_handler_available = true;
    diagnostics.complete(&Err(LocalDbError::new("failed read")), Some(profile));
    assert_eq!(
        diagnostics
            .failed_owner_profile_retained
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn partition_visitor_exports_client_grant_and_fi_read_stages() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let monitor = db.read_profile_monitor();
    let (partition, diagnostics) = &monitor.state.owner_diagnostics[0];
    let mut profile = super::super::super::tcp::TcpReadProfile::default();
    profile.client_grant_install.ns = 11;
    profile.client_grant_install.count = 1;
    profile.client_grant_install.max_ns = 11;
    profile.client_fi_read.ns = 23;
    profile.client_fi_read.count = 1;
    profile.client_fi_read.max_ns = 23;
    diagnostics.inline_complete(0, Some(profile), false);
    let mut exported = std::collections::HashMap::new();
    monitor.visit_partitions(&mut |owner, name, value| {
        if owner == *partition {
            exported.insert(name, value);
        }
    });
    assert_eq!(exported["read_profile_client_grant_install_ns"], 11);
    assert_eq!(exported["read_profile_client_grant_install_count"], 1);
    assert_eq!(exported["read_profile_client_grant_install_max_ns"], 11);
    assert_eq!(exported["read_profile_client_fi_read_ns"], 23);
    assert_eq!(exported["read_profile_client_fi_read_count"], 1);
    assert_eq!(exported["read_profile_client_fi_read_max_ns"], 23);
    db.shutdown().unwrap();
}

#[test]
fn resident_lane_snapshots_cover_actual_sessions_and_reject_stopped_service() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let monitor = db.read_evidence_monitor();
    let snapshots = monitor.sample_resident_read_lane_snapshots().unwrap();
    assert_eq!(
        snapshots.len(),
        fixture.config.endpoints.len() * fixture.config.read_lanes
    );
    for endpoint in &fixture.config.endpoints {
        let lanes = snapshots
            .iter()
            .filter(|row| row.partition == endpoint.partition)
            .collect::<Vec<_>>();
        assert_eq!(lanes.len(), fixture.config.read_lanes);
        for (lane, row) in lanes.iter().enumerate() {
            assert_eq!(row.lane, lane);
            assert_eq!(row.session_present, Some(true));
            assert_eq!(row.stats.grants_live, 0);
        }
    }
    db.shutdown().unwrap();
    assert!(monitor.sample_resident_read_lane_snapshots().is_err());
}

#[test]
fn operational_sampling_releases_read_ownership_before_activation_and_after_shutdown() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(42, 7)])
        .unwrap();
    assert_eq!(db.get(KeySpace(0), &42u32.to_be_bytes()).unwrap(), value(7));
    let grants = db.read_evidence_monitor().sample_owner_grants().unwrap();
    assert_eq!(grants.len(), fixture.config.endpoints.len());
    assert_eq!(
        grants
            .iter()
            .map(|(partition, _)| *partition)
            .collect::<Vec<_>>(),
        fixture
            .config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.partition)
            .collect::<Vec<_>>()
    );
    assert!(Arc::get_mut(&mut db.reads).is_some());

    let monitor = db.operational_read_io_monitor();
    let second_monitor = db.operational_read_io_monitor();
    let deadline = Instant::now() + Duration::from_secs(10);
    let generation = loop {
        let mut complete = None;
        monitor.visit(|snapshot, stopped| {
            assert!(!stopped);
            if snapshot.available && snapshot.resident_available && snapshot.grants_available {
                assert_eq!(
                    snapshot
                        .owners
                        .iter()
                        .map(|(_, stats)| stats.completed_reads)
                        .sum::<u64>(),
                    1
                );
                complete = Some(snapshot.generation);
                assert_eq!(snapshot.grants.len(), fixture.config.endpoints.len());
            }
        });
        if let Some(generation) = complete {
            break generation;
        }
        assert!(
            Instant::now() < deadline,
            "operational sampler did not publish a complete sample"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    second_monitor.visit(|snapshot, stopped| {
        assert!(!stopped);
        assert!(snapshot.generation >= generation);
        assert!(snapshot.available && snapshot.resident_available);
    });

    db.shutdown().unwrap();
    assert!(Arc::get_mut(&mut db.reads).is_some());
    let mut generation_at_stop = 0;
    monitor.visit(|snapshot, stopped| {
        assert!(stopped);
        assert!(!snapshot.available && !snapshot.resident_available);
        assert!(!snapshot.grants_available);
        generation_at_stop = snapshot.generation;
    });
    let after_stop_monitor = db.operational_read_io_monitor();
    assert!(Arc::get_mut(&mut db.reads).is_some());
    std::thread::sleep(Duration::from_millis(1100));
    after_stop_monitor.visit(|snapshot, stopped| {
        assert!(stopped);
        assert!(!snapshot.available && !snapshot.resident_available);
        assert_eq!(snapshot.generation, generation_at_stop);
    });
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.get(KeySpace(0), &42u32.to_be_bytes()).unwrap(),
        value(7)
    );
    reopened.shutdown().unwrap();
}
#[test]
fn sampled_owner_profiles_are_attributed_to_configured_partitions() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    assert_eq!(db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(), value(8));
    let mut global = std::collections::BTreeMap::new();
    monitor.visit(&mut |name, value| {
        global.insert(name, value);
    });
    let mut owners = std::collections::BTreeMap::new();
    monitor.visit_partitions(&mut |partition, name, value| {
        owners.insert((partition, name), value);
    });
    for partition in [0, 1] {
        assert_eq!(owners[&(partition, "read_profile_completed")], 1);
        assert_eq!(
            owners[&(partition, "read_profile_server_response_recheck_samples")],
            1
        );
    }
    for name in [
        "read_profile_completed",
        "read_profile_server_response_recheck_samples",
        "read_profile_server_executor_requests",
        "read_profile_server_async_count",
    ] {
        assert_eq!(
            owners
                .iter()
                .filter(|((_, key), _)| *key == name)
                .map(|(_, value)| *value)
                .sum::<u64>(),
            global[name]
        );
    }
    db.shutdown().unwrap();
}

#[test]
fn bounded_read_profiles_count_rpc_misses_not_hits_absences_or_backpressure() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(2, 9)],
    )
    .unwrap();
    let mut db = reopen_cold(&fixture, db);
    db.set_read_profile_limit(2);
    let monitor = db.read_profile_monitor();
    let count = |name| {
        let mut value = None;
        monitor.visit(&mut |key, n| {
            if key == name {
                value = Some(n);
            }
        });
        value.expect("named diagnostic counter")
    };
    let absent = db
        .submit_get(KeySpace(0), &99u32.to_be_bytes())
        .unwrap()
        .wait();
    assert_eq!(db.finish_ready_get(absent).unwrap(), None);
    assert_eq!(count("read_profile_selected"), 0);

    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert!(RemoteDatabase::read_backpressured(
        &db.submit_get(KeySpace(0), &1u32.to_be_bytes())
            .err()
            .unwrap()
    ));
    assert_eq!(count("read_profile_selected"), 1);
    drop(held);
    assert_eq!(db.finish_ready_get(first.wait()).unwrap(), value(7));
    let hit = db
        .submit_get(KeySpace(0), &0u32.to_be_bytes())
        .unwrap()
        .wait();
    assert_eq!(db.finish_ready_get(hit).unwrap(), value(7));
    assert_eq!(count("read_profile_selected"), 1);

    for key in 1u32..3 {
        let completed = db
            .submit_get(KeySpace(0), &key.to_be_bytes())
            .unwrap()
            .wait();
        assert_eq!(
            db.finish_ready_get(completed).unwrap(),
            value(key as u8 + 7)
        );
    }
    assert_eq!(db.read_rpc_count(), 3);
    assert_eq!(count("read_profile_selected"), 2);
    assert_eq!(count("read_profile_completed"), 2);
    assert_eq!(count("read_profile_errors"), 0);
    assert_eq!(count("read_profile_returned"), 2);
    assert_eq!(count("read_profile_server_response_recheck_samples"), 2);
    assert_eq!(count("read_profile_server_available"), 1);
    assert_eq!(count("read_profile_transport_progress_available"), 1);
    assert_eq!(count("read_profile_transport_cq_available"), 0); // TCP, not missing EFA evidence.
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

fn synchronous_read_budget_case(scan: bool, byte_limited: bool) {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = if byte_limited { 256 } else { 1 };
    let mut db = fixture.open();
    // More than one scan page exercises both per-page shard admission and
    // lookahead competing with an earlier page for the same global credits.
    let count = if scan { 260u32 } else { 8u32 };
    for batch in 0..count.div_ceil(4) {
        let mutations = (batch * 4..(batch * 4 + 4).min(count))
            .map(|key| put(key, key as u8))
            .collect();
        db.write_batch(fixture.operation(batch % 2, batch as u64), mutations)
            .unwrap();
    }
    if byte_limited {
        let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
        let singleton_charge = db.reads.state.retained.lock().1;
        db.finish_get(pending).unwrap();
        // One member fits, but a same-shard group does not. Synchronous reads
        // must split into bounded waves rather than reject a legal collection.
        Arc::get_mut(&mut db.reads).unwrap().max_bytes = singleton_charge;
    }
    if scan {
        let rows = db.scan(KeySpace(0)).unwrap();
        assert_eq!(rows.len(), count as usize);
        for (index, (key, actual)) in rows.into_iter().enumerate() {
            assert_eq!(key.as_ref(), (index as u32).to_be_bytes().as_slice());
            assert_eq!(Some(actual), value(index as u8));
        }
    } else {
        let keys: Vec<_> = (0..count).map(|key| key.to_be_bytes().to_vec()).collect();
        assert_eq!(
            db.get_many(KeySpace(0), &keys).unwrap(),
            (0..count).map(|key| value(key as u8)).collect::<Vec<_>>()
        );
    }
    assert!(db.healthy);
    assert!(!db.reads.failed());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn synchronous_get_many_progresses_with_one_request_credit() {
    synchronous_read_budget_case(false, false);
}

#[test]
fn synchronous_scan_progresses_with_one_request_credit() {
    synchronous_read_budget_case(true, false);
}

#[test]
fn synchronous_get_many_splits_groups_into_byte_budget_waves() {
    synchronous_read_budget_case(false, true);
}

#[test]
fn synchronous_scan_splits_groups_into_byte_budget_waves() {
    synchronous_read_budget_case(true, true);
}

#[test]
fn external_read_pressure_does_not_poison_synchronous_conveniences() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let lane = db.reads.lane_client(0, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    let keys = vec![1u32.to_be_bytes().to_vec()];
    assert!(RemoteDatabase::read_backpressured(
        &db.get_many(KeySpace(0), &keys).unwrap_err()
    ));
    // A scan may join the occupied immutable reference. Let that real I/O
    // complete; the original unconsumed handle still owns the only physical
    // credit, so the scan's distinct next shard must report backpressure.
    let scan = thread::spawn(move || {
        let result = db.scan(KeySpace(0));
        (db, pending, result)
    });
    drop(held);
    let (mut db, pending, result) = scan.join().unwrap();
    assert!(RemoteDatabase::read_backpressured(&result.unwrap_err()));
    assert!(db.healthy);
    assert!(!db.reads.failed());
    assert_eq!(db.finish_get(pending).unwrap(), value(7));
    assert_eq!(db.get_many(KeySpace(0), &keys).unwrap(), vec![value(8)]);
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 2);
    db.shutdown().unwrap();
}

#[test]
fn byte_bounded_reads_accept_request_limits_above_channel_capacity() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = usize::MAX;
    fixture.config.max_pending_read_bytes = 4096;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    for batch in 0u32..2 {
        db.write_batch(
            fixture.operation(batch % 2, u64::from(batch)),
            (batch * 4..batch * 4 + 4)
                .map(|key| put(key, key as u8))
                .collect(),
        )
        .unwrap();
    }
    let mut db = reopen_cold(&fixture, db);
    let keys = (0u32..8)
        .map(|key| key.to_be_bytes().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        db.get_many(KeySpace(0), &keys).unwrap(),
        (0..8).map(value).collect::<Vec<_>>()
    );
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn oversized_scalar_read_does_not_poison_write_authority() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_read_bytes = ReadService::MIN_FLIGHT_BYTES;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    assert!(admission_error(
        &db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap_err()
    ));
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    assert!(db.exists(KeySpace(0), &1u32.to_be_bytes()).unwrap());
    db.shutdown().unwrap();
}

#[test]
fn sui_bulk_accepts_borrowed_keys_with_duplicates() {
    let fixture = Fixture::new();
    let mut original = fixture.open();
    original
        .write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    original.shutdown().unwrap();
    drop(original);
    let db = crate::distributed::sui::Db::open_remote(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap();
    let key = 0u32.to_be_bytes();
    let absent = 99u32.to_be_bytes();
    let keys: [&[u8]; 3] = [&key, &absent, &key];
    let results = db.multi_get(db.ks("root"), &keys);
    assert_eq!(results[0].as_ref().unwrap(), &value(7));
    assert_eq!(results[1].as_ref().unwrap(), &None);
    assert_eq!(results[2].as_ref().unwrap(), &value(7));
    db.wait_for_background_threads_to_finish().unwrap();
}

#[test]
fn cancelled_batch_keeps_accepted_flight_until_completion() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();
    let mut groups = vec![Vec::new(); db.partition_indexes.len()];
    groups[node].extend([(0, reference), (1, reference)]);
    let mut batch = ReadBatch::new(&db.reads, vec![None; 2], groups).unwrap();
    batch.dispatch(&db.reads).unwrap();
    let slot = batch.reads[0].1.parts[0].0.slot;
    drop(batch);
    assert_eq!(db.reads.state.retained.lock().0, 1);
    drop(held);
    assert!(db.reads.state.flight_ready[slot].wait_until(Instant::now() + Duration::from_secs(5)));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    db.shutdown().unwrap();
}

#[test]
fn cancelled_owned_batch_hands_accepted_transport_work_back_to_actor() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    let mut groups = vec![Vec::new(); db.partition_indexes.len()];
    groups[node].extend([(0, reference), (1, reference)]);
    let mut batch = ReadBatch::new(&db.reads, vec![None; 2], groups).unwrap();
    batch.dispatch(&db.reads).unwrap();
    assert_eq!(
        batch.owned_lanes.len(),
        1,
        "the caller must own the idle lane"
    );
    let slot = batch.reads[0].1.parts[0].0.slot;
    let driver = batch.owned_lanes[0].lane.driver.clone();
    let mut driver = driver.lock();
    let client = driver.client.clone();
    let mut client = client.lock();
    let mut request = driver.queued.pop_front().unwrap();
    assert!(client.read_admission(&request.references, false).unwrap());
    let references = std::mem::take(&mut request.references);
    let id = client
        .submit_read(db.epoch, references, request.deadline, request.class, false)
        .unwrap()
        .expect("idle transport must accept the request");
    driver.active.insert(
        id,
        InflightRead {
            request,
            started: None,
        },
    );
    drop(client);

    // Do not poll the transport before cancellation: the active request and
    // its credits must survive ownership handback, even if a reply is ready.
    drop(batch);
    assert_eq!(driver.active.len(), 1);
    assert_eq!(db.reads.state.retained.lock().0, 1);
    drop(driver);
    assert!(db.reads.state.flight_ready[slot].wait_until(Instant::now() + Duration::from_secs(5)));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), value(7));
    db.shutdown().unwrap();
}

#[test]
fn empty_caller_progress_hands_accepted_work_to_actor_and_preserves_order() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let reference = |key: u32| {
        db.index_reference(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap()
    };
    let first = reference(0);
    let second = reference(1);
    let node = db.partition_indexes[&first.batch_address.partition];
    assert_eq!(node, db.partition_indexes[&second.batch_address.partition]);
    let mut groups = vec![Vec::new(); db.partition_indexes.len()];
    groups[node].extend([(0, first), (1, second), (2, first)]);
    let mut batch = ReadBatch::new(&db.reads, vec![None; 3], groups).unwrap();
    batch.dispatch(&db.reads).unwrap();
    assert_eq!(batch.owned_lanes.len(), 1);
    let lane = batch.owned_lanes[0].lane.clone();
    let mut driver = lane.driver.lock();
    let mut client = lane.client.lock();
    let mut request = driver.queued.pop_front().unwrap();
    assert!(client.read_admission(&request.references, false).unwrap());
    let references = std::mem::take(&mut request.references);
    let id = client
        .submit_read(db.epoch, references, request.deadline, request.class, false)
        .unwrap()
        .expect("idle transport must accept the request");
    driver.active.insert(
        id,
        InflightRead {
            request,
            started: None,
        },
    );
    drop(driver);

    // Holding the client forces every caller sweep to be empty and prevents
    // the actor from retiring the accepted request before handback is observed.
    let (done, result) = mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        let outcome = batch.finish_one();
        done.send((outcome, batch)).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while lane.scheduling.lock().inline_reserved {
        assert!(
            Instant::now() < deadline,
            "empty progress did not hand the lane back"
        );
        thread::yield_now();
    }
    assert!(matches!(result.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert_eq!(db.reads.state.retained.lock().0, 1);
    drop(client);
    let (outcome, batch) = result.recv_timeout(Duration::from_secs(5)).unwrap();
    outcome.unwrap();
    waiter.join().unwrap();
    assert_eq!(
        batch.values,
        vec![Some(put(0, 7)), Some(put(1, 8)), Some(put(0, 7))]
    );
    assert!(batch.complete());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn grouped_join_of_inline_flight_notifies_admission_after_readiness() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    let (entered, waiting) = mpsc::sync_channel(1);
    let (release, resumed) = mpsc::sync_channel(1);
    *db.reads.state.inline_pause.lock() = Some((entered, resumed));
    let reads = db.reads.clone();
    let epoch = db.epoch;
    let scalar = thread::spawn(move || reads.read_inline(node, epoch, &reference, 0, None, None));
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut groups = vec![Vec::new(); db.partition_indexes.len()];
    groups[node].push((0, reference));
    let mut batch = ReadBatch::new(&db.reads, vec![None], groups).unwrap();
    batch.dispatch(&db.reads).unwrap();
    assert_eq!(batch.reads.len(), 1);
    assert!(batch.owned_lanes.is_empty());
    let generation = db.reads.admission_generation();
    release.send(()).unwrap();
    assert!(scalar.join().unwrap().unwrap().is_some());
    // The joiner still owns the flight and both credits, so a credit drop
    // cannot be responsible for advancing this generation.
    assert!(db.reads.admission_generation() > generation);
    batch.finish_one().unwrap();
    assert_eq!(batch.values[0], Some(put(0, 7)));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn batch_retires_ready_owner_before_blocked_owner_and_preserves_duplicates() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.max_pending_reads = 4;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let first_node = db.partition_indexes[&first.batch_address.partition];
    let second_node = db.partition_indexes[&second.batch_address.partition];
    assert!(first_node < second_node);
    db.set_read_profile_limit(2);
    let lane = db.reads.lane_client(first_node, 0);
    let held = lane.lock();
    let mut groups = vec![Vec::new(); db.partition_indexes.len()];
    groups[first_node].push((0, first));
    groups[second_node].extend([(1, second), (2, second)]);
    let mut batch = ReadBatch::new(&db.reads, vec![None; 3], groups).unwrap();
    batch.dispatch(&db.reads).unwrap();
    assert_eq!(batch.reads.len(), 2);
    batch.finish_one().unwrap();
    assert!(batch.values[0].is_none());
    assert!(
        matches!(&batch.values[1], Some(Mutation::Put { value, .. }) if value.as_ref() == [8; 32])
    );
    assert!(
        matches!(&batch.values[2], Some(Mutation::Put { value, .. }) if value.as_ref() == [8; 32])
    );
    assert_eq!(db.reads.state.retained.lock().0, 1);
    drop(held);
    batch.finish_one().unwrap();
    assert!(
        matches!(&batch.values[0], Some(Mutation::Put { value, .. }) if value.as_ref() == [7; 32])
    );
    assert!(batch.complete());
    assert_eq!(
        db.reads.state.diagnostics.completed.load(Ordering::Relaxed),
        2
    );
    assert_eq!(
        db.reads
            .state
            .diagnostics
            .inline_completed
            .load(Ordering::Relaxed)
            + db.reads
                .state
                .diagnostics
                .actor_completed
                .load(Ordering::Relaxed),
        2
    );
    for node in [first_node, second_node] {
        assert_eq!(
            db.reads.state.owner_diagnostics[node]
                .1
                .completed
                .load(Ordering::Relaxed),
            1
        );
    }
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn inline_reads_reserve_idle_lanes_with_unrelated_retained_credits() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.max_pending_reads = 4;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let first_node = db.partition_indexes[&first.batch_address.partition];
    let second_node = db.partition_indexes[&second.batch_address.partition];
    assert_ne!(first_node, second_node);

    let lane = db.reads.lane_client(second_node, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 1);

    let inline = db
        .reads
        .read_inline(first_node, db.epoch, &first, 4, None, None)
        .unwrap()
        .expect("idle independent lane should make progress inline");
    assert!(
        matches!(inline.as_slice(), [Mutation::Put { value, .. }] if value.as_ref() == [7; 32])
    );
    assert_eq!(db.reads.state.retained.lock().0, 1);

    assert!(
        db.reads
            .read_inline(second_node, db.epoch, &second, 4, None, None)
            .unwrap()
            .is_none(),
        "queued actor ownership must block inline theft"
    );
    drop(held);
    assert_eq!(db.finish_get(pending).unwrap(), value(8));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn lane_reservation_backpressure_does_not_signal_false_capacity() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.max_pending_reads = 4;
    fixture.config.read_cache_bytes = 0;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    let lane = db.reads.lanes.read()[node][0].clone();
    lane.scheduling.lock().inline_reserved = true;
    let generation = db.reads.admission_generation();

    assert!(
        db.reads
            .submit(node, vec![reference], 0, false, None)
            .is_err()
    );
    assert_eq!(db.reads.admission_generation(), generation);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));

    lane.scheduling.lock().inline_reserved = false;
    db.shutdown().unwrap();
}

#[test]
fn inline_transport_failure_poison_is_terminal() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let reference = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = db.partition_indexes[&reference.batch_address.partition];
    db.clients[node]
        .client
        .lock()
        .fence_to(db.epoch + 1)
        .unwrap();
    assert!(
        db.reads
            .read_inline(node, db.epoch, &reference, 4, None, None)
            .is_err()
    );
    assert!(db.reads.failed());
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.shutdown().is_err());
}
#[test]
fn cached_hit_and_same_shard_miss_progress_while_first_miss_is_blocked() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(3, 10)],
    )
    .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(2, 9)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    assert_eq!(db.get(KeySpace(0), &2u32.to_be_bytes()).unwrap(), value(9));
    let first_node = node_for(&db, 0);
    assert_eq!(first_node, node_for(&db, 1));
    let lane = db.reads.lane_client(first_node, 0);
    let held = lane.lock();
    let mut first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert!(db.poll_get(&mut first).unwrap().is_none());
    let rpcs_before_hit = db.read_rpc_count();
    let mut hit = db.submit_get(KeySpace(0), &2u32.to_be_bytes()).unwrap();
    assert_eq!(db.poll_get(&mut hit).unwrap(), Some(value(9)));
    assert_eq!(db.read_rpc_count(), rpcs_before_hit);
    let second = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(second).unwrap(), value(8));
    assert!(db.poll_get(&mut first).unwrap().is_none());
    let third = db.submit_get(KeySpace(0), &3u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(third).unwrap(), value(10));
    drop(held);
    assert_eq!(db.finish_get(first).unwrap(), value(7));
    db.shutdown().unwrap();
}

#[test]
fn one_connection_admits_several_reads_and_retains_completion_credits() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    fixture.config.read_cache_bytes = 0;
    fixture.config.max_pending_reads = 4;
    let mut db = fixture.open();
    db.write_batch(
        fixture.operation(0, 0),
        vec![put(0, 7), put(1, 8), put(2, 9), put(3, 10)],
    )
    .unwrap();
    let lane = db.reads.lane_client(0, 0);
    let held = lane.lock();
    let mut pending = Vec::new();
    for key in 0u32..4 {
        pending.push(db.submit_get(KeySpace(0), &key.to_be_bytes()).unwrap());
    }
    assert_eq!(db.reads.state.retained.lock().0, 4);
    // An identical immutable miss joins even when all physical slots are full.
    let duplicate = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    assert_eq!(db.reads.state.retained.lock().0, 4);
    assert!(!db.reads.failed());
    drop(held);
    assert_eq!(db.finish_get(duplicate).unwrap(), value(7));
    // Caller consumption order is independent of transport completion order.
    for (key, request) in pending.into_iter().enumerate().rev() {
        assert_eq!(db.finish_get(request).unwrap(), value(key as u8 + 7));
    }
    assert!(db.reads.state.peak_connection_reads.load(Ordering::Relaxed) >= 2);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert!(!db.reads.failed());
    db.shutdown().unwrap();
}

#[test]
fn pending_read_keeps_submission_reference_across_overwrite_and_delete() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let lane = db.reads.lane_client(0, 0);
    let held = lane.lock();
    let old = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    // Writes have separate authenticated lanes and do not wait for the reader.
    db.write_batch(fixture.operation(0, 1), vec![put(0, 8)])
        .unwrap();
    db.write_batch(
        fixture.operation(0, 2),
        vec![Mutation::Delete {
            keyspace: KeySpace(0),
            key: 0u32.to_be_bytes().to_vec().into(),
        }],
    )
    .unwrap();
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), None);
    drop(held);
    assert_eq!(db.finish_get(old).unwrap(), value(7));
    assert_eq!(db.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(), None);
    db.shutdown().unwrap();
}

#[test]
fn dropped_handle_retains_worker_credit_and_shutdown_joins_before_release() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_reads = 1;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(first);
    assert_eq!(db.reads.state.retained.lock().0, 1);
    let error = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .err()
        .unwrap();
    assert!(RemoteDatabase::read_backpressured(&error));
    assert!(!db.reads.failed());
    let state = db.reads.state.clone();
    let (done, completed) = mpsc::sync_channel(1);
    let closer = thread::spawn(move || {
        let result = db.shutdown();
        done.send(result).unwrap();
    });
    assert!(matches!(
        completed.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    drop(held);
    completed
        .recv_timeout(Duration::from_secs(10))
        .unwrap()
        .unwrap();
    closer.join().unwrap();
    assert_eq!(*state.retained.lock(), (0, 0));
    // The native authority lock is gone only after lane shutdown finishes.
    let mut reopened = fixture.open();
    assert_eq!(
        reopened.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(),
        value(8)
    );
    reopened.shutdown().unwrap();
}

#[test]
fn byte_admission_counts_completed_but_unconsumed_results() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let first = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    if let GetState::Pending { read, .. } = &first.state {
        // Observe the real completion without consuming caller-owned credits.
        let slot = read.parts[0].0.slot;
        let completion = &read.parts[0].0.completion;
        let deadline = Instant::now() + Duration::from_secs(5);
        assert!(db.reads.state.flight_ready[slot].wait_until(deadline));
        assert!(completion.result.lock().is_some());
    } else {
        panic!("expected miss");
    }
    let charge = db.reads.state.retained.lock().1;
    Arc::get_mut(&mut db.reads).unwrap().max_bytes = charge;
    let error = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .err()
        .unwrap();
    assert!(RemoteDatabase::read_backpressured(&error));
    assert_eq!(db.finish_get(first).unwrap(), value(7));
    Arc::get_mut(&mut db.reads).unwrap().max_bytes = charge - 1;
    assert!(db.submit_get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    assert!(!db.reads.failed());
    Arc::get_mut(&mut db.reads).unwrap().max_bytes = charge;
    let second = db.submit_get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    assert_eq!(db.finish_get(second).unwrap(), value(8));
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn published_result_stays_unavailable_until_credit_retirement() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let state = db.reads.state.clone();
    let credit = Arc::new(Credit {
        state: state.clone(),
        bytes: 512,
        requests: 1,
    });
    *state.retained.lock() = (1, 512);
    state.flight_ready[0].begin();
    let flight = Arc::new(ReadFlight {
        node: 0,
        references: Vec::new(),
        slot: 0,
        grouped_inline: AtomicBool::new(false),
        completion: Arc::new(FlightCompletion::new()),
        _pin: None,
        _credit: credit.clone(),
    });
    *flight.completion.result.lock() = Some(Ok((vec![put(0, 7)], None)));
    let mut parts = SmallVec::new();
    parts.push((flight, 0));
    let read = ReadRequest {
        parts,
        credit: credit.clone(),
        deadline: Instant::now() + Duration::from_secs(5),
        sampled: false,
        profile: None,
    };
    drop(credit);

    assert!(read.try_result().unwrap().is_none());
    assert_eq!(*state.retained.lock(), (1, 512));

    let deadline = read.deadline;
    let waiter = thread::spawn(move || read.finish());
    while state.flight_ready[0].waiters.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(state.flight_ready[0].waiters.load(Ordering::Acquire), 1);
    assert_eq!(*state.retained.lock(), (1, 512));

    state.flight_ready[0].retire();
    let (values, _) = waiter.join().unwrap().unwrap();
    assert_eq!(values, vec![put(0, 7)]);
    assert_eq!(*state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn subset_joiner_keeps_cancelled_initiators_full_result_credit() {
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7), put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let first = db
        .index_reference(KeySpace(0), &0u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let second = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let node = node_for(&db, 0);
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();

    let initiator = db
        .reads
        .submit(node, vec![first, second], 0, false, None)
        .unwrap();
    let initiator_charge = initiator.credit.bytes;
    drop(initiator);
    let joiner = db.reads.submit(node, vec![first], 0, false, None).unwrap();
    let retained_after_join = initiator_charge + joiner.credit.bytes;
    let slot = joiner.parts[0].0.slot;
    drop(held);

    let deadline = Instant::now() + Duration::from_secs(5);
    assert!(db.reads.state.flight_ready[slot].wait_until(deadline));
    let retained = *db.reads.state.retained.lock();
    assert_eq!(retained.0, 1);
    assert_eq!(retained.1, retained_after_join);
    let (values, _) = joiner.finish().unwrap();
    assert_eq!(values, vec![put(0, 7)]);
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    db.shutdown().unwrap();
}

#[test]
fn dropped_failed_read_poison_cannot_be_hidden_by_a_cached_hit() {
    let mut fixture = Fixture::new();
    fixture.config.read_lanes = 1;
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    db.write_batch(fixture.operation(1, 1), vec![put(1, 8)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap();
    db.set_read_profile_limit(1);
    let profile = db.read_profile_monitor();
    let node = node_for(&db, 0);
    let lane = db.reads.lane_client(node, 0);
    let held = lane.lock();
    let pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    drop(pending);
    db.clients[node]
        .client
        .lock()
        .fence_to(db.epoch + 1)
        .unwrap();
    drop(held);
    db.reads.stop(); // deterministic completion barrier, including discarded replies
    assert!(db.reads.failed());
    assert_eq!(*db.reads.state.retained.lock(), (0, 0));
    let mut counts = std::collections::BTreeMap::new();
    profile.visit(&mut |name, value| {
        counts.insert(name, value);
    });
    assert_eq!(counts["read_profile_selected"], 1);
    assert_eq!(counts["read_profile_completed"], 1);
    assert_eq!(counts["read_profile_errors"], 1);
    assert_eq!(counts["read_profile_returned"], 0);
    assert_eq!(counts["read_profile_failed_owner_profile_retained"], 1);
    assert_eq!(counts["read_profile_failed_owner_profile_unavailable"], 0);
    assert_eq!(counts["read_profile_server_response_recheck_samples"], 1);
    assert_eq!(counts["read_profile_server_available"], 1);
    let mut owner_counts = std::collections::BTreeMap::new();
    profile.visit_partitions(&mut |partition, name, value| {
        owner_counts.insert((partition, name), value);
    });
    assert_eq!(
        owner_counts[&(0, "read_profile_failed_owner_profile_retained")],
        1
    );
    assert_eq!(
        owner_counts[&(0, "read_profile_server_response_recheck_samples")],
        1
    );
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.shutdown().is_err());
}

#[test]
fn expired_queue_poison_and_foreign_epoch_handles_are_rejected() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(0, 7)])
        .unwrap();
    let mut db = reopen_cold(&fixture, db);
    let lane = db.reads.lane_client(node_for(&db, 0), 0);
    let held = lane.lock();
    let mut pending = db.submit_get(KeySpace(0), &0u32.to_be_bytes()).unwrap();
    if let GetState::Pending { read, .. } = &mut pending.state {
        read.deadline = Instant::now() - Duration::from_secs(1);
    } else {
        panic!("expected miss");
    }
    assert!(db.poll_get(&mut pending).is_err());
    assert!(db.reads.failed());
    drop(held);
    drop(pending);
    assert!(db.shutdown().is_err());
    drop(db);
    let mut db = fixture.open();
    let old = db.submit_get(KeySpace(0), &999u32.to_be_bytes()).unwrap();
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert!(reopened.finish_get(old).is_err());
    assert_eq!(
        reopened.get(KeySpace(0), &0u32.to_be_bytes()).unwrap(),
        value(7)
    );
    reopened.shutdown().unwrap();
}

#[test]
fn detached_completion_rechecks_authority_after_wait() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(1, 7)])
        .unwrap();
    let completed = db
        .submit_get(KeySpace(0), &1u32.to_be_bytes())
        .unwrap()
        .wait();
    db.healthy = false;
    assert!(db.finish_ready_get(completed).is_err());
}

#[test]
fn detached_scan_index_mismatch_poisons_cached_reads() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(0, 0), vec![put(1, 7)])
        .unwrap();
    assert_eq!(db.get(KeySpace(0), &1u32.to_be_bytes()).unwrap(), value(7));
    let reference = db
        .index_reference(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
        .unwrap()
        .unwrap();
    let completed = db.submit_record(reference).unwrap().wait();
    assert!(
        db.finish_record(completed, KeySpace(0), &2u32.to_be_bytes())
            .is_err()
    );
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
}
