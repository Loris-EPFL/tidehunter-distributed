//! Public database API histories shared by the authority and parallel engines.
//! Seeds fix operations, key distributions, topology and crash cuts, not OS
//! thread interleavings. Every observed interleaving goes through the independent
//! sequential oracle in the parent module. Local-file leased I/O is not a power
//! loss simulator or a kernel NVMe-oF proof.
use super::*;
use rand::seq::SliceRandom;

pub(super) fn wave(
    db: &ConcurrentRemoteDatabase,
    readers: &Readers,
    recorder: &Recorder,
    case: &Case,
    operations: &[[u8; 16]],
    plan: &[Vec<Edit>],
    wave: usize,
) {
    // Four identities, each invoked twice by eight callers. A barrier makes
    // every invocation overlap, including duplicate identities during flight.
    let mut ids = std::array::from_fn::<_, 8, _>(|caller| wave * 4 + caller % 4);
    ids.shuffle(&mut StdRng::seed_from_u64(case.seed ^ wave as u64));
    let barrier = Barrier::new(9);
    thread::scope(|scope| {
        for id in ids {
            let barrier = &barrier;
            scope.spawn(move || {
                let call = recorder.begin(Action::Write {
                    operation: id,
                    edits: plan[id].clone(),
                });
                barrier.wait();
                let version = db
                    .write_batch(operations[id], mutations(&plan[id], case.value_bytes()))
                    .unwrap();
                recorder.ack(call, format!("{version:?}"));
            });
        }
        barrier.wait();
        observe(db, readers, recorder);
    });
}

fn case(seed: u64, owners: usize, hot: bool, native: bool) -> Case {
    Case {
        concurrent: true,
        native,
        gate_waves: 1 + (seed as usize & 1),
        ..Case::rpc(seed, owners, hot)
    }
}

#[test]
fn write_model_concurrent_bounded() {
    let started = Instant::now();
    for (owners, hot, native, boundary) in [
        (1, false, false, ""),
        (16, true, true, ""),
        (2, true, false, "publication_before"),
        (4, false, true, "checkpoint_root_after"),
    ] {
        run_case(&Case {
            boundary: boundary.into(),
            ..case(0xc011 + owners as u64, owners, hot, native)
        });
    }
    assert!(started.elapsed() < Duration::from_secs(60));
}

/// Optional filters leave the default 1000-schedule matrix unchanged. Start and
/// count select an exact subrange for replaying one retained failing schedule.
fn schedule_range() -> std::ops::Range<u64> {
    let start = env_number("TH_MODEL_START", 0);
    let count = env_number("TH_MODEL_ITERATIONS", 1000);
    assert!(start < 1000 && (1..=1000 - start).contains(&count));
    start..start + count
}

fn selection(name: &str, all: u64) -> u64 {
    let value = env_number(name, all);
    assert!(value <= all, "{name} out of range");
    value
}

#[test]
#[ignore = "1000 seeded schedules per owner count/distribution/tier; real subprocess crashes"]
fn write_model_concurrent_thousand_schedules() {
    // Unfiltered, each configuration gets exactly 1000 generated schedules.
    // Rotate all exposed cuts plus a no-cut control; never sample away a cut.
    let owner_filter = env_number("TH_MODEL_OWNERS", 0);
    assert!(owner_filter <= 16, "TH_MODEL_OWNERS must be 0..=16");
    let hot_filter = selection("TH_MODEL_HOT", 2);
    let native_filter = selection("TH_MODEL_NATIVE", 2);
    let seed = env_number("TH_MODEL_SEED", 0xc011);
    let schedules = schedule_range();
    let per_cell = per_cell_selection();
    let boundaries = if per_cell {
        per_cell_cuts(BOUNDARIES)
    } else {
        BOUNDARIES.to_vec()
    };
    for owners in 1..=16 {
        if owner_filter != 0 && owner_filter != owners as u64 {
            continue;
        }
        for hot in [false, true] {
            if hot_filter != 2 && hot_filter != u64::from(hot) {
                continue;
            }
            for native in [false, true] {
                if native_filter != 2 && native_filter != u64::from(native) {
                    continue;
                }
                for schedule in schedules.clone() {
                    let boundary = schedule as usize % (boundaries.len() + 1);
                    run_case(&Case {
                        boundary: boundary
                            .checked_sub(1)
                            .map_or_else(String::new, |index| boundaries[index].into()),
                        on_disk_history: schedule % 2 != 0,
                        per_cell,
                        ..case(seed.wrapping_add(schedule), owners, hot, native)
                    });
                }
            }
        }
    }
}

/// TH_MODEL_PER_CELL=1 selects the option C commit path and its cuts, in both
/// history modes; its reopen cuts run in `write_model_per_cell_crashes`.
fn per_cell_selection() -> bool {
    match env_number("TH_MODEL_PER_CELL", 0) {
        0 => false,
        1 => true,
        other => panic!("TH_MODEL_PER_CELL must be 0 or 1, got {other}"),
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
#[ignore = "1000 seeded schedules per leased owner count/distribution/durability policy"]
fn write_model_concurrent_leased_thousand_schedules() {
    let owner_filter = env_number("TH_MODEL_OWNERS", 0);
    assert!(owner_filter <= 16, "TH_MODEL_OWNERS must be 0..=16");
    let hot_filter = selection("TH_MODEL_HOT", 2);
    let native_filter = selection("TH_MODEL_NATIVE", 2);
    let profile_filter = selection("TH_MODEL_LEASED", 8);
    let seed = env_number("TH_MODEL_SEED", 0xc011);
    let schedules = schedule_range();
    let per_cell = per_cell_selection();
    assert!(
        profile_filter == 8 || native_filter == 2 || u64::from(profile_filter < 4) == native_filter,
        "TH_MODEL_NATIVE conflicts with TH_MODEL_LEASED"
    );
    for owners in 1..=16 {
        if owner_filter != 0 && owner_filter != owners as u64 {
            continue;
        }
        for hot in [false, true] {
            if hot_filter != 2 && hot_filter != u64::from(hot) {
                continue;
            }
            for profile in 0..8 {
                if profile_filter != 8 && profile_filter != profile as u64 {
                    continue;
                }
                if native_filter != 2 && native_filter != u64::from(profile < 4) {
                    continue;
                }
                for schedule in schedules.clone() {
                    let case = Case {
                        leased: Some(leased::profile(profile)),
                        per_cell,
                        ..case(seed.wrapping_add(schedule), owners, hot, false)
                    };
                    let boundaries = leased::boundaries(&case);
                    let boundary = schedule as usize % (boundaries.len() + 1);
                    run_case(&Case {
                        boundary: boundary
                            .checked_sub(1)
                            .map_or_else(String::new, |index| boundaries[index].into()),
                        ..case
                    });
                }
            }
        }
    }
}

#[test]
#[ignore = "exhaustive RPC crash cuts in both tiers; bounded controls run by default"]
fn write_model_concurrent_every_rpc_boundary() {
    for native in [false, true] {
        for (index, boundary) in BOUNDARIES.iter().enumerate() {
            run_case(&Case {
                boundary: (*boundary).into(),
                on_disk_history: index % 2 != 0,
                ..case(
                    0xc011 + index as u64,
                    1 + index % 16,
                    index % 2 == 0,
                    native,
                )
            });
        }
    }
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
#[ignore = "exhaustive leased crash cuts in Native, GroupFlush and FUA"]
fn write_model_concurrent_every_leased_boundary() {
    for profile in [0, 4, 6] {
        let case = Case {
            leased: Some(leased::profile(profile)),
            ..case(0xc011 + profile as u64, 2, profile == 0, false)
        };
        for boundary in leased::boundaries(&case) {
            run_case(&Case {
                boundary: boundary.into(),
                ..case.clone()
            });
        }
    }
}

fn fixture() -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("concurrent-model-scenario").unwrap(),
        2,
        128,
        KeyShape::new_single(4, 8, KeyType::uniform(8)),
        2,
        |config| {
            config.storage.batch_limits.max_operations = 32;
            config.max_index_bytes = 16 * 1024 * 1024;
        },
        |config| super::super::super::tcp::StorageNode::open(config, recovery_metrics()).unwrap(),
    )
    .synchronous()
}

#[test]
fn write_model_retry_during_flight_after_publication_and_flush_in_flight() {
    let fixture = fixture();
    let original = fixture.open();
    let (entered, release) = fixture.nodes[0]
        .pause_next_persist(original.epoch())
        .unwrap();
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let operation = fixture.operation(0, 42);
    let edits = vec![
        Edit {
            key: 0,
            value: Some(1),
        },
        Edit {
            key: 1,
            value: Some(1),
        },
    ];
    // Two writers share a capacity-two result channel; the flusher has one
    // result slot. All senders are joined after releasing the persistence gate.
    let (written, writes) = mpsc::sync_channel(2);
    let (flushed, flush) = mpsc::sync_channel(1);
    let (invoked, invocations) = mpsc::sync_channel(2);
    thread::scope(|scope| {
        let written_first = written.clone();
        let invoked_retry = invoked.clone();
        let edits = &edits;
        let db = &db;
        scope.spawn(move || {
            written_first
                .send(db.write_batch(operation, mutations(edits, 8)))
                .unwrap()
        });
        let paused = entered.recv_timeout(Duration::from_secs(5));
        scope.spawn(move || {
            invoked_retry.send(()).unwrap();
            written
                .send(db.write_batch(operation, mutations(edits, 8)))
                .unwrap();
        });
        scope.spawn(move || {
            // Invocation is recorded while the original write is held at sync.
            invoked.send(()).unwrap();
            flushed.send(db.flush_durable()).unwrap();
        });
        let retry_invoked = invocations.recv_timeout(Duration::from_secs(5));
        let flush_invoked = invocations.recv_timeout(Duration::from_secs(5));
        let deadline = Instant::now() + Duration::from_secs(5);
        let both_reserved = (0..1_000_000)
            .find_map(|_| {
                if db.pipeline_stats().concurrent_queued_peak >= 2 {
                    return Some(true);
                }
                if Instant::now() >= deadline {
                    return Some(false);
                }
                thread::yield_now();
                None
            })
            .unwrap_or(false);
        let before_release = writes.try_recv();
        let flush_before_release = flush.try_recv();
        // Release before asserting so a failed assertion cannot strand a join.
        release.send(()).unwrap();
        paused.unwrap();
        retry_invoked.unwrap();
        flush_invoked.unwrap();
        assert!(
            both_reserved,
            "retry never reserved while first write was in flight"
        );
        assert!(matches!(before_release, Err(mpsc::TryRecvError::Empty)));
        assert!(matches!(
            flush_before_release,
            Err(mpsc::TryRecvError::Empty)
        ));
    });
    let first = writes
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    let retry = writes
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    assert_eq!(first, retry);
    flush.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    // A published retry must not overwrite a newer value or allocate a version.
    db.write_batch(
        fixture.operation(1, 43),
        mutations(
            &[Edit {
                key: 0,
                value: Some(2),
            }],
            8,
        ),
    )
    .unwrap();
    assert_eq!(
        db.write_batch(operation, mutations(&edits, 8)).unwrap(),
        first
    );
    assert_eq!(
        db.get(KeySpace(0), &key_bytes(0))
            .unwrap()
            .unwrap()
            .as_ref(),
        2u64.to_le_bytes()
    );
    assert!(
        db.write_batch(
            operation,
            mutations(
                &[Edit {
                    key: 0,
                    value: Some(99)
                }],
                8
            )
        )
        .is_err()
    );
    db.flush_index().unwrap();
    db.shutdown().unwrap();
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(
        reopened
            .write_batch(operation, mutations(&edits, 8))
            .unwrap(),
        first
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &key_bytes(0))
            .unwrap()
            .unwrap()
            .as_ref(),
        2u64.to_le_bytes()
    );
    reopened.shutdown().unwrap();
}

#[test]
fn write_model_uncertain_append_fail_stops_and_recovers_identity() {
    let fixture = fixture();
    let db = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    let operation = fixture.operation(0, 99);
    let edits = vec![
        Edit {
            key: 0,
            value: Some(7),
        },
        Edit {
            key: 1,
            value: Some(7),
        },
    ];
    fixture.nodes[0].inject_drop_next_append_response();
    let error = db.write_batch(operation, mutations(&edits, 8)).unwrap_err();
    let uncertain = error
        .uncertain_version
        .expect("lost append reply must be unknown, not aborted");
    assert!(
        db.write_batch(fixture.operation(1, 100), mutations(&edits, 8))
            .is_err()
    );
    assert!(db.write_batch(operation, mutations(&edits, 8)).is_err());
    assert!(db.get(KeySpace(0), &key_bytes(0)).is_err());
    assert!(db.published_cut().is_err());
    assert!(db.flush_durable().is_err());
    assert!(db.flush_index().is_err());
    assert!(db.shutdown().is_err());
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    assert_eq!(
        reopened
            .write_batch(operation, mutations(&edits, 8))
            .unwrap(),
        uncertain
    );
    for key in 0..2 {
        assert_eq!(
            reopened
                .get(KeySpace(0), &key_bytes(key))
                .unwrap()
                .unwrap()
                .as_ref(),
            7u64.to_le_bytes()
        );
    }
    reopened.shutdown().unwrap();
}

/// A fresh lane has no file grant to bypass owner reference admission. This
/// keeps close paused after the coordinator has synced and acknowledged bytes
/// in its pre-leased successor, so the cold-grant check is not a timing race.
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn write_model_cold_live_read_returns_acknowledged_unadopted_value() {
    use crate::distributed::storage::LivePausePoint;
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("cold-leased-read").unwrap(),
        1,
        128,
        KeyShape::new_single(4, 8, KeyType::uniform(8)),
        1,
        |config| {
            config.storage.batch_limits.max_operations = 57;
            config.max_index_bytes = 16 * 1024 * 1024;
            config.timeout = Duration::from_secs(5);
            leased::configure(config, leased::profile(4));
        },
        leased::open_node,
    )
    .synchronous();
    let mut original = fixture.open();
    leased::prepare_database(&mut original);
    let writer = original.core.clients[0].client.clone();
    let log = fixture.nodes[0].log_for_test(original.epoch());
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    let first: Vec<_> = (0..57)
        .map(|key| Edit {
            key,
            value: Some(1),
        })
        .collect();
    db.write_batch(fixture.operation(0, 500), mutations(&first, 4096))
        .unwrap();
    let (entered, release) = log.pause_live(LivePausePoint::CloseSuffix);
    let second: Vec<_> = (32..54)
        .map(|key| Edit {
            key,
            value: Some(2),
        })
        .collect();
    let operation = fixture.operation(0, 501);
    let (written, result) = mpsc::sync_channel(1);
    let read = thread::scope(|scope| {
        let db = &db;
        scope.spawn(|| {
            written
                .send(db.write_batch(operation, mutations(&second, 4096)))
                .unwrap()
        });
        let paused = entered.recv_timeout(Duration::from_secs(5));
        let acknowledged = result.recv_timeout(Duration::from_secs(5));
        let read = if acknowledged.as_ref().is_ok_and(|result| result.is_ok()) {
            let (cut, _pin) = db.protected_published_cut().unwrap();
            let reference = db
                .index_reference(KeySpace(0), &key_bytes(32), cut.clone())
                .unwrap()
                .unwrap();
            eprintln!(
                "cold_live_read cut={cut:?} reference={reference:?} owner_appended={} owner_synced={} completed_released_durable={:?}",
                log.appended_frontier(),
                log.synced_frontier(),
                writer.lock().leased_test_completion_prefixes(),
            );
            Some(
                db.submit_get_cut(KeySpace(0), &key_bytes(32), cut)
                    .and_then(|pending| db.finish_ready_get(pending.wait())),
            )
        } else {
            None
        };
        // Always unblock the adoption worker before assertions or shutdown.
        release.send(()).unwrap();
        paused.unwrap();
        acknowledged.unwrap().unwrap();
        read.unwrap()
    });
    let shutdown = db.shutdown();
    let value = read.expect("a cold live read must return the synchronously acknowledged value");
    assert_eq!(value.unwrap().as_ref(), 2u64.to_le_bytes().repeat(512));
    shutdown.unwrap();
}
