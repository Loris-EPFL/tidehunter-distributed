//! Public database API histories shared by the authority and parallel engines.
//! Seeds fix operations, key distributions, topology and crash cuts, not OS
//! thread interleavings. Every observed interleaving goes through the independent
//! sequential oracle in the parent module. Local-file leased I/O is not a power
//! loss simulator or a kernel NVMe-oF proof.
use super::*;
use rand::seq::SliceRandom;

pub(super) fn wave(
    db: &ConcurrentRemoteDatabase,
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
        observe(db, recorder);
    });
}

/// `profile` selects the leased write policy and owner tier (`leased::profile`).
fn case(seed: u64, owners: usize, hot: bool, profile: usize) -> Case {
    Case {
        concurrent: true,
        gate_waves: 1 + (seed as usize & 1),
        ..Case::new(seed, owners, hot, leased::profile(profile))
    }
}

/// Bounded default controls: profile 0 is the Native tier, profile 4 the
/// Synchronous tier with its default GroupFlush policy.
#[test]
fn write_model_concurrent_bounded() {
    for (owners, hot, profile, boundary) in [
        (1, false, 4, ""),
        (16, true, 0, ""),
        (2, true, 4, "publication_before"),
        // The Exact-era checkpoint root boundary no longer exists: the hybrid
        // snapshot emits no model boundary. Snapshot-install crash coverage
        // lives in `hybrid/snapshot.rs` (`SnapshotBoundary::{AfterFlush,
        // AfterIndexSync, AfterDurableTarget, AfterDrain}`) and its tests in
        // `hybrid/snapshot_tests.rs`; this case keeps a write-path cut.
        (4, false, 0, "publication_after"),
    ] {
        run_case(&Case {
            boundary: boundary.into(),
            ..case(0xc011 + owners as u64, owners, hot, profile)
        });
    }
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
#[ignore = "1000 seeded schedules per leased owner count/distribution/durability policy"]
fn write_model_concurrent_leased_thousand_schedules() {
    let owner_filter = env_number("TH_MODEL_OWNERS", 0);
    assert!(owner_filter <= 16, "TH_MODEL_OWNERS must be 0..=16");
    let hot_filter = selection("TH_MODEL_HOT", 2);
    let native_filter = selection("TH_MODEL_NATIVE", 2);
    let profile_filter = selection("TH_MODEL_LEASED", 8);
    let seed = env_number("TH_MODEL_SEED", 0xc011);
    let schedules = schedule_range();
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
                    let case = case(seed.wrapping_add(schedule), owners, hot, profile);
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
#[ignore = "exhaustive leased crash cuts in Native, GroupFlush and FUA"]
fn write_model_concurrent_every_leased_boundary() {
    for profile in [0, 4, 6] {
        let case = case(0xc011 + profile as u64, 2, profile == 0, profile);
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
        },
        |_| {},
    )
    .synchronous()
}

/// Wait until the writer submitted the frame whose completion it holds.
fn wait_held_flight(client: &Arc<Mutex<Client>>) -> Result<(), &'static str> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while client.lock().leased_flights_in_flight() == 0 {
        if Instant::now() >= deadline {
            return Err("held leased write was never submitted");
        }
        thread::yield_now();
    }
    Ok(())
}

#[test]
fn write_model_retry_during_flight_after_publication_and_flush_in_flight() {
    let fixture = fixture();
    let original = fixture.open();
    // Hold the completion of owner 0's next leased frame: the original write
    // stays in flight until the test releases it.
    let writer = original.core.clients[0].client.clone();
    writer.lock().leased_test_hold_next_completion();
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
    // result slot. All senders are joined after releasing the held completion.
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
        let paused = wait_held_flight(&writer);
        scope.spawn(move || {
            invoked_retry.send(()).unwrap();
            written
                .send(db.write_batch(operation, mutations(edits, 8)))
                .unwrap();
        });
        scope.spawn(move || {
            // Invocation is recorded while the original write is held in flight.
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
        writer.lock().leased_test_release_held();
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
    // The plain index flush the production wrapper exposes, not a relocation pass.
    db.write_core().flush_index().unwrap();
    db.shutdown().unwrap();
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(writer);
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    // OWNER decision (BOARD 2026-11-20, decision 2): the retry window is empty
    // after a restart, so a retry after the reopen is a new write. It takes a
    // version in the new epoch and carries the old payload, so it overwrites
    // the newer value 2 of key 0 with 1; a second retry inside this epoch
    // returns the new version and applies nothing.
    let retried = reopened
        .write_batch(operation, mutations(&edits, 8))
        .unwrap();
    assert!(retried.epoch > first.epoch, "{retried:?} vs {first:?}");
    assert_eq!(
        reopened
            .write_batch(operation, mutations(&edits, 8))
            .unwrap(),
        retried
    );
    assert_eq!(
        reopened
            .get(KeySpace(0), &key_bytes(0))
            .unwrap()
            .unwrap()
            .as_ref(),
        1u64.to_le_bytes()
    );
    reopened.shutdown().unwrap();
}

#[test]
fn write_model_uncertain_append_fail_stops_and_recovers_identity() {
    let fixture = fixture();
    let original = fixture.open();
    // Owner 0's next frame reaches its WAL file but completes with EIO: the
    // outcome is unknown to the authority, as for a lost reply.
    original.core.clients[0]
        .client
        .lock()
        .leased_test_fail_write(-libc::EIO);
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
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
    let error = db.write_batch(operation, mutations(&edits, 8)).unwrap_err();
    let uncertain = error
        .uncertain_version
        .expect("unknown write outcome must be unknown, not aborted");
    assert!(
        db.write_batch(fixture.operation(1, 100), mutations(&edits, 8))
            .is_err()
    );
    assert!(db.write_batch(operation, mutations(&edits, 8)).is_err());
    assert!(db.get(KeySpace(0), &key_bytes(0)).is_err());
    assert!(db.published_cut().is_err());
    assert!(db.flush_durable().is_err());
    assert!(db.write_core().flush_index().is_err());
    assert!(db.shutdown().is_err());
    let reopened = ConcurrentRemoteDatabase::new(fixture.open()).unwrap();
    // Replay recovers the unknown outcome's frame before any retry.
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
    // OWNER decision (BOARD 2026-11-20, decision 2): the retry window is empty
    // after a restart, so a retry after the reopen is a new write. It takes a
    // version in the new epoch; a second retry inside this epoch returns that
    // version and allocates nothing.
    let retried = reopened
        .write_batch(operation, mutations(&edits, 8))
        .unwrap();
    assert!(
        retried.epoch > uncertain.epoch,
        "{retried:?} vs {uncertain:?}"
    );
    assert_eq!(
        reopened
            .write_batch(operation, mutations(&edits, 8))
            .unwrap(),
        retried
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
            config.timeout = Duration::from_secs(5);
            leased::configure(config, leased::profile(4));
            // Publication can cache an acknowledged frame before owner adoption.
            // This test must read that frame through the live lane instead.
            config.read_cache_bytes = 0;
            // One read lane, warmed below.
            config.read_lanes = Some(1);
        },
        |_| {},
    )
    .synchronous();
    let original = fixture.open();
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
    // A lane's first read sends OpenLiveExports, an exclusive owner control
    // that waits behind in-flight stream adoptions, so a first read on a
    // fresh lane would wait for the paused close below until its deadline.
    // Warm the only lane with a key the second batch does not write.
    {
        let warm = db.get(KeySpace(0), &key_bytes(0)).unwrap().unwrap();
        assert_eq!(warm.as_ref(), 1u64.to_le_bytes().repeat(512));
    }
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
            // The watermark sampled here covers the acknowledged frame.
            let cut = db.hybrid_table().current();
            let reference = db
                .hybrid_table()
                .get(KeySpace(0), &key_bytes(32), cut)
                .unwrap()
                .unwrap();
            eprintln!(
                "cold_live_read cut={cut:?} reference={reference:?} owner_appended={} owner_synced={} completed_released_durable={:?}",
                log.appended_frontier(),
                log.synced_frontier(),
                writer.lock().leased_test_completion_prefixes(),
            );
            // Checked after the release below: the value is acknowledged but
            // its frame is not adopted (the close that would adopt it is
            // paused before its scan), and the read takes the cold path.
            let address = reference.address;
            let unadopted = address.offset + u64::from(address.len) > log.appended_frontier();
            let before = db.read_cache_stats();
            let value = db.get(KeySpace(0), &key_bytes(32));
            let after = db.read_cache_stats();
            Some((value, unadopted, before, after))
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
    let (value, unadopted, before, after) = read;
    assert!(unadopted, "the read must target an unadopted frame");
    assert_eq!(
        after.hits, before.hits,
        "the read must not be served from the coordinator cache"
    );
    assert!(after.misses > before.misses);
    let value = value.expect("a cold live read must return the synchronously acknowledged value");
    assert_eq!(value.unwrap().as_ref(), 2u64.to_le_bytes().repeat(512));
    shutdown.unwrap();
}

/// A fresh lane must not wait for an adoption close scan, and its first read
/// must not run the owner-wide flush barrier. Its lane-start prefetch has
/// already cached the exports, so the read adds neither an exclusive-slot wait
/// nor a flush; the scheduler test
/// `live_exports_are_served_while_an_adoption_is_parked` pins the export's own
/// class when a read has to send it. The lane is not warmed here.
#[test]
fn write_model_first_read_on_a_fresh_lane_does_not_wait_for_the_adoption_close() {
    use crate::distributed::storage::LivePausePoint;
    // Waits that follow another thread's progress. The lane and write
    // deadline under test is `config.timeout` below, not these.
    const PROGRESS_WAIT: Duration = Duration::from_secs(60);
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("fresh-lane-cold-read").unwrap(),
        1,
        128,
        KeyShape::new_single(4, 8, KeyType::uniform(8)),
        1,
        |config| {
            config.storage.batch_limits.max_operations = 57;
            config.timeout = Duration::from_secs(5);
            leased::configure(config, leased::profile(4));
            // Publication can cache an acknowledged frame before owner adoption.
            // This test must read that frame through the live lane instead.
            config.read_cache_bytes = 0;
            // One read lane, never warmed: its first request is the read below.
            config.read_lanes = Some(1);
        },
        |_| {},
    )
    .synchronous();
    let original = fixture.open();
    let log = fixture.nodes[0].log_for_test(original.epoch());
    let db = ConcurrentRemoteDatabase::new(original).unwrap();
    // The lane opens its exports once when it starts. Wait for that exchange
    // to finish so this test does not read the counters while the prefetch is
    // still running; a failure is named by its counter.
    let monitor = db.read_profile_monitor();
    let prefetch_deadline = Instant::now() + PROGRESS_WAIT;
    let (prefetched, failures) = loop {
        let (mut prefetched, mut failures) = (0, 0);
        monitor.visit(&mut |name, value| {
            if name == "read_profile_live_exports_prefetched" {
                prefetched += value;
            } else if name == "read_profile_live_exports_prefetch_failures" {
                failures += value;
            }
        });
        if prefetched + failures >= 1 {
            break (prefetched, failures);
        }
        assert!(
            Instant::now() < prefetch_deadline,
            "the lane did not finish its export prefetch"
        );
        thread::yield_now();
    };
    assert_eq!(
        (prefetched, failures),
        (1, 0),
        "the lane's start-time export prefetch must succeed"
    );
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
    let (read, barriers, exports_opened) = thread::scope(|scope| {
        let db = &db;
        scope.spawn(|| {
            written
                .send(db.write_batch(operation, mutations(&second, 4096)))
                .unwrap()
        });
        let paused = entered.recv_timeout(PROGRESS_WAIT);
        let acknowledged = result.recv_timeout(PROGRESS_WAIT);
        let observed = if acknowledged.as_ref().is_ok_and(|result| result.is_ok()) {
            let served = fixture.nodes[0].live_exports_served();
            let barriers = fixture.nodes[0].flush_barriers_sent();
            // The frame is acknowledged but not adopted: the close that
            // would adopt it is paused before its scan, so the read is cold.
            let value = db.get(KeySpace(0), &key_bytes(32));
            let barriers = fixture.nodes[0].flush_barriers_sent() - barriers;
            let opened = fixture.nodes[0].live_exports_served() - served;
            Some((value, barriers, opened))
        } else {
            None
        };
        // Always unblock the adoption worker before assertions or shutdown.
        release.send(()).unwrap();
        paused.unwrap();
        acknowledged.unwrap().unwrap();
        observed.unwrap()
    });
    let shutdown = db.shutdown();
    // The value first: without the fix the first read waits behind the parked
    // adoption close and fails by its deadline, and that is the failure to
    // report; the owner has not counted its export request by then.
    let value = read.expect("a cold live read must return the acknowledged value");
    assert_eq!(
        value
            .expect("the first read waited for the parked adoption close")
            .as_ref(),
        2u64.to_le_bytes().repeat(512)
    );
    assert_eq!(
        barriers, 0,
        "the first read on a fresh lane ran an owner-wide flush barrier"
    );
    // The prefetch may serve it, or the read may send its own exchange; both
    // are one owner exchange. Never more.
    assert!(
        exports_opened <= 1,
        "the first read opened {exports_opened} export exchanges"
    );
    shutdown.unwrap();
}
