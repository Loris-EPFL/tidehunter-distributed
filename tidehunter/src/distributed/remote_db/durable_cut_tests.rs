//! Durable-prefix recovery against real loopback owners. A coordinator crash is
//! a shutdown without its checkpoint: owners keep every appended batch, and the
//! reopen fence syncs them, so only the cut decides what recovery keeps.

use super::durable_cut::RECORD;
use super::pipeline_tests::Fixture;
use super::*;
use crate::key_shape::KeyType;

/// No nudge can fire during a test: the cut moves only by flush or report.
const QUIET_LAG_MS: u64 = 3_600_000;
const WAIT: Duration = Duration::from_secs(30);

fn fixture(owners: usize, durable_cut: DurableCutConfig) -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("durable-cut").unwrap(),
        owners,
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |config| config.durable_cut = durable_cut,
        |_| {},
    )
}

/// `fixture` with 64 KiB fragments in 1 MiB WAL files. A leased frame is at
/// least one 4 KiB block, so with the default 4 KiB fragments every write
/// fills its extent and the extent close requests a sync. Here a frame stays
/// far below a fragment: no close, no new WAL file, so after the first lease
/// only a nudge or an explicit flush syncs the owner.
fn wide_fixture(owners: usize, durable_cut: DurableCutConfig) -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("durable-cut").unwrap(),
        owners,
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |config| {
            config.durable_cut = durable_cut;
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 1024 * 1024;
        },
        |_| {},
    )
}

fn quiet() -> DurableCutConfig {
    DurableCutConfig {
        lag_ms: QUIET_LAG_MS,
        ..DurableCutConfig::default()
    }
}

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 16].into(),
    }
}

fn value(db: &mut RemoteDatabase, key: u32) -> Option<u8> {
    db.get(KeySpace(0), &key.to_be_bytes())
        .unwrap()
        .map(|value| value[0])
}

/// Shutdown without the checkpoint: the tail stays in the owner WALs.
fn crash(db: RemoteDatabase) {
    db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
    db.shutdown().unwrap();
}

fn record_path(fixture: &Fixture) -> PathBuf {
    fixture.temp.path().join("authority").join(RECORD)
}

/// v0 (owner 0) and v1 (owner 1) are flushed; v2 (owner 0) is acknowledged
/// but above the persisted cut when the coordinator crashes.
fn write_flushed_prefix_and_unsynced_tail(fixture: &Fixture) {
    let db = fixture.open();
    assert!(db.durable_cut_stats().enabled);
    db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    db.write_batch(fixture.operation(1, 2), vec![put(2, 2)])
        .unwrap();
    db.core.flush_durable().unwrap();
    let stats = db.durable_cut_stats();
    assert_eq!((stats.persisted_next, stats.certifications), (2, 1));
    let tail = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    assert_eq!(
        tail,
        BatchVersion {
            epoch: 1,
            sequence: 2
        }
    );
    assert_eq!(db.durable_cut_stats().persisted_next, 2);
    crash(db);
}

#[test]
fn extend_keeps_the_surviving_run_after_a_coordinator_crash() {
    let fixture = fixture(2, quiet());
    write_flushed_prefix_and_unsynced_tail(&fixture);
    let mut db = fixture.open();
    let stats = db.recovery_stats();
    assert_eq!(
        (stats.cut_discarded_batches, stats.cut_extended_batches),
        (0, 1)
    );
    assert_eq!(
        stats.recovered_cut,
        Some(RecoveredCut { epoch: 1, next: 3 })
    );
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), Some(2), Some(3)]
    );
    // OWNER-ACCEPTED WEAKENING (remote_db.rs `recent: ReceiptWindow::new()`):
    // the retry window is empty after every open, so a retry of an
    // acknowledged operation is applied again as a new batch at the new epoch
    // (the old design answered it with the original version {1, 2}). The put
    // is idempotent: key 3 still reads 3, and keys 1 and 2 are unchanged.
    let retried = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    assert_eq!(
        retried,
        BatchVersion {
            epoch: db.epoch(),
            sequence: 0
        },
        "the retry is re-applied at the new epoch"
    );
    assert_eq!(db.epoch(), 2);
    // The new epoch's window must still deduplicate the operation it just
    // published, even though the prior epoch's window was not recovered.
    let repeated = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .expect("retry in the new epoch");
    assert_eq!(repeated, retried, "the new epoch retains its own receipt");
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), Some(2), Some(3)]
    );
}

/// v0 (owner 0) is flushed, v1 (owner 1) is aborted before its write, and v2
/// (owner 0) is acknowledged; then the coordinator crashes. Returns the fixture.
fn write_around_an_aborted_version(flushed: bool) -> Fixture {
    let fixture = fixture(2, quiet());
    let db = fixture.open();
    db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    db.core.flush_durable().unwrap();
    let failed = fixture.operation(1, 2);
    db.core.test_hooks.lock().fail_prepared_operation = Some(failed);
    assert!(db.write_batch(failed, vec![put(2, 2)]).is_err());
    let after = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    assert_eq!(after.sequence, 2);
    if flushed {
        db.core.flush_durable().unwrap();
        assert_eq!(db.durable_cut_stats().persisted_next, 3);
    }
    crash(db);
    fixture
}

/// Coordinator-only crash: the owner processes keep running (same incarnation)
/// and recover without a hole, so the aborted version was never written. Extend
/// skips it and keeps the acknowledged write after it, flushed or not.
#[test]
fn continuous_owners_keep_acknowledged_writes_above_an_aborted_version() {
    for flushed in [false, true] {
        let fixture = write_around_an_aborted_version(flushed);
        let mut db = fixture.open();
        let stats = db.recovery_stats();
        assert_eq!(stats.cut_continuity_bound, Some(u64::MAX));
        assert_eq!(
            (stats.cut_discarded_batches, stats.cut_skipped_versions),
            (0, u64::from(!flushed)),
            "flushed={flushed}"
        );
        assert_eq!(
            stats.recovered_cut,
            Some(RecoveredCut { epoch: 1, next: 3 })
        );
        assert_eq!(
            [1, 2, 3].map(|key| value(&mut db, key)),
            [Some(1), None, Some(3)],
            "flushed={flushed}"
        );
    }
}

/// An owner whose incarnation changed bounds the skip at its recorded synced
/// end: the aborted sequence 1 is skipped only when that owner's record covers
/// it, and a flush still keeps the acknowledged write either way.
#[test]
fn restarted_owner_bounds_the_skip_at_its_recorded_sync() {
    for (flushed, recorded, kept) in [(false, 1, false), (false, 2, true), (true, 0, true)] {
        let fixture = write_around_an_aborted_version(flushed);
        let authority = fixture.temp.path().join("authority");
        super::durable_cut::rewrite_marks_for_test(&authority, |marks| {
            marks[1].incarnation = [0x5a; 16];
            marks[1].synced_next = recorded;
        });
        let mut db = fixture.open();
        let stats = db.recovery_stats();
        assert_eq!(
            stats.cut_continuity_bound,
            Some(recorded),
            "flushed={flushed}"
        );
        let next = if kept { 3 } else { 1 };
        assert_eq!(stats.recovered_cut, Some(RecoveredCut { epoch: 1, next }));
        assert_eq!(
            [1, 2, 3].map(|key| value(&mut db, key)),
            [Some(1), None, kept.then_some(3)],
            "flushed={flushed} recorded={recorded}"
        );
    }
}

/// Same boot, new owner process. Owner 1 acknowledged v1 above the persisted
/// cut and lost it: a failed fsync drops dirty pages without a reboot, and the
/// owner stops and restarts. Owner 0 keeps v2. The restarted owner recovers
/// without a hole, but its incarnation changed, so Extend stops at the lost v1
/// and discards v2, and a later open keeps that truncation.
#[test]
fn restarted_owner_process_stops_extend_at_its_lost_version() {
    // Wide fragments: v1 must stay acknowledged but unsynced (no extent close).
    let mut fixture = wide_fixture(2, quiet());
    let db = fixture.open();
    let epoch = db.epoch();
    db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    db.core.flush_durable().unwrap();
    let lost = db
        .write_batch(fixture.operation(1, 2), vec![put(2, 2)])
        .unwrap();
    let kept = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    assert_eq!((lost.sequence, kept.sequence), (1, 2));
    assert_eq!(db.durable_cut_stats().persisted_next, 1);
    // Owner 1's syncer fsync fails: the failure is sticky, so its log never
    // seals and the owner must restart. As the owner's own flush and nudge
    // paths do, adopt the leased frame first, so the frontier covers v1.
    let log = fixture.nodes[1].log_for_test(epoch);
    log.self_adopt_live_extent().unwrap();
    assert!(log.frontier().unwrap().synced() < log.frontier().unwrap().appended());
    log.fail_next_persist();
    let frontier = log.frontier().unwrap().clone();
    drop(log);
    assert!(
        crate::distributed::durability::frontier::flush_all(&[frontier], None)
            .into_synced()
            .is_err()
    );
    // The coordinator fails too.
    super::tests::fail_stop_without_checkpoint(db);
    fixture.stop_owners();
    // v1 is owner 1's only frame of the epoch: the failed fsync dropped its
    // dirty pages. The leased WAL file is preallocated and bound to the live
    // target by inode, size and allocation, so the dropped pages read back as
    // the zeros of its never-synced blocks: zero the contents, keep the file.
    let wal = fixture
        .owner_database_root(1)
        .join(format!("epoch_{epoch:016x}/wal_0000000000000000"));
    let file = std::fs::OpenOptions::new().write(true).open(wal).unwrap();
    let len = file.metadata().unwrap().len();
    std::os::unix::fs::FileExt::write_all_at(&file, &vec![0; len as usize], 0).unwrap();
    drop(file);
    fixture.restart_owners().unwrap();
    let mut db = fixture.open();
    let stats = db.recovery_stats();
    // Both owners restarted; owner 1's mark has nothing synced.
    assert_eq!(stats.cut_continuity_bound, Some(0));
    assert_eq!(stats.recovered_cut, Some(RecoveredCut { epoch, next: 1 }));
    assert_eq!(
        (stats.cut_discarded_batches, stats.cut_skipped_versions),
        (1, 0)
    );
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), None, None]
    );
    crash(db);
    let mut db = fixture.open();
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), None, None]
    );
}

/// Every step of open is crash safe: an interruption after the record and
/// before activation, a crash of the next epoch, and a later open all keep
/// the truncation, so a discarded batch never returns.
#[test]
fn truncation_survives_every_later_crash() {
    // Owner 1 restarted with sequence 1 aborted and unsynced: recovery stops
    // at 1 and discards the acknowledged v2.
    let fixture = write_around_an_aborted_version(false);
    let root = fixture.temp.path().join("authority");
    super::durable_cut::rewrite_marks_for_test(&root, |marks| {
        marks[1].incarnation = [0x5a; 16];
        marks[1].synced_next = 1;
    });
    let error = RemoteDatabase::open_inner_local_fixture(
        &root,
        &fixture.shape,
        fixture.config.clone(),
        Some(OpenInterruption::AfterCutRecord),
    )
    .err()
    .unwrap();
    assert!(error.message().contains("after cut record"), "{error}");
    let manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert_eq!(manifest.generations[1].state, GenerationState::Reserved);
    // Every later open replays the older generation again: the hybrid open
    // writes no checkpoint after replay (native opens the same way, from the
    // last control-region snapshot), so the persisted cut record is the only
    // thing that keeps the discarded batch out. Re-replay is allowed and
    // `cut_discarded_batches` may repeat (bounded by the one discarded batch);
    // resurrection is not: the discarded v2 (key 3) and the aborted version
    // (key 2) read as absent on every reopen, and the kept key reads its bytes.
    for open_number in 1..=3u64 {
        let mut db = fixture.open();
        assert!(db.durable_cut_stats().enabled);
        let reopened = [1u32, 2, 3].map(|key| {
            db.get(KeySpace(0), &key.to_be_bytes())
                .expect("read recovered key")
                .map(|bytes| bytes.to_vec())
        });
        let stats = db.recovery_stats();
        if open_number == 1 {
            // The interrupted open reserved epoch 2: this open is epoch 3 and
            // is the one that actually discards the acknowledged v2.
            assert_eq!(db.epoch(), 3);
            assert_eq!(stats.cut_discarded_batches, 1);
        }
        eprintln!(
            "truncation open {open_number}: epoch {} values {reopened:?} discarded {} extended {} \
             recovered_cut {:?} stored_cut {:?}",
            db.epoch(),
            stats.cut_discarded_batches,
            stats.cut_extended_batches,
            stats.recovered_cut,
            db.core.table.stored_cut()
        );
        assert_eq!(
            reopened,
            [Some(vec![1; 16]), None, None],
            "open {open_number}: the discarded tail was re-applied"
        );
        assert!(
            stats.cut_discarded_batches <= 1,
            "open {open_number}: {stats:?}"
        );
        if open_number < 3 {
            crash(db);
        } else {
            db.shutdown().unwrap();
        }
    }
}

#[test]
fn synchronous_recovery_is_unchanged() {
    let fixture = fixture(2, quiet()).synchronous();
    let db = fixture.open();
    assert_eq!(db.durable_cut_stats(), DurableCutStats::default());
    db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    db.write_batch(fixture.operation(1, 2), vec![put(2, 2)])
        .unwrap();
    db.core.flush_durable().unwrap();
    db.write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    crash(db);
    let mut db = fixture.open();
    let stats = db.recovery_stats();
    assert_eq!(
        (
            stats.cut_discarded_batches,
            stats.cut_extended_batches,
            stats.recovered_cut
        ),
        (0, 0, None)
    );
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), Some(2), Some(3)]
    );
    assert!(!record_path(&fixture).exists());
}

/// Owner 1 never receives a batch and stays idle; owner 0 lags until the actor
/// nudges it. The nudge is the only sync trigger here (Native, frames far
/// below a fragment), so the paused sync proves it, and the cut moves past the
/// idle owner once the sync completes.
///
/// Leased route: the first write of the epoch takes owner 0's first lease,
/// whose WAL file and directory-entry durability step is the same owner
/// persist hook. A warm-up write therefore takes that lease and is itself
/// carried by a nudge before the hook is armed; the measured write then uses
/// the open extent, so the hook can only catch the nudge's sync.
#[test]
fn lagging_owner_is_nudged_and_idle_owner_does_not_pin_the_cut() {
    let fixture = wide_fixture(
        2,
        DurableCutConfig {
            lag_ms: 20,
            ..DurableCutConfig::default()
        },
    );
    let db = fixture.open();
    let cut = db.core.durable_cut.get().unwrap();
    let persisted = cut.persisted_events();
    db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    assert_eq!(persisted.recv_timeout(WAIT).unwrap(), 1);
    let warm = db.durable_cut_stats();
    assert_eq!(warm.persisted_next, 1);
    assert!(warm.nudges >= 1);
    let (entered, resume) = fixture.nodes[0]
        .log_for_test(db.epoch())
        .pause_next_persist();
    db.write_batch(fixture.operation(0, 2), vec![put(2, 2)])
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    assert!(persisted.try_recv().is_err(), "cut moved before the sync");
    assert!(db.durable_cut_stats().nudges > warm.nudges);
    resume.send(()).unwrap();
    assert_eq!(persisted.recv_timeout(WAIT).unwrap(), 2);
    let stats = db.durable_cut_stats();
    assert_eq!(stats.persisted_next, 2);
    assert!(stats.reports > warm.reports);
    db.shutdown().unwrap();
}

/// Native opt-in sharding is preserved by the hybrid. A small L0 entry limit
/// forces promotion without filling the fragment. Both modes reopen through
/// real owners and return the exact values, including after another promotion.
#[test]
fn native_sharding_option_controls_hybrid_promotion_and_reopen() {
    for (threshold, expect_shards) in [(Some(usize::MAX), false), (Some(2048), true)] {
        let fixture = Fixture::new_with_root(
            tempdir::TempDir::new("sharding-option").expect("fixture root"),
            1,
            512,
            KeyShape::new_single(4, 1, KeyType::uniform(1)),
            2,
            |_| {},
            |_| {},
        );
        let native = crate::config::Config {
            frag_size: 65536,
            wal_file_size: 65536,
            l0_max_entries: Some(8),
            snapshot_unload_threshold: 0,
            index_auto_shard_threshold: threshold,
            ..crate::config::Config::default()
        };
        let config = fixture.config.clone().with_native_index(&native);
        let open = || {
            crate::distributed::sui::Db::open_remote_local_fixture(
                &fixture.authority(),
                fixture.shape.clone(),
                config.clone(),
            )
            .expect("open Sui adapter")
        };
        let db = open();
        let ks = db.ks("root");
        let shard_count = |db: &crate::distributed::sui::Db| {
            let count = std::sync::atomic::AtomicUsize::new(0);
            db.remote_for_test()
                .write_core()
                .table
                .large_table()
                .each_entry(|entry| {
                    let levels = entry.levels();
                    if levels.is_sharded() {
                        count.store(levels.shards().len(), std::sync::atomic::Ordering::Relaxed);
                    } else if !expect_shards {
                        assert!(
                            levels.l1().is_some(),
                            "Some(usize::MAX) must retain one promoted L1 blob"
                        );
                    }
                });
            count.load(std::sync::atomic::Ordering::Relaxed)
        };
        let mut first_count = 0;
        for key in 1u32..=200 {
            let mut batch = db.write_batch();
            batch.write(ks, key.to_be_bytes().to_vec(), vec![(key % 251) as u8; 16]);
            batch.commit().expect("write");
            if key % 10 == 0 {
                db.remote_for_test()
                    .write_core()
                    .flush_durable()
                    .expect("certify writes");
                db.remote_for_test()
                    .write_core()
                    .flush_index()
                    .expect("flush index");
            }
            if key == 100 {
                first_count = shard_count(&db);
                if expect_shards {
                    assert!(first_count > 1, "first promotion must produce shards");
                } else {
                    assert_eq!(first_count, 0, "None keeps a single L1");
                }
            }
        }
        let final_count = shard_count(&db);
        if expect_shards {
            assert!(final_count >= first_count, "re-promotion lost shards");
        } else {
            assert_eq!(final_count, 0, "None keeps a single L1");
        }
        for key in 1u32..=200 {
            let actual = db.get(ks, &key.to_be_bytes()).expect("read before reopen");
            assert_eq!(
                actual.as_deref(),
                Some(&[(key % 251) as u8; 16][..]),
                "key {key}"
            );
        }
        db.wait_for_background_threads_to_finish()
            .expect("shutdown");
        let reopened = open();
        let ks = reopened.ks("root");
        assert_eq!(shard_count(&reopened), final_count, "reopen retains levels");
        for key in 1u32..=200 {
            let actual = reopened.get(ks, &key.to_be_bytes()).expect("read");
            assert_eq!(
                actual.as_deref(),
                Some(&[(key % 251) as u8; 16][..]),
                "key {key}"
            );
        }
        reopened
            .wait_for_background_threads_to_finish()
            .expect("shutdown");
    }
}
