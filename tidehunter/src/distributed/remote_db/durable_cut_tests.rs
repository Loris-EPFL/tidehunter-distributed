//! Durable-prefix recovery against real loopback owners. A coordinator crash is
//! a shutdown without its checkpoint: owners keep every appended batch, and the
//! reopen fence syncs them, so only the cut decides what recovery keeps.

use super::durable_cut::RECORD;
use super::pipeline_tests::Fixture;
use super::tests::recovery_metrics;
use super::*;
use crate::distributed::durability::{CutRecovery, NativeRecovery};
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
        |config| crate::distributed::tcp::StorageNode::open(config, recovery_metrics()).unwrap(),
    )
}

fn quiet(cut: CutRecovery) -> DurableCutConfig {
    DurableCutConfig {
        cut,
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
fn strict_discards_the_tail_above_the_persisted_cut() {
    let fixture = fixture(2, quiet(CutRecovery::Strict));
    write_flushed_prefix_and_unsynced_tail(&fixture);
    let mut db = fixture.open();
    let stats = db.recovery_stats();
    assert_eq!(
        (stats.cut_discarded_batches, stats.cut_extended_batches),
        (1, 0)
    );
    assert_eq!(
        stats.recovered_cut,
        Some(RecoveredCut { epoch: 1, next: 2 })
    );
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), Some(2), None]
    );
    // The discarded version is not a retry identity: a retry writes again.
    let retried = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    assert_eq!(retried.epoch, 2);
    assert_eq!(value(&mut db, 3), Some(3));
}

#[test]
fn extend_keeps_the_surviving_run_after_a_coordinator_crash() {
    let fixture = fixture(2, quiet(CutRecovery::Extend));
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
    // The acknowledged identity is still known after recovery.
    let retried = db
        .write_batch(fixture.operation(0, 3), vec![put(3, 3)])
        .unwrap();
    assert_eq!(
        retried,
        BatchVersion {
            epoch: 1,
            sequence: 2
        }
    );
}

/// v0 (owner 0) is flushed, v1 (owner 1) is aborted before its write, and v2
/// (owner 0) is acknowledged; then the coordinator crashes. Returns the fixture.
fn write_around_an_aborted_version(flushed: bool) -> Fixture {
    let fixture = fixture(2, quiet(CutRecovery::Extend));
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

/// Coordinator-only crash: the owners keep their boot and recover without a
/// hole, so the aborted version was never written. Extend skips it and keeps
/// the acknowledged write after it, flushed or not.
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

/// An owner whose boot changed bounds the skip at its recorded synced end: the
/// aborted sequence 1 is skipped only when that owner's record covers it, and
/// a flush still keeps the acknowledged write either way.
#[test]
fn rebooted_owner_bounds_the_skip_at_its_recorded_sync() {
    for (flushed, recorded, kept) in [(false, 1, false), (false, 2, true), (true, 0, true)] {
        let fixture = write_around_an_aborted_version(flushed);
        let authority = fixture.temp.path().join("authority");
        super::durable_cut::rewrite_marks_for_test(&authority, |marks| {
            marks[1].boot_id = [0x5a; 16];
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

/// Every step of open is crash safe: an interruption after the record and
/// before activation, a crash of the next epoch, and a later per-owner open
/// all keep the truncation, so a discarded batch never returns.
#[test]
fn truncation_survives_every_later_crash_and_mode() {
    let fixture = fixture(2, quiet(CutRecovery::Strict));
    write_flushed_prefix_and_unsynced_tail(&fixture);
    let root = fixture.temp.path().join("authority");
    let error = RemoteDatabase::open_inner(
        &root,
        &fixture.shape,
        fixture.config.clone(),
        None,
        Some(OpenInterruption::AfterCutRecord),
    )
    .err()
    .unwrap();
    assert!(error.message().contains("after cut record"), "{error}");
    let manifest: AuthorityManifest = read_metadata(&root.join(MANIFEST)).unwrap();
    assert!(!manifest.epochs[1].active);
    let mut db = fixture.open();
    assert_eq!(db.epoch(), 3);
    assert_eq!(db.recovery_stats().cut_discarded_batches, 1);
    assert_eq!(value(&mut db, 3), None);
    crash(db);
    let mut per_owner = fixture.config.clone();
    per_owner.durable_cut.recovery = NativeRecovery::PerOwner;
    let mut db =
        RemoteDatabase::open(&root, &fixture.shape, per_owner, recovery_metrics()).unwrap();
    assert!(!db.durable_cut_stats().enabled);
    // Epoch 3 checkpointed its recovered tail, so epoch 1 is not replayed again.
    assert_eq!(db.recovery_stats().cut_discarded_batches, 0);
    assert_eq!(
        [1, 2, 3].map(|key| value(&mut db, key)),
        [Some(1), Some(2), None]
    );
}

#[test]
fn per_owner_and_synchronous_recovery_are_unchanged() {
    let per_owner = fixture(
        2,
        DurableCutConfig {
            recovery: NativeRecovery::PerOwner,
            ..quiet(CutRecovery::Strict)
        },
    );
    let synchronous = fixture(2, quiet(CutRecovery::Strict)).synchronous();
    for fixture in [&per_owner, &synchronous] {
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
        assert!(!record_path(fixture).exists());
    }
}

/// Owner 1 never receives a batch and stays idle; owner 0 lags until the actor
/// nudges it. The nudge is the only sync trigger here (Native, far below a
/// fragment), so the paused sync proves it, and the cut moves past the idle
/// owner once the sync completes.
#[test]
fn lagging_owner_is_nudged_and_idle_owner_does_not_pin_the_cut() {
    let fixture = fixture(
        2,
        DurableCutConfig {
            lag_ms: 20,
            ..DurableCutConfig::default()
        },
    );
    let db = fixture.open();
    let cut = db.core.durable_cut.as_ref().unwrap();
    let persisted = cut.persisted_events();
    let (entered, resume) = fixture.nodes[0]
        .log_for_test(db.epoch())
        .pause_next_persist();
    db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
        .unwrap();
    entered.recv_timeout(WAIT).unwrap();
    assert!(persisted.try_recv().is_err(), "cut moved before the sync");
    assert!(db.durable_cut_stats().nudges >= 1);
    resume.send(()).unwrap();
    assert_eq!(persisted.recv_timeout(WAIT).unwrap(), 1);
    let stats = db.durable_cut_stats();
    assert_eq!(stats.persisted_next, 1);
    assert!(stats.reports >= 1);
    db.shutdown().unwrap();
}
