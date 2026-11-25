//! P1 at pass level: a relocation pass re-points the index, stores a region and
//! retires the old file only after its copies are durable-proven.
//!
//! The pass runs through the Sui adapter on the leased ordinary-file fixture,
//! Native tier, with a lag that never nudges by itself: nothing but the copy
//! wait ever asks the owner to sync after the writes were certified. Each test
//! names its negative control, a mutation run of production code recorded in
//! the handoff.

use super::pipeline_tests::Fixture;
use super::*;
use crate::Decision;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::sui::Db;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
use std::sync::atomic::AtomicBool;

// This module carries its own copy of the fixture surface `test_support`
// offers: that module is compiled only with `feature = "test_methods"`, and
// these tests must compile, list and run in every gate feature set (no added
// test file is allowed to be gated off). The geometry, the lag and the limits
// are the same as `test_support`'s, so a failure here means the same thing;
// keep the two in step.

pub(super) const KIB: u64 = 1024;
const FILE: u64 = 1024 * KIB;
/// Two fragments per 1 MiB file: batch and group limits stay below a fragment.
const FRAGMENT: u64 = 512 * KIB;
pub(super) const VALUE: usize = 8 * KIB as usize;
/// About 2.4 MiB of 8 KiB values: more than two whole 1 MiB files, so a pass
/// has a complete file to reclaim.
const WRITES: u64 = 300;
/// Quiet but reactive: the owner poller serves a nudge on its long poll, whose
/// wait is a quarter of the lag capped at 5 s, so the copy wait's own request
/// is served well inside [`COPY_WAIT`]. The cut's lag timer ALSO nudges once a
/// lag passes with an unsynced receipt; this bound is the maximum accepted lag,
/// so no test depends on a handshake finishing before that timer. Only
/// `a_native_tier_pass_syncs_its_copies_before_it_re_points` asserts that the
/// copy wait asked an owner to sync; the other tests assert the request delta and
/// the parked persist of the copies.
const QUIET_LAG_MS: u64 = 3_600_000;
/// The bound a test waits for a hook, a channel or a pass. Every wait follows
/// observed progress (a parked persist, a counter or a joined pass), so this is
/// a hang guard, not a latency premise (TST-9).
pub(super) const WAIT: Duration = Duration::from_secs(120);

/// The pass's own wait for its copies to become durable in the tests that
/// inject a failure of that sync. It only ends the wait (the cut never
/// advances after the injected failure), so it must outlast the hook handshake
/// of the test and the poller's nudge pickup. Every such test installs it as
/// `copy_durable_timeout`, so the wait never depends on the configured cut lag.
pub(super) const COPY_WAIT: Duration = Duration::from_secs(30);

/// A cut above every version of every epoch: the index holds only published
/// batches, so a lookup at it answers "what is indexed now".
pub(super) fn newest_cut() -> BatchVersion {
    BatchVersion {
        epoch: u64::MAX,
        sequence: u64::MAX,
    }
}

/// One keyspace with no relocation filter: the pass copies every live entry
/// below its stream's target and re-points it. A filtered keyspace copies
/// nothing at the parity tip (D24), so a filter would empty the copy path these
/// tests exercise.
pub(super) fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config("a", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    // A filtered keyspace whose first record stops the scan. Native sets its
    // terminal at a filter `StopRelocation` or at the first record at or above
    // its upper limit; the unfiltered data set gets a target only from one of
    // those, because a pass with neither does nothing.
    builder.add_key_space_config(
        "s",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|_: &[u8], _: &[u8]| Decision::StopRelocation),
    );
    builder.build()
}

/// The Stop record of [`shape`]'s filtered keyspace, key inside it.
const STOP_MARKER: u64 = 1;

/// Appends the scan's Stop: one put in the filtered keyspace.
fn stop_marker(db: &Arc<Db>) {
    let mut batch = db.write_batch();
    batch.write(db.ks("s"), key(STOP_MARKER), fill_value(STOP_MARKER));
    batch.commit().expect("write the stop marker");
}

fn fixture() -> Fixture {
    fixture_with(shape())
}

fn fixture_with(shape: KeyShape) -> Fixture {
    fixture_with_batch_bytes(shape, 64 * KIB as usize)
}

/// `batch_bytes` bounds one batch: a pass writes its copied entries in as
/// many batches (and so as many append groups) as it needs.
pub(super) fn fixture_with_batch_bytes(shape: KeyShape, batch_bytes: usize) -> Fixture {
    fixture_with_limits(shape, batch_bytes, 256 * KIB as usize)
}

/// [`fixture_with_batch_bytes`] with the append-group byte limit chosen too:
/// a copy set below it reaches the owner in one append group.
pub(super) fn fixture_with_limits(
    shape: KeyShape,
    batch_bytes: usize,
    group_bytes: usize,
) -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("copy-durable-pass").unwrap(),
        1,
        8192,
        shape,
        32,
        |config| {
            config.durable_cut = DurableCutConfig {
                lag_ms: QUIET_LAG_MS,
                ..DurableCutConfig::default()
            };
            config.storage.batch_limits = BatchLimits {
                max_operations: 32,
                max_encoded_bytes: batch_bytes,
            };
            config.storage.group_limits = GroupLimits {
                max_batches: 32,
                max_encoded_bytes: group_bytes,
            };
            config.storage.fragment_bytes = FRAGMENT;
            config.storage.wal_file_bytes = FILE;
            config.timeout = Duration::from_secs(60);
            // One pass chunk covers a whole file's copy set, so the tests can
            // choose the batch and group limits that decide the append shape.
            config.index.relocation_batch_max_bytes = Some(4 * 1024 * KIB as usize);
        },
        |_| {},
    )
}

pub(super) fn open(fixture: &Fixture) -> Arc<Db> {
    Db::open_remote_local_fixture(
        &fixture.authority(),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open")
}

pub(super) fn key(sequence: u64) -> Vec<u8> {
    sequence.to_be_bytes().to_vec()
}

/// Keys whose only write lies in the first WAL file: the live set a pass
/// copies, because the file is below its target.
pub(super) const SURVIVORS: u64 = 12;
/// Padding keys: they fill the first file and are then overwritten above it, so
/// their live versions lie above the one-file target and their first-file bytes
/// are dead. The copy set stays a few tens of KiB (one batch and one append
/// group at the fixture's limits) without a relocation filter.
const PADDING: u64 = 120;
/// First padding key, so a padding write never touches a survivor's entry.
const PAD_BASE: u64 = 1000;

/// Value bytes of `sequence`: the fixture's one value shape for both sets.
fn fill_value(sequence: u64) -> Vec<u8> {
    vec![(sequence % 251) as u8; VALUE]
}

/// Writes the data set and certifies it: every later unsynced byte is a copy.
///
/// The layout gives the pass one whole file to reclaim and a small copy set:
/// the survivors' only writes fill the start of file 0, dead padding fills the
/// rest of it, and the padding's live versions land above the one-file target.
pub(super) fn fill(db: &Arc<Db>) -> KeySpace {
    let ks = db.ks("a");
    let write = |sequence: u64| {
        let mut batch = db.write_batch();
        batch.write(ks, key(sequence), fill_value(sequence));
        batch.commit().expect("write");
    };
    for sequence in 0..SURVIVORS {
        write(sequence);
    }
    for sequence in PAD_BASE..PAD_BASE + PADDING {
        write(sequence);
    }
    for sequence in PAD_BASE..PAD_BASE + PADDING {
        write(sequence);
    }
    // The Stop lies above the data set, so the pass has a whole file before its
    // target and every certified byte is below it.
    stop_marker(db);
    db.remote_for_test()
        .write_core()
        .flush_durable()
        .expect("certify the data set");
    ks
}

/// One production relocation pass. The report carries the persistence evidence
/// the assertions read (`deferred`, `copied_survivors`, `stored_target`); the
/// same fields a pass receipt projects.
fn pass(db: &Db) -> RelocationReport {
    let cancel = AtomicBool::new(false);
    db.remote_for_test()
        .relocation_pass(false, &cancel)
        .expect("relocation pass")
}

fn offsets(db: &Db, ks: KeySpace) -> Vec<(u64, u64)> {
    (0..SURVIVORS)
        .map(|sequence| {
            let address = db
                .remote_for_test()
                .hybrid_table()
                .get(ks, &key(sequence), newest_cut())
                .expect("lookup")
                .expect("indexed")
                .address;
            (address.segment_generation, address.offset)
        })
        .collect()
}

/// Idle Native tier: with a quiet lag the pass is the only thing that syncs the
/// copies. It must ask for the sync, wait for the cut to cover every copy, and
/// only then re-point, so after it returns the owner holds the copies durably.
///
/// Negative control (mutation run `m01`): delete the `await_copies_durable`
/// call in `flush_relocated` and `targeted_syncs` stays 0 although survivors
/// were re-pointed.
#[test]
fn a_native_tier_pass_syncs_its_copies_before_it_re_points() {
    let fixture = fixture();
    let db = open(&fixture);
    let ks = fill(&db);
    let before = offsets(&db, ks);
    let core = db.remote_for_test().write_core();
    assert_eq!(core.durable_cut_stats().targeted_syncs, 0);
    let receipt = pass(&db);
    assert!(!receipt.deferred, "{receipt:?}");
    assert!(receipt.copied_survivors > 0, "{receipt:?}");
    assert_ne!(offsets(&db, ks), before, "survivors were re-pointed");
    let stats = core.durable_cut_stats();
    assert!(
        stats.targeted_syncs >= 1,
        "the quiet lag never nudges: only the copy wait asked an owner to sync"
    );
    assert!(
        stats.persisted_next > SURVIVORS,
        "the cut covers the copies the index now names: {stats:?}"
    );
    assert_eq!(db.remote_for_test().relocation_copy_deferrals(), 0);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A nudge wakes an idle poller at once. [`fill`] certifies the data set, so the
/// one owner's poller has nothing outstanding and parks on the fixture's
/// hour-long lag. A write is acknowledged at the owner's page cache without
/// nudging the cut, so the poller stays parked; a copy wait for that write must
/// reach the cut through the wake, never through a quarter lag (15 minutes at
/// this fixture's lag). Negative control: without the poller wake the wait ends
/// at `COPY_WAIT` with `copies_not_durable`.
#[test]
fn a_nudge_wakes_an_idle_poller_out_of_its_lag_park() {
    let fixture = fixture();
    let db = open(&fixture);
    let ks = fill(&db);
    let core = db.remote_for_test().write_core();
    // The certification left the owner idle: wait for the poller's park.
    let deadline = Instant::now() + WAIT;
    let parked = loop {
        let parks = core.durable_cut_park_samples(0);
        if parks > 0 {
            break parks;
        }
        assert!(
            Instant::now() < deadline,
            "the poller never parked after the certification",
        );
        std::thread::yield_now();
    };
    let mut batch = db.write_batch();
    batch.write(ks, key(9_000), fill_value(9_000));
    batch.commit().expect("write");
    assert_eq!(
        core.durable_cut_park_samples(0),
        parked,
        "premise: an acknowledged write does not wake the parked poller",
    );

    let published = db.remote_for_test().published_cut().expect("published cut");
    let last = BatchVersion {
        epoch: published.epoch,
        sequence: published
            .sequence
            .checked_sub(1)
            .expect("the fixture published the write"),
    };
    core.test_hooks.lock().copy_durable_timeout = Some(COPY_WAIT);
    core.await_copies_durable(last)
        .expect("the nudge wakes the parked poller within the copy wait");
    assert!(
        core.durable_cut_stats().persisted_next > last.sequence,
        "the cut covers the batch: {:?}",
        core.durable_cut_stats(),
    );
    assert!(
        core.durable_cut_park_samples(0) > parked,
        "the park ended before the wait returned",
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A parked owner sync: the wait gives up with the typed cause, the pass
/// applies, stores and retires nothing, and the deferral is counted. After the
/// owner syncs, the next pass completes.
///
/// The live root is caught up first, so a root-only barrier cannot take the hold
/// before the copies are appended. At the park the test asserts that the copies
/// advanced the appended end and are still unsynced, so the park is on a persist
/// of them.
#[test]
fn a_pass_whose_copies_cannot_be_synced_applies_nothing_and_the_next_pass_completes() {
    let fixture = fixture_with_batch_bytes(shape(), 128 * KIB as usize);
    let db = open(&fixture);
    let ks = fill(&db);
    // A copied key: key 0 is the first write of the serving stream, so it lies
    // in WAL file 0, which every qualifying target reclaims.
    let copied_offset = |db: &Db| -> (u64, u64) {
        let address = db
            .remote_for_test()
            .hybrid_table()
            .get(ks, &key(0), newest_cut())
            .expect("lookup")
            .expect("indexed")
            .address;
        (address.segment_generation, address.offset)
    };
    let before = copied_offset(&db);
    let all_before = offsets(&db, ks);
    let core = db.remote_for_test().write_core();
    core.test_hooks.lock().copy_durable_timeout = Some(COPY_WAIT);
    let epoch = core.epoch;
    let log = fixture.nodes[0].log_for_test(epoch);
    let frontier = log.frontier().unwrap();
    let pre_pass_appended = frontier.appended();
    assert_eq!(
        frontier.synced(),
        pre_pass_appended,
        "premise: the owner is synced before the pass"
    );
    // A lagging live root would let a root-only barrier persist take the hold
    // before the copies are appended, with the same lock hazard. Catch it up
    // first (a no-op when it is current).
    log.persist_live_root()
        .expect("the live root catches up before the pass");
    let (entered, resume) = log.pause_next_persist();
    let cancel = AtomicBool::new(false);
    // The pass must reach the parked persist before its own wait ends. The
    // deferral then comes from that parked sync, not from a timeout that ran
    // before any persist was parked.
    std::thread::scope(|scope| {
        let relocation = scope.spawn(|| db.remote_for_test().background_relocation_pass(&cancel));
        entered
            .recv_timeout(WAIT)
            .expect("the copy wait nudged the owner into its sync");
        assert!(
            frontier.appended() > pre_pass_appended,
            "premise: the copies advanced the appended end"
        );
        assert!(
            frontier.synced() < frontier.appended(),
            "premise: the copies are not synced yet, so the park is on a persist of them"
        );
        assert!(
            !relocation.is_finished(),
            "premise: the pass is still waiting at the parked sync"
        );
        relocation.join().unwrap();
    });
    assert_eq!(
        db.remote_for_test().relocation_copy_deferrals(),
        1,
        "the production pass counts its deferral and does not fail the database"
    );
    assert_eq!(
        offsets(&db, ks),
        all_before,
        "nothing was re-pointed or dropped"
    );
    core.ensure_healthy().expect("a deferral is not a failure");
    resume.send(()).unwrap();
    core.test_hooks.lock().copy_durable_timeout = Some(WAIT);
    let receipt = pass(&db);
    assert!(
        !receipt.deferred && receipt.copied_survivors > 0,
        "{receipt:?}"
    );
    assert_ne!(
        copied_offset(&db),
        before,
        "the completed pass re-pointed the copied key"
    );
    assert_eq!(
        db.remote_for_test().relocation_copy_deferrals(),
        1,
        "the completed pass defers nothing"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A failed durable-progress poll must defer the relocation pass without
/// applying its copied positions. This proves the poll-failure path only: the
/// injected failure fires at the top of the owner's progress nudge, before it
/// looks at what is persisted, so it may hit an owner whose copies are already
/// synced. An owner fsync failure of the copies is the claim of
/// `an_owner_sync_failure_of_the_copies_defers_the_pass_without_re_pointing`.
///
/// Every ordering is forced by hooks, not by time: the first progress nudge
/// parks before it runs; the test arms the single-shot failure and, before it
/// releases that nudge, parks the next one (the poller's retry), so no later
/// nudge can cover the copies. The test then requires proof that the failure
/// fired (`live_progress_failure_consumed` and exactly one more failed poll
/// counted by the durable cut) before it accepts the deferral. The pass's own
/// deadline only ends the wait; it is 30 s so that it never ends before the
/// first park is reached.
///
/// Negative controls (mutation runs): map the wait error to a database
/// failure (`m02`: `is_copies_not_durable` ignored in
/// `background_relocation_pass`) and `ensure_healthy` fails; omit
/// `fail_next_live_progress` and the consumed assertion fails.
#[test]
fn a_failed_progress_poll_defers_the_pass_without_re_pointing() {
    let fixture = fixture_with(shape());
    let db = open(&fixture);
    let ks = fill(&db);
    // A copied key: key 0 is the first write of the serving stream, so it lies
    // in WAL file 0, which every qualifying target reclaims.
    let copied_offset = |db: &Db| -> (u64, u64) {
        let address = db
            .remote_for_test()
            .hybrid_table()
            .get(ks, &key(0), newest_cut())
            .expect("lookup")
            .expect("indexed")
            .address;
        (address.segment_generation, address.offset)
    };
    let before = copied_offset(&db);
    let core = db.remote_for_test().write_core();
    core.test_hooks.lock().copy_durable_timeout = Some(COPY_WAIT);
    let log = fixture.nodes[0].log_for_test(core.epoch);
    let frontier = log.frontier().unwrap();
    let pre_pass_appended = frontier.appended();
    assert_eq!(
        frontier.synced(),
        pre_pass_appended,
        "premise: the owner is synced before the pass"
    );
    let report = |label: &str| {
        let frontier = log.frontier().unwrap();
        eprintln!(
            "{label}: root synced_end {} stream appended {} synced {}",
            log.position_frontier_for_test(),
            frontier.appended(),
            frontier.synced()
        );
    };
    let progress = crate::distributed::storage::LivePausePoint::ProgressBeforePersist;
    let (entered, resume) = log.pause_live(progress);
    let cancel = AtomicBool::new(false);
    let poll_failures_before = core.durable_cut_stats().poll_failures;
    let retry_parked = std::thread::scope(|scope| {
        let relocation = scope.spawn(|| db.remote_for_test().background_relocation_pass(&cancel));
        entered
            .recv_timeout(WAIT)
            .expect("the owner reached progress persistence");
        assert!(
            frontier.appended() > pre_pass_appended,
            "premise: the copies advanced the appended end"
        );
        assert!(
            frontier.synced() < frontier.appended(),
            "premise: the copies are not synced yet, so this poll is the one that covers them"
        );
        // The parked progress persist cannot advance durability, so the pass
        // must still wait for its copies when the failure is armed. The retry
        // park below is only the rendezvous of the completing pass: the
        // deferred pass may already have returned when it arrives, because it
        // observes the failure as soon as the released persist fails.
        assert!(
            !relocation.is_finished(),
            "premise: the pass is still waiting when the failed poll is armed"
        );
        report("progress persist parked");
        log.fail_next_live_progress();
        // Park the retry before the failing persist is released.
        let retry = log.pause_live(progress);
        resume.send(()).unwrap();
        retry
            .0
            .recv_timeout(WAIT)
            .expect("the poller retried after the failed progress persist");
        relocation.join().unwrap();
        retry.1
    });
    assert_eq!(
        core.durable_cut_stats().poll_failures,
        poll_failures_before + 1,
        "the durable cut counted the failed poll"
    );
    assert!(
        log.live_progress_failure_consumed(),
        "premise: the injected owner progress failure was consumed"
    );
    report("after the pass");
    assert_eq!(
        db.remote_for_test().relocation_copy_deferrals(),
        1,
        "the failed owner sync is a counted deferral"
    );
    assert_eq!(copied_offset(&db), before, "no re-point over a failed sync");
    for sequence in (0..SURVIVORS).chain(PAD_BASE..PAD_BASE + PADDING) {
        let value = db
            .get(ks, &key(sequence))
            .expect("read")
            .expect("key retained");
        assert_eq!(
            value.as_ref(),
            fill_value(sequence).as_slice(),
            "key {sequence}: a deferred pass preserves the value"
        );
    }
    core.ensure_healthy()
        .expect("a deferral is not a database failure");
    // The injected failure is single-shot and does not poison the owner: a
    // subsequent pass must sync and re-point the surviving values.
    retry_parked.send(()).unwrap();
    core.test_hooks.lock().copy_durable_timeout = Some(WAIT);
    let receipt = pass(&db);
    assert!(
        !receipt.deferred && receipt.copied_survivors > 0,
        "{receipt:?}"
    );
    assert_ne!(
        copied_offset(&db),
        before,
        "the completed pass re-pointed the copied key"
    );
    assert_eq!(db.remote_for_test().relocation_copy_deferrals(), 1);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// An owner persist failure of the RELOCATED COPIES must never re-point the
/// index. Unlike the progress-poll test above, the failure here fires on the
/// persist of the adopted copies themselves, so the copies are appended and
/// cannot become durable. The injected failure is fail-stop, exactly like a
/// real sync failure (the owner poisons its writer), so this test claims what
/// is provable: at the hold every key still names its old position (the copies
/// are parked unsynced and the index has not moved), the failure fired, no key
/// was re-pointed or dropped, and the engine reports the failure instead of
/// continuing. The byte-exact reads run BEFORE the pass, while the
/// database is quiescent, so no owner read lies inside the pass's wait. It
/// does not claim reads after the failure or a later pass.
///
/// The hold is the persistence lock, not the syncer. Two paths persist the
/// appended copies: the log syncer, and the poll worker that serves the copy
/// wait's nudge (`persist_live_progress`). Parking only the syncer leaves the
/// nudge free to sync the copies while the test runs, so the pass would apply
/// at once. `AppendedPersistenceLocked` parks the first appended persist of
/// either path inside `persist_scope_inner`, after it took the persistence
/// lock and before the injected failure is checked. The other path blocks on
/// that lock, so nothing syncs the copies until the test releases the hold.
/// Only the index is read at the hold. The pass's own wait bound (30 s) only
/// ends the wait; the release follows the hold in microseconds and the pass
/// ends at that bound, so the failure is consumed before the pass returns.
/// The test lasts about that bound by design: the wait never wakes on a
/// persist failure.
///
/// An unfiltered keyspace makes the copy set one whole WAL file of live
/// entries, so the fixture splits it into consecutive batches; the persist that
/// takes the hold is the first of them, and the pass never re-points a chunk
/// whose sync failed. Every early trigger (the copy wait's nudge, the durable
/// cut's lag timer, the syncer's fragment boundary) reaches the same hold, so
/// no premise rests on a timer being long.
///
/// Negative control (mutation run `m01`): remove the copy wait and the pass
/// re-points the copied keys over the failed persist.
#[test]
fn a_failed_owner_persist_of_the_copies_never_re_points_the_index() {
    let fixture = fixture_with_batch_bytes(shape(), 128 * KIB as usize);
    let db = open(&fixture);
    let ks = fill(&db);
    let before = offsets(&db, ks);
    let core = db.remote_for_test().write_core();
    let data_set_cut = core.table.current();
    core.test_hooks.lock().copy_durable_timeout = Some(COPY_WAIT);
    let log = fixture.nodes[0].log_for_test(core.epoch);
    let frontier = log.frontier().unwrap();
    let pre_pass_appended = frontier.appended();
    assert_eq!(
        frontier.synced(),
        pre_pass_appended,
        "premise: the owner is synced before the pass"
    );
    // A lagging live root would let a root-only barrier take the hold before
    // the pass appends its copies (a barrier persist has work while the root
    // lags the durable end, even when every byte is synced). Catch the root up
    // now with the explicit barrier that flush, fence and shutdown use; it is
    // a no-op when the root is current. The root's `synced_end` is not the
    // live root end, so it cannot serve as the premise.
    log.persist_live_root()
        .expect("the live root catches up before the pass");
    // Byte-exact reads of the keys a re-point would move, while nothing else
    // runs.
    for sequence in (0..SURVIVORS).chain(PAD_BASE..PAD_BASE + PADDING) {
        let value = db
            .get(ks, &key(sequence))
            .expect("read")
            .unwrap_or_else(|| panic!("key {sequence} unreadable before the pass"));
        assert_eq!(
            value.as_ref(),
            fill_value(sequence).as_slice(),
            "key {sequence}: the data set reads byte-equal before the pass"
        );
    }
    let (entered, resume) =
        log.pause_live(crate::distributed::storage::LivePausePoint::AppendedPersistenceLocked);
    let cancel = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let relocation = scope.spawn(|| db.remote_for_test().background_relocation_pass(&cancel));
        entered
            .recv_timeout(WAIT)
            .expect("an appended persist of the copies parked under the persistence lock");
        // The premise of the injected failure: the copies are appended and
        // not synced, and the persist parked here is the only one that can
        // sync them. Arming the failure now excludes an earlier persist
        // consuming it.
        assert!(
            frontier.appended() > pre_pass_appended,
            "premise: the copies advanced the appended end"
        );
        assert!(
            frontier.synced() < frontier.appended(),
            "premise: the copies are not synced yet"
        );
        // The pass is blocked in its copy wait: it has not returned and it has
        // counted no deferral. The index has not moved, so every key still
        // names its old position and none was dropped. The failure below is
        // fail-stop by design (the owner poisons its writer).
        assert!(
            !relocation.is_finished(),
            "premise: the pass is still waiting for its copies at the hold"
        );
        assert_eq!(
            db.remote_for_test().relocation_copy_deferrals(),
            0,
            "premise: no deferral before the release"
        );
        assert_eq!(
            offsets(&db, ks),
            before,
            "premise: no key moved or dropped at the hold"
        );
        log.fail_next_appended_persist();
        resume.send(()).unwrap();
        relocation.join().unwrap();
    });
    assert!(
        core.table.current() > data_set_cut,
        "premise: at least one batch of copies was published past the data set"
    );
    assert!(
        log.appended_persist_failure_consumed(),
        "premise: the injected owner persist failure of the copies fired"
    );
    assert_eq!(
        offsets(&db, ks),
        before,
        "no re-point and no drop over a failed owner persist"
    );
    assert!(
        core.ensure_healthy().is_err(),
        "a failed persist of the copies is fail-stop, never a silent skip"
    );
    // The injected failure is fail-stop: the owner poisons its writer and the
    // coordinator records the uncertain version, so no read is claimed here.
    // The sibling test at the parked-sync path owns the healthy-deferral and
    // next-pass claims; the crash test owns loss after a deferred pass.
}

/// Copies a directory tree: the owner's files frozen at one instant.
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create crash image directory");
    for entry in std::fs::read_dir(from).expect("read owner directory") {
        let entry = entry.expect("directory entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy owner file");
        }
    }
}

/// Rewrites every file of `image` into `live` through its existing path (same
/// inode) and removes the files `live` gained after the image was taken.
/// Directories are never replaced.
fn restore_in_place(image: &std::path::Path, live: &std::path::Path) {
    for entry in std::fs::read_dir(live).expect("read live directory") {
        let entry = entry.expect("directory entry");
        let counterpart = image.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            if counterpart.is_dir() {
                restore_in_place(&counterpart, &entry.path());
            } else {
                std::fs::remove_dir_all(entry.path()).expect("remove a post-image directory");
            }
        } else if !counterpart.exists() {
            std::fs::remove_file(entry.path()).expect("remove a post-image file");
        }
    }
    for entry in std::fs::read_dir(image).expect("read image directory") {
        let entry = entry.expect("directory entry");
        let target = live.join(entry.file_name());
        if entry.file_type().expect("entry type").is_dir() {
            std::fs::create_dir_all(&target).expect("create image directory");
            restore_in_place(&entry.path(), &target);
        } else {
            // `fs::copy` onto an existing path truncates and rewrites that inode.
            std::fs::copy(entry.path(), &target).expect("restore image file");
        }
    }
}

/// The data loss of a power cut: every byte of the epoch's WAL files above
/// `synced` was never made durable and reads back as zeros.
fn lose_everything_above(root: &std::path::Path, epoch: u64, synced: u64) {
    let mut index = synced / FILE;
    let mut from = synced % FILE;
    loop {
        let wal = root.join(format!("epoch_{epoch:016x}/wal_{index:016x}"));
        let Ok(file) = std::fs::OpenOptions::new().write(true).open(&wal) else {
            break;
        };
        let len = file.metadata().unwrap().len();
        std::os::unix::fs::FileExt::write_all_at(&file, &vec![0; (len - from) as usize], from)
            .unwrap();
        index += 1;
        from = 0;
    }
}

/// Count nonzero bytes in the copied owner image over the specified WAL range.
fn nonzero_image_bytes(root: &std::path::Path, epoch: u64, start: u64, end: u64) -> u64 {
    let mut offset = start;
    let mut count = 0;
    let mut buffer = [0u8; 4096];
    while offset < end {
        let path = root.join(format!("epoch_{epoch:016x}/wal_{:016x}", offset / FILE));
        let file = std::fs::File::open(path).expect("open image WAL file");
        let within_file = offset % FILE;
        let len = (end - offset)
            .min(FILE - within_file)
            .min(buffer.len() as u64) as usize;
        std::os::unix::fs::FileExt::read_exact_at(&file, &mut buffer[..len], within_file)
            .expect("read image WAL bytes");
        count += buffer[..len].iter().filter(|byte| **byte != 0).count() as u64;
        offset += len as u64;
    }
    count
}

/// Recovery invariant at every stored snapshot: the replay front the region
/// names for the serving stream never lies above the end the owner's installed
/// root covers. A front above it would make recovery skip bytes the owner
/// still holds; the root is what owner recovery trusts. Prints both values so
/// a failure and a pass are both readable in the log.
fn assert_front_within_root(db: &Db, fixture: &Fixture, label: &str) {
    let epoch = db.remote_for_test().write_core().epoch;
    let root_end = fixture.nodes[0]
        .log_for_test(epoch)
        .position_root_for_test()
        .2;
    for front in db.remote_for_test().hybrid_table().stored_fronts() {
        if front.generation != epoch {
            continue;
        }
        eprintln!(
            "{label}: stored front {} <= owner root-covered end {root_end}",
            front.replay_from
        );
        assert!(
            front.replay_from <= root_end,
            "{label}: stored front {} is above the owner root-covered end {root_end}",
            front.replay_from
        );
    }
}

/// Crash cut with data loss. The owner's sync is parked, so the deferred pass
/// has written its copies but no root covers them. The crash point is that
/// instant: an IMAGE of the owner's files is taken while the sync is still
/// parked (the persist has not installed a root), and every byte above the
/// root-certified end is zeroed in the image. The parked persist is released
/// only afterwards, to stop the running owner; its effects are not in the
/// image. The owner then restarts from the image and the database reopens with
/// every key readable from its OLD position, bytes compared, and the old WAL
/// file still present. With the wait missing the pass would have stored the
/// re-points and the removals, and the reopened index would name bytes the
/// owner lost or lack keys the owner still holds.
///
/// The fixture keeps the copies ABOVE the root's `synced_end`: the pass appends
/// them at the head after the certified data set, and the parked persist is the
/// only path that could sync them.
/// The premise `root_synced < copy_end` is asserted before the loss. At the
/// park the test asserts that the pass still waits. After the join it asserts
/// that the copy wait asked an owner to sync (a `targeted_syncs` delta: only
/// relocation copies bump it, and the wait bumps it as it starts to sample)
/// and counts the pass's deferral. The byte checks run before the deferral and
/// old-file assertions, so a run without the wait fails on lost values first.
///
/// Releasing the persist BEFORE zeroing is not a crash: the released persist
/// installs a root over the copies and the zeroing then wipes bytes that root
/// certifies, which owner recovery rightly rejects (`replay position is not a
/// frame boundary`).
///
/// Negative control (mutation run `m01`): remove the `await_copies_durable`
/// call in `flush_relocated`: the pass applies, stores and retires under the
/// parked sync, and a key reads `None` or errors after the restart.
#[test]
fn a_native_tier_crash_after_a_deferred_pass_loses_no_value() {
    let mut fixture = fixture_with_batch_bytes(shape(), 128 * KIB as usize);
    let db = open(&fixture);
    fill(&db);
    db.remote_for_test()
        .write_core()
        .flush_index()
        .expect("store a region over the data set");
    assert_front_within_root(&db, &fixture, "after the data set");
    let core = db.remote_for_test().write_core();
    core.test_hooks.lock().copy_durable_timeout = Some(COPY_WAIT);
    let epoch = core.epoch;
    let log = fixture.nodes[0].log_for_test(epoch);
    let frontier = log.frontier().unwrap();
    let synced = frontier.synced();
    assert_eq!(
        synced,
        frontier.appended(),
        "premise: the owner is synced before the pass, so `synced` is a frame boundary \
         and every byte below it survives a power cut"
    );
    // A lagging live root would let a root-only barrier persist take the hold
    // before the copies are appended; it holds the persistence lock, and the
    // next append's lease request would block on it until the owner deadline.
    // Catch it up first (a no-op when it is current).
    log.persist_live_root()
        .expect("the live root catches up before the pass");
    let (entered, resume) = log.pause_next_persist();
    drop(log);
    let cancel = AtomicBool::new(false);
    let targeted_syncs_before = core.durable_cut_stats().targeted_syncs;
    // The pass must reach the parked persist before its own wait ends, and the
    // image must be taken while that sync is still parked and the pass has
    // deferred (no re-points). A timeout that ran before any persist parked
    // would leave the crash point unproven.
    std::thread::scope(|scope| {
        let relocation = scope.spawn(|| db.remote_for_test().background_relocation_pass(&cancel));
        entered
            .recv_timeout(WAIT)
            .expect("the copy wait nudged the owner into its sync");
        assert!(
            !relocation.is_finished(),
            "premise: the pass is still waiting at the parked sync"
        );
        relocation.join().unwrap();
    });
    let deferrals = db.remote_for_test().relocation_copy_deferrals();
    // Not at the park: the lag timer, the syncer or an already-set nudge flag
    // can park the persist before the copy wait is bumped (`wait_through` swaps
    // the flag first and bumps the counter second). After the join the wait has
    // run to its bound, so a deferral proves the copy wait asked for a sync.
    let targeted_syncs = core.durable_cut_stats().targeted_syncs - targeted_syncs_before;
    assert!(
        targeted_syncs > 0,
        "premise: the copy wait asked an owner to sync before it deferred"
    );
    eprintln!("after the pass: targeted syncs {targeted_syncs}, deferrals {deferrals}");
    assert_front_within_root(&db, &fixture, "at the crash point");

    // The crash point: the owner's AND the coordinator's files with the sync
    // still parked. The coordinator's durable-cut record must not end up
    // naming bytes the owner image lost (the released persist would let the cut
    // actor record them in the live directory after this instant).
    let image = tempdir::TempDir::new("copy-durable-pass-image").unwrap();
    let owner_image = image.path().join("owner");
    let coordinator_image = image.path().join("coordinator");
    copy_dir(fixture.owner_database_root(0), &owner_image);
    copy_dir(&fixture.authority(), &coordinator_image);
    // A power cut keeps everything the owner's installed root certifies AND
    // everything the frontier fsynced; the two can differ by one fragment in
    // either direction. Bytes above both may be lost, so the boundary is their
    // maximum: zeroing from there removes only what a power cut can lose.
    let root_synced = fixture.nodes[0]
        .log_for_test(epoch)
        .position_root_for_test()
        .2;
    let copy_end = fixture.nodes[0]
        .log_for_test(epoch)
        .frontier()
        .unwrap()
        .appended();
    let durable = root_synced.max(synced);
    eprintln!(
        "crash point: pre-pass synced {synced}, root-certified synced_end {root_synced}, \
         durable {durable}, copies end at {copy_end}"
    );
    assert!(
        durable < copy_end,
        "premise: the copies (end {copy_end}) lie above the durable end {durable}; \
         a sync over them makes the crash lose nothing and this test blind"
    );
    let nonzero = nonzero_image_bytes(&owner_image, epoch, durable, copy_end);
    eprintln!("crash image: {nonzero} nonzero bytes above the durable end");
    assert!(
        nonzero > 0,
        "premise: the simulated power cut removes actual bytes"
    );
    lose_everything_above(&owner_image, epoch, durable);

    // Stop everything (the release only lets the live owner finish) and put the
    // images back IN PLACE: files are rewritten through their existing paths
    // and directories are kept, because the owner binds the identity (device
    // and inode) of its target state directory and WAL files, and a deleted and
    // re-created tree is a different machine state, not a crash.
    resume.send(()).unwrap();
    db.wait_for_background_threads_to_finish().unwrap();
    fixture.stop_owners();
    restore_in_place(&owner_image, fixture.owner_database_root(0));
    restore_in_place(&coordinator_image, &fixture.authority());
    fixture.restart_owners().unwrap();
    let db = open(&fixture);
    let ks = db.ks("a");
    for sequence in (0..SURVIVORS).chain(PAD_BASE..PAD_BASE + PADDING) {
        // `exists` is index-only for a keyspace without key check: read the
        // bytes, which is what a lost value would change.
        let value = db
            .get(ks, &key(sequence))
            .unwrap_or_else(|error| panic!("read {sequence} after the crash: {error:?}"))
            .unwrap_or_else(|| panic!("key {sequence} lost"));
        assert_eq!(
            value.as_ref(),
            fill_value(sequence).as_slice(),
            "key {sequence} has the wrong bytes"
        );
    }
    db.wait_for_background_threads_to_finish().unwrap();
    assert_eq!(deferrals, 1, "the pass deferred under the parked sync");
    assert!(
        owner_image
            .join(format!("epoch_{epoch:016x}/wal_{:016x}", 0))
            .exists(),
        "the old file is still there after a deferral"
    );
}

/// Consecutive passes complete without failure: the first pass waits for its
/// copies and completes, an ordinary write proceeds between the passes, and
/// the second pass returns without a deferral. The second pass may find no
/// reclaimable target; the test proves that neither pass fails or livelocks,
/// not that the second one waits.
#[test]
fn consecutive_passes_complete_without_failure() {
    let fixture = fixture();
    let db = open(&fixture);
    let ks = fill(&db);
    let first = pass(&db);
    assert!(!first.deferred, "{first:?}");
    assert!(
        first.copied_survivors > 0,
        "premise: the first pass has survivors to copy: {first:?}"
    );
    let mut batch = db.write_batch();
    batch.write(ks, key(WRITES), vec![9; VALUE]);
    batch.commit().expect("ordinary writes proceed");
    let second = pass(&db);
    assert!(!second.deferred, "{second:?}");
    assert_eq!(db.remote_for_test().relocation_copy_deferrals(), 0);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// The relocation scan freezes `Q` against the published floor. Frontiers are
/// exclusive, so an idle database after `flush_durable` has `Q == published_next`
/// (every batch below it published and synced), and the pass must run; a `Q`
/// one past the floor names a batch that may still be published under the scan
/// and is refused with the typed error.
#[test]
fn a_pass_accepts_a_stable_cut_at_the_published_floor_and_refuses_one_above_it() {
    let fixture = fixture();
    let db = open(&fixture);
    let ks = db.ks("a");
    let mut batch = db.write_batch();
    batch.write(ks, key(0), vec![1; VALUE]);
    batch.commit().expect("write");
    let remote = db.remote_for_test();
    let core = remote.write_core();
    core.flush_durable().expect("certify the write");
    let published_next = core.publisher.lock().published_next;
    let q = core.table.last_processed();
    assert_eq!(
        (q.epoch, q.sequence),
        (core.epoch, published_next),
        "premise: an idle certified database sits exactly at the floor"
    );
    let cancel = AtomicBool::new(false);
    remote
        .relocation_pass(false, &cancel)
        .expect("a stable cut at the published floor is accepted");

    let above = BatchVersion {
        epoch: core.epoch,
        sequence: published_next + 1,
    };
    core.table.advance_applied(above);
    core.table.cuts().set_durable(above);
    assert_eq!(
        core.table.last_processed(),
        above,
        "premise: Q moved one past the floor"
    );
    let error = remote
        .relocation_pass(false, &cancel)
        .expect_err("a stable cut above the published floor is refused");
    assert!(
        error.to_string().contains("exceeds published floor"),
        "{error}"
    );
}

const OLD_MARK: u8 = 0xEE;
const NEW_MARK: u8 = 0x11;

fn marked_value(mark: u8, sequence: u64) -> Vec<u8> {
    let mut value = vec![(sequence % 251) as u8; VALUE];
    value[0] = mark;
    value
}

/// Values per commit when a generation is written only to fill files. Seven
/// 8 KiB values plus their member headers stay below the fixture's 64 KiB
/// batch limit, and the same values still span the same 1 MiB files; the
/// commits are seven times fewer, so a loaded machine spends less time in
/// admission and append.
const KEYS_PER_COMMIT: u64 = 7;

/// Writes `0..WRITES` old-marked values in [`KEYS_PER_COMMIT`]-value commits.
fn write_old_generation(db: &Arc<Db>, ks: KeySpace) {
    for first in (0..WRITES).step_by(KEYS_PER_COMMIT as usize) {
        let mut batch = db.write_batch();
        for sequence in first..(first + KEYS_PER_COMMIT).min(WRITES) {
            batch.write(ks, key(sequence), marked_value(OLD_MARK, sequence));
        }
        batch.commit().expect("write old value");
    }
    // The old generation's scan stops on this record, so its earlier files have
    // a target (native's filter Stop).
    stop_marker(db);
}

/// A sealed-file Busy reply increments the coordinator counter once. A
/// completed retirement after the record lease is released does not.
#[test]
fn a_busy_sealed_retirement_reply_is_counted_but_completion_is_not() {
    let fixture = fixture_with(shape());
    let db = open(&fixture);
    let ks = db.ks("a");
    write_old_generation(&db, ks);
    let address = db
        .remote_for_test()
        .hybrid_table()
        .get(ks, &key(0), newest_cut())
        .expect("lookup")
        .expect("indexed")
        .address;
    let old_epoch = db.remote_for_test().write_core().epoch;
    db.remote_for_test().write_core().flush_durable().unwrap();
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let ks = db.ks("a");
    let mut batch = db.write_batch();
    batch.write(ks, key(1000), marked_value(NEW_MARK, 1000));
    batch.commit().expect("write new value");
    db.remote_for_test().write_core().flush_durable().unwrap();

    let endpoint = &fixture.config.endpoints[0];
    let mut tls = fixture.config.tls.clone();
    tls.server_name = endpoint.server_name.clone();
    let mut client = Client::connect(
        &endpoint.address,
        &tls,
        WAIT,
        fixture.config.partition_config(1, endpoint.partition),
        &fixture.shape,
    )
    .unwrap();
    let epoch = db.remote_for_test().write_core().epoch;
    let lease = client.resolve_live_records(epoch, &[address]).unwrap();
    let victim = lease.entries[0].segment.path.clone();
    let victim_file = lease.entries[0].segment.file_index * lease.entries[0].segment.bytes;
    let victim_log = fixture.nodes[0].sealed_log_for_test(old_epoch);
    let (earlier_events, earlier_dropped) = victim_log.take_busy_events();
    assert_eq!(
        (earlier_events.len(), earlier_dropped),
        (0, 0),
        "premise: no earlier Busy causes"
    );
    assert!(victim.exists(), "premise: the pinned old file exists");
    let remote = db.remote_for_test();
    let before = remote.retirement_busy_replies();
    assert_eq!(before, 0, "premise: no earlier Busy replies");
    // The lease is held before the first pass, so no pass can retire the
    // victim first. The old generation is three 1 MiB files (`WRITES` x `VALUE`
    // is 2.4 MiB); a pass retires only whole files below its span target, and
    // the target is file-aligned, so the victim (which holds the first record)
    // is in file 0. Three attempts bound the loop: the first pass names the
    // generation-1 front by its forced store, the second retires the files
    // below that front, and the third is a spare for a pass consumed by an
    // adoption step. A fourth attempt with no Busy reply means no pass reaches
    // the owner retirement.
    let mut blocked_passes = 0;
    while remote.retirement_busy_replies() == before {
        assert!(
            blocked_passes < 3,
            "no pass reached the owner retirement in three attempts"
        );
        let blocked = pass(&db);
        assert!(!blocked.deferred, "{blocked:?}");
        blocked_passes += 1;
    }
    assert_eq!(remote.retirement_busy_replies(), before + 1);
    let (events, dropped) = victim_log.take_busy_events();
    assert_eq!(dropped, 0, "no Busy causes were lost");
    assert_eq!(
        events,
        vec![crate::distributed::storage::BusyEvent {
            epoch: old_epoch,
            file: victim_file,
            cause: crate::distributed::storage::BusyCause::RecordPin,
        }],
        "the counted Busy came from the held record pin on this victim"
    );
    assert!(victim.exists(), "the pinned file was not retired");
    client
        .release_live_epoch_pin(epoch, lease.epoch_pin_token)
        .unwrap();
    // The next pass has the same whole-file target (it did not advance) and
    // retries the retirement, as native's gc does.
    let completed = pass(&db);
    assert!(!completed.deferred, "{completed:?}");
    assert!(!victim.exists(), "the unpinned file was retired");
    assert_eq!(address.segment_generation, old_epoch);
    let value = db.get(ks, &key(1000)).unwrap().unwrap();
    assert_eq!(value.as_ref(), marked_value(NEW_MARK, 1000).as_slice());
    db.wait_for_background_threads_to_finish().unwrap();
}

/// Owner crash after the retirement of its LAST data file.
///
/// Generation 1 writes until its newest write lies in the second fragment of a
/// file, so the stream ends exactly at a file boundary when the generation
/// closes. Generation 2 writes the keys that stay, and one pass removes every
/// generation-1 value: the whole sealed generation is dead, so every file of it
/// retires, including the one that ends at the allocator. The owner then
/// restarts (a crash: no shutdown of the owner state) and the database opens
/// again. Before fix A' the owner recovered its end from the scan origin only
/// and the open failed for good with "sealed WAL ends below the required
/// position". Every kept key must read back byte-equal, and the removed keys
/// must stay removed.
#[test]
fn an_owner_restart_after_the_last_data_file_retired_still_opens_and_serves_every_key() {
    let mut fixture = fixture_with(shape());
    let db = open(&fixture);
    let first_generation = db.remote_for_test().write_core().epoch;
    let ks = db.ks("a");
    let mut old_keys = 0;
    loop {
        // A geometry change that skips fragment 5 must fail here, not fill the
        // disk: two `WRITES` keys is twice the data set the fragment target
        // needs at 8 KiB per value.
        assert!(
            old_keys < 2 * WRITES,
            "{old_keys} keys never reached fragment 5 of the {FILE}-byte files"
        );
        let mut batch = db.write_batch();
        for sequence in old_keys..old_keys + KEYS_PER_COMMIT {
            batch.write(ks, key(sequence), marked_value(OLD_MARK, sequence));
        }
        batch.commit().expect("write");
        old_keys += KEYS_PER_COMMIT;
        let address = db
            .remote_for_test()
            .hybrid_table()
            .get(ks, &key(old_keys - 1), newest_cut())
            .expect("lookup")
            .expect("indexed")
            .address;
        // Fragment 5 is the second fragment of file 2: the stream ends there.
        if address.offset / FRAGMENT >= 5 {
            break;
        }
    }
    // The stop record lies above the data set, so the scan has a target and
    // the certified bytes are below it.
    stop_marker(&db);
    let marker = db
        .remote_for_test()
        .hybrid_table()
        .get(db.ks("s"), &key(STOP_MARKER), newest_cut())
        .expect("lookup")
        .expect("indexed");
    let stream_end = marker.address.offset + u64::from(marker.address.len);
    db.remote_for_test()
        .write_core()
        .flush_durable()
        .expect("certify generation 1");
    db.wait_for_background_threads_to_finish().unwrap();

    let db = open(&fixture);
    let ks = db.ks("a");
    const NEW_KEYS: u64 = 20;
    for sequence in 1000..1000 + NEW_KEYS {
        let mut batch = db.write_batch();
        batch.write(ks, key(sequence), marked_value(NEW_MARK, sequence));
        batch.commit().expect("write");
    }
    db.remote_for_test()
        .write_core()
        .flush_durable()
        .expect("certify generation 2");
    // Generation 1 closed before any region store, so its start is seeded at
    // open with no stored front: the first pass has no watermark for it
    // (`take_snapshot` skips such a stream) and its forced store is what
    // names a front. A later pass then retires the files, the last one
    // included. At most three passes may be needed; none retiring would be a
    // stream-bound leak.
    let epoch_directory = fixture
        .owner_database_root(0)
        .join(format!("epoch_{first_generation:016x}"));
    // Frame files only: files wholly after the stream end are lookahead and
    // hold no frame, so their retirement is not part of this property.
    let wal_files = || -> Vec<String> {
        std::fs::read_dir(&epoch_directory)
            .expect("generation 1 directory")
            .map(|entry| {
                entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| {
                name.strip_prefix("wal_")
                    .and_then(|index| u64::from_str_radix(index, 16).ok())
                    // The marker's own file holds a frame the generation needs.
                    .is_some_and(|index| {
                        index * FILE < marker.address.offset - marker.address.offset % FILE
                    })
            })
            .collect()
    };
    let mut retired_after_pass = None;
    for number in 1..=6 {
        let receipt = pass(&db);
        let fronts = db.remote_for_test().hybrid_table().stored_fronts();
        let remaining = wal_files();
        let remote = db.remote_for_test();
        eprintln!(
            "liveness pass {number}: receipt {receipt:?} stored fronts {fronts:?} \
             generation 1 WAL files {remaining:?} busy replies {} owner busy retirements {} \
             missing rows {}",
            remote.retirement_busy_replies(),
            fixture.nodes[0].file_maintenance_stats().busy_retirements,
            remote.retirement_missing_rows(),
        );
        assert!(!receipt.deferred, "pass {number}: {receipt:?}");
        if remaining.is_empty() {
            retired_after_pass = Some(number);
            break;
        }
    }
    let retired_after_pass = retired_after_pass.unwrap_or_else(|| {
        panic!(
            "generation 1 kept its WAL files {:?} after three passes: stream-bound leak",
            wal_files()
        )
    });
    eprintln!("generation 1 fully retired after pass {retired_after_pass}");
    // Premise of the frame-file filter above: every file it leaves out lies
    // wholly after the stream end and holds no frame. A file is either all
    // zero or holds only the owner's skip marker (8 x 0xff, then zeros): the
    // owner closes a granted live extent at the next file start with that
    // marker, and it is not a frame.
    // At least one such file must exist, or the exclusion would be untested.
    // Negative control: counting a frame-holding file as lookahead (the bound
    // moved one file down) makes the check fail.
    let mut lookahead = 0;
    for entry in std::fs::read_dir(&epoch_directory).expect("generation 1 directory") {
        let entry = entry.expect("entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(index) = name
            .strip_prefix("wal_")
            .and_then(|index| u64::from_str_radix(index, 16).ok())
        else {
            continue;
        };
        if index * FILE < stream_end {
            continue;
        }
        lookahead += 1;
        let bytes = std::fs::read(entry.path()).expect("read lookahead file");
        let only_zeros = bytes.iter().all(|byte| *byte == 0);
        let only_skip_marker = bytes.len() >= 8
            && bytes[..8].iter().all(|byte| *byte == 0xff)
            && bytes[8..].iter().all(|byte| *byte == 0);
        assert!(
            only_zeros || only_skip_marker,
            "lookahead file {name} holds a frame after the stream end {stream_end}"
        );
    }
    assert!(
        lookahead > 0,
        "premise: the owner kept a frameless lookahead file"
    );
    // The crash point: an image of the owner's and the coordinator's files right
    // after the retirement, before any shutdown. `stop_owners` is a graceful
    // stop (it may write a SEAL), so the first restart below puts this image
    // back in place: a kill.
    let image = tempdir::TempDir::new("liveness-image").unwrap();
    let owner_image = image.path().join("owner");
    let coordinator_image = image.path().join("coordinator");
    copy_dir(fixture.owner_database_root(0), &owner_image);
    copy_dir(&fixture.authority(), &coordinator_image);
    eprintln!(
        "image: generation 1 directory entries {:?}",
        std::fs::read_dir(owner_image.join(format!("epoch_{first_generation:016x}")))
            .map(|entries| entries
                .map(|entry| entry
                    .expect("entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned())
                .collect::<Vec<_>>())
            .unwrap_or_default()
    );
    db.wait_for_background_threads_to_finish().unwrap();

    // The owner restarts three times in a row. The first restart is a kill: the
    // image is restored in place, so generation 1 has no SEAL record when the
    // owner recovers it (the no-SEAL floor path); the first recovery writes the
    // SEAL and the later ones read it back. Every open must succeed and serve
    // every key.
    for restart in 1..=3 {
        fixture.stop_owners();
        if restart == 1 {
            restore_in_place(&owner_image, fixture.owner_database_root(0));
            restore_in_place(&coordinator_image, &fixture.authority());
        }
        fixture.restart_owners().unwrap();
        let db = open(&fixture);
        let ks = db.ks("a");
        for sequence in 1000..1000 + NEW_KEYS {
            let value = db
                .get(ks, &key(sequence))
                .unwrap_or_else(|error| panic!("restart {restart}: read {sequence}: {error:?}"))
                .unwrap_or_else(|| panic!("restart {restart}: key {sequence} lost"));
            assert_eq!(
                value.as_ref(),
                marked_value(NEW_MARK, sequence).as_slice(),
                "restart {restart}: key {sequence} has the wrong bytes"
            );
        }
        // An unfiltered keyspace copies every live entry below its target, so
        // generation 1's values survive their files' retirement at their
        // serving-generation copies.
        for sequence in 0..old_keys {
            let value = db
                .get(ks, &key(sequence))
                .unwrap_or_else(|error| panic!("restart {restart}: read {sequence}: {error:?}"))
                .unwrap_or_else(|| panic!("restart {restart}: copied key {sequence} lost"));
            assert_eq!(
                value.as_ref(),
                marked_value(OLD_MARK, sequence).as_slice(),
                "restart {restart}: key {sequence} has the wrong bytes"
            );
        }
        db.wait_for_background_threads_to_finish().unwrap();
    }
}
