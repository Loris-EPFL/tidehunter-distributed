//! Paper 4.4 re-point compare-and-set: a write that lands between a copy and
//! its re-point keeps the newer position; the pass installs no stale copy.

use super::copy_durable_pass_tests::{
    COPY_WAIT, KIB, VALUE, WAIT, fill, fixture_with_batch_bytes, newest_cut, open, shape,
};
use super::*;
use crate::distributed::sui::Db;
use std::sync::atomic::AtomicBool;

/// The indexed physical position of `key`: `(generation, offset)`.
fn position(db: &Db, ks: KeySpace, key: &[u8]) -> (u64, u64) {
    let address = db
        .remote_for_test()
        .hybrid_table()
        .get(ks, key, newest_cut())
        .expect("lookup")
        .expect("indexed")
        .address;
    (address.segment_generation, address.offset)
}

/// A survivor rewritten while the pass is parked between its copy and its
/// re-point keeps the newer position: `relocation_apply` compares the current
/// position with the scanned expectation (`same_physical`) and skips a stale
/// one. The other survivors still take their copies, so the parked chunk is a
/// real copy set, not an empty one.
///
/// Negative control (mutation run `m19`): ignore both compare-and-set guards
/// (the current position equals the scanned one, and the current position is
/// below the stream target). The pass then installs the copy over the newer
/// write, and the index refuses the colliding position in `apply_update`.
#[test]
fn a_rewrite_between_the_copy_and_the_re_point_keeps_the_newer_position() {
    let fixture = fixture_with_batch_bytes(shape(), 128 * KIB as usize);
    let db = open(&fixture);
    let ks = fill(&db);
    let core = db.remote_for_test().write_core();
    // The park below must not race the pass's own copy wait: that wait ends at
    // `COPY_WAIT`, below the test's `WAIT`, so a pass that never reaches the
    // park fails at the park wait instead of hiding behind a 120 s bound.
    core.test_hooks.lock().copy_durable_timeout = Some(COPY_WAIT);
    let (entered, paused) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    core.test_hooks.lock().relocation_copy_pause = Some((entered, resume));
    let cancel = AtomicBool::new(false);
    let (key_written, moved, report) = std::thread::scope(|scope| {
        // Move the release sender into the scope so a failing premise drops
        // it, the parked pass resumes, and the test fails instead of hanging.
        let release = release;
        let pass = scope.spawn(|| {
            db.remote_for_test()
                .relocation_pass(false, &cancel)
                .expect("relocation pass")
        });
        let (keyspace, first) = paused
            .recv_timeout(WAIT)
            .expect("the pass parked between the copy and the re-point");
        assert_eq!(
            keyspace, ks,
            "premise: the parked cell is the test keyspace"
        );
        let rewritten = first.expect("the parked chunk names a key").to_vec();
        let before = position(&db, ks, &rewritten);
        // The rewrite lands after the copy and before the re-point.
        let mut batch = db.write_batch();
        batch.write(ks, rewritten.clone(), vec![0xAB; VALUE]);
        batch.commit().expect("rewrite the parked survivor");
        let moved = position(&db, ks, &rewritten);
        assert_ne!(moved, before, "premise: the rewrite moved the key");
        let value = db
            .get(ks, &rewritten)
            .expect("read the rewrite")
            .expect("the rewrite is indexed");
        assert_eq!(
            value.as_ref(),
            vec![0xAB; VALUE].as_slice(),
            "premise: the rewrite is readable before the re-point"
        );
        release.send(()).unwrap();
        let report = pass.join().unwrap();
        (rewritten, moved, report)
    });
    assert!(!report.deferred, "{report:?}");
    assert!(
        report.copied_survivors > 0,
        "premise: the pass copied survivors: {report:?}"
    );
    assert_eq!(
        position(&db, ks, &key_written),
        moved,
        "the pass must not re-point over the newer write"
    );
    let value = db
        .get(ks, &key_written)
        .expect("read after the pass")
        .expect("the key is indexed");
    assert_eq!(
        value.as_ref(),
        vec![0xAB; VALUE].as_slice(),
        "the newer value survives the pass"
    );
    db.wait_for_background_threads_to_finish().unwrap();
}
