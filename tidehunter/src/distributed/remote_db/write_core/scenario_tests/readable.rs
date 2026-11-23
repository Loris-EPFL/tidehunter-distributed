//! Read-your-writes on the acknowledged path. It lives in its own module so
//! `scenario_tests.rs` stays under the file-length cap; helpers are the parent's.

use super::*;

/// An acknowledged write is readable by its writer (paper 3.1/3.2; native
/// inserts into the index before it returns). The earlier batch is held on its
/// owner worker before publication; the later write publishes beyond the floor
/// but must not return until the earlier one resolves, and then reads at once.
#[test]
fn an_acknowledged_write_waits_for_the_earlier_batch_and_then_reads() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let (hook, publishing, resume) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let earlier = spawn_write(&core, fixture.operation(0, 1), vec![put(1, 1)]);
    publishing.recv_timeout(WAIT).unwrap();
    let (waiting_hook, waiting) = mpsc::sync_channel(1);
    core.test_hooks.lock().drain_wait_entered = Some(waiting_hook);
    let later = spawn_write(&core, fixture.operation(1, 2), vec![put(2, 2)]);
    // The signal comes from the later call's prefix wait, after its batch
    // published beyond the floor.
    waiting.recv_timeout(WAIT).unwrap();
    assert!(core.is_published(version(&core, 1)));
    assert_eq!(core.published_next(), 0);
    assert!(
        matches!(later.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "the later write returned below an unresolved earlier batch"
    );
    resume.send(()).unwrap();
    assert_eq!(
        earlier.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 0)
    );
    assert_eq!(
        later.recv_timeout(WAIT).unwrap().unwrap(),
        version(&core, 1)
    );
    // Read-your-writes right after Ok, with no drain in between.
    assert_eq!(value(&mut db, 2), Some(vec![2; 32]));
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    drop(core);
    db.shutdown().unwrap();
}
