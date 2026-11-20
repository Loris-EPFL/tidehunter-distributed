//! The two scenarios that pin what the retry window and the allocation window
//! promise across a crash. They live in their own module so `scenario_tests.rs`
//! stays under the file-length cap; helpers are the parent's.

use super::*;
use crate::distributed::remote_db::RecentReceipt;
use crate::distributed::storage::WAL_REPLAY_WINDOW_BATCHES;
use crate::distributed::types::PayloadDigest;

/// The evicted-receipt half of RustReview's T5 pair: a retry whose operation id
/// *was* published, and whose receipt later publications pushed out of the
/// window, must re-apply — never report the old version from memory.
///
/// The pushes are synthetic because the alternative is 4,096 owner writes: the
/// ring's own eviction is what this exercises, and `receipts.rs` already covers
/// push/evict in isolation. What only `wait_retry` can answer is the composition:
/// slot 0's generation has advanced, the probe misses, and the result is
/// `Reapply` rather than `Published` for a write the window can no longer
/// confirm.
#[test]
fn a_receipt_the_window_pushed_out_reapplies() {
    let fixture = Fixture::new_with_config(128, shape(), 2, |config| {
        config.max_pending_batches = 2;
    });
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let published = fixture.operation(0, 1);
    assert_eq!(
        core.write(published, vec![put(1, 1)], None).unwrap(),
        version(&core, 0)
    );
    // Slot 0 is reusable, so `wait_retry` cannot answer from the generation.
    core.wait_settled().unwrap();
    core.write(fixture.operation(1, 2), vec![put(2, 1)], None)
        .unwrap();
    core.write(fixture.operation(1, 3), vec![put(3, 1)], None)
        .unwrap();
    core.wait_settled().unwrap();
    // Three writes on two slots: sequence 2 went back to slot 0, so its
    // generation advanced and `wait_retry` cannot answer from the slot.
    assert_eq!(core.sequencer.generation(0), Some(2));
    {
        let mut publisher = core.publisher.lock();
        assert!(
            publisher.recent.get(&published).is_some(),
            "the receipt must be live before the pushes",
        );
        let database = fixture.config.storage.database;
        for sequence in 0..WAL_REPLAY_WINDOW_BATCHES as u64 {
            // The fixture's operation ids only ever set bytes 0..8, so a marker in
            // the last byte keeps every synthetic id distinct from a real one: a
            // collision would replace the published entry in place instead of
            // pushing it out.
            let mut operation = [0xA5u8; 16];
            operation[..8].copy_from_slice(&sequence.to_be_bytes());
            publisher.recent.push(RecentReceipt {
                id: BatchId {
                    database,
                    operation,
                },
                version: BatchVersion {
                    epoch: core.epoch,
                    sequence: sequence + 100,
                },
                digest: PayloadDigest([1; 32]),
            });
        }
        assert!(
            publisher.recent.get(&published).is_none(),
            "the window must have pushed the published receipt out",
        );
    }
    assert_eq!(
        core.wait_retry(published, version(&core, 0), Instant::now() + WAIT)
            .unwrap(),
        Retried::Reapply,
    );
    assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
    drop(core);
    db.shutdown().unwrap();
}

/// The allocated-but-unpublished crash cut, end to end through the real caller,
/// owner worker and leased writer.
///
/// `v1` publishes key 2 = 21; a store at the next cut drains the stream, so the
/// replay start sits at `v1`'s end. The next batch (key 2 = 22, key 3 = 33) is
/// then allocated — its member frame is written at the owner and its reservation
/// is recorded — and publication is paused, exactly the state a snapshot can
/// observe: allocated, not applied. Three assertions follow from that state, and
/// the first two are the ones RustReview's re-review asked for:
///
/// 1. the start a store would record is still the drained tail. Negative control:
///    if the reservation published only the tail, `fronts()` would be the *end* of
///    the unpublished frame and the assertion below fails with that value;
/// 2. the batch is not in the index at all: at the table's publication watermark,
///    key 2 still resolves to `v1` and the new key 3 has no position. A *value*
///    read is not used while parked — owner 0's worker is the paused one, so it
///    would wait on it — and a typed read error would be a **failure**, because it
///    would mean a position exists whose bytes cannot be read. The counter-control
///    is the release below: once published, both keys read back as the new values,
///    so "not exposed" is distinguishable from "exposed and unreadable";
/// 3. after the crash cut (no shutdown checkpoint) the published values are
///    intact and both published batches are replayed, exactly once each, because
///    no checkpoint ran. Version B extends across a contiguous surviving prefix,
///    so a durable batch is replayed rather than hidden; "never exposed" below
///    durability (the cut-prefix discard) is S5/S6's and covered by
///    `durable_cut_tests`.
///
/// The typed "value evicted during read" error belongs to a different state: an
/// acknowledged, published position whose bytes are gone (native-tier page-cache
/// loss, replay stopping at the first bad frame). This test must never see it.
#[test]
fn an_allocated_but_unpublished_batch_stays_in_the_recorded_replay_window() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let core = Arc::clone(&db.core);
    let partition = 0;
    let owner = *core
        .partition_indexes
        .get(&partition)
        .expect("the owner set serves partition 0");
    assert_eq!(
        db.write_batch(
            fixture.operation(partition, 1),
            vec![put(1, 11), put(2, 21)],
        )
        .unwrap(),
        version(&core, 0)
    );
    core.clients[owner]
        .control_handle()
        .drain_to(version(&core, 1))
        .unwrap();
    let stored = replay_start(&core, owner, partition);
    assert!(
        stored > 0,
        "a drained stream starts replay at its tail, not at zero",
    );

    // Allocate the next batch and stop it inside publication: the reservation is
    // recorded when `submit_leased_group` returns, publication runs later.
    let (hook, entered, resume) = gate();
    core.test_hooks.lock().before_publish = Some(hook);
    let late = spawn_write(
        &core,
        fixture.operation(partition, 2),
        vec![put(2, 22), put(3, 33)],
    );
    entered.recv_timeout(WAIT).unwrap();
    assert_eq!(core.published_next(), 1, "nothing may publish while paused");
    assert_eq!(
        replay_start(&core, owner, partition),
        stored,
        "a store taken now would replay from the unpublished frame's end and lose it",
    );
    // The index must not mention the batch: key 2 still resolves to `v1` and key 3
    // has no position at all. The probe goes to the table at its publication
    // watermark (`current()`, advanced together with the applied front) rather
    // than a value read, so the check does not depend on owner 0's worker, which is
    // parked inside `before_publish` (a value fetch might need it; not relied on
    // either way). `last_processed()` is
    // the wrong cut here: it is `min(durable, applied, latch)` and can sit below an
    // already-published batch, which would read an older version or none.
    let paused_cut = core.table.current();
    // `get` is exclusive at its cut, so `current()` must be the *next* unpublished
    // version for `v1` to be visible; pin that so an off-by-one fails here and not
    // as a confusing missing position below.
    assert_eq!(
        paused_cut,
        version(&core, 1),
        "current() must be the next unpublished version",
    );
    let overwritten = core
        .table
        .get(KeySpace(0), &2u32.to_be_bytes(), paused_cut)
        .unwrap()
        .expect("the published batch keeps its position");
    assert_eq!(
        overwritten.batch,
        version(&core, 0),
        "an unpublished batch must not replace the older position",
    );
    assert!(
        core.table
            .get(KeySpace(0), &3u32.to_be_bytes(), paused_cut)
            .unwrap()
            .is_none(),
        "an unpublished new key must have no position",
    );

    resume.send(()).unwrap();
    assert_eq!(late.recv_timeout(WAIT).unwrap().unwrap(), version(&core, 1));
    // Counter-control: publication makes the same two reads answer the new values.
    assert_eq!(value(&mut db, 2), Some(vec![22; 32]));
    assert_eq!(value(&mut db, 3), Some(vec![33; 32]));

    // Crash: the durable image is all that is left, no shutdown checkpoint.
    core.test_hooks.lock().skip_shutdown_checkpoint = true;
    drop(core);
    db.shutdown().unwrap();
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 1), Some(vec![11; 32]));
    assert_eq!(value(&mut reopened, 2), Some(vec![22; 32]));
    assert_eq!(value(&mut reopened, 3), Some(vec![33; 32]));
    assert_eq!(
        reopened.recovery_stats().tail_batches,
        2,
        "no checkpoint ran, so both published batches are replayed from the owner WAL",
    );
    reopened.shutdown().unwrap();
}

/// The replay start the snapshot side reads for one owner stream: `fronts()`
/// carries every stream's `replay_from`, and a worker publishes its real
/// partition id, so the owner's entry is found by partition, never by dense slot
/// index.
fn replay_start(core: &WriteCore, owner: usize, partition: u32) -> u64 {
    core.clients[owner]
        .allocation_fronts()
        .into_iter()
        .find(|front| front.partition == partition)
        .expect("every served partition has a published front")
        .replay_from
}
