//! W1 end to end: a leased write stream survives one owner's data-queue break
//! through the owner worker, across two fixture owners (design 11.2).
//!
//! Every wait follows an observable event (a caller reply, the worker's failure
//! flag) under a bounded cap; nothing here is a sleep or a latency premise.

use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::leased_tests::worker;
use crate::distributed::codec::CompleteBatch;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::DurableBatch;
use crate::distributed::tcp::leased_tests::{Fixture, owner_read};

/// Read the record `receipt` acknowledged for `batch` back through the owner.
fn assert_reads_back(fixture: &Fixture, batch: &CompleteBatch, receipt: &DurableBatch) {
    let reference = batch
        .record_reference(receipt.appended().address(), 0)
        .expect("record reference");
    assert_eq!(
        owner_read(fixture, &reference),
        batch.mutations(),
        "batch {} does not read back after the break",
        batch.version().sequence
    );
}

/// Write 100 batches across two owners, break the second owner's data queue
/// mid-stream, and require every caller reply to arrive with a receipt that
/// reads back in order.
#[test]
fn write_survives_one_owner_connection_break() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixtures = [
            Fixture::single_partition(tier, 64 * 1024),
            Fixture::single_partition(tier, 64 * 1024),
        ];
        let mut workers = Vec::with_capacity(fixtures.len());
        for fixture in &fixtures {
            // The production worker client enables the write resume; the local
            // fixture has no DMA reaper, so its detached queue drains inline.
            let mut client = fixture.writer();
            client.leased_test_resume(true);
            workers.push(worker(fixture, client));
        }
        let mut acknowledged = 0usize;
        for sequence in 0..100u64 {
            // Break the second owner's queue before the 51st acknowledgement:
            // its next submission loses its controller and the worker resumes.
            if sequence == 50 {
                workers[1]
                    .0
                    .client
                    .lock()
                    .leased_test_fail_submit(libc::ENODEV);
            }
            let owner = (sequence % 2) as usize;
            let batch = fixtures[owner].batch(sequence);
            let reply = workers[owner]
                .0
                .enqueue(1, vec![batch.clone()])
                .expect("the worker admits the batch");
            let receipts = reply
                .recv_timeout(Duration::from_secs(30))
                .expect("the caller reply arrives")
                .expect("the batch is durable");
            assert_eq!(receipts.len(), 1, "one batch, one receipt");
            assert_eq!(
                receipts[0].appended().version(),
                batch.version(),
                "the receipt names its own batch, in order"
            );
            assert_reads_back(&fixtures[owner], &batch, &receipts[0]);
            acknowledged += 1;
        }
        assert_eq!(acknowledged, 100, "every acknowledged batch has a receipt");
        let counts = fixtures[1].metrics.snapshot().counts;
        assert_eq!(
            counts.resume_attempts, 1,
            "the second owner resumes once, tier {tier:?}"
        );
        assert_eq!(
            counts.resume_refused, 0,
            "no resume was refused, tier {tier:?}"
        );
        for (owner, (worker, failure)) in workers.into_iter().enumerate() {
            let _ = worker;
            assert!(
                !failure.load(Ordering::Acquire),
                "owner {owner} fail-stopped, tier {tier:?}"
            );
        }
    }
}

/// Put `mutations` in one batch routed to `partition`.
fn batch(fixture: &super::super::pipeline_tests::Fixture, partition: u32, seed: u64) -> [u8; 16] {
    fixture.operation(partition, seed)
}

fn put(key: u32, value: u8) -> crate::distributed::codec::Mutation {
    use crate::key_shape::KeySpace;
    crate::distributed::codec::Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

fn value(db: &mut super::super::RemoteDatabase, key: u32) -> Option<Vec<u8>> {
    use crate::key_shape::KeySpace;
    db.get(KeySpace(0), &key.to_be_bytes())
        .expect("the reopened database serves reads")
        .map(|value| value.to_vec())
}

/// A coordinator lease that lapses between two steps of one resume is a
/// fail-stop: the step that runs next observes the lapse, no resend is
/// submitted after the lapse, the write whose completion reported the break is
/// never acknowledged, no later write is admitted, and the reopened database
/// serves the acknowledged prefix as a whole per batch.
///
/// The step seam lapses the same stamp the production renewer would repair, and
/// `lapse_for_test` keeps it lapsed, so the assertion never races the renewer
/// thread or depends on a clock. The retained state is controlled: the batch
/// that breaks is submitted and retained, and no durable-cut nudge adopts it
/// behind the worker's back, so it is still owed a resend when the resume runs
/// and the per-frame lease check is the only thing that can stop that rewrite.
#[test]
fn lease_lapse_between_resume_steps_fail_stops_and_recovers_a_prefix() {
    use super::super::pipeline_tests::Fixture;
    use crate::key_shape::{KeyShape, KeyType};
    // The default fixture geometry is one 4 KiB frame per extent, so every write
    // rolls over and the resume would probe a preleased extent. A 64 KiB extent
    // keeps the break on one open extent. A durable-cut lag longer than the test
    // keeps the coordinator from nudging the owner, so the owed frame keeps its
    // place above the owner's frontier.
    let fixture = Fixture::new_with_config(
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |config| {
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 128 * 1024;
            config.durable_cut.lag_ms = 3_600_000;
            config.durable_cut.lag_bytes = 1 << 40;
        },
    );
    let mut db = fixture.open();
    // One acknowledged batch on the lapsed owner: the reopen has a prefix to
    // compare against, and the worker has an open extent the resume can probe.
    let acknowledged = batch(&fixture, 1, 1);
    db.write_batch(acknowledged, vec![put(1, 1), put(2, 2)])
        .expect("the first write is acknowledged before the lapse");
    // The lapse lands after the probe of the first attempt. The next write's
    // completion reports a lost controller: its frame is submitted and retained,
    // so the resume owes it a resend and the rewrite step is where the lapsed
    // lease must stop it.
    let validity = db.lease.validity(1);
    {
        let mut client = db.core.clients[1].client.lock();
        client.leased_test_lapse_lease_after_step(3, validity);
        client.leased_test_fail_write(-libc::ENODEV);
    }
    let error = db
        .write_batch(batch(&fixture, 1, 2), vec![put(3, 3), put(4, 4)])
        .expect_err("a lost completion is never acknowledged");
    assert!(
        error.uncertain_version.is_some(),
        "the unacknowledged batch is reported as uncertain: {error}"
    );
    // The lapse stops the first attempt at the step that rechecks the lease, and
    // the episode's retry stops at its probe. The first attempt's exact boundary
    // is the rewrite step when the retained frame is still owed and the
    // declaration step when the owner's syncer already adopted it; both are
    // inside the episode, and no resend is ever submitted after the lapse.
    let client = db.core.clients[1].client.lock();
    assert_eq!(
        client.leased_test_lapse_at(),
        Some((0, 3)),
        "the lapse lands after the probe of the first attempt"
    );
    let attempts = client.leased_test_attempt_steps().to_vec();
    assert_eq!(
        attempts.len(),
        2,
        "the episode retried once, saw attempts {attempts:?}"
    );
    assert_eq!(
        attempts[1], 2,
        "the retry stops at its probe, saw attempts {attempts:?}"
    );
    assert!(
        (3..=4).contains(&attempts[0]),
        "the first attempt stops at the data or declaration step, saw {attempts:?}"
    );
    assert!(
        client.leased_test_rewrites().is_empty(),
        "no resend is submitted after the lapse"
    );
    assert!(
        !client.leased_test_resume_targets().is_empty(),
        "the resume probed an extent before the lapse"
    );
    drop(client);
    // The database fail-stops: no write is admitted and no read is served.
    assert!(
        db.core.ensure_healthy().is_err(),
        "the database fail-stops on the lapse"
    );
    assert!(
        db.get(crate::key_shape::KeySpace(0), &5u32.to_be_bytes())
            .is_err(),
        "a failed database serves no read"
    );
    assert!(
        db.write_batch(batch(&fixture, 1, 4), vec![put(7, 7)])
            .is_err(),
        "a write after the lapse is refused"
    );
    let first = db
        .core
        .first_failure
        .get()
        .expect("the fail-stop records its first cause");
    assert!(
        first.contains("stage=live_resume"),
        "the cause names the resume: {first}"
    );
    assert!(
        first.contains("coordinator lease lapsed on owner partition 1"),
        "the cause names the lapse: {first}"
    );
    drop(db);
    // The reopen replays the durable image: the acknowledged batch is served,
    // the refused batch is absent, and the batch whose completion was lost is
    // resolved as a whole (its keys appear together or not at all) because its
    // frame may have reached the owner without an acknowledgement.
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 1), Some(vec![1; 32]));
    assert_eq!(value(&mut reopened, 2), Some(vec![2; 32]));
    assert_eq!(
        value(&mut reopened, 3).is_some(),
        value(&mut reopened, 4).is_some(),
        "an unacknowledged batch is served as a whole or not at all"
    );
    assert_eq!(value(&mut reopened, 7), None, "the refused batch is absent");
    reopened
        .write_batch(batch(&fixture, 1, 9), vec![put(6, 6)])
        .expect("the reopened database accepts a new write");
    assert_eq!(value(&mut reopened, 6), Some(vec![6; 32]));
}
