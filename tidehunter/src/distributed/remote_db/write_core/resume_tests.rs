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
/// fail-stop: the step that runs next observes the lapse, the write that
/// triggered the resume is never acknowledged, no later write is admitted, and
/// the reopened database serves exactly the acknowledged prefix.
///
/// The step seam lapses the same stamp the production renewer would repair, and
/// `lapse_for_test` keeps it lapsed, so the assertion never races the renewer
/// thread or depends on a clock.
#[test]
fn lease_lapse_between_resume_steps_fail_stops_and_recovers_a_prefix() {
    use super::super::pipeline_tests::Fixture;
    use crate::key_shape::{KeyShape, KeyType};
    // The default fixture geometry is one 4 KiB frame per extent, so every write
    // rolls over and the resume would probe a preleased extent. A 64 KiB extent
    // keeps the break on one open extent, which is what this test measures.
    let fixture = Fixture::new_with_config(
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |config| {
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 128 * 1024;
        },
    );
    let mut db = fixture.open();
    // One acknowledged batch on the lapsed owner: the reopen has a prefix to
    // compare against, and the worker has an open extent the resume can probe.
    let acknowledged = batch(&fixture, 1, 1);
    db.write_batch(acknowledged, vec![put(1, 1), put(2, 2)])
        .expect("the first write is acknowledged before the lapse");
    // Arm the lapse at the boundary after the extent probe, and lose the queue
    // of the next submission: that submission enters the resume, probes the
    // owner, and only then meets the lapsed lease.
    let validity = db.lease.validity(1);
    let mut client = db.core.clients[1].client.lock();
    client.leased_test_lapse_lease_after_step(3, validity);
    client.leased_test_fail_submit(libc::ENODEV);
    drop(client);
    let error = db
        .write_batch(batch(&fixture, 1, 2), vec![put(3, 3), put(4, 4)])
        .expect_err("no write is acknowledged after the lapse");
    assert!(
        error.uncertain_version.is_some(),
        "the unacknowledged batch is reported as uncertain: {error}"
    );
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
        db.write_batch(batch(&fixture, 1, 3), vec![put(5, 5)])
            .is_err(),
        "a write after the lapse is refused"
    );
    let client = db.core.clients[1].client.lock();
    assert!(
        !client.leased_test_resume_targets().is_empty(),
        "the resume probed an extent before the lapse"
    );
    let steps = client.leased_test_resume_steps();
    assert!(
        (2..7).contains(&steps),
        "the lapse must stop the resume between its steps, saw {steps} of 7"
    );
    drop(client);
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
    // the unacknowledged one is absent as a whole, and the database accepts
    // new writes again.
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 1), Some(vec![1; 32]));
    assert_eq!(value(&mut reopened, 2), Some(vec![2; 32]));
    for key in [3u32, 4, 5] {
        assert_eq!(
            value(&mut reopened, key),
            None,
            "key {key} of an unacknowledged batch is not served"
        );
    }
    reopened
        .write_batch(batch(&fixture, 1, 9), vec![put(6, 6)])
        .expect("the reopened database accepts a new write");
    assert_eq!(value(&mut reopened, 6), Some(vec![6; 32]));
}
