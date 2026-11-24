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
