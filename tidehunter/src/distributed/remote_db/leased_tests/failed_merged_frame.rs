//! One merged frame that fails after a partial write marks every command unknown.

use super::*;

#[test]
fn leased_worker_failed_merged_frame_marks_every_command_unknown() {
    for result in [-libc::EIO, 2048] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.leased_test_fail_write(result);
        let (worker, failure) = worker(&fixture, client);
        let guard = worker.client.lock();
        let batches = [fixture.batch(0), fixture.batch(1), fixture.batch(2)];
        let replies: Vec<_> = batches
            .iter()
            .map(|batch| worker.enqueue(1, vec![batch.clone()]).unwrap())
            .collect();
        drop(guard);
        for (batch, reply) in batches.iter().zip(replies) {
            let error = reply
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap_err();
            assert_eq!(error.uncertain_version, Some(batch.version()));
        }
        drop(worker);
        assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
        assert_eq!(fixture.metrics.snapshot().counts.leased_write_samples, 0);
        // Acquire observes the failure of the one merged frame.
        assert!(failure.load(Ordering::Acquire));
    }
}
