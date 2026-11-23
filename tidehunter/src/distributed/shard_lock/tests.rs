use super::*;
use crate::distributed::test_bounds::PROGRESS_CAP;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[test]
fn routing_hash_is_deterministic_and_spreads_sequential_inputs() {
    let hash = |n: u64| {
        StableHasher::new()
            .bytes(&[3])
            .bytes(&n.to_le_bytes())
            .finish()
    };
    assert_eq!(hash(7), hash(7));
    assert_ne!(hash(0), hash(1));
    let mut buckets = [0usize; 16];
    for n in 0..4096 {
        buckets[(hash(n) & 15) as usize] += 1;
    }
    // Sequential cell ids must not pile on a few shards.
    assert!(buckets.iter().all(|&count| (192..=320).contains(&count)));
}

#[test]
fn uncontended_acquisitions_record_no_wait_and_contention_records_wait() {
    let lock = Arc::new(ShardMutex::new(0u32));
    drop(lock.lock());
    drop(lock.lock());
    let stats = lock.stats();
    assert_eq!((stats.acquisitions, stats.contended), (2, 0));
    assert!(stats.hold_samples >= 1, "every hold is timed");

    let holder = lock.lock();
    let released = Arc::new(AtomicBool::new(false));
    let waiter = {
        let lock = Arc::clone(&lock);
        let released = Arc::clone(&released);
        std::thread::spawn(move || {
            let _guard = lock.lock();
            assert!(released.load(Ordering::SeqCst));
        })
    };
    // Progress wait: the waiter counts its blocked entry after it starts the
    // wait timer, so observing the count proves the acquisition will block
    // until the holder releases.
    let blocked = Instant::now() + PROGRESS_CAP;
    while lock.blocked_waits() == 0 {
        assert!(
            Instant::now() < blocked,
            "waiter never queued on the contended lock"
        );
        std::thread::yield_now();
    }
    released.store(true, Ordering::SeqCst);
    drop(holder);
    waiter.join().unwrap();
    let stats = lock.stats();
    assert_eq!(stats.contended, 1, "one acquisition waited on the holder");
    assert_eq!(
        stats.acquisitions, 4,
        "two plain locks, the holder and the waiter"
    );
    assert!(
        stats.wait_ns > 0,
        "the wait covers the interval from the queue signal"
    );
}
