use super::*;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

#[test]
fn routing_hash_is_deterministic_and_spreads_sequential_inputs() {
    let hash = |n: u64| StableHasher::new().byte(3).u64(n).finish();
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
fn shard_mask_accepts_only_powers_of_two() {
    assert_eq!(shard_mask(1), Some(0));
    assert_eq!(shard_mask(64), Some(63));
    assert_eq!(shard_mask(0), None);
    assert_eq!(shard_mask(48), None);
}

#[test]
fn uncontended_acquisitions_record_no_wait_and_contention_records_wait() {
    let lock = Arc::new(ShardRwLock::new(0u32));
    drop(lock.read());
    drop(lock.write());
    let stats = lock.stats();
    assert_eq!((stats.acquisitions, stats.contended), (2, 0));
    assert!(stats.hold_samples >= 1, "write holds are always sampled");

    let writer = lock.write();
    let released = Arc::new(AtomicBool::new(false));
    let waiter = {
        let lock = Arc::clone(&lock);
        let released = Arc::clone(&released);
        std::thread::spawn(move || {
            let _read = lock.read();
            assert!(released.load(Ordering::SeqCst));
        })
    };
    while lock.stats().acquisitions < 4 {
        std::thread::yield_now();
    }
    std::thread::sleep(Duration::from_millis(5));
    released.store(true, Ordering::SeqCst);
    drop(writer);
    waiter.join().unwrap();
    let stats = lock.stats();
    assert_eq!(stats.contended, 1);
    assert!(stats.wait_ns >= 1_000_000, "wait covers the blocked period");
}
