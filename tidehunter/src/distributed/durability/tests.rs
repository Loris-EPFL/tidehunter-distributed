//! Behavioral tests of the shared frontier/syncer and of both tier contracts, plus a
//! seeded crash model of the recovery invariant (checkpoints reference only synced
//! positions). Timing assumptions exist only for negative checks ("has not happened after
//! 100 ms"); every positive check waits on a channel or a frontier with a 5 s limit.

use super::frontier::{
    FlushTarget, Frontier, FrontierConfig, FrontierError, SyncPolicy, Syncer, flush_all,
};
use super::{DurabilityConfig, DurabilityTier, ReceiptDurability, native};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const SETTLE: Duration = Duration::from_millis(100);
const LIMIT: Duration = Duration::from_secs(5);

fn soon() -> Option<Instant> {
    Some(Instant::now() + LIMIT)
}

/// Syncs only on demand (flush, synchronous wait, stop).
fn lazy(start: u64, max_pending: usize) -> FrontierConfig {
    FrontierConfig {
        start,
        max_pending,
        policy: SyncPolicy {
            fragment_bytes: u64::MAX,
            max_unsynced_age_ms: None,
        },
    }
}

/// A syncer whose every fsync blocks until the test releases it.
struct Stalled {
    _syncer: Syncer,
    /// Receives the target of each fsync when it begins.
    entered: Receiver<u64>,
    /// One message lets one fsync return `Ok`.
    release: Sender<()>,
}

fn stalled(frontier: &Arc<Frontier>) -> Stalled {
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let syncer = Syncer::spawn("test-syncer", Arc::clone(frontier), move |end| {
        entered_tx.send(end).ok();
        // A dropped `release` sender lets shutdown proceed.
        release_rx.recv().ok();
        Ok(())
    })
    .unwrap();
    Stalled {
        _syncer: syncer,
        entered,
        release,
    }
}

fn recording(frontier: &Arc<Frontier>) -> (Syncer, Arc<Mutex<Vec<u64>>>) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&calls);
    let syncer = Syncer::spawn("test-syncer", Arc::clone(frontier), move |end| {
        sink.lock().unwrap().push(end);
        Ok(())
    })
    .unwrap();
    (syncer, calls)
}

fn poll_until(mut condition: impl FnMut() -> bool) {
    let limit = Instant::now() + LIMIT;
    while !condition() {
        assert!(Instant::now() < limit, "condition not reached in 5 s");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn out_of_order_completions_advance_only_contiguously() {
    let f = Frontier::with_config(lazy(1000, 8)).unwrap();
    f.complete(1030, 1040, 1).unwrap();
    f.complete(1010, 1020, 2).unwrap();
    assert_eq!(f.appended(), 1000);
    assert_eq!(f.stats().pending_completions, 2);

    // The head completion absorbs the 1010 range but stops at the gap before 1030.
    f.complete(1000, 1010, 4).unwrap();
    assert_eq!(f.appended(), 1020);
    assert_eq!(f.points().0.batches, 6);

    f.complete(1020, 1030, 8).unwrap();
    assert_eq!(f.appended(), 1040);
    let (appended, synced) = f.points();
    assert_eq!((appended.end, appended.batches), (1040, 15));
    assert_eq!((synced.end, synced.batches), (1000, 0));
    assert_eq!(f.stats().pending_completions, 0);
    assert_eq!(f.stats().peak_pending_completions, 2);
}

#[test]
fn invalid_ranges_are_rejected_without_changing_the_frontier() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    f.complete(10, 20, 1).unwrap();
    assert_eq!(
        f.complete(10, 20, 1),
        Err(FrontierError::Overlap),
        "duplicate"
    );
    assert_eq!(
        f.complete(15, 25, 1),
        Err(FrontierError::Overlap),
        "overlaps next"
    );
    assert_eq!(
        f.complete(5, 12, 1),
        Err(FrontierError::Overlap),
        "overlaps previous"
    );
    assert_eq!(f.complete(30, 30, 1), Err(FrontierError::EmptyRange));
    assert_eq!(f.complete(0, 10, 1), Ok(()));
    assert_eq!(f.appended(), 20);
    assert_eq!(
        f.complete(0, 10, 1),
        Err(FrontierError::Overlap),
        "below frontier"
    );
    assert_eq!(f.publish_appended(20, 1), Err(FrontierError::Overlap));
    assert_eq!(f.appended(), 20);
}

#[test]
fn publish_appended_covers_the_range_from_the_frontier() {
    let f = Frontier::with_config(lazy(64, 4)).unwrap();
    f.publish_appended(100, 3).unwrap();
    f.publish_appended(160, 2).unwrap();
    let (appended, _) = f.points();
    assert_eq!((appended.end, appended.batches), (160, 5));
}

#[test]
fn invalid_frontier_config_is_rejected() {
    assert_eq!(
        Frontier::with_config(lazy(0, 0)).err(),
        Some(FrontierError::InvalidConfig)
    );
    let mut config = lazy(0, 1);
    config.policy.fragment_bytes = 0;
    assert_eq!(
        Frontier::with_config(config).err(),
        Some(FrontierError::InvalidConfig)
    );
}

#[test]
fn synchronous_ack_never_precedes_the_covering_fsync() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let stall = stalled(&f);
    f.complete(0, 100, 1).unwrap();

    let (done_tx, done) = mpsc::channel();
    let waiter = {
        let f = Arc::clone(&f);
        thread::spawn(move || {
            done_tx
                .send(DurabilityTier::Synchronous.acknowledge(&f, 100, soon()))
                .unwrap();
        })
    };
    // The waiter alone started the fsync (lazy policy), and the fsync covers the batch.
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(100));
    assert!(
        done.recv_timeout(SETTLE).is_err(),
        "ack returned during the fsync"
    );
    assert_eq!(f.synced(), 0);

    stall.release.send(()).unwrap();
    assert_eq!(
        done.recv_timeout(LIMIT).unwrap(),
        Ok(ReceiptDurability::Synced)
    );
    assert_eq!(f.synced(), 100);
    waiter.join().unwrap();
}

#[test]
fn synchronous_ack_waits_for_earlier_batches_to_complete() {
    // Position 50..100 is complete, 0..50 is not: nothing is appended, so nothing is synced.
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let (syncer, calls) = recording(&f);
    f.complete(50, 100, 1).unwrap();
    assert_eq!(
        DurabilityTier::Synchronous.acknowledge(&f, 100, Some(Instant::now() + SETTLE)),
        Err(FrontierError::Timeout)
    );
    assert!(
        calls.lock().unwrap().is_empty(),
        "synced a range with a hole"
    );
    f.complete(0, 50, 1).unwrap();
    assert_eq!(
        DurabilityTier::Synchronous.acknowledge(&f, 100, soon()),
        Ok(ReceiptDurability::Synced)
    );
    assert_eq!(*calls.lock().unwrap(), vec![100]);
    syncer.shutdown().unwrap();
}

#[test]
fn native_ack_does_not_wait_for_fsync() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let stall = stalled(&f);

    // No fsync is due at all (lazy cadence): the ack is still immediate.
    f.complete(0, 100, 1).unwrap();
    assert_eq!(
        DurabilityTier::Native.acknowledge(&f, 100, Some(Instant::now())),
        Ok(ReceiptDurability::Appended)
    );
    assert_eq!(f.synced(), 0);

    // An fsync is stalled in flight: the next ack is still immediate.
    f.request_sync().unwrap();
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(100));
    f.complete(100, 200, 1).unwrap();
    assert_eq!(
        DurabilityTier::Native.acknowledge(&f, 200, Some(Instant::now())),
        Ok(ReceiptDurability::Appended)
    );
    assert_eq!(f.synced(), 0);
    assert_eq!(f.stats().synced_waits, 0);

    // An earlier position still unfinished: the native ack waits for it (contiguity).
    f.complete(300, 400, 1).unwrap();
    assert_eq!(
        DurabilityTier::Native.acknowledge(&f, 400, Some(Instant::now() + SETTLE)),
        Err(FrontierError::Timeout)
    );
    f.complete(200, 300, 1).unwrap();
    assert_eq!(
        DurabilityTier::Native.acknowledge(&f, 400, Some(Instant::now())),
        Ok(ReceiptDurability::Appended)
    );
    drop(stall.release);
}

#[test]
fn flush_waits_for_everything_appended_before_it() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let stall = stalled(&f);
    f.complete(0, 10, 1).unwrap();
    assert_eq!(
        DurabilityTier::Native.acknowledge(&f, 10, soon()),
        Ok(ReceiptDurability::Appended)
    );

    let first = {
        let f = Arc::clone(&f);
        thread::spawn(move || native::flush(&f, soon()))
    };
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(10));
    // `flush_waits` counts only waits that blocked. The first flusher may still be between
    // `begin_flush` and its wait; release the fsync only once it is parked.
    assert!(f.wait_parked_flush_waits(1, Instant::now() + LIMIT));

    // Acknowledged after the first flush began, before the second one begins.
    f.complete(10, 20, 1).unwrap();
    assert_eq!(
        DurabilityTier::Native.acknowledge(&f, 20, soon()),
        Ok(ReceiptDurability::Appended)
    );
    let ticket = f.begin_flush().unwrap();
    assert_eq!(ticket, 20);

    // The fsync of 0..10 finishes. The second flush must not be satisfied by it.
    stall.release.send(()).unwrap();
    assert_eq!(first.join().unwrap(), Ok(10));
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(20));
    assert_eq!(
        f.finish_flush(ticket, Some(Instant::now() + SETTLE)),
        Err(FrontierError::Timeout)
    );
    // The timed-out wait parked (count 2). Park the finishing wait (count 3) before the
    // fsync of 0..20 is released, for the same reason as the first one.
    let second = {
        let f = Arc::clone(&f);
        thread::spawn(move || f.finish_flush(ticket, soon()))
    };
    assert!(f.wait_parked_flush_waits(3, Instant::now() + LIMIT));
    stall.release.send(()).unwrap();
    assert_eq!(second.join().unwrap(), Ok(20));

    let stats = f.stats();
    assert_eq!(stats.flush_requests, 2);
    assert_eq!(stats.flush_waits, 3, "two finished waits and one timed out");
    assert_eq!(f.synced(), 20);
}

#[test]
fn flush_of_a_synced_owner_returns_without_waiting() {
    let f = Frontier::with_config(lazy(500, 8)).unwrap();
    let (syncer, calls) = recording(&f);
    assert_eq!(native::flush(&f, Some(Instant::now())), Ok(500));
    assert!(calls.lock().unwrap().is_empty());
    syncer.shutdown().unwrap();
}

#[test]
fn only_waits_that_park_are_counted() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    // Already reached: every kind returns from its lock-free check.
    assert_eq!(f.wait_appended(0, soon()), Ok(()));
    assert_eq!(f.wait_synced(0, soon()), Ok(()));
    assert_eq!(f.wait_flush(0, soon()), Ok(0));

    // Not reached: the wait parks until its deadline and is counted.
    assert_eq!(
        f.wait_appended(10, Some(Instant::now() + SETTLE)),
        Err(FrontierError::Timeout)
    );

    // Not reached, but the frontier already failed: the locked path returns before it
    // parks, so these calls did not block and are not counted.
    f.complete(0, 10, 1).unwrap();
    f.fail("test failure");
    assert_eq!(f.wait_appended(20, soon()), Err(FrontierError::Failed));
    assert_eq!(f.wait_synced(10, soon()), Err(FrontierError::Failed));
    assert_eq!(f.wait_flush(10, soon()), Err(FrontierError::Failed));

    let stats = f.stats();
    assert_eq!(
        (stats.appended_waits, stats.synced_waits, stats.flush_waits),
        (1, 0, 0)
    );
    assert!(stats.appended_wait_ns > 0);
    assert_eq!(
        (
            stats.synced_wait_ns,
            stats.flush_wait_ns,
            stats.flush_wait_max_ns
        ),
        (0, 0, 0)
    );
}

#[test]
fn one_fsync_covers_everything_completed_meanwhile() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let stall = stalled(&f);
    f.complete(0, 10, 1).unwrap();
    f.request_sync().unwrap();
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(10));

    // Nine more batches complete while the first fsync is stalled.
    for i in 1..10u64 {
        f.complete(i * 10, i * 10 + 10, 1).unwrap();
    }
    let ticket = f.begin_flush().unwrap();
    assert_eq!(ticket, 100);
    stall.release.send(()).unwrap();
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(100));
    stall.release.send(()).unwrap();
    assert_eq!(f.finish_flush(ticket, soon()), Ok(100));

    let stats = f.stats();
    assert_eq!(stats.fsync_calls, 2);
    assert_eq!(stats.fsync_batches, 10);
    assert_eq!(stats.fsync_bytes, 100);
    assert_eq!(stats.max_batches_per_fsync, 9);
    assert!((stats.batches_per_fsync() - 5.0).abs() < f64::EPSILON);
    assert_eq!(
        stats.peak_lag_bytes, 90,
        "lag at the start of the second fsync"
    );
    assert_eq!((stats.lag_bytes, stats.lag_batches), (0, 0));
    assert_eq!(stats.lag_age, Duration::ZERO);
}

#[test]
fn lag_counters_report_unsynced_bytes_batches_and_age() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    f.complete(0, 30, 2).unwrap();
    thread::sleep(Duration::from_millis(5));
    let stats = f.stats();
    assert_eq!((stats.lag_bytes, stats.lag_batches), (30, 2));
    assert!(stats.lag_age >= Duration::from_millis(5));
}

#[test]
fn native_cadence_syncs_when_a_fragment_is_finalized_and_has_no_age_bound() {
    let policy = SyncPolicy::native(100);
    assert_eq!(policy.max_unsynced_age(), None);
    let f = Frontier::with_config(FrontierConfig {
        start: 0,
        max_pending: 8,
        policy,
    })
    .unwrap();
    let (syncer, calls) = recording(&f);
    f.complete(0, 50, 1).unwrap();
    thread::sleep(SETTLE);
    assert_eq!(
        f.synced(),
        0,
        "inside the first fragment nothing is due, however old"
    );
    f.complete(50, 130, 1).unwrap();
    // The boundary at 100 was crossed. One fsync covers the whole appended frontier.
    poll_until(|| f.synced() == 130);
    assert_eq!(*calls.lock().unwrap(), vec![130]);
    f.complete(130, 190, 1).unwrap();
    thread::sleep(SETTLE);
    assert_eq!(f.synced(), 130, "the open fragment stays unsynced");
    syncer.shutdown().unwrap();
}

#[test]
fn opt_in_age_bound_syncs_an_open_fragment() {
    let f = Frontier::with_config(FrontierConfig {
        start: 0,
        max_pending: 8,
        policy: SyncPolicy {
            fragment_bytes: u64::MAX,
            max_unsynced_age_ms: Some(20),
        },
    })
    .unwrap();
    let (syncer, calls) = recording(&f);
    f.complete(0, 10, 1).unwrap();
    poll_until(|| f.synced() == 10);
    assert_eq!(*calls.lock().unwrap(), vec![10]);
    syncer.shutdown().unwrap();
}

#[test]
fn checkpoint_capture_requests_a_sync_and_cuts_at_the_synced_frontier() {
    // Native cadence, open fragment: nothing is synced until the capture asks.
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let (syncer, _) = recording(&f);
    f.complete(0, 40, 2).unwrap();
    thread::sleep(SETTLE);
    assert_eq!(f.synced(), 0);
    let ticket = f.begin_flush().unwrap();
    let cut = f.finish_flush(ticket, soon()).unwrap();
    assert!(cut >= ticket && cut == f.synced());
    assert_eq!(cut, 40);
    syncer.shutdown().unwrap();
}

#[test]
fn shutdown_drains_with_one_final_fsync() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let (syncer, calls) = recording(&f);
    f.complete(0, 64, 2).unwrap();
    f.complete(100, 164, 1).unwrap(); // behind a hole: never appended, never synced
    syncer.shutdown().unwrap();
    assert_eq!(f.synced(), 64);
    assert_eq!(*calls.lock().unwrap(), vec![64]);
    assert_eq!(f.complete(64, 100, 1), Err(FrontierError::Stopped));
    assert_eq!(f.wait_synced(64, Some(Instant::now())), Ok(()));
    assert_eq!(
        f.wait_synced(100, Some(Instant::now())),
        Err(FrontierError::Stopped)
    );
}

#[test]
fn dropping_the_syncer_drains_too() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let (syncer, calls) = recording(&f);
    f.complete(0, 8, 1).unwrap();
    drop(syncer);
    assert_eq!(f.synced(), 8);
    assert_eq!(*calls.lock().unwrap(), vec![8]);
}

#[test]
fn a_frontier_accepts_one_syncer() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let (syncer, _) = recording(&f);
    let second = Syncer::spawn("second", Arc::clone(&f), |_| Ok(()));
    assert_eq!(
        second.err().map(|e| e.kind()),
        Some(io::ErrorKind::AlreadyExists)
    );
    syncer.shutdown().unwrap();
}

#[test]
fn full_completion_buffer_blocks_non_head_writers_only() {
    let f = Frontier::with_config(lazy(0, 2)).unwrap();
    f.complete(10, 20, 1).unwrap();
    f.complete(30, 40, 1).unwrap();

    let (done_tx, done) = mpsc::channel();
    let blocked = {
        let f = Arc::clone(&f);
        thread::spawn(move || done_tx.send(f.complete(50, 60, 1)).unwrap())
    };
    assert!(
        done.recv_timeout(SETTLE).is_err(),
        "buffer bound was exceeded"
    );
    assert_eq!(f.stats().pending_completions, 2);

    // The head completion needs no slot even though the buffer is full, and it drains one.
    f.complete(0, 10, 1).unwrap();
    assert_eq!(f.appended(), 20);
    assert_eq!(done.recv_timeout(LIMIT).unwrap(), Ok(()));
    blocked.join().unwrap();
    let stats = f.stats();
    assert_eq!(stats.completion_full_waits, 1);
    assert_eq!(stats.peak_pending_completions, 2);
}

#[test]
fn stop_and_failure_release_blocked_writers() {
    for fail in [false, true] {
        let f = Frontier::with_config(lazy(0, 1)).unwrap();
        f.complete(10, 20, 1).unwrap();
        let (done_tx, done) = mpsc::channel();
        let blocked = {
            let f = Arc::clone(&f);
            thread::spawn(move || done_tx.send(f.complete(30, 40, 1)).unwrap())
        };
        assert!(done.recv_timeout(SETTLE).is_err());
        if fail {
            f.fail("test");
        } else {
            f.stop();
        }
        let expected = if fail {
            FrontierError::Failed
        } else {
            FrontierError::Stopped
        };
        assert_eq!(done.recv_timeout(LIMIT).unwrap(), Err(expected));
        blocked.join().unwrap();
    }
}

#[test]
fn failed_fsync_fails_every_waiter_and_never_advances_synced() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let syncer = Syncer::spawn("failing", Arc::clone(&f), |_| {
        Err(io::Error::other("disk gone"))
    })
    .unwrap();
    f.complete(0, 10, 1).unwrap();
    assert_eq!(
        DurabilityTier::Synchronous.acknowledge(&f, 10, soon()),
        Err(FrontierError::Failed)
    );
    assert_eq!(f.synced(), 0);
    assert_eq!(f.complete(10, 20, 1), Err(FrontierError::Failed));
    assert_eq!(f.request_sync(), Err(FrontierError::Failed));
    assert!(f.failure().unwrap().contains("fsync through 10"));
    assert_eq!(f.stats().fsync_failures, 1);
    assert_eq!(syncer.shutdown(), Err(FrontierError::Failed));
}

#[test]
fn syncer_panic_fails_the_frontier() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let syncer = Syncer::spawn("panicking", Arc::clone(&f), |_| panic!("injected")).unwrap();
    f.complete(0, 10, 1).unwrap();
    assert_eq!(
        DurabilityTier::Synchronous.acknowledge(&f, 10, soon()),
        Err(FrontierError::Failed)
    );
    assert_eq!(syncer.shutdown(), Err(FrontierError::Failed));
}

#[test]
fn synced_wait_times_out_while_the_fsync_is_stalled() {
    let f = Frontier::with_config(lazy(0, 8)).unwrap();
    let stall = stalled(&f);
    f.complete(0, 10, 1).unwrap();
    assert_eq!(
        f.wait_synced(10, Some(Instant::now() + SETTLE)),
        Err(FrontierError::Timeout)
    );
    assert_eq!(stall.entered.recv_timeout(LIMIT), Ok(10));
    stall.release.send(()).unwrap();
    assert_eq!(f.wait_synced(10, soon()), Ok(()));
}

#[test]
fn flush_all_starts_every_owner_before_waiting_for_any() {
    let owners: Vec<_> = (0..3)
        .map(|_| Frontier::with_config(lazy(0, 8)).unwrap())
        .collect();
    let stalls: Vec<_> = owners.iter().map(stalled).collect();
    for (i, owner) in owners.iter().enumerate() {
        owner.complete(0, 10 * (i as u64 + 1), 1).unwrap();
    }
    let flushing = {
        let owners = owners.clone();
        thread::spawn(move || flush_all(&owners, soon()))
    };
    // All three fsyncs are in flight at once: none has been released yet.
    for (i, stall) in stalls.iter().enumerate() {
        assert_eq!(
            stall.entered.recv_timeout(LIMIT),
            Ok(10 * (i as u64 + 1)),
            "owner {i} was not started in parallel"
        );
    }
    assert!(!flushing.is_finished());
    for stall in &stalls {
        stall.release.send(()).unwrap();
    }
    let report = flushing.join().unwrap();
    assert_eq!(report.into_synced().unwrap(), vec![10, 20, 30]);
}

#[test]
fn flush_all_reports_each_owner_and_waits_for_the_healthy_ones() {
    let owners: Vec<_> = (0..3)
        .map(|_| Frontier::with_config(lazy(0, 8)).unwrap())
        .collect();
    let syncers: Vec<_> = owners.iter().map(|o| recording(o).0).collect();
    for owner in &owners {
        owner.complete(0, 10, 1).unwrap();
    }
    owners[1].fail("injected");
    let report = flush_all(&owners, soon());
    assert_eq!(report.owners[0], Ok(10));
    assert_eq!(report.owners[1], Err(FrontierError::Failed));
    assert_eq!(report.owners[2], Ok(10));
    let error = report.into_synced().unwrap_err();
    assert_eq!((error.owner, error.error), (1, FrontierError::Failed));
    for syncer in syncers {
        let _ = syncer.shutdown();
    }
}

#[test]
fn tier_labels_cohorts_and_parsing_are_stable() {
    assert_eq!(DurabilityTier::default(), DurabilityTier::Native);
    assert_eq!(DurabilityTier::Native.as_str(), "native");
    assert_eq!(DurabilityTier::Synchronous.as_str(), "synchronous");
    assert_eq!(
        DurabilityTier::Native.comparison_cohort(),
        "native_pagecache"
    );
    assert_eq!(
        DurabilityTier::Synchronous.comparison_cohort(),
        "value_wal_barrier_and_valkey_appendfsync_always"
    );
    assert_eq!(ReceiptDurability::Appended.as_str(), "appended");
    assert_eq!(ReceiptDurability::Synced.as_str(), "synced");
    assert_eq!(
        DurabilityTier::Native.receipt_durability(),
        ReceiptDurability::Appended
    );
    assert!(DurabilityTier::Native.ack_after_append());
    assert!(!DurabilityTier::Synchronous.ack_after_append());
    assert_eq!(
        "synchronous".parse::<DurabilityTier>(),
        Ok(DurabilityTier::Synchronous)
    );
    assert!("Native".parse::<DurabilityTier>().is_err());
    assert!("".parse::<DurabilityTier>().is_err());
}

#[test]
fn config_defaults_to_native_and_parses_from_yaml() {
    assert_eq!(DurabilityConfig::default().tier, DurabilityTier::Native);
    let empty: DurabilityConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(empty, DurabilityConfig::default());
    let parsed: DurabilityConfig = serde_yaml::from_str(
        "tier: synchronous\nmax_unsynced_age_ms: 5\nmax_pending_completions: 16\n",
    )
    .unwrap();
    assert_eq!(parsed.tier, DurabilityTier::Synchronous);
    let frontier = parsed.frontier_config(77, 4096);
    assert_eq!(
        (frontier.start, frontier.max_pending, frontier.policy),
        (
            77,
            16,
            SyncPolicy {
                fragment_bytes: 4096,
                max_unsynced_age_ms: Some(5)
            }
        )
    );
    // The default is native: fragment trigger only, no age bound.
    assert_eq!(
        DurabilityConfig::default().frontier_config(0, 4096).policy,
        SyncPolicy::native(4096)
    );
    assert!(serde_yaml::from_str::<DurabilityConfig>("tier: fast").is_err());
    assert!(serde_yaml::from_str::<DurabilityConfig>("unknown: 1").is_err());
}

#[test]
fn durable_cut_config_defaults_to_extended_durable_prefix() {
    use super::{CutRecovery, DurableCutConfig, NativeRecovery};
    let default = DurableCutConfig::default();
    assert_eq!(
        (
            default.recovery,
            default.cut,
            default.lag_ms,
            default.lag_bytes
        ),
        (
            NativeRecovery::DurablePrefix,
            CutRecovery::Extend,
            1000,
            64 * 1024 * 1024
        )
    );
    assert!(default.is_default() && default.validate().is_ok());
    let empty: DurableCutConfig = serde_yaml::from_str("{}").unwrap();
    assert_eq!(empty, default);
    let parsed: DurableCutConfig =
        serde_yaml::from_str("recovery: per_owner\ncut: strict\nlag_ms: 5\nlag_bytes: 7\n")
            .unwrap();
    assert_eq!(
        (parsed.recovery, parsed.cut, parsed.lag_ms, parsed.lag_bytes),
        (NativeRecovery::PerOwner, CutRecovery::Strict, 5, 7)
    );
    assert!(!parsed.is_default());
    for invalid in ["lag_ms: 0", "lag_bytes: 0", "lag_ms: 3600001"] {
        let config: DurableCutConfig = serde_yaml::from_str(invalid).unwrap();
        assert!(config.validate().is_err(), "{invalid}");
    }
    assert!(serde_yaml::from_str::<DurableCutConfig>("cut: loose").is_err());
    assert!(serde_yaml::from_str::<DurableCutConfig>("unknown: 1").is_err());
}

/// Threads write, complete out of order, and acknowledge under both tiers while a
/// checkpoint reader samples the synced frontier. The simulated device updates its durable
/// end inside the fsync callback, before the frontier publishes it.
#[test]
fn concurrent_writers_keep_acknowledgements_and_checkpoints_behind_the_device() {
    const WRITERS: u64 = 4;
    const PER_WRITER: u64 = 1500;
    let f = Frontier::with_config(FrontierConfig {
        start: 0,
        max_pending: 16,
        policy: SyncPolicy {
            fragment_bytes: 512,
            max_unsynced_age_ms: Some(1),
        },
    })
    .unwrap();
    let device = Arc::new(AtomicU64::new(0));
    let syncer = {
        let device = Arc::clone(&device);
        Syncer::spawn("device", Arc::clone(&f), move |end| {
            thread::yield_now();
            device.fetch_max(end, Ordering::SeqCst);
            Ok(())
        })
        .unwrap()
    };
    let next = Arc::new(AtomicU64::new(0));
    let checkpoints = {
        let (f, device) = (Arc::clone(&f), Arc::clone(&device));
        thread::spawn(move || {
            let mut last = 0;
            while f.stats().appended.batches < WRITERS * PER_WRITER {
                let synced = f.synced();
                assert!(
                    synced <= device.load(Ordering::SeqCst),
                    "checkpoint beyond device"
                );
                assert!(synced >= last, "synced frontier moved back");
                last = synced;
                thread::yield_now();
            }
        })
    };
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let (f, device, next) = (Arc::clone(&f), Arc::clone(&device), Arc::clone(&next));
            thread::spawn(move || {
                for i in 0..PER_WRITER {
                    let len = 1 + (i * 7 + w) % 61;
                    let start = next.fetch_add(len, Ordering::SeqCst);
                    if i % 5 == 0 {
                        thread::yield_now(); // reorder completions
                    }
                    f.complete(start, start + len, 1).unwrap();
                    match (w + i) % 3 {
                        0 => {
                            let tier = DurabilityTier::Synchronous;
                            let ack = tier.acknowledge(&f, start + len, soon()).unwrap();
                            assert_eq!(ack, ReceiptDurability::Synced);
                            assert!(
                                device.load(Ordering::SeqCst) >= start + len,
                                "synchronous ack before the device held the batch"
                            );
                        }
                        1 => {
                            let tier = DurabilityTier::Native;
                            tier.acknowledge(&f, start + len, soon()).unwrap();
                            assert!(f.appended() >= start + len);
                        }
                        _ => {}
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    checkpoints.join().unwrap();
    let total = next.load(Ordering::SeqCst);
    assert_eq!(native::flush(&f, soon()), Ok(total));
    assert_eq!(device.load(Ordering::SeqCst), total);
    let stats = f.stats();
    assert_eq!(stats.appended.batches, WRITERS * PER_WRITER);
    assert_eq!(stats.fsync_batches, WRITERS * PER_WRITER);
    assert!(
        stats.fsync_calls < WRITERS * PER_WRITER,
        "no group commit happened: {} fsyncs",
        stats.fsync_calls
    );
    syncer.shutdown().unwrap();
}

struct Xorshift(u64);

impl Xorshift {
    fn step(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.step() % bound
    }
}

/// Seeded single-threaded crash model of the recovery invariant. Steps interleave
/// allocation, out-of-order completion, fsync capture, device persistence, and publication
/// of the synced frontier (the device can persist before the frontier publishes, so a
/// crash between the two keeps the device ahead, never behind). A coordinator checkpoint
/// is the owner's synced frontier at the time it is taken. After a crash that discards
/// every unsynced byte, each checkpoint must lie inside the device and inside the
/// contiguous completed prefix. The control uses the appended frontier instead, as an
/// acknowledge-before-barrier checkpoint would, and must violate the invariant on some
/// seed; otherwise the check would be vacuous.
#[test]
fn seeded_crash_model_checkpoints_reference_only_synced_positions() {
    let mut control_violations = 0u32;
    for seed in 1..=300u64 {
        let mut rng = Xorshift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let f = Frontier::with_config(lazy(0, 64)).unwrap();
        let mut next = 0u64;
        let mut in_flight: Vec<(u64, u64)> = Vec::new();
        let mut capture = None;
        let mut device_durable = 0u64;
        let mut checkpoints = Vec::new();
        let mut control_checkpoints = Vec::new();
        for _ in 0..400 {
            match rng.below(6) {
                0 if in_flight.len() < 16 => {
                    let len = 1 + rng.below(64);
                    in_flight.push((next, next + len));
                    next += len;
                }
                1 | 2 if !in_flight.is_empty() => {
                    let pick = rng.below(in_flight.len() as u64) as usize;
                    let (start, end) = in_flight.swap_remove(pick);
                    f.complete(start, end, 1).unwrap();
                }
                3 if capture.is_none() && f.appended() > f.synced() => {
                    f.request_sync().unwrap();
                    capture = f.next_capture();
                    assert!(capture.is_some());
                }
                4 => {
                    if let Some(c) = &capture {
                        device_durable = device_durable.max(c.end); // fsync reached the device
                    }
                    if rng.below(2) == 0
                        && let Some(c) = capture.take()
                    {
                        device_durable = device_durable.max(c.end);
                        f.finish_sync(&c);
                    }
                }
                5 => {
                    checkpoints.push(f.synced());
                    control_checkpoints.push((f.appended(), device_durable));
                }
                _ => {}
            }
            assert!(f.synced() <= f.appended());
            assert!(
                f.synced() <= device_durable,
                "seed {seed}: synced beyond device"
            );
            let lowest_unfinished = in_flight.iter().map(|r| r.0).min().unwrap_or(next);
            assert!(
                f.appended() <= lowest_unfinished,
                "seed {seed}: hole below frontier"
            );
        }
        // Crash: the device keeps `device_durable` bytes. Every checkpoint taken from the
        // synced frontier lies inside them, and the bytes below it were completed.
        let lowest_unfinished = in_flight.iter().map(|r| r.0).min().unwrap_or(next);
        for checkpoint in checkpoints {
            assert!(
                checkpoint <= device_durable,
                "seed {seed}: checkpoint {checkpoint} lost"
            );
            assert!(
                checkpoint <= lowest_unfinished,
                "seed {seed}: checkpoint spans a hole"
            );
        }
        control_violations += u32::from(
            control_checkpoints
                .iter()
                .any(|(appended, durable)| appended > durable),
        );
    }
    assert!(
        control_violations > 0,
        "the appended-frontier control never violated the invariant"
    );
}
