// The bench target sets `harness = false`, so `#[test]` bodies are not
// type-checked by `cargo check --all-targets`; keep the parent imports inside
// each test so the module compiles clean without a module-level unused import.
#[test]
fn concurrent_prefix_completion() {
    use super::*;
    prefix_stress(100_000);
    let tracker = Tracker::new(2, false);
    tracker.reserve(); // H retains this initial credit across many wraps.
    thread::scope(|scope| {
        let target = &tracker;
        let producer = scope.spawn(move || {
            pin_cpu(1);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("reuse scenario runtime");
            runtime.block_on(async {
                let mut stats = SchedulingStats::default();
                for operation in 0..100_000 {
                    target.reserve_async(&mut stats).await;
                    let seq = target.allocate(operation);
                    target.complete(seq, seq.is_multiple_of(7));
                }
            });
        });
        let deadline = Instant::now() + Duration::from_secs(30);
        // Scheduling counter only, deliberately NOT a payload handoff. H must
        // acquire retirement through allocate(), not through a test barrier.
        while tracker.next.load(Ordering::Relaxed) < 20_001 {
            if Instant::now() >= deadline {
                eprintln!("EARLY_RESERVATION_STRESS_DEADLINE");
                std::process::exit(1);
            }
            thread::yield_now();
        }
        let held = tracker.allocate(u64::MAX);
        assert!(held >= 20_001, "early credit must survive many ring wraps");
        tracker.complete(held, false);
        // Join AFTER H's reuse, so it cannot hide the missing allocator edge.
        producer.join().expect("generation-wrap producer");
    });
    tracker.check_drained();
}

#[test]
fn parking_and_credit_cancellation() {
    use super::*;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        // Completion between the final check and first poll must not be lost.
        let changed = Notify::new();
        let mut checks = 0;
        let mut stats = SchedulingStats::default();
        assert!(
            wait_until(&changed, &mut stats, || {
                checks += 1;
                if checks == SPIN_LIMIT + 2 {
                    changed.notify_waiters();
                    false
                } else {
                    checks > SPIN_LIMIT + 2
                }
            })
            .await
        );
        assert_eq!(stats.spin_checks, SPIN_LIMIT as u64);
        assert_eq!(stats.parks, 0, "already-notified is not an actual park");
        assert_eq!(stats.wakeups, 1);
        assert_eq!(stats.spurious_wakeups, 0);

        let tracker = Arc::new(Tracker::new(1, false));
        for cancel in [true, false] {
            tracker.reserve();
            let held = tracker.allocate(u64::MAX);
            let target = Arc::clone(&tracker);
            let entered = Arc::new(Notify::new());
            let started = Arc::clone(&entered);
            let waiter = tokio::spawn(async move {
                started.notify_one();
                let mut stats = SchedulingStats::default();
                target.reserve_async(&mut stats).await;
                stats
            });
            // On one worker, the child cannot return control until its full-ring
            // wait actually parks. No sleep or scheduler-yield guess is needed.
            entered.notified().await;
            assert!(!waiter.is_finished());
            if cancel {
                waiter.abort();
                assert!(waiter.await.expect_err("cancelled waiter").is_cancelled());
                tracker.complete(held, true);
            } else {
                tracker.complete(held, false);
                let stats = waiter.await.expect("woken waiter");
                assert_eq!(stats.spin_checks, SPIN_LIMIT as u64);
                assert_eq!(stats.parks, 1);
                assert_eq!(stats.wakeups, 1);
                assert_eq!(stats.spurious_wakeups, 0);
                let next = tracker.allocate(u64::MAX);
                tracker.complete(next, false);
            }
            tracker.check_drained();
        }
    });
}

#[test]
fn targeted_slot_and_frontier_notifications() {
    use super::*;
    struct WakeCount(AtomicU64);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let tracker = Tracker::new(3, true);
    let sequences: Vec<_> = (0..3)
        .map(|_| {
            tracker.reserve();
            tracker.allocate(u64::MAX)
        })
        .collect();
    let counter = Arc::new(WakeCount(AtomicU64::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&counter));
    let mut context = std::task::Context::from_waker(&waker);
    let mut waiting = std::pin::pin!(tracker.slot_changed[0].notified());
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    tracker.complete(sequences[1], false);
    assert_eq!(
        counter.0.load(Ordering::Relaxed),
        0,
        "unrelated slot woke waiter"
    );
    tracker.complete(sequences[2], false);
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert!(waiting.as_mut().poll(&mut context).is_ready());
    assert!(!tracker.covers(sequences[2]), "held head must gate B");
    let mut frontier = std::pin::pin!(tracker.slot_changed[0].notified());
    assert!(frontier.as_mut().poll(&mut context).is_pending());
    tracker.complete(sequences[0], false);
    assert_eq!(
        counter.0.load(Ordering::Relaxed),
        2,
        "frontier crossing must wake B"
    );
    assert!(frontier.as_mut().poll(&mut context).is_ready());
    assert!(tracker.covers(sequences[2]));
    tracker.check_drained();
}

#[test]
fn concurrent_slot_reuse() {
    use super::*;
    let tracker = Tracker::new(64, false);
    thread::scope(|scope| {
        for worker in 0..4 {
            let tracker = &tracker;
            scope.spawn(move || {
                pin_cpu(worker + 1);
                // No round barrier: credit publication is the reuse handoff.
                for operation in 0..100_000 {
                    let started = Instant::now();
                    let mut reserved = false;
                    for attempt in 0..WAIT_LIMIT {
                        if tracker.try_reserve() {
                            reserved = true;
                            break;
                        }
                        if attempt & 1023 == 0 && started.elapsed() >= Duration::from_secs(30) {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                    if !reserved {
                        eprintln!(
                            "SLOT_REUSE_STRESS_DEADLINE prefix={} next={} credits={}",
                            tracker.prefix.load(Ordering::Acquire),
                            tracker.next.load(Ordering::Relaxed),
                            tracker.credits.load(Ordering::Relaxed)
                        );
                        std::process::exit(1);
                    }
                    if operation % 127 == 0 {
                        // Exercise a reservation held across other allocations.
                        thread::yield_now();
                    }
                    let seq = tracker.allocate((operation * 4 + worker) as u64);
                    tracker.complete(seq, seq.is_multiple_of(7));
                }
            });
        }
    });
    tracker.check_drained();
}
