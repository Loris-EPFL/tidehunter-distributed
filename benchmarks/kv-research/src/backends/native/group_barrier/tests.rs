use super::*;
use std::sync::{Barrier, mpsc};
use std::thread;
use std::time::Duration;

fn barrier() -> GroupBarrier {
    GroupBarrier::new(&Registry::new()).unwrap()
}

#[test]
fn one_caller_runs_one_barrier() {
    let barrier = barrier();
    barrier.wait_covering(|| Ok::<_, &str>(())).unwrap();
    let snapshot = barrier.snapshot();
    assert_eq!(snapshot.runs, 1);
    assert_eq!(snapshot.callers_covered, 1);
    assert_eq!(snapshot.max_group_size, 1);
    assert_eq!(snapshot.errors, 0);
}

#[test]
fn concurrent_callers_share_only_covering_runs() {
    let barrier = barrier();
    let start = Barrier::new(16);
    let sequence = AtomicU64::new(0);
    let runs = Mutex::new(Vec::new());
    let calls = Mutex::new(Vec::new());
    thread::scope(|scope| {
        for _ in 0..16 {
            scope.spawn(|| {
                start.wait();
                let entry = sequence.fetch_add(1, Ordering::SeqCst);
                barrier
                    .wait_covering(|| {
                        let begin = sequence.fetch_add(1, Ordering::SeqCst);
                        thread::sleep(Duration::from_millis(10));
                        let end = sequence.fetch_add(1, Ordering::SeqCst);
                        runs.lock().unwrap().push((begin, end));
                        Ok::<_, &str>(())
                    })
                    .unwrap();
                let exit = sequence.fetch_add(1, Ordering::SeqCst);
                calls.lock().unwrap().push((entry, exit));
            });
        }
    });
    let runs = runs.into_inner().unwrap();
    assert!(runs.len() < 16);
    for (entry, exit) in calls.into_inner().unwrap() {
        assert!(runs.iter().any(|&(begin, end)| entry < begin && end < exit));
    }
    assert_eq!(barrier.snapshot().callers_covered, 16);
    assert_eq!(barrier.group_size.get_sample_sum(), 16.0);
}

#[test]
fn failed_run_fails_waiters_and_later_calls_without_retry() {
    let barrier = barrier();
    let (started, running) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    thread::scope(|scope| {
        let leader_barrier = &barrier;
        scope.spawn(move || {
            leader_barrier
                .wait_covering(|| {
                    started.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                    Ok::<_, &str>(())
                })
                .unwrap();
        });
        running.recv_timeout(Duration::from_secs(10)).unwrap();
        for _ in 0..7 {
            scope.spawn(|| {
                let error = barrier
                    .wait_covering(|| Err::<(), _>("test sync failure"))
                    .unwrap_err();
                assert_eq!(error.to_string(), "test sync failure");
            });
        }
        // Hold the first run until all seven waiters need a later run.
        let mut enrolled = false;
        for _ in 0..1000 {
            if barrier.state.lock().unwrap().next_ticket == 8 {
                enrolled = true;
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        assert!(enrolled, "waiters did not enter the barrier");
        release.send(()).unwrap();
    });
    let error = barrier
        .wait_covering(|| -> Result<(), &str> { panic!("poison must prevent retry") })
        .unwrap_err();
    assert_eq!(error.to_string(), "test sync failure");
    assert_eq!(barrier.snapshot().runs, 2);
    assert_eq!(barrier.snapshot().errors, 1);
    assert_eq!(barrier.snapshot().callers_covered, 1);
}

#[test]
fn panicking_leader_wakes_waiters_and_poisons_later_calls() {
    let barrier = Arc::new(barrier());
    let (started, running) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let leader_barrier = Arc::clone(&barrier);
    let leader = thread::spawn(move || {
        leader_barrier.wait_covering(|| -> Result<(), &str> {
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(10)).unwrap();
            panic!("test leader panic");
        })
    });
    running.recv_timeout(Duration::from_secs(10)).unwrap();
    let (done, completed) = mpsc::sync_channel(3);
    let mut followers = Vec::with_capacity(3);
    for _ in 0..3 {
        let barrier = Arc::clone(&barrier);
        let done = done.clone();
        followers.push(thread::spawn(move || {
            let error = barrier
                .wait_covering(|| -> Result<(), &str> {
                    panic!("follower must not retry the panicked run")
                })
                .unwrap_err();
            done.send(error.to_string()).unwrap();
        }));
    }
    let mut enrolled = false;
    for _ in 0..1000 {
        if barrier.state.lock().unwrap().next_ticket == 4 {
            enrolled = true;
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(enrolled, "followers did not enter the barrier");
    release.send(()).unwrap();
    for _ in 0..3 {
        assert_eq!(
            completed.recv_timeout(Duration::from_secs(10)).unwrap(),
            "group barrier run panicked"
        );
    }
    for follower in followers {
        follower.join().unwrap();
    }
    assert!(leader.join().is_err());
    let error = barrier
        .wait_covering(|| -> Result<(), &str> { panic!("poison must prevent retry") })
        .unwrap_err();
    assert_eq!(error.to_string(), "group barrier run panicked");
    assert!(!barrier.state.lock().unwrap().running);
    assert_eq!(barrier.snapshot().runs, 1);
    assert_eq!(barrier.snapshot().errors, 1);
    assert_eq!(barrier.snapshot().callers_covered, 0);
}

#[test]
fn stress_has_no_lost_wakeups_and_counts_every_caller() {
    let barrier = Arc::new(barrier());
    let (done, completed) = mpsc::sync_channel(8);
    let mut threads = Vec::with_capacity(8);
    for _ in 0..8 {
        let barrier = Arc::clone(&barrier);
        let done = done.clone();
        threads.push(thread::spawn(move || {
            for _ in 0..200 {
                barrier.wait_covering(|| Ok::<_, &str>(())).unwrap();
            }
            done.send(()).unwrap();
        }));
    }
    for _ in 0..8 {
        completed.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let snapshot = barrier.snapshot();
    assert!(snapshot.runs <= 1600);
    assert_eq!(snapshot.callers_covered, 1600);
    assert_eq!(snapshot.errors, 0);
    assert_eq!(barrier.group_size.get_sample_sum(), 1600.0);
    assert_eq!(barrier.group_size.get_sample_count(), snapshot.runs);
}

#[test]
fn poisoned_mutex_and_ticket_exhaustion_return_errors() {
    let barrier = barrier();
    barrier.state.lock().unwrap().next_ticket = u64::MAX;
    assert!(matches!(
        barrier.wait_covering(|| Ok::<_, &str>(())),
        Err(GroupBarrierError::TicketExhausted)
    ));
    let _ = std::panic::catch_unwind(|| {
        let _guard = barrier.state.lock().unwrap();
        panic!("test mutex poison");
    });
    assert!(matches!(
        barrier.wait_covering(|| Ok::<_, &str>(())),
        Err(GroupBarrierError::LockPoisoned)
    ));
}

#[test]
fn duplicate_metric_registration_returns_error() {
    let registry = Registry::new();
    let _barrier = GroupBarrier::new(&registry).unwrap();
    assert!(GroupBarrier::new(&registry).is_err());
}
