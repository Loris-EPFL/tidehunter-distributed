use prometheus::{Histogram, HistogramOpts, IntCounter, Opts, Registry};
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Debug)]
pub(super) enum GroupBarrierError {
    LockPoisoned,
    TicketExhausted,
    Persistence(Arc<str>),
}

impl fmt::Display for GroupBarrierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LockPoisoned => formatter.write_str("native group barrier lock poisoned"),
            Self::TicketExhausted => formatter.write_str("native group barrier tickets exhausted"),
            Self::Persistence(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for GroupBarrierError {}

#[derive(Default)]
struct State {
    next_ticket: u64,
    covered: u64,
    running: bool,
    poisoned: Option<Arc<str>>,
}

#[derive(Debug, Serialize)]
pub(super) struct Snapshot {
    pub runs: u64,
    pub callers_covered: u64,
    pub max_group_size: u64,
    pub errors: u64,
}

/// A successful caller has a ticket covered by a run that started after entry.
/// One mutex protects the two ticket counters, run flag and sticky error. A shared
/// barrier cannot use independent shards. An actor would add a thread hand-off per
/// sync. The leader releases this lock before I/O; followers sleep on the condvar.
/// There is no caller queue, allocation, timer or batching delay on the success path.
pub(super) struct GroupBarrier {
    state: Mutex<State>,
    changed: Condvar,
    runs: IntCounter,
    callers: IntCounter,
    errors: IntCounter,
    group_size: Histogram,
    max_group_size: AtomicU64,
}

/// Wake all waiters if persistence unwinds before it can publish a result.
struct LeaderRun<'a> {
    barrier: &'a GroupBarrier,
    armed: bool,
}

impl Drop for LeaderRun<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let message = Arc::from("group barrier run panicked");
        let mut state = match self.barrier.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.running = false;
        state.poisoned = Some(message);
        self.barrier.errors.inc();
        self.barrier.changed.notify_all();
    }
}

impl GroupBarrier {
    pub fn new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Self::with_labels(registry, HashMap::new())
    }

    pub fn new_for_shard(registry: &Registry, shard: usize) -> Result<Self, prometheus::Error> {
        Self::with_labels(
            registry,
            HashMap::from([("shard".to_owned(), shard.to_string())]),
        )
    }

    fn with_labels(
        registry: &Registry,
        labels: HashMap<String, String>,
    ) -> Result<Self, prometheus::Error> {
        let runs = IntCounter::with_opts(
            Opts::new(
                "kv_research_native_group_barrier_runs_total",
                "Value-WAL group barrier runs, including failed runs",
            )
            .const_labels(labels.clone()),
        )?;
        let callers = IntCounter::with_opts(
            Opts::new(
                "kv_research_native_group_barrier_callers_total",
                "Callers covered by successful Value-WAL group barrier runs",
            )
            .const_labels(labels.clone()),
        )?;
        let errors = IntCounter::with_opts(
            Opts::new(
                "kv_research_native_group_barrier_errors_total",
                "Failed Value-WAL group barrier runs",
            )
            .const_labels(labels.clone()),
        )?;
        let group_size = Histogram::with_opts(
            HistogramOpts::new(
                "kv_research_native_group_barrier_group_size",
                "Callers covered per successful Value-WAL group barrier run",
            )
            .buckets(vec![1., 2., 3., 4., 6., 8., 12., 16., 24., 32., 64.])
            .const_labels(labels),
        )?;
        registry.register(Box::new(runs.clone()))?;
        registry.register(Box::new(callers.clone()))?;
        registry.register(Box::new(errors.clone()))?;
        registry.register(Box::new(group_size.clone()))?;
        Ok(Self {
            state: Mutex::default(),
            changed: Condvar::new(),
            runs,
            callers,
            errors,
            group_size,
            max_group_size: AtomicU64::new(0),
        })
    }

    pub fn wait_covering<E: fmt::Display>(
        &self,
        run: impl FnOnce() -> Result<(), E>,
    ) -> Result<(), GroupBarrierError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| GroupBarrierError::LockPoisoned)?;
        if let Some(message) = &state.poisoned {
            return Err(GroupBarrierError::Persistence(Arc::clone(message)));
        }
        let ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or(GroupBarrierError::TicketExhausted)?;
        state.next_ticket = ticket;
        // This loop ends when this ticket is covered or the caller becomes leader.
        // Spurious wakeups must re-check the predicate, with no retry count limit.
        let through = loop {
            if let Some(message) = &state.poisoned {
                return Err(GroupBarrierError::Persistence(Arc::clone(message)));
            }
            if state.covered >= ticket {
                return Ok(());
            }
            if !state.running {
                state.running = true;
                break state.next_ticket;
            }
            state = self
                .changed
                .wait(state)
                .map_err(|_| GroupBarrierError::LockPoisoned)?;
        };
        drop(state);
        self.runs.inc();
        let mut leader = LeaderRun {
            barrier: self,
            armed: true,
        };
        let result = run().map_err(|error| Arc::<str>::from(error.to_string()));
        leader.armed = false;
        let mut state = self
            .state
            .lock()
            .map_err(|_| GroupBarrierError::LockPoisoned)?;
        state.running = false;
        let result = match result {
            Ok(()) => {
                let group_size = through - state.covered;
                self.callers.inc_by(group_size);
                self.group_size.observe(group_size as f64);
                // Relaxed: this is a statistic, not a state publication fence.
                self.max_group_size.fetch_max(group_size, Ordering::Relaxed);
                state.covered = through;
                Ok(())
            }
            Err(message) => {
                self.errors.inc();
                state.poisoned = Some(Arc::clone(&message));
                Err(GroupBarrierError::Persistence(message))
            }
        };
        self.changed.notify_all();
        // The leader's ticket is <= through, so its own successful run covers it.
        result
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            runs: self.runs.get(),
            callers_covered: self.callers.get(),
            // Relaxed: statistics are independent of the ticket-state lock.
            max_group_size: self.max_group_size.load(Ordering::Relaxed),
            errors: self.errors.get(),
        }
    }
}

#[cfg(test)]
mod tests {
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
}
