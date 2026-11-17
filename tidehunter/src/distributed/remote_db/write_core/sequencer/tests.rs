use super::*;
use std::sync::Barrier;
use std::thread;
use std::time::Duration;

struct Credits {
    available: Mutex<usize>,
    changed: Condvar,
}

impl Credits {
    fn new(capacity: usize) -> Self {
        Self {
            available: Mutex::new(capacity),
            changed: Condvar::new(),
        }
    }

    fn reserve(&self) -> Reservation<'_> {
        let mut available = self.available.lock();
        let deadline = Instant::now() + Duration::from_secs(5);
        while *available == 0 {
            assert!(
                !self
                    .changed
                    .wait_until(&mut available, deadline)
                    .timed_out(),
                "credit deadline"
            );
        }
        *available -= 1;
        Reservation {
            credits: self,
            claimed: false,
        }
    }

    fn release(&self, count: usize) {
        *self.available.lock() += count;
        self.changed.notify_all();
    }
}

struct Reservation<'a> {
    credits: &'a Credits,
    claimed: bool,
}
impl Reservation<'_> {
    fn allocate(mut self, sequencer: &Sequencer, operation: u64) -> u64 {
        self.claimed = true;
        sequencer.allocate(BatchId {
            database: [1; 16],
            operation: u128::from(operation).to_le_bytes(),
        })
    }
}
impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.claimed {
            self.credits.release(1);
        }
    }
}

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
}

fn publish(sequencer: &Sequencer, sequence: u64) {
    sequencer
        .transition(sequence, Resolution::Allocated, Resolution::Ready)
        .unwrap();
    sequencer
        .transition(sequence, Resolution::Ready, Resolution::Durable)
        .unwrap();
    sequencer
        .transition(sequence, Resolution::Durable, Resolution::Published)
        .unwrap();
}

fn outcome(sequencer: &Sequencer, sequence: u64) {
    let slot = sequencer.slot(sequence);
    let mut data = slot.data.lock();
    assert_eq!(slot.state.load(Ordering::Acquire) >> SHIFT, sequence);
    assert!(data.outcome.is_none());
    data.outcome = Some(Ok(version(sequence)));
    drop(data);
    slot.done.notify_all();
}

#[test]
fn prefix_stress() {
    for start in [0, 1 << 40] {
        prefix_stress_from(start);
    }
}

fn prefix_stress_from(start: u64) {
    const ROUNDS: u64 = 10_000;
    let sequencer = Sequencer::with_next(2, start);
    let credits = Credits::new(2);
    let round = Barrier::new(3);
    thread::scope(|scope| {
        for worker in 0..2 {
            let sequencer = &sequencer;
            let credits = &credits;
            let round = &round;
            scope.spawn(move || {
                for iteration in 0..ROUNDS {
                    round.wait();
                    let sequence = start + iteration * 2 + worker;
                    if sequence.is_multiple_of(7) {
                        sequencer
                            .transition(sequence, Resolution::Allocated, Resolution::Aborted)
                            .unwrap();
                    } else {
                        publish(sequencer, sequence);
                    }
                    outcome(sequencer, sequence);
                    credits.release(sequencer.complete_resolve(sequence));
                    round.wait();
                }
            });
        }
        for iteration in 0..ROUNDS {
            for worker in 0..2 {
                let sequence = credits
                    .reserve()
                    .allocate(&sequencer, iteration * 2 + worker);
                assert_eq!(sequence, start + iteration * 2 + worker);
                credits.release(sequencer.complete_release(sequence));
            }
            round.wait();
            round.wait();
            assert_eq!(sequencer.reuse_next(), start + iteration * 2 + 2);
            assert_eq!(*credits.available.lock(), 2);
        }
    });
    assert_eq!(sequencer.reuse_next(), sequencer.next());
}

#[test]
fn concurrent_prefix_completion() {
    for start in [0, 1 << 40] {
        concurrent_prefix_completion_from(start);
    }
}

fn concurrent_prefix_completion_from(start: u64) {
    let sequencer = Sequencer::with_next(2, start);
    let credits = Credits::new(2);
    let held = credits.reserve();
    thread::scope(|scope| {
        let producer = scope.spawn(|| {
            for operation in 0..10_000 {
                let sequence = credits.reserve().allocate(&sequencer, operation);
                publish(&sequencer, sequence);
                outcome(&sequencer, sequence);
                credits.release(sequencer.complete_resolve(sequence));
                credits.release(sequencer.complete_release(sequence));
            }
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        // Relaxed is deliberately not a metadata handoff. AcqRel allocation
        // must relay retirement to a caller holding an old admission grant.
        for attempt in 0..10_000_000 {
            if sequencer.next.load(Ordering::Relaxed) >= start + 2000 {
                break;
            }
            assert!(attempt + 1 < 10_000_000 && Instant::now() < deadline);
            thread::yield_now();
        }
        let sequence = held.allocate(&sequencer, u64::MAX);
        assert!(sequence >= start + 2000);
        publish(&sequencer, sequence);
        outcome(&sequencer, sequence);
        credits.release(sequencer.complete_resolve(sequence));
        credits.release(sequencer.complete_release(sequence));
        producer.join().unwrap();
    });
    assert_eq!(sequencer.reuse_next(), sequencer.next());
    assert_eq!(*credits.available.lock(), 2);
}

#[test]
fn concurrent_slot_reuse() {
    for start in [0, 1 << 40] {
        concurrent_slot_reuse_from(start);
    }
}

fn concurrent_slot_reuse_from(start: u64) {
    let sequencer = Sequencer::with_next(64, start);
    let credits = Credits::new(64);
    thread::scope(|scope| {
        for worker in 0..8 {
            let sequencer = &sequencer;
            let credits = &credits;
            scope.spawn(move || {
                for operation in 0..4000 {
                    let sequence = credits
                        .reserve()
                        .allocate(sequencer, operation * 8 + worker);
                    publish(sequencer, sequence);
                    outcome(sequencer, sequence);
                    credits.release(sequencer.complete_resolve(sequence));
                    assert_eq!(
                        sequencer
                            .wait(version(sequence), Instant::now() + Duration::from_secs(5))
                            .unwrap(),
                        version(sequence)
                    );
                    credits.release(sequencer.complete_release(sequence));
                }
            });
        }
    });
    assert_eq!(sequencer.reuse_next(), sequencer.next());
    assert_eq!(*credits.available.lock(), 64);
}

#[test]
fn released_early_slots_wait_for_concurrent_publishers_outcomes() {
    for start in [0, 1 << 40] {
        released_early_slots_wait_for_outcomes_from(start);
    }
}

fn released_early_slots_wait_for_outcomes_from(start: u64) {
    let sequencer = Sequencer::with_next(2, start);
    let credits = Credits::new(2);
    let before_outcome = Barrier::new(2);
    thread::scope(|scope| {
        for worker in 0..2 {
            let sequencer = &sequencer;
            let credits = &credits;
            let before_outcome = &before_outcome;
            scope.spawn(move || {
                for iteration in 0..4000 {
                    let sequence = credits
                        .reserve()
                        .allocate(sequencer, iteration * 2 + worker);
                    let timed_out = sequencer
                        .wait(version(sequence), Instant::now())
                        .unwrap_err();
                    assert_eq!(timed_out.uncertain_version, Some(version(sequence)));
                    assert_eq!(sequencer.complete_release(sequence), 0);
                    publish(sequencer, sequence);
                    before_outcome.wait();
                    // Both published generations remain unreclaimable until the
                    // resolver has installed the outcome and finished cleanup.
                    assert!(sequencer.reuse_next() <= sequence);
                    outcome(sequencer, sequence);
                    credits.release(sequencer.complete_resolve(sequence));
                    // Finish this pair before allocating the next one. Without
                    // this round boundary complete_release may correctly retire
                    // the previous round's finalized prefix, not its own slot.
                    // Keep the exact zero-retirement assertion above.
                    before_outcome.wait();
                }
            });
        }
    });
    assert_eq!(sequencer.reuse_next(), sequencer.next());
    assert_eq!(*credits.available.lock(), 2);
}

#[test]
fn credit_cancellation_and_unknown_never_reclaim() {
    let sequencer = Sequencer::new(1);
    let credits = Credits::new(1);
    drop(credits.reserve());
    assert_eq!(*credits.available.lock(), 1);
    let sequence = credits.reserve().allocate(&sequencer, 1);
    assert!(!sequencer.resolved(sequence));
    sequencer
        .transition(sequence, Resolution::Allocated, Resolution::Unknown)
        .unwrap();
    assert_eq!(sequencer.complete_release(sequence), 0);
    assert_eq!(sequencer.complete_resolve(sequence), 0);
    assert_eq!(sequencer.reuse_next(), 0);
    assert!(!sequencer.resolved(sequence));
    assert!(
        sequencer
            .transition(sequence, Resolution::Ready, Resolution::Durable)
            .is_err()
    );
}

#[test]
fn empty_slot_is_not_an_allocated_zero_generation() {
    let sequencer = Sequencer::new(1);
    assert_eq!(sequencer.next(), 0);
    assert_eq!(sequencer.reuse_next(), 0);
    assert_eq!(sequencer.generation(0), None);
    assert_eq!(sequencer.resolution(0), None);
    assert!(!sequencer.resolved(0));
    assert!(sequencer.wait(version(0), Instant::now()).is_err());
    assert!(
        sequencer
            .transition(0, Resolution::Allocated, Resolution::Ready)
            .is_err()
    );
    let credits = Credits::new(1);
    assert_eq!(credits.reserve().allocate(&sequencer, 0), 0);
    assert_eq!(sequencer.generation(0), Some(0));
    assert_eq!(sequencer.resolution(0), Some(Resolution::Allocated));
    assert_eq!(sequencer.slot(0).state.load(Ordering::Acquire), 1);
    publish(&sequencer, 0);
    outcome(&sequencer, 0);
    assert_eq!(sequencer.complete_resolve(0), 0);
    credits.release(sequencer.complete_release(0));
    assert_eq!(sequencer.reuse_next(), 1);
    assert_eq!(*credits.available.lock(), 1);
}
