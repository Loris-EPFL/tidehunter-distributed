//! Asynchronous R1 relocation worker of one remote database
//! (CAPACITY-R0-R1.md §5), the leased counterpart of native's relocator.
//!
//! One joined thread per database runs passes on the shared
//! `ConcurrentRemoteDatabase`; it never holds the Sui `Db`, so dropping the
//! last `Db` handle joins it without a self-join. `start` returns at once:
//! requests that arrive while a pass is queued or running coalesce into one
//! successor pass. One non-coalescing test slot carries a named observed pass;
//! a second test request while it is occupied is `Busy`. Shutdown rejects new
//! requests, cancels the running pass between batches (before adoption) and
//! joins the thread; a queued test request then observes `WorkerStopped`.
//!
//! The pending state is one cold mutex and condition variable, held only to
//! read or set the request bits; no pass work runs under it. It is bounded:
//! one start bit and one test slot.
use crate::distributed::remote_db::{ConcurrentRemoteDatabase, RelocationReport};
use crate::distributed::storage::LocalDbError;
#[cfg(feature = "test_methods")]
use crate::distributed::types::BatchVersion;
use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Instant;

/// What one distributed relocation pass persisted, beside the native-shaped
/// `RelocationObservation`.
///
/// Native relocation has no persistence step to report: a pass is complete when
/// the worker returns. The distributed pass re-points index entries and may
/// adopt callback starts and retire owner files only after a *covering*
/// snapshot stores those re-points (plan A18/A22-A24), so the observation alone
/// would show `DidWork` for a pass that was deferred and reclaimed nothing.
///
/// The three outcomes of a pass, without a third gate value:
/// * `NoWork` gate: nothing to do;
/// * `DidWork`, `deferred == false`, `stored_target: Some`: starts adopted,
///   files retired;
/// * `DidWork`, `deferred == true`, `stored_target: None`: the pass may have
///   mutated the index, persisted nothing and retired nothing (the routine
///   outcome under a held checkpoint latch).
///
/// Compared between runs of the same fixture: `deferred`, `stored_target`
/// presence, `stored_forced`, `copied_survivors`, `rejected_survivors`.
/// Versions (`scan_cut`, `promotion_cut`, the `stored_target` value) are run
/// identities, recorded but never compared across runs.
#[cfg(feature = "test_methods")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelocationReceipt {
    /// The pass may have mutated the index and did not persist that mutation.
    pub deferred: bool,
    /// The scan cut `Q` the pass copied against.
    pub scan_cut: BatchVersion,
    /// The stored target that covered the pass's re-points; `None` when no
    /// store covered them.
    pub stored_target: Option<BatchVersion>,
    /// The covering store was the forced relocation store.
    pub stored_forced: bool,
    /// The flush frontier the attempted store used; `None` when none was tried.
    pub promotion_cut: Option<BatchVersion>,
    /// Survivors copied to the serving generation and re-pointed.
    pub copied_survivors: u64,
    /// Survivors whose bytes failed the indexed CRC and kept their old position.
    pub rejected_survivors: u64,
}

/// Capacity-one reply of one observed pass. The worker uses `try_send`, so a
/// requester that timed out (dropped receiver) never blocks it.
type TestReply = mpsc::SyncSender<Result<RelocationReport, LocalDbError>>;

#[derive(Debug)]
pub(super) enum StartError {
    ShuttingDown,
    /// The worker thread ended: a relocation filter panicked in a pass.
    WorkerStopped,
}

#[cfg(feature = "test_methods")]
#[derive(Debug)]
pub(super) enum TestError {
    Busy,
    DeadlineExceeded,
    WorkerStopped,
    Pass(LocalDbError),
}

#[derive(Default)]
struct Pending {
    start: bool,
    test: Option<(Instant, TestReply)>,
    /// A test pass is queued or running: the test slot is occupied.
    #[cfg_attr(
        not(feature = "test_methods"),
        allow(dead_code, reason = "read only by the test request path")
    )]
    test_occupied: bool,
    shutdown: bool,
}

struct Shared {
    pending: Mutex<Pending>,
    wake: Condvar,
    /// Set with `Pending::shutdown`; read by the pass between batches.
    cancel: AtomicBool,
}

pub(super) struct RemoteRelocator {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
}

impl RemoteRelocator {
    pub(super) fn start_worker(db: Arc<ConcurrentRemoteDatabase>) -> std::io::Result<Self> {
        let shared = Arc::new(Shared {
            pending: Mutex::new(Pending::default()),
            wake: Condvar::new(),
            cancel: AtomicBool::new(false),
        });
        let worker = Arc::clone(&shared);
        let thread = std::thread::Builder::new()
            .name("remote-relocator".into())
            .spawn(move || run(&worker, &db))?;
        Ok(Self {
            shared,
            thread: Some(thread),
        })
    }

    /// Request a pass; coalesces with a queued one.
    pub(super) fn start(&self) -> Result<(), StartError> {
        if self.thread.as_ref().is_none_or(JoinHandle::is_finished) {
            return Err(StartError::WorkerStopped);
        }
        let mut pending = self.shared.pending.lock();
        if pending.shutdown {
            return Err(StartError::ShuttingDown);
        }
        pending.start = true;
        self.shared.wake.notify_one();
        Ok(())
    }

    /// Run one named observed pass and wait for it until `deadline`. A
    /// deadline drops only this waiter, never the pass.
    #[cfg(feature = "test_methods")]
    pub(super) fn run_for_test(&self, deadline: Instant) -> Result<RelocationReport, TestError> {
        if Instant::now() >= deadline {
            return Err(TestError::DeadlineExceeded);
        }
        let (reply, receiver) = mpsc::sync_channel(1);
        {
            let mut pending = self.shared.pending.lock();
            if pending.shutdown || self.thread.as_ref().is_none_or(JoinHandle::is_finished) {
                return Err(TestError::WorkerStopped);
            }
            if pending.test_occupied {
                return Err(TestError::Busy);
            }
            pending.test_occupied = true;
            pending.test = Some((deadline, reply));
            self.shared.wake.notify_one();
        }
        match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(result) => result.map_err(TestError::Pass),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(TestError::DeadlineExceeded),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(TestError::WorkerStopped),
        }
    }
}

impl Drop for RemoteRelocator {
    fn drop(&mut self) {
        {
            let mut pending = self.shared.pending.lock();
            pending.shutdown = true;
            // Relaxed: the mutex orders it for the worker loop; the pass reads
            // it only as a cancellation hint.
            self.shared.cancel.store(true, Ordering::Relaxed);
            self.shared.wake.notify_one();
        }
        if let Some(thread) = self.thread.take() {
            // A pass error never unwinds (production fail-stops the
            // authority). Only a panicking filter ends the thread early;
            // `start` then reports `WorkerStopped`.
            let _ = thread.join();
        }
    }
}

enum Work {
    Production,
    Test(Instant, TestReply),
}

fn run(shared: &Shared, db: &ConcurrentRemoteDatabase) {
    loop {
        let work = {
            let mut pending = shared.pending.lock();
            loop {
                if pending.shutdown {
                    // A queued test reply drops here: its waiter observes
                    // `WorkerStopped`.
                    return;
                }
                // A test pass also covers a start requested before it began.
                if let Some((deadline, reply)) = pending.test.take() {
                    pending.start = false;
                    break Work::Test(deadline, reply);
                }
                if pending.start {
                    pending.start = false;
                    break Work::Production;
                }
                shared.wake.wait(&mut pending);
            }
        };
        match work {
            Work::Production => db.background_relocation_pass(&shared.cancel),
            Work::Test(deadline, reply) => {
                let result = if Instant::now() >= deadline {
                    Err(LocalDbError::new(
                        "observed relocation exceeded its deadline",
                    ))
                } else {
                    db.relocation_pass(true, &shared.cancel)
                };
                shared.pending.lock().test_occupied = false;
                let _ = reply.try_send(result);
            }
        }
    }
}
