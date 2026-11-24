use crate::latch::Latch;
use rand::Rng;
use rand::prelude::ThreadRng;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

pub(crate) struct FailPoint {
    fp: Box<dyn Fn() + Send + Sync + 'static>,
}

impl Default for FailPoint {
    fn default() -> Self {
        Self {
            fp: Box::new(|| {}),
        }
    }
}

impl FailPoint {
    pub fn from_fn(fp: impl Fn() + Send + Sync + 'static) -> Self {
        Self { fp: Box::new(fp) }
    }

    pub fn sleep(range: Range<Duration>) -> Self {
        let fp = Box::new(move || {
            let mut rng = ThreadRng::default();
            let delay = rng.gen_range(range.clone());
            thread::sleep(delay)
        });
        Self { fp }
    }

    /// Failpoint that blocks until provided latch is unlocked
    pub fn latch(latch: Latch) -> Self {
        let fp = Box::new(move || {
            latch.latch();
        });
        Self { fp }
    }

    /// Like [`Self::latch`], but counts the callers that reached the
    /// failpoint in `arrivals` before they block, so a test can wait until
    /// every participant is parked before releasing the latch.
    pub fn latch_counting(latch: Latch, arrivals: Arc<AtomicUsize>) -> Self {
        let fp = Box::new(move || {
            let _: usize = arrivals.fetch_add(1, Ordering::SeqCst);
            latch.latch();
        });
        Self { fp }
    }

    /// Failpoint that panics after being called N times
    pub fn panic_after_n_calls(n: usize) -> Self {
        let counter = AtomicUsize::new(0);
        let fp = Box::new(move || {
            let count = counter.fetch_add(1, Ordering::SeqCst);
            if count >= n {
                panic!("failpoint: panic_after_n_calls triggered after {} calls", n);
            }
        });
        Self { fp }
    }

    pub fn fp(&self) {
        (self.fp)()
    }
}

/// Process-crash cuts for the distributed model checker. The arm step excludes
/// initialization and recovery. Nothing in this module exists in production.
#[cfg(all(test, feature = "experimental-distributed"))]
pub(crate) mod write_model {
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::sync::{Mutex, OnceLock};

    pub(crate) const CRASH_EXIT: i32 = 86;
    static STATE: OnceLock<Mutex<State>> = OnceLock::new();
    static FLUSH_SUBMISSIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    struct State {
        trace: File,
        target: String,
        acknowledged: usize,
        after_acks: usize,
        enabled: bool,
    }

    pub(crate) fn arm(trace: &std::path::Path, target: String, after_acks: usize) {
        assert!(after_acks >= 7);
        assert!(
            STATE
                .set(Mutex::new(State {
                    trace: OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(trace)
                        .unwrap(),
                    target,
                    acknowledged: 1,
                    after_acks,
                    enabled: false,
                }))
                .is_ok()
        );
    }

    pub(crate) fn acknowledged() {
        if let Some(state) = STATE.get() {
            state.lock().unwrap().acknowledged += 1;
        }
    }

    /// Enable only at a quiescent, seed-selected acknowledged prefix. Physical
    /// grouping and background sync counts cannot move or remove this gate.
    pub(crate) fn open_gate() {
        let mut state = STATE.get().unwrap().lock().unwrap();
        assert_eq!(state.acknowledged, state.after_acks);
        let acknowledged = state.acknowledged;
        writeln!(state.trace, "gate_after_acks={acknowledged}").unwrap();
        state.enabled = true;
    }

    pub(crate) fn submitted(write: bool, flush: bool) {
        if flush {
            // Relaxed: this is an independent test observation, not a hand-off.
            FLUSH_SUBMISSIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if write {
            boundary(if flush {
                "flush_submitted_to_kernel"
            } else {
                "write_submitted_to_kernel"
            });
        }
    }

    pub(crate) fn flush_submissions() -> u64 {
        // Relaxed: callers observe this after draining their own I/O.
        FLUSH_SUBMISSIONS.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn adoption(actual: bool, submitted: bool) {
        if actual {
            boundary(if submitted {
                "leased_adoption_submitted"
            } else {
                "leased_adoption_completed"
            });
        }
    }

    pub(crate) fn boundary(name: &'static str) {
        let Some(state) = STATE.get() else { return };
        let mut state = state.lock().unwrap();
        if !state.enabled {
            return;
        }
        writeln!(state.trace, "{name}").unwrap();
        if name == state.target {
            // A whole-process exit kills owner, syncer, checkpoint and caller
            // threads together, without destructors or a shutdown checkpoint.
            std::process::exit(CRASH_EXIT);
        }
    }
}
