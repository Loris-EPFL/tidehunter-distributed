//! Bounded post-measurement write stream for the relocation pass.
//!
//! A requested pass freezes its scan cut `Q = min(Version B durable, index
//! applied, held latch)` and derives its terminal from an observed
//! `StopRelocation` or from the first scanned batch at or above `Q`
//! (`remote_db/relocation.rs` `stream_reports`). A measurement that drained
//! leaves `Q` at the published tip, so a keyspace without a relocation filter
//! reaches `EndOfWal`, reports `NoWork`, adopts no callback start and retires
//! nothing. Relocation is requested after the measurement boundary, when the
//! closed loop has stopped, so the cell has to keep a small stream of writes in
//! flight for the pass to find a batch above `Q`.
//!
//! The stream rewrites existing dataset keys with their deterministic payloads,
//! so a post-restart audit compares the same bytes it expects. The appends also
//! allocate past the whole-file target the pass derives: a committed retirement
//! whose victim still ends at the allocator detaches nothing until the next
//! allocation (`storage/file_retirement.rs`), so this stream is also what keeps
//! the same pass able to unlink instead of leaving `cleanup_pending` behind.
//!
//! Bounded: one task, a fixed period, a wall-clock bound, and a stop flag set
//! when the completion gate is released or times out. While it runs it also
//! re-requests the pass at a fixed period, as Sui's periodic pruner does, so a
//! deferred pass (a committed retirement whose victim still ends at the
//! allocator) or a freeze above the cut gets another pass; a request while a
//! pass runs coalesces into one follow-up pass. A failed put is expected after
//! the driver kills an owner on a retirement cell; it is counted and never
//! fails the run, because the coordinator's own typed terminal failure is the
//! cell's evidence.
//!
//! The first rewrite runs inside the task, and its `ready` signal is what
//! `start` waits for. Nothing here can cancel a put whose body is synchronous
//! (the campaign adapter's `db.insert` waits for publication): abort stops at
//! the next await point only, so `finish` waits up to the caller's join bound,
//! which covers one put in flight, and reports an exceeded bound as an error.

use crate::backend::KvBackend;
use crate::config::RelocationFuelConfig;
use crate::workload::{key_bytes, value_bytes};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// What one stream rewrites and the bounds it runs under. The dataset
/// parameters are the same ones a measured item uses, so a rewrite is the
/// dataset's own bytes; the stream never invents a key.
pub struct FuelRequest {
    pub namespace: [u8; 16],
    pub seed: u64,
    pub keys: u64,
    pub value_size: usize,
    pub entropy: f64,
    pub plan: RelocationFuelConfig,
    /// Covers one put already in flight; see [`RelocationFuel::start`].
    pub join_bound: Duration,
}

/// One running stream: its stop flag, its task, its shared counts, and its plan.
pub struct RelocationFuel {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
    counts: Arc<Mutex<FuelCounts>>,
    plan: RelocationFuelConfig,
    /// Whether the task's first rewrite completed inside `start`'s bound.
    first_rewrite_ready: bool,
}

impl Drop for RelocationFuel {
    /// A dropped stream must not outlive its owner. The runner's early `?`
    /// returns drop this value, and cancelling `run` drops it inside `finish`'s
    /// join; a detached task would keep rewriting dataset keys after the run had
    /// already failed. The stop flag ends the loop at its next check, and the
    /// abort also cancels a call that is parked at an await point. The handle
    /// therefore stays on the value for its whole life: `finish` awaits it in
    /// place instead of taking it.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.task.abort();
    }
}

/// Counts of the stream. Errors after an owner kill are expected evidence, and
/// the counts stay readable after an aborted task, so a join that misses its
/// bound still reports what the stream did.
#[derive(Default)]
struct FuelCounts {
    puts: u64,
    /// Accepted re-requests; the runner makes the first request.
    requests: u64,
    errors: u64,
    first_error: Option<String>,
}

type SharedCounts = Arc<Mutex<FuelCounts>>;

fn counted(counts: &SharedCounts, update: impl FnOnce(&mut FuelCounts)) {
    let mut guard = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    update(&mut guard);
}

impl RelocationFuel {
    /// Start the stream and wait for its first rewrite.
    ///
    /// The task owns the first rewrite and signals `ready` after it, so the
    /// rewrite is inside `max_seconds`, inside the stop path and inside the
    /// counts. `start` waits for that signal up to `join_bound`, the same bound
    /// `finish` uses: it covers one put in flight, because a synchronous put
    /// body cannot be cancelled. A missing signal is reported in the record, not
    /// hidden.
    pub async fn start<B: KvBackend>(backend: Arc<B>, request: FuelRequest) -> Self {
        let FuelRequest {
            namespace,
            seed,
            keys,
            value_size,
            entropy,
            plan,
            join_bound,
        } = request;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let counts: SharedCounts = Arc::new(Mutex::new(FuelCounts::default()));
        let task_counts = Arc::clone(&counts);
        let (ready, first_rewrite) = tokio::sync::oneshot::channel();
        let period = period(plan);
        let retry = Duration::from_secs(u64::from(plan.retry_seconds.max(1)));
        let task = tokio::spawn(async move {
            // The bound starts before the first rewrite, so that rewrite is
            // inside `max_seconds` and not an unbounded prologue.
            let until = Instant::now() + Duration::from_secs(u64::from(plan.max_seconds));
            rewrite(
                backend.as_ref(),
                &namespace,
                seed,
                0,
                value_size,
                entropy,
                &task_counts,
            )
            .await;
            let _ = ready.send(());
            let mut cursor = 1_u64;
            let mut next = Instant::now() + period;
            let mut next_request = Instant::now() + retry;
            while !flag.load(Ordering::Relaxed) && Instant::now() < until {
                // A deferred pass (a committed retirement whose victim still
                // ends at the allocator) or a freeze above the cut needs another
                // pass. Asking again while a pass runs coalesces: the relocator
                // holds one pending flag, so any number of requests becomes one
                // follow-up pass and never a queue (`sui/relocation.rs`
                // "Request a pass; coalesces with a queued one").
                let now = Instant::now();
                if now >= next_request {
                    match backend.start_relocation() {
                        Ok(()) => counted(&task_counts, |counts| {
                            counts.requests = counts.requests.saturating_add(1);
                        }),
                        Err(error) => counted(&task_counts, |counts| {
                            counts.errors = counts.errors.saturating_add(1);
                            if counts.first_error.is_none() {
                                counts.first_error = Some(error.to_string());
                            }
                        }),
                    }
                    next_request += retry;
                    if next_request <= now {
                        next_request = now + retry;
                    }
                }
                let id = cursor % keys.max(1);
                cursor = cursor.wrapping_add(1);
                rewrite(
                    backend.as_ref(),
                    &namespace,
                    seed,
                    id,
                    value_size,
                    entropy,
                    &task_counts,
                )
                .await;
                // Fixed period: a slow call is not followed by a burst that
                // catches up, so the rate never exceeds `puts_per_second`.
                let now = Instant::now();
                if next > now {
                    tokio::time::sleep_until(next).await;
                } else {
                    next = now;
                }
                next += period;
            }
        });
        // The guard exists before this function awaits anything: a caller that
        // cancels `start` (a run deadline, a `select!`) drops `Self`, and its
        // Drop must reach the task. Only then wait for the first rewrite.
        let mut fuel = Self {
            stop,
            task,
            counts,
            plan,
            first_rewrite_ready: false,
        };
        fuel.first_rewrite_ready = tokio::time::timeout(join_bound, first_rewrite)
            .await
            .is_ok_and(|received| received.is_ok());
        fuel
    }

    /// Stop the stream and join it.
    ///
    /// `join_bound` must cover one put already in flight (the caller derives it
    /// from the engine's client bound, not from a constant): the stop flag ends
    /// the loop only between puts, and abort cannot cancel a synchronous put
    /// body. An exceeded bound aborts the task and is reported as an error.
    ///
    /// `request_to_release` is measured by the caller between `start_relocation`
    /// and the completion gate's return, never by this task: the join can wait on
    /// one slow put, and this stream's start is one rewrite before the request.
    pub async fn finish(
        mut self,
        join_bound: Duration,
        request_to_release: Duration,
    ) -> serde_json::Value {
        self.stop.store(true, Ordering::Relaxed);
        let plan = self.plan;
        let mut joined = true;
        let mut join_timed_out = false;
        // The handle is awaited in place, so a caller that cancels this future
        // during the join still reaches `Drop` with the task in hand.
        match tokio::time::timeout(join_bound, &mut self.task).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                joined = false;
                counted(&self.counts, |counts| {
                    counts.errors = counts.errors.saturating_add(1);
                    if counts.first_error.is_none() {
                        counts.first_error = Some(format!("fuel task failed: {error}"));
                    }
                });
            }
            Err(_) => {
                self.task.abort();
                joined = false;
                join_timed_out = true;
            }
        }
        let counts = {
            let guard = self
                .counts
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            FuelCounts {
                puts: guard.puts,
                requests: guard.requests,
                errors: guard.errors,
                first_error: guard.first_error.clone(),
            }
        };
        let first_error = match (counts.first_error, join_timed_out) {
            (Some(error), _) => Some(error),
            (None, true) => Some(format!(
                "fuel task did not stop within the join bound ({} ms); one put may still be in flight",
                join_bound.as_millis()
            )),
            (None, false) => None,
        };
        serde_json::json!({
            "requested": true,
            "puts_per_second": plan.puts_per_second,
            "max_seconds": plan.max_seconds,
            "retry_seconds": plan.retry_seconds,
            "puts": counts.puts,
            "requests": counts.requests,
            "errors": counts.errors,
            "first_error": first_error,
            "seconds_request_to_release": request_to_release.as_secs_f64(),
            "join_bound_seconds": join_bound.as_secs_f64(),
            "join_timed_out": join_timed_out,
            "first_rewrite_ready": self.first_rewrite_ready,
            "joined": joined,
            "purpose": "rewrites dataset keys with their deterministic payloads so the \
                        relocation pass finds a batch above its frozen scan cut and the \
                        owner can detach the file it retires",
        })
    }
}

fn period(plan: RelocationFuelConfig) -> Duration {
    Duration::from_secs_f64(1.0 / f64::from(plan.puts_per_second.max(1)))
}

async fn rewrite<B: KvBackend>(
    backend: &B,
    namespace: &[u8; 16],
    seed: u64,
    id: u64,
    value_size: usize,
    entropy: f64,
    counts: &SharedCounts,
) {
    let key = key_bytes(namespace, seed, id);
    let value = value_bytes(namespace, seed, id, value_size, entropy);
    match backend.put(key, value).await {
        Ok(()) => counted(counts, |counts| {
            counts.puts = counts.puts.saturating_add(1);
        }),
        Err(error) => counted(counts, |counts| {
            counts.errors = counts.errors.saturating_add(1);
            if counts.first_error.is_none() {
                counts.first_error = Some(error.to_string());
            }
        }),
    }
}

#[cfg(test)]
mod tests;
