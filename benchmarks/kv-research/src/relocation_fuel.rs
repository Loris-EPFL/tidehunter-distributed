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

use crate::backend::KvBackend;
use crate::config::RelocationFuelConfig;
use crate::workload::{key_bytes, value_bytes};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

/// One running stream: its stop flag, its optional task, and its plan.
pub struct RelocationFuel {
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<FuelCounts>>,
    plan: RelocationFuelConfig,
}

impl Drop for RelocationFuel {
    /// A dropped stream must not outlive its owner. The runner's early `?`
    /// returns drop this value; a detached task would keep rewriting dataset
    /// keys to `max_seconds` after the run had already failed. The stop flag
    /// ends the loop at its next check, and the abort also cancels a call that
    /// is already in flight at the backend.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Counts of the stream. Errors after an owner kill are expected evidence.
#[derive(Default)]
struct FuelCounts {
    puts: u64,
    /// Accepted re-requests; the runner makes the first request.
    requests: u64,
    errors: u64,
    first_error: Option<String>,
}

impl RelocationFuel {
    /// Start the stream. The first rewrite completes before this returns: the
    /// pass freezes `Q` as soon as its worker picks the request up, and only an
    /// appended batch is visible to the scan at or above that cut.
    pub async fn start<B: KvBackend>(
        backend: Arc<B>,
        namespace: [u8; 16],
        seed: u64,
        keys: u64,
        value_size: usize,
        entropy: f64,
        plan: RelocationFuelConfig,
    ) -> Self {
        let mut counts = FuelCounts::default();
        rewrite(
            backend.as_ref(),
            &namespace,
            seed,
            0,
            value_size,
            entropy,
            &mut counts,
        )
        .await;
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let period = period(plan);
        let retry = Duration::from_secs(u64::from(plan.retry_seconds.max(1)));
        let until = Instant::now() + Duration::from_secs(u64::from(plan.max_seconds));
        let task = tokio::spawn(async move {
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
                        Ok(()) => counts.requests = counts.requests.saturating_add(1),
                        Err(error) => {
                            counts.errors = counts.errors.saturating_add(1);
                            if counts.first_error.is_none() {
                                counts.first_error = Some(error.to_string());
                            }
                        }
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
                    &mut counts,
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
            counts
        });
        Self {
            stop,
            task: Some(task),
            plan,
        }
    }

    /// Stop the stream and join it. `grace` bounds the join: one call already in
    /// flight can hold the task until the backend's own timeout, and a stuck one
    /// is aborted rather than delaying the run.
    ///
    /// `request_to_release` is measured by the caller between `start_relocation`
    /// and the completion gate's return, never by this task: the join can wait on
    /// one slow put, and this stream's start is one rewrite before the request.
    pub async fn finish(
        mut self,
        grace: Duration,
        request_to_release: Duration,
    ) -> serde_json::Value {
        self.stop.store(true, Ordering::Relaxed);
        let plan = self.plan;
        let mut joined = true;
        let counts = match self.task.take() {
            Some(mut task) => match tokio::time::timeout(grace, &mut task).await {
                Ok(Ok(counts)) => counts,
                Ok(Err(error)) => {
                    joined = false;
                    FuelCounts {
                        first_error: Some(format!("fuel task failed: {error}")),
                        ..FuelCounts::default()
                    }
                }
                Err(_) => {
                    task.abort();
                    joined = false;
                    FuelCounts::default()
                }
            },
            // A consumed value cannot be `finish`ed twice; report no join
            // rather than a fabricated one.
            None => {
                joined = false;
                FuelCounts::default()
            }
        };
        serde_json::json!({
            "requested": true,
            "puts_per_second": plan.puts_per_second,
            "max_seconds": plan.max_seconds,
            "retry_seconds": plan.retry_seconds,
            "puts": counts.puts,
            "requests": counts.requests,
            "errors": counts.errors,
            "first_error": counts.first_error,
            "seconds_request_to_release": request_to_release.as_secs_f64(),
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
    counts: &mut FuelCounts,
) {
    let key = key_bytes(namespace, seed, id);
    let value = value_bytes(namespace, seed, id, value_size, entropy);
    match backend.put(key, value).await {
        Ok(()) => counts.puts = counts.puts.saturating_add(1),
        Err(error) => {
            counts.errors = counts.errors.saturating_add(1);
            if counts.first_error.is_none() {
                counts.first_error = Some(error.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests;
