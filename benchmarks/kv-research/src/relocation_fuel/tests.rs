use super::*;
use crate::backend::{BackendInfo, BackendResult, BatchAtomicity};
use crate::workload::{key_bytes, value_bytes};
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

/// Records every rewrite; fails every put at or after `fail_after`, and parks
/// every put at or after `park_after` until its future is dropped.
struct FuelBackend {
    puts: AtomicU64,
    relocation_requests: AtomicU64,
    writes: Mutex<Vec<(tidehunter::minibytes::Bytes, tidehunter::minibytes::Bytes)>>,
    fail_after: Option<u64>,
    park_after: Option<u64>,
    parked: AtomicU64,
    parked_dropped: AtomicU64,
}

/// Counts one parked call whose future was dropped: the only proof that a
/// cancellation reached a backend call already in flight.
struct ParkedPut<'a>(&'a AtomicU64);

impl Drop for ParkedPut<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

impl FuelBackend {
    fn new(fail_after: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            puts: AtomicU64::new(0),
            relocation_requests: AtomicU64::new(0),
            writes: Mutex::new(Vec::new()),
            fail_after,
            park_after: None,
            parked: AtomicU64::new(0),
            parked_dropped: AtomicU64::new(0),
        })
    }

    /// The first rewrite returns; every later one parks until it is cancelled.
    fn parking() -> Arc<Self> {
        Arc::new(Self {
            puts: AtomicU64::new(0),
            relocation_requests: AtomicU64::new(0),
            writes: Mutex::new(Vec::new()),
            fail_after: None,
            park_after: Some(1),
            parked: AtomicU64::new(0),
            parked_dropped: AtomicU64::new(0),
        })
    }
}

impl KvBackend for FuelBackend {
    fn info(&self) -> BackendInfo {
        BackendInfo {
            name: "fuel-test".to_owned(),
            server_version: "test".to_owned(),
            client_version: "test".to_owned(),
            acknowledgement: "test acknowledgement".to_owned(),
            consistency: "test sequential".to_owned(),
            replication_factor: 1,
            measurement_boundary: "in process".to_owned(),
            get_many_strategy: "ordered".to_owned(),
            batch_atomicity: BatchAtomicity::SerializableTransaction,
            topology: serde_json::json!({"owners": 1}),
            configuration: serde_json::json!({}),
        }
    }

    fn start_relocation(&self) -> BackendResult<()> {
        self.relocation_requests.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn get(&self, _key: &[u8]) -> BackendResult<Option<tidehunter::minibytes::Bytes>> {
        Ok(None)
    }

    async fn put(
        &self,
        key: tidehunter::minibytes::Bytes,
        value: tidehunter::minibytes::Bytes,
    ) -> BackendResult<()> {
        let index = self.puts.fetch_add(1, Ordering::Relaxed);
        if self.fail_after.is_some_and(|limit| index >= limit) {
            return Err(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "owner write failed").into(),
            );
        }
        if self.park_after.is_some_and(|limit| index >= limit) {
            self.parked.fetch_add(1, Ordering::Relaxed);
            let _parked = ParkedPut(&self.parked_dropped);
            std::future::pending::<()>().await;
        }
        self.writes.lock().unwrap().push((key, value));
        Ok(())
    }

    async fn delete(&self, _key: tidehunter::minibytes::Bytes) -> BackendResult<()> {
        Ok(())
    }

    async fn get_many(
        &self,
        keys: &[tidehunter::minibytes::Bytes],
    ) -> BackendResult<Vec<Option<tidehunter::minibytes::Bytes>>> {
        Ok(vec![None; keys.len()])
    }

    async fn put_batch(
        &self,
        entries: &[(tidehunter::minibytes::Bytes, tidehunter::minibytes::Bytes)],
    ) -> BackendResult<()> {
        for (key, value) in entries {
            self.put(key.clone(), value.clone()).await?;
        }
        Ok(())
    }
}

fn plan(puts_per_second: u32, max_seconds: u32, retry_seconds: u32) -> RelocationFuelConfig {
    RelocationFuelConfig {
        puts_per_second,
        max_seconds,
        retry_seconds,
    }
}

/// The rewrite stream exists for the pass's scan cut: one appended batch must
/// already exist when the request returns, the rate never exceeds the plan, and
/// the bytes are the dataset's own, so a post-restart audit is unchanged.
#[tokio::test]
async fn the_first_rewrite_lands_before_the_request_and_the_rate_is_bounded() {
    let backend = FuelBackend::new(None);
    let fuel = RelocationFuel::start(
        Arc::clone(&backend),
        [7; 16],
        11,
        100,
        64,
        1.0,
        plan(50, 5, 300),
    )
    .await;
    assert!(backend.puts.load(Ordering::Relaxed) >= 1);
    tokio::time::sleep(Duration::from_millis(220)).await;
    let request_to_release = Duration::from_millis(220);
    let record = fuel
        .finish(Duration::from_secs(2), request_to_release)
        .await;
    let puts = record["puts"].as_u64().unwrap();
    let seconds = record["seconds_request_to_release"].as_f64().unwrap();
    assert!((seconds - 0.220).abs() < 1e-9, "{record}");
    assert!(record["joined"].as_bool().unwrap(), "{record}");
    assert_eq!(record["errors"].as_u64(), Some(0), "{record}");
    assert_eq!(record["puts_per_second"].as_u64(), Some(50), "{record}");
    // The re-request period is far outside this window: the runner's own
    // request is the only one.
    assert_eq!(record["requests"].as_u64(), Some(0), "{record}");
    assert!(puts >= 2, "{record}");
    // At most the configured rate, plus the pre-request rewrite, plus one call
    // already in flight at the stop.
    assert!(
        puts as f64 <= seconds * 50.0 + 3.0,
        "rate exceeded: {record}"
    );
    let writes = backend.writes.lock().unwrap();
    assert_eq!(writes[0].0, key_bytes(&[7; 16], 11, 0));
    assert_eq!(writes[0].1, value_bytes(&[7; 16], 11, 0, 64, 1.0));
}

/// The wall-clock bound is a hard stop even when the completion gate never
/// releases: the task ends by itself and the join returns at once.
#[tokio::test]
async fn the_wall_clock_bound_ends_the_stream_without_a_stop() {
    let backend = FuelBackend::new(None);
    let fuel = RelocationFuel::start(
        Arc::clone(&backend),
        [3; 16],
        5,
        100,
        64,
        1.0,
        plan(1_000, 1, 300),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_400)).await;
    let record = fuel
        .finish(Duration::from_millis(200), Duration::from_secs(1))
        .await;
    assert!(record["joined"].as_bool().unwrap(), "{record}");
    assert_eq!(
        record["seconds_request_to_release"].as_f64(),
        Some(1.0),
        "{record}"
    );
    assert!(record["puts"].as_u64().unwrap() <= 1_003, "{record}");
}

/// A failed rewrite after an owner kill is expected evidence: it is counted and
/// returned, and it never fails the stream or the run.
#[tokio::test]
async fn failed_rewrites_are_counted_and_never_fail_the_run() {
    let backend = FuelBackend::new(Some(1));
    let fuel = RelocationFuel::start(
        Arc::clone(&backend),
        [9; 16],
        2,
        100,
        64,
        1.0,
        plan(500, 5, 300),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let record = fuel
        .finish(Duration::from_secs(2), Duration::from_millis(80))
        .await;
    assert!(record["joined"].as_bool().unwrap(), "{record}");
    assert_eq!(record["puts"].as_u64(), Some(1), "{record}");
    assert!(record["errors"].as_u64().unwrap() >= 1, "{record}");
    assert!(
        record["first_error"]
            .as_str()
            .unwrap()
            .contains("owner write failed"),
        "{record}"
    );
}

/// The gate-held stream re-requests the pass at its period, as Sui's periodic
/// pruner does, so a deferred pass or a late freeze gets another pass; it stops
/// asking the moment it is stopped.
#[tokio::test]
async fn the_pass_is_re_requested_at_the_period_and_stops_at_release() {
    let backend = FuelBackend::new(None);
    let fuel = RelocationFuel::start(
        Arc::clone(&backend),
        [5; 16],
        3,
        100,
        64,
        1.0,
        plan(1_000, 5, 1),
    )
    .await;
    // The runner makes the first request; the stream adds its own at the period.
    assert_eq!(backend.relocation_requests.load(Ordering::Relaxed), 0);
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    let re_requests = backend.relocation_requests.load(Ordering::Relaxed);
    assert!(re_requests >= 1, "no re-request within one period");
    let record = fuel
        .finish(Duration::from_secs(2), Duration::from_millis(1_250))
        .await;
    assert!(record["joined"].as_bool().unwrap(), "{record}");
    assert_eq!(record["requests"].as_u64(), Some(re_requests), "{record}");
    assert_eq!(
        record["seconds_request_to_release"].as_f64(),
        Some(1.25),
        "{record}"
    );
    assert_eq!(record["retry_seconds"].as_u64(), Some(1), "{record}");
    tokio::time::sleep(Duration::from_millis(1_250)).await;
    assert_eq!(
        backend.relocation_requests.load(Ordering::Relaxed),
        re_requests,
        "a stopped stream keeps requesting"
    );
}

/// A stream dropped without `finish` (the runner's early `?` returns) must not
/// keep running to `max_seconds`: the drop sets the stop flag and aborts the
/// task, which cancels a backend call already in flight. The parked call is the
/// seam: a stop flag alone cannot end it, so its dropped guard is the proof.
#[tokio::test]
async fn dropping_the_stream_cancels_its_parked_put_without_finish() {
    let backend = FuelBackend::parking();
    let fuel = RelocationFuel::start(
        Arc::clone(&backend),
        [6; 16],
        8,
        100,
        64,
        1.0,
        plan(1_000, 60, 10),
    )
    .await;
    // The first rewrite completed inside `start`; the loop's next one parks.
    for _ in 0..64 {
        if backend.parked.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        backend.parked.load(Ordering::Relaxed),
        1,
        "the loop never parked"
    );
    let puts = backend.puts.load(Ordering::Relaxed);
    assert_eq!(backend.parked_dropped.load(Ordering::Relaxed), 0);
    drop(fuel);
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        backend.parked_dropped.load(Ordering::Relaxed),
        1,
        "the parked call survived the drop"
    );
    assert_eq!(
        backend.puts.load(Ordering::Relaxed),
        puts,
        "a rewrite started after the drop"
    );
}

/// Cancelling the caller's future inside `finish`'s join (a deadline, a
/// `select!`) drops the whole `RelocationFuel`, so the parked backend call must
/// still be cancelled: the handle is awaited in place, never taken. A zero-time
/// outer timeout polls `finish` once — far enough to reach the join — and then
/// drops it, with no sleep in the premise.
#[tokio::test]
async fn cancelling_the_join_still_aborts_the_stream() {
    let backend = FuelBackend::parking();
    let fuel = RelocationFuel::start(
        Arc::clone(&backend),
        [2; 16],
        4,
        100,
        64,
        1.0,
        plan(1_000, 60, 10),
    )
    .await;
    for _ in 0..64 {
        if backend.parked.load(Ordering::Relaxed) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        backend.parked.load(Ordering::Relaxed),
        1,
        "the loop never parked"
    );
    let puts = backend.puts.load(Ordering::Relaxed);
    let cancelled = tokio::time::timeout(
        Duration::ZERO,
        fuel.finish(Duration::from_secs(60), Duration::ZERO),
    )
    .await;
    assert!(
        cancelled.is_err(),
        "the join returned while a put was parked"
    );
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        backend.parked_dropped.load(Ordering::Relaxed),
        1,
        "cancellation during the join detached the stream"
    );
    assert_eq!(
        backend.puts.load(Ordering::Relaxed),
        puts,
        "a rewrite started after the cancellation"
    );
}
