use super::*;
use crate::backend::{BackendInfo, BackendResult, BatchAtomicity};
use crate::workload::{key_bytes, value_bytes};
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

/// Records every rewrite; fails every put at or after `fail_after`.
struct FuelBackend {
    puts: AtomicU64,
    writes: Mutex<Vec<(tidehunter::minibytes::Bytes, tidehunter::minibytes::Bytes)>>,
    fail_after: Option<u64>,
}

impl FuelBackend {
    fn new(fail_after: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            puts: AtomicU64::new(0),
            writes: Mutex::new(Vec::new()),
            fail_after,
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
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "owner write failed",
            )
            .into());
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

fn plan(puts_per_second: u32, max_seconds: u32) -> RelocationFuelConfig {
    RelocationFuelConfig {
        puts_per_second,
        max_seconds,
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
        plan(50, 5),
    )
    .await;
    assert!(backend.puts.load(Ordering::Relaxed) >= 1);
    tokio::time::sleep(Duration::from_millis(220)).await;
    let record = fuel.finish(Duration::from_secs(2)).await;
    let puts = record["puts"].as_u64().unwrap();
    let seconds = record["seconds_request_to_release"].as_f64().unwrap();
    assert!(record["joined"].as_bool().unwrap(), "{record}");
    assert_eq!(record["errors"].as_u64(), Some(0), "{record}");
    assert_eq!(record["puts_per_second"].as_u64(), Some(50), "{record}");
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
        plan(1_000, 1),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1_400)).await;
    let record = fuel.finish(Duration::from_millis(200)).await;
    assert!(record["joined"].as_bool().unwrap(), "{record}");
    assert!(
        record["seconds_request_to_release"].as_f64().unwrap() >= 1.0,
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
        plan(500, 5),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let record = fuel.finish(Duration::from_secs(2)).await;
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
