//! Differential oracle: one committed Sui trace through the native engine and
//! the hybrid engine must return the same bytes for every read and end in the
//! same state, before and after a restart.
//!
//! Native is the single-node engine of `tidehunter::db`; hybrid is the leased
//! coordinator plus owner processes of this module. Both replay the same
//! prepared workload, then read every key the workload names (written, read but
//! never written, and a deterministic unseen key per table). A per-key
//! difference is a failure that names the first differing keys. The module is
//! a sibling test file of `replay/tests.rs`; the Rust Book states the placement
//! rule for a nested submodule file (#4a01ba, line:4690-4700).
#![cfg(all(feature = "test-utils", target_os = "linux"))]

use super::{BTreeMap, BTreeSet, Duration, Instant, Path, PathBuf, fixture_at, root, trace};
use crate::backends::native::replay::NativeReplayBackend;
use crate::backends::replay::{FinalRows, ReplayBackend, verify_rows};
use crate::mainnet_replay::{Counts, Prepared, execute, prepare};
use crate::mainnet_trace::Row;
use blake2::{Blake2b512, Digest};
use std::fmt::Write as _;
use std::sync::Arc;
use tidehunter::minibytes::Bytes;

/// Two repetitions of the committed fixture: the same workload as the CI
/// correctness replay, so the printed counters are comparable.
const REPETITIONS: u64 = 2;
const CALLERS: usize = 4;
const CASE_TIMEOUT_MS: u64 = 30_000;
const CASE_DEADLINE: Duration = Duration::from_secs(120);

type Key = (u16, Bytes);
type Reads = BTreeMap<Key, Option<Bytes>>;

fn hex(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut rendered, "{byte:02x}").expect("writing to a String cannot fail");
    }
    rendered
}

/// Final rows of the workload: every preload row, then every batch row in
/// replay order. A later row for one key replaces the earlier value.
fn expected_final_rows(prepared: &[Prepared]) -> FinalRows {
    let mut expected = FinalRows::new();
    for row in prepared.iter().flat_map(|repetition| &repetition.preload) {
        expected.insert((row.table, row.key.clone()), row.clone());
    }
    for row in prepared
        .iter()
        .flat_map(|repetition| repetition.batches.iter().flat_map(|batch| &batch.entries))
    {
        expected.insert((row.table, row.key.clone()), row.clone());
    }
    expected
}

fn written_keys(prepared: &[Prepared]) -> BTreeSet<Key> {
    prepared
        .iter()
        .flat_map(|repetition| {
            repetition
                .preload
                .iter()
                .chain(repetition.batches.iter().flat_map(|batch| &batch.entries))
        })
        .map(|row: &Row| (row.table, row.key.clone()))
        .collect()
}

/// Every key the workload names: written rows and rows that the trace only reads.
fn workload_keys(prepared: &[Prepared]) -> BTreeSet<Key> {
    let mut keys = written_keys(prepared);
    for read in prepared
        .iter()
        .flat_map(|repetition| repetition.reads.iter().flatten())
    {
        keys.insert((read.row.table, read.row.key.clone()));
    }
    keys
}

/// Read-absent set: keys the trace reads but never writes, plus one key per
/// table that is derived from a written key and appears nowhere in the trace.
/// Both engines must answer every one of them with `None`. Control tables use
/// the empty singleton key, so a table with no nonempty key adds nothing.
fn absent_keys(prepared: &[Prepared], keys: &BTreeSet<Key>) -> BTreeSet<Key> {
    let written = written_keys(prepared);
    let mut absent: BTreeSet<Key> = keys.difference(&written).cloned().collect();
    let mut tables: BTreeSet<u16> = written.iter().map(|(table, _)| *table).collect();
    while let Some(table) = tables.pop_first() {
        let (_, key) = written
            .iter()
            .find(|(candidate, _)| *candidate == table)
            .expect("every table in the set has one written key");
        let mut unseen = key.to_vec();
        let Some(last) = unseen.last_mut() else {
            continue;
        };
        *last ^= 0xff;
        let unseen = Bytes::from(unseen);
        if !written.contains(&(table, unseen.clone())) && !keys.contains(&(table, unseen.clone())) {
            absent.insert((table, unseen));
        }
    }
    absent
}

/// Read every named key through one engine, including the absent ones.
fn read_everything<B: ReplayBackend + ?Sized>(
    backend: &B,
    keys: &BTreeSet<Key>,
    absent: &BTreeSet<Key>,
) -> Reads {
    let mut reads = Reads::new();
    for (table, key) in keys.iter().chain(absent.iter()) {
        reads.insert(
            (*table, key.clone()),
            backend.get(*table, key).unwrap_or_else(|error| {
                panic!(
                    "read failed for table {table}, key {}: {error}",
                    hex(&key[..])
                )
            }),
        );
    }
    for (table, key) in absent {
        assert_eq!(
            reads[&(*table, key.clone())],
            None,
            "absent key must not be returned: table {table}, key {}",
            hex(&key[..])
        );
    }
    reads
}

/// Content address of one read map: table, key, presence and value bytes.
fn reads_digest(reads: &Reads) -> String {
    let mut hash = Blake2b512::new();
    for ((table, key), value) in reads {
        hash.update(table.to_be_bytes());
        hash.update((key.len() as u64).to_be_bytes());
        hash.update(&key[..]);
        match value {
            Some(bytes) => {
                hash.update([1]);
                hash.update((bytes.len() as u64).to_be_bytes());
                hash.update(&bytes[..]);
            }
            None => hash.update([0]),
        }
    }
    hex(&hash.finalize())
}

/// Fail loudly on any per-key difference and print the first differing keys.
fn assert_equal_reads(label: &str, left: &Reads, right: &Reads) {
    fn preview(value: &Option<Bytes>) -> Option<String> {
        value
            .as_ref()
            .map(|bytes| format!("{}B {}", bytes.len(), hex(&bytes[..bytes.len().min(8)])))
    }
    let mut differences = Vec::new();
    for ((table, key), value) in left {
        let other = right.get(&(*table, key.clone()));
        if other != Some(value) {
            differences.push(format!(
                "table {table} key {} left={:?} right={:?}",
                hex(&key[..]),
                preview(value),
                other.and_then(preview)
            ));
        }
    }
    for (table, key) in right.keys() {
        if !left.contains_key(&(*table, key.clone())) {
            differences.push(format!("table {table} key {} only on the right", hex(key)));
        }
    }
    let shown = differences.len().min(8);
    assert!(
        differences.is_empty(),
        "{label}: {} differing reads; first {shown}: {:?}",
        differences.len(),
        &differences[..shown]
    );
}

fn counts_json(counts: &Counts) -> serde_json::Value {
    serde_json::json!({
        "store_gets": counts.store_gets,
        "overlay_demands": counts.overlay_demands,
        "put_members": counts.put_members,
        "write_calls": counts.write_calls,
        "checkpoints": counts.checkpoints,
        "transactions": counts.transactions,
    })
}

/// The native overlay counter is not part of the hybrid contract (CI audit
/// 2026-11-20, item 3.3); every other counter is workload identity and must match.
const SHARED_COUNTERS: [&str; 5] = [
    "store_gets",
    "put_members",
    "write_calls",
    "checkpoints",
    "transactions",
];

fn deadline() -> Instant {
    Instant::now() + CASE_DEADLINE
}

fn native_options(root: &Path) -> serde_json::Value {
    serde_json::json!({
        "path": root.join("native"),
        "native_wal_geometry": "small",
        "native_shards": 1,
        "durability": "page_cache",
    })
}

/// Remove the fixture tree even when an assertion fails: a leaked tree holds
/// WAL and coordinator files and fills the shared disk every agent builds on.
struct Cleanup(PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_and_hybrid_replays_return_equal_reads_and_equal_final_state() {
    let root = root();
    let _cleanup = Cleanup(root.clone());
    let trace = trace();
    let prepared = Arc::new(prepare(&trace, REPETITIONS).unwrap());
    let expected = expected_final_rows(&prepared);
    let keys = workload_keys(&prepared);
    let absent = absent_keys(&prepared, &keys);
    assert!(
        !keys.is_empty() && !absent.is_empty(),
        "the oracle needs written and absent keys"
    );

    // Native engine: replay, read and verify, restart, read and verify.
    let options = native_options(&root);
    let native = Arc::new(NativeReplayBackend::open(&options, &trace.tables).unwrap());
    let native_counts = execute(
        Arc::clone(&native),
        &trace,
        Arc::clone(&prepared),
        CALLERS,
        deadline(),
        CASE_TIMEOUT_MS,
    )
    .await
    .unwrap();
    native.check_background_health().unwrap();
    let native_reads = read_everything(&*native, &keys, &absent);
    let native_final = verify_rows(&*native, &expected, deadline()).unwrap();
    drop(native);
    let native = Arc::new(NativeReplayBackend::open(&options, &trace.tables).unwrap());
    let native_restart = read_everything(&*native, &keys, &absent);
    let native_restart_final = verify_rows(&*native, &expected, deadline()).unwrap();
    drop(native);

    // Hybrid engine: same prepared workload over the leased ordinary-file fixture.
    let mut endpoints = BTreeMap::new();
    let (hybrid, owners) = fixture_at(&root, &trace, &mut endpoints);
    let hybrid_counts = execute(
        Arc::clone(&hybrid),
        &trace,
        Arc::clone(&prepared),
        CALLERS,
        deadline(),
        CASE_TIMEOUT_MS,
    )
    .await
    .unwrap();
    hybrid.check_background_health().unwrap();
    let hybrid_reads = read_everything(&*hybrid, &keys, &absent);
    let hybrid_final = verify_rows(&*hybrid, &expected, deadline()).unwrap();
    drop(hybrid);
    drop(owners);

    // Restart the coordinator and every owner on the recorded addresses.
    let (hybrid, owners) = fixture_at(&root, &trace, &mut endpoints);
    let hybrid_restart = read_everything(&*hybrid, &keys, &absent);
    let hybrid_restart_final = verify_rows(&*hybrid, &expected, deadline()).unwrap();
    hybrid.check_background_health().unwrap();
    drop(hybrid);
    drop(owners);

    assert_equal_reads("hybrid vs native", &native_reads, &hybrid_reads);
    assert_equal_reads("hybrid restart vs hybrid", &hybrid_reads, &hybrid_restart);
    assert_equal_reads("native restart vs native", &native_reads, &native_restart);
    assert_equal_reads("native vs hybrid restart", &native_reads, &hybrid_restart);

    // Equal final state: the same trace population, byte for byte, from both
    // engines and from both restarts. `verify_rows` hashes table, key and value.
    for field in ["content_blake2b512", "verified_rows", "table_rows"] {
        assert_eq!(
            native_final[field], hybrid_final[field],
            "final {field} differs"
        );
        assert_eq!(
            native_restart_final[field], hybrid_restart_final[field],
            "restart final {field} differs"
        );
        assert_eq!(
            hybrid_final[field], hybrid_restart_final[field],
            "hybrid restart changed {field}"
        );
    }
    assert_eq!(native_final["full_values"], serde_json::json!(true));
    assert_eq!(
        native_final["verified_rows"],
        serde_json::json!(expected.len())
    );

    let native_counters = counts_json(&native_counts);
    let hybrid_counters = counts_json(&hybrid_counts);
    for counter in SHARED_COUNTERS {
        assert_eq!(
            native_counters[counter], hybrid_counters[counter],
            "workload counter {counter} differs between engines"
        );
    }

    let receipt = serde_json::json!({
        "repetitions": REPETITIONS,
        "keys": keys.len(),
        "absent_keys": absent.len(),
        "native": {
            "read_digest": reads_digest(&native_reads),
            "restart_read_digest": reads_digest(&native_restart),
            "final": native_final,
            "restart_final": native_restart_final,
            "counts": native_counters,
        },
        "hybrid": {
            "read_digest": reads_digest(&hybrid_reads),
            "restart_read_digest": reads_digest(&hybrid_restart),
            "final": hybrid_final,
            "restart_final": hybrid_restart_final,
            "counts": hybrid_counters,
        },
    });
    println!(
        "native-hybrid-oracle-receipt {}",
        serde_json::to_string(&receipt).unwrap()
    );
}
