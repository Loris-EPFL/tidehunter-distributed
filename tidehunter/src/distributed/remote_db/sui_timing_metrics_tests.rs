//! The Sui scrape path may take the fallible timing visitor when a counter
//! guard is unavailable. It must still export every counter the infallible
//! visitor exports, so a scrape never silently loses gauges between attempts.
//! The same module pins the engine `db_op` observations of the remote Sui
//! adapter to the counts native `Db` records for the same calls.

use super::pipeline_tests::Fixture;
use crate::config::Config;
use crate::distributed::sui::Db;
use crate::key_shape::{KeyIndexing, KeyShape, KeySpaceConfig, KeyType};
use crate::metrics::{Metrics, get_histogram_sum_count};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Liveness bound for a scrape that must observe one complete snapshot; the
/// wait is on the visitor's own success, never on a fixed delay.
const SCRAPE_DEADLINE: Duration = Duration::from_secs(10);

/// Four-byte keys over four uniform cells with a native per-cell value cache:
/// the value reader's hit and miss counters are live in this shape.
fn cached_shape(cache: usize) -> KeyShape {
    KeyShape::new_single_config(
        4,
        4,
        KeyType::uniform(4),
        KeySpaceConfig::new().with_value_cache_size(cache),
    )
}

/// Collect one snapshot of the infallible visitor into a name-keyed map.
fn snapshot(db: &Db) -> BTreeMap<&'static str, u64> {
    let mut counters = BTreeMap::new();
    db.visit_timing_metrics(|name, value| {
        counters.insert(name, value);
    });
    counters
}

#[test]
fn fallible_scrape_exports_every_infallible_counter() {
    let fixture = Fixture::new_with_shape(128, cached_shape(8));
    let db = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open the fixture database");
    let mut batch = db.write_batch();
    batch.write(db.ks("root"), 1u32.to_be_bytes().to_vec(), vec![7u8; 32]);
    batch.commit_traced(1).expect("commit the fixture batch");

    let expected = snapshot(&db);
    // The counters the fallible path must carry too, including the retirement
    // gauge the visitor forwards to its own sink.
    for name in [
        "relocation_copy_deferrals",
        "older_rows_without_end",
        "retained_streams",
        "retained_stream_pressure",
        "retirement_missing_rows",
        "retirement_busy_replies",
        "value_cache_hits",
        "value_cache_misses",
        "value_decode_failures",
    ] {
        assert!(
            expected.contains_key(name),
            "the infallible visitor must export {name}: {expected:?}"
        );
    }
    assert_eq!(
        expected["retirement_missing_rows"], 0,
        "a fresh fixture pass found every stored row: {expected:?}"
    );
    assert_eq!(
        expected["older_rows_without_end"], 0,
        "a fresh fixture stores no row without a durable end: {expected:?}"
    );

    // The fallible visitor refuses a snapshot while a writer holds the
    // metadata-history guard; retry until it hands over a complete one.
    let deadline = Instant::now() + SCRAPE_DEADLINE;
    let observed = loop {
        let mut counters = BTreeMap::new();
        if db
            .try_visit_timing_metrics(|name, value| {
                counters.insert(name, value);
            })
            .is_some()
        {
            break counters;
        }
        assert!(
            Instant::now() < deadline,
            "the fallible visitor never observed a complete snapshot"
        );
        std::thread::yield_now();
    };

    assert_eq!(
        observed.keys().copied().collect::<Vec<_>>(),
        expected.keys().copied().collect::<Vec<_>>(),
        "both visitors must export the same counter set"
    );
    assert_eq!(
        observed["retirement_missing_rows"], 0,
        "the fallible visitor forwards the retirement gauge: {observed:?}"
    );
    db.wait_for_background_threads_to_finish()
        .expect("join the fixture background threads");
}

/// Number of keys the shared Sui call sequence writes and reads.
const ENGINE_OPS_KEYS: u32 = 4;

/// Eight-byte keys on a four-byte index reduction: the adapter's `exists` and
/// `get` take the member-key verification path, as Sui's reduced keyspaces do.
fn reduced_shape() -> KeyShape {
    KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default(),
    )
}

fn engine_op_key(index: u32) -> Vec<u8> {
    let mut key = vec![0_u8; 8];
    key[..4].copy_from_slice(&index.to_be_bytes());
    key
}

/// The calls the counts compare: one batch of puts, one `get`, one `exists`,
/// and one `multi_get` of every written key. Native `multi_get` runs one
/// `Db::get` per key.
fn exercise_engine_ops(db: &Arc<Db>, keys: &[Vec<u8>]) {
    let ks = db.ks("root");
    let mut batch = db.write_batch();
    for (index, key) in keys.iter().enumerate() {
        batch.write(ks, key.clone(), vec![index as u8; 32]);
    }
    batch.commit().expect("commit the fixture batch");
    let first = db.get(ks, &keys[0]).expect("get the first fixture key");
    assert!(
        first.is_some(),
        "the first written key must read back: keys={}",
        keys.len()
    );
    assert!(
        db.exists(ks, &keys[1])
            .expect("exists the second fixture key"),
        "the second written key must exist: keys={}",
        keys.len()
    );
    let values = db.multi_get(ks, keys);
    assert_eq!(
        values.len(),
        keys.len(),
        "multi_get returns one slot per requested key: slots={} keys={}",
        values.len(),
        keys.len()
    );
    for (index, value) in values.iter().enumerate() {
        assert!(
            value
                .as_ref()
                .expect("the fixture multi_get must not fail")
                .is_some(),
            "multi_get slot {index} of {} must be present",
            keys.len()
        );
    }
    let present = db.multi_exists(ks, keys);
    assert_eq!(
        present.len(),
        keys.len(),
        "multi_exists returns one slot per requested key: slots={} keys={}",
        present.len(),
        keys.len()
    );
    for (index, value) in present.iter().enumerate() {
        assert!(
            *value
                .as_ref()
                .expect("the fixture multi_exists must not fail"),
            "multi_exists slot {index} of {} must be present",
            keys.len()
        );
    }
}

/// Observations per `(op, keyspace)` of the three ops the remote adapter
/// records. `write_batch` keeps native's untagged `ks=""`.
fn engine_op_counts(metrics: &Metrics, ks: &str) -> BTreeMap<(&'static str, String), u64> {
    let mut counts = BTreeMap::new();
    for (op, ks_label) in [("get", ks), ("exists", ks), ("write_batch", "")] {
        let histogram = metrics.db_op_mcs.with_label_values(&[op, ks_label]);
        let (_, count) = get_histogram_sum_count(&histogram)
            .unwrap_or_else(|| panic!("db_op {op} for ks {ks_label:?} must be enabled"));
        counts.insert((op, ks_label.to_string()), count);
    }
    counts
}

#[test]
fn remote_engine_op_counts_match_native_db() {
    let shape = reduced_shape();
    let fixture = Fixture::new_with_shape(128, shape.clone());
    let remote = Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open the fixture database");
    let native_metrics = Metrics::new();
    let native_dir =
        tempdir::TempDir::new("sui-engine-op-native").expect("create the native fixture directory");
    let native = Db::open_native(
        native_dir.path(),
        shape,
        Arc::new(Config::small()),
        Arc::clone(&native_metrics),
    )
    .expect("open the native database");

    let keys: Vec<Vec<u8>> = (0..ENGINE_OPS_KEYS).map(engine_op_key).collect();
    exercise_engine_ops(&remote, &keys);
    exercise_engine_ops(&native, &keys);

    let remote_metrics = remote.remote_for_test().hybrid_table().metrics();
    let remote_counts = engine_op_counts(remote_metrics, "root");
    let native_counts = engine_op_counts(&native_metrics, "root");
    assert_eq!(
        remote_counts, native_counts,
        "the remote Sui adapter must record the engine ops native Db records: \
         remote={remote_counts:?} native={native_counts:?}"
    );
    assert_eq!(
        remote_counts[&("get", "root".to_string())],
        5,
        "one get plus one get per multi_get key: {remote_counts:?}"
    );
    assert_eq!(
        remote_counts[&("exists", "root".to_string())],
        5,
        "one exists plus one exists per multi_exists key: {remote_counts:?}"
    );
    assert_eq!(
        remote_counts[&("write_batch", String::new())],
        1,
        "the one committed batch records write_batch once: {remote_counts:?}"
    );
    // `write_batch_times` gets its increment from the same remote timer pair,
    // so it has no count to compare; its value is wall time and is not
    // asserted.

    remote
        .wait_for_background_threads_to_finish()
        .expect("join the remote fixture background threads");
    native
        .wait_for_background_threads_to_finish()
        .expect("join the native fixture background threads");
}
