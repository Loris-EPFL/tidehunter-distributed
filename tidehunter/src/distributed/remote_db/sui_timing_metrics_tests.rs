//! The Sui scrape path may take the fallible timing visitor when a counter
//! guard is unavailable. It must still export every counter the infallible
//! visitor exports, so a scrape never silently loses gauges between attempts.

use super::pipeline_tests::Fixture;
use crate::distributed::sui::Db;
use crate::key_shape::{KeyShape, KeySpaceConfig, KeyType};
use std::collections::BTreeMap;
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
