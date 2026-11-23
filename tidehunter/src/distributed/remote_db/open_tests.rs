//! Engine-level open tests: a real owner fixture, a real write path and real
//! reopens. They cover what the table-level tests cannot: that an open replays
//! only the retained **older** generations (the serving generation is new, its
//! owner stream is empty and the owner refuses recovery pages for an Active
//! generation), that the address generation a write is indexed under is the
//! manifest's fencing generation, and that Sui's native index configuration
//! reaches the table.

use super::pipeline_tests::Fixture;
use super::tests::{checkpoint_put, fail_stop_without_checkpoint};
use super::*;

/// A cut above every version of every epoch: the lookup answers "what is
/// indexed now", including a batch the publication cut has not covered yet.
fn newest_cut() -> BatchVersion {
    BatchVersion {
        epoch: u64::MAX,
        sequence: u64::MAX,
    }
}

const KEYS: u32 = 8;

fn value_of(key: u32) -> [u8; 32] {
    [key as u8 + 1; 32]
}

fn write_keys(fixture: &Fixture, db: &RemoteDatabase, first_seed: u64) {
    for key in 0..KEYS {
        db.write_batch(
            fixture.operation(key % 2, first_seed + u64::from(key)),
            vec![checkpoint_put(key, key as u8 + 1)],
        )
        .expect("write");
    }
}

fn assert_reads_back(db: &mut RemoteDatabase) {
    for key in 0..KEYS {
        assert_eq!(
            db.get(KeySpace(0), &key.to_be_bytes())
                .expect("read")
                .expect("the written key survived the reopen")
                .as_ref(),
            &value_of(key),
            "key {key}"
        );
    }
}

fn manifest_of(fixture: &Fixture) -> AuthorityManifest {
    read_metadata(&fixture.temp.path().join("authority").join(MANIFEST)).expect("manifest")
}

/// Fresh open, write, clean shutdown, reopen, read: the reopen replays the
/// older generation from the stored region and never asks for the pages of its
/// own (empty, Active) generation. The address of every indexed value carries
/// the fencing generation that wrote it, which is the manifest's generation:
/// publication, relocation and retirement key streams by that equality.
fn open_write_reopen_read(fixture: Fixture) {
    let db = fixture.open();
    let writing = db.epoch();
    write_keys(&fixture, &db, 1);
    for key in 0..KEYS {
        let position = db
            .core
            .table
            .get(KeySpace(0), &key.to_be_bytes(), newest_cut())
            .expect("lookup")
            .expect("indexed");
        assert_eq!(
            position.address.segment_generation, writing,
            "an address carries the fencing generation that wrote it"
        );
    }
    let served: Vec<u64> = manifest_of(&fixture)
        .generations
        .iter()
        .filter(|entry| entry.state == GenerationState::Served)
        .map(|entry| entry.generation)
        .collect();
    assert_eq!(
        served,
        vec![writing],
        "the written generation is in the manifest"
    );
    db.shutdown().expect("shutdown");
    drop(db);

    let mut db = fixture.open();
    assert!(db.epoch() > writing, "a reopen is a new fencing generation");
    assert_reads_back(&mut db);
    assert!(
        db.recovery_stats().checkpoint_loaded,
        "the clean shutdown stored a control region"
    );
    // New writes land in the new generation and read next to the old ones.
    db.write_batch(fixture.operation(0, 100), vec![checkpoint_put(KEYS, 9)])
        .expect("write after reopen");
    let position = db
        .core
        .table
        .get(KeySpace(0), &KEYS.to_be_bytes(), newest_cut())
        .expect("lookup")
        .expect("indexed");
    assert_eq!(position.address.segment_generation, db.epoch());
    assert_reads_back(&mut db);
}

#[test]
fn open_write_reopen_read_on_the_native_tier() {
    open_write_reopen_read(Fixture::new());
}

#[test]
fn open_write_reopen_read_on_the_synchronous_tier() {
    open_write_reopen_read(Fixture::new().synchronous());
}

/// The previous serving generation is still Active at the crash: no shutdown
/// snapshot, so the reopen has no stored front for it and replays it from its
/// start. The owner seals it at the new fence; asking for its pages before
/// that, or asking for the new generation's, fails with "recovery requires
/// sealed immutable epoch".
fn crash_then_reopen(fixture: Fixture) {
    let db = fixture.open();
    write_keys(&fixture, &db, 1);
    fail_stop_without_checkpoint(db);
    let mut db = fixture.open();
    assert_reads_back(&mut db);
    assert!(
        !db.recovery_stats().checkpoint_loaded,
        "no region was stored before the crash"
    );
    assert!(
        db.recovery_stats().tail_batches >= u64::from(KEYS),
        "every acknowledged batch of the Active generation replays: {}",
        db.recovery_stats().tail_batches
    );
}

#[test]
fn a_crash_with_an_active_generation_replays_it_on_the_native_tier() {
    crash_then_reopen(Fixture::new());
}

#[test]
fn a_crash_with_an_active_generation_replays_it_on_the_synchronous_tier() {
    crash_then_reopen(Fixture::new().synchronous());
}

/// An older Sealed generation (a clean close) and an Aborted reservation whose
/// owner stream is Abandoned (a coordinator that died after fencing one owner)
/// are both retained: the next open replays the first, proves the second empty
/// from its complete fenced ledger, and serves a third generation.
#[test]
fn sealed_and_abandoned_older_generations_both_reopen() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let sealed = db.epoch();
    write_keys(&fixture, &db, 1);
    db.shutdown().expect("shutdown");
    drop(db);

    assert!(
        RemoteDatabase::open_inner_local_fixture(
            &fixture.temp.path().join("authority"),
            &fixture.shape,
            fixture.config.clone(),
            Some(OpenInterruption::AfterFences(1)),
        )
        .is_err(),
        "the injected partial fence fails the open"
    );
    let aborted: Vec<u64> = manifest_of(&fixture)
        .generations
        .iter()
        .filter(|entry| entry.state == GenerationState::Reserved)
        .map(|entry| entry.generation)
        .collect();
    assert_eq!(aborted, vec![sealed + 1], "one reservation is left behind");

    let mut db = fixture.open();
    assert_eq!(db.epoch(), sealed + 2);
    assert_reads_back(&mut db);
    let states: Vec<(u64, GenerationState)> = manifest_of(&fixture)
        .generations
        .iter()
        .map(|entry| (entry.generation, entry.state))
        .collect();
    // The reservation was folded to Aborted at this open, and an empty Aborted
    // generation whose owners all prove it holds nothing is retired by the same
    // open's maintenance: it is gone or Aborted, never left Reserved.
    assert!(
        !states.contains(&(sealed + 1, GenerationState::Reserved)),
        "the abandoned reservation is no longer Reserved: {states:?}"
    );
    assert!(
        states.contains(&(sealed, GenerationState::Served))
            && states.contains(&(sealed + 2, GenerationState::Served)),
        "the sealed and the serving generation are both recorded: {states:?}"
    );
    db.write_batch(fixture.operation(0, 200), vec![checkpoint_put(KEYS, 7)])
        .expect("the third generation serves writes");
}

/// Sui's native values reach the table only through
/// `RemoteDatabaseConfig::index`, built by `with_native_index`; a default carrier
/// would silently run Sui with the defaults.
#[test]
fn non_default_native_values_reach_the_table() {
    let fixture = Fixture::new();
    let native = crate::config::Config {
        max_dirty_keys: 77,
        snapshot_written_bytes: 123 * 1024 * 1024,
        snapshot_unload_threshold: 100 * 1024 * 1024,
        relocation_max_reclaim_pct: 25,
        num_flusher_threads: 2,
        ..crate::config::Config::default()
    };
    let config = fixture.config.clone().with_native_index(&native);
    let db = fixture
        .try_open_with(config)
        .expect("open with native values");
    let table = db.core.table.config();
    assert_eq!(table.max_dirty_keys, 77);
    assert_eq!(table.snapshot_written_bytes, 123 * 1024 * 1024);
    assert_eq!(table.snapshot_unload_threshold, 100 * 1024 * 1024);
    assert_eq!(table.relocation_max_reclaim_pct, 25);
    assert_eq!(table.num_flusher_threads, 2);
    assert_eq!(
        db.core.config.index.relocation_max_reclaim_pct, 25,
        "the relocation passes read the same single value"
    );
}

#[test]
fn the_carrier_parses_from_config_and_rejects_unknown_keys() {
    let fixture = Fixture::new();
    let mut value = serde_json::to_value(&fixture.config).expect("config to json");
    value["index"] = serde_json::json!({ "max_dirty_keys": 55, "relocation_max_reclaim_pct": 40 });
    let parsed: RemoteDatabaseConfig = serde_json::from_value(value.clone()).expect("parse");
    assert_eq!(parsed.index.max_dirty_keys, 55);
    assert_eq!(parsed.index.relocation_max_reclaim_pct, 40);
    parsed.validate().expect("valid");

    value["index"] = serde_json::json!({ "max_dirty_keyz": 55 });
    assert!(
        serde_json::from_value::<RemoteDatabaseConfig>(value.clone()).is_err(),
        "an unknown index key fails at parse"
    );
    value["index"] = serde_json::json!({ "relocation_max_reclaim_pct": 101 });
    let over: RemoteDatabaseConfig = serde_json::from_value(value).expect("parse");
    assert!(
        over.validate().is_err(),
        "a percentage over 100 is refused before any filesystem or network access"
    );
}

/// Recovery diagnostics: a database with a stored region reports it and the
/// time it took to load; a database without one reports neither.
#[test]
fn recovery_stats_report_the_control_region_load() {
    let fixture = Fixture::new();
    let db = fixture.open();
    assert!(!db.recovery_stats().checkpoint_loaded);
    assert_eq!(
        db.recovery_stats().checkpoint_load_ns,
        db.core.table.region_load_ns(),
        "the stat is the table's measured load time"
    );
    write_keys(&fixture, &db, 1);
    db.shutdown().expect("shutdown");
    drop(db);
    let db = fixture.open();
    assert!(db.recovery_stats().checkpoint_loaded);
    assert!(db.recovery_stats().checkpoint_load_ns > 0);
}
