//! Control-region V4 and byte-cadence contract (plan S4 acceptance).

use super::{
    HybridControlRegion, HybridRegionStore, MAX_CONTROL_REGION_BYTES, MAX_REGION_STREAMS,
    StreamFront, is_stale, max_flushed_at, max_region_cells, owner_counter_backwards, snapshot_due,
    stale_threshold, stored_snapshot_offsets,
};
use crate::cell::CellId;
use crate::container::LargeTableContainer;
use crate::control::ControlRegion;
use crate::db::CONTROL_REGION_FILE;
use crate::distributed::types::BatchVersion;
use crate::key_shape::{KeyShape, KeyType, UniformKeyConfig};
use crate::wal::position::IndexCut;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn v(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

fn shape() -> KeyShape {
    KeyShape::new_single(8, 1, KeyType::Uniform(UniformKeyConfig::new(1)))
}

fn streams() -> Vec<StreamFront> {
    vec![
        StreamFront {
            partition: 0,
            generation: 4,
            replay_from: 1_024,
        },
        StreamFront {
            partition: 1,
            generation: 9,
            replay_from: 2_048,
        },
    ]
}

fn cell_bytes() -> BTreeMap<(u8, CellId), u64> {
    let mut cells = BTreeMap::new();
    cells.insert((0, CellId::Integer(3)), 4_096);
    cells.insert((2, CellId::Integer(7)), 8_192);
    cells
}

fn region(cut: BatchVersion) -> HybridControlRegion {
    let shape = shape();
    HybridControlRegion::new(
        LargeTableContainer { data: Vec::new() },
        cut,
        &shape,
        streams(),
        cell_bytes(),
    )
}

fn path() -> (tempdir::TempDir, PathBuf) {
    let dir = tempdir::TempDir::new("hybrid-region").expect("temp dir");
    let path = dir.path().join(CONTROL_REGION_FILE);
    (dir, path)
}

#[test]
fn v4_round_trips_with_streams_and_cell_bytes() {
    let (_dir, path) = path();
    let mut store = HybridRegionStore::new(path.clone());
    store.store(region(v(2, 9))).expect("store");

    let (reopened, loaded) = HybridRegionStore::open(path, &shape()).expect("open");
    let loaded = loaded.expect("region exists");
    assert_eq!(loaded.last_position(), v(2, 9));
    assert_eq!(loaded.streams, streams());
    assert_eq!(loaded.cell_bytes, cell_bytes());
    assert_eq!(reopened.last_position(), v(2, 9));
    assert_eq!(reopened.stream_fronts(), streams().as_slice());
}

#[test]
fn missing_file_is_not_an_error() {
    let (_dir, path) = path();
    assert!(HybridRegionStore::read(&path).expect("read").is_none());
}

#[test]
fn legacy_unversioned_bytes_are_refused() {
    let (_dir, path) = path();
    fs::write(&path, [0xABu8; 32]).expect("write");
    let error = HybridRegionStore::read(&path).expect_err("must refuse");
    assert!(error.to_string().contains("not a versioned"), "{error}");
}

#[test]
fn native_v3_is_refused_with_a_named_error() {
    let (_dir, path) = path();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    bytes.extend_from_slice(&3u32.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    fs::write(&path, bytes).expect("write");

    let error = HybridRegionStore::read(&path).expect_err("must refuse V3");
    let message = error.to_string();
    assert!(message.contains("version 3"), "{message}");
    assert!(message.contains("requires version 4"), "{message}");
}

#[test]
fn newer_versions_are_refused() {
    let (_dir, path) = path();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&u64::MAX.to_le_bytes());
    bytes
        .extend_from_slice(&(<BatchVersion as IndexCut>::CONTROL_REGION_VERSION + 1).to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    fs::write(&path, bytes).expect("write");

    let error = HybridRegionStore::read(&path).expect_err("must refuse newer");
    assert!(error.to_string().contains("version 5"), "{error}");
}

#[test]
fn the_native_v3_reader_refuses_v4() {
    // The other direction of the I9 format gate: the native reader must
    // refuse a hybrid directory with a typed error instead of reinterpreting
    // its rows.
    let (_dir, path) = path();
    let mut store = HybridRegionStore::new(path.clone());
    store.store(region(v(1, 1))).expect("store");
    let error = ControlRegion::<crate::wal::position::LastProcessed>::read(&path, &shape())
        .expect_err("native must refuse V4");
    let message = error.to_string();
    assert!(
        message.contains(crate::control::CONTROL_REGION_VERSION_MISMATCH),
        "{message}"
    );
    assert!(message.contains("file has version 4"), "{message}");
}

#[test]
fn regressing_store_is_rejected_before_writing() {
    let (_dir, path) = path();
    let mut store = HybridRegionStore::new(path.clone());
    store.store(region(v(2, 5))).expect("store");
    let error = store
        .store(region(v(2, 4)))
        .expect_err("must reject regression");
    assert!(error.to_string().contains("regressed"), "{error}");
    // The previous region remains readable.
    let loaded = HybridRegionStore::read(&path)
        .expect("read")
        .expect("present");
    assert_eq!(loaded.last_position(), v(2, 5));
}

#[test]
fn after_rename_hook_runs_before_the_directory_sync() {
    let (_dir, path) = path();
    let mut store = HybridRegionStore::new(path.clone());
    let seen = Arc::new(AtomicBool::new(false));
    let hook_seen = Arc::clone(&seen);
    let hook_path = path.clone();
    store.set_after_rename_hook(Box::new(move || {
        assert!(hook_path.exists(), "renamed file must exist at the hook");
        hook_seen.store(true, Ordering::Release);
    }));
    store.store(region(v(1, 2))).expect("store");
    assert!(seen.load(Ordering::Acquire));
}

#[test]
fn stored_offsets_sum_saturating() {
    assert_eq!(stored_snapshot_offsets(&streams()), 3_072);
    assert_eq!(stored_snapshot_offsets(&[]), 0);
    let huge = vec![
        StreamFront {
            partition: 0,
            generation: 1,
            replay_from: u64::MAX,
        },
        StreamFront {
            partition: 1,
            generation: 1,
            replay_from: 5,
        },
    ];
    assert_eq!(stored_snapshot_offsets(&huge), u64::MAX);
}

#[test]
fn root_trigger_is_strictly_greater_than_the_threshold() {
    assert!(!snapshot_due(100, 20, 80), "exactly at the threshold");
    assert!(snapshot_due(101, 20, 80));
    assert!(
        !snapshot_due(10, 20, 0),
        "stored above current saturates to zero distance, never due"
    );
}

#[test]
fn stale_threshold_matches_native_arithmetic() {
    let threshold = stale_threshold(1_000, 300);
    assert_eq!(threshold, 700);
    assert!(is_stale(700, threshold), "inclusive, as native");
    assert!(!is_stale(701, threshold));
    assert!(is_stale(0, threshold), "a never-flushed cell is stale");
    assert_eq!(stale_threshold(100, 300), 0, "saturating");
}

#[test]
fn max_flushed_at_seeds_the_cadence_floor() {
    assert_eq!(max_flushed_at(&cell_bytes()), 8_192);
    assert_eq!(max_flushed_at(&BTreeMap::new()), 0);
}

#[test]
fn backwards_owner_counter_has_the_exact_diagnostic() {
    let error = owner_counter_backwards(3, 500, 400);
    assert_eq!(
        error.to_string(),
        "owner byte counter went backwards: stream 3, flushed_at 500, sum 400",
    );
}

#[test]
fn verify_shape_uses_the_native_verifier() {
    let region = region(v(1, 1));
    region.verify_shape(&shape());
    assert_eq!(
        region.region.keyspace_names().first().map(String::as_str),
        Some("root"),
    );
}

#[test]
#[should_panic(expected = "Keyspace mismatch")]
fn verify_shape_rejects_a_different_keyspace_name() {
    let shape = shape();
    let region_with_entry = HybridControlRegion::new(
        LargeTableContainer {
            data: vec![BTreeMap::new()],
        },
        v(1, 1),
        &shape,
        streams(),
        cell_bytes(),
    );
    let mut builder = crate::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("other", 8, 1, KeyType::Uniform(UniformKeyConfig::new(1)));
    let other = builder.build();
    region_with_entry.verify_shape(&other);
}

#[test]
fn open_extends_a_region_for_newly_declared_keyspaces() {
    let (_dir, path) = path();
    let stored_shape = shape();
    let mut store = HybridRegionStore::new(path.clone());
    store
        .store(HybridControlRegion::new(
            LargeTableContainer {
                data: vec![BTreeMap::new()],
            },
            v(1, 1),
            &stored_shape,
            streams(),
            cell_bytes(),
        ))
        .expect("store");

    let mut builder = crate::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("root", 8, 1, KeyType::Uniform(UniformKeyConfig::new(1)));
    builder.add_key_space("added", 8, 1, KeyType::Uniform(UniformKeyConfig::new(1)));
    let wider = builder.build();

    let (_store, loaded) = HybridRegionStore::open(path, &wider).expect("open");
    let loaded = loaded.expect("region");
    assert_eq!(
        loaded.region.snapshot().data.len(),
        2,
        "the newly declared keyspace gets an empty cell map",
    );
    assert_eq!(
        loaded.region.keyspace_names(),
        ["root".to_owned(), "added".to_owned()].as_slice(),
    );
    assert_eq!(loaded.last_position(), v(1, 1), "the cut is unchanged");
}

#[test]
#[should_panic(expected = "Keyspace mismatch")]
fn open_refuses_a_region_from_a_different_keyspace() {
    let (_dir, path) = path();
    let mut store = HybridRegionStore::new(path.clone());
    store
        .store(HybridControlRegion::new(
            LargeTableContainer {
                data: vec![BTreeMap::new()],
            },
            v(1, 1),
            &shape(),
            streams(),
            cell_bytes(),
        ))
        .expect("store");

    let mut builder = crate::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("other", 8, 1, KeyType::Uniform(UniformKeyConfig::new(1)));
    let _ = HybridRegionStore::open(path, &builder.build());
}

#[test]
fn validate_bounds_rejects_streams_and_cells() {
    let stored = region(v(1, 1));
    assert!(stored.validate_bounds(2, 2).is_ok());

    let error = stored.validate_bounds(1, 2).expect_err("too many streams");
    assert!(error.to_string().contains("owner streams"), "{error}");

    let error = stored.validate_bounds(2, 1).expect_err("too many cells");
    assert!(error.to_string().contains("cells"), "{error}");
}

fn region_with_streams(count: usize) -> HybridControlRegion {
    let mut over = region(v(1, 1));
    over.streams = (0..count)
        .map(|index| StreamFront {
            partition: 0,
            generation: 1 + index as u64,
            replay_from: 0,
        })
        .collect();
    over
}

#[test]
fn a_region_above_the_stream_bound_is_refused_at_open_and_at_store() {
    let at_bound = region_with_streams(MAX_REGION_STREAMS);
    assert!(at_bound.validate_bounds(MAX_REGION_STREAMS, 2).is_ok());
    let over = region_with_streams(MAX_REGION_STREAMS + 1);
    let error = over
        .validate_bounds(MAX_REGION_STREAMS, 2)
        .expect_err("open refuses it");
    assert!(error.to_string().contains("owner streams"), "{error}");

    // The same bound stops a store, before any byte is written.
    let (_dir, path) = path();
    let mut store = HybridRegionStore::new(path.clone());
    let error = store.store(over).expect_err("store refuses it");
    assert!(error.to_string().contains("owner streams"), "{error}");
    assert!(!path.exists(), "a refused store wrote a region file");
    store
        .store(at_bound)
        .expect("a region at the bound is stored");
}

#[test]
fn max_region_cells_counts_uniform_cells_and_leaves_prefixed_keyspaces_open() {
    assert_eq!(max_region_cells(&shape()), 1);
    let mut builder = crate::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("a", 8, 4, KeyType::Uniform(UniformKeyConfig::new(2)));
    builder.add_key_space("b", 8, 1, KeyType::Uniform(UniformKeyConfig::new(1)));
    assert_eq!(max_region_cells(&builder.build()), 4 * 2 + 1);
    let mut builder = crate::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("p", 8, 1, KeyType::prefix_uniform(1, 0));
    assert_eq!(max_region_cells(&builder.build()), usize::MAX);
}

#[test]
fn an_oversized_region_file_is_rejected_before_reading() {
    let (_dir, path) = path();
    let file = fs::File::create(&path).expect("create");
    file.set_len(MAX_CONTROL_REGION_BYTES + 1)
        .expect("sparse len");
    let error = HybridRegionStore::read(&path).expect_err("too large");
    assert!(error.to_string().contains("above the"), "{error}");
}
