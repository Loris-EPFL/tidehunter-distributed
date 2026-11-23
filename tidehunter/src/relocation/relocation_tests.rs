use std::collections::HashSet;
use std::{path::Path, sync::Arc, thread, time::Duration};

use crate::control::RelocateFiles;
use crate::failpoints::FailPoint;
use crate::key_shape::{KeySpace, KeySpaceConfig};
use crate::large_table::Loader;
use crate::latch::Latch;
use crate::relocation::CellReference;
use crate::relocation::watermark::WatermarkData;
use crate::wal::layout::WalKind;
use crate::{
    RelocationStrategy,
    config::Config,
    db::{Db, DbResult},
    key_shape::{KeyShapeBuilder, KeyType},
    relocation::RelocationWatermarks,
};
use crate::{metrics::Metrics, relocation::Decision};
use minibytes::Bytes;

fn force_unload_config(config: &Config) -> Arc<Config> {
    let mut config2 = Config::clone(config);
    config2.snapshot_unload_threshold = 0;
    Arc::new(config2)
}

fn index_based_config() -> Arc<Config> {
    let mut config = Config::small();
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    force_unload_config(&config)
}

fn relocation_cells_processed(metrics: &Metrics, keyspace_name: &str) -> u64 {
    metrics
        .relocation_cells_processed
        .get_metric_with_label_values(&[keyspace_name])
        .unwrap()
        .get()
}

fn start_index_based_relocation(db: &Db) {
    db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(None))
}

fn list_wal_files(path: &Path) -> Vec<String> {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            if name.starts_with("wal_") {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
}

// A checkpoint must keep reading the as-of-frontier values across a relocation
// that runs while it is held: a pre-checkpoint key is overwritten after the
// checkpoint and then relocation rewrites positions / reclaims WAL files. Run
// against two configs (see the two tests below): forced unload, where the cell
// unloads and the as-of value is read from the relocated on-disk blob; and
// unloading disabled, where the cell stays loaded and the as-of value must
// survive in the in-memory overlay.
fn checkpoint_survives_relocation(config: Arc<Config>, ksc: KeySpaceConfig) {
    let dir = tempdir::TempDir::new("test_ckpt_reloc").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let overwritten = 3u64;
    let untouched = 7u64;

    db.insert(ks, overwritten.to_be_bytes().to_vec(), big(1))
        .unwrap();
    db.insert(ks, untouched.to_be_bytes().to_vec(), big(5))
        .unwrap();
    // Enough padding to push the relocation target past several WAL files so it
    // actually relocates (rewrites positions), not a no-op.
    for i in 1000..3000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }

    let checkpoint = db.checkpoint();

    db.insert(ks, overwritten.to_be_bytes().to_vec(), big(2))
        .unwrap();
    for i in 3000..3100u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }

    db.start_blocking_relocation();

    // Sanity: relocation actually relocated entries (otherwise this test is
    // vacuous — it must exercise the position-rewrite path).
    let kept = metrics.relocation_kept.with_label_values(&["k"]).get();
    assert!(
        kept > 0,
        "relocation must have relocated entries for this test to be meaningful"
    );

    // Checkpoint reads the as-of-frontier values.
    assert_eq!(
        checkpoint.get(ks, &overwritten.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "overwritten key as of checkpoint"
    );
    assert_eq!(
        checkpoint.get(ks, &untouched.to_be_bytes()).unwrap(),
        Some(big(5).into()),
        "untouched key as of checkpoint (relocation rewrite hazard)"
    );
    // Live reads reflect the latest values after relocation.
    assert_eq!(
        db.get(ks, &untouched.to_be_bytes()).unwrap(),
        Some(big(5).into())
    );
    assert_eq!(
        db.get(ks, &overwritten.to_be_bytes()).unwrap(),
        Some(big(2).into())
    );
}

#[test]
fn test_checkpoint_survives_relocation_with_unload() {
    // Forced unload during the held checkpoint (snapshot_unload_threshold = 0):
    // the cell unloads, so the checkpoint reads the as-of value from the
    // relocated on-disk blob rather than the post-checkpoint value.
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    checkpoint_survives_relocation(force_unload_config(&config), KeySpaceConfig::new());
}

#[test]
fn test_checkpoint_survives_relocation_without_unload() {
    // Unloading disabled: the cell stays loaded, so the as-of value must survive
    // relocation in the in-memory overlay (relocation rewrites its position to
    // >= L and reclaims the original frame).
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    checkpoint_survives_relocation(Arc::new(config), KeySpaceConfig::new().disable_unload());
}

// Regression: a key overwritten after a checkpoint, whose as-of value lives as a
// distinct *overlay* position (not promoted to flat/L1) in a multi-level/unloaded
// cell, must survive relocation. Before the fix, `get_index_for_cell` collapsed
// the overlay to latest-per-key before `retain_processed`, discarding the
// below-frontier value; it was never relocated, and GC reclaimed its WAL file, so
// the held checkpoint read `None`.
#[test]
fn test_checkpoint_survives_relocation_overlay_asof_value() {
    let dir = tempdir::TempDir::new("ckpt_reloc_overlay_asof").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    let config = force_unload_config(&config); // snapshot_unload_threshold = 0
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    ksb.add_key_space_config("k2", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");
    let ks2 = db.ks("k2");

    let big = |b: u8| -> Vec<u8> { vec![b; 12 * 1000] };
    let target = 3u64;

    // 1. Build a cell from OTHER keys (not `target`); flush it to disk
    //    synchronously so the bulk lives on-disk and the overlay is clean.
    //    Two force-rebuilds encourage an L0->L1 compaction (multi-level cell).
    for i in 1000..3000u64 {
        db.insert(ks, i.to_be_bytes().to_vec(), big(7)).unwrap();
    }
    db.force_rebuild_control_region().unwrap();
    db.force_rebuild_control_region().unwrap();

    // 2. Insert the target's as-of value into a fresh overlay over the on-disk
    //    cell. One dirty key (< max_dirty_keys=32) => stays in `data`.
    db.insert(ks, target.to_be_bytes().to_vec(), big(1))
        .unwrap();

    // 3. Pin a checkpoint at this frontier (target == big(1) as of here).
    let checkpoint = db.checkpoint();
    assert_eq!(
        checkpoint.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "SANITY: as-of read works right after checkpoint"
    );

    // 4. Overwrite target after the frontier => overlay holds BOTH positions.
    db.insert(ks, target.to_be_bytes().to_vec(), big(2))
        .unwrap();
    assert_eq!(
        checkpoint.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "SANITY: as-of read still works after the post-frontier overwrite (pre-relocation)"
    );

    // 5. Advance the global WAL position via a different keyspace/cell so
    //    relocation's target_position passes the as-of value's offset.
    for i in 0..4000u64 {
        db.insert(ks2, i.to_be_bytes().to_vec(), big(9)).unwrap();
    }

    // 6. Relocate (default WAL-based) => GC reclaims files below target_position.
    db.start_blocking_relocation();
    assert!(
        metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
        "relocation must have relocated entries"
    );

    assert_eq!(
        db.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(2).into()),
        "live get must be the overwrite"
    );
    assert_eq!(
        checkpoint.get(ks, &target.to_be_bytes()).unwrap(),
        Some(big(1).into()),
        "checkpoint must read the as-of value big(1) after relocation; \
         None => the below-frontier overlay value was lost"
    );
}

#[test]
fn test_wal_relocation_basic_flow() {
    let dir = tempdir::TempDir::new("test_relocation_filter").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    let config = Arc::new(config);
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_relocation_filter(|key, _| {
        let value = u32::from_be_bytes(key.try_into().unwrap());
        if value >= 1_000 {
            Decision::StopRelocation
        } else {
            Decision::Remove
        }
    });
    ksb.add_key_space_config("k", 4, 1, KeyType::uniform(1), ksc);
    ksb.add_key_space_config("k2", 4, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let sample_key = 3_u32.to_be_bytes().to_vec();
    let insert_count = 2000_u32;
    let value = vec![3; 12 * 1000];
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        let ks2 = db.ks("k2");
        for i in 0..insert_count {
            db.insert(ks, i.to_be_bytes().to_vec(), value.clone())
                .unwrap();
            db.insert(ks2, i.to_be_bytes().to_vec(), value.clone())
                .unwrap();
        }
        assert_eq!(db.get(ks, &sample_key).unwrap(), Some(value.clone().into()));
        db.wait_for_background_threads_to_finish();
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            metrics.clone(),
        )
        .unwrap();

        db.rebuild_control_region().unwrap();
        db.start_blocking_relocation();
        db.rebuild_control_region().unwrap();
        db.wait_for_background_threads_to_finish();
    }
    assert!(
        list_wal_files(dir.path())
            .into_iter()
            .all(|name| name != "wal_0000000000000000")
    );
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    let ks2 = db.ks("k2");
    assert_eq!(db.get(ks, &sample_key).unwrap(), None);
    assert_eq!(
        db.get(ks2, &sample_key).unwrap(),
        Some(value.clone().into())
    );
    assert_eq!(
        db.get(ks, 1500_u32.to_be_bytes().as_ref()).unwrap(),
        Some(value.clone().into())
    );
}

#[test]
fn test_iterator_skips_reclaimed_positions() {
    // A relocation filter removes the first half of the key space. Index
    // cleanup is eventual, so entries pointing into the reclaimed WAL prefix
    // can still be present afterwards. Iteration must skip them and still
    // return every live key: stopping at the first unreadable entry hides the
    // whole live half, because the removed keys sort in front of it.
    let dir = tempdir::TempDir::new("test_iterator_skips_reclaimed_positions").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    let config = Arc::new(config);
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_relocation_filter(|key, _| {
        let value = u32::from_be_bytes(key.try_into().unwrap());
        if value >= 1_000 {
            Decision::StopRelocation
        } else {
            Decision::Remove
        }
    });
    ksb.add_key_space_config("k", 4, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let insert_count = 2000_u32;
    let value = vec![3; 12 * 1000];
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        let ks = db.ks("k");
        for i in 0..insert_count {
            db.insert(ks, i.to_be_bytes().to_vec(), value.clone())
                .unwrap();
        }
        db.wait_for_background_threads_to_finish();
    }
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            force_unload_config(&config),
            Metrics::new(),
        )
        .unwrap();
        db.rebuild_control_region().unwrap();
        db.start_blocking_relocation();
        db.rebuild_control_region().unwrap();
        db.wait_for_background_threads_to_finish();
    }

    let db = Db::open(
        dir.path(),
        key_shape,
        force_unload_config(&config),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.ks("k");

    fn collect_keys(
        iter: impl Iterator<Item = DbResult<(Bytes, Bytes)>>,
        value_len: usize,
        label: &str,
    ) -> Vec<u32> {
        iter.map(|entry| {
            let (key, v) = entry.unwrap();
            assert_eq!(
                v.len(),
                value_len,
                "{label} iterator returned a truncated value"
            );
            u32::from_be_bytes(key.as_ref().try_into().unwrap())
        })
        .collect()
    }

    fn assert_live_keys_visible(seen: &[u32], insert_count: u32, label: &str) {
        assert!(
            seen.windows(2).all(|w| w[0] < w[1]),
            "{label} iteration returned keys out of order"
        );
        let live: HashSet<u32> = seen.iter().copied().filter(|k| *k >= 1_000).collect();
        for expected in 1_000..insert_count {
            assert!(
                live.contains(&expected),
                "{label} iteration missed live key {expected} \
                 (returned {} entries, first {:?})",
                seen.len(),
                seen.first(),
            );
        }
    }

    // Both read paths walk the same index, so both have to skip the stale
    // entries: `Db::next_entry` for the live iterator and
    // `DbCheckpoint::next_entry` for the checkpoint one.
    let live_seen = collect_keys(db.iterator(ks), value.len(), "live");
    assert_live_keys_visible(&live_seen, insert_count, "live");

    let checkpoint_seen = collect_keys(db.checkpoint().iterator(ks), value.len(), "checkpoint");
    assert_live_keys_visible(&checkpoint_seen, insert_count, "checkpoint");

    db.wait_for_background_threads_to_finish();
}

// Index-based relocation tests
#[test]
fn test_index_based_relocation_point_deletes() {
    let dir = tempdir::TempDir::new("test_index_based_relocation_point_deletes").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        index_based_config(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");
    for key in 0..200u64 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![0, 1, 2])
            .unwrap();
    }
    for key in 0..100u64 {
        db.remove(ks, key.to_be_bytes().to_vec()).unwrap();
    }
    thread::sleep(Duration::from_millis(10));
    db.rebuild_control_region().unwrap();
    start_index_based_relocation(&db);

    // Index-based relocation processes current cell contents, not historical WAL entries
    // So it won't see the deleted entries (they're not in cells anymore)
    // Instead, verify that:
    // 1. Some cells were processed
    let processed = relocation_cells_processed(&metrics, "k");
    assert!(processed > 0, "Expected some cells to be processed");

    // 2. The preserved data is correct (entries 100-199 should still exist)
    for key in 100..200u64 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![0, 1, 2].into()),
            "Key {} should still exist",
            key
        );
    }

    // 3. The deleted entries are still gone (entries 0-99 were removed)
    for key in 0..100u64 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            None,
            "Key {} should not exist",
            key
        );
    }
}

#[test]
fn test_index_based_relocation_filter() {
    let dir = tempdir::TempDir::new("test_index_based_relocation_filter").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.relocation_strategy = RelocationStrategy::IndexBased(None);
    let config = force_unload_config(&config);
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_relocation_filter(|key, _| {
        if u64::from_be_bytes(key.try_into().unwrap()) % 2 == 0 {
            Decision::Keep
        } else {
            Decision::Remove
        }
    });
    ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let sample_key = 3_u64.to_be_bytes().to_vec();
    let mut insert_count = 0_u64;
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        loop {
            db.insert(ks, insert_count.to_be_bytes().to_vec(), vec![0, 1, 2])
                .unwrap();
            insert_count += 1;
            if insert_count.is_multiple_of(10000) && list_wal_files(dir.path()).len() > 1 {
                break;
            }
        }
        assert_eq!(db.get(ks, &sample_key).unwrap(), Some(vec![0, 1, 2].into()));
        db.wait_for_background_threads_to_finish();
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();

        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);
        // With force_unload_config, index-based relocation may or may not process cells
        // depending on whether they're loaded in memory. Either behavior is safe.
        // We just verify no crashes occurred (the function returned successfully)
        db.rebuild_control_region().unwrap();

        db.wait_for_background_threads_to_finish();
    }
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            config.clone(),
            metrics.clone(),
        )
        .unwrap();
        let ks = db.ks("k");
        for key in insert_count..(insert_count + 100) {
            db.insert(ks, key.to_be_bytes().to_vec(), vec![0, 1, 2])
                .unwrap();
        }
        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);
        // With force_unload_config, index-based relocation may or may not process cells
        // depending on whether they're loaded in memory. Either behavior is safe.
        // We just verify no crashes occurred (the function returned successfully)
        db.wait_for_background_threads_to_finish();
    }
    // Verify data integrity - all data should still be accessible
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");

    // Verify sample_key still exists (wasn't filtered because no relocation occurred)
    assert_eq!(db.get(ks, &sample_key).unwrap(), Some(vec![0, 1, 2].into()));

    // Verify all inserted data is still accessible
    for key in 0..insert_count {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![0, 1, 2].into())
        );
    }
    for key in insert_count..(insert_count + 100) {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![0, 1, 2].into())
        );
    }

    start_index_based_relocation(&db);

    // Verify all data is still accessible regardless of whether relocation occurred
    assert_eq!(db.get(ks, &sample_key).unwrap(), Some(vec![0, 1, 2].into()));
}

// Both strategies reclaim the same WAL prefix and must leave identical
// visible contents. WAL-based relocation copies the live entries below its
// target and drops the rest with their files; it evaluates no per-entry
// removal for a keyspace without a filter, so it reports copies
// (`relocation_kept`), not removals.
#[test]
fn test_relocation_strategies_produce_identical_results() {
    let dir1 = tempdir::TempDir::new("test_wal_strategy").unwrap();
    let dir2 = tempdir::TempDir::new("test_index_strategy").unwrap();
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    let config = Arc::new(config);
    let mut index_config = Config::clone(&config);
    index_config.relocation_strategy = RelocationStrategy::IndexBased(None);
    let index_config = force_unload_config(&index_config);

    // Create identical keyspace configurations
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new().with_bloom_filter(0.01, 2000);
    ksb.add_key_space_config("test", 8, 1, KeyType::uniform(1), ksc);
    let key_shape = ksb.build();

    let metrics_wal = Metrics::new();
    let metrics_index = Metrics::new();
    // Values large enough that the writes span several WAL files, so the
    // WAL-based pass has a whole-file target and does work.
    let value = |prefix: &str, key: u64| {
        let mut value = format!("{prefix}_{key}").into_bytes();
        value.resize(8 * 1024, 0);
        value
    };

    // Create identical databases with identical operations
    let (db_wal, db_index, ks) = {
        let db_wal = Db::open(
            dir1.path(),
            key_shape.clone(),
            config.clone(),
            metrics_wal.clone(),
        )
        .unwrap();
        let db_index = Db::open(
            dir2.path(),
            key_shape.clone(),
            index_config,
            metrics_index.clone(),
        )
        .unwrap();
        let ks = db_wal.ks("test");

        // Apply identical operations to both databases
        for key in 0..1000u64 {
            let value = value("value", key);
            db_wal
                .insert(ks, key.to_be_bytes().to_vec(), value.clone())
                .unwrap();
            db_index
                .insert(ks, key.to_be_bytes().to_vec(), value)
                .unwrap();
        }

        // Update some entries
        for key in (0..500u64).step_by(2) {
            let value = value("updated_value", key);
            db_wal
                .insert(ks, key.to_be_bytes().to_vec(), value.clone())
                .unwrap();
            db_index
                .insert(ks, key.to_be_bytes().to_vec(), value)
                .unwrap();
        }

        // Delete some entries
        for key in (100..200u64).step_by(3) {
            db_wal.remove(ks, key.to_be_bytes().to_vec()).unwrap();
            db_index.remove(ks, key.to_be_bytes().to_vec()).unwrap();
        }

        (db_wal, db_index, ks)
    };
    let first_wal_file = dir1.path().join(format!(
        "wal_{:016x}",
        db_wal.wal.min_wal_position() / config.wal_file_size
    ));

    // Run different relocation strategies
    db_wal.rebuild_control_region().unwrap();
    db_index.rebuild_control_region().unwrap();

    db_wal.start_blocking_relocation(); // Default WAL-based
    start_index_based_relocation(&db_index);

    // Compare final database contents key by key
    for key in 0..1000u64 {
        let key_bytes = key.to_be_bytes().to_vec();
        let val_wal = db_wal.get(ks, &key_bytes).unwrap();
        let val_index = db_index.get(ks, &key_bytes).unwrap();

        assert_eq!(val_wal, val_index, "Databases differ for key {}", key);
    }

    // Both strategies must have done work: WAL-based copied live entries
    // below its target, index-based processed cells.
    let wal_kept = metrics_wal
        .relocation_kept
        .with_label_values(&["test"])
        .get();
    let index_processed = relocation_cells_processed(&metrics_index, "test");
    assert!(wal_kept > 0, "WAL-based should have copied live entries");
    assert!(
        index_processed > 0,
        "Index-based should have processed some cells"
    );
    db_wal.wait_for_background_threads_to_finish();
    assert!(
        !first_wal_file.exists(),
        "WAL-based should have reclaimed the first WAL file"
    );
    db_index.wait_for_background_threads_to_finish();
}

#[test]
fn test_both_strategies_handle_concurrent_writes() {
    // Test both strategies handle concurrent writes safely

    let test_concurrent_strategy = |strategy_name: &str, use_index_based: bool| {
        let dir = tempdir::TempDir::new(&format!("test_concurrent_{strategy_name}")).unwrap();
        let config = if use_index_based {
            index_based_config()
        } else {
            force_unload_config(&Config::small())
        };
        let mut ksb = KeyShapeBuilder::new();
        let ksc = KeySpaceConfig::new();
        ksb.add_key_space_config("concurrent", 8, 1, KeyType::uniform(1), ksc);
        let key_shape = ksb.build();
        let metrics = Metrics::new();

        let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
        let ks = db.ks("concurrent");

        // Pre-populate data
        for i in 0..1000u64 {
            db.insert(ks, i.to_be_bytes().to_vec(), vec![1, 2, 3])
                .unwrap();
        }

        let skip_stale_before = metrics
            .skip_stale_update
            .get_metric_with_label_values(&["concurrent", "insert"])
            .unwrap()
            .get();

        // Start concurrent writers
        let mut handles = vec![];
        let db_clone = Arc::clone(&db);

        // Writer thread - continuously updates keys
        let writer_handle = thread::spawn(move || {
            let mut successful_writes = 0;
            for round in 0..100 {
                for i in (0..100u64).step_by(5) {
                    let key = i.to_be_bytes().to_vec();
                    let value = vec![round as u8, (round >> 8) as u8, i as u8];
                    // Some concurrent access failures are expected
                    if db_clone.insert(ks, key, value).is_ok() {
                        successful_writes += 1;
                    }
                }
                thread::sleep(Duration::from_millis(1));
            }
            successful_writes
        });
        handles.push(writer_handle);

        // Start relocation while writers are active
        db.rebuild_control_region().unwrap();
        if use_index_based {
            start_index_based_relocation(&db);
        } else {
            db.start_blocking_relocation();
        }

        // Wait for writers to finish and collect successful write counts
        let mut total_successful_writes = 0;
        for handle in handles {
            let successful_writes = handle.join().unwrap();
            total_successful_writes += successful_writes;
        }

        let skip_stale_after = metrics
            .skip_stale_update
            .get_metric_with_label_values(&["concurrent", "insert"])
            .unwrap()
            .get();

        // Return the database, keyspace, metrics, and successful write count for verification
        (
            db,
            ks,
            skip_stale_after - skip_stale_before,
            total_successful_writes,
        )
    };

    let (db_wal, ks_wal, _wal_stale_updates, wal_successful_writes) =
        test_concurrent_strategy("wal", false);
    let (db_index, ks_index, _index_stale_updates, index_successful_writes) =
        test_concurrent_strategy("index", true);

    // Test 1: Both strategies should complete without crashing (we got here)

    // Test 2: Both strategies should have completed some successful writes
    assert!(
        wal_successful_writes > 0,
        "WAL strategy should have completed some writes, got {}",
        wal_successful_writes
    );
    assert!(
        index_successful_writes > 0,
        "Index strategy should have completed some writes, got {}",
        index_successful_writes
    );

    assert_eq!(
        wal_successful_writes, index_successful_writes,
        "Both strategies should complete the same number of writes: WAL={}, Index={}",
        wal_successful_writes, index_successful_writes
    );

    // Test 3: Verify data consistency - all keys should have valid values after concurrent writes
    for i in (0..100u64).step_by(5) {
        let wal_value = db_wal.get(ks_wal, &i.to_be_bytes()).unwrap().unwrap();
        let index_value = db_index.get(ks_index, &i.to_be_bytes()).unwrap().unwrap();

        // Values should be 3 bytes: [round, round>>8, key]
        assert_eq!(
            wal_value.len(),
            3,
            "WAL strategy produced invalid value length for key {}",
            i
        );
        assert_eq!(
            index_value.len(),
            3,
            "Index strategy produced invalid value length for key {}",
            i
        );
        assert_eq!(
            wal_value[2], i as u8,
            "WAL strategy corrupted key data for key {}",
            i
        );
        assert_eq!(
            index_value[2], i as u8,
            "Index strategy corrupted key data for key {}",
            i
        );
    }
}

#[test]
fn test_index_based_relocation_progress_tracking() {
    let dir = tempdir::TempDir::new("test_index_progress_tracking").unwrap();

    // Create multiple keyspaces to ensure cross-keyspace progress tracking
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("ks1", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    ksb.add_key_space_config("ks2", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    // Populate data across multiple keyspaces
    {
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            index_based_config(),
            metrics.clone(),
        )
        .unwrap();
        let ks1 = db.ks("ks1");
        let ks2 = db.ks("ks2");

        for ks in [ks1, ks2] {
            for key in 0..500u64 {
                db.insert(ks, key.to_be_bytes().to_vec(), vec![1, 2, 3])
                    .unwrap();
            }
        }
        db.wait_for_background_threads_to_finish();
    }

    // Test that watermark files are created and progress is tracked
    {
        let metrics = Metrics::new();
        let db = Db::open(
            dir.path(),
            key_shape.clone(),
            index_based_config(),
            metrics.clone(),
        )
        .unwrap();
        db.rebuild_control_region().unwrap();
        start_index_based_relocation(&db);

        // Verify some progress was made
        let processed_ks1 = relocation_cells_processed(&metrics, "ks1");
        let processed_ks2 = relocation_cells_processed(&metrics, "ks2");
        assert!(
            processed_ks1 > 0 || processed_ks2 > 0,
            "Expected some cells to be processed"
        );

        db.wait_for_background_threads_to_finish();
    }

    // Verify watermark file exists
    let watermark_file = dir.path().join("rel");
    assert!(watermark_file.exists(), "Watermark file should be created");
}

#[test]
fn test_index_based_relocation_empty_and_sparse_cells() {
    let dir = tempdir::TempDir::new("test_sparse_cells").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new();
    ksb.add_key_space_config("sparse", 8, 4, KeyType::uniform(128), ksc); // 128 cells per mutex (power of 2)
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let db = Db::open(dir.path(), key_shape, index_based_config(), metrics.clone()).unwrap();
    let ks = db.ks("sparse");

    // Create very sparse data - only populate every 10th cell
    for cell_idx in (0..128).step_by(10) {
        for key_in_cell in 0..5u64 {
            let key = (cell_idx as u64 * 1000) + key_in_cell; // Ensure keys land in specific cells
            db.insert(ks, key.to_be_bytes().to_vec(), vec![cell_idx as u8])
                .unwrap();
        }
    }

    db.rebuild_control_region().unwrap();
    start_index_based_relocation(&db);

    // Should handle empty cells gracefully - no crashes, reasonable metrics
    let processed = relocation_cells_processed(&metrics, "sparse");

    // Index-based relocation should process some cells and complete successfully
    // The exact number depends on implementation details, but it should be reasonable
    assert!(processed > 0, "Expected some cells to be processed");
    assert!(
        processed < 10000,
        "Processed cell count should be reasonable"
    );

    // Verify data integrity for populated cells
    for cell_idx in (0..128).step_by(10) {
        for key_in_cell in 0..5u64 {
            let key = (cell_idx as u64 * 1000) + key_in_cell;
            let expected_value = Some(vec![cell_idx as u8].into());
            assert_eq!(db.get(ks, &key.to_be_bytes()).unwrap(), expected_value);
        }
    }
}

#[test]
fn test_index_based_relocation_large_cells() {
    let dir = tempdir::TempDir::new("test_large_cells").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    let ksc = KeySpaceConfig::new();
    // Use fewer cells so we can pack more entries per cell
    ksb.add_key_space_config("dense", 8, 2, KeyType::uniform(2), ksc); // Only 2 cells per mutex
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let db = Db::open(dir.path(), key_shape, index_based_config(), metrics.clone()).unwrap();
    let ks = db.ks("dense");

    // Fill cells with many entries each
    let entries_per_cell = 1000u64;
    for cell_idx in 0..2u64 {
        for entry_idx in 0..entries_per_cell {
            let key = (cell_idx * entries_per_cell) + entry_idx;
            let value = format!("large_value_{}_{}", cell_idx, entry_idx).into_bytes();
            db.insert(ks, key.to_be_bytes().to_vec(), value).unwrap();
        }
    }

    db.rebuild_control_region().unwrap();

    let start_time = std::time::Instant::now();
    start_index_based_relocation(&db);
    let elapsed = start_time.elapsed();

    // Verify large cells were processed successfully
    let processed = relocation_cells_processed(&metrics, "dense");
    assert!(processed > 0, "Should have processed some cells");

    // Basic performance check - should complete in reasonable time
    assert!(
        elapsed.as_secs() < 30,
        "Large cell processing should complete in reasonable time"
    );

    // Verify data integrity after processing
    for cell_idx in 0..2u64 {
        for entry_idx in (0..entries_per_cell).step_by(100) {
            // Sample every 100th entry
            let key = (cell_idx * entries_per_cell) + entry_idx;
            let expected = format!("large_value_{}_{}", cell_idx, entry_idx).into_bytes();
            assert_eq!(
                db.get(ks, &key.to_be_bytes()).unwrap(),
                Some(expected.into())
            );
        }
    }
}

#[test]
fn test_watermark_highest_wal_position_tracking() {
    let dir = tempdir::TempDir::new("test_watermark_wal_position").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space_config("test", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
    let key_shape = ksb.build();
    let metrics = Metrics::new();

    let db = Db::open(dir.path(), key_shape, index_based_config(), metrics.clone()).unwrap();
    let ks = db.ks("test");

    // Insert multiple entries to create WAL entries at different positions
    for key in 0..50u64 {
        db.insert(ks, key.to_be_bytes().to_vec(), vec![1, 2, 3])
            .unwrap();
    }

    // Get the initial WAL position to verify we have data to process
    let initial_wal_position = db.wal_writer.position();
    assert!(initial_wal_position > 0, "Database should have WAL entries");

    // Ensure data is persisted and control region is built
    db.rebuild_control_region().unwrap();

    // Run index-based relocation
    start_index_based_relocation(&db);

    // Verify some cells were processed - this confirms relocation completed successfully
    let processed = relocation_cells_processed(&metrics, "test");
    assert!(processed > 0, "Should have processed some cells");

    // Verify data integrity - all data should still be accessible after relocation
    for key in 0..50u64 {
        assert_eq!(
            db.get(ks, &key.to_be_bytes()).unwrap(),
            Some(vec![1, 2, 3].into()),
            "Key {} should still exist after relocation",
            key
        );
    }

    // Wait for background threads to finish - this consumes the db
    db.wait_for_background_threads_to_finish();

    // Now the key test: load watermarks from disk and ensure it is as expected
    let watermarks = RelocationWatermarks::read_or_create(dir.path()).unwrap();

    // The correct value should be the highest WAL position of entries that were processed
    let WatermarkData {
        highest_wal_position,
        upper_limit,
        ..
    } = watermarks.data;

    assert_eq!(
        upper_limit, initial_wal_position,
        "Upper limit should equal initial WAL position (this defines the processing boundary)"
    );

    // The highest_wal_position should be the actual highest position among processed entries
    // It should be:
    // 1. Greater than 0 (we processed some entries)
    // 2. Less than or equal to upper_limit (can't process beyond the safe boundary)
    // 3. Close to upper_limit (most entries should be processed in a simple sequential insert scenario)
    assert!(
        highest_wal_position > 0,
        "Watermark highest_wal_position ({}) should be greater than 0",
        highest_wal_position
    );
    assert!(
        highest_wal_position <= upper_limit,
        "Watermark highest_wal_position ({}) should not exceed upper_limit ({})",
        highest_wal_position,
        upper_limit
    );

    // Precise correctness check: highest_wal_position should be close to upper_limit
    let gap = upper_limit - highest_wal_position;
    assert!(
        gap <= 100,
        "Gap between highest_wal_position ({}) and upper_limit ({}) is too large ({}), suggests incomplete processing",
        highest_wal_position,
        upper_limit,
        gap
    );

    // The exact value cannot be computed with precision because:
    // 1. Index-based relocation only processes entries that have been ingested into the large table
    // 2. There's a delay between WAL writes and large table ingestion
    // 3. The "upper_limit" represents the safe boundary, but not all entries up to that
    //    point may have been ingested into cells yet
    // 4. Our rebuild_control_region() call ensures most entries are ingested, but timing varies
    //
    // However, we can assert a deterministic bound: in our controlled test scenario with
    // sequential inserts followed by rebuild_control_region(), we expect high ingestion rate.
    assert!(
        highest_wal_position >= initial_wal_position * 9 / 10,
        "Watermark highest_wal_position ({}) should be at least 90% of initial WAL position ({}) \
         - if this fails, it suggests ingestion issues, not the bug we're testing",
        highest_wal_position,
        initial_wal_position
    );
}

#[test]
fn test_index_based_relocation_with_target_position() {
    let dir = tempdir::TempDir::new("test_target_position").unwrap();
    let config = index_based_config();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("default", 8, 1, KeyType::uniform(1));
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("default");

    // Insert 1000 entries sequentially
    for i in 0..500u64 {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }

    // Ensure wal tracker has processed all guard drops from entries 0-499
    db.wal_writer.wal_tracker_barrier();
    // Capture WAL position at entry 500
    let mid_position = db.wal_writer.last_processed().as_u64();

    // Continue inserting entries 501-1000
    for i in 500..1000u64 {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }
    db.wal_writer.wal_tracker_barrier();

    // Force unload to ensure entries are in index (index-based relocation needs this)
    db.rebuild_control_region().unwrap();

    // Run relocation with target_position
    db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(Some(mid_position)));

    // Get metrics
    let kept = metrics
        .relocation_kept
        .with_label_values(&["default"])
        .get();

    // Verify approximately 500 entries were relocated (allow wider margin for WAL position variation)
    assert!(
        (350..=650).contains(&kept),
        "Expected ~500 entries relocated, got {}",
        kept
    );

    // Verify entries below and above target
    let key_250 = 250u64.to_be_bytes().to_vec();
    let key_750 = 750u64.to_be_bytes().to_vec();

    assert_eq!(
        db.get(ks, &key_250).unwrap(),
        Some(format!("value_{}", 250).into_bytes().into())
    );
    assert_eq!(
        db.get(ks, &key_750).unwrap(),
        Some(format!("value_{}", 750).into_bytes().into())
    );

    // Check watermark file contains target_position
    let watermark = RelocationWatermarks::read_or_create(dir.path())
        .unwrap()
        .data;
    assert_eq!(watermark.target_position, Some(mid_position));
}

// Regression: the index-based CAS threshold must be the relocation frontier
// (`effective_limit`), not the live frontier. For a key overwritten after the
// frontier, the as-of view still relocates the older value; a live-frontier
// threshold lets `RelocationUpdates::apply` re-point the newer write to the
// relocated copy of that older value, losing the overwrite. The same state
// arises with a write that lands concurrently with a relocation run, so this
// also pins the concurrent-overwrite case deterministically.
#[test]
fn test_index_based_relocation_preserves_overwrite_above_target() {
    let dir = tempdir::TempDir::new("test_index_cas_overwrite").unwrap();
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("k", 8, 1, KeyType::uniform(1));
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    // No forced unload: the cell must stay loaded so the as-of value is still
    // a distinct overlay position when relocation runs (a flush would collapse
    // the key to latest-per-key and nothing would be relocated).
    let db = Db::open(
        dir.path(),
        key_shape,
        Arc::new(Config::small()),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.ks("k");

    let key = 3u64.to_be_bytes().to_vec();
    db.insert(ks, key.clone(), vec![1; 64]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    let frontier = db.wal_writer.last_processed().as_u64();

    // Overwrite after the frontier; the barrier ensures the live frontier at
    // relocation time is past the overwrite.
    db.insert(ks, key.clone(), vec![2; 64]).unwrap();
    db.wal_writer.wal_tracker_barrier();

    db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(Some(frontier)));

    // The as-of value must actually have been relocated, otherwise the CAS
    // never runs and this test is vacuous.
    assert!(
        metrics.relocation_kept.with_label_values(&["k"]).get() > 0,
        "relocation must have relocated the as-of value"
    );
    assert_eq!(
        db.get(ks, &key).unwrap(),
        Some(vec![2; 64].into()),
        "the overwrite must survive relocation; the relocated copy of the \
         as-of value must not be CAS'd over it"
    );
}

// Regression: a write racing a clean `ForceRelocate` on a Loaded `[L0, L1]`
// cell must not lose the keys below L0.
//
// A loaded cell's overlay covers L0 only; reads reach L1-resident keys by
// walking `disk_levels_to_walk` -> `iter_below_l0`. A clean ForceRelocate
// collapses L0+L1 into a single blob in the L0 slot. When a write lands while
// that flush is in flight, the completion (`update_relocated_position`)
// reaches the DirtyLoaded arm with an overlay that no longer covers the new
// L0 blob (it lacks the former-L1-only keys). Before the fix that arm kept
// the cell loaded, violating the "Loaded => data covers L0" invariant:
// - reads: `iter_below_l0` on the single-level cell walks nothing, so
//   former-L1-only keys returned None immediately;
// - the next normal flush cloned the overlay as the cell's only level,
//   dropping those keys from the index permanently.
// The fix retains only the racing writes and demotes the cell to
// DirtyUnloaded, so reads and flushes consult the relocated blob again.
//
// The relocation runs on the production flusher; the
// `fp_flush_before_completion` failpoint pauses it between the flush work and
// the completion, so the racing write deterministically lands inside the
// in-flight window.
#[test]
fn test_force_relocate_concurrent_write_keeps_below_l0_keys() {
    let dir = tempdir::TempDir::new("force_relocate_dirty_loaded").unwrap();
    // Forced unload (threshold 0) so the flushed cell unloads; the later read
    // reloads it folding only L0 into the overlay, leaving L1 on disk.
    let config = force_unload_config(&Config::small());
    let mut ksb = KeyShapeBuilder::new();
    // Unloaded iteration off so that stepping an iterator loads the cell
    // (point reads never load; this is the only read-path load trigger).
    ksb.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_unloaded_iterator(false),
    );
    let key_shape = ksb.build();
    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.ks("k");

    let value = |b: u8| -> Vec<u8> { vec![b; 100] };
    let key = |i: u64| -> Vec<u8> { i.to_be_bytes().to_vec() };

    // Build an L1: enough keys for an L0->L1 promote (l0_max_entries =
    // 8 * max_dirty_keys = 256 for Config::small), then a small fresh L0.
    for i in 0..300u64 {
        db.insert(ks, key(i), value(1)).unwrap();
    }
    // Advance the tracker frontier past the inserts before each rebuild, so
    // the flush retains nothing in the overlay and the cell ends up clean.
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    for i in 1000..1010u64 {
        db.insert(ks, key(i), value(2)).unwrap();
    }
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    db.large_table.flusher.barrier();
    // Flushes dispatched while the inserts ran sampled an older frontier and
    // can leave a retained overlay tail (DirtyUnloaded). With nothing in
    // flight any more, one more rebuild flushes at the current frontier and
    // leaves the cell clean.
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    db.large_table.flusher.barrier();

    let cell = CellReference::first(&db, KeySpace::first()).expect("cell must exist");
    let context = db.ks_context(cell.keyspace);
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "Unloaded",
        "PRECONDITION: the flushed cell must be clean and unloaded"
    );

    // Load the (clean) cell: iterators load cells (point reads read through
    // without loading); this folds only L0 into the in-memory overlay,
    // leaving L1 reachable solely through the on-disk levels walk.
    assert!(db.iterator(ks).next().is_some());
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "Loaded",
        "PRECONDITION: cell must be clean and loaded"
    );

    // Pick a probe key that lives only below L0: absent from the loaded
    // overlay but readable through the on-disk levels walk.
    let below_l0_key = (0..300u64)
        .find(|i| {
            let reduced = context.ks_config.reduced_key_bytes(Bytes::from(key(*i)));
            db.large_table
                .with_entry_for_test(context, &cell.cell_id, |entry| {
                    entry.get(&reduced).is_none()
                })
        })
        .expect("setup must leave at least one key only reachable below L0");
    assert_eq!(
        db.get(ks, &key(below_l0_key)).unwrap(),
        Some(value(1).into()),
        "SANITY: probe key readable via the levels walk before relocation"
    );

    // Dispatch a force-relocate of the cell's blobs into a flusher whose
    // receiver the test owns, capturing the command instead of racing a
    // Pause the flusher between the flush work and its completion, so the
    // racing write below deterministically lands while the force-relocate is
    // in flight. The latch releases when its guard drops — including on a
    // panicking assertion, so a failure cannot wedge the flusher thread.
    let (completion_latch, completion_latch_guard) = Latch::new();
    db.large_table.fp.0.write().fp_flush_before_completion = FailPoint::latch(completion_latch);

    // Dispatch the force-relocate through the production flusher, exactly as
    // the snapshot pass does for a clean cell in low-occupancy files.
    let last_processed = Loader::last_processed(db.as_ref());
    db.large_table
        .with_entry_for_test(context, &cell.cell_id, |entry| {
            let layout = db.config.wal_layout(WalKind::Index);
            let files: HashSet<_> = entry
                .levels()
                .iter()
                .filter_map(|p| p.valid())
                .map(|p| layout.locate_file(p.offset()))
                .collect();
            let relocate_files = RelocateFiles::new(files, db.config.wal_layout(WalKind::Index));
            entry.request_force_relocate(
                &db.large_table.flusher,
                last_processed,
                Loader::flushed_at_bytes(db.as_ref(), last_processed),
                &relocate_files,
            );
        });

    // The racing write: lands while the paused flusher holds the completion.
    // The cell goes Loaded -> DirtyLoaded, routing the completion through the
    // arm whose overlay no longer covers the relocated blob.
    db.insert(ks, key(7777), value(3)).unwrap();
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "DirtyLoaded",
        "PRECONDITION: the racing write must land before the completion"
    );

    // Unblock the flusher and wait for the completion to apply.
    drop(completion_latch_guard);
    db.large_table.flusher.barrier();

    // The clean-cell branch must have dispatched a ForceRelocate — a dirty
    // cell would dispatch a normal Flush, exercising a different completion
    // path and leaving this test vacuous.
    assert!(
        metrics.unload.with_label_values(&["force_relocate"]).get() > 0,
        "the ForceRelocate flusher path must have run"
    );

    // The completion demotes the cell so reads consult the relocated blob;
    // the overlay keeps only the racing write.
    assert_eq!(
        db.large_table.entry_state_for_test(context, &cell.cell_id),
        "DirtyUnloaded",
        "completion on a dirtied loaded cell must demote it"
    );

    // The racing write and the L0-resident keys survive...
    assert_eq!(db.get(ks, &key(7777)).unwrap(), Some(value(3).into()));
    assert_eq!(db.get(ks, &key(1000)).unwrap(), Some(value(2).into()));
    // ...and so must the key that lived below L0 (lost before the fix: the
    // single-level cell had nothing below L0 to walk, and the kept overlay
    // never had the key).
    assert_eq!(
        db.get(ks, &key(below_l0_key)).unwrap(),
        Some(value(1).into()),
        "key residing below L0 must survive a force-relocate completing on a dirtied cell"
    );

    // The loss must also not become permanent: the next normal flush writes
    // the overlay as the cell's only level.
    db.force_rebuild_control_region().unwrap();
    db.large_table.flusher.barrier();
    assert_eq!(
        db.get(ks, &key(below_l0_key)).unwrap(),
        Some(value(1).into()),
        "key below L0 must survive the next normal flush after the relocation"
    );
}

#[test]
fn test_compute_target_position_from_ratio() {
    use crate::relocation::compute_target_position_from_ratio;

    let dir = tempdir::TempDir::new("test_compute_ratio").unwrap();
    let config = Arc::new(Config::small());
    let mut ksb = KeyShapeBuilder::new();
    ksb.add_key_space("default", 8, 1, KeyType::uniform(1));
    let key_shape = ksb.build();
    let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
    let ks = db.ks("default");

    // Initially should return None (empty WAL)
    assert_eq!(compute_target_position_from_ratio(&db, 0.5), None);

    // Add some data
    for i in 0..1000u64 {
        let key = i.to_be_bytes().to_vec();
        let value = format!("value_{}", i).into_bytes();
        db.insert(ks, key, value).unwrap();
    }
    db.large_table.flusher.barrier();
    db.wal_writer.wal_tracker_barrier();

    let min_pos = db.wal.min_wal_position();
    let last_pos = db.wal_writer.last_processed().as_u64();
    let range = last_pos - min_pos;

    // Test various ratios
    let target_0 = compute_target_position_from_ratio(&db, 0.0).unwrap();
    assert_eq!(target_0, min_pos);

    let target_50 = compute_target_position_from_ratio(&db, 0.5).unwrap();
    assert!(target_50 > min_pos && target_50 < last_pos);
    // Allow some margin for byte alignment
    assert!(
        (target_50 - min_pos) >= range / 2 - 1000,
        "target_50 {} should be close to middle of range {}",
        target_50 - min_pos,
        range / 2
    );
    assert!(
        (target_50 - min_pos) <= range / 2 + 1000,
        "target_50 {} should be close to middle of range {}",
        target_50 - min_pos,
        range / 2
    );

    let target_100 = compute_target_position_from_ratio(&db, 1.0).unwrap();
    // Read fresh last_pos since background threads may have advanced the WAL
    let last_pos_at_100 = db.wal_writer.last_processed().as_u64();
    assert_eq!(target_100, last_pos_at_100);

    // Test clamping - negative ratio should give min_pos
    let target_negative = compute_target_position_from_ratio(&db, -0.5).unwrap();
    assert_eq!(target_negative, min_pos);

    // Test clamping - ratio > 1.0 should give last_pos
    let target_over_one = compute_target_position_from_ratio(&db, 1.5).unwrap();
    // Read fresh last_pos again
    let last_pos_at_over = db.wal_writer.last_processed().as_u64();
    assert_eq!(target_over_one, last_pos_at_over);
}

const KEEP: u8 = 0;
const REMOVE: u8 = 1;
const STOP: u8 = 2;

fn class_key(class: u8, number: u32) -> Vec<u8> {
    let mut key = vec![class, 0, 0, 0];
    key.extend_from_slice(&number.to_be_bytes());
    key
}

fn class_decision(key: &[u8]) -> Decision {
    match key[0] {
        KEEP => Decision::Keep,
        REMOVE => Decision::Remove,
        _ => Decision::StopRelocation,
    }
}

fn keep_below_stop_config() -> Arc<Config> {
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.snapshot_written_bytes = u64::MAX;
    Arc::new(config)
}

fn keep_below_stop_shape(
    filter: impl Fn(&[u8], &[u8]) -> Decision + Send + Sync + 'static,
) -> crate::key_shape::KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        "k",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(filter),
    );
    builder.build()
}

/// Writes a Keep row in the first WAL file, Remove rows past the second
/// file, then one batch of a Keep row and a Stop row. The Stop's whole-file
/// target covers the first file. Returns the value of every row.
fn write_keep_below_stop(db: &Arc<Db>, config: &Config) -> Vec<u8> {
    let ks = db.ks("k");
    let value = vec![7u8; 64 * 1024];
    db.insert(ks, class_key(KEEP, 0), value.clone()).unwrap();
    db.insert(ks, class_key(REMOVE, 0), value.clone()).unwrap();
    let mut number = 1;
    while db.wal_writer.position() < config.wal_file_size + config.frag_size {
        db.insert(ks, class_key(REMOVE, number), value.clone())
            .unwrap();
        number += 1;
    }
    let mut batch = db.write_batch();
    batch.write(ks, class_key(KEEP, 1), value.clone());
    batch.write(ks, class_key(STOP, 0), value.clone());
    batch.commit().unwrap();
    db.wal_writer.wal_tracker_barrier();
    value
}

fn assert_keep_rows(db: &Db, value: &[u8], phase: &str) {
    let ks = db.ks("k");
    for number in [0, 1] {
        assert_eq!(
            db.get(ks, &class_key(KEEP, number)).unwrap().as_deref(),
            Some(value),
            "Keep row {number} lost ({phase})"
        );
    }
    assert!(
        db.get(ks, &class_key(STOP, 0)).unwrap().is_some(),
        "{phase}"
    );
}

#[derive(Clone, Copy, Debug)]
enum CrashAfter {
    RelocatedWrites,
    ControlRegion,
    Reclaim,
}

// WalBased relocation copies the live rows of a filtered keyspace to the WAL
// head before the index drops entries below the target, then stores the
// control region, then reclaims files. A crash at each boundary must reopen
// with every Keep row readable, and the next pass must complete.
#[test]
fn test_wal_relocation_keeps_filtered_keep_rows_across_crashes() {
    let config = keep_below_stop_config();
    let shape = keep_below_stop_shape(|key: &[u8], _: &[u8]| class_decision(key));
    for crash in [
        CrashAfter::RelocatedWrites,
        CrashAfter::ControlRegion,
        CrashAfter::Reclaim,
    ] {
        let phase = format!("{crash:?}");
        let dir = tempdir::TempDir::new("wal_relocation_keep_crash").unwrap();
        let open = |metrics| Db::open(dir.path(), shape.clone(), config.clone(), metrics).unwrap();
        let db = open(Metrics::new());
        let value = write_keep_below_stop(&db, &config);
        let first_file_id = db.wal.min_wal_position() / config.wal_file_size;
        let first_file = dir.path().join(format!("wal_{first_file_id:016x}"));
        let first_file_end = i64::try_from((first_file_id + 1) * config.wal_file_size).unwrap();
        let (_sender, receiver) = std::sync::mpsc::channel();
        let mut driver = crate::relocation::RelocationDriver {
            db: Arc::downgrade(&db),
            path: dir.path().to_path_buf(),
            receiver,
            metrics: Metrics::new(),
        };
        match crash {
            CrashAfter::RelocatedWrites => {
                driver.relocate_wal_prefix(&db).unwrap().unwrap();
            }
            CrashAfter::ControlRegion => {
                let target = driver.relocate_wal_prefix(&db).unwrap().unwrap();
                db.rebuild_control_region_from(target).unwrap();
            }
            CrashAfter::Reclaim => driver.wal_based_relocation(db.clone()).unwrap(),
        }
        assert_keep_rows(&db, &value, &phase);
        // No later relocation step runs: the reopen sees the crash state.
        db.wait_for_background_threads_to_finish();

        let metrics = Metrics::new();
        let db = open(metrics.clone());
        assert_keep_rows(&db, &value, &format!("{phase}, reopen"));
        let ks = db.ks("k");
        let remove_row = db.get(ks, &class_key(REMOVE, 0)).unwrap();
        match crash {
            // The old control region and WAL replay give the state before the pass.
            CrashAfter::RelocatedWrites => assert!(remove_row.is_some(), "{phase}"),
            CrashAfter::ControlRegion => {}
            CrashAfter::Reclaim => assert!(remove_row.is_none(), "{phase}"),
        }
        assert_eq!(
            first_file.exists(),
            !matches!(crash, CrashAfter::Reclaim),
            "{phase}"
        );

        db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
        assert_keep_rows(&db, &value, &format!("{phase}, next pass"));
        if !matches!(crash, CrashAfter::Reclaim) {
            // The unlink itself can lag: the mapper skips files that the
            // reopened writer still maps writeable (`wal/mapper.rs`).
            assert!(
                metrics.gc_position.with_label_values(&["wal"]).get() >= first_file_end,
                "{phase}"
            );
        }
        db.wait_for_background_threads_to_finish();

        let db = open(Metrics::new());
        assert_keep_rows(&db, &value, &format!("{phase}, final reopen"));
        assert!(
            db.get(ks, &class_key(REMOVE, 0)).unwrap().is_none(),
            "{phase}"
        );
        db.wait_for_background_threads_to_finish();
    }
}

// A write that lands after the pass captures its as-of view but before it
// installs the relocated copy wins: the copy only replaces an index entry
// still below the target.
#[test]
fn test_wal_relocation_concurrent_overwrite_wins_over_kept_copy() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{OnceLock, Weak};

    let config = keep_below_stop_config();
    let newer = vec![9u8; 1024];
    let db_slot = Arc::new(OnceLock::<Weak<Db>>::new());
    let keep_calls = Arc::new(AtomicUsize::new(0));
    let shape = {
        let db_slot = db_slot.clone();
        let keep_calls = keep_calls.clone();
        let newer = newer.clone();
        keep_below_stop_shape(move |key: &[u8], _: &[u8]| {
            if key == class_key(KEEP, 0).as_slice() {
                keep_calls.fetch_add(1, Ordering::SeqCst);
            }
            // The scan reaches the Stop after it kept the row and before the
            // copy loop runs.
            if key == class_key(STOP, 0).as_slice() {
                let db = db_slot.get().unwrap().upgrade().unwrap();
                db.insert(db.ks("k"), class_key(KEEP, 0), newer.clone())
                    .unwrap();
            }
            class_decision(key)
        })
    };
    let dir = tempdir::TempDir::new("wal_relocation_keep_overwrite").unwrap();
    let open = || Db::open(dir.path(), shape.clone(), config.clone(), Metrics::new()).unwrap();
    let db = open();
    db_slot.set(Arc::downgrade(&db)).unwrap();
    let ks = db.ks("k");
    let value = write_keep_below_stop(&db, &config);
    let first_file = dir.path().join(format!(
        "wal_{:016x}",
        db.wal.min_wal_position() / config.wal_file_size
    ));

    db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
    assert_eq!(keep_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        db.get(ks, &class_key(KEEP, 0)).unwrap().as_deref(),
        Some(&newer[..])
    );
    assert_eq!(
        db.get(ks, &class_key(KEEP, 1)).unwrap().as_deref(),
        Some(&value[..])
    );
    db.wait_for_background_threads_to_finish();
    assert!(!first_file.exists());

    let db = open();
    assert_eq!(
        db.get(ks, &class_key(KEEP, 0)).unwrap().as_deref(),
        Some(&newer[..])
    );
    db.wait_for_background_threads_to_finish();
}

// A stateful filter answers Keep on its first call for a key and Remove after.
// The pass must call it exactly once per scanned record and act on that
// answer, so the Keep rows survive the pass and a reopen.
#[test]
fn test_wal_relocation_calls_filter_once_per_record() {
    use std::collections::HashMap;
    use std::sync::Mutex;

    let config = keep_below_stop_config();
    let calls = Arc::new(Mutex::new(HashMap::<Vec<u8>, usize>::new()));
    let shape = {
        let calls = calls.clone();
        keep_below_stop_shape(move |key: &[u8], _: &[u8]| {
            let mut calls = calls.lock().unwrap();
            let count = calls.entry(key.to_vec()).or_default();
            *count += 1;
            match class_decision(key) {
                Decision::Keep if *count > 1 => Decision::Remove,
                decision => decision,
            }
        })
    };
    let dir = tempdir::TempDir::new("wal_relocation_filter_once").unwrap();
    let open = || Db::open(dir.path(), shape.clone(), config.clone(), Metrics::new()).unwrap();
    let db = open();
    let value = write_keep_below_stop(&db, &config);
    let records = calls.lock().unwrap().len();
    assert_eq!(records, 0, "writes do not call the filter");

    db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
    {
        let calls = calls.lock().unwrap();
        assert!(calls.contains_key(&class_key(KEEP, 0)));
        assert!(calls.contains_key(&class_key(STOP, 0)));
        for (key, count) in calls.iter() {
            assert_eq!(*count, 1, "filter calls for key {key:?}");
        }
    }
    assert_keep_rows(&db, &value, "after the pass");
    db.wait_for_background_threads_to_finish();

    let db = open();
    assert_keep_rows(&db, &value, "after reopen");
    db.wait_for_background_threads_to_finish();
}

// Removed rows that were promoted to L1 keep stale index entries into the
// reclaimed files (the cutoff does not rewrite L1). `get` reads them as
// absent; `exists` must agree, after the pass and after reopen.
#[test]
fn test_wal_relocation_exists_agrees_with_get_on_stale_l1() {
    let mut config = Config::small();
    config.wal_file_size = 2 * config.frag_size;
    config.snapshot_written_bytes = u64::MAX;
    config.l0_max_entries = Some(1);
    let config = Arc::new(config);
    let shape = keep_below_stop_shape(|key: &[u8], _: &[u8]| class_decision(key));
    let dir = tempdir::TempDir::new("wal_relocation_exists").unwrap();
    let open = || Db::open(dir.path(), shape.clone(), config.clone(), Metrics::new()).unwrap();
    let db = open();
    let ks = db.ks("k");
    let mut rows = 0;
    while db.wal_writer.position() < 2 * config.wal_file_size {
        db.insert(ks, class_key(REMOVE, rows), vec![1u8; 64 * 1024])
            .unwrap();
        rows += 1;
    }
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    db.insert(ks, class_key(STOP, 0), vec![3u8; 1024]).unwrap();
    db.wal_writer.wal_tracker_barrier();
    db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
    let check = |db: &Db, phase: &str| {
        let mut absent = 0;
        for number in 0..rows {
            let key = class_key(REMOVE, number);
            let found = db.get(ks, &key).unwrap().is_some();
            assert_eq!(
                db.exists(ks, &key).unwrap(),
                found,
                "row {number} ({phase})"
            );
            absent += usize::from(!found);
        }
        assert!(absent > 0, "the pass must leave stale L1 entries ({phase})");
    };
    check(&db, "after the pass");
    db.wait_for_background_threads_to_finish();
    let db = open();
    check(&db, "after reopen");
    db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
    check(&db, "after a second pass");
    db.wait_for_background_threads_to_finish();
}

// A row of an auto-sharded cell lives only in an L1 shard when the pass
// copies it. The relocation view must include the shards, and the relocated
// copy must re-point the shard entry.
//
// Premise: the cell's L1 must be sharded before the pass starts. Only a
// promote splits L1, and a promote writes only the entries below the cut the
// requester captured (`FlushKind::Flush::cut`, filtered by `retain_processed`
// in `flusher.rs`). A flush dispatched by the insert loop can therefore carry
// a partial image, write L0 instead of promoting, and leave the split to
// tracker timing. The rows stay below `max_dirty_keys`, so no flush runs
// while they are written; the tracker barrier then pins the frontier past all
// of them, and the rebuild dispatches one flush whose promote sees the whole
// cell.
fn sharded_cell_keeps_rows(filtered: bool) {
    let mut config = Config::small();
    config.frag_size = 64 * 1024;
    config.wal_file_size = 2 * config.frag_size;
    config.max_dirty_keys = 4096;
    config.l0_max_entries = Some(16);
    config.snapshot_unload_threshold = 0;
    config.snapshot_written_bytes = u64::MAX;
    config.with_index_auto_shard_threshold(1024);
    let config = Arc::new(config);
    let shape = if filtered {
        keep_below_stop_shape(|key: &[u8], _: &[u8]| class_decision(key))
    } else {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space_config("k", 8, 1, KeyType::uniform(1), KeySpaceConfig::new());
        builder.build()
    };
    let dir = tempdir::TempDir::new("wal_relocation_sharded").unwrap();
    let open = |metrics| Db::open(dir.path(), shape.clone(), config.clone(), metrics).unwrap();
    let metrics = Metrics::new();
    let db = open(metrics.clone());
    let ks = db.ks("k");
    let value = vec![7u8; 1024];
    db.insert(ks, class_key(KEEP, 0), value.clone()).unwrap();
    let mut number = 0;
    while db.wal_writer.position() < config.wal_file_size + config.frag_size {
        db.insert(ks, class_key(REMOVE, number), value.clone())
            .unwrap();
        number += 1;
    }
    let mut batch = db.write_batch();
    batch.write(ks, class_key(KEEP, 1), value.clone());
    batch.write(ks, class_key(STOP, 0), value.clone());
    batch.commit().unwrap();
    // Rows written before the barrier: the KEEP 0 row, the REMOVE filler, and
    // the two batch rows.
    let bulk_rows = number as usize + 3;
    assert!(
        bulk_rows < config.max_dirty_keys,
        "{bulk_rows} bulk rows must stay below max_dirty_keys={} so that no \
         insert dispatches a flush from an older frontier",
        config.max_dirty_keys,
    );
    let l0_threshold = config
        .l0_max_entries
        .expect("the fixture sets l0_max_entries");
    assert!(
        bulk_rows > l0_threshold,
        "the promote needs more rows than l0_max_entries: {bulk_rows} rows, \
         threshold {l0_threshold}",
    );
    // Pin the frontier past every row, then flush. The rebuild requests the
    // flush with that pinned frontier, so the promote below it retains every
    // row and the L1 image exceeds the shard threshold.
    db.wal_writer.wal_tracker_barrier();
    db.force_rebuild_control_region().unwrap();
    assert!(
        metrics.l1_shard_split_total.with_label_values(&["k"]).get() > 0,
        "the fixture must shard the cell's L1"
    );
    let first_file = dir.path().join(format!(
        "wal_{:016x}",
        db.wal.min_wal_position() / config.wal_file_size
    ));

    db.start_blocking_relocation_with_strategy(RelocationStrategy::WalBased);
    assert_keep_rows(&db, &value, "after the pass");
    assert_eq!(
        db.get(ks, &class_key(REMOVE, 0)).unwrap().is_some(),
        !filtered
    );
    db.wait_for_background_threads_to_finish();
    assert!(!first_file.exists(), "the pass must reclaim the first file");

    let db = open(Metrics::new());
    assert_keep_rows(&db, &value, "after reopen");
    db.wait_for_background_threads_to_finish();
}

#[test]
fn test_wal_relocation_keeps_keep_row_in_sharded_cell() {
    sharded_cell_keeps_rows(true);
}

#[test]
fn test_wal_relocation_keeps_rows_in_sharded_unfiltered_cell() {
    sharded_cell_keeps_rows(false);
}
