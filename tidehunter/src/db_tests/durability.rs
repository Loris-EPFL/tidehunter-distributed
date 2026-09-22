use super::*;
use crate::key_shape::KeyShape;

#[test]
fn durable_public_mutations_reopen_after_snapshot_and_range_drop() {
    check_durable_public_mutations(false);
}

#[test]
fn durable_public_mutations_with_directory_coalescing_reopen() {
    check_durable_public_mutations(true);
}

fn check_durable_public_mutations(coalesce_wal_directory_sync: bool) {
    let directory = tempdir::TempDir::new("durable-mutations").unwrap();
    let shape = KeyShape::new_single(8, 1, KeyType::uniform(1));
    let config = Arc::new(Config {
        sync_writes: true,
        coalesce_wal_directory_sync,
        frag_size: 8192,
        wal_file_size: 16384,
        max_maps: 4,
        ..Config::small()
    });
    let db = Db::open(
        directory.path(),
        shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();
    for i in 0_u64..80 {
        let mut batch = db.write_batch();
        batch.write(ks, i.to_be_bytes().to_vec(), vec![17; 700]);
        batch.commit().unwrap();
    }
    db.force_rebuild_control_region().unwrap();
    db.remove(ks, 1_u64.to_be_bytes().to_vec()).unwrap();
    db.sync().unwrap();
    db.wait_for_background_threads_to_finish();
    let db = Db::open(
        directory.path(),
        shape.clone(),
        config.clone(),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.single_ks();
    for i in 0_u64..80 {
        assert_eq!(db.get(ks, &i.to_be_bytes()).unwrap().is_some(), i != 1);
    }
    db.drop_cells_in_range(ks, &[0; 8], &[255; 8]).unwrap();
    db.force_rebuild_control_region().unwrap();
    db.insert(ks, 3_u64.to_be_bytes().to_vec(), vec![42])
        .unwrap();
    db.wait_for_background_threads_to_finish();
    let db = Db::open(directory.path(), shape, config, Metrics::new()).unwrap();
    let ks = db.single_ks();
    for i in 0_u64..80 {
        assert_eq!(db.get(ks, &i.to_be_bytes()).unwrap().is_some(), i == 3);
    }
    db.wait_for_background_threads_to_finish();
}

#[test]
fn durable_control_publication_error_prevents_gc() {
    let directory = tempdir::TempDir::new("durable-control-error").unwrap();
    let db = Db::open(
        directory.path(),
        KeyShape::new_single(8, 1, KeyType::uniform(1)),
        Arc::new(Config::small()),
        Metrics::new(),
    )
    .unwrap();
    db.insert(db.single_ks(), vec![0; 8], vec![9]).unwrap();
    db.sync().unwrap();
    let scratch = directory.path().join("cr").with_extension(".bak");
    std::fs::create_dir(&scratch).unwrap();
    let before = db.indexes.file_ids();
    assert!(db.force_rebuild_control_region().is_err());
    assert!(before.iter().all(|id| db.indexes.file_ids().contains(id)));
    std::fs::remove_dir(scratch).unwrap();
    db.force_rebuild_control_region().unwrap();
    db.wait_for_background_threads_to_finish();
}
