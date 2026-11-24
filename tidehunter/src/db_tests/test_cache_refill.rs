use super::*;
use crate::failpoints::FailPoint;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Mutation {
    Overwrite,
    Delete,
    BatchOverwrite,
    BatchDelete,
    DropCell,
}

fn delayed_refill_does_not_hide_completed_mutation(
    mutation: Mutation,
    iterator: bool,
    unloaded: bool,
    pause_before_index_read: bool,
) {
    let dir = tempdir::TempDir::new("cache-refill-race").unwrap();
    let mut shape = KeyShapeBuilder::new();
    shape.add_key_space_config(
        "k",
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_value_cache_size(1),
    );
    let db = Db::open(
        dir.path(),
        shape.build(),
        Arc::new(Config::small()),
        Metrics::new(),
    )
    .unwrap();
    let ks = db.ks("k");
    db.insert(ks, vec![0, 0, 0, 1], vec![10]).unwrap();
    // Both keys occupy one cell. Evict key 1 so the reader leaves the row
    // lock, reads its old WAL value, and reaches the refill failpoint.
    db.insert(ks, vec![0, 0, 0, 2], vec![20]).unwrap();
    if unloaded {
        db.wal_writer.wal_tracker_barrier();
        db.force_rebuild_control_region().unwrap();
        assert!(db.large_table.is_all_clean());
        db.large_table.force_unload_clean();
    }

    let (reached_tx, reached_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume_rx = parking_lot::Mutex::new(resume_rx);
    let first = AtomicBool::new(true);
    let pause = FailPoint::from_fn(move || {
        if first.swap(false, Ordering::SeqCst) {
            reached_tx.send(()).unwrap();
            resume_rx
                .lock()
                .recv_timeout(Duration::from_secs(20))
                .unwrap();
        }
    });
    if pause_before_index_read {
        assert!(unloaded && !iterator);
        db.large_table.fp.0.write().fp_lookup_after_lock_drop = pause;
    } else {
        db.large_table.fp.0.write().fp_before_value_cache_refill = pause;
    }
    let reader_db = db.clone();
    let reader = std::thread::spawn(move || {
        if iterator {
            let (key, value) = reader_db.iterator(ks).next().unwrap().unwrap();
            assert_eq!(key.as_ref(), &[0, 0, 0, 1]);
            Some(value)
        } else {
            reader_db.get(ks, &[0, 0, 0, 1]).unwrap()
        }
    });
    reached_rx.recv_timeout(Duration::from_secs(20)).unwrap();
    match mutation {
        Mutation::Overwrite => db.insert(ks, vec![0, 0, 0, 1], vec![30]).unwrap(),
        Mutation::Delete => db.remove(ks, vec![0, 0, 0, 1]).unwrap(),
        Mutation::BatchOverwrite => {
            let mut batch = db.write_batch();
            batch.write(ks, vec![0, 0, 0, 1], vec![30]);
            batch.commit().unwrap();
        }
        Mutation::BatchDelete => {
            let mut batch = db.write_batch();
            batch.delete(ks, vec![0, 0, 0, 1]);
            batch.commit().unwrap();
        }
        Mutation::DropCell => db.drop_cells_in_range(ks, &[0; 4], &[255; 4]).unwrap(),
    }
    resume_tx.send(()).unwrap();
    // This overlapping reader may return its old value. It must not poison
    // the cache used by a reader starting after the mutation completed.
    assert_eq!(reader.join().unwrap(), Some(vec![10].into()));
    let expected = match mutation {
        Mutation::Overwrite | Mutation::BatchOverwrite => Some(vec![30].into()),
        Mutation::Delete | Mutation::BatchDelete | Mutation::DropCell => None,
    };
    assert_eq!(
        db.get(ks, &[0, 0, 0, 1]).unwrap(),
        expected,
        "{mutation:?}, iterator={iterator}, unloaded={unloaded}"
    );
    db.wait_for_background_threads_to_finish();
}

#[test]
fn delayed_cache_refill_preserves_completed_overwrite() {
    delayed_refill_matrix(Mutation::Overwrite);
}

#[test]
fn delayed_cache_refill_preserves_completed_delete() {
    delayed_refill_matrix(Mutation::Delete);
}

#[test]
fn delayed_cache_refill_preserves_completed_batch_overwrite() {
    delayed_refill_matrix(Mutation::BatchOverwrite);
}

#[test]
fn delayed_cache_refill_preserves_completed_batch_delete() {
    delayed_refill_matrix(Mutation::BatchDelete);
}

#[test]
fn delayed_cache_refill_preserves_dropped_cell() {
    delayed_refill_matrix(Mutation::DropCell);
}

fn delayed_refill_matrix(mutation: Mutation) {
    for iterator in [false, true] {
        for unloaded in [false, true] {
            delayed_refill_does_not_hide_completed_mutation(mutation, iterator, unloaded, false);
        }
    }
}

#[test]
fn delayed_disk_lookup_cannot_refill_after_completed_mutation() {
    for mutation in [Mutation::Overwrite, Mutation::Delete] {
        delayed_refill_does_not_hide_completed_mutation(mutation, false, true, true);
    }
}
