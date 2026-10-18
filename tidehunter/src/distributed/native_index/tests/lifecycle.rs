use super::*;
use std::collections::BTreeSet;

#[test]
fn publication_during_persistence_preserves_overwrites_deletes_and_new_cells_on_replay() {
    let temp = tempdir::TempDir::new("index-concurrent-persist").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(0, vec![put(0, A, b"old"), put(0, B, b"stable")]),
        batch(1, vec![delete(A), put(0, C, b"before-capture")]),
        batch(
            2,
            vec![
                delete(A),
                put(0, A, b"after-capture"),
                delete(B),
                put(2, b"new-cell", b"new"),
            ],
        ),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    install(&mut index, &log, &batches[1], receipts[1]);
    let captured_root = temp.path().join("captured.index");
    let prepared = index.prepare_image(cut(0, 1)).unwrap();
    assert!(index.stats().transient_metadata_bytes <= BUDGET);
    let candidates = prepared.new_files(&captured_root).unwrap();
    assert_eq!(candidates.len(), 1);
    let (created_tx, created_rx) = std::sync::mpsc::sync_channel(0);
    let (continue_tx, continue_rx) = std::sync::mpsc::sync_channel(0);
    let persisted = std::thread::scope(|scope| {
        let root = &captured_root;
        let worker = scope.spawn(move || {
            prepared
                .persist_with_hook(root, || {
                    created_tx.send(()).unwrap();
                    continue_rx.recv().unwrap();
                })
                .unwrap()
        });
        // The shared container now exists and persistence is in progress, but
        // the root has not been created and no serving-index guard is owned.
        created_rx.recv().unwrap();
        install(&mut index, &log, &batches[2], receipts[2]);
        assert_eq!(value(&index, &log, A, cut(2, 0)), None);
        assert_eq!(
            value(&index, &log, A, IndexReadCut::Latest).as_deref(),
            Some(b"after-capture".as_slice())
        );
        continue_tx.send(()).unwrap();
        worker.join().unwrap()
    });
    assert_eq!(
        persisted.files().into_iter().collect::<BTreeSet<_>>(),
        candidates.into_iter().collect()
    );
    index.install_image(persisted);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    assert_eq!(
        value(&index, &log, A, cut(0, 1)).as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(value(&index, &log, A, cut(1, 1)), None);
    assert_eq!(value(&index, &log, A, cut(2, 0)), None);
    assert_eq!(
        value(&index, &log, A, IndexReadCut::Latest).as_deref(),
        Some(b"after-capture".as_slice())
    );
    assert_eq!(value(&index, &log, B, IndexReadCut::Latest), None);
    assert_eq!(
        value(&index, &log, C, IndexReadCut::Latest).as_deref(),
        Some(b"before-capture".as_slice())
    );
    assert!(
        index
            .get(KeySpace(2), b"new-cell", IndexReadCut::Latest)
            .unwrap()
            .is_some()
    );
    assert_eq!(index.stats().retained_versions, 8);

    // A crash before the next checkpoint restores the captured cut and replays
    // every acknowledged whole WAL batch after it, including the new cell.
    let mut recovered =
        NativeIndex::open_snapshot(&captured_root, &shape(), DATABASE, BUDGET).unwrap();
    for n in 1..batches.len() {
        install(&mut recovered, &log, &batches[n], receipts[n]);
    }
    assert_eq!(
        keys(&recovered, false, IndexReadCut::Latest),
        keys(&index, false, IndexReadCut::Latest)
    );
    assert_eq!(
        recovered
            .get(KeySpace(2), b"new-cell", IndexReadCut::Latest)
            .unwrap(),
        index
            .get(KeySpace(2), b"new-cell", IndexReadCut::Latest)
            .unwrap()
    );
    assert_eq!(
        value(&recovered, &log, A, IndexReadCut::Latest).as_deref(),
        Some(b"after-capture".as_slice())
    );
    let complete = temp.path().join("complete.index");
    recovered
        .flush_and_unload(&complete, IndexReadCut::Latest)
        .unwrap();
    let reopened = NativeIndex::open_snapshot(&complete, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(reopened.stats().retained_versions, 8);
    assert_eq!(value(&reopened, &log, B, IndexReadCut::Latest), None);
    assert_eq!(
        value(&reopened, &log, A, cut(0, 1)).as_deref(),
        Some(b"old".as_slice())
    );
}

#[test]
fn checkpoints_reuse_unchanged_cell_ranges_and_clean_roots_write_no_container() {
    let temp = tempdir::TempDir::new("index-incremental-cells").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(0, vec![put(0, A, b"old"), put(0, B, b"stable")]),
        batch(1, vec![put(0, A, b"new")]),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    let first = temp.path().join("first.index");
    index
        .flush_and_unload(&first, IndexReadCut::Latest)
        .unwrap();
    let old_files: BTreeSet<_> = index.reachable_files().into_iter().collect();
    install(&mut index, &log, &batches[1], receipts[1]);
    let second = temp.path().join("second.index");
    let prepared = index.prepare_image(IndexReadCut::Latest).unwrap();
    let new_files = prepared.new_files(&second).unwrap();
    assert_eq!(new_files.len(), 1, "only A's native cell is dirty");
    let persisted = prepared.persist(&second).unwrap();
    let current: BTreeSet<_> = persisted.files().into_iter().collect();
    assert_eq!(current.intersection(&old_files).count(), 1);
    assert_eq!(
        current.difference(&old_files).cloned().collect::<Vec<_>>(),
        new_files
    );
    index.install_image(persisted);
    let clean = temp.path().join("clean.index");
    let prepared = index.prepare_image(IndexReadCut::Latest).unwrap();
    assert!(prepared.new_files(&clean).unwrap().is_empty());
    index.install_image(prepared.persist(&clean).unwrap());
    assert_eq!(
        std::fs::read(&second).unwrap(),
        std::fs::read(&clean).unwrap()
    );
    let reopened = NativeIndex::open_snapshot(&clean, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(
        value(&reopened, &log, A, cut(0, 1)).as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(
        value(&reopened, &log, B, IndexReadCut::Latest).as_deref(),
        Some(b"stable".as_slice())
    );
}

#[test]
fn disk_history_larger_than_resident_budget_stays_queryable_and_reload_is_bounded() {
    let temp = tempdir::TempDir::new("index-pageable-history").unwrap();
    let log = log(&temp.path().join("wal"));
    let budget = cell_directory_charge(&CellId::Integer(0)) + 2 * (VERSION_CHARGE + A.len());
    let mut index = NativeIndex::new(&shape(), DATABASE, budget).unwrap();
    let mut root = temp.path().join("unused.index");
    for sequence in 0..12 {
        let batch = batch(sequence, vec![put(0, A, &sequence.to_be_bytes())]);
        index
            .check_capacity(index.estimated_batch_bytes(&batch).unwrap())
            .unwrap();
        let receipt = durable(&log, std::slice::from_ref(&batch))[0];
        install(&mut index, &log, &batch, receipt);
        root = temp.path().join(format!("generation-{sequence}.index"));
        let prepared = index.prepare_image(IndexReadCut::Latest).unwrap();
        assert!(index.stats().transient_metadata_bytes <= budget);
        index.install_image(prepared.persist(&root).unwrap());
        assert!(index.stats().charged_metadata_bytes <= budget);
    }
    assert!(index.retained_metadata_bytes() > budget);
    assert_eq!(index.stats().retained_versions, 12);
    let mut reopened = NativeIndex::open_snapshot(&root, &shape(), DATABASE, budget).unwrap();
    assert_eq!(reopened.stats().unloaded_cells, 1);
    assert_eq!(reopened.stats().retained_versions, 12);
    for sequence in 0u64..12 {
        assert_eq!(
            value(&reopened, &log, A, cut(sequence, 0)).as_deref(),
            Some(sequence.to_be_bytes().as_slice())
        );
        assert_eq!(keys(&reopened, false, cut(sequence, 0)), [A.to_vec()]);
    }
    assert!(matches!(reopened.reload(), Err(NativeIndexError::Capacity)));
    assert_eq!(
        reopened.stats().unloaded_cells,
        1,
        "failed reload does not partially expand history"
    );
}

#[test]
fn protected_cuts_constrain_persisted_compaction_and_unprotected_stale_cuts_fail() {
    let temp = tempdir::TempDir::new("index-retained-cuts").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(0, vec![put(0, A, b"old"), put(0, B, b"stable")]),
        batch(1, vec![delete(A)]),
        batch(2, vec![put(0, A, b"new"), put(0, C, b"third")]),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    for n in 0..batches.len() {
        install(&mut index, &log, &batches[n], receipts[n]);
    }
    let held = index.pin_cut(cut(0, 1)).unwrap();
    let clone = held.clone();
    let duplicate = index.pin_cut(cut(0, 1)).unwrap();
    drop(held);
    let empty_pin = index.pin_cut(IndexReadCut::BeforeAll).unwrap();
    let empty_root = temp.path().join("empty-pin.index");
    let captured = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    index.install_image(captured.persist(&empty_root).unwrap());
    assert_eq!(index.retention_floor(), IndexReadCut::BeforeAll);
    assert_eq!(value(&index, &log, A, empty_pin.cut()), None);
    drop(empty_pin);
    let first = temp.path().join("held.index");
    let captured = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    index.install_image(captured.persist(&first).unwrap());
    assert_eq!(
        index.retention_floor(),
        clone.cut(),
        "requested floor clamps to the oldest pin"
    );
    assert_eq!(
        value(&index, &log, A, clone.cut()).as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(keys(&index, false, clone.cut()), [A.to_vec(), B.to_vec()]);
    drop(
        index
            .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
            .unwrap(),
    );
    drop(clone);
    let duplicate_root = temp.path().join("duplicate-pin.index");
    let captured = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    index.install_image(captured.persist(&duplicate_root).unwrap());
    assert_eq!(
        index.retention_floor(),
        cut(0, 1),
        "an independent duplicate still clamps the floor"
    );
    drop(duplicate);

    // Aborting a capture releases its floor reservation without expiring a cut.
    let aborted = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    assert_eq!(index.retention_floor(), cut(0, 1));
    assert!(matches!(
        index.pin_cut(cut(0, 1)),
        Err(NativeIndexError::RetentionInProgress)
    ));
    drop(aborted);
    assert!(index.validate_cut(cut(0, 1)).is_ok());
    assert_eq!(index.stats().transient_metadata_bytes, 0);

    let compacted = temp.path().join("compacted.index");
    let captured = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    let persisted = captured.persist(&compacted).unwrap();
    assert_eq!(
        index.retention_floor(),
        cut(0, 1),
        "persistence is not durable authority adoption"
    );
    index.install_image(persisted);
    assert_eq!(index.stats().retained_versions, 4);
    assert_eq!(index.retention_floor(), cut(1, 0));
    for index in [
        &index,
        &NativeIndex::open_snapshot(&compacted, &shape(), DATABASE, BUDGET).unwrap(),
    ] {
        assert!(matches!(
            index.get(KeySpace(0), A, cut(0, 1)),
            Err(NativeIndexError::ExpiredCut)
        ));
        assert!(matches!(
            index.scan(KeySpace(0), None, false, 10, cut(0, 1)),
            Err(NativeIndexError::ExpiredCut)
        ));
        assert_eq!(
            value(index, &log, A, cut(1, 0)),
            None,
            "the retained anchor is a tombstone"
        );
        assert_eq!(
            value(index, &log, B, cut(1, 0)).as_deref(),
            Some(b"stable".as_slice())
        );
        assert_eq!(
            value(index, &log, A, IndexReadCut::Latest).as_deref(),
            Some(b"new".as_slice())
        );
    }
    // The earlier independently durable root remains exact while it is retained.
    let old = NativeIndex::open_snapshot(&first, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(
        value(&old, &log, A, cut(0, 1)).as_deref(),
        Some(b"old".as_slice())
    );
}

#[test]
fn failed_persistence_releases_capture_and_does_not_replace_serving_state() {
    let temp = tempdir::TempDir::new("index-failed-persist").unwrap();
    let log = log(&temp.path().join("wal"));
    let first = batch(0, vec![put(0, A, b"old")]);
    let receipt = durable(&log, std::slice::from_ref(&first))[0];
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &first, receipt);
    let root = temp.path().join("root.index");
    index.flush_and_unload(&root, IndexReadCut::Latest).unwrap();
    let captured = index.prepare_image(IndexReadCut::Latest).unwrap();
    assert!(matches!(
        index.prepare_image(IndexReadCut::Latest),
        Err(NativeIndexError::CheckpointInProgress)
    ));
    assert!(
        captured.persist(&root).is_err(),
        "create_new cannot change an adopted image"
    );
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    assert!(index.prepare_image(IndexReadCut::Latest).is_ok());
    assert_eq!(
        value(&index, &log, A, IndexReadCut::Latest).as_deref(),
        Some(b"old".as_slice())
    );
}

#[test]
fn repaired_cell_crc_and_root_digest_do_not_hide_invalid_reference_descriptors() {
    let temp = tempdir::TempDir::new("index-corrupt-cell").unwrap();
    let log = log(&temp.path().join("wal"));
    let first = batch(0, vec![put(0, A, b"old"), put(0, B, b"second range")]);
    let receipt = durable(&log, std::slice::from_ref(&first))[0];
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &first, receipt);
    let root = temp.path().join("root.index");
    index.flush_and_unload(&root, IndexReadCut::Latest).unwrap();
    let bad_dir = temp.path().join("bad");
    std::fs::create_dir(&bad_dir).unwrap();
    let (mut manifest, bindings) = super::containers::root_bindings(&root);
    let binding = bindings.iter().find(|binding| binding.offset != 0).unwrap();
    let mut container = std::fs::read(temp.path().join(&binding.name)).unwrap();
    let range = binding.range();
    let mut cell = CrcFrame::read_from_slice(&container, range.start)
        .unwrap()
        .to_vec();
    let kind_at = cell.len() - 45;
    cell[kind_at] = 7;
    let frame = CrcFrame::new(&cell);
    assert_eq!(frame.as_ref().len(), range.len());
    container[range].copy_from_slice(frame.as_ref());
    std::fs::write(bad_dir.join(&binding.name), container).unwrap();
    manifest[binding.digest_at..binding.digest_at + 32]
        .copy_from_slice(&Blake2b::<U32>::digest(frame.as_ref()));
    let bad_root = bad_dir.join("root.index");
    std::fs::write(&bad_root, CrcFrame::new(&manifest).as_ref()).unwrap();
    assert!(matches!(
        NativeIndex::open_snapshot(&bad_root, &shape(), DATABASE, BUDGET),
        Err(NativeIndexError::Invalid("reference kind"))
    ));
}
