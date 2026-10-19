use super::*;

#[test]
fn obsolete_cell_unlink_preserves_existing_mapped_read_owners() {
    let temp = tempdir::TempDir::new("index-mapped-owner-lifetime").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [batch(0, vec![put(0, A, b"old")]), batch(1, vec![delete(A)])];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    let old_root = temp.path().join("old.index");
    index
        .flush_and_unload(&old_root, IndexReadCut::Latest)
        .unwrap();
    let reader = NativeIndex::open_snapshot(&old_root, &shape(), DATABASE, BUDGET).unwrap();
    let old_cells = index.reachable_files();
    install(&mut index, &log, &batches[1], receipts[1]);
    let new_root = temp.path().join("new.index");
    index
        .flush_and_unload(&new_root, IndexReadCut::Latest)
        .unwrap();
    let current = index.reachable_files();
    assert!(old_cells.iter().all(|file| !current.contains(file)));
    // Stand in for the authority's durable root switch, then unlink only files
    // that are not shared with that root. Never truncate or reuse an inode.
    std::fs::remove_file(&old_root).unwrap();
    for name in old_cells {
        std::fs::remove_file(temp.path().join(name)).unwrap();
    }
    assert_eq!(
        value(&reader, &log, A, IndexReadCut::Latest).as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(
        value(&index, &log, A, cut(0, 0)).as_deref(),
        Some(b"old".as_slice())
    );
    assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
    let reopened = NativeIndex::open_snapshot(&new_root, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(value(&reopened, &log, A, IndexReadCut::Latest), None);
}

#[test]
fn wal_reachability_includes_old_puts_until_protected_compaction_releases_them() {
    let temp = tempdir::TempDir::new("index-wal-reachability").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [batch(0, vec![put(0, A, b"old")]), batch(1, vec![delete(A)])];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    let address = index
        .get(KeySpace(0), A, IndexReadCut::Latest)
        .unwrap()
        .unwrap()
        .batch_address;
    let end = address.offset + u64::from(address.len);
    let references = |index: &NativeIndex| {
        index.references_wal_file(
            address.partition,
            address.segment_generation,
            address.offset,
            end,
        )
    };
    assert!(references(&index), "dirty overlay is reachable");
    let pin = index.pin_cut(cut(0, 0)).unwrap();
    install(&mut index, &log, &batches[1], receipts[1]);
    assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
    let retained_root = temp.path().join("retained.index");
    let captured = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    index.install_image(captured.persist(&retained_root).unwrap());
    assert!(
        references(&index),
        "the old Put stays mapped for the protected cut"
    );
    assert!(!index.references_wal_file(
        address.partition + 1,
        address.segment_generation,
        address.offset,
        end
    ));
    assert!(!index.references_wal_file(
        address.partition,
        address.segment_generation + 1,
        address.offset,
        end
    ));
    assert!(!index.references_wal_file(
        address.partition,
        address.segment_generation,
        end,
        end + 1
    ));
    drop(pin);
    let reclaimed_root = temp.path().join("reclaimed.index");
    let captured = index
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap();
    index.install_image(captured.persist(&reclaimed_root).unwrap());
    assert!(
        !references(&index),
        "a retained index tombstone needs no old payload"
    );
    let reopened = NativeIndex::open_snapshot(&reclaimed_root, &shape(), DATABASE, BUDGET).unwrap();
    assert!(!references(&reopened));
    assert!(matches!(
        reopened.get(KeySpace(0), A, cut(0, 0)),
        Err(NativeIndexError::ExpiredCut)
    ));
    // The independently retained root still owns the older payload history.
    let retained = NativeIndex::open_snapshot(&retained_root, &shape(), DATABASE, BUDGET).unwrap();
    assert!(references(&retained));
}
