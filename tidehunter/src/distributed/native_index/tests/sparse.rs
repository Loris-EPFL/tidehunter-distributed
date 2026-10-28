use super::*;

#[test]
fn sparse_view_survives_hole_closure_pages_history_and_compaction() {
    let temp = tempdir::TempDir::new("sparse-index-view").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(0, vec![put(0, A, b"old-a"), put(0, B, b"old-b")]),
        batch(1, vec![delete(A), put(0, B, b"late-b")]),
        batch(2, vec![put(0, B, b"new-b"), put(0, C, b"new-c")]),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    index
        .flush_and_unload(&temp.path().join("base.index"), IndexReadCut::Latest)
        .unwrap();
    install(&mut index, &log, &batches[2], receipts[2]);
    let upper = match cut(2, 1) {
        IndexReadCut::At(v) => v,
        _ => unreachable!(),
    };
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    index
        .install_live_view(upper, cut(0, 1), vec![hole])
        .unwrap();
    let frozen = index.latest_cut();
    let pin = index.pin_cut(frozen.clone()).unwrap();
    assert!(matches!(
        index.prepare_image(frozen.clone()),
        Err(NativeIndexError::Invalid(_))
    ));
    let foreign = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    assert!(matches!(
        foreign.validate_cut(frozen.clone()),
        Err(NativeIndexError::Invalid(_))
    ));
    let first = index
        .scan(KeySpace(0), None, false, 1, frozen.clone())
        .unwrap();
    assert_eq!(first[0].0.as_ref(), A);
    let mut cursor = index
        .scan_cursor(KeySpace(0), None, false, frozen.clone())
        .unwrap();
    assert_eq!(cursor.next(&index).unwrap().0.as_ref(), A);
    // Capture only the genuinely published prefix while the sparse view is
    // live. The missing batch must remain in the overlay when this capture is
    // persisted and installed after that batch publishes.
    let prefix_capture = index.prepare_compacted_image(cut(0, 1), cut(0, 1)).unwrap();
    install(&mut index, &log, &batches[1], receipts[1]);
    index.install_live_view(upper, cut(0, 1), vec![]).unwrap();
    let prefix_path = temp.path().join("prefix-during-hole.index");
    index.install_image(prefix_capture.persist(&prefix_path).unwrap());
    let recovered = NativeIndex::open_snapshot(&prefix_path, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(
        value(&recovered, &log, A, IndexReadCut::Latest).as_deref(),
        Some(b"old-a".as_slice())
    );
    assert_eq!(
        value(&recovered, &log, B, IndexReadCut::Latest).as_deref(),
        Some(b"old-b".as_slice())
    );
    assert_eq!(value(&recovered, &log, C, IndexReadCut::Latest), None);
    assert_eq!(index.retention_floor(), cut(0, 1));
    assert_eq!(
        value(&index, &log, A, frozen.clone()).as_deref(),
        Some(b"old-a".as_slice())
    );
    assert_eq!(
        value(&index, &log, B, frozen.clone()).as_deref(),
        Some(b"new-b".as_slice())
    );
    assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
    assert_eq!(
        value(&index, &log, B, IndexReadCut::Latest).as_deref(),
        Some(b"new-b".as_slice())
    );
    let remainder = index
        .scan(
            KeySpace(0),
            Some(first[0].0.as_ref()),
            false,
            10,
            frozen.clone(),
        )
        .unwrap();
    assert_eq!(
        remainder
            .iter()
            .map(|(key, _)| key.to_vec())
            .collect::<Vec<_>>(),
        vec![B.to_vec(), C.to_vec()]
    );
    assert_eq!(cursor.next(&index).unwrap().0.as_ref(), B);
    assert_eq!(cursor.next(&index).unwrap().0.as_ref(), C);
    assert!(cursor.next(&index).is_none());
    drop(cursor);
    assert_eq!(
        value(&index, &log, A, frozen.clone()).as_deref(),
        Some(b"old-a".as_slice())
    );
    assert_eq!(
        value(&index, &log, B, frozen.clone()).as_deref(),
        Some(b"new-b".as_slice())
    );
    assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
    assert_eq!(
        value(&index, &log, B, IndexReadCut::Latest).as_deref(),
        Some(b"new-b".as_slice())
    );
    for reverse in [false, true] {
        let mut after = None;
        let mut rows = Vec::new();
        loop {
            let page = index
                .scan(KeySpace(0), after.as_deref(), reverse, 1, frozen.clone())
                .unwrap();
            let Some((key, _)) = page.into_iter().next() else {
                break;
            };
            after = Some(key.clone());
            rows.push(key.to_vec());
        }
        let expected = if reverse {
            vec![C.to_vec(), B.to_vec(), A.to_vec()]
        } else {
            vec![A.to_vec(), B.to_vec(), C.to_vec()]
        };
        assert_eq!(rows, expected);
    }
    let mut history = index
        .relocation_history(KeySpace(0), frozen.clone())
        .unwrap();
    let mut seen_newer = false;
    while let Some((_, reference)) = history.next(&index) {
        assert_ne!(reference.batch_version, hole);
        seen_newer |= reference.batch_version.sequence == 2;
    }
    assert!(seen_newer, "history must not stop at an excluded version");
    drop(history);
    let capture = index.prepare_compacted_image(cut(2, 1), cut(2, 1)).unwrap();
    index.install_image(capture.persist(&temp.path().join("held.index")).unwrap());
    assert_eq!(index.retention_floor(), cut(0, 1));
    assert_eq!(
        value(&index, &log, A, frozen.clone()).as_deref(),
        Some(b"old-a".as_slice())
    );
    drop(pin);
    drop(frozen);
    let capture = index.prepare_compacted_image(cut(2, 1), cut(2, 1)).unwrap();
    index.install_image(
        capture
            .persist(&temp.path().join("released.index"))
            .unwrap(),
    );
    assert_eq!(index.retention_floor(), cut(2, 1));
}

#[test]
fn sparse_view_metadata_is_bounded_and_failed_install_preserves_live_view() {
    let temp = tempdir::TempDir::new("sparse-index-budget").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(0, vec![put(0, A, b"old")]),
        batch(2, vec![put(0, B, b"new")]),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    install(&mut index, &log, &batches[1], receipts[1]);
    let upper = match cut(2, 0) {
        IndexReadCut::At(v) => v,
        _ => unreachable!(),
    };
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    index
        .install_live_view(upper, cut(0, 0), vec![hole])
        .unwrap();
    let old = index.latest_cut();
    let charged = index.stats().transient_metadata_bytes;
    assert!(charged > 0);
    let mut oversized = Vec::with_capacity(BUDGET / std::mem::size_of::<BatchVersion>() + 1);
    oversized.push(hole);
    assert!(matches!(
        index.install_live_view(upper, cut(0, 0), oversized),
        Err(NativeIndexError::Capacity)
    ));
    assert_eq!(index.latest_cut(), old);
    let capture = index.prepare_compacted_image(cut(2, 0), cut(2, 0)).unwrap();
    assert!(matches!(
        index.install_live_view(upper, IndexReadCut::BeforeAll, vec![hole]),
        Err(NativeIndexError::RetentionInProgress)
    ));
    drop(capture);
    index.install_live_view(upper, cut(0, 0), vec![]).unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, charged);
    drop(old);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn publication_reservations_are_bounded_and_released() {
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let one = index.reserve_publication_view(1).unwrap();
    let charge = index.stats().transient_metadata_bytes;
    assert!(charge > 0);
    index.max_metadata_bytes = charge * 2;
    let two = index.reserve_publication_view(1).unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, charge * 2);
    assert!(matches!(
        index.reserve_publication_view(1),
        Err(NativeIndexError::Capacity)
    ));
    assert!(matches!(
        index.reserve_publication_view(usize::MAX),
        Err(NativeIndexError::Capacity)
    ));
    drop(one);
    assert_eq!(index.stats().transient_metadata_bytes, charge);
    let replacement = index.reserve_publication_view(1).unwrap();
    drop(two);
    drop(replacement);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn publication_reservation_growth_is_preflighted_and_never_shrinks() {
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let foreign = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let charge = NativeIndex::publication_view_charge(1).unwrap();
    index.max_metadata_bytes = charge * 2;
    let mut first = index.reserve_publication_view(0).unwrap();
    let mut second = index.reserve_publication_view(0).unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    let additional = first.additional_charge(1).unwrap() + second.additional_charge(1).unwrap();
    assert_eq!(additional, charge * 2);
    assert!(matches!(
        index.check_publication_view_capacity(additional + 1),
        Err(NativeIndexError::Capacity)
    ));
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    index.check_publication_view_capacity(additional).unwrap();
    assert!(matches!(
        foreign.grow_publication_view(&mut first, 1),
        Err(NativeIndexError::Invalid(_))
    ));
    index.grow_publication_view(&mut first, 1).unwrap();
    index.grow_publication_view(&mut second, 1).unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, charge * 2);
    assert!(matches!(
        index.grow_publication_view(&mut first, 2),
        Err(NativeIndexError::Capacity)
    ));
    assert_eq!(first.additional_charge(1).unwrap(), 0);
    index.grow_publication_view(&mut first, 0).unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, charge * 2);
    drop(second);
    index.grow_publication_view(&mut first, 2).unwrap();
    assert_eq!(
        index.stats().transient_metadata_bytes,
        NativeIndex::publication_view_charge(2).unwrap()
    );
    drop(first);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn reserved_view_conversion_keeps_frozen_clones_charged_until_last_drop() {
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let upper = match cut(2, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    index.latest = Some(upper);
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    let reservation = index.reserve_publication_view(2).unwrap();
    let reserved = index.stats().transient_metadata_bytes;
    index
        .install_reserved_live_view(upper, IndexReadCut::BeforeAll, vec![hole], reservation)
        .unwrap();
    let charged = index.stats().transient_metadata_bytes;
    assert!(charged > 0 && charged < reserved);
    let frozen = index.latest_cut();
    let clone = frozen.clone();
    // Replacing the last live owner takes the same retention lock in Drop.
    // This must finish without holding that lock, even for an empty view.
    index
        .install_live_view(upper, IndexReadCut::BeforeAll, vec![])
        .unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, charged);
    drop(frozen);
    assert_eq!(index.stats().transient_metadata_bytes, charged);
    drop(clone);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    index
        .install_live_view(upper, IndexReadCut::BeforeAll, vec![hole])
        .unwrap();
    index
        .install_live_view(upper, IndexReadCut::BeforeAll, vec![hole])
        .unwrap();
    let reservation = index.reserve_publication_view(1).unwrap();
    index
        .install_reserved_live_view(upper, IndexReadCut::BeforeAll, vec![], reservation)
        .unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn active_full_capture_has_independent_view_allowance() {
    let temp = tempdir::TempDir::new("sparse-independent-budgets").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(0, vec![put(0, A, b"old")]),
        batch(2, vec![put(0, B, b"new")]),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    install(&mut index, &log, &batches[1], receipts[1]);
    index.max_metadata_bytes = index.resident_bytes();
    let capture = index.prepare_compacted_image(cut(0, 0), cut(0, 0)).unwrap();
    assert_eq!(
        index.stats().transient_metadata_bytes,
        index.max_metadata_bytes
    );
    let reservation = index.reserve_publication_view(1).unwrap();
    assert!(index.stats().transient_metadata_bytes > index.max_metadata_bytes);
    let upper = match cut(2, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    index
        .install_reserved_live_view(
            upper,
            cut(0, 0),
            vec![BatchVersion {
                epoch: EPOCH,
                sequence: 1,
            }],
            reservation,
        )
        .unwrap();
    assert!(index.stats().transient_metadata_bytes > index.max_metadata_bytes);
    drop(capture);
    assert!(index.stats().transient_metadata_bytes <= index.max_metadata_bytes);
}

#[test]
fn foreign_and_undersized_reservations_reject_without_changing_view() {
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let foreign = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let upper = match cut(2, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    index.latest = Some(upper);
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    let token = foreign.reserve_publication_view(1).unwrap();
    assert!(matches!(
        index.install_reserved_live_view(upper, IndexReadCut::BeforeAll, vec![hole], token),
        Err(NativeIndexError::Invalid(_))
    ));
    assert_eq!(foreign.stats().transient_metadata_bytes, 0);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    let token = index.reserve_publication_view(1).unwrap();
    let mut exclusions = Vec::with_capacity(2);
    exclusions.push(hole);
    assert!(matches!(
        index.install_reserved_live_view(upper, IndexReadCut::BeforeAll, exclusions, token),
        Err(NativeIndexError::Invalid(_))
    ));
    assert_eq!(index.latest_cut(), IndexReadCut::At(upper));
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}
