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
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    index
        .writer()
        .flush_and_unload(&temp.path().join("base.index"), IndexReadCut::Latest)
        .unwrap();
    install(&index, &log, &batches[2], receipts[2]);
    let upper = match cut(2, 1) {
        IndexReadCut::At(v) => v,
        _ => unreachable!(),
    };
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    index
        .writer()
        .install_live_view(upper, cut(0, 1), vec![hole])
        .unwrap();
    let frozen = index.latest_cut();
    let pin = index.pin_cut(frozen.clone()).unwrap();
    assert!(matches!(
        index.writer().prepare_image(frozen.clone()),
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
    let prefix_capture = index
        .writer()
        .prepare_compacted_image(cut(0, 1), cut(0, 1))
        .unwrap();
    install(&index, &log, &batches[1], receipts[1]);
    index
        .writer()
        .install_live_view(upper, cut(0, 1), vec![])
        .unwrap();
    let prefix_path = temp.path().join("prefix-during-hole.index");
    index
        .writer()
        .begin_install_image(prefix_capture.persist(&prefix_path).unwrap());
    assert!(!index.writer().install_image_step(1));
    assert_eq!(
        value(&index, &log, A, frozen.clone()).as_deref(),
        Some(b"old-a".as_slice())
    );
    assert_eq!(
        value(&index, &log, B, frozen.clone()).as_deref(),
        Some(b"new-b".as_slice())
    );
    assert_eq!(value(&index, &log, A, IndexReadCut::Latest), None);
    assert!(index.writer().install_image_step(1));
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
    let capture = index
        .writer()
        .prepare_compacted_image(cut(2, 1), cut(2, 1))
        .unwrap();
    index
        .writer()
        .install_image(capture.persist(&temp.path().join("held.index")).unwrap());
    assert_eq!(index.retention_floor(), cut(0, 1));
    assert_eq!(
        value(&index, &log, A, frozen.clone()).as_deref(),
        Some(b"old-a".as_slice())
    );
    drop(pin);
    drop(frozen);
    let capture = index
        .writer()
        .prepare_compacted_image(cut(2, 1), cut(2, 1))
        .unwrap();
    index.writer().install_image(
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
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    let upper = match cut(2, 0) {
        IndexReadCut::At(v) => v,
        _ => unreachable!(),
    };
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    index
        .writer()
        .install_live_view(upper, cut(0, 0), vec![hole])
        .unwrap();
    let old = index.latest_cut();
    let charged = index.stats().transient_metadata_bytes;
    assert!(charged > 0);
    let mut oversized = Vec::with_capacity(BUDGET / std::mem::size_of::<BatchVersion>() + 1);
    oversized.push(hole);
    assert!(matches!(
        index
            .writer()
            .install_live_view(upper, cut(0, 0), oversized),
        Err(NativeIndexError::Capacity)
    ));
    assert_eq!(index.latest_cut(), old);
    let capture = index
        .writer()
        .prepare_compacted_image(cut(0, u32::MAX), cut(0, 0))
        .unwrap();
    assert!(matches!(
        index
            .writer()
            .install_live_view(upper, IndexReadCut::BeforeAll, vec![hole]),
        Err(NativeIndexError::RetentionInProgress)
    ));
    drop(capture);
    index
        .writer()
        .install_live_view(upper, cut(0, 0), vec![])
        .unwrap();
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

/// The standing budget is the only view quota: with nothing else left, every
/// replacement install succeeds, reader-held replaced views add no charge,
/// and the reserved total stays twice the worst-case view charge.
#[test]
fn standing_budget_replacements_take_no_quota_and_stay_constant() {
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let upper = match cut(3, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    index.writer.lock().latest = Some(upper);
    let budget = index.reserve_standing_view_budget(2).unwrap();
    let charge = index.stats().transient_metadata_bytes;
    assert_eq!(charge, 2 * view_charge(2).unwrap());
    index.max_metadata_bytes = charge;
    assert!(matches!(
        index.reserve_publication_view(1),
        Err(NativeIndexError::Capacity)
    ));
    let view = view_charge(2).unwrap();
    let mut readers = Vec::new();
    for round in 0..8u64 {
        let mut excluded = Vec::with_capacity(2);
        excluded.push(BatchVersion {
            epoch: EPOCH,
            sequence: 1 + round % 2,
        });
        index
            .writer()
            .install_standing_live_view(upper, IndexReadCut::BeforeAll, excluded, &budget)
            .unwrap();
        let installed = index.latest_cut();
        assert!(matches!(installed, IndexReadCut::Sparse(_)));
        readers.push(installed);
        assert_eq!(index.stats().transient_metadata_bytes, charge);
        // Every earlier view is replaced and still held by `readers`.
        let held = usize::try_from(round).unwrap() * view;
        assert_eq!(index.stats().reader_held_view_bytes, held);
    }
    let installed = index.latest_cut();
    let mut oversized = Vec::with_capacity(3);
    oversized.push(BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    });
    assert!(matches!(
        index.writer().install_standing_live_view(
            upper,
            IndexReadCut::BeforeAll,
            oversized,
            &budget
        ),
        Err(NativeIndexError::Invalid(_))
    ));
    let foreign = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let foreign_budget = foreign.reserve_standing_view_budget(2).unwrap();
    assert!(matches!(
        index.writer().install_standing_live_view(
            upper,
            IndexReadCut::BeforeAll,
            vec![BatchVersion {
                epoch: EPOCH,
                sequence: 1,
            }],
            &foreign_budget
        ),
        Err(NativeIndexError::Invalid(_))
    ));
    assert_eq!(index.latest_cut(), installed);
    drop(installed);
    index
        .writer()
        .install_standing_live_view(upper, IndexReadCut::BeforeAll, vec![], &budget)
        .unwrap();
    assert_eq!(index.latest_cut(), IndexReadCut::At(upper));
    assert_eq!(index.stats().reader_held_view_bytes, 8 * view);
    drop(readers);
    assert_eq!(index.stats().transient_metadata_bytes, charge);
    assert_eq!(index.stats().reader_held_view_bytes, 0);
    drop(budget);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn reserved_view_conversion_keeps_frozen_clones_charged_until_last_drop() {
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let upper = match cut(2, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    index.writer.lock().latest = Some(upper);
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    let reservation = index.reserve_publication_view(2).unwrap();
    let reserved = index.stats().transient_metadata_bytes;
    index
        .writer()
        .install_reserved_live_view(upper, IndexReadCut::BeforeAll, vec![hole], reservation)
        .unwrap();
    let charged = index.stats().transient_metadata_bytes;
    assert!(charged > 0 && charged < reserved);
    let frozen = index.latest_cut();
    let clone = frozen.clone();
    // Replacing the last live owner takes the same retention lock in Drop.
    // This must finish without holding that lock, even for an empty view.
    index
        .writer()
        .install_live_view(upper, IndexReadCut::BeforeAll, vec![])
        .unwrap();
    assert_eq!(index.stats().transient_metadata_bytes, charged);
    // A charged replaced view is inside the budget, not reader-held extra.
    assert_eq!(index.stats().reader_held_view_bytes, 0);
    drop(frozen);
    assert_eq!(index.stats().transient_metadata_bytes, charged);
    drop(clone);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    index
        .writer()
        .install_live_view(upper, IndexReadCut::BeforeAll, vec![hole])
        .unwrap();
    index
        .writer()
        .install_live_view(upper, IndexReadCut::BeforeAll, vec![hole])
        .unwrap();
    let reservation = index.reserve_publication_view(1).unwrap();
    index
        .writer()
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
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    index.max_metadata_bytes = index.resident_bytes();
    let capture = index
        .writer()
        .prepare_compacted_image(cut(0, 0), cut(0, 0))
        .unwrap();
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
        .writer()
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
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let foreign = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    let upper = match cut(2, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    index.writer.lock().latest = Some(upper);
    index.writer().publish_latest();
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    let token = foreign.reserve_publication_view(1).unwrap();
    assert!(matches!(
        index.writer().install_reserved_live_view(
            upper,
            IndexReadCut::BeforeAll,
            vec![hole],
            token
        ),
        Err(NativeIndexError::Invalid(_))
    ));
    assert_eq!(foreign.stats().transient_metadata_bytes, 0);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    let token = index.reserve_publication_view(1).unwrap();
    let mut exclusions = Vec::with_capacity(2);
    exclusions.push(hole);
    assert!(matches!(
        index.writer().install_reserved_live_view(
            upper,
            IndexReadCut::BeforeAll,
            exclusions,
            token
        ),
        Err(NativeIndexError::Invalid(_))
    ));
    assert_eq!(index.latest_cut(), IndexReadCut::At(upper));
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn sparse_gap_generations_merge_for_install_and_abandoned_capture() {
    let temp = tempdir::TempDir::new("sparse-generation-merge").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches: Vec<_> = (0..10)
        .map(|sequence| {
            let value = format!("v{sequence}");
            let mut mutations = vec![put(0, A, value.as_bytes())];
            if sequence == 0 {
                mutations.push(put(0, B, b"other"));
            }
            batch(sequence, mutations)
        })
        .collect();
    let mut receipts = Vec::new();
    for group in batches.chunks(4) {
        receipts.extend(durable(&log, group));
    }
    let upper = match cut(8, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!(),
    };
    for install_image in [true, false] {
        let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
        for n in [0, 2, 4, 6, 8] {
            install(&index, &log, &batches[n], receipts[n]);
        }
        let excluded = [1, 3, 5, 7]
            .into_iter()
            .map(|sequence| BatchVersion {
                epoch: EPOCH,
                sequence,
            })
            .collect();
        index
            .writer()
            .install_live_view(upper, cut(0, 0), excluded)
            .unwrap();
        let frozen = index.latest_cut();
        let _pin = index.pin_cut(frozen.clone()).unwrap();
        index
            .writer()
            .begin_compacted_capture(cut(0, 1), cut(0, 0))
            .unwrap();
        assert!(index.writer().capture_step(1).is_none());
        for n in [1, 3, 5, 7, 9] {
            install(&index, &log, &batches[n], receipts[n]);
        }
        let captured = index.writer().capture_step(1).unwrap();
        if install_image {
            let persisted = captured.persist(&temp.path().join("merged.index")).unwrap();
            index.writer().begin_install_image(persisted);
            assert!(!index.writer().install_image_step(1));
            assert_eq!(
                value(&index, &log, A, frozen.clone()).as_deref(),
                Some(b"v8".as_slice())
            );
            assert!(index.writer().install_image_step(1));
        } else {
            drop(captured);
            assert!(!index.writer().cleanup_failed_capture_step(1).unwrap());
            assert!(index.writer().cleanup_failed_capture_step(1).unwrap());
        }
        assert_eq!(
            value(&index, &log, A, frozen.clone()).as_deref(),
            Some(b"v8".as_slice())
        );
        for sequence in 0..10 {
            let expected = format!("v{sequence}");
            assert_eq!(
                value(&index, &log, A, cut(sequence, u32::MAX)).as_deref(),
                Some(expected.as_bytes())
            );
        }
        assert_eq!(keys(&index, false, frozen), [A.to_vec(), B.to_vec()]);
    }
}
