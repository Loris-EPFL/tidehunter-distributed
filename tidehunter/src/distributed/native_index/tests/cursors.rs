use super::*;
use std::collections::BTreeSet;

#[test]
fn paused_scan_and_retained_history_cursors_survive_unload_and_base_replacement() {
    let temp = tempdir::TempDir::new("index-cursor-base-replacement").unwrap();
    let log = log(&temp.path().join("wal"));
    let middle = [0, 0, 0, 2];
    let last = [0, 0, 0, 3];
    let future = [0, 0, 0, 4];
    let batches = [
        batch(0, vec![put(0, A, b"old"), put(0, &last, b"last")]),
        batch(1, vec![put(0, A, b"new"), put(0, &middle, b"middle")]),
        batch(2, vec![delete(A), put(0, &future, b"future")]),
    ];
    let receipts = durable(&log, &batches);
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batches[0], receipts[0]);
    index
        .flush_and_unload(&temp.path().join("base.index"), IndexReadCut::Latest)
        .unwrap();
    install(&mut index, &log, &batches[1], receipts[1]);
    let cut = index.latest_cut();
    let pin = index.pin_cut(cut).unwrap();
    let expected = index.scan(KeySpace(0), None, false, 100, cut).unwrap();
    let mut expected_reverse = expected.clone();
    expected_reverse.reverse();
    let mut baseline = index.relocation_history(KeySpace(0), cut).unwrap();
    let expected_history: BTreeSet<_> = std::iter::from_fn(|| baseline.next(&index))
        .map(|(key, reference)| (key.to_vec(), reference.mutation_version()))
        .collect();
    drop(baseline);
    let mut history = index.relocation_history(KeySpace(0), cut).unwrap();
    let mut history_rows = vec![history.next(&index).unwrap()];
    let mut forward = index.scan_cursor(KeySpace(0), None, false, cut).unwrap();
    let mut reverse = index.scan_cursor(KeySpace(0), None, true, cut).unwrap();
    let mut forward_rows = vec![forward.next(&index).unwrap()];
    let mut reverse_rows = vec![reverse.next(&index).unwrap()];
    // Physical positions will change when captured overlay keys are merged into
    // the new base. The history cursor protects its *whole* supported history.
    let prepared = index.prepare_compacted_image(cut, cut).unwrap();
    install(&mut index, &log, &batches[2], receipts[2]);
    let root = temp.path().join("replacement.index");
    index.install_image(prepared.persist(&root).unwrap());
    assert_eq!(index.retention_floor(), IndexReadCut::BeforeAll);
    while let Some(row) = forward.next(&index) {
        forward_rows.push(row);
    }
    while let Some(row) = reverse.next(&index) {
        reverse_rows.push(row);
    }
    while let Some(row) = history.next(&index) {
        history_rows.push(row);
    }
    assert_eq!(forward_rows, expected);
    assert_eq!(reverse_rows, expected_reverse);
    let count = history_rows.len();
    let actual: BTreeSet<_> = history_rows
        .into_iter()
        .map(|(key, reference)| (key.to_vec(), reference.mutation_version()))
        .collect();
    assert_eq!(actual, expected_history);
    assert_eq!(
        count,
        actual.len(),
        "base refresh cannot duplicate a history version"
    );
    drop(history);
    drop(pin);
    // Once history ownership is released, a later compaction can advance.
    let compacted = index
        .prepare_compacted_image(IndexReadCut::Latest, cut)
        .unwrap();
    index.install_image(
        compacted
            .persist(&temp.path().join("compacted.index"))
            .unwrap(),
    );
    assert_eq!(index.retention_floor(), cut);
}
