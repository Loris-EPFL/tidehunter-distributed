//! `DropCells` masks: visibility by cut, retained views, scans, replay
//! application, Exact capture with retained and absorbed masks, reaping as
//! its own bounded step, birth IDs, churn bounds and root mask validation.

use super::*;
use crate::distributed::native_publication::{NativeDropCells, NativePendingSink};
use crate::index::pending_table::Transaction;
use std::sync::atomic::Ordering as AtomicOrdering;

fn drop_cells(sequence: u64, first: usize, last: usize) -> CompleteBatch {
    batch(
        sequence,
        vec![Mutation::DropCells {
            keyspace: KeySpace(0),
            first_cell: CellId::Integer(first),
            last_cell: CellId::Integer(last),
        }],
    )
}

fn present(index: &NativeIndex, key: &[u8], cut: IndexReadCut) -> bool {
    index.get(KeySpace(0), key, cut).unwrap().is_some()
}

/// A in cell 0; B and C in cell 1. Batch 3 drops cell 0, batch 4 rewrites A.
fn fixture(
    dir: &Path,
) -> (
    NativeIndex,
    PartitionLog,
    Vec<CompleteBatch>,
    Vec<DurableBatch>,
) {
    let log = log(&dir.join("wal"));
    let batches = vec![
        batch(0, vec![put(0, A, b"a0")]),
        batch(1, vec![put(0, B, b"b1")]),
        batch(2, vec![put(0, C, b"c2"), put(0, A, b"a2")]),
        drop_cells(3, 0, 0),
        batch(4, vec![put(0, A, b"a4")]),
    ];
    // A group holds at most four batches in this configuration.
    let mut receipts = durable(&log, &batches[..3]);
    receipts.extend(durable(&log, &batches[3..]));
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    (index, log, batches, receipts)
}

#[test]
fn drop_batch_has_one_member_and_no_descriptor() {
    let batch = drop_cells(3, 0, 1);
    assert_eq!(batch.mutations().len(), 1);
    assert!(batch.record_descriptors().is_empty());
    assert_eq!(
        batch.mutation_version(0).unwrap().batch,
        BatchVersion {
            epoch: EPOCH,
            sequence: 3
        }
    );
    // Size depends on the cell endpoints only, never on how many keys exist.
    assert_eq!(batch.encoded().len(), drop_cells(9, 0, 0).encoded().len());
    let decoded =
        CompleteBatch::decode(&shape(), batch.encoded().clone(), config().batch_limits).unwrap();
    assert_eq!(decoded.mutations(), batch.mutations());
    assert!(decoded.record_descriptors().is_empty());
}

#[test]
fn mask_hides_lower_versions_from_cuts_that_contain_it() {
    let temp = tempdir::TempDir::new("native-drop-cells-mask").unwrap();
    let (index, log, batches, receipts) = fixture(temp.path());
    for n in 0..4 {
        install(&index, &log, &batches[n], receipts[n]);
    }
    assert!(!present(&index, A, IndexReadCut::Latest));
    assert!(present(&index, B, IndexReadCut::Latest));
    assert!(present(&index, C, IndexReadCut::Latest));
    // A cut below the drop still reads the old value (retained view).
    assert!(present(&index, A, cut(2, 1)));
    assert_eq!(
        keys(&index, false, IndexReadCut::Latest),
        vec![B.to_vec(), C.to_vec()]
    );
    assert_eq!(
        keys(&index, true, cut(2, 1)),
        vec![C.to_vec(), B.to_vec(), A.to_vec()]
    );
    // A newer point in the dropped range survives the older mask.
    install(&index, &log, &batches[4], receipts[4]);
    assert!(present(&index, A, IndexReadCut::Latest));
    assert!(!present(&index, A, cut(3, 0)));
}

#[test]
fn empty_range_still_advances_the_publication_frontier() {
    let temp = tempdir::TempDir::new("native-drop-cells-empty").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = vec![batch(0, vec![put(0, B, b"b")]), drop_cells(1, 0, 0)];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    assert_eq!(index.call_cut(), cut(1, 0));
    assert!(present(&index, B, IndexReadCut::Latest));
}

#[test]
fn retained_mask_survives_reopen_and_absorbed_mask_reaps_the_cell() {
    let temp = tempdir::TempDir::new("native-drop-cells-root").unwrap();
    let (index, log, batches, receipts) = fixture(temp.path());
    for n in 0..4 {
        install(&index, &log, &batches[n], receipts[n]);
    }
    // Floor below the drop: the root keeps cell 0's rows and its mask.
    let retained = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(2, 1))
        .unwrap()
        .persist(&temp.path().join("index-1-1.snapshot"))
        .unwrap();
    index.writer().install_image(retained);
    let reopened = NativeIndex::open_snapshot(
        &temp.path().join("index-1-1.snapshot"),
        &shape(),
        DATABASE,
        BUDGET,
    )
    .unwrap();
    assert_eq!(reopened.call_cut(), cut(3, 0));
    assert!(!present(&reopened, A, IndexReadCut::Latest));
    assert!(present(&reopened, A, cut(2, 1)));
    assert!(present(&reopened, B, IndexReadCut::Latest));

    // Floor at the drop: the mask is absorbed, cell 0 has no row left and
    // leaves the directory without a reopen.
    let cells_before = index.stats().cells;
    let absorbed = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(3, 0))
        .unwrap()
        .persist(&temp.path().join("index-1-2.snapshot"))
        .unwrap();
    index.writer().install_image(absorbed);
    assert_eq!(index.stats().cells, cells_before - 1);
    assert!(!present(&index, A, IndexReadCut::Latest));
    let reopened = NativeIndex::open_snapshot(
        &temp.path().join("index-1-2.snapshot"),
        &shape(),
        DATABASE,
        BUDGET,
    )
    .unwrap();
    // The root cut is restored although no stored reference has its version.
    assert_eq!(reopened.call_cut(), cut(3, 0));
    assert!(!present(&reopened, A, IndexReadCut::Latest));
    assert!(present(&reopened, C, IndexReadCut::Latest));
}

#[test]
fn many_keys_in_a_dropped_cell_cost_one_mask() {
    let temp = tempdir::TempDir::new("native-drop-cells-many").unwrap();
    let log = log(&temp.path().join("wal"));
    // Cell 0 of keyspace 0 holds keys whose first byte is below 128.
    let puts: Vec<_> = (0..16u8).map(|n| put(0, &[n, 0, 0, n], b"v")).collect();
    let batches = vec![batch(0, puts), drop_cells(1, 0, 0)];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    let versions = index.stats().retained_versions;
    install(&index, &log, &batches[1], receipts[1]);
    // No tombstone rows: retained versions are unchanged by the drop.
    assert_eq!(index.stats().retained_versions, versions);
    assert!(keys(&index, false, IndexReadCut::Latest).is_empty());
}

/// Keyspace 2 (`prefix`) routes by the first byte: 256 disjoint cells.
const PREFIX: u8 = 2;

fn version(sequence: u64) -> MutationVersion {
    MutationVersion {
        batch: BatchVersion {
            epoch: EPOCH,
            sequence,
        },
        operation_index: 0,
    }
}

fn integer_cell(identity: usize) -> (KeySpace, CellId) {
    (KeySpace(0), CellId::Integer(identity))
}

fn prefix_cell(first: u8) -> CellId {
    CellId::Bytes([first][..].into())
}

fn birth(index: &NativeIndex, identity: &(KeySpace, CellId)) -> Option<u64> {
    index
        .shard_for(identity)
        .read()
        .get(identity)
        .map(|cell| cell.birth)
}

fn directory_charge(index: &NativeIndex) -> usize {
    index.accounting.directory.load(AtomicOrdering::Relaxed)
}

/// Stage, promote and publish one `DropCells` member at `sequence` through
/// the publication sink, with explicit preflight targets or (`None`) the
/// replay enumeration.
fn stage_drop(
    index: &NativeIndex,
    sequence: u64,
    keyspace: u8,
    first: CellId,
    last: CellId,
    targets: Option<Vec<DropCellsTarget>>,
) -> Result<(), NativeIndexError> {
    let mut writer = index.writer();
    let drop = NativeDropCells {
        keyspace: KeySpace(keyspace),
        first,
        last,
        targets,
    };
    NativePendingSink::drop_cells(&mut writer, &drop, version(sequence))?;
    writer.promote_committed()?;
    writer.publish_latest();
    Ok(())
}

fn targets(
    index: &NativeIndex,
    keyspace: u8,
    first: &CellId,
    last: &CellId,
) -> Vec<DropCellsTarget> {
    let mut targets = Vec::new();
    let mut after = None;
    loop {
        after = index.writer().drop_cells_targets_step(
            KeySpace(keyspace),
            first,
            last,
            after.as_ref(),
            2,
            &mut targets,
        );
        if after.is_none() {
            return targets;
        }
    }
}

fn synthetic(sequence: u64, operation_index: u32, key_len: u32) -> ValueRecordReference {
    ValueRecordReference {
        batch_id: BatchId {
            database: DATABASE,
            operation: (sequence as u128).to_be_bytes(),
        },
        batch_version: BatchVersion {
            epoch: EPOCH,
            sequence,
        },
        batch_digest: PayloadDigest([0; 32]),
        batch_address: PhysicalValueAddress {
            partition: 0,
            segment_generation: 1,
            offset: sequence * 64,
            len: 64,
        },
        descriptor: RecordDescriptor {
            operation_index,
            offset: 0,
            len: 16,
            keyspace: PREFIX,
            kind: RecordKind::Put,
            key_len,
            value_offset: 8,
            value_len: 8,
            checksum: MemberChecksum(0),
        },
    }
}

/// Publish one synthetic batch of keyspace-2 puts at `sequence`.
fn publish_synthetic(index: &NativeIndex, sequence: u64, keys: &[Vec<u8>]) {
    let transaction = Transaction::default();
    let mut writer = index.writer();
    for (operation, key) in keys.iter().enumerate() {
        let reduced = Bytes::copy_from_slice(key);
        let cell = index
            .descriptor(KeySpace(PREFIX))
            .unwrap()
            .cell_id(&reduced);
        writer
            .with_pending_table(KeySpace(PREFIX), &cell, |table| {
                table.insert(
                    reduced,
                    synthetic(sequence, operation as u32, key.len() as u32),
                    None,
                    &transaction,
                );
            })
            .unwrap();
    }
    transaction.commit();
    writer.promote_committed().unwrap();
    writer.publish_latest();
}

fn values(index: &NativeIndex, log: &PartitionLog, cut: IndexReadCut) -> [Option<Bytes>; 3] {
    [A, B, C].map(|key| value(index, log, key, cut.clone()))
}

#[test]
fn overlapping_drops_and_later_points_resolve_by_logical_version() {
    let temp = tempdir::TempDir::new("native-drop-cells-overlap").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = vec![
        batch(0, vec![put(0, A, b"a0"), put(0, B, b"b0")]),
        drop_cells(1, 0, 0),
        batch(2, vec![put(0, A, b"a2")]),
        drop_cells(3, 0, 1),
        batch(4, vec![put(0, B, b"b4")]),
    ];
    let mut receipts = durable(&log, &batches[..3]);
    receipts.extend(durable(&log, &batches[3..]));
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    for (batch, receipt) in batches.iter().zip(&receipts) {
        install(&index, &log, batch, *receipt);
    }
    let some = |bytes: &[u8]| Some(Bytes::copy_from_slice(bytes));
    for (read_cut, expected) in [
        (cut(0, 1), [some(b"a0"), some(b"b0"), None]),
        (cut(1, 0), [None, some(b"b0"), None]),
        (cut(2, 0), [some(b"a2"), some(b"b0"), None]),
        (cut(3, 0), [None, None, None]),
        (cut(4, 0), [None, some(b"b4"), None]),
        (IndexReadCut::Latest, [None, some(b"b4"), None]),
    ] {
        assert_eq!(
            values(&index, &log, read_cut.clone()),
            expected,
            "{read_cut:?}"
        );
        let visible: Vec<Vec<u8>> = [A, B, C]
            .into_iter()
            .zip(&expected)
            .filter(|(_, value)| value.is_some())
            .map(|(key, _)| key.to_vec())
            .collect();
        assert_eq!(keys(&index, false, read_cut.clone()), visible);
        let mut reversed = visible;
        reversed.reverse();
        assert_eq!(keys(&index, true, read_cut), reversed);
    }
}

#[test]
fn masks_use_the_sparse_exclusion_of_a_live_view() {
    let temp = tempdir::TempDir::new("native-drop-cells-sparse").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = vec![
        batch(0, vec![put(0, A, b"a")]),
        batch(2, vec![put(0, C, b"c")]),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    let hole = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    index
        .writer()
        .install_live_view(version(2), cut(0, 0), vec![hole])
        .unwrap();
    let sparse = index.call_cut();
    assert!(matches!(sparse, IndexReadCut::Sparse(_)));
    assert!(sparse.contains_mutation(version(0)));
    assert!(!sparse.contains_mutation(version(1)));
    assert!(sparse.contains_mutation(version(2)));
    assert!(!sparse.contains_mutation(version(3)));
    assert!(IndexReadCut::Latest.contains_mutation(version(u64::MAX)));
    assert!(!IndexReadCut::BeforeAll.contains_mutation(version(0)));
}

#[test]
fn pinned_scan_keeps_its_rows_across_a_later_drop() {
    let temp = tempdir::TempDir::new("native-drop-cells-pinned-scan").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = vec![
        batch(0, vec![put(0, A, b"a"), put(0, B, b"b"), put(0, C, b"c")]),
        drop_cells(1, 0, 1),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    let pin = index.pin_cut(IndexReadCut::Latest).unwrap();
    for reverse in [false, true] {
        let mut cursor = index
            .scan_cursor(KeySpace(0), None, reverse, pin.cut())
            .unwrap();
        let first = cursor.next(&index).unwrap().0.to_vec();
        if !reverse {
            install(&index, &log, &batches[1], receipts[1]);
        }
        let mut seen = vec![first];
        while let Some((key, _)) = cursor.next(&index) {
            seen.push(key.to_vec());
        }
        let mut expected = vec![A.to_vec(), B.to_vec(), C.to_vec()];
        if reverse {
            expected.reverse();
        }
        assert_eq!(seen, expected, "reverse={reverse}");
    }
    assert!(keys(&index, false, IndexReadCut::Latest).is_empty());
    assert_eq!(
        values(&index, &log, pin.cut())[1].as_deref(),
        Some(&b"b"[..])
    );
}

#[test]
fn reaped_targets_are_skipped_and_recreated_cells_are_refused() {
    let temp = tempdir::TempDir::new("native-drop-cells-birth").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = vec![
        batch(0, vec![put(0, A, b"a0")]),
        drop_cells(1, 0, 0),
        batch(3, vec![put(0, A, b"a3")]),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    let first_birth = birth(&index, &integer_cell(0)).unwrap();
    // A preflight of a later range collected cell 0 with its birth.
    let stale = targets(&index, 0, &CellId::Integer(0), &CellId::Integer(1));
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].birth, first_birth);
    // Checkpoint installation absorbs the mask and reaps cell 0.
    let image = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap()
        .persist(&temp.path().join("index-1-1.snapshot"))
        .unwrap();
    index.writer().install_image(image);
    assert_eq!(birth(&index, &integer_cell(0)), None);
    assert!(
        !index
            .writer
            .lock()
            .cells
            .contains_key(&(0, CellId::Integer(0)))
    );
    // The missing target is skipped; the frontier still advances.
    stage_drop(
        &index,
        2,
        0,
        CellId::Integer(0),
        CellId::Integer(1),
        Some(stale.clone()),
    )
    .unwrap();
    assert_eq!(index.call_cut(), cut(2, 0));
    // Repopulation creates a fresh materialization with a new birth.
    install(&index, &log, &batches[2], receipts[2]);
    let second_birth = birth(&index, &integer_cell(0)).unwrap();
    assert!(second_birth > first_birth);
    assert!(matches!(
        stage_drop(
            &index,
            4,
            0,
            CellId::Integer(0),
            CellId::Integer(1),
            Some(stale)
        ),
        Err(NativeIndexError::Invalid(
            "DropCells target cell was recreated"
        ))
    ));
    assert_eq!(
        value(&index, &log, A, IndexReadCut::Latest).as_deref(),
        Some(&b"a3"[..])
    );
    // A lower or equal version than the frontier is refused outright.
    assert!(matches!(
        stage_drop(&index, 3, 0, CellId::Integer(0), CellId::Integer(0), None),
        Err(NativeIndexError::Invalid(
            "DropCells below the publication frontier"
        ))
    ));
}

#[test]
fn capture_skips_cells_born_after_begin() {
    let temp = tempdir::TempDir::new("native-drop-cells-late-birth").unwrap();
    let log = log(&temp.path().join("wal"));
    // Two cells exist at `begin`: ks0 cell 0 and one ks2 cell. The capture
    // completes on the step that freezes the last of them, so a one-cell step
    // leaves it open after ks0 cell 0.
    let batches = vec![
        batch(0, vec![put(0, A, b"a"), put(2, b"new-cell", b"x")]),
        batch(1, vec![put(0, B, b"b")]),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    index.writer().begin_capture(IndexReadCut::Latest).unwrap();
    assert!(index.writer().capture_step(1).is_none());
    // ks0 cell 1 is born after the capture boundary, after the cursor's key
    // and before the ks2 cell still to freeze.
    install(&index, &log, &batches[1], receipts[1]);
    let late = birth(&index, &integer_cell(1)).unwrap();
    let prepared = loop {
        if let Some(prepared) = index.writer().capture_step(1) {
            break prepared;
        }
    };
    let root = temp.path().join("index-1-1.snapshot");
    let persisted = prepared.persist(&root).unwrap();
    index.writer().install_image(persisted);
    // The late cell kept its birth and its row; the root never saw it.
    assert_eq!(birth(&index, &integer_cell(1)), Some(late));
    assert!(value(&index, &log, B, IndexReadCut::Latest).is_some());
    let reopened = NativeIndex::open_snapshot(&root, &shape(), DATABASE, BUDGET).unwrap();
    assert!(value(&reopened, &log, A, IndexReadCut::Latest).is_some());
    assert!(value(&reopened, &log, B, IndexReadCut::Latest).is_none());
}

#[test]
fn reaping_is_its_own_step_and_rechecks_each_candidate() {
    let temp = tempdir::TempDir::new("native-drop-cells-reap-step").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = vec![
        batch(0, vec![put(0, A, b"a0")]),
        drop_cells(1, 0, 0),
        batch(2, vec![put(0, A, b"a2")]),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    let persisted = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(1, 0))
        .unwrap()
        .persist(&temp.path().join("index-1-1.snapshot"))
        .unwrap();
    index.writer().begin_install_image(persisted);
    // Installation completes without reaping; the empty cell is a candidate.
    assert!(index.writer().install_image_step(1));
    assert_eq!(index.writer.lock().reap.len(), 1);
    assert!(birth(&index, &integer_cell(0)).is_some());
    // No capture begins while a candidate remains.
    assert!(matches!(
        index.writer().begin_capture(IndexReadCut::Latest),
        Err(NativeIndexError::CheckpointInProgress)
    ));
    // A publication repopulates the cell before the reap step: it stays.
    install(&index, &log, &batches[2], receipts[2]);
    assert!(index.writer().reap_step(1));
    assert!(birth(&index, &integer_cell(0)).is_some());
    assert_eq!(
        value(&index, &log, A, IndexReadCut::Latest).as_deref(),
        Some(&b"a2"[..])
    );
    index.writer().begin_capture(IndexReadCut::Latest).unwrap();
    index.writer().abort_capture();
}

#[test]
fn create_drop_install_reap_churn_keeps_the_directory_bounded() {
    // Without reaping, 250 dead cells exceed this budget.
    let budget = 96 * 1024;
    let temp = tempdir::TempDir::new("native-drop-cells-churn").unwrap();
    let index = NativeIndex::new(&shape(), DATABASE, budget).unwrap();
    let base = directory_charge(&index);
    for first in 0..250u8 {
        let sequence = 2 * u64::from(first);
        publish_synthetic(&index, sequence, &[vec![first, 1], vec![first, 2]]);
        let cell = prefix_cell(first);
        stage_drop(&index, sequence + 1, PREFIX, cell.clone(), cell, None).unwrap();
        let image = index
            .writer()
            .prepare_compacted_image(IndexReadCut::Latest, cut(sequence + 1, 0))
            .unwrap()
            .persist(&temp.path().join(format!("index-1-{first}.snapshot")))
            .unwrap();
        index.writer().install_image(image);
        assert_eq!(index.stats().cells, 0, "iteration {first}");
        assert!(index.writer.lock().cells.is_empty());
        assert_eq!(directory_charge(&index), base, "iteration {first}");
    }
    // Births are never reused: the next cell gets a new identity.
    publish_synthetic(&index, 600, &[vec![7, 1]]);
    let identity = (KeySpace(PREFIX), prefix_cell(7));
    assert!(birth(&index, &identity).unwrap() >= 250);
}

#[test]
fn drop_cost_follows_occupied_cells_not_keys() {
    let index = NativeIndex::new(&shape(), DATABASE, 256 * 1024 * 1024).unwrap();
    let mut sequence = 0;
    for chunk in 0..50u16 {
        let keys: Vec<Vec<u8>> = (0..1000u16)
            .map(|n| {
                let id = chunk * 1000 + n;
                vec![(id % 4) as u8, (id >> 8) as u8, id as u8]
            })
            .collect();
        publish_synthetic(&index, sequence, &keys);
        sequence += 1;
    }
    let before = index.stats();
    let directory = directory_charge(&index);
    stage_drop(
        &index,
        sequence,
        PREFIX,
        prefix_cell(0),
        prefix_cell(255),
        None,
    )
    .unwrap();
    // Four occupied cells cost four masks, whatever the 50,000 keys.
    assert_eq!(directory_charge(&index) - directory, 4 * MASK_CHARGE);
    assert_eq!(index.stats().retained_versions, before.retained_versions);
    assert!(
        index
            .scan(KeySpace(PREFIX), None, false, 10, IndexReadCut::Latest)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        index
            .scan(KeySpace(PREFIX), None, false, 10, cut(sequence - 1, 999))
            .unwrap()
            .len(),
        10
    );
}

/// Root of the `fixture` index with cell 0's mask retained (floor below the
/// drop): one masked cell, one ordinal-0 version at the end of the payload.
fn masked_root(dir: &Path) -> Vec<u8> {
    let (index, log, batches, receipts) = fixture(dir);
    for n in 0..4 {
        install(&index, &log, &batches[n], receipts[n]);
    }
    let root = dir.join("index-1-1.snapshot");
    let persisted = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(2, 1))
        .unwrap()
        .persist(&root)
        .unwrap();
    index.writer().install_image(persisted);
    let bytes: Bytes = std::fs::read(&root).unwrap().into();
    CrcFrame::read_from_bytes(&bytes, 0).unwrap().to_vec()
}

#[test]
fn root_mask_section_is_validated_and_the_previous_format_has_none() {
    let temp = tempdir::TempDir::new("native-drop-cells-root-forgery").unwrap();
    let payload = masked_root(temp.path());
    assert_eq!(payload[8..10], FORMAT.to_be_bytes());
    // count +0..4 | ks +4 | cell tag +5, be_u64 id +6..14 | version count
    // +14..18 | epoch +18..26 | sequence +26..34 | ordinal +34..38
    let section = payload.len() - 38;
    assert_eq!(payload[section..section + 4], 1u32.to_be_bytes());
    assert_eq!(payload[section + 4], 0);
    assert_eq!(payload[section + 5], 0);
    assert_eq!(payload[section + 6..section + 14], 0u64.to_be_bytes());
    assert_eq!(payload[section + 14..section + 18], 1u32.to_be_bytes());
    assert_eq!(
        payload[payload.len() - 12..payload.len() - 4],
        3u64.to_be_bytes()
    );
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    let forge = |change: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = payload.clone();
        change(&mut bytes);
        bytes
    };
    cases.push((
        "more masked cells than cells",
        forge(&|b| b[section..section + 4].copy_from_slice(&9u32.to_be_bytes())),
    ));
    cases.push((
        // Keyspace 0 has cells 0 and 1 only: cell 2 is in no root.
        "absent cell",
        forge(&|b| b[section + 6..section + 14].copy_from_slice(&2u64.to_be_bytes())),
    ));
    cases.push(("unknown cell tag", forge(&|b| b[section + 5] = 7)));
    cases.push((
        "zero versions",
        forge(&|b| b[section + 14..section + 18].copy_from_slice(&0u32.to_be_bytes())),
    ));
    cases.push((
        "nonzero ordinal",
        forge(&|b| {
            let end = b.len();
            b[end - 4..].copy_from_slice(&1u32.to_be_bytes())
        }),
    ));
    cases.push((
        "above the cut",
        forge(&|b| {
            let end = b.len();
            b[end - 12..end - 4].copy_from_slice(&9u64.to_be_bytes())
        }),
    ));
    cases.push((
        "at the floor",
        forge(&|b| {
            let end = b.len();
            b[end - 12..end - 4].copy_from_slice(&2u64.to_be_bytes())
        }),
    ));
    cases.push((
        "duplicate version",
        forge(&|b| {
            let version = b[b.len() - 20..].to_vec();
            b[section + 14..section + 18].copy_from_slice(&2u32.to_be_bytes());
            b.extend_from_slice(&version);
        }),
    ));
    cases.push(("trailing byte", forge(&|b| b.push(0))));
    cases.push(("truncated section", forge(&|b| b.truncate(b.len() - 1))));
    // The previous format has no section: its bytes become trailing payload.
    cases.push((
        "previous format with masks",
        forge(&|b| b[8..10].copy_from_slice(&PREVIOUS_FORMAT.to_be_bytes())),
    ));
    cases.push((
        "next format",
        forge(&|b| b[8..10].copy_from_slice(&(FORMAT + 1).to_be_bytes())),
    ));
    for (name, forged) in cases {
        let path = temp
            .path()
            .join(format!("{}.snapshot", name.replace(' ', "-")));
        std::fs::write(&path, CrcFrame::new(&forged).as_ref()).unwrap();
        assert!(
            NativeIndex::open_snapshot(&path, &shape(), DATABASE, BUDGET).is_err(),
            "{name}"
        );
    }
    // The previous format without a section still opens (no masks).
    let mut previous = payload[..section].to_vec();
    previous[8..10].copy_from_slice(&PREVIOUS_FORMAT.to_be_bytes());
    let path = temp.path().join("previous.snapshot");
    std::fs::write(&path, CrcFrame::new(&previous).as_ref()).unwrap();
    let opened = NativeIndex::open_snapshot(&path, &shape(), DATABASE, BUDGET).unwrap();
    assert!(present(&opened, A, cut(2, 1)));
}
