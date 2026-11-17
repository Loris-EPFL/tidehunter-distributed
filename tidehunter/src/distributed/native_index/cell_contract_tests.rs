//! The frozen option C contract: prefix-cut order, selection and bytes,
//! canonical file names, the flusher count, the format-5 root rules and the
//! mixed-cut replay oracle.

use super::*;

fn batch(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

fn through(epoch: u64, sequence: u64) -> PrefixCut {
    PrefixCut::Through(batch(epoch, sequence))
}

fn mutation(epoch: u64, sequence: u64, operation_index: u32) -> MutationVersion {
    MutationVersion {
        batch: batch(epoch, sequence),
        operation_index,
    }
}

#[test]
fn prefix_cuts_order_like_recovery_and_select_whole_batches() {
    let mut cuts = vec![
        through(2, 0),
        through(1, 9),
        PrefixCut::BeforeAll,
        through(1, 10),
    ];
    cuts.sort();
    assert_eq!(
        cuts,
        [
            PrefixCut::BeforeAll,
            through(1, 9),
            through(1, 10),
            through(2, 0)
        ]
    );
    assert!(!PrefixCut::BeforeAll.contains(batch(0, 0)));
    assert!(through(1, 9).contains(batch(1, 9)));
    assert!(through(1, 9).contains(batch(0, u64::MAX)));
    assert!(!through(1, 9).contains(batch(1, 10)));
    assert_eq!(PrefixCut::BeforeAll.index_cut(), IndexReadCut::BeforeAll);
    let selection = position(&through(1, 9).index_cut());
    for operation_index in [0, 7, u32::MAX] {
        assert!(Some(mutation(1, 9, operation_index)) <= selection);
    }
    assert!(Some(mutation(1, 10, 0)) > selection);
    assert!(PrefixCut::BeforeAll.holds_floor(None));
    assert!(!PrefixCut::BeforeAll.holds_floor(Some(mutation(0, 0, 0))));
    assert!(through(1, 9).holds_floor(Some(mutation(1, 9, u32::MAX))));
    assert!(!through(1, 9).holds_floor(Some(mutation(1, 10, 0))));
}

#[test]
fn prefix_cut_bytes_are_frozen_and_reject_other_tags() {
    let mut bytes = Vec::new();
    PrefixCut::BeforeAll.encode_into(&mut bytes);
    through(0x0102, 0x0304_0506).encode_into(&mut bytes);
    assert_eq!(
        bytes,
        [
            [0u8; 17].as_slice(),
            &[1, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 3, 4, 5, 6],
        ]
        .concat()
    );
    assert_eq!(bytes.len(), 2 * PREFIX_CUT_BYTES);
    let mut cursor = Cursor::new(&bytes);
    assert_eq!(
        PrefixCut::decode(&mut cursor).unwrap(),
        PrefixCut::BeforeAll
    );
    assert_eq!(
        PrefixCut::decode(&mut cursor).unwrap(),
        through(0x0102, 0x0304_0506)
    );
    assert_eq!(cursor.remaining(), 0);
    let mut nonzero = [0u8; 17];
    nonzero[16] = 1;
    let mut latest = [0u8; 17];
    latest[0] = 2;
    for invalid in [nonzero.as_slice(), &latest, &[1; 16]] {
        assert!(PrefixCut::decode(&mut Cursor::new(invalid)).is_err());
    }
}

#[test]
fn index_store_files_have_one_canonical_name() {
    let legacy = IndexStoreFileId::LegacyContainer {
        authority_epoch: 3,
        generation: 17,
    };
    let segment = IndexStoreFileId::Segment {
        authority_epoch: 4,
        stream_id: 7,
        segment_seq: 0,
    };
    assert_eq!(legacy.canonical_name(), "index-3-17.snapshot.cells");
    assert_eq!(segment.canonical_name(), "index-store-4-7-0.cells");
    for id in [legacy, segment] {
        assert_eq!(
            IndexStoreFileId::parse_canonical(&id.canonical_name()),
            Some(id)
        );
    }
    assert!(legacy < segment);
    for name in [
        "index-store-04-7-0.cells",
        "index-store-+4-7-0.cells",
        "index-store-4-7-0-1.cells",
        "index-store-4-70000-0.cells",
        "index-store-4-7.cells",
        "index-03-17.snapshot.cells",
        "index-3-17.snapshot",
        "index-3-17-1.snapshot.cells",
        "../index-store-4-7-0.cells",
        "index-3-17.snapshot.cells.pending",
    ] {
        assert_eq!(IndexStoreFileId::parse_canonical(name), None, "{name}");
    }
}

#[test]
fn flusher_count_is_bounded_by_shards_cpus_and_cap() {
    assert_eq!(cell_flushers(0), 1);
    assert_eq!(cell_flushers(1), 1);
    for shards in [2, 64, MAX_INDEX_SHARDS] {
        let flushers = cell_flushers(shards);
        assert!((1..=MAX_CELL_FLUSHERS).contains(&flushers));
        assert!(flushers <= crate::parallelism::available().min(shards));
    }
}

fn root() -> CellRoot {
    let legacy = IndexStoreFileId::LegacyContainer {
        authority_epoch: 3,
        generation: 1,
    };
    let first = IndexStoreFileId::Segment {
        authority_epoch: 4,
        stream_id: 0,
        segment_seq: 1,
    };
    let second = IndexStoreFileId::Segment {
        authority_epoch: 4,
        stream_id: 1,
        segment_seq: 0,
    };
    let file = |id: IndexStoreFileId, sealed_len| RootFile {
        id,
        name: id.canonical_name(),
        sealed_len,
    };
    let base = |file, offset, framed_len, frame_cut, frame_floor| RootCellBase {
        file,
        offset,
        framed_len,
        digest: [9; 32],
        frame_cut,
        frame_floor,
    };
    let cell = |keyspace, cell, coverage, base| RootCell {
        keyspace: KeySpace(keyspace),
        cell,
        coverage,
        base,
    };
    CellRoot {
        header: CellRootHeader {
            format: PER_CELL_FORMAT,
            flags: 0,
            database: [7; 16],
            schema: [3; 32],
            authority_epoch: 4,
            snapshot_generation: 2,
            lifecycle_generation: 1,
            directory_target: through(4, 10),
            replay_floor: through(4, 5),
            max_frame_cut: through(4, 12),
            history_floor: Some(mutation(4, 3, 0)),
        },
        files: vec![file(legacy, 1000), file(first, 500), file(second, 12)],
        cells: vec![
            // A migrated exact frame whose old global cut is its coverage.
            cell(
                0,
                CellId::Integer(1),
                through(4, 5),
                Some(base(legacy, 0, 100, through(3, 9), None)),
            ),
            // A frame installed during root capture: B_i > T, C_i capped at T.
            cell(
                0,
                CellId::Integer(2),
                through(4, 10),
                Some(base(first, 0, 200, through(4, 12), Some(mutation(4, 3, 0)))),
            ),
            // A known cell with no frame.
            cell(0, CellId::Integer(3), through(4, 7), None),
            // A clean advance: C_i > B_i. The range ends at the sealed length.
            cell(
                1,
                CellId::Bytes([1, 2].as_slice().into()),
                through(4, 10),
                Some(base(first, 200, 300, through(4, 8), None)),
            ),
            cell(
                1,
                CellId::Bytes([9].as_slice().into()),
                through(4, 6),
                Some(base(second, 0, 12, through(4, 6), None)),
            ),
        ],
    }
}

fn check(root: &CellRoot) -> Result<(), &'static str> {
    root.check([7; 16], [3; 32]).map_err(|error| match error {
        NativeIndexError::Invalid(message) => message,
        other => panic!("unexpected root error {other:?}"),
    })
}

#[test]
fn valid_root_passes_including_ahead_frames_and_empty_cells() {
    check(&root()).unwrap();
    let mut empty = root();
    empty.files.clear();
    empty.cells.clear();
    // Zero cells: F is the captured T.
    assert_eq!(
        check(&empty),
        Err("replay floor is not the minimum coverage")
    );
    empty.header.replay_floor = empty.header.directory_target;
    empty.header.history_floor = None;
    check(&empty).unwrap();
}

#[test]
fn root_rules_reject_each_violation() {
    type Mutate = fn(&mut CellRoot);
    let cases: [(&str, Mutate); 22] = [
        ("cell root format", |root| root.header.format = FORMAT),
        ("cell root format", |root| root.header.flags = 1),
        ("cell root database or schema", |root| {
            root.header.database = [8; 16];
        }),
        ("cell root database or schema", |root| {
            root.header.schema = [4; 32];
        }),
        ("cell root generation", |root| {
            root.header.snapshot_generation = 0;
        }),
        ("history floor above replay floor", |root| {
            root.header.history_floor = Some(mutation(4, 6, 0));
        }),
        ("frame cut bound below target", |root| {
            root.header.max_frame_cut = through(4, 9);
        }),
        ("cell frame cut", |root| {
            root.header.max_frame_cut = through(4, 11);
        }),
        ("cell coverage above target", |root| {
            root.cells[2].coverage = through(4, 11);
        }),
        ("replay floor is not the minimum coverage", |root| {
            root.cells[0].coverage = through(4, 6);
        }),
        ("cell frame cut", |root| {
            root.cells[0].base.as_mut().unwrap().frame_cut = PrefixCut::BeforeAll;
        }),
        ("cell frame floor", |root| {
            // H_i above H.
            root.cells[1].base.as_mut().unwrap().frame_floor = Some(mutation(4, 4, 0));
        }),
        ("cell frame floor", |root| {
            // H_i above B_i.
            root.cells[4].base.as_mut().unwrap().frame_floor = Some(mutation(4, 7, 0));
            root.header.history_floor = Some(mutation(4, 5, 0));
        }),
        ("file table order", |root| root.files.swap(1, 2)),
        ("file table order", |root| {
            root.files[2] = root.files[1].clone()
        }),
        ("file table name", |root| {
            root.files[1].name = "index-store-4-0-01.cells".into();
        }),
        ("cell directory order", |root| root.cells.swap(0, 1)),
        // Mixed id kinds in one keyspace are rejected, not a panic.
        ("cell directory order", |root| {
            root.cells[0].cell = CellId::Bytes([1].as_slice().into());
        }),
        ("cell file id", |root| {
            root.cells[4].base.as_mut().unwrap().file = IndexStoreFileId::Segment {
                authority_epoch: 4,
                stream_id: 2,
                segment_seq: 0,
            };
        }),
        ("cell range bounds", |root| {
            root.cells[3].base.as_mut().unwrap().framed_len = 301;
        }),
        ("overlapping cell ranges", |root| {
            root.cells[3].base.as_mut().unwrap().offset = 199;
        }),
        ("unreferenced file table entry", |root| {
            root.cells[4].base = None;
        }),
    ];
    for (expected, mutate) in cases {
        let mut root = root();
        mutate(&mut root);
        assert_eq!(check(&root), Err(expected));
    }
    let mut short = root();
    short.cells[4].base.as_mut().unwrap().framed_len = 11;
    assert_eq!(check(&short), Err("cell range bounds"));
    let mut overflow = root();
    overflow.cells[4].base.as_mut().unwrap().offset = u64::MAX;
    assert_eq!(check(&overflow), Err("cell range overflow"));
}

#[test]
fn replay_oracle_splits_batches_by_cell_coverage() {
    let root = root();
    let a = (KeySpace(0), CellId::Integer(2)); // B = (4,12), C = (4,10)
    let b = (KeySpace(0), CellId::Integer(3)); // no base, C = (4,7)
    let clean = (KeySpace(1), CellId::Bytes([1, 2].as_slice().into())); // B = (4,8), C = (4,10)
    let absent = (KeySpace(1), CellId::Integer(5));
    // At or below F: the whole batch is skipped.
    assert_eq!(root.replay_plan(batch(4, 5), &[a.clone(), b.clone()]), None);
    assert_eq!(
        root.replay_plan(batch(3, 99), std::slice::from_ref(&absent)),
        None
    );
    // The split batch of spec §8.2: A's frame holds it, B's does not.
    assert_eq!(
        root.replay_plan(batch(4, 11), &[a.clone(), b.clone(), a.clone()]),
        Some(vec![
            MemberReplay::CompareBase,
            MemberReplay::Apply,
            MemberReplay::CompareBase
        ])
    );
    // Covered by coverage alone (a clean advance), and by neither.
    assert_eq!(
        root.replay_plan(batch(4, 9), std::slice::from_ref(&clean)),
        Some(vec![MemberReplay::CompareBase])
    );
    assert_eq!(
        root.replay_plan(batch(4, 11), &[clean, absent.clone()]),
        Some(vec![MemberReplay::Apply, MemberReplay::Apply])
    );
    // A fully covered batch above F, including a no-base cell whose coverage
    // holds it, is still replayed and accounted once.
    assert_eq!(
        root.replay_plan(batch(4, 6), &[a, b]),
        Some(vec![MemberReplay::CompareBase, MemberReplay::CompareBase])
    );
    // The migrated cell covers only through F = (4,5).
    assert_eq!(
        root.replay_plan(batch(4, 6), &[(KeySpace(0), CellId::Integer(1)), absent]),
        Some(vec![MemberReplay::Apply, MemberReplay::Apply])
    );
}
