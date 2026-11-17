//! Frozen exact-mode index root 4 fixtures. The bytes were produced by the
//! encoder before the buffered cell encoder (base revision e3b920d). Exact
//! mode must keep writing them byte for byte and keep reading them.

use super::*;

const ROOT_1: &[u8] = include_bytes!("fixtures/index-1-1.snapshot");
const CELLS_1: &[u8] = include_bytes!("fixtures/index-1-1.snapshot.cells");
const ROOT_2: &[u8] = include_bytes!("fixtures/index-1-2.snapshot");
const CELLS_2: &[u8] = include_bytes!("fixtures/index-1-2.snapshot.cells");

/// Keys of every keyspace that the fixture batches write.
const KEYS: [(u8, &[u8]); 7] = [
    (0, A),
    (0, B),
    (0, C),
    (1, REDUCED),
    (2, b"z-last"),
    (2, b"a-first"),
    (3, b"hash key"),
];

/// Two generations in `dir`: the first rewrites every cell with floor (0,5);
/// the second rewrites only the cells that batch 2 touches and reuses the
/// first container for the others.
pub(super) fn build_fixture_index(dir: &Path) -> (NativeIndex, PartitionLog) {
    let log = log(&dir.join("wal"));
    let batches = [
        batch(
            0,
            vec![
                put(0, A, b"first"),
                put(0, B, b"middle"),
                put(1, REDUCED, b"reduced"),
                put(2, b"z-last", b"z"),
                put(3, b"hash key", b"hashed"),
                delete(A),
                put(0, A, b"final"),
            ],
        ),
        batch(1, vec![delete(B), put(0, C, b"third")]),
        batch(2, vec![put(0, C, b"fourth"), put(2, b"a-first", b"a")]),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    let first = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(0, 5))
        .unwrap()
        .persist(&dir.join("index-1-1.snapshot"))
        .unwrap();
    index.writer().install_image(first);
    install(&index, &log, &batches[2], receipts[2]);
    let second = index
        .writer()
        .prepare_compacted_image(IndexReadCut::Latest, cut(0, 5))
        .unwrap()
        .persist(&dir.join("index-1-2.snapshot"))
        .unwrap();
    index.writer().install_image(second);
    (index, log)
}

#[test]
fn exact_mode_writes_the_frozen_root_4_bytes() {
    let temp = tempdir::TempDir::new("native-legacy-root4").unwrap();
    let (index, _log) = build_fixture_index(temp.path());
    for (name, frozen) in [
        ("index-1-1.snapshot", ROOT_1),
        ("index-1-1.snapshot.cells", CELLS_1),
        ("index-1-2.snapshot", ROOT_2),
        ("index-1-2.snapshot.cells", CELLS_2),
    ] {
        assert_eq!(
            std::fs::read(temp.path().join(name)).unwrap(),
            frozen,
            "{name}"
        );
    }
    assert_eq!(
        index.reachable_files(),
        ["index-1-1.snapshot.cells", "index-1-2.snapshot.cells"]
    );
}

#[test]
fn exact_mode_reads_the_frozen_root_4_bytes() {
    let live = tempdir::TempDir::new("native-legacy-root4-live").unwrap();
    let (index, _log) = build_fixture_index(live.path());
    let frozen = tempdir::TempDir::new("native-legacy-root4-frozen").unwrap();
    for (name, bytes) in [
        ("index-1-1.snapshot.cells", CELLS_1),
        ("index-1-2.snapshot", ROOT_2),
        ("index-1-2.snapshot.cells", CELLS_2),
    ] {
        std::fs::write(frozen.path().join(name), bytes).unwrap();
    }
    let reopened = NativeIndex::open_snapshot(
        &frozen.path().join("index-1-2.snapshot"),
        &shape(),
        DATABASE,
        BUDGET,
    )
    .unwrap();
    assert_eq!(reopened.latest_cut(), index.latest_cut());
    assert_eq!(reopened.latest_cut(), cut(2, 1));
    assert_eq!(reopened.reachable_files(), index.reachable_files());
    for read_cut in [IndexReadCut::Latest, cut(0, 5), cut(0, 6), cut(1, 1)] {
        for (keyspace, key) in KEYS {
            assert_eq!(
                reopened
                    .get(KeySpace(keyspace), key, read_cut.clone())
                    .unwrap(),
                index
                    .get(KeySpace(keyspace), key, read_cut.clone())
                    .unwrap(),
                "keyspace {keyspace} key {key:?} at {read_cut:?}"
            );
        }
    }
    // The fixture retains history: batch 1 deleted B; at (0,6) it is visible.
    assert!(
        reopened
            .get(KeySpace(0), B, IndexReadCut::Latest)
            .unwrap()
            .is_none()
    );
    assert!(reopened.get(KeySpace(0), B, cut(0, 6)).unwrap().is_some());
}
