//! Relocation scan arithmetic: per-owner span budgets, monotone callback
//! starts, and per-stream terminals and whole-file targets.
//!
//! The pass itself runs on the leased owner fixture; these tests pin the
//! arithmetic that decides what may be reclaimed, where an off-by-one file
//! would either leak capacity or retire live bytes.
use super::*;

const FILE: u64 = 1 << 20;

fn scan(node: usize, partition: u32, generation: u64, from: u64, end: u64) -> StreamScan {
    StreamScan {
        node,
        partition,
        generation,
        from,
        required_end: end,
    }
}

fn cursor(
    scan: StreamScan,
    start: u64,
    upper: Option<u64>,
    head: Option<u64>,
    version: BatchVersion,
) -> Cursor {
    let mut buffer = VecDeque::new();
    if let Some(offset) = head {
        buffer.push_back(RelocationBatch {
            version,
            address: crate::distributed::types::PhysicalValueAddress {
                partition: scan.partition,
                segment_generation: scan.generation,
                offset,
                len: 4096,
            },
            puts: Vec::new(),
        });
    }
    Cursor {
        next: None,
        scan,
        buffer,
        start: Some(start),
        upper,
    }
}

#[test]
fn a_two_generation_cell_classifies_by_its_own_stream() {
    // One cell holding entries of generations 3 and 5 at the same offset 50.
    // Generation 3's target is 100 bytes, generation 5's is 40, so the two
    // entries must land on opposite sides.
    let targets = [(0, 3, 100), (0, 5, 40)];
    let old = position(0, 3, 50);
    let live = position(0, 5, 50);
    assert!(
        below_target(&old, &targets),
        "generation 3 at 50 is below its own 100-byte target"
    );
    assert!(
        !below_target(&live, &targets),
        "generation 5 at 50 is above its own 40-byte target"
    );
    // A check that ignored the generation would classify one of them wrongly:
    // against generation 3's larger target the live entry looks dead.
    assert!(!below_target(&live, &[(0, 3, 100)]));
    // Against generation 5's smaller target the old entry keeps its position.
    assert!(!below_target(&old, &[(0, 5, 40)]));
}

fn position(partition: u32, generation: u64, offset: u64) -> VersionPosition {
    VersionPosition::from_member(
        BatchVersion {
            epoch: generation,
            sequence: 1,
        },
        0,
        crate::distributed::types::PhysicalValueAddress {
            partition,
            segment_generation: generation,
            offset,
            len: 4096,
        },
        0,
    )
}

#[test]
fn span_budget_takes_at_least_one_whole_file() {
    // A tenth of a 10-file span is one file: the minimum wins.
    let ends = span_budget_end(
        &[OwnerSpan {
            generation: 1,
            lo: 0,
            hi: 10 * FILE,
        }],
        FILE,
        10,
    );
    assert_eq!(ends.get(&1).copied(), Some(FILE));
}

#[test]
fn span_budget_consumes_the_oldest_generation_first() {
    let ends = span_budget_end(
        &[
            OwnerSpan {
                generation: 2,
                lo: 0,
                hi: 4 * FILE,
            },
            OwnerSpan {
                generation: 1,
                lo: 0,
                hi: 4 * FILE,
            },
        ],
        FILE,
        50,
    );
    assert_eq!(ends.get(&1).copied(), Some(4 * FILE));
    assert_eq!(ends.get(&2).copied(), Some(4 * FILE));
}

#[test]
fn span_budget_never_passes_a_stream_end() {
    let ends = span_budget_end(
        &[
            OwnerSpan {
                generation: 1,
                lo: 0,
                hi: 2 * FILE,
            },
            OwnerSpan {
                generation: 2,
                lo: 0,
                hi: 6 * FILE,
            },
        ],
        FILE,
        100,
    );
    assert_eq!(ends.get(&1).copied(), Some(2 * FILE));
    assert_eq!(ends.get(&2).copied(), Some(6 * FILE));
}

#[test]
fn callback_starts_only_move_up() {
    let adopted = BTreeMap::from([((0, 1), 2 * FILE)]);
    let proposed = BTreeMap::from([((0, 1), FILE)]);
    let merged = merge_callback_starts(
        &adopted,
        &proposed,
        &[vec![1]],
        &HashMap::from([(0, 0)]),
        FILE,
    )
    .unwrap();
    assert_eq!(merged.get(&(0, 1)).copied(), Some(2 * FILE));
}

#[test]
fn callback_starts_reject_a_misaligned_or_unretained_stream() {
    let adopted = BTreeMap::new();
    let misaligned = BTreeMap::from([((0, 1), FILE + 1)]);
    assert!(
        merge_callback_starts(
            &adopted,
            &misaligned,
            &[vec![1]],
            &HashMap::from([(0, 0)]),
            FILE
        )
        .is_err()
    );
    let unretained = BTreeMap::from([((0, 2), FILE)]);
    assert!(
        merge_callback_starts(
            &adopted,
            &unretained,
            &[vec![1]],
            &HashMap::from([(0, 0)]),
            FILE
        )
        .is_err()
    );
}

#[test]
fn no_stop_reclaims_up_to_the_clean_tail() {
    let cursors = vec![cursor(
        scan(0, 0, 1, 0, 4 * FILE + 4096),
        0,
        None,
        None,
        BatchVersion {
            epoch: 1,
            sequence: 0,
        },
    )];
    let reports = stream_reports(&cursors, None, FILE, 100, 1).unwrap();
    assert_eq!(reports[0].terminal_position, 4 * FILE + 4096);
    assert_eq!(reports[0].rounded_target, 4 * FILE);
}

#[test]
fn a_stop_ends_the_stop_stream_and_bounds_the_others() {
    let stop_version = BatchVersion {
        epoch: 1,
        sequence: 7,
    };
    let cursors = vec![
        cursor(scan(0, 0, 1, 0, 8 * FILE), 0, None, None, stop_version),
        cursor(
            scan(1, 1, 1, 0, 8 * FILE),
            0,
            None,
            Some(5 * FILE),
            BatchVersion {
                epoch: 1,
                sequence: 9,
            },
        ),
    ];
    let reports =
        stream_reports(&cursors, Some((0, stop_version, 3 * FILE)), FILE, 100, 1).unwrap();
    // The Stop stream ends at the Stop batch; the other stream ends at its
    // first unconsumed head, which is after the Stop in logical order.
    assert_eq!(reports[0].terminal_position, 3 * FILE);
    assert_eq!(reports[0].rounded_target, 3 * FILE);
    assert_eq!(reports[1].terminal_position, 5 * FILE);
    assert_eq!(reports[1].rounded_target, 5 * FILE);
}

#[test]
fn a_partial_file_never_becomes_a_target() {
    let cursors = vec![cursor(
        scan(0, 0, 1, 0, 8 * FILE),
        0,
        None,
        None,
        BatchVersion {
            epoch: 1,
            sequence: 0,
        },
    )];
    // Terminal below one whole file past the origin: target rounds to lo.
    let mut cursors = cursors;
    cursors[0].upper = Some(FILE - 4096);
    let reports = stream_reports(&cursors, None, FILE, 100, 1).unwrap();
    assert_eq!(reports[0].terminal_position, FILE - 4096);
    assert_eq!(reports[0].rounded_target, 0);
}

fn version(sequence: u64) -> BatchVersion {
    BatchVersion { epoch: 1, sequence }
}

#[test]
fn a_store_that_did_not_store_is_never_usable() {
    let q = version(10);
    for forced in [false, true] {
        for cut in [version(9), version(10), version(11)] {
            for promotion_cut in [version(9), version(10), version(11)] {
                assert!(
                    !usable_after_snapshot(false, forced, cut, promotion_cut, q),
                    "forced={forced} cut={cut:?} promotion={promotion_cut:?}"
                );
            }
        }
    }
}

#[test]
fn an_unforced_store_must_cover_the_scan_cut_itself() {
    let q = version(10);
    // `promotion_cut` is irrelevant on this branch: it is varied against the
    // answer to prove it.
    for promotion_cut in [version(0), version(10), version(99)] {
        assert!(!usable_after_snapshot(
            true,
            false,
            version(9),
            promotion_cut,
            q
        ));
        assert!(usable_after_snapshot(
            true,
            false,
            version(10),
            promotion_cut,
            q
        ));
        assert!(usable_after_snapshot(
            true,
            false,
            version(11),
            promotion_cut,
            q
        ));
    }
}

#[test]
fn a_forced_store_is_bounded_by_its_flush_frontier_not_its_cut() {
    let q = version(10);
    // The forced cut is the previous target: it is not the gate value, so it is
    // varied below, at and above `q` to show that only the frontier the store
    // used matters.
    for cut in [version(0), version(9), version(10), version(99)] {
        assert!(!usable_after_snapshot(true, true, cut, version(9), q));
        assert!(usable_after_snapshot(true, true, cut, version(10), q));
        assert!(usable_after_snapshot(true, true, cut, version(11), q));
    }
}
