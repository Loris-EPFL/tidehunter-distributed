//! Deterministic R0 tests on synthetic retained sets and owner inventories.
use super::*;
use crate::distributed::types::{
    BatchId, BatchVersion, MemberChecksum, PayloadDigest, PhysicalValueAddress, RecordDescriptor,
    RecordKind, ValueRecordReference,
};

const FILE: u64 = 1 << 20;

fn reference(
    partition: u32,
    offset: u64,
    len: u32,
    operation: u8,
    sequence: u64,
    kind: RecordKind,
) -> ValueRecordReference {
    let mut id = [0; 16];
    id[0] = operation;
    ValueRecordReference {
        batch_id: BatchId {
            database: [7; 16],
            operation: id,
        },
        batch_version: BatchVersion { epoch: 3, sequence },
        batch_digest: PayloadDigest([operation; 32]),
        batch_address: PhysicalValueAddress {
            partition,
            segment_generation: 3,
            offset,
            len,
        },
        descriptor: RecordDescriptor {
            operation_index: 0,
            offset: 8,
            len: 40,
            keyspace: 0,
            kind,
            key_len: 8,
            value_offset: 16,
            value_len: 24,
            checksum: MemberChecksum(0),
        },
    }
}

fn put(partition: u32, offset: u64, operation: u8) -> ValueRecordReference {
    reference(
        partition,
        offset,
        4096,
        operation,
        u64::from(operation),
        RecordKind::Put,
    )
}

fn file(partition: u32, start: u64) -> FileId {
    FileId {
        partition,
        generation: 3,
        start,
    }
}

#[test]
fn retained_counts_members_and_distinct_batches_per_file() {
    let mut accumulator = RetainedAccumulator::new(FILE, 1024, None).unwrap();
    // Cell 1: two members of one batch in file 0, one batch in file 1.
    accumulator.add_put(&put(0, 0, 1)).unwrap();
    accumulator.add_put(&put(0, 0, 1)).unwrap();
    accumulator.add_put(&put(0, FILE + 8192, 2)).unwrap();
    accumulator.end_cell();
    // Cell 2: the same batch again (another key) and a delete.
    accumulator.add_put(&put(0, 0, 1)).unwrap();
    accumulator
        .add_put(&reference(0, 2 * FILE, 4096, 9, 9, RecordKind::Delete))
        .unwrap();
    accumulator.end_cell();
    let (files, error) = accumulator.finish();
    assert!(error.is_none());
    assert_eq!(
        files[&file(0, 0)],
        FileRetained {
            put_members: 3,
            member_bytes: 120,
            value_bytes: 72,
            batches: 1,
            batch_bytes: 4096,
            affected_cells: 2,
        }
    );
    assert_eq!(files[&file(0, FILE)].batches, 1);
    assert_eq!(files[&file(0, FILE)].affected_cells, 1);
    // A delete has no value read obligation.
    assert!(!files.contains_key(&file(0, 2 * FILE)));
}

#[test]
fn equal_address_with_another_identity_fails_batch_counts_only() {
    let mut accumulator = RetainedAccumulator::new(FILE, 1024, None).unwrap();
    accumulator.add_put(&put(0, 0, 1)).unwrap();
    accumulator.add_put(&put(0, 0, 2)).unwrap();
    let (files, error) = accumulator.finish();
    assert!(error.is_some());
    assert_eq!(files[&file(0, 0)].put_members, 2);
}

#[test]
fn spilled_runs_merge_to_the_in_memory_result() {
    let scratch = tempdir::TempDir::new("r0-spill").unwrap();
    let mut spilled = RetainedAccumulator::new(FILE, 4, Some(scratch.path().join("runs"))).unwrap();
    let mut memory = RetainedAccumulator::new(FILE, 1 << 20, None).unwrap();
    // 40 distinct batches over three files, each seen twice in shuffled order.
    for round in 0..2u64 {
        for n in 0..40u64 {
            let n = (n * 17 + round * 5) % 40;
            let offset = n * 8192 + (n % 3) * FILE;
            let reference = put(1, offset % (3 * FILE), u8::try_from(n).unwrap());
            spilled.add_put(&reference).unwrap();
            memory.add_put(&reference).unwrap();
        }
        spilled.end_cell();
        memory.end_cell();
    }
    let (spilled, spill_error) = spilled.finish();
    let (memory, memory_error) = memory.finish();
    assert!(spill_error.is_none() && memory_error.is_none());
    assert_eq!(spilled, memory);
    assert_eq!(spilled.values().map(|f| f.batches).sum::<u64>(), 40);
    // Run files are removed with the accumulator.
    assert_eq!(
        std::fs::read_dir(scratch.path().join("runs"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn spilled_conflict_and_missing_scratch_are_reported() {
    let scratch = tempdir::TempDir::new("r0-spill-conflict").unwrap();
    let mut accumulator =
        RetainedAccumulator::new(FILE, 2, Some(scratch.path().to_path_buf())).unwrap();
    accumulator.add_put(&put(0, 0, 1)).unwrap();
    accumulator.add_put(&put(0, 8192, 2)).unwrap();
    accumulator.add_put(&put(0, 0, 3)).unwrap();
    accumulator.add_put(&put(0, 16384, 4)).unwrap();
    assert!(accumulator.finish().1.is_some());
    let mut bounded = RetainedAccumulator::new(FILE, 2, None).unwrap();
    for n in 0..3 {
        bounded
            .add_put(&put(0, n * 8192, u8::try_from(n).unwrap()))
            .unwrap();
    }
    assert!(bounded.finish().1.is_some());
}

fn row(start: u64) -> WalFileObservation {
    WalFileObservation {
        file_start: start,
        device: 1,
        inode: 100 + start / FILE,
        logical_bytes: FILE,
        allocated_bytes: FILE,
        state: FileUseState::Used,
        allocator_past: true,
        write_lease: false,
        validated_through: Some(start + FILE),
        read_pins: Some(0),
        export_pins: Some(0),
        target_access: TargetAccess::None,
    }
}

fn stream() -> StreamFacts {
    StreamFacts {
        same_generation: true,
        complete_stream: false,
        position: Some(PositionFacts {
            replay_from: 4 * FILE,
            last_offset: Some(4 * FILE + 64),
            durable_end: 4 * FILE + 4096,
        }),
        owner_root_scan_from: Some(4 * FILE),
        owner_root_synced_end: Some(4 * FILE + 4096),
        processed_end: 4 * FILE + 4096,
        page_stable: true,
        persisted_cut_next: Some(100),
    }
}

fn inputs<'a>(row: &'a WalFileObservation, stream: &'a StreamFacts) -> FileInputs<'a> {
    FileInputs {
        file_start: row.file_start,
        file_end: row.file_start + FILE,
        observation: Some(row),
        stream,
        adopted_root: true,
        obsolete_root_pending: false,
        retained_puts: 0,
        volatile_reference: false,
        sequence_bound: Some(90),
        within_budget: true,
        changed: false,
    }
}

#[test]
fn fully_proven_dead_file_is_eligible() {
    let row = row(FILE);
    let stream = stream();
    assert_eq!(evaluate(&inputs(&row, &stream)), Vec::new());
}

/// Coordinator-side facts of one blocker case.
#[derive(Clone, Copy)]
struct Flags {
    adopted_root: bool,
    obsolete_root_pending: bool,
    retained_puts: u64,
    volatile_reference: bool,
    sequence_bound: Option<u64>,
    within_budget: bool,
    changed: bool,
}

#[test]
fn every_missing_proof_is_a_blocker() {
    let stream = stream();
    let base = row(FILE);
    // Each case starts from the eligible facts and changes only what it names.
    let check = |mutate: &dyn Fn(&mut WalFileObservation, &mut StreamFacts, &mut Flags),
                 expected: &[Blocker]| {
        let mut row = base.clone();
        let mut facts = stream.clone();
        let mut flags = Flags {
            adopted_root: true,
            obsolete_root_pending: false,
            retained_puts: 0,
            volatile_reference: false,
            sequence_bound: Some(90),
            within_budget: true,
            changed: false,
        };
        mutate(&mut row, &mut facts, &mut flags);
        let input = FileInputs {
            file_start: row.file_start,
            file_end: row.file_start + FILE,
            observation: Some(&row),
            stream: &facts,
            adopted_root: flags.adopted_root,
            obsolete_root_pending: flags.obsolete_root_pending,
            retained_puts: flags.retained_puts,
            volatile_reference: flags.volatile_reference,
            sequence_bound: flags.sequence_bound,
            within_budget: flags.within_budget,
            changed: flags.changed,
        };
        assert_eq!(evaluate(&input), expected, "case {expected:?}");
    };
    check(
        &|row, _, _| row.state = FileUseState::Unknown,
        &[Blocker::UnvalidatedInventory],
    );
    check(
        &|row, _, _| {
            row.state = FileUseState::Lookahead;
            row.allocator_past = false;
            row.validated_through = None;
        },
        &[
            Blocker::Lookahead,
            Blocker::AllocatorNotPast,
            Blocker::UnvalidatedFrameOrSkip,
        ],
    );
    check(
        &|row, _, _| {
            row.state = FileUseState::Leased;
            row.write_lease = true;
        },
        &[Blocker::WriteLease],
    );
    check(
        &|row, _, _| row.validated_through = Some(FILE + 4096),
        &[Blocker::UnvalidatedFrameOrSkip],
    );
    check(
        &|_, facts, _| facts.owner_root_synced_end = Some(FILE),
        &[Blocker::OwnerRootBehind],
    );
    check(
        &|row, _, _| {
            row.target_access = TargetAccess::Writable;
            row.export_pins = Some(1);
        },
        &[Blocker::UnsafeWritableExport, Blocker::ExportPin],
    );
    // A writable export with no other users is modeled as revocable.
    check(&|row, _, _| row.target_access = TargetAccess::Writable, &[]);
    check(
        &|_, _, input| input.adopted_root = false,
        &[Blocker::NoAdoptedRoot],
    );
    check(
        &|_, _, input| input.retained_puts = 1,
        &[Blocker::SelectableRootRef],
    );
    check(
        &|_, _, input| input.obsolete_root_pending = true,
        &[Blocker::SelectableRootRef],
    );
    check(
        &|_, _, input| input.volatile_reference = true,
        &[Blocker::LiveOrCapturedRef],
    );
    check(
        &|_, facts, _| facts.owner_root_scan_from = Some(FILE),
        &[Blocker::OwnerRecoveryWindow],
    );
    check(
        &|_, facts, _| {
            if let Some(position) = &mut facts.position {
                position.replay_from = FILE;
            }
        },
        &[Blocker::CoordinatorReplayWindow],
    );
    check(
        &|_, facts, _| facts.position = None,
        &[Blocker::CoordinatorReplayWindow],
    );
    check(
        &|_, facts, _| {
            if let Some(position) = &mut facts.position {
                position.last_offset = Some(FILE + 8);
            }
        },
        &[Blocker::TailOrIdentityProof],
    );
    check(
        &|_, _, input| input.sequence_bound = Some(101),
        &[Blocker::VersionBCut],
    );
    check(
        &|_, _, input| input.sequence_bound = None,
        &[Blocker::VersionBCut],
    );
    // Synchronous tier: no tracked durable cut, no Version B requirement.
    check(
        &|_, facts, input| {
            facts.persisted_cut_next = None;
            input.sequence_bound = None;
        },
        &[],
    );
    check(&|row, _, _| row.read_pins = None, &[Blocker::ReadPin]);
    check(
        &|row, _, _| row.export_pins = Some(2),
        &[Blocker::ExportPin],
    );
    check(
        &|row, _, _| row.target_access = TargetAccess::Unknown,
        &[Blocker::TargetDrain],
    );
    check(
        &|row, _, _| row.state = FileUseState::RetiredCleanupPending,
        &[Blocker::CleanupPending],
    );
    check(
        &|_, _, input| input.within_budget = false,
        &[Blocker::SpanBudget],
    );
    check(
        &|_, _, input| input.changed = true,
        &[Blocker::ChangedDuringObservation],
    );
    check(
        &|_, facts, _| facts.page_stable = false,
        &[Blocker::ChangedDuringObservation],
    );
}

#[test]
fn absent_owner_row_and_older_streams() {
    let mut facts = stream();
    let base = row(FILE);
    let mut input = inputs(&base, &facts);
    input.observation = None;
    assert_eq!(evaluate(&input), vec![Blocker::UnvalidatedInventory]);
    // Older sealed stream: owner root and Version B rules do not apply, but
    // the committed completion does.
    facts.same_generation = false;
    facts.owner_root_scan_from = None;
    facts.owner_root_synced_end = None;
    facts.persisted_cut_next = None;
    let input = inputs(&base, &facts);
    assert_eq!(evaluate(&input), vec![Blocker::CoordinatorReplayWindow]);
    facts.complete_stream = true;
    let input = inputs(&base, &facts);
    assert_eq!(evaluate(&input), Vec::new());
}

#[test]
fn span_budget_consumes_older_generations_first_with_one_file_minimum() {
    let spans = [
        OwnerSpan {
            generation: 5,
            lo: 0,
            hi: 10 * FILE,
        },
        OwnerSpan {
            generation: 4,
            lo: 2 * FILE,
            hi: 6 * FILE,
        },
    ];
    // Total 14 files; 50% = 7 files: all 4 of generation 4, then 3 of 5.
    let ends = span_budget_end(&spans, FILE, 50);
    assert_eq!(ends[&4], 6 * FILE);
    assert_eq!(ends[&5], 3 * FILE);
    // Zero percent keeps native's one-file minimum.
    let ends = span_budget_end(&spans, FILE, 0);
    assert_eq!(ends[&4], 3 * FILE);
    assert_eq!(ends[&5], 0);
    // 100 covers every complete span; rounding is down to whole files.
    let partial = [OwnerSpan {
        generation: 1,
        lo: 0,
        hi: 3 * FILE + 17,
    }];
    assert_eq!(span_budget_end(&partial, FILE, 100)[&1], 3 * FILE);
}

fn ledger_file(partition: u32, start: u64, allocated: u64, eligible: bool) -> LedgerFile {
    LedgerFile {
        partition,
        generation: 3,
        start,
        device: 1,
        inode: 1000 + u64::from(partition) * 100 + start / FILE,
        allocated,
        eligible,
        retained_puts: 0,
    }
}

const DAY: u64 = 86_400_000_000_000;

#[test]
fn growth_counts_new_allocation_once_and_credits_first_eligible_sample() {
    let mut ledger = GrowthLedger::default();
    let owners = [0, 1];
    // Begin: owner 0 has file 0 (1 MiB), owner 1 has file 0 (1 MiB).
    ledger.observe(
        &owners,
        &[
            ledger_file(0, 0, FILE, false),
            ledger_file(1, 0, FILE, false),
        ],
        true,
        false,
    );
    // Owner 0 grows file 0 by 1 MiB and creates file 1; owner 1 creates file 1.
    let reclaimed = ledger.observe(
        &owners,
        &[
            ledger_file(0, 0, 2 * FILE, true),
            ledger_file(0, FILE, FILE, false),
            ledger_file(1, 0, FILE, false),
            ledger_file(1, FILE, 3 * FILE, false),
        ],
        true,
        false,
    );
    assert_eq!(reclaimed, vec![2 * FILE, 0, 0, 0]);
    // Still eligible later: never credited twice. An incomplete sample credits nothing.
    let reclaimed = ledger.observe(
        &owners,
        &[
            ledger_file(0, 0, 2 * FILE, true),
            ledger_file(0, FILE, FILE, true),
            ledger_file(1, 0, FILE, true),
            ledger_file(1, FILE, 3 * FILE, false),
        ],
        false,
        false,
    );
    assert_eq!(reclaimed, vec![0; 4]);
    ledger.observe(
        &owners,
        &[
            ledger_file(0, 0, 2 * FILE, true),
            ledger_file(0, FILE, FILE, false),
            ledger_file(1, 0, FILE, false),
            ledger_file(1, FILE, 3 * FILE, false),
        ],
        true,
        true,
    );
    assert!(ledger.errors().is_empty());
    let (owners, worst) = ledger.finish(DAY / 2, true);
    // Owner 0: new = 1 (growth) + 1 (file 1) = 2 MiB; reclaimed 2 MiB; net 0.
    assert_eq!(owners[0].new_allocated_bytes, 2 * FILE);
    assert_eq!(owners[0].newly_shadow_reclaimed_bytes, 2 * FILE);
    assert_eq!(owners[0].net_allocated_growth_bytes_per_day, Some(0));
    assert_eq!(owners[0].baseline_allocated_bytes, FILE);
    assert_eq!(owners[0].end_allocated_bytes, 3 * FILE);
    // Owner 1: new 3 MiB in half a day = 6 MiB/day.
    assert_eq!(
        owners[1].net_allocated_growth_bytes_per_day,
        Some(i64::try_from(6 * FILE).unwrap())
    );
    assert_eq!(worst.partition, Some(1));
}

#[test]
fn equal_slopes_pick_the_lowest_partition() {
    let mut ledger = GrowthLedger::default();
    ledger.observe(&[2, 1], &[], true, false);
    ledger.observe(
        &[2, 1],
        &[
            ledger_file(2, 0, FILE, false),
            ledger_file(1, 0, FILE, false),
        ],
        true,
        true,
    );
    let (_, worst) = ledger.finish(DAY, true);
    assert_eq!(worst.partition, Some(1));
    assert_eq!(worst.net_allocated_growth_bytes_per_day, Some(1 << 20));
}

#[test]
fn measurement_errors_null_every_slope() {
    // Inode change of a tracked file.
    let mut ledger = GrowthLedger::default();
    ledger.observe(&[0], &[ledger_file(0, 0, FILE, false)], true, false);
    let mut moved = ledger_file(0, 0, FILE, false);
    moved.inode += 1;
    ledger.observe(&[0], &[moved], true, true);
    assert_eq!(ledger.errors().len(), 1);
    let (owners, worst) = ledger.finish(DAY, true);
    assert_eq!(owners[0].net_allocated_growth_bytes_per_day, None);
    assert!(worst.partition.is_none() && worst.reason.is_some());

    // A missing file in a no-unlink run.
    let mut ledger = GrowthLedger::default();
    ledger.observe(
        &[0],
        &[
            ledger_file(0, 0, FILE, false),
            ledger_file(0, FILE, FILE, false),
        ],
        true,
        false,
    );
    ledger.observe(&[0], &[ledger_file(0, FILE, FILE, false)], true, true);
    assert_eq!(ledger.errors().len(), 1);

    // A later root contradicts an earlier shadow retirement.
    let mut ledger = GrowthLedger::default();
    ledger.observe(&[0], &[ledger_file(0, 0, FILE, false)], true, false);
    ledger.observe(&[0], &[ledger_file(0, 0, FILE, true)], true, false);
    let mut referenced = ledger_file(0, 0, FILE, false);
    referenced.retained_puts = 1;
    ledger.observe(&[0], &[referenced], true, true);
    assert_eq!(ledger.errors().len(), 1);

    // Incomplete baseline and incomplete runs.
    let mut ledger = GrowthLedger::default();
    ledger.observe(&[0], &[ledger_file(0, 0, FILE, false)], false, false);
    assert_eq!(ledger.errors().len(), 1);
    let mut ledger = GrowthLedger::default();
    ledger.observe(&[0], &[ledger_file(0, 0, FILE, false)], true, true);
    let (owners, _) = ledger.finish(DAY, false);
    assert_eq!(owners[0].net_allocated_growth_bytes_per_day, None);
    assert_eq!(owners[0].reason.as_deref(), Some("run incomplete"));
}

#[test]
fn roll_crossings_carry_their_own_image_and_count_drops() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let (wake, inbox) = std::sync::mpsc::sync_channel(2);
    let install_seq = std::sync::Arc::new(AtomicU64::new(4));
    let cursor = RollCursor::new(3, wake, install_seq.clone());
    assert_eq!(cursor.latest(), None);
    // File 1 rolls under image 4; image 6 is installed before file 2 rolls.
    // The interleaving that broke the atomic cursor (read file 1, then the
    // next crossing's image) cannot pair file 1 with image 6: each event is
    // one coherent value.
    cursor.crossed(1, 10);
    install_seq.store(6, Ordering::Release);
    cursor.crossed(2, 25);
    // A full queue drops the third crossing and counts it.
    cursor.crossed(3, 40);
    assert_eq!(cursor.dropped(), 1);
    let received: Vec<_> = inbox
        .try_iter()
        .map(|event| match event {
            ObserverEvent::Roll(crossing) => crossing,
            _ => panic!("unexpected event"),
        })
        .collect();
    assert_eq!(
        received,
        [
            Crossing {
                owner: 3,
                file: 1,
                sequence_bound: 10,
                image: 4,
            },
            Crossing {
                owner: 3,
                file: 2,
                sequence_bound: 25,
                image: 6,
            },
        ]
    );
    // The dropped crossing still advances the bound used for sequences.
    assert_eq!(cursor.latest(), Some((3, 40)));
    // A stale report never moves the latest values backwards.
    cursor.crossed(1, 5);
    assert_eq!(cursor.latest(), Some((3, 40)));
}

#[test]
fn report_types_serialize_with_stable_names() {
    let blockers = serde_json::to_value([Blocker::VersionBCut, Blocker::SpanBudget]).unwrap();
    assert_eq!(
        blockers,
        serde_json::json!(["version_b_cut", "span_budget"])
    );
    let state = serde_json::to_value(FileUseState::RetiredCleanupPending).unwrap();
    assert_eq!(state, serde_json::json!("retired_cleanup_pending"));
    assert_eq!(hex(&[0, 0xab, 0x10]), "00ab10");
}
