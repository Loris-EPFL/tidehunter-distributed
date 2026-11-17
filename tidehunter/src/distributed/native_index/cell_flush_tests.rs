//! Per-cell capture, reads during flights, token-checked installation, lease
//! bounds and the standing view budget. References are synthetic: the index
//! never reads values, and the oracle compares logical versions.

use super::*;
use crate::distributed::native_publication::NativePendingSink;
use crate::index::pending_table::Transaction;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64};

const DATABASE: [u8; 16] = [7; 16];
const EPOCH: u64 = 11;
const KS: KeySpace = KeySpace(0);
const BUDGET: usize = 16 << 20;

fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    // Variable keys routed by their first byte: one cell per first byte.
    builder.add_key_space_config_indexing(
        "cells",
        KeyIndexing::variable_length(),
        1,
        KeyType::prefix_uniform(1, 0),
        KeySpaceConfig::new(),
    );
    builder.build()
}

fn index(shards: usize, budget: usize) -> NativeIndex {
    NativeIndex::with_shards(&shape(), DATABASE, budget, shards).unwrap()
}

fn batch(sequence: u64) -> BatchVersion {
    BatchVersion {
        epoch: EPOCH,
        sequence,
    }
}

fn through(sequence: u64) -> PrefixCut {
    PrefixCut::Through(batch(sequence))
}

fn reference(sequence: u64, operation_index: u32, put: bool) -> ValueRecordReference {
    ValueRecordReference {
        batch_id: BatchId {
            database: DATABASE,
            operation: u128::from(sequence).to_be_bytes(),
        },
        batch_version: batch(sequence),
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
            keyspace: KS.0,
            kind: if put {
                RecordKind::Put
            } else {
                RecordKind::Delete
            },
            key_len: 2,
            value_offset: 8,
            value_len: 8,
            checksum: MemberChecksum(0),
        },
    }
}

fn cell_of(index: &NativeIndex, key: &[u8]) -> CellId {
    index
        .descriptor(KS)
        .unwrap()
        .cell_id(&Bytes::copy_from_slice(key))
}

type Ops = Vec<(Vec<u8>, bool)>;

/// Stage and promote one batch under one writer hold, as the option-D
/// publisher does, without moving the watermark.
fn apply(writer: &mut IndexWriter<'_>, sequence: u64, ops: &[(Vec<u8>, bool)]) {
    let transaction = Transaction::default();
    for (operation, (key, put)) in ops.iter().enumerate() {
        let cell = cell_of(writer, key);
        let reduced = Bytes::copy_from_slice(key);
        let value = reference(sequence, operation as u32, *put);
        writer
            .with_pending_table(KS, &cell, |table| {
                if *put {
                    table.insert(reduced, value, None, &transaction);
                } else {
                    table.remove(reduced, value, &transaction);
                }
            })
            .unwrap();
    }
    transaction.commit();
    writer.promote_committed().unwrap();
}

fn publish(index: &NativeIndex, sequence: u64, ops: &[(Vec<u8>, bool)]) {
    let mut writer = index.writer();
    apply(&mut writer, sequence, ops);
    writer.publish_latest();
}

fn read(index: &NativeIndex, key: &[u8], cut: IndexReadCut) -> Option<u64> {
    index
        .get(KS, key, cut)
        .unwrap()
        .map(|reference| reference.batch_version.sequence)
}

fn at(sequence: u64) -> IndexReadCut {
    through(sequence).index_cut()
}

/// Test encoder of a capture with history floor `BeforeAll`: every reference
/// through the target, rows sorted, each version once. The production
/// encoder and its byte equivalence belong to the snapshot stage.
fn encode(index: &NativeIndex, source: &CellEncodeSource<'_>) -> NewCellBase {
    assert_eq!(source.floor, IndexReadCut::BeforeAll);
    let mut rows: BTreeMap<Vec<u8>, Vec<ValueRecordReference>> = BTreeMap::new();
    if let Some(base) = source.base {
        for n in 0..base.count {
            let (key, versions) = base.entry(n);
            for bytes in versions.chunks_exact(REFERENCE_BYTES) {
                let reference = decode_reference(bytes).unwrap();
                assert!(source.cut.contains(&reference), "base above its target");
                rows.entry(key.to_vec()).or_default().push(reference);
            }
        }
    }
    for overlay in [source.carry, Some(&**source.active)].into_iter().flatten() {
        for (key, values) in overlay {
            for value in values.iter().filter(|value| source.cut.contains(value)) {
                rows.entry(key.to_vec()).or_default().push(*value);
            }
        }
    }
    rows.retain(|_, versions| !versions.is_empty());
    if rows.is_empty() {
        return NewCellBase::empty();
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    let directory = payload.len();
    payload.resize(directory + 4 * rows.len(), 0);
    for (row, (key, versions)) in rows.iter_mut().enumerate() {
        versions.sort_by_key(|version| version.mutation_version());
        assert!(
            versions
                .windows(2)
                .all(|pair| pair[0].mutation_version() < pair[1].mutation_version()),
            "a captured version appears twice"
        );
        let offset = payload.len() as u32;
        payload[directory + 4 * row..directory + 4 * row + 4]
            .copy_from_slice(&offset.to_be_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&(versions.len() as u32).to_be_bytes());
        for version in versions.iter() {
            encode_reference(version, &mut payload);
        }
    }
    let (keyspace, cell) = source.identity;
    NewCellBase::frame(
        SortedBlock::validated(
            payload.into(),
            false,
            DATABASE,
            *keyspace,
            index.descriptor(*keyspace).unwrap(),
            cell,
            source.cut.clone(),
        )
        .unwrap(),
    )
}

#[derive(Debug, PartialEq)]
enum Flushed {
    Installed(PrefixCut),
    Clean(PrefixCut),
    Skipped(CellSkip),
}

/// One complete flush: permit, capture, residual, encoding, installation.
fn flush(index: &NativeIndex, cell: &CellId, target: PrefixCut) -> Flushed {
    let permit = index.reserve_cell_flush(0).unwrap();
    let outcome = index
        .writer()
        .capture_cell(KS, cell, target, permit)
        .unwrap();
    let capture = match outcome {
        CellCaptureOutcome::Captured(capture) => capture,
        CellCaptureOutcome::Clean { coverage, permit } => {
            // A clean advance consumes nothing; the permit is reusable.
            assert_eq!(permit.bytes(), 0);
            return Flushed::Clean(coverage);
        }
        CellCaptureOutcome::Skipped { reason, .. } => return Flushed::Skipped(reason),
    };
    let prepared = capture.prepare().unwrap();
    let base = encode(index, &prepared.encode_source());
    let installed = index.writer().install_cell(prepared.finish(base));
    match installed {
        CellInstall::Installed { coverage, retired } => {
            drop(retired);
            Flushed::Installed(coverage)
        }
        CellInstall::Stale { .. } => panic!("unexpected stale result"),
        CellInstall::Rejected { error, .. } => panic!("rejected: {error}"),
    }
}

fn captured(outcome: CellCaptureOutcome) -> CellCapture {
    match outcome {
        CellCaptureOutcome::Captured(capture) => capture,
        CellCaptureOutcome::Clean { coverage, .. } => panic!("clean at {coverage:?}"),
        CellCaptureOutcome::Skipped { reason, .. } => panic!("skipped: {reason:?}"),
    }
}

fn capture(index: &NativeIndex, cell: &CellId, target: PrefixCut) -> CellCapture {
    let permit = index.reserve_cell_flush(0).unwrap();
    captured(
        index
            .writer()
            .capture_cell(KS, cell, target, permit)
            .unwrap(),
    )
}

fn installed(install: CellInstall) -> PrefixCut {
    match install {
        CellInstall::Installed { coverage, retired } => {
            drop(retired);
            coverage
        }
        CellInstall::Stale { .. } => panic!("unexpected stale result"),
        CellInstall::Rejected { error, .. } => panic!("rejected: {error}"),
    }
}

/// Accounting after incremental installs equals a full recount.
fn assert_accounting_recounts(index: &mut NativeIndex) {
    let before = index.stats();
    let retained = index.retained_metadata_bytes();
    index.recount_retained();
    let after = index.stats();
    assert_eq!(before.retained_versions, after.retained_versions);
    assert_eq!(before.charged_metadata_bytes, after.charged_metadata_bytes);
    assert_eq!(retained, index.retained_metadata_bytes());
}

/// Deterministic workload: 1..=3 distinct keys per batch over `CELLS` cells
/// of `KEYS` keys, one delete in five operations.
const CELLS: u64 = 6;
const KEYS: u64 = 4;

fn ops(sequence: u64) -> Ops {
    let mut ops: Ops = Vec::new();
    for n in 0..=(sequence % 3) {
        let x = sequence
            .wrapping_mul(2_654_435_761)
            .wrapping_add(n * 40_503);
        let key = vec![(x % CELLS) as u8, ((x / CELLS) % KEYS) as u8];
        if ops.iter().all(|(existing, _)| *existing != key) {
            ops.push((key, !(x / 97).is_multiple_of(5)));
        }
    }
    ops
}

fn all_keys() -> Vec<Vec<u8>> {
    (0..CELLS)
        .flat_map(|cell| (0..KEYS).map(move |key| vec![cell as u8, key as u8]))
        .collect()
}

/// Expected visible sequence of every key at every complete batch cut.
struct Oracle(BTreeMap<Vec<u8>, Vec<(u64, bool)>>);

impl Oracle {
    fn new(last: u64) -> Self {
        let mut history: BTreeMap<Vec<u8>, Vec<(u64, bool)>> = BTreeMap::new();
        for sequence in 1..=last {
            for (key, put) in ops(sequence) {
                history.entry(key).or_default().push((sequence, put));
            }
        }
        Self(history)
    }

    fn get(&self, key: &[u8], cut: u64) -> Option<u64> {
        self.0
            .get(key)?
            .iter()
            .rev()
            .find(|(sequence, _)| *sequence <= cut)
            .and_then(|(sequence, put)| put.then_some(*sequence))
    }

    fn versions(&self) -> usize {
        self.0.values().map(Vec::len).sum()
    }
}

fn check_cut(index: &NativeIndex, oracle: &Oracle, cut: u64) {
    let read_cut = if cut == 0 {
        IndexReadCut::BeforeAll
    } else {
        at(cut)
    };
    let mut live = Vec::new();
    for key in all_keys() {
        let expected = oracle.get(&key, cut);
        assert_eq!(
            read(index, &key, read_cut.clone()),
            expected,
            "{key:?}@{cut}"
        );
        if expected.is_some() {
            live.push(key);
        }
    }
    // A scan returns each live row once, in order: no lost or duplicated row.
    let scanned: Vec<_> = index
        .scan(KS, None, false, 1000, read_cut)
        .unwrap()
        .into_iter()
        .map(|(key, _)| key.to_vec())
        .collect();
    assert_eq!(scanned, live, "scan@{cut}");
}

#[test]
fn concurrent_flushes_publication_and_reads_keep_every_row_exactly_once() {
    const LAST: u64 = 400;
    let index = Arc::new(index(8, BUDGET));
    let oracle = Arc::new(Oracle::new(LAST));
    let cells: Vec<_> = (0..CELLS as u8)
        .map(|cell| cell_of(&index, &[cell, 0]))
        .collect();
    let done = Arc::new(AtomicBool::new(false));
    let installs = Arc::new(AtomicU64::new(0));
    // Halfway, publication waits for one install and one check per reader, so
    // flushes and reads overlap publication on both sides of that point.
    // Bounded; a full or closed channel means the signal is already given.
    let (progress, progressed) = std::sync::mpsc::sync_channel::<()>(4);
    // Two flushers own disjoint cell sets, like disjoint shard ranges.
    let flushers: Vec<_> = cells
        .chunks(CELLS as usize / 2)
        .map(|owned| {
            let (index, done, installs) =
                (Arc::clone(&index), Arc::clone(&done), Arc::clone(&installs));
            let progress = progress.clone();
            let owned = owned.to_vec();
            std::thread::spawn(move || {
                loop {
                    // Read the flag first: the last pass starts after the
                    // final publication.
                    let last_pass = done.load(AtomicOrdering::Acquire);
                    for cell in &owned {
                        // A scheduler only dispatches existing cells; cells
                        // are never removed once inserted.
                        if index.cell_dirty(KS, cell).is_none() {
                            continue;
                        }
                        let target = index.published_prefix();
                        if let Flushed::Installed(coverage) = flush(&index, cell, target) {
                            assert!(coverage >= target);
                            if installs.fetch_add(1, AtomicOrdering::Relaxed) == 0 {
                                let _ = progress.try_send(());
                            }
                        }
                    }
                    if last_pass {
                        break;
                    }
                }
            })
        })
        .collect();
    let readers: Vec<_> = (0..3)
        .map(|reader| {
            let (index, done, oracle) =
                (Arc::clone(&index), Arc::clone(&done), Arc::clone(&oracle));
            let progress = progress.clone();
            std::thread::spawn(move || {
                let mut checks = 0u64;
                while !done.load(AtomicOrdering::Acquire) {
                    let cut = index.latest_cut();
                    let latest = match &cut {
                        IndexReadCut::At(version) => version.batch.sequence,
                        _ => 0,
                    };
                    // Pin the cut and an older one for the whole check; a flush
                    // moving references into a base must not change either.
                    let _pin = index.pin_cut(cut.clone()).unwrap();
                    check_cut(&index, &oracle, latest);
                    let older = latest / (2 + reader);
                    let _old_pin = index.pin_cut(at(older.max(1))).ok();
                    if older >= 1 {
                        check_cut(&index, &oracle, older);
                    }
                    if checks == 0 {
                        let _ = progress.try_send(());
                    }
                    checks += 1;
                }
                checks
            })
        })
        .collect();
    drop(progress);
    for sequence in 1..=LAST {
        publish(&index, sequence, &ops(sequence));
        if sequence == LAST / 2 {
            for _ in 0..4 {
                // A deadline turns a failed worker into a test failure, not
                // a hang: the other workers keep their senders alive.
                progressed
                    .recv_timeout(std::time::Duration::from_secs(60))
                    .expect("a flusher or reader made no progress");
            }
        }
    }
    done.store(true, AtomicOrdering::Release);
    for flusher in flushers {
        flusher.join().unwrap();
    }
    let checks: u64 = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .sum();
    assert!(checks > 0);
    assert!(installs.load(AtomicOrdering::Relaxed) > 0);
    let mut index = Arc::try_unwrap(index).unwrap_or_else(|_| panic!("index still shared"));
    for cut in 0..=LAST {
        check_cut(&index, &oracle, cut);
    }
    // The final pass flushed everything through the last batch: every cell
    // covers it and holds no unrepresented reference.
    for cell in &cells {
        let dirty = index.cell_dirty(KS, cell).unwrap();
        assert_eq!(dirty.coverage, through(LAST));
        assert_eq!((dirty.versions, dirty.oldest_uncovered), (0, None));
        assert!(!dirty.in_flight);
    }
    assert_eq!(index.stats().retained_versions, oracle.versions());
    assert_eq!(index.writer().cell_flights(), 0);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    assert_accounting_recounts(&mut index);
}

#[test]
fn publication_during_flight_and_sparse_gap_stay_visible_and_survive_install() {
    let mut index = index(8, BUDGET);
    let (a1, a2) = (vec![1, 1], vec![1, 2]);
    let cell = cell_of(&index, &a1);
    publish(&index, 1, &[(a1.clone(), true), (a2.clone(), true)]);
    publish(&index, 2, &[(a1.clone(), true)]);
    // Batch 4 publishes above the unpublished hole 3.
    {
        let mut writer = index.writer();
        apply(&mut writer, 4, &[(a2.clone(), true)]);
        let upper = MutationVersion {
            batch: batch(4),
            operation_index: 0,
        };
        writer
            .install_live_view(upper, at(2), vec![batch(3)])
            .unwrap();
    }
    assert_eq!(index.published_prefix(), through(2));
    let check = |index: &NativeIndex, a1_latest: u64| {
        assert_eq!(read(index, &a1, IndexReadCut::Latest), Some(a1_latest));
        assert_eq!(read(index, &a2, IndexReadCut::Latest), Some(4));
        assert_eq!(read(index, &a1, at(1)), Some(1));
        assert_eq!(read(index, &a2, at(2)), Some(1));
    };
    // A target above the published prefix is refused before any effect.
    let permit = index.reserve_cell_flush(0).unwrap();
    assert!(matches!(
        index.writer().capture_cell(KS, &cell, through(3), permit),
        Err(NativeIndexError::UnpublishedCut)
    ));
    let flight = capture(&index, &cell, through(2));
    assert_eq!(flight.target(), through(2));
    assert_eq!(flight.identity(), &(KS, cell.clone()));
    assert_eq!(flight.totals().versions, 4);
    assert!(index.cell_dirty(KS, &cell).unwrap().in_flight);
    check(&index, 2);
    // The hole closes during the flight: batch 3 enters the new active
    // overlay and is visible next to the captured sources.
    {
        let mut writer = index.writer();
        apply(&mut writer, 3, &[(a1.clone(), true)]);
        writer.publish_latest();
    }
    check(&index, 3);
    let prepared = flight.prepare().unwrap();
    // The residual holds the post-target sparse reference of batch 4 only.
    assert_eq!(prepared.residual_totals().versions, 1);
    assert_eq!(prepared.residual_totals().min, Some(batch(4)));
    assert_eq!(prepared.target(), through(2));
    assert_eq!(prepared.identity(), &(KS, cell.clone()));
    check(&index, 3);
    let base = encode(&index, &prepared.encode_source());
    let result = prepared.finish(base);
    assert_eq!(result.identity(), &(KS, cell.clone()));
    assert_eq!(installed(index.writer().install_cell(result)), through(2));
    check(&index, 3);
    let dirty = index.cell_dirty(KS, &cell).unwrap();
    // Carry (batch 4) plus active (batch 3) remain unrepresented.
    assert_eq!(dirty.versions, 2);
    assert_eq!(dirty.oldest_uncovered, Some(batch(3)));
    assert_accounting_recounts(&mut index);
    // The next flush captures carry and active together into the base.
    assert_eq!(
        flush(&index, &cell, index.published_prefix()),
        Flushed::Installed(through(4))
    );
    check(&index, 3);
    let dirty = index.cell_dirty(KS, &cell).unwrap();
    assert_eq!((dirty.versions, dirty.oldest_uncovered), (0, None));
    assert_eq!(index.stats().retained_versions, 5);
    assert_accounting_recounts(&mut index);
    // Exact replay duplicates of flushed references are still recognized.
    publish(&index, 2, &[(a1.clone(), true)]);
    assert_eq!(index.stats().retained_versions, 5);
}

#[test]
fn clean_cells_advance_coverage_and_dirty_metadata_tracks_sources() {
    let index = index(8, BUDGET);
    let (a, b) = (vec![1, 0], vec![2, 0]);
    let (cell_a, cell_b) = (cell_of(&index, &a), cell_of(&index, &b));
    assert!(!index.take_cell_dirty_signal());
    publish(
        &index,
        1,
        &[(a.clone(), true), (b.clone(), true), (vec![2, 1], false)],
    );
    assert!(index.take_cell_dirty_signal());
    assert!(!index.take_cell_dirty_signal());
    let dirty = index.cell_dirty(KS, &cell_b).unwrap();
    assert_eq!(dirty.versions, 2);
    // Two rows of key length 2 (12 + 2 bytes each) and two references.
    assert_eq!(dirty.upper, 2 * (12 + 2) + 2 * REFERENCE_BYTES);
    assert_eq!(dirty.oldest_uncovered, Some(batch(1)));
    assert!(dirty.dirty_since.is_some() && !dirty.queued && !dirty.in_flight);
    assert_eq!(dirty.coverage, PrefixCut::BeforeAll);
    assert_eq!(
        flush(&index, &cell_b, through(1)),
        Flushed::Installed(through(1))
    );
    // A tombstone-only row is still a retained row.
    assert_eq!(read(&index, &[2, 1], IndexReadCut::Latest), None);
    assert!(index.stats().cells >= 2);
    publish(&index, 2, &[(a.clone(), true)]);
    // No reference of cell B is missing through 2: coverage advances, no I/O.
    assert_eq!(
        flush(&index, &cell_b, through(2)),
        Flushed::Clean(through(2))
    );
    let dirty = index.cell_dirty(KS, &cell_b).unwrap();
    assert_eq!(
        (dirty.coverage, dirty.versions, dirty.dirty_since),
        (through(2), 0, None)
    );
    // Scheduler marks: one queued descriptor per cell, cleared by capture.
    assert!(index.try_queue_cell(KS, &cell_a));
    assert!(!index.try_queue_cell(KS, &cell_a));
    index.unqueue_cell(KS, &cell_a);
    assert!(index.try_queue_cell(KS, &cell_a));
    let flight = capture(&index, &cell_a, through(2));
    assert!(
        !index.try_queue_cell(KS, &cell_a),
        "a flight blocks a queued descriptor"
    );
    let mut shard_cells = Vec::with_capacity(8);
    index.shard_dirty_cells(index.shard_of_cell(KS, &cell_a), &mut shard_cells);
    let (_, observed) = shard_cells
        .iter()
        .find(|(identity, _)| *identity == (KS, cell_a.clone()))
        .unwrap();
    assert!(observed.in_flight && !observed.queued);
    assert_eq!(observed.oldest_uncovered, Some(batch(1)));
    index.shard_dirty_cells(usize::MAX, &mut shard_cells);
    assert!(shard_cells.is_empty());
    let restored = flight.restore().unwrap();
    assert_eq!(
        installed(index.writer().install_cell(restored)),
        PrefixCut::BeforeAll
    );
    // Restore keeps every reference unrepresented and the base unchanged.
    let dirty = index.cell_dirty(KS, &cell_a).unwrap();
    assert_eq!((dirty.versions, dirty.in_flight), (2, false));
    assert_eq!(read(&index, &a, IndexReadCut::Latest), Some(2));
    // New cells start at the adopted floor, which never regresses.
    index.writer().set_cell_floor(through(2)).unwrap();
    assert!(index.writer().set_cell_floor(through(1)).is_err());
    assert!(matches!(
        index.writer().set_cell_floor(through(3)),
        Err(NativeIndexError::UnpublishedCut)
    ));
    publish(&index, 3, &[(vec![3, 0], true)]);
    let new_cell = cell_of(&index, &[3, 0]);
    assert_eq!(
        index.cell_dirty(KS, &new_cell).unwrap().coverage,
        through(2)
    );
}

#[test]
fn flush_leases_are_bounded_and_independent_of_the_view_budget() {
    const M: usize = 1 << 20;
    let index = index(8, M);
    let limit = index.cell_flight_limit();
    assert_eq!(limit, CELL_FLIGHTS_PER_FLUSHER * cell_flushers(8));
    let mut permits: Vec<_> = (0..limit)
        .map(|_| index.reserve_cell_flush(0).unwrap())
        .collect();
    assert!(matches!(
        index.reserve_cell_flush(0),
        Err(NativeIndexError::Capacity)
    ));
    permits.pop();
    permits.push(index.reserve_cell_flush(0).unwrap());
    drop(permits);
    // The byte allowance is the index budget.
    let whole = index.reserve_cell_flush(M).unwrap();
    assert_eq!(whole.bytes(), M);
    assert_eq!(index.stats().transient_metadata_bytes, M);
    assert!(matches!(
        index.reserve_cell_flush(1),
        Err(NativeIndexError::Capacity)
    ));
    // Views use their own allowance: a full flush allowance takes none of it.
    let standing = index.reserve_standing_view_budget(M / 64).unwrap();
    let view = index.reserve_publication_view(M / 64).unwrap();
    drop(whole);
    assert!(matches!(
        index.reserve_cell_flush(M + 1),
        Err(NativeIndexError::Capacity)
    ));
    let whole = index.reserve_cell_flush(M).unwrap();
    drop((whole, view));

    // A capture whose residual bound exceeds its permit grows it without
    // waiting, or returns the permit with the needed size.
    let key = vec![1, 1];
    let cell = cell_of(&index, &key);
    publish(&index, 1, &[(key.clone(), true), (vec![1, 2], true)]);
    assert!(index.try_queue_cell(KS, &cell));
    let blocker = index.reserve_cell_flush(M).unwrap();
    let permit = index.reserve_cell_flush(0).unwrap();
    let (reason, permit) = match index
        .writer()
        .capture_cell(KS, &cell, through(1), permit)
        .unwrap()
    {
        CellCaptureOutcome::Skipped { reason, permit } => (reason, permit),
        _ => panic!("capture must wait for a permit"),
    };
    let needed = 2 * (VERSION_CHARGE + key.len());
    assert_eq!(reason, CellSkip::NeedsPermit { bytes: needed });
    // The skipped descriptor stays with the worker: the mark remains.
    let dirty = index.cell_dirty(KS, &cell).unwrap();
    assert!(!dirty.in_flight && dirty.queued);
    drop(blocker);
    let flight = captured(
        index
            .writer()
            .capture_cell(KS, &cell, through(1), permit)
            .unwrap(),
    );
    assert_eq!(flight.permit.bytes(), needed);
    assert!(!index.cell_dirty(KS, &cell).unwrap().queued);
    // The one flight of a cell excludes a second capture.
    let second = index.reserve_cell_flush(0).unwrap();
    assert!(matches!(
        index
            .writer()
            .capture_cell(KS, &cell, through(1), second)
            .unwrap(),
        CellCaptureOutcome::Skipped {
            reason: CellSkip::InFlight,
            ..
        }
    ));
    // Publication through the standing budget during the flight: a sparse
    // view installs while the flush allowance is in use.
    {
        let mut writer = index.writer();
        apply(&mut writer, 3, &[(key.clone(), true)]);
        let upper = MutationVersion {
            batch: batch(3),
            operation_index: 0,
        };
        writer
            .install_standing_live_view(upper, at(1), vec![batch(2)], &standing)
            .unwrap();
    }
    assert_eq!(read(&index, &key, IndexReadCut::Latest), Some(3));
    let prepared = flight.prepare().unwrap();
    let base = encode(&index, &prepared.encode_source());
    assert_eq!(
        installed(index.writer().install_cell(prepared.finish(base))),
        through(1)
    );
    assert_eq!(read(&index, &key, IndexReadCut::Latest), Some(3));
    assert_eq!(read(&index, &key, at(1)), Some(1));
    drop(standing);
    assert_eq!(index.writer().cell_flights(), 0);
}

#[test]
fn stale_foreign_and_forged_results_are_rejected_and_reuse_installs() {
    let index = index(8, BUDGET);
    let (key, other) = (vec![1, 1], vec![1, 2]);
    let cell = cell_of(&index, &key);
    publish(&index, 1, &[(key.clone(), true), (other.clone(), true)]);
    publish(&index, 2, &[(key.clone(), false)]);

    // A lifecycle change makes the flight stale: the new base is not
    // installed, the captured sources are restored as the carry.
    let flight = capture(&index, &cell, through(1));
    index.writer().advance_cell_lifecycle();
    let prepared = flight.prepare().unwrap();
    let base = encode(&index, &prepared.encode_source());
    let prepared = match index.writer().install_cell(prepared.finish(base)) {
        CellInstall::Stale {
            prepared,
            discarded,
        } => {
            drop(discarded);
            prepared
        }
        _ => panic!("a lifecycle change must make the result stale"),
    };
    assert_eq!(read(&index, &key, IndexReadCut::Latest), None);
    assert_eq!(read(&index, &key, at(1)), Some(1));
    let restored = prepared.restore().unwrap();
    assert_eq!(
        installed(index.writer().install_cell(restored)),
        PrefixCut::BeforeAll
    );
    assert_eq!(index.writer().cell_flights(), 0);
    assert_eq!(read(&index, &other, at(1)), Some(1));

    // A result installed into another index is foreign, even when that
    // index holds the same cells and versions.
    let foreign = self::index(8, BUDGET);
    publish(&foreign, 1, &[(key.clone(), true), (other.clone(), true)]);
    let flight = capture(&index, &cell, through(1));
    let prepared = flight.prepare().unwrap();
    let base = encode(&index, &prepared.encode_source());
    let result = match foreign.writer().install_cell(prepared.finish(base)) {
        CellInstall::Rejected { error, result } => {
            assert!(matches!(error, NativeIndexError::Invalid(_)));
            result
        }
        _ => panic!("a foreign result must be rejected"),
    };
    assert_eq!(read(&foreign, &key, IndexReadCut::Latest), Some(1));

    // Forged tokens are rejected and change nothing; the genuine result then
    // installs. The flight id of an earlier flight of the same cell is stale.
    let mut result = result;
    let genuine = result.capture.flight;
    result.capture.flight = genuine - 1;
    let mut result = match index.writer().install_cell(result) {
        CellInstall::Rejected { result, .. } => result,
        _ => panic!("an earlier flight id must be rejected"),
    };
    result.capture.flight = genuine;
    result.capture.base_token += 1;
    let mut result = match index.writer().install_cell(result) {
        CellInstall::Rejected { result, .. } => result,
        _ => panic!("a forged base token must be rejected"),
    };
    result.capture.base_token -= 1;
    assert!(index.cell_dirty(KS, &cell).unwrap().in_flight);
    assert_eq!(read(&index, &key, IndexReadCut::Latest), None);
    assert_eq!(installed(index.writer().install_cell(result)), through(1));

    // A frame selected at another cut than its target is rejected.
    let flight = capture(&index, &cell, through(2));
    let prepared = flight.prepare().unwrap();
    let mut source = prepared.encode_source();
    source.cut = at(1);
    let wrong = encode(&index, &source);
    drop(source);
    let result = match index.writer().install_cell(prepared.finish(wrong)) {
        CellInstall::Rejected { error, result } => {
            assert!(matches!(error, NativeIndexError::Invalid(_)));
            result
        }
        _ => panic!("a frame at another cut must be rejected"),
    };
    let restored = result.capture.restore().unwrap();
    assert_eq!(installed(index.writer().install_cell(restored)), through(1));

    // Install after reuse: the cell's next flight installs normally.
    assert_eq!(
        flush(&index, &cell, through(2)),
        Flushed::Installed(through(2))
    );
    assert_eq!(read(&index, &key, IndexReadCut::Latest), None);
    assert_eq!(read(&index, &key, at(1)), Some(1));
    assert_eq!(read(&index, &other, IndexReadCut::Latest), Some(1));
    assert_eq!(index.stats().retained_versions, 3);
    assert_eq!(index.stats().transient_metadata_bytes, 0);
}

#[test]
fn exact_and_per_cell_checkpoints_are_exclusive() {
    let temp = tempdir::TempDir::new("cell-flush-exclusive").unwrap();
    let key = vec![1, 1];
    let exact = index(8, BUDGET);
    let cell = cell_of(&exact, &key);
    publish(&exact, 1, &[(key.clone(), true)]);
    exact
        .writer()
        .flush_and_unload(&temp.path().join("exact.index"), IndexReadCut::Latest)
        .unwrap();
    let permit = exact.reserve_cell_flush(0).unwrap();
    assert!(matches!(
        exact
            .writer()
            .capture_cell(KS, &cell, through(1), permit)
            .unwrap(),
        CellCaptureOutcome::Skipped {
            reason: CellSkip::ExactMode,
            ..
        }
    ));
    let per_cell = index(8, BUDGET);
    publish(&per_cell, 1, &[(key.clone(), true)]);
    assert_eq!(
        flush(&per_cell, &cell, through(1)),
        Flushed::Installed(through(1))
    );
    assert!(matches!(
        per_cell.writer().begin_capture(IndexReadCut::Latest),
        Err(NativeIndexError::Invalid(_))
    ));
}

#[test]
fn root_descriptors_export_capped_coverage_and_pin_bases() {
    let index = index(8, BUDGET);
    let (a, b) = (vec![1, 0], vec![2, 0]);
    let (cell_a, cell_b) = (cell_of(&index, &a), cell_of(&index, &b));
    publish(&index, 1, &[(a.clone(), true), (b.clone(), true)]);
    publish(&index, 2, &[(a.clone(), true)]);
    assert_eq!(
        flush(&index, &cell_a, through(2)),
        Flushed::Installed(through(2))
    );
    publish(&index, 3, &[(b.clone(), true)]);
    let len = index.writer().cell_directory_len();
    assert_eq!(len, 2);
    let mut output = Vec::new();
    assert!(
        index
            .writer()
            .capture_cell_descriptors(0..len, through(3), &mut output)
            .is_err(),
        "the descriptor buffer must be reserved outside the locks"
    );
    output.reserve(ROOT_DESCRIPTOR_SLICE);
    assert!(
        index
            .writer()
            .capture_cell_descriptors(0..ROOT_DESCRIPTOR_SLICE + 1, through(3), &mut output)
            .is_err()
    );
    assert!(matches!(
        index
            .writer()
            .capture_cell_descriptors(0..len, through(4), &mut output),
        Err(NativeIndexError::UnpublishedCut)
    ));
    index
        .writer()
        .capture_cell_descriptors(0..len, through(3), &mut output)
        .unwrap();
    // A: nothing missing through 3, so coverage advances and exports T; its
    // frame cut stays at the flushed target.
    assert_eq!(find(&output, &cell_a).coverage(), through(3));
    assert_eq!(find(&output, &cell_a).frame_cut(), Some(through(2)));
    let frame = find(&output, &cell_a).base.as_ref().unwrap();
    assert_eq!(frame.frame_floor, None);
    assert_eq!(frame.block.count, 1);
    // B: batches 1 and 3 are unrepresented; no base.
    assert_eq!(find(&output, &cell_b).coverage(), PrefixCut::BeforeAll);
    assert_eq!(find(&output, &cell_b).frame_cut(), None);
    assert_eq!(index.cell_dirty(KS, &cell_a).unwrap().coverage, through(3));
    // Exported coverage is capped at the root target.
    output.clear();
    index
        .writer()
        .capture_cell_descriptors(0..len, through(1), &mut output)
        .unwrap();
    assert_eq!(find(&output, &cell_a).coverage(), through(1));
    assert_eq!(find(&output, &cell_a).frame_cut(), Some(through(2)));
}

fn find<'a>(output: &'a [RootCellDescriptor], cell: &CellId) -> &'a RootCellDescriptor {
    output
        .iter()
        .find(|descriptor| descriptor.identity() == &(KS, cell.clone()))
        .unwrap()
}

/// Pins the capture lifetime contract: a capture dropped without install or
/// restore (fail-stop path only) releases its permit and keeps reads exact,
/// but the cell keeps its flight and cannot be captured again.
#[test]
fn dropped_capture_keeps_reads_and_its_flight() {
    let index = index(8, BUDGET);
    let key = vec![1, 1];
    let cell = cell_of(&index, &key);
    publish(&index, 1, &[(key.clone(), true)]);
    drop(capture(&index, &cell, through(1)).prepare().unwrap());
    assert_eq!(index.stats().transient_metadata_bytes, 0);
    assert_eq!(read(&index, &key, IndexReadCut::Latest), Some(1));
    assert!(index.cell_dirty(KS, &cell).unwrap().in_flight);
    assert_eq!(index.writer().cell_flights(), 1);
    assert_eq!(
        flush(&index, &cell, through(1)),
        Flushed::Skipped(CellSkip::InFlight)
    );
}
