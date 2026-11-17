//! Mixed-cut replay (spec §8.1-§8.3) checked against the C0 replay oracle
//! `CellRoot::replay_plan` and a sequential per-key oracle. WAL batches are
//! canonical batches with exact member references; cell bases are validated
//! frames installed by per-cell flushes, then given the loader's documented
//! state (watermark and promoted frontier at `F`).

use super::*;
use crate::distributed::native_index::{
    CellRootHeader, IndexReadCut, IndexStoreFileId, MemberReplay, RootCell, RootCellBase, RootFile,
};
use crate::distributed::native_publication::NativePendingSink;
use crate::distributed::types::PhysicalValueAddress;
use crate::index::pending_table::Transaction;
use crate::key_shape::{KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType};
use std::collections::BTreeMap;

const DATABASE: [u8; 16] = [7; 16];
const SCHEMA: [u8; 32] = [3; 32];
const KS: KeySpace = KeySpace(0);
const LIMITS: BatchLimits = BatchLimits {
    max_operations: 64,
    max_encoded_bytes: 1 << 20,
};
const BUDGET: usize = 16 << 20;
/// Native CRC frame header around every encoded batch.
const CRC_HEADER: usize = 8;

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

fn v(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

fn through(epoch: u64, sequence: u64) -> PrefixCut {
    PrefixCut::Through(v(epoch, sequence))
}

fn cell_id(cell: u8) -> CellId {
    shape().ks(KS).cell_id(&[cell, 0])
}

/// One operation on key `[cell, key]`: a put of `value`, or a delete.
type Op = (u8, u8, Option<u8>);

struct Wal {
    batch: CompleteBatch,
    durable: DurableBatch,
}

impl Wal {
    fn version(&self) -> BatchVersion {
        self.batch.version()
    }

    fn partition(&self) -> u32 {
        self.durable.appended().address().partition
    }

    fn offset(&self) -> u64 {
        self.durable.appended().address().offset
    }
}

/// The complete database history, in version order, with owner partition
/// and stream offsets assigned per `(partition, owner epoch)`.
struct History(Vec<Wal>);

impl History {
    fn new(batches: &[(BatchVersion, u32, Vec<Op>)]) -> Self {
        let shape = shape();
        let mut offsets: BTreeMap<(u32, u64), u64> = BTreeMap::new();
        let mut output: Vec<Wal> = Vec::new();
        for (version, partition, ops) in batches {
            assert!(output.last().is_none_or(|last| last.version() < *version));
            let mutations = ops
                .iter()
                .map(|&(cell, key, value)| {
                    let key = Bytes::copy_from_slice(&[cell, key]);
                    match value {
                        Some(value) => Mutation::Put {
                            keyspace: KS,
                            key,
                            value: Bytes::copy_from_slice(&[value; 4]),
                        },
                        None => Mutation::Delete { keyspace: KS, key },
                    }
                })
                .collect();
            let mut operation = [0; 16];
            operation[..8].copy_from_slice(&version.epoch.to_be_bytes());
            operation[8..].copy_from_slice(&version.sequence.to_be_bytes());
            let id = BatchId {
                database: DATABASE,
                operation,
            };
            let batch = CompleteBatch::new(&shape, id, *version, mutations, LIMITS).unwrap();
            let len = batch.encoded().len() + CRC_HEADER;
            let offset = offsets.entry((*partition, version.epoch)).or_default();
            let address = PhysicalValueAddress {
                partition: *partition,
                segment_generation: version.epoch,
                offset: *offset,
                len: len as u32,
            };
            *offset += len as u64;
            let durable = DurableBatch::from_validated_parts(id, *version, batch.digest(), address);
            output.push(Wal { batch, durable });
        }
        Self(output)
    }

    fn members(wal: &Wal) -> Vec<(KeySpace, CellId)> {
        let shape = shape();
        wal.batch
            .mutations()
            .iter()
            .map(|mutation| (KS, shape.ks(KS).cell_id(mutation.key())))
            .collect()
    }

    /// Key history: `(version, operation index, put)` in version order.
    fn oracle(&self) -> BTreeMap<Vec<u8>, Vec<(BatchVersion, u32, bool)>> {
        let mut keys: BTreeMap<Vec<u8>, Vec<(BatchVersion, u32, bool)>> = BTreeMap::new();
        for wal in &self.0 {
            for (n, mutation) in wal.batch.mutations().iter().enumerate() {
                keys.entry(mutation.key().to_vec()).or_default().push((
                    wal.version(),
                    n as u32,
                    matches!(mutation, Mutation::Put { .. }),
                ));
            }
        }
        keys
    }
}

/// One root cell: byte, coverage `C_i`, frame cut `B_i`.
type CellSpec = (u8, PrefixCut, Option<PrefixCut>);

fn root(cells: &[CellSpec], target: PrefixCut) -> CellRoot {
    let mut sorted: Vec<_> = cells
        .iter()
        .map(|(cell, coverage, frame)| (cell_id(*cell), *coverage, *frame))
        .collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut files = Vec::new();
    let mut entries = Vec::new();
    for (cell, coverage, frame) in sorted {
        let base = frame.map(|frame_cut| {
            let id = IndexStoreFileId::Segment {
                authority_epoch: 9,
                stream_id: 0,
                segment_seq: files.len() as u64 + 1,
            };
            files.push(RootFile {
                id,
                name: id.canonical_name(),
                sealed_len: 4096,
            });
            RootCellBase {
                file: id,
                offset: 0,
                framed_len: 64,
                digest: [0; 32],
                frame_cut,
                frame_floor: None,
            }
        });
        entries.push(RootCell {
            keyspace: KS,
            cell,
            coverage,
            base,
        });
    }
    let floor = entries
        .iter()
        .map(|cell| cell.coverage)
        .min()
        .unwrap_or(target);
    let max_frame_cut = entries
        .iter()
        .filter_map(|cell| cell.base.as_ref().map(|base| base.frame_cut))
        .fold(target, PrefixCut::max);
    let root = CellRoot {
        header: CellRootHeader {
            format: 5,
            flags: 0,
            database: DATABASE,
            schema: SCHEMA,
            authority_epoch: 9,
            snapshot_generation: 1,
            lifecycle_generation: 0,
            directory_target: target,
            replay_floor: floor,
            max_frame_cut,
            history_floor: None,
        },
        files,
        cells: entries,
    };
    root.check(DATABASE, SCHEMA).unwrap();
    root
}

/// Deliberate corruption of the fixture bases.
#[derive(Clone, Copy, Default)]
struct Tamper {
    /// Leave this member out of its cell base.
    omit: Option<(BatchVersion, u32)>,
    /// Store a different reference at this member's version.
    forge: Option<(BatchVersion, u32)>,
}

/// The loaded per-cell index of `root` over `history`: each base cell holds
/// exactly its members through its frame cut, in a validated frame at that
/// cut; the state is then set to the loader's (watermark at `F`).
fn loaded(history: &History, root: &CellRoot, budget: usize, tamper: Tamper) -> NativeIndex {
    let shape = shape();
    let index =
        NativeIndex::with_mode(&shape, DATABASE, budget, 8, CheckpointMode::PerCell).unwrap();
    let frames: HashMap<CellId, BatchVersion> = root
        .cells
        .iter()
        .filter_map(|cell| match cell.base.as_ref()?.frame_cut {
            PrefixCut::Through(batch) => Some((cell.cell.clone(), batch)),
            PrefixCut::BeforeAll => None,
        })
        .collect();
    let newest = frames.values().max().copied();
    {
        let mut writer = index.writer();
        for wal in history.0.iter().filter(|wal| Some(wal.version()) <= newest) {
            let version = wal.version();
            let prepared = PreparedNativePublication::prepare_authenticated(
                &shape,
                LIMITS,
                &wal.batch,
                wal.durable,
            )
            .unwrap();
            let mut apply = Vec::new();
            let mut forged = None;
            for (operation, reference) in prepared.members() {
                let held = frames
                    .get(operation.cell_id())
                    .is_some_and(|frame| version <= *frame);
                let at = Some((version, reference.descriptor.operation_index));
                if held && tamper.forge == at {
                    let mut forgery = *reference;
                    forgery.batch_address.offset += 1;
                    forged = Some((
                        operation.ks(),
                        operation.cell_id().clone(),
                        operation.reduced_key().clone(),
                        forgery,
                    ));
                }
                apply.push(held && tamper.omit != at && tamper.forge != at);
            }
            prepared.publish_members(&apply, &mut writer).unwrap();
            if let Some((keyspace, cell, key, forgery)) = forged {
                let transaction = Transaction::default();
                writer
                    .with_pending_table(keyspace, &cell, |table| {
                        if forgery.descriptor.kind == crate::distributed::types::RecordKind::Put {
                            table.insert(key, forgery, None, &transaction);
                        } else {
                            table.remove(key, forgery, &transaction);
                        }
                    })
                    .unwrap();
                transaction.commit();
            }
            writer.promote_committed().unwrap();
            let last = wal
                .batch
                .mutation_version(wal.batch.mutations().len() - 1)
                .unwrap();
            writer.advance_replayed(last);
        }
        writer.publish_latest();
    }
    for (cell, frame) in &frames {
        let permit = index.reserve_cell_flush(0).unwrap();
        let outcome = index
            .writer()
            .capture_cell(KS, cell, PrefixCut::Through(*frame), permit)
            .unwrap();
        let CellCaptureOutcome::Captured(capture) = outcome else {
            panic!("fixture base cell {cell:?} holds no member");
        };
        let prepared = capture.prepare().unwrap();
        let base =
            NativeIndex::encode_mapped_test_base(&shape, DATABASE, &prepared.encode_source())
                .unwrap();
        let installed = index.writer().install_cell(prepared.finish(base));
        assert!(matches!(installed, CellInstall::Installed { .. }));
    }
    index.writer().emulate_loaded_root(root).unwrap();
    index
}

/// An owner stream over the in-memory WAL, filtered by its cursor.
struct MemoryStream {
    epoch: u64,
    cursor: StreamCursor,
    batches: VecDeque<(CompleteBatch, DurableBatch)>,
}

impl ReplayStream for MemoryStream {
    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn fill(
        &mut self,
        limit: usize,
        page: &mut VecDeque<(CompleteBatch, DurableBatch)>,
    ) -> Result<bool, LocalDbError> {
        while page.len() < limit {
            let Some((batch, durable)) = self.batches.pop_front() else {
                break;
            };
            if self.cursor.reads(durable.appended().address().offset) {
                page.push_back((batch, durable));
            }
        }
        Ok(!page.is_empty())
    }
}

/// One stream per `(partition, owner epoch)` over the batches `keep` selects,
/// each read from offset 0 (WAL scan from the first fragment).
fn streams(history: &History, keep: impl Fn(&Wal) -> bool) -> Vec<MemoryStream> {
    let mut lanes: BTreeMap<(u32, u64), MemoryStream> = BTreeMap::new();
    for wal in history.0.iter().filter(|wal| keep(wal)) {
        lanes
            .entry((wal.partition(), wal.version().epoch))
            .or_insert_with(|| MemoryStream {
                epoch: wal.version().epoch,
                cursor: StreamCursor::Wal {
                    from: 0,
                    skip_through: None,
                    durable_end: u64::MAX,
                },
                batches: VecDeque::new(),
            })
            .batches
            .push_back((wal.batch.clone(), wal.durable));
    }
    lanes.into_values().collect()
}

/// Writes canonical "segments" only in memory; fails on flush number
/// `fail_at` (1-based), as a crash during an offline flush would.
struct TestFlusher {
    shape: KeyShape,
    flushes: usize,
    fail_at: Option<usize>,
}

impl TestFlusher {
    fn new(fail_at: Option<usize>) -> Self {
        Self {
            shape: shape(),
            flushes: 0,
            fail_at,
        }
    }
}

impl OfflineCellFlusher for TestFlusher {
    fn flush(&mut self, prepared: PreparedCell) -> Result<CellFlushResult, LocalDbError> {
        self.flushes += 1;
        if self.fail_at == Some(self.flushes) {
            return Err(LocalDbError::new("injected crash during offline flush"));
        }
        let base =
            NativeIndex::encode_mapped_test_base(&self.shape, DATABASE, &prepared.encode_source())
                .map_err(LocalDbError::component)?;
        Ok(prepared.finish(base))
    }
}

fn config(shape: &KeyShape, page_limit: usize) -> CellReplayConfig<'_> {
    CellReplayConfig {
        shape,
        limits: LIMITS,
        page_limit,
        tail_bound: None,
    }
}

fn replay(
    root: &CellRoot,
    index: NativeIndex,
    mut lanes: Vec<MemoryStream>,
    page_limit: usize,
    flusher: &mut TestFlusher,
) -> Result<CellReplayed, LocalDbError> {
    let shape = shape();
    replay_mixed_cuts(
        index,
        CoveragePlan::new(root),
        &mut lanes,
        &config(&shape, page_limit),
        flusher,
        |_| Ok(false),
    )
}

fn error(result: Result<CellReplayed, LocalDbError>) -> String {
    match result {
        Ok(_) => panic!("replay must fail closed"),
        Err(error) => error.to_string(),
    }
}

/// Reads at every batch cut through `last` and at the watermark equal the
/// oracle; every version is retained exactly once (no member is charged
/// twice, none is lost).
fn check_reads(index: &NativeIndex, history: &History, last: BatchVersion) {
    let oracle = history.oracle();
    let mut cuts: Vec<_> = history
        .0
        .iter()
        .map(Wal::version)
        .filter(|version| *version <= last)
        .collect();
    cuts.push(last);
    for cut in cuts {
        for (key, versions) in &oracle {
            let expected = versions
                .iter()
                .rev()
                .find(|(version, _, _)| *version <= cut)
                .and_then(|(version, operation, put)| put.then_some((*version, *operation)));
            let read = |cut: IndexReadCut| {
                index.get(KS, key, cut).unwrap().map(|reference| {
                    (
                        reference.batch_version,
                        reference.descriptor.operation_index,
                    )
                })
            };
            assert_eq!(
                read(PrefixCut::Through(cut).index_cut()),
                expected,
                "{key:?}@{cut:?}"
            );
            if cut == last {
                assert_eq!(read(IndexReadCut::Latest), expected, "{key:?}@latest");
            }
        }
    }
    let versions = oracle
        .values()
        .flatten()
        .filter(|(version, _, _)| *version <= last)
        .count();
    assert_eq!(index.stats().retained_versions, versions);
}

/// Expected `(covered, applied)` member counts from the C0 oracle.
fn oracle_plan(history: &History, root: &CellRoot) -> (u64, u64) {
    let mut counts = (0, 0);
    for wal in &history.0 {
        for member in root
            .replay_plan(wal.version(), &History::members(wal))
            .unwrap_or_default()
        {
            match member {
                MemberReplay::CompareBase => counts.0 += 1,
                MemberReplay::Apply => counts.1 += 1,
            }
        }
    }
    counts
}

/// `known` holds exactly the batches above `F`, fully covered ones included.
fn assert_identities(replayed: &CellReplayed, history: &History, root: &CellRoot) {
    let floor = root.header.replay_floor;
    let expected: BTreeMap<_, _> = history
        .0
        .iter()
        .filter(|wal| !floor.contains(wal.version()))
        .map(|wal| (wal.batch.id(), wal.durable.appended()))
        .collect();
    let known: BTreeMap<_, _> = replayed.known.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(known, expected);
}

/// Spec §8.2 across two owners and two owner epochs: batch (1,4) writes A
/// and B; A's frame includes it, B's does not. The batch also deletes and
/// rewrites one key at two operation indexes, and A's base already holds a
/// newer value of that key from epoch 2. Batch (2,2) splits the same way in
/// epoch 2, and cell C appears only after the root.
#[test]
fn split_batch_across_owners_and_epochs_applies_each_member_once() {
    const A: u8 = 1;
    const B: u8 = 2;
    const C: u8 = 3;
    let history = History::new(&[
        (v(1, 1), 0, vec![(A, 1, Some(1)), (B, 1, Some(1))]),
        (v(1, 2), 1, vec![(A, 2, Some(2)), (B, 2, Some(2))]),
        (v(1, 3), 0, vec![(A, 3, Some(3))]),
        (
            v(1, 4),
            1,
            vec![
                (A, 1, Some(4)),
                (B, 9, Some(4)),
                (A, 1, None),
                (A, 5, Some(4)),
            ],
        ),
        (v(1, 5), 0, vec![(B, 1, None), (C, 1, Some(5))]),
        (v(2, 1), 1, vec![(A, 1, Some(6))]),
        (v(2, 2), 0, vec![(A, 7, Some(7)), (B, 7, Some(7))]),
        (v(2, 3), 1, vec![(B, 2, Some(8)), (A, 8, Some(8))]),
        (v(2, 4), 0, vec![(C, 1, None), (A, 1, Some(9))]),
    ]);
    // F = (1,2). A's frame is in epoch 2, ahead of both its coverage and F.
    let root = root(
        &[
            (A, through(1, 2), Some(through(2, 2))),
            (B, through(1, 3), Some(through(1, 2))),
        ],
        through(1, 3),
    );
    assert_eq!(root.header.replay_floor, through(1, 2));
    let index = loaded(&history, &root, BUDGET, Tamper::default());
    // Before replay the loaded watermark hides A's base content above F.
    assert_eq!(
        index
            .get(KS, &[A, 1], IndexReadCut::Latest)
            .unwrap()
            .unwrap()
            .batch_version,
        v(1, 1)
    );
    let lanes = streams(&history, |_| true);
    assert_eq!(lanes.len(), 4);
    let replayed = replay(&root, index, lanes, 2, &mut TestFlusher::new(None)).unwrap();
    assert_eq!(replayed.prefix, through(2, 4));
    assert_eq!(replayed.stats.skipped_batches, 2);
    assert_eq!(replayed.stats.batches, 7);
    assert_eq!(
        (
            replayed.stats.covered_members,
            replayed.stats.applied_members
        ),
        oracle_plan(&history, &root)
    );
    // (1,4): three A members compared, B's applied. (1,3),(2,1),(2,2)-A covered.
    assert_eq!(replayed.stats.covered_members, 6);
    assert_eq!(replayed.stats.offline_flush_passes, 0);
    assert_identities(&replayed, &history, &root);
    check_reads(&replayed.index, &history, v(2, 4));
}

/// Coverage far behind frame cuts: every cell exports `C_i = F` or near it
/// while its frame is tens of batches ahead, in three owner streams.
#[test]
fn coverage_far_behind_frame_cuts_compares_instead_of_reinserting() {
    let mut batches = Vec::new();
    for sequence in 1..=48u64 {
        let ops = (0..=(sequence % 3))
            .map(|n| {
                let x = sequence * 7 + n * 13;
                let cell = (x % 4) as u8 + 1;
                (
                    cell,
                    (x % 5) as u8,
                    (!x.is_multiple_of(6)).then_some(sequence as u8),
                )
            })
            .collect::<Vec<_>>();
        let mut unique: Vec<Op> = Vec::new();
        for op in ops {
            if unique
                .iter()
                .all(|existing| (existing.0, existing.1) != (op.0, op.1))
            {
                unique.push(op);
            }
        }
        batches.push((v(3, sequence), (sequence % 3) as u32, unique));
    }
    let history = History::new(&batches);
    let root = root(
        &[
            (1, through(3, 5), Some(through(3, 44))),
            (2, through(3, 5), Some(through(3, 30))),
            (3, through(3, 9), Some(through(3, 12))),
            (4, through(3, 6), Some(through(3, 40))),
        ],
        through(3, 9),
    );
    let index = loaded(&history, &root, BUDGET, Tamper::default());
    let lanes = streams(&history, |_| true);
    let replayed = replay(&root, index, lanes, 3, &mut TestFlusher::new(None)).unwrap();
    let (covered, applied) = oracle_plan(&history, &root);
    assert!(covered > applied, "most suffix members are ahead in bases");
    assert_eq!(
        (
            replayed.stats.covered_members,
            replayed.stats.applied_members
        ),
        (covered, applied)
    );
    assert_eq!(replayed.prefix, through(3, 48));
    assert_identities(&replayed, &history, &root);
    check_reads(&replayed.index, &history, v(3, 48));
}

/// Fully covered batches above F are still authenticated, ordered and
/// entered as identities, and the watermark covers them even when the last
/// batch adds no reference.
#[test]
fn fully_covered_batches_are_authenticated_and_used_for_identity() {
    const A: u8 = 1;
    const B: u8 = 2;
    let history = History::new(&[
        (v(1, 1), 0, vec![(A, 1, Some(1)), (B, 1, Some(1))]),
        (v(1, 2), 1, vec![(A, 2, Some(2))]),
        (v(1, 3), 0, vec![(B, 2, Some(3))]),
        (v(1, 4), 1, vec![(A, 1, Some(4)), (A, 3, None)]),
    ]);
    let root = root(
        &[
            (A, through(1, 1), Some(through(1, 4))),
            (B, through(1, 3), Some(through(1, 3))),
        ],
        through(1, 3),
    );
    let build = || loaded(&history, &root, BUDGET, Tamper::default());
    let replayed = replay(
        &root,
        build(),
        streams(&history, |_| true),
        4,
        &mut TestFlusher::new(None),
    )
    .unwrap();
    assert_eq!(replayed.stats.batches, 3);
    assert_eq!(replayed.stats.fully_covered_batches, 3);
    assert_eq!(replayed.stats.applied_members, 0);
    assert_eq!(replayed.prefix, through(1, 4));
    assert_eq!(
        replayed.index.latest_cut(),
        IndexReadCut::At(history.0[3].batch.mutation_version(1).unwrap())
    );
    assert_identities(&replayed, &history, &root);
    check_reads(&replayed.index, &history, v(1, 4));

    // A covered batch with a forged receipt digest is rejected.
    let mut lanes = streams(&history, |_| true);
    for lane in &mut lanes {
        for (batch, durable) in &mut lane.batches {
            if batch.version() == v(1, 2) {
                let appended = durable.appended();
                let mut digest = appended.digest();
                digest.0[0] ^= 1;
                *durable = DurableBatch::from_validated_parts(
                    appended.id(),
                    appended.version(),
                    digest,
                    appended.address(),
                );
            }
        }
    }
    let message = error(replay(
        &root,
        build(),
        lanes,
        4,
        &mut TestFlusher::new(None),
    ));
    assert!(message.contains("does not match"), "{message}");

    // Its identity collides with the through-F retry authority.
    let shape = shape();
    let duplicate = history.0[2].batch.id();
    let result = replay_mixed_cuts(
        build(),
        CoveragePlan::new(&root),
        &mut streams(&history, |_| true),
        &config(&shape, 4),
        &mut TestFlusher::new(None),
        |id| Ok(id == duplicate),
    );
    assert!(error(result).contains("duplicate authoritative identity"));

    // Two suffix batches with one identity.
    let mut lanes = streams(&history, |_| true);
    let first = history.0[1].batch.clone();
    for lane in &mut lanes {
        for (batch, durable) in &mut lane.batches {
            if batch.version() == v(1, 4) {
                let forged = CompleteBatch::new(
                    &shape,
                    first.id(),
                    batch.version(),
                    batch.mutations().to_vec(),
                    LIMITS,
                )
                .unwrap();
                let address = durable.appended().address();
                *durable = DurableBatch::from_validated_parts(
                    forged.id(),
                    forged.version(),
                    forged.digest(),
                    address,
                );
                *batch = forged;
            }
        }
    }
    let message = error(replay(
        &root,
        build(),
        lanes,
        4,
        &mut TestFlusher::new(None),
    ));
    assert!(
        message.contains("duplicate authoritative identity"),
        "{message}"
    );
}

/// Mismatched coverage claims fail closed and return no index.
#[test]
fn covered_member_mismatch_fails_closed() {
    const A: u8 = 1;
    const B: u8 = 2;
    let history = History::new(&[
        (v(1, 1), 0, vec![(A, 1, Some(1)), (B, 1, Some(1))]),
        (v(1, 2), 0, vec![(A, 2, Some(2)), (B, 2, None)]),
        (v(1, 3), 1, vec![(A, 1, None), (B, 3, Some(3))]),
        (v(1, 4), 1, vec![(B, 1, Some(4))]),
    ]);
    let root = root(
        &[
            (A, through(1, 1), Some(through(1, 3))),
            (B, through(1, 1), Some(through(1, 1))),
        ],
        through(1, 1),
    );
    let run = |tamper: Tamper, keep: &dyn Fn(&Wal) -> bool| {
        error(replay(
            &root,
            loaded(&history, &root, BUDGET, tamper),
            streams(&history, keep),
            2,
            &mut TestFlusher::new(None),
        ))
    };
    // The untampered fixture replays.
    replay(
        &root,
        loaded(&history, &root, BUDGET, Tamper::default()),
        streams(&history, |_| true),
        2,
        &mut TestFlusher::new(None),
    )
    .unwrap();
    // A claimed covered put, then a covered delete, absent from the base.
    for at in [(v(1, 2), 0), (v(1, 3), 0)] {
        let message = run(
            Tamper {
                omit: Some(at),
                ..Tamper::default()
            },
            &|_| true,
        );
        assert!(message.contains("absent from or different"), "{message}");
    }
    // A different reference at the covered member's version.
    let message = run(
        Tamper {
            forge: Some((v(1, 3), 0)),
            ..Tamper::default()
        },
        &|_| true,
    );
    assert!(message.contains("absent from or different"), "{message}");
    // A base reference above F whose batch the suffix lacks.
    let message = run(Tamper::default(), &|wal| wal.version() != v(1, 3));
    assert!(message.contains("no suffix batch replayed"), "{message}");

    // Coverage above the frame (a clean advance) with a WAL change inside
    // it: the claimed coverage is false.
    let lying = root_with(&[
        (A, through(1, 2), Some(through(1, 1))),
        (B, through(1, 1), Some(through(1, 1))),
    ]);
    let message = error(replay(
        &lying,
        loaded(&history, &lying, BUDGET, Tamper::default()),
        streams(&history, |_| true),
        2,
        &mut TestFlusher::new(None),
    ));
    assert!(message.contains("absent from or different"), "{message}");
}

fn root_with(cells: &[CellSpec]) -> CellRoot {
    let target = cells.iter().map(|cell| cell.1).max().unwrap();
    root(cells, target)
}

/// A suffix whose cumulative index charge is several times `M`: offline
/// flushes keep the resident charge within `M`, recovery completes, and an
/// injected crash during an offline flush restarts from the same root.
#[test]
fn suffix_larger_than_budget_flushes_offline_and_survives_a_second_crash() {
    const BUDGET_M: usize = 64 << 10;
    let mut batches = Vec::new();
    for sequence in 1..=720u64 {
        let x = sequence.wrapping_mul(2_654_435_761);
        let ops = vec![
            (
                (x % 6) as u8 + 1,
                (sequence % 251) as u8,
                Some(sequence as u8),
            ),
            (
                ((x >> 8) % 6) as u8 + 1,
                ((sequence + 7) % 249) as u8,
                (sequence % 5 != 0).then_some(1),
            ),
        ];
        let ops = if (ops[0].0, ops[0].1) == (ops[1].0, ops[1].1) {
            ops[..1].to_vec()
        } else {
            ops
        };
        batches.push((v(4, sequence), (sequence % 2) as u32, ops));
    }
    let history = History::new(&batches);
    // Every cell is in the root (a cell absent from it would have no member
    // through T); frames are ahead of F by up to 50 batches.
    let root = root(
        &[
            (1, through(4, 10), Some(through(4, 60))),
            (2, through(4, 10), Some(through(4, 30))),
            (3, through(4, 12), Some(through(4, 30))),
            (4, through(4, 12), Some(through(4, 30))),
            (5, through(4, 12), Some(through(4, 40))),
            (6, through(4, 12), Some(through(4, 30))),
        ],
        through(4, 12),
    );
    let run = |budget: usize, flusher: &mut TestFlusher| {
        replay(
            &root,
            loaded(&history, &root, budget, Tamper::default()),
            streams(&history, |_| true),
            8,
            flusher,
        )
    };
    let unbounded = run(BUDGET, &mut TestFlusher::new(None)).unwrap();
    assert_eq!(unbounded.stats.offline_flush_passes, 0);
    assert!(
        unbounded.stats.peak_resident_bytes > 3 * BUDGET_M,
        "the suffix must exceed M: {}",
        unbounded.stats.peak_resident_bytes
    );

    // First recovery crashes inside its third offline flush.
    let mut crashing = TestFlusher::new(Some(3));
    let message = error(run(BUDGET_M, &mut crashing));
    assert!(message.contains("injected crash"), "{message}");
    assert_eq!(crashing.flushes, 3);

    // Restart: the same adopted root and the full retained suffix; the
    // crashed attempt's outputs were never referenced by the root.
    let mut flusher = TestFlusher::new(None);
    let replayed = run(BUDGET_M, &mut flusher).unwrap();
    assert!(
        replayed.stats.offline_flush_passes >= 3,
        "{:?}",
        replayed.stats
    );
    assert_eq!(
        replayed.stats.offline_flushed_cells as usize,
        flusher.flushes
    );
    assert!(
        replayed.stats.peak_resident_bytes <= BUDGET_M,
        "{:?}",
        replayed.stats
    );
    assert!(replayed.index.charged_metadata_bytes() <= BUDGET_M);
    assert_eq!(replayed.prefix, unbounded.prefix);
    assert_eq!(
        (
            replayed.stats.covered_members,
            replayed.stats.applied_members
        ),
        (
            unbounded.stats.covered_members,
            unbounded.stats.applied_members
        )
    );
    assert_identities(&replayed, &history, &root);
    check_reads(&replayed.index, &history, v(4, 720));
}

/// Both history modes' wrapper cursors (C0 `CellCheckpointStream`), the
/// through-F physical skip, and the logical floor filter of batches that a
/// fragment-aligned scan returns.
#[test]
fn wrapper_cursors_skip_through_f_in_both_history_modes() {
    let proof = |prefix| CheckpointStream {
        proof: CheckpointProof {
            database: DATABASE,
            partition: 0,
            epoch: 1,
            prefix,
            retained: prefix + 3,
            digest: [0; 32],
        },
        state: [0; 32],
    };
    let stream = |complete, receipt_prefix| CellCheckpointStream {
        partition: 0,
        epoch: 1,
        replay_from: 4096,
        last_offset: Some(5000),
        durable_end: 9000,
        complete,
        receipt_prefix,
    };
    let wal = HistoryPersistence::WalReplay;
    let receipts = HistoryPersistence::OnDisk;
    let cursor = StreamCursor::from_wrapper(&stream(false, None), wal).unwrap();
    assert_eq!(
        cursor,
        StreamCursor::Wal {
            from: 4096,
            skip_through: Some(5000),
            durable_end: 9000,
        }
    );
    assert!(!cursor.reads(5000) && cursor.reads(5001));
    assert_eq!(
        StreamCursor::from_wrapper(&stream(true, None), wal).unwrap(),
        StreamCursor::Complete
    );
    assert!(!StreamCursor::Complete.reads(u64::MAX));
    assert_eq!(
        StreamCursor::from_wrapper(&stream(false, Some(proof(17))), receipts).unwrap(),
        StreamCursor::Receipts { from: 17 }
    );
    assert_eq!(
        StreamCursor::from_wrapper(&stream(true, Some(proof(17))), receipts).unwrap(),
        StreamCursor::Complete
    );
    assert!(StreamCursor::from_wrapper(&stream(false, Some(proof(1))), wal).is_err());
    assert!(StreamCursor::from_wrapper(&stream(false, None), receipts).is_err());

    // A WAL stream skips physically through its last through-F offset; a
    // fragment-aligned scan of the other stream returns batches at or below
    // F, which the logical floor skips.
    let history = History::new(&[
        (v(1, 1), 0, vec![(1, 1, Some(1))]),
        (v(1, 2), 1, vec![(1, 2, Some(2))]),
        (v(1, 3), 0, vec![(1, 3, Some(3))]),
        (v(1, 4), 1, vec![(2, 1, Some(4))]),
        (v(1, 5), 0, vec![(1, 1, None)]),
    ]);
    let root = root(&[(1, through(1, 3), Some(through(1, 3)))], through(1, 3));
    let mut lanes = streams(&history, |_| true);
    lanes[0].cursor = StreamCursor::Wal {
        from: 0,
        skip_through: Some(history.0[2].offset()),
        durable_end: u64::MAX,
    };
    let replayed = replay(
        &root,
        loaded(&history, &root, BUDGET, Tamper::default()),
        lanes,
        1,
        &mut TestFlusher::new(None),
    )
    .unwrap();
    assert_eq!(replayed.stats.skipped_batches, 1, "(1,2) from partition 1");
    assert_eq!(replayed.stats.batches, 2);
    check_reads(&replayed.index, &history, v(1, 5));

    // A tail bound (receipt-history mode) counts whole batches.
    let shape = shape();
    let mut bounded = config(&shape, 2);
    bounded.tail_bound = Some(ReplayLimits {
        max_batches: 1,
        max_encoded_bytes: 1 << 20,
    });
    let result = replay_mixed_cuts(
        loaded(&history, &root, BUDGET, Tamper::default()),
        CoveragePlan::new(&root),
        &mut streams(&history, |_| true),
        &bounded,
        &mut TestFlusher::new(None),
        |_| Ok(false),
    );
    assert!(error(result).contains("retained tail exceeds"));
}

/// Stream order is enforced per stream and across streams, and an empty
/// suffix keeps the complete prefix at F.
#[test]
fn stream_order_violations_fail_and_an_empty_suffix_keeps_f() {
    let history = History::new(&[
        (v(1, 1), 0, vec![(1, 1, Some(1))]),
        (v(1, 2), 0, vec![(1, 2, Some(2))]),
        (v(1, 3), 1, vec![(2, 1, Some(3))]),
    ]);
    let root = root(&[(1, through(1, 1), Some(through(1, 1)))], through(1, 1));
    let run = |lanes: Vec<MemoryStream>| {
        replay(
            &root,
            loaded(&history, &root, BUDGET, Tamper::default()),
            lanes,
            2,
            &mut TestFlusher::new(None),
        )
    };
    let empty = run(Vec::new()).unwrap();
    assert_eq!(empty.prefix, through(1, 1));
    assert!(empty.known.is_empty());
    assert_eq!(empty.index.latest_cut(), through(1, 1).index_cut());

    // Out of order inside one stream.
    let mut lanes = streams(&history, |_| true);
    lanes[0].batches.swap(0, 1);
    assert!(error(run(lanes)).contains("not strictly ordered"));
    // A batch in another owner epoch than its stream.
    let mut lanes = streams(&history, |_| true);
    lanes[1].epoch = 2;
    assert!(error(run(lanes)).contains("not strictly ordered"));
    // One version in two streams.
    let mut lanes = streams(&history, |_| true);
    let copy = lanes[0].batches[1].clone();
    lanes[1].batches.push_front(copy);
    assert!(error(run(lanes)).contains("duplicate logical version"));
}
