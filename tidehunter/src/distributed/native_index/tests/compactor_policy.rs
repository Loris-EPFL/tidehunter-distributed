//! Compactor at Exact capture (COMPACTOR-PARITY.md): index-only marks stored
//! in their cell frames, the native strict dirty threshold, stale periodic
//! evaluation, recovery baseline, stepped install, folding, reopen, the mark
//! budget, read views across install, and the callback contract.

use super::*;
use crate::distributed::native_index::{CompactorCadence, CompactorPolicy, MarkBudget};
use crate::key_shape::Compactor;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrder};
use std::time::{Duration, Instant};

/// Cell 1 of `plain` (first byte at or above 128).
const K1: &[u8] = &[128, 0, 0, 1];
const K2: &[u8] = &[128, 0, 0, 2];
const K3: &[u8] = &[200, 0, 0, 3];
/// Cell 0 of `plain`.
const L1: &[u8] = &[0, 0, 0, 1];
const L2: &[u8] = &[1, 0, 0, 0];

fn cadence(max_dirty_keys: usize) -> CompactorCadence {
    CompactorCadence {
        max_dirty_keys,
        unload_jitter_pct: 0,
        snapshot_written_bytes: u64::MAX,
        snapshot_unload_threshold: u64::MAX,
    }
}

#[derive(Clone, Default)]
struct Calls {
    count: Arc<AtomicUsize>,
    inputs: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
}

impl Calls {
    fn count(&self) -> usize {
        self.count.load(AtomicOrder::SeqCst)
    }
}

/// Sui-objects-like policy: keep the greatest key of the cell. Records every
/// invocation and its complete input.
fn keep_last(calls: &Calls) -> Compactor {
    let calls = calls.clone();
    Box::new(move |keys| {
        calls.count.fetch_add(1, AtomicOrder::SeqCst);
        let input: Vec<Vec<u8>> = keys.map(|key| key.to_vec()).collect();
        let retain = input.last().map(|key| Bytes::copy_from_slice(key));
        calls.inputs.lock().push(input);
        retain.into_iter().collect()
    })
}

fn policy(compactor: Compactor, cadence: CompactorCadence) -> Arc<CompactorPolicy> {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        "plain",
        4,
        1,
        KeyType::uniform(2),
        KeySpaceConfig::new().with_compactor(compactor),
    );
    CompactorPolicy::from_shape(&builder.build(), cadence).expect("callback keyspace")
}

struct World {
    temp: tempdir::TempDir,
    log: PartitionLog,
    index: NativeIndex,
    next: u64,
    roots: usize,
    /// Prune-mark counters of the newest image.
    folded: u64,
}

impl World {
    fn new(name: &str, policy: Option<Arc<CompactorPolicy>>) -> Self {
        Self::with_budget(name, policy, BUDGET)
    }

    /// A world whose index metadata budget (and so its per-cell scratch) is
    /// `budget` bytes.
    fn with_budget(name: &str, policy: Option<Arc<CompactorPolicy>>, budget: usize) -> Self {
        let temp = tempdir::TempDir::new(name).unwrap();
        let log = log(&temp.path().join("wal"));
        let index = NativeIndex::new(&shape(), DATABASE, budget).unwrap();
        if let Some(policy) = policy {
            index.set_compactor_policy(policy).unwrap();
        }
        Self {
            temp,
            log,
            index,
            next: 0,
            roots: 0,
            folded: 0,
        }
    }

    fn write(&mut self, mutations: Vec<Mutation>) {
        let batch = batch(self.next, mutations);
        let receipt = durable(&self.log, std::slice::from_ref(&batch))[0];
        install(&self.index, &self.log, &batch, receipt);
        self.next += 1;
    }

    fn root(&mut self) -> PathBuf {
        self.roots += 1;
        self.temp.path().join(format!("root-{}.index", self.roots))
    }

    fn prepare(
        &mut self,
        delta: u64,
        force: bool,
        recovery: bool,
        floor: Option<IndexReadCut>,
    ) -> PreparedIndexImage {
        let mut writer = self.index.writer();
        let cut = self.index.call_cut();
        match floor {
            Some(floor) => writer.begin_compacted_capture(cut, floor),
            None => writer.begin_capture(cut),
        }
        .unwrap();
        writer.begin_capture_policy(delta, force, recovery).unwrap();
        writer.capture_step(usize::MAX).unwrap()
    }

    /// One complete Exact image: capture, persist, adopt, install.
    fn checkpoint(&mut self, force: bool) -> CompactorStats {
        self.checkpoint_with(0, force, false, None)
    }

    fn checkpoint_with(
        &mut self,
        delta: u64,
        force: bool,
        recovery: bool,
        floor: Option<IndexReadCut>,
    ) -> CompactorStats {
        let prepared = self.prepare(delta, force, recovery, floor);
        let root = self.root();
        let persisted = prepared.persist(&root).unwrap();
        let stats = persisted.compactor_stats;
        self.folded = persisted.prune_stats.keys_folded;
        self.index.writer().install_image(persisted);
        stats
    }

    fn reopen(&mut self, policy: Option<Arc<CompactorPolicy>>) {
        let root = self.temp.path().join(format!("root-{}.index", self.roots));
        self.index = NativeIndex::open_snapshot(&root, &shape(), DATABASE, BUDGET).unwrap();
        if let Some(policy) = policy {
            self.index.set_compactor_policy(policy).unwrap();
        }
    }

    fn visible(&self, key: &[u8], cut: IndexReadCut) -> bool {
        self.index.get(KeySpace(0), key, cut).unwrap().is_some()
    }

    fn scan(&self, reverse: bool) -> Vec<Vec<u8>> {
        self.scan_at(reverse, IndexReadCut::Latest)
    }

    fn scan_at(&self, reverse: bool, cut: IndexReadCut) -> Vec<Vec<u8>> {
        self.index
            .scan(KeySpace(0), None, reverse, usize::MAX, cut)
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.to_vec())
            .collect()
    }

    /// Container and offset of the frame that stores `key`'s cell.
    fn cell_frame(&self, key: &[u8]) -> (PathBuf, u64) {
        let identity = (
            KeySpace(0),
            self.index.descriptor(KeySpace(0)).unwrap().cell_id(key),
        );
        let shard = self.index.shard_for(&identity).read();
        let file = shard
            .get(&identity)
            .and_then(|cell| cell.base.as_ref())
            .and_then(|base| base.file.as_ref())
            .expect("stored cell");
        (file.path.clone(), file.offset)
    }

    /// CRC-framed payload of the newest root.
    fn root_payload(&self) -> Vec<u8> {
        let root = self.temp.path().join(format!("root-{}.index", self.roots));
        std::fs::read(root).unwrap()[8..].to_vec()
    }
}

/// Keep every key: an evaluation that adds no mark.
fn keep_all(calls: &Calls) -> Compactor {
    let calls = calls.clone();
    Box::new(move |keys| {
        calls.count.fetch_add(1, AtomicOrder::SeqCst);
        keys.cloned().collect()
    })
}

/// Keep no key: every selectable head of the cell is marked.
fn keep_none(calls: &Calls) -> Compactor {
    let calls = calls.clone();
    Box::new(move |keys| {
        calls.count.fetch_add(1, AtomicOrder::SeqCst);
        keys.for_each(drop);
        std::collections::HashSet::new()
    })
}

/// Records the reservation balance; refuses every reservation when `refuse`.
struct Budget {
    held: Arc<AtomicUsize>,
    refuse: bool,
}

impl MarkBudget for Budget {
    fn reserve(&mut self, bytes: usize) -> bool {
        if self.refuse {
            return false;
        }
        self.held.fetch_add(bytes, AtomicOrder::SeqCst);
        true
    }

    fn release(&mut self, bytes: usize) {
        self.held.fetch_sub(bytes, AtomicOrder::SeqCst);
    }
}

#[test]
fn named_force_marks_rejected_heads_without_a_logical_version() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-force",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, L1, b"l1")]);
    let published = world.index.call_cut();
    // An ordinary capture below the threshold never calls back.
    assert_eq!(world.checkpoint(false).evaluated_cells, 0);
    assert_eq!(calls.count(), 0);
    let stats = world.checkpoint(true);
    // One callback per dirty cell, on its complete sorted input.
    assert_eq!(calls.count(), 2);
    assert_eq!(
        (
            stats.force_cells,
            stats.evaluated_cells,
            stats.keep_all_cells
        ),
        (2, 2, 1)
    );
    assert_eq!(stats.keys_marked, 1);
    let mut inputs = calls.inputs.lock().clone();
    inputs.sort();
    assert_eq!(
        inputs,
        vec![vec![L1.to_vec()], vec![K1.to_vec(), K2.to_vec()]]
    );
    // No user version: the published cut and the next allocation are unchanged.
    assert_eq!(world.index.call_cut(), published);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    assert!(world.visible(L1, IndexReadCut::Latest));
    // A cut below the evaluation cut keeps the old state; absence never falls
    // back to an older version.
    assert!(world.visible(K1, cut(1, 0)));
    assert_eq!(world.scan(false), vec![L1.to_vec(), K2.to_vec()]);
    assert_eq!(world.scan(true), vec![K2.to_vec(), L1.to_vec()]);
    // A write after the evaluation cut wins over the mark.
    world.write(vec![put(0, K1, b"k1-new")]);
    assert!(world.visible(K1, IndexReadCut::Latest));
    assert!(!world.visible(K1, cut(2, 0)));
}

#[test]
fn marks_and_progress_survive_reopen_without_a_callback() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-reopen",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.checkpoint(true);
    assert_eq!(calls.count(), 1);
    let reopened = Calls::default();
    world.reopen(Some(policy(keep_last(&reopened), cadence(1000))));
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    // A clean cell has no debt: a later forced image does not call back.
    world.checkpoint(true);
    assert_eq!(reopened.count(), 0);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    // Without any callback keyspace the marks stay; the progress drops.
    world.reopen(None);
    world.checkpoint(false);
    // That root ends at its empty mask section and still serves the marks:
    // they are in the reused cell frame, not in the root.
    assert!(world.root_payload().ends_with(&0u32.to_be_bytes()));
    world.reopen(None);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
}

#[test]
fn pressure_is_strictly_above_the_threshold_and_counts_distinct_keys() {
    let calls = Calls::default();
    let threshold = 2;
    let mut world = World::new(
        "compactor-threshold",
        Some(policy(keep_last(&calls), cadence(threshold))),
    );
    // One key overwritten many times is one dirty key.
    for value in 0..4u8 {
        world.write(vec![put(0, K1, &[value])]);
    }
    world.write(vec![put(0, K2, b"k2")]);
    // D = T: no callback, debt is kept.
    assert_eq!(world.checkpoint(false).evaluated_cells, 0);
    assert_eq!(calls.count(), 0);
    // The progress (D = 2) is persisted and restored.
    let restored = Calls::default();
    world.reopen(Some(policy(keep_last(&restored), cadence(threshold))));
    world.write(vec![put(0, K3, b"k3")]);
    // D = T + 1 on the first covering capture, although that capture covers
    // only one new key.
    let stats = world.checkpoint(false);
    assert_eq!((stats.pressure_cells, stats.evaluated_cells), (1, 1));
    assert_eq!(restored.count(), 1);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(!world.visible(K2, IndexReadCut::Latest));
    assert!(world.visible(K3, IndexReadCut::Latest));
    // Evaluation cleared the debt: the next ordinary capture is quiet.
    world.write(vec![put(0, K3, b"k3-new")]);
    world.checkpoint(false);
    assert_eq!(restored.count(), 1);
}

#[test]
fn real_delete_counts_as_dirt_but_never_enters_the_callback() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-delete",
        Some(policy(keep_last(&calls), cadence(1))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![delete(K2)]);
    world.checkpoint(false);
    assert_eq!(calls.count(), 1);
    assert_eq!(*calls.inputs.lock(), vec![vec![K1.to_vec()]]);
    // Keep-all (a single input key) is a valid evaluation without a mark.
    assert!(world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn recovery_baseline_counts_dirt_and_never_calls_back() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-recovery",
        Some(policy(keep_last(&calls), cadence(1))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, K3, b"k3")]);
    let stats = world.checkpoint_with(0, true, true, None);
    assert_eq!((stats.evaluated_cells, stats.force_cells), (0, 0));
    assert_eq!(calls.count(), 0);
    assert!(world.visible(K1, IndexReadCut::Latest));
    // The carried debt triggers on the next serving capture, with no new write.
    let stats = world.checkpoint(false);
    assert_eq!(stats.pressure_cells, 1);
    assert_eq!(calls.count(), 1);
    assert!(!world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn stepped_install_exposes_new_marks_only_after_the_last_cell() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-stepped",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, L1, b"l1"), put(0, L2, b"l2")]);
    world.write(vec![put(0, K1, b"k1"), put(0, K2, b"k2")]);
    let prepared = world.prepare(0, true, false, None);
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    assert_eq!(persisted.compactor_stats.keys_marked, 2);
    world.index.writer().begin_install_image(persisted);
    // Cell 0 is installed with its mark, which stays hidden.
    assert!(!world.index.writer().install_image_step(1));
    assert!(world.visible(L1, IndexReadCut::Latest));
    assert!(world.visible(K1, IndexReadCut::Latest));
    assert!(world.index.writer().install_image_step(1));
    world.index.writer().reap_step(usize::MAX);
    assert!(!world.visible(L1, IndexReadCut::Latest));
    assert!(!world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn a_floor_past_the_mark_folds_it_with_the_versions_it_covers() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-fold",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K1, b"k1-b")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.checkpoint(true);
    let versions = world.index.stats().retained_versions;
    // The adopting image never folds its own new marks.
    world.checkpoint(false);
    assert_eq!(world.folded, 0);
    let floor = world.index.call_cut();
    world.checkpoint_with(0, false, false, Some(floor));
    assert_eq!(world.folded, 1);
    assert_eq!(world.index.stats().retained_versions, versions - 2);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    world.reopen(None);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
}

#[test]
fn a_callback_returning_more_keys_than_its_input_fails_the_image() {
    let mut world = World::new(
        "compactor-contract",
        Some(policy(
            Box::new(|_| {
                [K1, K2, K3]
                    .into_iter()
                    .map(Bytes::copy_from_slice)
                    .collect()
            }),
            cadence(1000),
        )),
    );
    world.write(vec![put(0, K1, b"k1")]);
    let prepared = world.prepare(0, true, false, None);
    let root = world.root();
    assert!(matches!(
        prepared.persist(&root),
        Err(NativeIndexError::Invalid(_))
    ));
}

#[test]
fn periodic_evaluation_needs_the_poll_and_strictly_more_written_bytes() {
    let calls = Calls::default();
    let cadence = CompactorCadence {
        max_dirty_keys: 1000,
        unload_jitter_pct: 0,
        snapshot_written_bytes: 100,
        snapshot_unload_threshold: 50,
    };
    let policy = policy(keep_last(&calls), cadence);
    let start = Instant::now();
    let mut world = World::new("compactor-periodic", Some(Arc::clone(&policy)));
    world.write(vec![put(0, K1, b"k1")]);
    // Before the first 60 s poll nothing is periodic.
    policy.set_test_now(start + Duration::from_secs(30));
    world.checkpoint_with(100, false, false, None);
    // At the poll, written bytes equal to the threshold do not trigger.
    policy.set_test_now(start + Duration::from_secs(61));
    world.checkpoint_with(0, false, false, None);
    assert_eq!(calls.count(), 0);
    world.write(vec![put(0, K2, b"k2")]);
    // 101 > 100, but no poll within 60 s of the last one.
    policy.set_test_now(start + Duration::from_secs(62));
    world.checkpoint_with(1, false, false, None);
    assert_eq!(calls.count(), 0);
    // Next poll: the written threshold is exceeded and the dirty cell is
    // stale (evaluated last at 0 covered bytes <= 101 - 50).
    policy.set_test_now(start + Duration::from_secs(122));
    let stats = world.checkpoint_with(0, false, false, None);
    assert_eq!((stats.stale_cells, stats.evaluated_cells), (1, 1));
    assert_eq!(calls.count(), 1);
    assert!(!world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn marks_are_stored_in_their_cell_frame_and_reused_with_it() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-cell-frame",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, L1, b"l1")]);
    world.checkpoint(false);
    let unmarked = world.cell_frame(K1);
    // A new mark rewrites its cell, although the cell has no new reference.
    assert_eq!(world.checkpoint(true).keys_marked, 1);
    let marked = world.cell_frame(K1);
    assert_ne!(marked, unmarked);
    // An image that changes only cell 0 reuses the marked frame unchanged.
    world.write(vec![put(0, L2, b"l2")]);
    world.checkpoint(false);
    assert_eq!(world.cell_frame(K1), marked);
    world.reopen(Some(policy(keep_last(&Calls::default()), cadence(1000))));
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    assert_eq!(
        world.scan(false),
        vec![L1.to_vec(), L2.to_vec(), K2.to_vec()]
    );
}

#[test]
fn root_size_does_not_grow_with_marks() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-root-size",
        Some(policy(keep_last(&calls), cadence(1_000_000))),
    );
    // 200 keys of cell 1 (within one encoder's head scratch), so the
    // evaluation marks 199 of them.
    let keys: Vec<[u8; 4]> = (0..200u16)
        .map(|n| {
            let [high, low] = n.to_be_bytes();
            [128, 1, high, low]
        })
        .collect();
    for chunk in keys.chunks(20) {
        world.write(chunk.iter().map(|key| put(0, key, b"v")).collect());
    }
    world.checkpoint(false);
    let before = world.root_payload().len();
    assert_eq!(world.checkpoint(true).keys_marked, 199);
    // Same cell directory and the same policy override entry: the marks add
    // no root byte.
    assert_eq!(world.root_payload().len(), before);
}

#[test]
fn keep_all_evaluation_of_a_clean_cell_reuses_its_frame() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-keep-all",
        Some(policy(keep_all(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    // Below the threshold: the cell is rewritten and carries its debt.
    world.checkpoint(false);
    let frame = world.cell_frame(K1);
    let stats = world.checkpoint(true);
    assert_eq!((stats.evaluated_cells, stats.keep_all_cells), (1, 1));
    // Only the root policy progress changed: the frame is reused.
    assert_eq!(world.cell_frame(K1), frame);
    // The cleared debt survives reopen: a forced image does not call back.
    let reopened = Calls::default();
    world.reopen(Some(policy(keep_all(&reopened), cadence(1000))));
    world.checkpoint(true);
    assert_eq!(reopened.count(), 0);
    assert!(world.visible(K1, IndexReadCut::Latest));
}

/// Main decision (2): reads carry no mark revision. A view at a cut at or
/// above the evaluation cut `C` that was pinned before installation sees the
/// new marks after it, as a native reader sees the index after the flush
/// that compacted it; a view below `C` is unaffected.
#[test]
fn views_at_or_above_the_evaluation_cut_see_marks_after_install() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-views",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    let below = world.index.pin_cut(cut(0, 0)).unwrap();
    let at = world.index.pin_cut(IndexReadCut::Latest).unwrap();
    assert!(world.visible(K1, at.cut()));
    world.checkpoint(true);
    assert!(!world.visible(K1, at.cut()));
    assert_eq!(world.scan_at(false, at.cut()), vec![K2.to_vec()]);
    assert_eq!(world.scan_at(true, at.cut()), vec![K2.to_vec()]);
    assert!(world.visible(K1, below.cut()));
    assert_eq!(world.scan_at(false, below.cut()), vec![K1.to_vec()]);
}

#[test]
fn a_range_drop_resets_dirt_below_its_mask() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-drop-reset",
        Some(policy(keep_last(&calls), cadence(2))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    // D = T = 2: no callback; the debt is carried.
    world.checkpoint(false);
    world.write(vec![Mutation::DropCells {
        keyspace: KeySpace(0),
        first_cell: CellId::Integer(1),
        last_cell: CellId::Integer(1),
    }]);
    world.write(vec![put(0, K3, b"k3")]);
    // Without the reset D would be 3 > 2. The drop cancels the debt below
    // its mask and adds none itself: D = 1.
    world.checkpoint(false);
    assert_eq!(calls.count(), 0);
    world.write(vec![put(0, K1, b"k1-new")]);
    world.write(vec![put(0, K2, b"k2-new")]);
    let stats = world.checkpoint(false);
    assert_eq!((stats.pressure_cells, stats.evaluated_cells), (1, 1));
    assert_eq!(
        *calls.inputs.lock(),
        vec![vec![K1.to_vec(), K2.to_vec(), K3.to_vec()]]
    );
    assert_eq!(world.scan(false), vec![K3.to_vec()]);
}

#[test]
fn keep_none_then_fold_restores_the_root_cut_at_reopen() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-keep-none",
        Some(policy(keep_none(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    assert_eq!(world.checkpoint(true).keys_marked, 2);
    assert!(world.scan(false).is_empty());
    let published = world.index.call_cut();
    // The floor reaches the marks: they fold with every version they cover,
    // so the cell stores no row and gets no frame.
    world.checkpoint_with(0, false, false, Some(published.clone()));
    assert_eq!(world.folded, 2);
    assert_eq!(world.index.stats().retained_versions, 0);
    world.reopen(None);
    // The published cut comes from the root, not from a stored reference.
    assert_eq!(world.index.call_cut(), published);
    assert!(world.scan(false).is_empty());
    assert!(!world.visible(K1, IndexReadCut::Latest));
    world.write(vec![put(0, K1, b"k1-new")]);
    assert!(world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn a_refused_mark_reservation_carries_the_debt_without_marks() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-budget-refused",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    let held = Arc::new(AtomicUsize::new(0));
    let prepared = world
        .prepare(0, true, false, None)
        .with_mark_budget(Box::new(Budget {
            held: Arc::clone(&held),
            refuse: true,
        }));
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    let stats = persisted.compactor_stats;
    world.index.writer().install_image(persisted);
    // Never a callback on a cell whose marks may not fit, never a partial
    // suppression; the debt stays.
    assert_eq!((stats.capacity_cells, stats.evaluated_cells), (1, 0));
    assert_eq!(calls.count(), 0);
    assert!(world.visible(K1, IndexReadCut::Latest));
    assert_eq!(world.checkpoint(true).evaluated_cells, 1);
    assert!(!world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn the_budget_keeps_only_the_charge_of_added_marks() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-budget-held",
        Some(policy(keep_last(&calls), cadence(1000))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, K3, b"k3")]);
    let held = Arc::new(AtomicUsize::new(0));
    let prepared = world
        .prepare(0, true, false, None)
        .with_mark_budget(Box::new(Budget {
            held: Arc::clone(&held),
            refuse: false,
        }));
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    // Three heads reserved, one retained: two marks stay charged until the
    // caller drops the budget after live install.
    assert_eq!(
        held.load(AtomicOrder::SeqCst),
        2 * super::super::prune_marks::key_charge(K1.len())
    );
    world.index.writer().install_image(persisted);
    // Installation moves the captured overlay into the new base, so the
    // resident total changes by more than the marks. Compare with a twin that
    // installs the same rows after a keep-all evaluation (no mark): the only
    // difference is the two installed marks.
    let twin_calls = Calls::default();
    let mut twin = World::new(
        "compactor-budget-twin",
        Some(policy(keep_all(&twin_calls), cadence(1000))),
    );
    twin.write(vec![put(0, K1, b"k1")]);
    twin.write(vec![put(0, K2, b"k2")]);
    twin.write(vec![put(0, K3, b"k3")]);
    assert_eq!(twin.checkpoint(true).keys_marked, 0);
    assert_eq!(
        world.index.stats().charged_metadata_bytes - twin.index.stats().charged_metadata_bytes,
        2 * super::super::prune_marks::key_charge(K1.len())
    );
}

/// An R1 `Remove` of the current head of `key` (the agreed R1 hand-off:
/// `PreparedIndexImage::with_prune_plan`).
fn r1_candidate(world: &World, key: &[u8]) -> super::super::prune_marks::PruneCandidate {
    let identity = (
        KeySpace(0),
        world.index.descriptor(KeySpace(0)).unwrap().cell_id(key),
    );
    let birth = world
        .index
        .shard_for(&identity)
        .read()
        .get(&identity)
        .unwrap()
        .birth;
    super::super::prune_marks::PruneCandidate {
        keyspace: identity.0,
        cell: identity.1,
        birth,
        key: Bytes::copy_from_slice(key),
        expected: world
            .index
            .get(KeySpace(0), key, IndexReadCut::Latest)
            .unwrap()
            .unwrap(),
    }
}

/// One ordinary Exact image carrying an R1 plan, sorted by `(cell, key)`.
fn r1_checkpoint(
    world: &mut World,
    plan: Vec<super::super::prune_marks::PruneCandidate>,
) -> (CompactorStats, crate::distributed::native_index::PruneStats) {
    let prepared = world
        .prepare(0, false, false, None)
        .with_prune_plan(crate::distributed::native_index::PrunePlan { candidates: plan });
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    let stats = (persisted.compactor_stats, persisted.prune_stats);
    world.index.writer().install_image(persisted);
    stats
}

/// COMPACTOR-PARITY.md §5 test 4: an accepted R1 plan coalesced with
/// automatic pressure in the same capture runs the callback once on the
/// touched cell; the input already excludes the R1-removed head, and both
/// mark sources are adopted together.
#[test]
fn an_r1_plan_coalesced_with_pressure_calls_back_once_per_cell() {
    let calls = Calls::default();
    let mut world = World::new(
        "compactor-r1-pressure",
        Some(policy(keep_last(&calls), cadence(1))),
    );
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, K3, b"k3")]);
    world.write(vec![put(0, L1, b"l1")]);
    let published = world.index.call_cut();
    let plan = vec![r1_candidate(&world, K1)];
    let (stats, prune) = r1_checkpoint(&mut world, plan);
    assert_eq!((prune.plan_accepted, prune.plan_stale), (1, 0));
    // Cell 1 (three dirty keys > 1) by pressure and R1 together, once; cell 0
    // (one dirty key) is not selected.
    assert_eq!(
        (
            stats.pressure_cells,
            stats.relocation_cells,
            stats.evaluated_cells
        ),
        (1, 1, 1)
    );
    assert_eq!(calls.count(), 1);
    assert_eq!(
        calls.inputs.lock().clone(),
        vec![vec![K2.to_vec(), K3.to_vec()]]
    );
    assert_eq!(stats.keys_marked, 1);
    assert_eq!(world.index.call_cut(), published);
    assert_eq!(world.scan(false), vec![L1.to_vec(), K3.to_vec()]);
    assert_eq!(world.scan(true), vec![K3.to_vec(), L1.to_vec()]);
    // Reopen without a callback keeps both mark sources and calls nothing.
    world.reopen(Some(policy(keep_last(&calls), cadence(1))));
    assert_eq!(world.scan(false), vec![L1.to_vec(), K3.to_vec()]);
    assert_eq!(calls.count(), 1);
}

/// COMPACTOR-PARITY.md §2 step 6(d) and §5 test 4: an R1 pass without work
/// (no plan, or only stale candidates) causes no evaluation; an accepted
/// plan selects an otherwise clean callback cell once, and only that cell.
#[test]
fn only_an_accepted_r1_plan_selects_a_clean_callback_cell() {
    let calls = Calls::default();
    // Keep every key, recording each input.
    let recorded = calls.clone();
    let compactor: Compactor = Box::new(move |keys| {
        recorded.count.fetch_add(1, AtomicOrder::SeqCst);
        let input: Vec<Vec<u8>> = keys.map(|key| key.to_vec()).collect();
        let retain = input
            .iter()
            .map(|key| Bytes::copy_from_slice(key))
            .collect();
        recorded.inputs.lock().push(input);
        retain
    });
    let mut world = World::new("compactor-r1-clean", Some(policy(compactor, cadence(1000))));
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, L1, b"l1")]);
    // Both cells evaluated and clean (keep-all: no mark, no debt).
    assert_eq!(world.checkpoint(true).evaluated_cells, 2);
    assert_eq!(calls.count(), 2);
    // NoWork: an empty plan.
    let (stats, prune) = r1_checkpoint(&mut world, Vec::new());
    assert_eq!((stats.relocation_cells, stats.evaluated_cells), (0, 0));
    assert_eq!(prune.plan_accepted, 0);
    // A stale candidate (older birth) adds no mark and selects nothing.
    let mut stale = r1_candidate(&world, K1);
    stale.birth += 1000;
    let (stats, prune) = r1_checkpoint(&mut world, vec![stale]);
    assert_eq!((prune.plan_accepted, prune.plan_stale), (0, 1));
    assert_eq!((stats.relocation_cells, stats.evaluated_cells), (0, 0));
    assert_eq!(calls.count(), 2);
    // An accepted removal selects the clean cell 1: one callback, on the
    // heads that remain after the removal.
    let plan = vec![r1_candidate(&world, K1)];
    let (stats, prune) = r1_checkpoint(&mut world, plan);
    assert_eq!(prune.plan_accepted, 1);
    assert_eq!(
        (
            stats.relocation_cells,
            stats.evaluated_cells,
            stats.keep_all_cells
        ),
        (1, 1, 1)
    );
    assert_eq!(calls.count(), 3);
    assert_eq!(calls.inputs.lock().last(), Some(&vec![K2.to_vec()]));
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    assert!(world.visible(L1, IndexReadCut::Latest));
}

/// COMPACTOR-PARITY.md §4 "Work": cost of one Exact image over one large
/// callback cell, in three arms on the same rows: no callback keyspace, a
/// callback keyspace that is not selected (dirt counting only), and a forced
/// evaluation with Sui's objects policy. The 1 MiB test budget would send a
/// cell this large to the capacity path, so the index budget is 256 MiB (the
/// per-cell scratch is then the 8 MiB ceiling). Run on demand:
/// `cargo test --release --locked -p tidehunter --features
/// experimental-distributed,test-utils --lib
/// compactor_policy::measure_one_large_callback_cell -- --ignored --nocapture`.
#[test]
#[ignore = "measurement; prints one JSON line per arm"]
fn measure_one_large_callback_cell() {
    const BIG_BUDGET: usize = 256 << 20;
    const OBJECTS: u32 = 16_384;
    const VERSIONS: u8 = 3;
    const BATCH: usize = 32;
    // Cell 1 of `plain`: a 3-byte object id below 2^23 with the top bit set,
    // then the version, as in Sui's `ObjectID || version` keys.
    let key = |object: u32, version: u8| {
        let id = object.to_be_bytes();
        vec![0x80 | id[1], id[2], id[3], version]
    };
    let objects = |calls: &Calls| -> Compactor {
        let calls = calls.clone();
        Box::new(move |keys| {
            calls.count.fetch_add(1, AtomicOrder::SeqCst);
            let mut retain = std::collections::HashSet::new();
            let mut last: Option<[u8; 3]> = None;
            for key in keys.rev() {
                let id = [key[0], key[1], key[2]];
                if last != Some(id) {
                    last = Some(id);
                    retain.insert(key.clone());
                }
            }
            retain
        })
    };
    let rows = OBJECTS as usize * usize::from(VERSIONS);
    for arm in ["no_callback", "not_selected", "forced"] {
        let calls = Calls::default();
        let policy = match arm {
            "no_callback" => None,
            _ => Some(policy(objects(&calls), cadence(usize::MAX))),
        };
        let mut world = World::with_budget("compactor-measure", policy, BIG_BUDGET);
        for version in 0..VERSIONS {
            let mutations: Vec<Mutation> = (0..OBJECTS)
                .map(|object| put(0, &key(object, version), b"value"))
                .collect();
            for chunk in mutations.chunks(BATCH) {
                world.write(chunk.to_vec());
            }
        }
        let started = Instant::now();
        let prepared = world.prepare(0, arm == "forced", false, None);
        let capture_ns = started.elapsed().as_nanos();
        let root = world.root();
        let started = Instant::now();
        let persisted = prepared.persist(&root).unwrap();
        let persist_ns = started.elapsed().as_nanos();
        let stats = persisted.compactor_stats;
        let image_bytes = persisted.metadata.bytes;
        let started = Instant::now();
        world.index.writer().install_image(persisted);
        let install_ns = started.elapsed().as_nanos();
        eprintln!(
            "compactor-measure {{\"arm\":\"{arm}\",\"rows\":{rows},\"capture_ns\":{capture_ns},\
             \"persist_ns\":{persist_ns},\"install_ns\":{install_ns},\"image_bytes\":{image_bytes},\
             \"evaluated_cells\":{},\"capacity_cells\":{},\"counted_cells\":{},\
             \"keys_visited\":{},\"keys_marked\":{},\"callback_ns\":{},\"scan_ns\":{},\
             \"scratch_peak_bytes\":{},\"callbacks\":{}}}",
            stats.evaluated_cells,
            stats.capacity_cells,
            stats.counted_cells,
            stats.keys_visited,
            stats.keys_marked,
            stats.callback_ns,
            stats.scan_ns,
            stats.scratch_peak_bytes,
            calls.count(),
        );
        match arm {
            "forced" => {
                assert_eq!((stats.evaluated_cells, stats.capacity_cells), (1, 0));
                assert_eq!(stats.keys_visited, rows as u64);
                assert_eq!(
                    stats.keys_marked,
                    u64::from(OBJECTS) * u64::from(VERSIONS - 1)
                );
                assert_eq!(calls.count(), 1);
                let visible = world.scan(false);
                assert_eq!(visible.len(), OBJECTS as usize);
                assert!(visible.iter().all(|key| key[3] == VERSIONS - 1));
            }
            _ => {
                assert_eq!(stats.evaluated_cells, 0);
                assert_eq!(calls.count(), 0);
                assert_eq!(world.scan(false).len(), rows);
            }
        }
    }
}
