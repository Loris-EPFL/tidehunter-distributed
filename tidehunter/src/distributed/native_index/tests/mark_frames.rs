//! Index-only prune marks (COMPACTOR-PARITY.md §3): prune plans checked on
//! the frozen image, marks stored in their cell frames, read suppression
//! without fallback, folding at the retention floor, the mark budget, read
//! views across install, stepped install and trailer validation.

use super::super::prune_marks::{
    PruneCandidate, PruneMark, decode_trailer, encode_trailer, key_charge,
};
use super::*;
use crate::distributed::native_index::{MarkBudget, PrunePlan, PruneStats};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrder};

/// Cell 1 of `plain` (first byte at or above 128).
const K1: &[u8] = &[128, 0, 0, 1];
const K2: &[u8] = &[128, 0, 0, 2];
/// Cell 0 of `plain`.
const L1: &[u8] = &[0, 0, 0, 1];
const L2: &[u8] = &[1, 0, 0, 0];

struct World {
    temp: tempdir::TempDir,
    log: PartitionLog,
    index: NativeIndex,
    next: u64,
    roots: usize,
}

impl World {
    fn new(name: &str) -> Self {
        let temp = tempdir::TempDir::new(name).unwrap();
        let log = log(&temp.path().join("wal"));
        let index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
        Self {
            temp,
            log,
            index,
            next: 0,
            roots: 0,
        }
    }

    fn write(&mut self, mutations: Vec<Mutation>) {
        let batch = batch(self.next, mutations);
        let receipt = durable(&self.log, std::slice::from_ref(&batch))[0];
        install(&self.index, &self.log, &batch, receipt);
        self.next += 1;
    }

    fn identity(&self, key: &[u8]) -> (KeySpace, CellId) {
        (
            KeySpace(0),
            self.index.descriptor(KeySpace(0)).unwrap().cell_id(key),
        )
    }

    /// A candidate for the current head of `key` in its current cell.
    fn candidate(&self, key: &[u8]) -> PruneCandidate {
        let identity = self.identity(key);
        let birth = self
            .index
            .shard_for(&identity)
            .read()
            .get(&identity)
            .unwrap()
            .birth;
        PruneCandidate {
            keyspace: identity.0,
            cell: identity.1,
            birth,
            key: Bytes::copy_from_slice(key),
            expected: self
                .index
                .get(KeySpace(0), key, IndexReadCut::Latest)
                .unwrap()
                .unwrap(),
        }
    }

    fn prepare(&mut self, floor: Option<IndexReadCut>) -> PreparedIndexImage {
        let mut writer = self.index.writer();
        let cut = self.index.call_cut();
        match floor {
            Some(floor) => writer.begin_compacted_capture(cut, floor),
            None => writer.begin_capture(cut),
        }
        .unwrap();
        writer.capture_step(usize::MAX).unwrap()
    }

    fn root(&mut self) -> PathBuf {
        self.roots += 1;
        self.temp.path().join(format!("root-{}.index", self.roots))
    }

    /// Capture, persist, adopt and install one image with `plan`.
    fn checkpoint(&mut self, plan: Vec<PruneCandidate>, floor: Option<IndexReadCut>) -> PruneStats {
        let prepared = self
            .prepare(floor)
            .with_prune_plan(PrunePlan { candidates: plan });
        let root = self.root();
        let persisted = prepared.persist(&root).unwrap();
        let stats = persisted.prune_stats;
        self.index.writer().install_image(persisted);
        stats
    }

    fn reopen(&mut self) {
        let root = self.temp.path().join(format!("root-{}.index", self.roots));
        self.index = NativeIndex::open_snapshot(&root, &shape(), DATABASE, BUDGET).unwrap();
    }

    fn visible(&self, key: &[u8], cut: IndexReadCut) -> bool {
        self.index.get(KeySpace(0), key, cut).unwrap().is_some()
    }

    fn scan(&self, reverse: bool, cut: IndexReadCut) -> Vec<Vec<u8>> {
        keys(&self.index, reverse, cut)
    }

    fn cell_frame(&self, key: &[u8]) -> (PathBuf, u64) {
        let identity = self.identity(key);
        let shard = self.index.shard_for(&identity).read();
        let file = shard
            .get(&identity)
            .and_then(|cell| cell.base.as_ref())
            .and_then(|base| base.file.as_ref())
            .expect("stored cell");
        (file.path.clone(), file.offset)
    }

    fn root_payload(&self) -> Vec<u8> {
        let root = self.temp.path().join(format!("root-{}.index", self.roots));
        std::fs::read(root).unwrap()[8..].to_vec()
    }
}

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
fn a_plan_marks_exact_heads_only_and_never_falls_back() {
    let mut world = World::new("marks-plan");
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, L1, b"l1")]);
    let k1 = world.candidate(K1);
    // K2 was planned on an older head; L1 on an older birth of its cell.
    let mut k2 = world.candidate(K2);
    world.write(vec![put(0, K2, b"k2-new")]);
    k2.expected = world
        .index
        .get(KeySpace(0), K2, cut(1, 0))
        .unwrap()
        .unwrap();
    let mut l1 = world.candidate(L1);
    l1.birth += 1000;
    let published = world.index.call_cut();
    let mut plan = vec![l1, k1, k2];
    plan.sort_by(|a, b| (a.keyspace.0, &a.cell, &a.key).cmp(&(b.keyspace.0, &b.cell, &b.key)));
    let stats = world.checkpoint(plan, None);
    assert_eq!((stats.plan_accepted, stats.plan_stale), (1, 2));
    // No user version: the published cut is unchanged.
    assert_eq!(world.index.call_cut(), published);
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    assert!(world.visible(L1, IndexReadCut::Latest));
    // A cut below the mark's cut keeps the old state; absence never falls
    // back to an older version.
    assert!(world.visible(K1, cut(0, 0)));
    assert_eq!(
        world.scan(false, IndexReadCut::Latest),
        vec![L1.to_vec(), K2.to_vec()]
    );
    assert_eq!(
        world.scan(true, IndexReadCut::Latest),
        vec![K2.to_vec(), L1.to_vec()]
    );
    // A write after the mark's cut wins over it.
    world.write(vec![put(0, K1, b"k1-new")]);
    assert!(world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn marks_live_in_their_cell_frame_and_survive_reopen() {
    let mut world = World::new("marks-frames");
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    world.write(vec![put(0, L1, b"l1")]);
    world.checkpoint(Vec::new(), None);
    let unmarked = world.cell_frame(K1);
    let k1 = world.candidate(K1);
    assert_eq!(world.checkpoint(vec![k1], None).plan_accepted, 1);
    // A new mark rewrites its cell, although the cell has no new reference.
    let marked = world.cell_frame(K1);
    assert_ne!(marked, unmarked);
    // An image that changes only cell 0 reuses the marked frame.
    world.write(vec![put(0, L2, b"l2")]);
    world.checkpoint(Vec::new(), None);
    assert_eq!(world.cell_frame(K1), marked);
    // The root ends at its empty mask section: no mark is in the root.
    assert!(world.root_payload().ends_with(&0u32.to_be_bytes()));
    world.reopen();
    assert!(!world.visible(K1, IndexReadCut::Latest));
    assert!(world.visible(K2, IndexReadCut::Latest));
    assert_eq!(
        world.scan(false, IndexReadCut::Latest),
        vec![L1.to_vec(), L2.to_vec(), K2.to_vec()]
    );
}

#[test]
fn root_size_does_not_grow_with_marks() {
    let mut world = World::new("marks-root-size");
    let keys: Vec<[u8; 4]> = (0..500u16)
        .map(|n| {
            let [high, low] = n.to_be_bytes();
            [128, 1, high, low]
        })
        .collect();
    // At most `config().batch_limits.max_operations` (32) puts per batch.
    for chunk in keys.chunks(25) {
        world.write(chunk.iter().map(|key| put(0, key, b"v")).collect());
    }
    world.checkpoint(Vec::new(), None);
    let before = world.root_payload().len();
    let plan = keys.iter().map(|key| world.candidate(key)).collect();
    assert_eq!(world.checkpoint(plan, None).plan_accepted, 500);
    assert_eq!(world.root_payload().len(), before);
}

/// Main decision (2): reads carry no mark revision. A view at a cut at or
/// above the mark's cut `C`, pinned before installation, sees the mark after
/// it, as a native reader sees the index after the flush that compacted it;
/// a view below `C` is unaffected.
#[test]
fn views_at_or_above_the_mark_cut_see_it_after_install() {
    let mut world = World::new("marks-views");
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K2, b"k2")]);
    let below = world.index.pin_cut(cut(0, 0)).unwrap();
    let at = world.index.pin_cut(IndexReadCut::Latest).unwrap();
    assert!(world.visible(K1, at.cut()));
    let k1 = world.candidate(K1);
    world.checkpoint(vec![k1], None);
    assert!(!world.visible(K1, at.cut()));
    assert_eq!(world.scan(false, at.cut()), vec![K2.to_vec()]);
    assert!(world.visible(K1, below.cut()));
    assert_eq!(world.scan(false, below.cut()), vec![K1.to_vec()]);
}

#[test]
fn stepped_install_exposes_new_marks_only_after_the_last_cell() {
    let mut world = World::new("marks-stepped");
    world.write(vec![put(0, L1, b"l1"), put(0, K1, b"k1")]);
    let plan = vec![world.candidate(L1), world.candidate(K1)];
    let prepared = world
        .prepare(None)
        .with_prune_plan(PrunePlan { candidates: plan });
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    assert_eq!(persisted.prune_stats.plan_accepted, 2);
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
fn a_floor_past_the_mark_folds_it_and_the_root_keeps_its_cut() {
    let mut world = World::new("marks-fold");
    world.write(vec![put(0, K1, b"k1")]);
    world.write(vec![put(0, K1, b"k1-b")]);
    world.write(vec![put(0, K2, b"k2")]);
    let plan = vec![world.candidate(K1), world.candidate(K2)];
    world.checkpoint(plan, None);
    // The adopting image never folds its own marks; a later one at a floor
    // below them does not either.
    assert_eq!(world.checkpoint(Vec::new(), Some(cut(1, 0))).keys_folded, 0);
    let published = world.index.call_cut();
    let stats = world.checkpoint(Vec::new(), Some(published.clone()));
    assert_eq!(stats.keys_folded, 2);
    // Every version both marks covered is gone; the cell stores no row.
    assert_eq!(world.index.stats().retained_versions, 0);
    world.reopen();
    // The published cut comes from the root, not from a stored reference.
    assert_eq!(world.index.call_cut(), published);
    assert!(world.scan(false, IndexReadCut::Latest).is_empty());
    world.write(vec![put(0, K1, b"k1-new")]);
    assert!(world.visible(K1, IndexReadCut::Latest));
}

#[test]
fn a_refused_reservation_adds_no_mark_of_the_cell() {
    let mut world = World::new("marks-budget");
    world.write(vec![put(0, K1, b"k1"), put(0, K2, b"k2")]);
    let plan = vec![world.candidate(K1), world.candidate(K2)];
    let held = Arc::new(AtomicUsize::new(0));
    let prepared = world
        .prepare(None)
        .with_prune_plan(PrunePlan {
            candidates: plan.clone(),
        })
        .with_mark_budget(Box::new(Budget {
            held: Arc::clone(&held),
            refuse: true,
        }));
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    assert_eq!(
        (
            persisted.prune_stats.plan_capacity,
            persisted.prune_stats.plan_accepted
        ),
        (2, 0)
    );
    world.index.writer().install_image(persisted);
    assert!(world.visible(K1, IndexReadCut::Latest));
    // An accepting budget holds exactly the charge of the added marks, which
    // installation then charges to the index.
    let prepared = world
        .prepare(None)
        .with_prune_plan(PrunePlan { candidates: plan })
        .with_mark_budget(Box::new(Budget {
            held: Arc::clone(&held),
            refuse: false,
        }));
    let root = world.root();
    let persisted = prepared.persist(&root).unwrap();
    assert_eq!(held.load(AtomicOrder::SeqCst), 2 * key_charge(K1.len()));
    let charged = world.index.stats().charged_metadata_bytes;
    world.index.writer().install_image(persisted);
    assert_eq!(
        world.index.stats().charged_metadata_bytes - charged,
        2 * key_charge(K1.len())
    );
}

#[test]
fn forged_mark_trailers_fail_closed() {
    let shape = shape();
    let descriptor = shape.ks(KeySpace(0));
    let cell = descriptor.cell_id(K1);
    let version = |sequence| match cut(sequence, 0) {
        IndexReadCut::At(version) => version,
        _ => unreachable!("a sequence cut is a scalar cut"),
    };
    let mark = |key: &[u8], visible: u64, through: u64| PruneMark {
        key: Bytes::copy_from_slice(key),
        visible_from: version(visible),
        suppress_through: version(through),
        epoch: 0,
    };
    let encode = |marks: Vec<PruneMark>| {
        let marks: Arc<[PruneMark]> = Arc::from(marks);
        let mut bytes = Vec::new();
        encode_trailer(Some(&marks), &mut |chunk| {
            bytes.extend_from_slice(chunk);
            Ok(())
        })
        .unwrap();
        bytes
    };
    let root_cut = cut(5, 0);
    let valid = encode(vec![mark(K1, 3, 2), mark(K1, 4, 4), mark(K2, 3, 1)]);
    let (marks, charge) = decode_trailer(&valid, descriptor, &cell, &root_cut).unwrap();
    assert_eq!(marks.len(), 3);
    assert_eq!(charge, 3 * key_charge(K1.len()));
    let mut trailing = valid.clone();
    trailing.push(0);
    let mut overcount = valid.clone();
    overcount[..4].copy_from_slice(&4u32.to_be_bytes());
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("zero count", 0u32.to_be_bytes().to_vec()),
        (
            "unsorted keys",
            encode(vec![mark(K2, 3, 1), mark(K1, 3, 1)]),
        ),
        ("duplicate", encode(vec![mark(K1, 3, 1), mark(K1, 3, 1)])),
        ("foreign cell", encode(vec![mark(L1, 3, 1)])),
        ("beyond the root cut", encode(vec![mark(K1, 6, 1)])),
        ("suppress above visible", encode(vec![mark(K1, 3, 4)])),
        ("truncated", valid[..valid.len() - 1].to_vec()),
        ("trailing byte", trailing),
        ("count above the marks", overcount),
    ];
    for (name, bytes) in cases {
        assert!(
            decode_trailer(&bytes, descriptor, &cell, &root_cut).is_err(),
            "{name}"
        );
    }
}
