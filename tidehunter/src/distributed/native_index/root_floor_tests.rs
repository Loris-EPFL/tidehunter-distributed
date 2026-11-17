//! The per-cell root-floor lease (option C spec §5.2, §7.2 step 4, §9.2
//! step 1): `H` advances only to `max(H, min(requested, F, oldest pin))`,
//! monotonically, never past a pinned reader and never above `F`; in-flight
//! cell captures keep the committed floor and over-retain; retained bytes
//! freed by later flushes are exact; a crash before adoption keeps the old
//! `H` and a reopen after adoption keeps the new one, both at or below `F`.
//! Thread interleavings use barriers, never sleeps.

use super::*;
use crate::distributed::native_index::cell_flush::{
    CellCapture, CellCaptureOutcome, CellInstall, NewCellBase, PreparedCell,
};
use crate::distributed::native_publication::NativePendingSink;
use crate::index::pending_table::Transaction;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
use std::sync::Barrier;
use tempdir::TempDir;

const DATABASE: [u8; 16] = [7; 16];
const EPOCH: u64 = 11;
const KS: KeySpace = KeySpace(0);
const BUDGET: usize = 16 << 20;
const SHARDS: usize = 4;
const SCRATCH: usize = 1 << 20;

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

fn new_index() -> NativeIndex {
    // Cell captures need per-cell flush state, which only `PerCell` has.
    NativeIndex::with_mode(&shape(), DATABASE, BUDGET, SHARDS, CheckpointMode::PerCell).unwrap()
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

/// The last mutation of batch `sequence`.
fn end_of(sequence: u64) -> MutationVersion {
    MutationVersion {
        batch: batch(sequence),
        operation_index: u32::MAX,
    }
}

fn at(sequence: u64) -> IndexReadCut {
    IndexReadCut::At(end_of(sequence))
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

/// Stage, promote and publish one batch of puts, as the option-D publisher.
fn publish(index: &NativeIndex, sequence: u64, keys: &[&[u8]]) {
    let mut writer = index.writer();
    let transaction = Transaction::default();
    for (operation, key) in keys.iter().enumerate() {
        let cell = cell_of(&writer, key);
        let value = reference(sequence, operation as u32, true);
        writer
            .with_pending_table(KS, &cell, |table| {
                table.insert(Bytes::copy_from_slice(key), value, None, &transaction);
            })
            .unwrap();
    }
    transaction.commit();
    writer.promote_committed().unwrap();
    writer.publish_latest();
}

fn read(index: &NativeIndex, key: &[u8], cut: IndexReadCut) -> Option<u64> {
    index
        .get(KS, key, cut)
        .unwrap()
        .map(|reference| reference.batch_version.sequence)
}

fn captured(outcome: CellCaptureOutcome) -> CellCapture {
    match outcome {
        CellCaptureOutcome::Captured(capture) => capture,
        CellCaptureOutcome::Clean { coverage, .. } => panic!("clean at {coverage:?}"),
        CellCaptureOutcome::Skipped { reason, .. } => panic!("skipped: {reason:?}"),
    }
}

fn capture(index: &NativeIndex, key: &[u8], target: PrefixCut) -> CellCapture {
    let permit = index.reserve_cell_flush(0).unwrap();
    let cell = cell_of(index, key);
    captured(
        index
            .writer()
            .capture_cell(KS, &cell, target, permit)
            .unwrap(),
    )
}

fn install(index: &NativeIndex, result: CellFlushResult) {
    match index.writer().install_cell(result) {
        CellInstall::Installed { retired, .. } => drop(retired),
        CellInstall::Stale { .. } => panic!("unexpected stale result"),
        CellInstall::Rejected { error, .. } => panic!("rejected: {error}"),
    }
}

/// Append one prepared cell to `output` and seal it: the durable base.
fn store_base(output: &mut IndexStoreWriter, prepared: &PreparedCell) -> NewCellBase {
    output.append(prepared).unwrap();
    let mut bases = Vec::new();
    output.seal(&mut bases).unwrap();
    assert_eq!(bases.len(), 1);
    bases.pop().unwrap()
}

/// Flush one cell into a durable index-store segment.
fn flush_store(index: &NativeIndex, output: &mut IndexStoreWriter, key: &[u8], target: PrefixCut) {
    let prepared = capture(index, key, target).prepare().unwrap();
    let base = store_base(output, &prepared);
    install(index, prepared.finish(base));
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

#[test]
fn root_floor_is_capped_by_f_and_pins_and_never_regresses() {
    let index = new_index();
    for sequence in 1..=6 {
        publish(&index, sequence, &[b"a1"]);
    }
    let transient = index.stats().transient_metadata_bytes;

    // A request past F stops at F, never above it.
    let lease = index.begin_root_floor(at(6), through(4)).unwrap();
    assert_eq!(
        (lease.previous(), lease.floor(), lease.advances()),
        (None, Some(end_of(4)), true)
    );
    assert_eq!(index.stats().transient_metadata_bytes, transient);
    // In progress: the committed floor is unchanged, no pin registers below
    // the reserved one, and the lease is exclusive with every other floor
    // owner.
    assert_eq!(index.retention_floor(), IndexReadCut::BeforeAll);
    assert!(matches!(
        index.pin_cut(at(3)),
        Err(NativeIndexError::RetentionInProgress)
    ));
    let held = index.pin_cut(at(4)).unwrap();
    assert!(matches!(
        index.begin_root_floor(at(6), through(6)),
        Err(NativeIndexError::CheckpointInProgress)
    ));
    assert!(matches!(
        index.capture_lease(&at(6), IndexReadCut::BeforeAll, Some(end_of(6))),
        Err(NativeIndexError::CheckpointInProgress)
    ));
    index.install_root_floor(lease).unwrap();
    assert_eq!(index.retention_floor(), at(4));
    assert!(matches!(
        index.pin_cut(at(3)),
        Err(NativeIndexError::ExpiredCut)
    ));

    // Monotonic: a lower request keeps H.
    let lease = index.begin_root_floor(at(2), through(6)).unwrap();
    assert_eq!(
        (lease.previous(), lease.floor(), lease.advances()),
        (Some(end_of(4)), Some(end_of(4)), false)
    );
    index.install_root_floor(lease).unwrap();
    assert_eq!(index.retention_floor(), at(4));
    drop(held);

    // An older pin clamps the floor; it stays valid. Inside a batch too.
    let inside = MutationVersion {
        batch: batch(5),
        operation_index: 0,
    };
    let pinned = index.pin_cut(IndexReadCut::At(inside)).unwrap();
    let lease = index.begin_root_floor(at(6), through(6)).unwrap();
    assert_eq!(lease.floor(), Some(inside));
    assert_eq!(read(&index, b"a1", pinned.cut()), Some(5));
    // Abandoned root: the committed floor stays and the reservation ends.
    drop(lease);
    assert_eq!(index.retention_floor(), at(4));
    drop(index.pin_cut(at(4)).unwrap());
    drop(pinned);

    // Refusals leave no reservation behind.
    assert!(matches!(
        index.begin_root_floor(IndexReadCut::BeforeAll, through(3)),
        Err(NativeIndexError::Invalid(_))
    ));
    assert!(matches!(
        index.begin_root_floor(IndexReadCut::Latest, through(6)),
        Err(NativeIndexError::Invalid(_))
    ));
    assert!(matches!(
        index.begin_root_floor(IndexReadCut::BeforeAll, through(9)),
        Err(NativeIndexError::UnpublishedCut)
    ));
    let other = new_index();
    publish(&other, 1, &[b"a1"]);
    let foreign = other.begin_root_floor(at(1), through(1)).unwrap();
    assert!(matches!(
        index.install_root_floor(foreign),
        Err(NativeIndexError::Invalid(_))
    ));
    assert_eq!(other.retention_floor(), IndexReadCut::BeforeAll);
    drop(other.pin_cut(IndexReadCut::BeforeAll).unwrap());
    let lease = index.begin_root_floor(at(6), through(6)).unwrap();
    assert_eq!(lease.floor(), Some(end_of(6)));
    index.install_root_floor(lease).unwrap();
    assert_eq!(index.retention_floor(), at(6));
    assert_eq!(index.stats().transient_metadata_bytes, transient);
}

#[test]
fn root_floor_waits_for_a_pinned_reader_at_each_barrier() {
    let index = new_index();
    for sequence in 1..=8 {
        publish(&index, sequence, &[b"a1"]);
    }
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| {
            let held = index.pin_cut(at(3)).unwrap();
            barrier.wait(); // 1: the reader holds 3.
            barrier.wait(); // 2: the lease reserved its floor.
            assert!(matches!(
                index.pin_cut(at(2)),
                Err(NativeIndexError::RetentionInProgress)
            ));
            let equal = index.pin_cut(at(3)).unwrap();
            assert_eq!(read(&index, b"a1", equal.cut()), Some(3));
            barrier.wait(); // 3: checked during the lease.
            barrier.wait(); // 4: installed.
            assert!(matches!(
                index.pin_cut(at(2)),
                Err(NativeIndexError::ExpiredCut)
            ));
            assert_eq!(read(&index, b"a1", held.cut()), Some(3));
            drop((held, equal));
            barrier.wait(); // 5: released.
        });
        barrier.wait(); // 1
        let lease = index.begin_root_floor(at(7), through(8)).unwrap();
        assert_eq!(lease.floor(), Some(end_of(3)), "clamped by the pin");
        barrier.wait(); // 2
        barrier.wait(); // 3
        index.install_root_floor(lease).unwrap();
        assert_eq!(index.retention_floor(), at(3));
        barrier.wait(); // 4
        barrier.wait(); // 5
        reader.join().unwrap();
    });
    let lease = index.begin_root_floor(at(7), through(8)).unwrap();
    assert_eq!(lease.floor(), Some(end_of(7)));
    index.install_root_floor(lease).unwrap();
    assert_eq!(index.retention_floor(), at(7));
}

#[test]
fn concurrent_pins_never_fall_below_the_committed_floor() {
    const READERS: u64 = 4;
    const ROUNDS: u64 = 2_000;
    const BATCHES: u64 = 16;
    let index = new_index();
    for sequence in 1..=BATCHES {
        publish(&index, sequence, &[b"a1"]);
    }
    let start = Barrier::new(READERS as usize + 1);
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..READERS)
            .map(|reader| {
                let (index, start) = (&index, &start);
                scope.spawn(move || {
                    start.wait();
                    let mut state = reader.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
                    let mut pinned = 0u64;
                    for _ in 0..ROUNDS {
                        // xorshift: a reproducible cut choice per reader.
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let cut = at(1 + state % BATCHES);
                        match index.pin_cut(cut.clone()) {
                            Ok(pin) => {
                                pinned += 1;
                                // Held: the committed floor cannot pass it.
                                assert!(position(&index.retention_floor()) <= position(&cut));
                                assert_eq!(
                                    read(index, b"a1", pin.cut()),
                                    position(&cut).map(|version| version.batch.sequence)
                                );
                                assert!(position(&index.retention_floor()) <= position(&cut));
                            }
                            Err(
                                NativeIndexError::ExpiredCut
                                | NativeIndexError::RetentionInProgress,
                            ) => {}
                            Err(error) => panic!("pin {cut:?}: {error}"),
                        }
                    }
                    pinned
                })
            })
            .collect();
        start.wait();
        for round in 0..ROUNDS {
            // The request rises slowly to the last batch.
            let requested = 1 + round * BATCHES / ROUNDS;
            let lease = index
                .begin_root_floor(at(requested), through(BATCHES))
                .unwrap();
            let floor = lease.floor();
            assert!(floor >= lease.previous() && floor <= Some(end_of(requested)));
            if round % 3 == 2 {
                drop(lease);
                continue;
            }
            index.install_root_floor(lease).unwrap();
            assert_eq!(position(&index.retention_floor()), floor);
            for pin in index.retention.pin_counts().keys() {
                assert!(*pin >= floor, "a registered pin is below the floor");
            }
        }
        let pinned: u64 = readers
            .into_iter()
            .map(|reader| reader.join().unwrap())
            .sum();
        assert!(pinned > 0, "readers never pinned");
    });
    assert_eq!(index.retention.registrations(), 0);
}

#[test]
fn in_flight_capture_over_retains_and_freed_retained_bytes_are_exact() {
    let mut index = new_index();
    publish(&index, 1, &[b"a1", b"b1"]);
    publish(&index, 2, &[b"a1", b"b1"]);
    publish(&index, 3, &[b"c1"]);
    publish(&index, 4, &[b"a1", b"b1"]);
    publish(&index, 5, &[b"c1"]);
    // Bases come from a real index store, as in a flusher: the root
    // descriptor reads each frame floor `H_i` from the base's file.
    let temp = TempDir::new("root-floor-over-retain").unwrap();
    let (index_store, _) = IndexStore::open(temp.path(), EPOCH, &[]).unwrap();
    let mut store = IndexStoreWriter::new(&index_store, 0, SCRATCH).unwrap();
    let charge = VERSION_CHARGE + 2;
    let barrier = Barrier::new(2);
    let (deltas, floors) = std::thread::scope(|scope| {
        let (index, barrier, store) = (&index, &barrier, &mut store);
        let flusher = scope.spawn(move || {
            // Captured at the committed floor, before the lease.
            let in_flight = capture(index, b"a1", through(5));
            barrier.wait(); // 1: captured.
            barrier.wait(); // 2: the lease reserved H = 3.
            let prepared = in_flight.prepare().unwrap();
            let source = prepared.encode_source();
            assert_eq!(
                source.floor,
                IndexReadCut::BeforeAll,
                "never the reserved floor"
            );
            let base = store_base(store, &prepared);
            barrier.wait(); // 3: encoded.
            barrier.wait(); // 4: H = 3 committed.
            let before = index.retained_metadata_bytes();
            install(index, prepared.finish(base));
            // Over-retains: version 1 of a1 stays below the new floor.
            let a = index.retained_metadata_bytes() as i64 - before as i64;
            // Captured after the install: compacts at H = 3.
            let before = index.retained_metadata_bytes();
            let prepared = capture(index, b"b1", through(5)).prepare().unwrap();
            assert_eq!(prepared.encode_source().floor, at(3));
            let base = store_base(store, &prepared);
            install(index, prepared.finish(base));
            let b = index.retained_metadata_bytes() as i64 - before as i64;
            let mut descriptors = Vec::with_capacity(3);
            index
                .writer()
                .capture_cell_descriptors(0..3, through(5), &mut descriptors)
                .unwrap();
            let floors: Vec<_> = descriptors
                .iter()
                .filter_map(|descriptor| {
                    let frame = descriptor.base.as_ref()?;
                    Some((descriptor.identity.1.clone(), frame.frame_floor))
                })
                .collect();
            ((a, b), floors)
        });
        barrier.wait(); // 1
        let lease = index.begin_root_floor(at(3), through(5)).unwrap();
        assert_eq!(lease.floor(), Some(end_of(3)));
        barrier.wait(); // 2
        barrier.wait(); // 3
        index.install_root_floor(lease).unwrap();
        barrier.wait(); // 4
        flusher.join().unwrap()
    });
    assert_eq!(
        deltas,
        (0, -(charge as i64)),
        "exactly version 1 of b1 freed"
    );
    let floors: BTreeMap<_, _> = floors.into_iter().collect();
    assert_eq!(
        floors[&cell_of(&index, b"a1")],
        None,
        "over-retaining frame"
    );
    assert_eq!(floors[&cell_of(&index, b"b1")], Some(end_of(3)));
    for floor in floors.values() {
        assert!(*floor <= Some(end_of(3)), "a frame floor above H");
    }
    // Reads at and above H are unchanged; below H is expired.
    for key in [b"a1", b"b1"] {
        assert_eq!(read(&index, key, at(3)), Some(2));
        assert_eq!(read(&index, key, IndexReadCut::Latest), Some(4));
        assert!(matches!(
            index.get(KS, key, at(2)),
            Err(NativeIndexError::ExpiredCut)
        ));
    }
    assert_eq!(index.stats().retained_versions, 3 + 2 + 2);
    assert_accounting_recounts(&mut index);
}

/// Capture, persist and return one root with the lease's `H`.
fn persist_root(
    index: &NativeIndex,
    dir: &Path,
    store: &IndexStore,
    generation: u64,
    requested: IndexReadCut,
) -> (RootFloorLease, CellRoot, IndexImageMetadata) {
    let mut capture = index.writer().begin_cell_root_capture();
    capture.reserve().unwrap();
    while !capture.capture_slice(&mut index.writer()).unwrap() {}
    let floor = capture.replay_floor().unwrap();
    let lease = index.begin_root_floor(requested, floor).unwrap();
    let prepared = capture
        .finish(
            index,
            dir,
            CellRootParams {
                authority_epoch: 1,
                snapshot_generation: generation,
                lifecycle_generation: 0,
                history_floor: lease.floor(),
            },
            index.published_prefix(),
            |id| store.sealed_len(id),
        )
        .unwrap();
    assert_eq!(prepared.root().header.replay_floor, floor);
    let metadata = prepared.persist(dir).unwrap();
    (lease, prepared.root().clone(), metadata)
}

/// Reopen as recovery does: fence a new store epoch, then load `root`.
fn reopen(
    dir: &Path,
    epoch: u64,
    retained: &[&[RootFile]],
    root: &CellRoot,
    metadata: IndexImageMetadata,
) -> NativeIndex {
    let (store, _) = IndexStore::open(dir, epoch, retained).unwrap();
    let name = cell_root_name(root.header.authority_epoch, root.header.snapshot_generation);
    let loaded = read_cell_root(&dir.join(name), metadata, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(&loaded, root);
    NativeIndex::open_cell_root(&loaded, dir, &shape(), DATABASE, BUDGET, SHARDS, |file| {
        store.map_sealed(file.id)
    })
    .unwrap()
}

#[test]
fn crash_before_adoption_keeps_old_h_and_reopen_after_keeps_new_h_at_or_below_f() {
    let temp = TempDir::new("root-floor-crash").unwrap();
    let dir = temp.path();
    let (store, _) = IndexStore::open(dir, EPOCH, &[]).unwrap();
    let mut output = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    let index = new_index();
    for sequence in 1..=6 {
        let keys: &[&[u8]] = match sequence % 3 {
            0 => &[b"a1", b"c1"],
            1 => &[b"a1", b"b1"],
            _ => &[b"b1", b"c1"],
        };
        publish(&index, sequence, keys);
    }
    for key in [b"a1", b"b1", b"c1"] {
        flush_store(&index, &mut output, key, through(6));
    }
    let (lease, first, first_meta) = persist_root(&index, dir, &store, 1, IndexReadCut::BeforeAll);
    assert!(!lease.advances());
    index.install_root_floor(lease).unwrap();

    publish(&index, 7, &[b"a1"]);
    publish(&index, 8, &[b"b1"]);
    for key in [b"a1", b"b1"] {
        flush_store(&index, &mut output, key, through(8));
    }
    // The pinned reader at 5 clamps the request (7) inside F (8).
    let reader = index.pin_cut(at(5)).unwrap();
    let (lease, second, second_meta) = persist_root(&index, dir, &store, 2, at(7));
    drop(reader);
    assert_eq!(lease.floor(), Some(end_of(5)));
    for root in [&first, &second] {
        let header = &root.header;
        assert!(header.replay_floor.holds_floor(header.history_floor));
        for cell in &root.cells {
            if let Some(base) = &cell.base {
                assert!(base.frame_floor <= header.history_floor);
            }
        }
    }
    assert_eq!(second.header.replay_floor, through(8));
    // The crash happens before the manifest adopts the second root: the
    // lease is never installed and the process state is gone.
    drop(lease);
    drop((index, output, store));
    let retained = [first.files.as_slice(), second.files.as_slice()];

    // Recovery from the adopted (first) root: the old H.
    let old = reopen(dir, EPOCH + 1, &retained, &first, first_meta);
    assert_eq!(old.retention_floor(), IndexReadCut::BeforeAll);
    assert_eq!(read(&old, b"a1", at(2)), Some(1));
    assert_eq!(read(&old, b"b1", at(6)), Some(5));
    drop(old.pin_cut(at(1)).unwrap());
    drop(old);

    // Recovery after the manifest adopted the second root: the new H <= F.
    let new = reopen(dir, EPOCH + 2, &retained, &second, second_meta);
    assert_eq!(new.retention_floor(), at(5));
    assert!(matches!(
        new.pin_cut(at(4)),
        Err(NativeIndexError::ExpiredCut)
    ));
    assert_eq!(read(&new, b"a1", at(5)), Some(4));
    assert_eq!(read(&new, b"a1", at(8)), Some(7));
    assert_eq!(read(&new, b"b1", at(5)), Some(5));
    assert_eq!(read(&new, b"c1", at(8)), Some(6));
    // The next root starts from the recovered H: monotonic, capped by F.
    let lease = new.begin_root_floor(at(8), through(8)).unwrap();
    assert_eq!(
        (lease.previous(), lease.floor()),
        (Some(end_of(5)), Some(end_of(8)))
    );
}
