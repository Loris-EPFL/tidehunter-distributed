//! Sharded index: independent progress, atomic batch visibility, shard-count
//! independence of persisted state and capture results.

use super::*;
use crate::distributed::native_publication::NativePendingSink;
use crate::distributed::test_bounds::PROGRESS_CAP;
use crate::index::pending_table::Transaction;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};

/// Keyspace 2 is `prefix`: variable-length keys routed by their first byte, so
/// 256 distinct cells spread over the shards.
const PREFIX: u8 = 2;

/// Runs `work` on another thread and fails the test instead of hanging when it
/// does not finish: that means it blocked on a lock the caller holds.
fn finishes_within_deadline<R: Send + 'static>(work: impl FnOnce() -> R + Send + 'static) -> R {
    let (done, result) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = done.send(work());
    });
    let value = result
        .recv_timeout(Duration::from_secs(20))
        .expect("operation blocked on a lock held by the test");
    worker.join().unwrap();
    value
}

fn synthetic(sequence: u64, operation_index: u32) -> ValueRecordReference {
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
            key_len: 2,
            value_offset: 8,
            value_len: 8,
            checksum: MemberChecksum(0),
        },
    }
}

fn key_for(first: u8) -> Vec<u8> {
    vec![first, 1]
}

fn shard_of_key(index: &NativeIndex, key: &[u8]) -> usize {
    let (_, _, cell) = index.route(KeySpace(PREFIX), key).unwrap();
    index.shard_of_cell(KeySpace(PREFIX), &cell)
}

/// Stage and commit one synthetic batch writing `keys`, without promoting it.
fn stage_batch(index: &NativeIndex, sequence: u64, keys: &[Vec<u8>]) {
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
                    synthetic(sequence, operation as u32),
                    None,
                    &transaction,
                );
            })
            .unwrap();
    }
    transaction.commit();
}

fn apply_batch(index: &NativeIndex, sequence: u64, keys: &[Vec<u8>]) {
    stage_batch(index, sequence, keys);
    index.writer().promote_committed().unwrap();
}

fn publish_batch(index: &NativeIndex, sequence: u64, keys: &[Vec<u8>]) {
    apply_batch(index, sequence, keys);
    index.writer().publish_latest();
}

fn read_sequence(index: &NativeIndex, key: &[u8], cut: IndexReadCut) -> Option<u64> {
    index
        .get(KeySpace(PREFIX), key, cut)
        .unwrap()
        .map(|reference| reference.batch_version.sequence)
}

/// First bytes whose cells land in at least `wanted` distinct shards, `count`
/// keys in total.
fn keys_spanning_shards(index: &NativeIndex, count: u8, wanted: usize) -> Vec<Vec<u8>> {
    let keys: Vec<_> = (0..count).map(key_for).collect();
    let shards: HashSet<_> = keys.iter().map(|key| shard_of_key(index, key)).collect();
    assert!(
        shards.len() >= wanted,
        "workload spans {} shards, wanted {wanted}",
        shards.len()
    );
    keys
}

#[test]
fn readers_and_publishers_on_other_shards_never_wait_for_a_busy_shard() {
    let index = Arc::new(NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 8).unwrap());
    let keys = keys_spanning_shards(&index, 32, 4);
    let busy = shard_of_key(&index, &keys[0]);
    let (busy_keys, other_keys): (Vec<_>, Vec<_>) = keys
        .iter()
        .cloned()
        .partition(|key| shard_of_key(&index, key) == busy);
    let other = shard_of_key(&index, &other_keys[0]);
    let same_other: Vec<_> = other_keys
        .iter()
        .filter(|key| shard_of_key(&index, key) == other)
        .cloned()
        .collect();
    publish_batch(&index, 1, &keys);

    // A publication holds `busy` exclusively. Readers of every other shard
    // and a publisher that touches only another shard proceed.
    let held = index.shards[busy].write();
    let worker = Arc::clone(&index);
    let (first_other, first_same_other) = (other_keys[0].clone(), same_other.clone());
    finishes_within_deadline(move || {
        assert_eq!(
            read_sequence(&worker, &first_other, IndexReadCut::Latest),
            Some(1)
        );
        publish_batch(&worker, 2, &first_same_other);
        assert_eq!(
            read_sequence(&worker, &first_same_other[0], IndexReadCut::Latest),
            Some(2)
        );
        assert!(worker.pin_cut(IndexReadCut::Latest).is_ok());
    });
    drop(held);

    // A reader holds `other` shared (a lookup in progress). A publication that
    // touches only `busy` does not wait for it, and neither does a reader.
    let held = index.shards[other].read();
    let worker = Arc::clone(&index);
    let busy_keys_for_worker = busy_keys.clone();
    finishes_within_deadline(move || {
        publish_batch(&worker, 3, &busy_keys_for_worker);
        assert_eq!(
            read_sequence(&worker, &busy_keys_for_worker[0], IndexReadCut::Latest),
            Some(3)
        );
    });
    drop(held);
    assert_eq!(
        read_sequence(&index, &same_other[0], IndexReadCut::Latest),
        Some(2)
    );
    assert_eq!(
        read_sequence(&index, &busy_keys[0], IndexReadCut::Latest),
        Some(3)
    );
}

#[test]
fn readers_never_take_the_writer_mutex() {
    let index = Arc::new(NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 4).unwrap());
    let keys = keys_spanning_shards(&index, 8, 2);
    publish_batch(&index, 1, &keys);
    // The publisher/maintenance handle is held; no reader path may block on it.
    let writer = index.writer();
    let worker = Arc::clone(&index);
    finishes_within_deadline(move || {
        for key in &keys {
            assert_eq!(read_sequence(&worker, key, IndexReadCut::Latest), Some(1));
        }
        let cut = worker.call_cut();
        let _pin = worker.pin_cut(cut.clone()).unwrap();
        worker.validate_cut(cut).unwrap();
        assert!(worker.stats().cells >= 2);
        assert!(worker.try_stats().is_some());
        assert_eq!(worker.shard_stats().shards.len(), 4);
        let scan = worker
            .scan_reduced(KeySpace(PREFIX), None, false, 256, IndexReadCut::Latest)
            .unwrap();
        assert_eq!(scan.len(), 8);
    });
    drop(writer);
}

#[test]
fn atomic_batch_spanning_shards_is_never_partially_visible_to_a_cut_reader() {
    // 400 batches of 16 retained versions each need more than the default budget.
    let index =
        Arc::new(NativeIndex::with_shards(&shape(), DATABASE, 16 * 1024 * 1024, 8).unwrap());
    let keys = keys_spanning_shards(&index, 16, 3);
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let index = Arc::clone(&index);
            let keys = keys.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                let mut observed = 0u64;
                while !done.load(AtomicOrdering::Acquire) {
                    // One cut, then every key at that cut: a batch is entirely
                    // inside it or entirely outside it.
                    let cut = index.call_cut();
                    let versions: Vec<_> = keys
                        .iter()
                        .map(|key| read_sequence(&index, key, cut.clone()))
                        .collect();
                    assert!(
                        versions.windows(2).all(|pair| pair[0] == pair[1]),
                        "partially visible batch at {cut:?}: {versions:?}"
                    );
                    observed += 1;
                }
                observed
            })
        })
        .collect();
    for sequence in 1..=400 {
        publish_batch(&index, sequence, &keys);
    }
    done.store(true, AtomicOrdering::Release);
    let observed: u64 = readers
        .into_iter()
        .map(|reader| reader.join().unwrap())
        .sum();
    assert!(observed > 0);
    for key in &keys {
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(400));
    }
}

#[test]
fn partially_applied_batch_is_invisible_at_the_watermark_until_publication() {
    let index = Arc::new(NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 8).unwrap());
    let keys = keys_spanning_shards(&index, 16, 3);
    publish_batch(&index, 1, &keys);
    let before = index.call_cut();

    // Stage batch 2 first, then block its apply at the highest shard. Apply
    // order is ascending shard index, so lower shards are applied by the time
    // the publisher waits.
    stage_batch(&index, 2, &keys);
    let highest = keys
        .iter()
        .map(|key| shard_of_key(&index, key))
        .max()
        .unwrap();
    let held = index.shards[highest].write();
    let acquisitions = index.shards[highest].stats().acquisitions;
    let publisher = {
        let index = Arc::clone(&index);
        std::thread::spawn(move || index.writer().promote_committed().unwrap())
    };
    // Progress wait: the publisher's blocked entry into the held shard is
    // counted; only a dead publisher exhausts the cap.
    let blocked = Instant::now() + PROGRESS_CAP;
    while index.shards[highest].stats().acquisitions == acquisitions {
        assert!(
            Instant::now() < blocked,
            "publisher never reached the busy shard"
        );
        std::thread::yield_now();
    }
    // Floor after the observed block: the publisher is parked on the held
    // shard, so load only lengthens the wait; it cannot finish early.
    std::thread::sleep(Duration::from_millis(20));
    assert!(!publisher.is_finished(), "apply waits for the busy shard");

    // The apply is physically partial. Inspect the shards directly: lower
    // shards already hold the new version, the blocked shard does not.
    let version_two = IndexReadCut::At(MutationVersion {
        batch: BatchVersion {
            epoch: EPOCH,
            sequence: 2,
        },
        operation_index: u32::MAX,
    });
    let mut applied = HashSet::new();
    for key in &keys {
        let shard = shard_of_key(&index, key);
        if shard == highest {
            continue;
        }
        let (_, reduced, cell) = index.route(KeySpace(PREFIX), key).unwrap();
        let cells = index.shards[shard].read();
        let physical = cells[&(KeySpace(PREFIX), cell)]
            .get(&reduced, &version_two)
            .map(|reference| reference.batch_version.sequence);
        applied.insert(physical);
    }
    assert_eq!(
        applied,
        HashSet::from([Some(2)]),
        "lower shards already hold the new version"
    );

    // A public explicit `At` cut naming the half-applied batch must not expose
    // it: every entry point rejects it, whichever shard the key lives on.
    for key in &keys {
        assert!(matches!(
            index.get(KeySpace(PREFIX), key, version_two.clone()),
            Err(NativeIndexError::UnpublishedCut)
        ));
    }
    assert!(matches!(
        index.scan_reduced(KeySpace(PREFIX), None, false, 256, version_two.clone()),
        Err(NativeIndexError::UnpublishedCut)
    ));
    assert!(matches!(
        index.pin_cut(version_two.clone()),
        Err(NativeIndexError::UnpublishedCut)
    ));
    assert!(matches!(
        index.validate_cut(version_two.clone()),
        Err(NativeIndexError::UnpublishedCut)
    ));

    // Every reader of the watermark still sees one consistent snapshot.
    assert_eq!(index.call_cut(), before);
    for key in &keys {
        if shard_of_key(&index, key) != highest {
            assert_eq!(
                read_sequence(&index, key, IndexReadCut::Latest),
                Some(1),
                "watermark readers cannot see the partial apply"
            );
        }
    }

    drop(held);
    assert_eq!(publisher.join().unwrap(), keys.len());
    // Applied everywhere, still unpublished: nothing changed for any reader,
    // and the explicit cut that names the unpublished batch is still rejected.
    for key in &keys {
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(1));
        assert!(matches!(
            index.get(KeySpace(PREFIX), key, version_two.clone()),
            Err(NativeIndexError::UnpublishedCut)
        ));
    }
    // The publisher held the writer mutex while blocked, so maintenance is
    // checked only now.
    assert!(matches!(
        index.writer().begin_capture(version_two),
        Err(NativeIndexError::UnpublishedCut)
    ));
    index.writer().publish_latest();
    for key in &keys {
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(2));
        assert_eq!(read_sequence(&index, key, before.clone()), Some(1));
    }
}

#[test]
fn explicit_cuts_cannot_reach_an_unpublished_late_older_batch() {
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 8).unwrap();
    let keys = keys_spanning_shards(&index, 8, 2);
    let at = |sequence: u64| {
        IndexReadCut::At(MutationVersion {
            batch: BatchVersion {
                epoch: EPOCH,
                sequence,
            },
            operation_index: u32::MAX,
        })
    };
    publish_batch(&index, 1, &keys);
    // Batch 3 publishes while the older batch 2 is applied but unpublished.
    apply_batch(&index, 3, &keys);
    apply_batch(&index, 2, &keys);
    let upper = index.writer.lock().latest.unwrap();
    index
        .writer()
        .install_live_view(
            upper,
            at(1),
            vec![BatchVersion {
                epoch: EPOCH,
                sequence: 2,
            }],
        )
        .unwrap();
    for key in &keys {
        // The watermark skips the hole: batch 3 over batch 1.
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(3));
        assert_eq!(read_sequence(&index, key, at(1)), Some(1));
        // Explicit cuts that include the unpublished batch are rejected, even
        // though its entries are already in the shards.
        for sequence in [2, 3] {
            assert!(matches!(
                index.get(KeySpace(PREFIX), key, at(sequence)),
                Err(NativeIndexError::UnpublishedCut)
            ));
        }
    }
    // Closing the hole publishes batch 2; its cut is then valid.
    index
        .writer()
        .install_live_view(upper, IndexReadCut::At(upper), vec![])
        .unwrap();
    for key in &keys {
        assert_eq!(read_sequence(&index, key, at(2)), Some(2));
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(3));
    }
}

#[test]
fn publication_histogram_counts_shards_touched_per_publication() {
    assert_eq!(PublicationHistogram::bucket(0), 0);
    assert_eq!(PublicationHistogram::bucket(1), 0);
    assert_eq!(PublicationHistogram::bucket(2), 1);
    assert_eq!(PublicationHistogram::bucket(3), 2);
    assert_eq!(PublicationHistogram::bucket(4), 2);
    assert_eq!(PublicationHistogram::bucket(5), 3);
    assert_eq!(PublicationHistogram::bucket(64), 6);
    assert_eq!(PublicationHistogram::bucket(65), 7);
    assert_eq!(
        PublicationHistogram::bucket(MAX_INDEX_SHARDS),
        PUBLICATION_HISTOGRAM_BUCKETS - 1
    );

    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 8).unwrap();
    let keys = keys_spanning_shards(&index, 16, 3);
    publish_batch(&index, 1, &keys[..1]);
    publish_batch(&index, 2, &keys);
    let stats = index.shard_stats();
    let touched = keys
        .iter()
        .map(|key| shard_of_key(&index, key))
        .collect::<HashSet<_>>()
        .len();
    assert_eq!(stats.publication_shards_touched[0], 1);
    assert_eq!(
        stats.publication_shards_touched[PublicationHistogram::bucket(touched)],
        1
    );
    assert_eq!(stats.publication_shards_touched.iter().sum::<u64>(), 2);
    assert_eq!(stats.shards.len(), 8);
    assert_eq!(
        stats.shards.iter().map(|shard| shard.cells).sum::<usize>(),
        16
    );
    assert!(
        stats
            .shards
            .iter()
            .all(|shard| shard.locks.acquisitions > 0 || shard.cells == 0)
    );
    assert!(
        stats
            .shards
            .iter()
            .any(|shard| shard.locks.hold_samples > 0),
        "write holds are always sampled"
    );
}

#[test]
fn shard_count_is_validated_in_memory_state_only() {
    for bad in [0usize, 3, 48, MAX_INDEX_SHARDS * 2] {
        assert!(matches!(
            NativeIndex::with_shards(&shape(), DATABASE, BUDGET, bad),
            Err(NativeIndexError::Invalid(_))
        ));
    }
    for good in [1usize, 2, 64, MAX_INDEX_SHARDS] {
        let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, good).unwrap();
        assert_eq!(index.shard_count(), good);
    }
    assert_eq!(
        NativeIndex::new(&shape(), DATABASE, BUDGET)
            .unwrap()
            .shard_count(),
        DEFAULT_INDEX_SHARDS
    );
}

struct CaptureOutcome {
    first: (u64, [u8; 32]),
    second: (u64, [u8; 32]),
    first_container: Vec<u8>,
    second_container: Vec<u8>,
    latest_rows: Vec<(Vec<u8>, MutationVersion)>,
    cut_rows: Vec<(Vec<u8>, MutationVersion)>,
    stats: (usize, usize, usize, usize),
    floor: IndexReadCut,
}

fn rows(index: &NativeIndex, keyspace: u8, cut: IndexReadCut) -> Vec<(Vec<u8>, MutationVersion)> {
    let mut rows: Vec<_> = index
        .scan_reduced(KeySpace(keyspace), None, false, usize::MAX, cut)
        .unwrap()
        .into_iter()
        .map(|(key, reference)| (key.to_vec(), reference.mutation_version()))
        .collect();
    rows.sort();
    rows
}

/// One fixed workload, run with `shards` shards in `dir`: publications that
/// interleave with a stepped capture, an installation, and a second capture.
fn capture_workload(shards: usize, dir: &Path) -> CaptureOutcome {
    let log = log(&dir.join("wal"));
    let prefix_puts = |range: std::ops::Range<u8>, value: &[u8]| -> Vec<Mutation> {
        range.map(|n| put(PREFIX, &[n, 7], value)).collect()
    };
    let mut first = prefix_puts(0..9, b"one");
    first.push(put(0, A, b"a1"));
    let mut second = prefix_puts(5..12, b"two");
    second.push(delete(A));
    second.push(put(0, B, b"b2"));
    let third = prefix_puts(10..19, b"three");
    let fourth = prefix_puts(0..4, b"four");
    let batches = [
        batch(0, first),
        batch(1, second),
        batch(2, third),
        batch(3, fourth),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, shards).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);

    index.writer().begin_capture(cut(1, u32::MAX)).unwrap();
    assert!(index.writer().capture_step(3).is_none());
    // Published while the capture is stepping: must stay out of this image.
    install(&index, &log, &batches[2], receipts[2]);
    let prepared = loop {
        if let Some(prepared) = index.writer().capture_step(3) {
            break prepared;
        }
    };
    let persisted = prepared.persist(&dir.join("capture.index")).unwrap();
    let first_metadata = persisted.metadata;
    index.writer().install_image(persisted);
    install(&index, &log, &batches[3], receipts[3]);

    let persisted = index
        .writer()
        .prepare_image(IndexReadCut::Latest)
        .unwrap()
        .persist(&dir.join("second.index"))
        .unwrap();
    let second_metadata = persisted.metadata;
    index.writer().install_image(persisted);

    let stats = index.stats();
    CaptureOutcome {
        first: (first_metadata.bytes, first_metadata.digest),
        second: (second_metadata.bytes, second_metadata.digest),
        first_container: std::fs::read(dir.join("capture.index.cells")).unwrap(),
        second_container: std::fs::read(dir.join("second.index.cells")).unwrap(),
        latest_rows: [0, 1, PREFIX]
            .into_iter()
            .flat_map(|keyspace| rows(&index, keyspace, IndexReadCut::Latest))
            .collect(),
        cut_rows: [0, 1, PREFIX]
            .into_iter()
            .flat_map(|keyspace| rows(&index, keyspace, cut(1, u32::MAX)))
            .collect(),
        stats: (
            stats.cells,
            stats.retained_versions,
            stats.charged_metadata_bytes,
            stats.transient_metadata_bytes,
        ),
        floor: index.retention_floor(),
    }
}

#[test]
fn checkpoint_capture_is_identical_for_every_shard_count() {
    // One shard is exactly the former single-table layout.
    let baseline_dir = tempdir::TempDir::new("index-capture-one-shard").unwrap();
    let baseline = capture_workload(1, baseline_dir.path());
    assert!(baseline.stats.0 > 10, "workload spans many cells");
    for shards in [2, 8, 64] {
        let dir = tempdir::TempDir::new("index-capture-many-shards").unwrap();
        let sharded = capture_workload(shards, dir.path());
        assert_eq!(sharded.first, baseline.first, "{shards} shards: root");
        assert_eq!(
            sharded.second, baseline.second,
            "{shards} shards: second root"
        );
        assert_eq!(
            sharded.first_container, baseline.first_container,
            "{shards} shards: container bytes"
        );
        assert_eq!(
            sharded.second_container, baseline.second_container,
            "{shards} shards: second container bytes"
        );
        assert_eq!(sharded.latest_rows, baseline.latest_rows);
        assert_eq!(sharded.cut_rows, baseline.cut_rows);
        assert_eq!(sharded.stats, baseline.stats);
        assert_eq!(sharded.floor, baseline.floor);
    }
}

#[test]
fn any_shard_count_opens_any_snapshot() {
    let temp = tempdir::TempDir::new("index-reshard-open").unwrap();
    let log = log(&temp.path().join("wal"));
    let batches = [
        batch(
            0,
            (0..12)
                .map(|n| put(PREFIX, &[n, 3], b"value"))
                .collect::<Vec<_>>(),
        ),
        batch(1, vec![put(0, A, b"a"), put(0, B, b"b")]),
    ];
    let receipts = durable(&log, &batches);
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 64).unwrap();
    install(&index, &log, &batches[0], receipts[0]);
    install(&index, &log, &batches[1], receipts[1]);
    let root = temp.path().join("reshard.index");
    index
        .writer()
        .flush_and_unload(&root, IndexReadCut::Latest)
        .unwrap();
    let expected: Vec<_> = [0, PREFIX]
        .into_iter()
        .flat_map(|keyspace| rows(&index, keyspace, IndexReadCut::Latest))
        .collect();
    for shards in [1, 4, 16, 256] {
        let reopened =
            NativeIndex::open_snapshot_with_shards(&root, &shape(), DATABASE, BUDGET, shards)
                .unwrap();
        assert_eq!(reopened.shard_count(), shards);
        let actual: Vec<_> = [0, PREFIX]
            .into_iter()
            .flat_map(|keyspace| rows(&reopened, keyspace, IndexReadCut::Latest))
            .collect();
        assert_eq!(actual, expected, "{shards} shards");
        assert_eq!(reopened.stats().cells, index.stats().cells);
        assert_eq!(reopened.call_cut(), index.call_cut());
    }
}

/// The last operation of a 16-key batch (also inside an 8-key batch's range).
fn batch_cut(sequence: u64) -> IndexReadCut {
    IndexReadCut::At(MutationVersion {
        batch: BatchVersion {
            epoch: EPOCH,
            sequence,
        },
        operation_index: 15,
    })
}

#[test]
fn empty_exclusion_view_publishes_upper_not_the_newest_promoted_batch() {
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 8).unwrap();
    let keys = keys_spanning_shards(&index, 8, 2);
    publish_batch(&index, 1, &keys);
    // Batch 2 is promoted (latest) but not published.
    apply_batch(&index, 2, &keys);
    let upper = MutationVersion {
        batch: BatchVersion {
            epoch: EPOCH,
            sequence: 1,
        },
        operation_index: keys.len() as u32 - 1,
    };
    index
        .writer()
        .install_live_view(upper, IndexReadCut::At(upper), vec![])
        .unwrap();
    assert_eq!(index.call_cut(), IndexReadCut::At(upper));
    for key in &keys {
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(1));
        assert!(matches!(
            index.get(KeySpace(PREFIX), key, batch_cut(2)),
            Err(NativeIndexError::UnpublishedCut)
        ));
    }
    index.writer().publish_latest();
    for key in &keys {
        assert_eq!(read_sequence(&index, key, IndexReadCut::Latest), Some(2));
    }
}

#[test]
fn public_scan_pins_its_cut_against_a_concurrent_checkpoint_floor() {
    let index = Arc::new(NativeIndex::with_shards(&shape(), DATABASE, BUDGET, 8).unwrap());
    let keys = keys_spanning_shards(&index, 16, 3);
    publish_batch(&index, 1, &keys);
    publish_batch(&index, 2, &keys);

    // Control: without a pin, a capture at batch 2 holds back reads of batch 1.
    index
        .writer()
        .begin_compacted_capture(batch_cut(2), batch_cut(2))
        .unwrap();
    assert!(matches!(
        index.validate_cut(batch_cut(1)),
        Err(NativeIndexError::RetentionInProgress)
    ));
    index.writer().abort_capture();
    assert!(index.validate_cut(batch_cut(1)).is_ok());

    // Stall a scan of batch 1 inside its traversal, on the last shard.
    let last = index.shard_count() - 1;
    let held = index.shards[last].write();
    let acquisitions = index.shards[last].stats().acquisitions;
    let scanner = {
        let index = Arc::clone(&index);
        std::thread::spawn(move || {
            index
                .scan(KeySpace(PREFIX), None, false, 100, batch_cut(1))
                .unwrap()
        })
    };
    // Progress wait: the scanner's blocked entry into the held shard is
    // counted; only a dead scanner exhausts the cap.
    let blocked = Instant::now() + PROGRESS_CAP;
    while index.shards[last].stats().acquisitions == acquisitions {
        assert!(
            Instant::now() < blocked,
            "scan never reached the busy shard"
        );
        std::thread::yield_now();
    }
    // Floor after the observed block: the scan is parked inside its traversal
    // on the held shard, so load only lengthens the wait.
    std::thread::sleep(Duration::from_millis(20));
    assert!(!scanner.is_finished(), "scan waits inside its traversal");

    // A newer checkpoint is selected meanwhile. The scan's pin clamps its
    // floor, so batch 1 stays readable instead of being compacted away.
    index
        .writer()
        .begin_compacted_capture(batch_cut(2), batch_cut(2))
        .unwrap();
    assert!(index.validate_cut(batch_cut(1)).is_ok());
    index.writer().abort_capture();

    drop(held);
    let rows = scanner.join().unwrap();
    assert_eq!(
        rows.len(),
        keys.len(),
        "complete result of the original cut"
    );
    assert!(
        rows.iter()
            .all(|(_, reference)| reference.batch_version.sequence == 1)
    );
    assert_eq!(index.retention.registrations(), 0, "the pin is released");
}
