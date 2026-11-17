//! Segment append, seal, mapping, open-time orphan scan and in-memory GC.
//! Raw frames use the encoder's reserve/record protocol with arbitrary
//! payloads to reach exact sizes; the end-to-end tests encode real captures.

use super::*;
use crate::distributed::native_index::cell_flush::CellCaptureOutcome;
use crate::distributed::native_publication::NativePendingSink;
use crate::index::pending_table::Transaction;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};
use std::sync::Barrier;
use tempdir::TempDir;

const DATABASE: [u8; 16] = [7; 16];
const EPOCH: u64 = 11;
const KS: KeySpace = KeySpace(0);
const SCRATCH: usize = 1 << 20;
const MIB: usize = 1 << 20;

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

fn reference(sequence: u64, operation_index: u32, put: bool) -> ValueRecordReference {
    ValueRecordReference {
        batch_id: BatchId {
            database: DATABASE,
            operation: u128::from(sequence).to_be_bytes(),
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

const CELLS: u8 = 6;
const KEYS: u8 = 4;

fn cell_of(index: &NativeIndex, key: &[u8]) -> CellId {
    index
        .descriptor(KS)
        .unwrap()
        .cell_id(&Bytes::copy_from_slice(key))
}

/// Deterministic batch `sequence`: two distinct keys, one delete in five.
fn ops(sequence: u64) -> Vec<(Vec<u8>, bool)> {
    let x = sequence.wrapping_mul(2_654_435_761);
    let first = vec![(x % 6) as u8, ((x / 6) % 4) as u8];
    let second = vec![((x / 24) % 6) as u8, ((x / 144) % 4) as u8];
    let mut ops = vec![(first, !sequence.is_multiple_of(5))];
    if ops[0].0 != second {
        ops.push((second, true));
    }
    ops
}

fn publish(index: &NativeIndex, sequence: u64) {
    let mut writer = index.writer();
    let transaction = Transaction::default();
    for (operation, (key, put)) in ops(sequence).iter().enumerate() {
        let cell = cell_of(&writer, key);
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
    writer.publish_latest();
}

/// An index with `batches` published batches over every cell.
fn index(batches: u64) -> NativeIndex {
    let index =
        NativeIndex::with_mode(&shape(), DATABASE, 16 << 20, 4, CheckpointMode::PerCell).unwrap();
    for sequence in 1..=batches {
        publish(&index, sequence);
    }
    index
}

/// An index without bases: GC sees no live index reference.
fn empty_index() -> NativeIndex {
    index(0)
}

fn cells(index: &NativeIndex) -> Vec<CellId> {
    (0..CELLS).map(|cell| cell_of(index, &[cell, 0])).collect()
}

fn prepare(index: &NativeIndex, cell: &CellId) -> PreparedCell {
    let permit = index.reserve_cell_flush(0).unwrap();
    let outcome = index
        .writer()
        .capture_cell(KS, cell, index.published_prefix(), permit)
        .unwrap();
    let CellCaptureOutcome::Captured(capture) = outcome else {
        panic!("cell not captured");
    };
    capture.prepare().unwrap()
}

/// Every visible value at the latest and two older cuts.
fn snapshot(index: &NativeIndex) -> Vec<Option<u64>> {
    let cuts = [
        IndexReadCut::Latest,
        PrefixCut::Through(BatchVersion {
            epoch: EPOCH,
            sequence: 9,
        })
        .index_cut(),
        PrefixCut::Through(BatchVersion {
            epoch: EPOCH,
            sequence: 23,
        })
        .index_cut(),
    ];
    let mut values = Vec::new();
    for cut in cuts {
        for cell in 0..CELLS {
            for key in 0..KEYS {
                values.push(
                    index
                        .get(KS, &[cell, key], cut.clone())
                        .unwrap()
                        .map(|reference| reference.batch_version.sequence),
                );
            }
        }
    }
    values
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    let hash = blake2b_simd::Params::new().hash_length(32).hash(bytes);
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.as_bytes());
    digest
}

/// Write one CRC frame of `payload` as the encoder does: reserve the framed
/// length, write header then payload, record the frame.
fn append_raw(writer: &mut IndexStoreWriter, payload: &[u8]) -> CellFrame {
    let framed = payload.len() as u64 + 8;
    let (file, offset) = writer.output.reserve(framed).unwrap();
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    header[4..].copy_from_slice(&crc32fast::hash(payload).to_be_bytes());
    file.write_all(&header).unwrap();
    file.write_all(payload).unwrap();
    let mut framed_bytes = header.to_vec();
    framed_bytes.extend_from_slice(payload);
    let frame = CellFrame {
        offset,
        bytes: framed,
        digest: digest(&framed_bytes),
        count: 1,
        retained_bytes: 0,
        retained_versions: 0,
    };
    writer
        .output
        .record(frame, IndexReadCut::BeforeAll, IndexReadCut::BeforeAll)
        .unwrap();
    frame
}

fn seal_blocks(writer: &mut IndexStoreWriter) -> Vec<Option<SortedBlock>> {
    let mut blocks = Vec::new();
    writer.output.seal_with(|block| blocks.push(block)).unwrap();
    blocks
}

fn id_of(block: &SortedBlock) -> IndexStoreFileId {
    let path = &block.file.as_ref().unwrap().path;
    IndexStoreFileId::parse_canonical(path.file_name().unwrap().to_str().unwrap()).unwrap()
}

fn segments(directory: &Path) -> BTreeSet<IndexStoreFileId> {
    std::fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            IndexStoreFileId::parse_canonical(entry.unwrap().file_name().to_str()?)
                .filter(|id| matches!(id, IndexStoreFileId::Segment { .. }))
        })
        .collect()
}

fn root_file(store: &IndexStore, id: IndexStoreFileId) -> RootFile {
    RootFile {
        id,
        name: id.canonical_name(),
        sealed_len: store.sealed_len(id).unwrap(),
    }
}

/// Authenticate the block's range on disk with the frame reader: BLAKE2b-32
/// of the framed range, CRC32 of the payload, and the mapped payload bytes.
/// Returns the payload.
fn authenticate(block: &SortedBlock) -> Bytes {
    let file = block.file.as_ref().unwrap();
    let disk = std::fs::read(&file.path).unwrap();
    let start = usize::try_from(file.offset).unwrap();
    let range = &disk[start..start + usize::try_from(file.bytes).unwrap()];
    assert_eq!(digest(range), file.digest, "frame digest");
    let payload = CrcFrame::read_from_slice(range, 0).expect("frame CRC");
    assert_eq!(payload.len() + 8, range.len(), "framed length");
    assert_eq!(payload, &block.bytes[..], "mapped payload");
    assert!(block.mapped);
    Bytes::copy_from_slice(payload)
}

fn segment_seq(id: &IndexStoreFileId) -> u64 {
    let IndexStoreFileId::Segment { segment_seq, .. } = *id else {
        panic!("not a segment");
    };
    segment_seq
}

#[test]
fn prepared_cells_seal_to_authenticated_mapped_bases_and_install() {
    let temp = TempDir::new("index-store-flush").unwrap();
    let (store, scan) = IndexStore::open(temp.path(), EPOCH, &[]).unwrap();
    assert_eq!(
        scan,
        OrphanScan {
            next_sequence: 0,
            deleted: Vec::new(),
        }
    );
    let index = index(40);
    let before = snapshot(&index);
    let target = index.published_prefix();
    // Stream 0 encodes buffered; stream 1 has no scratch and streams.
    let mut writers = [
        IndexStoreWriter::new(&store, 0, SCRATCH).unwrap(),
        IndexStoreWriter::new(&store, 1, 0).unwrap(),
    ];
    let cells = cells(&index);
    let prepared: Vec<PreparedCell> = cells.iter().map(|cell| prepare(&index, cell)).collect();
    let mut written = [0u64; 2];
    for (n, cell) in prepared.iter().enumerate() {
        let bytes = writers[n % 2].append(cell).unwrap();
        assert!(bytes > 0, "every captured cell retains a row");
        written[n % 2] += bytes;
    }
    let mut blocks: Vec<Vec<Option<SortedBlock>>> = writers.iter_mut().map(seal_blocks).collect();
    let streamed = writers[1].encoder.stats();
    assert_eq!(streamed.buffered_cells, 0);
    assert_eq!(streamed.streamed_cells, 3);
    assert_eq!(writers[0].encoder.stats().buffered_cells, 3);
    // One segment per stream, exactly the appended bytes, sealed.
    for (stream, written) in written.iter().enumerate() {
        let id = IndexStoreFileId::Segment {
            authority_epoch: EPOCH,
            stream_id: stream as u16,
            segment_seq: 0,
        };
        assert_eq!(store.sealed_len(id).unwrap(), *written);
        assert_eq!(
            std::fs::metadata(temp.path().join(id.canonical_name()))
                .unwrap()
                .len(),
            *written
        );
    }
    assert_eq!(segments(temp.path()).len(), 2);
    let descriptor = index.descriptor(KS).unwrap();
    let mut results = Vec::new();
    for (n, (prepared, cell)) in prepared.into_iter().zip(&cells).enumerate() {
        let block = blocks[n % 2].remove(0).expect("frame");
        assert_eq!(block.cut, target.index_cut());
        assert_eq!(
            block.file.as_ref().unwrap().path.parent(),
            Some(temp.path())
        );
        let payload = authenticate(&block);
        let validated = SortedBlock::validated(
            payload,
            true,
            DATABASE,
            KS,
            descriptor,
            cell,
            block.cut.clone(),
        )
        .unwrap();
        assert_eq!(validated.count, block.count);
        assert_eq!(validated.retained_bytes, block.retained_bytes);
        assert_eq!(validated.retained_versions, block.retained_versions);
        results.push(prepared.finish(NewCellBase::frame(block)));
    }
    for result in results {
        let CellInstall::Installed { retired, .. } = index.writer().install_cell(result) else {
            panic!("install refused");
        };
        drop(retired);
    }
    // The mapped bases replace the captured sources with the same rows.
    assert_eq!(snapshot(&index), before);
    // Only the bases pin the segments; no root references them.
    assert!(
        store
            .collect_garbage(&[], &index)
            .unwrap()
            .deleted
            .is_empty()
    );
    // Reloaded bases hold no mapping pin, but the index still names their
    // segments, so GC keeps them.
    index.writer().reload().unwrap();
    assert!(
        store
            .collect_garbage(&[], &index)
            .unwrap()
            .deleted
            .is_empty()
    );
    assert_eq!(segments(temp.path()).len(), 2);
    // Without that index reference, both unpinned segments are garbage.
    let sweep = store.collect_garbage(&[], &empty_index()).unwrap();
    assert_eq!(sweep.deleted.len(), 2);
}

#[test]
fn group_bounds_seal_early_and_an_oversize_frame_is_alone() {
    let temp = TempDir::new("index-store-groups").unwrap();
    let (store, _) = IndexStore::open(temp.path(), EPOCH, &[]).unwrap();
    // Cell bound: 31 frames and one empty result fill a group; the next
    // frame seals it first. Empty results keep their position.
    let mut writer = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    for n in 0..33 {
        if n == 10 {
            writer.output.push_empty().unwrap();
        }
        append_raw(&mut writer, &[n; 100]);
    }
    let blocks = seal_blocks(&mut writer);
    assert_eq!(blocks.len(), 34);
    assert!(blocks[10].is_none());
    let sequences: Vec<u64> = blocks
        .iter()
        .flatten()
        .map(|block| segment_seq(&id_of(block)))
        .collect();
    assert_eq!(sequences, [vec![0; 31], vec![1; 2]].concat());
    for (n, block) in blocks.iter().flatten().enumerate() {
        assert_eq!(authenticate(block), Bytes::from(vec![n as u8; 100]));
    }

    // Byte bound: 8 MiB fits exactly; one more frame starts a new group.
    let mut writer = IndexStoreWriter::new(&store, 1, SCRATCH).unwrap();
    for _ in 0..3 {
        append_raw(&mut writer, &vec![1; 4 * MIB - 8]);
    }
    // An oversize frame seals the open group first and forms a group alone;
    // the next small frame starts another group.
    append_raw(&mut writer, &vec![2; 9 * MIB]);
    append_raw(&mut writer, &[3; 100]);
    let blocks = seal_blocks(&mut writer);
    let ids: Vec<IndexStoreFileId> = blocks.iter().flatten().map(id_of).collect();
    let sequences: Vec<u64> = ids.iter().map(segment_seq).collect();
    assert_eq!(sequences, [0, 0, 1, 2, 3]);
    let lengths: Vec<u64> = [0, 1, 2, 3]
        .iter()
        .map(|seq| {
            store
                .sealed_len(IndexStoreFileId::Segment {
                    authority_epoch: EPOCH,
                    stream_id: 1,
                    segment_seq: *seq,
                })
                .unwrap()
        })
        .collect();
    assert_eq!(
        lengths,
        [GROUP_BYTES, 4 << 20, (9 << 20) + 8, 108],
        "at most 8 MiB per group except the lone oversize frame"
    );
    for block in blocks.iter().flatten() {
        authenticate(block);
    }
    // Without appends, a seal creates no file and emits nothing.
    assert!(seal_blocks(&mut writer).is_empty());
    assert_eq!(segments(temp.path()).len(), 6);
}

#[test]
fn names_never_alias_across_restarts_and_epochs() {
    let temp = TempDir::new("index-store-names").unwrap();
    let mut seen = BTreeSet::new();
    let (store, _) = IndexStore::open(temp.path(), 5, &[]).unwrap();
    {
        let mut first = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
        let mut second = IndexStoreWriter::new(&store, 1, SCRATCH).unwrap();
        assert!(
            IndexStoreWriter::new(&store, 0, SCRATCH).is_err(),
            "one writer per stream"
        );
        for round in 0..3 {
            append_raw(&mut first, &[round; 64]);
            append_raw(&mut second, &[round; 64]);
            for writer in [&mut first, &mut second] {
                for block in seal_blocks(writer).iter().flatten() {
                    assert!(seen.insert(id_of(block)));
                }
            }
        }
    }
    // A new writer of a stream continues its sequence.
    let mut again = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    append_raw(&mut again, &[9; 64]);
    let block = seal_blocks(&mut again).pop().flatten().unwrap();
    assert_eq!(
        id_of(&block),
        IndexStoreFileId::Segment {
            authority_epoch: 5,
            stream_id: 0,
            segment_seq: 3,
        }
    );
    seen.insert(id_of(&block));
    drop((block, again));
    let root: Vec<RootFile> = seen.iter().map(|id| root_file(&store, *id)).collect();
    drop(store);

    // A reopen in the same epoch starts above every sequence on disk.
    let (store, scan) = IndexStore::open(temp.path(), 5, &[&root]).unwrap();
    assert_eq!(scan.next_sequence, 4);
    assert!(scan.deleted.is_empty());
    let mut writer = IndexStoreWriter::new(&store, 1, SCRATCH).unwrap();
    append_raw(&mut writer, &[1; 64]);
    let block = seal_blocks(&mut writer).pop().flatten().unwrap();
    assert!(seen.insert(id_of(&block)), "new name aliases an old one");
    drop((block, writer, store));

    // A new epoch never aliases an older one.
    let (store, scan) = IndexStore::open(temp.path(), 6, &[&root]).unwrap();
    assert_eq!(scan.deleted.len(), 1, "the unadopted segment is an orphan");
    for stream in 0..3 {
        let mut writer = IndexStoreWriter::new(&store, stream, SCRATCH).unwrap();
        append_raw(&mut writer, &[2; 64]);
        let block = seal_blocks(&mut writer).pop().flatten().unwrap();
        let id = id_of(&block);
        assert!(matches!(
            id,
            IndexStoreFileId::Segment {
                authority_epoch: 6,
                segment_seq: 5,
                ..
            }
        ));
        assert!(seen.insert(id), "new name aliases an old one");
    }
    drop(store);
    // An older authority must not open newer output.
    assert!(matches!(
        IndexStore::open(temp.path(), 5, &[&root]),
        Err(NativeIndexError::Invalid(
            "index segment from a newer authority epoch"
        ))
    ));
}

#[test]
fn a_canonical_name_collision_is_refused_without_overwrite() {
    let temp = TempDir::new("index-store-collision").unwrap();
    let (store, scan) = IndexStore::open(temp.path(), EPOCH, &[]).unwrap();
    // A file that appears after the scan under the next name.
    let next = IndexStoreFileId::Segment {
        authority_epoch: EPOCH,
        stream_id: 0,
        segment_seq: scan.next_sequence,
    };
    let path = temp.path().join(next.canonical_name());
    std::fs::write(&path, b"keep").unwrap();
    let index = index(12);
    let cell = prepare(&index, &cells(&index)[0]);
    let mut writer = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    assert!(matches!(
        writer.append(&cell),
        Err(NativeIndexError::Invalid("index segment name collision"))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), b"keep");
    // Fail-stop: the writer refuses every later call and emits nothing.
    let mut bases = Vec::new();
    assert!(writer.seal(&mut bases).is_err());
    assert!(writer.append(&cell).is_err());
    assert!(bases.is_empty());
}

#[test]
fn crash_cuts_leave_orphans_that_the_next_open_deletes() {
    let temp = TempDir::new("index-store-orphans").unwrap();
    let directory = temp.path();
    let (store, _) = IndexStore::open(directory, EPOCH, &[]).unwrap();
    let mut writer = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    // Adopted: referenced by the root.
    append_raw(&mut writer, &[1; 300]);
    let adopted = seal_blocks(&mut writer).pop().flatten().unwrap();
    let root = vec![root_file(&store, id_of(&adopted))];
    // Sealed and synced, but no root adopted it.
    append_raw(&mut writer, &[2; 300]);
    let unadopted = id_of(&seal_blocks(&mut writer).pop().flatten().unwrap());
    // Dropped before seal and sync, with part of a frame written.
    let mut dropped = IndexStoreWriter::new(&store, 1, SCRATCH).unwrap();
    append_raw(&mut dropped, &vec![3; 64 << 10]);
    drop(dropped);
    // A torn frame left by a crash, with the highest sequence on disk.
    let torn = IndexStoreFileId::Segment {
        authority_epoch: EPOCH,
        stream_id: 2,
        segment_seq: 40,
    };
    std::fs::write(directory.join(torn.canonical_name()), [0, 0, 1]).unwrap();
    // Names outside the segment namespace stay.
    let foreign = ["index-store-011-0-0.cells", "index-1-2.snapshot.cells"];
    for name in foreign {
        std::fs::write(directory.join(name), b"x").unwrap();
    }
    let unsealed = IndexStoreFileId::Segment {
        authority_epoch: EPOCH,
        stream_id: 1,
        segment_seq: 0,
    };
    assert_eq!(
        segments(directory),
        BTreeSet::from([id_of(&adopted), unadopted, unsealed, torn])
    );
    drop((adopted, writer, store));

    // A root that references a missing or changed segment fails closed.
    let mut changed = root.clone();
    changed[0].sealed_len += 1;
    assert!(IndexStore::open(directory, EPOCH + 1, &[&changed]).is_err());
    let mut missing = root.clone();
    missing[0].id = IndexStoreFileId::Segment {
        authority_epoch: EPOCH,
        stream_id: 7,
        segment_seq: 1,
    };
    missing[0].name = missing[0].id.canonical_name();
    assert!(IndexStore::open(directory, EPOCH + 1, &[&missing]).is_err());
    assert_eq!(
        segments(directory).len(),
        4,
        "a refused open deletes nothing"
    );

    let (store, scan) = IndexStore::open(directory, EPOCH + 1, &[&root]).unwrap();
    assert_eq!(scan.next_sequence, 41, "orphans count in the maximum");
    assert_eq!(scan.deleted, vec![unadopted, unsealed, torn]);
    assert_eq!(segments(directory), BTreeSet::from([root[0].id]));
    for name in foreign {
        assert!(directory.join(name).exists());
    }
    // The adopted segment maps and authenticates.
    let mapped = store.map_sealed(root[0].id).unwrap();
    assert_eq!(
        digest(&mapped),
        digest(&std::fs::read(directory.join(&root[0].name)).unwrap())
    );
    let payload = CrcFrame::read_from_bytes(&mapped, 0).unwrap();
    assert_eq!(&payload[..], &[1; 300]);
    drop((mapped, store));
    // A repeated scan (a crash during cleanup) finds nothing more to delete.
    let (_, scan) = IndexStore::open(directory, EPOCH + 2, &[&root]).unwrap();
    assert!(scan.deleted.is_empty());
    assert_eq!(scan.next_sequence, 1);
}

#[test]
fn gc_never_deletes_pinned_root_referenced_or_open_segments() {
    let temp = TempDir::new("index-store-gc").unwrap();
    let directory = temp.path();
    let (store, _) = IndexStore::open(directory, EPOCH, &[]).unwrap();
    let mut writer = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    let mut seal_one = |byte: u8| {
        append_raw(&mut writer, &[byte; 200]);
        seal_blocks(&mut writer).pop().flatten().unwrap()
    };
    let live = seal_one(1);
    let rooted = seal_one(2);
    let unreferenced = seal_one(3);
    let read = seal_one(4);
    let [live_id, rooted_id, unreferenced_id, read_id] =
        [&live, &rooted, &unreferenced, &read].map(id_of);
    let root = vec![root_file(&store, rooted_id)];
    // A reader keeps only a slice of the mapping.
    let reader = read.bytes.slice(4..);
    drop((rooted, unreferenced, read));
    let mut open = IndexStoreWriter::new(&store, 1, SCRATCH).unwrap();
    append_raw(&mut open, &[5; 200]);
    let open_id = IndexStoreFileId::Segment {
        authority_epoch: EPOCH,
        stream_id: 1,
        segment_seq: 0,
    };
    assert!(store.sealed_len(open_id).is_err());
    assert!(store.map_sealed(open_id).is_err());

    let empty = empty_index();
    let sweep = store.collect_garbage(&[&root], &empty).unwrap();
    assert_eq!(
        sweep,
        GcSweep {
            deleted: vec![unreferenced_id],
            more: false,
        }
    );
    assert_eq!(
        segments(directory),
        BTreeSet::from([live_id, rooted_id, read_id, open_id])
    );
    assert!(store.map_sealed(unreferenced_id).is_err());

    drop((live, reader));
    let sweep = store.collect_garbage(&[&root], &empty).unwrap();
    assert_eq!(sweep.deleted, vec![live_id, read_id]);
    // A mapping pins a segment after its root is no longer retained.
    let mapped = store.map_sealed(rooted_id).unwrap();
    assert!(
        store
            .collect_garbage(&[], &empty)
            .unwrap()
            .deleted
            .is_empty()
    );
    drop(mapped);
    assert_eq!(
        store.collect_garbage(&[], &empty).unwrap().deleted,
        vec![rooted_id]
    );
    // The open segment is never collected; sealed and unpinned, it is.
    assert_eq!(segments(directory), BTreeSet::from([open_id]));
    drop(seal_blocks(&mut open));
    assert_eq!(
        store.collect_garbage(&[], &empty).unwrap().deleted,
        vec![open_id]
    );
    assert!(segments(directory).is_empty());
}

#[test]
fn concurrent_workers_write_separate_streams() {
    const WORKERS: u16 = 4;
    const ROUNDS: u8 = 3;
    let temp = TempDir::new("index-store-workers").unwrap();
    let (store, _) = IndexStore::open(temp.path(), EPOCH, &[]).unwrap();
    let barrier = Barrier::new(usize::from(WORKERS));
    let blocks: Vec<Vec<SortedBlock>> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..WORKERS)
            .map(|stream| {
                let (store, barrier) = (&store, &barrier);
                scope.spawn(move || {
                    let mut writer = IndexStoreWriter::new(store, stream, SCRATCH).unwrap();
                    let mut blocks = Vec::new();
                    for round in 0..ROUNDS {
                        for _ in 0..3 {
                            append_raw(&mut writer, &[stream as u8 * 16 + round; 500]);
                        }
                        // Worker 0 keeps its group open while the others seal.
                        barrier.wait();
                        if stream != 0 {
                            blocks.extend(seal_blocks(&mut writer).into_iter().flatten());
                        }
                        barrier.wait();
                        if stream == 0 {
                            blocks.extend(seal_blocks(&mut writer).into_iter().flatten());
                        }
                    }
                    blocks
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    });
    let mut all = BTreeSet::new();
    for (stream, blocks) in blocks.iter().enumerate() {
        assert_eq!(blocks.len(), 3 * usize::from(ROUNDS));
        for (n, block) in blocks.iter().enumerate() {
            let id = id_of(block);
            let IndexStoreFileId::Segment {
                stream_id,
                segment_seq,
                ..
            } = id
            else {
                panic!("not a segment");
            };
            // Each worker writes only its own stream, one segment per round.
            assert_eq!(usize::from(stream_id), stream);
            assert_eq!(segment_seq, (n / 3) as u64);
            let round = (n / 3) as u8;
            assert_eq!(
                authenticate(block),
                Bytes::from(vec![stream as u8 * 16 + round; 500])
            );
            all.insert(id);
        }
    }
    assert_eq!(all.len(), usize::from(WORKERS) * usize::from(ROUNDS));
    assert_eq!(segments(temp.path()), all);
}

#[test]
fn a_failed_sync_returns_no_result() {
    let temp = TempDir::new("index-store-failure").unwrap();
    let directory = temp.path().join("store");
    std::fs::create_dir(&directory).unwrap();
    let (store, _) = IndexStore::open(&directory, EPOCH, &[]).unwrap();
    let mut writer = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    // 33 frames: the first group is sealed inside the appends.
    for n in 0..33 {
        append_raw(&mut writer, &[n; 64]);
    }
    assert_eq!(writer.output.ready.len(), 32);
    // The directory disappears, so the directory sync of the seal fails.
    for id in segments(&directory) {
        std::fs::remove_file(directory.join(id.canonical_name())).unwrap();
    }
    std::fs::remove_dir(&directory).unwrap();
    let mut bases = Vec::new();
    assert!(matches!(
        writer.seal(&mut bases),
        Err(NativeIndexError::Io(_))
    ));
    assert!(bases.is_empty(), "the earlier sealed group is not returned");
    assert!(writer.seal(&mut bases).is_err());
    assert!(bases.is_empty());
}

#[test]
fn one_gc_sweep_deletes_a_bounded_number_of_segments() {
    let temp = TempDir::new("index-store-sweep").unwrap();
    let directory = temp.path();
    // Root segments written directly: the scan registers them as sealed.
    let root: Vec<RootFile> = (0..=GC_SWEEP_FILES as u64)
        .map(|segment_seq| {
            let id = IndexStoreFileId::Segment {
                authority_epoch: EPOCH,
                stream_id: 0,
                segment_seq,
            };
            std::fs::write(directory.join(id.canonical_name()), [0; 12]).unwrap();
            RootFile {
                id,
                name: id.canonical_name(),
                sealed_len: 12,
            }
        })
        .collect();
    let (store, scan) = IndexStore::open(directory, EPOCH + 1, &[&root]).unwrap();
    assert!(scan.deleted.is_empty());
    let empty = empty_index();
    assert!(
        store
            .collect_garbage(&[&root], &empty)
            .unwrap()
            .deleted
            .is_empty()
    );
    // The root is no longer retained: two sweeps delete every segment.
    let first = store.collect_garbage(&[], &empty).unwrap();
    assert_eq!(first.deleted.len(), GC_SWEEP_FILES);
    assert!(first.more);
    let second = store.collect_garbage(&[], &empty).unwrap();
    assert_eq!(second.deleted, vec![root[GC_SWEEP_FILES].id]);
    assert!(!second.more);
    assert!(segments(directory).is_empty());
}

#[test]
fn an_unlink_failure_returns_unremoved_segments_to_sealed() {
    let temp = TempDir::new("index-store-unlink").unwrap();
    let directory = temp.path();
    let (store, _) = IndexStore::open(directory, EPOCH, &[]).unwrap();
    let mut writer = IndexStoreWriter::new(&store, 0, SCRATCH).unwrap();
    let ids: Vec<IndexStoreFileId> = (0..3)
        .map(|byte| {
            append_raw(&mut writer, &[byte; 64]);
            id_of(&seal_blocks(&mut writer).pop().flatten().unwrap())
        })
        .collect();
    let path = |id: IndexStoreFileId| directory.join(id.canonical_name());
    // The first segment is already absent; the second cannot be unlinked
    // because a non-empty directory replaced it.
    std::fs::remove_file(path(ids[0])).unwrap();
    std::fs::remove_file(path(ids[1])).unwrap();
    std::fs::create_dir(path(ids[1])).unwrap();
    std::fs::write(path(ids[1]).join("x"), b"x").unwrap();
    let empty = empty_index();
    assert!(store.collect_garbage(&[], &empty).is_err());
    assert!(
        store.sealed_len(ids[0]).is_err(),
        "an absent segment is done"
    );
    for id in &ids[1..] {
        assert_eq!(store.sealed_len(*id).unwrap(), 72, "returned to sealed");
    }
    std::fs::remove_dir_all(path(ids[1])).unwrap();
    let sweep = store.collect_garbage(&[], &empty).unwrap();
    assert_eq!(sweep.deleted, ids[1..]);
    assert!(segments(directory).is_empty());
}
