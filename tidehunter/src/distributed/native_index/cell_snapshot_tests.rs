//! Root format 5: golden layout, codec, bounded capture, persistence crash
//! cuts and the eager loader's rejection of corrupt or inconsistent roots
//! (checkpoint option C spec §7, §8.1, §10.2, §11.3). Frames are real cell
//! frames written by the exact-mode encoder into legacy containers.

use super::*;
use crate::distributed::native_publication::NativePendingSink;
use crate::index::pending_table::Transaction;
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig};

const DATABASE: [u8; 16] = [7; 16];
const EPOCH: u64 = 11;
const KS: KeySpace = KeySpace(0);
const BUDGET: usize = 16 << 20;
const SHARDS: usize = 4;

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

/// Stage, promote and publish one batch, as the option-D publisher does.
fn publish(index: &NativeIndex, sequence: u64, ops: &[(&[u8], bool)]) {
    let mut writer = index.writer();
    let transaction = Transaction::default();
    for (operation, (key, put)) in ops.iter().enumerate() {
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

fn read(index: &NativeIndex, key: &[u8], cut: IndexReadCut) -> Option<(u64, RecordKind)> {
    index
        .get(KS, key, cut)
        .unwrap()
        .map(|reference| (reference.batch_version.sequence, reference.descriptor.kind))
}

fn params(generation: u64, history_floor: Option<MutationVersion>) -> CellRootParams {
    CellRootParams {
        authority_epoch: 1,
        snapshot_generation: generation,
        lifecycle_generation: 0,
        history_floor,
    }
}

fn stat_len(dir: &Path) -> impl FnMut(IndexStoreFileId) -> Result<u64, NativeIndexError> + '_ {
    move |id| Ok(std::fs::metadata(dir.join(id.canonical_name()))?.len())
}

/// Capture a whole root in slices, as the snapshot actor does.
fn capture(
    index: &NativeIndex,
    dir: &Path,
    generation: u64,
    history_floor: Option<MutationVersion>,
) -> PreparedCellRoot {
    let mut capture = index.writer().begin_cell_root_capture();
    capture.reserve().unwrap();
    let slices = capture.end / ROOT_DESCRIPTOR_SLICE + 1;
    assert!((0..slices).any(|_| capture.capture_slice(&mut index.writer()).unwrap()));
    capture
        .finish(
            index,
            dir,
            params(generation, history_floor),
            index.published_prefix(),
            stat_len(dir),
        )
        .unwrap()
}

const KEYS: [&[u8]; 6] = [b"a1", b"a2", b"b1", b"b2", b"c1", b"d1"];
/// History floor of the fixture frames: batch 4 complete.
const FRAME_FLOOR: MutationVersion = MutationVersion {
    batch: BatchVersion {
        epoch: EPOCH,
        sequence: 4,
    },
    operation_index: u32::MAX,
};

/// Batches 1-6 in a legacy container `index-1-1.snapshot.cells` (frame cut
/// 6, floor 4), reopened from its format-4 root, every cell clean-advanced
/// to 6, new cells start at 6, then batch 7 dirties cell `a` and creates
/// cell `e` without a frame.
fn fixture(dir: &Path) -> NativeIndex {
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, SHARDS).unwrap();
    publish(
        &index,
        1,
        &[(b"a1", true), (b"b1", true), (b"c1", true), (b"d1", true)],
    );
    publish(&index, 2, &[(b"a1", true), (b"a2", true)]);
    publish(&index, 3, &[(b"b1", false), (b"c1", true)]);
    publish(&index, 4, &[(b"a1", true)]);
    publish(&index, 5, &[(b"d1", true), (b"a2", false)]);
    publish(&index, 6, &[(b"c1", true), (b"b2", true)]);
    index.retention.set_floor(IndexReadCut::At(FRAME_FLOOR));
    index
        .writer()
        .flush_and_unload(&dir.join("index-1-1.snapshot"), IndexReadCut::Latest)
        .unwrap();
    // Legacy migration: open the exact root, then a capture at the old cut
    // raises every cell's coverage to it (C2 report, risk 4). The root is
    // reopened because per-cell capture refuses an index that ran an exact
    // capture in this process (nonzero exact generation).
    let index = reopen(dir, "index-1-1.snapshot");
    drop(capture(&index, dir, 1, Some(FRAME_FLOOR)));
    index.writer().set_cell_floor(through(6)).unwrap();
    publish(&index, 7, &[(b"a1", true), (b"e1", true)]);
    index
}

fn reopen(dir: &Path, name: &str) -> NativeIndex {
    NativeIndex::open_snapshot_with_shards(&dir.join(name), &shape(), DATABASE, BUDGET, SHARDS)
        .unwrap()
}

fn container() -> IndexStoreFileId {
    IndexStoreFileId::LegacyContainer {
        authority_epoch: 1,
        generation: 1,
    }
}

fn load(
    dir: &Path,
    name: &str,
    metadata: IndexImageMetadata,
) -> Result<NativeIndex, NativeIndexError> {
    let root = read_cell_root(&dir.join(name), metadata, &shape(), DATABASE, BUDGET)?;
    NativeIndex::open_cell_root(&root, dir, &shape(), DATABASE, BUDGET, SHARDS, |file| {
        map_root_file(dir, file)
    })
}

/// Write a root without checking it, for corrupt-root cases.
fn write_raw(dir: &Path, name: &str, root: &CellRoot) -> IndexImageMetadata {
    write_bytes(dir, name, &encode_cell_root(root).unwrap())
}

fn write_bytes(dir: &Path, name: &str, bytes: &[u8]) -> IndexImageMetadata {
    write_root_file(dir, name, bytes, |_| Ok(())).unwrap()
}

/// A CRC frame around `payload`.
fn frame_of(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + 8);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&crc32fast::hash(payload).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

fn invalid(result: Result<impl Sized, NativeIndexError>) -> &'static str {
    match result {
        Err(NativeIndexError::Invalid(reason)) => reason,
        Err(error) => panic!("expected an invalid-root error, got {error}"),
        Ok(_) => panic!("expected an invalid-root error, got success"),
    }
}

fn cell_named(root: &mut CellRoot, first: u8) -> &mut RootCell {
    root.cells
        .iter_mut()
        .find(|cell| cell.cell == CellId::Bytes([first].as_slice().into()))
        .unwrap()
}

fn golden_root() -> CellRoot {
    let legacy = IndexStoreFileId::LegacyContainer {
        authority_epoch: 3,
        generation: 1,
    };
    let segment = IndexStoreFileId::Segment {
        authority_epoch: 3,
        stream_id: 2,
        segment_seq: 7,
    };
    let at = |sequence| PrefixCut::Through(BatchVersion { epoch: 3, sequence });
    let floor = MutationVersion {
        batch: BatchVersion {
            epoch: 3,
            sequence: 8,
        },
        operation_index: 2,
    };
    CellRoot {
        header: CellRootHeader {
            format: PER_CELL_FORMAT,
            flags: 0,
            database: DATABASE,
            schema: [9; 32],
            authority_epoch: 3,
            snapshot_generation: 4,
            lifecycle_generation: 5,
            directory_target: at(20),
            replay_floor: at(10),
            max_frame_cut: at(25),
            history_floor: Some(floor),
        },
        files: vec![
            RootFile {
                id: legacy,
                name: legacy.canonical_name(),
                sealed_len: 4096,
            },
            RootFile {
                id: segment,
                name: segment.canonical_name(),
                sealed_len: 100,
            },
        ],
        cells: vec![
            RootCell {
                keyspace: KeySpace(0),
                cell: CellId::Bytes(b"a".as_slice().into()),
                coverage: at(10),
                base: Some(RootCellBase {
                    file: legacy,
                    offset: 0,
                    framed_len: 40,
                    digest: [1; 32],
                    frame_cut: at(25),
                    frame_floor: None,
                }),
            },
            RootCell {
                keyspace: KeySpace(0),
                cell: CellId::Bytes(b"b".as_slice().into()),
                coverage: at(20),
                base: None,
            },
            RootCell {
                keyspace: KeySpace(1),
                cell: CellId::Integer(5),
                coverage: at(20),
                base: Some(RootCellBase {
                    file: segment,
                    offset: 16,
                    framed_len: 12,
                    digest: [2; 32],
                    frame_cut: at(12),
                    frame_floor: Some(floor),
                }),
            },
        ],
    }
}

/// The format-5 layout written out field by field, independent of the
/// encoder. Changing it is a format change.
#[test]
fn golden_layout_and_codec_round_trip() {
    let root = golden_root();
    root.check(DATABASE, [9; 32]).unwrap();
    let mut expected = Vec::new();
    let through = |out: &mut Vec<u8>, sequence: u64| {
        out.push(1);
        out.extend_from_slice(&3u64.to_be_bytes());
        out.extend_from_slice(&sequence.to_be_bytes());
    };
    let floor = |out: &mut Vec<u8>| {
        out.push(1);
        out.extend_from_slice(&3u64.to_be_bytes());
        out.extend_from_slice(&8u64.to_be_bytes());
        out.extend_from_slice(&2u32.to_be_bytes());
    };
    expected.extend_from_slice(b"THINDEX1");
    expected.extend_from_slice(&5u16.to_be_bytes());
    expected.extend_from_slice(&0u16.to_be_bytes());
    expected.extend_from_slice(&[7; 16]);
    expected.extend_from_slice(&[9; 32]);
    for value in [3u64, 4, 5] {
        expected.extend_from_slice(&value.to_be_bytes());
    }
    through(&mut expected, 20);
    through(&mut expected, 10);
    through(&mut expected, 25);
    floor(&mut expected);
    expected.extend_from_slice(&2u32.to_be_bytes());
    expected.extend_from_slice(&3u32.to_be_bytes());
    assert_eq!(expected.len(), ROOT_HEADER_BYTES);
    // File table.
    expected.push(0);
    expected.extend_from_slice(&3u64.to_be_bytes());
    expected.extend_from_slice(&1u64.to_be_bytes());
    let legacy_name = b"index-3-1.snapshot.cells";
    expected.extend_from_slice(&(legacy_name.len() as u16).to_be_bytes());
    expected.extend_from_slice(legacy_name);
    expected.extend_from_slice(&4096u64.to_be_bytes());
    let segment_id = |out: &mut Vec<u8>| {
        out.push(1);
        out.extend_from_slice(&3u64.to_be_bytes());
        out.extend_from_slice(&2u16.to_be_bytes());
        out.extend_from_slice(&7u64.to_be_bytes());
    };
    segment_id(&mut expected);
    let segment_name = b"index-store-3-2-7.cells";
    expected.extend_from_slice(&(segment_name.len() as u16).to_be_bytes());
    expected.extend_from_slice(segment_name);
    expected.extend_from_slice(&100u64.to_be_bytes());
    // Cell `a`: bytes id, base in the legacy container, no frame floor.
    expected.extend_from_slice(&[0, 1, 0, 1, b'a']);
    through(&mut expected, 10);
    expected.push(1);
    expected.push(0);
    expected.extend_from_slice(&3u64.to_be_bytes());
    expected.extend_from_slice(&1u64.to_be_bytes());
    expected.extend_from_slice(&0u64.to_be_bytes());
    expected.extend_from_slice(&40u64.to_be_bytes());
    expected.extend_from_slice(&[1; 32]);
    through(&mut expected, 25);
    expected.push(0);
    expected.extend_from_slice(&[0; 20]);
    // Cell `b`: no frame.
    expected.extend_from_slice(&[0, 1, 0, 1, b'b']);
    through(&mut expected, 20);
    expected.push(0);
    // Cell 5 of keyspace 1: integer id, base in the segment.
    expected.extend_from_slice(&[1, 0]);
    expected.extend_from_slice(&5u64.to_be_bytes());
    through(&mut expected, 20);
    expected.push(1);
    segment_id(&mut expected);
    expected.extend_from_slice(&16u64.to_be_bytes());
    expected.extend_from_slice(&12u64.to_be_bytes());
    expected.extend_from_slice(&[2; 32]);
    through(&mut expected, 12);
    floor(&mut expected);

    let encoded = encode_cell_root(&root).unwrap();
    assert_eq!(encoded, frame_of(&expected));
    assert_eq!(encoded_payload_len(&root).unwrap(), expected.len());
    assert_eq!(decode_cell_root(&expected, usize::MAX).unwrap(), root);
    let id_bytes: usize = root.cells.iter().map(|cell| cell.cell.len()).sum();
    let bound = cell_root_bytes_bound(root.cells.len(), id_bytes, root.files.len()).unwrap();
    assert!(encoded.len() <= bound, "{} > {bound}", encoded.len());

    // The file-name bound is the longest canonical name.
    let longest = IndexStoreFileId::Segment {
        authority_epoch: u64::MAX,
        stream_id: u16::MAX,
        segment_seq: u64::MAX,
    };
    assert_eq!(longest.canonical_name().len(), MAX_FILE_NAME_BYTES);
    assert_eq!(cell_root_name(3, 4), "index-root-3-4.cellroot");
    assert_eq!(
        parse_cell_root_name("index-root-3-4.cellroot"),
        Some((3, 4))
    );
    for name in [
        "index-root-03-4.cellroot",
        "index-root-3-4-5.cellroot",
        "index-root-3.cellroot",
        ".index-root-3-4.cellroot.pending",
        "index-3-4.snapshot.cells",
    ] {
        assert_eq!(parse_cell_root_name(name), None, "{name}");
    }
    assert_eq!(
        cell_root_staging_name("index-root-3-4.cellroot"),
        ".index-root-3-4.cellroot.pending"
    );
}

/// Every unsupported tag and every format other than 5 fails closed in the
/// decoder.
#[test]
fn decoder_rejects_unsupported_tags_and_formats() {
    let root = golden_root();
    let payload = encode_cell_root(&root).unwrap()[8..].to_vec();
    let cells_at = ROOT_HEADER_BYTES
        + root
            .files
            .iter()
            .map(|file| file_id_len(file.id) + 2 + file.name.len() + 8)
            .sum::<usize>();
    let base_flag_at = cells_at + 1 + 4 + PREFIX_CUT_BYTES;
    let cases: [(usize, u8, &str); 9] = [
        (8, 4, "cell root format"),
        (8, 6, "cell root format"),
        (84, 2, "prefix cut tag"),
        (84 + PREFIX_CUT_BYTES, 3, "prefix cut tag"),
        (84 + 3 * PREFIX_CUT_BYTES, 2, "logical cut tag"),
        (ROOT_HEADER_BYTES, 2, "file id tag"),
        (cells_at + 1, 2, "cell tag"),
        (base_flag_at, 2, "cell base tag"),
        (base_flag_at + 1, 2, "file id tag"),
    ];
    for (at, value, reason) in cases {
        let mut bytes = payload.clone();
        // Format is a u16: patch its low byte.
        let at = if at == 8 { 9 } else { at };
        bytes[at] = value;
        assert_eq!(
            invalid(decode_cell_root(&bytes, usize::MAX)),
            reason,
            "byte {at}"
        );
    }
    // A nonzero `BeforeAll` body is not canonical.
    let mut bytes = payload.clone();
    bytes[84] = 0;
    assert_eq!(
        invalid(decode_cell_root(&bytes, usize::MAX)),
        "prefix cut tag"
    );
    // Truncation anywhere and trailing bytes are rejected.
    for length in [0, 7, 100, payload.len() - 1] {
        assert_eq!(
            invalid(decode_cell_root(&payload[..length], usize::MAX)),
            "truncated index image",
            "length {length}"
        );
    }
    // Counts that need more bytes than remain.
    assert_eq!(
        invalid(decode_cell_root(&payload[..ROOT_HEADER_BYTES], usize::MAX)),
        "cell root counts"
    );
    let mut trailing = payload.clone();
    trailing.push(0);
    assert_eq!(
        invalid(decode_cell_root(&trailing, usize::MAX)),
        "trailing cell root bytes"
    );
    // Counts above the remaining bytes or the budget allocate nothing.
    let mut counts = payload.clone();
    counts[ROOT_HEADER_BYTES - 8..ROOT_HEADER_BYTES - 4].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        invalid(decode_cell_root(&counts, usize::MAX)),
        "cell root counts"
    );
    assert!(matches!(
        decode_cell_root(&payload, 2),
        Err(NativeIndexError::Capacity)
    ));
    // A file name above the canonical bound.
    let mut long = root.clone();
    long.files[0].name = "x".repeat(MAX_FILE_NAME_BYTES + 1);
    assert_eq!(invalid(encode_cell_root(&long)), "file table name");
}

#[test]
fn capture_persist_load_round_trip() {
    let temp = tempdir::TempDir::new("cell-root-round-trip").unwrap();
    let dir = temp.path();
    let index = fixture(dir);
    let prepared = capture(&index, dir, 2, Some(FRAME_FLOOR));
    let root = prepared.root().clone();
    let header = &root.header;
    assert_eq!(header.directory_target, through(7));
    assert_eq!(header.replay_floor, through(6));
    assert_eq!(header.max_frame_cut, through(7));
    assert_eq!(header.history_floor, Some(FRAME_FLOOR));
    assert_eq!(root.files.len(), 1);
    assert_eq!(root.files[0].id, container());
    assert_eq!(
        root.files[0].sealed_len,
        std::fs::metadata(dir.join("index-1-1.snapshot.cells"))
            .unwrap()
            .len()
    );
    let summary: Vec<_> = root
        .cells
        .iter()
        .map(|cell| {
            let CellId::Bytes(id) = &cell.cell else {
                unreachable!()
            };
            (
                id[0],
                cell.coverage,
                cell.base
                    .as_ref()
                    .map(|base| (base.frame_cut, base.frame_floor)),
            )
        })
        .collect();
    let frame = Some((through(6), Some(FRAME_FLOOR)));
    assert_eq!(
        summary,
        [
            (b'a', through(6), frame),
            (b'b', through(7), frame),
            (b'c', through(7), frame),
            (b'd', through(7), frame),
            (b'e', through(6), None),
        ]
    );

    let metadata = prepared.persist(dir).unwrap();
    let name = cell_root_name(1, 2);
    assert_eq!(
        metadata.bytes,
        std::fs::metadata(dir.join(&name)).unwrap().len()
    );
    assert!(!dir.join(cell_root_staging_name(&name)).exists());
    let decoded = read_cell_root(&dir.join(&name), metadata, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(decoded, root);
    let loaded = load(dir, &name, metadata).unwrap();

    // Every directory cell, with its coverage; new cells start at F.
    assert_eq!(loaded.stats().cells, 5);
    assert_eq!(loaded.stats().unloaded_cells, 4);
    for cell in &root.cells {
        let dirty = loaded.cell_dirty(cell.keyspace, &cell.cell).unwrap();
        assert_eq!(dirty.coverage, cell.coverage);
        assert_eq!(dirty.versions, 0);
    }
    let mut writer = loaded.writer();
    assert_eq!(writer.state.cells.floor, through(6));
    assert!(matches!(
        writer.begin_capture(IndexReadCut::Latest),
        Err(NativeIndexError::Invalid("per-cell flush state is active"))
    ));
    drop(writer);
    // Published at F, not at a frame cut; retention floor H.
    assert_eq!(loaded.published_prefix(), through(6));
    assert_eq!(loaded.retention_floor(), IndexReadCut::At(FRAME_FLOOR));
    // Reads at every retained cut match the source index; batch 7 is in the
    // replay suffix, not in any frame.
    for sequence in 4..=6 {
        for key in KEYS.iter().chain([&b"e1".as_slice()]) {
            let cut = through(sequence).index_cut();
            assert_eq!(
                read(&loaded, key, cut.clone()),
                read(&index, key, cut),
                "{key:?} at {sequence}"
            );
        }
    }
    assert_eq!(
        read(&loaded, b"a1", IndexReadCut::Latest),
        Some((4, RecordKind::Put))
    );
    assert_eq!(read(&loaded, b"e1", IndexReadCut::Latest), None);

    // A root captured from the loaded index references the same frames.
    let again = capture(&loaded, dir, 3, Some(FRAME_FLOOR));
    assert_eq!(again.root().header.directory_target, through(6));
    assert_eq!(again.root().files, root.files);
    for (left, right) in again.root().cells.iter().zip(&root.cells) {
        assert_eq!((&left.cell, &left.base), (&right.cell, &right.base));
        assert_eq!(left.coverage, through(6));
    }

    // A generation name is immutable.
    assert_eq!(
        match prepared.persist(dir) {
            Err(NativeIndexError::Io(error)) => error.kind(),
            _ => panic!("persisted over an existing root"),
        },
        std::io::ErrorKind::AlreadyExists
    );
}

/// Slices of at most 32 descriptors into one buffer reserved before the
/// first slice; cells created after the boundary are excluded; a base
/// installed during the capture exports `B_i > T` with `C_i <= T`, and `U`
/// must be covered by the observed complete prefix.
#[test]
fn capture_is_sliced_preallocated_and_handles_newer_bases() {
    let temp = tempdir::TempDir::new("cell-root-slices").unwrap();
    let dir = temp.path();
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, SHARDS).unwrap();
    let keys: Vec<[u8; 2]> = (0..70u8).map(|first| [first, 1]).collect();
    let ops: Vec<(&[u8], bool)> = keys.iter().map(|key| (key.as_slice(), true)).collect();
    publish(&index, 1, &ops);
    index
        .writer()
        .flush_and_unload(&dir.join("index-1-1.snapshot"), IndexReadCut::Latest)
        .unwrap();
    let index = reopen(dir, "index-1-1.snapshot");
    // New cells start at the legacy cut. An exact install recounts each
    // installed cell's dirty totals, so a cell whose base holds every
    // reference clean-advances to the capture target.
    index.writer().set_cell_floor(through(1)).unwrap();

    // Without the reserved buffer a slice is refused before any effect.
    let mut unreserved = index.writer().begin_cell_root_capture();
    assert_eq!(
        invalid(unreserved.capture_slice(&mut index.writer())),
        "root descriptor slice"
    );

    let run = |sequence: u64, generation: u64, complete: Option<PrefixCut>| {
        let mut capture = index.writer().begin_cell_root_capture();
        assert_eq!(capture.target(), through(sequence - 1));
        // Run 1 created cell 200 after its boundary; run 2 includes it.
        let end = 69 + generation as usize;
        assert_eq!(capture.end, end);
        capture.reserve().unwrap();
        let buffer = (capture.descriptors.as_ptr(), capture.descriptors.capacity());
        assert_eq!(buffer.1, end);
        let mut slices = 0;
        let mut done = false;
        while !done {
            assert!(slices < 3, "more slices than the boundary needs");
            done = capture.capture_slice(&mut index.writer()).unwrap();
            slices += 1;
            assert!(capture.descriptors.len() <= slices * ROOT_DESCRIPTOR_SLICE);
            if slices == 1 {
                // A newer base for a later cell, and a new cell (first byte
                // 199 + generation) that the boundary excludes.
                let new_cell = [199 + generation as u8, 1];
                publish(&index, sequence, &[(&[68, 1], true), (&new_cell, true)]);
                index
                    .writer()
                    .flush_and_unload(
                        &dir.join(format!("index-1-{sequence}.snapshot")),
                        IndexReadCut::Latest,
                    )
                    .unwrap();
            }
        }
        assert_eq!(slices, 3);
        assert_eq!(
            (capture.descriptors.as_ptr(), capture.descriptors.capacity()),
            buffer,
            "a slice reallocated"
        );
        capture.finish(
            &index,
            dir,
            params(generation, None),
            complete.unwrap_or_else(|| index.published_prefix()),
            stat_len(dir),
        )
    };
    assert_eq!(
        invalid(run(2, 1, Some(through(1)))),
        "frame cut above the complete published prefix"
    );
    let prepared = run(3, 2, None).unwrap();
    let root = prepared.root();
    assert_eq!(root.header.directory_target, through(2));
    assert_eq!(root.header.max_frame_cut, through(3));
    assert_eq!(root.cells.len(), 71);
    assert!(
        !root
            .cells
            .iter()
            .any(|cell| cell.cell == CellId::Bytes([201].as_slice().into())),
        "a cell created after the boundary was captured"
    );
    let cell_68 = &root.cells[68];
    assert_eq!(cell_68.cell, CellId::Bytes([68].as_slice().into()));
    let base = cell_68.base.as_ref().unwrap();
    assert_eq!(base.frame_cut, through(3));
    // The exact flush during run 2 installed `B_68 = 3`; the cell misses
    // nothing, so it clean-advances to `T = 2 < B_68`.
    assert_eq!(cell_68.coverage, through(2));
    assert_eq!(root.header.replay_floor, through(2));
    assert_eq!(
        base.file,
        IndexStoreFileId::LegacyContainer {
            authority_epoch: 1,
            generation: 3,
        }
    );
    assert!(root.files.len() >= 2);
    let metadata = prepared.persist(dir).unwrap();
    let loaded = load(dir, &cell_root_name(1, 2), metadata).unwrap();
    assert_eq!(loaded.stats().cells, 71);
    assert_eq!(loaded.published_prefix(), through(2));
    assert_eq!(
        read(&loaded, &[68, 1], IndexReadCut::Latest),
        Some((2, RecordKind::Put))
    );
    assert!(
        matches!(
            loaded.get(KS, &[68, 1], through(3).index_cut()),
            Err(NativeIndexError::UnpublishedCut)
        ),
        "published at F: the frame's newer batch is not visible yet"
    );
}

#[test]
fn empty_directory_root_uses_its_target() {
    let temp = tempdir::TempDir::new("cell-root-empty").unwrap();
    let dir = temp.path();
    let index = NativeIndex::with_shards(&shape(), DATABASE, BUDGET, SHARDS).unwrap();
    let prepared = capture(&index, dir, 1, None);
    let root = prepared.root();
    assert!(root.cells.is_empty() && root.files.is_empty());
    assert_eq!(root.header.replay_floor, PrefixCut::BeforeAll);
    assert_eq!(root.header.replay_floor, root.header.directory_target);
    let metadata = prepared.persist(dir).unwrap();
    assert_eq!(metadata.bytes as usize, CELL_ROOT_FIXED_BYTES);
    let loaded = load(dir, &cell_root_name(1, 1), metadata).unwrap();
    assert_eq!(loaded.stats().cells, 0);
    assert_eq!(loaded.published_prefix(), PrefixCut::BeforeAll);
}

/// Segment ids load from the same frames; each file is mapped once.
#[test]
fn segment_file_ids_load_and_map_once() {
    let temp = tempdir::TempDir::new("cell-root-segment").unwrap();
    let dir = temp.path();
    let index = fixture(dir);
    let mut root = capture(&index, dir, 2, Some(FRAME_FLOOR)).root().clone();
    let segment = IndexStoreFileId::Segment {
        authority_epoch: 1,
        stream_id: 0,
        segment_seq: 5,
    };
    std::fs::copy(
        dir.join("index-1-1.snapshot.cells"),
        dir.join(segment.canonical_name()),
    )
    .unwrap();
    root.files[0].id = segment;
    root.files[0].name = segment.canonical_name();
    for cell in &mut root.cells {
        if let Some(base) = &mut cell.base {
            base.file = segment;
        }
    }
    root.check(DATABASE, schema_digest(&shape()).unwrap())
        .unwrap();
    let name = cell_root_name(1, 2);
    let metadata = write_raw(dir, &name, &root);
    let decoded = read_cell_root(&dir.join(&name), metadata, &shape(), DATABASE, BUDGET).unwrap();
    let mut maps = Vec::new();
    let loaded =
        NativeIndex::open_cell_root(&decoded, dir, &shape(), DATABASE, BUDGET, SHARDS, |file| {
            maps.push(file.id);
            map_root_file(dir, file)
        })
        .unwrap();
    assert_eq!(maps, [segment]);
    std::fs::remove_file(dir.join("index-1-1.snapshot.cells")).unwrap();
    let again = capture(&loaded, dir, 3, Some(FRAME_FLOOR));
    assert_eq!(again.root().files[0].id, segment);
    assert_eq!(
        read(&loaded, b"c1", IndexReadCut::Latest),
        Some((6, RecordKind::Put))
    );
}

/// Spec §11.3 negative loader cases on a valid fixture root.
#[test]
fn loader_rejects_corrupt_and_inconsistent_roots() {
    let temp = tempdir::TempDir::new("cell-root-negative").unwrap();
    let dir = temp.path();
    let index = fixture(dir);
    let valid = capture(&index, dir, 2, Some(FRAME_FLOOR)).root().clone();
    let container_name = container().canonical_name();
    let container_bytes = std::fs::read(dir.join(&container_name)).unwrap();
    let mut generation = 10;
    let mut check = |mutate: &dyn Fn(&mut CellRoot), reason: &str| {
        let mut root = valid.clone();
        mutate(&mut root);
        generation += 1;
        let name = format!("case-{generation}");
        let metadata = write_raw(dir, &name, &root);
        match load(dir, &name, metadata) {
            Err(NativeIndexError::Invalid(actual)) => assert_eq!(actual, reason),
            Err(error) => panic!("{reason}: got {error}"),
            Ok(_) => panic!("{reason}: loaded"),
        }
    };

    // Structural rules (CellRoot::check).
    check(
        &|root| cell_named(root, b'b').coverage = through(8),
        "cell coverage above target",
    );
    check(
        &|root| root.header.replay_floor = through(7),
        "replay floor is not the minimum coverage",
    );
    check(
        &|root| root.header.replay_floor = through(5),
        "replay floor is not the minimum coverage",
    );
    check(
        &|root| root.header.history_floor = Some(end_of(7)),
        "history floor above replay floor",
    );
    check(&|root| root.header.history_floor = None, "cell frame floor");
    check(
        &|root| root.header.max_frame_cut = through(5),
        "frame cut bound below target",
    );
    check(
        &|root| root.header.database = [8; 16],
        "cell root database or schema",
    );
    check(
        &|root| root.header.schema = [0; 32],
        "cell root database or schema",
    );
    check(
        &|root| root.header.snapshot_generation = 0,
        "cell root generation",
    );
    check(&|root| root.header.flags = 1, "cell root format");
    check(
        &|root| {
            let duplicate = cell_named(root, b'b').base.clone();
            cell_named(root, b'c').base = duplicate;
        },
        "overlapping cell ranges",
    );
    check(
        &|root| {
            // `c` starts one byte inside `b`.
            let mut inside = cell_named(root, b'b').base.clone().unwrap();
            inside.offset += 1;
            inside.framed_len -= 1;
            cell_named(root, b'c').base = Some(inside);
        },
        "overlapping cell ranges",
    );
    check(
        &|root| root.files.push(root.files[0].clone()),
        "file table order",
    );
    check(
        &|root| {
            let extra = IndexStoreFileId::Segment {
                authority_epoch: 1,
                stream_id: 0,
                segment_seq: 9,
            };
            root.files.push(RootFile {
                id: extra,
                name: extra.canonical_name(),
                sealed_len: 1 << 20,
            });
        },
        "unreferenced file table entry",
    );
    check(
        &|root| {
            cell_named(root, b'a').base.as_mut().unwrap().file =
                IndexStoreFileId::LegacyContainer {
                    authority_epoch: 1,
                    generation: 9,
                };
        },
        "cell file id",
    );
    check(
        &|root| root.files[0].name = format!("../{}", root.files[0].name),
        "file table name",
    );
    check(
        &|root| root.files[0].name = "index-01-1.snapshot.cells".to_owned(),
        "file table name",
    );
    check(
        &|root| {
            let sealed = root.files[0].sealed_len;
            cell_named(root, b'a').base.as_mut().unwrap().offset = sealed;
        },
        "cell range bounds",
    );
    check(
        &|root| cell_named(root, b'a').base.as_mut().unwrap().frame_cut = through(8),
        "cell frame cut",
    );
    check(
        &|root| cell_named(root, b'a').base.as_mut().unwrap().frame_cut = PrefixCut::BeforeAll,
        "cell frame cut",
    );
    check(
        &|root| {
            cell_named(root, b'a').base.as_mut().unwrap().frame_floor = Some(end_of(7));
            root.header.history_floor = Some(end_of(7));
        },
        "history floor above replay floor",
    );
    check(&|root| root.cells.swap(0, 1), "cell directory order");
    check(
        &|root| {
            let first = root.cells[0].clone();
            root.cells.insert(0, first);
        },
        "cell directory order",
    );

    // Frame validation against the frame's own cuts. Cell `a` holds a
    // batch-5 delete; `B_i = 4 < C_i` is allowed, the reference is not.
    check(
        &|root| cell_named(root, b'a').base.as_mut().unwrap().frame_cut = through(4),
        "index reference domain, order or cut",
    );
    check(
        &|root| {
            cell_named(root, b'a').base.as_mut().unwrap().frame_floor = Some(end_of(6));
            root.header.history_floor = Some(end_of(6));
        },
        "frame history floor",
    );
    check(
        &|root| cell_named(root, b'a').base.as_mut().unwrap().digest[0] ^= 1,
        "cell range digest",
    );
    check(
        &|root| {
            root.files[0].sealed_len += 1;
        },
        "cell file length",
    );
    // A frame range that is not a whole frame: digest of the shorter range.
    check(
        &|root| {
            let base = cell_named(root, b'a').base.as_mut().unwrap();
            base.framed_len -= 1;
            let start = base.offset as usize;
            base.digest = cell_digest(&container_bytes[start..start + base.framed_len as usize]);
        },
        "index image CRC",
    );
    // A cell routed to another keyspace or kind.
    check(
        &|root| {
            root.cells.push(RootCell {
                keyspace: KeySpace(9),
                cell: CellId::Integer(0),
                coverage: through(7),
                base: None,
            });
        },
        "unknown or destroyed keyspace",
    );
    check(
        &|root| {
            root.cells.push(RootCell {
                keyspace: KS,
                cell: CellId::Bytes(b"zz".as_slice().into()),
                coverage: through(7),
                base: None,
            });
        },
        "cell does not belong to keyspace",
    );

    // A corrupt byte inside a frame whose digest the root records anyway.
    let corrupt = IndexStoreFileId::LegacyContainer {
        authority_epoch: 2,
        generation: 1,
    };
    let mut bytes = container_bytes.clone();
    let mut root = valid.clone();
    let base = cell_named(&mut root, b'a').base.as_mut().unwrap();
    let start = base.offset as usize;
    let end = start + base.framed_len as usize;
    bytes[start + 12] ^= 0xff;
    base.file = corrupt;
    base.digest = cell_digest(&bytes[start..end]);
    std::fs::write(dir.join(corrupt.canonical_name()), &bytes).unwrap();
    root.files.push(RootFile {
        id: corrupt,
        name: corrupt.canonical_name(),
        sealed_len: bytes.len() as u64,
    });
    let metadata = write_raw(dir, "case-crc", &root);
    assert_eq!(invalid(load(dir, "case-crc", metadata)), "index image CRC");

    // Root file corruption.
    let name = cell_root_name(1, 2);
    let metadata = write_raw(dir, &name, &valid);
    let good = std::fs::read(dir.join(&name)).unwrap();
    let mut flipped = good.clone();
    flipped[100] ^= 1;
    std::fs::write(dir.join("case-digest"), &flipped).unwrap();
    assert_eq!(
        invalid(load(dir, "case-digest", metadata)),
        "cell root digest"
    );
    let flipped_metadata = write_bytes(dir, "case-root-crc", &flipped);
    assert_eq!(
        invalid(load(dir, "case-root-crc", flipped_metadata)),
        "cell root CRC"
    );
    let truncated = write_bytes(dir, "case-truncated", &good[..good.len() - 1]);
    assert_eq!(
        invalid(load(dir, "case-truncated", truncated)),
        "cell root CRC"
    );
    let mut longer = good.clone();
    longer.push(0);
    let longer_metadata = write_bytes(dir, "case-trailing", &longer);
    assert_eq!(
        invalid(load(dir, "case-trailing", longer_metadata)),
        "trailing cell root bytes"
    );
    assert_eq!(
        invalid(load(
            dir,
            &name,
            IndexImageMetadata {
                bytes: metadata.bytes + 1,
                digest: metadata.digest,
            }
        )),
        "cell root length"
    );

    // Unsupported tag through the full loader (frame recomputed).
    let mut tagged = good[8..].to_vec();
    tagged[84] = 2;
    let tagged_metadata = write_bytes(dir, "case-tag", &frame_of(&tagged));
    assert_eq!(
        invalid(load(dir, "case-tag", tagged_metadata)),
        "prefix cut tag"
    );

    // Mixed formats: each loader rejects the other format.
    let mut format_4 = good[8..].to_vec();
    format_4[8..10].copy_from_slice(&4u16.to_be_bytes());
    let format_metadata = write_bytes(dir, "case-format", &frame_of(&format_4));
    assert_eq!(
        invalid(load(dir, "case-format", format_metadata)),
        "cell root format"
    );
    let legacy_root = std::fs::read(dir.join("index-1-1.snapshot")).unwrap();
    let legacy_metadata = IndexImageMetadata {
        bytes: legacy_root.len() as u64,
        digest: Blake2b::<U32>::digest(&legacy_root).into(),
    };
    assert_eq!(
        invalid(load(dir, "index-1-1.snapshot", legacy_metadata)),
        "cell root format"
    );
    assert_eq!(
        invalid(NativeIndex::open_snapshot(
            &dir.join(&name),
            &shape(),
            DATABASE,
            BUDGET
        )),
        "index image format"
    );

    // Directory larger than the budget can charge: refused before decoding
    // the entries.
    assert!(matches!(
        read_cell_root(
            &dir.join(&name),
            metadata,
            &shape(),
            DATABASE,
            CELL_CHARGE * 2
        ),
        Err(NativeIndexError::Capacity)
    ));

    // Missing and truncated files.
    let truncated_container = dir.join(&container_name);
    std::fs::write(
        &truncated_container,
        &container_bytes[..container_bytes.len() - 1],
    )
    .unwrap();
    assert_eq!(invalid(load(dir, &name, metadata)), "cell file length");
    std::fs::remove_file(&truncated_container).unwrap();
    assert!(matches!(
        load(dir, &name, metadata),
        Err(NativeIndexError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    std::fs::remove_file(dir.join(&name)).unwrap();
    assert!(matches!(
        load(dir, &name, metadata),
        Err(NativeIndexError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
}

#[derive(Clone, Copy, Debug)]
enum Cut {
    /// Staging file created, nothing written.
    Created,
    /// Frame written, not synced; the crash keeps half of it.
    TornWrite,
    /// Staging file synced, not renamed.
    Synced,
    /// Renamed, directory not synced; the rename is lost.
    RenamedOld,
    /// Renamed, directory not synced; the rename survives.
    RenamedNew,
    /// Directory synced: complete.
    Complete,
}

/// Crash at every root persistence step (spec §8.3, §11.3). The manifest is
/// written only after `persist` returns, so it still selects generation 1:
/// that root and its frames must load unchanged, a new final name exists only
/// as a complete authenticated root, and a restarted attempt completes.
#[test]
fn root_persistence_crash_cuts() {
    for cut in [
        Cut::Created,
        Cut::TornWrite,
        Cut::Synced,
        Cut::RenamedOld,
        Cut::RenamedNew,
        Cut::Complete,
    ] {
        let temp = tempdir::TempDir::new("cell-root-crash").unwrap();
        let dir = temp.path();
        let index = fixture(dir);
        let first = capture(&index, dir, 1, Some(FRAME_FLOOR));
        let adopted = first.persist(dir).unwrap();
        let first_name = cell_root_name(1, 1);
        let first_bytes = std::fs::read(dir.join(&first_name)).unwrap();
        publish(&index, 8, &[(b"b1", true)]);
        let second = capture(&index, dir, 2, Some(FRAME_FLOOR));
        assert_ne!(second.root().cells, first.root().cells);
        let frame = encode_cell_root(second.root()).unwrap();
        let intended = IndexImageMetadata {
            bytes: frame.len() as u64,
            digest: Blake2b::<U32>::digest(&frame).into(),
        };
        let name = cell_root_name(1, 2);
        let final_path = dir.join(&name);
        let staging = dir.join(cell_root_staging_name(&name));
        let crash_at = match cut {
            Cut::Created => Some(RootPersistStep::Created),
            Cut::TornWrite => Some(RootPersistStep::Written),
            Cut::Synced => Some(RootPersistStep::Synced),
            Cut::RenamedOld | Cut::RenamedNew => Some(RootPersistStep::Renamed),
            Cut::Complete => None,
        };
        let result = persist_root(dir, second.root(), |step| {
            if Some(step) == crash_at {
                Err(std::io::Error::other("injected crash"))
            } else {
                Ok(())
            }
        });
        // Directory state the crash leaves.
        match cut {
            Cut::Complete => assert_eq!(result.unwrap().digest, intended.digest),
            Cut::TornWrite => {
                assert!(result.is_err());
                let file = OpenOptions::new().write(true).open(&staging).unwrap();
                file.set_len(frame.len() as u64 / 2).unwrap();
            }
            Cut::RenamedOld => {
                assert!(result.is_err());
                std::fs::rename(&final_path, &staging).unwrap();
            }
            _ => assert!(result.is_err()),
        }

        // Restart: the manifest still selects generation 1.
        assert_eq!(
            std::fs::read(dir.join(&first_name)).unwrap(),
            first_bytes,
            "{cut:?}"
        );
        let old =
            read_cell_root(&dir.join(&first_name), adopted, &shape(), DATABASE, BUDGET).unwrap();
        assert_eq!(&old, first.root(), "{cut:?}");
        load(dir, &first_name, adopted).unwrap();
        let final_exists = matches!(cut, Cut::RenamedNew | Cut::Complete);
        assert_eq!(final_path.exists(), final_exists, "{cut:?}");
        assert_eq!(staging.exists(), !final_exists, "{cut:?}");
        assert_eq!(
            parse_cell_root_name(&cell_root_staging_name(&name)),
            None,
            "a staging name is never a root name"
        );
        if matches!(cut, Cut::Created | Cut::TornWrite) {
            assert!(read_cell_root(&staging, intended, &shape(), DATABASE, BUDGET).is_err());
        }
        if final_exists {
            // The final name only ever holds the complete synced root.
            let decoded =
                read_cell_root(&final_path, intended, &shape(), DATABASE, BUDGET).unwrap();
            assert_eq!(&decoded, second.root());
            assert!(matches!(
                second.persist(dir),
                Err(NativeIndexError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
            ));
        } else {
            // A restarted attempt replaces the staging leftover.
            assert_eq!(
                second.persist(dir).unwrap().digest,
                intended.digest,
                "{cut:?}"
            );
            assert!(!staging.exists());
        }
        load(dir, &name, intended).unwrap();
    }
}

#[test]
fn root_bound_covers_every_admitted_directory() {
    let temp = tempdir::TempDir::new("cell-root-bound").unwrap();
    let dir = temp.path();
    let index = fixture(dir);
    let prepared = capture(&index, dir, 2, Some(FRAME_FLOOR));
    let root = prepared.root();
    let bytes = encode_cell_root(root).unwrap().len();
    let id_bytes: usize = root.cells.iter().map(|cell| cell.cell.len()).sum();
    assert!(bytes <= cell_root_bytes_bound(root.cells.len(), id_bytes, root.files.len()).unwrap());
    assert!(bytes <= index.max_cell_root_bytes());
    // The largest budget (half the image limit) still fits the format cap;
    // the module's const assertions prove it for any directory.
    let max_cells = (MAX_IMAGE_BYTES / 2) / CELL_CHARGE;
    assert!(cell_root_fits(max_cells, max_cells * 9, max_cells));
    assert!(!cell_root_fits(usize::MAX / 2, 0, 0));
    assert!(!cell_root_fits(1, MAX_IMAGE_BYTES, 1));
    assert_eq!(cell_root_bytes_bound(usize::MAX, 0, 0), None);
}
