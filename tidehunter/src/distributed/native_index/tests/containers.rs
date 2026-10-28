use super::*;
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
pub(super) struct RootBinding {
    pub identity: (KeySpace, CellId),
    pub name: String,
    pub offset: u64,
    pub length: u64,
    pub digest: [u8; 32],
    pub floor: IndexReadCut,
    pub name_at: usize,
    pub offset_at: usize,
    pub length_at: usize,
    pub digest_at: usize,
}

impl RootBinding {
    pub fn range(&self) -> std::ops::Range<usize> {
        usize::try_from(self.offset).unwrap()
            ..usize::try_from(self.offset.checked_add(self.length).unwrap()).unwrap()
    }
}

/// Decode root-bound ranges for integrity forgeries; containers are not assumed
/// to contain one cell, and digest positions are not inferred from the footer.
pub(super) fn root_bindings(path: &Path) -> (Vec<u8>, Vec<RootBinding>) {
    let bytes: Bytes = std::fs::read(path).unwrap().into();
    let payload = CrcFrame::read_from_bytes(&bytes, 0).unwrap().to_vec();
    let mut cursor = Cursor::new(&payload);
    assert_eq!(cursor.take(8).unwrap(), MAGIC);
    assert_eq!(cursor.u16().unwrap(), FORMAT);
    assert_eq!(cursor.u16().unwrap(), 0);
    cursor.take(16 + 32).unwrap();
    decode_cut(&mut cursor).unwrap();
    decode_cut(&mut cursor).unwrap();
    let count = cursor.u32().unwrap();
    let mut bindings = Vec::new();
    for _ in 0..count {
        let keyspace = KeySpace(cursor.u8().unwrap());
        let cell = decode_cell(&mut cursor).unwrap();
        let name_length = cursor.u16().unwrap() as usize;
        let name_at = cursor.position;
        let name = std::str::from_utf8(cursor.take(name_length).unwrap())
            .unwrap()
            .to_owned();
        let offset_at = cursor.position;
        let offset = cursor.u64().unwrap();
        let length_at = cursor.position;
        let length = cursor.u64().unwrap();
        let digest_at = cursor.position;
        let digest = cursor.take(32).unwrap().try_into().unwrap();
        let floor = decode_cut(&mut cursor).unwrap();
        bindings.push(RootBinding {
            identity: (keyspace, cell),
            name,
            offset,
            length,
            digest,
            floor,
            name_at,
            offset_at,
            length_at,
            digest_at,
        });
    }
    assert_eq!(cursor.remaining(), 0);
    (payload, bindings)
}

fn install_shape_batch(
    index: &mut NativeIndex,
    log: &PartitionLog,
    shape: &KeyShape,
    config: &PartitionConfig,
    batch: &CompleteBatch,
) {
    let group = CompleteGroup::new(
        shape,
        vec![batch.clone()],
        config.batch_limits,
        config.group_limits,
    )
    .unwrap();
    let appended = log
        .append_group(RequestAuthority { epoch: EPOCH }, &group)
        .unwrap()[0];
    let durable = log.persist_through(appended).unwrap();
    install(index, log, batch, durable);
}

fn physical_files(path: &Path) -> usize {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_type().unwrap().is_file())
        .count()
}

#[test]
fn more_than_4096_tiny_cells_share_one_container_reuse_ranges_and_survive_unlink() {
    const CELLS: u32 = 4097;
    const BUDGET: usize = 8 * 1024 * 1024;
    let temp = tempdir::TempDir::new("index-pooled-tiny-cells").unwrap();
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("tiny-cells", 4, 1, KeyType::uniform(8192));
    let shape = builder.build();
    let mut config = config();
    config.batch_limits = BatchLimits {
        max_operations: 8192,
        max_encoded_bytes: 512 * 1024,
    };
    config.group_limits = GroupLimits {
        max_batches: 4,
        max_encoded_bytes: 1024 * 1024 - 8,
    };
    config.fragment_bytes = 1024 * 1024;
    config.wal_file_bytes = 2 * 1024 * 1024;
    config.max_reference_metadata_bytes = BUDGET;
    let log = PartitionLog::create(
        &temp.path().join("wal"),
        config.clone(),
        &shape,
        Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
    .unwrap();
    let key = |n: u32| (n << 19).to_be_bytes();
    let make = |sequence: u64, mutations| {
        CompleteBatch::new(
            &shape,
            BatchId {
                database: DATABASE,
                operation: (sequence as u128).to_be_bytes(),
            },
            BatchVersion {
                epoch: EPOCH,
                sequence,
            },
            mutations,
            config.batch_limits,
        )
        .unwrap()
    };
    let first = make(0, (0..CELLS).map(|n| put(0, &key(n), &[1])).collect());
    let mut index = NativeIndex::new(&shape, DATABASE, BUDGET).unwrap();
    install_shape_batch(&mut index, &log, &shape, &config, &first);
    assert_eq!(index.stats().cells, CELLS as usize);
    let first_root = temp.path().join("first.index");
    let capture = index.prepare_image(IndexReadCut::Latest).unwrap();
    assert_eq!(capture.new_files(&first_root).unwrap().len(), 1);
    assert!(index.stats().transient_metadata_bytes <= BUDGET);
    let persisted = capture.persist(&first_root).unwrap();
    let first_files = persisted.files();
    assert_eq!(
        first_files.len(),
        1,
        "thousands of cells have one data barrier backing"
    );
    index.install_image(persisted);
    assert_eq!(index.reachable_files(), first_files);
    assert_eq!(
        physical_files(temp.path()),
        2,
        "one container plus one root, not a file per cell"
    );
    let (_, original) = root_bindings(&first_root);
    assert_eq!(original.len(), CELLS as usize);
    assert!(original.iter().all(|cell| cell.name == first_files[0]));
    assert!(
        original
            .windows(2)
            .all(|pair| pair[0].offset + pair[0].length == pair[1].offset)
    );
    let old_reader = NativeIndex::open_snapshot(&first_root, &shape, DATABASE, BUDGET).unwrap();
    let old_pin = index.pin_cut(IndexReadCut::Latest).unwrap();

    let clean_root = temp.path().join("clean.index");
    let capture = index.prepare_image(IndexReadCut::Latest).unwrap();
    assert!(capture.new_files(&clean_root).unwrap().is_empty());
    let persisted = capture.persist(&clean_root).unwrap();
    assert_eq!(persisted.files(), first_files);
    index.install_image(persisted);
    assert_eq!(
        physical_files(temp.path()),
        3,
        "clean capture creates only its root"
    );
    assert_eq!(
        std::fs::read(&first_root).unwrap(),
        std::fs::read(&clean_root).unwrap()
    );

    let changed = 2048;
    let second = make(1, vec![put(0, &key(changed), &[2])]);
    install_shape_batch(&mut index, &log, &shape, &config, &second);
    let second_root = temp.path().join("second.index");
    let capture = index.prepare_image(IndexReadCut::Latest).unwrap();
    assert_eq!(capture.new_files(&second_root).unwrap().len(), 1);
    let persisted = capture.persist(&second_root).unwrap();
    let second_files = persisted.files();
    assert_eq!(
        second_files.len(),
        2,
        "one old shared container plus one changed-cell container"
    );
    index.install_image(persisted);
    let (_, current) = root_bindings(&second_root);
    for (old, new) in original.iter().zip(&current) {
        assert_eq!(old.identity, new.identity);
        if old.identity.1 == CellId::Integer(changed as usize) {
            assert_ne!(old.name, new.name);
        } else {
            assert_eq!(
                (&old.name, old.offset, old.length, old.digest, &old.floor),
                (&new.name, new.offset, new.length, new.digest, &new.floor),
                "unchanged cells reuse the exact original range"
            );
        }
    }
    let new_container = second_files
        .iter()
        .find(|name| !first_files.contains(name))
        .unwrap();
    let changed_range = current
        .iter()
        .find(|cell| cell.name == *new_container)
        .unwrap();
    assert_eq!(
        std::fs::metadata(temp.path().join(new_container))
            .unwrap()
            .len(),
        changed_range.length,
        "selective capture writes only changed cell data"
    );
    let reopened = NativeIndex::open_snapshot(&second_root, &shape, DATABASE, BUDGET).unwrap();
    let expected_keys: Vec<_> = (0..CELLS).map(|n| key(n).to_vec()).collect();
    assert_eq!(
        reopened
            .scan(
                KeySpace(0),
                None,
                false,
                CELLS as usize + 1,
                IndexReadCut::Latest
            )
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.to_vec())
            .collect::<Vec<_>>(),
        expected_keys
    );
    assert_eq!(
        value(&reopened, &log, &key(changed), old_pin.cut()).as_deref(),
        Some([1].as_slice())
    );
    assert_eq!(
        value(&reopened, &log, &key(changed), IndexReadCut::Latest).as_deref(),
        Some([2].as_slice())
    );

    // Advancing to the held old cut rewrites all ranges into one new container;
    // its anchors still support the pin while obsolete container names retire.
    let compacted_root = temp.path().join("compacted.index");
    let capture = index
        .prepare_compacted_image(IndexReadCut::Latest, index.latest_cut())
        .unwrap();
    let persisted = capture.persist(&compacted_root).unwrap();
    assert_eq!(persisted.files().len(), 1);
    index.install_image(persisted);
    assert_eq!(index.retention_floor(), old_pin.cut());
    let live: BTreeSet<_> = index.reachable_files().into_iter().collect();
    for name in second_files {
        assert!(!live.contains(&name));
        std::fs::remove_file(temp.path().join(name)).unwrap();
    }
    for root in [&first_root, &clean_root, &second_root] {
        std::fs::remove_file(root).unwrap();
    }
    assert_eq!(physical_files(temp.path()), 2);
    assert_eq!(
        old_reader
            .scan(KeySpace(0), None, false, CELLS as usize + 1, old_pin.cut())
            .unwrap()
            .into_iter()
            .map(|(key, _)| key.to_vec())
            .collect::<Vec<_>>(),
        expected_keys
    );
    assert_eq!(
        value(&old_reader, &log, &key(changed), IndexReadCut::Latest).as_deref(),
        Some([1].as_slice())
    );
    let reopened = NativeIndex::open_snapshot(&compacted_root, &shape, DATABASE, BUDGET).unwrap();
    assert_eq!(
        value(&reopened, &log, &key(changed), old_pin.cut()).as_deref(),
        Some([1].as_slice())
    );
    assert_eq!(
        value(&reopened, &log, &key(changed), IndexReadCut::Latest).as_deref(),
        Some([2].as_slice())
    );
}

#[test]
fn root_bound_container_ranges_reject_overlap_overflow_wrong_cells_and_bad_digests() {
    let temp = tempdir::TempDir::new("index-container-range-integrity").unwrap();
    let log = log(&temp.path().join("wal"));
    let batch = batch(0, vec![put(0, A, b"first"), put(0, B, b"second")]);
    let receipt = durable(&log, std::slice::from_ref(&batch))[0];
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batch, receipt);
    let root = temp.path().join("root.index");
    index.flush_and_unload(&root, IndexReadCut::Latest).unwrap();
    let (payload, bindings) = root_bindings(&root);
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].name, bindings[1].name);
    let a = &bindings[0];
    let b = &bindings[1];
    let mut cases = Vec::new();
    let mut overlap = payload.clone();
    overlap[b.offset_at..b.offset_at + 8].copy_from_slice(&(a.offset + a.length / 2).to_be_bytes());
    cases.push(("overlap", overlap));
    let mut overflow = payload.clone();
    overflow[a.offset_at..a.offset_at + 8].copy_from_slice(&(u64::MAX - 1).to_be_bytes());
    cases.push(("overflow", overflow));
    let mut truncated = payload.clone();
    truncated[b.length_at..b.length_at + 8].copy_from_slice(&(b.length - 1).to_be_bytes());
    cases.push(("truncated", truncated));
    let mut past_end = payload.clone();
    past_end[b.offset_at..b.offset_at + 8].copy_from_slice(&(b.offset + b.length).to_be_bytes());
    cases.push(("past-end", past_end));
    let mut wrong_cell = payload.clone();
    wrong_cell[a.offset_at..a.offset_at + 8].copy_from_slice(&b.offset.to_be_bytes());
    wrong_cell[a.length_at..a.length_at + 8].copy_from_slice(&b.length.to_be_bytes());
    wrong_cell[a.digest_at..a.digest_at + 32].copy_from_slice(&b.digest);
    cases.push(("wrong-cell", wrong_cell));
    let mut digest = payload.clone();
    digest[a.digest_at] ^= 1;
    cases.push(("digest", digest));
    let mut oversized_cell = payload.clone();
    oversized_cell[a.length_at..a.length_at + 8]
        .copy_from_slice(&(MAX_IMAGE_BYTES as u64 + 1).to_be_bytes());
    cases.push(("oversized-cell", oversized_cell));
    let mut unsafe_name = payload.clone();
    unsafe_name[a.name_at] = b'/';
    cases.push(("unsafe-name", unsafe_name));
    let mut old_format = payload;
    old_format[8..10].copy_from_slice(&3u16.to_be_bytes());
    cases.push(("old-format", old_format));
    for (case, payload) in cases {
        let bad = temp.path().join(format!("{case}.index"));
        std::fs::write(&bad, CrcFrame::new(&payload).as_ref()).unwrap();
        let error = NativeIndex::open_snapshot(&bad, &shape(), DATABASE, BUDGET)
            .err()
            .unwrap();
        if case == "overlap" {
            assert!(matches!(
                error,
                NativeIndexError::Invalid("overlapping cell ranges")
            ));
        }
    }
    // Even repaired CRC/digest metadata cannot substitute an empty frame for a
    // live cell while the other cell still supplies the root's latest version.
    let empty_dir = temp.path().join("empty-frame");
    std::fs::create_dir(&empty_dir).unwrap();
    let (mut manifest, _) = root_bindings(&root);
    let mut data = std::fs::read(temp.path().join(&a.name)).unwrap();
    let empty = CrcFrame::new(&0u32.to_be_bytes().to_vec());
    let start = a.range().start;
    data[start..start + empty.as_ref().len()].copy_from_slice(empty.as_ref());
    std::fs::write(empty_dir.join(&a.name), data).unwrap();
    manifest[a.length_at..a.length_at + 8]
        .copy_from_slice(&(empty.as_ref().len() as u64).to_be_bytes());
    manifest[a.digest_at..a.digest_at + 32]
        .copy_from_slice(&Blake2b::<U32>::digest(empty.as_ref()));
    let empty_root = empty_dir.join("root.index");
    std::fs::write(&empty_root, CrcFrame::new(&manifest).as_ref()).unwrap();
    assert!(matches!(
        NativeIndex::open_snapshot(&empty_root, &shape(), DATABASE, BUDGET),
        Err(NativeIndexError::Invalid("block offset count"))
    ));
}

#[cfg(target_pointer_width = "64")]
#[test]
fn shared_container_offsets_above_one_gib_do_not_raise_the_per_cell_cap() {
    use std::io::{Seek, SeekFrom, Write};
    let temp = tempdir::TempDir::new("index-large-sparse-container").unwrap();
    let log = log(&temp.path().join("wal"));
    let batch = batch(0, vec![put(0, A, b"small range")]);
    let receipt = durable(&log, std::slice::from_ref(&batch))[0];
    let mut index = NativeIndex::new(&shape(), DATABASE, BUDGET).unwrap();
    install(&mut index, &log, &batch, receipt);
    let root = temp.path().join("root.index");
    index.flush_and_unload(&root, IndexReadCut::Latest).unwrap();
    let (mut payload, bindings) = root_bindings(&root);
    let cell = &bindings[0];
    let original = std::fs::read(temp.path().join(&cell.name)).unwrap();
    let frame = &original[cell.range()];
    let sparse_dir = temp.path().join("sparse");
    std::fs::create_dir(&sparse_dir).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(sparse_dir.join(&cell.name))
        .unwrap();
    let offset = MAX_IMAGE_BYTES as u64 + 4096;
    file.set_len(offset + cell.length).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(frame).unwrap();
    file.sync_all().unwrap();
    payload[cell.offset_at..cell.offset_at + 8].copy_from_slice(&offset.to_be_bytes());
    let sparse_root = sparse_dir.join("root.index");
    std::fs::write(&sparse_root, CrcFrame::new(&payload).as_ref()).unwrap();
    let reopened = NativeIndex::open_snapshot(&sparse_root, &shape(), DATABASE, BUDGET).unwrap();
    assert_eq!(
        value(&reopened, &log, A, IndexReadCut::Latest).as_deref(),
        Some(b"small range".as_slice())
    );
    assert_eq!(
        reopened.stats().charged_metadata_bytes,
        index.stats().charged_metadata_bytes
    );
}
