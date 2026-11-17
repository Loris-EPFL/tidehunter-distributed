//! The buffered cell encoder against the frozen pre-buffering encoder: equal
//! frames, digests and summaries, a bounded scratch, and an explicit streaming
//! fallback above the scratch limit. The option C encoder of base, captured
//! carry and captured active must equal the single-overlay encoder run on one
//! overlay holding both captured overlays.

use super::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io::BufWriter;
use std::sync::Arc;

const DATABASE: [u8; 16] = [7; 16];
const EPOCH: u64 = 11;
const OFFSET: u64 = 4096;

type History = BTreeMap<Vec<u8>, Vec<ValueRecordReference>>;
/// The single-overlay merged rows of the encoder before buffering, frozen,
/// so the reference does not share code with the encoder under test.
struct LegacyRows<'a> {
    base: Option<&'a SortedBlock>,
    position: usize,
    overlay: std::iter::Peekable<
        std::collections::btree_map::Iter<'a, Bytes, NativeVersions<ValueRecordReference>>,
    >,
}

impl<'a> LegacyRows<'a> {
    fn new(cell: &'a FrozenCell) -> Self {
        Self {
            base: cell.base.as_ref(),
            position: 0,
            overlay: cell.overlay.iter().peekable(),
        }
    }
}

impl<'a> Iterator for LegacyRows<'a> {
    type Item = (&'a [u8], &'a [u8], &'a [ValueRecordReference]);
    fn next(&mut self) -> Option<Self::Item> {
        let base = self
            .base
            .filter(|base| self.position < base.count)
            .map(|base| base.entry(self.position));
        let overlay = self.overlay.peek();
        match (base, overlay) {
            (None, None) => None,
            (Some((key, versions)), None) => {
                self.position += 1;
                Some((key, versions, &[]))
            }
            (None, Some(_)) => self
                .overlay
                .next()
                .map(|(key, values)| (key.as_ref(), &[][..], values.as_slice())),
            (Some((key, versions)), Some((other, _))) => match key.cmp(other.as_ref()) {
                Ordering::Less => {
                    self.position += 1;
                    Some((key, versions, &[]))
                }
                Ordering::Greater => self
                    .overlay
                    .next()
                    .map(|(key, values)| (key.as_ref(), &[][..], values.as_slice())),
                Ordering::Equal => {
                    self.position += 1;
                    let (_, values) = self.overlay.next().unwrap();
                    Some((key, versions, values))
                }
            },
        }
    }
}

struct LegacyVersions<'a> {
    base: std::slice::ChunksExact<'a, u8>,
    overlay: std::slice::Iter<'a, ValueRecordReference>,
    a: Option<ValueRecordReference>,
    b: Option<ValueRecordReference>,
}

impl<'a> LegacyVersions<'a> {
    fn new(base: &'a [u8], overlay: &'a [ValueRecordReference]) -> Self {
        let mut result = Self {
            base: base.chunks_exact(REFERENCE_BYTES),
            overlay: overlay.iter(),
            a: None,
            b: None,
        };
        result.a = result
            .base
            .next()
            .map(|bytes| decode_reference(bytes).unwrap());
        result.b = result.overlay.next().copied();
        result
    }
}

impl Iterator for LegacyVersions<'_> {
    type Item = ValueRecordReference;
    fn next(&mut self) -> Option<Self::Item> {
        let result = match (self.a, self.b) {
            (None, None) => return None,
            (Some(a), None) => a,
            (None, Some(b)) => b,
            (Some(a), Some(b)) => {
                if a.mutation_version() <= b.mutation_version() {
                    a
                } else {
                    b
                }
            }
        };
        if self
            .a
            .is_some_and(|a| a.mutation_version() == result.mutation_version())
        {
            self.a = self
                .base
                .next()
                .map(|bytes| decode_reference(bytes).unwrap());
        }
        if self
            .b
            .is_some_and(|b| b.mutation_version() == result.mutation_version())
        {
            self.b = self.overlay.next().copied();
        }
        Some(result)
    }
}

fn legacy_retained<'a>(
    base: &'a [u8],
    overlay: &'a [ValueRecordReference],
    cut: &IndexReadCut,
    floor: &IndexReadCut,
) -> LegacyVersions<'a> {
    let count = base.len() / REFERENCE_BYTES;
    let reference =
        |n| decode_reference(&base[n * REFERENCE_BYTES..(n + 1) * REFERENCE_BYTES]).unwrap();
    let at_floor = lower_bound_by(count, |n| floor.contains(&reference(n)));
    let base_anchor = (at_floor > 0).then(|| reference(at_floor - 1));
    let overlay_anchor = matching_prefix(overlay, |value| floor.contains(value))
        .last()
        .copied();
    let anchor = newest(base_anchor, overlay_anchor).map(|value| value.mutation_version());
    let base_start = lower_bound_by(count, |n| {
        anchor.is_some_and(|anchor| reference(n).mutation_version() < anchor)
    });
    let base_end = lower_bound_by(count, |n| cut.contains(&reference(n)));
    let overlay_start = overlay
        .partition_point(|value| anchor.is_some_and(|anchor| value.mutation_version() < anchor));
    let overlay_end = overlay.partition_point(|value| cut.contains(value));
    LegacyVersions::new(
        &base[base_start * REFERENCE_BYTES..base_end * REFERENCE_BYTES],
        &overlay[overlay_start..overlay_end],
    )
}

fn legacy_payload(
    cell: &FrozenCell,
    count: usize,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    reference_bytes: &mut Vec<u8>,
    mut emit: impl FnMut(&[u8]) -> Result<(), NativeIndexError>,
) -> Result<PayloadSummary, NativeIndexError> {
    let mut length = 0usize;
    let mut write = |bytes: &[u8]| -> Result<(), NativeIndexError> {
        length = length
            .checked_add(bytes.len())
            .ok_or(NativeIndexError::Capacity)?;
        if length > MAX_IMAGE_BYTES - 8 {
            return Err(NativeIndexError::Capacity);
        }
        emit(bytes)
    };
    write(&u32::try_from(count).unwrap().to_be_bytes())?;
    let mut offset = 4 + count * 4;
    for (key, base, overlay) in LegacyRows::new(cell) {
        let n = legacy_retained(base, overlay, cut, floor).count();
        if n == 0 {
            continue;
        }
        write(&u32::try_from(offset).unwrap().to_be_bytes())?;
        offset += n * REFERENCE_BYTES + 8 + key.len();
    }
    let mut retained_bytes = 0usize;
    let mut retained_versions = 0usize;
    for (key, base, overlay) in LegacyRows::new(cell) {
        let n = legacy_retained(base, overlay, cut, floor).count();
        if n == 0 {
            continue;
        }
        write(&u32::try_from(key.len()).unwrap().to_be_bytes())?;
        write(key)?;
        write(&u32::try_from(n).unwrap().to_be_bytes())?;
        for reference in legacy_retained(base, overlay, cut, floor) {
            reference_bytes.clear();
            encode_reference(&reference, reference_bytes);
            write(reference_bytes)?;
            retained_bytes += VERSION_CHARGE + key.len();
            retained_versions += 1;
        }
    }
    Ok(PayloadSummary {
        length,
        retained_bytes,
        retained_versions,
    })
}

/// The encoder before buffering, frozen: a row count pass, then one encoding
/// for the CRC and one to hash and write. It rejects a cell without rows.
fn legacy_append_cell(
    cell: &FrozenCell,
    writer: &mut impl Write,
    path: &Path,
    offset: u64,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    reference_bytes: &mut Vec<u8>,
) -> Result<WrittenCell, NativeIndexError> {
    let count = LegacyRows::new(cell)
        .filter(|(_, base, overlay)| legacy_retained(base, overlay, cut, floor).next().is_some())
        .count();
    if count == 0 {
        return Err(NativeIndexError::Invalid("empty cell append"));
    }
    let mut crc = crc32fast::Hasher::new();
    let summary = legacy_payload(cell, count, cut, floor, reference_bytes, |bytes| {
        crc.update(bytes);
        Ok(())
    })?;
    let length = u32::try_from(summary.length).map_err(|_| NativeIndexError::Capacity)?;
    let bytes = u64::from(length) + 8;
    offset
        .checked_add(bytes)
        .ok_or(NativeIndexError::Capacity)?;
    let mut header = [0u8; 8];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4..].copy_from_slice(&crc.finalize().to_be_bytes());
    let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
    hash.update(&header);
    writer.write_all(&header)?;
    let emitted = legacy_payload(cell, count, cut, floor, reference_bytes, |bytes| {
        hash.update(bytes);
        writer.write_all(bytes)?;
        Ok(())
    })?;
    assert_eq!(emitted, summary, "immutable captured cell serialization");
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.finalize().as_bytes());
    Ok(WrittenCell {
        file: CellFile {
            path: path.to_owned(),
            offset,
            bytes,
            digest,
            floor: floor.clone(),
        },
        count,
        retained_bytes: summary.retained_bytes,
        retained_versions: summary.retained_versions,
    })
}

fn version(sequence: u64, operation_index: u32) -> MutationVersion {
    MutationVersion {
        batch: BatchVersion {
            epoch: EPOCH,
            sequence,
        },
        operation_index,
    }
}

/// `salt` changes the content but not the mutation version, so an equal
/// version in base and overlay can differ.
fn reference(
    at: MutationVersion,
    kind: RecordKind,
    key_len: usize,
    salt: u8,
) -> ValueRecordReference {
    ValueRecordReference {
        batch_id: BatchId {
            database: DATABASE,
            operation: u128::from(at.batch.sequence).to_be_bytes(),
        },
        batch_version: at.batch,
        batch_digest: PayloadDigest([salt; 32]),
        batch_address: PhysicalValueAddress {
            partition: u32::from(salt),
            segment_generation: 1,
            offset: at.batch.sequence * 64,
            len: 64,
        },
        descriptor: RecordDescriptor {
            operation_index: at.operation_index,
            offset: 0,
            len: 16,
            keyspace: 0,
            kind,
            key_len: u32::try_from(key_len).unwrap(),
            value_offset: 8,
            value_len: if kind == RecordKind::Put { 8 } else { 0 },
            checksum: MemberChecksum(u32::from(salt)),
        },
    }
}

fn overlay(history: &History) -> Arc<Overlay<ValueRecordReference>> {
    Arc::new(
        history
            .iter()
            .filter(|(_, versions)| !versions.is_empty())
            .map(|(key, versions)| {
                (
                    Bytes::copy_from_slice(key),
                    versions.iter().copied().collect(),
                )
            })
            .collect(),
    )
}

/// An immutable base holding every version of `history`, encoded by the
/// frozen encoder exactly as a persisted base.
fn base(history: &History) -> Option<SortedBlock> {
    let source = FrozenCell {
        identity: (KeySpace(0), CellId::Integer(0)),
        base: None,
        overlay: overlay(history),
    };
    let mut frame = Vec::new();
    let written = legacy_append_cell(
        &source,
        &mut frame,
        Path::new("base.cells"),
        0,
        &IndexReadCut::Latest,
        &IndexReadCut::BeforeAll,
        &mut Vec::new(),
    )
    .ok()?;
    Some(SortedBlock {
        bytes: Bytes::from(frame[8..].to_vec()),
        count: written.count,
        mapped: false,
        cut: IndexReadCut::Latest,
        file: None,
        retained_bytes: written.retained_bytes,
        retained_versions: written.retained_versions,
    })
}

fn cell(base_history: &History, overlay_history: &History) -> FrozenCell {
    FrozenCell {
        identity: (KeySpace(0), CellId::Integer(0)),
        base: base(base_history),
        overlay: overlay(overlay_history),
    }
}

fn exact_bound(cell: &FrozenCell) -> usize {
    buffered_bound(CellSources::frozen(cell), usize::MAX)
        .unwrap()
        .1
}

/// Encode with `limit` and with the frozen encoder; assert equal bytes,
/// framing, digest and summary. Returns the counters of the new encoder.
fn assert_matches_legacy(
    cell: &FrozenCell,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    limit: usize,
) -> CellEncodeStats {
    let path = Path::new("image.cells");
    let mut expected_bytes = Vec::new();
    let expected = legacy_append_cell(
        cell,
        &mut expected_bytes,
        path,
        OFFSET,
        cut,
        floor,
        &mut Vec::new(),
    );
    let mut encoder = CellEncoder::new(limit);
    let mut actual_bytes = Vec::new();
    let actual = encoder
        .append(cell, Some(&mut actual_bytes), path, OFFSET, cut, floor)
        .unwrap();
    match expected {
        Err(NativeIndexError::Invalid("empty cell append")) => {
            assert!(actual.is_none(), "legacy rejects an empty cell");
            assert!(actual_bytes.is_empty());
            assert_eq!(encoder.stats.empty_cells, 1);
        }
        Ok(expected) => {
            let actual = actual.expect("legacy wrote a frame");
            assert_eq!(actual_bytes, expected_bytes, "frame bytes");
            assert_eq!(actual.file.path, expected.file.path);
            assert_eq!(actual.file.offset, expected.file.offset);
            assert_eq!(actual.file.bytes, expected.file.bytes);
            assert_eq!(actual.file.digest, expected.file.digest);
            assert_eq!(actual.file.floor, expected.file.floor);
            assert_eq!(actual.count, expected.count);
            assert_eq!(actual.retained_bytes, expected.retained_bytes);
            assert_eq!(actual.retained_versions, expected.retained_versions);
            // Independent framing checks: CRC32 over the payload, BLAKE2b-32
            // over the final header and the payload.
            assert_eq!(actual.file.bytes, actual_bytes.len() as u64);
            let payload = &actual_bytes[8..];
            assert_eq!(&actual_bytes[..4], &(payload.len() as u32).to_be_bytes());
            assert_eq!(&actual_bytes[4..8], &crc32fast::hash(payload).to_be_bytes());
            assert_eq!(actual.file.digest, cell_digest(&actual_bytes));
            assert_eq!(&payload[..4], &(actual.count as u32).to_be_bytes());
            assert_eq!(
                encoder.stats.buffered_cells + encoder.stats.streamed_cells,
                1
            );
        }
        Err(error) => panic!("legacy encoder failed: {error:?}"),
    }
    assert!(encoder.payload.capacity() <= encoder.limit);
    encoder.stats
}

fn random_key(rng: &mut StdRng, width: Option<usize>) -> Vec<u8> {
    let len = width.unwrap_or_else(|| rng.gen_range(1..40));
    (0..len).map(|_| rng.r#gen()).collect()
}

/// Random base and overlay histories: shared and private keys, equal
/// versions with different content, puts and tombstones.
fn random_histories(rng: &mut StdRng) -> (History, History) {
    let width = rng.gen_bool(0.5).then_some(4);
    let mut base_history = History::new();
    let mut overlay_history = History::new();
    for _ in 0..rng.gen_range(0..48) {
        let key = random_key(rng, width);
        let mut versions: Vec<MutationVersion> = (0..rng.gen_range(1..=6))
            .map(|_| version(rng.gen_range(1..64), rng.gen_range(0..3)))
            .collect();
        versions.sort_unstable();
        versions.dedup();
        let mut in_base = Vec::new();
        let mut in_overlay = Vec::new();
        for at in versions {
            let kind = if rng.gen_bool(0.25) {
                RecordKind::Delete
            } else {
                RecordKind::Put
            };
            match rng.gen_range(0..5) {
                0 | 1 => in_base.push(reference(at, kind, key.len(), 0)),
                2 | 3 => in_overlay.push(reference(at, kind, key.len(), 0)),
                _ => {
                    in_base.push(reference(at, kind, key.len(), 0));
                    in_overlay.push(reference(at, kind, key.len(), 1));
                }
            }
        }
        if !in_base.is_empty() {
            base_history.insert(key.clone(), in_base);
        }
        if !in_overlay.is_empty() {
            overlay_history.insert(key, in_overlay);
        }
    }
    (base_history, overlay_history)
}

fn random_cut(rng: &mut StdRng) -> (IndexReadCut, IndexReadCut) {
    if rng.gen_bool(0.05) {
        return (IndexReadCut::BeforeAll, IndexReadCut::BeforeAll);
    }
    let cut = version(rng.gen_range(0..70), rng.gen_range(0..3));
    let floor = if rng.gen_bool(0.4) {
        IndexReadCut::BeforeAll
    } else {
        IndexReadCut::At(
            version(rng.gen_range(0..=cut.batch.sequence), rng.gen_range(0..3)).min(cut),
        )
    };
    (IndexReadCut::At(cut), floor)
}

#[test]
fn random_cells_match_legacy_bytes_digests_and_summaries() {
    let mut rng = StdRng::seed_from_u64(0x5eed_00c1);
    let (mut buffered, mut empty, mut gaps) = (0, 0, 0);
    for round in 0..600 {
        let (base_history, overlay_history) = random_histories(&mut rng);
        let cell = cell(&base_history, &overlay_history);
        let (cut, floor) = random_cut(&mut rng);
        let bound = exact_bound(&cell);
        let stats = assert_matches_legacy(&cell, &cut, &floor, CELL_SCRATCH_CEILING);
        // Every random cell fits the ceiling, so none streams.
        assert_eq!(stats.streamed_cells, 0, "round {round}");
        assert_eq!(stats.row_passes, 1, "round {round}");
        buffered += stats.buffered_cells;
        empty += stats.empty_cells;
        gaps += u64::from(stats.moved_bytes != 0);
        let streamed = assert_matches_legacy(&cell, &cut, &floor, 0);
        assert_eq!(streamed.buffered_cells, 0, "round {round}");
        let exact = assert_matches_legacy(&cell, &cut, &floor, bound);
        assert_eq!(exact.streamed_cells, 0, "round {round}");
    }
    // The generator covers all three outcomes.
    assert!(
        buffered > 300 && empty > 10 && gaps > 100,
        "{buffered} {empty} {gaps}"
    );
}

#[test]
fn rows_without_retained_versions_write_no_frame() {
    let key = vec![1, 2, 3, 4];
    let mut later = History::new();
    later.insert(
        key.clone(),
        vec![reference(version(9, 0), RecordKind::Put, 4, 0)],
    );
    let cut = IndexReadCut::At(version(8, 0));
    let floor = IndexReadCut::BeforeAll;
    // No cell rows, a before-all cut and only post-cut versions are all empty.
    for (cell, cut) in [
        (cell(&History::new(), &History::new()), cut.clone()),
        (cell(&later, &History::new()), IndexReadCut::BeforeAll),
        (cell(&History::new(), &later), cut.clone()),
        (cell(&later, &later), cut),
    ] {
        for limit in [CELL_SCRATCH_CEILING, 0] {
            let stats = assert_matches_legacy(&cell, &cut, &floor, limit);
            assert_eq!(stats.empty_cells, 1);
            assert_eq!(stats.encode_passes, 0);
            assert_eq!(stats.row_passes, 1);
        }
    }
}

#[test]
fn single_row_and_tombstone_only_rows_are_written() {
    let mut single = History::new();
    single.insert(
        vec![5],
        vec![reference(version(1, 0), RecordKind::Put, 1, 0)],
    );
    let mut tombstones = History::new();
    for key in 0..4u8 {
        tombstones.insert(
            vec![key, key],
            vec![reference(
                version(u64::from(key) + 1, 0),
                RecordKind::Delete,
                2,
                0,
            )],
        );
    }
    let cut = IndexReadCut::At(version(10, 0));
    for history in [&single, &tombstones] {
        for (base_history, overlay_history) in
            [(history, &History::new()), (&History::new(), history)]
        {
            let cell = cell(base_history, overlay_history);
            let stats =
                assert_matches_legacy(&cell, &cut, &IndexReadCut::BeforeAll, CELL_SCRATCH_CEILING);
            assert_eq!(stats.buffered_cells, 1);
            assert_eq!(stats.references_encoded, history.len() as u64);
        }
    }
}

#[test]
fn floor_anchor_and_later_versions_are_retained() {
    for anchor_kind in [RecordKind::Put, RecordKind::Delete] {
        let key = vec![3; 8];
        let kind = |sequence| {
            if sequence == 5 {
                anchor_kind
            } else {
                RecordKind::Put
            }
        };
        let mut base_history = History::new();
        let mut overlay_history = History::new();
        base_history.insert(
            key.clone(),
            (1..=6)
                .map(|s| reference(version(s, 0), kind(s), 8, 0))
                .collect(),
        );
        overlay_history.insert(
            key,
            (4..=12)
                .map(|s| reference(version(s, 0), kind(s), 8, 1))
                .collect(),
        );
        let cell = cell(&base_history, &overlay_history);
        let cut = IndexReadCut::At(version(8, 0));
        let floor = IndexReadCut::At(version(5, 0));
        let stats = assert_matches_legacy(&cell, &cut, &floor, CELL_SCRATCH_CEILING);
        // Anchor 5, then 6, 7 and 8; equal base and overlay versions once.
        assert_eq!(stats.references_encoded, 4);
        assert_matches_legacy(&cell, &cut, &floor, 0);
    }
}

#[test]
fn maximum_rows_long_histories_and_gap_compaction_match_legacy() {
    let mut rng = StdRng::seed_from_u64(7);
    // Many single-version rows, near the ceiling.
    let mut many = History::new();
    while many.len() < 40_000 {
        let key = random_key(&mut rng, Some(16));
        many.insert(key, vec![reference(version(1, 0), RecordKind::Put, 16, 0)]);
    }
    let cut = IndexReadCut::At(version(100, 0));
    let floor = IndexReadCut::BeforeAll;
    let many_cell = cell(&History::new(), &many);
    assert!(exact_bound(&many_cell) > CELL_SCRATCH_CEILING / 2);
    let stats = assert_matches_legacy(&many_cell, &cut, &floor, CELL_SCRATCH_CEILING);
    assert_eq!((stats.buffered_cells, stats.moved_bytes), (1, 0));
    // One key with a long merged history.
    let key = vec![1; 4];
    let mut long_base = History::new();
    let mut long_overlay = History::new();
    long_base.insert(
        key.clone(),
        (1..4000)
            .step_by(2)
            .map(|s| reference(version(s, 0), RecordKind::Put, 4, 0))
            .collect(),
    );
    long_overlay.insert(
        key,
        (2..4000)
            .step_by(2)
            .map(|s| reference(version(s, 1), RecordKind::Put, 4, 0))
            .collect(),
    );
    let long = cell(&long_base, &long_overlay);
    let cut = IndexReadCut::At(version(3000, 0));
    let stats = assert_matches_legacy(
        &long,
        &cut,
        &IndexReadCut::At(version(10, 0)),
        CELL_SCRATCH_CEILING,
    );
    assert_eq!(stats.buffered_cells, 1);
    // Most candidate rows are after the cut: their directory slots are
    // dropped by one body move.
    let mut sparse = History::new();
    for n in 0..2000u32 {
        let sequence = if n % 100 == 0 { 1 } else { 50 };
        sparse.insert(
            n.to_be_bytes().to_vec(),
            vec![reference(version(sequence, 0), RecordKind::Put, 4, 0)],
        );
    }
    let sparse = cell(&History::new(), &sparse);
    let stats = assert_matches_legacy(
        &sparse,
        &IndexReadCut::At(version(10, 0)),
        &floor,
        CELL_SCRATCH_CEILING,
    );
    assert_eq!(stats.buffered_cells, 1);
    // Twenty rows of 4 + 4 + 4 + 134 bytes moved over 1980 slots.
    assert_eq!(stats.moved_bytes, 20 * 146);
}

#[test]
fn bound_selects_buffered_at_and_above_and_streaming_below() {
    let mut rng = StdRng::seed_from_u64(11);
    let mut checked = 0;
    while checked < 50 {
        let (base_history, overlay_history) = random_histories(&mut rng);
        let cell = cell(&base_history, &overlay_history);
        let cut = IndexReadCut::At(version(64, 0));
        let floor = IndexReadCut::BeforeAll;
        let bound = exact_bound(&cell);
        if bound <= 4 {
            continue;
        }
        let below = assert_matches_legacy(&cell, &cut, &floor, bound - 1);
        assert_eq!((below.buffered_cells, below.streamed_cells), (0, 1));
        assert_eq!(below.encode_passes, 2);
        assert_eq!(below.row_passes, 5);
        assert_eq!(below.scratch_capacity, 0);
        for limit in [bound, bound + 1] {
            let stats = assert_matches_legacy(&cell, &cut, &floor, limit);
            // One canonical encoding, one row traversal and one encoding of
            // each retained reference; no count or second encoding pass.
            assert_eq!((stats.buffered_cells, stats.streamed_cells), (1, 0));
            assert_eq!((stats.encode_passes, stats.row_passes), (1, 1));
            assert_eq!(stats.references_encoded, below.references_encoded / 2);
            assert!(stats.scratch_capacity <= bound as u64);
        }
        checked += 1;
    }
}

#[test]
fn streaming_counts_a_conservative_fallback_when_the_payload_fits() {
    // Every overlay version is after the cut except one, so the bound is far
    // above the final payload.
    let mut history = History::new();
    for n in 0..64u32 {
        let sequence = if n == 0 { 1 } else { 50 };
        history.insert(
            n.to_be_bytes().to_vec(),
            vec![reference(version(sequence, 0), RecordKind::Put, 4, 0)],
        );
    }
    let cell = cell(&History::new(), &history);
    let cut = IndexReadCut::At(version(10, 0));
    let bound = exact_bound(&cell);
    let payload = 4 + 4 + 4 + 4 + 4 + REFERENCE_BYTES;
    let limit = bound / 2;
    assert!(payload < limit);
    let stats = assert_matches_legacy(&cell, &cut, &IndexReadCut::BeforeAll, limit);
    assert_eq!(stats.streamed_cells, 1);
    assert_eq!(stats.conservative_fallbacks, 1);
    assert_eq!(stats.streamed_bytes, payload as u64 + 8);
    // A payload above the limit is not a conservative fallback.
    let stats = assert_matches_legacy(&cell, &cut, &IndexReadCut::BeforeAll, payload - 1);
    assert_eq!((stats.streamed_cells, stats.conservative_fallbacks), (1, 0));
}

#[test]
fn scratch_is_reused_and_never_exceeds_the_limit() {
    let mut rng = StdRng::seed_from_u64(13);
    let limit = 16 * 1024;
    let mut encoder = CellEncoder::new(limit);
    let mut output = Vec::new();
    let mut peak = 0;
    let mut written = 0u64;
    for _ in 0..400 {
        let (base_history, overlay_history) = random_histories(&mut rng);
        let cell = cell(&base_history, &overlay_history);
        let (cut, floor) = random_cut(&mut rng);
        let before = output.len() as u64;
        if let Some(cell) = encoder
            .append(
                &cell,
                Some(&mut output),
                Path::new("x.cells"),
                before,
                &cut,
                &floor,
            )
            .unwrap()
        {
            assert_eq!(cell.file.offset, before);
            written += cell.file.bytes;
        }
        assert!(encoder.payload.capacity() <= limit);
        // Lengths are cleared between cells; capacity is kept.
        assert!(encoder.payload.capacity() >= peak);
        peak = encoder.payload.capacity();
    }
    let stats = encoder.stats;
    assert!(stats.buffered_cells > 0 && stats.streamed_cells > 0);
    assert_eq!(stats.buffered_bytes + stats.streamed_bytes, written);
    assert_eq!(written, output.len() as u64);
    assert_eq!(stats.scratch_capacity, peak as u64);
    assert!(stats.scratch_capacity <= limit as u64);
    assert_eq!(
        stats.encode_passes,
        stats.buffered_cells + 2 * stats.streamed_cells
    );
}

#[test]
fn scratch_limit_is_one_flusher_share_of_a_quarter_of_the_index_budget() {
    const MIB: usize = 1 << 20;
    assert_eq!(cell_scratch_limit(32 * MIB, 8), MIB);
    assert_eq!(cell_scratch_limit(MIB, 1), MIB / 4);
    assert_eq!(cell_scratch_limit(MIB, 0), MIB / 4);
    assert_eq!(cell_scratch_limit(512 * MIB, 1), CELL_SCRATCH_CEILING);
    for budget in [1, 4095, MIB, 7 * MIB + 3, 512 * MIB] {
        for flushers in 1..=cell_contract::MAX_CELL_FLUSHERS {
            let limit = cell_scratch_limit(budget, flushers);
            assert!(limit * flushers <= budget / 4);
            assert!(limit <= MAX_IMAGE_BYTES - 8);
        }
    }
    // Capture binds the index budget and the flusher count of its shards.
    for (budget, shards) in [(MIB, DEFAULT_INDEX_SHARDS), (64 * MIB, 1)] {
        let index = NativeIndex::with_shards(&plain_shape(), DATABASE, budget, shards).unwrap();
        let prepared = index
            .writer()
            .prepare_compacted_image(IndexReadCut::BeforeAll, IndexReadCut::BeforeAll)
            .unwrap();
        assert_eq!(
            prepared.scratch_limit,
            cell_scratch_limit(budget, cell_flushers(shards))
        );
    }
}

#[test]
fn limits_are_clamped_and_overflowing_bounds_stream() {
    assert_eq!(CellEncoder::new(usize::MAX).limit, CELL_SCRATCH_CEILING);
    let impossible = FrozenCell {
        identity: (KeySpace(0), CellId::Integer(0)),
        base: Some(SortedBlock {
            bytes: Bytes::from(vec![0; 8]),
            count: usize::MAX,
            mapped: false,
            cut: IndexReadCut::Latest,
            file: None,
            retained_bytes: 0,
            retained_versions: 0,
        }),
        overlay: overlay(&BTreeMap::from([(
            vec![1],
            vec![reference(version(1, 0), RecordKind::Put, 1, 0)],
        )])),
    };
    // The candidate row count overflows: streaming is chosen before encoding.
    assert_eq!(
        buffered_bound(CellSources::frozen(&impossible), usize::MAX),
        None
    );
    let mut long = History::new();
    long.insert(
        vec![1],
        vec![reference(version(1, 0), RecordKind::Put, 1, 0)],
    );
    let small = cell(&History::new(), &long);
    assert_eq!(
        buffered_bound(CellSources::frozen(&small), 4 + 12 + 1 + REFERENCE_BYTES),
        Some((1, 151))
    );
    assert_eq!(buffered_bound(CellSources::frozen(&small), 150), None);
}

/// One overlay holding the versions of both captured overlays, ascending.
fn merged(carry: &History, active: &History) -> History {
    let mut merged = carry.clone();
    for (key, versions) in active {
        let row = merged.entry(key.clone()).or_default();
        row.extend_from_slice(versions);
        row.sort_by_key(|value| value.mutation_version());
    }
    merged
}

/// Conservative bound `U` of base, carry and active.
fn source_bound(base: Option<&SortedBlock>, carry: Option<&History>, active: &History) -> usize {
    let carry = carry.map(overlay);
    let active = overlay(active);
    let sources = CellSources {
        base,
        overlays: [carry.as_deref(), Some(&*active)],
    };
    buffered_bound(sources, usize::MAX).unwrap().1
}

/// Encode base, captured carry and captured active through the option C
/// entry point with `limit`. Assert the frame, its framing, its summary and
/// the single `choose` call equal the single-overlay encoder (itself checked
/// against the frozen encoder) run on the merged overlay. Returns the
/// counters of the option C encoding.
fn assert_source_matches_merged(
    base: Option<&SortedBlock>,
    carry: Option<&History>,
    active: &History,
    cut: &IndexReadCut,
    floor: &IndexReadCut,
    limit: usize,
) -> CellEncodeStats {
    let identity = (KeySpace(0), CellId::Integer(0));
    let empty = History::new();
    let single = FrozenCell {
        identity: identity.clone(),
        base: base.cloned(),
        overlay: overlay(&merged(carry.unwrap_or(&empty), active)),
    };
    assert_matches_legacy(&single, cut, floor, CELL_SCRATCH_CEILING);
    let path = Path::new("image.cells");
    let mut expected_bytes = Vec::new();
    let expected = CellEncoder::new(CELL_SCRATCH_CEILING)
        .append(&single, Some(&mut expected_bytes), path, OFFSET, cut, floor)
        .unwrap();

    let carry = carry.map(overlay);
    let active = overlay(active);
    let source = CellEncodeSource {
        identity: &identity,
        base,
        carry: carry.as_deref(),
        active: &active,
        cut: cut.clone(),
        floor: floor.clone(),
    };
    let mut encoder = CellEncoder::new(limit);
    let mut actual_bytes = Vec::new();
    let mut chosen = Vec::new();
    let (output, lengths) = (&mut actual_bytes, &mut chosen);
    let frame = encoder
        .encode_source(&source, move |length| {
            lengths.push(length);
            Ok((output, OFFSET))
        })
        .unwrap();
    match expected {
        None => {
            assert_eq!(frame, None, "the merged overlay retains no row");
            assert!(actual_bytes.is_empty() && chosen.is_empty());
            assert_eq!(encoder.stats.empty_cells, 1);
        }
        Some(expected) => {
            let frame = frame.expect("the merged overlay wrote a frame");
            assert_eq!(actual_bytes, expected_bytes, "frame bytes");
            assert_eq!(chosen, [frame.bytes], "length before write, once");
            assert_eq!(frame.offset, OFFSET);
            assert_eq!(frame.bytes, expected.file.bytes);
            assert_eq!(frame.digest, expected.file.digest);
            assert_eq!(frame.count, expected.count);
            assert_eq!(frame.retained_bytes, expected.retained_bytes);
            assert_eq!(frame.retained_versions, expected.retained_versions);
            assert_eq!(frame.bytes, actual_bytes.len() as u64);
            let payload = &actual_bytes[8..];
            assert_eq!(&actual_bytes[..4], &(payload.len() as u32).to_be_bytes());
            assert_eq!(&actual_bytes[4..8], &crc32fast::hash(payload).to_be_bytes());
            assert_eq!(frame.digest, cell_digest(&actual_bytes));
            assert_eq!(&payload[..4], &(frame.count as u32).to_be_bytes());
            assert_eq!(
                encoder.stats.buffered_cells + encoder.stats.streamed_cells,
                1
            );
        }
    }
    assert!(encoder.payload.capacity() <= encoder.limit);
    assert_eq!(encoder.stats(), encoder.stats);
    encoder.stats
}

/// Random base, carry and active histories with distinct versions across
/// the two overlays. A base version can repeat in one overlay with other
/// content; the base copy wins, as in the single-overlay encoder. The share
/// of overlay versions sent to the carry varies by round: none, all, half or
/// random.
fn random_split(rng: &mut StdRng) -> (History, History, History) {
    let width = rng.gen_bool(0.5).then_some(4);
    let carry_share = match rng.gen_range(0..4) {
        0 => 0.0,
        1 => 1.0,
        2 => 0.5,
        _ => rng.r#gen(),
    };
    let with_base = rng.gen_bool(0.7);
    let (mut base, mut carry, mut active) = (History::new(), History::new(), History::new());
    for _ in 0..rng.gen_range(0..48) {
        let key = random_key(rng, width);
        let mut versions: Vec<MutationVersion> = (0..rng.gen_range(1..=6))
            .map(|_| version(rng.gen_range(1..64), rng.gen_range(0..3)))
            .collect();
        versions.sort_unstable();
        versions.dedup();
        for at in versions {
            let kind = if rng.gen_bool(0.25) {
                RecordKind::Delete
            } else {
                RecordKind::Put
            };
            let in_base = with_base && rng.gen_bool(0.4);
            if in_base {
                base.entry(key.clone())
                    .or_default()
                    .push(reference(at, kind, key.len(), 0));
            }
            if !in_base || rng.gen_bool(0.2) {
                let target = if rng.gen_bool(carry_share) {
                    &mut carry
                } else {
                    &mut active
                };
                target
                    .entry(key.clone())
                    .or_default()
                    .push(reference(at, kind, key.len(), 1));
            }
        }
    }
    (base, carry, active)
}

#[test]
fn captured_sources_match_the_merged_overlay_for_random_histories() {
    let mut rng = StdRng::seed_from_u64(0x5eed_00c3);
    let (mut buffered, mut empty, mut gaps, mut split) = (0, 0, 0, 0);
    for round in 0..600 {
        let (base_history, carry, active) = random_split(&mut rng);
        let base = base(&base_history);
        let carry = (!carry.is_empty() || rng.gen_bool(0.5)).then_some(carry);
        split += u64::from(carry.as_ref().is_some_and(|c| !c.is_empty()) && !active.is_empty());
        let (cut, floor) = random_cut(&mut rng);
        let run = |limit| {
            assert_source_matches_merged(
                base.as_ref(),
                carry.as_ref(),
                &active,
                &cut,
                &floor,
                limit,
            )
        };
        let stats = run(CELL_SCRATCH_CEILING);
        assert_eq!(stats.streamed_cells, 0, "round {round}");
        assert_eq!(stats.row_passes, 1, "round {round}");
        buffered += stats.buffered_cells;
        empty += stats.empty_cells;
        gaps += u64::from(stats.moved_bytes != 0);
        assert_eq!(run(0).buffered_cells, 0, "round {round}");
        let bound = source_bound(base.as_ref(), carry.as_ref(), &active);
        if bound > 4 {
            let below = run(bound - 1);
            assert_eq!(below.buffered_cells, 0, "round {round}");
        }
        for limit in [bound, bound + 1] {
            let stats = run(limit);
            assert_eq!(
                (stats.streamed_cells, stats.row_passes),
                (0, 1),
                "round {round}"
            );
            assert!(stats.scratch_capacity <= bound as u64);
        }
    }
    assert!(
        buffered > 300 && empty > 10 && gaps > 100 && split > 100,
        "{buffered} {empty} {gaps} {split}"
    );
}

#[test]
fn every_partition_of_a_history_matches_the_merged_overlay() {
    // Five versions of one key, the third a tombstone, plus a base-only key.
    let key = vec![9; 4];
    let kind = |sequence| {
        if sequence == 3 {
            RecordKind::Delete
        } else {
            RecordKind::Put
        }
    };
    let floors = [
        IndexReadCut::BeforeAll,
        IndexReadCut::At(version(2, 0)),
        // The tombstone is the anchor at the history floor.
        IndexReadCut::At(version(3, 0)),
    ];
    let cuts = [
        IndexReadCut::BeforeAll,
        IndexReadCut::At(version(3, 0)),
        IndexReadCut::At(version(4, 0)),
        IndexReadCut::At(version(5, u32::MAX)),
    ];
    let mut checked = 0;
    for assignment in 0..3u32.pow(5) {
        let (mut base_history, mut carry, mut active) =
            (History::new(), History::new(), History::new());
        base_history.insert(
            vec![1; 4],
            vec![reference(version(1, 1), RecordKind::Put, 4, 0)],
        );
        for sequence in 1..=5u64 {
            let target = match assignment / 3u32.pow(sequence as u32 - 1) % 3 {
                0 => &mut base_history,
                1 => &mut carry,
                _ => &mut active,
            };
            target.entry(key.clone()).or_default().push(reference(
                version(sequence, 0),
                kind(sequence),
                4,
                2,
            ));
        }
        let base = base(&base_history);
        let carries = if carry.is_empty() {
            vec![None, Some(&carry)]
        } else {
            vec![Some(&carry)]
        };
        for carry in carries {
            for cut in &cuts {
                for floor in &floors {
                    if retention::position(floor) > retention::position(cut) {
                        continue;
                    }
                    for limit in [CELL_SCRATCH_CEILING, 0] {
                        assert_source_matches_merged(
                            base.as_ref(),
                            carry,
                            &active,
                            cut,
                            floor,
                            limit,
                        );
                        checked += 1;
                    }
                }
            }
        }
    }
    assert!(checked > 3000, "{checked}");
    // All five versions in the overlays, split by parity: the tombstone
    // anchor at the floor and every later version through the cut are kept.
    let mut carry = History::new();
    let mut active = History::new();
    for sequence in 1..=5u64 {
        let target = if sequence % 2 == 0 {
            &mut carry
        } else {
            &mut active
        };
        target.entry(key.clone()).or_default().push(reference(
            version(sequence, 0),
            kind(sequence),
            4,
            2,
        ));
    }
    let stats = assert_source_matches_merged(
        None,
        Some(&carry),
        &active,
        &IndexReadCut::At(version(5, 0)),
        &IndexReadCut::At(version(3, 0)),
        CELL_SCRATCH_CEILING,
    );
    assert_eq!(stats.references_encoded, 3);
}

#[test]
fn sparse_post_cut_references_stay_out_of_the_frame() {
    // The carry holds published sparse references above the cut; the active
    // overlay holds references through it. One carry-only key has nothing
    // through the cut, so its directory slot is dropped.
    let cut = IndexReadCut::At(version(20, u32::MAX));
    let mut carry = History::new();
    let mut active = History::new();
    for n in 0..64u32 {
        let key = n.to_be_bytes().to_vec();
        carry.insert(
            key.clone(),
            vec![reference(
                version(30 + u64::from(n), 0),
                RecordKind::Put,
                4,
                1,
            )],
        );
        if n != 7 {
            active.insert(
                key,
                vec![reference(
                    version(1 + u64::from(n % 20), 0),
                    RecordKind::Put,
                    4,
                    0,
                )],
            );
        }
    }
    for limit in [CELL_SCRATCH_CEILING, 0] {
        let stats = assert_source_matches_merged(
            None,
            Some(&carry),
            &active,
            &cut,
            &IndexReadCut::BeforeAll,
            limit,
        );
        assert_eq!(
            stats.references_encoded,
            63 * if limit == 0 { 2 } else { 1 }
        );
    }
    let stats = assert_source_matches_merged(
        None,
        Some(&carry),
        &active,
        &cut,
        &IndexReadCut::BeforeAll,
        CELL_SCRATCH_CEILING,
    );
    // 128 candidate rows, 63 emitted: 65 slots dropped by one body move.
    assert_eq!(stats.moved_bytes, 63 * (4 + 4 + 4 + REFERENCE_BYTES as u64));
}

#[test]
fn long_histories_split_across_sources_match_the_merged_overlay() {
    let key = vec![1; 4];
    let (mut base_history, mut carry, mut active) =
        (History::new(), History::new(), History::new());
    for sequence in 1..4000u64 {
        let target = match sequence % 4 {
            1 | 3 => &mut base_history,
            2 => &mut carry,
            _ => &mut active,
        };
        target.entry(key.clone()).or_default().push(reference(
            version(sequence, 0),
            RecordKind::Put,
            4,
            0,
        ));
    }
    let base = base(&base_history);
    let bound = source_bound(base.as_ref(), Some(&carry), &active);
    for floor in [
        IndexReadCut::BeforeAll,
        IndexReadCut::At(version(10, 0)),
        IndexReadCut::At(version(2999, 0)),
    ] {
        let cut = IndexReadCut::At(version(3000, 0));
        for limit in [CELL_SCRATCH_CEILING, 0, bound - 1, bound, bound + 1] {
            let stats = assert_source_matches_merged(
                base.as_ref(),
                Some(&carry),
                &active,
                &cut,
                &floor,
                limit,
            );
            let streamed = limit < bound;
            assert_eq!(stats.streamed_cells, u64::from(streamed));
            assert_eq!(stats.encode_passes, if streamed { 2 } else { 1 });
        }
    }
}

#[test]
fn versions_repeated_across_overlays_fail_before_any_write() {
    let cut = IndexReadCut::At(version(10, 0));
    let duplicate = |first_row: bool, first_version: bool| {
        let mut carry = History::new();
        let mut active = History::new();
        active.insert(
            vec![0],
            vec![reference(version(1, 0), RecordKind::Put, 1, 0)],
        );
        let key = if first_row { vec![0] } else { vec![5] };
        let at = if first_version { 0 } else { 2 };
        active.entry(key.clone()).or_default().push(reference(
            version(4, at),
            RecordKind::Put,
            1,
            0,
        ));
        carry.insert(key, vec![reference(version(4, at), RecordKind::Put, 1, 1)]);
        for versions in active.values_mut() {
            versions.sort_by_key(|value| value.mutation_version());
        }
        (carry, active)
    };
    for (first_row, first_version) in [(true, false), (false, true), (false, false)] {
        let (carry, active) = duplicate(first_row, first_version);
        let identity = (KeySpace(0), CellId::Integer(0));
        let (carry, active) = (overlay(&carry), overlay(&active));
        for limit in [CELL_SCRATCH_CEILING, 0] {
            let source = CellEncodeSource {
                identity: &identity,
                base: None,
                carry: Some(&carry),
                active: &active,
                cut: cut.clone(),
                floor: IndexReadCut::BeforeAll,
            };
            let mut output = Vec::new();
            let mut called = false;
            let (sink, flag) = (&mut output, &mut called);
            let result = CellEncoder::new(limit).encode_source(&source, move |_| {
                *flag = true;
                Ok((sink, 0))
            });
            assert!(
                matches!(
                    result,
                    Err(NativeIndexError::Invalid(
                        "version repeated across overlay sources"
                    ))
                ),
                "{first_row} {first_version} {limit}"
            );
            assert!(!called && output.is_empty());
        }
    }
}

#[test]
fn heap_bases_of_captures_pass_frame_validation() {
    let shape = plain_shape();
    let index = NativeIndex::new(&shape, DATABASE, 1 << 20).unwrap();
    let mut rng = StdRng::seed_from_u64(23);
    let mut history = History::new();
    while history.len() < 200 {
        let key = random_key(&mut rng, Some(4));
        history.insert(key, Vec::new());
    }
    let cells = cells_of(&index, &history);
    let (id, keys) = cells
        .iter()
        .max_by_key(|(_, keys)| keys.len())
        .expect("routed cells");
    let (mut carry, mut active) = (History::new(), History::new());
    for (n, key) in keys.keys().enumerate() {
        let n = n as u64;
        let kind = if n.is_multiple_of(5) {
            RecordKind::Delete
        } else {
            RecordKind::Put
        };
        carry.insert(
            key.clone(),
            vec![reference(version(2 * n + 1, 0), kind, 4, 0)],
        );
        active.insert(
            key.clone(),
            vec![reference(version(2 * n + 2, 0), kind, 4, 0)],
        );
    }
    let identity = (KeySpace(0), id.clone());
    let (carry, active) = (overlay(&carry), overlay(&active));
    for cut in [IndexReadCut::BeforeAll, IndexReadCut::At(version(1000, 0))] {
        let source = CellEncodeSource {
            identity: &identity,
            base: None,
            carry: Some(&carry),
            active: &active,
            cut,
            floor: IndexReadCut::BeforeAll,
        };
        encode_heap_base(&index, &source).unwrap();
        let mut frame = Vec::new();
        let encoded = CellEncoder::new(CELL_SCRATCH_CEILING)
            .encode_source(&source, |_| Ok((&mut frame, 0)))
            .unwrap();
        let Some(encoded) = encoded else {
            assert_eq!(source.cut, IndexReadCut::BeforeAll);
            continue;
        };
        let block = SortedBlock::validated(
            Bytes::from(frame).slice(8..),
            false,
            DATABASE,
            KeySpace(0),
            index.descriptor(KeySpace(0)).unwrap(),
            id,
            source.cut.clone(),
        )
        .unwrap();
        assert_eq!(block.count, encoded.count);
        assert_eq!(block.count, keys.len());
        assert_eq!(block.retained_bytes, encoded.retained_bytes);
        assert_eq!(block.retained_versions, encoded.retained_versions);
        assert_eq!(block.retained_versions, 2 * keys.len());
    }
}

fn plain_shape() -> KeyShape {
    let mut builder = crate::key_shape::KeyShapeBuilder::new();
    builder.add_key_space("plain", 4, 1, KeyType::uniform(4));
    builder.build()
}

/// Group `history` into the cells of keyspace 0 of `index`.
fn cells_of(index: &NativeIndex, history: &History) -> BTreeMap<CellId, History> {
    let descriptor = index.descriptor(KeySpace(0)).unwrap();
    let mut cells: BTreeMap<CellId, History> = BTreeMap::new();
    for (key, versions) in history {
        cells
            .entry(descriptor.cell_id(key))
            .or_default()
            .insert(key.clone(), versions.clone());
    }
    cells
}

/// An image with the given frozen cells, cut and floor, persisted with
/// `limit`. Returns the root bytes, the container bytes and the image.
fn persist_cells(
    shape: &KeyShape,
    dir: &Path,
    cells: Vec<FrozenCell>,
    cut: IndexReadCut,
    floor: IndexReadCut,
    limit: usize,
) -> (Vec<u8>, Vec<u8>, PersistedIndexImage) {
    let index = NativeIndex::new(shape, DATABASE, 1 << 20).unwrap();
    let mut prepared = index
        .writer()
        .prepare_compacted_image(IndexReadCut::BeforeAll, IndexReadCut::BeforeAll)
        .unwrap();
    prepared.cells = cells;
    prepared.cut = cut;
    prepared.lease.floor = floor;
    prepared.scratch_limit = limit;
    let root = dir.join("image.index");
    let persisted = prepared.persist(&root).unwrap();
    (
        std::fs::read(&root).unwrap(),
        std::fs::read(dir.join("image.index.cells")).unwrap(),
        persisted,
    )
}

#[test]
fn persisted_images_are_identical_for_buffered_and_streamed_cells() {
    let shape = plain_shape();
    let probe = NativeIndex::new(&shape, DATABASE, 1 << 20).unwrap();
    let mut rng = StdRng::seed_from_u64(17);
    let mut history = History::new();
    for _ in 0..400 {
        let key = random_key(&mut rng, Some(4));
        let mut versions: Vec<MutationVersion> = (0..rng.gen_range(1..=4))
            .map(|_| version(rng.gen_range(1..40), rng.gen_range(0..2)))
            .collect();
        versions.sort_unstable();
        versions.dedup();
        let kind = if rng.gen_bool(0.2) {
            RecordKind::Delete
        } else {
            RecordKind::Put
        };
        history.insert(
            key,
            versions
                .into_iter()
                .map(|at| reference(at, kind, 4, 0))
                .collect(),
        );
    }
    let first = cells_of(&probe, &history);
    let frozen = |cells: &BTreeMap<CellId, History>| -> Vec<FrozenCell> {
        cells
            .iter()
            .map(|(id, history)| FrozenCell {
                identity: (KeySpace(0), id.clone()),
                base: None,
                overlay: overlay(history),
            })
            .collect()
    };
    let cut = IndexReadCut::At(version(30, 1));
    let floor = IndexReadCut::At(version(5, 0));
    let temp = tempdir::TempDir::new("cell-encode").unwrap();
    let mut images = Vec::new();
    for (name, limit) in [("buffered", CELL_SCRATCH_CEILING), ("streamed", 0)] {
        let dir = temp.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        images.push(persist_cells(
            &shape,
            &dir,
            frozen(&first),
            cut.clone(),
            floor.clone(),
            limit,
        ));
        super::open(&dir.join("image.index"), &shape, DATABASE, 1 << 20, 1).unwrap();
    }
    let (streamed, buffered) = (images.pop().unwrap(), images.pop().unwrap());
    assert_eq!(buffered.0, streamed.0, "root bytes");
    assert_eq!(buffered.1, streamed.1, "container bytes");
    assert_eq!(buffered.2.metadata.digest, streamed.2.metadata.digest);
    assert_eq!(buffered.2.encode_stats.streamed_cells, 0);
    assert_eq!(streamed.2.encode_stats.buffered_cells, 0);
    assert_eq!(
        buffered.2.encode_stats.buffered_cells,
        streamed.2.encode_stats.streamed_cells
    );
    assert!(buffered.2.encode_stats.buffered_cells > 1);

    // Rewrite every cell from its mapped base plus newer overlay versions,
    // with a higher floor that discards old history.
    let mut newer = History::new();
    for key in history.keys().step_by(3) {
        newer.insert(
            key.clone(),
            vec![reference(version(35, 0), RecordKind::Put, 4, 2)],
        );
    }
    let newer = cells_of(&probe, &newer);
    let rewritten = || -> Vec<FrozenCell> {
        buffered
            .2
            .cells
            .iter()
            .map(|cell| {
                let base = cell.base.clone().expect("persisted cell base");
                assert!(base.mapped);
                FrozenCell {
                    identity: cell.identity.clone(),
                    base: Some(base),
                    overlay: overlay(newer.get(&cell.identity.1).unwrap_or(&History::new())),
                }
            })
            .collect()
    };
    let cut = IndexReadCut::At(version(36, 0));
    let floor = IndexReadCut::At(version(20, 0));
    let mut images = Vec::new();
    for (name, limit) in [
        ("second-buffered", CELL_SCRATCH_CEILING),
        ("second-streamed", 0),
    ] {
        let dir = temp.path().join(name);
        std::fs::create_dir(&dir).unwrap();
        images.push(persist_cells(
            &shape,
            &dir,
            rewritten(),
            cut.clone(),
            floor.clone(),
            limit,
        ));
        super::open(&dir.join("image.index"), &shape, DATABASE, 1 << 20, 1).unwrap();
    }
    let (streamed, buffered) = (images.pop().unwrap(), images.pop().unwrap());
    assert_eq!(buffered.0, streamed.0, "second root bytes");
    assert_eq!(buffered.1, streamed.1, "second container bytes");
    assert_eq!(buffered.2.encode_stats.streamed_cells, 0);
    assert!(buffered.2.encode_stats.buffered_cells > 1);
}

/// CPU time of this thread in nanoseconds (Linux scheduler accounting).
fn thread_cpu_ns() -> u64 {
    let stat = std::fs::read_to_string("/proc/thread-self/schedstat").unwrap();
    stat.split_whitespace().next().unwrap().parse().unwrap()
}

/// Before/after encoder CPU per cell. Run in release:
/// `cargo test --release ... cell_encode_tests::measure -- --ignored --nocapture`.
#[test]
#[ignore = "measurement, run explicitly in release mode"]
fn measure_cell_encode_cpu() {
    let mut rng = StdRng::seed_from_u64(19);
    let shapes: [(&str, usize, usize, usize, u64); 5] = [
        // name, rows, versions per row, key bytes, floor sequence
        ("8x1 k4", 8, 1, 4, 0),
        ("256x2 k4", 256, 2, 4, 0),
        ("1024x1 k32", 1024, 1, 32, 0),
        ("4096x1 k8", 4096, 1, 8, 0),
        ("512x8 k8 floor", 512, 8, 8, 6),
    ];
    println!(
        "cell | payload B | legacy ns/cell | buffered ns/cell | legacy ns/B | buffered ns/B | ratio"
    );
    for (name, rows, versions, key_len, floor_sequence) in shapes {
        let mut base_history = History::new();
        let mut overlay_history = History::new();
        while base_history.len() < rows {
            let key = random_key(&mut rng, Some(key_len));
            let all: Vec<_> = (1..=versions as u64)
                .map(|s| reference(version(s, 0), RecordKind::Put, key_len, 0))
                .collect();
            let (old, new) = all.split_at(versions / 2);
            if !old.is_empty() {
                base_history.insert(key.clone(), old.to_vec());
            } else {
                base_history.insert(key.clone(), new[..1].to_vec());
            }
            overlay_history.insert(key, new.to_vec());
        }
        let cell = cell(&base_history, &overlay_history);
        let cut = IndexReadCut::At(version(1000, 0));
        let floor = if floor_sequence == 0 {
            IndexReadCut::BeforeAll
        } else {
            IndexReadCut::At(version(floor_sequence, 0))
        };
        let payload = assert_matches_legacy(&cell, &cut, &floor, CELL_SCRATCH_CEILING);
        assert_eq!(payload.buffered_cells, 1);
        let bytes = payload.buffered_bytes - 8;
        let iterations = (64 << 20) / bytes.max(1) + 16;
        let path = Path::new("x.cells");
        let mut sink = BufWriter::with_capacity(16 * 1024, std::io::sink());
        let mut reference_bytes = Vec::with_capacity(REFERENCE_BYTES);
        let measure = |run: &mut dyn FnMut(u64)| {
            let mut best = u64::MAX;
            for _ in 0..5 {
                let start = thread_cpu_ns();
                for n in 0..iterations {
                    run(n);
                }
                best = best.min((thread_cpu_ns() - start) / iterations);
            }
            best
        };
        let legacy = measure(&mut |n| {
            legacy_append_cell(
                &cell,
                &mut sink,
                path,
                n,
                &cut,
                &floor,
                &mut reference_bytes,
            )
            .unwrap();
        });
        let mut encoder = CellEncoder::new(CELL_SCRATCH_CEILING);
        let mut sink = BufWriter::with_capacity(16 * 1024, std::io::sink());
        let buffered = measure(&mut |n| {
            encoder
                .append(&cell, Some(&mut sink), path, n, &cut, &floor)
                .unwrap();
        });
        println!(
            "{name} | {bytes} | {legacy} | {buffered} | {:.3} | {:.3} | {:.2}",
            legacy as f64 / bytes as f64,
            buffered as f64 / bytes as f64,
            legacy as f64 / buffered as f64
        );
    }
}
