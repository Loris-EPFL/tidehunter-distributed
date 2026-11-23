//! The on-disk index blob of the hybrid position, per index format and key
//! class: flush through the real index WAL, load, and read the same blob back
//! through the lazy on-disk lookup.
//!
//! The fixed-size zero-copy shortcut of `IndexTable::deserialize_fixed_size`
//! hands a clean blob straight to the flat buffer, which is valid only when the
//! position's blob row is its clean flat row. Every case therefore decodes every
//! field of every entry (clean blobs take the shortcut, blobs with tombstones
//! the rebuild path) and reads each key back through `lookup_unloaded`.
//!
//! Each case also states whether the load must take the zero-copy shortcut
//! ([`ZeroCopy::Taken`]: a clean fixed-key blob, counter +1) or must not
//! ([`ZeroCopy::NotTaken`]: a blob with a tombstone or a variable-length key,
//! counter +0). The counter is the native `#[cfg(test)]` hook
//! `zero_copy_loads_for_test()`, per thread; `loader.load` runs on the calling
//! thread.
//!
//! Negative control for every case (mutation run of production code): restore
//! the old blob layout in `VersionPosition::{write_blob, read_blob}` (epoch word
//! first, operation index after the sequence): the loaded `batch` is shifted by
//! four bytes (`{1, 4}` reads back as `{1 << 32, 4 << 32}`) and the case fails
//! on its first row comparison.

use super::{assert_rows, varied};
use crate::distributed::durability::DurabilityTier;
use crate::distributed::hybrid::config::HybridIndexConfig;
use crate::distributed::hybrid::loader::CutSources;
use crate::distributed::hybrid::position::VersionPosition;
use crate::distributed::hybrid::table::HybridTable;
use crate::index::index_format::{IndexFormat, IndexFormatType};
use crate::index::index_table::IndexTable;
use crate::index::index_table::zero_copy_loads_for_test;
use crate::index::uniform_lookup::UniformLookupIndex;
use crate::key_shape::{KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType};
use crate::large_table::Loader;
use crate::metrics::Metrics;
use minibytes::Bytes;
use std::sync::Arc;

fn table(
    name: &str,
    indexing: KeyIndexing,
    format: IndexFormatType,
) -> (tempdir::TempDir, HybridTable) {
    let dir = tempdir::TempDir::new(name).expect("scratch directory");
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config_indexing(
        "layout",
        indexing,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::default().with_index_format(format),
    );
    let table = HybridTable::open(
        dir.path(),
        Arc::new(builder.build()),
        &HybridIndexConfig::default(),
        CutSources::new(1, DurabilityTier::Native),
        Metrics::new(),
    )
    .expect("open table");
    (dir, table)
}

/// Whether `IndexTable::deserialize_fixed_size` hands the blob to the flat
/// buffer without a rebuild.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ZeroCopy {
    Taken,
    NotTaken,
}

/// Flush `keys` (index keys, ascending; the entries at `removed` are stored as
/// tombstones), load the blob back and check the lazy lookup of every key.
fn round_trip(
    name: &str,
    indexing: KeyIndexing,
    format: IndexFormatType,
    keys: &[Vec<u8>],
    removed: &[usize],
    absent: &[u8],
    zero_copy: ZeroCopy,
) {
    assert!(
        keys.windows(2).all(|pair| pair[0] < pair[1]),
        "ascending keys"
    );
    let (_dir, table) = table(name, indexing, format);
    let ks = table.shape().iter_ks().next().expect("one keyspace").id();
    let desc = table.ks_desc(ks);
    let loader = table.loader();

    let mut data = IndexTable::<VersionPosition>::default();
    let mut live = Vec::new();
    let mut stored = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let written = varied(
            10 + index as u64,
            1_000 * (index as u64 + 1),
            3 * index as u32 + 1,
            crc32fast::hash(key),
        );
        let key = Bytes::copy_from_slice(key);
        if removed.contains(&index) {
            data.remove(key.clone(), written);
        } else {
            data.insert(key.clone(), written);
            live.push((key.clone(), written));
        }
        stored.push((key, written));
    }
    let at = loader.flush(ks, &data).expect("write the index frame");
    let loads_before = zero_copy_loads_for_test();
    let loaded = loader.load(desc, at).expect("load the frame");
    let loads = zero_copy_loads_for_test() - loads_before;
    eprintln!("{name}: zero-copy loads {loads}, expected {zero_copy:?}");
    assert_eq!(
        loads,
        u64::from(zero_copy == ZeroCopy::Taken),
        "{name}: zero-copy shortcut {zero_copy:?}"
    );

    assert_rows(&loaded, &live);
    assert_eq!(
        loaded.iter_with_tombstones().count(),
        keys.len(),
        "{name}: removed rows stay stored"
    );

    // The lazy path reads the same blob: no `load`, one random reader.
    let reader = loader.index_reader(at).expect("random reader");
    let lookup = |key: &[u8]| {
        desc.index_format()
            .lookup_unloaded::<VersionPosition>(desc, &reader, key, table.metrics())
    };
    for (index, (key, written)) in stored.iter().enumerate() {
        let found = lookup(key);
        if removed.contains(&index) {
            assert!(
                found.is_none_or(|position| !position.is_valid()),
                "{name}: a tombstone must not read as a live entry: {found:?}"
            );
        } else {
            let found = found.unwrap_or_else(|| panic!("{name}: lazy lookup misses {key:?}"));
            assert_eq!(found.batch, written.batch, "{name}: {key:?}");
            assert_eq!(found.operation_index, written.operation_index);
            assert_eq!(found.payload_crc, written.payload_crc);
            assert_eq!(found.address, written.address);
        }
    }
    assert!(
        lookup(absent).is_none(),
        "{name}: an absent key is not found"
    );
}

/// Four-byte keys spread over the whole key space (the uniform format locates a
/// key by its numeric position).
fn spread_keys(count: u32) -> Vec<Vec<u8>> {
    let step = u32::MAX / (count + 1);
    (1..=count)
        .map(|n| (n * step).to_be_bytes().to_vec())
        .collect()
}

fn uniform() -> IndexFormatType {
    IndexFormatType::Uniform(UniformLookupIndex::new())
}

#[test]
fn a_clean_fixed_key_lookup_header_blob_round_trips() {
    round_trip(
        "layout-header-clean",
        KeyIndexing::fixed(4),
        IndexFormatType::Header,
        &spread_keys(9),
        &[],
        &[0x42; 4],
        ZeroCopy::Taken,
    );
}

#[test]
fn a_fixed_key_lookup_header_blob_with_tombstones_round_trips() {
    round_trip(
        "layout-header-tombstones",
        KeyIndexing::fixed(4),
        IndexFormatType::Header,
        &spread_keys(9),
        &[1, 4, 8],
        &[0x42; 4],
        ZeroCopy::NotTaken,
    );
}

#[test]
fn a_clean_fixed_key_uniform_lookup_blob_round_trips() {
    round_trip(
        "layout-uniform-clean",
        KeyIndexing::fixed(4),
        uniform(),
        &spread_keys(9),
        &[],
        &[0x42; 4],
        ZeroCopy::Taken,
    );
}

#[test]
fn a_fixed_key_uniform_lookup_blob_with_tombstones_round_trips() {
    round_trip(
        "layout-uniform-tombstones",
        KeyIndexing::fixed(4),
        uniform(),
        &spread_keys(9),
        &[0, 5],
        &[0x42; 4],
        ZeroCopy::NotTaken,
    );
}

/// Variable-length keys: the on-disk row is the flat row with its kind bits, so
/// a Removed row and a Modified row must both survive.
#[test]
fn a_variable_length_blob_keeps_modified_and_removed_rows() {
    let keys: Vec<Vec<u8>> = vec![
        b"a".to_vec(),
        b"alpha".to_vec(),
        b"beta-key".to_vec(),
        b"gamma".to_vec(),
        b"zeta-zeta-zeta".to_vec(),
    ];
    round_trip(
        "layout-varlen",
        KeyIndexing::variable_length(),
        IndexFormatType::Header,
        &keys,
        &[1, 3],
        b"nothing-here",
        ZeroCopy::NotTaken,
    );
}

/// A reduced key (Sui's `key_reduction`): the index holds four bytes of an
/// eight-byte key.
#[test]
fn a_reduction_keyspace_blob_round_trips() {
    round_trip(
        "layout-reduction",
        KeyIndexing::key_reduction(8, 0..4),
        IndexFormatType::Header,
        &spread_keys(7),
        &[2],
        &[0x42; 4],
        ZeroCopy::NotTaken,
    );
    round_trip(
        "layout-reduction-clean",
        KeyIndexing::key_reduction(8, 0..4),
        IndexFormatType::Header,
        &spread_keys(7),
        &[],
        &[0x42; 4],
        ZeroCopy::Taken,
    );
}

/// A hashed key: the index holds the 16-byte hash.
#[test]
fn a_hash_keyspace_blob_round_trips() {
    let keys: Vec<Vec<u8>> = (1u8..=6)
        .map(|n| {
            let mut key = vec![0u8; 16];
            key[0] = n * 40;
            key[15] = n;
            key
        })
        .collect();
    round_trip(
        "layout-hash",
        KeyIndexing::hash(),
        IndexFormatType::Header,
        &keys,
        &[],
        &[0x42; 16],
        ZeroCopy::Taken,
    );
    round_trip(
        "layout-hash-tombstone",
        KeyIndexing::hash(),
        IndexFormatType::Header,
        &keys,
        &[0, 5],
        &[0x42; 16],
        ZeroCopy::NotTaken,
    );
}
