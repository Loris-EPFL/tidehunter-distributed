//! A reduced-index keyspace in a batch read: Sui's `transactions`, `effects`
//! and `executed_effects` index a slice of the key, so the index entry alone
//! cannot bind a value. The batch keeps the scalar read's full-key check and
//! still costs one owner read per owner per page.
//!
//! Every test writes and reads through the Sui adapter over the leased fixture,
//! so the batch under test is the one Sui calls (`get_many_live`).

use super::pipeline_tests::Fixture;
use super::*;
use crate::distributed::sui::Db;
use crate::key_shape::{KeyIndexing, KeyShape, KeySpaceConfig, KeyType};
use std::collections::BTreeSet;
use std::io;
use std::path::Path;
use std::sync::Arc;

/// 4-byte keys whose first two bytes are the index key, the shape of a Sui
/// digest keyspace (a reduction of the full key), with a native per-cell value
/// cache of `cache` entries.
fn reduced_shape(cache: usize) -> KeyShape {
    KeyShape::new_single_config_indexing(
        KeyIndexing::key_reduction(4, 0..2),
        4,
        KeyType::uniform(4),
        KeySpaceConfig::new().with_value_cache_size(cache),
    )
}

fn fixture(name: &str, shape: KeyShape) -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new(name).expect("scratch directory"),
        2,
        128,
        shape,
        2,
        |_| {},
        |_| {},
    )
}

fn open(fixture: &Fixture) -> Arc<Db> {
    Db::open_remote_local_fixture(
        &fixture.temp.path().join("authority"),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .expect("open the fixture database")
}

/// The full key of reduction `index`: the index keeps the first two bytes only.
fn key(index: u16) -> Vec<u8> {
    key_with(index, [0xaa, 0x55])
}

/// A full key of reduction `index` with payload bytes the index never sees.
fn key_with(index: u16, payload: [u8; 2]) -> Vec<u8> {
    let mut key = [0u8; 4];
    key[..2].copy_from_slice(&index.to_be_bytes());
    key[2..].copy_from_slice(&payload);
    key.to_vec()
}

/// The member position of every key at `cut`. Panics on a resident value, so a
/// caller that needs the batch path states that premise.
fn positions_of(
    table: &HybridTable,
    ks: KeySpace,
    keys: &[Vec<u8>],
    cut: BatchVersion,
) -> Vec<VersionPosition> {
    keys.iter()
        .map(|key| match table.get_value(ks, key, cut) {
            Ok(Some(ResolvedValue::Position(position))) => position,
            other => panic!("premise: key {key:?} has an owner position, got {other:?}"),
        })
        .collect()
}

/// Publish every write, advance the durable cut the fixture's records lag, and
/// flush: every dirty cell becomes Unloaded, and the value LRU of an unloaded
/// cell is never served, so a read resolves a member position.
fn unload_all(db: &Arc<Db>) {
    let remote = db.remote_for_test();
    let core = remote.write_core();
    core.wait_settled().expect("settled");
    let table = remote.hybrid_table();
    table.cuts().set_durable(table.current());
    core.flush_index()
        .expect("flush the dirty cells into an index blob");
}

#[test]
fn a_reduced_multi_get_answers_every_key_like_a_scalar_get() {
    /// The highest reduction written; the six above it are absent queries.
    const WRITTEN: u16 = 20;
    const QUERIED: u16 = 26;
    let fixture = fixture("reduced-batch-values", reduced_shape(16));
    let ks = KeySpace(0);
    {
        let db = open(&fixture);
        for index in 1..=WRITTEN {
            db.insert(ks, key(index), vec![index as u8; 8])
                .expect("write");
        }
        db.wait_for_background_threads_to_finish()
            .expect("shut down before the reopen");
    }

    // A cold open: the batch is the first reader, so nothing is resident.
    let db = open(&fixture);
    let queries: Vec<Vec<u8>> = (1..=QUERIED).map(key).collect();
    let batch = db.multi_get(ks, &queries);
    for (slot, index) in queries.iter().enumerate() {
        let index = u16::from_be_bytes([index[0], index[1]]);
        let expected = (index <= WRITTEN).then(|| vec![index as u8; 8]);
        assert_eq!(
            batch[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            expected.as_deref(),
            "slot {slot} holds key {index} in input order",
        );
    }

    // The scalar read of the same keys agrees with the batch, present and absent.
    for (slot, query) in queries.iter().enumerate() {
        let scalar = db.get(ks, query).expect("scalar read");
        assert_eq!(
            scalar.as_deref(),
            batch[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            "the scalar read of {query:?} answers the value of batch slot {slot}",
        );
    }
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A page of unloaded cells: no value is resident, so the batch must fetch every
/// miss, and the misses must leave as one read per owner, not one per key. A
/// per-key fallback would show 26 submissions instead of the owner count.
#[test]
fn a_reduced_multi_get_fetches_each_owner_once_for_unloaded_cells() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("reduced-batch-unloaded").expect("scratch directory"),
        2,
        128,
        reduced_shape(16),
        2,
        // Every dirty cell is stale: the forced pass flushes and unloads.
        |config| config.index.snapshot_unload_threshold = 0,
        |_| {},
    );
    let ks = KeySpace(0);
    let db = open(&fixture);
    let indices: Vec<u16> = (1..=26).collect();
    let keys: Vec<Vec<u8>> = indices.iter().copied().map(key).collect();
    for (index, key) in indices.iter().zip(&keys) {
        db.insert(ks, key.clone(), vec![*index as u8; 8])
            .expect("write");
    }
    unload_all(&db);
    let remote = db.remote_for_test();
    let table = remote.hybrid_table();
    for key in &keys {
        let cell = fixture.shape.ks(ks).cell_id(key);
        assert_eq!(
            table
                .large_table()
                .entry_state_for_test(table.contexts().ks_context(ks), &cell),
            "Unloaded",
            "premise: the forced flush unloaded the cell of {key:?}",
        );
    }
    remote.cache.clear();
    let cut = remote.published_cut().expect("published cut");
    let owners: BTreeSet<u32> = positions_of(table, ks, &keys, cut)
        .into_iter()
        .map(|position| position.address.partition)
        .collect();
    assert!(
        owners.len() >= 2,
        "premise: the page spans both owners, got {owners:?}",
    );

    let submissions = remote.live_read_submission_count();
    let values = db.multi_get(ks, &keys);
    for (slot, index) in indices.iter().enumerate() {
        assert_eq!(
            values[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            Some([*index as u8; 8].as_slice()),
            "slot {slot} holds the member of key {index}",
        );
    }
    assert_eq!(
        remote.live_read_submission_count() - submissions,
        owners.len() as u64,
        "the misses went out as one batched read per owner, not one per key",
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A retired member is the one failure the index can repair. The page cannot say
/// which slot held it, so every slot re-reads through the scalar reduced read,
/// which re-resolves once and checks the full key. Any other failure is terminal.
#[test]
fn a_reduced_multi_get_repairs_a_retired_member_and_fails_other_kinds() {
    let fixture = fixture("reduced-batch-retired", reduced_shape(16));
    let ks = KeySpace(0);
    {
        let db = open(&fixture);
        for index in 1..=4u16 {
            db.insert(ks, key(index), vec![index as u8; 8])
                .expect("write");
        }
        db.wait_for_background_threads_to_finish()
            .expect("shut down before the reopen");
    }
    let db = open(&fixture);
    let keys: Vec<Vec<u8>> = (1..=4u16).map(key).collect();
    let reader = &db.remote_for_test().reader;

    reader.set_read_many_fault_for_test(Some(io::ErrorKind::PermissionDenied));
    let failed = db.multi_get(ks, &keys);
    reader.set_read_many_fault_for_test(None);
    for (slot, value) in failed.iter().enumerate() {
        assert!(
            value.is_err(),
            "a non-retired page failure fails slot {slot}",
        );
    }

    // Nothing was fetched above, so all four slots are still cold.
    reader.set_read_many_fault_for_test(Some(io::ErrorKind::NotFound));
    let values = db.multi_get(ks, &keys);
    reader.set_read_many_fault_for_test(None);
    for (slot, index) in (1..=4u16).enumerate() {
        assert_eq!(
            values[slot]
                .as_ref()
                .expect("the scalar fallback answers the slot")
                .as_deref(),
            Some([index as u8; 8].as_slice()),
            "slot {slot} holds the member of key {index} through the fallback",
        );
    }
    db.wait_for_background_threads_to_finish().unwrap();
}

/// Two full keys with one reduction share an index entry. Both slots resolve
/// that one member position, and only the slot whose key the member itself names
/// may answer with the value; a batch that trusts the index entry answers the
/// other key too, which is the bug this test kills. Every read runs with the
/// cells unloaded, so the value LRU cannot answer either slot.
#[test]
fn a_reduced_multi_get_binds_a_shared_reduction_to_the_members_own_key() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("reduced-batch-collision").expect("scratch directory"),
        2,
        128,
        reduced_shape(16),
        2,
        |config| config.index.snapshot_unload_threshold = 0,
        |_| {},
    );
    let ks = KeySpace(0);
    let db = open(&fixture);
    let first = key_with(0x0102, [0x00, 0x01]);
    let second = key_with(0x0102, [0x00, 0x02]);
    assert_eq!(
        &first[..2],
        &second[..2],
        "premise: the two full keys share the index key",
    );
    db.insert(ks, first.clone(), vec![0x11; 8])
        .expect("write the first key");
    db.insert(ks, second.clone(), vec![0x22; 8])
        .expect("write the second key");
    unload_all(&db);
    let cut = db.remote_for_test().published_cut().expect("published cut");
    let keys = [first.clone(), second.clone()];
    let positions = positions_of(db.remote_for_test().hybrid_table(), ks, &keys, cut);
    assert!(
        positions[0].same_physical(&positions[1]),
        "premise: both full keys name the one member of their shared index entry",
    );

    let queries = [first.clone(), second.clone(), second.clone(), first.clone()];
    let values = db.multi_get(ks, &queries);
    let expected: [Option<Vec<u8>>; 4] = [None, Some(vec![0x22; 8]), Some(vec![0x22; 8]), None];
    for (slot, expected) in expected.iter().enumerate() {
        assert_eq!(
            values[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            expected.as_deref(),
            "slot {slot} of the shared reduction",
        );
    }

    // Writing the first key again moves the entry to its member; the second key
    // becomes the absent one, and the scalar read still agrees with the batch.
    db.insert(ks, first.clone(), vec![0x33; 8])
        .expect("rewrite the first key");
    unload_all(&db);
    let cut = db.remote_for_test().published_cut().expect("published cut");
    let positions = positions_of(db.remote_for_test().hybrid_table(), ks, &keys, cut);
    assert!(
        positions[0].same_physical(&positions[1]),
        "premise: both full keys still name one member position",
    );
    let values = db.multi_get(ks, &keys);
    let expected: [Option<Vec<u8>>; 2] = [Some(vec![0x33; 8]), None];
    for (slot, key) in keys.iter().enumerate() {
        assert_eq!(
            values[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            expected[slot].as_deref(),
            "slot {slot} of the rewritten reduction",
        );
        let scalar = db.get(ks, key).expect("scalar read");
        assert_eq!(
            scalar.as_deref(),
            values[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            "the scalar read of {key:?} answers the value of batch slot {slot}",
        );
    }
    db.wait_for_background_threads_to_finish().unwrap();
}

/// 300 keys cross the 256-key page bound. Each slot keeps its own index into the
/// call's result vector: a page-local slot index would overwrite the first 44
/// answers of the first page and leave the last 44 slots unset, which this test
/// kills. The owner reads are counted per page.
#[test]
fn a_reduced_multi_get_across_a_page_boundary_keeps_every_slot() {
    /// The page bound of `RemoteDatabase::get_many_with`.
    const PAGE_KEYS: usize = 256;
    // No native value cache: every slot resolves a member position.
    let fixture = fixture("reduced-batch-pages", reduced_shape(0));
    let ks = KeySpace(0);
    let indices: Vec<u16> = (1..=300).collect();
    let keys: Vec<Vec<u8>> = indices.iter().copied().map(key).collect();
    let expected: Vec<Vec<u8>> = indices
        .iter()
        .map(|index| index.to_be_bytes().to_vec())
        .collect();
    let db = open(&fixture);
    for (chunk, values) in keys.chunks(4).zip(expected.chunks(4)) {
        let mut batch = db.write_batch();
        for (key, value) in chunk.iter().zip(values) {
            batch.write(ks, key.clone(), value.clone());
        }
        batch.commit().expect("write");
    }
    let remote = db.remote_for_test();
    remote.write_core().wait_settled().expect("settled");
    let table = remote.hybrid_table();
    remote.cache.clear();
    let cut = remote.published_cut().expect("published cut");
    let page_owners: Vec<usize> = keys
        .chunks(PAGE_KEYS)
        .map(|page| {
            positions_of(table, ks, page, cut)
                .into_iter()
                .map(|position| position.address.partition)
                .collect::<BTreeSet<_>>()
                .len()
        })
        .collect();
    assert_eq!(page_owners.len(), 2, "premise: 300 keys span two pages");
    assert!(
        page_owners.iter().any(|owners| *owners >= 2),
        "premise: a page spans both owners, got {page_owners:?}",
    );

    let submissions = remote.live_read_submission_count();
    let values = db.multi_get(ks, &keys);
    for (slot, expected) in expected.iter().enumerate() {
        assert_eq!(
            values[slot]
                .as_ref()
                .expect("the batch answers the slot")
                .as_deref(),
            Some(expected.as_slice()),
            "slot {slot} holds the value of its own key",
        );
    }
    assert_eq!(
        remote.live_read_submission_count() - submissions,
        page_owners.iter().sum::<usize>() as u64,
        "one batched read per owner per page, got {page_owners:?}",
    );
    db.wait_for_background_threads_to_finish().unwrap();
}

/// A damaged member frame on a reduced page never yields a value: every slot of
/// the page fails, exactly as a plain batched page fails when one of its frames
/// cannot be decoded. The damage is detected at one of two layers, and both are
/// fail-closed: the reader refuses the payload CRC, or the owner refuses the
/// damaged log before it serves the frame, which fail-stops the engine. A page
/// that ignored the damage would answer `Some` for the rewritten value; a page
/// that mapped it to absent would answer `Ok(None)`. Both are killed here.
#[test]
fn a_corrupt_member_frame_fails_the_whole_reduced_batch() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("reduced-batch-corrupt").expect("scratch directory"),
        2,
        128,
        // No native value cache, and no coordinator read cache: the owner's
        // member frame is what the batch fetches and decodes.
        reduced_shape(0),
        2,
        |config| config.read_cache_bytes = 0,
        |_| {},
    );
    let ks = KeySpace(0);
    let db = open(&fixture);
    let first = key(1);
    let second = key(2);
    let corrupt = [0xABu8; 32];
    db.insert(ks, first.clone(), corrupt.to_vec())
        .expect("write the first key");
    db.insert(ks, second.clone(), vec![0xCDu8; 32])
        .expect("write the second key");
    let remote = db.remote_for_test();
    remote.write_core().wait_settled().expect("settled");
    remote.flush_durable().expect("durable");
    // Every durable copy of the value is damaged, so no read can dodge it.
    let flipped: usize = (0..fixture.nodes.len())
        .map(|owner| damage_wal_value(fixture.owner_database_root(owner), &corrupt))
        .sum();
    assert!(flipped > 0, "the written value must be in an owner WAL");
    let cut = remote.published_cut().expect("published cut");
    let keys = [first, second];
    // Premise: both keys still resolve to the members the index names, so the
    // batch fetches both frames and only the damaged bytes can fail it.
    positions_of(remote.hybrid_table(), ks, &keys, cut);

    let decodes = remote.reader.decode_failure_count();
    let values = db.multi_get(ks, &keys);
    let mut messages = Vec::with_capacity(values.len());
    for (slot, value) in values.iter().enumerate() {
        let Err(error) = value else {
            panic!("slot {slot} served the damaged member: {value:?}");
        };
        messages.push(format!("{error:?}"));
    }
    // A slot either names the member check, or the call fails because the owner
    // refused the damaged log and the engine recorded that cause. Either way the
    // failure names the damage and no slot answers.
    let recorded = remote
        .ensure_healthy()
        .err()
        .map(|cause| format!("{cause:?}"))
        .unwrap_or_default();
    let named = messages
        .iter()
        .any(|message| message.contains("member payload CRC mismatch"))
        || recorded.contains("log control authority is poisoned");
    assert!(
        named,
        "the failure names the damaged member, got {messages:?} and {recorded}",
    );
    if messages
        .iter()
        .any(|message| message.contains("member payload CRC mismatch"))
    {
        assert_eq!(
            remote.reader.decode_failure_count() - decodes,
            1,
            "the reader refused the damaged payload once",
        );
    } else {
        assert!(
            !recorded.is_empty(),
            "an owner refusal must fail-stop the engine",
        );
    }
    if remote.ensure_healthy().is_ok() {
        db.wait_for_background_threads_to_finish().unwrap();
    }
}

/// Flip one byte of every occurrence of `marker` inside an owner's `wal_` files,
/// so the member frame the index names no longer matches the payload CRC the
/// index carries, and no copy of the value is left intact. Returns the number of
/// damaged copies. Only `wal_` files are candidates: the same bytes can sit in
/// control or metadata files, and flipping those would fail a read for another
/// reason.
fn damage_wal_value(root: &Path, marker: &[u8]) -> usize {
    let mut flipped = 0;
    let Ok(entries) = std::fs::read_dir(root) else {
        return flipped;
    };
    for entry in entries.flatten() {
        let epoch = entry.path();
        if !epoch.is_dir() {
            continue;
        }
        let Ok(files) = std::fs::read_dir(&epoch) else {
            continue;
        };
        for file in files.flatten() {
            let path = file.path();
            if !path
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("wal_"))
            {
                continue;
            }
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            let offsets: Vec<usize> = bytes
                .windows(marker.len())
                .enumerate()
                .filter_map(|(offset, window)| (window == marker).then_some(offset))
                .collect();
            if offsets.is_empty() {
                continue;
            }
            let wal = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("open the owner WAL");
            for offset in offsets {
                std::os::unix::fs::FileExt::write_all_at(&wal, &[0xAA], offset as u64)
                    .expect("flip the durable byte");
                flipped += 1;
            }
            wal.sync_all().expect("sync the owner WAL");
        }
    }
    flipped
}
