//! Native-vs-hybrid differential for `RelocationStrategy::IndexBased`, plus the
//! crash and concurrency cuts of that pass.
//!
//! Both arms run the same deterministic plan on one shape. The native arm runs
//! its own `index_based_relocation` through
//! `Db::start_blocking_relocation_with_strategy` (native's observed entry
//! refuses `IndexBased`); the hybrid arm runs the coordinator pass of the same
//! name. Every probe key is compared after each phase, after each pass and
//! after a reopen.
//!
//! The owner WAL file is one fragment, so each of the fixture's five streams
//! holds whole files after two write generations. That matters: the hybrid pass
//! reclaims only whole files, and an owner stream below one file would make
//! every probe stream `NoWork`.
//!
//! The filtered gates compare absence too. Native makes a `Remove` entry
//! unreadable by garbage-collecting its frame below the pass's watermark; the
//! hybrid removes the entry and retires the same whole files. The plans write
//! every `Remove` key before every `Keep` key, so each stream's removed bytes
//! sit in its first files and both floors cover them. A `Remove` key in a
//! stream's last partial file would stay readable natively (its frame is above
//! the whole-file floor) and be absent in the hybrid, which is a recorded
//! difference of the reclaim boundary, not a value difference: the plan keeps
//! that case out.
#![cfg(feature = "test_methods")]

use super::super::RelocationGate;
use super::Fixture;
use super::native_parity_gates::{
    Edit, Phase, apply_phase, assert_same_reads, close_arm, hex, key, keys, native_arm_config,
    native_config, open_hybrid, open_native, read_one, sequential_apply, shape, value,
};
use crate::Decision;
use crate::config::Config;
use crate::distributed::hybrid::HybridIndexConfig;
use crate::distributed::remote_db::RelocationReport;
use crate::distributed::sui::Db as SuiDb;
use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use crate::relocation::RelocationStrategy;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

type KeySet = Vec<Vec<u8>>;

/// One owner WAL file: one fragment, so two write generations fill whole files
/// in every stream. The group frame of two full batches fits, which 4096 did
/// not once the values were widened for the differential.
const OWNER_FILE: u64 = 8192;
const KS_NAME: &str = "root";
const PAD_KS: &str = "pad";

/// The index geometry both arms share, with `target` selected. Owner offsets
/// start at zero per stream, so the same number bounds every stream.
fn index_arm_config(target: Option<u64>) -> Arc<Config> {
    let mut config = (*native_config()).clone();
    config.relocation_strategy = RelocationStrategy::IndexBased(target);
    Arc::new(config)
}

/// The native arm's value WAL: one file per fragment, so a whole-file reclaim
/// is possible in this key domain. The fragment holds one keyspace's index
/// blob, so it must fit the widest keyspace of the test. WAL layout only.
fn value_arm_config(target: Option<u64>) -> Arc<Config> {
    let mut config = (*native_arm_config()).clone();
    config.frag_size = OWNER_FILE;
    config.wal_file_size = OWNER_FILE;
    config.relocation_strategy = RelocationStrategy::IndexBased(target);
    Arc::new(config)
}

/// A five-owner fixture whose streams hold whole files after two generations,
/// and whose coordinator index runs `index_config`'s strategy.
fn small_file_fixture(index_config: &Arc<Config>, shape: KeyShape) -> Fixture {
    Fixture::new_with_config(512, shape, 2, |remote| {
        remote.storage.fragment_bytes = OWNER_FILE;
        remote.storage.wal_file_bytes = OWNER_FILE;
        remote.index = HybridIndexConfig::from_native(index_config);
    })
}

fn open_hybrid_with(fixture: &Fixture, config: super::super::RemoteDatabaseConfig) -> Arc<SuiDb> {
    SuiDb::open_remote_local_fixture(&fixture.authority(), fixture.shape.clone(), config)
        .unwrap_or_else(|error| panic!("hybrid open failed: {error:?}"))
}

/// One hybrid pass on the calling thread. A pass with work must store its
/// capture and adopt its starts; a deferral here would hide the difference
/// under test.
fn hybrid_pass(db: &Arc<SuiDb>, label: &str) -> RelocationReport {
    let cancel = AtomicBool::new(false);
    let report = db
        .remote_for_test()
        .relocation_pass(true, &cancel)
        .unwrap_or_else(|error| panic!("{label}: hybrid pass failed: {error:?}"));
    assert!(
        !report.deferred,
        "{label}: the pass must store its capture, got {report:?}"
    );
    assert!(
        report.stored_target.is_some(),
        "{label}: the pass must adopt a covering store, got {report:?}"
    );
    report
}

/// One native pass on the calling thread. The WAL tracker barrier makes the
/// GC floor readable.
fn native_pass(db: &Arc<SuiDb>, target: Option<u64>, label: &str) {
    let native = db
        .native()
        .unwrap_or_else(|| panic!("{label}: the native arm is expected"));
    // The pass reads `last_processed`; the tracker records it after a thread
    // hop from the write that dropped its guard.
    native.wal_writer.wait_processed();
    // Native's index-based GC is capped at the stored control region's replay
    // frontier, so a region must exist. Sui's cadence stores one; these tests
    // store it explicitly.
    native
        .force_rebuild_control_region()
        .unwrap_or_else(|error| panic!("{label}: native control region failed: {error:?}"));
    native.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(target));
    native.wal_writer.wal_tracker_barrier();
}

fn native_floor(db: &Arc<SuiDb>) -> u64 {
    db.native()
        .expect("the native arm is expected")
        .wal
        .min_wal_position()
}

/// Every owner WAL file path of one owner, all generations.
fn owner_wal_paths(fixture: &Fixture, owner: usize) -> Vec<std::path::PathBuf> {
    let root = fixture.owner_database_root(owner);
    let mut paths = Vec::new();
    for epoch in std::fs::read_dir(root).into_iter().flatten().flatten() {
        if !epoch.path().is_dir() {
            continue;
        }
        for file in std::fs::read_dir(epoch.path())
            .into_iter()
            .flatten()
            .flatten()
        {
            if file.file_name().to_string_lossy().starts_with("wal_") {
                paths.push(file.path());
            }
        }
    }
    paths.sort();
    paths
}

/// Owner WAL files of one stream, all generations.
fn owner_wal_files(fixture: &Fixture, owner: usize) -> usize {
    owner_wal_paths(fixture, owner).len()
}

/// One batch of at most four puts, the fixture's operation limit.
fn put_batch(db: &Arc<SuiDb>, ks: KeySpace, pairs: &[(Vec<u8>, Vec<u8>)]) {
    let mut batch = db.write_batch();
    for (key, value) in pairs {
        batch.write(ks, key.clone(), value.clone());
    }
    batch.commit().expect("batch commit");
}

fn puts(db: &Arc<SuiDb>, ks: KeySpace, rows: &[(Vec<u8>, Vec<u8>)]) {
    for chunk in rows.chunks(4) {
        put_batch(db, ks, chunk);
    }
}

/// Apply one phase to both arms and compare every probe against the model.
fn compare_phase(
    seed: u64,
    name: &str,
    phase: &Phase,
    native: &Arc<SuiDb>,
    hybrid: &Arc<SuiDb>,
    model: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    probes: &[Vec<u8>],
) {
    let ks = native.ks(KS_NAME);
    sequential_apply(model, phase);
    apply_phase(native, ks, phase, &format!("native/{name}"));
    apply_phase(hybrid, ks, phase, &format!("hybrid/{name}"));
    assert_same_reads(seed, name, ks, native, hybrid, probes, model);
}

/// Gate (a): the gate fixture's phase plan, then an `IndexBased(None)` pass on
/// both arms, a second plan, a second pass, and a reopen. Every phase and every
/// pass compares all probe keys with the sequential model.
#[test]
fn index_based_none_matches_native_and_the_model_after_each_pass_and_reopen() {
    let seed = 0x1db4_5ed0_u64;
    let index_config = index_arm_config(None);
    let value_config = value_arm_config(None);
    let shape = shape();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let native_root = tempdir::TempDir::new("index-based-none-native").expect("native root");
    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    let ks = native.ks(KS_NAME);
    let all = keys();
    let probes = all.clone();
    let mut model = BTreeMap::new();

    // Two full generations: every stream then holds whole files.
    let phases = [
        (
            "puts",
            Phase {
                singles: all
                    .iter()
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 0)))
                    .collect(),
                ..Phase::default()
            },
        ),
        (
            "stale_churn",
            Phase {
                singles: all
                    .iter()
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 5)))
                    .collect(),
                ..Phase::default()
            },
        ),
        (
            "overwrites",
            Phase {
                batches: all
                    .chunks(4)
                    .take(6)
                    .map(|chunk| {
                        chunk
                            .iter()
                            .map(|key| Edit::Put(key.clone(), value(seed, key, 1)))
                            .collect()
                    })
                    .collect(),
                ..Phase::default()
            },
        ),
        (
            "deletes",
            Phase {
                singles: all
                    .iter()
                    .skip(48)
                    .map(|key| Edit::Delete(key.clone()))
                    .collect(),
                ..Phase::default()
            },
        ),
    ];
    for (name, phase) in &phases {
        compare_phase(seed, name, phase, &native, &hybrid, &mut model, &probes);
    }

    native
        .flush()
        .expect("flush the native arm before the pass");
    let native_before = native_floor(&native);
    hybrid.flush().expect("flush before the pass");
    let report = hybrid_pass(&hybrid, "hybrid/index-pass-1");
    assert_eq!(
        report.gate,
        RelocationGate::DidWork,
        "the first hybrid pass must reclaim whole files: {report:?}"
    );
    assert!(
        report.copied_survivors > 0,
        "the first hybrid pass must copy live survivors: {report:?}"
    );
    native_pass(&native, None, "native/index-pass-1");
    assert!(
        native_floor(&native) > native_before,
        "the native pass must reclaim whole files: floor {native_before} -> {}",
        native_floor(&native)
    );
    assert_same_reads(seed, "pass-1", ks, &native, &hybrid, &probes, &model);

    // Four more generations: enough new value bytes per stream for the second
    // pass to find whole files beyond the start the first pass adopted (the
    // first pass's copies occupy part of the space it left).
    let second = [
        (
            "dead_churn",
            Phase {
                singles: all
                    .iter()
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 9)))
                    .collect(),
                ..Phase::default()
            },
        ),
        (
            "churn_10",
            Phase {
                batches: all
                    .chunks(4)
                    .map(|chunk| {
                        chunk
                            .iter()
                            .map(|key| Edit::Put(key.clone(), value(seed, key, 10)))
                            .collect()
                    })
                    .collect(),
                ..Phase::default()
            },
        ),
        (
            "churn_11",
            Phase {
                batches: all
                    .chunks(4)
                    .map(|chunk| {
                        chunk
                            .iter()
                            .map(|key| Edit::Put(key.clone(), value(seed, key, 11)))
                            .collect()
                    })
                    .collect(),
                ..Phase::default()
            },
        ),
        (
            "repopulate",
            Phase {
                singles: all
                    .iter()
                    .take(40)
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 12)))
                    .collect(),
                ..Phase::default()
            },
        ),
    ];
    for (name, phase) in &second {
        compare_phase(seed, name, phase, &native, &hybrid, &mut model, &probes);
    }
    native
        .flush()
        .expect("flush the native arm before the second pass");
    hybrid.flush().expect("flush before the second pass");
    let report = hybrid_pass(&hybrid, "hybrid/index-pass-2");
    assert_eq!(
        report.gate,
        RelocationGate::DidWork,
        "the second hybrid pass must reclaim the new dead prefix: {report:?}"
    );
    native_pass(&native, None, "native/index-pass-2");
    assert_same_reads(seed, "pass-2", ks, &native, &hybrid, &probes, &model);

    // The persisted image is the same on both arms.
    close_arm(native, "native before reopen");
    close_arm(hybrid, "hybrid before reopen");
    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    assert_same_reads(seed, "reopen", ks, &native, &hybrid, &probes, &model);

    // After a reopen the old stream is sealed; one more pass re-points its
    // survivors into the new serving generation and retires it.
    native.flush().expect("flush the native arm after reopen");
    hybrid.flush().expect("flush after reopen");
    let report = hybrid_pass(&hybrid, "hybrid/index-pass-3");
    assert_eq!(
        report.gate,
        RelocationGate::DidWork,
        "the sealed stream must be relocated after a reopen: {report:?}"
    );
    native_pass(&native, None, "native/index-pass-3");
    assert_same_reads(seed, "pass-3", ks, &native, &hybrid, &probes, &model);
    close_arm(native, "native after reopen");
    close_arm(hybrid, "hybrid after reopen");
}

/// Gate (b): `IndexBased(Some(t))` processes the prefix below `t` per stream,
/// exactly `t`'s worth of native offsets. One pass with `t` = two owner files
/// leaves the rest untouched and keeps every value; repeating it is `NoWork`
/// because no whole file sits between the adopted start and `t`; a reopen with
/// `None` finishes the stream.
#[test]
fn an_index_based_byte_limit_processes_only_the_reclaimable_prefix() {
    let seed = 0x2c0f_fee0_u64;
    let target = Some(2 * OWNER_FILE);
    let index_config = index_arm_config(target);
    let value_config = value_arm_config(target);
    let shape = shape();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let native_root = tempdir::TempDir::new("index-based-limit-native").expect("native root");
    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    let ks = native.ks(KS_NAME);
    let all = keys();
    let probes = all.clone();
    let mut model = BTreeMap::new();

    for (name, phase) in [
        (
            "puts",
            Phase {
                singles: all
                    .iter()
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 0)))
                    .collect(),
                ..Phase::default()
            },
        ),
        // A few keys move above the limit; every other key's live entry stays
        // in its stream's first frames, where the limited pass must copy it.
        (
            "early_churn",
            Phase {
                batches: all[..4]
                    .iter()
                    .enumerate()
                    .map(|(index, key)| {
                        vec![Edit::Put(key.clone(), value(seed, key, 5 + index as u64))]
                    })
                    .collect(),
                ..Phase::default()
            },
        ),
    ] {
        compare_phase(seed, name, &phase, &native, &hybrid, &mut model, &probes);
    }

    native
        .flush()
        .expect("flush the native arm before the pass");
    hybrid.flush().expect("flush before the pass");
    let files_before: usize = (0..fixture.nodes.len())
        .map(|owner| owner_wal_files(&fixture, owner))
        .sum();
    let report = hybrid_pass(&hybrid, "hybrid/limit-pass-1");
    assert_eq!(
        report.gate,
        RelocationGate::DidWork,
        "a two-file limit must reclaim: {report:?}"
    );
    assert!(
        report.copied_survivors > 0,
        "the limited pass must copy the live prefix it reclaims: {report:?}"
    );
    assert!(
        report
            .streams
            .iter()
            .all(|stream| stream.rounded_target <= 2 * OWNER_FILE),
        "every stream's target must stay inside the limit: {report:?}"
    );
    // The unfiltered shape makes every callback a read entry, so the callbacks
    // are exactly what the walk touched. One above the configured limit means
    // the walk ignored its stream's target.
    assert!(
        !report.callbacks.is_empty(),
        "premise: the walk records the entries it read: {report:?}"
    );
    for callback in &report.callbacks {
        assert!(
            callback.wal_position < 2 * OWNER_FILE,
            "the walk must read only below the configured per-stream limit \
             {}: {callback:?}",
            2 * OWNER_FILE
        );
    }
    let files_after: usize = (0..fixture.nodes.len())
        .map(|owner| owner_wal_files(&fixture, owner))
        .sum();
    assert!(
        files_after < files_before,
        "the limited pass must retire whole files: {files_before} -> {files_after}"
    );
    native_pass(&native, target, "native/limit-pass-1");
    assert_same_reads(seed, "limit-pass-1", ks, &native, &hybrid, &probes, &model);

    // The adopted start already covers the limit: no live survivor is left
    // below it, so a second limited pass is `NoWork` and copies nothing.
    let cancel = AtomicBool::new(false);
    let report = hybrid
        .remote_for_test()
        .relocation_pass(true, &cancel)
        .expect("a repeated limited pass must not fail");
    assert_eq!(
        report.gate,
        RelocationGate::NoWork,
        "a repeated limit with no new whole file must reclaim nothing: {report:?}"
    );
    assert_eq!(
        report.copied_survivors, 0,
        "a repeated limit must find no survivor left below it: {report:?}"
    );
    assert_same_reads(seed, "limit-pass-2", ks, &native, &hybrid, &probes, &model);

    // `None` finishes the stream, on both arms after a reopen.
    close_arm(native, "native before reopen");
    close_arm(hybrid, "hybrid before reopen");
    let unlimited = value_arm_config(None);
    let native = open_native(native_root.path(), &unlimited, &shape);
    let mut config = fixture.config.clone();
    config.index.relocation_strategy = RelocationStrategy::IndexBased(None);
    let hybrid = open_hybrid_with(&fixture, config);
    assert_same_reads(seed, "limit-reopen", ks, &native, &hybrid, &probes, &model);
    hybrid.flush().expect("flush after reopen");
    let report = hybrid_pass(&hybrid, "hybrid/limit-pass-3");
    assert_eq!(
        report.gate,
        RelocationGate::DidWork,
        "the unlimited pass must finish the sealed stream: {report:?}"
    );
    native_pass(&native, None, "native/limit-pass-3");
    assert_same_reads(seed, "limit-pass-3", ks, &native, &hybrid, &probes, &model);
    close_arm(native, "native after reopen");
    close_arm(hybrid, "hybrid after reopen");
}

/// A filter that removes every even key and keeps every odd one. Both native
/// strategies accept it; it never stops, so the differential compares a full
/// pass. The even keys are written first, so each stream's removed bytes sit in
/// its first whole files and both arms' floors cover them.
fn parity_shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        KS_NAME,
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|key: &[u8], _: &[u8]| {
            if key[3].is_multiple_of(2) {
                Decision::Remove
            } else {
                Decision::Keep
            }
        }),
    );
    // Volume only, in enough cells that the native arm's index blob stays
    // inside one 4 KiB fragment. No filter: every pad key is a survivor.
    builder.add_key_space_config(PAD_KS, 4, 16, KeyType::uniform(1), KeySpaceConfig::new());
    builder.build()
}

/// `(removed, kept, padding)` keys of one filtered run. The removed keys are
/// written first, the padding second and the kept keys last, so every stream's
/// removed bytes sit in its first whole files.
fn removal_plan() -> (KeySet, KeySet, KeySet) {
    let all = keys();
    let removed: KeySet = all
        .iter()
        .filter(|key| key[3].is_multiple_of(2))
        .cloned()
        .collect();
    let kept: KeySet = all
        .iter()
        .filter(|key| !key[3].is_multiple_of(2))
        .cloned()
        .collect();
    // Eight prefixes: 128 keys, so the native arm's padding keyspace index
    // blob (one row per key) still fits one owner-sized fragment.
    let mut pad = Vec::with_capacity(128);
    for prefix in 0x80..0x88u8 {
        for index in 0..16u8 {
            pad.push(key(prefix, index));
        }
    }
    (removed, kept, pad)
}

/// Every listed key of one arm must read its model value.
fn assert_model(
    seed: u64,
    label: &str,
    db: &Arc<SuiDb>,
    ks: KeySpace,
    keys: &[Vec<u8>],
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
) {
    for key in keys {
        let actual = read_one(db, ks, key);
        assert_eq!(
            actual.as_deref(),
            Some(model[key].as_slice()),
            "{label} (seed={seed:#x}): key {} must keep its value",
            hex(key)
        );
    }
}

fn rows(seed: u64, keys: &[Vec<u8>]) -> Vec<(Vec<u8>, Vec<u8>)> {
    keys.iter()
        .map(|key| (key.clone(), value(seed, key, 0)))
        .collect()
}

/// Compare the filtered keyspace and the padding keyspace on both arms with
/// the model.
#[allow(
    clippy::too_many_arguments,
    reason = "one call site per phase; the arguments are the phase's own probes"
)]
fn assert_reads(
    seed: u64,
    label: &str,
    native: &Arc<SuiDb>,
    hybrid: &Arc<SuiDb>,
    ks: KeySpace,
    pad_ks: KeySpace,
    root_probes: &[Vec<u8>],
    pad_probes: &[Vec<u8>],
    model: &BTreeMap<Vec<u8>, Vec<u8>>,
) {
    assert_same_reads(seed, label, ks, native, hybrid, root_probes, model);
    assert_same_reads(
        seed,
        &format!("{label}-pad"),
        pad_ks,
        native,
        hybrid,
        pad_probes,
        model,
    );
}

/// Gate (c): the filtered keyspace. Native answers a removed key absent once
/// its frame is reclaimed; the hybrid removes the entry and retires the same
/// files. The padding keyspace fills whole files in every stream, so each
/// stream's removed prefix is below both arms' whole-file floors. Kept and
/// padding keys keep their values on both arms, through the pass and a reopen.
#[test]
fn index_based_removals_of_a_filtered_keyspace_match_native() {
    let seed = 0x4e_11_ce_u64;
    let index_config = index_arm_config(None);
    let value_config = value_arm_config(None);
    let shape = parity_shape();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let native_root = tempdir::TempDir::new("index-based-filter-native").expect("native root");
    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    let ks = native.ks(KS_NAME);
    let pad_ks = native.ks(PAD_KS);
    let (removed, kept, pad) = removal_plan();
    let mut model = BTreeMap::new();
    for key in removed.iter().chain(pad.iter()).chain(kept.iter()) {
        model.insert(key.clone(), value(seed, key, 0));
    }
    for arm in [&native, &hybrid] {
        puts(arm, ks, &rows(seed, &removed));
        puts(arm, pad_ks, &rows(seed, &pad));
        puts(arm, ks, &rows(seed, &kept));
    }
    let mut root_probes = removed.clone();
    root_probes.extend(kept.iter().cloned());
    assert_reads(
        seed,
        "filtered-baseline",
        &native,
        &hybrid,
        ks,
        pad_ks,
        &root_probes,
        &pad,
        &model,
    );

    native
        .flush()
        .expect("flush the native arm before the pass");
    hybrid.flush().expect("flush before the pass");
    let native_before = native_floor(&native);
    let report = hybrid_pass(&hybrid, "hybrid/filtered-pass");
    assert_eq!(report.gate, RelocationGate::DidWork, "{report:?}");
    assert!(
        report.copied_survivors > 0,
        "the kept keys below the target must be copied: {report:?}"
    );
    native_pass(&native, None, "native/filtered-pass");
    assert!(
        native_floor(&native) > native_before,
        "the native filtered pass must reclaim the removed prefix: {native_before} -> {}",
        native_floor(&native)
    );

    // The removed keys are absent on both arms; kept and padding keys read
    // back their values.
    for key in &removed {
        let native_value = read_one(&native, ks, key);
        let hybrid_value = read_one(&hybrid, ks, key);
        assert_eq!(
            native_value,
            None,
            "native must answer the removed key {} absent",
            hex(key)
        );
        assert_eq!(
            hybrid_value,
            None,
            "the hybrid must answer the removed key {} absent",
            hex(key)
        );
        model.remove(key);
    }
    assert_reads(
        seed,
        "filtered-pass",
        &native,
        &hybrid,
        ks,
        pad_ks,
        &root_probes,
        &pad,
        &model,
    );

    close_arm(native, "native before reopen");
    close_arm(hybrid, "hybrid before reopen");
    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    assert_reads(
        seed,
        "filtered-reopen",
        &native,
        &hybrid,
        ks,
        pad_ks,
        &root_probes,
        &pad,
        &model,
    );

    native.flush().expect("flush the native arm after reopen");
    hybrid.flush().expect("flush after reopen");
    let report = hybrid_pass(&hybrid, "hybrid/filtered-pass-2");
    assert_eq!(report.gate, RelocationGate::DidWork, "{report:?}");
    native_pass(&native, None, "native/filtered-pass-2");
    assert_reads(
        seed,
        "filtered-pass-2",
        &native,
        &hybrid,
        ks,
        pad_ks,
        &root_probes,
        &pad,
        &model,
    );
    close_arm(native, "native after reopen");
    close_arm(hybrid, "hybrid after reopen");
}

/// A filter that stops at the first entry of its cell. An index walk visits
/// keys in key order, so the stop cannot mean "every byte below this record is
/// dead" the way a WAL scan can. The pass ends that cell's walk and caps the
/// stream's start below the lowest skipped byte, so every skipped value stays
/// readable; a pass that advanced the start anyway would retire those frames
/// and a live read would fail with the typed eviction.
#[test]
fn a_stop_keeps_every_skipped_value_readable() {
    let seed = 0x5170_0000_u64;
    let index_config = index_arm_config(None);
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        KS_NAME,
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|key: &[u8], _: &[u8]| {
            if key[3] == 0 {
                Decision::StopRelocation
            } else {
                Decision::Keep
            }
        }),
    );
    let shape = builder.build();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let hybrid = open_hybrid(&fixture);
    let ks = hybrid.ks(KS_NAME);
    let all = keys();
    let mut model = BTreeMap::new();
    for key in &all {
        model.insert(key.clone(), value(seed, key, 0));
    }
    puts(&hybrid, ks, &rows(seed, &all));
    hybrid.flush().expect("flush before the pass");

    let files_before: usize = (0..fixture.nodes.len())
        .map(|owner| owner_wal_files(&fixture, owner))
        .sum();
    let report = hybrid_pass(&hybrid, "hybrid/stop-pass");
    assert_eq!(
        report.copied_survivors, 0,
        "the stop ends the cell's walk before any survivor is copied: {report:?}"
    );
    for stream in &report.streams {
        assert_eq!(
            stream.rounded_target, 0,
            "the first skipped byte is inside the first file, so the stream keeps \
             no whole-file target: {report:?}"
        );
    }
    let files_after: usize = (0..fixture.nodes.len())
        .map(|owner| owner_wal_files(&fixture, owner))
        .sum();
    assert_eq!(
        files_before, files_after,
        "no start moved, so no owner file may be retired"
    );
    for pass in 0..2 {
        hybrid_pass(&hybrid, "hybrid/stop-pass-again");
        assert_model(
            seed,
            &format!("stop-pass-{pass}"),
            &hybrid,
            ks,
            &all,
            &model,
        );
    }
    assert_model(seed, "stop-pass", &hybrid, ks, &all, &model);

    close_arm(hybrid, "hybrid before reopen");
    let hybrid = open_hybrid(&fixture);
    hybrid.flush().expect("flush after reopen");
    let report = hybrid_pass(&hybrid, "hybrid/stop-after-reopen");
    assert_eq!(report.copied_survivors, 0, "{report:?}");
    assert_model(seed, "stop-after-reopen", &hybrid, ks, &all, &model);
    hybrid.wait_for_background_threads_to_finish().unwrap();
}

/// A write that lands between a copy and its re-point keeps the newer value:
/// the pass compares the current position with the scanned expectation
/// (`same_physical`) and skips the stale one. The parked chunk is a real copy
/// set, so the test observes the compare-and-set, not an empty pass.
#[test]
fn a_write_between_the_copy_and_the_re_point_survives_an_index_based_pass() {
    let seed = 0x77_aa_11_u64;
    let index_config = index_arm_config(None);
    let shape = shape();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let hybrid = open_hybrid(&fixture);
    let ks = hybrid.ks(KS_NAME);
    let all = keys();
    let rows: Vec<(Vec<u8>, Vec<u8>)> = all
        .iter()
        .map(|key| (key.clone(), value(seed, key, 0)))
        .collect();
    puts(&hybrid, ks, &rows);
    hybrid.flush().expect("flush before the pass");

    let core = hybrid.remote_for_test().write_core();
    let (entered, paused) = std::sync::mpsc::sync_channel(1);
    let (release, resume) = std::sync::mpsc::sync_channel(1);
    core.test_hooks.lock().relocation_copy_pause = Some((entered, resume));
    let cancel = AtomicBool::new(false);
    let rewrite = value(seed, &all[0], 9);
    std::thread::scope(|scope| {
        // A failing premise drops the release sender: the parked pass resumes
        // and the test fails instead of hanging.
        let release = release;
        let paused = paused;
        let pass = scope.spawn(|| {
            hybrid
                .remote_for_test()
                .relocation_pass(true, &cancel)
                .expect("index-based pass")
        });
        let (parked_ks, first) = paused
            .recv_timeout(Duration::from_secs(120))
            .expect("the pass parked between the copy and the re-point");
        assert_eq!(
            parked_ks, ks,
            "premise: the parked cell is the test keyspace"
        );
        let rewritten = first.expect("the parked chunk names a key").to_vec();
        let before = read_one(&hybrid, ks, &rewritten);
        assert!(
            before.is_some(),
            "premise: the key is readable before the pass"
        );

        // The write lands after the copy and before the re-point.
        put_batch(&hybrid, ks, &[(rewritten.clone(), rewrite.clone())]);
        let during = read_one(&hybrid, ks, &rewritten);
        assert_eq!(
            during.as_deref(),
            Some(rewrite.as_slice()),
            "the rewrite is readable while the pass is parked"
        );
        release.send(()).expect("release the parked pass");
        let report = pass.join().expect("the pass thread");
        assert!(!report.deferred, "{report:?}");
        assert!(
            report.copied_survivors > 0,
            "premise: the pass copied survivors: {report:?}"
        );

        // The newer write survives the parked chunk's re-point.
        let after = read_one(&hybrid, ks, &rewritten);
        assert_eq!(
            after.as_deref(),
            Some(rewrite.as_slice()),
            "the pass must not re-point over the newer write"
        );
    });

    // Every other key still reads its pre-pass value, and a second pass keeps
    // the newer write.
    let rewritten_key = all[0].clone();
    for key in &all {
        let (expected, label) = if *key == rewritten_key {
            (rewrite.clone(), "the rewrite")
        } else {
            (value(seed, key, 0), "the original value")
        };
        let actual = read_one(&hybrid, ks, key);
        assert_eq!(
            actual.as_deref(),
            Some(expected.as_slice()),
            "key {} after the parked pass must read {label}",
            hex(key)
        );
    }
    hybrid.flush().expect("flush before the second pass");
    let report = hybrid_pass(&hybrid, "hybrid/after-rewrite");
    assert!(report.copied_survivors > 0, "{report:?}");
    let after = read_one(&hybrid, ks, &rewritten_key);
    assert_eq!(
        after.as_deref(),
        Some(rewrite.as_slice()),
        "a later pass keeps the newer write"
    );
    hybrid.wait_for_background_threads_to_finish().unwrap();
}

/// A `Remove` decision that loses its compare-and-set keeps the newer write:
/// the arm compares the live row with the scanned expectation
/// (`same_physical`), shadows the removal and lets the newer row win, so the
/// rewrite stays readable and its file is kept. Every other removed key of the
/// same cell is still gone. A removal that acted on the key alone would erase
/// the concurrent write.
#[test]
fn a_remove_that_loses_the_compare_and_set_keeps_the_newer_write() {
    let seed = 0x5a_1e_0b_u64;
    let index_config = index_arm_config(None);
    let shape = parity_shape();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let hybrid = open_hybrid(&fixture);
    let ks = hybrid.ks(KS_NAME);
    let pad_ks = hybrid.ks(PAD_KS);
    let (removed, kept, pad) = removal_plan();
    puts(&hybrid, ks, &rows(seed, &removed));
    puts(&hybrid, pad_ks, &rows(seed, &pad));
    puts(&hybrid, ks, &rows(seed, &kept));
    hybrid.flush().expect("flush before the pass");

    // The first removed key is the first frame of its stream and below every
    // target, so the pass's removal list names it.
    let rewritten = removed[0].clone();
    let rewrite = value(seed, &rewritten, 9);
    let core = hybrid.remote_for_test().write_core();
    let (entered, paused) = std::sync::mpsc::sync_channel(1);
    let (release, resume) = std::sync::mpsc::sync_channel(1);
    core.test_hooks.lock().relocation_copy_pause = Some((entered, resume));
    let cancel = AtomicBool::new(false);
    let report = std::thread::scope(|scope| {
        // A failing premise drops the release sender: the parked pass resumes
        // and the test fails instead of hanging.
        let release = release;
        let pass = scope.spawn(|| {
            hybrid
                .remote_for_test()
                .relocation_pass(true, &cancel)
                .expect("index-based pass")
        });
        let (parked_ks, first) = paused
            .recv_timeout(Duration::from_secs(120))
            .expect("the pass parked on a Keep chunk");
        assert_eq!(
            parked_ks, ks,
            "premise: the parked chunk is the filtered keyspace"
        );
        assert!(
            first.is_some(),
            "premise: the parked chunk names a survivor"
        );
        // The rewrite lands after the view was read and before this cell's
        // removals run.
        put_batch(&hybrid, ks, &[(rewritten.clone(), rewrite.clone())]);
        assert_eq!(
            read_one(&hybrid, ks, &rewritten).as_deref(),
            Some(rewrite.as_slice()),
            "premise: the rewrite is readable while the pass is parked"
        );
        release.send(()).expect("release the parked pass");
        pass.join().expect("the pass thread")
    });
    assert!(!report.deferred, "{report:?}");
    assert!(
        report.copied_survivors > 0,
        "premise: the pass copied the parked cell's survivors: {report:?}"
    );

    // The newer write survives; every other removed key is absent.
    assert_eq!(
        read_one(&hybrid, ks, &rewritten).as_deref(),
        Some(rewrite.as_slice()),
        "the removal must not erase the newer write"
    );
    for key in &removed {
        if *key == rewritten {
            continue;
        }
        assert_eq!(
            read_one(&hybrid, ks, key),
            None,
            "the removed key {} must be absent",
            hex(key)
        );
    }
    for key in &kept {
        assert_eq!(
            read_one(&hybrid, ks, key).as_deref(),
            Some(value(seed, key, 0).as_slice()),
            "the kept key {} must read its value",
            hex(key)
        );
    }
    for key in &pad {
        assert_eq!(
            read_one(&hybrid, pad_ks, key).as_deref(),
            Some(value(seed, key, 0).as_slice()),
            "the padding key {} must read its value",
            hex(key)
        );
    }

    // The store persisted both the removals and the newer write.
    close_arm(hybrid, "hybrid before reopen");
    let hybrid = open_hybrid(&fixture);
    assert_eq!(
        read_one(&hybrid, ks, &rewritten).as_deref(),
        Some(rewrite.as_slice()),
        "the newer write survives the reopen"
    );
    for key in &removed {
        if *key == rewritten {
            continue;
        }
        assert_eq!(
            read_one(&hybrid, ks, key),
            None,
            "the persisted removal of {} must survive the reopen",
            hex(key)
        );
    }
    hybrid.wait_for_background_threads_to_finish().unwrap();
}

/// A crash after the durable capture (the region store plus the adopted starts)
/// and before any pruning: the interrupted pass reports the error, retires no
/// owner file, and a reopen keeps the removals and every value.
#[test]
fn a_crash_after_an_index_based_capture_keeps_the_model_on_reopen() {
    let seed = 0x33_c0_de_u64;
    let index_config = index_arm_config(None);
    let shape = parity_shape();
    let fixture = small_file_fixture(&index_config, shape.clone());
    let hybrid = open_hybrid(&fixture);
    let ks = hybrid.ks(KS_NAME);
    let pad_ks = hybrid.ks(PAD_KS);
    let (removed, kept, pad) = removal_plan();
    let mut model = BTreeMap::new();
    for key in removed.iter().chain(pad.iter()).chain(kept.iter()) {
        model.insert(key.clone(), value(seed, key, 0));
    }
    puts(&hybrid, ks, &rows(seed, &removed));
    puts(&hybrid, pad_ks, &rows(seed, &pad));
    puts(&hybrid, ks, &rows(seed, &kept));
    hybrid.flush().expect("flush before the pass");

    let files_before: Vec<Vec<std::path::PathBuf>> = (0..fixture.nodes.len())
        .map(|owner| owner_wal_paths(&fixture, owner))
        .collect();
    hybrid
        .remote_for_test()
        .write_core()
        .test_hooks
        .lock()
        .relocation_prune_interruption = true;
    let cancel = AtomicBool::new(false);
    let error = hybrid
        .remote_for_test()
        .relocation_pass(true, &cancel)
        .expect_err("the injected prune interruption fails the pass");
    assert!(
        format!("{error:?}").contains("injected relocation prune interruption"),
        "the pass failed at the prune boundary, got {error:?}"
    );
    for (owner, before) in files_before.iter().enumerate() {
        let after = owner_wal_paths(&fixture, owner);
        let missing: Vec<&std::path::PathBuf> =
            before.iter().filter(|path| !after.contains(path)).collect();
        assert!(
            missing.is_empty(),
            "a crash before pruning must retire no owner file of owner {owner}: {missing:?}"
        );
    }
    let files_after: Vec<usize> = (0..fixture.nodes.len())
        .map(|owner| owner_wal_files(&fixture, owner))
        .collect();

    // Reopen: the capture is durable even though no prune ran.
    drop(hybrid);
    let hybrid = open_hybrid(&fixture);
    for key in &removed {
        let absent = read_one(&hybrid, ks, key);
        assert_eq!(
            absent,
            None,
            "the persisted removal of {} must survive the crash",
            hex(key)
        );
    }
    for key in kept.iter().chain(pad.iter()) {
        let actual = read_one(&hybrid, ks, key).or_else(|| read_one(&hybrid, pad_ks, key));
        assert_eq!(
            actual.as_deref(),
            Some(model[key].as_slice()),
            "the value of {} must survive the crash",
            hex(key)
        );
    }
    // The next pass retires the files the crash left behind.
    hybrid.flush().expect("flush after reopen");
    let report = hybrid_pass(&hybrid, "hybrid/after-crash");
    assert_eq!(report.gate, RelocationGate::DidWork, "{report:?}");
    let files_retired: Vec<usize> = (0..fixture.nodes.len())
        .map(|owner| owner_wal_files(&fixture, owner))
        .collect();
    assert!(
        files_retired.iter().sum::<usize>() < files_after.iter().sum::<usize>(),
        "the pass after the crash must retire the old generation: {files_after:?} -> \
         {files_retired:?}"
    );
    hybrid.wait_for_background_threads_to_finish().unwrap();
}
