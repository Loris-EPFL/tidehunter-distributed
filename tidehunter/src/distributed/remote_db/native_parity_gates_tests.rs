//! Native-vs-hybrid local correctness gates over the leased fixture owners.
//!
//! One deterministic phase workload (puts, stale churn, overwrites, deletes,
//! batches, `drop_cells`, relocation passes, reopen) runs through a native
//! `sui::Db` and through the hybrid coordinator adapter of the same type, and
//! every probe key is compared after each phase. A second gate compares ordered
//! scans (forward, reverse, bounds, prefix) without and with a held checkpoint.
//! The expected state is the sequential replay of the same plan, so a failure
//! names the phase, the key and the seed.
//!
//! Both arms compare the same observable state, but their WAL geometry is not
//! identical. [`native_config`] keeps `Config::small()`'s sizes and supplies the
//! index geometry of both arms (the hybrid receives
//! [`HybridIndexConfig::from_native`] of it); the native arm opens with
//! [`native_arm_config`], whose value WAL uses the fixture's owner geometry
//! (two [`GATE_FRAG_SIZE`] fragments in a [`GATE_WAL_FILE`] file). WAL file size
//! is a layout knob only: native's WAL-based relocation cuts exclusively on
//! whole-file boundaries, so with the default 4 MiB files this key domain never
//! fills one file and every native pass reports `NoWork`. The smaller files let
//! the native arm reclaim real whole files, the hybrid arm already cuts on its
//! 8 KiB owner files, and no read, scan, reopen or model comparison can observe
//! the difference. Automatic snapshots are off (`snapshot_written_bytes`
//! unsigned-max) in both configs, so only the explicit store before a reopen
//! decides the persisted image.
//!
//! The relocation filter of the golden gate stops at the first put whose value
//! is the live value of its key and removes every earlier put. That is the
//! filter contract native relocation assumes: every entry below the Stop is
//! dead, so the prefix may be dropped and its files retired while every live
//! index entry stays above the terminal.
//!
//! These are module tests of the distributed tree in a sibling file (TST-3);
//! the owner comparison is observable state only, never an internal address.
#![cfg(feature = "test_methods")]

use super::super::pipeline_tests::Fixture;
use super::super::tests::recovery_metrics;
use crate::config::Config;
use crate::distributed::hybrid::HybridIndexConfig;
use crate::distributed::sui::{Checkpoint, Db as SuiDb};
use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use crate::{Decision, RelocationGate};
use minibytes::Bytes;
use parking_lot::Mutex;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

const KS_NAME: &str = "root";
/// Probed key domain: `PREFIXES * KEYS_PER_PREFIX` keys a phase may write,
/// plus [`SENTINELS`] keys no phase writes (absent-key probes).
const PREFIXES: u8 = 4;
const KEYS_PER_PREFIX: u8 = 16;
const SENTINELS: u8 = 4;
const SENTINEL_PREFIX: u8 = 0xee;
const VALUE_BYTES: usize = 160;
/// Completion deadline of one observed relocation pass. The wait ends on the
/// worker's completion signal; the deadline only bounds a stuck worker (TST-9).
pub(super) const PASS_DEADLINE: Duration = Duration::from_secs(120);

pub(super) type Model = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Debug)]
pub(super) enum Edit {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[derive(Clone, Debug, Default)]
pub(super) struct Phase {
    /// Single-key inserts and removes, applied in order.
    pub(super) singles: Vec<Edit>,
    /// Batches committed in order; each holds at most the fixture's batch
    /// operation limit (four).
    pub(super) batches: Vec<Vec<Edit>>,
    pub(super) drop_cells: bool,
    pub(super) relocate: bool,
}

impl Phase {
    pub(super) fn relocate() -> Self {
        Self {
            relocate: true,
            ..Self::default()
        }
    }
}

/// Ordered scan bounds: lower inclusive, upper exclusive, optional reversal,
/// exactly as [`crate::iterators::db_iterator::DbIterator`] applies them.
#[derive(Clone, Debug)]
pub(super) struct Bounds {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    reverse: bool,
}

impl Bounds {
    pub(super) fn all() -> Self {
        Self {
            lower: None,
            upper: None,
            reverse: false,
        }
    }

    fn reverse_all() -> Self {
        Self {
            reverse: true,
            ..Self::all()
        }
    }

    fn between(lower: Vec<u8>, upper: Vec<u8>, reverse: bool) -> Self {
        Self {
            lower: Some(lower),
            upper: Some(upper),
            reverse,
        }
    }
}

/// The unfiltered single-keyspace shape: key bytes are the index bytes, and one
/// cell holds the whole domain.
pub(super) fn shape() -> KeyShape {
    KeyShape::new_single(4, 1, KeyType::uniform(1))
}

/// The same shape with a native relocation filter over the live model: `Stop` at
/// the first live value, `Remove` before it.
pub(super) fn filtered_shape(live: Arc<Mutex<Model>>) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config(
        KS_NAME,
        4,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(move |key: &[u8], value: &[u8]| {
            let model = live.lock();
            if model
                .get(key)
                .is_some_and(|expected| expected.as_slice() == value)
            {
                Decision::StopRelocation
            } else {
                Decision::Remove
            }
        }),
    );
    builder.build()
}

/// The native arm's value WAL fragment: the fixture's owner fragment, so an
/// index or record frame that fits an owner WAL also fits a native one.
pub(super) const GATE_FRAG_SIZE: u64 = 4096;
/// The native arm's value WAL file: two fragments, matching the fixture's owner
/// WAL file. Native relocation reclaims only whole files, so a file larger than
/// this fixture's dead-prefix block would report `NoWork` on every pass.
pub(super) const GATE_WAL_FILE: u64 = 8192;

/// The shared index geometry of both arms and the hybrid's own index WAL.
/// Automatic snapshots are disabled so only an explicit store changes the
/// persisted index image.
pub(super) fn native_config() -> Arc<Config> {
    let mut config = Config::small();
    config.snapshot_written_bytes = u64::MAX;
    Arc::new(config)
}

/// The native arm's configuration: [`native_config`] with the fixture's owner
/// WAL geometry, so its WAL-based relocation finds whole dead files to reclaim.
/// Only the value/index WAL layout changes; every observable result is the same.
pub(super) fn native_arm_config() -> Arc<Config> {
    let mut config = Config::small();
    config.frag_size = GATE_FRAG_SIZE;
    config.wal_file_size = GATE_WAL_FILE;
    config.snapshot_written_bytes = u64::MAX;
    Arc::new(config)
}

/// A provisioned five-owner fixture whose coordinator index geometry mirrors
/// `config`, so both arms share the index layout.
pub(super) fn fixture(config: &Arc<Config>, shape: KeyShape) -> Fixture {
    Fixture::new_with_config(128, shape, 2, |remote| {
        remote.index = HybridIndexConfig::from_native(config);
    })
}

pub(super) fn open_native(root: &Path, config: &Arc<Config>, shape: &KeyShape) -> Arc<SuiDb> {
    SuiDb::open_native(root, shape.clone(), Arc::clone(config), recovery_metrics())
        .unwrap_or_else(|error| panic!("native open failed: {error:?}"))
}

pub(super) fn open_hybrid(fixture: &Fixture) -> Arc<SuiDb> {
    SuiDb::open_remote_local_fixture(
        &fixture.authority(),
        fixture.shape.clone(),
        fixture.config.clone(),
    )
    .unwrap_or_else(|error| panic!("hybrid open failed: {error:?}"))
}

/// Flush values, store the index, then join every background thread. The
/// stored index keeps the next open's replay bounded, as a clean Sui close.
pub(super) fn close_arm(db: Arc<SuiDb>, label: &str) {
    db.flush()
        .unwrap_or_else(|error| panic!("{label}: flush before close failed: {error:?}"));
    db.force_rebuild_control_region()
        .unwrap_or_else(|error| panic!("{label}: forced index store failed: {error:?}"));
    db.wait_for_background_threads_to_finish()
        .unwrap_or_else(|error| panic!("{label}: shutdown failed: {error:?}"));
}

pub(super) fn key(prefix: u8, index: u8) -> Vec<u8> {
    vec![prefix, 0, 0, index]
}

pub(super) fn keys() -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(usize::from(PREFIXES) * usize::from(KEYS_PER_PREFIX));
    for prefix in 0..PREFIXES {
        for index in 0..KEYS_PER_PREFIX {
            keys.push(key(prefix, index));
        }
    }
    keys
}

fn sentinel_keys() -> Vec<Vec<u8>> {
    (0..SENTINELS)
        .map(|index| key(SENTINEL_PREFIX, index))
        .collect()
}

fn probe_keys() -> Vec<Vec<u8>> {
    let mut probes = keys();
    probes.extend(sentinel_keys());
    probes
}

fn mix(mut word: u64) -> u64 {
    word = word.wrapping_add(0x9e37_79b9_7f4a_7c15);
    word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    word ^ (word >> 31)
}

/// Deterministic value bytes for `(seed, key, generation)`.
pub(super) fn value(seed: u64, key: &[u8], generation: u64) -> Vec<u8> {
    let mut word = mix(seed ^ generation.rotate_left(17));
    for &byte in key {
        word = mix(word ^ u64::from(byte));
    }
    (0..VALUE_BYTES)
        .map(|index| {
            word = mix(word ^ index as u64);
            (word >> 24) as u8
        })
        .collect()
}

pub(super) fn hex(key: &[u8]) -> String {
    let mut rendered = String::with_capacity(key.len() * 2);
    for byte in key {
        rendered.push(char::from_digit(u32::from(byte >> 4), 16).expect("high nibble"));
        rendered.push(char::from_digit(u32::from(byte & 0xf), 16).expect("low nibble"));
    }
    rendered
}

fn describe(value: Option<&[u8]>) -> String {
    match value {
        None => "absent".to_owned(),
        Some(bytes) => format!("{}B {:02x?}", bytes.len(), &bytes[..bytes.len().min(8)]),
    }
}

pub(super) fn read_one(db: &Arc<SuiDb>, ks: KeySpace, key: &[u8]) -> Option<Vec<u8>> {
    db.get(ks, key)
        .unwrap_or_else(|error| panic!("get key {} failed: {error:?}", hex(key)))
        .map(|value| value.to_vec())
}

/// Compare every probe key of both arms against the sequential model.
pub(super) fn assert_same_reads(
    seed: u64,
    phase: &str,
    ks: KeySpace,
    native: &Arc<SuiDb>,
    hybrid: &Arc<SuiDb>,
    probes: &[Vec<u8>],
    expected: &Model,
) {
    let mut differing = 0;
    let mut differences = Vec::new();
    for key in probes {
        let want = expected.get(key).map(Vec::as_slice);
        let native_value = read_one(native, ks, key);
        let hybrid_value = read_one(hybrid, ks, key);
        if native_value.as_deref() != want || hybrid_value.as_deref() != want {
            differing += 1;
            if differences.len() < 8 {
                differences.push(format!(
                    "key {}: native={} hybrid={} expected={}",
                    hex(key),
                    describe(native_value.as_deref()),
                    describe(hybrid_value.as_deref()),
                    describe(want),
                ));
            }
        }
    }
    assert!(
        differing == 0,
        "{phase} (seed={seed:#x}): {differing} of {} probe keys differ; first: {differences:#?}",
        probes.len()
    );
}

pub(super) fn model_edit(model: &mut Model, edit: &Edit) {
    match edit {
        Edit::Put(key, value) => {
            model.insert(key.clone(), value.clone());
        }
        Edit::Delete(key) => {
            model.remove(key);
        }
    }
}

/// Sequential oracle for one phase: the same plan applied to a map.
pub(super) fn sequential_apply(model: &mut Model, phase: &Phase) {
    for edit in &phase.singles {
        model_edit(model, edit);
    }
    for batch in &phase.batches {
        for edit in batch {
            model_edit(model, edit);
        }
    }
    if phase.drop_cells {
        model.clear();
    }
}

/// Apply one phase to one arm. Returns the relocation gate of a pass, if the
/// phase ran one.
pub(super) fn apply_phase(
    db: &Arc<SuiDb>,
    ks: KeySpace,
    phase: &Phase,
    label: &str,
) -> Option<RelocationGate> {
    for edit in &phase.singles {
        match edit {
            Edit::Put(key, value) => db.insert(ks, key.clone(), value.clone()),
            Edit::Delete(key) => db.remove(ks, key.clone()),
        }
        .unwrap_or_else(|error| {
            panic!(
                "{label}: single edit on key {} failed: {error:?}",
                hex(edit.key())
            )
        });
    }
    for (index, batch) in phase.batches.iter().enumerate() {
        let mut writes = db.write_batch();
        for edit in batch {
            match edit {
                Edit::Put(key, value) => writes.write(ks, key.clone(), value.clone()),
                Edit::Delete(key) => writes.delete(ks, key.clone()),
            }
        }
        writes
            .commit()
            .unwrap_or_else(|error| panic!("{label}: batch {index} commit failed: {error:?}"));
    }
    if phase.drop_cells {
        db.drop_cells_in_range(ks, &[0; 4], &[0xff; 4])
            .unwrap_or_else(|error| panic!("{label}: drop_cells failed: {error:?}"));
    }
    if !phase.relocate {
        return None;
    }
    db.flush()
        .unwrap_or_else(|error| panic!("{label}: flush before relocation failed: {error:?}"));
    let (observation, receipt) = db
        .start_relocation_for_test(Instant::now() + PASS_DEADLINE)
        .unwrap_or_else(|error| panic!("{label}: relocation pass failed: {error:?}"));
    assert_eq!(
        receipt.is_some(),
        db.native().is_none(),
        "{label}: only the hybrid reports a relocation receipt"
    );
    eprintln!(
        "{label}: relocation gate {:?}, {} streams, {} copied survivors",
        observation.gate,
        observation.streams.len(),
        observation.copied_survivors
    );
    Some(observation.gate)
}

impl Edit {
    fn key(&self) -> &[u8] {
        match self {
            Self::Put(key, _) | Self::Delete(key) => key,
        }
    }
}

/// Ordered rows of one arm under `bounds`.
pub(super) fn scan(db: &Arc<SuiDb>, ks: KeySpace, bounds: &Bounds) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iterator = db.iterator(ks);
    if let Some(lower) = &bounds.lower {
        iterator.set_lower_bound(lower.clone());
    }
    if let Some(upper) = &bounds.upper {
        iterator.set_upper_bound(upper.clone());
    }
    if bounds.reverse {
        iterator.reverse();
    }
    iterator
        .collect::<std::result::Result<Vec<(Bytes, Bytes)>, _>>()
        .unwrap_or_else(|error| panic!("ordered scan failed: {error:?}"))
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}

fn checkpoint_scan(
    checkpoint: Checkpoint,
    ks: KeySpace,
    bounds: &Bounds,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut iterator = checkpoint.iterator(ks);
    if let Some(lower) = &bounds.lower {
        iterator.set_lower_bound(lower.clone());
    }
    if let Some(upper) = &bounds.upper {
        iterator.set_upper_bound(upper.clone());
    }
    if bounds.reverse {
        iterator.reverse();
    }
    iterator
        .collect::<std::result::Result<Vec<(Bytes, Bytes)>, _>>()
        .unwrap_or_else(|error| panic!("checkpoint scan failed: {error:?}"))
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}

/// Rows the model predicts for `bounds`: index order is raw-key byte order for
/// this fixed-length, unreduced shape.
pub(super) fn expected_rows(model: &Model, bounds: &Bounds) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = model
        .iter()
        .filter(|(key, _)| {
            bounds
                .lower
                .as_ref()
                .is_none_or(|lower| key.as_slice() >= lower.as_slice())
                && bounds
                    .upper
                    .as_ref()
                    .is_none_or(|upper| key.as_slice() < upper.as_slice())
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    if bounds.reverse {
        rows.reverse();
    }
    rows
}

fn row_text(row: Option<&(Vec<u8>, Vec<u8>)>) -> String {
    match row {
        None => "none".to_owned(),
        Some((key, value)) => format!("{}={}", hex(key), describe(Some(value))),
    }
}

pub(super) fn assert_same_rows(
    seed: u64,
    label: &str,
    native: &[(Vec<u8>, Vec<u8>)],
    hybrid: &[(Vec<u8>, Vec<u8>)],
    expected: &[(Vec<u8>, Vec<u8>)],
) {
    if native == expected && hybrid == expected {
        return;
    }
    let limit = native.len().max(hybrid.len()).max(expected.len());
    let mut differences = Vec::new();
    for index in 0..limit {
        if differences.len() == 8 {
            break;
        }
        let want = expected.get(index);
        let native_row = native.get(index);
        let hybrid_row = hybrid.get(index);
        if native_row != want || hybrid_row != want {
            differences.push(format!(
                "row {index}: native={} hybrid={} expected={}",
                row_text(native_row),
                row_text(hybrid_row),
                row_text(want)
            ));
        }
    }
    assert!(
        differences.is_empty(),
        "{label} (seed={seed:#x}): ordered rows differ (native {} rows, hybrid {} rows, \
         expected {} rows); first: {differences:#?}",
        native.len(),
        hybrid.len(),
        expected.len()
    );
}

/// The golden workload. The first two phases write only superseded versions, so
/// the first relocation pass has a dead prefix of more than one owner WAL file
/// and its Stop lands on a live overwrite.
fn golden_phases(seed: u64) -> Vec<(&'static str, Phase)> {
    let all = keys();
    let mut order = all.clone();
    order.shuffle(&mut StdRng::seed_from_u64(seed));
    let live_keys = &order[..48];
    let dead_keys = &order[48..];
    let mut phases: Vec<(&'static str, Phase)> = Vec::new();

    phases.push((
        "puts",
        Phase {
            singles: all
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, 0)))
                .collect(),
            ..Phase::default()
        },
    ));
    phases.push((
        "stale_churn",
        Phase {
            singles: all
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, 5)))
                .collect(),
            ..Phase::default()
        },
    ));
    phases.push((
        "overwrites",
        Phase {
            singles: live_keys
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, 1)))
                .collect(),
            ..Phase::default()
        },
    ));
    phases.push((
        "deletes",
        Phase {
            singles: dead_keys
                .iter()
                .map(|key| Edit::Delete(key.clone()))
                .collect(),
            ..Phase::default()
        },
    ));
    phases.push((
        "batches",
        Phase {
            batches: vec![
                sentinel_keys()
                    .iter()
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 2)))
                    .collect(),
                dead_keys[..4]
                    .iter()
                    .map(|key| Edit::Put(key.clone(), value(seed, key, 3)))
                    .collect(),
            ],
            ..Phase::default()
        },
    ));
    phases.push(("relocation", Phase::relocate()));
    phases.push((
        "drop_cells",
        Phase {
            drop_cells: true,
            ..Phase::default()
        },
    ));
    // `repopulate` overwrites every key below, so this whole block is dead when
    // the second pass scans it. Native's value WAL reclaims only whole files
    // (`GATE_WAL_FILE` bytes), and the first pass leaves less than one file
    // below its cut, so without this block the second pass would find no whole
    // dead file before `repopulate`'s first live value and report `NoWork`.
    phases.push((
        "dead_churn",
        Phase {
            singles: all
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, 9)))
                .collect(),
            ..Phase::default()
        },
    ));
    phases.push((
        "repopulate",
        Phase {
            singles: order[..40]
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, 11)))
                .collect(),
            batches: order[40..]
                .chunks(4)
                .map(|chunk| {
                    chunk
                        .iter()
                        .map(|key| Edit::Put(key.clone(), value(seed, key, 12)))
                        .collect()
                })
                .collect(),
            ..Phase::default()
        },
    ));
    phases.push(("relocation_after_repopulate", Phase::relocate()));
    phases
}

fn scan_cases() -> Vec<(&'static str, Bounds)> {
    vec![
        ("forward-all", Bounds::all()),
        ("reverse-all", Bounds::reverse_all()),
        (
            "forward-prefix-1",
            Bounds::between(key(1, 0), vec![2, 0, 0, 0], false),
        ),
        (
            "reverse-prefix-1",
            Bounds::between(key(1, 0), vec![2, 0, 0, 0], true),
        ),
        (
            "forward-middle",
            Bounds::between(key(1, 3), key(2, 5), false),
        ),
        (
            "reverse-middle",
            Bounds::between(key(1, 3), key(2, 5), true),
        ),
        (
            "forward-empty",
            Bounds::between(key(3, 8), key(3, 8), false),
        ),
    ]
}

/// Gate (a): one deterministic workload through both engines, compared after
/// every phase and again after a reopen.
#[test]
fn golden_phases_keep_native_and_hybrid_equal_to_the_sequential_model() {
    let seed = 0x900d_5eed_2026_1117;
    eprintln!("golden phases: seed={seed:#x}");
    let plan = golden_phases(seed);
    let index_config = native_config();
    let value_config = native_arm_config();
    let live = Arc::new(Mutex::new(Model::new()));
    let shape = filtered_shape(Arc::clone(&live));
    let fixture = fixture(&index_config, shape.clone());
    let native_root = tempdir::TempDir::new("native-golden-phases").expect("native root");
    let probes = probe_keys();

    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    let ks = native.ks(KS_NAME);
    for (name, phase) in plan {
        {
            let mut model = live.lock();
            sequential_apply(&mut model, &phase);
        }
        let expected = live.lock().clone();
        let native_gate = apply_phase(&native, ks, &phase, &format!("native/{name}"));
        let hybrid_gate = apply_phase(&hybrid, ks, &phase, &format!("hybrid/{name}"));
        if phase.relocate {
            eprintln!("{name}: native gate {native_gate:?}, hybrid gate {hybrid_gate:?}");
            for (arm, gate) in [("native", native_gate), ("hybrid", hybrid_gate)] {
                assert!(
                    matches!(gate, Some(RelocationGate::DidWork)),
                    "{name} (seed={seed:#x}): the {arm} relocation pass must reclaim the dead \
                     prefix below the first live value, got {gate:?}"
                );
            }
        }
        assert_same_reads(seed, name, ks, &native, &hybrid, &probes, &expected);
    }
    close_arm(native, "native before reopen");
    close_arm(hybrid, "hybrid before reopen");

    let native = open_native(native_root.path(), &value_config, &shape);
    let hybrid = open_hybrid(&fixture);
    let expected = live.lock().clone();
    assert_same_reads(seed, "reopen", ks, &native, &hybrid, &probes, &expected);
    close_arm(native, "native after reopen");
    close_arm(hybrid, "hybrid after reopen");
}

/// Gate (b): ordered scans without and with a held checkpoint. Both arms must
/// answer the model, so a wrong row, order, bound or checkpoint filter fails.
#[test]
fn ordered_scans_match_native_and_the_model_without_and_with_a_held_checkpoint() {
    let seed = 0x5ca9_2026_1117;
    eprintln!("scan parity: seed={seed:#x}");
    let config = native_config();
    let shape = shape();
    let fixture = fixture(&config, shape.clone());
    let native_root = tempdir::TempDir::new("native-scan-parity").expect("native root");
    let native = open_native(native_root.path(), &config, &shape);
    let hybrid = open_hybrid(&fixture);
    let ks = native.ks(KS_NAME);
    let all = keys();

    // Phase A: baseline state both engines reach before the checkpoint.
    let mut baseline = Model::new();
    let phase_a = Phase {
        singles: all
            .iter()
            .enumerate()
            .map(|(position, key)| {
                if position % 7 == 0 {
                    Edit::Delete(key.clone())
                } else {
                    Edit::Put(key.clone(), value(seed, key, 0))
                }
            })
            .collect(),
        ..Phase::default()
    };
    sequential_apply(&mut baseline, &phase_a);
    apply_phase(&native, ks, &phase_a, "native/baseline");
    apply_phase(&hybrid, ks, &phase_a, "hybrid/baseline");
    assert_same_reads(
        seed,
        "baseline",
        ks,
        &native,
        &hybrid,
        &probe_keys(),
        &baseline,
    );

    // Views pinned before the post-checkpoint writes.
    let native_forward = native.checkpoint();
    let hybrid_forward = hybrid.checkpoint();
    let native_reverse = native.checkpoint();
    let hybrid_reverse = hybrid.checkpoint();

    let mut after = baseline.clone();
    let phase_b = Phase {
        singles: all
            .iter()
            .step_by(5)
            .map(|key| Edit::Put(key.clone(), value(seed, key, 5)))
            .collect(),
        batches: vec![
            all.iter()
                .step_by(9)
                .take(4)
                .map(|key| Edit::Delete(key.clone()))
                .collect(),
            sentinel_keys()
                .iter()
                .map(|key| Edit::Put(key.clone(), value(seed, key, 6)))
                .collect(),
        ],
        ..Phase::default()
    };
    sequential_apply(&mut after, &phase_b);
    apply_phase(&native, ks, &phase_b, "native/after-checkpoint");
    apply_phase(&hybrid, ks, &phase_b, "hybrid/after-checkpoint");

    let bounds = Bounds::all();
    assert_same_rows(
        seed,
        "checkpoint forward",
        &checkpoint_scan(native_forward, ks, &bounds),
        &checkpoint_scan(hybrid_forward, ks, &bounds),
        &expected_rows(&baseline, &bounds),
    );
    let reverse = Bounds::reverse_all();
    assert_same_rows(
        seed,
        "checkpoint reverse",
        &checkpoint_scan(native_reverse, ks, &reverse),
        &checkpoint_scan(hybrid_reverse, ks, &reverse),
        &expected_rows(&baseline, &reverse),
    );

    for (name, bounds) in scan_cases() {
        assert_same_rows(
            seed,
            name,
            &scan(&native, ks, &bounds),
            &scan(&hybrid, ks, &bounds),
            &expected_rows(&after, &bounds),
        );
    }

    close_arm(native, "native before reopen");
    close_arm(hybrid, "hybrid before reopen");
    let native = open_native(native_root.path(), &config, &shape);
    let hybrid = open_hybrid(&fixture);
    for (name, bounds) in scan_cases() {
        assert_same_rows(
            seed,
            &format!("{name}-after-reopen"),
            &scan(&native, ks, &bounds),
            &scan(&hybrid, ks, &bounds),
            &expected_rows(&after, &bounds),
        );
    }
    assert_same_reads(seed, "reopen", ks, &native, &hybrid, &probe_keys(), &after);
    close_arm(native, "native after reopen");
    close_arm(hybrid, "hybrid after reopen");
}
