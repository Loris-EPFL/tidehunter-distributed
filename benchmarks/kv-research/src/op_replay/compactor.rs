//! Objects-compactor qualification (OPS-FORMAT.md, "Not covered by the trace";
//! COMPACTOR-PARITY.md §5 test 1).
//!
//! The native compactor runs at background index flush, outside any traced
//! call, so the exact trace disables it. This deterministic replay installs
//! Sui's objects compactor callback on both arms, writes the same object
//! versions in one single-level, quiescent geometry with no early pressure or
//! stale trigger, and forces compaction at one fixed point with the same API
//! on both arms: `force_rebuild_control_region`. Native flushes every dirty
//! cell and its flusher runs the compactor; leased runs the same forced
//! snapshot over the coordinator's native index, whose flusher runs the same
//! compactor. Oracle: after the force and after reopen, both arms hold exactly the
//! raw sorted `(key, value)` rows of the model (the latest row per ObjectID,
//! object tombstones included), through scans in both directions, point
//! reads and multi-reads. Reopen never calls the callback.
//!
//! Why raw rows on the native arm right after the force (COMPACTOR-PARITY.md
//! §7, Main's oracle split): native `force_rebuild_control_region` waits for
//! the WAL tracker and rebuilds from the current position (`db.rs:1602-1606`),
//! so pass 1 queues a flush of every dirty cell (`large_table.rs:2276`
//! `request_flush_if_behind`, threshold = current position) and waits for the
//! flusher (`db.rs:1626-1629`). The flusher writes the compacted level
//! (`flusher.rs:422`, single level; `:493` on promote). The compactor result
//! is not applied to the in-memory table (`flusher.rs:497`), but the flushed
//! cell does not keep that table: `update_flushed_index` →
//! `clear_after_flush(.., unload = true)` keeps the old in-memory rows only
//! when the keyspace disables unloading (`large_table.rs:2561`); otherwise it
//! retains only unprocessed entries (`:2585`), so reads use the compacted
//! level. This fixture uses `KeySpaceConfig::new()` (unloading enabled, no
//! value cache, `key_shape.rs:82,86`) and writes nothing during the force, so
//! every loaded dirty cell is flushed and dropped. A keyspace with
//! `disable_unload` or a value cache would need the compactor-invariant view
//! on the native arm instead.

use super::arm::{self, Arm, Kind};
use super::trace::OpenSpec;
use crate::backend::BackendResult;
use crate::leased_fixture::LeasedOwner;
use serde::Serialize;
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tidehunter::config::Config;
use tidehunter::key_shape::{Compactor, KeyIndexing, KeyShapeBuilder, KeySpaceConfig, KeyType};
use tidehunter::minibytes::Bytes;

const KEYSPACE: &str = "objects";
const OID: usize = 32;
const KEY: usize = OID + 8;
const OBJECTS: u8 = 64;
const MAX_VERSIONS: u64 = 5;
/// Latest value of every seventh object: Sui writes deletions as rows.
const TOMBSTONE: &[u8] = b"deleted-object-marker";

/// Sui's objects compactor (`sui-core/src/authority/authority_store_tables.rs:231-245`
/// at the Sui pin): within the visited keys, keep the highest key per ObjectID
/// (keys are `ObjectID || version` in big-endian bincode). Counts invocations.
fn objects_compactor(calls: &Arc<AtomicUsize>) -> Compactor {
    let calls = Arc::clone(calls);
    Box::new(move |iter: &mut dyn DoubleEndedIterator<Item = &Bytes>| {
        calls.fetch_add(1, Ordering::SeqCst);
        let mut retain = HashSet::new();
        let mut previous: Option<&[u8]> = None;
        for key in iter.rev() {
            if previous.is_some_and(|prev| prev == &key[..OID]) {
                continue;
            }
            previous = Some(&key[..OID]);
            retain.insert(key.clone());
        }
        retain
    })
}

fn key(object: u8, version: u64) -> Vec<u8> {
    let mut key = vec![0; KEY];
    // Spread ObjectIDs over the uniform cells by their leading bytes.
    key[..4].copy_from_slice(&(u32::from(object).wrapping_mul(0x9e37_79b9)).to_be_bytes());
    key[4] = object;
    key[OID..].copy_from_slice(&version.to_be_bytes());
    key
}

fn versions(object: u8) -> u64 {
    1 + u64::from(object) % MAX_VERSIONS
}

fn value(object: u8, version: u64) -> Vec<u8> {
    if version == versions(object) && object.is_multiple_of(7) {
        return TOMBSTONE.to_vec();
    }
    let mut value = vec![object; 48];
    value[..8].copy_from_slice(&version.to_be_bytes());
    value
}

/// Raw rows, sorted by key.
type Rows = BTreeMap<Vec<u8>, Vec<u8>>;

/// How the leased arm is forced. `FlushOnly` is the negative control: the
/// owners' durability barrier with no index snapshot, which never runs the
/// compactor. It leaves the old rows and must fail the raw oracle, which is
/// what shows the forced snapshot, not a side effect, does the compaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum LeasedForce {
    Forced,
    FlushOnly,
}

#[derive(Debug, Default, Serialize)]
pub struct CompactorArm {
    pub rows_before: usize,
    pub rows_after_compaction: usize,
    pub rows_after_reopen: usize,
    /// Callback invocations by the forced compaction.
    pub invocations: usize,
    /// Callback invocations from reopen, replay and every check after it.
    pub invocations_after_reopen: usize,
}

#[derive(Debug, Serialize)]
pub struct CompactorReport {
    pub objects: usize,
    pub versions_written: usize,
    pub model_rows: usize,
    pub native_forcing: &'static str,
    pub leased_forcing: &'static str,
    pub leased_force: LeasedForce,
    pub native: CompactorArm,
    pub leased: CompactorArm,
    pub problems: Vec<String>,
    pub passed: bool,
}

/// Run the qualification in the fresh directory `work`.
pub fn compactor_parity(work: &Path) -> BackendResult<CompactorReport> {
    compactor_parity_with(work, LeasedForce::Forced)
}

/// Run the qualification with the chosen leased forcing.
pub fn compactor_parity_with(work: &Path, force: LeasedForce) -> BackendResult<CompactorReport> {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config_indexing(
        KEYSPACE,
        KeyIndexing::fixed(KEY),
        8,
        KeyType::uniform(1),
        KeySpaceConfig::new(),
    );
    // Small files; flush and snapshot thresholds stay at the native defaults,
    // so no compaction runs before the forced point. The leased arm takes
    // the same cadence from this native configuration.
    let config = Config {
        frag_size: 1 << 20,
        wal_file_size: 4 << 20,
        ..Config::default()
    };
    let spec = OpenSpec {
        shape: serde_yaml::to_string(&builder.build())?,
        config: serde_yaml::to_string(&config)?,
        durable: false,
        filters: Vec::new(),
    };
    std::fs::create_dir(work)?;
    std::fs::create_dir(work.join("leased"))?;
    let mut arms = [
        Arm::new(Kind::Native, work.join("native")),
        Arm::new(Kind::Leased, work.join("leased").join("coordinator")),
    ];
    let mut owner = None;
    let result = qualify(&mut arms, &mut owner, &spec, force);
    for arm in &mut arms {
        if arm.db.is_some() {
            arm.close()?;
        }
    }
    if let Some(owner) = owner {
        owner.stop()?;
    }
    result
}

fn qualify(
    arms: &mut [Arm; 2],
    owner: &mut Option<LeasedOwner>,
    spec: &OpenSpec,
    force: LeasedForce,
) -> BackendResult<CompactorReport> {
    let mut written = Rows::new();
    let mut latest = BTreeMap::new();
    let calls = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    for (arm, calls) in arms.iter_mut().zip(&calls) {
        arm.open(
            spec,
            true,
            owner,
            Some((KEYSPACE, objects_compactor(calls))),
        )?;
    }
    // One batch per version round, identical on both arms.
    for version in 1..=MAX_VERSIONS {
        let objects: Vec<u8> = (0..OBJECTS)
            .filter(|object| versions(*object) >= version)
            .collect();
        for arm in arms.iter() {
            let db = arm.db()?;
            let ks = arm::keyspace(db, 0)?;
            let mut batch = db.write_batch();
            for object in &objects {
                batch.write(ks, key(*object, version), value(*object, version));
            }
            batch
                .commit()
                .map_err(|error| format!("{}: {error:?}", arm.kind.name()))?;
        }
        for object in &objects {
            let key = key(*object, version);
            written.insert(key.clone(), value(*object, version));
            latest.insert(key[..OID].to_vec(), key);
        }
    }
    let model: Rows = latest
        .into_values()
        .map(|key| {
            let value = written[&key].clone();
            (key, value)
        })
        .collect();
    let mut report = CompactorReport {
        objects: usize::from(OBJECTS),
        versions_written: written.len(),
        model_rows: model.len(),
        native_forcing: NATIVE_FORCING,
        leased_forcing: match force {
            LeasedForce::Forced => LEASED_FORCING,
            LeasedForce::FlushOnly => LEASED_FLUSH_ONLY,
        },
        leased_force: force,
        native: CompactorArm::default(),
        leased: CompactorArm::default(),
        problems: Vec::new(),
        passed: false,
    };
    let before = observe(
        arms,
        &written,
        &written,
        "before compaction",
        &mut report.problems,
    )?;
    for arm in arms.iter() {
        let db = arm.db()?;
        let name = arm.kind.name();
        let failed = |error| format!("{name}: forced compaction: {error:?}");
        match (arm.kind, force) {
            (Kind::Native, _) => db.force_rebuild_control_region().map_err(failed)?,
            (Kind::Leased, LeasedForce::FlushOnly) => db.flush().map_err(failed)?,
            (Kind::Leased, LeasedForce::Forced) => {
                // The hybrid force flushes below min(durable, applied, latch). On
                // the page-cache tier (`durable: false`) the durable cut can lag
                // the last puts, so the owners' barrier comes first.
                db.flush().map_err(failed)?;
                db.force_rebuild_control_region().map_err(failed)?;
            }
        }
    }
    let compacted = observe(
        arms,
        &written,
        &model,
        "after compaction",
        &mut report.problems,
    )?;
    let reopen_calls = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    for (arm, calls) in arms.iter_mut().zip(&reopen_calls) {
        arm.close()?;
        arm.open(
            spec,
            false,
            owner,
            Some((KEYSPACE, objects_compactor(calls))),
        )?;
    }
    let reopened = observe(arms, &written, &model, "after reopen", &mut report.problems)?;
    for (index, arm) in arms.iter().enumerate() {
        let after_reopen = reopen_calls[index].load(Ordering::SeqCst);
        if after_reopen != 0 {
            report.problems.push(format!(
                "{}: {after_reopen} callback invocations after reopen",
                arm.kind.name()
            ));
        }
        let summary = CompactorArm {
            rows_before: before[index],
            rows_after_compaction: compacted[index],
            rows_after_reopen: reopened[index],
            invocations: calls[index].load(Ordering::SeqCst),
            invocations_after_reopen: after_reopen,
        };
        match arm.kind {
            Kind::Native => report.native = summary,
            Kind::Leased => report.leased = summary,
        }
    }
    report.passed = report.problems.is_empty();
    Ok(report)
}

/// Check both arms against the raw `expected` rows: forward and reverse
/// scans, and point and multi reads of every written key. Returns the row
/// counts.
fn observe(
    arms: &[Arm; 2],
    written: &Rows,
    expected: &Rows,
    point: &str,
    problems: &mut Vec<String>,
) -> BackendResult<[usize; 2]> {
    let mut counts = [0; 2];
    let mut raws = Vec::with_capacity(2);
    let keys: Vec<Vec<u8>> = written.keys().cloned().collect();
    for (arm, count) in arms.iter().zip(&mut counts) {
        let name = arm.kind.name();
        let db = arm.db()?;
        let ks = arm::keyspace(db, 0)?;
        let mut raw = Rows::new();
        for row in db.iterator(ks) {
            let (row_key, row_value) = row.map_err(|error| format!("{error:?}"))?;
            if written.get(&row_key[..]).map(Vec::as_slice) != Some(&row_value[..]) {
                let row = arm::hex(&row_key);
                problems.push(format!("{name} {point}: unwritten row {row}"));
            }
            raw.insert(row_key.to_vec(), row_value.to_vec());
        }
        *count = raw.len();
        let mut reverse = db.iterator(ks);
        reverse.reverse();
        let mut backward = Vec::with_capacity(raw.len());
        for row in reverse {
            let (row_key, row_value) = row.map_err(|error| format!("{error:?}"))?;
            backward.push((row_key.to_vec(), row_value.to_vec()));
        }
        backward.reverse();
        if !backward.iter().cloned().eq(raw.clone()) {
            problems.push(format!("{name} {point}: reverse scan differs from forward"));
        }
        if &raw != expected {
            let missing = expected
                .keys()
                .filter(|key| !raw.contains_key(*key))
                .count();
            let extra = raw
                .keys()
                .filter(|key| !expected.contains_key(*key))
                .count();
            problems.push(format!(
                "{name} {point}: raw rows differ from the model ({missing} missing, {extra} extra)"
            ));
        }
        let gets = db.multi_get(ks, &keys);
        let exists = db.multi_exists(ks, &keys);
        for ((key, got), present) in keys.iter().zip(gets).zip(exists) {
            let want = expected.get(key);
            let got = got.map_err(|error| format!("{error:?}"))?;
            let single = db.get(ks, key).map_err(|error| format!("{error:?}"))?;
            let present = present.map_err(|error| format!("{error:?}"))?;
            let single_exists = db.exists(ks, key).map_err(|error| format!("{error:?}"))?;
            if got.as_deref() != want.map(Vec::as_slice)
                || single.as_deref() != want.map(Vec::as_slice)
                || present != want.is_some()
                || single_exists != want.is_some()
            {
                let key = arm::hex(key);
                problems.push(format!("{name} {point}: point or multi read of {key}"));
            }
        }
        raws.push(raw);
    }
    if raws[0] != raws[1] {
        problems.push(format!("{point}: native and leased raw rows differ"));
    }
    Ok(counts)
}

const NATIVE_FORCING: &str = "force_rebuild_control_region: flushes every dirty cell at the current position; the flusher runs the compactor (flusher.rs:422,493) and the unloading flush drops the uncompacted in-memory rows (large_table.rs:2561,2585), so raw rows are compared";
const LEASED_FORCING: &str = "force_rebuild_control_region after the owners' durability barrier: a forced snapshot over the coordinator's native index; its flusher runs the compactor on every dirty cell below the flush frontier";
const LEASED_FLUSH_ONLY: &str =
    "negative control: the owners' durability barrier only, no index snapshot, so no compaction";
