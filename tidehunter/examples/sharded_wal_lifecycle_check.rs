//! Bounded local lifecycle correctness exercise, not a throughput benchmark.
//! Uses five real TCP storage children and the synchronous native WAL oracle.
//! Native is a synchronous Put/Delete/reopen oracle. Filter-model removals use
//! explicit native tombstones, not a claim of native/remote pruner equivalence.
//! Run: cargo run -p tidehunter --release --features experimental-distributed,test-utils
//!      --example sharded_wal_lifecycle_check -- NEW_OUTPUT_DIRECTORY
//!
//! `lifecycle.yaml` contains checked operation/state/retry/recovery results,
//! charged metadata and measured WAL-path retirement, not synthetic timings.
//! The controller enforces a 180-second worker deadline, stops owned children
//! on success or failure, and removes only its temporary data (256 MiB bound).
//! Node/worker logs and the report remain in the new output directory.

#[path = "support/tcp_cluster.rs"]
mod tcp_cluster;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tidehunter::Decision;
use tidehunter::config::Config;
use tidehunter::distributed::codec::{BatchLimits, Mutation};
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::native_index::IndexReadCut;
use tidehunter::distributed::remote_db::{RemoteDatabase, RemoteDatabaseConfig};
use tidehunter::distributed::storage::WalMemberReadPolicy;
use tidehunter::distributed::sui::{Db, DbIterator};
use tidehunter::distributed::tcp::{PollingPolicy, Transport};
use tidehunter::distributed::types::{BatchVersion, ValueRecordReference};
use tidehunter::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

type Result<T> = tcp_cluster::Result<T>;
type State = BTreeMap<Vec<u8>, Vec<u8>>;
const KIB: usize = 1024;
const MIB: usize = KIB * KIB;
const HISTORY_BATCHES: u64 = 4096;
const SUFFIX_BATCHES: u64 = 1024;
const KEYS: u64 = 128;
const KEEP_KEY: u64 = 1000;
const STOP_KEY: u64 = 1001;
const SAME_BATCH_KEY: u64 = 1002;
const AFTER_STOP_KEY: u64 = 1003;
const BEFORE_STOP_KEY: u64 = 1004;
const RETRY_BATCH: u64 = 1024;
const INDEX_BUDGET: usize = MIB;
const RESIDENT_BATCHES: usize = 64;
const NODE_RESIDENT_BATCHES: usize = 32;
const WAL_FILE_BYTES: u64 = (256 * KIB) as u64;
const MAX_DATA_BYTES: u64 = (256 * MIB) as u64;
const WORKER_TIMEOUT: Duration = Duration::from_secs(180);

fn error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}
fn adapter_error(error: tidehunter::db::DbError) -> io::Error {
    self::error(format!("Sui adapter: {error:?}"))
}
fn require(condition: bool, message: &str) -> Result<()> {
    if !condition {
        return Err(error(message).into());
    }
    Ok(())
}
fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}
fn key(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}
fn operation(id: u64) -> [u8; 16] {
    let mut operation = [0x6c; 16];
    operation[8..].copy_from_slice(&id.to_be_bytes());
    operation
}
fn value(id: u64, generation: u64, decision: u8) -> Bytes {
    let mut bytes = vec![0; 512];
    bytes[0] = decision;
    bytes[1..9].copy_from_slice(&id.to_be_bytes());
    bytes[9..17].copy_from_slice(&generation.to_be_bytes());
    for (offset, byte) in bytes[17..].iter_mut().enumerate() {
        *byte = ((id * 31 + generation * 17 + offset as u64 * 73) % 251) as u8;
    }
    bytes.into()
}
fn put(ks: KeySpace, id: u64, generation: u64, decision: u8) -> Mutation {
    Mutation::Put {
        keyspace: ks,
        key: key(id).into(),
        value: value(id, generation, decision),
    }
}
fn mutations(ks: KeySpace, batch: u64, key_count: u64) -> Vec<Mutation> {
    let id = batch % key_count;
    vec![
        put(ks, id, batch * 2, 0),
        put(ks, (id + 1) % key_count, batch, 0),
        Mutation::Delete {
            keyspace: ks,
            key: key((id + 2) % key_count).into(),
        },
        put(ks, id, batch * 2 + 1, 0),
    ]
}
fn shape(stop: Arc<AtomicBool>) -> KeyShape {
    KeyShape::new_single_config(
        8,
        4,
        KeyType::uniform(2),
        KeySpaceConfig::new().with_relocation_filter(move |_, value: &[u8]| {
            match value.first().copied() {
                Some(1) => Decision::Keep,
                Some(2) if stop.load(Ordering::Acquire) => Decision::StopRelocation,
                _ => Decision::Remove,
            }
        }),
    )
}
fn storage() -> LocalInventoryConfig {
    LocalInventoryConfig {
        database: [0x6c; 16],
        partitions: vec![0, 1, 2, 3, 4],
        batch_limits: BatchLimits {
            max_operations: 16,
            max_encoded_bytes: 16 * KIB,
        },
        group_limits: GroupLimits {
            max_batches: 16,
            max_encoded_bytes: 64 * KIB - 8,
        },
        fragment_bytes: (64 * KIB) as u64,
        wal_file_bytes: WAL_FILE_BYTES,
        max_batches_per_partition: NODE_RESIDENT_BATCHES,
        max_reference_metadata_bytes: 256 * KIB,
        max_epochs: 16,
    }
}
fn native_config() -> Arc<Config> {
    let mut config = Config::small();
    config.frag_size = (64 * KIB) as u64;
    config.wal_file_size = WAL_FILE_BYTES;
    config.max_maps = 3;
    config.max_index_maps = Some(3);
    config.max_dirty_keys = 32;
    config.snapshot_written_bytes = (4 * MIB) as u64;
    config.snapshot_unload_threshold = (8 * MIB) as u64;
    config.metrics_enabled = false;
    Arc::new(config)
}

#[derive(Serialize, Deserialize)]
struct Launch {
    data: PathBuf,
    output: PathBuf,
    remote: RemoteDatabaseConfig,
}
#[derive(Default, Serialize, Deserialize)]
struct OperationCounts {
    client_batches: u64,
    puts: u64,
    deletes: u64,
    exact_retries: u64,
    changed_identity_rejections: u64,
    pruning_passes: u64,
    explicit_checkpoints: u64,
    coordinator_reopens: u64,
    native_pruning_delete_batches: u64,
    native_pruning_deletes: u64,
}
#[derive(Serialize, Deserialize)]
struct FileCharge {
    logical_bytes: u64,
    allocated_bytes: Option<u64>,
}
#[derive(Serialize, Deserialize)]
struct DiskSnapshot {
    phase: String,
    all_data_logical_bytes: u64,
    all_data_allocated_bytes: Option<u64>,
    wal_files: BTreeMap<String, FileCharge>,
    authority_manifest_crc32: u32,
}
#[derive(Serialize, Deserialize)]
struct MetadataSnapshot {
    phase: String,
    resident_index_bytes: usize,
    transient_index_bytes: usize,
    logical_index_history_bytes: usize,
    retained_index_versions: usize,
    coordinator_outcome_resident_bytes: Option<usize>,
    coordinator_outcome_disk_bytes: Option<u64>,
    storage_reference_metadata_bytes: usize,
    checkpoint_count: u64,
    background_checkpoint_ns: u64,
    foreground_checkpoint_count: u64,
    /// Post-cut admissions until root adoption, not physical I/O overlap time.
    background_checkpoint_suffix_batches: u64,
    recovery_tail_batches: u64,
    retention_floor: String,
}
#[derive(Default, Serialize, Deserialize)]
struct Report {
    schema: u32,
    purpose: String,
    status: String,
    failure: Option<String>,
    checkpoint_counter_semantics: String,
    topology: String,
    native_durability: String,
    native_pruning_oracle: String,
    deterministic_identity_scheme: String,
    resident_index_budget_bytes: usize,
    resident_coordinator_batches: usize,
    resident_batches_per_partition: usize,
    history_batches: u64,
    suffix_batches: u64,
    worker_timeout_seconds: u64,
    disk_limit_bytes: u64,
    operations: OperationCounts,
    verified: BTreeMap<String, bool>,
    metadata: Vec<MetadataSnapshot>,
    adapter_counters: BTreeMap<String, u64>,
    disk: Vec<DiskSnapshot>,
    retired_file_count: usize,
    retired_logical_bytes: u64,
    retired_allocated_bytes: Option<u64>,
    pinned_payload_file: Option<String>,
    original_retry_version: Option<BatchVersion>,
    original_retry_payload_file: Option<String>,
    expired_cut_error: Option<String>,
    unavailable_counters: Vec<String>,
    owned_children_stopped: bool,
    owned_data_cleaned: bool,
}
fn save_report(output: &Path, report: &Report) -> Result<()> {
    let pending = output.join("lifecycle.pending.yaml");
    serde_yaml::to_writer(File::create(&pending)?, report)?;
    fs::rename(pending, output.join("lifecycle.yaml"))?;
    Ok(())
}
fn initial_report() -> Report {
    Report {
        schema: 1,
        purpose: "local operation/correctness/recovery exercise; not a speed or physical-I/O claim".into(),
        status: "running".into(),
        topology: "five owned TCP storage processes on one shared local SSD".into(),
        native_durability: "native synchronous value-WAL barrier after every foreground Put/Delete batch, including filter-model tombstones".into(),
        native_pruning_oracle: "independent synchronous Put/Delete/reopen operation oracle; explicit eligible filter-model keys become native tombstones; no mixed-filter WalBased pruner equivalence claim".into(),
        deterministic_identity_scheme: "raw: fixed 8-byte namespace + big-endian counter; Sui: fenced epoch + adapter counter".into(),
        checkpoint_counter_semantics: "background_checkpoint_suffix_batches counts post-cut admissions through root adoption; background_checkpoint_ns is captured lifetime; neither isolates device-I/O overlap".into(),
        resident_index_budget_bytes: INDEX_BUDGET,
        resident_coordinator_batches: RESIDENT_BATCHES,
        resident_batches_per_partition: NODE_RESIDENT_BATCHES,
        history_batches: HISTORY_BATCHES,
        suffix_batches: SUFFIX_BATCHES,
        worker_timeout_seconds: WORKER_TIMEOUT.as_secs(),
        disk_limit_bytes: MAX_DATA_BYTES,
        unavailable_counters: vec![
            "coordinator outcome disk byte charge is not exported; resident charge excludes allocator overhead".into(),
            "node retirement counters are not exported; physical path removals are measured directly".into(),
            "filesystem allocated bytes are unavailable on non-Unix targets".into(),
        ],
        ..Report::default()
    }
}
fn count_and_apply(state: &mut State, report: &mut Report, changes: &[Mutation]) {
    report.operations.client_batches += 1;
    for mutation in changes {
        match mutation {
            Mutation::Put { key, value, .. } => {
                state.insert(key.to_vec(), value.to_vec());
                report.operations.puts += 1;
            }
            Mutation::Delete { key, .. } => {
                state.remove(key.as_ref());
                report.operations.deletes += 1;
            }
        }
    }
}
fn adapter_apply(db: &Arc<Db>, changes: &[Mutation]) -> Result<()> {
    let mut batch = db.write_batch();
    for mutation in changes {
        match mutation {
            Mutation::Put {
                keyspace,
                key,
                value,
            } => batch.write(*keyspace, key.clone(), value.clone()),
            Mutation::Delete { keyspace, key } => batch.delete(*keyspace, key.clone()),
        }
    }
    batch.commit().map_err(adapter_error)?;
    Ok(())
}
fn collect(iter: DbIterator) -> Result<State> {
    let mut state = State::new();
    for row in iter {
        let (key, value) = row.map_err(adapter_error)?;
        require(
            state.insert(key.to_vec(), value.to_vec()).is_none(),
            "iterator returned a duplicate key",
        )?;
    }
    Ok(state)
}
fn value_summary(value: Option<&[u8]>) -> String {
    match value {
        None => "absent".into(),
        Some(value) => {
            let decision = match value.first() {
                Some(0) => "Remove",
                Some(1) => "Keep",
                Some(2) => "StopOrReleasedRemove",
                _ => "unknown",
            };
            let generation = value
                .get(9..17)
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_be_bytes);
            format!(
                "marker={decision},generation={generation:?},len={},crc32={:08x}",
                value.len(),
                crc32fast::hash(value)
            )
        }
    }
}
fn compare_value(
    phase: &str,
    source: &str,
    key: &[u8],
    actual: Option<&[u8]>,
    expected: Option<&[u8]>,
) -> Result<()> {
    if actual != expected {
        return Err(error(format!(
            "phase={phase} source={source} key={key:02x?} actual=({}) expected=({})",
            value_summary(actual),
            value_summary(expected),
        ))
        .into());
    }
    Ok(())
}
fn compare_state(phase: &str, source: &str, actual: &State, expected: &State) -> Result<()> {
    for (key, value) in expected {
        compare_value(
            phase,
            source,
            key,
            actual.get(key).map(Vec::as_slice),
            Some(value),
        )?;
    }
    for (key, value) in actual {
        if !expected.contains_key(key) {
            compare_value(phase, source, key, Some(value), None)?;
        }
    }
    Ok(())
}
fn verify_adapter(
    db: &Arc<Db>,
    native: &Arc<Db>,
    ks: KeySpace,
    expected: &State,
    phase: &str,
) -> Result<()> {
    compare_state(phase, "remote_scan", &collect(db.iterator(ks))?, expected)?;
    compare_state(
        phase,
        "native_scan",
        &collect(native.iterator(ks))?,
        expected,
    )?;
    let keys: Vec<_> = (0..KEYS)
        .chain([
            KEEP_KEY,
            STOP_KEY,
            SAME_BATCH_KEY,
            AFTER_STOP_KEY,
            BEFORE_STOP_KEY,
        ])
        .map(key)
        .collect();
    let remote_values = db.multi_get(ks, &keys);
    require(
        remote_values.len() == keys.len(),
        "multi_get changed result cardinality",
    )?;
    for (key, remote) in keys.iter().zip(remote_values) {
        let remote = remote.map_err(adapter_error)?;
        let local = native.get(ks, key).map_err(adapter_error)?;
        compare_value(
            phase,
            "remote_multi_get",
            key,
            remote.as_deref(),
            expected.get(key).map(Vec::as_slice),
        )?;
        compare_value(
            phase,
            "native_get",
            key,
            local.as_deref(),
            expected.get(key).map(Vec::as_slice),
        )?;
    }
    Ok(())
}
fn verify_raw(
    db: &mut RemoteDatabase,
    native: &Arc<Db>,
    ks: KeySpace,
    expected: &State,
    phase: &str,
) -> Result<()> {
    let state: State = db
        .scan(ks)?
        .into_iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect();
    compare_state(phase, "raw_remote_scan", &state, expected)?;
    compare_state(
        phase,
        "native_scan",
        &collect(native.iterator(ks))?,
        expected,
    )?;
    for id in (0..KEYS).chain([
        KEEP_KEY,
        STOP_KEY,
        SAME_BATCH_KEY,
        AFTER_STOP_KEY,
        BEFORE_STOP_KEY,
    ]) {
        let key = key(id);
        let remote = db.get(ks, &key)?;
        let local = native.get(ks, &key).map_err(adapter_error)?;
        compare_value(
            phase,
            "raw_remote_get",
            &key,
            remote.as_deref(),
            expected.get(&key).map(Vec::as_slice),
        )?;
        compare_value(
            phase,
            "native_get",
            &key,
            local.as_deref(),
            expected.get(&key).map(Vec::as_slice),
        )?;
    }
    Ok(())
}
fn metadata(db: &RemoteDatabase, phase: &str) -> Result<MetadataSnapshot> {
    let index = db.index_stats();
    require(
        index.charged_metadata_bytes <= INDEX_BUDGET,
        "resident index charge exceeds configured budget",
    )?;
    require(
        index.transient_metadata_bytes <= INDEX_BUDGET,
        "transient index charge exceeds configured budget",
    )?;
    let authority = db.authority_write_stats();
    Ok(MetadataSnapshot {
        phase: phase.into(),
        resident_index_bytes: index.charged_metadata_bytes,
        transient_index_bytes: index.transient_metadata_bytes,
        logical_index_history_bytes: db.index_history_bytes(),
        retained_index_versions: index.retained_versions,
        coordinator_outcome_resident_bytes: Some(db.metadata_history_bytes()),
        coordinator_outcome_disk_bytes: None,
        storage_reference_metadata_bytes: db.storage_stats()?.reference_metadata_bytes,
        checkpoint_count: authority.checkpoint_count,
        foreground_checkpoint_count: authority.foreground_checkpoint_count,
        background_checkpoint_ns: authority.background_checkpoint_ns,
        background_checkpoint_suffix_batches: authority.background_checkpoint_suffix_batches,
        recovery_tail_batches: db.recovery_stats().tail_batches,
        retention_floor: format!("{:?}", db.retention_floor()),
    })
}
fn file_charge(metadata: &fs::Metadata) -> FileCharge {
    #[cfg(unix)]
    let allocated_bytes = {
        use std::os::unix::fs::MetadataExt;
        Some(metadata.blocks() * 512)
    };
    #[cfg(not(unix))]
    let allocated_bytes = None;
    FileCharge {
        logical_bytes: metadata.len(),
        allocated_bytes,
    }
}
fn disk(data: &Path, phase: &str) -> Result<DiskSnapshot> {
    let mut result = DiskSnapshot {
        phase: phase.into(),
        all_data_logical_bytes: 0,
        all_data_allocated_bytes: Some(0),
        wal_files: BTreeMap::new(),
        authority_manifest_crc32: crc32fast::hash(&fs::read(
            data.join("remote/tcp-database-authority-v1"),
        )?),
    };
    let mut directories = vec![data.to_path_buf()];
    while let Some(directory) = directories.pop() {
        // Active checkpoint generations can disappear while this non-atomic
        // diagnostic snapshot walks them. A vanished path contributes no bytes.
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if metadata.is_dir() {
                directories.push(entry.path());
            } else if metadata.is_file() {
                let charge = file_charge(&metadata);
                result.all_data_logical_bytes += charge.logical_bytes;
                result.all_data_allocated_bytes = result
                    .all_data_allocated_bytes
                    .zip(charge.allocated_bytes)
                    .map(|(a, b)| a + b);
                let path = entry.path();
                if path.starts_with(data.join("cluster"))
                    && entry
                        .file_name()
                        .to_str()
                        .is_some_and(|name| name.starts_with("wal_"))
                {
                    result.wal_files.insert(
                        path.strip_prefix(data)?.to_string_lossy().into_owned(),
                        charge,
                    );
                }
            }
        }
    }
    require(
        result.all_data_logical_bytes <= MAX_DATA_BYTES,
        "owned data exceeded the 256 MiB disk bound",
    )?;
    require(
        result
            .all_data_allocated_bytes
            .is_none_or(|bytes| bytes <= MAX_DATA_BYTES),
        "allocated owned data exceeded the 256 MiB disk bound",
    )?;
    Ok(result)
}
fn payload_file(reference: ValueRecordReference) -> String {
    format!(
        "cluster/node-{}/storage/epoch_{:016x}/wal_{:016x}",
        reference.batch_address.partition,
        reference.batch_address.segment_generation,
        reference.batch_address.offset / WAL_FILE_BYTES
    )
}
fn retry(
    db: &mut RemoteDatabase,
    ks: KeySpace,
    version: BatchVersion,
    report: &mut Report,
) -> Result<()> {
    let before = db.storage_stats()?;
    let cut = db.published_cut()?;
    require(
        db.write_batch(operation(RETRY_BATCH), mutations(ks, RETRY_BATCH, KEYS))? == version,
        "old exact retry returned a different logical version",
    )?;
    report.operations.exact_retries += 1;
    let after = db.storage_stats()?;
    require(
        before.appended_groups == after.appended_groups
            && before.registered_records == after.registered_records,
        "old exact retry appended new authority",
    )?;
    require(
        db.published_cut()? == cut,
        "old exact retry advanced the published cut",
    )?;
    let changed = vec![put(ks, RETRY_BATCH % KEYS, HISTORY_BATCHES * 8, 0)];
    require(
        db.write_batch(operation(RETRY_BATCH), changed).is_err(),
        "same retry identity accepted different mutations",
    )?;
    report.operations.changed_identity_rejections += 1;
    db.validate_cached_read()?;
    Ok(())
}
fn native_pruning_deletes(
    db: &Arc<Db>,
    ks: KeySpace,
    keys: &[Vec<u8>],
    report: &mut Report,
) -> Result<()> {
    for keys in keys.chunks(16) {
        let mutations: Vec<_> = keys
            .iter()
            .map(|key| Mutation::Delete {
                keyspace: ks,
                key: Bytes::copy_from_slice(key),
            })
            .collect();
        // set_native_durable(true) makes adapter_apply use the real barrier.
        adapter_apply(db, &mutations)?;
        report.operations.native_pruning_delete_batches += 1;
        report.operations.native_pruning_deletes += keys.len() as u64;
    }
    Ok(())
}

fn exercise(launch: &Launch, report: &mut Report) -> Result<()> {
    let stop = Arc::new(AtomicBool::new(true));
    let shape = shape(stop.clone());
    let ks = shape
        .iter_ks()
        .next()
        .ok_or_else(|| error("missing keyspace"))?
        .id();
    let native_path = launch.data.join("native");
    let remote_path = launch.data.join("remote");
    fs::create_dir(&native_path)?;
    fs::create_dir(&remote_path)?;
    let native = Db::open_native(&native_path, shape.clone(), native_config(), metrics())
        .map_err(adapter_error)?;
    native.set_native_durable(true);
    let mut remote = RemoteDatabase::open(&remote_path, &shape, launch.remote.clone(), metrics())?;
    let mut expected = State::new();
    let initial = vec![put(ks, KEEP_KEY, 0, 1), put(ks, BEFORE_STOP_KEY, 0, 0)];
    remote.write_batch(operation(HISTORY_BATCHES + 100), initial.clone())?;
    adapter_apply(&native, &initial)?;
    count_and_apply(&mut expected, report, &initial);
    let mut retry_version = None;
    for batch in 0..HISTORY_BATCHES {
        let changes = mutations(ks, batch, KEYS);
        let version = remote.write_batch(operation(batch), changes.clone())?;
        adapter_apply(&native, &changes)?;
        count_and_apply(&mut expected, report, &changes);
        if batch == RETRY_BATCH {
            retry_version = Some(version);
            let reference = remote
                .index_reference(ks, &key(batch % KEYS), remote.published_cut()?)?
                .ok_or_else(|| error("original retry payload reference is missing"))?;
            report.original_retry_payload_file = Some(payload_file(reference));
        }
        if batch % 512 == 511 {
            report.metadata.push(metadata(&remote, "history_writes")?);
            report.disk.push(disk(&launch.data, "history_writes")?);
            save_report(&launch.output, report)?;
        }
    }
    let version = retry_version.ok_or_else(|| error("retry operation was not written"))?;
    report.original_retry_version = Some(version);
    verify_raw(&mut remote, &native, ks, &expected, "full_history")?;
    let before_flush = metadata(&remote, "automatic_checkpointing")?;
    require(
        before_flush.checkpoint_count > before_flush.foreground_checkpoint_count,
        "history did not complete a non-foreground automatic checkpoint",
    )?;
    require(
        before_flush.background_checkpoint_suffix_batches > 0,
        "no writes were admitted while a background checkpoint was active",
    )?;
    require(
        before_flush.logical_index_history_bytes > INDEX_BUDGET,
        "history did not exceed the former lifetime metadata bound",
    )?;
    require(
        before_flush.retained_index_versions > RESIDENT_BATCHES * 4,
        "history did not exceed resident batch lifetime bound",
    )?;
    report.metadata.push(before_flush);
    retry(&mut remote, ks, version, report)?;
    report
        .verified
        .insert("pageable_history_exact_retry".into(), true);
    report
        .verified
        .insert("background_checkpoint_suffix_admission".into(), true);
    remote.flush_index()?;
    report.operations.explicit_checkpoints += 1;
    let old_cut = remote.published_cut()?;
    let reference = remote
        .index_reference(ks, &key((HISTORY_BATCHES - 1) % KEYS), old_cut)?
        .ok_or_else(|| error("pinned payload reference is missing"))?;
    let pinned_file = payload_file(reference);
    report.pinned_payload_file = Some(pinned_file.clone());
    remote.shutdown()?;
    drop(remote);
    let baseline_disk = disk(&launch.data, "baseline_root_committed")?;
    require(
        baseline_disk.wal_files.contains_key(&pinned_file),
        "captured payload WAL file does not exist",
    )?;
    report.disk.push(baseline_disk);
    report
        .verified
        .insert("full_history_native_state".into(), true);
    save_report(&launch.output, report)?;

    // Reopen into the real Sui adapter, not a model of its pruning decisions.
    let db = Db::open_remote(&remote_path, shape.clone(), launch.remote.clone())
        .map_err(adapter_error)?;
    report.operations.coordinator_reopens += 1;
    let held_checkpoint = db.checkpoint();
    // This owner remains alive after the old iterators have been consumed.
    let file_pin = db.checkpoint();
    let mut held_iterator = db.iterator(ks);
    let mut held_native_iterator = native.checkpoint().iterator(ks);
    let baseline_state = expected.clone();
    let first = held_iterator.next().transpose().map_err(adapter_error)?;
    let native_first = held_native_iterator
        .next()
        .transpose()
        .map_err(adapter_error)?;
    require(
        first == native_first && first.is_some(),
        "held iterator initial native differential mismatch",
    )?;
    let stop_batch = vec![put(ks, STOP_KEY, 0, 2), put(ks, SAME_BATCH_KEY, 0, 0)];
    adapter_apply(&db, &stop_batch)?;
    adapter_apply(&native, &stop_batch)?;
    count_and_apply(&mut expected, report, &stop_batch);
    for batch in 0..SUFFIX_BATCHES {
        let changes = mutations(ks, HISTORY_BATCHES + batch, KEYS);
        adapter_apply(&db, &changes)?;
        adapter_apply(&native, &changes)?;
        count_and_apply(&mut expected, report, &changes);
    }
    let after_stop = vec![put(ks, AFTER_STOP_KEY, 0, 0)];
    adapter_apply(&db, &after_stop)?;
    adapter_apply(&native, &after_stop)?;
    count_and_apply(&mut expected, report, &after_stop);
    verify_adapter(&db, &native, ks, &expected, "suffix_before_pruning")?;
    // Only the native checkpoint iterator has a fixed-cut guarantee; regular
    // native iterators read live state. Consume it before the oracle receives
    // the filter model's synchronous native tombstone operations.
    let mut held_native_state = State::new();
    if let Some((key, value)) = native_first {
        held_native_state.insert(key.to_vec(), value.to_vec());
    }
    held_native_state.extend(collect(held_native_iterator)?);
    compare_state(
        "held_native_cut_before_pruning",
        "native_checkpoint_iterator",
        &held_native_state,
        &baseline_state,
    )?;
    report.disk.push(disk(&launch.data, "before_pruning_root")?);
    require(
        launch.data.join(&pinned_file).exists(),
        "payload disappeared before pruning root commit",
    )?;
    db.start_relocation().map_err(adapter_error)?;
    native_pruning_deletes(&native, ks, &[key(BEFORE_STOP_KEY)], report)?;
    report.operations.pruning_passes += 1;
    expected.remove(&key(BEFORE_STOP_KEY));
    require(
        !db.exists(ks, &key(BEFORE_STOP_KEY))
            .map_err(adapter_error)?,
        "eligible Remove row before Stop survived pruning",
    )?;
    // The filter model removes only the pre-Stop row; every current suffix row
    // and the whole Stop batch survives. Native applies that explicit delete.
    verify_adapter(&db, &native, ks, &expected, "stop_aware_pruning")?;
    require(
        db.exists(ks, &key(KEEP_KEY)).map_err(adapter_error)?
            && db.exists(ks, &key(STOP_KEY)).map_err(adapter_error)?
            && db.exists(ks, &key(SAME_BATCH_KEY)).map_err(adapter_error)?
            && db.exists(ks, &key(AFTER_STOP_KEY)).map_err(adapter_error)?,
        "Stop/Keep/whole-batch pruning protection failed",
    )?;
    report
        .verified
        .insert("sui_stop_keep_whole_batch".into(), true);

    // Release the application Stop while old read owners still exist. The
    // compacted root may advance only as far as their captured cut permits.
    stop.store(false, Ordering::Release);
    db.start_relocation().map_err(adapter_error)?;
    let removed: Vec<_> = expected
        .iter()
        .filter(|(_, value)| value[0] != 1)
        .map(|(key, _)| key.clone())
        .collect();
    native_pruning_deletes(&native, ks, &removed, report)?;
    report.operations.pruning_passes += 1;
    expected.retain(|_, value| value[0] == 1);
    verify_adapter(&db, &native, ks, &expected, "stop_released_pins_held")?;
    let mut held_state = State::new();
    if let Some((key, value)) = first {
        held_state.insert(key.to_vec(), value.to_vec());
    }
    held_state.extend(collect(held_iterator)?);
    compare_state(
        "pins_held_after_pruning",
        "remote_iterator",
        &held_state,
        &baseline_state,
    )?;
    compare_state(
        "pins_held_after_pruning",
        "remote_checkpoint",
        &collect(held_checkpoint.iterator(ks))?,
        &baseline_state,
    )?;
    report
        .verified
        .insert("retained_iterator_and_checkpoint_state".into(), true);
    let protected = disk(&launch.data, "pinned_compacted_root")?;
    require(
        protected.wal_files.contains_key(&pinned_file),
        "pinned payload file was reclaimed before read owner release",
    )?;
    report.disk.push(protected);
    report
        .verified
        .insert("pinned_payload_file_preserved".into(), true);
    drop(file_pin);
    let released = disk(&launch.data, "read_owners_released_before_new_root")?;
    require(
        released.wal_files.contains_key(&pinned_file),
        "dropping read owners reclaimed payload without a new durable root",
    )?;
    require(
        report.disk.last().is_some_and(|protected| {
            protected.authority_manifest_crc32 == released.authority_manifest_crc32
        }),
        "authority root changed during read-owner release observation",
    )?;
    report.disk.push(released);
    db.start_relocation().map_err(adapter_error)?;
    report.operations.pruning_passes += 1;
    db.flush().map_err(adapter_error)?;
    report.operations.explicit_checkpoints += 1;
    verify_adapter(&db, &native, ks, &expected, "released_root_committed")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while launch.data.join(&pinned_file).exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    require(
        !launch.data.join(&pinned_file).exists(),
        "obsolete sealed WAL file was not reclaimed after release and root commit",
    )?;
    let retry_file = report
        .original_retry_payload_file
        .as_ref()
        .ok_or_else(|| error("missing original retry payload file"))?;
    require(
        !launch.data.join(retry_file).exists(),
        "old retry payload file was not physically retired",
    )?;
    let retired = disk(&launch.data, "released_root_committed")?;
    let protected = report
        .disk
        .iter()
        .find(|snapshot| snapshot.phase == "pinned_compacted_root")
        .ok_or_else(|| error("missing protected disk evidence"))?;
    require(
        protected.authority_manifest_crc32 != retired.authority_manifest_crc32,
        "retirement did not publish a different durable authority root",
    )?;
    report.retired_allocated_bytes = Some(0);
    for (path, charge) in &protected.wal_files {
        if !retired.wal_files.contains_key(path) {
            report.retired_file_count += 1;
            report.retired_logical_bytes += charge.logical_bytes;
            report.retired_allocated_bytes = report
                .retired_allocated_bytes
                .zip(charge.allocated_bytes)
                .map(|(a, b)| a + b);
        }
    }
    require(
        report.retired_file_count > 0 && report.retired_logical_bytes > 0,
        "no physical obsolete WAL files were reclaimed",
    )?;
    report.disk.push(retired);
    report
        .verified
        .insert("physical_wal_reclaimed_after_release_and_root".into(), true);
    report
        .verified
        .insert("sui_remove_native_state".into(), true);
    db.visit_timing_metrics(|name, value| {
        report.adapter_counters.insert(name.into(), value);
    });
    db.wait_for_background_threads_to_finish()
        .map_err(adapter_error)?;
    save_report(&launch.output, report)?;

    let mut remote = RemoteDatabase::open(&remote_path, &shape, launch.remote.clone(), metrics())?;
    report.operations.coordinator_reopens += 1;
    verify_raw(
        &mut remote,
        &native,
        ks,
        &expected,
        "cold_reopen_after_retirement",
    )?;
    require(
        remote.recovery_stats().tail_batches
            <= tidehunter::distributed::remote_db::CHECKPOINT_MAX_TAIL_BATCHES as u64,
        "recovery exceeded configured hard tail bound",
    )?;
    report
        .metadata
        .push(metadata(&remote, "cold_reopen_after_retirement")?);
    retry(&mut remote, ks, version, report)?;
    verify_raw(
        &mut remote,
        &native,
        ks,
        &expected,
        "retired_payload_exact_retry",
    )?;
    let expired = remote
        .index_reference(ks, &key(0), old_cut)
        .err()
        .ok_or_else(|| error("unprotected old raw cut returned state below persisted floor"))?;
    let expired = expired.to_string();
    require(
        matches!((old_cut, remote.retention_floor()), (IndexReadCut::At(old), IndexReadCut::At(floor)) if old < floor),
        "rejected old cut was not below the installed retention floor",
    )?;
    remote.validate_cached_read()?;
    report.expired_cut_error = Some(expired);
    report
        .verified
        .insert("expired_raw_cut_rejected".into(), true);
    report
        .verified
        .insert("retired_payload_exact_retry_no_append".into(), true);
    report
        .verified
        .insert("cold_reopen_exact_native_state".into(), true);
    remote.flush_index()?;
    report.operations.explicit_checkpoints += 1;
    remote.shutdown()?;
    drop(remote);
    native.flush().map_err(adapter_error)?;
    native
        .wait_for_background_threads_to_finish()
        .map_err(adapter_error)?;
    let native =
        Db::open_native(&native_path, shape, native_config(), metrics()).map_err(adapter_error)?;
    compare_state(
        "native_reopen",
        "native_scan",
        &collect(native.iterator(ks))?,
        &expected,
    )?;
    native
        .wait_for_background_threads_to_finish()
        .map_err(adapter_error)?;
    report
        .verified
        .insert("native_oracle_recovery".into(), true);
    report.disk.push(disk(&launch.data, "final_recovery")?);
    Ok(())
}

struct OwnedWorker(Option<Child>);
impl Drop for OwnedWorker {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn run_worker(launch_file: &Path, output: &Path) -> Result<()> {
    let log = |name| {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output.join(name))
    };
    let child = Command::new(std::env::current_exe()?)
        .arg("--lifecycle-worker")
        .arg(launch_file)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log("worker.stdout.log")?))
        .stderr(Stdio::from(log("worker.stderr.log")?))
        .spawn()?;
    let mut worker = OwnedWorker(Some(child));
    let deadline = Instant::now() + WORKER_TIMEOUT;
    loop {
        let child = worker
            .0
            .as_mut()
            .ok_or_else(|| error("missing owned worker"))?;
        if let Some(status) = child.try_wait()? {
            worker.0.take();
            require(
                status.success(),
                &format!("lifecycle worker failed: {status}; see worker.stderr.log"),
            )?;
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "lifecycle worker exceeded 180 seconds",
            )
            .into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
fn controller(output: &Path) -> Result<()> {
    fs::create_dir(output)?;
    let output = fs::canonicalize(output)?;
    let temporary = tempdir::TempDir::new_in(&output, "owned-data")?;
    let data = temporary.path().to_path_buf();
    let mut report = initial_report();
    save_report(&output, &report)?;
    let shape = shape(Arc::new(AtomicBool::new(true)));
    let mut cluster = None;
    let result = (|| -> Result<()> {
        let owned = tcp_cluster::LoopbackCluster::start_with_transport(
            &data.join("cluster"),
            &output.join("nodes"),
            &shape,
            storage(),
            RESIDENT_BATCHES,
            INDEX_BUDGET,
            None,
            Transport::Tcp,
            2,
            WalMemberReadPolicy::Async,
            0,
            128 * MIB,
            PollingPolicy::Adaptive,
            true,
            tidehunter::distributed::tcp::ResidentReadConfig::default(),
        )?;
        let mut remote = owned.config.clone();
        cluster = Some(owned);
        remote.timeout = Duration::from_secs(5);
        remote.read_cache_bytes = 0;
        remote.read_memory_allowance_bytes = 32 * MIB;
        remote.max_pending_read_bytes = 8 * MIB;
        remote.max_pending_batches = 16;
        remote.max_pending_bytes = MIB;
        let launch = Launch {
            data,
            output: output.clone(),
            remote,
        };
        let launch_file = temporary.path().join("lifecycle-launch.yaml");
        serde_yaml::to_writer(File::create(&launch_file)?, &launch)?;
        run_worker(&launch_file, &output)
    })();
    // Always attempt both cleanup operations, including on worker failure.
    let stopped = match cluster.as_mut() {
        Some(cluster) => cluster.shutdown(),
        None => Ok(()),
    };
    drop(cluster);
    let cleaned = temporary.close();
    report = serde_yaml::from_slice(&fs::read(output.join("lifecycle.yaml"))?)?;
    report.owned_children_stopped = stopped.is_ok();
    report.owned_data_cleaned = cleaned.is_ok();
    let result = result.and(stopped).and(cleaned.map_err(Into::into));
    match &result {
        Ok(()) => report.status = "verified".into(),
        Err(error) => {
            report.status = "failed".into();
            report.failure = Some(match report.failure.take() {
                Some(worker) => format!("{error}; worker: {worker}"),
                None => error.to_string(),
            });
        }
    }
    save_report(&output, &report)?;
    result?;
    println!(
        "lifecycle=verified storage_processes=5 children_stopped=true owned_data_cleaned=true retired_files={} retired_logical_bytes={} evidence={}",
        report.retired_file_count,
        report.retired_logical_bytes,
        output.join("lifecycle.yaml").display()
    );
    Ok(())
}
fn main() -> Result<()> {
    if tcp_cluster::maybe_run_storage_node()? {
        return Ok(());
    }
    let mut args = std::env::args_os().skip(1);
    let first = args
        .next()
        .ok_or_else(|| error("usage: sharded_wal_lifecycle_check NEW_OUTPUT_DIRECTORY"))?;
    if first == "--lifecycle-worker" {
        let file = args
            .next()
            .ok_or_else(|| error("missing lifecycle launch file"))?;
        require(
            args.next().is_none(),
            "unexpected lifecycle worker arguments",
        )?;
        let launch: Launch = serde_yaml::from_slice(&fs::read(file)?)?;
        let mut report = initial_report();
        let result = exercise(&launch, &mut report);
        if let Err(error) = &result {
            report.failure = Some(error.to_string());
            report.status = "failed".into();
        }
        save_report(&launch.output, &report)?;
        return result;
    }
    require(args.next().is_none(), "expected one new output directory")?;
    controller(Path::new(&first))
}
