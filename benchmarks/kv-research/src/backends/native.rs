mod group_barrier;
pub mod replay;

#[cfg(test)]
mod shard_tests;

use crate::backend::{
    BackendInfo, BackendResult, BatchAtomicity, KvBackend, native_error, unsupported,
};
use group_barrier::GroupBarrier;
use serde::Deserialize;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tidehunter::config::Config;
use tidehunter::db::{Db, NativeWriteStages, ValueWalPersistenceProfile};
use tidehunter::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Durability {
    PageCache,
    ValueWalBarrier,
    ValueWalGroupBarrier,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeWalGeometry {
    Small,
    LargeFiles,
    OwnerMatched,
    #[default]
    NativeDefault,
}

impl NativeWalGeometry {
    fn name(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::LargeFiles => "large_files",
            Self::OwnerMatched => "owner_matched",
            Self::NativeDefault => "native_default",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    path: PathBuf,
    #[serde(default = "default_key_count")]
    key_count: u64,
    #[serde(default)]
    durability: Durability,
    #[serde(default)]
    native_wal_geometry: NativeWalGeometry,
    #[serde(default = "default_native_shards")]
    native_shards: usize,
    /// Per-keyspace value-cache entries; zero disables the cache (the default).
    #[serde(default)]
    value_cache_size: usize,
}

fn default_key_count() -> u64 {
    1 << 20
}

fn default_native_shards() -> usize {
    1
}

impl Default for Durability {
    fn default() -> Self {
        Self::PageCache
    }
}

#[derive(Default)]
struct NativeWriteDiagnostics {
    limit: usize,
    stages: NativeWriteStages,
    persistence: ValueWalPersistenceProfile,
    profiled_barriers: u64,
    barrier_errors: u64,
    start_inventory: Option<serde_json::Value>,
    sampled_registered_files_min: Option<u64>,
    sampled_registered_files_max: Option<u64>,
}

struct NativeShard {
    db: Arc<Db>,
    keyspace: KeySpace,
    path: PathBuf,
    group_barrier: GroupBarrier,
}

/// `open` creates a fixed, nonempty set of at most 16 independent databases.
pub struct NativeBackend {
    shards: Vec<NativeShard>,
    shard_filesystem_device_ids: Vec<u64>,
    config: Arc<Config>,
    durability: Durability,
    native_wal_geometry: NativeWalGeometry,
    path: PathBuf,
    key_count: u64,
    value_cache_size: usize,
    diagnostics: Mutex<NativeWriteDiagnostics>,
}

fn parse_options(value: &serde_json::Value) -> BackendResult<Options> {
    let options = Options::deserialize(value)?;
    if !(1..=16).contains(&options.native_shards) {
        return Err("native_shards must be between 1 and 16".into());
    }
    if options.key_count == 0 {
        return Err("native key_count must be positive".into());
    }
    if options.path.as_os_str().is_empty() {
        return Err("native path must not be empty".into());
    }
    if options.value_cache_size > 1_000_000 {
        return Err("native value_cache_size must not exceed 1000000 entries per keyspace".into());
    }
    usize::try_from(options.key_count).map_err(|_| "native key_count exceeds platform limit")?;
    Ok(options)
}

pub fn validate_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

pub fn normalize_options(value: &mut serde_json::Value) -> BackendResult<()> {
    let parsed = parse_options(value)?;
    let options = value
        .as_object_mut()
        .ok_or("native options must be an object")?;
    options.insert(
        "native_wal_geometry".to_owned(),
        parsed.native_wal_geometry.name().into(),
    );
    options.insert("native_shards".to_owned(), parsed.native_shards.into());
    Ok(())
}

/// Static key routing uses unsigned FNV-1a, not distributed operation-ID routing.
/// Distinct uniform keys have the same statistical load per instance, but this
/// placement does not reproduce distributed whole-batch placement or overwrites.
fn shard_index(key: &[u8], shards: usize) -> usize {
    if shards == 1 {
        return 0;
    }
    let hash = key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
    });
    (hash % shards as u64) as usize
}

fn keyspace_shape(value_cache_size: usize) -> KeyShape {
    KeyShape::new_single_config(
        32,
        16,
        KeyType::uniform(64),
        KeySpaceConfig::default().with_value_cache_size(value_cache_size),
    )
}

fn native_config(geometry: NativeWalGeometry) -> Config {
    let mut config = Config::default();
    match geometry {
        NativeWalGeometry::Small => {
            config.frag_size = 1024 * 1024;
            config.wal_file_size = 4 * 1024 * 1024;
        }
        NativeWalGeometry::LargeFiles => {
            config.frag_size = 1024 * 1024;
            config.wal_file_size = super::tidehunter::RESEARCH_WAL_FILE_BYTES;
        }
        NativeWalGeometry::OwnerMatched => {
            config.frag_size = super::tidehunter::RESEARCH_FRAGMENT_BYTES as u64;
            config.wal_file_size = super::tidehunter::RESEARCH_WAL_FILE_BYTES;
        }
        NativeWalGeometry::NativeDefault => {}
    }
    config.metrics_enabled = true;
    config
}

/// Equal filesystem device numbers do not prove a shared physical SSD.
fn same_filesystem(device_ids: &[u64]) -> bool {
    device_ids
        .first()
        .is_some_and(|first| device_ids.iter().all(|device| device == first))
}

// All backend factories share an async interface; native Db opens synchronously.
#[allow(clippy::unused_async)]
pub async fn open(
    options: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<NativeBackend> {
    let options = parse_options(options)?;
    let config = Arc::new(native_config(options.native_wal_geometry));
    let mut shards = Vec::with_capacity(options.native_shards);
    let mut shard_filesystem_device_ids = Vec::with_capacity(if options.native_shards > 1 {
        options.native_shards
    } else {
        0
    });
    for index in 0..options.native_shards {
        let path = if options.native_shards == 1 {
            options.path.clone()
        } else {
            options.path.join(format!("shard-{index:02}"))
        };
        let group_barrier = if options.native_shards == 1 {
            GroupBarrier::new(registry)?
        } else {
            GroupBarrier::new_for_shard(registry, index)?
        };
        fs::create_dir_all(&path)?;
        if options.native_shards > 1 {
            shard_filesystem_device_ids.push(fs::metadata(&path)?.dev());
        }
        // `key_count` is the dataset bound, not cells per mutex.
        let shape = keyspace_shape(options.value_cache_size);
        let private_registry = prometheus::Registry::new();
        let engine_registry = if options.native_shards == 1 {
            registry
        } else {
            &private_registry
        };
        let metrics = Metrics::new_in_enabled(engine_registry, true);
        let db = Db::open_for_benchmark(&path, shape, Arc::clone(&config), metrics)
            .map_err(native_error)?;
        let keyspace = db.single_ks();
        shards.push(NativeShard {
            db,
            keyspace,
            path,
            group_barrier,
        });
    }
    Ok(NativeBackend {
        shards,
        shard_filesystem_device_ids,
        config,
        durability: options.durability,
        native_wal_geometry: options.native_wal_geometry,
        path: options.path,
        key_count: options.key_count,
        value_cache_size: options.value_cache_size,
        diagnostics: Mutex::default(),
    })
}

impl NativeBackend {
    fn shard(&self, key: &[u8]) -> &NativeShard {
        // `open` guarantees 1..=16 shards; the hash result is below this count.
        &self.shards[shard_index(key, self.shards.len())]
    }

    fn profile_shard(&self) -> BackendResult<&NativeShard> {
        if self.shards.len() != 1 {
            return Err(unsupported(
                "native write profiling requires native_shards=1",
            ));
        }
        Ok(&self.shards[0])
    }

    fn value_wal_inventory(shard: &NativeShard) -> serde_json::Value {
        match shard.db.value_wal_inventory_for_benchmark() {
            Ok((files, bytes)) => serde_json::json!({
                "available": true,
                "registered_files": files,
                "logical_file_length_bytes": bytes
            }),
            Err(error) => serde_json::json!({
                "available": false,
                "error": error.to_string()
            }),
        }
    }

    fn foreground_barrier(&self, shard: &NativeShard) -> BackendResult<()> {
        match self.durability {
            Durability::PageCache => {}
            Durability::ValueWalBarrier => shard
                .db
                .persist_value_wal_for_benchmark()
                .map_err(native_error)?,
            Durability::ValueWalGroupBarrier => shard.group_barrier.wait_covering(|| {
                shard
                    .db
                    .persist_value_wal_for_benchmark()
                    .map_err(native_error)
            })?,
        }
        Ok(())
    }

    fn profile_barrier(
        &self,
        shard: &NativeShard,
        profile: &mut ValueWalPersistenceProfile,
    ) -> BackendResult<()> {
        match self.durability {
            Durability::ValueWalBarrier => shard
                .db
                .persist_value_wal_profiled_for_benchmark(profile)
                .map_err(native_error)?,
            Durability::ValueWalGroupBarrier => self.foreground_barrier(shard)?,
            Durability::PageCache => {}
        }
        Ok(())
    }

    fn add_profile(
        &self,
        stages: NativeWriteStages,
        persistence: ValueWalPersistenceProfile,
        barrier_attempted: bool,
        barrier_error: bool,
    ) -> BackendResult<()> {
        let mut diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| "native write diagnostic lock poisoned")?;
        diagnostics.stages.selected_operations += stages.selected_operations;
        diagnostics.stages.wal_append_ns += stages.wal_append_ns;
        diagnostics.stages.wal_append_errors += stages.wal_append_errors;
        diagnostics.stages.index_apply_ns += stages.index_apply_ns;
        diagnostics.stages.index_apply_errors += stages.index_apply_errors;
        diagnostics.persistence.file_calls += persistence.file_calls;
        diagnostics.persistence.file_successes += persistence.file_successes;
        diagnostics.persistence.file_ns += persistence.file_ns;
        diagnostics.persistence.namespace_calls += persistence.namespace_calls;
        diagnostics.persistence.namespace_successes += persistence.namespace_successes;
        diagnostics.persistence.namespace_ns += persistence.namespace_ns;
        if barrier_attempted {
            if matches!(self.durability, Durability::ValueWalBarrier) {
                let files = persistence.registered_files;
                diagnostics.sampled_registered_files_min = Some(
                    diagnostics
                        .sampled_registered_files_min
                        .map_or(files, |min| min.min(files)),
                );
                diagnostics.sampled_registered_files_max = Some(
                    diagnostics
                        .sampled_registered_files_max
                        .map_or(files, |max| max.max(files)),
                );
            }
            diagnostics.profiled_barriers += 1;
            if barrier_error {
                diagnostics.barrier_errors += 1;
            }
        }
        Ok(())
    }
    fn info(&self) -> BackendInfo {
        let mut info = BackendInfo {
            name: "native".to_owned(),
            server_version: "Tidehunter 0.1.0".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            acknowledgement: match self.durability {
                Durability::PageCache => "native write acknowledgement; no foreground Value-WAL fsync".to_owned(),
                Durability::ValueWalBarrier => "native write acknowledgement followed by explicit Value-WAL file persistence barrier".to_owned(),
                Durability::ValueWalGroupBarrier => "native write acknowledgement followed by a shared Value-WAL file persistence barrier that started after this operation returned".to_owned(),
            },
            consistency: "native Tidehunter Db keyspace semantics".to_owned(),
            replication_factor: 1,
            measurement_boundary: match self.durability {
                Durability::PageCache => "foreground native Db operation on a dedicated benchmark worker; kernel page-cache/WAL path; no added durability barrier".to_owned(),
                Durability::ValueWalBarrier => "foreground native Db operation on a dedicated benchmark worker, including persistence of all currently registered Value-WAL files and their directory".to_owned(),
                Durability::ValueWalGroupBarrier => "foreground native Db operation on a dedicated benchmark worker, including a Value-WAL persistence barrier shared with concurrent callers".to_owned(),
            },
            get_many_strategy: "ordered scalar Db::get calls; returned Bytes retained without copying".to_owned(),
            batch_atomicity: if self.shards.len() == 1 {
                BatchAtomicity::CrashRecovery
            } else {
                BatchAtomicity::Unsupported
            },
            topology: serde_json::json!({
                "processes": 1,
                "owners": self.shards.len(),
                "process_id": std::process::id(),
                "path": self.path,
                "native_shards": self.shards.len(),
                "shard_paths": self.shards.iter().map(|shard| &shard.path).collect::<Vec<_>>(),
                "routing": "fnv1a64(key) mod N",
                "backing_block_device": "not verified by the adapter"
            }),
            configuration: serde_json::json!({
                "transport": "none",
                "transport_security": "in_process",
                "durability": match self.durability { Durability::PageCache => "page_cache", Durability::ValueWalBarrier => "value_wal_barrier", Durability::ValueWalGroupBarrier => "value_wal_group_barrier" },
                "key_count": self.key_count,
                "value_cache_entries_per_keyspace": self.value_cache_size,
                "native_shards": self.shards.len(),
                "key_bytes": 32,
                "keyshape_mutexes": 16,
                "keyshape_cells_per_mutex": 64,
                "keyshape_total_cells": 1024,
                "native_config_preset": "default",
                "native_wal_geometry": self.native_wal_geometry.name(),
                "value_wal_fragment_bytes": self.config.frag_size,
                "value_wal_file_bytes": self.config.wal_file_size,
                "value_wal_max_maps": self.config.max_maps,
                "relocation_batch_max_bytes_effective": self.config.relocation_batch_max_bytes(),
                "max_dirty_keys": self.config.max_dirty_keys,
                "snapshot_written_bytes": self.config.snapshot_written_bytes,
                "snapshot_unload_threshold_bytes": self.config.snapshot_unload_threshold,
                "unload_jitter_pct": self.config.unload_jitter_pct,
                "relocation_max_reclaim_pct": self.config.relocation_max_reclaim_pct,
                "direct_io": self.config.direct_io,
                "physical_wal_gc": true,
                "wal_policy": "native Tidehunter relocation may reclaim obsolete Value-WAL segments"
            }),
        };
        if self.shards.len() > 1 {
            info.topology["shard_filesystem_device_ids"] =
                serde_json::json!(self.shard_filesystem_device_ids);
            info.topology["same_filesystem"] =
                same_filesystem(&self.shard_filesystem_device_ids).into();
            let instances = format!(
                "; {} independent Db instances in one process; callers are routed by key hash",
                self.shards.len()
            );
            info.acknowledgement.push_str(&instances);
            info.measurement_boundary.push_str(&instances);
        }
        info
    }
}

impl KvBackend for NativeBackend {
    fn info(&self) -> BackendInfo {
        self.info()
    }
    fn check_background_health(&self) -> Option<BackendResult<()>> {
        for (index, shard) in self.shards.iter().enumerate() {
            if let Err(error) = shard.db.check_background_health() {
                return Some(Err(format!(
                    "native shard {index} ({}): {error:?}",
                    shard.path.display()
                )
                .into()));
            }
        }
        Some(Ok(()))
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    fn evict_mapped_wal_before_measurement(&self) -> BackendResult<serde_json::Value> {
        if self.shards.len() == 1 {
            let evidence = self.shards[0].db.read_evidence_for_benchmark(true)?;
            return Ok(serde_json::json!({"backend":"native", "process":evidence}));
        }
        let mut evidence = Vec::with_capacity(self.shards.len());
        for (index, shard) in self.shards.iter().enumerate() {
            evidence.push(serde_json::json!({
                "shard": index,
                "path": shard.path,
                "process": shard.db.read_evidence_for_benchmark(true)?,
            }));
        }
        Ok(serde_json::json!({"backend":"native", "shards":evidence}))
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let shard = self.shard(key);
        shard.db.get(shard.keyspace, key).map_err(native_error)
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let shard = self.shard(&key);
        shard
            .db
            .insert(shard.keyspace, key, value)
            .map_err(native_error)?;
        self.foreground_barrier(shard)
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        let shard = self.shard(&key);
        shard.db.remove(shard.keyspace, key).map_err(native_error)?;
        self.foreground_barrier(shard)
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        keys.iter()
            .map(|key| {
                let shard = self.shard(key);
                shard.db.get(shard.keyspace, key).map_err(native_error)
            })
            .collect()
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        if self.shards.len() != 1 {
            return Err(unsupported(
                "native put_batch with native_shards>1: cross-shard atomicity is not provided",
            ));
        }
        if entries.is_empty() {
            return Ok(());
        }
        let shard = &self.shards[0];
        let mut batch = shard.db.write_batch();
        for (key, value) in entries {
            batch.write(shard.keyspace, key.clone(), value.clone());
        }
        batch.commit().map_err(native_error)?;
        self.foreground_barrier(shard)
    }
    fn arm_write_profile(&self, limit: usize) -> BackendResult<()> {
        let shard = self.profile_shard()?;
        if limit > 65_536 {
            return Err("native write profile limit exceeds 65536".into());
        }
        let start_inventory = Self::value_wal_inventory(shard);
        let mut diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| "native write diagnostic lock poisoned")?;
        *diagnostics = NativeWriteDiagnostics {
            start_inventory: Some(start_inventory),
            limit,
            ..NativeWriteDiagnostics::default()
        };
        Ok(())
    }

    async fn finish_write_profile(&self) -> BackendResult<serde_json::Value> {
        let shard = self.profile_shard()?;
        let end_inventory = Self::value_wal_inventory(shard);
        let diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| "native write diagnostic lock poisoned")?;
        Ok(serde_json::json!({
            "supported": true,
            "runtime_observation_mode": "runtime_selected_native_scalar_write_stages",
            "selection_policy": "first N accepted scalar put/delete requests in measurement; native put_batch is excluded",
            "sample_limit": diagnostics.limit,
            "value_wal_inventory": {
                "scope": "start after measurement gate before dispatch; end after measurement and optional completion gate before post-qualification; background maintenance may continue",
                "length_semantics": "logical file lengths include preallocation; not dirty bytes, allocated disk space, bytes synced, or physical I/O",
                "start": diagnostics.start_inventory,
                "end": end_inventory
            },
            "selected_operations": diagnostics.stages.selected_operations,
            "stages": {
                "selected_operations": diagnostics.stages.selected_operations,
                "wal_append_ns": diagnostics.stages.wal_append_ns,
                "wal_append_errors": diagnostics.stages.wal_append_errors,
                "index_apply_ns": diagnostics.stages.index_apply_ns,
                "index_apply_errors": diagnostics.stages.index_apply_errors
            },
            "value_wal_persistence": match self.durability {
                Durability::PageCache => serde_json::json!({
                    "status": "not_applicable",
                    "reason": "native page_cache durability has no foreground Value-WAL persistence barrier"
                }),
                Durability::ValueWalBarrier => serde_json::json!({
                    "status": "sampled",
                    "profiled_barriers": diagnostics.profiled_barriers,
                    "barrier_errors": diagnostics.barrier_errors,
                    "sampled_registered_files_min": diagnostics.sampled_registered_files_min,
                    "sampled_registered_files_max": diagnostics.sampled_registered_files_max,
                    "file_calls": diagnostics.persistence.file_calls,
                    "file_successes": diagnostics.persistence.file_successes,
                    "file_errors": diagnostics.persistence.file_calls.saturating_sub(diagnostics.persistence.file_successes),
                    "file_ns": diagnostics.persistence.file_ns,
                    "namespace_calls": diagnostics.persistence.namespace_calls,
                    "namespace_successes": diagnostics.persistence.namespace_successes,
                    "namespace_errors": diagnostics.persistence.namespace_calls.saturating_sub(diagnostics.persistence.namespace_successes),
                    "namespace_ns": diagnostics.persistence.namespace_ns
                }),
                Durability::ValueWalGroupBarrier => serde_json::json!({
                    "status": "grouped",
                    "reason": "concurrent callers share barrier runs; per-operation file and namespace calls are not attributable",
                    "group_barrier": shard.group_barrier.snapshot()
                })
            },
            "clock_scope": "per-selected scalar operation; wal_append_ns measures WalWriter::write; index_apply_ns measures LargeTable insert/remove; independent Value-WAL persistence clocks are nested inside the profiled foreground barrier and must not be added to the outer stages",
            "overlap_semantics": "native scalar append and index application stages execute sequentially; Value-WAL persistence is a separate post-operation barrier and file/namespace times are components of that barrier",
            "unsupported_stages": ["native_put_batch_append_index", "per-operation_write_percentiles", "physical_device_io"]
        }))
    }

    async fn profiled_put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let shard = self.profile_shard()?;
        let mut stages = NativeWriteStages::default();
        let mut persistence = ValueWalPersistenceProfile::default();
        let (result, barrier_attempted) =
            match shard
                .db
                .insert_profiled_for_benchmark(shard.keyspace, key, value, &mut stages)
            {
                Ok(()) => (self.profile_barrier(shard, &mut persistence), true),
                Err(error) => (Err(native_error(error)), false),
            };
        let barrier_error = barrier_attempted && result.is_err();
        self.add_profile(stages, persistence, barrier_attempted, barrier_error)?;
        result
    }

    async fn profiled_delete(&self, key: Bytes) -> BackendResult<()> {
        let shard = self.profile_shard()?;
        let mut stages = NativeWriteStages::default();
        let mut persistence = ValueWalPersistenceProfile::default();
        let (result, barrier_attempted) =
            match shard
                .db
                .remove_profiled_for_benchmark(shard.keyspace, key, &mut stages)
            {
                Ok(()) => (self.profile_barrier(shard, &mut persistence), true),
                Err(error) => (Err(native_error(error)), false),
            };
        let barrier_error = barrier_attempted && result.is_err();
        self.add_profile(stages, persistence, barrier_attempted, barrier_error)?;
        result
    }
}
