mod group_barrier;

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
use tidehunter::key_shape::{KeyShape, KeySpace, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;
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
    diagnostics: Mutex<NativeWriteDiagnostics>,
}

fn parse_options(value: &serde_json::Value) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
    if !(1..=16).contains(&options.native_shards) {
        return Err("native_shards must be between 1 and 16".into());
    }
    if options.key_count == 0 {
        return Err("native key_count must be positive".into());
    }
    if options.path.as_os_str().is_empty() {
        return Err("native path must not be empty".into());
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
        let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sampled_native_scalar_write_reports_real_append_and_index_stages() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-write-profile-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let backend = open(
            &serde_json::json!({"path": root.clone(), "durability": "page_cache"}),
            &prometheus::Registry::new(),
        )
        .await
        .unwrap();
        backend.arm_write_profile(1).unwrap();
        const KEY: [u8; 32] = [7; 32];
        backend
            .profiled_put(Bytes::from_static(&KEY), Bytes::from_static(b"value"))
            .await
            .unwrap();
        assert_eq!(backend.get(&KEY).await.unwrap().unwrap().as_ref(), b"value");
        let evidence = backend.finish_write_profile().await.unwrap();
        assert_eq!(evidence["selected_operations"], 1);
        assert!(evidence["stages"]["wal_append_ns"].as_u64().unwrap() > 0);
        assert!(evidence["stages"]["index_apply_ns"].as_u64().unwrap() > 0);
        assert_eq!(
            evidence["value_wal_persistence"]["status"],
            "not_applicable"
        );
        backend.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn native_barrier_profile_reports_inventory_and_layout() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-barrier-inventory-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let backend = open(
            &serde_json::json!({"path": root.clone(), "durability": "value_wal_barrier"}),
            &prometheus::Registry::new(),
        )
        .await
        .unwrap();
        backend.arm_write_profile(1).unwrap();
        const KEY: [u8; 32] = [8; 32];
        backend
            .profiled_put(Bytes::from_static(&KEY), Bytes::from_static(b"value"))
            .await
            .unwrap();
        let evidence = backend.finish_write_profile().await.unwrap();
        assert_eq!(evidence["value_wal_inventory"]["start"]["available"], true);
        assert_eq!(evidence["value_wal_inventory"]["end"]["available"], true);
        let persistence = &evidence["value_wal_persistence"];
        assert_eq!(persistence["profiled_barriers"], 1);
        assert_eq!(persistence["barrier_errors"], 0);
        assert_eq!(
            persistence["sampled_registered_files_min"],
            persistence["sampled_registered_files_max"]
        );
        assert_eq!(
            persistence["sampled_registered_files_min"],
            persistence["file_calls"]
        );
        backend.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn owner_matched_native_geometry_keeps_barrier_and_reads() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-owner-geometry-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let backend = open(
            &serde_json::json!({
                "path": root.clone(),
                "durability": "value_wal_barrier",
                "native_wal_geometry": "owner_matched"
            }),
            &prometheus::Registry::new(),
        )
        .await
        .unwrap();
        let info = backend.info();
        assert_eq!(info.configuration["native_wal_geometry"], "owner_matched");
        assert_eq!(info.configuration["value_wal_fragment_bytes"], 4_194_304);
        assert_eq!(
            info.configuration["value_wal_file_bytes"],
            1_073_741_824_u64
        );
        assert_eq!(info.configuration["durability"], "value_wal_barrier");
        backend.arm_write_profile(1).unwrap();
        const KEY: [u8; 32] = [9; 32];
        backend
            .profiled_put(Bytes::from_static(&KEY), Bytes::from(vec![3; 65_536]))
            .await
            .unwrap();
        assert_eq!(
            backend.get(&KEY).await.unwrap().unwrap().as_ref(),
            &[3; 65_536]
        );
        let evidence = backend.finish_write_profile().await.unwrap();
        assert_eq!(evidence["value_wal_persistence"]["profiled_barriers"], 1);
        assert_eq!(evidence["value_wal_persistence"]["barrier_errors"], 0);
        backend.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn reopening_native_with_mismatched_geometry_is_rejected() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-geometry-mismatch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let registry = prometheus::Registry::new();
        let options = serde_json::json!({
            "path": root.clone(),
            "durability": "value_wal_barrier",
            "native_wal_geometry": "small"
        });
        let backend = open(&options, &registry).await.unwrap();
        const KEY: [u8; 32] = [10; 32];
        backend
            .put(Bytes::from_static(&KEY), Bytes::from_static(b"preserved"))
            .await
            .unwrap();
        backend.shutdown().await.unwrap();
        for geometry in ["large_files", "owner_matched", "native_default"] {
            let mismatch = serde_json::json!({
                "path": root.clone(), "native_wal_geometry": geometry
            });
            let error = open(&mismatch, &prometheus::Registry::new())
                .await
                .err()
                .unwrap();
            assert!(error.to_string().contains("WAL geometry mismatch"));
        }
        let reopened = open(&options, &prometheus::Registry::new()).await.unwrap();
        assert_eq!(
            reopened.get(&KEY).await.unwrap().unwrap().as_ref(),
            b"preserved"
        );
        reopened.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn large_files_native_geometry_keeps_barrier_and_reads() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-large-files-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let backend = open(
            &serde_json::json!({
                "path": root.clone(),
                "durability": "value_wal_barrier",
                "native_wal_geometry": "large_files"
            }),
            &prometheus::Registry::new(),
        )
        .await
        .unwrap();
        backend.arm_write_profile(1).unwrap();
        const KEY: [u8; 32] = [11; 32];
        backend
            .profiled_put(Bytes::from_static(&KEY), Bytes::from(vec![4; 65_536]))
            .await
            .unwrap();
        assert_eq!(
            backend.get(&KEY).await.unwrap().unwrap().as_ref(),
            &[4; 65_536]
        );
        let evidence = backend.finish_write_profile().await.unwrap();
        assert_eq!(evidence["value_wal_persistence"]["profiled_barriers"], 1);
        assert_eq!(evidence["value_wal_persistence"]["barrier_errors"], 0);
        backend.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn native_benchmark_rejects_nonempty_path_without_geometry_witness() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-unwitnessed-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("wal_0000000000000000"), b"existing").unwrap();
        let error = open(
            &serde_json::json!({"path": root.clone(), "native_wal_geometry": "large_files"}),
            &prometheus::Registry::new(),
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("no WAL geometry witness"));
        assert_eq!(
            std::fs::read(root.join("wal_0000000000000000")).unwrap(),
            b"existing"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn grouped_barrier_covers_concurrent_mutations_and_exports_metrics() {
        let root = std::env::temp_dir().join(format!(
            "kv-research-native-group-barrier-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let options = serde_json::json!({
            "path": root,
            "durability": "value_wal_group_barrier",
            "native_wal_geometry": "small",
        });
        validate_options(&options).unwrap();
        let registry = prometheus::Registry::new();
        let backend = open(&options, &registry).await.unwrap();
        let info = backend.info();
        assert_eq!(info.configuration["durability"], "value_wal_group_barrier");
        assert_eq!(
            info.acknowledgement,
            "native write acknowledgement followed by a shared Value-WAL file persistence barrier that started after this operation returned"
        );
        assert_eq!(
            info.measurement_boundary,
            "foreground native Db operation on a dedicated benchmark worker, including a Value-WAL persistence barrier shared with concurrent callers"
        );
        let start = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for worker in 0..8_u32 {
                let backend = &backend;
                let start = &start;
                scope.spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap();
                    start.wait();
                    runtime.block_on(async {
                        for offset in 0..64 {
                            let mut key = [0; 32];
                            key[..4].copy_from_slice(&(worker * 64 + offset).to_le_bytes());
                            backend
                                .put(Bytes::copy_from_slice(&key), Bytes::from_static(b"value"))
                                .await
                                .unwrap();
                        }
                    });
                });
            }
        });
        for id in 0..512_u32 {
            let mut key = [0; 32];
            key[..4].copy_from_slice(&id.to_le_bytes());
            assert_eq!(backend.get(&key).await.unwrap().unwrap().as_ref(), b"value");
        }
        // Whole-case counters retain the unprofiled writes when profiling is armed.
        backend.arm_write_profile(2).unwrap();
        let key = Bytes::from_static(&[254; 32]);
        backend
            .profiled_put(key.clone(), Bytes::from_static(b"profiled"))
            .await
            .unwrap();
        backend.profiled_delete(key.clone()).await.unwrap();
        assert!(backend.get(&key).await.unwrap().is_none());
        backend
            .put_batch(&[(key.clone(), Bytes::from_static(b"batch"))])
            .await
            .unwrap();
        assert_eq!(backend.get(&key).await.unwrap().unwrap().as_ref(), b"batch");
        backend.delete(key.clone()).await.unwrap();
        assert!(backend.get(&key).await.unwrap().is_none());
        backend.put_batch(&[]).await.unwrap();
        let profile = backend.finish_write_profile().await.unwrap();
        assert_eq!(profile["selected_operations"], 2);
        assert!(profile["stages"]["wal_append_ns"].as_u64().unwrap() > 0);
        assert!(profile["stages"]["index_apply_ns"].as_u64().unwrap() > 0);
        let persistence = &profile["value_wal_persistence"];
        assert_eq!(persistence["status"], "grouped");
        assert_eq!(
            persistence["reason"],
            "concurrent callers share barrier runs; per-operation file and namespace calls are not attributable"
        );
        assert!(persistence.get("file_calls").is_none());
        let snapshot = backend.shards[0].group_barrier.snapshot();
        assert_eq!(snapshot.callers_covered, 516);
        assert!(snapshot.runs > 0 && snapshot.runs <= snapshot.callers_covered);
        assert!(snapshot.max_group_size > 0 && snapshot.max_group_size <= 8);
        assert_eq!(snapshot.errors, 0);
        assert_eq!(
            persistence["group_barrier"],
            serde_json::to_value(&snapshot).unwrap()
        );
        let metrics = registry.gather();
        let metric = |name: &str| {
            metrics
                .iter()
                .find(|family| family.name() == name)
                .unwrap()
                .get_metric()[0]
                .clone()
        };
        assert_eq!(
            metric("kv_research_native_group_barrier_runs_total")
                .get_counter()
                .value(),
            snapshot.runs as f64
        );
        assert_eq!(
            metric("kv_research_native_group_barrier_callers_total")
                .get_counter()
                .value(),
            516.0
        );
        assert_eq!(
            metric("kv_research_native_group_barrier_errors_total")
                .get_counter()
                .value(),
            0.0
        );
        let histogram = metric("kv_research_native_group_barrier_group_size")
            .get_histogram()
            .clone();
        assert_eq!(histogram.sample_count(), snapshot.runs);
        assert_eq!(histogram.sample_sum(), 516.0);
        assert_eq!(
            histogram
                .get_bucket()
                .iter()
                .map(|bucket| bucket.upper_bound())
                .collect::<Vec<_>>(),
            vec![1., 2., 3., 4., 6., 8., 12., 16., 24., 32., 64.]
        );
        backend.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn group_barrier_metric_registration_error_propagates_from_open() {
        let registry = prometheus::Registry::new();
        let _barrier = GroupBarrier::new(&registry).unwrap();
        let error = open(
            &serde_json::json!({
                "path": std::env::temp_dir().join("kv-research-native-unused-path"),
                "durability": "value_wal_group_barrier"
            }),
            &registry,
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("Duplicate"));
    }
}
