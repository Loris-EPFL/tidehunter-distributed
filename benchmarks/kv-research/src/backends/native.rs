use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend, native_error};
use serde::Deserialize;
use std::fs;
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
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    path: PathBuf,
    #[serde(default = "default_key_count")]
    key_count: u64,
    #[serde(default)]
    durability: Durability,
}

fn default_key_count() -> u64 {
    1 << 20
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
}

pub struct NativeBackend {
    db: Arc<Db>,
    keyspace: KeySpace,
    durability: Durability,
    path: PathBuf,
    key_count: u64,
    diagnostics: Mutex<NativeWriteDiagnostics>,
}

fn parse_options(value: &serde_json::Value) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
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

// All backend factories share an async interface; native Db opens synchronously.
#[allow(clippy::unused_async)]
pub async fn open(
    options: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<NativeBackend> {
    let options = parse_options(options)?;
    fs::create_dir_all(&options.path)?;
    // Match the established adapter benchmark's bounded 16 x 64-cell geometry.
    // `key_count` is the dataset bound, not cells per mutex.
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let metrics = Metrics::new_in_enabled(registry, true);
    let mut config = Config::small();
    // This selects Tidehunter's existing key/value and WAL behavior. A foreground
    // Value-WAL fsync is an independent opt-in and is not the default cache path.
    config.metrics_enabled = true;
    let db = Db::open(&options.path, shape, Arc::new(config), metrics).map_err(native_error)?;
    let keyspace = db.single_ks();
    Ok(NativeBackend {
        db,
        keyspace,
        durability: options.durability,
        path: options.path,
        key_count: options.key_count,
        diagnostics: Mutex::default(),
    })
}

impl NativeBackend {
    fn foreground_barrier(&self) -> BackendResult<()> {
        if matches!(self.durability, Durability::ValueWalBarrier) {
            self.db
                .persist_value_wal_for_benchmark()
                .map_err(native_error)?;
        }
        Ok(())
    }

    fn profile_barrier(&self, profile: &mut ValueWalPersistenceProfile) -> BackendResult<()> {
        if matches!(self.durability, Durability::ValueWalBarrier) {
            self.db
                .persist_value_wal_profiled_for_benchmark(profile)
                .map_err(native_error)?;
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
            diagnostics.profiled_barriers += 1;
            if barrier_error {
                diagnostics.barrier_errors += 1;
            }
        }
        Ok(())
    }
    fn info(&self) -> BackendInfo {
        BackendInfo {
            name: "native".to_owned(),
            server_version: "Tidehunter 0.1.0".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            acknowledgement: match self.durability {
                Durability::PageCache => "native write acknowledgement; no foreground Value-WAL fsync".to_owned(),
                Durability::ValueWalBarrier => "native write acknowledgement followed by explicit Value-WAL file persistence barrier".to_owned(),
            },
            consistency: "native Tidehunter Db keyspace semantics".to_owned(),
            replication_factor: 1,
            measurement_boundary: match self.durability {
                Durability::PageCache => "foreground native Db operation; kernel page-cache/WAL path; no added durability barrier".to_owned(),
                Durability::ValueWalBarrier => "foreground native Db operation including persistence of all currently registered Value-WAL files and their directory".to_owned(),
            },
            get_many_strategy: "ordered scalar Db::get calls; returned Bytes retained without copying".to_owned(),
            batch_atomicity: BatchAtomicity::CrashRecovery,
            topology: serde_json::json!({"processes": 1, "owners": 1, "process_id": std::process::id(), "path": self.path}),
            configuration: serde_json::json!({
                "transport": "none",
                "transport_security": "in_process",
                "durability": match self.durability { Durability::PageCache => "page_cache", Durability::ValueWalBarrier => "value_wal_barrier" },
                "key_count": self.key_count,
                "key_bytes": 32,
                "keyshape_mutexes": 16,
                "keyshape_cells_per_mutex": 64,
                "keyshape_total_cells": 1024,
                "physical_wal_gc": true,
                "wal_policy": "native Tidehunter relocation may reclaim obsolete Value-WAL segments"
            }),
        }
    }
}

impl KvBackend for NativeBackend {
    fn info(&self) -> BackendInfo {
        self.info()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        self.db.get(self.keyspace, key).map_err(native_error)
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        self.db
            .insert(self.keyspace, key, value)
            .map_err(native_error)?;
        self.foreground_barrier()
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.db.remove(self.keyspace, key).map_err(native_error)?;
        self.foreground_barrier()
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        keys.iter()
            .map(|key| self.db.get(self.keyspace, key).map_err(native_error))
            .collect()
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = self.db.write_batch();
        for (key, value) in entries {
            batch.write(self.keyspace, key.clone(), value.clone());
        }
        batch.commit().map_err(native_error)?;
        self.foreground_barrier()
    }
    fn arm_write_profile(&self, limit: usize) -> BackendResult<()> {
        if limit > 65_536 {
            return Err("native write profile limit exceeds 65536".into());
        }
        let mut diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| "native write diagnostic lock poisoned")?;
        *diagnostics = NativeWriteDiagnostics {
            limit,
            ..NativeWriteDiagnostics::default()
        };
        Ok(())
    }

    async fn finish_write_profile(&self) -> BackendResult<serde_json::Value> {
        let diagnostics = self
            .diagnostics
            .lock()
            .map_err(|_| "native write diagnostic lock poisoned")?;
        Ok(serde_json::json!({
            "supported": true,
            "runtime_observation_mode": "runtime_selected_native_scalar_write_stages",
            "selection_policy": "first N accepted scalar put/delete requests in measurement; native put_batch is excluded",
            "sample_limit": diagnostics.limit,
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
                    "file_calls": diagnostics.persistence.file_calls,
                    "file_successes": diagnostics.persistence.file_successes,
                    "file_errors": diagnostics.persistence.file_calls.saturating_sub(diagnostics.persistence.file_successes),
                    "file_ns": diagnostics.persistence.file_ns,
                    "namespace_calls": diagnostics.persistence.namespace_calls,
                    "namespace_successes": diagnostics.persistence.namespace_successes,
                    "namespace_errors": diagnostics.persistence.namespace_calls.saturating_sub(diagnostics.persistence.namespace_successes),
                    "namespace_ns": diagnostics.persistence.namespace_ns
                })
            },
            "clock_scope": "per-selected scalar operation; wal_append_ns measures WalWriter::write; index_apply_ns measures LargeTable insert/remove; independent Value-WAL persistence clocks are nested inside the profiled foreground barrier and must not be added to the outer stages",
            "overlap_semantics": "native scalar append and index application stages execute sequentially; Value-WAL persistence is a separate post-operation barrier and file/namespace times are components of that barrier",
            "unsupported_stages": ["native_put_batch_append_index", "per-operation_write_percentiles", "physical_device_io"]
        }))
    }

    async fn profiled_put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let mut stages = NativeWriteStages::default();
        let mut persistence = ValueWalPersistenceProfile::default();
        let (result, barrier_attempted) =
            match self
                .db
                .insert_profiled_for_benchmark(self.keyspace, key, value, &mut stages)
            {
                Ok(()) => (self.profile_barrier(&mut persistence), true),
                Err(error) => (Err(native_error(error)), false),
            };
        let barrier_error = barrier_attempted && result.is_err();
        self.add_profile(stages, persistence, barrier_attempted, barrier_error)?;
        result
    }

    async fn profiled_delete(&self, key: Bytes) -> BackendResult<()> {
        let mut stages = NativeWriteStages::default();
        let mut persistence = ValueWalPersistenceProfile::default();
        let (result, barrier_attempted) =
            match self
                .db
                .remove_profiled_for_benchmark(self.keyspace, key, &mut stages)
            {
                Ok(()) => (self.profile_barrier(&mut persistence), true),
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
}
