use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend, native_error};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::Db;
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

pub struct NativeBackend {
    db: Arc<Db>,
    keyspace: KeySpace,
    durability: Durability,
    path: PathBuf,
    key_count: u64,
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
}
