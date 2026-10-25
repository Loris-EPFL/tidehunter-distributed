use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use serde::Deserialize;
use std::path::PathBuf;
use tidehunter::minibytes::Bytes;

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Durability {
    WalAsync,
    WalSync,
}

impl Default for Durability {
    fn default() -> Self {
        Self::WalAsync
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionsConfig {
    path: PathBuf,
    #[serde(default)]
    durability: Durability,
    #[serde(default = "default_blob_threshold")]
    blob_min_bytes: u64,
    #[serde(default)]
    blob_gc: bool,
}

fn default_blob_threshold() -> u64 {
    1024
}

pub struct RocksBackend {
    db: DB,
    name: &'static str,
    durability: Durability,
    blob_min_bytes: u64,
    blob_gc: bool,
    path: PathBuf,
}

fn parse_options(value: &serde_json::Value) -> BackendResult<OptionsConfig> {
    let config: OptionsConfig = serde_json::from_value(value.clone())?;
    if config.path.as_os_str().is_empty() {
        return Err("RocksDB path must not be empty".into());
    }
    Ok(config)
}

pub fn validate_options(value: &serde_json::Value, _blobdb: bool) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

// All backend factories share an async interface; RocksDB opens synchronously.
#[allow(clippy::unused_async)]
pub async fn open(
    options: &serde_json::Value,
    _registry: &prometheus::Registry,
    blobdb: bool,
) -> BackendResult<RocksBackend> {
    let config = parse_options(options)?;
    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    if blobdb {
        db_options.set_enable_blob_files(true);
        db_options.set_min_blob_size(config.blob_min_bytes);
        db_options.set_enable_blob_gc(config.blob_gc);
    }
    let db = DB::open(&db_options, &config.path)?;
    Ok(RocksBackend {
        db,
        name: if blobdb { "blobdb" } else { "rocksdb" },
        durability: config.durability,
        blob_min_bytes: config.blob_min_bytes,
        blob_gc: config.blob_gc,
        path: config.path,
    })
}

impl RocksBackend {
    fn write_options(&self) -> WriteOptions {
        let mut options = WriteOptions::default();
        options.set_sync(matches!(self.durability, Durability::WalSync));
        options
    }

    fn info(&self) -> BackendInfo {
        let engine = if self.name == "blobdb" {
            "RocksDB 8.1.1 integrated BlobDB (librocksdb-sys 0.11.0, rocksdb crate 0.21.0)"
        } else {
            "RocksDB 8.1.1 (librocksdb-sys 0.11.0, rocksdb crate 0.21.0)"
        };
        BackendInfo {
            name: self.name.to_owned(),
            server_version: engine.to_owned(),
            client_version: "rocksdb crate 0.21.0".to_owned(),
            acknowledgement: match self.durability {
                Durability::WalAsync => "RocksDB WAL enabled; sync=false".to_owned(),
                Durability::WalSync => {
                    "RocksDB WAL enabled; sync=true for each mutation and write batch".to_owned()
                }
            },
            consistency: "single-process RocksDB atomic WriteBatch; no distributed replication"
                .to_owned(),
            replication_factor: 1,
            measurement_boundary: "embedded RocksDB API; local database files share the host SSD"
                .to_owned(),
            get_many_strategy: "ordered scalar RocksDB get calls".to_owned(),
            batch_atomicity: BatchAtomicity::CrashRecovery,
            topology: serde_json::json!({"processes": 1, "owners": 1, "process_id": std::process::id(), "path": self.path}),
            configuration: serde_json::json!({
                "transport": "none",
                "transport_security": "in_process",
                "durability": match self.durability { Durability::WalAsync => "wal_async", Durability::WalSync => "wal_sync" },
                "wal_enabled": true,
                "blob_files_enabled": self.name == "blobdb",
                "blob_min_bytes": if self.name == "blobdb" { Some(self.blob_min_bytes) } else { None },
                "blob_gc": if self.name == "blobdb" { Some(self.blob_gc) } else { None },
                "replication_factor": 1
            }),
        }
    }
}

impl KvBackend for RocksBackend {
    fn info(&self) -> BackendInfo {
        self.info()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        self.db
            .get(key)?
            .map(Bytes::from)
            .map_or(Ok(None), |value| Ok(Some(value)))
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        self.db.put_opt(&key, &value, &self.write_options())?;
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        self.db.delete_opt(&key, &self.write_options())?;
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        keys.iter()
            .map(|key| {
                self.db
                    .get(key)
                    .map(|value| value.map(Bytes::from))
                    .map_err(Into::into)
            })
            .collect()
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for (key, value) in entries {
            batch.put(key, value);
        }
        self.db.write_opt(batch, &self.write_options())?;
        Ok(())
    }
}
