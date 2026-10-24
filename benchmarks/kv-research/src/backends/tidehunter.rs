// Reuse the engine's existing harness unchanged, including its explicit startup sequences.
#[allow(clippy::too_many_lines)]
// Its TLS setup is inherited and outside timed calls; do not restyle the shared engine helper.
#[allow(clippy::assigning_clones)]
#[path = "../../../../tidehunter/examples/support/tcp_cluster.rs"]
mod tcp_cluster;

use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend, native_error};
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{PollingPolicy, Transport};
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::minibytes::Bytes;

pub fn maybe_run_storage_node() -> BackendResult<bool> {
    tcp_cluster::maybe_run_storage_node().map_err(|error| error.to_string().into())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    path: PathBuf,
    #[serde(default)]
    evidence: Option<PathBuf>,
    #[serde(default = "default_owners")]
    owners: usize,
    #[serde(default = "default_key_count")]
    key_count: u64,
    #[serde(default = "default_retained_batches")]
    max_batches_per_partition: usize,
    #[serde(default)]
    external_remote_config: Option<tidehunter::distributed::remote_db::RemoteDatabaseConfig>,
}

fn default_owners() -> usize {
    5
}
fn default_key_count() -> u64 {
    1 << 20
}
fn default_retained_batches() -> usize {
    16_384
}

pub struct TidehunterBackend {
    db: Option<std::sync::Arc<sui::Db>>,
    keyspace: tidehunter::key_shape::KeySpace,
    cluster: std::sync::Mutex<Option<tcp_cluster::LoopbackCluster>>,
    endpoints: Vec<(u32, String)>,
    owners: usize,
    evidence_path: PathBuf,
    effective_configuration: serde_json::Value,
}

fn parse_options(value: &serde_json::Value) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
    if options.path.as_os_str().is_empty()
        || options
            .evidence
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err("tidehunter_tcp path and evidence path must not be empty".into());
    }
    if !matches!(options.owners, 1 | 2 | 5) {
        return Err("tidehunter_tcp owners must be one of 1, 2, or 5".into());
    }
    if options.key_count == 0 || options.max_batches_per_partition == 0 {
        return Err("key_count and max_batches_per_partition must be positive".into());
    }
    usize::try_from(options.key_count).map_err(|_| "key_count exceeds platform limit")?;
    if let Some(config) = &options.external_remote_config {
        if config.storage.partitions.len() != options.owners
            || config.endpoints.len() != options.owners
        {
            return Err("external remote configuration must contain exactly one partition and endpoint per owner".into());
        }
        let partitions: HashSet<u32> = config.storage.partitions.iter().copied().collect();
        let endpoints: HashSet<u32> = config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.partition)
            .collect();
        let group_limit_with_header = config.storage.group_limits.max_encoded_bytes.checked_add(8);
        if partitions.len() != options.owners
            || endpoints.len() != options.owners
            || endpoints != partitions
            || config
                .endpoints
                .iter()
                .any(|endpoint| endpoint.address.is_empty())
            || config.timeout.is_zero()
            || group_limit_with_header
                .is_none_or(|minimum| config.storage.fragment_bytes < minimum as u64)
            || config.storage.fragment_bytes == 0
            || !config.storage.fragment_bytes.is_multiple_of(8)
            || config.storage.wal_file_bytes == 0
            || !config
                .storage
                .wal_file_bytes
                .is_multiple_of(config.storage.fragment_bytes)
            || config.storage.max_batches_per_partition == 0
            || config.storage.max_batches_per_partition > 1_000_000
            || config.storage.max_epochs == 0
            || config.storage.max_epochs > 1024
        {
            return Err("invalid external remote owner configuration".into());
        }
    }
    Ok(options)
}

pub fn validate_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

// All backend factories share an async interface; the existing owner harness opens synchronously.
#[allow(clippy::unused_async)]
// Keep owned/external startup and the admitted effective transport settings together.
#[allow(clippy::too_many_lines)]
pub async fn open(
    value: &serde_json::Value,
    _registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    let options = parse_options(value)?;
    let evidence_path = options
        .evidence
        .clone()
        .unwrap_or_else(|| options.path.join("evidence"));
    fs::create_dir_all(&options.path)?;
    fs::create_dir_all(&evidence_path)?;
    // Match the native control and established adapter benchmark's bounded
    // 16 x 64-cell geometry; dataset key_count is not cell capacity.
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let (cluster, remote_config) = if let Some(config) = options.external_remote_config {
        (None, config)
    } else {
        fs::create_dir_all(options.path.join("owners"))?;
        let partitions: Vec<u32> = (0..u32::try_from(options.owners)?).collect();
        let storage = LocalInventoryConfig {
            database: [0x4b; 16],
            partitions,
            batch_limits: tidehunter::distributed::codec::BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 320 * 1024,
            },
            group_limits: tidehunter::distributed::group::GroupLimits {
                max_batches: 32,
                max_encoded_bytes: 1024 * 1024 - 8,
            },
            fragment_bytes: 1024 * 1024,
            wal_file_bytes: 1024 * 1024 * 1024,
            max_batches_per_partition: options.max_batches_per_partition,
            max_reference_metadata_bytes: 64 * 1024 * 1024,
            max_epochs: 4,
        };
        let cluster = tcp_cluster::LoopbackCluster::start_with_transport(
            &options.path.join("owners"),
            &evidence_path,
            &shape,
            storage,
            4096,
            128 * 1024 * 1024,
            None,
            Transport::Tcp,
            2,
            tidehunter::distributed::storage::WalMemberReadPolicy::Async,
            8 * 1024 * 1024,
            512 * 1024 * 1024,
            PollingPolicy::Adaptive,
            false,
            tidehunter::distributed::tcp::ResidentReadConfig::default(),
        )
        .map_err(|error| std::io::Error::other(error.to_string()))?;
        let config = cluster.config.clone();
        (Some(cluster), config)
    };
    let effective_max_batches = remote_config.storage.max_batches_per_partition;
    let endpoints: Vec<(u32, String)> = remote_config
        .endpoints
        .iter()
        .map(|endpoint| (endpoint.partition, endpoint.address.clone()))
        .collect();
    let configured_partitions: HashSet<u32> =
        remote_config.storage.partitions.iter().copied().collect();
    let endpoint_partitions: HashSet<u32> =
        endpoints.iter().map(|(partition, _)| *partition).collect();
    if configured_partitions.len() != options.owners
        || endpoints.len() != options.owners
        || endpoint_partitions.len() != endpoints.len()
        || endpoint_partitions != configured_partitions
    {
        return Err(
            "remote configuration must provide exactly one endpoint per owner partition".into(),
        );
    }
    let coordinator_path = options.path.join("coordinator");
    let transport = remote_config.tls.transport;
    let locally_owned = cluster.is_some();
    let effective_configuration = serde_json::json!({
        "transport": transport.label(),
        "transport_security": if transport.is_native() { "trusted_network_no_tls" } else { "mutual_tls" },
        "owner_count": options.owners,
        "replication_factor": 1,
        "physical_wal_gc": false,
        "retained_history_capacity_batches_per_partition": effective_max_batches,
        "batch_atomicity_scope": "crash_recovery_only; no serializable visibility/isolation claim",
        "key_count": options.key_count,
        "key_bytes": 32,
        "keyshape_mutexes": 16,
        "keyshape_cells_per_mutex": 64,
        "keyshape_total_cells": 1024,
        "owner_storage": "native Tidehunter PartitionLog Value-WAL; not independent Db indexes",
        "placement": if locally_owned { "single host and one shared SSD" } else { "external topology; placement not observed" },
        "owner_limits": {
            "batch_max_operations": remote_config.storage.batch_limits.max_operations,
            "batch_max_encoded_bytes": remote_config.storage.batch_limits.max_encoded_bytes,
            "group_max_batches": remote_config.storage.group_limits.max_batches,
            "group_max_encoded_bytes": remote_config.storage.group_limits.max_encoded_bytes,
            "fragment_bytes": remote_config.storage.fragment_bytes,
            "wal_file_bytes": remote_config.storage.wal_file_bytes,
            "max_reference_metadata_bytes": remote_config.storage.max_reference_metadata_bytes,
            "max_epochs": remote_config.storage.max_epochs,
            "read_workers": if locally_owned { Some(2usize) } else { None },
            "read_io_timeout_ms": if locally_owned { Some(60_000usize) } else { None },
            "read_cache_bytes": if locally_owned { Some(8 * 1024 * 1024usize) } else { None },
            "read_memory_allowance_bytes": if locally_owned { Some(512 * 1024 * 1024usize) } else { None },
            "read_max_inflight_requests": if locally_owned { Some(32usize) } else { None },
            "read_max_inflight_bytes": if locally_owned { Some(64 * 1024 * 1024usize) } else { None },
            "read_max_device_reads": if locally_owned { Some(128usize) } else { None },
            "read_resident_first": if locally_owned { Some(false) } else { None },
            "resident_reads_enabled": if locally_owned { Some(false) } else { None }
        },
        "client": {
            "timeout_ms": remote_config.timeout.as_millis(),
            "max_admitted_batches": remote_config.max_admitted_batches,
            "max_index_bytes": remote_config.max_index_bytes,
            "max_pending_batches": remote_config.max_pending_batches,
            "max_pending_bytes": remote_config.max_pending_bytes,
            "pipelined_writes": remote_config.pipelined_writes,
            "read_cache_bytes": remote_config.read_cache_bytes,
            "read_memory_allowance_bytes": remote_config.read_memory_allowance_bytes,
            "preparation_workers": remote_config.preparation_workers,
            "scan_prefetch_pages": remote_config.scan_prefetch_pages,
            "read_lanes": remote_config.read_lanes,
            "max_pending_reads": remote_config.max_pending_reads,
            "max_pending_read_bytes": remote_config.max_pending_read_bytes,
            "write_profile_limit": remote_config.write_profile_limit
        },
        "coordinator_path": coordinator_path
    });
    fs::create_dir_all(&coordinator_path)?;
    let db = sui::Db::open_remote(&coordinator_path, shape, remote_config).map_err(native_error)?;
    let keyspace = db.ks("root");
    Ok(TidehunterBackend {
        db: Some(db),
        keyspace,
        cluster: std::sync::Mutex::new(cluster),
        endpoints,
        owners: options.owners,
        evidence_path,
        effective_configuration,
    })
}

impl TidehunterBackend {
    fn info(&self) -> BackendInfo {
        let locally_owned = self.cluster.lock().is_ok_and(|cluster| cluster.is_some());
        let evidence_file = self.evidence_path.join("processes.yaml");
        let owner_processes = locally_owned
            .then(|| {
                fs::read(&evidence_file)
                    .ok()
                    .and_then(|bytes| serde_yaml::from_slice::<serde_json::Value>(&bytes).ok())
            })
            .flatten();
        let endpoints = self.endpoints.iter()
            .map(|(partition, address)| serde_json::json!({"partition": partition, "address": address}))
            .collect::<Vec<_>>();
        let topology = serde_json::json!({
            "processes": if locally_owned { Some(self.owners + 1) } else { None::<usize> },
            "locally_observed_processes": 1,
            "owners": self.owners,
            "coordinator_process_id": std::process::id(),
            "endpoints": endpoints,
            "owner_processes": owner_processes,
            "owner_process_evidence": if locally_owned { Some(evidence_file) } else { None::<PathBuf> }
        });
        BackendInfo {
            name: "tidehunter_tcp".to_owned(),
            server_version: "Tidehunter 0.1.0".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            acknowledgement: "RF1 complete Value-WAL batch persisted on the configured owner; no replication".to_owned(),
            consistency: "single coordinator NativeIndex with complete-batch owner allocation; RF1; no replication, serializable multi-key transaction, or failover".to_owned(),
            replication_factor: 1,
            measurement_boundary: "coordinator API through TCP owner processing and RF1 Value-WAL persistence barrier; local owners share one SSD".to_owned(),
            get_many_strategy: "distributed ordered grouped reads; one result per caller key".to_owned(),
            batch_atomicity: BatchAtomicity::CrashRecovery,
            topology,
            configuration: self.effective_configuration.clone(),
        }
    }
}

impl KvBackend for TidehunterBackend {
    fn info(&self) -> BackendInfo {
        self.info()
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.get(self.keyspace, key).map_err(native_error)
    }

    async fn put(&self, key: Bytes, value: Bytes) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.insert(self.keyspace, key, value).map_err(native_error)?;
        Ok(())
    }

    async fn delete(&self, key: Bytes) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.remove(self.keyspace, key).map_err(native_error)?;
        Ok(())
    }

    async fn get_many(&self, keys: &[Bytes]) -> BackendResult<Vec<Option<Bytes>>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.to_vec()).collect();
        db.multi_get(self.keyspace, &keys)
            .into_iter()
            .map(|result| result.map_err(native_error))
            .collect()
    }

    async fn put_batch(&self, entries: &[(Bytes, Bytes)]) -> BackendResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        let mut batch = db.write_batch();
        for (key, value) in entries {
            batch.write(self.keyspace, key.clone(), value.clone());
        }
        batch.commit().map_err(native_error)?;
        Ok(())
    }

    async fn shutdown(mut self) -> BackendResult<()> {
        let mut errors = Vec::new();
        if let Some(db) = self.db.take()
            && let Err(error) = db.wait_for_background_threads_to_finish()
        {
            errors.push(format!("coordinator drain failed: {error:?}"));
        }
        let cluster = match self.cluster.get_mut() {
            Ok(cluster) => cluster,
            Err(poisoned) => {
                errors.push("owner cluster lock was poisoned during shutdown".to_owned());
                poisoned.into_inner()
            }
        };
        if let Some(mut cluster) = cluster.take()
            && let Err(error) = cluster.shutdown()
        {
            errors.push(format!("owner drain failed: {error}"));
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(std::io::Error::other(errors.join("; ")).into())
        }
    }
}

impl Drop for TidehunterBackend {
    fn drop(&mut self) {
        let mut errors = Vec::new();
        if let Some(db) = self.db.take()
            && let Err(error) = db.wait_for_background_threads_to_finish()
        {
            errors.push(format!("coordinator drain failed: {error:?}"));
        }
        let cluster = match self.cluster.get_mut() {
            Ok(cluster) => cluster,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(cluster) = cluster.as_mut()
            && let Err(error) = cluster.shutdown()
        {
            errors.push(format!("owner drain failed: {error}"));
        }
        if !errors.is_empty() {
            let _ = fs::write(
                self.evidence_path.join("lifecycle-errors.txt"),
                errors.join("\n"),
            );
        }
    }
}
