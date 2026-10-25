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
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{PollingPolicy, Transport};
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::minibytes::Bytes;

pub fn maybe_run_storage_node() -> BackendResult<bool> {
    tcp_cluster::maybe_run_storage_node().map_err(|error| error.to_string().into())
}

#[derive(Clone, Debug, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ExternalOwnerReadConfig {
    partition: u32,
    read_workers: usize,
    wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
    read_resident_first: bool,
    read_cache_bytes: usize,
    read_memory_allowance_bytes: usize,
    read_max_inflight_requests: usize,
    read_max_inflight_bytes: usize,
    read_max_device_reads: usize,
    resident_reads: tidehunter::distributed::tcp::ResidentReadConfig,
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
    coordinator_read_cache_bytes: Option<usize>,
    #[serde(default)]
    external_remote_config: Option<tidehunter::distributed::remote_db::RemoteDatabaseConfig>,
    #[serde(default)]
    external_owner_read_config: Option<Vec<ExternalOwnerReadConfig>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdapterTransport {
    Tcp,
    Efa,
    EfaDirect,
    EfaNative,
}

impl AdapterTransport {
    fn backend_name(self) -> &'static str {
        match self {
            Self::Tcp => "tidehunter_tcp",
            Self::Efa => "tidehunter_efa",
            Self::EfaDirect => "tidehunter_efa_direct",
            Self::EfaNative => "tidehunter_efa_native",
        }
    }

    fn engine_transport(self) -> Transport {
        match self {
            Self::Tcp => Transport::Tcp,
            Self::Efa => Transport::Efa,
            Self::EfaDirect => Transport::EfaDirect,
            Self::EfaNative => Transport::EfaNative,
        }
    }
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
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalPreparationInput {
    guest_root: PathBuf,
    addresses: Vec<String>,
    transport: Transport,
    max_batches_per_partition: usize,
    max_admitted_batches: usize,
    max_index_bytes: usize,
    coordinator_cache_bytes: usize,
    owner_read_workers: usize,
    owner_wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
    owner_read_cache_bytes: usize,
    owner_read_memory_allowance_bytes: usize,
    owner_read_resident_first: bool,
    resident_reads: tidehunter::distributed::tcp::ResidentReadConfig,
    polling: PollingPolicy,
}

/// Generate external node launch files with the exact engine harness schema.
// Keep emitted owner budgets beside their admission checks to prevent drift.
#[allow(clippy::too_many_lines)]
pub fn prepare_external_case_file(input: &Path, output: &Path) -> BackendResult<()> {
    let input: ExternalPreparationInput = serde_json::from_slice(&fs::read(input)?)?;
    let resident = input.resident_reads;
    let resident_enabled = (resident.source_bytes != 0 && resident.source_regions != 0)
        || resident.destination_slots != 0;
    if !input.guest_root.is_absolute()
        || input.addresses.len() != 5
        || input.max_batches_per_partition == 0
        || input.max_batches_per_partition > 1_000_000
        || input.max_admitted_batches == 0
        || input.max_admitted_batches > 1_000_000
        || input.max_index_bytes == 0
        || input.owner_read_workers == 0
        || input.owner_read_memory_allowance_bytes == 0
        || input
            .owner_read_cache_bytes
            .checked_add(64 * 1024 * 1024)
            .is_none_or(|bytes| bytes > input.owner_read_memory_allowance_bytes)
        || input
            .coordinator_cache_bytes
            .checked_add(64 * 1024 * 1024)
            .is_none_or(|bytes| bytes > 512 * 1024 * 1024)
        || ((input.owner_read_cache_bytes != 0
            || input.owner_read_resident_first
            || resident_enabled)
            && input.owner_wal_member_read
                != tidehunter::distributed::storage::WalMemberReadPolicy::Async)
    {
        return Err("external case requires five nodes, absolute guest_root, supported positive limits, async reads for caches/resident reads, and an owner budget including the 64MiB in-flight allowance".into());
    }
    if (resident.source_bytes == 0) != (resident.source_regions == 0)
        || (resident.destination_bytes == 0) != (resident.destination_slots == 0)
        || (resident.destination_bytes == 0) != (resident.max_handles == 0)
        || resident_enabled != (resident.max_read_bytes != 0)
        || (resident.require_device_read && !resident_enabled)
        || (resident.destination_bytes != 0 && resident.max_read_bytes > resident.destination_bytes)
    {
        return Err("invalid external resident_reads limits".into());
    }
    let addresses = input
        .addresses
        .iter()
        .map(|address| address.parse::<std::net::SocketAddr>())
        .collect::<Result<Vec<_>, _>>()?;
    if addresses.iter().collect::<HashSet<_>>().len() != addresses.len() {
        return Err("external case addresses must be unique".into());
    }
    input
        .transport
        .validate()
        .map_err(|error| format!("external case transport unavailable: {error}"))?;
    let storage = LocalInventoryConfig {
        database: [0x4b; 16],
        partitions: (0..5).collect(),
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
        max_batches_per_partition: input.max_batches_per_partition,
        max_reference_metadata_bytes: 64 * 1024 * 1024,
        max_epochs: 4,
    };
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    tcp_cluster::prepare_external_case(
        output,
        &input.guest_root,
        &addresses,
        &shape,
        storage,
        input.max_admitted_batches,
        input.max_index_bytes,
        input.transport,
        input.owner_read_workers,
        input.owner_wal_member_read,
        input.owner_read_cache_bytes,
        input.owner_read_memory_allowance_bytes,
        input.polling,
        input.owner_read_resident_first,
        input.resident_reads,
    )
    .map_err(|error| std::io::Error::other(error.to_string()))?;

    let remote_path = output.join("remote.yaml");
    let mut remote: tidehunter::distributed::remote_db::RemoteDatabaseConfig =
        serde_yaml::from_slice(&fs::read(&remote_path)?)?;
    remote.read_cache_bytes = input.coordinator_cache_bytes;
    if remote
        .read_cache_bytes
        .checked_add(remote.max_pending_read_bytes)
        .is_none_or(|bytes| bytes > remote.read_memory_allowance_bytes)
    {
        return Err("coordinator read-cache budget exceeds read_memory_allowance_bytes".into());
    }
    fs::write(&remote_path, serde_yaml::to_string(&remote)?)?;
    fs::write(
        output.join("remote.json"),
        serde_json::to_vec_pretty(&remote)?,
    )?;

    let mut owner_configs = Vec::with_capacity(5);
    for partition in 0..5_u32 {
        let launch_path = output.join(format!("node-{partition}")).join("launch.yaml");
        let launch: serde_yaml::Value = serde_yaml::from_slice(&fs::read(launch_path)?)?;
        let node = launch
            .get("node")
            .ok_or("generated launch.yaml is missing node")?;
        let field = |name| {
            node.get(name)
                .cloned()
                .ok_or_else(|| format!("generated NodeConfig is missing {name}"))
        };
        let owner = ExternalOwnerReadConfig {
            partition,
            read_workers: serde_yaml::from_value(field("read_workers")?)?,
            wal_member_read: serde_yaml::from_value(field("wal_member_read")?)?,
            read_resident_first: serde_yaml::from_value(field("read_resident_first")?)?,
            read_cache_bytes: serde_yaml::from_value(field("read_cache_bytes")?)?,
            read_memory_allowance_bytes: serde_yaml::from_value(field(
                "read_memory_allowance_bytes",
            )?)?,
            read_max_inflight_requests: serde_yaml::from_value(field(
                "read_max_inflight_requests",
            )?)?,
            read_max_inflight_bytes: serde_yaml::from_value(field("read_max_inflight_bytes")?)?,
            read_max_device_reads: serde_yaml::from_value(field("read_max_device_reads")?)?,
            resident_reads: serde_yaml::from_value(field("resident_reads")?)?,
        };
        owner_configs.push(owner);
    }
    fs::write(
        output.join("external_owner_read_config.json"),
        serde_json::to_vec_pretty(&owner_configs)?,
    )?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "prepared_directory": output,
            "transport": input.transport.label(),
            "remote_config_yaml": output.join("remote.yaml"),
            "remote_config_json": output.join("remote.json"),
            "owner_read_configuration": output.join("external_owner_read_config.json"),
            "owner_launch_files": (0..5).map(|partition| output.join(format!("node-{partition}/launch.yaml"))).collect::<Vec<_>>()
        }))?
    );
    Ok(())
}

pub struct TidehunterBackend {
    db: Option<std::sync::Arc<sui::Db>>,
    keyspace: tidehunter::key_shape::KeySpace,
    cluster: std::sync::Mutex<Option<tcp_cluster::LoopbackCluster>>,
    endpoints: Vec<(u32, String)>,
    owners: usize,
    backend_name: &'static str,
    evidence_path: PathBuf,
    effective_configuration: serde_json::Value,
    operational_read_io: Option<tidehunter::distributed::remote_db::OperationalReadIoMonitor>,
    read_evidence_written: bool,
}

// External transport, owner identity and payload bounds form one admission boundary.
#[allow(clippy::too_many_lines)]
fn parse_options(value: &serde_json::Value, adapter: AdapterTransport) -> BackendResult<Options> {
    let options: Options = serde_json::from_value(value.clone())?;
    let name = adapter.backend_name();
    if options.path.as_os_str().is_empty()
        || options
            .evidence
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err(format!("{name} path and evidence path must not be empty").into());
    }
    if !matches!(options.owners, 1 | 2 | 5) {
        return Err(format!("{name} owners must be one of 1, 2, or 5").into());
    }
    if options.key_count == 0 || options.max_batches_per_partition == 0 {
        return Err("key_count and max_batches_per_partition must be positive".into());
    }
    usize::try_from(options.key_count).map_err(|_| "key_count exceeds platform limit")?;
    adapter
        .engine_transport()
        .validate()
        .map_err(|error| format!("{name} transport unavailable: {error}"))?;
    if let Some(config) = &options.external_remote_config {
        if config.tls.transport != adapter.engine_transport() {
            return Err(format!(
                "{name} requires external_remote_config.tls.transport={}",
                adapter.engine_transport().label()
            )
            .into());
        }
        let owner_configs = options
            .external_owner_read_config
            .as_ref()
            .ok_or("external_owner_read_config is required with external_remote_config")?;
        if config.storage.partitions.len() != options.owners
            || config.endpoints.len() != options.owners
            || owner_configs.len() != options.owners
        {
            return Err("external remote configuration must contain exactly one partition, endpoint, and owner read configuration per owner".into());
        }
        let partitions: HashSet<u32> = config.storage.partitions.iter().copied().collect();
        let endpoints: HashSet<u32> = config
            .endpoints
            .iter()
            .map(|endpoint| endpoint.partition)
            .collect();
        let owner_partitions: HashSet<u32> =
            owner_configs.iter().map(|owner| owner.partition).collect();
        let coordinator_cache_bytes = options
            .coordinator_read_cache_bytes
            .unwrap_or(config.read_cache_bytes);
        if coordinator_cache_bytes
            .checked_add(config.max_pending_read_bytes)
            .is_none_or(|bytes| bytes > config.read_memory_allowance_bytes)
        {
            return Err(
                "coordinator read cache plus pending reads exceeds read_memory_allowance_bytes"
                    .into(),
            );
        }
        let group_limit_with_header = config.storage.group_limits.max_encoded_bytes.checked_add(8);
        if partitions.len() != options.owners
            || endpoints.len() != options.owners
            || endpoints != partitions
            || owner_partitions.len() != options.owners
            || owner_partitions != partitions
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
        for owner in owner_configs {
            if owner.read_workers == 0
                || owner.read_memory_allowance_bytes == 0
                || owner.read_max_inflight_requests == 0
                || owner.read_max_inflight_bytes == 0
                || owner.read_max_device_reads == 0
                || ((owner.read_cache_bytes != 0 || owner.read_resident_first)
                    && owner.wal_member_read
                        != tidehunter::distributed::storage::WalMemberReadPolicy::Async)
                || owner
                    .read_cache_bytes
                    .checked_add(owner.read_max_inflight_bytes)
                    .is_none_or(|bytes| bytes > owner.read_memory_allowance_bytes)
            {
                return Err(format!(
                    "invalid external owner read configuration for partition {}",
                    owner.partition
                )
                .into());
            }
        }
    } else if options.external_owner_read_config.is_some() {
        return Err("external_owner_read_config requires external_remote_config".into());
    }
    Ok(options)
}

pub fn validate_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value, AdapterTransport::Tcp).map(|_| ())
}

pub fn validate_efa_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value, AdapterTransport::Efa).map(|_| ())
}

pub fn validate_efa_direct_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value, AdapterTransport::EfaDirect).map(|_| ())
}
pub fn validate_efa_native_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value, AdapterTransport::EfaNative).map(|_| ())
}

pub async fn open(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    open_for(value, registry, AdapterTransport::Tcp).await
}

pub async fn open_efa(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    open_for(value, registry, AdapterTransport::Efa).await
}

pub async fn open_efa_direct(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    open_for(value, registry, AdapterTransport::EfaDirect).await
}
pub async fn open_efa_native(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    open_for(value, registry, AdapterTransport::EfaNative).await
}

// Shared async factory interface; the existing owner harness opens synchronously.
#[allow(clippy::unused_async)]
#[allow(clippy::too_many_lines)]
async fn open_for(
    value: &serde_json::Value,
    _registry: &prometheus::Registry,
    adapter: AdapterTransport,
) -> BackendResult<TidehunterBackend> {
    let options = parse_options(value, adapter)?;
    let evidence_path = options
        .evidence
        .clone()
        .unwrap_or_else(|| options.path.join("evidence"));
    fs::create_dir_all(&options.path)?;
    fs::create_dir_all(&evidence_path)?;
    // Match the native control and established adapter benchmark's bounded
    // 16 x 64-cell geometry; dataset key_count is not cell capacity.
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let owner_read_configuration;
    let (cluster, mut remote_config) =
        if let Some(config) = options.external_remote_config {
            owner_read_configuration =
                serde_json::to_value(options.external_owner_read_config.as_ref().ok_or(
                    "external_owner_read_config is required with external_remote_config",
                )?)?;
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
            owner_read_configuration = serde_json::json!((0..options.owners).map(|partition| {
            serde_json::json!({
                "partition": partition,
                "read_workers": 2,
                "wal_member_read": "async",
                "read_resident_first": false,
                "read_cache_bytes": 8 * 1024 * 1024,
                "read_memory_allowance_bytes": 512 * 1024 * 1024,
                "read_max_inflight_requests": 32,
                "read_max_inflight_bytes": 64 * 1024 * 1024,
                "read_max_device_reads": 128,
                "resident_reads": tidehunter::distributed::tcp::ResidentReadConfig::default()
            })
        }).collect::<Vec<_>>());
            let cluster = tcp_cluster::LoopbackCluster::start_with_transport(
                &options.path.join("owners"),
                &evidence_path,
                &shape,
                storage,
                4096,
                128 * 1024 * 1024,
                None,
                adapter.engine_transport(),
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
    if remote_config.tls.transport != adapter.engine_transport() {
        return Err(format!(
            "{} requires transport {}",
            adapter.backend_name(),
            adapter.engine_transport().label()
        )
        .into());
    }
    if let Some(cache_bytes) = options.coordinator_read_cache_bytes {
        remote_config.read_cache_bytes = cache_bytes;
    }
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
    let placement = if locally_owned {
        "local harness; all owners share the host storage"
    } else {
        "external topology; placement and physical-device independence are not observed here"
    };
    let effective_configuration = serde_json::json!({
        "backend": adapter.backend_name(),
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
        "placement": placement,
        "owner_limits": {
            "batch_max_operations": remote_config.storage.batch_limits.max_operations,
            "batch_max_encoded_bytes": remote_config.storage.batch_limits.max_encoded_bytes,
            "group_max_batches": remote_config.storage.group_limits.max_batches,
            "group_max_encoded_bytes": remote_config.storage.group_limits.max_encoded_bytes,
            "fragment_bytes": remote_config.storage.fragment_bytes,
            "wal_file_bytes": remote_config.storage.wal_file_bytes,
            "max_reference_metadata_bytes": remote_config.storage.max_reference_metadata_bytes,
            "max_epochs": remote_config.storage.max_epochs
        },
        "owner_read_configuration": owner_read_configuration,
        "coordinator": {
            "read_cache_bytes": remote_config.read_cache_bytes,
            "read_memory_allowance_bytes": remote_config.read_memory_allowance_bytes
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
    let operational_read_io = db.operational_read_io_monitor();
    Ok(TidehunterBackend {
        db: Some(db),
        keyspace,
        cluster: std::sync::Mutex::new(cluster),
        endpoints,
        owners: options.owners,
        backend_name: adapter.backend_name(),
        evidence_path,
        effective_configuration,
        operational_read_io,
        read_evidence_written: false,
    })
}

impl TidehunterBackend {
    fn write_read_evidence(&mut self) -> std::io::Result<()> {
        if self.read_evidence_written {
            return Ok(());
        }
        let coordinator = self.db.as_ref().and_then(|db| {
            db.remote_counters().map(|(cache, _, read_rpc_count)| {
                serde_json::json!({
                    "read_rpc_count": read_rpc_count,
                    "cache": {
                        "hits": cache.hits,
                        "misses": cache.misses,
                        "inserts": cache.inserts,
                        "evictions": cache.evictions,
                        "rejected_oversize": cache.rejected_oversize,
                        "rejected_mismatch": cache.rejected_mismatch,
                        "bypass_disabled": cache.bypass_disabled,
                        "bypass_admission": cache.bypass_admission,
                        "bypass_eviction_limit": cache.bypass_eviction_limit,
                        "admitted_bytes": cache.admitted_bytes,
                        "served_bytes": cache.served_bytes,
                        "retained_bytes": cache.retained_bytes,
                        "metadata_bytes": cache.metadata_bytes,
                        "entries": cache.entries,
                        "budget_bytes": cache.budget_bytes
                    }
                })
            })
        });
        let mut owner_io = serde_json::Value::Null;
        if let Some(monitor) = &self.operational_read_io {
            // Fast workloads can finish before the one-second passive sampler.
            // Wait only after timing, before drain; never add probes to API calls.
            let cut = Instant::now();
            let deadline = cut + Duration::from_secs(3);
            loop {
                let mut finished = false;
                monitor.visit(|snapshot, stopped| {
                    finished = stopped
                        || (snapshot.last_attempt.is_some_and(|attempt| attempt >= cut)
                            && snapshot.available
                            && snapshot.resident_available);
                });
                if finished || Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            monitor.visit(|snapshot, stopped| {
                owner_io = serde_json::json!({
                    "available": snapshot.available,
                    "resident_available": snapshot.resident_available,
                    "generation": snapshot.generation,
                    "failed_samples": snapshot.failed_samples,
                    "stopped": stopped,
                    "post_workload_sample": snapshot.last_attempt.is_some_and(|attempt| attempt >= cut),
                    "last_sample_age_ms": snapshot.last_attempt.map(|time| time.elapsed().as_millis()),
                    "owner_read_executor_stats": snapshot.owners.iter().map(|(partition, stats)| {
                        serde_json::json!({"partition": partition, "stats": stats})
                    }).collect::<Vec<_>>(),
                    "resident_read_stats": snapshot.residents.iter().map(|(partition, stats)| {
                        serde_json::json!({"partition": partition, "stats": stats})
                    }).collect::<Vec<_>>(),
                    "counter_semantics": {
                        "wal_reads": "logical owner Value-WAL member reads, not device IOPS",
                        "async_read_submissions": "io_uring submissions, not confirmed physical-device reads",
                        "resident_read_successes": "successful NOWAIT prefix reads",
                        "rma_hits": "completed client-side RDMA reads"
                    }
                });
            });
        }
        let evidence = serde_json::json!({
            "schema_version": 1,
            "backend": self.backend_name,
            "counter_scope": "cumulative from adapter open through post-workload qualification; owner IO fields come from the latest passive sample",
            "coordinator": coordinator,
            "owner_io": owner_io,
            "owner_read_configuration": self.effective_configuration["owner_read_configuration"],
            "effective_configuration": self.effective_configuration
        });
        fs::write(
            self.evidence_path.join("read-evidence.json"),
            serde_json::to_vec_pretty(&evidence)
                .map_err(|error| std::io::Error::other(error.to_string()))?,
        )?;
        self.read_evidence_written = true;
        Ok(())
    }
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
            name: self.backend_name.to_owned(),
            server_version: "Tidehunter 0.1.0".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            acknowledgement: "RF1 complete Value-WAL batch persisted on the configured owner; no replication".to_owned(),
            consistency: "single coordinator NativeIndex with complete-batch owner allocation; RF1; no replication, serializable multi-key transaction, or failover".to_owned(),
            replication_factor: 1,
            measurement_boundary: format!(
                "coordinator API through {} owner processing and RF1 Value-WAL persistence barrier",
                self.backend_name.strip_prefix("tidehunter_").unwrap_or("transport")
            ),
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
        if let Err(error) = self.write_read_evidence() {
            errors.push(format!("read evidence export failed: {error}"));
        }
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
        if let Err(error) = self.write_read_evidence() {
            errors.push(format!("read evidence export failed: {error}"));
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_authority_limit_above_engine_bound_before_emitting_launch_files() {
        let root = std::env::temp_dir().join(format!(
            "kv-external-limit-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        let output = root.join("prepared");
        let config = serde_json::json!({
            "guest_root": root.join("guest"),
            "addresses": (0..5).map(|i| format!("127.0.0.1:{}", 44000 + i)).collect::<Vec<_>>(),
            "transport": "tcp",
            "max_batches_per_partition": 16384,
            "max_admitted_batches": 1_048_576,
            "max_index_bytes": 512 * 1024 * 1024,
            "coordinator_cache_bytes": 0,
            "owner_read_workers": 4,
            "owner_wal_member_read": "async",
            "owner_read_cache_bytes": 0,
            "owner_read_memory_allowance_bytes": 256 * 1024 * 1024,
            "owner_read_resident_first": false,
            "resident_reads": {
                "source_bytes": 0, "source_regions": 0,
                "destination_bytes": 0, "destination_slots": 0,
                "max_read_bytes": 0, "max_handles": 0, "require_device_read": false
            },
            "polling": "adaptive"
        });
        fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
        let result = prepare_external_case_file(&input, &output);
        let output_exists = output.exists();
        fs::remove_dir_all(&root).unwrap();
        assert!(result.is_err());
        assert!(!output_exists);
    }

    #[cfg(not(feature = "efa"))]
    #[test]
    fn rejects_efa_kinds_when_forwarded_feature_is_disabled() {
        let options = serde_json::json!({"path": "/tmp/unused"});
        assert!(validate_efa_options(&options).is_err());
        assert!(validate_efa_direct_options(&options).is_err());
        assert!(validate_efa_native_options(&options).is_err());
    }
}
