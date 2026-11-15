// Reuse the engine's existing harness unchanged, including its explicit startup sequences.
#[cfg(all(feature = "nvmeof", target_os = "linux"))]
mod live_qualification;
#[cfg(all(feature = "nvmeof", target_os = "linux"))]
mod live_write_profile;
#[cfg(all(feature = "nvmeof", target_os = "linux"))]
mod nvmeof_campaign;
#[allow(clippy::too_many_lines)]
// Its TLS setup is inherited and outside timed calls; do not restyle the shared engine helper.
#[allow(clippy::assigning_clones)]
#[path = "../../../../tidehunter/examples/support/tcp_cluster.rs"]
mod tcp_cluster;

#[cfg(all(feature = "nvmeof", target_os = "linux"))]
pub fn prepare_nvmeof_dataset(input: &Path, output: &Path) -> BackendResult<()> {
    nvmeof_campaign::prepare(input, output)
}

#[cfg(all(feature = "nvmeof", target_os = "linux"))]
pub fn export_nvmeof_dataset(launch: &Path, prepared: &Path, output: &Path) -> BackendResult<()> {
    nvmeof_campaign::export(launch, prepared, output)
}
#[cfg(all(feature = "nvmeof", target_os = "linux"))]
pub fn qualify_live_nvmeof(input: &Path, output: &Path) -> BackendResult<()> {
    live_qualification::run(input, output)
}
#[cfg(all(feature = "nvmeof", target_os = "linux"))]
pub fn qualify_live_nvmeof_child(input: &Path, ready: &Path) -> BackendResult<()> {
    live_qualification::run_abrupt_child(input, ready)
}

use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend, native_error};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tidehunter::distributed::durability::{DurabilityConfig, DurabilityTier};
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::remote_db::{RemoteDatabaseConfig, ReplayLimits};
use tidehunter::distributed::storage::HistoryPersistence;
use tidehunter::distributed::storage::{NATIVE_FRAGMENT_BYTES, NATIVE_WAL_FILE_BYTES};
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{FabricSecurity, PollingPolicy, Transport};
use tidehunter::key_shape::{KeyShape, KeyType};
use tidehunter::minibytes::Bytes;

// A 32-member batch of 64 KiB values must remain one native atomic batch.
const RESEARCH_BATCH_BYTES: usize = 3 * 1024 * 1024;
pub(super) const RESEARCH_FRAGMENT_BYTES: usize = 4 * 1024 * 1024;
pub(super) const RESEARCH_WAL_FILE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum OwnerWalGeometry {
    #[default]
    NativeDefault,
    Research,
    SuiHarness,
}

impl OwnerWalGeometry {
    fn from_layout(fragment_bytes: u64, wal_file_bytes: u64) -> Option<Self> {
        [Self::NativeDefault, Self::Research, Self::SuiHarness]
            .into_iter()
            .find(|geometry| {
                geometry.fragment_bytes() == fragment_bytes
                    && geometry.wal_file_bytes() == wal_file_bytes
            })
    }

    const fn fragment_bytes(self) -> u64 {
        match self {
            Self::NativeDefault => NATIVE_FRAGMENT_BYTES,
            Self::Research => RESEARCH_FRAGMENT_BYTES as u64,
            Self::SuiHarness => 16 * 1024 * 1024,
        }
    }

    const fn wal_file_bytes(self) -> u64 {
        match self {
            Self::NativeDefault => NATIVE_WAL_FILE_BYTES,
            Self::Research => RESEARCH_WAL_FILE_BYTES,
            Self::SuiHarness => 256 * 1024 * 1024,
        }
    }
}

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
    read_memory_allowance_bytes: usize,
    read_max_inflight_requests: usize,
    read_max_inflight_bytes: usize,
    read_max_device_reads: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Options {
    transport: Transport,
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
    owner_wal_geometry: OwnerWalGeometry,
    #[serde(default)]
    coordinator_read_cache_bytes: Option<usize>,
    #[serde(default)]
    replay_limits: Option<ReplayLimits>,
    #[serde(default)]
    owner_append_occupancy: Option<usize>,
    #[serde(default)]
    external_remote_config: Option<tidehunter::distributed::remote_db::RemoteDatabaseConfig>,
    #[serde(default)]
    external_owner_read_config: Option<Vec<ExternalOwnerReadConfig>>,
    #[serde(default = "default_resident_first")]
    owner_read_resident_first: bool,
    /// Owner acknowledgement tier. Applied inside each owner process and confirmed back by it.
    #[serde(default)]
    durability_tier: DurabilityTier,
    /// External topology only: one owner-published `ready.durability.json` per owner, in
    /// partition order, copied to the coordinator host. Required there; a missing file fails open.
    #[serde(default)]
    external_owner_durability_receipts: Vec<PathBuf>,
    /// Independently admitted sealed-WAL namespaces; control/write transport stays separate.
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    #[serde(default)]
    nvmeof_readers: Vec<tidehunter::distributed::nvmeof::NvmeofReaderConfig>,
    #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
    #[serde(default)]
    nvmeof_readers: Vec<serde_json::Value>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalLiveNvmeof {
    addresses: Vec<std::net::IpAddr>,
    transport: String,
    service: u16,
    port_id: u16,
    commissioning_service: u16,
    commissioning_port_id: u16,
    max_inflight_requests: usize,
    #[serde(default)]
    leased_write_window: Option<usize>,
    #[serde(default)]
    leased_sync: tidehunter::distributed::tcp::LeasedSync,
    max_inflight_bytes: usize,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
    #[serde(default)]
    write_mode: tidehunter::distributed::tcp::LiveWriteMode,
    #[serde(default = "default_resident_first")]
    inline_reads: bool,
    #[serde(default)]
    queue_geometry: tidehunter::distributed::tcp::LiveQueueGeometry,
    #[serde(default)]
    export_mode: tidehunter::distributed::tcp::LiveExportMode,
    #[serde(default = "default_resident_first")]
    lease_ahead: bool,
    #[serde(default)]
    polled_writes: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalPreparationInput {
    guest_root: PathBuf,
    addresses: Vec<String>,
    transport: Transport,
    #[serde(default)]
    owner_transports: Option<Vec<Transport>>,
    #[serde(default)]
    live_nvmeof: Option<ExternalLiveNvmeof>,
    max_batches_per_partition: usize,
    #[serde(default)]
    owner_wal_geometry: OwnerWalGeometry,
    #[serde(default)]
    owner_wal_file_bytes: Option<u64>,
    #[serde(default = "default_external_max_epochs")]
    max_epochs: usize,
    max_admitted_batches: usize,
    max_index_bytes: usize,
    coordinator_cache_bytes: usize,
    #[serde(default = "default_coordinator_read_memory_allowance_bytes")]
    coordinator_read_memory_allowance_bytes: usize,
    #[serde(default)]
    pin_read_lanes: bool,
    owner_read_workers: usize,
    owner_wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
    owner_read_memory_allowance_bytes: usize,
    #[serde(default = "default_resident_first")]
    owner_read_resident_first: bool,
    /// Tier written into every owner launch file; absent means the native tier.
    #[serde(default)]
    durability_tier: DurabilityTier,
    #[serde(default = "HistoryPersistence::legacy")]
    history_persistence: HistoryPersistence,
    polling: PollingPolicy,
}

fn default_resident_first() -> bool {
    true
}

fn default_coordinator_read_memory_allowance_bytes() -> usize {
    512 * 1024 * 1024
}

fn default_external_max_epochs() -> usize {
    4
}

fn backend_name(transport: &Transport) -> &'static str {
    match transport {
        Transport::Tcp => "tidehunter_tcp",
        Transport::TcpTrusted => "tidehunter_tcp_trusted",
        Transport::Ofi { fabric, security } => match (&fabric.backend, security) {
            (tidehunter::distributed::tcp::FabricBackend::Efa, FabricSecurity::MutualTls) => {
                "tidehunter_efa"
            }
            (tidehunter::distributed::tcp::FabricBackend::EfaDirect, FabricSecurity::MutualTls) => {
                "tidehunter_efa_direct"
            }
            (tidehunter::distributed::tcp::FabricBackend::EfaDirect, FabricSecurity::Trusted) => {
                "tidehunter_efa_native"
            }
            (tidehunter::distributed::tcp::FabricBackend::Verbs, FabricSecurity::MutualTls) => {
                "tidehunter_verbs"
            }
            (tidehunter::distributed::tcp::FabricBackend::Verbs, FabricSecurity::Trusted) => {
                "tidehunter_verbs_native"
            }
            (
                tidehunter::distributed::tcp::FabricBackend::SoftwareTcp,
                FabricSecurity::MutualTls,
            ) => "tidehunter_software_tcp",
            (tidehunter::distributed::tcp::FabricBackend::SoftwareTcp, FabricSecurity::Trusted) => {
                "tidehunter_software_tcp_native"
            }
            (tidehunter::distributed::tcp::FabricBackend::Efa, FabricSecurity::Trusted) => {
                "tidehunter_efa_trusted"
            }
        },
    }
}
fn select_owner_transports(
    coordinator: &Transport,
    owner_transports: Option<Vec<Transport>>,
    owner_count: usize,
) -> BackendResult<Vec<Transport>> {
    let transports = match owner_transports {
        Some(transports) if transports.len() == owner_count => transports,
        Some(_) => return Err("owner_transports must contain one transport per owner".into()),
        None => {
            if matches!(
                coordinator,
                Transport::Ofi { fabric, .. } if fabric.source_address.is_some()
            ) {
                return Err(
                    "owner_transports is required when coordinator transport binds a source address"
                        .into(),
                );
            }
            vec![coordinator.clone(); owner_count]
        }
    };
    for (partition, transport) in transports.iter().enumerate() {
        transport
            .validate()
            .map_err(|error| format!("owner transport {partition} unavailable: {error}"))?;
        if !coordinator.same_protocol(transport) {
            return Err(format!(
                "owner transport {partition} must match coordinator provider/security"
            )
            .into());
        }
    }
    Ok(transports)
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
// Keep unvalidated launch files private and remove them on every failed preparation.
struct ExternalPreparationStaging(PathBuf);

impl Drop for ExternalPreparationStaging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Generate external node launch files with the exact engine harness schema.
// Keep emitted owner budgets beside their admission checks to prevent drift.
#[allow(clippy::too_many_lines)]
pub fn prepare_external_case_file(input: &Path, output: &Path) -> BackendResult<()> {
    let input: ExternalPreparationInput = serde_json::from_slice(&fs::read(input)?)?;
    if !input.guest_root.is_absolute()
        || !matches!(input.addresses.len(), 1 | 2 | 5)
        || input.max_batches_per_partition == 0
        || input.max_batches_per_partition > 1_000_000
        || !(1..=1024).contains(&input.max_epochs)
        || input.max_admitted_batches == 0
        || input.max_admitted_batches > 1_000_000
        || input.max_index_bytes == 0
        || input.owner_read_workers == 0
        || input.owner_read_memory_allowance_bytes == 0
        || input.owner_read_memory_allowance_bytes < 64 * 1024 * 1024
        || input
            .coordinator_cache_bytes
            .checked_add(64 * 1024 * 1024)
            .is_none_or(|bytes| bytes > input.coordinator_read_memory_allowance_bytes)
        || (input.owner_read_resident_first
            && input.owner_wal_member_read
                != tidehunter::distributed::storage::WalMemberReadPolicy::Async)
    {
        return Err("external case requires one, two or five nodes, absolute guest_root, supported positive limits, and budgets including the 64MiB in-flight allowance".into());
    }
    if input.owner_wal_file_bytes.is_some_and(|bytes| {
        bytes < 2 * input.owner_wal_geometry.fragment_bytes()
            || bytes > input.owner_wal_geometry.wal_file_bytes()
            || bytes % input.owner_wal_geometry.fragment_bytes() != 0
            || bytes % 4096 != 0
    }) {
        return Err("invalid owner WAL file size override".into());
    }
    if let Some(live) = &input.live_nvmeof {
        if !cfg!(all(feature = "nvmeof", target_os = "linux")) {
            return Err(format!(
                "live NVMe-oF preparation ({:?}, inline_reads={}, lease_ahead={}, polled_writes={}) requires Linux and the nvmeof feature",
                live.write_mode, live.inline_reads, live.lease_ahead, live.polled_writes
            ).into());
        }
        live.queue_geometry.validate()?;
        if live.queue_geometry.poll_queues > 0 && !live.inline_reads {
            return Err("polled live reads require inline reads".into());
        }
        if live.addresses.len() != input.addresses.len()
            || live
                .addresses
                .iter()
                .any(|ip| ip.is_unspecified() || ip.is_multicast())
            || live.addresses.iter().collect::<HashSet<_>>().len() != live.addresses.len()
            || !matches!(live.transport.as_str(), "tcp" | "rdma")
            || live.service == 0
            || live.port_id == 0
            || live.commissioning_service == 0
            || live.commissioning_port_id == 0
            || live.service == live.commissioning_service
            || live.port_id == live.commissioning_port_id
            || live.max_inflight_requests == 0
            || live.max_inflight_bytes == 0
            || live.connect_timeout_ms == 0
            || live.io_timeout_ms == 0
        {
            return Err(
                "invalid live NVMe-oF addresses, target identity, ports or client limits".into(),
            );
        }
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
    let owner_transports =
        select_owner_transports(&input.transport, input.owner_transports, addresses.len())?;
    let storage = LocalInventoryConfig {
        database: [0x4b; 16],
        partitions: (0..u32::try_from(addresses.len())?).collect(),
        batch_limits: tidehunter::distributed::codec::BatchLimits {
            max_operations: 32,
            max_encoded_bytes: RESEARCH_BATCH_BYTES,
        },
        group_limits: tidehunter::distributed::group::GroupLimits {
            max_batches: 32,
            max_encoded_bytes: RESEARCH_FRAGMENT_BYTES - 8,
        },
        fragment_bytes: input.owner_wal_geometry.fragment_bytes(),
        wal_file_bytes: input
            .owner_wal_file_bytes
            .unwrap_or_else(|| input.owner_wal_geometry.wal_file_bytes()),
        max_batches_per_partition: input.max_batches_per_partition,
        max_reference_metadata_bytes: 64 * 1024 * 1024,
        max_epochs: input.max_epochs,
    };
    match fs::symlink_metadata(output) {
        Ok(_) => return Err("external preparation output must not exist".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staging = parent.join(format!(
        ".kv-external-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    fs::create_dir(&staging)?;
    let staging = ExternalPreparationStaging(staging);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&staging.0, fs::Permissions::from_mode(0o700))?;
    }
    let prepared = staging.0.join("case");
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    tcp_cluster::prepare_external_case_with_durability(
        &prepared,
        &input.guest_root,
        &addresses,
        &shape,
        storage,
        input.max_admitted_batches,
        input.max_index_bytes,
        input.transport.clone(),
        &owner_transports,
        input.owner_read_workers,
        input.owner_wal_member_read,
        input.owner_read_memory_allowance_bytes,
        input.polling,
        input.owner_read_resident_first,
        DurabilityConfig::with_tier(input.durability_tier),
    )
    .map_err(|error| std::io::Error::other(error.to_string()))?;
    let live_identity = input.live_nvmeof.as_ref().map(|_| {
        let mut id: [u8; 16] = rand::random();
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        let nqn = format!(
            "nqn.2014-08.org.nvmexpress:uuid:{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
            id[8], id[9], id[10], id[11], id[12], id[13], id[14], id[15]
        );
        (id, nqn)
    });

    let remote_path = prepared.join("remote.yaml");
    let mut remote: tidehunter::distributed::remote_db::RemoteDatabaseConfig =
        serde_yaml::from_slice(&fs::read(&remote_path)?)?;
    remote.history_persistence = input.history_persistence;
    remote.preparation_workers = 8;
    remote.pin_read_lanes = input.pin_read_lanes;
    remote.read_cache_bytes = input.coordinator_cache_bytes;
    remote.read_memory_allowance_bytes = input.coordinator_read_memory_allowance_bytes;
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    if let (Some(live), Some((host_id, host_nqn))) = (&input.live_nvmeof, &live_identity) {
        remote.live_nvme = Some(tidehunter::distributed::tcp::LiveClientConfig {
            lease_ahead: live.lease_ahead,
            polled_writes: live.polled_writes,
            export_mode: live.export_mode,
            host_nqn: host_nqn.clone(),
            host_id: *host_id,
            owner_identities: Default::default(),
            max_inflight_requests: live.max_inflight_requests,
            leased_write_window: live.leased_write_window,
            leased_sync: live.leased_sync,
            max_inflight_bytes: live.max_inflight_bytes,
            connect_timeout_ms: live.connect_timeout_ms,
            io_timeout_ms: live.io_timeout_ms,
            write_mode: live.write_mode,
            leased_durability: (live.write_mode
                == tidehunter::distributed::tcp::LiveWriteMode::Leased)
                .then_some(input.durability_tier),
            inline_reads: live.inline_reads,
            queue_geometry: live.queue_geometry,
        });
    }
    if remote
        .read_cache_bytes
        .checked_add(remote.max_pending_read_bytes)
        .is_none_or(|bytes| bytes > remote.read_memory_allowance_bytes)
    {
        return Err("coordinator read-cache budget exceeds read_memory_allowance_bytes".into());
    }
    remote
        .validate()
        .map_err(|error| format!("invalid prepared remote configuration: {error}"))?;
    fs::write(&remote_path, serde_yaml::to_string(&remote)?)?;
    fs::write(
        prepared.join("remote.json"),
        serde_json::to_vec_pretty(&remote)?,
    )?;

    let mut owner_configs = Vec::with_capacity(addresses.len());
    for partition in 0..u32::try_from(addresses.len())? {
        let launch_path = prepared
            .join(format!("node-{partition}"))
            .join("launch.yaml");
        let mut launch: serde_yaml::Value = serde_yaml::from_slice(&fs::read(&launch_path)?)?;
        launch["history_persistence"] = serde_yaml::to_value(input.history_persistence)?;
        fs::write(&launch_path, serde_yaml::to_string(&launch)?)?;
        if let (Some(live), Some((_, host_nqn))) = (&input.live_nvmeof, &live_identity) {
            let config = serde_json::json!({
                "address": live.addresses[partition as usize].to_string(),
                "service": live.service,
                "transport": live.transport,
                "host_nqn": host_nqn,
                "port_id": live.port_id,
                "state_dir": input.guest_root.join(format!("node-{partition}")).join("live-nvmeof"),
                "commissioning_port_id": live.commissioning_port_id,
                "commissioning_service": live.commissioning_service,
                "export_mode": live.export_mode
            });
            launch["node"]["live_target"] = serde_yaml::to_value(config)?;
            fs::write(&launch_path, serde_yaml::to_string(&launch)?)?;
        }
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
            read_memory_allowance_bytes: serde_yaml::from_value(field(
                "read_memory_allowance_bytes",
            )?)?,
            read_max_inflight_requests: serde_yaml::from_value(field(
                "read_max_inflight_requests",
            )?)?,
            read_max_inflight_bytes: serde_yaml::from_value(field("read_max_inflight_bytes")?)?,
            read_max_device_reads: serde_yaml::from_value(field("read_max_device_reads")?)?,
        };
        owner_configs.push(owner);
    }
    fs::write(
        prepared.join("external_owner_read_config.json"),
        serde_json::to_vec_pretty(&owner_configs)?,
    )?;
    fs::rename(&prepared, output)?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "prepared_directory": output,
            "transport": input.transport.label(),
            "remote_config_yaml": output.join("remote.yaml"),
            "remote_config_json": output.join("remote.json"),
            "coordinator_read_memory_allowance_bytes": remote.read_memory_allowance_bytes,
            "owner_read_configuration": output.join("external_owner_read_config.json"),
            "owner_launch_files": (0..addresses.len()).map(|partition| output.join(format!("node-{partition}/launch.yaml"))).collect::<Vec<_>>()
        }))?
    );
    Ok(())
}
// Checkpoint phase sums are charged on completion, including work begun before
// the measurement boundary. They are not per-request latency or network clocks.
const COORDINATOR_INTERVAL_COUNTERS: [&str; 29] = [
    "write_storage_wait_ns",
    "write_publication_ns",
    "write_dispatch_ns",
    "write_preparation_admission_ns",
    "write_checkpoint_ns",
    "write_checkpoint_count",
    "write_foreground_checkpoint_ns",
    "write_foreground_checkpoint_count",
    "write_concurrent_checkpoint_deferrals",
    "write_concurrent_checkpoint_wait_ns",
    "write_background_checkpoint_ns",
    "write_background_checkpoint_suffix_batches",
    "write_durable_receipt_publication_blocked_ns",
    "write_durable_receipt_publication_blocked_batches",
    "write_append_requests",
    "write_direct_appends",
    "write_durable_batches",
    "write_concurrent_admitted",
    "write_concurrent_completed",
    "write_out_of_prefix_published_batches",
    "write_dependency_wait_batches",
    "write_checkpoint_diagnostic_root_adoption_ns",
    "write_checkpoint_diagnostic_root_adoption_count",
    "write_checkpoint_diagnostic_owner_adoption_ns",
    "write_checkpoint_diagnostic_owner_adoption_count",
    "write_checkpoint_diagnostic_obsolete_cleanup_ns",
    "write_checkpoint_diagnostic_obsolete_cleanup_count",
    "write_checkpoint_diagnostic_index_prepare_ns",
    "write_checkpoint_diagnostic_index_prepare_count",
];

fn coordinator_counter_snapshot(db: &sui::Db) -> BTreeMap<&'static str, u64> {
    let mut counters = BTreeMap::new();
    db.visit_timing_metrics(|name, value| {
        if COORDINATOR_INTERVAL_COUNTERS.contains(&name) {
            counters.insert(name, value);
        }
    });
    counters
}
fn coordinator_interval_delta(
    baseline: &BTreeMap<&'static str, u64>,
    end: &BTreeMap<&'static str, u64>,
) -> Result<BTreeMap<&'static str, u64>, String> {
    let mut deltas = BTreeMap::new();
    for key in COORDINATOR_INTERVAL_COUNTERS {
        let start = baseline
            .get(key)
            .ok_or_else(|| format!("coordinator interval baseline missing {key}"))?;
        let finish = end
            .get(key)
            .ok_or_else(|| format!("coordinator interval end snapshot missing {key}"))?;
        let delta = finish
            .checked_sub(*start)
            .ok_or_else(|| format!("coordinator interval counter decreased: {key}"))?;
        deltas.insert(key, delta);
    }
    Ok(deltas)
}

pub struct TidehunterBackend {
    db: Option<std::sync::Arc<sui::Db>>,
    keyspace: tidehunter::key_shape::KeySpace,
    cluster: std::sync::Mutex<Option<tcp_cluster::LoopbackCluster>>,
    endpoints: Vec<(u32, String)>,
    owners: usize,
    backend_name: &'static str,
    durability_tier: DurabilityTier,
    evidence_path: PathBuf,
    effective_configuration: serde_json::Value,
    operational_read_io: Option<tidehunter::distributed::remote_db::OperationalReadIoMonitor>,
    read_evidence_written: bool,
    coordinator_interval_baseline: std::sync::Mutex<Option<BTreeMap<&'static str, u64>>>,
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    live_write_profile: std::sync::Mutex<Option<live_write_profile::Window>>,
}

// External transport, owner identity and payload bounds form one admission boundary.
#[allow(clippy::too_many_lines)]
fn parse_options(value: &serde_json::Value) -> BackendResult<Options> {
    let options = Options::deserialize(value)?;
    let name = backend_name(&options.transport);
    if let Some(limits) = options.replay_limits {
        limits
            .validate()
            .map_err(|error| format!("{name} replay limits: {error}"))?;
    }
    if options.owner_append_occupancy.is_some_and(|limit| {
        !(1..=RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY).contains(&limit)
    }) {
        return Err(
            format!("{name} owner append occupancy exceeds the reserved-control window").into(),
        );
    }
    if options
        .external_remote_config
        .as_ref()
        .is_some_and(|config| config.live_nvme.is_some())
        && !cfg!(all(feature = "nvmeof", target_os = "linux"))
    {
        return Err("live NVMe-oF requires the Linux nvmeof benchmark feature".into());
    }
    if !options.nvmeof_readers.is_empty() {
        if !cfg!(all(feature = "nvmeof", target_os = "linux")) {
            return Err("NVMe-oF requires the Linux nvmeof benchmark feature".into());
        }
        if options.external_remote_config.is_none() {
            return Err("NVMe-oF readers require explicit external owner configuration and prepared sealed data".into());
        }
    }
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    if let Some(first) = options.nvmeof_readers.first()
        && options
            .nvmeof_readers
            .iter()
            .any(|reader| reader.binding.transport != first.binding.transport)
    {
        return Err("one benchmark case cannot pool different NVMe-oF transports".into());
    }
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    if options
        .external_remote_config
        .as_ref()
        .is_some_and(|config| config.live_nvme.is_some())
        && !options.nvmeof_readers.is_empty()
    {
        return Err("live NVMe-oF and sealed-WAL readers are separate benchmark modes".into());
    }
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
    options
        .transport
        .validate()
        .map_err(|error| format!("{name} transport unavailable: {error}"))?;
    if let Some(config) = &options.external_remote_config {
        if !options.transport.same_protocol(&config.tls.transport) {
            return Err(format!(
                "{name} requires external_remote_config.tls.transport={}",
                options.transport.label()
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
                || owner.read_resident_first != options.owner_read_resident_first
                || (owner.read_resident_first
                    && owner.wal_member_read
                        != tidehunter::distributed::storage::WalMemberReadPolicy::Async)
                || owner.read_max_inflight_bytes > owner.read_memory_allowance_bytes
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
    parse_options(value).map(|_| ())
}

#[cfg(all(feature = "nvmeof", target_os = "linux"))]
fn compact_nvmeof_reader(
    reader: &tidehunter::distributed::nvmeof::NvmeofReaderConfig,
) -> BackendResult<serde_json::Value> {
    use blake2::{Blake2b512, Digest};
    struct DigestWriter(Blake2b512);
    impl std::io::Write for DigestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let manifest = &reader.manifest;
    let mut digest = DigestWriter(Blake2b512::new());
    // Hash the compact serde serialization without allocating a second catalog.
    serde_json::to_writer(&mut digest, manifest)?;
    let hash: String = digest
        .0
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(serde_json::json!({
        "binding": reader.binding,
        "manifest": {
            "version": manifest.version,
            "database_uuid": manifest.database_uuid,
            "epoch": manifest.epoch,
            "partition": manifest.partition,
            "generation": manifest.generation,
            "export_uuid": manifest.export_uuid,
            "nqn": manifest.nqn,
            "nsid": manifest.nsid,
            "wal_file_bytes": manifest.wal_file_bytes,
            "source_file_index": manifest.source_file_index,
            "image_len": manifest.image_len,
            "block_size": manifest.block_size,
            "source_device": manifest.source_device,
            "source_inode": manifest.source_inode,
            "durable_frontier": manifest.durable_frontier,
            "catalog_count": manifest.catalog.len(),
            "manifest_hash_blake2b512": hash,
            "hash_encoding": "compact serde_json serialization of ExportManifest"
        },
        "max_inflight_requests": reader.max_inflight_requests,
        "max_inflight_members": reader.max_inflight_members,
        "max_inflight_bytes": reader.max_inflight_bytes,
        "max_catalog_bytes": reader.max_catalog_bytes
    }))
}

pub async fn open(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    open_for(value, registry).await
}

/// Every partition must be confirmed by exactly one receipt that names it, its database and its
/// endpoint, and that carries the requested tier. A receipt copied from another owner, another
/// database or an unrelated address cannot confirm a partition.
fn confirm_owner_receipts(
    receipts: &[tcp_cluster::OwnerReceipt],
    partitions: &[u32],
    database: [u8; 16],
    endpoints: &[(u32, String)],
    tier: DurabilityTier,
) -> Result<(), String> {
    if receipts.len() != partitions.len() {
        return Err(format!(
            "{} owner receipts for {} partitions",
            receipts.len(),
            partitions.len()
        ));
    }
    for partition in partitions {
        let mut named = receipts
            .iter()
            .filter(|receipt| receipt.partition == *partition);
        let (Some(receipt), None) = (named.next(), named.next()) else {
            return Err(format!(
                "partition {partition} needs exactly one owner receipt"
            ));
        };
        let address = endpoints
            .iter()
            .find(|(candidate, _)| candidate == partition)
            .map(|(_, address)| address);
        if receipt.database != database
            || address != Some(&receipt.address)
            || receipt.launch_nonce.is_empty()
            || receipt.durability.tier != tier
        {
            return Err(format!(
                "owner receipt for partition {partition} does not match its database, endpoint or tier {tier}"
            ));
        }
    }
    Ok(())
}

// Shared async factory interface; the existing owner harness opens synchronously.
#[allow(clippy::unused_async)]
#[allow(clippy::too_many_lines)]
async fn open_for(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    let options = parse_options(value)?;
    let transport = options.transport;
    let evidence_path = options
        .evidence
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
                    max_encoded_bytes: RESEARCH_BATCH_BYTES,
                },
                group_limits: tidehunter::distributed::group::GroupLimits {
                    max_batches: 32,
                    max_encoded_bytes: RESEARCH_FRAGMENT_BYTES - 8,
                },
                fragment_bytes: options.owner_wal_geometry.fragment_bytes(),
                wal_file_bytes: options.owner_wal_geometry.wal_file_bytes(),
                max_batches_per_partition: options.max_batches_per_partition,
                max_reference_metadata_bytes: 64 * 1024 * 1024,
                max_epochs: 4,
            };
            owner_read_configuration = serde_json::json!(
                (0..options.owners)
                    .map(|partition| {
                        serde_json::json!({
                            "partition": partition,
                            "read_workers": 2,
                            "wal_member_read": "async",
                            "read_resident_first": options.owner_read_resident_first,
                            "read_memory_allowance_bytes": 512 * 1024 * 1024,
                            "read_max_inflight_requests": 32,
                            "read_max_inflight_bytes": 64 * 1024 * 1024,
                            "read_max_device_reads": 128
                        })
                    })
                    .collect::<Vec<_>>()
            );
            let cluster = tcp_cluster::LoopbackCluster::start_with_durability(
                &options.path.join("owners"),
                &evidence_path,
                &shape,
                storage,
                4096,
                128 * 1024 * 1024,
                None,
                transport.clone(),
                2,
                tidehunter::distributed::storage::WalMemberReadPolicy::Async,
                512 * 1024 * 1024,
                PollingPolicy::Adaptive,
                options.owner_read_resident_first,
                DurabilityConfig::with_tier(options.durability_tier),
            )
            .map_err(|error| std::io::Error::other(error.to_string()))?;
            let config = cluster.config.clone();
            (Some(cluster), config)
        };
    remote_config.preparation_workers = 8;
    if !remote_config.tls.transport.same_protocol(&transport) {
        return Err(format!(
            "{} requires transport {}",
            backend_name(&transport),
            transport.label()
        )
        .into());
    }
    // Report only what every owner process confirmed. Each publishes a receipt naming its
    // partition, database, bound address and process before its ready file; a missing, incomplete
    // or foreign one fails the open.
    let owner_receipts: Vec<tcp_cluster::OwnerReceipt> = match &cluster {
        Some(cluster) => cluster.owner_receipts().to_vec(),
        None => options
            .external_owner_durability_receipts
            .iter()
            .map(|path| tcp_cluster::read_owner_receipt(path).map_err(|error| error.to_string()))
            .collect::<Result<_, _>>()?,
    };
    let receipt_endpoints: Vec<(u32, String)> = remote_config
        .endpoints
        .iter()
        .map(|endpoint| (endpoint.partition, endpoint.address.clone()))
        .collect();
    confirm_owner_receipts(
        &owner_receipts,
        &remote_config.storage.partitions,
        remote_config.storage.database,
        &receipt_endpoints,
        options.durability_tier,
    )?;
    let durability_receipt = serde_json::json!({
        "requested_tier": options.durability_tier.as_str(),
        "receipt_durability": options.durability_tier.receipt_durability().as_str(),
        "comparison_cohort": options.durability_tier.comparison_cohort(),
        "source": "each owner process published a receipt naming its partition, database, address, process and launch before its ready file",
        "owners": owner_receipts,
    });
    if let Some(cache_bytes) = options.coordinator_read_cache_bytes {
        remote_config.read_cache_bytes = cache_bytes;
    }
    if let Some(limits) = options.replay_limits {
        remote_config.replay_limits = limits;
    }
    if let Some(occupancy) = options.owner_append_occupancy {
        remote_config.owner_append_occupancy = occupancy;
    }
    // Top-level diagnostics are armed only after warm-up; never consume samples
    // in startup qualification, preload or warm-up via open-time configuration.
    remote_config.write_profile_limit = 0;
    let effective_max_batches = remote_config.storage.max_batches_per_partition;
    let endpoints = receipt_endpoints;
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
    let locally_owned = cluster.is_some();
    let placement = if locally_owned {
        "local harness; all owners share the host storage"
    } else {
        "external topology; placement and physical-device independence are not observed here"
    };
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    let live_nvme = remote_config.live_nvme.is_some();
    #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
    let live_nvme = false;
    let selected_backend = backend_name(&transport);
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    let selected_backend = options
        .nvmeof_readers
        .first()
        .map_or(selected_backend, |reader| match reader.binding.transport {
            tidehunter::distributed::nvmeof::NvmeofTransport::Tcp => {
                "tidehunter_nvme_tcp_sealed_read"
            }
            tidehunter::distributed::nvmeof::NvmeofTransport::Rdma => {
                "tidehunter_nvme_rdma_sealed_read"
            }
        });
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    let selected_backend = if live_nvme {
        "tidehunter_live_nvmeof"
    } else {
        selected_backend
    };
    let mut effective_configuration = serde_json::json!({
        "backend": selected_backend,
        "history_persistence": remote_config.history_persistence,
        "live_queue_geometry": remote_config.live_nvme.as_ref().map(|live| live.queue_geometry),
        "live_export_mode": remote_config.live_nvme.as_ref().map(|live| live.export_mode),
        "transport": transport.label(),
        "transport_security": transport.security_label(),
        // Full serialized selector, so every transport knob (provider, domain, slots) is receipted.
        "transport_config": serde_json::to_value(&transport)?,
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
            "owner_wal_geometry": OwnerWalGeometry::from_layout(
                remote_config.storage.fragment_bytes,
                remote_config.storage.wal_file_bytes,
            ),
            "wal_file_bytes": remote_config.storage.wal_file_bytes,
            "max_reference_metadata_bytes": remote_config.storage.max_reference_metadata_bytes,
            "max_epochs": remote_config.storage.max_epochs
        },
        "cache_policy_receipt": {
            "coordinator_explicit_cache": {
                "requested_bytes": options.coordinator_read_cache_bytes,
                "configured_bytes": remote_config.read_cache_bytes,
                "effective_observation": "configured coordinator limit; usage is in measurement boundary snapshots"
            },
            "linux_page_cache": {
                "requested_policy": null,
                "effective_policy": null,
                "observation": "not controlled or verified by this adapter; application cache disablement does not disable Linux page cache or prove physical NVMe"
            }
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
            "replay_limits": remote_config.replay_limits,
            "owner_append_occupancy": remote_config.owner_append_occupancy,
            "pipelined_writes": remote_config.pipelined_writes,
            "read_cache_bytes": remote_config.read_cache_bytes,
            "read_memory_allowance_bytes": remote_config.read_memory_allowance_bytes,
            "preparation_workers": remote_config.preparation_workers,
            "scan_prefetch_pages": remote_config.scan_prefetch_pages,
            "read_lanes": remote_config.read_lanes,
            "pin_read_lanes": remote_config.pin_read_lanes,
            "max_pending_reads": remote_config.max_pending_reads,
            "max_pending_read_bytes": remote_config.max_pending_read_bytes,
            "write_profile_limit": remote_config.write_profile_limit
        },
        "coordinator_path": coordinator_path
    });
    effective_configuration["live_leased_write_window"] = serde_json::json!(
        remote_config
            .live_nvme
            .as_ref()
            .map(|live| live.leased_write_window)
    );
    effective_configuration["live_leased_sync"] = serde_json::json!(
        remote_config
            .live_nvme
            .as_ref()
            .map(|live| live.leased_sync)
    );
    effective_configuration["live_leased_write_window_bound"] =
        serde_json::json!(remote_config.live_nvme.as_ref().map(|live| {
            let occupancy = if live.write_mode
                == tidehunter::distributed::tcp::LiveWriteMode::Leased
                && live.leased_write_window.is_none()
            {
                tidehunter::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY
            } else {
                remote_config.owner_append_occupancy
            };
            live.max_inflight_requests.min(occupancy)
        }));
    effective_configuration["live_lease_ahead"] = serde_json::json!(
        remote_config
            .live_nvme
            .as_ref()
            .map(|live| live.lease_ahead)
    );
    effective_configuration["live_polled_writes"] = serde_json::json!(
        remote_config
            .live_nvme
            .as_ref()
            .map(|live| live.polled_writes)
    );
    let live_write_evidence = remote_config.live_nvme.as_ref().map(|live| serde_json::json!({
        "write_mode": live.write_mode,
        "leased_durability": live.leased_durability,
        "inline_reads": live.inline_reads,
        "acknowledgement_contract": live.write_mode.acknowledgement(options.durability_tier),
        "recovery_outcome": "an operation with a lost acknowledgement can be present after recovery: unknown outcome, not exactly-once",
    }));
    fs::create_dir_all(&coordinator_path)?;
    let db = sui::Db::open_remote(&coordinator_path, shape, remote_config).map_err(native_error)?;
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    for reader in &options.nvmeof_readers {
        db.install_nvmeof_reader(reader.clone())
            .map_err(native_error)?;
    }
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    if !options.nvmeof_readers.is_empty() || db.live_nvmeof_stats().is_some() {
        db.register_nvmeof_metrics(registry).map_err(native_error)?;
    }
    #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
    let _ = registry;
    #[cfg(feature = "fabric")]
    {
        effective_configuration["fabric_provider_evidence"] =
            match db.fabric_provider_evidence() {
                Some(rows) => serde_json::Value::Array(
                    rows.into_iter()
                        .map(|evidence| {
                            serde_json::json!({
                                "partition": evidence.partition,
                                "scope": "owner-monitor connection; not all lanes",
                                "provider": evidence.provider,
                                "open_profile": evidence.open_profile.map(|open| serde_json::json!({
                                    "requested_tx": (open.requested_tx != 0).then_some(open.requested_tx),
                                    "requested_rx": (open.requested_rx != 0).then_some(open.requested_rx),
                                    "selected_tx": open.selected_tx,
                                    "selected_rx": open.selected_rx,
                                    "mtu": (open.mtu != 0).then_some(open.mtu),
                                    "provider_version": (open.provider_version != 0).then_some(open.provider_version),
                                })),
                                "hardware_qp_send_wqes": null,
                                "hardware_qp_receive_wqes": null,
                                "hardware_cq_capacity": null,
                                "hardware_geometry_observation": "not exposed by OFI endpoint setup",
                            })
                        })
                        .collect(),
                ),
                None => serde_json::Value::Null,
            };
    }
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    {
        effective_configuration["nvmeof_readers"] = serde_json::Value::Array(
            options
                .nvmeof_readers
                .iter()
                .map(compact_nvmeof_reader)
                .collect::<BackendResult<Vec<_>>>()?,
        );
    }
    #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
    {
        effective_configuration["nvmeof_readers"] = serde_json::json!([]);
    }
    effective_configuration["read_backend"] = serde_json::json!(if live_nvme {
        "live_nvmeof"
    } else if options.nvmeof_readers.is_empty() {
        "owner_record_protocol"
    } else {
        "linux_nvmeof_sealed_read_with_owner_record_fallback"
    });
    effective_configuration["write_backend"] = serde_json::json!(if live_nvme {
        "live_nvmeof"
    } else {
        "owner_record_protocol"
    });
    effective_configuration["data_route_evidence"] = serde_json::json!({
        "scope": "configured routes only; actual payload transport requires independent live I/O counters",
        "live_route_verified": false
    });
    effective_configuration["durability_tier"] =
        serde_json::json!(options.durability_tier.as_str());
    effective_configuration["durability"] = durability_receipt;
    if let Some(evidence) = live_write_evidence {
        effective_configuration["live_write"] = evidence.clone();
        effective_configuration["durability"]["live_write"] = evidence;
    }
    let keyspace = db.ks("root");
    let operational_read_io = db.operational_read_io_monitor();
    Ok(TidehunterBackend {
        db: Some(db),
        keyspace,
        cluster: std::sync::Mutex::new(cluster),
        endpoints,
        owners: options.owners,
        backend_name: selected_backend,
        durability_tier: options.durability_tier,
        evidence_path,
        effective_configuration,
        operational_read_io,
        read_evidence_written: false,
        coordinator_interval_baseline: std::sync::Mutex::new(None),
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        live_write_profile: std::sync::Mutex::new(None),
    })
}

impl TidehunterBackend {
    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    fn live_boundary_snapshot(
        &self,
        db: &sui::Db,
    ) -> BackendResult<Option<tidehunter::distributed::nvmeof::live_metrics::LiveNvmeStats>> {
        let leased = self.effective_configuration["live_write"]["write_mode"] == "leased";
        Ok(live_write_profile::boundary_snapshot(
            leased,
            || db.live_nvmeof_stats(),
            Duration::from_secs(5),
        )?)
    }

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
                            && snapshot.available);
                });
                if finished || Instant::now() >= deadline {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            monitor.visit(|snapshot, stopped| {
                owner_io = serde_json::json!({
                    "available": snapshot.available,
                    "generation": snapshot.generation,
                    "failed_samples": snapshot.failed_samples,
                    "stopped": stopped,
                    "post_workload_sample": snapshot.last_attempt.is_some_and(|attempt| attempt >= cut),
                    "last_sample_age_ms": snapshot.last_attempt.map(|time| time.elapsed().as_millis()),
                    "owner_read_executor_stats": snapshot.owners.iter().map(|(partition, stats)| {
                        serde_json::json!({"partition": partition, "stats": stats})
                    }).collect::<Vec<_>>(),
                    "counter_semantics": {
                        "wal_reads": "logical owner Value-WAL member reads, not device IOPS",
                        "async_read_submissions": "io_uring submissions, not confirmed physical-device reads"
                    }
                });
            });
        }
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        let live_nvmeof = self.db.as_ref().and_then(|db| db.live_nvmeof_stats());
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        let live_nvmeof: Option<serde_json::Value> = None;
        let evidence = serde_json::json!({
            "schema_version": 1,
            "backend": self.backend_name,
            "counter_scope": "cumulative from adapter open through post-workload qualification; owner IO fields come from the latest passive sample",
            "coordinator": coordinator,
            "owner_io": owner_io,
            "live_nvmeof": live_nvmeof,
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
            acknowledgement: match self.durability_tier {
                DurabilityTier::Native => "RF1 complete batch and all earlier batches appended to the configured owner Value-WAL (page cache); background fsync, explicit flush; no replication".to_owned(),
                DurabilityTier::Synchronous => "RF1 complete Value-WAL batch persisted (covering fsync complete) on the configured owner; no replication".to_owned(),
            },
            consistency: "single coordinator NativeIndex with complete-batch owner allocation; RF1; no replication, serializable multi-key transaction, or failover".to_owned(),
            replication_factor: 1,
            measurement_boundary: format!(
                "coordinator API on a dedicated benchmark worker through {} owner processing and the {} durability tier",
                self.backend_name.strip_prefix("tidehunter_").unwrap_or("transport"),
                self.durability_tier
            ),
            get_many_strategy: "distributed ordered grouped reads; one result per caller key".to_owned(),
            batch_atomicity: BatchAtomicity::CrashRecovery,
            topology,
            configuration: self.effective_configuration.clone(),
        }
    }
}

fn partition_snapshot<T: serde::Serialize>(
    rows: Vec<(u32, T)>,
    endpoints: &[(u32, String)],
) -> serde_json::Value {
    let mut partitions = BTreeMap::new();
    for (partition, stats) in rows {
        if partitions.insert(partition.to_string(), stats).is_some() {
            return serde_json::json!({"available": false, "error": format!("duplicate partition {partition}")});
        }
    }
    let expected: HashSet<String> = endpoints
        .iter()
        .map(|(partition, _)| partition.to_string())
        .collect();
    if partitions.len() != expected.len()
        || partitions
            .keys()
            .any(|partition| !expected.contains(partition))
    {
        return serde_json::json!({"available": false, "error": "sampled partitions differ from configured owners", "partitions": partitions});
    }
    serde_json::json!({"available": true, "partitions": partitions})
}

impl KvBackend for TidehunterBackend {
    fn info(&self) -> BackendInfo {
        self.info()
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    fn evict_mapped_wal_before_measurement(&self) -> BackendResult<serde_json::Value> {
        let db = self.db.as_ref().ok_or("backend is shutting down")?;
        let owners = db.evict_mapped_wal_for_benchmark().map_err(native_error)?;
        Ok(
            serde_json::json!({"backend":"tidehunter", "owners": owners.into_iter()
            .map(|(partition, evidence)| serde_json::json!({"partition":partition,"process":evidence}))
            .collect::<Vec<_>>()}),
        )
    }

    fn read_observation(&self) -> BackendResult<serde_json::Value> {
        let db = self.db.as_ref().ok_or("backend is shutting down")?;
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        let nvmeof = {
            let readers = db.nvmeof_stats();
            serde_json::json!({
                "available": !readers.is_empty(),
                "readers": readers,
                "scope": "lifetime host reader counters; submit_to_cq includes kernel/network/target/device",
            })
        };
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        let nvmeof =
            serde_json::json!({"available": false, "reason": "Linux NVMe-oF feature not enabled"});
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        let live_nvmeof = self.live_boundary_snapshot(db)?;
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        let live_nvmeof: Option<serde_json::Value> = None;
        let mut coordinator = db.remote_counters().map(|(cache, _, rpc)| serde_json::json!({
            "read_rpc_count": rpc,
            "hits": cache.hits, "misses": cache.misses, "inserts": cache.inserts,
            "evictions": cache.evictions, "rejected_oversize": cache.rejected_oversize,
            "rejected_mismatch": cache.rejected_mismatch, "bypass_disabled": cache.bypass_disabled,
            "bypass_admission": cache.bypass_admission, "bypass_eviction_limit": cache.bypass_eviction_limit,
            "admitted_bytes": cache.admitted_bytes, "served_bytes": cache.served_bytes,
            "retained_bytes": cache.retained_bytes, "metadata_bytes": cache.metadata_bytes,
            "entries": cache.entries, "budget_bytes": cache.budget_bytes
        }));
        if let Some(coordinator) = coordinator
            .as_mut()
            .and_then(serde_json::Value::as_object_mut)
        {
            db.visit_timing_metrics(|name, value| {
                if matches!(
                    name,
                    "read_profile_live_inline"
                        | "read_profile_live_inline_fallback_drain"
                        | "read_profile_live_inline_fallback_busy"
                        | "read_profile_live_inline_fallback_grant"
                        | "read_profile_live_inline_fallback_credit"
                        | "read_profile_live_inline_fallback_paused"
                        | "read_profile_live_inline_fallback_coalesced"
                ) {
                    coordinator.insert(name.to_owned(), serde_json::json!(value));
                }
            });
        }
        let Some(monitor) = db.read_evidence_monitor() else {
            return Ok(
                serde_json::json!({"available": true, "coordinator": coordinator,
                "nvmeof": nvmeof, "live_nvmeof": live_nvmeof,
                "owner_executor": {"available": false, "error": "read evidence monitor unavailable"}}),
            );
        };
        let owners = match monitor.sample_owner_read_io() {
            Ok(rows) => partition_snapshot(rows, &self.endpoints),
            Err(error) => serde_json::json!({"available": false, "error": error.to_string()}),
        };
        Ok(
            serde_json::json!({"available": true, "coordinator": coordinator,
            "owner_executor": owners,
            "nvmeof": nvmeof, "live_nvmeof": live_nvmeof}),
        )
    }

    fn arm_read_profile(&self, limit: usize) -> BackendResult<()> {
        if limit > 65_536 {
            return Err("read profile limit exceeds 65536".into());
        }
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.set_read_profile_limit(limit as u64);
        Ok(())
    }

    async fn finish_read_profile(&self) -> BackendResult<serde_json::Value> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        let mut counters = BTreeMap::new();
        db.visit_timing_metrics(|name, value| {
            if name.starts_with("read_profile_") {
                counters.insert(name.to_owned(), value);
            }
        });
        let mut partition_counters: BTreeMap<u32, BTreeMap<String, u64>> = BTreeMap::new();
        db.visit_read_profile_partitions(|partition, name, value| {
            partition_counters
                .entry(partition)
                .or_default()
                .insert(name.to_owned(), value);
        });
        let resolution = db.coordinator_resolution_snapshot().map(|snapshot| {
            macro_rules! stage {
                ($name:ident) => {{
                    let value = &snapshot.$name;
                    serde_json::json!({
                        "available": value.count > 0,
                        "sum_ns": (value.count > 0).then_some(value.sum_ns),
                        "count": value.count,
                        "max_ns": (value.count > 0).then_some(value.max_ns),
                        "unavailable_reason": if value.count > 0 { None } else { Some("no observations for this stage") }
                    })
                }};
            }
            serde_json::json!({
                "selection_policy": "independent first N point and bulk API resolution attempts; not the remote-flight prefix; errors count preparation failures, later stages count only completed spans",
                "selected": snapshot.selected,
                "point": snapshot.point,
                "bulk": snapshot.bulk,
                "errors": snapshot.errors,
                "logical_members": snapshot.logical_members,
                "cache_hits": snapshot.cache_hits,
                "index_misses": snapshot.index_misses,
                "remote_misses": snapshot.remote_misses,
                "member_errors": snapshot.member_errors,
                "stages": {
                    "index_lock_wait": stage!(index_lock_wait),
                    "index_lookup": stage!(index_lookup),
                    "cache_lock_wait": stage!(cache_lock_wait),
                    "cache_lookup": stage!(cache_lookup),
                    "cut_validation": stage!(cut_validation),
                    "cut_capture": stage!(cut_capture),
                    "route_preparation": stage!(route_preparation),
                    "pin_preparation": stage!(pin_preparation),
                    "batch_preparation": stage!(batch_preparation),
                    "batch_wait": stage!(batch_wait),
                    "postread_cache_fill": stage!(postread_cache_fill),
                    "result_materialization": stage!(result_materialization)
                }
            })
        });
        let caller_trace = db.read_trace_snapshot();
        // Disable before any later qualification, even when evidence is incomplete.
        db.set_read_profile_limit(0);
        let get = |name: &str| counters.get(name).copied().unwrap_or(0);
        let selected = get("read_profile_selected");
        let completed = get("read_profile_completed");
        let owner_responses = get("read_profile_server_response_recheck_samples");
        let nvme_completed = get("read_profile_nvme_completed");
        let coverage_reason = if selected == 0 {
            "no read flights selected (reads may have resolved locally or workload contained no reads)"
        } else if owner_responses == 0 && nvme_completed > 0 {
            "selected NVMe flights bypass owner record-read RPCs; owner timing is not applicable"
        } else if owner_responses == 0 {
            "no selected owner responses supplied timing"
        } else {
            "owner timing covers selected successful responses and error responses with retained profiles; other failures have no owner timing"
        };
        let errors = get("read_profile_errors");
        let finalization_error = if selected != completed || errors != 0 {
            Some(format!(
                "incomplete read profile: {completed}/{selected} selected requests completed; {errors} selected requests failed or lacked a profile"
            ))
        } else {
            None
        };
        let make_stages = |counters: &BTreeMap<String, u64>,
                           owner_only: bool,
                           coverage_reason: &str| {
            let get = |name: &str| counters.get(name).copied().unwrap_or(0);
            let owner_responses = get("read_profile_server_response_recheck_samples");
            let mut stages = serde_json::Map::new();
            for (name, sum, count, max) in [
                (
                    "nvme_flight",
                    "read_profile_nvme_flight_ns",
                    "read_profile_nvme_flight_count",
                    Some("read_profile_nvme_flight_max_ns"),
                ),
                (
                    "server_planning",
                    "read_profile_server_planning_ns",
                    "read_profile_server_planning_count",
                    Some("read_profile_server_planning_max_ns"),
                ),
                (
                    "server_executor_residence",
                    "read_profile_server_executor_residence_ns",
                    "read_profile_server_executor_residence_count",
                    Some("read_profile_server_executor_residence_max_ns"),
                ),
                (
                    "server_nowait",
                    "read_profile_server_nowait_ns",
                    "read_profile_server_nowait_count",
                    Some("read_profile_server_nowait_max_ns"),
                ),
                (
                    "server_async",
                    "read_profile_server_async_ns",
                    "read_profile_server_async_count",
                    Some("read_profile_server_async_max_ns"),
                ),
                (
                    "server_response_ready",
                    "read_profile_server_response_ready_ns",
                    "read_profile_server_response_ready_count",
                    Some("read_profile_server_response_ready_max_ns"),
                ),
                (
                    "actor_queue",
                    "read_profile_actor_queue_ns",
                    "read_profile_actor_timed_success_count",
                    None,
                ),
                (
                    "actor_service",
                    "read_profile_actor_service_ns",
                    "read_profile_actor_timed_success_count",
                    None,
                ),
                (
                    "inline",
                    "read_profile_inline_ns",
                    "read_profile_inline_completed",
                    None,
                ),
                (
                    "request_encode",
                    "read_profile_request_encode_ns",
                    "read_profile_request_encode_count",
                    None,
                ),
                (
                    "exchange",
                    "read_profile_exchange_ns",
                    "read_profile_exchange_count",
                    None,
                ),
                (
                    "response_decode",
                    "read_profile_response_decode_ns",
                    "read_profile_response_decode_count",
                    None,
                ),
                (
                    "record_verify",
                    "read_profile_record_verify_ns",
                    "read_profile_record_verify_count",
                    None,
                ),
                (
                    "server_handler",
                    "read_profile_server_handler_ns",
                    "read_profile_server_response_recheck_samples",
                    None,
                ),
                (
                    "server_authority_lookup",
                    "read_profile_server_authority_lookup_ns",
                    "read_profile_server_response_recheck_samples",
                    None,
                ),
                (
                    "server_response_recheck_wait",
                    "read_profile_server_response_recheck_wait_ns",
                    "read_profile_server_response_recheck_samples",
                    None,
                ),
                (
                    "server_response_recheck",
                    "read_profile_server_response_recheck_ns",
                    "read_profile_server_response_recheck_samples",
                    None,
                ),
                (
                    "server_record_read",
                    "read_profile_server_record_read_ns",
                    "read_profile_server_response_recheck_samples",
                    None,
                ),
                (
                    "transport_output_queue",
                    "read_profile_transport_output_queue_ns",
                    "read_profile_transport_output_queue_samples",
                    None,
                ),
                (
                    "transport_output_submit",
                    "read_profile_transport_output_submit_ns",
                    "read_profile_transport_output_submit_samples",
                    None,
                ),
                (
                    "transport_output_retire",
                    "read_profile_transport_output_retire_ns",
                    "read_profile_transport_output_retire_samples",
                    None,
                ),
                (
                    "transport_cq_call",
                    "read_profile_transport_cq_call_ns",
                    "read_profile_transport_cq_timed_calls",
                    None,
                ),
            ] {
                if owner_only && !name.starts_with("server_") && !name.starts_with("client_") {
                    continue;
                }
                let n = get(count);
                let available = n > 0 && counters.contains_key(sum);
                stages.insert(name.to_owned(), serde_json::json!({
                "available": available,
                "sum_ns": if available { Some(get(sum)) } else { None },
                "count": n,
                "max_ns": if available { max.and_then(|key| counters.get(key).copied()) } else { None },
                "unavailable_reason": if available { None } else { Some(if owner_responses == 0 { coverage_reason } else { "no observations for this stage" }) }
            }));
            }
            stages
        };
        let stages = make_stages(&counters, false, coverage_reason);
        let owners: BTreeMap<String, serde_json::Value> = partition_counters
            .into_iter()
            .map(|(partition, counters)| {
                let stages = make_stages(
                    &counters,
                    true,
                    "no profiled owner responses for this partition",
                );
                (
                    partition.to_string(),
                    serde_json::json!({"stages": stages, "counters": counters}),
                )
            })
            .collect();
        Ok(serde_json::json!({
            "runtime_enabled": get("read_profile_enabled") == 1,
            "runtime_limit": get("read_profile_limit"),
            "runtime_observation_mode": "bounded_remote_read_requests",
            "caller_correlation": {"available": caller_trace.is_some(), "trace": caller_trace,
                "clock_scope": "coordinator-local monotonic elapsed nanoseconds relative to each API call; no host-clock subtraction",
                "granularity": "API calls contain owner dispatch attempts and logical member indices; SQE tokens are scoped to the child/queue, slots can be reused; coalesced work is explicitly marked",
                "membership_scope": "members and coalesced_members are logical API indices; coalesced_members identifies members joining pre-existing flights only. Within-attempt duplicates may share one physical reference, so neither membership count nor their difference is a physical I/O count. For live I/O, observed completions are unique paired nonzero (io_queue_id, io_token) events; sealed NVMe events describe logical requests, not SQEs.",
                "limitations": "best effort; contention and bounded-cap drops are explicit; client elapsed time is not wire or media latency"},
            "selection_policy": "first N newly admitted remote physical flights (scalar caller-owned, grouped caller-owned or actor); includes failures, excludes coordinator cache hits, index misses and coalesced followers",
            "grouped_inline_scope": "admission origin, not exclusive caller execution: flights can be handed to the actor after cancellation or the bounded empty-progress burst; inline_ns measures submission to completion and can include actor takeover",
            "selected_operations": selected,
            "completed_operations": completed,
            "coverage": {"selected_remote_requests": selected, "completed_remote_requests": completed, "nvme_completed": nvme_completed, "profiled_owner_responses": owner_responses, "reason": coverage_reason},
            "stages": stages,
            "owners": owners,
            "coordinator_resolution": resolution,
            "counters": counters,
            "clock_scope": "local monotonic elapsed clocks in coordinator, transport and owner; grouped_inline_completed classifies grouped caller admission origin, not executing thread, because progress can be handed to the actor; inline_ns includes elapsed time until completion, not CPU time; actor queue/service are elapsed spans, not CPU time; nvme_flight runs from flight admission to terminal completion and verification, including queueing, not pure network or device time; async starts during SQE preparation before kernel acceptance and ends at CQE observation, including submission retry and progress, not physical device latency; transport CQ call is local provider-call wall time, not wire latency",
            "overlap_semantics": "API-resolution and remote-flight prefixes are independently selected; stage scopes overlap and may be nested; sums and percentiles are not additive; host timestamps are not subtracted; global and partition snapshots are sequential rather than globally atomic",
            "unsupported_stages": ["physical_device_io", "wire_latency", "per_stage_percentiles", "per_response_owner_response_codec", "per_response_owner_response_output"],
            "finalization_error": finalization_error
        }))
    }

    fn arm_write_profile(&self, limit: usize) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        if self.backend_name == "tidehunter_live_nvmeof" {
            let window = live_write_profile::Window::arm(limit, self.live_boundary_snapshot(db)?)?;
            *self
                .live_write_profile
                .lock()
                .map_err(|_| "live NVMe write profile lock poisoned")? = Some(window);
        } else {
            db.arm_write_profile(limit).map_err(native_error)?;
        }
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        db.arm_write_profile(limit).map_err(native_error)?;
        let baseline = coordinator_counter_snapshot(db);
        *self
            .coordinator_interval_baseline
            .lock()
            .map_err(|_| "coordinator interval baseline lock poisoned")? = Some(baseline);
        Ok(())
    }

    fn snapshot_write_profile(&self) -> BackendResult<serde_json::Value> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        if self.backend_name == "tidehunter_live_nvmeof" {
            let stats = self.live_boundary_snapshot(db)?;
            self.live_write_profile
                .lock()
                .map_err(|_| "live NVMe write profile lock poisoned")?
                .as_mut()
                .ok_or("live NVMe write profile baseline unavailable")?
                .freeze(stats)?;
        }
        let end = coordinator_counter_snapshot(db);
        let baseline = self
            .coordinator_interval_baseline
            .lock()
            .map_err(|_| "coordinator interval baseline lock poisoned")?;
        let baseline = baseline
            .as_ref()
            .ok_or("coordinator interval baseline unavailable")?;
        let stages = coordinator_interval_delta(baseline, &end)?;
        Ok(serde_json::json!({
            "available": true,
            "scope": "coordinator_arm_through_pre_completion_gate",
            "clock_scope": "cumulative counters sampled after measurement and before the completion gate; checkpoint counters are completion-charged and may include work begun before arm or exclude pending work; independent overlapping populations, not per-request latency",
            "stages": stages
        }))
    }

    async fn finish_write_profile(&self) -> BackendResult<serde_json::Value> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        // The runner's completion gate has already returned. This window can
        // include checkpoint progress during that wait, but excludes finalizer RPCs.
        let coordinator_end = coordinator_counter_snapshot(db);
        let coordinator_interval = match self.coordinator_interval_baseline.lock() {
            Ok(mut baseline) => match baseline.take() {
                Some(baseline) => coordinator_interval_delta(&baseline, &coordinator_end),
                None => Err("coordinator interval baseline unavailable".to_owned()),
            },
            Err(_) => Err("coordinator interval baseline lock poisoned".to_owned()),
        };
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        if self.backend_name == "tidehunter_live_nvmeof" {
            let window = self
                .live_write_profile
                .lock()
                .map_err(|_| "live NVMe write profile lock poisoned")?
                .take()
                .ok_or("live NVMe write profile baseline unavailable")?;
            let mut evidence = window.finish()?;
            let stages = coordinator_interval?;
            evidence["coordinator_interval"] = serde_json::json!({
                "scope": "measurement_boundary_cumulative_delta",
                "clock_scope": "coordinator cumulative counters from profile arm through post-completion-gate snapshot before finalizer RPCs; includes background progress during gate observation, not an exact measured-request interval; checkpoint sums/counts are completion-charged and may include work begun before arm or exclude pending checkpoints; clocks overlap and are not wall time or per-request durations",
                "stages": stages
            });
            return Ok(evidence);
        }
        let finalization = db.finish_write_profile().map_err(native_error);
        let mut counters = BTreeMap::new();
        db.visit_timing_metrics(|name, value| {
            if name.starts_with("write_profile_")
                || name == "write_checkpoint_diagnostics_available"
            {
                counters.insert(name.to_owned(), value);
            }
        });
        let sampled_requests = counters
            .get("write_profile_sampled_requests")
            .copied()
            .unwrap_or(0);
        let expected_connections = counters
            .get("write_profile_expected_connections")
            .copied()
            .unwrap_or(0);
        let captured_connections = counters
            .get("write_profile_captured_connections")
            .copied()
            .unwrap_or(0);
        let profile_limit = counters.get("write_profile_limit").copied().unwrap_or(0);
        let activation_errors = counters
            .get("write_profile_activation_errors")
            .copied()
            .unwrap_or(0);
        let capture_errors = counters
            .get("write_profile_capture_errors")
            .copied()
            .unwrap_or(0);
        let checkpoint_diagnostics_available = counters
            .remove("write_checkpoint_diagnostics_available")
            .map(|value| value == 1);
        let mut errors = Vec::new();
        if let Err(error) = finalization {
            errors.push(error.to_string());
        }
        if activation_errors != 0
            || capture_errors != 0
            || captured_connections < expected_connections
        {
            errors.push(format!(
                "incomplete write profile: captured {captured_connections}/{expected_connections} connections; activation errors={activation_errors}, capture errors={capture_errors}"
            ));
        }
        let unsupported_stages = [
            "native_client_index_apply",
            "per-operation_write_percentiles",
            "physical_device_io",
            "write_profile_connection_receive_frame_including_idle_ns",
            "write_profile_client_request_send_ns",
            "write_profile_client_response_header_ns",
            "write_profile_client_response_body_ns",
        ];
        // Multiplexed frame I/O is not attributable to one request. Legacy wire
        // fields remain zero for compatibility, but must not appear as samples.
        for stage in unsupported_stages {
            counters.remove(stage);
        }
        let mut evidence = serde_json::json!({
            "runtime_observation_mode": "dynamically_armed_remote_append_connection_profiles",
            "selection_policy": "first N append requests per append-owning connection after measurement arm; profile_limit is per connection",
            "sampled_requests": sampled_requests,
            "limit_per_connection": profile_limit,
            "expected_connections": expected_connections,
            "captured_connections": captured_connections,
            "max_profiled_requests": profile_limit.saturating_mul(expected_connections),
            "coordinator_checkpoint_diagnostics_available": checkpoint_diagnostics_available,
            "stages": counters,
            "clock_scope": "append/checkpoint-control connection, client, transport and owner storage clocks have independent populations; per-request multiplexed send/receive clocks are unavailable",
            "overlap_semantics": "client/transport/server clocks overlap and nested components are not additive; storage append and persistence occur on owner workers; no idle interval or coordinator elapsed fraction is inferred",
            "unsupported_stages": unsupported_stages
        });
        let evidence_object = evidence
            .as_object_mut()
            .ok_or("write profile evidence must be an object")?;
        match coordinator_interval {
            Ok(stages) => {
                evidence_object.insert(
                    "coordinator_interval".to_owned(),
                    serde_json::json!({
                        "scope": "measurement_boundary_cumulative_delta",
                        "clock_scope": "coordinator cumulative counters from profile arm through post-completion-gate snapshot before finalizer RPCs; includes background progress during gate observation, not an exact measured-request interval; checkpoint sums/counts are completion-charged and may include work begun before arm or exclude pending checkpoints; clocks overlap and are not wall time or per-request durations",
                        "stages": stages
                    }),
                );
            }
            Err(error) => {
                errors.push(error.clone());
                evidence_object.insert(
                    "coordinator_interval_error".to_owned(),
                    serde_json::Value::String(error),
                );
            }
        }
        evidence_object.insert(
            "finalization_error".to_owned(),
            if errors.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(errors.join("; "))
            },
        );
        Ok(evidence)
    }

    fn arm_write_timeline(&self, limit: usize) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.arm_write_timeline(limit).map_err(native_error)
    }

    fn finish_write_timeline(
        &self,
        deadline: std::time::Instant,
    ) -> BackendResult<serde_json::Value> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        // The existing timeline arm also captures fixed-size local wait counts.
        // The runner binds write_wait_metrics to the case and writes its sidecar.
        Ok(serde_json::to_value(
            db.finish_write_timeline_until(deadline)
                .map_err(native_error)?,
        )?)
    }

    async fn traced_put(&self, key: Bytes, value: Bytes, id: u64) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        let mut batch = db.write_batch();
        batch.write(self.keyspace, key, value);
        batch.commit_traced(id).map_err(native_error)
    }

    async fn traced_delete(&self, key: Bytes, id: u64) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        let mut batch = db.write_batch();
        batch.delete(self.keyspace, key);
        batch.commit_traced(id).map_err(native_error)
    }

    async fn traced_put_batch(&self, entries: &[(Bytes, Bytes)], id: u64) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        let mut batch = db.write_batch();
        for (key, value) in entries {
            batch.write(self.keyspace, key.clone(), value.clone());
        }
        batch.commit_traced(id).map_err(native_error)
    }

    async fn get(&self, key: &[u8]) -> BackendResult<Option<Bytes>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.get(self.keyspace, key).map_err(native_error)
    }
    async fn traced_get(&self, key: &[u8], id: u64) -> BackendResult<Option<Bytes>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.traced_get(self.keyspace, key, id).map_err(native_error)
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
        db.multi_get(self.keyspace, keys)
            .into_iter()
            .map(|result| result.map_err(native_error))
            .collect()
    }
    async fn traced_get_many(&self, keys: &[Bytes], id: u64) -> BackendResult<Vec<Option<Bytes>>> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.traced_multi_get(self.keyspace, keys, id)
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
    fn owner_read_configuration_rejects_retired_value_cache_and_grants() {
        let base = serde_json::json!({
            "partition": 0, "read_workers": 2, "wal_member_read": "async", "read_resident_first": true,
            "read_memory_allowance_bytes": 536870912,
            "read_max_inflight_requests": 32, "read_max_inflight_bytes": 67108864,
            "read_max_device_reads": 128
        });
        assert!(serde_json::from_value::<ExternalOwnerReadConfig>(base.clone()).is_ok());
        for (field, retired) in [
            ("read_cache_bytes", serde_json::json!(0)),
            (
                "resident_reads",
                serde_json::json!({
                    "source_bytes": 0, "source_regions": 0, "destination_bytes": 0,
                    "destination_slots": 0, "max_read_bytes": 0, "max_handles": 0,
                    "require_device_read": false
                }),
            ),
        ] {
            let mut value = base.clone();
            value[field] = retired;
            assert!(serde_json::from_value::<ExternalOwnerReadConfig>(value).is_err());
        }
    }

    #[cfg(all(feature = "nvmeof", target_os = "linux"))]
    #[test]
    fn compact_reader_receipt_keeps_identity_and_hash_without_catalog() {
        use blake2::{Blake2b512, Digest};
        use tidehunter::distributed::nvmeof::{
            ExportManifest, NamespaceBinding, NvmeofReaderConfig, NvmeofTransport,
        };
        let reader = NvmeofReaderConfig {
            binding: NamespaceBinding {
                namespace_path: "/dev/nvme0n1".into(),
                transport: NvmeofTransport::Tcp,
                target: Default::default(),
            },
            manifest: ExportManifest {
                version: 1,
                database_uuid: [1; 16],
                epoch: 2,
                partition: 3,
                generation: 2,
                export_uuid: [4; 16],
                nqn: "nqn.2026-01.local.tidehunter:test".into(),
                wal_file_bytes: 4096,
                source_file_index: 1,
                nsid: 1,
                image_len: 4096,
                block_size: 4096,
                source_device: 7,
                source_inode: 8,
                durable_frontier: 8192,
                catalog: Vec::new(),
            },
            max_inflight_requests: 2,
            max_inflight_members: 4,
            max_inflight_bytes: 8192,
            max_catalog_bytes: 65536,
        };
        let receipt = compact_nvmeof_reader(&reader).unwrap();
        let hash: String = Blake2b512::digest(serde_json::to_vec(&reader.manifest).unwrap())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(receipt["manifest"]["manifest_hash_blake2b512"], hash);
        assert_eq!(receipt["manifest"]["catalog_count"], 0);
        assert!(receipt["manifest"].get("catalog").is_none());
        assert_eq!(receipt["binding"]["transport"], "tcp");
    }

    #[test]
    fn largest_research_batch_fits_native_group_and_fragment() {
        use tidehunter::distributed::{
            codec::{BatchLimits, CompleteBatch, Mutation},
            group::{CompleteGroup, GroupLimits},
            types::{BatchId, BatchVersion},
        };
        let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
        let keyspace = shape.iter_ks().next().unwrap().id();
        let batch_limits = BatchLimits {
            max_operations: 32,
            max_encoded_bytes: RESEARCH_BATCH_BYTES,
        };
        let group_limits = GroupLimits {
            max_batches: 32,
            max_encoded_bytes: RESEARCH_FRAGMENT_BYTES - 8,
        };
        let mutations = (0..32)
            .map(|id| Mutation::Put {
                keyspace,
                key: crate::workload::key_bytes(&[1; 16], 5, id),
                value: crate::workload::value_bytes(&[1; 16], 5, id, 65_536, 1.0),
            })
            .collect();
        let batch = CompleteBatch::new(
            &shape,
            BatchId {
                database: [1; 16],
                operation: [2; 16],
            },
            BatchVersion {
                epoch: 1,
                sequence: 1,
            },
            mutations,
            batch_limits,
        )
        .unwrap();
        assert!(batch.encoded().len() > 2 * 1024 * 1024);
        CompleteGroup::new(&shape, vec![batch], batch_limits, group_limits).unwrap();
        assert_eq!((1024 * 1024 * 1024) % RESEARCH_FRAGMENT_BYTES, 0);
    }

    #[test]
    fn partition_samples_reject_duplicates_and_missing_owners() {
        let endpoints = vec![(3, "a".to_owned()), (8, "b".to_owned())];
        assert_eq!(
            partition_snapshot(vec![(3, 1), (3, 2)], &endpoints)["available"],
            false
        );
        assert_eq!(
            partition_snapshot(vec![(3, 1)], &endpoints)["available"],
            false
        );
        assert_eq!(
            partition_snapshot(vec![(3, 1), (9, 2)], &endpoints)["available"],
            false
        );
        let sample = partition_snapshot(vec![(8, 4), (3, 2)], &endpoints);
        assert_eq!(sample["partitions"]["3"], 2);
        assert_eq!(sample["partitions"]["8"], 4);
    }

    #[test]
    fn external_live_options_default_inline_and_reject_unknown_fields() {
        let mut input = serde_json::json!({
            "addresses": ["10.1.0.1"], "transport": "rdma", "service": 4420, "port_id": 4420,
            "commissioning_service": 4421, "commissioning_port_id": 4421,
            "max_inflight_requests": 8, "max_inflight_bytes": 8388608,
            "connect_timeout_ms": 10000, "io_timeout_ms": 10000
        });
        let parsed: ExternalLiveNvmeof = serde_json::from_value(input.clone()).unwrap();
        assert!(parsed.inline_reads);
        assert_eq!(
            parsed.export_mode,
            tidehunter::distributed::tcp::LiveExportMode::OnDemand
        );
        assert_eq!(
            serde_json::to_value(parsed.queue_geometry).unwrap(),
            serde_json::json!({"io_queues": 4, "queue_size": 128, "poll_queues": 0})
        );
        assert_eq!(
            parsed.write_mode,
            tidehunter::distributed::tcp::LiveWriteMode::Reserved
        );
        input["inline_reads"] = serde_json::json!(false);
        assert!(
            !serde_json::from_value::<ExternalLiveNvmeof>(input.clone())
                .unwrap()
                .inline_reads
        );
        input["leased_durability"] = serde_json::json!("native");
        assert!(serde_json::from_value::<ExternalLiveNvmeof>(input).is_err());
    }

    #[test]
    fn owner_wal_geometry_requires_both_layout_dimensions() {
        for (fragment_bytes, wal_file_bytes, expected) in [
            (
                134217728,
                10737418240,
                Some(OwnerWalGeometry::NativeDefault),
            ),
            (4194304, 1073741824, Some(OwnerWalGeometry::Research)),
            (16777216, 268435456, Some(OwnerWalGeometry::SuiHarness)),
            (4194304, 10737418240, None),
        ] {
            assert_eq!(
                OwnerWalGeometry::from_layout(fragment_bytes, wal_file_bytes),
                expected
            );
        }
        assert_eq!(
            serde_json::to_value(OwnerWalGeometry::from_layout(
                NATIVE_FRAGMENT_BYTES,
                RESEARCH_WAL_FILE_BYTES,
            ))
            .unwrap(),
            serde_json::Value::Null
        );
        assert_eq!(OwnerWalGeometry::from_layout(4096, 8192), None);
    }

    #[test]
    fn external_preparation_history_persistence_is_feature_gated() {
        let root = std::env::temp_dir().join(format!(
            "kv-history-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        for mode in [
            None,
            Some("on_disk"),
            Some("wal_replay"),
            Some("memory_only"),
        ] {
            let mut config = serde_json::json!({
                "guest_root": root.join("guest"), "addresses": ["127.0.0.1:44000"],
                "transport": {"kind": "tcp"}, "max_batches_per_partition": 16384,
                "max_admitted_batches": 16384, "max_index_bytes": 536870912,
                "coordinator_cache_bytes": 0, "owner_read_workers": 4,
                "owner_wal_member_read": "async", "owner_read_memory_allowance_bytes": 268435456,
                "polling": "adaptive"
            });
            if let Some(mode) = mode {
                config["history_persistence"] = serde_json::json!(mode);
            }
            fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
            let output = root.join(mode.unwrap_or("legacy"));
            if mode == Some("memory_only") && !cfg!(feature = "test-utils") {
                assert!(prepare_external_case_file(&input, &output).is_err());
                assert!(!output.exists());
                continue;
            }
            prepare_external_case_file(&input, &output).unwrap();
            let launch: serde_yaml::Value =
                serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap())
                    .unwrap();
            assert_eq!(
                launch["history_persistence"].as_str(),
                Some(mode.unwrap_or("on_disk"))
            );
            let remote: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
            assert_eq!(remote["history_persistence"], mode.unwrap_or("on_disk"));
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn external_preparation_selects_owner_wal_geometry() {
        let root = std::env::temp_dir().join(format!(
            "kv-external-wal-geometry-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        let mut config = serde_json::json!({
            "guest_root": root.join("guest"),
            "addresses": ["127.0.0.1:44000", "127.0.0.1:44001"],
            "transport": {"kind": "tcp"},
            "max_batches_per_partition": 16384, "max_admitted_batches": 16384,
            "max_index_bytes": 512 * 1024 * 1024,
            "coordinator_cache_bytes": 0,
            "owner_read_workers": 4, "owner_wal_member_read": "async",
            "owner_read_memory_allowance_bytes": 256 * 1024 * 1024,
            "polling": "adaptive"
        });
        for (geometry, override_bytes, fragment_bytes, wal_file_bytes) in [
            (None, None, 134217728_u64, 10737418240_u64),
            (
                Some("native_default"),
                Some(268435456_u64),
                134217728,
                268435456,
            ),
            (Some("research"), None, 4194304, 1073741824),
            (Some("sui_harness"), None, 16777216, 268435456),
        ] {
            if let Some(geometry) = geometry {
                config["owner_wal_geometry"] = serde_json::json!(geometry);
            }
            config["owner_wal_file_bytes"] = serde_json::json!(override_bytes);
            fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
            let output = root.join(format!(
                "{}-{wal_file_bytes}",
                geometry.unwrap_or("native_default")
            ));
            prepare_external_case_file(&input, &output).unwrap();
            let remote: RemoteDatabaseConfig =
                serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
            assert_eq!(remote.preparation_workers, 8);
            assert_eq!(remote.storage.fragment_bytes, fragment_bytes);
            assert_eq!(remote.storage.wal_file_bytes, wal_file_bytes);
            if geometry == Some("research") {
                assert_eq!(
                    OwnerWalGeometry::from_layout(
                        remote.storage.fragment_bytes,
                        remote.storage.wal_file_bytes,
                    ),
                    Some(OwnerWalGeometry::Research)
                );
            }
            for partition in 0..2 {
                let launch: serde_yaml::Value = serde_yaml::from_slice(
                    &fs::read(output.join(format!("node-{partition}/launch.yaml"))).unwrap(),
                )
                .unwrap();
                let storage = &launch["node"]["partition"];
                assert_eq!(storage["fragment_bytes"].as_u64(), Some(fragment_bytes));
                assert_eq!(storage["wal_file_bytes"].as_u64(), Some(wal_file_bytes));
            }
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn external_preparation_rejects_invalid_owner_file_sizes() {
        let root = std::env::temp_dir().join(format!("kv-invalid-wal-{}", rand::random::<u64>()));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        let mut config = controller_live_preparation_input();
        config.as_object_mut().unwrap().remove("live_nvmeof");
        config["transport"] = serde_json::json!({"kind": "tcp"});
        for bytes in [0, 134217728, 268435457, 10871635968_u64] {
            config["owner_wal_file_bytes"] = serde_json::json!(bytes);
            fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
            assert!(prepare_external_case_file(&input, &root.join("output")).is_err());
            assert!(!root.join("output").exists());
        }
        fs::remove_dir_all(root).unwrap();
    }

    // Run-12 G18W-r0-219b8b655801a7844229 input with current write settings.
    fn controller_live_preparation_input() -> serde_json::Value {
        serde_json::json!({
            "addresses": ["10.128.15.228:9400"],
            "coordinator_cache_bytes": 0, "durability_tier": "native",
            "guest_root": "/mnt/tidehunter-kv-comparison/bench-b2eec4ef24bc4f90/cells/G18W-r0-219b8b655801a7844229",
            "history_persistence": "wal_replay",
            "live_nvmeof": {
                "addresses": ["10.20.15.197"],
                "commissioning_port_id": 1001, "commissioning_service": 4421,
                "connect_timeout_ms": 30000, "export_mode": "per_owner",
                "inline_reads": true, "io_timeout_ms": 30000,
                "max_inflight_bytes": 16777216, "max_inflight_requests": 32,
                "port_id": 1000,
                "queue_geometry": {"io_queues": 1, "poll_queues": 0, "queue_size": 32},
                "service": 4420, "transport": "rdma", "write_mode": "leased",
                "lease_ahead": true, "polled_writes": false
            },
            "max_admitted_batches": 1000000, "max_batches_per_partition": 16384,
            "max_index_bytes": 536870912,
            "owner_read_memory_allowance_bytes": 536870912,
            "owner_read_resident_first": false, "owner_read_workers": 4,
            "owner_transports": [{
                "fabric": {"backend": "verbs", "source_address": "10.20.15.197"},
                "kind": "ofi", "security": "trusted"
            }],
            "owner_wal_geometry": "native_default", "owner_wal_member_read": "async",
            "polling": "adaptive",
            "transport": {
                "fabric": {"backend": "verbs", "source_address": "10.20.15.196"},
                "kind": "ofi", "security": "trusted"
            }
        })
    }

    #[test]
    fn external_live_input_accepts_explicit_write_settings() {
        let mut value = controller_live_preparation_input();
        value["live_nvmeof"]["lease_ahead"] = serde_json::json!(false);
        value["live_nvmeof"]["polled_writes"] = serde_json::json!(true);
        value["live_nvmeof"]["leased_write_window"] = serde_json::json!(8);
        value["live_nvmeof"]["leased_sync"] = serde_json::json!("group_flush");
        let input: ExternalPreparationInput = serde_json::from_value(value).unwrap();
        let live = input.live_nvmeof.unwrap();
        assert!(!live.lease_ahead);
        assert!(live.polled_writes);
        assert_eq!(live.leased_write_window, Some(8));
        assert_eq!(
            live.leased_sync,
            tidehunter::distributed::tcp::LeasedSync::GroupFlush
        );
        assert_eq!(input.history_persistence, HistoryPersistence::WalReplay);
    }

    #[test]
    fn external_live_input_defaults_write_settings_when_absent() {
        let mut value = controller_live_preparation_input();
        let live = value["live_nvmeof"].as_object_mut().unwrap();
        live.remove("lease_ahead");
        live.remove("polled_writes");
        let input: ExternalPreparationInput = serde_json::from_value(value).unwrap();
        let live = input.live_nvmeof.unwrap();
        assert!(live.lease_ahead);
        assert!(!live.polled_writes);
        assert_eq!(live.leased_write_window, None);
        assert_eq!(
            live.leased_sync,
            tidehunter::distributed::tcp::LeasedSync::PerFrameFua
        );
    }

    #[cfg(all(feature = "fabric", feature = "nvmeof", target_os = "linux"))]
    #[test]
    fn controller_live_input_preserves_write_settings_and_owner_documents() {
        let root = std::env::temp_dir().join(format!(
            "kv-controller-live-settings-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        for (name, mode, export, ahead, polled) in [
            ("controller", "leased", "per_owner", true, false),
            ("polled", "leased", "per_owner", false, true),
            ("reserved", "reserved", "on_demand", true, false),
        ] {
            let mut value = controller_live_preparation_input();
            value["live_nvmeof"]["write_mode"] = serde_json::json!(mode);
            value["live_nvmeof"]["export_mode"] = serde_json::json!(export);
            value["live_nvmeof"]["lease_ahead"] = serde_json::json!(ahead);
            value["live_nvmeof"]["polled_writes"] = serde_json::json!(polled);
            value["live_nvmeof"]["queue_geometry"]["poll_queues"] =
                serde_json::json!(u8::from(polled));
            fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
            let output = root.join(name);
            prepare_external_case_file(&input, &output).unwrap();
            let remote: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
            assert_eq!(remote["history_persistence"], "wal_replay");
            assert_eq!(remote["live_nvme"]["lease_ahead"], ahead);
            assert_eq!(remote["live_nvme"]["polled_writes"], polled);
            assert_eq!(remote["live_nvme"]["write_mode"], mode);
            assert_eq!(remote["live_nvme"]["export_mode"], export);
            assert_eq!(
                remote["live_nvme"]["queue_geometry"],
                value["live_nvmeof"]["queue_geometry"]
            );
            let launch: serde_yaml::Value =
                serde_yaml::from_slice(&fs::read(output.join("node-0/launch.yaml")).unwrap())
                    .unwrap();
            assert_eq!(launch["history_persistence"].as_str(), Some("wal_replay"));
            assert_eq!(
                launch["node"]["live_target"]["export_mode"].as_str(),
                Some(export)
            );
            assert_eq!(
                launch["node"]["live_target"]["host_nqn"].as_str(),
                remote["live_nvme"]["host_nqn"].as_str()
            );
            assert_eq!(
                launch["node"]["partition"]["fragment_bytes"].as_u64(),
                Some(134217728)
            );
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn external_coordinator_cache_requires_explicit_adequate_allowance() {
        let root = std::env::temp_dir().join(format!(
            "kv-external-cache-budget-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let input = root.join("input.json");
        let mut config = serde_json::json!({
            "guest_root": root.join("guest"),
            "addresses": (0..5).map(|i| format!("127.0.0.1:{}", 44000 + i)).collect::<Vec<_>>(),
            "transport": {"kind": "tcp"},
            "owner_wal_geometry": "research",
            "max_batches_per_partition": 16384, "max_admitted_batches": 16384,
            "max_index_bytes": 512 * 1024 * 1024,
            "coordinator_cache_bytes": 1024_u64 * 1024 * 1024,
            "owner_read_workers": 4, "owner_wal_member_read": "async",
            "owner_read_memory_allowance_bytes": 256 * 1024 * 1024,
            "polling": "adaptive"
        });
        let write_input = |value: &serde_json::Value| {
            fs::write(&input, serde_json::to_vec(value).unwrap()).unwrap();
        };
        let default_input: ExternalPreparationInput =
            serde_json::from_value(config.clone()).unwrap();
        assert_eq!(
            default_input.coordinator_read_memory_allowance_bytes,
            512 * 1024 * 1024
        );
        write_input(&config);
        assert!(prepare_external_case_file(&input, &root.join("default-large")).is_err());
        config["coordinator_read_memory_allowance_bytes"] =
            serde_json::json!(1024_u64 * 1024 * 1024);
        write_input(&config);
        assert!(prepare_external_case_file(&input, &root.join("insufficient")).is_err());
        config["coordinator_cache_bytes"] = serde_json::json!(usize::MAX);
        config["coordinator_read_memory_allowance_bytes"] = serde_json::json!(usize::MAX);
        write_input(&config);
        assert!(prepare_external_case_file(&input, &root.join("overflow")).is_err());
        config["coordinator_cache_bytes"] = serde_json::json!(1024_u64 * 1024 * 1024);
        let allowance = 1024_u64 * 1024 * 1024 + 64 * 1024 * 1024;
        config["coordinator_read_memory_allowance_bytes"] = serde_json::json!(allowance);
        write_input(&config);
        let output = root.join("explicit");
        prepare_external_case_file(&input, &output).unwrap();
        let remote: serde_json::Value =
            serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
        assert_eq!(remote["read_cache_bytes"], 1024_u64 * 1024 * 1024);
        assert_eq!(remote["read_memory_allowance_bytes"], allowance);
        let owners: Vec<ExternalOwnerReadConfig> = serde_json::from_slice(
            &fs::read(output.join("external_owner_read_config.json")).unwrap(),
        )
        .unwrap();
        assert!(owners.iter().all(|owner| owner.read_resident_first));
        config["owner_read_resident_first"] = serde_json::json!(false);
        write_input(&config);
        let cold_output = root.join("cold");
        prepare_external_case_file(&input, &cold_output).unwrap();
        let cold_owners: Vec<ExternalOwnerReadConfig> = serde_json::from_slice(
            &fs::read(cold_output.join("external_owner_read_config.json")).unwrap(),
        )
        .unwrap();
        assert!(cold_owners.iter().all(|owner| !owner.read_resident_first));
        #[cfg(not(all(feature = "nvmeof", target_os = "linux")))]
        for mode in ["reserved", "leased"] {
            config["live_nvmeof"] = serde_json::json!({
                "addresses": ["10.1.0.1", "10.1.0.2", "10.1.0.3", "10.1.0.4", "10.1.0.5"],
                "transport": "rdma", "service": 4420, "port_id": 4420,
                "commissioning_service": 4421, "commissioning_port_id": 4421,
                "max_inflight_requests": 8, "max_inflight_bytes": 8388608,
                "connect_timeout_ms": 10000, "io_timeout_ms": 10000, "write_mode": mode
            });
            write_input(&config);
            let output = root.join(format!("unavailable-{mode}"));
            let error = prepare_external_case_file(&input, &output).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires Linux and the nvmeof feature")
            );
            assert!(!output.exists());
        }
        #[cfg(all(feature = "nvmeof", target_os = "linux"))]
        {
            config["live_nvmeof"] = serde_json::json!({
                "addresses": ["10.1.0.1", "10.1.0.2", "10.1.0.3", "10.1.0.4", "10.1.0.5"],
                "transport": "rdma", "service": 4420, "port_id": 4420,
                "commissioning_service": 4421, "commissioning_port_id": 4421,
                "max_inflight_requests": 8, "max_inflight_bytes": 8388608,
                "connect_timeout_ms": 10000, "io_timeout_ms": 10000
            });
            write_input(&config);
            let output = root.join("live");
            prepare_external_case_file(&input, &output).unwrap();
            let remote: serde_json::Value =
                serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap()).unwrap();
            assert_eq!(remote["live_nvme"]["write_mode"], "reserved");
            assert_eq!(remote["pin_read_lanes"], false);
            assert_eq!(remote["live_nvme"]["inline_reads"], true);
            assert_eq!(remote["live_nvme"]["export_mode"], "on_demand");
            assert_eq!(remote["live_nvme"]["lease_ahead"], true);
            assert_eq!(remote["live_nvme"]["polled_writes"], false);
            assert_eq!(
                remote["live_nvme"]["queue_geometry"],
                serde_json::json!({"io_queues": 4, "queue_size": 128, "poll_queues": 0})
            );
            let host_nqn = remote["live_nvme"]["host_nqn"].as_str().unwrap();
            assert!(host_nqn.starts_with("nqn.2014-08.org.nvmexpress:uuid:"));
            for partition in 0..5 {
                let launch: serde_yaml::Value = serde_yaml::from_slice(
                    &fs::read(output.join(format!("node-{partition}/launch.yaml"))).unwrap(),
                )
                .unwrap();
                let node: tidehunter::distributed::tcp::NodeConfig =
                    serde_yaml::from_value(launch["node"].clone()).unwrap();
                assert!(node.live_target.is_some());
                let target = &launch["node"]["live_target"];
                assert_eq!(target["export_mode"].as_str(), Some("on_demand"));
                assert_eq!(
                    target["address"].as_str().unwrap(),
                    format!("10.1.0.{}", partition + 1)
                );
                assert_eq!(target["transport"].as_str().unwrap(), "rdma");
                assert_eq!(target["host_nqn"].as_str(), Some(host_nqn));
                assert_eq!(target["commissioning_service"].as_u64(), Some(4421));
            }
            config["live_nvmeof"]["export_mode"] = serde_json::json!("per_owner");
            config["live_nvmeof"]["write_mode"] = serde_json::json!("leased");
            write_input(&config);
            let permanent = root.join("live-per-owner");
            prepare_external_case_file(&input, &permanent).unwrap();
            let remote: serde_json::Value =
                serde_json::from_slice(&fs::read(permanent.join("remote.json")).unwrap()).unwrap();
            assert_eq!(remote["live_nvme"]["export_mode"], "per_owner");
            for partition in 0..5 {
                let launch: serde_yaml::Value = serde_yaml::from_slice(
                    &fs::read(permanent.join(format!("node-{partition}/launch.yaml"))).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    launch["node"]["live_target"]["export_mode"].as_str(),
                    Some("per_owner")
                );
            }
            config["live_nvmeof"]["export_mode"] = serde_json::json!("on_demand");
            config["live_nvmeof"]["queue_geometry"] =
                serde_json::json!({"io_queues": 2, "queue_size": 32, "poll_queues": 0});
            config["pin_read_lanes"] = serde_json::json!(true);
            for (mode, tier) in [
                ("reserved", "native"),
                ("leased", "native"),
                ("leased", "synchronous"),
            ] {
                for inline_reads in [true, false] {
                    config["live_nvmeof"]["write_mode"] = serde_json::json!(mode);
                    config["live_nvmeof"]["inline_reads"] = serde_json::json!(inline_reads);
                    config["durability_tier"] = serde_json::json!(tier);
                    write_input(&config);
                    let output = root.join(format!("live-{mode}-{tier}-{inline_reads}"));
                    prepare_external_case_file(&input, &output).unwrap();
                    let remote: serde_json::Value =
                        serde_json::from_slice(&fs::read(output.join("remote.json")).unwrap())
                            .unwrap();
                    assert_eq!(remote["live_nvme"]["write_mode"], mode);
                    assert_eq!(remote["pin_read_lanes"], true);
                    assert_eq!(remote["live_nvme"]["inline_reads"], inline_reads);
                    assert_eq!(
                        remote["live_nvme"]["queue_geometry"],
                        config["live_nvmeof"]["queue_geometry"]
                    );
                    let client: tidehunter::distributed::tcp::LiveClientConfig =
                        serde_json::from_value(remote["live_nvme"].clone()).unwrap();
                    assert_eq!(client.queue_geometry.io_queues, 2);
                    assert_eq!(client.queue_geometry.queue_size, 32);
                    assert_eq!(client.queue_geometry.poll_queues, 0);
                    assert_eq!(
                        remote["live_nvme"]["leased_durability"],
                        if mode == "leased" {
                            serde_json::json!(tier)
                        } else {
                            serde_json::Value::Null
                        }
                    );
                }
            }
            config["live_nvmeof"]["addresses"][1] = serde_json::json!("10.1.0.1");
            write_input(&config);
            let invalid = root.join("live-duplicate");
            assert!(prepare_external_case_file(&input, &invalid).is_err());
            assert!(!invalid.exists());
            config["addresses"] = serde_json::json!(["127.0.0.1:44000"]);
            config["live_nvmeof"]["addresses"] = serde_json::json!(["10.1.0.1"]);
            config["live_nvmeof"]["max_inflight_bytes"] = serde_json::json!(32 * 1024);
            write_input(&config);
            let undersized = root.join("live-undersized");
            assert!(prepare_external_case_file(&input, &undersized).is_err());
            assert!(!undersized.exists());
            let required =
                (2 * (RESEARCH_BATCH_BYTES + 8) + 3 * 4095).max(RESEARCH_FRAGMENT_BYTES + 4095);
            config["live_nvmeof"]["max_inflight_bytes"] = serde_json::json!(required - 1);
            write_input(&config);
            let below_boundary = root.join("live-below-boundary");
            assert!(prepare_external_case_file(&input, &below_boundary).is_err());
            assert!(!below_boundary.exists());
            config["live_nvmeof"]["max_inflight_bytes"] = serde_json::json!(required);
            write_input(&config);
            let one = root.join("one-owner");
            prepare_external_case_file(&input, &one).unwrap();
            let remote: tidehunter::distributed::remote_db::RemoteDatabaseConfig =
                serde_json::from_slice(&fs::read(one.join("remote.json")).unwrap()).unwrap();
            assert_eq!(remote.endpoints.len(), 1);
            assert!(remote.validate().is_ok());
            assert_eq!(
                remote.storage.batch_limits.max_encoded_bytes,
                RESEARCH_BATCH_BYTES
            );
            assert!(one.join("node-0/launch.yaml").exists());
            assert!(!one.join("node-1").exists());
        }
        fs::remove_dir_all(&root).unwrap();
    }

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
            "transport": {"kind":"tcp"},
            "max_batches_per_partition": 16384,
            "max_admitted_batches": 1_048_576,
            "max_index_bytes": 512 * 1024 * 1024,
            "coordinator_cache_bytes": 0,
            "owner_read_workers": 4,
            "owner_wal_member_read": "async",
            "owner_read_memory_allowance_bytes": 256 * 1024 * 1024,
            "polling": "adaptive"
        });
        fs::write(&input, serde_json::to_vec(&config).unwrap()).unwrap();
        let result = prepare_external_case_file(&input, &output);
        let output_exists = output.exists();
        fs::remove_dir_all(&root).unwrap();
        assert!(result.is_err());
        assert!(!output_exists);
    }

    fn receipt(partition: u32, address: &str, tier: DurabilityTier) -> tcp_cluster::OwnerReceipt {
        tcp_cluster::OwnerReceipt {
            partition,
            database: [4; 16],
            address: address.to_owned(),
            launch_nonce: "n".to_owned(),
            pid: 1,
            boot_id: "b".to_owned(),
            started_unix_ns: 1,
            history_persistence: HistoryPersistence::OnDisk,
            durability: tcp_cluster::ReceiptDurability {
                tier,
                max_unsynced_age_ms: None,
                max_pending_completions: 8,
            },
        }
    }

    #[test]
    fn one_owner_receipt_cannot_confirm_several_partitions_or_foreign_identities() {
        let endpoints = vec![
            (0, "10.0.0.1:9400".to_owned()),
            (1, "10.0.0.2:9400".to_owned()),
        ];
        let good = [
            receipt(0, "10.0.0.1:9400", DurabilityTier::Synchronous),
            receipt(1, "10.0.0.2:9400", DurabilityTier::Synchronous),
        ];
        let confirm = |receipts: &[tcp_cluster::OwnerReceipt], database, tier| {
            confirm_owner_receipts(receipts, &[0, 1], database, &endpoints, tier)
        };
        assert!(confirm(&good, [4; 16], DurabilityTier::Synchronous).is_ok());
        // The same receipt listed for both partitions.
        let copied = [good[0].clone(), good[0].clone()];
        assert!(confirm(&copied, [4; 16], DurabilityTier::Synchronous).is_err());
        // A receipt from another endpoint, another database or another tier.
        let foreign = [
            good[0].clone(),
            receipt(1, "10.0.0.9:9400", DurabilityTier::Synchronous),
        ];
        assert!(confirm(&foreign, [4; 16], DurabilityTier::Synchronous).is_err());
        assert!(confirm(&good, [5; 16], DurabilityTier::Synchronous).is_err());
        assert!(confirm(&good, [4; 16], DurabilityTier::Native).is_err());
        // A missing receipt, and a receipt with no launch nonce.
        assert!(confirm(&good[..1], [4; 16], DurabilityTier::Synchronous).is_err());
        let mut unlaunched = good;
        unlaunched[1].launch_nonce.clear();
        assert!(confirm(&unlaunched, [4; 16], DurabilityTier::Synchronous).is_err());
    }

    #[test]
    fn owner_transport_selection_never_reuses_a_bound_coordinator_source() {
        let coordinator = Transport::Ofi {
            fabric: tidehunter::distributed::tcp::FabricConfig {
                backend: tidehunter::distributed::tcp::FabricBackend::Verbs,
                domain: Some("mlx5_0".into()),
                source_address: Some(std::net::IpAddr::from([10, 0, 0, 1])),
                slots: None,
            },
            security: FabricSecurity::Trusted,
        };
        assert!(select_owner_transports(&coordinator, None, 2).is_err());
        assert!(select_owner_transports(&Transport::Tcp, Some(vec![Transport::Tcp]), 2).is_err());

        let tcp = select_owner_transports(&Transport::Tcp, None, 2).unwrap();
        assert_eq!(tcp, [Transport::Tcp, Transport::Tcp]);
    }

    #[cfg(feature = "fabric")]
    #[test]
    fn owner_transport_selection_preserves_host_local_bindings() {
        use tidehunter::distributed::tcp::{FabricBackend, FabricConfig};

        let coordinator = Transport::Ofi {
            fabric: FabricConfig {
                backend: FabricBackend::Verbs,
                domain: Some("coordinator0".into()),
                source_address: Some(std::net::IpAddr::from([10, 0, 0, 1])),
                slots: None,
            },
            security: FabricSecurity::Trusted,
        };
        let owner_transports = vec![
            Transport::Ofi {
                fabric: FabricConfig {
                    backend: FabricBackend::Verbs,
                    domain: Some("owner0".into()),
                    source_address: Some(std::net::IpAddr::from([10, 1, 0, 1])),
                    slots: None,
                },
                security: FabricSecurity::Trusted,
            },
            Transport::Ofi {
                fabric: FabricConfig {
                    backend: FabricBackend::Verbs,
                    domain: Some("owner1".into()),
                    source_address: Some(std::net::IpAddr::from([10, 2, 0, 1])),
                    slots: None,
                },
                security: FabricSecurity::Trusted,
            },
        ];
        let selected =
            select_owner_transports(&coordinator, Some(owner_transports.clone()), 2).unwrap();
        assert_eq!(selected, owner_transports);
    }

    #[test]
    fn coordinator_interval_requires_monotone_complete_boundary_snapshots() {
        let baseline = COORDINATOR_INTERVAL_COUNTERS
            .into_iter()
            .map(|key| (key, 10))
            .collect::<BTreeMap<_, _>>();
        let end = COORDINATOR_INTERVAL_COUNTERS
            .into_iter()
            .map(|key| (key, 15))
            .collect::<BTreeMap<_, _>>();
        let delta = coordinator_interval_delta(&baseline, &end).unwrap();
        assert_eq!(delta.len(), COORDINATOR_INTERVAL_COUNTERS.len());
        assert!(delta.values().all(|value| *value == 5));

        let mut missing = end.clone();
        missing.remove(COORDINATOR_INTERVAL_COUNTERS[0]);
        assert!(coordinator_interval_delta(&baseline, &missing).is_err());

        let mut reset = end;
        reset.insert(COORDINATOR_INTERVAL_COUNTERS[0], 9);
        assert!(coordinator_interval_delta(&baseline, &reset).is_err());
    }

    #[cfg(not(feature = "fabric"))]
    #[test]
    fn rejects_selected_ofi_before_services_when_feature_is_disabled() {
        let options = serde_json::json!({
            "path": "/tmp/unused",
            "transport": {
                "kind": "ofi",
                "fabric": {"backend": "efa", "domain": "efa"},
                "security": "mutual-tls"
            }
        });
        assert!(validate_options(&options).is_err());
    }
}
