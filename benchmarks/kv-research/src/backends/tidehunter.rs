// Reuse the engine's existing harness unchanged, including its explicit startup sequences.
mod live_qualification;
mod live_write_profile;
mod open_record;
mod options;
use open_record::{OpenKind, OpenRecord};
// Only the Linux test-utils fixture names the geometry outside this module.
#[cfg(all(feature = "test-utils", target_os = "linux"))]
pub(crate) use options::{FixtureIndexGeometry, LocalGeometry};
use options::{Options, parse_options};
pub mod replay;
#[allow(clippy::too_many_lines)]
// Its TLS setup is inherited and outside timed calls; do not restyle the shared engine helper.
#[allow(clippy::assigning_clones)]
#[path = "../../../../tidehunter/examples/support/tcp_cluster.rs"]
mod tcp_cluster;

pub fn qualify_live_nvmeof(input: &Path, output: &Path) -> BackendResult<()> {
    live_qualification::run(input, output)
}
pub fn qualify_live_nvmeof_child(input: &Path, ready: &Path) -> BackendResult<()> {
    live_qualification::run_abrupt_child(input, ready)
}

/// Supervised leased-fixture owner child; must run before CLI parsing.
#[cfg(all(feature = "test-utils", target_os = "linux"))]
pub fn maybe_run_leased_fixture_owner() -> BackendResult<bool> {
    crate::leased_fixture::maybe_run_owner_child()
}

use crate::backend::{BackendInfo, BackendResult, BatchAtomicity, KvBackend, native_error};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
// Only the Linux live-route boundary snapshot waits with a timeout.
use std::time::Duration;
use tidehunter::distributed::durability::{DurabilityConfig, DurabilityTier};
use tidehunter::distributed::nvmeof::NvmeofTransport;
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::storage::{
    LocalInventoryConfig, NATIVE_FRAGMENT_BYTES, NATIVE_WAL_FILE_BYTES,
};
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{FabricSecurity, LiveTargetPool, PollingPolicy, Transport};
use tidehunter::key_shape::{KeyShape, KeySpaceConfig, KeyType};
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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalLiveNvmeof {
    addresses: Vec<std::net::IpAddr>,
    transport: NvmeofTransport,
    /// Owner 0's public pool bases. Other owners follow after both ranges.
    service: u16,
    port_id: u16,
    #[serde(default = "default_owner_max_databases")]
    max_databases: usize,
    max_inflight_requests: usize,
    #[serde(default)]
    leased_write_window: Option<usize>,
    #[serde(default)]
    leased_sync: tidehunter::distributed::tcp::LeasedSync,
    #[serde(
        default = "tidehunter::distributed::tcp::LiveClientConfig::default_leased_flush_min_frame_bytes"
    )]
    leased_flush_min_frame_bytes: usize,
    max_inflight_bytes: usize,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
    #[serde(default = "default_resident_first")]
    inline_reads: bool,
    #[serde(default)]
    queue_geometry: tidehunter::distributed::tcp::LiveQueueGeometry,
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
    /// Complete-batch admission bounds for table-aware replay.
    #[serde(default)]
    batch_limits: Option<tidehunter::distributed::codec::BatchLimits>,
    /// Pinned node DB table declarations, not the synthetic root keyspace.
    #[serde(default)]
    replay_node_db_id: Option<u16>,
    #[serde(default)]
    owner_transports: Option<Vec<Transport>>,
    /// Required: leased live NVMe-oF is the only distributed route.
    live_nvmeof: ExternalLiveNvmeof,
    max_batches_per_partition: usize,
    #[serde(default)]
    owner_wal_geometry: OwnerWalGeometry,
    #[serde(default)]
    owner_wal_file_bytes: Option<u64>,
    max_admitted_batches: usize,
    coordinator_cache_bytes: usize,
    #[serde(default = "default_coordinator_read_memory_allowance_bytes")]
    coordinator_read_memory_allowance_bytes: usize,
    #[serde(default)]
    read_lanes: Option<usize>,
    #[serde(default)]
    pin_read_lanes: bool,
    owner_wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
    /// Tier written into every owner launch file; absent means the native tier.
    #[serde(default)]
    durability_tier: DurabilityTier,
    polling: PollingPolicy,
}

fn default_resident_first() -> bool {
    true
}

fn default_coordinator_read_memory_allowance_bytes() -> usize {
    512 * 1024 * 1024
}

fn default_owner_max_databases() -> usize {
    1
}

fn backend_name(transport: &Transport) -> &'static str {
    match transport {
        Transport::Tcp => "tidehunter_tcp",
        Transport::TcpTrusted => "tidehunter_tcp_trusted",
        Transport::Ofi { fabric, security } => match (&fabric.backend, security) {
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

// Keep unvalidated launch files private and remove them on every failed preparation.
struct ExternalPreparationStaging(PathBuf);

impl Drop for ExternalPreparationStaging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Each owner gets `M` consecutive public slots at `base + 2*p*M`,
/// followed by `M` private commissioning slots, for ports and services.
fn owner_target_pool(live: &ExternalLiveNvmeof, partition: u32) -> BackendResult<LiveTargetPool> {
    const INVALID: &str = "invalid live NVMe-oF target ports or max_databases";
    if !(1..=16).contains(&live.max_databases) {
        return Err(INVALID.into());
    }
    let slots = u32::try_from(live.max_databases)?;
    let offset = |base: u16, extra: u32| {
        partition
            .checked_mul(2 * slots)
            .and_then(|start| start.checked_add(extra + u32::from(base)))
            .and_then(|start| u16::try_from(start).ok())
            .ok_or(INVALID)
    };
    let pool = LiveTargetPool {
        address: live.addresses[partition as usize].to_string(),
        transport: live.transport,
        public_port_base: offset(live.port_id, 0)?,
        public_service_base: offset(live.service, 0)?,
        commissioning_port_base: offset(live.port_id, slots)?,
        commissioning_service_base: offset(live.service, slots)?,
    };
    pool.validate(live.max_databases)
        .map_err(|error| format!("owner {partition} target pool: {error}"))?;
    Ok(pool)
}

/// Generate external node launch files with the exact engine harness schema.
// Keep emitted owner budgets beside their admission checks to prevent drift.
#[allow(clippy::too_many_lines)]
pub fn prepare_external_case_file(input: &Path, output: &Path) -> BackendResult<()> {
    let input: ExternalPreparationInput = serde_json::from_slice(&fs::read(input)?)?;
    if input.replay_node_db_id.is_some_and(|id| id > 2) {
        return Err("replay_node_db_id must be 0, 1 or 2".into());
    }
    if input.replay_node_db_id.is_some() && input.batch_limits.is_none() {
        return Err("replay owner preparation requires explicit batch_limits".into());
    }
    let batch_limits = input
        .batch_limits
        .unwrap_or(tidehunter::distributed::codec::BatchLimits {
            max_operations: 32,
            max_encoded_bytes: RESEARCH_BATCH_BYTES,
        });
    batch_limits.validate()?;
    if !input.guest_root.is_absolute()
        || !matches!(input.addresses.len(), 1 | 2 | 4 | 5 | 16)
        || input.max_batches_per_partition == 0
        || input.max_batches_per_partition > 1_000_000
        || input.max_admitted_batches == 0
        || input.max_admitted_batches > 1_000_000
        || input
            .read_lanes
            .is_some_and(|lanes| !(1..=1024).contains(&lanes))
        || input
            .coordinator_cache_bytes
            .checked_add(64 * 1024 * 1024)
            .is_none_or(|bytes| bytes > input.coordinator_read_memory_allowance_bytes)
    {
        return Err("external case requires one, two, four, five or sixteen owners, absolute guest_root, supported positive limits, and budgets including the 64MiB in-flight allowance".into());
    }
    if input.owner_wal_file_bytes.is_some_and(|bytes| {
        bytes < 2 * input.owner_wal_geometry.fragment_bytes()
            || bytes > input.owner_wal_geometry.wal_file_bytes()
            || bytes % input.owner_wal_geometry.fragment_bytes() != 0
            || bytes % 4096 != 0
    }) {
        return Err("invalid owner WAL file size override".into());
    }
    let live = &input.live_nvmeof;
    live.queue_geometry.validate()?;
    if live.queue_geometry.poll_queues > 0 && !live.inline_reads {
        return Err("polled live reads require inline reads".into());
    }
    if live.addresses.len() != input.addresses.len()
        || live
            .addresses
            .iter()
            .any(|ip| ip.is_unspecified() || ip.is_multicast())
        || live.service == 0
        || live.port_id == 0
        || live.max_inflight_requests == 0
        || live.max_inflight_bytes == 0
        || live.connect_timeout_ms == 0
        || live.io_timeout_ms == 0
    {
        return Err(
            "invalid live NVMe-oF addresses, target identity, ports or client limits".into(),
        );
    }
    // The last owner's upper bound covers every earlier slot range.
    owner_target_pool(live, u32::try_from(input.addresses.len() - 1)?)?;
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
        database: [0; 16],
        partitions: (0..u32::try_from(addresses.len())?).collect(),
        batch_limits,
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
    let shape = input.replay_node_db_id.map_or_else(
        || KeyShape::new_single(32, 16, KeyType::uniform(64)),
        |db| replay::shape(&crate::mainnet_trace::tables(), db),
    );
    let live_identities: Vec<_> = (0..input.addresses.len()).map(|_| {
        let mut id: [u8; 16] = rand::random();
        id[6] = (id[6] & 0x0f) | 0x40;
        id[8] = (id[8] & 0x3f) | 0x80;
        let nqn = format!(
            "nqn.2014-08.org.nvmexpress:uuid:{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            id[0], id[1], id[2], id[3], id[4], id[5], id[6], id[7],
            id[8], id[9], id[10], id[11], id[12], id[13], id[14], id[15]
        );
        (id, nqn)
    }).collect();
    let (host_id, host_nqn) = &live_identities[0];
    let live_client = tidehunter::distributed::tcp::LiveClientConfig {
        lease_ahead: live.lease_ahead,
        polled_writes: live.polled_writes,
        host_nqn: host_nqn.clone(),
        host_id: *host_id,
        owner_identities: live_identities
            .iter()
            .enumerate()
            .map(|(partition, (id, nqn))| {
                (
                    partition as u32,
                    tidehunter::distributed::tcp::LiveInitiatorIdentity {
                        host_id: *id,
                        host_nqn: nqn.clone(),
                    },
                )
            })
            .collect(),
        max_inflight_requests: live.max_inflight_requests,
        leased_write_window: live.leased_write_window,
        leased_sync: live.leased_sync,
        leased_flush_min_frame_bytes: live.leased_flush_min_frame_bytes,
        max_inflight_bytes: live.max_inflight_bytes,
        connect_timeout_ms: live.connect_timeout_ms,
        io_timeout_ms: live.io_timeout_ms,
        leased_durability: input.durability_tier,
        inline_reads: live.inline_reads,
        queue_geometry: live.queue_geometry,
    };
    tcp_cluster::prepare_external_case(
        &prepared,
        &input.guest_root,
        &addresses,
        &shape,
        storage,
        input.max_admitted_batches,
        input.transport.clone(),
        &owner_transports,
        input.read_lanes,
        input.owner_wal_member_read,
        input.polling,
        DurabilityConfig::with_tier(input.durability_tier),
        live_client,
    )
    .map_err(|error| std::io::Error::other(error.to_string()))?;

    let remote_path = prepared.join("remote.yaml");
    let mut remote: tidehunter::distributed::remote_db::RemoteDatabaseConfig =
        serde_yaml::from_slice(&fs::read(&remote_path)?)?;
    remote.pin_read_lanes = input.pin_read_lanes;
    remote.read_lanes = input.read_lanes;
    remote.read_cache_bytes = input.coordinator_cache_bytes;
    remote.read_memory_allowance_bytes = input.coordinator_read_memory_allowance_bytes;
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

    for partition in 0..u32::try_from(addresses.len())? {
        let launch_path = prepared
            .join(format!("node-{partition}"))
            .join("launch.yaml");
        let mut launch: serde_yaml::Value = serde_yaml::from_slice(&fs::read(&launch_path)?)?;
        launch["host"]["max_databases"] = serde_yaml::to_value(live.max_databases)?;
        launch["host"]["live_target_pool"] =
            serde_yaml::to_value(owner_target_pool(live, partition)?)?;
        fs::write(&launch_path, serde_yaml::to_string(&launch)?)?;
    }
    fs::rename(&prepared, output)?;
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "prepared_directory": output,
            "transport": input.transport.label(),
            "remote_config_yaml": output.join("remote.yaml"),
            "remote_config_json": output.join("remote.json"),
            "coordinator_read_memory_allowance_bytes": remote.read_memory_allowance_bytes,
            "owner_launch_files": (0..addresses.len()).map(|partition| output.join(format!("node-{partition}/launch.yaml"))).collect::<Vec<_>>()
        }))?
    );
    Ok(())
}
// Checkpoint phase sums are charged on completion, including work begun before
// the measurement boundary. They are not per-request latency or network clocks.
const COORDINATOR_INTERVAL_COUNTERS: [&str; 18] = [
    "write_storage_wait_ns",
    "write_publication_ns",
    "write_dispatch_ns",
    "write_preparation_admission_ns",
    "write_authority_prepare_ns",
    "write_authority_admit_ns",
    "write_authority_windows",
    "write_authority_batches",
    "write_checkpoint_ns",
    "write_checkpoint_count",
    "write_background_checkpoint_ns",
    "write_background_checkpoint_suffix_batches",
    "write_durable_receipt_publication_blocked_ns",
    "write_durable_receipt_publication_blocked_batches",
    "write_append_requests",
    "write_concurrent_admitted",
    "write_out_of_prefix_published_batches",
    "write_dependency_wait_batches",
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

/// Owner persistence counters are sampled only at drained measurement
/// boundaries: each sample is an owner control that fences the append stream
/// and runs a flush barrier, so a sample inside the window would perturb it.
#[derive(Default)]
struct OwnerSampleGate {
    measuring: std::sync::atomic::AtomicBool,
}

impl OwnerSampleGate {
    fn set_measuring(&self, open: bool) {
        // SeqCst: the runner's boundary calls are sequential; no data rides on it.
        self.measuring
            .store(open, std::sync::atomic::Ordering::SeqCst);
    }

    fn sample<T>(&self, sample: impl FnOnce() -> T) -> Result<T, String> {
        if self.measuring.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("owner sample refused inside the measurement window".to_owned());
        }
        Ok(sample())
    }
}

pub struct TidehunterBackend {
    owner_sample_gate: OwnerSampleGate,
    db: Option<std::sync::Arc<sui::Db>>,
    keyspace: tidehunter::key_shape::KeySpace,
    endpoints: Vec<(u32, String)>,
    owners: usize,
    backend_name: &'static str,
    durability_tier: DurabilityTier,
    evidence_path: PathBuf,
    effective_configuration: serde_json::Value,
    read_evidence_written: bool,
    coordinator_interval_baseline: std::sync::Mutex<Option<BTreeMap<&'static str, u64>>>,
    live_write_profile: std::sync::Mutex<Option<live_write_profile::Window>>,
    open_record: OpenRecord,
    #[cfg(all(feature = "test-utils", target_os = "linux"))]
    local_owners: Vec<crate::leased_fixture::OwnerChild>,
}

pub fn validate_options(value: &serde_json::Value) -> BackendResult<()> {
    parse_options(value).map(|_| ())
}

pub async fn open(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    open_for(value, registry).await
}

/// Every partition needs one receipt for its owner process and endpoint.
/// The coordinator separately verifies the provisioned database's tier.
fn confirm_owner_receipts(
    receipts: &[tcp_cluster::OwnerReceipt],
    partitions: &[u32],
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
        if address != Some(&receipt.address)
            || receipt.launch_nonce.is_empty()
            || receipt.durability.tier != tier
        {
            return Err(format!(
                "owner receipt for partition {partition} does not match its endpoint or tier {tier}"
            ));
        }
    }
    Ok(())
}

fn synthetic_shape(value_cache_size: usize) -> KeyShape {
    KeyShape::new_single_config(
        32,
        16,
        KeyType::uniform(64),
        KeySpaceConfig::default().with_value_cache_size(value_cache_size),
    )
}

// Shared async factory interface; the existing owner harness opens synchronously.
#[allow(clippy::unused_async)]
#[allow(clippy::too_many_lines)]
async fn open_for(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
) -> BackendResult<TidehunterBackend> {
    let options = parse_options(value)?;
    open_with_shape(value, registry, synthetic_shape(options.value_cache_size)).await
}

#[allow(clippy::unused_async)]
#[allow(clippy::too_many_lines)]
async fn open_with_shape(
    value: &serde_json::Value,
    registry: &prometheus::Registry,
    shape: KeyShape,
) -> BackendResult<TidehunterBackend> {
    let options = parse_options(value)?;
    let transport = options.transport;
    let evidence_path = options
        .evidence
        .unwrap_or_else(|| options.path.join("evidence"));
    fs::create_dir_all(&options.path)?;
    fs::create_dir_all(&evidence_path)?;
    let local = options.external_remote_config.is_none();
    #[cfg(all(feature = "test-utils", target_os = "linux"))]
    let mut local_owners = Vec::new();
    let mut remote_config = match options.external_remote_config {
        Some(config) => config,
        #[cfg(all(feature = "test-utils", target_os = "linux"))]
        None => {
            let (config, children) = open_local_fixture(
                &options.path,
                &evidence_path,
                options.owners,
                options.durability_tier,
                options.max_batches_per_partition,
                options.batch_limits,
                LocalFixtureTuning {
                    durable_cut: options.durable_cut,
                    geometry: options.local_geometry,
                    index: options.index,
                },
            )?;
            local_owners = children;
            config
        }
        #[cfg(not(all(feature = "test-utils", target_os = "linux")))]
        None => {
            return Err(
                "tidehunter backends require external_remote_config with live_nvme (leased live NVMe-oF)"
                    .into(),
            )
        }
    };
    if !remote_config.tls.transport.same_protocol(&transport) {
        return Err(format!(
            "{} requires transport {}",
            backend_name(&transport),
            transport.label()
        )
        .into());
    }
    // Report only what every owner process confirmed. Each publishes a receipt naming its
    // partition, bound address and process before its ready file.
    let owner_receipts: Vec<tcp_cluster::OwnerReceipt> = options
        .external_owner_durability_receipts
        .iter()
        .map(|path| tcp_cluster::read_owner_receipt(path).map_err(|error| error.to_string()))
        .collect::<Result<_, _>>()?;
    let receipt_endpoints: Vec<(u32, String)> = remote_config
        .endpoints
        .iter()
        .map(|endpoint| (endpoint.partition, endpoint.address.clone()))
        .collect();
    if !local {
        confirm_owner_receipts(
            &owner_receipts,
            &remote_config.storage.partitions,
            &receipt_endpoints,
            options.durability_tier,
        )?;
    }
    let durability_receipt = serde_json::json!({
        "requested_tier": options.durability_tier.as_str(),
        "receipt_durability": options.durability_tier.receipt_durability().as_str(),
        "comparison_cohort": options.durability_tier.comparison_cohort(),
        "source": if local {
            "ordinary-file fixture; provisioned tier verified by the engine"
        } else {
            "each owner process published a receipt naming its partition, address, process and launch before its ready file"
        },
        "owners": owner_receipts,
    });
    if let Some(cache_bytes) = options.coordinator_read_cache_bytes {
        remote_config.read_cache_bytes = cache_bytes;
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
    let selected_backend = "tidehunter_live_nvmeof";
    let mut effective_configuration = serde_json::json!({
        "backend": selected_backend,
        "live_queue_geometry": remote_config.live_nvme.queue_geometry,
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
        "value_cache_entries_per_keyspace": options.value_cache_size,
        "key_bytes": 32,
        "keyshape_mutexes": 16,
        "keyshape_cells_per_mutex": 64,
        "keyshape_total_cells": 1024,
        "owner_storage": "native Tidehunter PartitionLog Value-WAL; not independent Db indexes",
        "placement": if local {
            "supervised loopback owner processes on the same host; no NVMe-oF controller or independent device"
        } else {
            "external topology; placement and physical-device independence are not observed here"
        },
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
            "max_reference_metadata_bytes": remote_config.storage.max_reference_metadata_bytes
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
        "coordinator": {
            "read_cache_bytes": remote_config.read_cache_bytes,
            "read_memory_allowance_bytes": remote_config.read_memory_allowance_bytes,
            "index_frag_bytes": remote_config.index.frag_size,
            "index_wal_file_bytes": remote_config.index.wal_file_size
        },
        "client": {
            "timeout_ms": remote_config.timeout.as_millis(),
            "max_admitted_batches": remote_config.max_admitted_batches,
            "max_pending_batches": remote_config.max_pending_batches,
            "max_pending_bytes": remote_config.max_pending_bytes,
            "owner_append_occupancy": remote_config.owner_append_occupancy,
            "pipelined_writes": remote_config.pipelined_writes,
            "leased_submit_redrain": remote_config.leased_submit_redrain,
            "read_cache_bytes": remote_config.read_cache_bytes,
            "read_memory_allowance_bytes": remote_config.read_memory_allowance_bytes,
            "read_lanes": remote_config.effective_read_lanes(),
            "pin_read_lanes": remote_config.pin_read_lanes,
            "max_pending_reads": remote_config.max_pending_reads,
            "max_pending_read_bytes": remote_config.max_pending_read_bytes,
            "write_profile_limit": remote_config.write_profile_limit
        },
        "coordinator_path": coordinator_path
    });
    effective_configuration["client"]["lifecycle_timeout_ms"] = serde_json::json!(
        remote_config
            .timeout
            .max(tidehunter::distributed::tcp::LIFECYCLE_TIMEOUT)
            .as_millis()
    );
    let live = &remote_config.live_nvme;
    effective_configuration["live_leased_write_window"] =
        serde_json::json!(live.leased_write_window);
    effective_configuration["live_leased_sync"] = serde_json::json!(live.leased_sync);
    effective_configuration["live_leased_flush_min_frame_bytes"] =
        serde_json::json!(live.leased_flush_min_frame_bytes);
    let occupancy = if live.leased_write_window.is_none() {
        RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY
    } else {
        remote_config.owner_append_occupancy
    };
    effective_configuration["live_leased_write_window_bound"] =
        serde_json::json!(live.max_inflight_requests.min(occupancy));
    effective_configuration["live_lease_ahead"] = serde_json::json!(live.lease_ahead);
    effective_configuration["live_polled_writes"] = serde_json::json!(live.polled_writes);
    let live_write_evidence = serde_json::json!({
        "leased_durability": live.leased_durability,
        "inline_reads": live.inline_reads,
        "acknowledgement_contract": tidehunter::distributed::tcp::LiveClientConfig::acknowledgement(options.durability_tier),
        "recovery_outcome": "an operation with a lost acknowledgement can be present after recovery: unknown outcome, not exactly-once",
    });
    fs::create_dir_all(&coordinator_path)?;
    let keyspace = shape
        .iter_ks()
        .next()
        .ok_or("empty distributed key shape")?
        .id();
    let open_kind = if fs::read_dir(&coordinator_path)?.next().is_some() {
        OpenKind::Reopen
    } else {
        OpenKind::Create
    };
    let open_start_ns = tidehunter::distributed::remote_db::diagnostics_clock_ns();
    let db = if local {
        #[cfg(all(feature = "test-utils", target_os = "linux"))]
        {
            crate::leased_fixture::open_coordinator(&coordinator_path, shape, remote_config)?
        }
        #[cfg(not(all(feature = "test-utils", target_os = "linux")))]
        return Err("ordinary-file leased fixture is not compiled".into());
    } else {
        sui::Db::open_remote(&coordinator_path, shape, remote_config).map_err(native_error)?
    };
    let open_record = OpenRecord::new(open_kind, open_start_ns, &db);
    if db.live_nvmeof_stats().is_some() {
        db.register_nvmeof_metrics(registry).map_err(native_error)?;
    }
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
    let database = db
        .remote_database_id()
        .ok_or("opened backend is not remote")?;
    effective_configuration["database"] = serde_json::json!(
        tidehunter::distributed::tcp::host::identity::database_hex(&database)
    );
    let data_backend = if local {
        "leased_ordinary_file_fixture"
    } else {
        "live_nvmeof"
    };
    effective_configuration["read_backend"] = serde_json::json!(data_backend);
    effective_configuration["write_backend"] = serde_json::json!(data_backend);
    effective_configuration["data_route_evidence"] = serde_json::json!({
        "scope": "configured routes only; actual payload transport requires independent live I/O counters",
        "live_route_verified": false
    });
    effective_configuration["durability_tier"] =
        serde_json::json!(options.durability_tier.as_str());
    effective_configuration["durable_cut"] = serde_json::to_value(options.durable_cut)?;
    effective_configuration["durability"] = durability_receipt;
    effective_configuration["live_write"] = live_write_evidence.clone();
    effective_configuration["durability"]["live_write"] = live_write_evidence;
    Ok(TidehunterBackend {
        db: Some(db),
        keyspace,
        endpoints,
        owners: options.owners,
        backend_name: selected_backend,
        durability_tier: options.durability_tier,
        evidence_path,
        effective_configuration,
        read_evidence_written: false,
        owner_sample_gate: OwnerSampleGate::default(),
        coordinator_interval_baseline: std::sync::Mutex::new(None),
        live_write_profile: std::sync::Mutex::new(None),
        open_record,
        #[cfg(all(feature = "test-utils", target_os = "linux"))]
        local_owners,
    })
}

/// Coordinator and owner WAL tuning of the local ordinary-file fixture.
#[cfg(all(feature = "test-utils", target_os = "linux"))]
struct LocalFixtureTuning {
    durable_cut: Option<tidehunter::distributed::durability::DurableCutConfig>,
    geometry: Option<LocalGeometry>,
    index: Option<FixtureIndexGeometry>,
}

#[cfg(all(feature = "test-utils", target_os = "linux"))]
fn open_local_fixture(
    path: &Path,
    evidence: &Path,
    owner_count: usize,
    tier: DurabilityTier,
    max_batches: usize,
    batch_limits: Option<tidehunter::distributed::codec::BatchLimits>,
    tuning: LocalFixtureTuning,
) -> BackendResult<(RemoteDatabaseConfig, Vec<crate::leased_fixture::OwnerChild>)> {
    use crate::leased_fixture::{BenchmarkGeometry, FixtureSpec, OwnerChild, fresh_owners};

    let root = path.join("fixture");
    let saved = if root.try_exists()? {
        Some(FixtureSpec::read(&root)?)
    } else {
        if path.join("coordinator").try_exists()? {
            return Err(
                "cannot reopen a local coordinator without its saved leased fixture".into(),
            );
        }
        None
    };
    let addresses = match &saved {
        Some(saved) if saved.owners.len() == owner_count => saved.owners.clone(),
        Some(_) => return Err("local leased fixture owner count changed on reopen".into()),
        None => fresh_owners(u32::try_from(owner_count)?)?,
    };
    let LocalFixtureTuning {
        durable_cut,
        geometry,
        index,
    } = tuning;
    let geometry = geometry.unwrap_or(LocalGeometry {
        fragment_bytes: NATIVE_FRAGMENT_BYTES,
        wal_file_bytes: NATIVE_WAL_FILE_BYTES,
    });
    let mut spec = BenchmarkGeometry {
        fragment_bytes: geometry.fragment_bytes,
        wal_file_bytes: geometry.wal_file_bytes,
        max_admitted_batches: max_batches,
        durable_cut,
        index,
    }
    .spec(root, tier, addresses);
    if let Some(limits) = batch_limits {
        spec.batch_limits = limits;
    }
    if let Some(saved) = saved.as_ref() {
        spec.host_id = saved.host_id;
        if spec != *saved {
            return Err("local leased fixture configuration changed on reopen".into());
        }
    }
    let config = spec.remote_config()?;
    if saved.is_none() {
        spec.create()?;
    }
    let evidence = evidence.join(format!("local-owners-{:016x}", rand::random::<u64>()));
    fs::create_dir(&evidence)?;
    let mut children = Vec::with_capacity(owner_count);
    for owner in &spec.owners {
        children.push(OwnerChild::spawn(
            &spec,
            owner.partition,
            &evidence,
            &format!("owner-{}", owner.partition),
        )?);
    }
    Ok((config, children))
}

impl TidehunterBackend {
    fn live_boundary_snapshot(
        &self,
        db: &sui::Db,
    ) -> BackendResult<Option<tidehunter::distributed::nvmeof::live_metrics::LiveNvmeStats>> {
        let leased = !self.effective_configuration["live_write"].is_null();
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
        let live_nvmeof = self.db.as_ref().and_then(|db| db.live_nvmeof_stats());
        let evidence = serde_json::json!({
            "schema_version": 1,
            "backend": self.backend_name,
            "counter_scope": "cumulative from adapter open through post-workload qualification",
            "coordinator": coordinator,
            "live_nvmeof": live_nvmeof,
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
        let endpoints = self.endpoints.iter()
            .map(|(partition, address)| serde_json::json!({"partition": partition, "address": address}))
            .collect::<Vec<_>>();
        let topology = serde_json::json!({
            "locally_observed_processes": 1,
            "owners": self.owners,
            "coordinator_process_id": std::process::id(),
            "endpoints": endpoints,
        });
        BackendInfo {
            name: self.backend_name.to_owned(),
            server_version: "Tidehunter 0.1.0".to_owned(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            acknowledgement: match self.durability_tier {
                DurabilityTier::Native => "RF1 complete batch and all earlier batches appended to the configured owner Value-WAL (page cache); background fsync, explicit flush; no replication".to_owned(),
                DurabilityTier::Synchronous => "RF1 complete Value-WAL batch persisted (covering fsync complete) on the configured owner; no replication".to_owned(),
            },
            consistency: "single coordinator native index (LargeTable over the index WAL) with complete-batch owner allocation; RF1; no replication, serializable multi-key transaction, or failover".to_owned(),
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

    fn set_measurement_window(&self, open: bool) {
        self.owner_sample_gate.set_measuring(open);
    }

    fn start_relocation(&self) -> BackendResult<()> {
        let db = self.db.as_ref().ok_or("backend is shutting down")?;
        db.start_relocation().map_err(native_error)
    }

    fn read_observation(&self) -> BackendResult<serde_json::Value> {
        let db = self.db.as_ref().ok_or("backend is shutting down")?;
        let live_nvmeof = self.live_boundary_snapshot(db)?;
        let mut coordinator = db.remote_counters().map(|(cache, _, rpc)| serde_json::json!({
            "read_rpc_count": rpc,
            "hits": cache.hits, "misses": cache.misses, "inserts": cache.inserts,
            "evictions": cache.evictions, "rejected_oversize": cache.rejected_oversize,
            "bypass_disabled": cache.bypass_disabled,
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
                    "read_singleflight_metadata_bytes"
                        | "read_profile_live_inline"
                        | "write_authority_prepare_ns"
                        | "write_authority_admit_ns"
                        | "write_authority_windows"
                        | "write_authority_batches"
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
        let owner_storage = match self
            .owner_sample_gate
            .sample(|| db.owner_storage_stats())
            .and_then(|stats| stats.map_err(|error| format!("{error:?}")))
        {
            Ok(Some(stats)) => serde_json::json!({
                "available": true,
                "persistence_barriers": stats.persistence_barriers,
                "wal_file_syncs": stats.wal_file_syncs,
                "namespace_syncs": stats.namespace_syncs,
                "inventory_witnesses": stats.inventory_witnesses,
                "live_frontier_requests": stats.live_frontier_requests,
                "live_frontier_installs": stats.live_frontier_installs,
                "live_frontier_bytes": stats.live_frontier_bytes,
                "live_frontier_deferred": stats.live_frontier_deferred,
                "read_persist_requests": stats.read_persist_requests,
                "cold_grant_syncs": stats.cold_grant_syncs,
                "live_frontier_file_syncs": stats.live_frontier_file_syncs,
                "live_frontier_namespace_syncs": stats.live_frontier_namespace_syncs,
                "scope": "summed over owners; latest epoch handle; cumulative",
            }),
            Ok(None) => serde_json::json!({"available": false, "reason": "native backend"}),
            Err(error) => serde_json::json!({"available": false, "error": error}),
        };
        let durable_cut = db.durable_cut_stats().map(|stats| {
            serde_json::json!({"enabled": stats.enabled, "epoch": stats.epoch, "nudges": stats.nudges})
        });
        Ok(
            serde_json::json!({"available": true, "coordinator": coordinator,
            "live_nvmeof": live_nvmeof, "owner_storage": owner_storage, "durable_cut": durable_cut,
            "coordinator_open": self.open_record.json()}),
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
        if self.backend_name == "tidehunter_live_nvmeof" {
            let window = live_write_profile::Window::arm(limit, self.live_boundary_snapshot(db)?)?;
            *self
                .live_write_profile
                .lock()
                .map_err(|_| "live NVMe write profile lock poisoned")? = Some(window);
        } else {
            db.arm_write_profile(limit).map_err(native_error)?;
        }
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
            if name.starts_with("write_profile_") {
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

    fn arm_write_timeline(&self, limit: usize, sample_every: usize) -> BackendResult<()> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| std::io::Error::other("backend is shutting down"))?;
        db.arm_write_timeline(limit, sample_every)
            .map_err(native_error)
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
        self.close()
    }
}

impl TidehunterBackend {
    fn close(&mut self) -> BackendResult<()> {
        let mut errors = Vec::new();
        if let Err(error) = self.write_read_evidence() {
            errors.push(format!("read evidence export failed: {error}"));
        }
        if let Some(db) = self.db.take()
            && let Err(error) = db.wait_for_background_threads_to_finish()
        {
            errors.push(format!("coordinator drain failed: {error:?}"));
        }
        #[cfg(all(feature = "test-utils", target_os = "linux"))]
        for owner in self.local_owners.drain(..) {
            if let Err(error) = owner.stop() {
                errors.push(format!("leased fixture owner drain failed: {error}"));
            }
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
        if let Err(error) = self.close() {
            let _ = fs::write(
                self.evidence_path.join("lifecycle-errors.txt"),
                error.to_string(),
            );
        }
    }
}

/// Valid `live_nvmeof` preparation input for `owners` owners (distinct target
/// addresses, commissioning ports one above the service/port ids).
#[cfg(test)]
pub(crate) fn test_live_nvmeof(owners: usize) -> serde_json::Value {
    serde_json::json!({
        "addresses": (1..=owners).map(|owner| format!("10.1.0.{owner}")).collect::<Vec<_>>(),
        "transport": "rdma", "service": 4420, "port_id": 4420,
        "max_inflight_requests": 8, "max_inflight_bytes": 8388608,
        "connect_timeout_ms": 10000, "io_timeout_ms": 10000
    })
}

#[cfg(test)]
#[path = "tidehunter/replay_preparation_tests.rs"]
mod replay_preparation_tests;

#[cfg(test)]
#[path = "tidehunter/option_tests.rs"]
mod option_tests;

#[cfg(test)]
mod tests;
