//! Option document of this backend and its admission.
use super::backend_name;
use crate::backend::BackendResult;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use tidehunter::distributed::durability::DurabilityTier;
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::tcp::Transport;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Options {
    pub(super) transport: Transport,
    pub(super) path: PathBuf,
    #[serde(default)]
    pub(super) evidence: Option<PathBuf>,
    #[serde(default = "default_owners")]
    pub(super) owners: usize,
    #[serde(default = "default_key_count")]
    pub(super) key_count: u64,
    #[serde(default = "default_retained_batches")]
    pub(super) max_batches_per_partition: usize,
    /// Explicit complete-batch admission for table-aware checkpoint replay.
    #[serde(default)]
    pub(super) batch_limits: Option<tidehunter::distributed::codec::BatchLimits>,
    #[serde(default)]
    pub(super) coordinator_read_cache_bytes: Option<usize>,
    #[serde(default)]
    pub(super) owner_append_occupancy: Option<usize>,
    /// Coordinator index geometry of the leased fixture. Production default
    /// (Sui geometry) when absent; rejected together with an external config.
    #[serde(default)]
    pub(super) index: Option<FixtureIndexGeometry>,
    /// Kernel route when present; only Linux test-utils admits a local fixture.
    pub(super) external_remote_config: Option<RemoteDatabaseConfig>,
    /// Owner acknowledgement tier. Applied inside each owner process and confirmed back by it.
    #[serde(default)]
    pub(super) durability_tier: DurabilityTier,
    /// Local ordinary-file fixture only: coordinator durable-cut tuning.
    /// External configurations must carry their own `durable_cut`.
    #[serde(default)]
    pub(super) durable_cut: Option<tidehunter::distributed::durability::DurableCutConfig>,
    /// Local ordinary-file fixture only: owner WAL geometry (default: native).
    #[serde(default)]
    pub(super) local_geometry: Option<LocalGeometry>,
    /// External topology only: one owner-published `ready.durability.json` per owner, in
    /// partition order, copied to the coordinator host. Required there; a missing file fails open.
    #[serde(default)]
    pub(super) external_owner_durability_receipts: Vec<PathBuf>,
    /// Per-keyspace value-cache entries; zero disables the cache (the default).
    #[serde(default)]
    pub(super) value_cache_size: usize,
}

/// Local ordinary-file fixture owner WAL geometry. The fixture's default is the
/// engine-native 128 MiB fragment / 10 GiB file, and the live target allocates
/// the full file per owner; benchmarks that roll several cases must lower it.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalGeometry {
    pub(crate) fragment_bytes: u64,
    pub(crate) wal_file_bytes: u64,
}

impl LocalGeometry {
    pub(crate) fn validate(self) -> Result<(), &'static str> {
        if self.fragment_bytes == 0
            || self.wal_file_bytes == 0
            || !self.fragment_bytes.is_multiple_of(4096)
            || !self.wal_file_bytes.is_multiple_of(self.fragment_bytes)
        {
            return Err(
                "local_geometry needs a positive 4 KiB-multiple fragment_bytes that divides wal_file_bytes",
            );
        }
        Ok(())
    }
}

/// Coordinator index WAL geometry of the leased fixture, in native field names.
/// Every other index row keeps the hybrid's production default.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixtureIndexGeometry {
    pub(crate) frag_size: u64,
    pub(crate) wal_file_size: u64,
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

// External transport, owner identity and payload bounds form one admission boundary.
#[allow(clippy::too_many_lines)]
pub(super) fn parse_options(value: &serde_json::Value) -> BackendResult<Options> {
    // Checked on the document so `--validate-config` rejects the pair even
    // when the nested external document is itself invalid. `index` describes
    // the leased fixture only; an external document owns its own index rows.
    let present = |field: &str| value.get(field).is_some_and(|item| !item.is_null());
    if present("index") && present("external_remote_config") {
        return Err(
            "index applies to the leased fixture only; set the index geometry inside external_remote_config"
                .into(),
        );
    }
    let options = Options::deserialize(value)?;
    let name = backend_name(&options.transport);
    if let Some(index) = options.index {
        // `Wal::layout` asserts these; refusing them here keeps a bad
        // qualified geometry from panicking inside the WAL open.
        if index.frag_size == 0
            || index.wal_file_size == 0
            || index.frag_size > u64::from(u32::MAX)
            || !index.frag_size.is_multiple_of(8)
            || index.wal_file_size % index.frag_size != 0
        {
            return Err(format!(
                "{name} index geometry needs a positive, 8-byte-aligned frag_size up to {}, and a wal_file_size that is a multiple of it",
                u32::MAX
            )
            .into());
        }
    }
    if options.value_cache_size > 1_000_000 {
        return Err(format!(
            "{name} value_cache_size must not exceed 1000000 entries per keyspace"
        )
        .into());
    }
    if let Some(limits) = options.batch_limits {
        limits.validate()?;
    }
    if options.owner_append_occupancy.is_some_and(|limit| {
        !(1..=RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY).contains(&limit)
    }) {
        return Err(
            format!("{name} owner append occupancy exceeds the reserved-control window").into(),
        );
    }
    if options.path.as_os_str().is_empty()
        || options
            .evidence
            .as_ref()
            .is_some_and(|path| path.as_os_str().is_empty())
    {
        return Err(format!("{name} path and evidence path must not be empty").into());
    }
    if !matches!(options.owners, 1 | 2 | 4 | 5 | 16) {
        return Err(format!("{name} owners must be one of 1, 2, 4, 5, or 16").into());
    }
    if options.key_count == 0 || options.max_batches_per_partition == 0 {
        return Err("key_count and max_batches_per_partition must be positive".into());
    }
    usize::try_from(options.key_count).map_err(|_| "key_count exceeds platform limit")?;
    options
        .transport
        .validate()
        .map_err(|error| format!("{name} transport unavailable: {error}"))?;
    let Some(config) = options.external_remote_config.as_ref() else {
        if cfg!(all(feature = "test-utils", target_os = "linux"))
            && options.transport == Transport::TcpTrusted
            && options.external_owner_durability_receipts.is_empty()
        {
            if let Some(cut) = &options.durable_cut {
                cut.validate()
                    .map_err(|error| format!("{name} durable_cut: {error}"))?;
            }
            if let Some(geometry) = options.local_geometry {
                geometry
                    .validate()
                    .map_err(|error| format!("{name} {error}"))?;
            }
            return Ok(options);
        }
        return Err(format!(
            "{name} backends require external_remote_config with live_nvme (leased live NVMe-oF); only the Linux test-utils trusted TCP fixture may omit it"
        )
        .into());
    };
    if options.durable_cut.is_some() {
        return Err(format!(
            "{name} durable_cut is a local-fixture option; set durable_cut inside external_remote_config"
        )
        .into());
    }
    if options.local_geometry.is_some() {
        return Err(format!("{name} local_geometry is a local-fixture option").into());
    }
    if !options.transport.same_protocol(&config.tls.transport) {
        return Err(format!(
            "{name} requires external_remote_config.tls.transport={}",
            options.transport.label()
        )
        .into());
    }
    if config.storage.partitions.len() != options.owners || config.endpoints.len() != options.owners
    {
        return Err(
            "external remote configuration must contain exactly one partition and endpoint per owner"
                .into(),
        );
    }
    let partitions: HashSet<u32> = config.storage.partitions.iter().copied().collect();
    let endpoints: HashSet<u32> = config
        .endpoints
        .iter()
        .map(|endpoint| endpoint.partition)
        .collect();
    let coordinator_cache_bytes = options
        .coordinator_read_cache_bytes
        .unwrap_or(config.read_cache_bytes);
    if coordinator_cache_bytes
        .checked_add(config.max_pending_read_bytes)
        .is_none_or(|bytes| bytes > config.read_memory_allowance_bytes)
    {
        return Err(
            "coordinator read cache plus pending reads exceeds read_memory_allowance_bytes".into(),
        );
    }
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
    {
        return Err("invalid external remote owner configuration".into());
    }
    Ok(options)
}
