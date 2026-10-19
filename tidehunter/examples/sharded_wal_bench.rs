//! Bounded comparison of native Db, local and separately running TCP partitions.
//! The spawned topology shares one SSD; no claimed independent-NVMe or Sui result.
//! See docs/distributed/LOCAL_BACKEND_BENCHMARK_2026-10-06.md.

#[path = "support/cache_workloads.rs"]
mod cache_workloads;
#[cfg(feature = "experimental-fabric")]
#[path = "support/fabric_profile.rs"]
mod fabric_profile;
#[path = "support/native_reads.rs"]
mod native_reads;
#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[path = "support/read_campaign.rs"]
mod read_campaign;
#[path = "support/tcp_cluster.rs"]
mod tcp_cluster;

use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tidehunter::checkpoint::DbCheckpoint;
use tidehunter::config::Config;
use tidehunter::db::{Db, DbError};
use tidehunter::distributed::codec::{BatchLimits, Mutation};
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::local_db::{LocalCheckpoint, LocalDatabase, LocalDatabaseConfig};
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::remote_db::{
    RemoteDatabase, RemoteDatabaseConfig, RemoteTransportPool,
};
use tidehunter::distributed::tcp::{PollingPolicy, Transport};
use tidehunter::distributed::worker::WorkerConfig;
use tidehunter::key_shape::{KeyShape, KeySpace, KeySpaceConfig, KeyType};
use tidehunter::metrics::Metrics;
use tidehunter::minibytes::Bytes;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const MIB: usize = 1024 * 1024;
const INDEX_MUTEXES: usize = 16;
const CELLS_PER_MUTEX: usize = 64;

const MAX_BENCH_BATCHES: usize = 4096;
const MAX_VALUE_HISTORY_BYTES: usize = 1024 * MIB;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalLayoutPreset {
    Small,
    Sui,
}

struct WalGeometry {
    fragment_bytes: u64,
    wal_file_bytes: u64,
    group_max_batches: u32,
    group_encoded_bytes: usize,
}

impl WalGeometry {
    fn from_storage(storage: &LocalInventoryConfig) -> Self {
        Self {
            fragment_bytes: storage.fragment_bytes,
            wal_file_bytes: storage.wal_file_bytes,
            group_max_batches: storage.group_limits.max_batches,
            group_encoded_bytes: storage.group_limits.max_encoded_bytes,
        }
    }
}

impl WalLayoutPreset {
    fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Sui => "sui",
        }
    }

    fn geometry(self) -> WalGeometry {
        match self {
            Self::Small => WalGeometry {
                fragment_bytes: MIB as u64,
                wal_file_bytes: (128 * MIB) as u64,
                group_max_batches: 32,
                group_encoded_bytes: MIB - 8,
            },
            Self::Sui => WalGeometry {
                fragment_bytes: (16 * MIB) as u64,
                wal_file_bytes: (256 * MIB) as u64,
                group_max_batches: 32,
                group_encoded_bytes: 12 * MIB,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    NativeAsync,
    NativeBarrier,
    Sharded,
    Tcp,
}
impl Kind {
    fn label(self) -> &'static str {
        match self {
            Self::NativeAsync => "native_async_reference",
            Self::NativeBarrier => "native_value_wal_barrier",
            Self::Sharded => "experimental_local_authority",
            Self::Tcp => "experimental_tcp_authority",
        }
    }
    fn durability(self) -> &'static str {
        match self {
            Self::NativeAsync => "foreground_page_cache",
            Self::NativeBarrier | Self::Sharded | Self::Tcp => {
                "value_wal_file_and_directory_barrier"
            }
        }
    }
}

struct Args {
    transport: Transport,
    tcp_polling: PollingPolicy,
    tcp_resident_first: bool,
    output: PathBuf,
    wal_layout: Option<WalLayoutPreset>,
    batches: usize,
    mixed: usize,
    reads: usize,
    repetitions: usize,
    values: Vec<usize>,
    batch_sizes: Vec<usize>,
    windows: Vec<usize>,
    kinds: Vec<Kind>,
    tcp_read_cache_bytes: Option<usize>,
    tcp_shard_cache_bytes: Option<usize>,
    native_read_cache_bytes: usize,
    read_memory_allowance_bytes: usize,
    shard_read_memory_allowance_bytes: Option<usize>,
    native_read_workers: usize,
    read_resource_evidence: bool,
    tcp_pipeline: Option<bool>,
    tcp_preparation_workers: Option<usize>,
    tcp_scan_prefetch_pages: Option<usize>,
    tcp_read_lanes: Option<usize>,
    tcp_max_pending_reads: Option<usize>,
    tcp_max_pending_read_bytes: Option<usize>,
    tcp_server_read_workers: Option<usize>,
    tcp_wal_member_read: Option<tidehunter::distributed::storage::WalMemberReadPolicy>,
    read_window: usize,
    tcp_preparation_chunk_batches: usize,
    hot_read_pass: bool,
    cold_read_pass: bool,
    cache_probe_keys: usize,
    concurrent_mixed: bool,
    transport_diagnostics: bool,
    remote_config: Option<PathBuf>,
    prepare_external_case: Option<PathBuf>,
    guest_case_root: Option<PathBuf>,
    remote_endpoints: Vec<std::net::SocketAddr>,
}

impl Args {
    fn parse() -> Result<Self> {
        let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let mut args = Self {
            transport: Transport::Tcp,
            tcp_polling: PollingPolicy::Adaptive,
            tcp_resident_first: false,
            output: PathBuf::from(format!("logs/sharded-local-bench-{stamp}")),
            wal_layout: None,
            batches: 128,
            mixed: 32,
            reads: 512,
            repetitions: 1,
            values: vec![1024, 8192],
            batch_sizes: vec![1, 32],
            windows: vec![1, 8],
            kinds: vec![
                Kind::NativeBarrier,
                Kind::Sharded,
                Kind::Tcp,
                Kind::NativeAsync,
            ],
            tcp_read_cache_bytes: None,
            tcp_shard_cache_bytes: None,
            native_read_cache_bytes: 0,
            read_memory_allowance_bytes: 512 * MIB,
            shard_read_memory_allowance_bytes: None,
            native_read_workers: 1,
            read_resource_evidence: false,
            tcp_pipeline: None,
            tcp_preparation_workers: None,
            tcp_scan_prefetch_pages: None,
            tcp_read_lanes: None,
            tcp_max_pending_reads: None,
            tcp_max_pending_read_bytes: None,
            tcp_server_read_workers: None,
            tcp_wal_member_read: None,
            read_window: 1,
            tcp_preparation_chunk_batches: 16,
            hot_read_pass: false,
            cold_read_pass: false,
            cache_probe_keys: 0,
            concurrent_mixed: false,
            transport_diagnostics: false,
            remote_config: None,
            prepare_external_case: None,
            guest_case_root: None,
            remote_endpoints: Vec::new(),
        };
        let mut input = std::env::args().skip(1);
        while let Some(flag) = input.next() {
            if flag == "--help" {
                println!(
                    "sharded_wal_bench [--transport tcp|efa|efa-direct|fabric-tcp] [--wal-layout small|sui (default small)] [--output DIR] [--backend all|local-all|native-sync|native-async|sharded|tcp] [--tcp-read-cache-bytes N] [--native-read-cache-bytes N (nominal value bytes, excludes metadata)] [--tcp-pipeline true|false] [--tcp-preparation-workers 1..8] [--tcp-preparation-chunk-batches 1..32] [--tcp-scan-prefetch-pages 1|2] [--hot-read-pass true|false] [--cold-read-pass true|false (Linux test-utils)] [--transport-diagnostics true|false] [--remote-config YAML] [--batches 128] [--mixed 32] [--reads 512] [--repetitions 1] [--values 1024,8192] [--batch-sizes 1,32] [--windows 1,8] [--prepare-external-case DIR --guest-case-root ABSOLUTE_PATH --remote-endpoints IP:PORT,...]"
                );
                println!(
                    "Campaign bounds: --batches 1..4096, --reads 1..8192; each case is limited to 1 GiB of checked value history."
                );
                println!(
                    "WAL geometry: omitted means small locally/prepared; omitted with --remote-config uses supplied geometry. Explicit presets must match external config."
                );
                println!(
                    "Read controls: --read-window 1..256 --tcp-read-lanes 1..8 --tcp-max-pending-reads 1..4096 --tcp-max-pending-read-bytes N --tcp-server-read-workers 1..8 --tcp-wal-member-read async|mapped|positional (default async) --tcp-shard-cache-bytes N (default 0; per-node retained bytes, including metadata; server controls apply only to local launch or --prepare-external-case)"
                );
                println!(
                    "Memory controls: --read-memory-allowance-bytes N (coordinator/native; default 512 MiB) --shard-read-memory-allowance-bytes N (each storage node; default 512 MiB). Distributed allowances bound cache plus in-flight read bytes, not total RSS. Native cache bytes are nominal values only."
                );
                println!(
                    "Node progress: --tcp-polling adaptive|busy (default adaptive; applies to local launch or --prepare-external-case; busy consumes one CPU per open session)"
                );
                println!(
                    "Node file cache: --tcp-resident-first true|false (default false; opt-in NOWAIT with async cold fallback; applies to local/prepared nodes with async read policy)"
                );
                println!(
                    "Native window: --native-read-workers 1..32 (default 1, shared Db/cache, effective concurrency min(workers, read-window)); --read-resource-evidence true|false (default false, Linux test-utils, outside measured timers)"
                );
                println!(
                    "Workloads: --cache-probe-keys 0..8192 (0 disables recorded fill/hot/scan/hot probe); --concurrent-mixed true|false (default false, bounded reads overlap writes on disjoint keys; retirement includes caller write work)"
                );
                std::process::exit(0);
            }
            let value = input.next().ok_or_else(|| invalid("missing flag value"))?;
            match flag.as_str() {
                "--wal-layout" => {
                    args.wal_layout = Some(match value.as_str() {
                        "small" => WalLayoutPreset::Small,
                        "sui" => WalLayoutPreset::Sui,
                        _ => return Err(invalid("WAL layout must be small or sui").into()),
                    })
                }
                "--transport" => {
                    args.transport = match value.as_str() {
                        "tcp" => Transport::Tcp,
                        "efa" => Transport::Efa,
                        "efa-direct" => Transport::EfaDirect,
                        "fabric-tcp" => Transport::FabricTcp,
                        _ => {
                            return Err(invalid(
                                "transport must be tcp, efa, efa-direct or fabric-tcp",
                            )
                            .into());
                        }
                    }
                }
                "--tcp-polling" => {
                    args.tcp_polling = match value.as_str() {
                        "adaptive" => PollingPolicy::Adaptive,
                        "busy" => PollingPolicy::Busy,
                        _ => return Err(invalid("polling must be adaptive or busy").into()),
                    };
                }
                "--tcp-resident-first" => args.tcp_resident_first = value.parse()?,
                "--output" => args.output = value.into(),
                "--batches" => args.batches = value.parse()?,
                "--mixed" => args.mixed = value.parse()?,
                "--reads" => args.reads = value.parse()?,
                "--repetitions" => args.repetitions = value.parse()?,
                "--values" => args.values = number_list(&value)?,
                "--batch-sizes" => args.batch_sizes = number_list(&value)?,
                "--windows" => args.windows = number_list(&value)?,
                "--tcp-read-cache-bytes" => args.tcp_read_cache_bytes = Some(value.parse()?),
                "--tcp-shard-cache-bytes" => args.tcp_shard_cache_bytes = Some(value.parse()?),
                "--native-read-cache-bytes" => args.native_read_cache_bytes = value.parse()?,
                "--read-memory-allowance-bytes" => {
                    args.read_memory_allowance_bytes = value.parse()?
                }
                "--shard-read-memory-allowance-bytes" => {
                    args.shard_read_memory_allowance_bytes = Some(value.parse()?)
                }
                "--native-read-workers" => args.native_read_workers = value.parse()?,
                "--read-resource-evidence" => args.read_resource_evidence = value.parse()?,
                "--tcp-pipeline" => args.tcp_pipeline = Some(value.parse()?),
                "--tcp-preparation-workers" => args.tcp_preparation_workers = Some(value.parse()?),
                "--tcp-scan-prefetch-pages" => args.tcp_scan_prefetch_pages = Some(value.parse()?),
                "--tcp-read-lanes" => args.tcp_read_lanes = Some(value.parse()?),
                "--tcp-max-pending-reads" => args.tcp_max_pending_reads = Some(value.parse()?),
                "--tcp-max-pending-read-bytes" => {
                    args.tcp_max_pending_read_bytes = Some(value.parse()?)
                }
                "--tcp-server-read-workers" => args.tcp_server_read_workers = Some(value.parse()?),
                "--tcp-wal-member-read" => {
                    args.tcp_wal_member_read = Some(match value.as_str() {
                        "async" => tidehunter::distributed::storage::WalMemberReadPolicy::Async,
                        "mapped" => tidehunter::distributed::storage::WalMemberReadPolicy::Mapped,
                        "positional" => {
                            tidehunter::distributed::storage::WalMemberReadPolicy::Positional
                        }
                        _ => {
                            return Err(invalid(
                                "WAL member read must be async, mapped or positional",
                            )
                            .into());
                        }
                    })
                }
                "--read-window" => args.read_window = value.parse()?,
                "--tcp-preparation-chunk-batches" => {
                    args.tcp_preparation_chunk_batches = value.parse()?
                }
                "--hot-read-pass" => args.hot_read_pass = value.parse()?,
                "--cold-read-pass" => args.cold_read_pass = value.parse()?,
                "--cache-probe-keys" => args.cache_probe_keys = value.parse()?,
                "--concurrent-mixed" => args.concurrent_mixed = value.parse()?,
                "--transport-diagnostics" => args.transport_diagnostics = value.parse()?,
                "--remote-config" => args.remote_config = Some(value.into()),
                "--prepare-external-case" => args.prepare_external_case = Some(value.into()),
                "--guest-case-root" => args.guest_case_root = Some(value.into()),
                "--remote-endpoints" => {
                    args.remote_endpoints = value
                        .split(',')
                        .map(str::parse)
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                }
                "--backend" => {
                    args.kinds = match value.as_str() {
                        "all" => vec![
                            Kind::NativeBarrier,
                            Kind::Sharded,
                            Kind::Tcp,
                            Kind::NativeAsync,
                        ],
                        "local-all" => vec![Kind::NativeBarrier, Kind::Sharded, Kind::NativeAsync],
                        "native-sync" => vec![Kind::NativeBarrier],
                        "native-async" => vec![Kind::NativeAsync],
                        "sharded" => vec![Kind::Sharded],
                        "tcp" => vec![Kind::Tcp],
                        _ => return Err(invalid("unknown backend").into()),
                    }
                }
                _ => return Err(invalid("unknown flag").into()),
            }
        }
        if !(1..=256).contains(&args.read_window)
            || !(1..=32).contains(&args.native_read_workers)
            || args.tcp_read_lanes.is_some_and(|n| !(1..=8).contains(&n))
            || args
                .tcp_server_read_workers
                .is_some_and(|n| !(1..=8).contains(&n))
            || args
                .tcp_max_pending_reads
                .is_some_and(|n| !(1..=4096).contains(&n))
            || args
                .tcp_max_pending_read_bytes
                .is_some_and(|n| !(1..=256 * MIB).contains(&n))
        {
            return Err(invalid("invalid bounded read configuration").into());
        }
        let read_bytes = args.tcp_max_pending_read_bytes.unwrap_or(64 * MIB);
        let coordinator_cache = args.tcp_read_cache_bytes.unwrap_or(8 * MIB);
        let shard_allowance = args.shard_read_memory_allowance_bytes.unwrap_or(512 * MIB);
        if args.read_memory_allowance_bytes == 0
            || shard_allowance == 0
            || ((args.kinds.contains(&Kind::Tcp) || args.prepare_external_case.is_some())
                && (coordinator_cache
                    .checked_add(read_bytes)
                    .is_none_or(|bytes| bytes > args.read_memory_allowance_bytes)
                    || args
                        .tcp_shard_cache_bytes
                        .unwrap_or(0)
                        .checked_add(64 * MIB)
                        .is_none_or(|bytes| bytes > shard_allowance)))
        {
            return Err(invalid(
                "cache plus in-flight read bytes exceeds its explicit memory allowance",
            )
            .into());
        }
        if args.remote_config.is_some()
            && (args.tcp_server_read_workers.is_some()
                || args.tcp_wal_member_read.is_some()
                || args.tcp_shard_cache_bytes.is_some()
                || args.shard_read_memory_allowance_bytes.is_some())
        {
            return Err(invalid("server read controls must be set in node YAML before launch, not with --remote-config").into());
        }
        args.transport.validate()?;
        if !(1..=MAX_BENCH_BATCHES).contains(&args.batches)
            || args.mixed > args.batches
            || !(1..=8192).contains(&args.reads)
            || !(1..=5).contains(&args.repetitions)
            || args.values.iter().any(|v| ![256, 1024, 8192].contains(v))
            || args.batch_sizes.iter().any(|v| ![1, 32].contains(v))
            || args
                .windows
                .iter()
                .any(|v| *v == 0 || *v > 32 || *v > args.batches)
        {
            return Err(invalid("configuration exceeds the bounded local campaign").into());
        }
        if args.native_read_cache_bytes > args.read_memory_allowance_bytes
            || (args.native_read_cache_bytes != 0
                && args.values.iter().any(|value| {
                    args.native_read_cache_bytes / (INDEX_MUTEXES * CELLS_PER_MUTEX * value) == 0
                }))
        {
            return Err(invalid(
                "native cache must be zero or fit one value per index cell within the nominal read allowance",
            )
            .into());
        }
        if args.read_resource_evidence {
            if !cfg!(all(target_os = "linux", feature = "test-utils")) {
                return Err(invalid("resource evidence requires Linux and test-utils").into());
            }
            if args.kinds.contains(&Kind::Sharded) {
                return Err(invalid("resource evidence requires native or TCP backends").into());
            }
        }
        if args.cold_read_pass {
            if !cfg!(all(target_os = "linux", feature = "test-utils")) {
                return Err(
                    invalid("cold reads require Linux and the test-utils build feature").into(),
                );
            }
            if args
                .kinds
                .iter()
                .any(|k| !matches!(k, Kind::NativeBarrier | Kind::Tcp))
                || args.tcp_read_cache_bytes.is_some_and(|n| n != 0)
                || args.tcp_shard_cache_bytes.is_some_and(|n| n != 0)
                || args.native_read_cache_bytes != 0
            {
                return Err(invalid(
                    "cold reads require native-sync or tcp and disabled application value caches",
                )
                .into());
            }
            args.tcp_read_cache_bytes = Some(0);
        }
        if !(1..=32).contains(&args.tcp_preparation_chunk_batches)
            || args
                .tcp_preparation_workers
                .is_some_and(|workers| !(1..=8).contains(&workers))
            || args
                .tcp_scan_prefetch_pages
                .is_some_and(|pages| !(1..=2).contains(&pages))
        {
            return Err(invalid(
                "TCP preparation workers must be 1..8, chunks 1..32 and scan pages 1..2",
            )
            .into());
        }
        if args.cache_probe_keys > 8192
            || args
                .batch_sizes
                .iter()
                .any(|batch| args.cache_probe_keys > args.batches * batch)
            || (args.concurrent_mixed
                && (args.mixed == 0
                    || args.mixed >= args.batches
                    || args.kinds.contains(&Kind::Sharded)))
        {
            return Err(invalid(
                "cache/concurrent workload exceeds its bounded key or backend shape",
            )
            .into());
        }
        for value in &args.values {
            for batch in &args.batch_sizes {
                let live_and_updates = args
                    .batches
                    .checked_add(args.mixed)
                    .and_then(|count| count.checked_mul(*batch))
                    .and_then(|operations| operations.checked_mul(*value))
                    .ok_or_else(|| invalid("value history size overflows"))?;
                if live_and_updates > MAX_VALUE_HISTORY_BYTES {
                    return Err(invalid("case would exceed 1 GiB of value history").into());
                }
            }
        }
        if args.remote_config.is_some()
            && (args.kinds != [Kind::Tcp]
                || args.repetitions != 1
                || args.values.len() != 1
                || args.batch_sizes.len() != 1
                || args.windows.len() != 1)
        {
            return Err(invalid("external TCP config requires --backend tcp, one repetition and exactly one value/batch/window shape on a fresh dedicated cluster").into());
        }
        if args.prepare_external_case.is_some() {
            if args.remote_config.is_some()
                || args.remote_endpoints.len() != 5
                || args
                    .remote_endpoints
                    .iter()
                    .any(|address| address.port() == 0)
                || args
                    .remote_endpoints
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != 5
                || !args
                    .guest_case_root
                    .as_ref()
                    .is_some_and(|root| root.is_absolute())
            {
                return Err(invalid("external preparation needs five distinct nonzero numeric endpoints and an absolute guest root, without --remote-config").into());
            }
        } else if args.guest_case_root.is_some() || !args.remote_endpoints.is_empty() {
            return Err(invalid("guest root/endpoints require --prepare-external-case").into());
        }
        Ok(args)
    }
}

fn native_error(error: DbError) -> Box<dyn Error> {
    io::Error::other(format!("native Db: {error:?}")).into()
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn validate_remote_wal_layout(
    preset: Option<WalLayoutPreset>,
    remote: &RemoteDatabaseConfig,
) -> Result<()> {
    let Some(preset) = preset else {
        return Ok(());
    };
    let expected = preset.geometry();
    let actual = &remote.storage;
    if actual.fragment_bytes != expected.fragment_bytes
        || actual.wal_file_bytes != expected.wal_file_bytes
        || actual.group_limits.max_batches != expected.group_max_batches
        || actual.group_limits.max_encoded_bytes != expected.group_encoded_bytes
    {
        return Err(invalid("remote config WAL geometry does not match --wal-layout").into());
    }
    Ok(())
}

fn campaign_wal_geometry(args: &Args) -> Result<(WalGeometry, &'static str, &'static str)> {
    if let Some(path) = &args.remote_config {
        let remote: RemoteDatabaseConfig = serde_yaml::from_slice(&fs::read(path)?)?;
        validate_remote_wal_layout(args.wal_layout, &remote)?;
        return Ok((
            WalGeometry::from_storage(&remote.storage),
            args.wal_layout
                .map_or("external_config", WalLayoutPreset::label),
            "external_config",
        ));
    }

    let preset = args.wal_layout.unwrap_or(WalLayoutPreset::Small);
    Ok((
        preset.geometry(),
        preset.label(),
        if args.wal_layout.is_some() {
            "explicit_benchmark_preset"
        } else {
            "default_small_preset"
        },
    ))
}
fn number_list(value: &str) -> Result<Vec<usize>> {
    let values = value
        .split(',')
        .map(str::parse)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err(invalid("empty number list").into());
    }
    Ok(values)
}
fn ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

struct BackendParameters {
    kind: Kind,
    path: PathBuf,
    shape: KeyShape,
    native: Arc<Config>,
    local: LocalDatabaseConfig,
    remote: Option<RemoteDatabaseConfig>,
    transports: Option<Arc<RemoteTransportPool>>,
}

impl BackendParameters {
    fn new(kind: Kind, path: PathBuf, args: &Args, value_bytes: usize) -> Self {
        // Native capacity is entries per cell, not a global byte limit. This
        // nominal value budget excludes keys and LRU/allocator metadata.
        let cache_entries = if matches!(kind, Kind::NativeAsync | Kind::NativeBarrier) {
            args.native_read_cache_bytes / (INDEX_MUTEXES * CELLS_PER_MUTEX * value_bytes)
        } else {
            0
        };
        let geometry = args.wal_layout.unwrap_or(WalLayoutPreset::Small).geometry();
        let shape = KeyShape::new_single_config(
            8,
            INDEX_MUTEXES,
            KeyType::uniform(CELLS_PER_MUTEX),
            KeySpaceConfig::new().with_value_cache_size(cache_entries),
        );
        let mut native = Config::small();
        native.frag_size = geometry.fragment_bytes;
        native.wal_file_size = geometry.wal_file_bytes;
        // Match three value WAL mappings per partition across five partitions.
        native.max_maps = 15;
        native.max_index_maps = Some(3);
        native.max_dirty_keys = 1024;
        native.snapshot_written_bytes = u64::MAX / 2;
        native.snapshot_unload_threshold = u64::MAX / 2;
        native.metrics_enabled = false;
        let max_batches = args.batches + 2 * args.mixed + 8;
        Self {
            kind,
            path,
            shape,
            native: Arc::new(native),
            remote: None,
            transports: (kind == Kind::Tcp).then(|| Arc::new(RemoteTransportPool::new())),
            local: LocalDatabaseConfig {
                storage: LocalInventoryConfig {
                    database: [42; 16],
                    partitions: vec![0, 1, 2, 3, 4],
                    batch_limits: BatchLimits {
                        max_operations: 32,
                        max_encoded_bytes: 320 * 1024,
                    },
                    group_limits: GroupLimits {
                        max_batches: geometry.group_max_batches,
                        max_encoded_bytes: geometry.group_encoded_bytes,
                    },
                    fragment_bytes: geometry.fragment_bytes,
                    wal_file_bytes: geometry.wal_file_bytes,
                    max_batches_per_partition: max_batches,
                    max_reference_metadata_bytes: 16 * MIB,
                    max_epochs: 4,
                },
                worker: WorkerConfig {
                    max_pending_requests: 64,
                    max_pending_bytes: 16 * MIB,
                    max_group_wait: std::time::Duration::from_micros(100),
                },
                max_admitted_batches: max_batches,
                max_index_bytes: 128 * MIB,
            },
        }
    }
    fn open(&self) -> Result<Backend> {
        let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
        match self.kind {
            Kind::NativeAsync | Kind::NativeBarrier => {
                fs::create_dir_all(&self.path)?;
                Ok(Backend::Native {
                    db: Db::open(&self.path, self.shape.clone(), self.native.clone(), metrics)
                        .map_err(native_error)?,
                    barrier: self.kind == Kind::NativeBarrier,
                    timings: BTreeMap::new(),
                })
            }
            Kind::Sharded => Ok(Backend::Sharded(LocalDatabase::open(
                &self.path,
                &self.shape,
                self.local.clone(),
                metrics,
            )?)),
            Kind::Tcp => Ok(Backend::Tcp(RemoteDatabase::open_with_transport_pool(
                &self.path,
                &self.shape,
                self.remote
                    .clone()
                    .ok_or_else(|| invalid("TCP endpoints were not prepared"))?,
                metrics,
                Arc::clone(self.transports.as_ref().expect("TCP parameters own a pool")),
            )?)),
        }
    }
}

// One long-lived backend per case, never a dense collection. Keep the databases
// inline rather than add a backend-specific allocation/indirection to timed reads.
#[allow(clippy::large_enum_variant)]
enum Backend {
    Native {
        db: Arc<Db>,
        barrier: bool,
        timings: BTreeMap<&'static str, u64>,
    },
    Sharded(LocalDatabase),
    Tcp(RemoteDatabase),
}
enum Checkpoint {
    Native(Arc<DbCheckpoint>),
    Sharded(LocalCheckpoint),
}

impl Backend {
    fn submit_window(
        &mut self,
        batches: Vec<(u64, Vec<Mutation>)>,
        chunk_batches: usize,
    ) -> Result<()> {
        if batches.len() > 1
            && let Self::Tcp(db) = self
        {
            let window: Vec<_> = batches
                .into_iter()
                .map(|(operation, mutations)| {
                    let mut id = [0; 16];
                    id[..8].copy_from_slice(&operation.to_be_bytes());
                    (id, mutations)
                })
                .collect();
            // The outstanding window and final durability barrier stay the
            // same as native. Each preparation chunk is preflighted before
            // admission; the driver window is NOT one atomic application batch.
            for chunk in window.chunks(chunk_batches) {
                db.submit_many(chunk)?;
            }
            return Ok(());
        }
        // Preserve the native/local scalar preparation and commit path. The
        // caller still uses the identical explicit durability barrier per window.
        for (operation, mutations) in batches {
            self.submit(operation, mutations)?;
        }
        Ok(())
    }

    fn submit(&mut self, operation: u64, mutations: Vec<Mutation>) -> Result<()> {
        match self {
            Self::Native { db, timings, .. } => {
                let start = Instant::now();
                let mut batch = db.write_batch();
                for mutation in mutations {
                    match mutation {
                        Mutation::Put {
                            keyspace,
                            key,
                            value,
                        } => batch.write(keyspace, key, value),
                        Mutation::Delete { keyspace, key } => batch.delete(keyspace, key),
                    }
                }
                *timings.entry("native_batch_prepare_ns").or_default() += ns(start);
                let start = Instant::now();
                batch.commit().map_err(native_error)?;
                *timings.entry("native_batch_commit_ns").or_default() += ns(start);
            }
            Self::Sharded(db) => {
                let mut id = [0; 16];
                id[..8].copy_from_slice(&operation.to_be_bytes());
                db.submit(id, mutations)?;
            }
            Self::Tcp(db) => {
                let mut id = [0; 16];
                id[..8].copy_from_slice(&operation.to_be_bytes());
                db.submit(id, mutations)?;
            }
        }
        Ok(())
    }
    fn drain(&mut self) -> Result<()> {
        match self {
            Self::Native {
                db,
                barrier,
                timings,
            } => {
                if *barrier {
                    let start = Instant::now();
                    db.persist_value_wal_for_benchmark().map_err(native_error)?;
                    *timings.entry("native_value_barrier_ns").or_default() += ns(start);
                    *timings.entry("native_value_barriers").or_default() += 1;
                }
            }
            Self::Sharded(db) => db.drain()?,
            Self::Tcp(db) => db.drain()?,
        }
        Ok(())
    }
    fn get(&mut self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>> {
        Ok(match self {
            Self::Native { db, .. } => db.get(ks, key).map_err(native_error)?,
            Self::Sharded(db) => db.get(ks, key)?,
            Self::Tcp(db) => db.get(ks, key)?,
        })
    }
    fn exists(&mut self, ks: KeySpace, key: &[u8]) -> Result<bool> {
        Ok(match self {
            Self::Native { db, .. } => db.exists(ks, key).map_err(native_error)?,
            Self::Sharded(db) => db.exists(ks, key)?,
            Self::Tcp(db) => db.exists(ks, key)?,
        })
    }
    fn get_many(&mut self, ks: KeySpace, keys: &[Vec<u8>]) -> Result<Vec<Option<Bytes>>> {
        // The native API is scalar. Report this as one explicit 32-key driver
        // call, preserving its real scalar implementation for the comparison.
        if let Self::Tcp(db) = self {
            Ok(db.get_many(ks, keys)?)
        } else {
            keys.iter().map(|key| self.get(ks, key)).collect()
        }
    }
    fn checkpoint(&mut self) -> Result<Checkpoint> {
        Ok(match self {
            Self::Native { db, .. } => Checkpoint::Native(db.checkpoint()),
            Self::Sharded(db) => Checkpoint::Sharded(db.checkpoint()?),
            Self::Tcp(db) => Checkpoint::Sharded(db.checkpoint()?),
        })
    }
    fn get_at(
        &mut self,
        ks: KeySpace,
        key: &[u8],
        checkpoint: &Checkpoint,
    ) -> Result<Option<Bytes>> {
        Ok(match (self, checkpoint) {
            (Self::Native { .. }, Checkpoint::Native(checkpoint)) => {
                checkpoint.get(ks, key).map_err(native_error)?
            }
            (Self::Sharded(db), Checkpoint::Sharded(checkpoint)) => {
                db.get_at(ks, key, checkpoint)?
            }
            (Self::Tcp(db), Checkpoint::Sharded(checkpoint)) => db.get_at(ks, key, checkpoint)?,
            _ => return Err(invalid("checkpoint belongs to another backend").into()),
        })
    }
    fn scan(&mut self, ks: KeySpace) -> Result<Vec<(Bytes, Bytes)>> {
        Ok(match self {
            Self::Native { db, .. } => db
                .iterator(ks)
                .collect::<std::result::Result<_, _>>()
                .map_err(native_error)?,
            Self::Sharded(db) => db.scan(ks)?,
            Self::Tcp(db) => db.scan(ks)?,
        })
    }
    fn flush_index(&mut self) -> Result<()> {
        match self {
            Self::Native { db, .. } => {
                db.force_rebuild_control_region().map_err(native_error)?;
            }
            Self::Sharded(db) => db.flush_index()?,
            Self::Tcp(db) => db.flush_index()?,
        }
        Ok(())
    }
    fn authority_write_stats(
        &self,
    ) -> Option<tidehunter::distributed::remote_db::AuthorityWriteStats> {
        match self {
            Self::Tcp(db) => Some(db.authority_write_stats()),
            _ => None,
        }
    }
    fn counters(&self) -> Result<BTreeMap<&'static str, u64>> {
        Ok(match self {
            Self::Native { timings, .. } => timings.clone(),
            Self::Sharded(db) => {
                let stats = db.stats();
                let storage = db.storage_stats();
                let worker = db.worker_stats();
                BTreeMap::from([
                    ("admitted_batches", stats.admitted_batches),
                    ("published_batches", stats.published_batches),
                    ("validation_ns", stats.validation_ns),
                    ("dispatch_ns", stats.dispatch_ns),
                    ("storage_wait_ns", stats.storage_wait_ns),
                    ("publication_ns", stats.publication_ns),
                    ("read_resolve_ns", stats.read_resolve_ns),
                    ("value_read_ns", stats.value_read_ns),
                    ("appended_groups", storage.appended_groups),
                    ("explicit_namespace_syncs", storage.namespace_syncs),
                    (
                        "explicit_group_persistence_calls",
                        storage.persistence_barriers,
                    ),
                    (
                        "worker_admission_validation_ns",
                        worker.admission_validation_ns,
                    ),
                    ("worker_ownership_ns", worker.ownership_ns),
                    ("worker_queue_lock_wait_ns", worker.queue_lock_wait_ns),
                    ("worker_queue_wait_sum_ns", worker.queue_wait_ns),
                    ("worker_group_build_ns", worker.group_build_ns),
                    ("worker_append_ns", worker.append_ns),
                    ("worker_persist_ns", worker.persist_ns),
                    ("worker_coalescing_waits", worker.coalescing_waits),
                    ("worker_submitted_windows", worker.submitted_windows),
                ])
            }
            Self::Tcp(db) => {
                let stats = db.stats();
                // This performs real RPCs. All snapshots are outside measured
                // API/phase timers; a failed stats query fails the campaign.
                let storage = db.storage_stats()?;
                let pipeline = db.pipeline_stats();
                let cache = db.read_cache_stats();
                let authority = db.authority_write_stats();
                BTreeMap::from([
                    ("prepare_calls", pipeline.prepare_calls),
                    (
                        "prepare_progress_completions",
                        pipeline.prepare_progress_completions,
                    ),
                    ("prepare_progress_passes", pipeline.prepare_progress_passes),
                    (
                        "parallel_prepared_batches",
                        pipeline.parallel_prepared_batches,
                    ),
                    ("prepare_wall_ns", pipeline.prepare_wall_ns),
                    ("prepare_task_elapsed_sum_ns", pipeline.prepare_task_ns),
                    (
                        "authority_preparation_admission_ns",
                        authority.preparation_admission_ns,
                    ),
                    ("checkpoint_ns", authority.checkpoint_ns),
                    ("checkpoint_count", authority.checkpoint_count),
                    (
                        "foreground_checkpoint_ns",
                        authority.foreground_checkpoint_ns,
                    ),
                    (
                        "foreground_checkpoint_count",
                        authority.foreground_checkpoint_count,
                    ),
                    (
                        "prepare_reserved_bytes_peak",
                        pipeline.prepare_reserved_bytes_peak as u64,
                    ),
                    ("pipeline_append_requests", pipeline.append_requests),
                    ("pipeline_direct_appends", pipeline.direct_appends),
                    ("pipeline_early_dispatches", pipeline.early_dispatches),
                    (
                        "pipeline_completions_before_drain",
                        pipeline.completions_before_drain,
                    ),
                    (
                        "pipeline_peak_inflight_nodes",
                        pipeline.peak_inflight_nodes as u64,
                    ),
                    ("cache_hits", cache.hits),
                    ("cache_misses", cache.misses),
                    ("cache_inserts", cache.inserts),
                    ("cache_evictions", cache.evictions),
                    ("cache_bypass_disabled", cache.bypass_disabled),
                    ("cache_bypass_eviction_limit", cache.bypass_eviction_limit),
                    ("cache_admitted_charge_bytes", cache.admitted_bytes),
                    ("cache_served_accounted_bytes", cache.served_bytes),
                    ("admitted_batches", stats.admitted_batches),
                    ("published_batches", stats.published_batches),
                    ("validation_ns", stats.validation_ns),
                    ("dispatch_ns", stats.dispatch_ns),
                    ("storage_wait_ns", stats.storage_wait_ns),
                    ("publication_ns", stats.publication_ns),
                    ("read_resolve_ns", stats.read_resolve_ns),
                    ("value_read_ns", stats.value_read_ns),
                    ("appended_groups", storage.appended_groups),
                    ("explicit_namespace_syncs", storage.namespace_syncs),
                    (
                        "explicit_group_persistence_calls",
                        storage.persistence_barriers,
                    ),
                ])
            }
        })
    }
    fn close(self) -> Result<Option<tidehunter::distributed::remote_db::ShutdownStats>> {
        match self {
            Self::Native { db, .. } => db.wait_for_background_threads_to_finish(),
            Self::Sharded(mut db) => db.shutdown()?,
            Self::Tcp(mut db) => {
                db.shutdown()?;
                return Ok(Some(db.shutdown_stats()));
            }
        }
        Ok(None)
    }
}

#[derive(Clone)]
struct PlannedBatch {
    mutations: Vec<Mutation>,
    changes: Vec<(usize, Option<u64>)>,
}

fn mix(mut v: u64) -> u64 {
    v ^= v >> 30;
    v = v.wrapping_mul(0xbf58476d1ce4e5b9);
    v ^= v >> 27;
    v = v.wrapping_mul(0x94d049bb133111eb);
    v ^ (v >> 31)
}
fn key(index: usize) -> [u8; 8] {
    mix(index as u64).to_be_bytes()
}
fn value(index: usize, generation: u64, len: usize) -> Bytes {
    let seed = mix(index as u64 ^ generation.rotate_left(23));
    let mut bytes = vec![0; len];
    for (word, chunk) in bytes.chunks_mut(8).enumerate() {
        let pattern = mix(seed.wrapping_add(word as u64)).to_le_bytes();
        chunk.copy_from_slice(&pattern[..chunk.len()]);
    }
    bytes.into()
}
fn planned(
    ks: KeySpace,
    first: usize,
    count: usize,
    generation: Option<u64>,
    len: usize,
    keys: usize,
) -> PlannedBatch {
    let changes: Vec<_> = (first..first + count)
        .map(|i| (i % keys, generation))
        .collect();
    let mutations = changes
        .iter()
        .map(|&(index, generation)| match generation {
            Some(generation) => Mutation::Put {
                keyspace: ks,
                key: key(index).to_vec().into(),
                value: value(index, generation, len),
            },
            None => Mutation::Delete {
                keyspace: ks,
                key: key(index).to_vec().into(),
            },
        })
        .collect();
    PlannedBatch { mutations, changes }
}

struct Sample {
    elapsed_ns: u64,
    operations: usize,
    bytes: usize,
}

fn write_exchange_profile(
    output: &mut Output,
    case: &Case,
    state: &str,
    sample: usize,
    p: &tidehunter::distributed::tcp::TcpReadProfile,
) -> Result<()> {
    writeln!(
        output.exchange_profiles,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        case.label(),
        case.value_bytes,
        case.batch_size,
        case.window,
        case.repetition,
        state,
        sample,
        p.request_send_ns,
        p.response_header_ns,
        p.response_body_ns,
        p.socket.read_ns,
        p.socket.write_ns,
        p.socket.timeout_setup_ns,
        p.socket.read_calls,
        p.socket.write_calls,
        p.socket.timeout_sets,
        p.socket.read_bytes,
        p.socket.write_bytes,
        p.socket.fabric_drain_ns,
        p.socket.fabric_drains
    )?;
    Ok(())
}

fn write_node_profiles(
    output: &mut Output,
    case: &Case,
    phase: &str,
    nodes: Vec<(u32, tidehunter::distributed::tcp::ConnectionProfile)>,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Record<'a> {
        backend: &'a str,
        value_bytes: usize,
        batch_size: usize,
        window: usize,
        repetition: usize,
        phase: &'a str,
        nodes: Vec<(u32, tidehunter::distributed::tcp::ConnectionProfile)>,
    }
    writeln!(output.node_profiles, "---")?;
    serde_yaml::to_writer(
        &mut output.node_profiles,
        &Record {
            backend: case.label(),
            value_bytes: case.value_bytes,
            batch_size: case.batch_size,
            window: case.window,
            repetition: case.repetition,
            phase,
            nodes,
        },
    )?;
    output.node_profiles.flush()?;
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
fn profile_native_reads(
    db: &Db,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    state: &str,
) -> Result<()> {
    for (sample, (index, generation)) in oracle
        .iter()
        .enumerate()
        .filter_map(|(i, g)| g.map(|g| (i, g)))
        .take(count.min(256))
        .enumerate()
    {
        let expected = value(index, generation, case.value_bytes);
        let started = Instant::now();
        let (found, p) = db
            .get_profiled_for_benchmark(ks, &key(index))
            .map_err(native_error)?;
        let whole = ns(started);
        if found.as_ref() != Some(&expected) {
            return Err(invalid("native diagnostic value mismatch").into());
        }
        writeln!(
            output.native_read_profiles,
            "{},{},{},{},{},{},{},{},{},{},{}",
            case.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            state,
            sample,
            p.index_ns,
            p.wal_and_decode_ns,
            p.value_cache_hit,
            whole
        )?;
    }
    output.native_read_profiles.flush()?;
    Ok(())
}

// Explicit phase names keep cache state and diagnostic provenance at the call site.
#[allow(clippy::too_many_arguments)]
fn profile_remote_reads(
    db: &mut RemoteDatabase,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    state: &str,
    node_phase: Option<&str>,
) -> Result<()> {
    // Force remote work, but keep shard WAL pages warm.
    db.clear_read_cache();
    if node_phase.is_some() {
        db.connection_profiles(true, true)?;
    }
    for (sample, (index, generation)) in oracle
        .iter()
        .enumerate()
        .filter_map(|(index, generation)| generation.map(|generation| (index, generation)))
        .take(count.min(256))
        .enumerate()
    {
        let expected = value(index, generation, case.value_bytes);
        let started = Instant::now();
        let (found, profile) = db.get_profiled(ks, &key(index))?;
        let whole = ns(started);
        if found.as_ref() != Some(&expected) {
            return Err(invalid("profiled remote get verification failed").into());
        }
        writeln!(
            output.remote_read_profiles,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            case.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            state,
            sample,
            profile.index_ns,
            profile.dispatch_ns,
            profile.wait_ns,
            profile.actor_queue_ns,
            profile.actor_service_ns,
            profile.actor_return_ns,
            profile.tcp.request_encode_ns,
            profile.tcp.exchange_ns,
            profile.tcp.response_decode_ns,
            profile.tcp.record_verify_ns,
            profile.tcp.server_handler_ns,
            profile.tcp.server_inventory_wait_ns,
            profile.tcp.server_authority_lookup_ns,
            profile.tcp.server_record_read_ns,
            profile.cache_hits,
            profile.cache_misses,
            whole,
            profile.inline_read_ns,
            profile.tcp.server_response_recheck_wait_ns,
            profile.tcp.server_response_recheck_ns,
            profile.tcp.server_response_recheck_samples,
            profile.tcp.server_response_recheck_deferrals
        )?;
        write_exchange_profile(output, case, state, sample, &profile.tcp)?;
    }
    output.remote_read_profiles.flush()?;
    if let Some(phase) = node_phase {
        write_node_profiles(output, case, phase, db.connection_profiles(false, false)?)?;
    }
    Ok(())
}

struct Output {
    samples: BufWriter<File>,
    summary: BufWriter<File>,
    stages: BufWriter<File>,
    write_windows: BufWriter<File>,
    read_profiles: BufWriter<File>,
    remote_read_profiles: BufWriter<File>,
    exchange_profiles: BufWriter<File>,
    node_profiles: BufWriter<File>,
    read_io: BufWriter<File>,
    read_path_evidence: BufWriter<File>,
    native_read_profiles: BufWriter<File>,
    read_resources: Option<BufWriter<File>>,
}
impl Output {
    fn new(path: &Path, resource_evidence: bool) -> Result<Self> {
        fs::create_dir_all(path.parent().unwrap_or(Path::new(".")))?;
        fs::create_dir(path)?;
        let create = |name| {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(path.join(name))
                .map(BufWriter::new)
        };
        let mut output = Self {
            samples: create("samples.csv")?,
            summary: create("summary.csv")?,
            stages: create("stages.csv")?,
            write_windows: create("write_windows.csv")?,
            read_profiles: create("read_profiles.csv")?,
            remote_read_profiles: create("remote_read_profiles.csv")?,
            exchange_profiles: create("exchange_profiles.csv")?,
            node_profiles: create("node_profiles.yaml")?,
            read_io: create("read_io.csv")?,
            read_path_evidence: create("read_path_evidence.csv")?,
            native_read_profiles: create("native_read_profiles.csv")?,
            read_resources: if resource_evidence {
                Some(create("read_resources.csv")?)
            } else {
                None
            },
        };
        writeln!(
            output.samples,
            "backend,durability,value_bytes,batch_size,window,repetition,phase,sample,latency_ns,logical_operations,value_bytes_processed"
        )?;
        writeln!(
            output.summary,
            "backend,durability,value_bytes,batch_size,window,repetition,phase,samples,logical_operations,phase_wall_ns,measured_api_ns,mean_ns,p50_ns,p95_ns,p99_ns,max_ns,instrumented_phase_ops_per_sec,transport,sample_unit,rate_scope"
        )?;
        writeln!(
            output.stages,
            "backend,value_bytes,batch_size,window,repetition,phase,counter,delta"
        )?;
        writeln!(
            output.write_windows,
            "backend,transport,value_bytes,batch_size,window,repetition,phase,sample,api_ns,preparation_admission_ns,storage_wait_ns,publication_ns,dispatch_ns,checkpoint_ns,checkpoint_count,foreground_checkpoint_ns,foreground_checkpoint_count"
        )?;
        writeln!(
            output.read_profiles,
            "backend,value_bytes,batch_size,window,repetition,state,sample,index_ns,membership_ns,wal_range_ns,verification_decode_ns,whole_call_ns"
        )?;
        writeln!(
            output.remote_read_profiles,
            "backend,value_bytes,batch_size,window,repetition,state,sample,index_ns,dispatch_ns,wait_ns,actor_queue_ns,actor_service_ns,actor_return_ns,request_encode_ns,exchange_ns,response_decode_ns,record_verify_ns,server_handler_ns,server_inventory_wait_ns,server_authority_lookup_ns,server_record_read_ns,cache_hits,cache_misses,whole_call_ns,inline_read_ns,server_response_recheck_wait_ns,server_response_recheck_ns,server_response_recheck_samples,server_response_recheck_deferrals"
        )?;
        writeln!(
            output.exchange_profiles,
            "backend,value_bytes,batch_size,window,repetition,state,sample,request_send_ns,response_header_ns,response_body_ns,socket_read_ns,socket_write_ns,timeout_setup_ns,socket_reads,socket_writes,timeout_sets,socket_read_bytes,socket_write_bytes,fabric_drain_ns,fabric_drains"
        )?;
        writeln!(
            output.read_io,
            "backend,value_bytes,batch_size,window,repetition,phase,partition,pid,physical_read_bytes,minor_faults,major_faults,file_bytes,resident_bytes_before,resident_bytes_after"
        )?;
        writeln!(
            output.read_path_evidence,
            "backend,value_bytes,batch_size,window,repetition,phase,boundary,role,partition,cache_budget_bytes,cache_retained_bytes,cache_metadata_bytes,cache_entries,cache_hits,cache_misses,cache_evictions,wal_reads,wal_read_bytes,inflight_requests,inflight_bytes,inflight_reads,peak_inflight_requests,peak_inflight_bytes,peak_inflight_reads,peak_device_reads,cache_bypass_disabled,cache_bypass_eviction_limit,cache_bypass_scan,cache_bypass_generation,cache_admitted_charge_bytes,cache_served_accounted_bytes,idle_buffer_bytes,idle_buffers,buffer_reuses,buffer_allocations,resident_read_attempts,resident_read_successes,resident_read_fallbacks,resident_read_unsupported,async_read_submissions,resident_read_partial"
        )?;
        writeln!(
            output.native_read_profiles,
            "backend,value_bytes,batch_size,window,repetition,state,sample,index_ns,wal_and_decode_ns,value_cache_hit,whole_call_ns"
        )?;
        if let Some(resources) = &mut output.read_resources {
            writeln!(
                resources,
                "stage,role,partition,pid,rss_bytes,peak_rss_bytes,user_cpu_ns,system_cpu_ns,native_registered_value_wal_files"
            )?;
        }
        Ok(output)
    }
    fn phase(&mut self, case: &Case, phase: &str, elapsed: u64, samples: &[Sample]) -> Result<()> {
        if samples.is_empty() {
            return Ok(());
        }
        let mut durations: Vec<_> = samples.iter().map(|s| s.elapsed_ns).collect();
        durations.sort_unstable();
        let quantile =
            |pct: usize| durations[(durations.len() * pct).div_ceil(100).saturating_sub(1)];
        let operations: usize = samples.iter().map(|s| s.operations).sum();
        let total_ns: u64 = samples.iter().map(|s| s.elapsed_ns).sum();
        let rate = operations as f64 * 1e9 / elapsed.max(1) as f64;
        let sample_unit = if phase.ends_with("_get_window") {
            "operation"
        } else if phase.ends_with("_window") {
            "complete_window"
        } else if phase.contains("get_many") {
            "complete_get_many"
        } else if samples.len() == 1 {
            "whole_phase"
        } else {
            "operation"
        };
        let rate_scope = if phase.starts_with("mixed_") || phase.starts_with("concurrent_mixed_") {
            "shared_read_write_wall"
        } else {
            "phase_wall"
        };
        for (index, sample) in samples.iter().enumerate() {
            writeln!(
                self.samples,
                "{},{},{},{},{},{},{},{},{},{},{}",
                case.label(),
                case.kind.durability(),
                case.value_bytes,
                case.batch_size,
                case.window,
                case.repetition,
                phase,
                index,
                sample.elapsed_ns,
                sample.operations,
                sample.bytes
            )?;
        }
        write!(
            self.summary,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},",
            case.label(),
            case.kind.durability(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            phase,
            samples.len(),
            operations,
            elapsed,
            total_ns,
            total_ns as f64 / samples.len() as f64,
            quantile(50),
        )?;
        // Do not emit a one-sample lifecycle p99 or a ten-window maximum as p99.
        if samples.len() >= 20 {
            write!(self.summary, "{}", quantile(95))?;
        }
        write!(self.summary, ",")?;
        if samples.len() >= 100 {
            write!(self.summary, "{}", quantile(99))?;
        }
        writeln!(
            self.summary,
            ",{},{},{},{},{}",
            durations[durations.len() - 1],
            rate,
            case.transport.label(),
            sample_unit,
            rate_scope,
        )?;
        self.samples.flush()?;
        self.summary.flush()?;
        Ok(())
    }
    fn stages(
        &mut self,
        case: &Case,
        phase: &str,
        before: BTreeMap<&'static str, u64>,
        after: BTreeMap<&'static str, u64>,
    ) -> Result<()> {
        for (name, value) in after {
            writeln!(
                self.stages,
                "{},{},{},{},{},{},{},{}",
                case.label(),
                case.value_bytes,
                case.batch_size,
                case.window,
                case.repetition,
                phase,
                name,
                value.saturating_sub(*before.get(name).unwrap_or(&0))
            )?;
        }
        self.stages.flush()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)] // Each row binds one timed window and its two local snapshots.
    fn write_window(
        &mut self,
        case: &Case,
        phase: &str,
        sample: usize,
        elapsed_ns: u64,
        before: tidehunter::distributed::remote_db::AuthorityWriteStats,
        after: tidehunter::distributed::remote_db::AuthorityWriteStats,
    ) -> Result<()> {
        writeln!(
            self.write_windows,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            case.label(),
            case.transport.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            phase,
            sample,
            elapsed_ns,
            after
                .preparation_admission_ns
                .saturating_sub(before.preparation_admission_ns),
            after.storage_wait_ns.saturating_sub(before.storage_wait_ns),
            after.publication_ns.saturating_sub(before.publication_ns),
            after.dispatch_ns.saturating_sub(before.dispatch_ns),
            after.checkpoint_ns.saturating_sub(before.checkpoint_ns),
            after
                .checkpoint_count
                .saturating_sub(before.checkpoint_count),
            after
                .foreground_checkpoint_ns
                .saturating_sub(before.foreground_checkpoint_ns),
            after
                .foreground_checkpoint_count
                .saturating_sub(before.foreground_checkpoint_count),
        )?;
        Ok(())
    }

    // Snapshot outside timers. WAL submissions are not physical-device IOPS;
    // read_campaign separately requires operating-system physical-read evidence.
    fn read_path_evidence(
        &mut self,
        case: &Case,
        backend: &Backend,
        phase: &str,
        boundary: &str,
    ) -> Result<()> {
        let Backend::Tcp(db) = backend else {
            return Ok(());
        };
        let cache = db.read_cache_stats();
        write!(
            self.read_path_evidence,
            "{},{},{},{},{},{},{},frontend,,{},{},{},{},{},{},{},0,0,0,0,0,0,0,0,0",
            case.label(),
            case.value_bytes,
            case.batch_size,
            case.window,
            case.repetition,
            phase,
            boundary,
            cache.budget_bytes,
            cache.retained_bytes,
            cache.metadata_bytes,
            cache.entries,
            cache.hits,
            cache.misses,
            cache.evictions,
        )?;
        writeln!(
            self.read_path_evidence,
            ",{},{},0,0,{},{},0,0,0,0,0,0,0,0,0,0",
            cache.bypass_disabled,
            cache.bypass_eviction_limit,
            cache.admitted_bytes,
            cache.served_bytes,
        )?;
        for (partition, stats) in db.read_io_stats()? {
            write!(
                self.read_path_evidence,
                "{},{},{},{},{},{},{},storage,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                case.label(),
                case.value_bytes,
                case.batch_size,
                case.window,
                case.repetition,
                phase,
                boundary,
                partition,
                stats.cache_budget,
                stats.cache_bytes,
                stats.cache_metadata_bytes,
                stats.cache_entries,
                stats.cache_hits,
                stats.cache_misses,
                stats.cache_evictions,
                stats.wal_reads,
                stats.wal_read_bytes,
                stats.inflight_requests,
                stats.inflight_bytes,
                stats.inflight_reads,
                stats.peak_inflight_requests,
                stats.peak_inflight_bytes,
                stats.peak_inflight_reads,
                stats.peak_device_reads,
            )?;
            writeln!(
                self.read_path_evidence,
                ",{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                stats.cache_bypass_disabled,
                stats.cache_bypass_eviction_limit,
                stats.cache_bypass_scan,
                stats.cache_bypass_generation,
                stats.cache_admitted_bytes,
                stats.cache_served_bytes,
                stats.idle_buffer_bytes,
                stats.idle_buffers,
                stats.buffer_reuses,
                stats.buffer_allocations,
                stats.resident_read_attempts,
                stats.resident_read_successes,
                stats.resident_read_fallbacks,
                stats.resident_read_unsupported,
                stats.async_read_submissions,
                stats.resident_read_partial,
            )?;
        }
        self.read_path_evidence.flush()?;
        Ok(())
    }

    fn recovery_evidence(&mut self, case: &Case, backend: &Backend, phase: &str) -> Result<()> {
        let Backend::Tcp(db) = backend else {
            return Ok(());
        };
        let stats = db.recovery_stats();
        self.stages(
            case,
            phase,
            BTreeMap::new(),
            BTreeMap::from([
                ("checkpoint_loaded", u64::from(stats.checkpoint_loaded)),
                ("checkpoint_load_ns", stats.checkpoint_load_ns),
                ("proof_validation_ns", stats.proof_validation_ns),
                ("tail_replay_ns", stats.tail_replay_ns),
                ("checkpoint_commit_ns", stats.checkpoint_commit_ns),
                ("connection_setup_ns", stats.connection_setup_ns),
                ("tls_config_ns", stats.tls_config_ns),
                ("transport_checkout_ns", stats.transport_checkout_ns),
                ("read_lane_setup_ns", stats.read_lane_setup_ns),
                ("read_lane_wait_ns", stats.read_lane_wait_ns),
                ("created_connections", stats.created_connections),
                ("reused_connections", stats.reused_connections),
                ("connect_worker_elapsed_sum_ns", stats.connect_worker_ns),
                ("status_worker_elapsed_sum_ns", stats.status_worker_ns),
                ("tls_worker_elapsed_sum_ns", stats.tls_worker_ns),
                ("fencing_ns", stats.fencing_ns),
                ("tail_batches", stats.tail_batches),
                ("tail_bytes", stats.tail_bytes),
            ]),
        )?;
        if let Some(elapsed) = stats.fabric_worker_ns {
            self.stages(
                case,
                phase,
                BTreeMap::new(),
                BTreeMap::from([("fabric_worker_elapsed_sum_ns", elapsed)]),
            )?;
        }
        #[cfg(feature = "experimental-fabric")]
        if let Some(profile) = stats.fabric_open_worker {
            self.stages(
                case,
                phase,
                BTreeMap::new(),
                BTreeMap::from(fabric_profile::counters(profile)),
            )?;
        }
        Ok(())
    }
}
struct Case {
    transport: Transport,
    kind: Kind,
    value_bytes: usize,
    batch_size: usize,
    window: usize,
    repetition: usize,
    preparation_chunk_batches: usize,
}

impl Case {
    fn label(&self) -> &'static str {
        if self.kind == Kind::Tcp {
            match self.transport {
                Transport::Tcp => "experimental_tcp_authority",
                Transport::Efa => "experimental_efa_authority",
                Transport::EfaDirect => "experimental_efa_direct_authority",
                Transport::FabricTcp => "experimental_fabric_tcp_authority",
            }
        } else {
            self.kind.label()
        }
    }
}

fn writes(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    phase: &str,
    batches: &[PlannedBatch],
    oracle: &mut [Option<u64>],
    operation: &mut u64,
) -> Result<()> {
    let counters = backend.counters()?;
    let mut samples = Vec::with_capacity(batches.len().div_ceil(case.window));
    let mut timings = Vec::with_capacity(if matches!(backend, Backend::Tcp(_)) {
        batches.len().div_ceil(case.window)
    } else {
        0
    });
    let wall = Instant::now();
    for window in batches.chunks(case.window) {
        // Cloning immutable Bytes handles is driver preparation, outside API latency.
        let ready: Vec<_> = window
            .iter()
            .enumerate()
            .map(|(offset, batch)| (*operation + offset as u64, batch.mutations.clone()))
            .collect();
        let before = backend.authority_write_stats();
        let start = Instant::now();
        backend.submit_window(ready, case.preparation_chunk_batches)?;
        *operation += window.len() as u64;
        backend.drain()?;
        let elapsed_ns = ns(start);
        if let (Some(before), Some(after)) = (before, backend.authority_write_stats()) {
            timings.push((before, after));
        }
        samples.push(Sample {
            elapsed_ns,
            operations: window.iter().map(|b| b.mutations.len()).sum(),
            bytes: window
                .iter()
                .flat_map(|b| &b.mutations)
                .map(|m| match m {
                    Mutation::Put { value, .. } => value.len(),
                    _ => 0,
                })
                .sum(),
        });
        for batch in window {
            for &(index, generation) in &batch.changes {
                oracle[index] = generation;
            }
        }
    }
    output.phase(case, phase, ns(wall), &samples)?;
    for (index, (before, after)) in timings.into_iter().enumerate() {
        output.write_window(case, phase, index, samples[index].elapsed_ns, before, after)?;
    }
    output.write_windows.flush()?;
    output.stages(case, phase, counters, backend.counters()?)
}

// Keep the workload and evidence parameters explicit in this benchmark-only
// driver; they do not belong to a production read-path abstraction.
#[allow(clippy::too_many_arguments)]
fn reads(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    phase: &str,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    miss: bool,
    exists: bool,
) -> Result<()> {
    let counters = backend.counters()?;
    output.read_path_evidence(case, backend, phase, "before")?;
    let mut samples = Vec::with_capacity(count);
    // Prepare deterministic verification payloads outside the measured phase.
    // The command caps this at 8,192 reads / 64 MiB for 8 KiB values.
    let queries: Vec<_> = (0..count)
        .map(|n| {
            let index = if miss {
                oracle.len() + n
            } else {
                mix(n as u64) as usize % oracle.len()
            };
            let expected = if miss {
                None
            } else {
                oracle[index].map(|g| {
                    if exists {
                        Bytes::new()
                    } else {
                        value(index, g, case.value_bytes)
                    }
                })
            };
            (key(index), expected)
        })
        .collect();
    let wall = Instant::now();
    for (key, expected) in queries {
        let start = Instant::now();
        let elapsed;
        if exists {
            let found = backend.exists(ks, &key)?;
            elapsed = ns(start);
            if found != expected.is_some() {
                return Err(invalid("exists verification failed").into());
            }
        } else {
            let found = backend.get(ks, &key)?;
            elapsed = ns(start);
            if found != expected {
                return Err(invalid("get verification failed").into());
            }
        }
        samples.push(Sample {
            elapsed_ns: elapsed,
            operations: 1,
            bytes: if exists {
                0
            } else {
                expected.map_or(0, |v| v.len())
            },
        });
    }
    output.phase(case, phase, ns(wall), &samples)?;
    output.read_path_evidence(case, backend, phase, "after")?;
    output.stages(case, phase, counters, backend.counters()?)
}

fn single(
    output: &mut Output,
    case: &Case,
    phase: &str,
    elapsed: u64,
    operations: usize,
    bytes: usize,
) -> Result<()> {
    output.phase(
        case,
        phase,
        elapsed,
        &[Sample {
            elapsed_ns: elapsed,
            operations,
            bytes,
        }],
    )
}

fn batched_reads(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    phase: &str,
) -> Result<()> {
    grouped_reads(backend, case, output, ks, oracle, count, phase, 32)
}

#[allow(clippy::too_many_arguments)]
fn grouped_reads(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    phase: &str,
    window: usize,
) -> Result<()> {
    let queries: Vec<_> = (0..count)
        .map(|n| {
            let index = mix(n as u64) as usize % oracle.len();
            (
                key(index).to_vec(),
                oracle[index].map(|g| value(index, g, case.value_bytes)),
            )
        })
        .collect();
    let counters = backend.counters()?;
    output.read_path_evidence(case, backend, phase, "before")?;
    let mut samples = Vec::with_capacity(count.div_ceil(window));
    let wall = Instant::now();
    for group in queries.chunks(window) {
        let keys: Vec<_> = group.iter().map(|(key, _)| key.clone()).collect();
        let start = Instant::now();
        let found = backend.get_many(ks, &keys)?;
        let elapsed_ns = ns(start);
        if found.len() != group.len()
            || found
                .iter()
                .zip(group)
                .any(|(value, (_, expected))| value != expected)
        {
            return Err(invalid("get_many verification failed").into());
        }
        samples.push(Sample {
            elapsed_ns,
            operations: group.len(),
            bytes: found.iter().flatten().map(|value| value.len()).sum(),
        });
    }
    output.phase(case, phase, ns(wall), &samples)?;
    output.read_path_evidence(case, backend, phase, "after")?;
    output.stages(case, phase, counters, backend.counters()?)
}

#[allow(clippy::too_many_arguments)]
fn windowed_reads(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    count: usize,
    phase: &str,
    window: usize,
    native_workers: &mut Option<native_reads::NativeReadWorkers>,
) -> Result<()> {
    let counters = backend.counters()?;
    output.read_path_evidence(case, backend, phase, "before")?;
    let queries: Vec<_> = (0..count)
        .map(|n| {
            let index = mix(n as u64) as usize % oracle.len();
            (
                key(index),
                oracle[index].map(|g| value(index, g, case.value_bytes)),
            )
        })
        .collect();
    let mut samples = Vec::with_capacity(count);
    let mut record = |found: Option<Bytes>, index: usize, start: Instant| -> Result<()> {
        let elapsed_ns = ns(start);
        if found != queries[index].1 {
            return Err(invalid("split-phase window verification failed").into());
        }
        samples.push(Sample {
            elapsed_ns,
            operations: 1,
            bytes: found.map_or(0, |v| v.len()),
        });
        Ok(())
    };
    let native_pending = std::collections::VecDeque::with_capacity(
        native_workers.as_ref().map_or(0, |workers| workers.len()),
    );
    let wall = Instant::now();
    if let Backend::Tcp(db) = backend {
        let mut pending = std::collections::VecDeque::new();
        for (index, (key, _)) in queries.iter().enumerate() {
            let start = Instant::now();
            let mut handle = loop {
                match db.submit_get(ks, key) {
                    Ok(handle) => break handle,
                    Err(error)
                        if RemoteDatabase::read_backpressured(&error) && !pending.is_empty() =>
                    {
                        let (handle, index, start) = pending.pop_front().unwrap();
                        record(db.finish_get(handle)?, index, start)?;
                    }
                    Err(error) => return Err(error.into()),
                }
            };
            if let Some(found) = db.poll_get(&mut handle)? {
                record(found, index, start)?;
            } else {
                pending.push_back((handle, index, start));
            }
            if pending.len() == window {
                let (handle, index, start) = pending.pop_front().unwrap();
                record(db.finish_get(handle)?, index, start)?;
            }
        }
        for (handle, index, start) in pending {
            record(db.finish_get(handle)?, index, start)?;
        }
    } else if let Some(workers) = native_workers {
        let mut pending = native_pending;
        for (index, (key, _)) in queries.iter().enumerate() {
            let slot = index % workers.len();
            if pending.len() == workers.len() {
                let (previous_slot, index, start) = pending.pop_front().unwrap();
                record(workers.finish(previous_slot)?, index, start)?;
            }
            let start = Instant::now();
            workers.submit(slot, ks, *key)?;
            pending.push_back((slot, index, start));
        }
        for (slot, index, start) in pending {
            record(workers.finish(slot)?, index, start)?;
        }
    } else {
        // Default native window and the scalar/get_many controls stay serial.
        for (index, (key, _)) in queries.iter().enumerate() {
            let start = Instant::now();
            record(backend.get(ks, key)?, index, start)?;
        }
    }
    let elapsed = ns(wall);
    output.phase(case, phase, elapsed, &samples)?;
    output.read_path_evidence(case, backend, phase, "after")?;
    output.stages(case, phase, counters, backend.counters()?)?;
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(backend, output, phase)?;
    Ok(())
}

fn audit_reopen(
    backend: &mut Backend,
    case: &Case,
    output: &mut Output,
    ks: KeySpace,
    oracle: &[Option<u64>],
    phase: &str,
) -> Result<()> {
    let wall = Instant::now();
    let mut samples = Vec::with_capacity(oracle.len());
    for (index, generation) in oracle.iter().enumerate() {
        let expected = generation.map(|g| value(index, g, case.value_bytes));
        let k = key(index);
        let start = Instant::now();
        let found = backend.get(ks, &k)?;
        let elapsed = ns(start);
        if found != expected {
            return Err(invalid("reopen audit failed").into());
        }
        samples.push(Sample {
            elapsed_ns: elapsed,
            operations: 1,
            bytes: expected.map_or(0, |v| v.len()),
        });
    }
    output.phase(case, phase, ns(wall), &samples)
}

fn run_case(args: &Args, case: &Case, output: &mut Output) -> Result<()> {
    // Keep data on the caller-selected results filesystem; /tmp may be tmpfs.
    let temporary = tempdir::TempDir::new_in(&args.output, "database")?;
    let mut parameters = BackendParameters::new(
        case.kind,
        temporary.path().join("database"),
        args,
        case.value_bytes,
    );
    let mut cluster = None;
    if case.kind == Kind::Tcp {
        if let Some(config) = &args.remote_config {
            let remote = serde_yaml::from_slice(&fs::read(config)?)?;
            validate_remote_wal_layout(args.wal_layout, &remote)?;
            parameters.remote = Some(remote);
        } else {
            let evidence = args.output.join(format!(
                "tcp-v{}-b{}-w{}-r{}",
                case.value_bytes, case.batch_size, case.window, case.repetition
            ));
            let start = Instant::now();
            let owned = tcp_cluster::LoopbackCluster::start_with_transport(
                &temporary.path().join("nodes"),
                &evidence,
                &parameters.shape,
                parameters.local.storage.clone(),
                parameters.local.max_admitted_batches,
                parameters.local.max_index_bytes,
                None,
                args.transport,
                args.tcp_server_read_workers.unwrap_or(2),
                args.tcp_wal_member_read
                    .unwrap_or(tidehunter::distributed::storage::WalMemberReadPolicy::Async),
                args.tcp_shard_cache_bytes.unwrap_or(0),
                args.shard_read_memory_allowance_bytes.unwrap_or(512 * MIB),
                args.tcp_polling,
                args.tcp_resident_first,
            )?;
            parameters.remote = Some(owned.config.clone());
            cluster = Some(owned);
            single(output, case, "tcp_cluster_startup", ns(start), 1, 0)?;
        }
    }
    if let Some(remote) = &mut parameters.remote {
        remote.read_memory_allowance_bytes = args.read_memory_allowance_bytes;
        if remote.tls.transport != args.transport {
            return Err(
                invalid("remote transport differs from explicit benchmark transport").into(),
            );
        }
        if let Some(bytes) = args.tcp_read_cache_bytes {
            remote.read_cache_bytes = bytes;
        }
        if let Some(enabled) = args.tcp_pipeline {
            remote.pipelined_writes = enabled;
        }
        if let Some(workers) = args.tcp_preparation_workers {
            remote.preparation_workers = workers;
        }
        if let Some(pages) = args.tcp_scan_prefetch_pages {
            remote.scan_prefetch_pages = pages;
        }
        if let Some(lanes) = args.tcp_read_lanes {
            remote.read_lanes = lanes;
        }
        if let Some(count) = args.tcp_max_pending_reads {
            remote.max_pending_reads = count;
        }
        if let Some(bytes) = args.tcp_max_pending_read_bytes {
            remote.max_pending_read_bytes = bytes;
        }
        println!(
            "coordinator_read_memory_allowance_bytes={} cache_budget_bytes={} cache_metadata_before_allocation_bytes={}",
            remote.read_memory_allowance_bytes,
            remote.read_cache_bytes,
            remote.read_cache_metadata_bytes(),
        );
    }
    let ks = parameters.shape.iter_ks().next().unwrap().id();
    let start = Instant::now();
    let mut backend = parameters.open()?;
    single(output, case, "open", ns(start), 1, 0)?;
    output.recovery_evidence(case, &backend, "open_recovery")?;
    if args.cold_read_pass
        && let Backend::Tcp(db) = &backend
        && db
            .read_io_stats()?
            .iter()
            .any(|(_, stats)| stats.cache_budget != 0)
    {
        return Err(invalid("cold reads require disabled shard application caches").into());
    }
    // Start and acknowledge every worker before opened evidence or read timers.
    // One pool owns at most min(workers, window) requests across all phases.
    let mut native_workers = match &backend {
        Backend::Native { db, .. } if args.native_read_workers > 1 || args.concurrent_mixed => {
            Some(native_reads::NativeReadWorkers::open(
                db,
                args.native_read_workers,
                args.read_window,
            )?)
        }
        _ => None,
    };
    if matches!(&backend, Backend::Native { .. }) {
        output.stages(
            case,
            "read_configuration",
            BTreeMap::new(),
            BTreeMap::from([
                ("native_read_workers", args.native_read_workers as u64),
                (
                    "native_read_effective_concurrency",
                    native_workers.as_ref().map_or(1, |workers| workers.len()) as u64,
                ),
            ]),
        )?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "opened")?;
    let key_count = args.batches * case.batch_size;
    let mut oracle = vec![None; key_count];
    let mut operation = 0u64;
    let load: Vec<_> = (0..args.batches)
        .map(|b| {
            planned(
                ks,
                b * case.batch_size,
                case.batch_size,
                Some(1),
                case.value_bytes,
                key_count,
            )
        })
        .collect();
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        db.connection_profiles(true, true)?;
    }
    writes(
        &mut backend,
        case,
        output,
        "load_write_window",
        &load,
        &mut oracle,
        &mut operation,
    )?;
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        write_node_profiles(
            output,
            case,
            "load-write-diagnostics",
            db.connection_profiles(false, false)?,
        )?;
    }
    drop(load);
    if args.cache_probe_keys != 0 {
        cache_workloads::hot_scan(
            &mut backend,
            case,
            output,
            ks,
            &oracle,
            args.cache_probe_keys,
            args.reads,
        )?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "loaded")?;
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        db.connection_profiles(true, true)?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "warm-scalar-before")?;
    reads(
        &mut backend,
        case,
        output,
        "point_get",
        ks,
        &oracle,
        args.reads,
        false,
        false,
    )?;
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        write_node_profiles(
            output,
            case,
            "warm-scalar-read-diagnostics",
            db.connection_profiles(false, false)?,
        )?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "warm-scalar-after")?;
    // Independent bounded samples; their clocks never enter the ordinary latency rows.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    if args.transport_diagnostics
        && let Backend::Native { db, .. } = &backend
    {
        profile_native_reads(db, case, output, ks, &oracle, args.reads, "warm")?;
    }
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        profile_remote_reads(
            db,
            case,
            output,
            ks,
            &oracle,
            args.reads,
            "warm",
            Some("warm-profiled-read-diagnostics"),
        )?;
    }
    if args.hot_read_pass {
        reads(
            &mut backend,
            case,
            output,
            "point_get_hot",
            ks,
            &oracle,
            args.reads,
            false,
            false,
        )?;
    }
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        db.connection_profiles(true, true)?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "warm-window-before")?;
    windowed_reads(
        &mut backend,
        case,
        output,
        ks,
        &oracle,
        args.reads,
        "point_get_window",
        args.read_window,
        &mut native_workers,
    )?;
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        write_node_profiles(
            output,
            case,
            "warm-window-read-diagnostics",
            db.connection_profiles(false, false)?,
        )?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "warm-window-after")?;

    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        db.connection_profiles(true, true)?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "warm-batched-before")?;
    batched_reads(
        &mut backend,
        case,
        output,
        ks,
        &oracle,
        args.reads,
        "point_get_many",
    )?;
    if args.transport_diagnostics
        && let Backend::Tcp(db) = &mut backend
    {
        write_node_profiles(
            output,
            case,
            "warm-batched-read-diagnostics",
            db.connection_profiles(false, false)?,
        )?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    read_campaign::resources(&mut backend, output, "warm-batched-after")?;
    if args.hot_read_pass {
        batched_reads(
            &mut backend,
            case,
            output,
            ks,
            &oracle,
            args.reads,
            "point_get_many_hot",
        )?;
    }
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    if args.cold_read_pass {
        read_campaign::run(
            &mut backend,
            case,
            output,
            ks,
            &oracle,
            args.reads,
            args.read_window,
            &mut native_workers,
            args.transport_diagnostics,
        )?;
    }
    reads(
        &mut backend,
        case,
        output,
        "point_get_missing",
        ks,
        &oracle,
        args.reads,
        true,
        false,
    )?;
    reads(
        &mut backend,
        case,
        output,
        "exists",
        ks,
        &oracle,
        args.reads,
        false,
        true,
    )?;
    reads(
        &mut backend,
        case,
        output,
        "exists_missing",
        ks,
        &oracle,
        args.reads,
        true,
        true,
    )?;

    // Read one batch's keys before each update batch: equal logical read/write counts.
    let counters = backend.counters()?;
    let mixed: Vec<_> = (0..args.mixed)
        .map(|b| {
            planned(
                ks,
                b * case.batch_size,
                case.batch_size,
                Some(2),
                case.value_bytes,
                key_count,
            )
        })
        .collect();
    if args.concurrent_mixed {
        cache_workloads::concurrent_mixed(
            &mut backend,
            case,
            output,
            ks,
            &mut oracle,
            &mut operation,
            &mixed,
            args.read_window,
            &mut native_workers,
        )?;
    } else {
        let mixed_expected: Vec<Vec<_>> = mixed
            .iter()
            .map(|batch| {
                batch
                    .changes
                    .iter()
                    .map(|&(index, _)| oracle[index].map(|g| value(index, g, case.value_bytes)))
                    .collect()
            })
            .collect();
        let mut write_timings = Vec::with_capacity(if matches!(backend, Backend::Tcp(_)) {
            mixed.len().div_ceil(case.window)
        } else {
            0
        });
        let wall = Instant::now();
        let mut read_samples = Vec::new();
        let mut write_samples = Vec::new();
        for (window_index, window) in mixed.chunks(case.window).enumerate() {
            for (batch_index, batch) in window.iter().enumerate() {
                for (operation_index, &(index, _)) in batch.changes.iter().enumerate() {
                    let expected =
                        &mixed_expected[window_index * case.window + batch_index][operation_index];
                    let k = key(index);
                    let start = Instant::now();
                    let found = backend.get(ks, &k)?;
                    let elapsed = ns(start);
                    if &found != expected {
                        return Err(invalid("mixed read verification failed").into());
                    }
                    read_samples.push(Sample {
                        elapsed_ns: elapsed,
                        operations: 1,
                        bytes: case.value_bytes,
                    });
                }
            }
            let ready: Vec<_> = window.iter().map(|b| b.mutations.clone()).collect();
            let before = backend.authority_write_stats();
            let start = Instant::now();
            let ready = ready
                .into_iter()
                .enumerate()
                .map(|(offset, batch)| (operation + offset as u64, batch))
                .collect();
            backend.submit_window(ready, case.preparation_chunk_batches)?;
            operation += window.len() as u64;
            backend.drain()?;
            let elapsed_ns = ns(start);
            if let (Some(before), Some(after)) = (before, backend.authority_write_stats()) {
                write_timings.push((before, after));
            }
            write_samples.push(Sample {
                elapsed_ns,
                operations: window.len() * case.batch_size,
                bytes: window.len() * case.batch_size * case.value_bytes,
            });
            for batch in window {
                for &(index, generation) in &batch.changes {
                    oracle[index] = generation;
                }
            }
        }
        let mixed_wall = ns(wall);
        output.phase(case, "mixed_get", mixed_wall, &read_samples)?;
        output.phase(case, "mixed_write_window", mixed_wall, &write_samples)?;
        for (index, (before, after)) in write_timings.into_iter().enumerate() {
            output.write_window(
                case,
                "mixed_write_window",
                index,
                write_samples[index].elapsed_ns,
                before,
                after,
            )?;
        }
        output.write_windows.flush()?;
        output.stages(case, "mixed", counters, backend.counters()?)?;
    }
    drop(mixed);

    let start = Instant::now();
    let checkpoint = backend.checkpoint()?;
    single(output, case, "checkpoint_capture", ns(start), 1, 0)?;
    let old = oracle[0].map(|g| value(0, g, case.value_bytes));
    let change = planned(ks, 0, 1, Some(3), case.value_bytes, key_count);
    writes(
        &mut backend,
        case,
        output,
        "checkpoint_intervening_write",
        &[change],
        &mut oracle,
        &mut operation,
    )?;
    let start = Instant::now();
    let found = backend.get_at(ks, &key(0), &checkpoint)?;
    let elapsed = ns(start);
    if found != old {
        return Err(invalid("checkpoint value verification failed").into());
    }
    single(output, case, "checkpoint_get", elapsed, 1, case.value_bytes)?;
    drop(checkpoint);

    let deletes: Vec<_> = (0..args.mixed)
        .map(|b| {
            planned(
                ks,
                b * case.batch_size,
                case.batch_size,
                None,
                case.value_bytes,
                key_count,
            )
        })
        .collect();
    writes(
        &mut backend,
        case,
        output,
        "delete_window",
        &deletes,
        &mut oracle,
        &mut operation,
    )?;
    let start = Instant::now();
    let scanned = backend.scan(ks)?;
    let elapsed = ns(start);
    let mut expected: Vec<_> = oracle
        .iter()
        .enumerate()
        .filter_map(|(i, g)| g.map(|g| (key(i).to_vec(), value(i, g, case.value_bytes))))
        .collect();
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    if scanned.len() != expected.len()
        || scanned
            .iter()
            .zip(&expected)
            .any(|((k, v), (ek, ev))| k.as_ref() != ek.as_slice() || v != ev)
    {
        return Err(invalid("ordered scan verification failed").into());
    }
    single(
        output,
        case,
        "full_scan",
        elapsed,
        scanned.len(),
        scanned.len() * case.value_bytes,
    )?;
    drop(scanned);
    drop(expected);

    let start = Instant::now();
    backend.flush_index()?;
    single(output, case, "maintenance_flush_index", ns(start), 1, 0)?;
    // LocalDatabase::flush_index already writes the image and unloads its cells.
    // Calling unload_index again would create another snapshot of the same data.
    reads(
        &mut backend,
        case,
        output,
        "post_flush_get",
        ks,
        &oracle,
        args.reads,
        false,
        false,
    )?;
    // Release shared Db references and join outside shutdown/reopen timers.
    drop(native_workers);
    let start = Instant::now();
    let shutdown = backend.close()?;
    single(output, case, "shutdown", ns(start), 1, 0)?;
    if let Some(stats) = shutdown {
        output.stages(
            case,
            "shutdown",
            BTreeMap::new(),
            BTreeMap::from([
                ("drain_ns", stats.drain_ns),
                ("checkpoint_ns", stats.checkpoint_ns),
                ("seal_ns", stats.seal_ns),
                ("join_ns", stats.join_ns),
            ]),
        )?;
    }
    // Preserve the historical fresh-transport reopen measurement. The warm
    // process-owned transport lifecycle is reported as a distinct phase below.
    if let Some(pool) = &parameters.transports {
        pool.clear();
    }
    let start = Instant::now();
    let mut backend = parameters.open()?;
    single(output, case, "reopen", ns(start), 1, 0)?;
    output.recovery_evidence(case, &backend, "reopen_recovery")?;
    // Every expected key, including tombstones, is audited after reopening.
    audit_reopen(&mut backend, case, output, ks, &oracle, "reopen_get_audit")?;
    // This diagnostic pass is outside every main timed phase. Extra substage
    // clocks must not be confused with the normal warm scalar-get measurement.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    if let Backend::Native { db, .. } = &backend {
        profile_native_reads(db, case, output, ks, &oracle, args.reads, "reopened-warm")?;
    }
    if let Backend::Sharded(db) = &mut backend {
        for (sample, (index, generation)) in oracle
            .iter()
            .enumerate()
            .filter_map(|(index, generation)| generation.map(|generation| (index, generation)))
            .take(args.reads.min(256))
            .enumerate()
        {
            let expected = value(index, generation, case.value_bytes);
            let started = Instant::now();
            let (found, profile) = db.get_profiled(ks, &key(index))?;
            let whole = ns(started);
            if found.as_ref() != Some(&expected) {
                return Err(invalid("profiled get verification failed").into());
            }
            writeln!(
                output.read_profiles,
                "{},{},{},{},{},reopened,{},{},{},{},{},{}",
                case.label(),
                case.value_bytes,
                case.batch_size,
                case.window,
                case.repetition,
                sample,
                profile.index_ns,
                profile.record.membership_ns,
                profile.record.wal_range_ns,
                profile.record.verification_decode_ns,
                whole
            )?;
        }
        output.read_profiles.flush()?;
    }
    if let Backend::Tcp(db) = &mut backend {
        profile_remote_reads(
            db,
            case,
            output,
            ks,
            &oracle,
            args.reads,
            "reopened-cache-cleared",
            args.transport_diagnostics
                .then_some("reopened-read-diagnostics"),
        )?;
    }
    let _ = backend.close()?;
    if case.kind == Kind::Tcp {
        let started = Instant::now();
        let mut backend = parameters.open()?;
        single(output, case, "reopen_reused_transport", ns(started), 1, 0)?;
        output.recovery_evidence(case, &backend, "reopen_reused_transport_recovery")?;
        audit_reopen(
            &mut backend,
            case,
            output,
            ks,
            &oracle,
            "reopen_reused_get_audit",
        )?;
        backend.close()?;
    }
    if let Some(pool) = &parameters.transports {
        pool.clear();
    }
    if let Some(cluster) = &mut cluster {
        let start = Instant::now();
        cluster.shutdown()?;
        single(output, case, "tcp_cluster_shutdown", ns(start), 1, 0)?;
    }
    println!(
        "verified backend={} value_bytes={} batch_size={} window={} repetition={} keys={} all_reopened_keys_verified=true",
        case.label(),
        case.value_bytes,
        case.batch_size,
        case.window,
        case.repetition,
        key_count
    );
    Ok(())
}

fn main() -> Result<()> {
    if tcp_cluster::maybe_run_storage_node()? {
        return Ok(());
    }
    let args = Args::parse()?;
    if let Some(directory) = &args.prepare_external_case {
        let parameters =
            BackendParameters::new(Kind::Tcp, directory.join("database"), &args, args.values[0]);
        tcp_cluster::prepare_external_case(
            directory,
            args.guest_case_root.as_ref().unwrap(),
            &args.remote_endpoints,
            &parameters.shape,
            parameters.local.storage,
            parameters.local.max_admitted_batches,
            parameters.local.max_index_bytes,
            args.transport,
            args.tcp_server_read_workers.unwrap_or(2),
            args.tcp_wal_member_read
                .unwrap_or(tidehunter::distributed::storage::WalMemberReadPolicy::Async),
            args.tcp_shard_cache_bytes.unwrap_or(0),
            args.shard_read_memory_allowance_bytes.unwrap_or(512 * MIB),
            args.tcp_polling,
            args.tcp_resident_first,
        )?;
        // Freeze the requested experimental controls into the generated remote
        // config as well as applying them when an existing config is executed.
        let config_path = directory.join("remote.yaml");
        let mut config: RemoteDatabaseConfig = serde_yaml::from_slice(&fs::read(&config_path)?)?;
        config.read_memory_allowance_bytes = args.read_memory_allowance_bytes;
        if let Some(lanes) = args.tcp_read_lanes {
            config.read_lanes = lanes;
        }
        if let Some(count) = args.tcp_max_pending_reads {
            config.max_pending_reads = count;
        }
        if let Some(bytes) = args.tcp_max_pending_read_bytes {
            config.max_pending_read_bytes = bytes;
        }
        if let Some(bytes) = args.tcp_read_cache_bytes {
            config.read_cache_bytes = bytes;
        }
        if let Some(enabled) = args.tcp_pipeline {
            config.pipelined_writes = enabled;
        }
        if let Some(workers) = args.tcp_preparation_workers {
            config.preparation_workers = workers;
        }
        if let Some(pages) = args.tcp_scan_prefetch_pages {
            config.scan_prefetch_pages = pages;
        }
        fs::write(config_path, serde_yaml::to_string(&config)?)?;
        println!(
            "prepared_external_case={} no_network_requests=true",
            directory.display()
        );
        return Ok(());
    }
    let (geometry, layout_name, layout_source) = campaign_wal_geometry(&args)?;
    let mut output = Output::new(&args.output, args.read_resource_evidence)?;
    let mut metadata = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(args.output.join("campaign.txt"))?;
    writeln!(
        metadata,
        "tcp_preparation_chunk_batches={}\nwrite_window_preflight=per_preparation_chunk\nwrite_window_durability=all_batches_before_return\nwrite_window_outstanding_bound=unchanged",
        args.tcp_preparation_chunk_batches
    )?;
    writeln!(
        metadata,
        "wal_layout={}\nwal_layout_source={}\nwal_layout_preset_explicit={}\nwal_layout_scope=wal_geometry_only_not_full_sui_configuration\nwal_fragment_bytes={}\nwal_file_bytes={}\nwal_group_max_batches={}\nwal_group_encoded_bytes={}",
        layout_name,
        layout_source,
        args.wal_layout.is_some(),
        geometry.fragment_bytes,
        geometry.wal_file_bytes,
        geometry.group_max_batches,
        geometry.group_encoded_bytes,
    )?;
    writeln!(
        metadata,
        "cold_read_pass={}\ntransport_diagnostics={}\ncold_mode=per-case-WAL-advisory-eviction-with-physical-read-evidence\ncold_mode_is_not_capacity_scaling=true\ndiagnostic_node_receive_time_includes_idle=true",
        args.cold_read_pass, args.transport_diagnostics
    )?;
    writeln!(
        metadata,
        "diagnostic_timing_schema=6\nstage_times_are_nested_not_additive=true\ntransport_io_time_includes_provider_work=true\nstorage_persist_time_includes_file_lookup_and_sync=true\nresource_deltas_include_control_requests=true\nwarm_diagnostic_coordinator_cache_cleared=true"
    )?;
    writeln!(
        metadata,
        "transport={}\nrdma_hardware_requested={}\none_sided_wal_reads=false\nnative_async_is_not_durability_equivalent=true\nlatency_unit=api_call_or_complete_window\nthroughput_includes_driver_and_verification_overhead=true\nvalues={:?}\nbatch_sizes={:?}\nwindows={:?}\nload_batches={}\nmixed_batches={}\npoint_reads={}\nrepetitions={}\nmax_value_history_per_case_bytes={}\nsource_and_binary_hashes=must_be_captured_by_orchestrator",
        args.transport.label(),
        matches!(args.transport, Transport::Efa | Transport::EfaDirect),
        args.values,
        args.batch_sizes,
        args.windows,
        args.batches,
        args.mixed,
        args.reads,
        args.repetitions,
        MAX_VALUE_HISTORY_BYTES
    )?;
    writeln!(
        metadata,
        "topology={}\nrpc={}\npoint_get_many_max_keys=32\nexternal_node_cleanup=operator_owned",
        if args.remote_config.is_some() {
            "explicit_external_tcp_nodes"
        } else {
            "one_shared_local_ssd_with_optional_five_child_nodes"
        },
        args.kinds.contains(&Kind::Tcp)
    )?;
    writeln!(
        metadata,
        "experimental_backend_id={}",
        tidehunter::distributed::local_db::BACKEND_ID
    )?;
    if args.kinds.contains(&Kind::Tcp) {
        writeln!(
            metadata,
            "tcp_backend_id={}",
            tidehunter::distributed::remote_db::BACKEND_ID
        )?;
    }
    writeln!(
        metadata,
        "tcp_shard_cache_bytes_override={:?}\ntcp_wal_member_read_override={:?}\ncache_population=on_demand_only\nshard_placement=unchanged_batch_id_routing",
        args.tcp_shard_cache_bytes, args.tcp_wal_member_read,
    )?;
    writeln!(
        metadata,
        "tcp_node_launch_polling={:?}\ntcp_node_launch_resident_first={}\nexternal_node_read_policy=owned_by_server_launch_configuration",
        args.tcp_polling, args.tcp_resident_first,
    )?;
    writeln!(
        metadata,
        "read_memory_allowance_bytes={}\nshard_read_memory_allowance_bytes={}\nread_allowance_scope=cache_and_bounded_inflight_reads_not_total_host_memory\nnative_allowance_scope=nominal_cached_values_excluding_metadata",
        args.read_memory_allowance_bytes,
        args.shard_read_memory_allowance_bytes.unwrap_or(512 * MIB),
    )?;
    writeln!(
        metadata,
        "cache_probe_keys={}\nconcurrent_mixed={}\nconcurrent_mixed_scope=disjoint_keys_read_service_overlaps_write_window_retirement_includes_caller_write_work\npercentile_minimum_samples=p95:20;p99:100\nwrite_window_parent_spans=preparation_admission_ns+storage_wait_ns+publication_ns+dispatch_ns+checkpoint_ns\nforeground_checkpoint_is_subset=true\nworker_elapsed_sums_are_not_cpu_or_wall=true\ncache_served_byte_scope=frontend_charged_detached_payload_storage_encoded_hit_length\ncache_plus_reply_accounting=conservative_shared_ownership_not_unique_RSS",
        args.cache_probe_keys, args.concurrent_mixed,
    )?;
    writeln!(
        metadata,
        "tcp_read_cache_bytes_override={:?}\ntcp_pipeline_override={:?}\ntcp_preparation_workers_override={:?}\ntcp_scan_prefetch_pages_override={:?}\ntcp_write_window_api=bounded_submit_many_chunks_when_window_gt_one\nhot_read_pass={}",
        args.tcp_read_cache_bytes,
        args.tcp_pipeline,
        args.tcp_preparation_workers,
        args.tcp_scan_prefetch_pages,
        args.hot_read_pass
    )?;
    writeln!(
        metadata,
        "native_read_cache_bytes={}\nnative_cache_accounting=nominal_value_bytes_excluding_keys_and_metadata\nnative_cache_cells={}",
        args.native_read_cache_bytes,
        INDEX_MUTEXES * CELLS_PER_MUTEX,
    )?;
    writeln!(
        metadata,
        "native_read_workers={}\nnative_read_effective_concurrency={}\nread_window={}\nread_resource_evidence={}\nnative_window_semantics=bounded_admission_to_retirement_shared_db_and_cache\nnative_scalar_and_get_many_remain_serial=true\nresource_snapshot_timing=outside_measured_timers_after_named_phase",
        args.native_read_workers,
        args.native_read_workers.min(args.read_window),
        args.read_window,
        args.read_resource_evidence,
    )?;
    for value_bytes in &args.values {
        writeln!(
            metadata,
            "native_cache_entries_per_cell_v{value_bytes}={}",
            args.native_read_cache_bytes / (INDEX_MUTEXES * CELLS_PER_MUTEX * value_bytes),
        )?;
    }
    for repetition in 0..args.repetitions {
        for &value_bytes in &args.values {
            for &batch_size in &args.batch_sizes {
                for &window in &args.windows {
                    // Rotate backend order between rounds to expose order/cache effects.
                    for offset in 0..args.kinds.len() {
                        let kind = args.kinds[(offset + repetition) % args.kinds.len()];
                        run_case(
                            &args,
                            &Case {
                                transport: args.transport,
                                preparation_chunk_batches: args.tcp_preparation_chunk_batches,
                                kind,
                                value_bytes,
                                batch_size,
                                window,
                                repetition,
                            },
                            &mut output,
                        )?;
                    }
                }
            }
        }
    }
    fs::write(
        args.output.join("COMPLETE"),
        "all selected cases and reopen audits passed\n",
    )?;
    println!("results={}", args.output.display());
    println!(
        "Verify the topology metadata: spawned nodes share one SSD; this is not Sui or RDMA evidence. External node cleanup remains operator-owned."
    );
    Ok(())
}
