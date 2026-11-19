//! Shared real-process owner harness: external case preparation and the owner
//! process entry point. All node data belongs to the caller's case directory;
//! only logs and public process/address evidence remain.

#[path = "tls_material.rs"]
mod tls_material;

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tidehunter::distributed::durability::{DurabilityConfig, DurabilityTier};
use tidehunter::distributed::remote_db::{RemoteDatabaseConfig, RemoteEndpoint};
use tidehunter::distributed::storage::{LocalInventoryConfig, PartitionConfig, StaticInventory};
use tidehunter::distributed::tcp::{
    LiveClientConfig, NodeConfig, PollingPolicy, StorageNode, TcpLimits, TlsFiles, Transport,
};
use tidehunter::key_shape::KeyShape;
use tidehunter::metrics::Metrics;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Write one fresh external namespace using the same serde types as the runtime.
/// No sockets are opened and no remote data is reset. Callers transport each
/// node's leaf key/config separately; the CA signing key never belongs on nodes.
/// Leased NVMe-oF is the only distributed route, so the coordinator config
/// always carries `live_nvme`; each owner's `live_target` is added by the caller.
/// `durability` is the config every owner launch applies.
#[allow(clippy::too_many_arguments)] // Explicit harness inputs bind the external namespace and transport.
pub fn prepare_external_case(
    directory: &Path,
    guest_root: &Path,
    addresses: &[SocketAddr],
    shape: &KeyShape,
    storage: LocalInventoryConfig,
    max_admitted_batches: usize,
    max_index_bytes: usize,
    transport: Transport,
    owner_transports: &[Transport],
    read_lanes: Option<usize>,
    wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
    polling: PollingPolicy,
    durability: DurabilityConfig,
    live_nvme: LiveClientConfig,
) -> Result<()> {
    let max_connections = RemoteDatabaseConfig::generated_owner_connection_limit(read_lanes)?;
    if !guest_root.is_absolute()
        || addresses.len() != storage.partitions.len()
        || owner_transports.len() != addresses.len()
    {
        return Err(
            io::Error::other("invalid external case root/endpoints/owner transports").into(),
        );
    }
    transport.validate()?;
    for owner_transport in owner_transports {
        owner_transport.validate()?;
        if !transport.same_protocol(owner_transport) {
            return Err(io::Error::other(
                "owner transport provider/security differs from coordinator transport",
            )
            .into());
        }
    }
    fs::create_dir(directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    fs::write(
        directory.join("owner_transports.json"),
        serde_json::to_vec_pretty(owner_transports)?,
    )?;
    let certs = if transport.is_native() {
        None
    } else {
        Some(tls_material::TestCertificates::generate(
            &directory.join("tls"),
            addresses.len(),
        )?)
    };
    let rebase = |root: &Path, path: &Path| root.join("tls").join(path.file_name().unwrap());
    let mut endpoints = Vec::new();
    for (i, ((&partition, &address), owner_transport)) in storage
        .partitions
        .iter()
        .zip(addresses)
        .zip(owner_transports)
        .enumerate()
    {
        let local_node = directory.join(format!("node-{partition}"));
        fs::create_dir(&local_node)?;
        let node_root = guest_root.join(format!("node-{partition}"));
        let (ca, cert, key, server_name) = if let Some(certs) = certs.as_ref() {
            let identity = &certs.nodes[i];
            // The prepared tree is also executable in place. Keep each owner's
            // complete identity with its launch file, never in a shared TLS bind.
            fs::create_dir(local_node.join("tls"))?;
            fs::copy(&certs.ca, rebase(&local_node, &certs.ca))?;
            fs::rename(
                &identity.certificate,
                rebase(&local_node, &identity.certificate),
            )?;
            fs::rename(
                &identity.private_key,
                rebase(&local_node, &identity.private_key),
            )?;
            (
                rebase(&node_root, &certs.ca),
                rebase(&node_root, &identity.certificate),
                rebase(&node_root, &identity.private_key),
                identity.server_name.clone(),
            )
        } else {
            (
                PathBuf::new(),
                PathBuf::new(),
                PathBuf::new(),
                format!("tidehunter-node-{partition}"),
            )
        };
        let launch = NodeLaunch {
            node: NodeConfig {
                root: node_root.join("storage"),
                partition: PartitionConfig {
                    inventory: StaticInventory {
                        database: storage.database,
                        epoch: 1,
                        segment_generation: 1,
                        partitions: storage.partitions.clone(),
                    },
                    partition,
                    batch_limits: storage.batch_limits,
                    group_limits: storage.group_limits,
                    fragment_bytes: storage.fragment_bytes,
                    wal_file_bytes: storage.wal_file_bytes,
                    max_batches: storage.max_batches_per_partition,
                    max_reference_metadata_bytes: storage.max_reference_metadata_bytes,
                },
                shape_yaml: serde_yaml::to_string(shape)?,
                max_epochs: storage.max_epochs,
                wal_member_read,
                // Legacy harness behaviour, written explicitly because the owner
                // default is on. The cloud and local harnesses rebind each case's
                // owner_wal_prewrite before validating the launch.
                wal_prewrite: false,
                read_max_device_reads: 128,
                live_target: None,
            },
            tls: TlsFiles {
                transport: owner_transport.clone(),
                ca,
                cert,
                key,
                server_name: server_name.clone(),
            },
            limits: TcpLimits {
                max_connections,
                io_timeout: Duration::from_secs(60),
                polling,
                ..TcpLimits::default()
            },
            listen: address,
            ready_file: node_root.join("ready"),
            stop_file: node_root.join("stop"),
            durability,
            launch_nonce: String::new(),
        };
        fs::write(
            local_node.join("launch.yaml"),
            serde_yaml::to_string(&launch)?,
        )?;
        endpoints.push(RemoteEndpoint {
            partition,
            address: address.to_string(),
            server_name,
        });
    }
    let client_tls = if let Some(certs) = certs.as_ref() {
        TlsFiles {
            transport,
            ca: rebase(guest_root, &certs.ca),
            cert: rebase(guest_root, &certs.authority.certificate),
            key: rebase(guest_root, &certs.authority.private_key),
            server_name: "unused-client-template".into(),
        }
    } else {
        TlsFiles {
            transport,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "unused-client-template".into(),
        }
    };
    let config = RemoteDatabaseConfig {
        storage,
        endpoints,
        tls: client_tls,
        timeout: Duration::from_secs(10),
        max_admitted_batches,
        max_index_bytes,
        max_pending_batches: 128,
        max_pending_bytes: 64 * 1024 * 1024,
        replay_limits: Default::default(),
        owner_append_occupancy: RemoteDatabaseConfig::DEFAULT_OWNER_APPEND_OCCUPANCY,
        live_nvme,
        pipelined_writes: true,
        leased_submit_redrain: true,
        read_cache_bytes: 8 * 1024 * 1024,
        read_memory_allowance_bytes: 512 * 1024 * 1024,
        scan_prefetch_pages: 2,
        read_lanes,
        pin_read_lanes: false,
        max_pending_reads: 256,
        max_pending_read_bytes: 64 * 1024 * 1024,
        write_profile_limit: 0,
        index_shards: tidehunter::distributed::native_index::DEFAULT_INDEX_SHARDS,
        durable_cut: Default::default(),
    };
    fs::write(
        directory.join("remote.yaml"),
        serde_yaml::to_string(&config)?,
    )?;
    fs::write(directory.join("shape.yaml"), serde_yaml::to_string(shape)?)?;
    // Declared tier of every owner launch file; each owner process publishes its effective one.
    fs::write(
        directory.join("durability.json"),
        serde_json::to_vec_pretty(&durability)?,
    )?;
    Ok(())
}

#[derive(Clone, Serialize, Deserialize)]
struct NodeLaunch {
    node: NodeConfig,
    tls: TlsFiles,
    limits: TcpLimits,
    listen: SocketAddr,
    ready_file: PathBuf,
    stop_file: PathBuf,
    /// Absent in older launch files: the native tier, the engine default.
    #[serde(default)]
    durability: DurabilityConfig,
    /// Fresh per start (the controller writes it); the owner echoes it in its receipt so a
    /// stale receipt is recognizable.
    #[serde(default)]
    launch_nonce: String,
}

/// Owner process output that proves the tier it applied; written before the ready file.
fn durability_file(ready_file: &Path) -> PathBuf {
    ready_file.with_extension("durability.json")
}

/// Temporary file, fsync, rename, directory fsync: a reader never sees a partial file.
fn publish_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let pending = path.with_extension("pending");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&pending)?;
        io::Write::write_all(&mut file, bytes)?;
        file.sync_all()?;
    }
    fs::rename(&pending, path)?;
    match path.parent() {
        Some(parent) => fs::File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

/// Durability fields of a receipt, all required: unlike the launch config, a receipt that omits
/// the tier (or the sync age, even when it is `null`) must never be read as the native default.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiptDurability {
    pub tier: DurabilityTier,
    #[serde(deserialize_with = "present_option")]
    pub max_unsynced_age_ms: Option<u64>,
    pub max_pending_completions: usize,
}

/// `Option` fields default to `None` when absent; going through `deserialize_with` makes the
/// field required (it may still be an explicit `null`).
fn present_option<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<u64>, D::Error> {
    <Option<u64> as Deserialize>::deserialize(deserializer)
}

impl ReceiptDurability {
    fn of(config: DurabilityConfig) -> Self {
        Self {
            tier: config.tier,
            max_unsynced_age_ms: config.max_unsynced_age_ms,
            max_pending_completions: config.max_pending_completions,
        }
    }
}

/// What one owner process publishes before its ready file. It names the owner (partition,
/// database, bound address), the exact process (pid, boot id, start time) and the launch
/// (nonce), so one receipt can neither confirm another partition nor outlive a restart.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerReceipt {
    pub partition: u32,
    pub database: [u8; 16],
    pub address: String,
    pub launch_nonce: String,
    pub pid: u32,
    pub boot_id: String,
    pub started_unix_ns: u64,
    pub durability: ReceiptDurability,
}

/// Owner receipt from `path`. A missing, unreadable or incomplete file is an error: a tier the
/// owner did not confirm cannot be reported.
pub fn read_owner_receipt(path: &Path) -> Result<OwnerReceipt> {
    let bytes = fs::read(path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("owner receipt {} unreadable: {error}", path.display()),
        )
    })?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn qualified_live_target_shutdown(
    serve_ok: bool,
    target: Option<&tidehunter::distributed::nvmeof::LiveTargetStats>,
) -> bool {
    serve_ok
        && target.is_some_and(|stats| {
            !stats.fixture_only && stats.errors == 0 && stats.owned_namespace_count == 0
        })
}

/// Must run before normal CLI parsing in either example executable.
pub fn maybe_run_storage_node() -> Result<bool> {
    let mut args = std::env::args().skip(1);
    let mode = args.next();
    if mode.as_deref() == Some("--experimental-validate-config") {
        let config = args
            .next()
            .ok_or_else(|| io::Error::other("missing coordinator config"))?;
        #[derive(Deserialize)]
        struct Deployment {
            databases: std::collections::BTreeMap<String, RemoteDatabaseConfig>,
        }
        let deployment: Deployment = serde_json::from_slice(&fs::read(config)?)?;
        if deployment.databases.is_empty() || deployment.databases.len() > 16 {
            return Err(io::Error::other("expected 1..16 coordinator databases").into());
        }
        for (namespace, database) in deployment.databases {
            database
                .validate()
                .map_err(|e| io::Error::other(format!("{namespace}: {e}")))?;
            database.tls.transport.validate()?;
        }
        let mut count = 0;
        for file in args.take(81) {
            count += 1;
            if count > 80 {
                return Err(io::Error::other("too many owner configurations").into());
            }
            let launch: NodeLaunch = serde_yaml::from_slice(&fs::read(&file)?)?;
            launch
                .node
                .validate()
                .map_err(|e| io::Error::other(format!("{file}: {e}")))?;
            launch.limits.validate()?;
            launch.tls.transport.validate()?;
        }
        if count == 0 {
            return Err(io::Error::other("missing owner configurations").into());
        }
        println!("validated coordinator and {count} owner configurations; no resources opened");
        return Ok(true);
    }
    if mode.as_deref() != Some("--experimental-storage-node") {
        return Ok(false);
    }
    let file = args
        .next()
        .ok_or_else(|| io::Error::other("missing node configuration"))?;
    if args.next().is_some() {
        return Err(io::Error::other("unexpected node arguments").into());
    }
    // If a controller is killed, its children must not remain as orphan servers.
    #[cfg(target_os = "linux")]
    unsafe {
        // SAFETY: getppid takes no arguments; this prctl option accepts only an
        // integer signal value and neither reads nor retains a user pointer.
        let parent = libc::getppid();
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
            return Err(io::Error::last_os_error().into());
        }
        if libc::getppid() != parent {
            return Err(io::Error::other("controller exited during node startup").into());
        }
    }
    let launch_bytes = fs::read(&file)?;
    let config: NodeLaunch = serde_yaml::from_slice(&launch_bytes)?;
    let live_shutdown_owner = config.node.live_target.as_ref().map(|_| {
        serde_json::json!({
            "database": config.node.partition.inventory.database,
            "partition": config.node.partition.partition,
            "root": config.node.root,
        })
    });
    let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let partition = config.node.partition.partition;
    let database = config.node.partition.inventory.database;
    let owner_events =
        if std::env::var_os("TIDEHUNTER_OWNER_EVENTS").is_some_and(|value| value == "1") {
            tidehunter::distributed::owner_events::enable(65_536);
            Some(Path::new(&file).with_extension("owner-events.json"))
        } else {
            None
        };
    let node = StorageNode::open(config.node, metrics)?;
    // Applied before the listener exists, so no client request can see another tier.
    node.set_durability(config.durability)?;
    let effective = node.durability();
    println!("durability_tier={}", effective.tier);
    let listener = TcpListener::bind(config.listen)?;
    let address = listener.local_addr()?;
    // The receipt names this exact process and launch and lands before the ready file.
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    if boot_id.is_empty() {
        return Err(io::Error::other("empty boot id").into());
    }
    let receipt = OwnerReceipt {
        partition,
        database,
        address: address.to_string(),
        launch_nonce: config.launch_nonce,
        pid: std::process::id(),
        boot_id,
        started_unix_ns: u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?,
        durability: ReceiptDurability::of(effective),
    };
    publish_synced(
        &durability_file(&config.ready_file),
        &serde_json::to_vec_pretty(&receipt)?,
    )?;
    let pending_ready = config.ready_file.with_extension("pending");
    fs::write(&pending_ready, address.to_string())?;
    fs::rename(pending_ready, &config.ready_file)?;
    println!("storage_node_pid={} address={address}", std::process::id());
    let shutdown = Arc::new(AtomicBool::new(false));
    let monitor_shutdown = shutdown.clone();
    let monitor = std::thread::spawn(move || {
        while !monitor_shutdown.load(Ordering::Acquire) {
            if config.stop_file.exists() {
                monitor_shutdown.store(true, Ordering::Release);
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    let result = node.serve(listener, config.tls, config.limits, shutdown.clone());
    shutdown.store(true, Ordering::Release);
    monitor
        .join()
        .map_err(|_| io::Error::other("node stop monitor panicked"))?;
    if let Some(owner) = live_shutdown_owner {
        use blake2::{Blake2b512, Digest};
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt;
        // This runs only after every serving session has joined. Ordinary
        // database close does not destroy the target: it must permit reopen.
        let cleanup = node.finish_live_target_shutdown();
        let target = cleanup.as_ref().ok().and_then(Option::as_ref);
        let complete = qualified_live_target_shutdown(result.is_ok(), target);
        let scope = "kernel_live_target_shutdown";
        let launch_hash: String = Blake2b512::digest(&launch_bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let process_stat = fs::read_to_string("/proc/self/stat")?;
        let process_start_ticks = process_stat
            .rsplit_once(')')
            .and_then(|(_, fields)| fields.split_whitespace().nth(19))
            .ok_or_else(|| io::Error::other("missing process start time"))?
            .parse::<u64>()?;
        let (wal_prewrite_bytes, wal_prewrite_ns, wal_prewrite_files) = node.wal_prewrite_stats();
        let proof = serde_json::json!({
            "schema_version": 1,
            "scope": scope,
            "complete": complete,
            "launch_blake2b512": launch_hash,
            "owner": owner,
            "process_id": std::process::id(),
            "process_start_ticks": process_start_ticks,
            "target": target,
            "preparation": {
                "wal_prewrite_bytes": wal_prewrite_bytes,
                "wal_prewrite_ns": wal_prewrite_ns,
                "wal_prewrite_files": wal_prewrite_files,
            },
            // Exports a request created inline while its caller waited.
            "inline_exports": node.live_inline_export_stats(),
            "serve_error": result.as_ref().err().map(ToString::to_string),
            "cleanup_error": cleanup.as_ref().err().map(ToString::to_string),
        });
        let path = Path::new(&file).with_extension("live-target-shutdown.json");
        let pending = path.with_extension(format!("pending-{}", rand::random::<u64>()));
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&pending)?;
        output.write_all(&serde_json::to_vec_pretty(&proof)?)?;
        output.sync_all()?;
        fs::rename(&pending, &path)?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::File::open(parent)?.sync_all()?;
        result
            .as_ref()
            .map_err(|error| io::Error::other(error.to_string()))?;
        cleanup?;
        if !complete {
            return Err(io::Error::other("live target shutdown was not qualified").into());
        }
    }
    result?;
    if let Some(path) = owner_events {
        tidehunter::distributed::owner_events::write_json(&path)?;
    }
    Ok(true)
}

#[cfg(test)]
mod durability_receipt_tests {
    use super::{
        DurabilityConfig, OwnerReceipt, ReceiptDurability, durability_file, publish_synced,
        read_owner_receipt,
    };
    use tidehunter::distributed::durability::DurabilityTier;

    fn scratch(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "tcp-cluster-{name}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn receipt(partition: u32, tier: DurabilityTier) -> OwnerReceipt {
        OwnerReceipt {
            partition,
            database: [7; 16],
            address: "127.0.0.1:9400".to_owned(),
            launch_nonce: "nonce".to_owned(),
            pid: 1,
            boot_id: "boot".to_owned(),
            started_unix_ns: 5,
            durability: ReceiptDurability::of(DurabilityConfig::with_tier(tier)),
        }
    }

    #[test]
    fn owner_receipt_round_trips_and_a_missing_file_fails_closed() {
        let directory = scratch("receipt");
        let receipt_path = durability_file(&directory.join("ready"));
        assert!(read_owner_receipt(&receipt_path).is_err());
        let written = receipt(2, DurabilityTier::Synchronous);
        publish_synced(&receipt_path, &serde_json::to_vec(&written).unwrap()).unwrap();
        assert_eq!(read_owner_receipt(&receipt_path).unwrap(), written);
        // The temporary name is gone: a reader sees only complete receipts.
        assert!(!receipt_path.with_extension("pending").exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_incomplete_receipt_is_never_read_as_the_native_default() {
        let directory = scratch("incomplete");
        let path = durability_file(&directory.join("ready"));
        let mut value = serde_json::to_value(receipt(0, DurabilityTier::Synchronous)).unwrap();
        let cases = [
            serde_json::json!({}),
            {
                let mut missing_tier = value.clone();
                missing_tier["durability"] = serde_json::json!({});
                missing_tier
            },
            {
                let mut missing_age = value.clone();
                missing_age["durability"]
                    .as_object_mut()
                    .unwrap()
                    .remove("max_unsynced_age_ms");
                missing_age
            },
            {
                let mut missing_identity = value.clone();
                missing_identity
                    .as_object_mut()
                    .unwrap()
                    .remove("partition");
                missing_identity
            },
        ];
        for case in cases {
            publish_synced(&path, &serde_json::to_vec(&case).unwrap()).unwrap();
            assert!(read_owner_receipt(&path).is_err(), "{case}");
        }
        value["unexpected"] = serde_json::json!(1);
        publish_synced(&path, &serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(read_owner_receipt(&path).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod live_shutdown_tests {
    use super::qualified_live_target_shutdown;
    use tidehunter::distributed::nvmeof::LiveTargetStats;

    #[test]
    fn shutdown_requires_clean_counters() {
        let mut stats = LiveTargetStats::default();
        assert!(qualified_live_target_shutdown(true, Some(&stats)));
        assert!(!qualified_live_target_shutdown(false, Some(&stats)));
        assert!(!qualified_live_target_shutdown(true, None));
        stats.owned_namespace_count = 1;
        assert!(!qualified_live_target_shutdown(true, Some(&stats)));
        stats.owned_namespace_count = 0;
        stats.errors = 1;
        assert!(!qualified_live_target_shutdown(true, Some(&stats)));
        stats.errors = 0;
        stats.fixture_only = true;
        assert!(!qualified_live_target_shutdown(true, Some(&stats)));
    }
}
