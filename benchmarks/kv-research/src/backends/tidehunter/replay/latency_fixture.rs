//! Local leased mainnet replay over the rootless ordinary-file live fixture.
//!
//! One coordinator per replayed node database, one in-process L2 owner host per
//! partition on loopback trusted TCP, fake configfs trees
//! (`OwnerHost::open_with_live_fixture`), and the coordinator opened with
//! `sui::Db::open_remote_with_local_live_fixture`; the live data route is an
//! ordinary file with the kernel target's buffered backing. Nothing here is a
//! hardware qualification route, and no kernel NVMe/TCP object is created.
//!
//! The replay itself uses the reviewed `mainnet_replay::run_prepared` path, so
//! `samples.csv`, `summary.json` and `measurement-read-evidence.json` have the
//! same schema as a fleet replay case. The scalar-only diagnostics (write
//! profile, write timeline, read profile) are rejected for replay by the
//! workload validator and are not claimed here.
//!
//! Driver JSON (paths are absolute):
//! ```json
//! {
//!   "manifest_path": "/abs/experiments/sui-mainnet/fixture/manifest.json",
//!   "manifest_sha256": "4264e79e…",
//!   "node_databases": [0],
//!   "owners": 2,
//!   "repetitions": 512,
//!   "concurrency": 1,
//!   "tier": "native",
//!   "durable_cut": {"lag_ms": 1000, "lag_bytes": 67108864},
//!   "buffered": true,
//!   "deadline_seconds": 600,
//!   "shutdown_grace_seconds": 20,
//!   "limits": {"max_keys": 8000000, "max_bytes": 17179869184, "max_operations": 8000000}
//! }
//! ```
use super::{TidehunterBackend, shape};
use crate::backend::{BackendResult, KvBackend, native_error};
use crate::backends::replay::ReplayBackend;
use crate::backends::tidehunter::open_record::{OpenKind, OpenRecord};
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tidehunter::distributed::durability::{DurabilityTier, DurableCutConfig};
use tidehunter::distributed::nvmeof::NvmeofTransport;
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::{
    LiveTargetPool, OwnerHost, OwnerHostConfig, TcpError, TcpLimits, TlsFiles, Transport,
};
use tidehunter::metrics::Metrics;

const SERVER_NAME: &str = "leased-replay-fixture";
const TRUSTED_COORDINATOR: [u8; 32] = [0x52; 32];
/// Fixture partitions per node database; the live target slots of all node
/// databases in one process must not overlap.
const MAX_PARTITIONS: u32 = 16;
/// Largest node database id the fixture hosts (ids `0..=MAX_NODE_DATABASE_ID`).
const MAX_NODE_DATABASE_ID: u8 = 2;
/// Placeholder owner ports of the admission-only configuration. The admission
/// check validates addresses but this module never binds them; the real owners
/// bind ephemeral loopback ports.
const PLACEHOLDER_PORT_BASE: u32 = 19_100;
/// Live target ports reserved per fixture slot (public plus commissioning).
const SLOT_PORT_STRIDE: u16 = 32;
const PUBLIC_PORT_BASE: u16 = 1;
const PUBLIC_SERVICE_BASE: u16 = 4420;
const COMMISSIONING_PORT_BASE: u16 = 17;
const COMMISSIONING_SERVICE_BASE: u16 = 4436;

// The highest slot's highest service port must fit in u16.
const _: () = assert!(
    (MAX_NODE_DATABASE_ID as u32 + 1) * MAX_PARTITIONS * SLOT_PORT_STRIDE as u32
        + COMMISSIONING_SERVICE_BASE as u32
        <= u16::MAX as u32
);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Driver {
    manifest_path: PathBuf,
    manifest_sha256: String,
    node_databases: Vec<u8>,
    #[serde(default = "default_owners")]
    owners: usize,
    repetitions: u64,
    concurrency: usize,
    #[serde(default)]
    tier: DurabilityTier,
    /// `None` keeps the engine defaults (nudges at 1000 ms or 64 MiB).
    #[serde(default)]
    durable_cut: Option<DurableCutConfig>,
    /// Buffered ordinary-file backing (the kernel target's file backing).
    #[serde(default = "default_buffered")]
    buffered: bool,
    #[serde(default = "default_deadline_seconds")]
    deadline_seconds: u64,
    #[serde(default = "default_grace_seconds")]
    shutdown_grace_seconds: u64,
    #[serde(default)]
    limits: DriverLimits,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DriverLimits {
    #[serde(default = "default_max_operations")]
    max_operations: u64,
    #[serde(default = "default_max_keys")]
    max_keys: u64,
    #[serde(default = "default_max_bytes")]
    max_bytes: u64,
}

impl Default for DriverLimits {
    fn default() -> Self {
        Self {
            max_operations: default_max_operations(),
            max_keys: default_max_keys(),
            max_bytes: default_max_bytes(),
        }
    }
}

impl Driver {
    /// Reject drivers outside the fixture's slot, owner and durable-cut bounds.
    fn validate(&self) -> BackendResult<()> {
        if self.node_databases.is_empty()
            || self
                .node_databases
                .iter()
                .any(|id| *id > MAX_NODE_DATABASE_ID)
            || !self.node_databases.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(format!(
                "node_databases must be strictly increasing ids in 0..={MAX_NODE_DATABASE_ID}"
            )
            .into());
        }
        if !(1..=MAX_PARTITIONS as usize).contains(&self.owners) {
            return Err(format!("owners must be within 1..={MAX_PARTITIONS}").into());
        }
        if self.concurrency == 0 || self.repetitions == 0 {
            return Err("concurrency and repetitions must be positive".into());
        }
        if let Some(cut) = &self.durable_cut {
            cut.validate()
                .map_err(|error| format!("durable_cut: {error}"))?;
        }
        Ok(())
    }
}

const fn default_owners() -> usize {
    1
}
const fn default_buffered() -> bool {
    true
}
const fn default_deadline_seconds() -> u64 {
    900
}
const fn default_grace_seconds() -> u64 {
    20
}
const fn default_max_operations() -> u64 {
    8_000_000
}
const fn default_max_keys() -> u64 {
    8_000_000
}
const fn default_max_bytes() -> u64 {
    16 * 1024 * 1024 * 1024
}

/// One in-process owner host per partition of one node database. Every owner
/// is joined by `stop` or `Drop`, so no owner outlives the roots it serves.
struct Owners {
    stop: Arc<AtomicBool>,
    servers: Vec<JoinHandle<Result<(), TcpError>>>,
}

impl Owners {
    /// Stop serving, join every owner and return one line per failed owner.
    fn stop(mut self) -> Vec<String> {
        self.halt()
    }

    fn halt(&mut self) -> Vec<String> {
        // Release wakes every owner reactor, as the other fixture owners.
        self.stop.store(true, Ordering::Release);
        self.servers
            .drain(..)
            .enumerate()
            .filter_map(|(partition, server)| match server.join() {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(format!("owner {partition}: serve failed: {error}")),
                Err(payload) => {
                    let detail = payload
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "non-string panic payload".to_owned());
                    Some(format!(
                        "owner {partition}: server thread panicked: {detail}"
                    ))
                }
            })
            .collect()
    }
}

impl Drop for Owners {
    fn drop(&mut self) {
        // A failed run reaches Drop without `stop`; join before the caller
        // removes the roots and report the failures, which cannot be returned here.
        for failure in self.halt() {
            eprintln!("latency fixture: {failure}");
        }
    }
}

fn tls() -> TlsFiles {
    TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: SERVER_NAME.into(),
    }
}

/// Coordinator configuration of one node database with `owners` partitions and
/// the pinned replay batch limits (one 519-member complete batch is one frame).
fn remote_config(
    db: u8,
    tier: DurabilityTier,
    addresses: &[String],
    durable_cut: Option<DurableCutConfig>,
) -> BackendResult<RemoteDatabaseConfig> {
    let partitions = (0..addresses.len())
        .map(u32::try_from)
        .collect::<Result<Vec<u32>, _>>()?;
    let mut document = json!({
        "storage": {
            "partitions": partitions,
            "batch_limits": {"max_operations": 519, "max_encoded_bytes": 500_000},
            "group_limits": {"max_batches": 32, "max_encoded_bytes": 2 * 1024 * 1024},
            "fragment_bytes": 4 * 1024 * 1024,
            "wal_file_bytes": 1024 * 1024 * 1024,
            "max_batches_per_partition": 8192,
            "max_reference_metadata_bytes": 64 * 1024 * 1024
        },
        "endpoints": addresses.iter().zip(&partitions).map(|(address, partition)| json!({
            "partition": partition, "address": address, "server_name": SERVER_NAME
        })).collect::<Vec<_>>(),
        "tls": tls(),
        "timeout": {"secs": 60, "nanos": 0},
        "max_admitted_batches": 8192,
        "max_pending_batches": 64,
        "max_pending_bytes": 32 * 1024 * 1024,
        "owner_append_occupancy": 16,
        "pipelined_writes": true,
        "read_cache_bytes": 0,
        "read_memory_allowance_bytes": 128 * 1024 * 1024,
        "read_lanes": 2,
        "pin_read_lanes": false,
        "max_pending_reads": 64,
        "max_pending_read_bytes": 16 * 1024 * 1024,
        "write_profile_limit": 0,
        "live_nvme": {
            "host_nqn": format!("nqn.2026-11.test:leased-replay-{db}"),
            "host_id": vec![db + 1; 16],
            "max_inflight_requests": 16,
            "max_inflight_bytes": 4 * 1024 * 1024,
            "connect_timeout_ms": 10_000,
            "io_timeout_ms": 60_000,
            "leased_durability": match tier {
                DurabilityTier::Native => "native",
                DurabilityTier::Synchronous => "synchronous",
            },
            "leased_write_window": 1,
            "leased_sync": "per_frame_fua",
            "leased_flush_min_frame_bytes": 0,
            "inline_reads": true,
            "lease_ahead": true,
            "polled_writes": false
        }
    });
    if let Some(cut) = durable_cut {
        document["durable_cut"] = serde_json::to_value(cut)?;
    }
    let config: RemoteDatabaseConfig = serde_json::from_value(document)?;
    config
        .validate()
        .map_err(|error| format!("leased replay fixture configuration: {error}"))?;
    Ok(config)
}

/// The replay configuration of one case. The backend is opened by this module,
/// so the options only have to pass the reviewed admission checks; their
/// `external_remote_config` mirrors the real fixture configuration.
fn replay_config(
    driver: &Driver,
    output: &Path,
) -> BackendResult<crate::mainnet_replay::ReplayConfig> {
    let databases: Vec<serde_json::Value> = driver
        .node_databases
        .iter()
        .map(|id| {
            let owner_addresses = (0..driver.owners)
                .map(|partition| {
                    let partition = u32::try_from(partition)?;
                    Ok(format!(
                        "127.0.0.1:{}",
                        PLACEHOLDER_PORT_BASE + u32::from(*id) * MAX_PARTITIONS + partition
                    ))
                })
                .collect::<BackendResult<Vec<String>>>()?;
            Ok(json!({
                "db_id": *id,
                "options": {
                    "transport": {"kind": "tcp-trusted"},
                    "path": output.join(format!("db-{id}")),
                    "owners": driver.owners,
                    "external_remote_config": remote_config(
                        *id,
                        driver.tier,
                        &owner_addresses,
                        driver.durable_cut,
                    )?
                }
            }))
        })
        .collect::<BackendResult<_>>()?;
    serde_json::from_value(json!({
        "schema_version": 1,
        "backend": {"kind": "tidehunter_tcp", "options": {"databases": databases}},
        "workload": {
            "kind": "sui_mainnet_replay",
            "manifest_path": driver.manifest_path,
            "manifest_sha256": driver.manifest_sha256,
            "repetitions": driver.repetitions,
            "minimum_measurement_seconds": 0.0,
            "node_databases": driver.node_databases,
        },
        "execution": {
            "mode": "closed_loop",
            "concurrency": driver.concurrency,
            "client_shards": 1,
            "queue_capacity": driver.concurrency.max(1),
            "offered_ops_per_second": null,
            "request_timeout_ms": 30_000,
        },
        "limits": {
            "max_keys": driver.limits.max_keys,
            "max_bytes": driver.limits.max_bytes,
            "max_operations": driver.limits.max_operations,
            "deadline_seconds": driver.deadline_seconds,
            "shutdown_grace_seconds": driver.shutdown_grace_seconds,
        },
        "diagnostics": {},
    }))
    .map_err(Into::into)
}

fn now_unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0.0, |elapsed| elapsed.as_secs_f64())
}

/// Start one fixture owner host per partition of `db` and return their bound
/// loopback addresses. Owners serve the control plane; the live data route is
/// their ordinary-file WAL under the owner root.
fn start_owners(
    root: &Path,
    db: u8,
    partitions: usize,
) -> BackendResult<(Owners, Vec<String>, Vec<Arc<OwnerHost>>)> {
    let mut owners = Owners {
        stop: Arc::new(AtomicBool::new(false)),
        servers: Vec::with_capacity(partitions),
    };
    let mut addresses = Vec::with_capacity(partitions);
    let mut hosts = Vec::with_capacity(partitions);
    for partition in 0..partitions {
        let partition = u32::try_from(partition)?;
        let base = root.join(format!("db-{db}"));
        fs::create_dir_all(&base)?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        addresses.push(listener.local_addr()?.to_string());
        // Distinct live target slots for every (database, partition) in this process.
        let slot = u16::try_from(u32::from(db) * MAX_PARTITIONS + partition)?;
        let host_config = OwnerHostConfig {
            root: base.join(format!("owner-{partition}")),
            max_databases: 1,
            live_target_pool: LiveTargetPool {
                address: "127.0.0.1".into(),
                transport: NvmeofTransport::Tcp,
                public_port_base: PUBLIC_PORT_BASE + slot * SLOT_PORT_STRIDE,
                public_service_base: PUBLIC_SERVICE_BASE + slot * SLOT_PORT_STRIDE,
                commissioning_port_base: COMMISSIONING_PORT_BASE + slot * SLOT_PORT_STRIDE,
                commissioning_service_base: COMMISSIONING_SERVICE_BASE + slot * SLOT_PORT_STRIDE,
            },
            trusted_coordinator_id: Some(TRUSTED_COORDINATOR),
            wal_member_read_policy: Default::default(),
            wal_prewrite: false,
            max_device_reads_per_status: 64,
            durability: Default::default(),
        };
        host_config.validate()?;
        let configfs = base.join(format!("configfs-{partition}"));
        for name in ["hosts", "ports", "subsystems"] {
            fs::create_dir_all(configfs.join(name))?;
        }
        let host = OwnerHost::open_with_live_fixture(
            host_config,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            configfs,
        )
        .map_err(|error| format!("leased replay owner {db}/{partition}: {error}"))?;
        let serving = Arc::clone(&host);
        let stop = Arc::clone(&owners.stop);
        let name = format!("replay-owner-{db}-{partition}");
        owners.servers.push(
            std::thread::Builder::new()
                .name(name)
                .spawn(move || serving.serve(listener, tls(), TcpLimits::default(), stop))?,
        );
        hosts.push(host);
    }
    Ok((owners, addresses, hosts))
}

/// Run one replay case over the local leased fixture and write the standard
/// replay artifacts plus `latency-fixture.json` into `output`.
pub async fn run(driver_path: &Path, output: &Path) -> BackendResult<()> {
    let driver: Driver = serde_json::from_slice(&fs::read(driver_path)?)?;
    // The standard replay reporter requires a fresh output directory, so every
    // fixture root lives beside it under the case directory.
    let case_root = output
        .parent()
        .ok_or("replay output directory has no case parent")?;
    let fixture_root = case_root.join("fixture");
    driver.validate()?;
    let config = replay_config(&driver, &fixture_root)?;
    let started = Instant::now();
    let started_unix = now_unix();
    let (trace, mut prepared, mut resources) = config.admit()?;
    resources["binary"] = crate::runner::binary_identity();
    let root = fixture_root;
    let mut owner_sets = Vec::new();
    let mut databases = BTreeMap::new();
    for id in &driver.node_databases {
        let (owner_set, addresses, hosts) = start_owners(&root, *id, driver.owners)?;
        let configuration = remote_config(*id, driver.tier, &addresses, driver.durable_cut)?;
        let base = root.join(format!("db-{id}"));
        let coordinator = base.join("coordinator");
        fs::create_dir_all(&coordinator)?;
        let open_kind =
            if fs::read_dir(&coordinator).is_ok_and(|mut entries| entries.next().is_some()) {
                OpenKind::Reopen
            } else {
                OpenKind::Create
            };
        let open_start_ns = tidehunter::distributed::remote_db::diagnostics_clock_ns();
        let db = sui::Db::open_remote_with_local_live_fixture(
            &coordinator,
            shape(&trace.tables, u16::from(*id)),
            configuration.clone(),
            driver.buffered,
        )
        .map_err(native_error)?;
        let database = db
            .remote_database_id()
            .ok_or("leased replay fixture did not provision a remote database")?;
        if hosts
            .iter()
            .flat_map(|host| host.databases())
            .any(|hosted| hosted != database)
        {
            return Err("leased replay owner hosts another database".into());
        }
        let first_table = trace
            .tables
            .iter()
            .find(|table| table.db_id == u16::from(*id))
            .ok_or("replayed node database has no tables")?;
        let keyspace = db.ks(&first_table.node_name);
        let open_record = OpenRecord::new(open_kind, open_start_ns, &db);
        databases.insert(
            u16::from(*id),
            TidehunterBackend {
                db: Some(db),
                keyspace,
                relocation_marker: None,
                endpoints: configuration
                    .endpoints
                    .iter()
                    .map(|endpoint| (endpoint.partition, endpoint.address.clone()))
                    .collect(),
                owners: driver.owners,
                backend_name: "tidehunter_leased_fixture",
                durability_tier: driver.tier,
                evidence_path: base.clone(),
                effective_configuration: json!({
                    "backend": "tidehunter_leased_fixture",
                    "fixture_route": "leased ordinary-file live fixture",
                    "data_backend": if driver.buffered { "buffered ordinary file" } else { "direct ordinary file" },
                    "database": tidehunter::distributed::tcp::host::identity::database_hex(&database),
                    "durability_tier": driver.tier.as_str(),
                    "durable_cut": driver.durable_cut,
                    "remote": configuration,
                }),
                read_evidence_written: false,
                owner_sample_gate: Default::default(),
                coordinator_interval_baseline: Mutex::new(None),
                live_write_profile: Mutex::new(None),
                open_record,
                local_owners: Vec::new(),
            },
        );
        owner_sets.push((*id, owner_set));
    }
    let tables = trace
        .tables
        .iter()
        .map(|table| {
            databases
                .get(&table.db_id)
                .map(|backend| {
                    Ok((
                        table.db_id,
                        backend
                            .db
                            .as_ref()
                            .ok_or("leased replay DB closed")?
                            .ks(&table.node_name),
                    ))
                })
                .transpose()
        })
        .collect::<BackendResult<_>>()?;
    let backend: Arc<super::TidehunterReplayBackend> =
        Arc::new(super::TidehunterReplayBackend { databases, tables });
    let info = backend.info()?;
    // The replay workload rejects the scalar-only diagnostics, but the engine read
    // profile is a backend counter: arm it for the first bounded flights of the
    // measurement and collect it after the run. Read stage timings are nested
    // annotations of one remote flight, not additive leaves.
    let read_profile_limit = 256_u64;
    for database in backend.databases.values() {
        if let Some(db) = database.db.as_ref() {
            db.set_read_profile_limit(read_profile_limit);
        }
    }
    resources["prepared_placement"] = backend.prepare_case(&mut prepared)?;
    let prepared = Arc::new(prepared);
    let result = crate::mainnet_replay::run_prepared(
        config,
        output,
        trace,
        prepared,
        Arc::clone(&backend) as Arc<dyn ReplayBackend>,
        resources,
        (started, started_unix, Some(info)),
    )
    .await;
    let mut read_profiles = serde_json::Map::new();
    let mut read_profile_error = None;
    for (id, database) in &backend.databases {
        match database.finish_read_profile().await {
            Ok(profile) => {
                read_profiles.insert(id.to_string(), profile);
            }
            Err(error) => {
                if read_profile_error.is_none() {
                    read_profile_error = Some(error.to_string());
                }
            }
        }
    }
    fs::write(
        output.join("read-diagnostics.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "requested_limit": read_profile_limit,
            "status": if read_profile_error.is_some() { "failed" } else { "complete" },
            "error": read_profile_error,
            "profiles": read_profiles,
            "interpretation": "read profile samples the first bounded remote flights and the first bounded API resolutions; the two populations are never subtracted. Server and transport stages are nested annotations. index_lock_wait and cache_lock_wait are exported but not instrumented; absent is not zero.",
        }))?,
    )?;
    let stats = backend
        .databases
        .iter()
        .map(|(id, database)| {
            let cut = database
                .db
                .as_ref()
                .and_then(|db| db.durable_cut_stats())
                .map(|stats| {
                    json!({
                        "enabled": stats.enabled,
                        "epoch": stats.epoch,
                        "persisted_next": stats.persisted_next,
                        "reports": stats.reports,
                        "nudges": stats.nudges,
                        "persists": stats.persists,
                        "persist_failures": stats.persist_failures,
                        "poll_failures": stats.poll_failures,
                        "certifications": stats.certifications,
                        "targeted_syncs": stats.targeted_syncs,
                    })
                });
            let observation = database
                .read_observation()
                .unwrap_or_else(|error| json!({"available": false, "error": error.to_string()}));
            (
                id.to_string(),
                json!({"durable_cut": cut, "live_nvmeof": observation}),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    // Close the coordinators before the owners stop, so their shutdown does not
    // talk to stopped owners, and surface the close errors that `Drop` would
    // only write to `lifecycle-errors.txt`.
    let mut close_failures: Vec<String> = match Arc::try_unwrap(backend) {
        Ok(replay) => replay
            .databases
            .into_iter()
            .filter_map(|(id, mut database)| {
                database
                    .close()
                    .err()
                    .map(|error| format!("node database {id}: coordinator close: {error}"))
            })
            .collect(),
        Err(shared) => {
            // Detached workers still hold the backend; its Drop closes it later.
            drop(shared);
            vec!["replay backend still shared after the run; closed by Drop".to_owned()]
        }
    };
    // Join every owner after the last owner-facing read; a serve failure is
    // evidence, and a run that stopped dirty is not a clean result.
    close_failures.extend(owner_sets.into_iter().flat_map(|(id, owners)| {
        owners
            .stop()
            .into_iter()
            .map(move |failure| format!("node database {id}: {failure}"))
    }));
    fs::write(
        output.join("latency-fixture.json"),
        serde_json::to_vec_pretty(&json!({
            "schema_version": 1,
            "driver": driver_path,
            "node_databases": driver.node_databases,
            "owners": driver.owners,
            "tier": driver.tier.as_str(),
            "durable_cut": driver.durable_cut,
            "buffered": driver.buffered,
            "concurrency": driver.concurrency,
            "repetitions": driver.repetitions,
            "databases": stats,
            "shutdown_failures": close_failures,
            "route": "rootless ordinary-file leased live fixture; no kernel NVMe/TCP object, no external network",
        }))?,
    )?;
    match (result, close_failures.is_empty()) {
        (Ok(()), false) => Err(format!("shutdown failures: {}", close_failures.join("; ")).into()),
        (result, _) => result,
    }
}

#[cfg(test)]
#[path = "latency_fixture_tests.rs"]
mod tests;
