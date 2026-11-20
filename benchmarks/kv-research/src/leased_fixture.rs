//! Shared rootless leased ordinary-file fixture route: one real L2
//! `OwnerHost` per partition on loopback trusted TCP with fake configfs
//! trees (`OwnerHost::open_with_live_fixture`), PerOwner live export,
//! WAL-replay history, and the coordinator opened with
//! `sui::Db::open_remote_with_local_live_fixture` (buffered file backing, as
//! the kernel target's file backing). The coordinator open provisions the
//! database (identity, geometry, shape, tier) on every owner host; owner and
//! coordinator use the provisioned tier.
//!
//! One route for operation replay, A1 index calibration, the Q1 local proof,
//! and the local benchmark adapter. Owners run
//! either in-process ([`LeasedOwner`]) or as supervised child processes of
//! this executable ([`OwnerChild`]), which survive a crash-style coordinator
//! stop. Owner roots, fake control trees and fixed listen addresses persist
//! across processes under [`FixtureSpec::root`]: the authority manifest binds
//! the endpoints, so every process reuses them.
//!
//! Linux, `test-utils` only; no production configuration can select it.
#![cfg(all(feature = "test-utils", target_os = "linux"))]

use crate::backend::BackendResult;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tidehunter::distributed::codec::BatchLimits;
use tidehunter::distributed::durability::DurabilityTier;
use tidehunter::distributed::group::GroupLimits;
use tidehunter::distributed::nvmeof::NvmeofTransport;
use tidehunter::distributed::remote_db::RemoteDatabaseConfig;
use tidehunter::distributed::sui;
use tidehunter::distributed::tcp::host::identity::database_hex;
use tidehunter::distributed::tcp::{
    LiveTargetPool, OwnerHost, OwnerHostConfig, TcpError, TcpLimits, TlsFiles, Transport,
};
use tidehunter::key_shape::KeyShape;
use tidehunter::metrics::Metrics;

#[cfg(test)]
mod tests;

const MIB: usize = 1024 * 1024;
const PAGE: usize = 4096;
const SERVER_NAME: &str = "leased-fixture";
/// Command-line mode of the supervised owner child.
const OWNER_CHILD_MODE: &str = "--experimental-leased-fixture-owner";
const READY_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(120);
const POLL: Duration = Duration::from_millis(10);
/// Principal of the trusted fixture coordinator (trusted transports only).
const TRUSTED_COORDINATOR: [u8; 32] = [0x51; 32];
/// Partitions per fixture: each owner takes 32 target slots of the pool.
const MAX_OWNERS: u32 = 16;

/// Coordinator admission and memory bounds of the fixture database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoordinatorSpec {
    pub timeout_secs: u64,
    pub max_admitted_batches: usize,
    pub max_pending_batches: usize,
    pub max_pending_bytes: usize,
    pub read_memory_allowance_bytes: usize,
}

/// Leased write tuning; the route itself is fixed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LiveSpec {
    /// `None` uses queue credit and owner append occupancy only.
    pub write_window: Option<usize>,
    /// Per-frame FUA instead of group flush (Synchronous tier only; the
    /// Native tier acknowledges at completion either way).
    pub per_frame_fua: bool,
}

/// One owner host: its partition and fixed loopback listen address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixtureOwner {
    pub partition: u32,
    pub address: SocketAddr,
}

/// Complete, serializable description of one fixture database.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FixtureSpec {
    /// Holds `owner-p<N>/` (owner host roots) and `configfs-p<N>/` (fake
    /// control trees). Absolute in cross-process use.
    pub root: PathBuf,
    /// Coordinator live initiator identity (`live_nvme.host_id`). The
    /// database identity is provisioned by the coordinator open.
    pub host_id: [u8; 16],
    /// Partitions `0..N` in order.
    pub owners: Vec<FixtureOwner>,
    pub tier: DurabilityTier,
    pub host_nqn: String,
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
    pub batch_limits: BatchLimits,
    pub group_limits: GroupLimits,
    pub max_batches_per_partition: usize,
    pub max_reference_metadata_bytes: usize,
    pub coordinator: CoordinatorSpec,
    pub live: LiveSpec,
}

/// A fresh live initiator identity: random, never all zero.
pub(crate) fn fresh_host_id() -> [u8; 16] {
    let mut id: [u8; 16] = rand::random();
    id[0] |= 1;
    id
}

/// Owners for partitions `0..count`, each on a fresh loopback address.
pub(crate) fn fresh_owners(count: u32) -> BackendResult<Vec<FixtureOwner>> {
    if !(1..=MAX_OWNERS).contains(&count) {
        return Err("leased fixture supports one to sixteen owners".into());
    }
    // Hold the listeners while picking all ports so assignments cannot collide.
    let listeners = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    listeners
        .iter()
        .zip(0..count)
        .map(|(listener, partition)| {
            Ok(FixtureOwner {
                partition,
                address: listener.local_addr()?,
            })
        })
        .collect()
}

fn trusted_tls() -> TlsFiles {
    TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: SERVER_NAME.into(),
    }
}

impl FixtureSpec {
    pub(crate) fn read(root: &Path) -> BackendResult<Self> {
        let path = root.join("spec.json");
        let bytes = fs::read(&path)
            .map_err(|error| format!("leased fixture spec {}: {error}", path.display()))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    pub(crate) fn create(&self) -> BackendResult<()> {
        // Claim a fresh root; never replace an existing fixture identity.
        fs::create_dir(&self.root)?;
        publish_record(&self.root.join("spec.json"), self)?;
        fs::File::open(&self.root)?.sync_all()?;
        let parent = self
            .root
            .parent()
            .ok_or("leased fixture root has no parent")?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    }

    fn tier_name(&self) -> &'static str {
        match self.tier {
            DurabilityTier::Native => "native",
            DurabilityTier::Synchronous => "synchronous",
        }
    }

    /// Owner host root of `partition`.
    pub(crate) fn owner_root(&self, partition: u32) -> PathBuf {
        self.root.join(format!("owner-p{partition}"))
    }

    fn configfs(&self, partition: u32) -> PathBuf {
        self.root.join(format!("configfs-p{partition}"))
    }

    fn owner(&self, partition: u32) -> BackendResult<FixtureOwner> {
        self.owners
            .iter()
            .copied()
            .find(|owner| owner.partition == partition)
            .ok_or_else(|| format!("leased fixture has no owner for partition {partition}").into())
    }

    /// Owners must be partitions `0..N`, `N` in `1..=16`, on loopback.
    fn validate_owners(&self) -> BackendResult<()> {
        let ordered =
            self.owners.iter().enumerate().all(|(index, owner)| {
                u32::try_from(index).is_ok_and(|index| index == owner.partition)
            });
        let count = u32::try_from(self.owners.len()).unwrap_or(u32::MAX);
        if !ordered
            || !(1..=MAX_OWNERS).contains(&count)
            || !self
                .owners
                .iter()
                .all(|owner| owner.address.ip().is_loopback() && owner.address.port() != 0)
            || self.owners.iter().enumerate().any(|(index, owner)| {
                self.owners[..index]
                    .iter()
                    .any(|prior| prior.address == owner.address)
            })
        {
            return Err("leased fixture owners must be partitions 0..N (N <= 16) on distinct loopback endpoints".into());
        }
        Ok(())
    }

    /// One live write frame (group + 8, page aligned) for the in-flight bound.
    fn frame_bytes(&self) -> BackendResult<usize> {
        let frame = (self.group_limits.max_encoded_bytes + 8).next_multiple_of(PAGE);
        if u64::try_from(frame)? > self.fragment_bytes {
            return Err(format!(
                "leased fixture frame {frame} B exceeds fragment {} B",
                self.fragment_bytes
            )
            .into());
        }
        Ok(frame)
    }

    /// Coordinator configuration that reaches every owner. It names no
    /// database: the coordinator open provisions it.
    pub(crate) fn remote_config(&self) -> BackendResult<RemoteDatabaseConfig> {
        self.validate_owners()?;
        let frame = self.frame_bytes()?;
        let coordinator = &self.coordinator;
        let mut live = json!({
            "host_nqn": self.host_nqn, "host_id": self.host_id,
            "max_inflight_requests": 16, "max_inflight_bytes": 2 * frame + 4 * PAGE,
            "connect_timeout_ms": 10_000, "io_timeout_ms": 60_000,
            "leased_durability": self.tier_name(),
            "lease_ahead": true, "inline_reads": true, "polled_writes": false,
        });
        if let Some(window) = self.live.write_window {
            live["leased_write_window"] = json!(window);
        }
        if self.live.per_frame_fua {
            live["leased_sync"] = json!("per_frame_fua");
            live["leased_flush_min_frame_bytes"] = json!(0);
        }
        let partitions: Vec<u32> = self.owners.iter().map(|owner| owner.partition).collect();
        let endpoints: Vec<_> = self
            .owners
            .iter()
            .map(|owner| {
                json!({"partition": owner.partition, "address": owner.address.to_string(),
                    "server_name": SERVER_NAME})
            })
            .collect();
        let document = json!({
            "storage": {"partitions": partitions,
                "batch_limits": self.batch_limits, "group_limits": self.group_limits,
                "fragment_bytes": self.fragment_bytes, "wal_file_bytes": self.wal_file_bytes,
                "max_batches_per_partition": self.max_batches_per_partition,
                "max_reference_metadata_bytes": self.max_reference_metadata_bytes},
            "endpoints": endpoints,
            "tls": trusted_tls(), "timeout": {"secs": coordinator.timeout_secs, "nanos": 0},
            "max_admitted_batches": coordinator.max_admitted_batches,
            "max_pending_batches": coordinator.max_pending_batches,
            "max_pending_bytes": coordinator.max_pending_bytes,
            "owner_append_occupancy": 16, "read_cache_bytes": 0,
            "read_memory_allowance_bytes": coordinator.read_memory_allowance_bytes,
            "read_lanes": 2, "max_pending_reads": 64, "max_pending_read_bytes": 16 * MIB,
            "live_nvme": live,
        });
        let config: RemoteDatabaseConfig = serde_json::from_value(document)?;
        config
            .validate()
            .map_err(|error| format!("leased fixture configuration: {error}"))?;
        Ok(config)
    }

    /// Owner host configuration of `partition`: one database, a private slot
    /// range of the target pool, the trusted fixture coordinator.
    pub(crate) fn host_config(&self, partition: u32) -> BackendResult<OwnerHostConfig> {
        let base = u16::try_from(partition)
            .ok()
            .filter(|_| partition < MAX_OWNERS)
            .map(|partition| partition * 32)
            .ok_or("leased fixture supports at most sixteen owners")?;
        let config = OwnerHostConfig {
            root: self.owner_root(partition),
            max_databases: 1,
            live_target_pool: LiveTargetPool {
                address: "127.0.0.1".into(),
                transport: NvmeofTransport::Tcp,
                public_port_base: 1 + base,
                public_service_base: 4420 + base,
                commissioning_port_base: 17 + base,
                commissioning_service_base: 4436 + base,
            },
            trusted_coordinator_id: Some(TRUSTED_COORDINATOR),
            wal_member_read_policy: Default::default(),
            // Sparse owner files: actual blocks are monitored, never prewritten.
            wal_prewrite: false,
            max_device_reads_per_status: 64,
            durability: Default::default(),
        };
        config.validate()?;
        Ok(config)
    }

    /// Open (or recover) the owner host of `partition` on the ordinary-file
    /// target fixture. The fake control tree is created once and kept.
    fn open_host(&self, partition: u32) -> BackendResult<Arc<OwnerHost>> {
        let configfs = self.configfs(partition);
        for name in ["hosts", "ports", "subsystems"] {
            fs::create_dir_all(configfs.join(name))?;
        }
        Ok(OwnerHost::open_with_live_fixture(
            self.host_config(partition)?,
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
            configfs,
        )?)
    }
}

/// Open the coordinator of a fixture database on the leased route.
pub(crate) fn open_coordinator(
    path: &Path,
    shape: KeyShape,
    config: RemoteDatabaseConfig,
) -> BackendResult<Arc<sui::Db>> {
    // Buffered ordinary-file backing, as the kernel target's file backing.
    sui::Db::open_remote_with_local_live_fixture(path, shape, config, true)
        .map_err(|error| format!("{error:?}").into())
}

/// In-process owner hosts, one per partition, over trusted loopback TCP.
pub(crate) struct LeasedOwner {
    stop: Arc<AtomicBool>,
    servers: Vec<JoinHandle<Result<(), TcpError>>>,
    config: RemoteDatabaseConfig,
    pub tier: DurabilityTier,
}

impl LeasedOwner {
    pub(crate) fn start(spec: &FixtureSpec) -> BackendResult<Self> {
        let config = spec.remote_config()?;
        let stop = Arc::new(AtomicBool::new(false));
        let mut owner = Self {
            stop,
            servers: Vec::with_capacity(spec.owners.len()),
            config,
            tier: spec.tier,
        };
        for fixture in &spec.owners {
            let host = spec.open_host(fixture.partition)?;
            let listener = TcpListener::bind(fixture.address)?;
            let stopping = Arc::clone(&owner.stop);
            owner.servers.push(
                std::thread::Builder::new()
                    .name(format!("leased-fixture-owner-{}", fixture.partition))
                    .spawn(move || {
                        host.serve(listener, trusted_tls(), TcpLimits::default(), stopping)
                    })?,
            );
        }
        Ok(owner)
    }

    pub(crate) fn open(&self, path: &Path, shape: KeyShape) -> BackendResult<Arc<sui::Db>> {
        open_coordinator(path, shape, self.config.clone())
    }

    /// Stop serving and return the first server error.
    pub(crate) fn stop(mut self) -> BackendResult<()> {
        self.halt()
    }

    fn halt(&mut self) -> BackendResult<()> {
        // Release pairs with the owner service's shutdown check.
        self.stop.store(true, Ordering::Release);
        let mut result = Ok(());
        for server in self.servers.drain(..) {
            let joined = match server.join() {
                Ok(served) => served.map_err(Into::into),
                Err(_) => Err("owner server thread panicked".into()),
            };
            if result.is_ok() {
                result = joined;
            }
        }
        result
    }
}

impl Drop for LeasedOwner {
    fn drop(&mut self) {
        // Join before the caller removes the owners' directories; `stop`
        // reports errors.
        let _ = self.halt();
    }
}

/// Launch document of one supervised owner child.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerLaunch {
    spec: FixtureSpec,
    partition: u32,
    ready_file: PathBuf,
    stop_file: PathBuf,
    /// Start receipt, published before the ready file.
    receipt_file: PathBuf,
    /// Shutdown record, published after serving stopped.
    shutdown_file: PathBuf,
}

/// Owner host in a supervised child process of this executable. The child
/// dies with its parent (`PR_SET_PDEATHSIG`); a graceful stop is a stop file.
pub(crate) struct OwnerChild {
    child: Option<Child>,
    stop_file: PathBuf,
}

impl OwnerChild {
    /// Start the owner host of `partition`; control files, receipts and logs
    /// go to `evidence`, named by `label`.
    pub(crate) fn spawn(
        spec: &FixtureSpec,
        partition: u32,
        evidence: &Path,
        label: &str,
    ) -> BackendResult<Self> {
        spec.validate_owners()?;
        let address = spec.owner(partition)?.address;
        fs::create_dir_all(evidence)?;
        let file = |kind: &str| evidence.join(format!("{label}.{kind}"));
        let launch = OwnerLaunch {
            spec: spec.clone(),
            partition,
            ready_file: file("ready"),
            stop_file: file("stop"),
            receipt_file: file("receipt.json"),
            shutdown_file: file("shutdown.json"),
        };
        for path in [
            &launch.ready_file,
            &launch.stop_file,
            &launch.receipt_file,
            &launch.shutdown_file,
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let launch_path = file("launch.json");
        fs::write(&launch_path, serde_json::to_vec_pretty(&launch)?)?;
        let log = |kind: &str| {
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(file(&format!("{kind}.log")))
        };
        let mut child = Command::new(std::env::current_exe()?)
            .arg(OWNER_CHILD_MODE)
            .arg(&launch_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log("stdout")?))
            .stderr(Stdio::from(log("stderr")?))
            .spawn()?;
        match wait_ready(&mut child, &launch.ready_file) {
            Ok(ready) if ready == address => Ok(Self {
                child: Some(child),
                stop_file: launch.stop_file,
            }),
            outcome => {
                // A std `Child` does not kill its process on drop.
                let _ = child.kill();
                let _ = child.wait();
                Err(match outcome {
                    Ok(ready) => format!("owner child bound {ready}, expected {address}").into(),
                    Err(error) => error,
                })
            }
        }
    }

    /// Request a graceful stop and wait for a successful exit. A child that
    /// does not exit within the bound is killed and reported as an error.
    pub(crate) fn stop(mut self) -> BackendResult<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        fs::write(&self.stop_file, "stop\n")?;
        let deadline = Instant::now() + STOP_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait()? {
                self.child = None;
                return if status.success() {
                    Ok(())
                } else {
                    Err(format!("owner child stop failed: {status}").into())
                };
            }
            std::thread::sleep(POLL);
        }
        child.kill()?;
        child.wait()?;
        self.child = None;
        Err("owner child did not stop within the bound; killed".into())
    }
}

impl Drop for OwnerChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// The owner's bound address once its ready file lands (bounded wait).
fn wait_ready(child: &mut Child, ready_file: &Path) -> BackendResult<SocketAddr> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("owner child exited before readiness: {status}").into());
        }
        match fs::read_to_string(ready_file) {
            Ok(address) => return Ok(address.trim().parse()?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if Instant::now() >= deadline {
            return Err("owner child readiness timeout".into());
        }
        std::thread::sleep(POLL);
    }
}

/// Temporary file, file sync, rename: a reader never sees a partial record.
fn publish_record(path: &Path, value: &impl Serialize) -> BackendResult<()> {
    let pending = path.with_extension("pending");
    {
        let mut file = fs::File::create(&pending)?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(value)?)?;
        file.sync_all()?;
    }
    fs::rename(&pending, path)?;
    Ok(())
}

/// Owner child entry; must run before normal CLI parsing. Returns `false`
/// when this process is not an owner child.
pub(crate) fn maybe_run_owner_child() -> BackendResult<bool> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some(OWNER_CHILD_MODE) {
        return Ok(false);
    }
    let launch_path = args.next().ok_or("missing owner launch document")?;
    if args.next().is_some() {
        return Err("unexpected owner child arguments".into());
    }
    // The owner must not outlive its supervising coordinator process.
    // SAFETY: getppid takes no arguments; this prctl option takes only an
    // integer signal value and neither reads nor retains a user pointer.
    let parent = unsafe { libc::getppid() };
    // SAFETY: as above.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: getppid takes no arguments.
    if unsafe { libc::getppid() } != parent {
        return Err("supervisor exited during owner startup".into());
    }
    let launch: OwnerLaunch = serde_json::from_slice(&fs::read(&launch_path)?)?;
    let address = launch.spec.owner(launch.partition)?.address;
    let host = launch.spec.open_host(launch.partition)?;
    let listener = TcpListener::bind(address)?;
    let bound = listener.local_addr()?;
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned();
    // Names this exact process; lands before the ready file.
    publish_record(
        &launch.receipt_file,
        &json!({
            "schema_version": 1,
            "kind": "leased_fixture_owner_start",
            "route": "ordinary-file live target fixture; not a kernel target",
            "partition": launch.partition,
            "address": bound.to_string(),
            "root": launch.spec.owner_root(launch.partition),
            "pid": std::process::id(),
            "boot_id": boot_id,
            "launch_nonce": database_hex(&fresh_host_id()),
            "databases_at_start": host.databases().iter().map(database_hex).collect::<Vec<_>>(),
        }),
    )?;
    let pending = launch.ready_file.with_extension("pending");
    fs::write(&pending, bound.to_string())?;
    fs::rename(&pending, &launch.ready_file)?;
    println!(
        "leased_fixture_owner pid={} partition={} address={bound}",
        std::process::id(),
        launch.partition
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let monitor_shutdown = Arc::clone(&shutdown);
    let stop_file = launch.stop_file;
    let monitor = std::thread::Builder::new()
        .name("leased-fixture-stop".into())
        .spawn(move || {
            // Release pairs with the owner service's shutdown check.
            while !monitor_shutdown.load(Ordering::Acquire) {
                if stop_file.exists() {
                    monitor_shutdown.store(true, Ordering::Release);
                    break;
                }
                std::thread::sleep(POLL);
            }
        })?;
    let served = host.serve(
        listener,
        trusted_tls(),
        TcpLimits::default(),
        Arc::clone(&shutdown),
    );
    shutdown.store(true, Ordering::Release);
    monitor.join().map_err(|_| "owner stop monitor panicked")?;
    // After every serving session joined, as the production owner launcher.
    let cleanup = host.finish_live_target_shutdown();
    let targets: Vec<_> = cleanup
        .as_ref()
        .map(|targets| {
            targets
                .iter()
                .map(|(database, stats)| json!({"database": database_hex(database), "target": stats}))
                .collect()
        })
        .unwrap_or_default();
    publish_record(
        &launch.shutdown_file,
        &json!({
            "schema_version": 2,
            "scope": "fixture_live_target_shutdown",
            "fixture_only": true,
            "qualifies_kernel_target": false,
            "owner": {"partition": launch.partition, "root": launch.spec.owner_root(launch.partition)},
            "process_id": std::process::id(),
            "targets": targets,
            "serve_error": served.as_ref().err().map(ToString::to_string),
            "cleanup_error": cleanup.as_ref().err().map(ToString::to_string),
        }),
    )?;
    served?;
    cleanup?;
    Ok(true)
}

/// Fixture geometry used by the Q1 local proof and the shared synthetic
/// route, from the benchmark adapter's checked overrides: batch 32 ops /
/// 320 KiB, group at most 32 batches / 2 MiB, pending 64 batches / 32 MiB,
/// reference metadata 64 MiB, read memory 128 MiB per side, one leased write
/// flight, per-frame FUA for Synchronous, no coordinator value cache.
pub(crate) struct BenchmarkGeometry {
    pub fragment_bytes: u64,
    pub wal_file_bytes: u64,
    pub max_admitted_batches: usize,
}

impl BenchmarkGeometry {
    pub(crate) fn spec(
        &self,
        root: PathBuf,
        tier: DurabilityTier,
        owners: Vec<FixtureOwner>,
    ) -> FixtureSpec {
        FixtureSpec {
            root,
            host_id: fresh_host_id(),
            owners,
            tier,
            host_nqn: "nqn.2026-11.test:leased-fixture".into(),
            fragment_bytes: self.fragment_bytes,
            wal_file_bytes: self.wal_file_bytes,
            batch_limits: BatchLimits {
                max_operations: 32,
                max_encoded_bytes: 320 * 1024,
            },
            group_limits: GroupLimits {
                max_batches: 32,
                max_encoded_bytes: 2 * MIB,
            },
            max_batches_per_partition: self.max_admitted_batches,
            max_reference_metadata_bytes: 64 * MIB,
            coordinator: CoordinatorSpec {
                timeout_secs: 60,
                max_admitted_batches: self.max_admitted_batches,
                max_pending_batches: 64,
                max_pending_bytes: 32 * MIB,
                read_memory_allowance_bytes: 128 * MIB,
            },
            live: LiveSpec {
                write_window: Some(1),
                per_frame_fua: true,
            },
        }
    }
}
