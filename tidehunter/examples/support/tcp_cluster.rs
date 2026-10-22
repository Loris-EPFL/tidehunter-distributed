//! Shared real-process storage harness. All node data belongs to the caller's
//! temporary case directory; only logs and public process/address evidence remain.

#[path = "tls_material.rs"]
mod tls_material;

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tidehunter::distributed::local_inventory::LocalInventoryConfig;
use tidehunter::distributed::remote_db::{RemoteDatabaseConfig, RemoteEndpoint};
use tidehunter::distributed::storage::{PartitionConfig, StaticInventory};
use tidehunter::distributed::tcp::{
    NodeConfig, PollingPolicy, StorageNode, TcpLimits, TlsFiles, Transport,
};
use tidehunter::key_shape::KeyShape;
use tidehunter::metrics::Metrics;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// Write one fresh external namespace using the same serde types as the runtime.
/// No sockets are opened and no remote data is reset. Callers transport each
/// node's leaf key/config separately; the CA signing key never belongs on nodes.
#[allow(dead_code)] // Only the benchmark executable generates external configurations.
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
    read_workers: usize,
    wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
    read_cache_bytes: usize,
    read_memory_allowance_bytes: usize,
    polling: PollingPolicy,
    read_resident_first: bool,
) -> Result<()> {
    if !guest_root.is_absolute() || addresses.len() != storage.partitions.len() {
        return Err(io::Error::other("invalid external case root/endpoints").into());
    }
    transport.validate()?;
    fs::create_dir(directory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    let certs = if transport.is_native() {
        None
    } else {
        Some(tls_material::TestCertificates::generate(
            &directory.join("tls"),
            addresses.len(),
        )?)
    };
    let rebase = |path: &Path| guest_root.join("tls").join(path.file_name().unwrap());
    let mut endpoints = Vec::new();
    for (i, (&partition, &address)) in storage.partitions.iter().zip(addresses).enumerate() {
        let local_node = directory.join(format!("node-{partition}"));
        fs::create_dir(&local_node)?;
        let node_root = guest_root.join(format!("node-{partition}"));
        let (ca, cert, key, server_name) = if let Some(certs) = certs.as_ref() {
            let identity = &certs.nodes[i];
            (
                rebase(&certs.ca),
                rebase(&identity.certificate),
                rebase(&identity.private_key),
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
                read_workers,
                wal_member_read,
                read_resident_first,
                read_cache_bytes,
                read_memory_allowance_bytes,
                read_max_inflight_requests: 32,
                read_max_inflight_bytes: 64 * 1024 * 1024,
                read_max_device_reads: 128,
            },
            tls: TlsFiles {
                transport,
                ca: ca.clone(),
                cert: cert.clone(),
                key: key.clone(),
                server_name: server_name.clone(),
            },
            limits: TcpLimits {
                io_timeout: Duration::from_secs(60),
                polling,
                ..TcpLimits::default()
            },
            listen: address,
            ready_file: node_root.join("ready"),
            stop_file: node_root.join("stop"),
            drop_first_append_response: false,
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
            ca: rebase(&certs.ca),
            cert: rebase(&certs.authority.certificate),
            key: rebase(&certs.authority.private_key),
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
        pipelined_writes: true,
        read_cache_bytes: 8 * 1024 * 1024,
        read_memory_allowance_bytes: 512 * 1024 * 1024,
        preparation_workers: 2,
        scan_prefetch_pages: 2,
        read_lanes: 2,
        max_pending_reads: 256,
        max_pending_read_bytes: 64 * 1024 * 1024,
        write_profile_limit: 0,
    };
    fs::write(
        directory.join("remote.yaml"),
        serde_yaml::to_string(&config)?,
    )?;
    fs::write(directory.join("shape.yaml"), serde_yaml::to_string(shape)?)?;
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
    drop_first_append_response: bool,
}

/// Must run before normal CLI parsing in either example executable.
pub fn maybe_run_storage_node() -> Result<bool> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("--experimental-storage-node") {
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
    let config: NodeLaunch = serde_yaml::from_slice(&fs::read(file)?)?;
    println!(
        "shard_read_memory_allowance_bytes={} cache_budget_bytes={} cache_metadata_before_allocation_bytes={}",
        config.node.read_memory_allowance_bytes,
        config.node.read_cache_bytes,
        config.node.read_cache_metadata_bytes(),
    );
    println!("read_resident_first={}", config.node.read_resident_first);
    let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
    let node = StorageNode::open(config.node, metrics)?;
    if config.drop_first_append_response {
        #[cfg(feature = "test-utils")]
        node.inject_drop_next_append_response();
        #[cfg(not(feature = "test-utils"))]
        return Err(io::Error::other("response-loss injection requires test-utils").into());
    }
    let listener = TcpListener::bind(config.listen)?;
    let address = listener.local_addr()?;
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
    result?;
    Ok(true)
}

struct ManagedNode {
    config: NodeLaunch,
    config_file: PathBuf,
    evidence: PathBuf,
    child: Option<Child>,
    start_number: usize,
}

impl ManagedNode {
    fn start(&mut self) -> Result<SocketAddr> {
        if self.child.is_some() {
            return Err(io::Error::other("node already has an owned process").into());
        }
        for file in [&self.config.ready_file, &self.config.stop_file] {
            match fs::remove_file(file) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
        }
        fs::write(&self.config_file, serde_yaml::to_string(&self.config)?)?;
        let log = |kind| {
            OpenOptions::new().create_new(true).write(true).open(
                self.evidence
                    .join(format!("start-{}.{}.log", self.start_number, kind)),
            )
        };
        let child = Command::new(std::env::current_exe()?)
            .arg("--experimental-storage-node")
            .arg(&self.config_file)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log("stdout")?))
            .stderr(Stdio::from(log("stderr")?))
            .spawn()?;
        self.child = Some(child);
        self.start_number += 1;
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait()? {
                return Err(io::Error::other(format!(
                    "storage child exited before readiness: {status}"
                ))
                .into());
            }
            match fs::read_to_string(&self.config.ready_file) {
                Ok(address) => {
                    let address: SocketAddr = address.trim().parse()?;
                    // Reuse exactly this endpoint if this node is restarted.
                    self.config.listen = address;
                    return Ok(address);
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "storage child readiness timeout",
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn stop(&mut self, graceful: bool) -> Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(child) = &self.child {
            // Preserve each child's resident/high-water values. Summing these
            // high-water marks is an upper bound, not a simultaneous tree peak.
            if let Ok(status) = fs::read_to_string(format!("/proc/{}/status", child.id())) {
                fs::write(
                    self.evidence.join(format!(
                        "start-{}.process-status.txt",
                        self.start_number - 1
                    )),
                    status,
                )?;
            }
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if graceful {
            fs::write(&self.config.stop_file, "stop\n")?;
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline {
                if let Some(status) = child.try_wait()? {
                    self.child.take();
                    if !status.success() {
                        return Err(
                            io::Error::other(format!("storage shutdown failed: {status}")).into(),
                        );
                    }
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        if child.try_wait()?.is_none() {
            child.kill()?;
        }
        child.wait()?;
        self.child.take();
        if graceful {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "storage shutdown exceeded ten seconds; child killed",
            )
            .into());
        }
        Ok(())
    }
}

impl Drop for ManagedNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

pub struct LoopbackCluster {
    nodes: Vec<ManagedNode>,
    pub config: RemoteDatabaseConfig,
}

// The timing driver shares startup/shutdown but intentionally omits fault helpers.
#[allow(dead_code)]
impl LoopbackCluster {
    pub fn start(
        data: &Path,
        evidence: &Path,
        shape: &KeyShape,
        storage: LocalInventoryConfig,
        max_admitted_batches: usize,
        max_index_bytes: usize,
        drop_first_on_partition: Option<u32>,
    ) -> Result<Self> {
        Self::start_with_transport(
            data,
            evidence,
            shape,
            storage,
            max_admitted_batches,
            max_index_bytes,
            drop_first_on_partition,
            Transport::Tcp,
            2,
            tidehunter::distributed::storage::WalMemberReadPolicy::Async,
            0,
            512 * 1024 * 1024,
            PollingPolicy::Adaptive,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)] // Same bounded harness configuration, plus explicit transport.
    pub fn start_with_transport(
        data: &Path,
        evidence: &Path,
        shape: &KeyShape,
        storage: LocalInventoryConfig,
        max_admitted_batches: usize,
        max_index_bytes: usize,
        drop_first_on_partition: Option<u32>,
        transport: Transport,
        read_workers: usize,
        wal_member_read: tidehunter::distributed::storage::WalMemberReadPolicy,
        read_cache_bytes: usize,
        read_memory_allowance_bytes: usize,
        polling: PollingPolicy,
        read_resident_first: bool,
    ) -> Result<Self> {
        transport.validate()?;
        fs::create_dir_all(data)?;
        fs::create_dir_all(evidence)?;
        let data = fs::canonicalize(data)?;
        let certs = if transport.is_native() {
            None
        } else {
            Some(tls_material::TestCertificates::generate(
                &data.join("tls"),
                storage.partitions.len(),
            )?)
        };
        let limits = TcpLimits {
            io_timeout: Duration::from_secs(60),
            polling,
            ..TcpLimits::default()
        };
        let mut nodes = Vec::new();
        let mut endpoints = Vec::new();
        for (i, &partition) in storage.partitions.iter().enumerate() {
            let node_data = data.join(format!("node-{partition}"));
            fs::create_dir(&node_data)?;
            let node_evidence = evidence.join(format!("node-{partition}"));
            fs::create_dir(&node_evidence)?;
            let (ca, cert, key, server_name) = if let Some(certs) = certs.as_ref() {
                let identity = &certs.nodes[i];
                (
                    certs.ca.clone(),
                    identity.certificate.clone(),
                    identity.private_key.clone(),
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
                    root: node_data.join("storage"),
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
                    read_workers,
                    wal_member_read,
                    read_resident_first,
                    read_cache_bytes,
                    read_memory_allowance_bytes,
                    read_max_inflight_requests: 32,
                    read_max_inflight_bytes: 64 * 1024 * 1024,
                    read_max_device_reads: 128,
                },
                tls: TlsFiles {
                    transport,
                    ca: ca.clone(),
                    cert: cert.clone(),
                    key: key.clone(),
                    server_name: server_name.clone(),
                },
                limits,
                listen: "127.0.0.1:0".parse()?,
                ready_file: node_data.join("ready"),
                stop_file: node_data.join("stop"),
                drop_first_append_response: drop_first_on_partition == Some(partition),
            };
            let mut node = ManagedNode {
                config: launch,
                config_file: node_data.join("launch.yaml"),
                evidence: node_evidence,
                child: None,
                start_number: 0,
            };
            let address = node.start()?;
            endpoints.push(RemoteEndpoint {
                partition,
                address: address.to_string(),
                server_name: server_name.clone(),
            });
            nodes.push(node);
        }
        let client_tls = if let Some(certs) = certs {
            TlsFiles {
                transport,
                ca: certs.ca,
                cert: certs.authority.certificate,
                key: certs.authority.private_key,
                server_name: "unused-client-template".to_owned(),
            }
        } else {
            TlsFiles {
                transport,
                ca: PathBuf::new(),
                cert: PathBuf::new(),
                key: PathBuf::new(),
                server_name: "unused-client-template".to_owned(),
            }
        };
        let config = RemoteDatabaseConfig {
            storage,
            endpoints,
            tls: client_tls,
            timeout: Duration::from_secs(5),
            max_admitted_batches,
            max_index_bytes,
            max_pending_batches: 128,
            max_pending_bytes: 64 * 1024 * 1024,
            pipelined_writes: true,
            read_cache_bytes: 8 * 1024 * 1024,
            read_memory_allowance_bytes: 512 * 1024 * 1024,
            preparation_workers: 2,
            scan_prefetch_pages: 2,
            read_lanes: 2,
            max_pending_reads: 256,
            max_pending_read_bytes: 64 * 1024 * 1024,
            write_profile_limit: 0,
        };
        // TLS configuration contains only paths, but it must not be presented as
        // a persistent usable credential bundle after the private tempdir closes.
        fs::write(
            evidence.join("processes.yaml"),
            serde_yaml::to_string(
                &nodes
                    .iter()
                    .zip(&config.endpoints)
                    .map(|(node, endpoint)| {
                        (
                            endpoint.partition,
                            node.child.as_ref().unwrap().id(),
                            &endpoint.address,
                            &endpoint.server_name,
                        )
                    })
                    .collect::<Vec<_>>(),
            )?,
        )?;
        Ok(Self { nodes, config })
    }

    pub fn kill_partition(&mut self, partition: u32) -> Result<()> {
        self.node_mut(partition)?.stop(false)
    }
    pub fn client(
        &self,
        partition: u32,
        shape: &KeyShape,
    ) -> Result<tidehunter::distributed::tcp::Client> {
        let node = self
            .nodes
            .iter()
            .find(|node| node.config.node.partition.partition == partition)
            .ok_or_else(|| io::Error::other("unknown owned partition"))?;
        let endpoint = self
            .config
            .endpoints
            .iter()
            .find(|endpoint| endpoint.partition == partition)
            .unwrap();
        let mut tls = self.config.tls.clone();
        tls.server_name = endpoint.server_name.clone();
        Ok(tidehunter::distributed::tcp::Client::connect(
            &endpoint.address,
            &tls,
            self.config.timeout,
            node.config.node.partition.clone(),
            shape,
        )?)
    }
    pub fn template(&self, partition: u32) -> Result<PartitionConfig> {
        self.nodes
            .iter()
            .find(|node| node.config.node.partition.partition == partition)
            .map(|node| node.config.node.partition.clone())
            .ok_or_else(|| io::Error::other("unknown owned partition").into())
    }
    pub fn untrusted_client_tls(&self, directory: &Path) -> Result<TlsFiles> {
        let foreign = tls_material::TestCertificates::generate(directory, 1)?;
        Ok(TlsFiles {
            transport: self.config.tls.transport,
            ca: self.config.tls.ca.clone(),
            cert: foreign.authority.certificate,
            key: foreign.authority.private_key,
            server_name: String::new(),
        })
    }
    pub fn restart_partition(&mut self, partition: u32) -> Result<()> {
        self.node_mut(partition)?.start()?;
        Ok(())
    }
    fn node_mut(&mut self, partition: u32) -> Result<&mut ManagedNode> {
        self.nodes
            .iter_mut()
            .find(|node| node.config.node.partition.partition == partition)
            .ok_or_else(|| io::Error::other("unknown owned partition").into())
    }
    pub fn shutdown(&mut self) -> Result<()> {
        let mut first = None;
        for node in &mut self.nodes {
            if let Err(error) = node.stop(true)
                && first.is_none()
            {
                first = Some(error);
            }
        }
        if let Some(error) = first {
            return Err(error);
        }
        Ok(())
    }
}
