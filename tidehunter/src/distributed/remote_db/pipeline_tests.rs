//! Bounded five-node mTLS tests for the publication boundary, not timing tests.
//! Tidehunter requires whole batches and a stable ordered checkpoint prefix;
//! independent durable batches may publish beyond an unresolved gap.
//! Existing connection guards and storage persistence hooks deterministically
//! pause append progress; tests make no assumptions about SSD speed or scheduling.
//!
//! Every owner serves the leased live route (the only write route) through
//! the ordinary-file LiveIo fixture: a commissioned fixture target per owner,
//! the production leased worker, real frame I/O and CRC checks, and no kernel
//! controller or NVMe device.

use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::NvmeofTransport;
use crate::distributed::tcp::{
    LiveClientConfig, LiveTargetPool, NodeConfig, OwnerHost, OwnerHostConfig, StorageNode,
    TcpError, TcpLimits,
};
use crate::key_shape::KeyType;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "../../../examples/support/tls_material.rs"]
mod tls_material;

/// Host NQN of the default fixture client and of every fixture target's allowed
/// host. A test that configures another `live_nvme.host_nqn` gets targets that
/// admit that host instead.
pub(super) const LIVE_HOST_NQN: &str = "nqn.2026-01.test:pipeline";

/// Default live client of the fixture: the leased route at the fixture tier,
/// with caller-inline reads on as in production. A test that needs reads
/// only through the lane worker sets `inline_reads = false` with a reason.
/// The queue byte bound admits the default write frame and the recovery
/// buffers of the fixture's batch limits (validated at open).
pub(super) fn live_config(tier: DurabilityTier) -> LiveClientConfig {
    LiveClientConfig {
        host_nqn: LIVE_HOST_NQN.into(),
        host_id: [1; 16],
        owner_identities: Default::default(),
        max_inflight_requests: 8,
        leased_write_window: None,
        leased_sync: Default::default(),
        leased_flush_min_frame_bytes: LiveClientConfig::default_leased_flush_min_frame_bytes(),
        max_inflight_bytes: 4 * 1024 * 1024,
        connect_timeout_ms: 5000,
        io_timeout_ms: 5000,
        leased_durability: tier,
        lease_ahead: true,
        polled_writes: false,
        inline_reads: true,
        queue_geometry: Default::default(),
    }
}

/// Fixture coordinator configuration at the native tier, with no endpoints.
pub(super) fn coordinator_config(
    owner_count: usize,
    max_batches: usize,
    group_max_batches: u32,
    tls: TlsFiles,
) -> RemoteDatabaseConfig {
    RemoteDatabaseConfig {
        storage: LocalInventoryConfig {
            database: [0; 16],
            partitions: (0..owner_count as u32).collect(),
            batch_limits: BatchLimits {
                max_operations: 4,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: group_max_batches,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches_per_partition: max_batches,
            max_reference_metadata_bytes: 1024 * 1024,
        },
        endpoints: Vec::new(),
        tls,
        // Five-node startup/replay shares disk and CPU with the full suite.
        // This is a bounded liveness guard, not a latency assertion.
        timeout: Duration::from_secs(30),
        max_admitted_batches: max_batches,
        max_index_bytes: 1024 * 1024,
        max_pending_batches: 64,
        max_pending_bytes: 1024 * 1024,
        replay_limits: Default::default(),
        owner_append_occupancy: DEFAULT_OWNER_APPEND_OCCUPANCY,
        pipelined_writes: true,
        leased_submit_redrain: true,
        read_cache_bytes: 64 * 1024,
        read_memory_allowance_bytes: 512 * 1024 * 1024,
        scan_prefetch_pages: 2,
        read_lanes: Some(2),
        pin_read_lanes: false,
        max_pending_reads: 256,
        max_pending_read_bytes: 64 * 1024 * 1024,
        write_profile_limit: 0,
        live_nvme: live_config(DurabilityTier::Native),
        durable_cut: Default::default(),
        relocation_max_reclaim_pct: 100,
        index: Default::default(),
    }
}

/// Owner host whose databases use the ordinary-file live target fixture; the
/// host derives each database's target (registry under the database root,
/// effective initiator NQN, per-owner exports) from its slot pool.
pub(super) fn open_live_fixture_host(config: OwnerHostConfig) -> Result<Arc<OwnerHost>, TcpError> {
    let configfs = config.root.with_extension("configfs");
    for name in ["hosts", "ports", "subsystems"] {
        std::fs::create_dir_all(configfs.join(name))?;
    }
    OwnerHost::open_with_live_fixture(config, metrics(), configfs)
}

/// Owner host configuration of the coordinator fixtures: the per-database
/// settings the fixtures used before owners became multi-database hosts.
pub(super) fn owner_host_config(root: PathBuf) -> OwnerHostConfig {
    OwnerHostConfig {
        root,
        max_databases: 16,
        live_target_pool: LiveTargetPool {
            address: "127.0.0.1".into(),
            transport: NvmeofTransport::Tcp,
            public_port_base: 42,
            public_service_base: 4420,
            commissioning_port_base: 142,
            commissioning_service_base: 5420,
        },
        trusted_coordinator_id: None,
        wal_member_read_policy: Default::default(),
        wal_prewrite: false,
        max_device_reads_per_status: 128,
        durability: Default::default(),
    }
}

pub(super) fn trusted_owner_host_config(root: PathBuf) -> OwnerHostConfig {
    let mut config = owner_host_config(root);
    config.max_databases = 1;
    config.trusted_coordinator_id = Some([1; 32]);
    config.max_device_reads_per_status = 64;
    config
}

fn owner_limits() -> TcpLimits {
    TcpLimits {
        io_timeout: Duration::from_secs(30),
        ..TcpLimits::default()
    }
}

/// Coordinator fixture over `OwnerHost`s (one per partition). The database
/// is provisioned at construction through the real coordinator open path,
/// so `config.storage.database` and `nodes` name it before the first open.
pub(super) struct Fixture {
    pub(super) config: RemoteDatabaseConfig,
    pub(super) shape: KeyShape,
    /// The provisioned database on each owner host, in partition order.
    pub(super) nodes: Vec<Arc<StorageNode>>,
    pub(super) hosts: Vec<Arc<OwnerHost>>,
    shutdown: Arc<AtomicBool>,
    servers: Vec<JoinHandle<Result<(), TcpError>>>,
    /// Per owner: the database's owner config as derived by its host (its
    /// `root` is `<host root>/db-<hex>`), the host TLS identity and listener.
    pub(super) startups: Vec<(NodeConfig, TlsFiles, TcpListener)>,
    pub(super) host_configs: Vec<OwnerHostConfig>,
    pub(super) temp: tempdir::TempDir,
}

impl Fixture {
    pub(super) fn new() -> Self {
        Self::new_with_capacity(128)
    }

    pub(super) fn new_with_capacity(max_batches: usize) -> Self {
        Self::new_with_shape(max_batches, KeyShape::new_single(4, 1, KeyType::uniform(1)))
    }

    pub(super) fn new_with_shape(max_batches: usize, shape: KeyShape) -> Self {
        Self::new_with_shape_and_group(max_batches, shape, 2)
    }

    fn new_with_shape_and_group(
        max_batches: usize,
        shape: KeyShape,
        group_max_batches: u32,
    ) -> Self {
        Self::new_with_config(max_batches, shape, group_max_batches, |_| {})
    }

    /// Opt one test into the synchronous tier: for tests whose assertions need
    /// persistence to stay pending (prior durability, paused syncs) or receipts
    /// that are already durable. Call before the first write. The leased client
    /// requests the same tier, as a grant of another tier is refused.
    pub(super) fn synchronous(mut self) -> Self {
        for node in &self.nodes {
            node.set_durability(crate::distributed::durability::DurabilityConfig::with_tier(
                DurabilityTier::Synchronous,
            ))
            .unwrap();
        }
        self.config.live_nvme.leased_durability = DurabilityTier::Synchronous;
        self
    }

    pub(super) fn new_with_config(
        max_batches: usize,
        shape: KeyShape,
        group_max_batches: u32,
        configure: impl FnOnce(&mut RemoteDatabaseConfig),
    ) -> Self {
        Self::new_with_root(
            tempdir::TempDir::new("remote-pipeline-mtls").unwrap(),
            5,
            max_batches,
            shape,
            group_max_batches,
            configure,
            |_| {},
        )
    }

    pub(super) fn new_with_root(
        temp: tempdir::TempDir,
        owner_count: usize,
        max_batches: usize,
        shape: KeyShape,
        group_max_batches: u32,
        configure: impl FnOnce(&mut RemoteDatabaseConfig),
        configure_node: impl Fn(&mut NodeConfig),
    ) -> Self {
        assert!((1..=16).contains(&owner_count));
        let tier = DurabilityTier::Native;
        let certs = tls_material::TestCertificates::generate(&temp.path().join("tls"), owner_count)
            .unwrap();
        let mut config = coordinator_config(
            owner_count,
            max_batches,
            group_max_batches,
            TlsFiles {
                transport: Default::default(),
                ca: certs.ca.clone(),
                cert: certs.authority.certificate.clone(),
                key: certs.authority.private_key.clone(),
                server_name: String::new(),
            },
        );
        configure(&mut config);
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut hosts = Vec::new();
        let mut host_configs = Vec::new();
        let mut servers = Vec::new();
        let mut listeners = Vec::new();
        for (index, identity) in certs.nodes.into_iter().enumerate() {
            let mut node_config = NodeConfig {
                root: temp.path().join(format!("node-{index}")),
                partition: config.partition_config(1, index as u32),
                shape_yaml: shape.to_yaml().unwrap(),
                wal_member_read_policy: Default::default(),
                wal_prewrite: false,
                max_device_reads_per_status: 128,
                live_target: None,
            };
            configure_node(&mut node_config);
            let mut host_config = owner_host_config(node_config.root);
            host_config.wal_member_read_policy = node_config.wal_member_read_policy;
            host_config.wal_prewrite = node_config.wal_prewrite;
            host_config.max_device_reads_per_status = node_config.max_device_reads_per_status;
            let host = open_live_fixture_host(host_config.clone()).unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            config.endpoints.push(RemoteEndpoint {
                partition: index as u32,
                address: listener.local_addr().unwrap().to_string(),
                server_name: identity.server_name.clone(),
            });
            let tls = TlsFiles {
                transport: Default::default(),
                ca: certs.ca.clone(),
                cert: identity.certificate,
                key: identity.private_key,
                server_name: identity.server_name,
            };
            listeners.push((tls.clone(), listener.try_clone().unwrap()));
            let serving = Arc::clone(&host);
            let stop = Arc::clone(&shutdown);
            servers.push(thread::spawn(move || {
                serving.serve(listener, tls, owner_limits(), stop)
            }));
            hosts.push(host);
            host_configs.push(host_config);
        }
        authority_lifecycle::provision_for_test(
            &temp.path().join("authority"),
            &shape,
            &mut config,
        )
        .unwrap();
        let database = config.storage.database;
        let mut nodes = Vec::new();
        let mut startups = Vec::new();
        for (host, (tls, listener)) in hosts.iter().zip(listeners) {
            let node = host.database(&database).unwrap();
            node.set_durability(crate::distributed::durability::DurabilityConfig::with_tier(
                tier,
            ))
            .unwrap();
            startups.push((node.config_for_test(), tls, listener));
            nodes.push(node);
        }
        Self {
            config,
            shape,
            nodes,
            hosts,
            shutdown,
            servers,
            startups,
            host_configs,
            temp,
        }
    }

    pub(super) fn authority(&self) -> PathBuf {
        self.temp.path().join("authority")
    }

    /// The provisioned database directory, also available after owner stop.
    pub(super) fn owner_database_root(&self, owner: usize) -> &Path {
        &self.startups[owner].0.root
    }

    /// Stop serving and release every owner host: the next restart sees
    /// only the durable image.
    pub(super) fn stop_owners(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for server in self.servers.drain(..) {
            server.join().unwrap().unwrap();
        }
        self.nodes.clear();
        self.hosts.clear();
    }

    /// Reopen every stopped owner with its commissioned fixture target.
    pub(super) fn restart_owners(&mut self) -> Result<(), TcpError> {
        self.shutdown.store(false, Ordering::Release);
        let database = self.config.storage.database;
        for (host_config, (_, tls, listener)) in self.host_configs.iter().zip(&self.startups) {
            let host = open_live_fixture_host(host_config.clone())?;
            let node = host.database(&database).ok_or_else(|| {
                TcpError::with_kind(
                    std::io::ErrorKind::NotFound,
                    "restarted owner host lost the database",
                )
            })?;
            node.set_durability(crate::distributed::durability::DurabilityConfig::with_tier(
                self.config.live_nvme.leased_durability,
            ))?;
            let serving = host.clone();
            let tls = tls.clone();
            let listener = listener.try_clone()?;
            let stop = self.shutdown.clone();
            self.servers.push(thread::spawn(move || {
                serving.serve(listener, tls, owner_limits(), stop)
            }));
            self.hosts.push(host);
            self.nodes.push(node);
        }
        Ok(())
    }

    pub(super) fn open(&self) -> RemoteDatabase {
        self.try_open_with(self.config.clone()).unwrap()
    }

    /// Open the fixture authority with `config` through the ordinary-file
    /// live fixture: recovery and every later live read and write use it.
    pub(super) fn try_open_with(
        &self,
        config: RemoteDatabaseConfig,
    ) -> Result<RemoteDatabase, LocalDbError> {
        RemoteDatabase::open_local_live_fixture(
            &self.temp.path().join("authority"),
            &self.shape,
            config,
        )
    }

    pub(super) fn operation(&self, partition: u32, seed: u64) -> [u8; 16] {
        (seed * 1024..seed * 1024 + 1024)
            .map(|number| {
                let mut operation = [0u8; 16];
                operation[..8].copy_from_slice(&number.to_le_bytes());
                operation
            })
            .find(|operation| {
                self.config
                    .inventory(1)
                    .partition_for(BatchId {
                        database: self.config.storage.database,
                        operation: *operation,
                    })
                    .unwrap()
                    == partition
            })
            .unwrap()
    }

    /// Fence owner `node` to `epoch` over a fresh authenticated control
    /// session, as a competing authority would. An authority worker's leased
    /// session may own its single control exchange, so tests do not fence
    /// through it.
    pub(super) fn fence_owner(&self, node: usize, epoch: u64) {
        let endpoint = &self.config.endpoints[node];
        let mut tls = self.config.tls.clone();
        tls.server_name = endpoint.server_name.clone();
        let mut client = Client::connect(
            &endpoint.address,
            &tls,
            Duration::from_secs(5),
            self.config.partition_config(1, endpoint.partition),
            &self.shape,
        )
        .unwrap();
        client.fence_to(epoch).unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for server in self.servers.drain(..) {
            let result = server.join();
            if !thread::panicking() {
                assert!(result.unwrap().is_ok(), "storage listener failed");
            }
        }
    }
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

fn value(db: &mut RemoteDatabase, key: u32) -> Option<Vec<u8>> {
    db.get(KeySpace(0), &key.to_be_bytes())
        .unwrap()
        .map(|v| v.to_vec())
}

/// Wait until the owner worker has processed every expected receipt under
/// writer→publisher: published, or durable and parked behind an unresolved
/// predecessor. Workers publish on their own thread; there is no authority
/// completion route left to consume.
fn settle_publication(db: &RemoteDatabase, expected: &[u64]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    for &sequence in expected {
        while !db.core.publication_settled(sequence) {
            db.core.ensure_healthy().expect("owner append failed");
            assert!(
                Instant::now() < deadline,
                "owner receipts were not processed"
            );
            thread::yield_now();
        }
    }
}

/// Validate each real owner receipt (exact version and owner, strictly
/// increasing physical owner stream) and return the worker's exact formed
/// groups, which must cover `expected` exactly. Leased workers can merge
/// WorkerItems; this RPC fixture forms one group per WorkerItem.
pub(super) fn complete_owner(db: &RemoteDatabase, owner: usize, expected: &[u64]) -> Vec<Vec<u64>> {
    settle_publication(db, expected);
    let partition = db.core.config.endpoints[owner].partition;
    let receipts: Vec<_> = expected
        .iter()
        .map(|&sequence| {
            let receipt = db
                .core
                .durable_receipt(sequence)
                .expect("processed generation retains its receipt");
            assert_eq!(
                receipt.appended().version(),
                BatchVersion {
                    epoch: db.epoch(),
                    sequence
                }
            );
            assert_eq!(receipt.appended().address().partition, partition);
            receipt
        })
        .collect();
    for pair in receipts.windows(2) {
        let earlier = pair[0].appended().address();
        let later = pair[1].appended().address();
        assert!(
            (earlier.segment_generation, earlier.offset) < (later.segment_generation, later.offset),
            "correlated callback order must not reorder the physical owner stream"
        );
    }
    let mut groups: Vec<Vec<u64>> = db.core.clients[owner]
        .accounting
        .formed_groups
        .lock()
        .iter()
        .filter(|group| group.iter().any(|sequence| expected.contains(sequence)))
        .cloned()
        .collect();
    groups.sort_unstable_by_key(|group| group[0]);
    assert_eq!(
        groups.iter().flatten().copied().collect::<Vec<_>>(),
        expected
    );
    groups
}

/// Publication finalization (slot return, Admission release) follows the
/// published-floor notification, so tests settle before asserting zero.
fn settle(db: &RemoteDatabase) {
    db.core.wait_settled().unwrap();
}

/// Wait until exactly `batches` admitted generations still hold their
/// partition charge. Finalization (outcome, key cleanup) precedes that
/// release, so every other generation has finished its publication pass.
fn wait_unpublished(db: &RemoteDatabase, batches: usize) {
    assert!(
        db.core
            .admission
            .wait_charges(Instant::now() + Duration::from_secs(10), |charges| charges
                .partition_batches
                == batches),
        "publication finalization did not complete"
    );
}

fn worker_groups(db: &RemoteDatabase, owner: usize) -> usize {
    db.core.clients[owner]
        .accounting
        .groups
        .load(Ordering::Relaxed)
}

fn worker_bytes(db: &RemoteDatabase, owner: usize) -> usize {
    db.core.clients[owner]
        .accounting
        .bytes
        .load(Ordering::Relaxed)
}

/// Exact worker transport charge for groups whose members have not yet
/// published (their canonical payload is still retained by the slot).
fn assert_worker_bytes(db: &RemoteDatabase, owner: usize, groups: usize, sequences: &[u64]) {
    let batch_bytes: usize = sequences
        .iter()
        .map(|&sequence| {
            db.core
                .pending_encoded_len(sequence)
                .expect("unpublished generation retains its payload")
                + 64
        })
        .sum();
    assert_eq!(worker_bytes(db, owner), groups * 64 + batch_bytes);
    assert!(worker_bytes(db, owner) <= super::super::tcp::APPEND_SESSION_BYTES - 64 * 1024);
}

/// Observe submitted RPC flights, not worker-local queued groups. Persistence
/// is explicitly paused by the caller, so the expected count cannot retire.
fn wait_for_flights(db: &RemoteDatabase, owner: usize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let flights = db.core.clients[owner]
            .client
            .lock()
            .append_flights_in_flight();
        assert!(
            flights <= expected,
            "worker exceeded its exact flight window"
        );
        if flights == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not submit its ready groups"
        );
        thread::yield_now();
    }
}

#[test]
fn full_groups_dispatch_before_drain_on_five_nodes_and_recover_exact_state() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    // Hold every connection before any append is enqueued. This proves a
    // bounded per-node window dispatches groups without waiting for a reply.
    let connections: Vec<_> = db
        .core
        .clients
        .iter()
        .map(|worker| Arc::clone(&worker.client))
        .collect();
    let held: Vec<_> = connections
        .iter()
        .map(|connection| connection.lock())
        .collect();
    let mut originals = Vec::new();
    for key in 0..32u32 {
        let operation = fixture.operation(key % 5, u64::from(key));
        let mutation = put(key, key as u8);
        let version = db.submit(operation, vec![mutation.clone()]).unwrap();
        originals.push((operation, mutation, version));
    }
    let queued: Vec<_> = db
        .core
        .clients
        .iter()
        .map(|worker| worker.queue.snapshot())
        .collect();
    let before = (
        db.core
            .clients
            .iter()
            .map(|worker| worker.accounting.formed.load(Ordering::Relaxed))
            .sum::<u64>(),
        (0..5).map(|owner| worker_groups(&db, owner)).sum::<usize>(),
        (0..5).map(|owner| worker_bytes(&db, owner)).sum::<usize>(),
        db.core.published_next(),
        db.core.admission.charges(),
    );
    drop(held);
    // Group formation now runs under each worker's client lock: all 32
    // batches are READY, but no WorkerItem or transport flight exists yet.
    assert_eq!((before.0, before.1, before.2), (0, 0, 0));
    for (owner, (head, next, entries)) in queued.iter().enumerate() {
        let expected: Vec<_> = (0..32u64).filter(|seq| *seq % 5 == owner as u64).collect();
        assert_eq!((*head, *next), (1, expected.len() as u64 + 1));
        assert_eq!(
            *entries,
            expected
                .iter()
                .enumerate()
                .map(|(offset, &seq)| (offset as u64 + 1, seq, 1))
                .collect::<Vec<_>>()
        );
    }
    assert_eq!(before.3, 0);
    // Callers returned their request/byte credit at submit; each allocated
    // generation keeps its slot, index and partition charges until published.
    assert_eq!((before.4.requests, before.4.bytes), (0, 0));
    assert_eq!(before.4.slots, 32);
    assert_eq!(before.4.partition_batches, 32);
    assert!(before.4.index_bytes > 0);
    // Owner populations are 7,7,6,6,6; max_batches=2 yields exactly
    // 4+4+3+3+3=17 WorkerItems, including the two one-member tails.
    for owner in 0..5 {
        let sequences: Vec<_> = (0..32u64).filter(|seq| *seq % 5 == owner as u64).collect();
        let expected: Vec<_> = sequences.chunks(2).map(<[u64]>::to_vec).collect();
        assert_eq!(complete_owner(&db, owner, &sequences), expected);
        assert_eq!(
            db.core.clients[owner]
                .accounting
                .formed
                .load(Ordering::Relaxed),
            expected.len() as u64
        );
    }
    db.drain().unwrap();
    settle(&db);
    assert_eq!(db.core.published_next(), 32);
    let charges = db.core.admission.charges();
    assert_eq!(
        (charges.slots, charges.bytes, charges.index_bytes),
        (0, 0, 0)
    );
    for key in 0..32u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    // Repeated immutable references become cache hits after complete verification.
    // Prime immediately before the assertion: set-associative cache collisions
    // during the preceding 32-key scan are legal and must not make this flaky.
    assert_eq!(value(&mut db, 0), Some(vec![0; 32]));
    let hits = db.read_cache_stats().hits;
    assert_eq!(value(&mut db, 0), Some(vec![0; 32]));
    assert!(db.read_cache_stats().hits > hits);
    let checkpoint = db.checkpoint().unwrap();
    db.write_batch(
        fixture.operation(3, 100),
        vec![
            put(0, 99),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: 1u32.to_be_bytes().to_vec().into(),
            },
        ],
    )
    .unwrap();
    assert_eq!(value(&mut db, 0), Some(vec![99; 32]));
    assert_eq!(value(&mut db, 1), None);
    for key in [0u32, 1] {
        assert_eq!(
            db.get_at(KeySpace(0), &key.to_be_bytes(), &checkpoint)
                .unwrap()
                .unwrap()
                .as_ref(),
            vec![key as u8; 32].as_slice()
        );
    }
    // The same original ID never creates a new version or overwrites newer state.
    let (operation, mutation, version) = &originals[0];
    let admissions = db.stats().admitted_batches;
    assert_eq!(
        db.submit(*operation, vec![mutation.clone()]).unwrap(),
        *version
    );
    assert_eq!(db.stats().admitted_batches, admissions);
    db.drain().unwrap();
    assert_eq!(value(&mut db, 0), Some(vec![99; 32]));
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 31);
    db.shutdown().unwrap();
    drop(db);
    // Client clones keep the live connection registry (and its lock) alive.
    drop(connections);
    let mut reopened = fixture.open();
    assert_eq!(reopened.epoch(), 2);
    assert_eq!(value(&mut reopened, 0), Some(vec![99; 32]));
    assert_eq!(value(&mut reopened, 1), None);
    for key in 2..32u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8; 32]));
    }
    assert_eq!(
        reopened.submit(*operation, vec![mutation.clone()]).unwrap(),
        *version
    );
    assert_eq!(reopened.stats().admitted_batches, 0);
    reopened.shutdown().unwrap();
}

/// Writes take no sparse-view quota: the publisher's standing budget, reserved
/// at open, covers every view install. With all remaining view quota taken
/// and a full window in flight, reserved view bytes stay constant and every
/// sparse replacement under writer → publisher still succeeds.
#[test]
fn full_window_in_flight_takes_no_view_quota_and_sparse_replacements_never_fail() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let mut reservations = Vec::new();
    loop {
        match db.core.index.reserve_publication_view(1) {
            Ok(reservation) => reservations.push(reservation),
            Err(super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }
    let reserved = db.core.index.stats().transient_metadata_bytes;
    let held_connection = Arc::clone(&db.core.clients[0].client);
    let held = held_connection.lock();
    let window = db.core.config.max_pending_batches - 1;
    let first = db
        .submit(fixture.operation(0, 1000), vec![put(1000, 1)])
        .unwrap();
    let mut later = Vec::with_capacity(window - 1);
    let mut reader_view = None;
    for n in 1..window as u64 {
        let owner = 1 + (n % 4) as u32;
        let key = 1000 + n as u32;
        let version = db
            .submit(fixture.operation(owner, 1000 + n), vec![put(key, n as u8)])
            .unwrap();
        later.push(version.sequence);
        assert_eq!(db.core.index.stats().transient_metadata_bytes, reserved);
        if n == window as u64 / 2 {
            settle_publication(&db, &later);
            // A reader-held replaced view keeps only its floor pin.
            reader_view = Some(db.core.index.call_cut());
        }
    }
    settle_publication(&db, &later);
    for &sequence in &later {
        assert!(db.core.is_published(BatchVersion {
            epoch: db.epoch(),
            sequence,
        }));
    }
    assert!(!db.core.is_published(first));
    assert!(matches!(reader_view, Some(IndexReadCut::Sparse(_))));
    assert!(matches!(db.core.index.call_cut(), IndexReadCut::Sparse(_)));
    assert_eq!(db.core.index.stats().transient_metadata_bytes, reserved);
    assert!(db.core.ensure_healthy().is_ok());
    drop(held);
    db.drain().unwrap();
    assert!(matches!(db.core.index.call_cut(), IndexReadCut::At(_)));
    drop(reader_view);
    assert_eq!(db.core.index.stats().transient_metadata_bytes, reserved);
    assert_eq!(value(&mut db, 1000), Some(vec![1; 32]));
    assert_eq!(
        value(&mut db, 1000 + window as u32 - 1),
        Some(vec![(window - 1) as u8; 32])
    );
    drop(reservations);
    db.shutdown().unwrap();
}

/// Public cuts retained across successive sparse publications take owner
/// charges from the index view allowance. When it is full, `published_cut`
/// fails with the index capacity error while writes keep publishing, and
/// dropping the cuts releases each charge exactly once.
#[test]
fn retained_published_cuts_are_charged_and_only_the_reader_fails_when_full() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let transient = |db: &RemoteDatabase| db.core.index.stats().transient_metadata_bytes;
    // Every sparse view of this publisher has max_pending_batches - 1
    // exclusion capacity, so one owner charge equals one such reservation.
    // Fill the allowance, then free room for exactly three owner charges.
    let max_excluded = db.core.config.max_pending_batches - 1;
    let mut reservations = Vec::new();
    loop {
        match db.core.index.reserve_publication_view(max_excluded) {
            Ok(reservation) => reservations.push(reservation),
            Err(super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }
    assert!(reservations.len() >= 3);
    let full = transient(&db);
    reservations.truncate(reservations.len() - 3);
    let base = transient(&db);
    let view = (full - base) / 3;
    let held_connection = Arc::clone(&db.core.clients[0].client);
    let held = held_connection.lock();
    let first = db
        .submit(fixture.operation(0, 2000), vec![put(2000, 1)])
        .unwrap();
    let mut later = Vec::new();
    let mut write = |db: &mut RemoteDatabase, n: u64| {
        let owner = 1 + (n % 4) as u32;
        let version = db
            .submit(
                fixture.operation(owner, 2000 + n),
                vec![put(2000 + n as u32, n as u8)],
            )
            .unwrap();
        later.push(version.sequence);
        settle_publication(db, &later);
    };
    let mut cuts = Vec::new();
    for n in 1..=3 {
        write(&mut db, n);
        let cut = db.published_cut().unwrap();
        assert!(matches!(cut, IndexReadCut::Sparse(_)));
        cuts.push(cut);
        assert_eq!(transient(&db), base + cuts.len() * view);
        assert_eq!(db.core.index.stats().reader_held_view_bytes, 0);
    }
    write(&mut db, 4);
    let error = db.published_cut().unwrap_err();
    assert!(error.message().contains("capacity exceeded"), "{error}");
    assert_eq!(error.uncertain_version, None);
    // Writes keep publishing after the reader-side failure.
    for n in 5..=8 {
        write(&mut db, n);
    }
    for &sequence in &later {
        assert!(db.core.is_published(BatchVersion {
            epoch: db.epoch(),
            sequence,
        }));
    }
    assert!(!db.core.is_published(first));
    assert!(db.core.ensure_healthy().is_ok());
    assert_eq!(transient(&db), base + 3 * view);
    // A clone keeps the shared charge; the last reference releases it once.
    let clone = cuts[0].clone();
    drop(cuts.remove(0));
    assert_eq!(transient(&db), base + 3 * view);
    drop(clone);
    assert_eq!(transient(&db), base + 2 * view);
    drop(cuts);
    assert_eq!(transient(&db), base);
    drop(held);
    db.drain().unwrap();
    assert!(matches!(db.core.index.call_cut(), IndexReadCut::At(_)));
    assert_eq!(value(&mut db, 2000), Some(vec![1; 32]));
    assert_eq!(value(&mut db, 2008), Some(vec![8; 32]));
    drop(reservations);
    db.shutdown().unwrap();
}

#[test]
fn active_checkpoint_capture_and_sparse_publication_have_separate_budgets() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(2, 50), vec![put(50, 50)])
        .unwrap();
    let max_excluded = db.core.config.max_pending_batches - 1;
    let mut reservations = Vec::new();
    loop {
        match db.core.index.reserve_publication_view(max_excluded) {
            Ok(reservation) => reservations.push(reservation),
            Err(super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }
    assert!(reservations.len() >= 3);
    let before_capture = db.core.index.stats().transient_metadata_bytes;
    let capture = db
        .core
        .index
        .writer()
        .prepare_image(IndexReadCut::Latest)
        .unwrap();
    assert!(db.core.index.stats().transient_metadata_bytes > before_capture);
    for _ in 0..3 {
        drop(reservations.pop().unwrap());
    }

    let blocked = Arc::clone(&db.core.clients[0].client);
    let guard = blocked.lock();
    let later_connection = Arc::clone(&db.core.clients[1].client);
    let later_guard = later_connection.lock();
    let first = db
        .submit(fixture.operation(0, 51), vec![put(51, 51)])
        .unwrap();
    let later = db
        .submit(fixture.operation(1, 52), vec![put(52, 52)])
        .unwrap();
    db.submit(fixture.operation(1, 53), vec![put(53, 53)])
        .unwrap();
    drop(later_guard);
    assert_eq!(
        complete_owner(&db, 1, &[later.sequence, later.sequence + 1]),
        vec![vec![later.sequence, later.sequence + 1]]
    );
    let first_published = db.core.is_published(first);
    let later_published = db.core.is_published(later);
    let later_value = value(&mut db, 52);
    let first_value = value(&mut db, 51);
    drop(guard);
    assert!(!first_published);
    assert!(later_published);
    assert_eq!(later_value, Some(vec![52; 32]));
    assert_eq!(first_value, None);
    drop(reservations);
    db.drain().unwrap();
    assert_eq!(value(&mut db, 51), Some(vec![51; 32]));
    drop(capture);
    assert!(
        db.core
            .index
            .writer()
            .cleanup_failed_capture_step(32)
            .unwrap()
    );
    db.shutdown().unwrap();
}

#[test]
fn nonempty_dependency_tables_shrink_while_a_prefix_holds_window_credit() {
    use crate::key_shape::KeyShapeBuilder;

    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("first", 4, 1, KeyType::uniform(1));
    builder.add_key_space("second", 4, 1, KeyType::uniform(1));
    let mut fixture = Fixture::new_with_shape(128, builder.build());
    fixture.config.max_pending_batches = 33;
    let mut db = fixture.open();
    let mutation = |space: u8, key: u32| Mutation::Put {
        keyspace: KeySpace(space),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![space + 1; 32].into(),
    };

    let first_client = Arc::clone(&db.core.clients[0].client);
    let first_guard = first_client.lock();
    let suffix_client = Arc::clone(&db.core.clients[1].client);
    let suffix_guard = suffix_client.lock();
    let sentinel = db
        .submit(
            fixture.operation(0, 100),
            vec![mutation(0, 0), mutation(1, 0)],
        )
        .unwrap();
    for key in 1..=32u32 {
        db.submit(
            fixture.operation(1, 100 + u64::from(key)),
            vec![mutation(0, key), mutation(1, key)],
        )
        .unwrap();
    }
    // One sharded registry covers both keyspaces: 33 batches × 2 keys.
    assert_eq!(db.core.pending_dependencies(), 66);
    assert!(db.core.dependency_capacity() > 4 * 2);
    assert_eq!(db.core.retained_slots(), 33);
    assert_eq!(worker_groups(&db, 1), 0);
    assert_eq!(worker_bytes(&db, 1), 0);
    let (head, next, entries) = db.core.clients[1].queue.snapshot();
    assert_eq!((head, next), (1, 33));
    assert_eq!(
        entries,
        (1..=32).map(|seq| (seq, seq, 2)).collect::<Vec<_>>()
    );
    drop(suffix_guard);
    let sequences: Vec<_> = (1..=32).collect();
    assert_eq!(
        complete_owner(&db, 1, &sequences),
        sequences.chunks(2).map(<[u64]>::to_vec).collect::<Vec<_>>()
    );
    // Key cleanup follows publication; the sentinel keeps one partition charge.
    wait_unpublished(&db, 1);
    assert!(!db.core.is_published(sentinel));
    assert_eq!(db.core.published_next(), 0);
    assert_eq!(db.core.retained_slots(), 33);
    assert_eq!(db.core.unresolved_slots(), 1);
    assert_eq!(db.core.pending_dependencies(), 2);
    assert!(db.core.dependency_capacity() <= 4 * db.core.pending_dependencies());
    assert!(
        db.submit(fixture.operation(2, 200), vec![mutation(0, 100)])
            .is_err()
    );
    assert!(db.core.ensure_healthy().is_ok());
    assert_eq!(db.core.next_sequence(), 33);
    drop(first_guard);
    db.drain().unwrap();
    settle(&db);
    assert_eq!(db.core.pending_dependencies(), 0);
    db.submit(fixture.operation(2, 200), vec![mutation(0, 100)])
        .unwrap();
    db.drain().unwrap();
    assert_eq!(value(&mut db, 100), Some(vec![1; 32]));
    db.shutdown().unwrap();
}

#[test]
fn sparse_publication_retains_bounded_window_until_earlier_prefix_publishes() {
    let mut fixture = Fixture::new();
    fixture.config.max_pending_batches = 4;
    let mut db = fixture.open();
    let blocked = Arc::clone(&db.core.clients[0].client);
    let guard = blocked.lock();
    let later_connection = Arc::clone(&db.core.clients[1].client);
    let later_guard = later_connection.lock();
    let first = fixture.operation(0, 0);
    let version = db.submit(first, vec![put(0, 1)]).unwrap();
    db.submit(fixture.operation(0, 1), vec![put(1, 2)]).unwrap();
    db.submit(fixture.operation(1, 2), vec![put(2, 3)]).unwrap();
    db.submit(fixture.operation(1, 3), vec![put(3, 4)]).unwrap();
    let credits = db.core.admission.charges();
    // The two later READY batches form one WorkerItem only after their
    // owner's lock is released; they are not authority-dispatched groups.
    assert_eq!(
        db.core.clients[1].queue.snapshot(),
        (1, 3, vec![(1, 2, 1), (2, 3, 1)])
    );
    assert_eq!(worker_groups(&db, 1), 0);
    assert_eq!(worker_bytes(&db, 1), 0);
    drop(later_guard);
    assert_eq!(complete_owner(&db, 1, &[2, 3]), vec![vec![2, 3]]);
    wait_unpublished(&db, 2);
    let prefix = db.core.published_next();
    let pending = db.core.admission.charges();
    let retained = db.core.retained_slots();
    let unpublished = db.core.unresolved_slots();
    let later_value = db.get(KeySpace(0), &2u32.to_be_bytes());
    let retry = db.submit(first, vec![put(0, 1)]);
    let conflicting_retry = db.submit(first, vec![put(0, 9)]);
    let excess = db.submit(fixture.operation(2, 4), vec![put(4, 5)]);
    let next = db.core.next_sequence();
    let healthy = db.core.ensure_healthy().is_ok();
    drop(guard);
    // Published batches return their index charge; their slots stay behind
    // the unresolved prefix, so the bounded window remains fully charged.
    assert_eq!(credits.slots, 4);
    assert!(credits.index_bytes > 0);
    assert_eq!(prefix, 0);
    assert_eq!(pending.slots, 4);
    assert!(pending.index_bytes < credits.index_bytes);
    assert_eq!(retained, 4);
    assert_eq!(unpublished, 2);
    assert_eq!(later_value.unwrap().unwrap().as_ref(), &[3; 32]);
    assert_eq!(retry.unwrap(), version);
    assert!(conflicting_retry.is_err());
    assert!(excess.is_err());
    assert_eq!(next, 4);
    assert!(healthy);
    db.drain().unwrap();
    settle(&db);
    assert_eq!(db.core.published_next(), 4);
    let charges = db.core.admission.charges();
    assert_eq!(
        (charges.bytes, charges.index_bytes, charges.slots),
        (0, 0, 0)
    );
    assert_eq!(db.core.retained_slots(), 0);
    assert_eq!(db.core.unresolved_slots(), 0);
    assert_eq!(db.core.pending_dependencies(), 0);
    assert_eq!(value(&mut db, 2), Some(vec![3; 32]));
    db.submit(fixture.operation(2, 4), vec![put(4, 5)]).unwrap();
    db.drain().unwrap();
    assert_eq!(value(&mut db, 4), Some(vec![5; 32]));
    db.shutdown().unwrap();
}

#[test]
fn conflicting_and_transitive_batches_wait_for_their_whole_predecessors() {
    for (transitive, harvest_before_last) in
        [(false, false), (false, true), (true, false), (true, true)]
    {
        let fixture = Fixture::new();
        let mut db = fixture.open();
        let connection = Arc::clone(&db.core.clients[0].client);
        let held = connection.lock();
        let last_connection = Arc::clone(&db.core.clients[2].client);
        let last_guard = last_connection.lock();
        let first = db
            .submit(fixture.operation(0, 10), vec![put(0, 1)])
            .unwrap();
        let middle = if transitive {
            vec![put(0, 2), put(1, 2), put(1, 3)]
        } else {
            vec![Mutation::Delete {
                keyspace: KeySpace(0),
                key: 0u32.to_be_bytes().to_vec().into(),
            }]
        };
        let second = db.submit(fixture.operation(1, 11), middle).unwrap();
        let last_key = if transitive { 1 } else { 0 };
        let third = db
            .submit(fixture.operation(2, 12), vec![put(last_key, 4)])
            .unwrap();
        // Fill both groups so their real durable replies are available without
        // dispatching or waiting on the blocked first owner.
        db.submit(fixture.operation(1, 13), vec![put(10, 10)])
            .unwrap();
        if harvest_before_last {
            // Force owner 1's real receipts to be processed before the next
            // submit; its worker has consumed both READY queue entries.
            settle_publication(&db, &[second.sequence, second.sequence + 2]);
            assert!(db.core.clients[1].queue.snapshot().2.is_empty());
        }
        db.submit(fixture.operation(2, 14), vec![put(11, 11)])
            .unwrap();
        drop(last_guard);
        // Wait for all four receipts without flushing the blocked first owner.
        settle_publication(&db, &[1, 2, 3, 4]);
        let before = (
            db.core.is_published(first),
            db.core.is_published(second),
            db.core.is_published(third),
            db.get(KeySpace(0), &0u32.to_be_bytes()),
            db.get(KeySpace(0), &1u32.to_be_bytes()),
            db.get(KeySpace(0), &10u32.to_be_bytes()),
            db.get(KeySpace(0), &11u32.to_be_bytes()),
        );
        drop(held);
        assert!(!before.0 && !before.1 && !before.2);
        assert!(before.3.unwrap().is_none());
        assert!(before.4.unwrap().is_none());
        assert_eq!(before.5.unwrap().unwrap().as_ref(), &[10; 32]);
        assert_eq!(before.6.unwrap().unwrap().as_ref(), &[11; 32]);
        db.drain().unwrap();
        assert_eq!(
            value(&mut db, 0),
            Some(vec![if transitive { 2 } else { 4 }; 32])
        );
        if transitive {
            assert_eq!(value(&mut db, 1), Some(vec![4; 32]));
        }
        settle(&db);
        assert_eq!(db.core.retained_slots(), 0);
        assert_eq!(db.core.unresolved_slots(), 0);
        db.shutdown().unwrap();
    }
}

#[test]
fn distinct_keys_with_same_native_reduction_cannot_publish_out_of_order() {
    use crate::key_shape::{KeyIndexing, KeySpaceConfig};
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::Reduction(8, 0..4),
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new(),
    );
    let fixture = Fixture::new_with_shape(128, shape);
    let mut db = fixture.open();
    let connection = Arc::clone(&db.core.clients[0].client);
    let held = connection.lock();
    let later_connection = Arc::clone(&db.core.clients[1].client);
    let later_guard = later_connection.lock();
    let mutation = |key: [u8; 8], value: u8| Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_vec().into(),
        value: vec![value; 32].into(),
    };
    let first_key = [0, 0, 0, 1, 0, 0, 0, 1];
    let second_key = [0, 0, 0, 1, 0, 0, 0, 2];
    let other_key = [0, 0, 0, 2, 0, 0, 0, 0];
    db.submit(fixture.operation(0, 20), vec![mutation(first_key, 1)])
        .unwrap();
    let second = db
        .submit(fixture.operation(1, 21), vec![mutation(second_key, 2)])
        .unwrap();
    db.submit(fixture.operation(1, 22), vec![mutation(other_key, 3)])
        .unwrap();
    drop(later_guard);
    assert_eq!(
        complete_owner(&db, 1, &[second.sequence, second.sequence + 1]),
        vec![vec![second.sequence, second.sequence + 1]]
    );
    let second_published = db.core.is_published(second);
    let independent = db.get(KeySpace(0), &other_key);
    drop(held);
    assert!(!second_published);
    assert_eq!(independent.unwrap().unwrap().as_ref(), &[3; 32]);
    db.drain().unwrap();
    assert_eq!(
        db.get(KeySpace(0), &second_key).unwrap().unwrap().as_ref(),
        &[2; 32]
    );
    db.shutdown().unwrap();
}

#[test]
fn lost_append_response_blocks_cached_reads_and_reopen_recovers_uncertain_group() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.submit(fixture.operation(0, 0), vec![put(0, 1)]).unwrap();
    db.submit(fixture.operation(0, 1), vec![put(1, 2)]).unwrap();
    db.drain().unwrap();
    assert_eq!(value(&mut db, 0), Some(vec![1; 32]));
    assert_eq!(value(&mut db, 0), Some(vec![1; 32]));
    assert!(db.read_cache_stats().hits > 0);
    assert_eq!(db.core.published_next(), 2);
    // The owner-2 frame reaches its WAL file but its completion reports EIO:
    // the outcome is unknown to the authority, as for a lost reply.
    let connection = Arc::clone(&db.core.clients[2].client);
    let mut held = connection.lock();
    held.leased_test_fail_write(-libc::EIO);
    let operation = fixture.operation(2, 2);
    let version = db.submit(operation, vec![put(2, 3)]).unwrap();
    db.submit(fixture.operation(2, 3), vec![put(3, 4)]).unwrap();
    drop(held);
    assert!(db.drain().is_err());
    assert!(db.core.ensure_healthy().is_err());
    assert_eq!(db.core.published_next(), 2);
    // UNKNOWN generations never return their slot or index charge.
    let charges = db.core.admission.charges();
    assert!(charges.slots > 0 && charges.index_bytes > 0);
    // A verified hot-cache record cannot let an unhealthy authority keep serving.
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.get_profiled(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.submit(operation, vec![put(2, 3)]).is_err());
    assert!(db.shutdown().is_err());
    drop(db);
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connection);
    let mut reopened = fixture.open();
    for key in 0..4u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8 + 1; 32]));
    }
    assert_eq!(
        reopened.submit(operation, vec![put(2, 3)]).unwrap(),
        version
    );
    assert_eq!(reopened.stats().admitted_batches, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn observed_stale_epoch_read_stops_authority_before_any_later_cache_hit() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.submit(fixture.operation(0, 0), vec![put(0, 1)]).unwrap();
    // Key 1 lives on another owner: on the leased route a read is uncached
    // (needs an owner exchange) only while its file grant is not cached, and
    // reading key 0 below caches owner 0's grant.
    db.submit(fixture.operation(1, 1), vec![put(1, 2)]).unwrap();
    db.drain().unwrap();
    db.clear_read_cache();
    assert_eq!(value(&mut db, 0), Some(vec![1; 32]));
    assert_eq!(value(&mut db, 0), Some(vec![1; 32]));
    assert!(db.read_cache_stats().hits > 0);

    // Simulate a competing authority becoming known to this storage node. The
    // authority has no background lease protocol: this test begins its safety
    // assertion when an uncached read actually observes that its epoch is stale.
    let next_epoch = db.epoch() + 1;
    fixture.fence_owner(1, next_epoch);
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert!(db.core.ensure_healthy().is_err());
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.get_profiled(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.shutdown().is_err());
}

#[test]
fn drain_refills_a_ready_node_while_the_first_logical_group_is_stalled() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let first_connection = Arc::clone(&db.core.clients[0].client);
    let later_connection = Arc::clone(&db.core.clients[1].client);
    let first_guard = first_connection.lock();
    let later_guard = later_connection.lock();
    for key in 0..6u32 {
        let node = if key < 2 { 0 } else { 1 };
        db.submit(
            fixture.operation(node, u64::from(key)),
            vec![put(key, key as u8)],
        )
        .unwrap();
    }
    assert_eq!(
        db.core.clients[0].queue.snapshot(),
        (1, 3, vec![(1, 0, 1), (2, 1, 1)])
    );
    assert_eq!(
        db.core.clients[1].queue.snapshot(),
        (1, 5, vec![(1, 2, 1), (2, 3, 1), (3, 4, 1), (4, 5, 1)])
    );
    assert_eq!((worker_groups(&db, 0), worker_groups(&db, 1)), (0, 0));
    assert_eq!(db.core.published_next(), 0);
    drop(later_guard);
    // The worker drains both ready pairs without an authority refill pass.
    // Owner 0 is still locked throughout both real completion callbacks.
    assert_eq!(
        complete_owner(&db, 1, &[2, 3, 4, 5]),
        vec![vec![2, 3], vec![4, 5]]
    );
    assert_eq!(
        db.core.clients[1].accounting.formed.load(Ordering::Relaxed),
        2
    );
    assert_eq!(db.core.published_next(), 0);
    for key in 2..6u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    let (entered, entering) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().drain_wait_entered = Some(entered);
    let (finished, completion) = mpsc::sync_channel(1);
    let drainer = thread::spawn(move || {
        let result = db.drain();
        assert!(finished.send((db, result)).is_ok());
    });
    // The signal comes from drain's unresolved-work completion wait, not
    // thread startup. Owner 0 remains locked throughout this observation.
    entering.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        matches!(completion.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "drain must not acknowledge beyond the unresolved prefix"
    );
    drop(first_guard);
    let (mut db, result) = completion.recv_timeout(Duration::from_secs(10)).unwrap();
    drainer.join().unwrap();
    result.unwrap();
    assert_eq!(db.core.published_next(), 6);
    settle(&db);
    let charges = db.core.admission.charges();
    assert_eq!(
        (charges.bytes, charges.slots, charges.index_bytes),
        (0, 0, 0)
    );
    for key in 0..6u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
}

#[test]
fn lone_worker_append_keeps_durability_and_unknown_response_recovery_rules() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let first_operation = fixture.operation(0, 0);
    let first_version = db.write_batch(first_operation, vec![put(0, 17)]).unwrap();
    // Acquire the worker lock after the callback before sampling released
    // transport credits; receipt delivery can precede GroupCompletion::drop.
    let connection = Arc::clone(&db.core.clients[0].client);
    let settled = connection.lock();
    // A lone batch uses the same worker queue and one-member WorkerItem as
    // every other write; the removed direct authority path has no counter.
    assert_eq!(
        db.core.clients[0].accounting.formed.load(Ordering::Relaxed),
        1
    );
    assert_eq!(db.core.clients[0].queue.snapshot(), (2, 2, vec![]));
    assert_eq!(worker_groups(&db, 0), 0);
    assert_eq!(worker_bytes(&db, 0), 0);
    settle(&db);
    let charges = db.core.admission.charges();
    assert_eq!(
        (charges.bytes, charges.slots, charges.index_bytes),
        (0, 0, 0)
    );
    assert_eq!(db.core.published_next(), 1);
    assert!(db.core.ensure_healthy().is_ok());
    drop(settled);
    assert_eq!(value(&mut db, 0), Some(vec![17; 32]));
    assert_eq!(value(&mut db, 0), Some(vec![17; 32]));
    assert!(db.read_cache_stats().hits > 0);

    // An unknown worker write outcome (frame written, completion EIO) cannot
    // refund the batch or keep cached reads serving, exactly as for a
    // multi-member frame.
    db.core.clients[1]
        .client
        .lock()
        .leased_test_fail_write(-libc::EIO);
    let uncertain_operation = fixture.operation(1, 1);
    let uncertain_version = db.submit(uncertain_operation, vec![put(1, 231)]).unwrap();
    assert_eq!(
        db.core.clients[0].accounting.formed.load(Ordering::Relaxed),
        1
    );
    let error = db.drain().unwrap_err();
    assert_eq!(error.uncertain_version, Some(uncertain_version));
    assert_eq!(
        db.core.clients[1].accounting.formed.load(Ordering::Relaxed),
        1
    );
    assert!(db.core.ensure_healthy().is_err());
    assert_eq!(db.core.published_next(), 1);
    // UNKNOWN retains publication/admission charges; the worker has consumed
    // owner generation 1 into its failed frame.
    assert_eq!(db.core.clients[1].queue.snapshot(), (2, 2, vec![]));
    let charges = db.core.admission.charges();
    assert!(charges.slots > 0 && charges.index_bytes > 0);
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.submit(uncertain_operation, vec![put(1, 231)]).is_err());
    assert!(db.shutdown().is_err());
    drop(db);
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connection);

    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 0), Some(vec![17; 32]));
    assert_eq!(value(&mut reopened, 1), Some(vec![231; 32]));
    assert_eq!(
        reopened.submit(first_operation, vec![put(0, 17)]).unwrap(),
        first_version
    );
    assert_eq!(
        reopened
            .submit(uncertain_operation, vec![put(1, 231)])
            .unwrap(),
        uncertain_version
    );
    assert_eq!(reopened.stats().admitted_batches, 0);
    assert_eq!(reopened.pipeline_stats().append_requests, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn submit_many_preserves_order_retries_and_complete_recovery() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let known = fixture.operation(0, 0);
    let active = fixture.operation(2, 1);
    let first = fixture.operation(1, 2);
    let last = fixture.operation(3, 3);
    let known_version = db.write_batch(known, vec![put(0, 7)]).unwrap();
    let active_version = db.submit(active, vec![put(1, 8)]).unwrap();
    let window = vec![
        (known, vec![put(0, 7)]),
        (first, vec![put(2, 9)]),
        (active, vec![put(1, 8)]),
        (first, vec![put(2, 9)]),
        (last, vec![put(3, 10)]),
    ];
    let versions = db.submit_many(&window).unwrap();
    assert_eq!(versions[0], known_version);
    assert_eq!(versions[2], active_version);
    assert_eq!(versions[1], versions[3]);
    assert_eq!(
        versions.iter().map(|v| v.sequence).collect::<Vec<_>>(),
        [0, 2, 1, 2, 3]
    );
    assert_eq!(db.stats().admitted_batches, 4);
    db.drain().unwrap();
    settle(&db);
    assert_eq!(db.core.admission.charges().bytes, 0);
    assert_eq!(db.core.published_next(), 4);
    for key in 0..4 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8 + 7; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
    let reopened = fixture.open();
    assert_eq!(reopened.submit_many(&window).unwrap(), versions);
    assert_eq!(reopened.stats().admitted_batches, 0);
    assert_eq!(reopened.pipeline_stats().append_requests, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn submit_many_rejects_late_conflicts_without_admitting_a_prefix() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let known = fixture.operation(0, 0);
    let next = fixture.operation(1, 1);
    db.write_batch(known, vec![put(0, 21)]).unwrap();
    let appends = db.pipeline_stats().append_requests;
    assert!(
        db.submit_many(&[(next, vec![put(1, 22)]), (known, vec![put(0, 99)])])
            .is_err()
    );
    assert!(
        db.submit_many(&[(next, vec![put(1, 22)]), (next, vec![put(1, 23)])])
            .is_err()
    );
    // A later malformed key also fails before encoding/admission can allocate.
    assert!(
        db.submit_many(&[
            (next, vec![put(1, 22)]),
            (
                fixture.operation(3, 2),
                vec![Mutation::Put {
                    keyspace: KeySpace(0),
                    key: Bytes::from(vec![1]),
                    value: Bytes::from(vec![2]),
                }]
            )
        ])
        .is_err()
    );
    assert!(db.core.ensure_healthy().is_ok());
    assert_eq!(db.core.next_sequence(), 1);
    assert_eq!(db.pipeline_stats().append_requests, appends);
    assert_eq!(db.core.admission.charges().bytes, 0);
    assert_eq!(value(&mut db, 0), Some(vec![21; 32]));
    assert_eq!(value(&mut db, 1), None);
    assert_eq!(db.write_batch(next, vec![put(1, 22)]).unwrap().sequence, 1);
    db.shutdown().unwrap();
}

#[test]
fn preparation_budget_and_lost_reply_keep_the_existing_failure_contract() {
    let fixture = Fixture::new();
    let window = vec![
        (fixture.operation(0, 0), vec![put(0, 41)]),
        (fixture.operation(0, 1), vec![put(1, 42)]),
    ];
    // The core's configuration is immutable: open once with the smallest byte
    // budget open accepts (one replay group per owner) and submit a window
    // whose whole-window credit exceeds it, proving rejection precedes
    // encoding and allocation.
    let mut tight = fixture.config.clone();
    tight.max_pending_bytes = tight.storage.group_limits.max_encoded_bytes * tight.endpoints.len();
    let oversized: Vec<_> = (0..64u64)
        .map(|seed| (fixture.operation(0, 100 + seed), vec![put(seed as u32, 41)]))
        .collect();
    let db = fixture.try_open_with(tight).unwrap();
    let error = db.submit_many(&oversized).unwrap_err();
    assert!(
        error.to_string().contains("admission byte budget"),
        "{error}"
    );
    assert_eq!(db.pipeline_stats().prepare_calls, 0);
    assert_eq!(db.core.next_sequence(), 0);
    assert_eq!(db.core.retained_slots(), 0);
    db.shutdown().unwrap();
    drop(db);
    let mut db = fixture.open();
    let connection = Arc::clone(&db.core.clients[0].client);
    let mut held = connection.lock();
    held.leased_test_fail_write(-libc::EIO);
    let versions = db.submit_many(&window).unwrap();
    drop(held);
    assert!(db.drain().is_err());
    assert!(db.core.ensure_healthy().is_err());
    assert!(db.core.admission.charges().slots > 0);
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.shutdown().is_err());
    drop(db);
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connection);
    let mut reopened = fixture.open();
    assert_eq!(reopened.submit_many(&window).unwrap(), versions);
    assert_eq!(reopened.stats().admitted_batches, 0);
    assert_eq!(value(&mut reopened, 0), Some(vec![41; 32]));
    assert_eq!(value(&mut reopened, 1), Some(vec![42; 32]));
    reopened.shutdown().unwrap();
}

#[test]
fn paused_caller_does_not_stall_owner_progress_or_admit_invalid_chunk() {
    let fixture = Fixture::new();
    let db = fixture.open();
    let connection = Arc::clone(&db.core.clients[0].client);
    let guard = connection.lock();
    for key in 0..6u32 {
        db.submit(
            fixture.operation(0, u64::from(key)),
            vec![put(key, key as u8)],
        )
        .unwrap();
    }
    let before = db.core.clients[0].queue.snapshot();
    assert_eq!((worker_groups(&db, 0), worker_bytes(&db, 0)), (0, 0));
    // A caller holding a granted retry window is paused after its identity
    // check. Owner workers form and submit groups without any caller.
    let (entered, entering) = mpsc::sync_channel(1);
    let (resume, resuming) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().after_grant = Some((entered, resuming));
    let retry = vec![(fixture.operation(0, 0), vec![put(0, 0)])];
    // The conflicting identity is caught by the retry digest check, before any
    // allocation: no member of this chunk may be admitted.
    let next = vec![
        (fixture.operation(1, 7), vec![put(7, 7)]),
        (fixture.operation(0, 0), vec![put(0, 99)]),
    ];
    let endpoint = &fixture.config.endpoints[0];
    let mut tls = fixture.config.tls.clone();
    tls.server_name = endpoint.server_name.clone();
    let mut observer = Client::connect(
        &endpoint.address,
        &tls,
        Duration::from_secs(5),
        fixture.config.partition_config(1, endpoint.partition),
        &fixture.shape,
    )
    .unwrap();
    let (finished, completion) = mpsc::sync_channel(1);
    let preparer = thread::spawn(move || {
        let retried = db.submit_many(&retry);
        let invalid = db.submit_many(&next);
        assert!(finished.send((db, retried, invalid)).is_ok());
    });
    let entered_in_time = entering.recv_timeout(Duration::from_secs(10)).is_ok();
    drop(guard);
    let deadline = Instant::now() + Duration::from_secs(10);
    let refilled = loop {
        if observer.stats().unwrap().appended_groups == 3 {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        thread::yield_now();
    };
    // Always release the explicit gate before checking assertions.
    resume.send(()).unwrap();
    let (mut db, retried, result) = completion.recv_timeout(Duration::from_secs(10)).unwrap();
    preparer.join().unwrap();
    // Six READY batches wait in the owner ring; the worker forms three pairs.
    assert_eq!(
        before,
        (1, 7, (0..6).map(|seq| (seq + 1, seq, 1)).collect())
    );
    assert!(entered_in_time);
    assert!(
        refilled,
        "owner workers must make progress while a caller is paused"
    );
    assert_eq!(retried.unwrap()[0].sequence, 0);
    assert!(result.is_err());
    assert_eq!(
        db.core.next_sequence(),
        6,
        "invalid chunk cannot allocate logical slots"
    );
    assert!(db.core.ensure_healthy().is_ok());
    db.drain().unwrap();
    assert_eq!(value(&mut db, 7), None);
    for key in 0..6u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connection);
    let mut reopened = fixture.open();
    assert_eq!(value(&mut reopened, 7), None);
    for key in 0..6u32 {
        let version = reopened
            .submit(
                fixture.operation(0, u64::from(key)),
                vec![put(key, key as u8)],
            )
            .unwrap();
        assert_eq!(version.sequence, u64::from(key));
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8; 32]));
    }
    assert_eq!(reopened.stats().admitted_batches, 0);
    reopened.shutdown().unwrap();
}

/// Room for several frames in one leased extent: frames in flight together
/// must share an extent, and the default 4 KiB fragment holds a single frame.
fn frame_room(config: &mut RemoteDatabaseConfig) {
    config.storage.fragment_bytes = 64 * 1024;
    config.storage.wal_file_bytes = 256 * 1024;
}

/// Hold owner `owner`'s first leased frame completion, so its prefix (and,
/// in frame order, every later receipt) stays unacknowledged; returns the
/// write CQEs reaped before any test frame.
fn hold_first_frame(db: &RemoteDatabase, owner: usize) -> u64 {
    let mut client = db.core.clients[owner].client.lock();
    client.leased_test_hold_completions(&[1]);
    client.leased_test_reaped_writes()
}

/// Wait until `frames` more write CQEs were reaped on `owner`: those frames
/// are in its WAL file, held completions included.
fn wait_frames_written(db: &RemoteDatabase, owner: usize, before: u64, frames: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if db.core.clients[owner]
            .client
            .lock()
            .leased_test_reaped_writes()
            >= before + frames
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::yield_now();
    }
}

fn release_frames(db: &RemoteDatabase, owner: usize) {
    db.core.clients[owner]
        .client
        .lock()
        .leased_test_release_held();
}

#[test]
fn same_shard_window_appends_successor_before_prefix_is_durable() {
    let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    let fixture = Fixture::new_with_config(128, shape, 2, frame_room).synchronous();
    let mut db = fixture.open();
    // The first frame's completion is held: its prefix is not durable.
    let reaped = hold_first_frame(&db, 0);
    let operations: Vec<_> = (0..4u64).map(|seed| fixture.operation(0, seed)).collect();
    let connection = Arc::clone(&db.core.clients[0].client);
    let first_guard = connection.lock();
    for (key, operation) in operations.iter().copied().enumerate().take(2) {
        db.submit(operation, vec![put(key as u32, key as u8)])
            .unwrap();
    }
    drop(first_guard);
    assert!(wait_frames_written(&db, 0, reaped, 1));
    let later_guard = connection.lock();
    for (key, operation) in operations.iter().copied().enumerate().skip(2) {
        db.submit(operation, vec![put(key as u32, key as u8)])
            .unwrap();
    }
    drop(later_guard);
    drop(connection);

    assert!(
        wait_frames_written(&db, 0, reaped, 2),
        "successor must reach the owner WAL during the stall"
    );
    assert_eq!(worker_groups(&db, 0), 2);
    wait_for_flights(&db, 0, 2);
    assert_eq!(db.core.clients[0].queue.snapshot(), (5, 5, vec![]));
    assert_worker_bytes(&db, 0, 2, &[0, 1, 2, 3]);
    assert_eq!(db.core.published_next(), 0);
    assert!(
        (0..4).all(|sequence| db.core.durable_receipt(sequence).is_none()),
        "the later append must not make a response while its prefix is stalled"
    );
    assert!((0..4u32).all(|key| {
        db.core
            .index
            .get(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .is_none()
    }));

    release_frames(&db, 0);
    assert_eq!(
        complete_owner(&db, 0, &[0, 1, 2, 3]),
        vec![vec![0, 1], vec![2, 3]]
    );
    db.drain().unwrap();
    settle(&db);
    assert_eq!(db.core.published_next(), 4);
    assert_eq!(db.core.admission.charges().slots, 0);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
}

#[test]
fn tail_flush_dispatches_third_group_before_prior_durability() {
    let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    let fixture = Fixture::new_with_config(128, shape, 1, frame_room).synchronous();
    let mut db = fixture.open();
    let reaped = hold_first_frame(&db, 0);
    let operations: Vec<_> = (0..3u64)
        .map(|seed| fixture.operation(0, 800 + seed))
        .collect();
    db.submit(operations[0], vec![put(800, 0)]).unwrap();
    assert!(wait_frames_written(&db, 0, reaped, 1));
    db.submit(operations[1], vec![put(801, 1)]).unwrap();
    db.submit(operations[2], vec![put(802, 2)]).unwrap();
    let reached_owner = wait_frames_written(&db, 0, reaped, 3);
    let in_flight = worker_groups(&db, 0);
    wait_for_flights(&db, 0, 3);
    assert_worker_bytes(&db, 0, 3, &[0, 1, 2]);
    let acknowledgements_held = (0..3).all(|sequence| db.core.durable_receipt(sequence).is_none());
    let published = db.core.published_next();
    release_frames(&db, 0);
    assert!(
        reached_owner,
        "third group must reach the owner WAL while the prefix is not durable"
    );
    assert_eq!(in_flight, 3);
    assert!(acknowledgements_held);
    assert_eq!(published, 0);
    assert_eq!(
        complete_owner(&db, 0, &[0, 1, 2]),
        vec![vec![0], vec![1], vec![2]]
    );
    db.drain().unwrap();
    for key in 800..803u32 {
        assert_eq!(value(&mut db, key), Some(vec![(key - 800) as u8; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    for key in 800..803u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![(key - 800) as u8; 32]));
    }
    reopened.shutdown().unwrap();
}

/// The leased frame window (`live_nvme.leased_write_window`) bounds frames in
/// flight; groups beyond it stay staged in the owner queue.
#[test]
fn tail_flush_respects_bounded_leased_frame_window() {
    const WINDOW: usize = 4;
    let shape = KeyShape::new_single(4, 1, KeyType::uniform(1));
    let mut fixture = Fixture::new_with_config(128, shape, 1, frame_room).synchronous();
    fixture.config.owner_append_occupancy = WINDOW;
    fixture.config.live_nvme.leased_write_window = Some(WINDOW);
    let db = fixture.open();
    let reaped = hold_first_frame(&db, 0);
    let count = WINDOW + 4;
    db.submit(fixture.operation(0, 900), vec![put(900, 0)])
        .unwrap();
    assert!(wait_frames_written(&db, 0, reaped, 1));
    for key in 1..count {
        db.submit(
            fixture.operation(0, 900 + key as u64),
            vec![put(900 + key as u32, key as u8)],
        )
        .unwrap();
    }
    wait_for_flights(&db, 0, WINDOW);
    let in_flight = worker_groups(&db, 0);
    let (head, next, queued) = db.core.clients[0].queue.snapshot();
    let staged = queued.len();
    assert_eq!(head, in_flight as u64 + 1);
    assert_eq!(next, count as u64 + 1);
    assert_eq!(
        queued,
        (in_flight as u64..count as u64)
            .map(|seq| (seq + 1, seq, 1))
            .collect::<Vec<_>>()
    );
    assert_worker_bytes(
        &db,
        0,
        in_flight,
        &(0..in_flight as u64).collect::<Vec<_>>(),
    );
    release_frames(&db, 0);
    assert_eq!(in_flight, WINDOW);
    assert_eq!(staged, count - WINDOW);
    let sequences: Vec<_> = (0..count as u64).collect();
    assert_eq!(
        complete_owner(&db, 0, &sequences),
        sequences.iter().map(|&seq| vec![seq]).collect::<Vec<_>>()
    );
    db.drain().unwrap();
    db.shutdown().unwrap();
}

#[test]
fn large_groups_bypass_small_tail_window_but_respect_admission_cap() {
    let fixture = Fixture::new_with_config(
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        1,
        |config| {
            config.storage.batch_limits.max_encoded_bytes = 40 * 1024;
            config.storage.group_limits.max_encoded_bytes = 64 * 1024 - 8;
            config.storage.fragment_bytes = 64 * 1024;
            config.storage.wal_file_bytes = 256 * 1024;
            config.max_pending_batches = 4;
        },
    );
    let mut db = fixture.open();
    let connection = Arc::clone(&db.core.clients[0].client);
    let held = connection.lock();
    let mutations = |key: u32| {
        vec![
            Mutation::Put {
                keyspace: KeySpace(0),
                key: key.to_be_bytes().to_vec().into(),
                value: vec![key as u8; 33 * 1024].into(),
            },
            put(100 + key, key as u8),
        ]
    };
    let mut versions = Vec::new();
    for key in 0..4u32 {
        versions.push(
            db.submit(fixture.operation(0, u64::from(key)), mutations(key))
                .unwrap(),
        );
    }
    // The held client prevents worker grouping. Admission still bounds four
    // whole two-key batches before any transport-side effect.
    let queued = db.core.clients[0].queue.snapshot();
    assert_eq!(
        queued,
        (1, 5, (0..4).map(|seq| (seq + 1, seq, 2)).collect())
    );
    assert_eq!((worker_groups(&db, 0), worker_bytes(&db, 0)), (0, 0));
    assert_eq!(db.core.published_next(), 0);
    let charges = db.core.admission.charges();
    let next = db.core.next_sequence();
    let error = db
        .submit(fixture.operation(0, 4), mutations(4))
        .unwrap_err();
    assert!(error.uncertain_version.is_none());
    assert!(db.core.ensure_healthy().is_ok());
    assert_eq!(db.core.next_sequence(), next);
    assert_eq!(db.core.admission.charges(), charges);
    assert_eq!(db.core.clients[0].queue.snapshot(), queued);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, key), None);
        assert_eq!(value(&mut db, 100 + key), None);
    }
    drop(held);
    db.drain().unwrap();
    assert_eq!(
        db.core.clients[0].accounting.formed.load(Ordering::Relaxed),
        4
    );
    assert!(
        db.core.clients[0]
            .accounting
            .peak_groups
            .load(Ordering::Relaxed)
            <= db.core.config.owner_append_occupancy
    );
    settle(&db);
    assert_eq!(db.core.admission.charges().bytes, 0);
    assert_eq!(db.core.published_next(), 4);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 33 * 1024]));
        assert_eq!(value(&mut db, 100 + key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
    // A client clone keeps the live connection registry (and its lock) alive.
    drop(connection);
    let mut reopened = fixture.open();
    for key in 0..4u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8; 33 * 1024]));
        assert_eq!(value(&mut reopened, 100 + key), Some(vec![key as u8; 32]));
        assert_eq!(
            reopened
                .submit(fixture.operation(0, u64::from(key)), mutations(key))
                .unwrap(),
            versions[key as usize]
        );
    }
    assert_eq!(value(&mut reopened, 4), None);
    assert_eq!(value(&mut reopened, 104), None);
    reopened.shutdown().unwrap();
}

#[test]
fn busy_owner_coalesces_staged_whole_batches_and_other_owner_progresses() {
    let fixture =
        Fixture::new_with_shape_and_group(128, KeyShape::new_single(4, 1, KeyType::uniform(1)), 4);
    let mut db = fixture.open();
    let connection = Arc::clone(&db.core.clients[0].client);
    let held = connection.lock();
    let other_connection = Arc::clone(&db.core.clients[1].client);
    let other_guard = other_connection.lock();
    let mut versions = Vec::new();
    for key in 0..11u32 {
        versions.push(
            db.submit(
                fixture.operation(0, u64::from(key)),
                vec![
                    put(key, key as u8),
                    put(100 + key, key as u8),
                    put(300, key as u8),
                ],
            )
            .unwrap(),
        );
    }
    for key in 0..4u32 {
        db.submit(
            fixture.operation(1, 20 + u64::from(key)),
            vec![put(200 + key, key as u8)],
        )
        .unwrap();
    }
    // All eleven whole batches remain READY until the worker owns its client.
    // With max_batches=4 it will form [0..4), [4..8), [8..11), not authority
    // groups plus three staged batches.
    assert_eq!(
        db.core.clients[0].queue.snapshot(),
        (1, 12, (0..11).map(|seq| (seq + 1, seq, 3)).collect())
    );
    assert_eq!(
        db.core.clients[1].queue.snapshot(),
        (1, 5, (11..15).map(|seq| (seq - 10, seq, 1)).collect())
    );
    assert_eq!((worker_groups(&db, 0), worker_groups(&db, 1)), (0, 0));
    assert_eq!((worker_bytes(&db, 0), worker_bytes(&db, 1)), (0, 0));
    drop(other_guard);
    assert_eq!(
        complete_owner(&db, 1, &[11, 12, 13, 14]),
        vec![vec![11, 12, 13, 14]]
    );
    assert_eq!(db.core.published_next(), 0);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, 200 + key), Some(vec![key as u8; 32]));
    }
    for key in 0..11u32 {
        assert_eq!(value(&mut db, key), None);
        assert_eq!(value(&mut db, 100 + key), None);
    }
    assert_eq!(value(&mut db, 300), None);
    // Pause owner 0's worker before its first publication pass, then observe
    // pinned cuts while its groups publish: a batch is visible whole or not
    // at all, and the key-300 chain makes visibility a prefix of 0..11.
    let (entered, entering) = mpsc::sync_channel(1);
    let (resume, resuming) = mpsc::sync_channel(1);
    db.core.test_hooks.lock().before_publish = Some((entered, resuming));
    drop(held);
    entering.recv_timeout(Duration::from_secs(10)).unwrap();
    let visible_prefix = |db: &RemoteDatabase| {
        let (cut, pin) = db.protected_published_cut().unwrap();
        let visible: Vec<bool> = (0..11u32)
            .map(|key| {
                let reference = |key: u32| {
                    db.index_reference(KeySpace(0), &key.to_be_bytes(), cut.clone())
                        .unwrap()
                        .is_some()
                };
                let first = reference(key);
                assert_eq!(first, reference(100 + key), "batch visible only whole");
                first
            })
            .collect();
        drop(pin);
        assert!(
            visible.windows(2).all(|pair| pair[0] || !pair[1]),
            "same-key successors stay invisible until predecessors publish"
        );
        visible.iter().filter(|visible| **visible).count()
    };
    let paused = visible_prefix(&db);
    let paused_published = db.core.published_next();
    resume.send(()).unwrap();
    assert_eq!(paused, 0);
    assert_eq!(paused_published, 0);
    visible_prefix(&db);
    assert_eq!(
        complete_owner(&db, 0, &(0..11).collect::<Vec<_>>()),
        vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7], vec![8, 9, 10]]
    );
    assert_eq!(visible_prefix(&db), 11);
    assert_eq!(db.core.clients[0].queue.snapshot(), (12, 12, vec![]));
    assert_eq!(
        db.core.clients[0].accounting.formed.load(Ordering::Relaxed),
        3
    );
    assert_eq!(
        db.core.clients[1].accounting.formed.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        db.core.clients[0]
            .accounting
            .peak_groups
            .load(Ordering::Relaxed),
        3
    );
    assert!(
        db.core.clients[0]
            .accounting
            .peak_groups
            .load(Ordering::Relaxed)
            <= db.core.config.owner_append_occupancy
    );
    assert!(worker_bytes(&db, 0) <= super::super::tcp::APPEND_SESSION_BYTES - 64 * 1024);
    db.drain().unwrap();
    assert_eq!(db.core.published_next(), 15);
    for key in 0..11u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
        assert_eq!(value(&mut db, 100 + key), Some(vec![key as u8; 32]));
    }
    assert_eq!(value(&mut db, 300), Some(vec![10; 32]));
    db.shutdown().unwrap();
    drop(db);
    // Client clones keep the live connection registry (and its lock) alive.
    drop((connection, other_connection));
    let mut reopened = fixture.open();
    for key in 0..11u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8; 32]));
        assert_eq!(value(&mut reopened, 100 + key), Some(vec![key as u8; 32]));
        let version = reopened
            .submit(
                fixture.operation(0, u64::from(key)),
                vec![
                    put(key, key as u8),
                    put(100 + key, key as u8),
                    put(300, key as u8),
                ],
            )
            .unwrap();
        assert_eq!(version, versions[key as usize]);
    }
    assert_eq!(value(&mut reopened, 300), Some(vec![10; 32]));
    for key in 0..4u32 {
        assert_eq!(value(&mut reopened, 200 + key), Some(vec![key as u8; 32]));
    }
    reopened.shutdown().unwrap();
}

#[test]
fn checkpoint_diagnostic_cursor_survives_profile_rearming() {
    let fixture = Fixture::new();
    let db = fixture.open();
    db.arm_write_profile(1).unwrap();
    db.write_batch(fixture.operation(0, 301), vec![put(301, 1)])
        .unwrap();
    db.flush_index().unwrap();
    let first = db.checkpoint_diagnostics_since(0);
    assert!(!first.records.is_empty());
    let cursor = first.next_cursor;
    db.arm_write_profile(0).unwrap();
    let disabled = db.checkpoint_diagnostics_since(0);
    assert!(disabled.records.is_empty());
    assert_eq!(disabled.next_cursor, cursor);
    db.arm_write_profile(1).unwrap();
    assert!(db.checkpoint_diagnostics_since(0).records.is_empty());
    db.write_batch(fixture.operation(1, 302), vec![put(302, 2)])
        .unwrap();
    db.flush_index().unwrap();
    let second = db.checkpoint_diagnostics_since(cursor);
    assert!(!second.records.is_empty());
    assert!(second.records.iter().all(|record| record.cursor >= cursor));
    assert!(second.next_cursor > cursor);
    db.shutdown().unwrap();
}

#[test]
fn write_profile_limit_stops_dispatch_after_one_selected_append() {
    let mut fixture = Fixture::new();
    fixture.config.write_profile_limit = 1;
    let db = fixture.open();
    for key in 0..4u64 {
        db.submit(fixture.operation(0, key), vec![put(key as u32, key as u8)])
            .unwrap();
    }
    db.drain().unwrap();
    db.finish_write_profile().unwrap();
    let profile = *db.core.write_profile.lock();
    assert_eq!(profile.sampled_requests, 1);
    assert_eq!(profile.captured_connections, 1);
    db.shutdown().unwrap();
}
