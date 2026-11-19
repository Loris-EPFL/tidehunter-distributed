//! Owner host: identities, tombstones, catalogue, and provision/drop over the
//! wire (trusted TCP), including every persisted owner crash boundary.
//! A "crash" is the step hook failing at a boundary followed by dropping the
//! host and reopening the same root: the durable image is exactly the one
//! of that boundary (each step is one atomic install or directory sync).

use super::actor::*;
use super::catalog::*;
use super::tombstone::{self, DroppedDb};
use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::NvmeofTransport;
use crate::distributed::storage::{PartitionConfig, StaticInventory};
use crate::distributed::tcp::{HostClient, Transport};
use crate::key_shape::{KeyShape, KeyType};
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

const COORDINATOR: [u8; 32] = [7; 32];

fn pool() -> LiveTargetPool {
    LiveTargetPool {
        address: "127.0.0.1".into(),
        transport: NvmeofTransport::Tcp,
        public_port_base: 100,
        public_service_base: 4420,
        commissioning_port_base: 200,
        commissioning_service_base: 5420,
    }
}

fn host_config(root: &Path, max_databases: usize) -> OwnerHostConfig {
    OwnerHostConfig {
        root: root.to_path_buf(),
        max_databases,
        live_target_pool: pool(),
        trusted_coordinator_id: Some(COORDINATOR),
        wal_member_read: Default::default(),
        wal_prewrite: false,
        read_max_device_reads: 128,
        durability: HostDurabilityDefaults::default(),
    }
}

fn trusted() -> TlsFiles {
    TlsFiles {
        transport: Transport::TcpTrusted,
        ca: PathBuf::new(),
        cert: PathBuf::new(),
        key: PathBuf::new(),
        server_name: String::new(),
    }
}

fn spec(database: [u8; 16]) -> DatabaseProvisionSpec {
    DatabaseProvisionSpec {
        partition: PartitionConfig {
            inventory: StaticInventory {
                database,
                epoch: 1,
                segment_generation: 1,
                partitions: vec![0],
            },
            partition: 0,
            batch_limits: BatchLimits {
                max_operations: 8,
                max_encoded_bytes: 2048,
            },
            group_limits: GroupLimits {
                max_batches: 8,
                max_encoded_bytes: 4088,
            },
            fragment_bytes: 4096,
            wal_file_bytes: 8192,
            max_batches: 64,
            max_reference_metadata_bytes: 1024 * 1024,
        },
        shape_yaml: KeyShape::new_single(4, 1, KeyType::uniform(1))
            .to_yaml()
            .unwrap(),
        live_target: DatabaseLiveTarget {
            initiator_nqn: "nqn.2026-01.test:owner-host".into(),
            initiator_id: [1; 16],
            durability_tier: Default::default(),
        },
    }
}

fn open_fixture(temp: &Path, max_databases: usize) -> Arc<OwnerHost> {
    try_open_fixture(temp, max_databases, None).unwrap()
}

fn try_open_fixture(
    temp: &Path,
    max_databases: usize,
    events: Option<crate::distributed::r1_events::R1EventConfig>,
) -> Result<Arc<OwnerHost>, TcpError> {
    let root = temp.join("owner");
    let configfs = temp.join("configfs");
    for name in ["hosts", "ports", "subsystems"] {
        fs::create_dir_all(configfs.join(name))?;
    }
    OwnerHost::open_inner(
        host_config(&root, max_databases),
        metrics(),
        events,
        Some(configfs),
        None,
    )
}

struct Served {
    host: Arc<OwnerHost>,
    address: String,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<Result<(), TcpError>>>,
}

impl Served {
    fn open(root: &Path, max_databases: usize) -> Self {
        Self::serve(open_fixture(root, max_databases))
    }

    fn serve(host: Arc<OwnerHost>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let serving = host.clone();
        let stop_server = stop.clone();
        let server = std::thread::spawn(move || {
            serving.serve(listener, trusted(), TcpLimits::default(), stop_server)
        });
        Self {
            host,
            address,
            stop,
            server: Some(server),
        }
    }

    fn client(&self) -> HostClient {
        HostClient::connect(&self.address, &trusted(), Duration::from_secs(30)).unwrap()
    }

    /// Stop serving and drop every handle: the next open sees only disk.
    fn crash(mut self) {
        self.stop_serving();
    }

    fn stop_serving(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            server.join().unwrap().unwrap();
        }
    }
}

fn metrics() -> Arc<Metrics> {
    Metrics::new_in_enabled(&prometheus::Registry::new(), false)
}

fn database(seed: u8) -> [u8; 16] {
    [seed; 16]
}

#[test]
fn effective_identities_are_scoped_deterministic_and_bounded() {
    let a = identity::effective_host_nqn(&database(1), 0);
    assert_eq!(a, identity::effective_host_nqn(&database(1), 0));
    assert_ne!(a, identity::effective_host_nqn(&database(1), 1));
    assert_ne!(a, identity::effective_host_nqn(&database(2), 0));
    assert!(a.len() <= 223 && a.starts_with("nqn."));
    let id = identity::effective_host_id(&[3; 16], &database(1), 0);
    assert_ne!(id, identity::effective_host_id(&[3; 16], &database(1), 1));
    assert_ne!(id, identity::effective_host_id(&[4; 16], &database(1), 0));
    assert_eq!(
        identity::parse_database_hex(&identity::database_hex(&database(9))),
        Some(database(9))
    );
    assert_eq!(identity::parse_database_hex("AB"), None);
    let one = identity::principal_from_anchors(&[vec![1, 2], vec![3]]).unwrap();
    assert_eq!(
        one,
        identity::principal_from_anchors(&[vec![3], vec![1, 2], vec![3]]).unwrap()
    );
    assert!(identity::principal_from_anchors(&[]).is_err());
}

#[test]
fn tombstones_are_append_only_and_checked() {
    let temp = tempdir::TempDir::new("host-tombstone").unwrap();
    let root = temp.path();
    tombstone::ensure_directory(root).unwrap();
    let record = DroppedDb {
        database: database(5),
        principal: COORDINATOR,
        final_generation: 3,
    };
    assert_eq!(tombstone::load(root, &record.database).unwrap(), None);
    tombstone::install(root, &record).unwrap();
    tombstone::install(root, &record).unwrap();
    assert_eq!(
        tombstone::load(root, &record.database).unwrap(),
        Some(record)
    );
    let other = DroppedDb {
        final_generation: 4,
        ..record
    };
    assert!(tombstone::install(root, &other).is_err());
    let path = root
        .join(tombstone::DROPPED_DIR)
        .join(identity::database_hex(&record.database));
    let mut bytes = fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    fs::write(&path, bytes).unwrap();
    assert_eq!(
        tombstone::load(root, &record.database).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn catalogue_slots_and_pool_bounds() {
    assert!(pool().validate(16).is_ok());
    let overlapping = LiveTargetPool {
        commissioning_port_base: 110,
        ..pool()
    };
    assert!(overlapping.validate(16).is_err());
    let overflow = LiveTargetPool {
        public_port_base: u16::MAX,
        ..pool()
    };
    assert!(overflow.validate(2).is_err());
    let mut catalog = HostCatalog::empty(pool());
    for (seed, slot) in [(3, 0), (1, 2)] {
        catalog.insert(HostEntry {
            database: database(seed),
            principal: COORDINATOR,
            slot,
            spec: spec(database(seed)),
            state: HostState::Ready,
        });
    }
    catalog.validate(4).unwrap();
    assert_eq!(catalog.least_free_slot(4), Some(1));
    assert!(catalog.validate(1).is_err());
    let target = pool()
        .target_config(1, &database(1), 0, Path::new("/owner/db-x"))
        .unwrap();
    assert_eq!((target.port_id, target.service), (101, 4421));
    assert_eq!(
        target.host_nqn,
        identity::effective_host_nqn(&database(1), 0)
    );
}

#[test]
fn provision_is_idempotent_bounded_and_a_drop_frees_the_slot() {
    let temp = tempdir::TempDir::new("host-provision").unwrap();
    let served = Served::open(temp.path(), 2);
    let mut client = served.client();
    assert_eq!(client.status().unwrap().principal, COORDINATOR);
    client.provision(database(1), &spec(database(1))).unwrap();
    client.provision(database(1), &spec(database(1))).unwrap();
    let mut changed = spec(database(1));
    changed.shape_yaml = KeyShape::new_single(8, 1, KeyType::uniform(1))
        .to_yaml()
        .unwrap();
    let error = client.provision(database(1), &changed).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("provision spec differs"));
    client.provision(database(2), &spec(database(2))).unwrap();
    // max_databases + 1 is rejected before any effect.
    let error = client
        .provision(database(3), &spec(database(3)))
        .unwrap_err();
    assert!(
        error.to_string().contains("owner database limit 2 reached"),
        "{error}"
    );
    assert!(!catalog::database_root(&served.host.core.root, &database(3)).exists());
    // Wrong principal.
    let error = client.drop_database(database(1), [9; 32], 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    client.drop_database(database(1), COORDINATOR, 0).unwrap();
    // Exact retry is idempotent; a different terminal generation is not.
    client.drop_database(database(1), COORDINATOR, 0).unwrap();
    assert!(client.drop_database(database(1), COORDINATOR, 1).is_err());
    assert!(!catalog::database_root(&served.host.core.root, &database(1)).exists());
    let error = client
        .provision(database(1), &spec(database(1)))
        .unwrap_err();
    assert!(
        error.to_string().contains("database was dropped"),
        "{error}"
    );
    // The freed slot admits a new identity.
    client.provision(database(3), &spec(database(3))).unwrap();
    assert_eq!(served.host.databases(), vec![database(2), database(3)]);
    served.crash();
}

#[test]
fn drop_before_provision_blocks_a_delayed_provision() {
    let temp = tempdir::TempDir::new("host-early-drop").unwrap();
    let served = Served::open(temp.path(), 2);
    let mut client = served.client();
    client.drop_database(database(4), COORDINATOR, 2).unwrap();
    let error = client
        .provision(database(4), &spec(database(4)))
        .unwrap_err();
    assert!(error.to_string().contains("database was dropped"));
    served.crash();
}

const PROVISION_STEPS: [HostStep; 5] = [
    HostStep::ProvisionBeforeReserve,
    HostStep::ProvisionAfterReserve,
    HostStep::ProvisionAfterDirectory,
    HostStep::ProvisionAfterSetup,
    HostStep::ProvisionAfterReady,
];

const DROP_STEPS: [HostStep; 8] = [
    HostStep::DropAfterClose,
    HostStep::DropAfterIntent,
    HostStep::DropAfterTargetCleanup,
    HostStep::DropAfterFilesOnly,
    HostStep::DropAfterUnlink,
    HostStep::DropAfterRootSync,
    HostStep::DropAfterTombstone,
    HostStep::DropAfterCatalogClear,
];

fn fail_at(step: HostStep) -> StepHook {
    Arc::new(move |current| {
        if current == step {
            Err(TcpError::new(format!("injected crash at {current:?}")))
        } else {
            Ok(())
        }
    })
}

#[test]
fn provision_recovers_at_every_owner_crash_boundary() {
    for step in PROVISION_STEPS {
        let temp = tempdir::TempDir::new("host-provision-cut").unwrap();
        let served = Served::open(temp.path(), 2);
        served.host.set_step_hook(Some(fail_at(step)));
        let error = served
            .client()
            .provision(database(1), &spec(database(1)))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("injected crash at {step:?}")),
            "{error}"
        );
        served.crash();
        // Reopen twice: recovery itself must leave a recoverable image.
        Served::open(temp.path(), 2).crash();
        let served = Served::open(temp.path(), 2);
        let mut client = served.client();
        client.provision(database(1), &spec(database(1))).unwrap();
        let catalog = HostCatalog::load(&served.host.core.root).unwrap().unwrap();
        assert_eq!(
            catalog.entries.len(),
            1,
            "{step:?}: one reservation, one slot"
        );
        assert_eq!(catalog.entries[0].slot, 0, "{step:?}");
        assert_eq!(catalog.entries[0].state, HostState::Ready, "{step:?}");
        // A second database still fits: no slot leaked by the crash.
        client.provision(database(2), &spec(database(2))).unwrap();
        served.crash();
    }
}

#[test]
fn first_provision_creates_target_only_after_inventory_is_durable() {
    let temp = tempdir::TempDir::new("host-bootstrap").unwrap();
    let served = Served::open(temp.path(), 1);
    let root = catalog::database_root(&served.host.core.root, &database(1));
    let target = root.join(catalog::LIVE_TARGET_DIR);
    let marker = super::super::inventory::read_live_target_marker;
    served
        .host
        .set_step_hook(Some(fail_at(HostStep::ProvisionAfterDirectory)));
    let error = served
        .client()
        .provision(database(1), &spec(database(1)))
        .unwrap_err();
    assert!(error.to_string().contains("injected crash"));
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    served.crash();

    let served = Served::open(temp.path(), 1);
    served
        .client()
        .provision(database(1), &spec(database(1)))
        .unwrap();
    assert!(target.is_dir());
    assert!(marker(&root).unwrap().is_some());
    let catalog = HostCatalog::load(&served.host.core.root).unwrap().unwrap();
    let config = served
        .host
        .core
        .database_config(catalog.entry(&database(1)).unwrap())
        .unwrap();
    assert_eq!(
        super::super::inventory::NodeInventory::persisted_high_water(&config).unwrap(),
        0
    );
    served.crash();
}

#[test]
fn ready_owner_refuses_missing_inventory_or_target_marker() {
    use super::super::Request;
    use super::super::inventory::{LIVE_TARGET_CONFIG, MANIFEST};

    for (label, fence, missing) in [
        ("empty-directory", false, MANIFEST),
        ("fenced-marker", true, LIVE_TARGET_CONFIG),
        ("never-fenced-marker", false, LIVE_TARGET_CONFIG),
    ] {
        let temp = tempdir::TempDir::new(label).unwrap();
        let served = Served::open(temp.path(), 1);
        let id = database(1);
        served.client().provision(id, &spec(id)).unwrap();
        if fence {
            served
                .host
                .database(&id)
                .unwrap()
                .handle(Request::Fence { epoch: 1 }, TcpLimits::default())
                .unwrap();
        }
        let root = catalog::database_root(&served.host.core.root, &id);
        served.crash();
        let path = root.join(missing);
        if missing == MANIFEST {
            fs::remove_dir_all(&root).unwrap();
            fs::create_dir(&root).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
        }
        let error = try_open_fixture(temp.path(), 1, None)
            .err()
            .expect("Ready state must not be recreated");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}: {error}");
        assert!(error.to_string().contains(missing), "{label}: {error}");
        assert!(!path.exists(), "{label}: missing state was recreated");
        assert!(!root.join(LIVE_TARGET_CONFIG).exists(), "{label}");
    }
}

#[test]
fn drop_completes_at_every_owner_crash_boundary() {
    for step in DROP_STEPS {
        let temp = tempdir::TempDir::new("host-drop-cut").unwrap();
        let served = Served::open(temp.path(), 1);
        served
            .client()
            .provision(database(1), &spec(database(1)))
            .unwrap();
        served.host.set_step_hook(Some(fail_at(step)));
        let error = served
            .client()
            .drop_database(database(1), COORDINATOR, 0)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("injected crash at {step:?}")),
            "{error}"
        );
        served.crash();
        Served::open(temp.path(), 1).crash();
        let served = Served::open(temp.path(), 1);
        // Before the owner intent a restart may serve it; after intent it must not.
        assert_eq!(
            served.host.database(&database(1)).is_some(),
            step == HostStep::DropAfterClose
        );
        served
            .client()
            .drop_database(database(1), COORDINATOR, 0)
            .unwrap();
        assert!(
            !catalog::database_root(&served.host.core.root, &database(1)).exists(),
            "{step:?}"
        );
        assert_eq!(
            tombstone::load(&served.host.core.root, &database(1)).unwrap(),
            Some(DroppedDb {
                database: database(1),
                principal: COORDINATOR,
                final_generation: 0,
            }),
            "{step:?}"
        );
        assert!(
            HostCatalog::load(&served.host.core.root)
                .unwrap()
                .unwrap()
                .entries
                .is_empty()
        );
        // The single slot is free again.
        served
            .client()
            .provision(database(2), &spec(database(2)))
            .unwrap();
        served.crash();
    }
}

#[test]
fn queued_fence_cannot_pass_drop_and_unrouted_retry_checks_the_durable_high_water() {
    use super::super::{Request, control::ControlWork};
    use std::sync::mpsc::sync_channel;

    let temp = tempdir::TempDir::new("host-close-fence").unwrap();
    let served = Served::open(temp.path(), 1);
    let mut client = served.client();
    let id = database(1);
    client.provision(id, &spec(id)).unwrap();
    let node = served.host.database(&id).unwrap();
    let (completed, completions) = sync_channel(1);
    let submit_fence = |epoch| {
        assert!(
            node.control
                .try_submit(ControlWork {
                    id: epoch,
                    request: Request::Fence { epoch },
                    limits: TcpLimits::default(),
                    measured: false,
                    queued: None,
                    submitted: None,
                    charge: 0,
                    completed: completed.clone(),
                    progress: None,
                })
                .is_ok()
        );
    };
    let timeout = Duration::from_secs(30);
    submit_fence(1);
    completions.recv_timeout(timeout).unwrap().response.unwrap();
    // Fence wins first: a drop below its high-water cannot close this database.
    let error = client.drop_database(id, COORDINATOR, 0).unwrap_err();
    assert!(error.to_string().contains("stale fencing generation 0 < 1"));
    assert!(!node.is_closed());

    // Drop wins next: stop a genuinely admitted Fence immediately before
    // it acquires the same gates as close_for_drop.
    let (entered, waiting) = sync_channel(1);
    let (resume, parked) = sync_channel(1);
    *node.control_context.before_fence.lock() = Some(Box::new(move || {
        entered.send(()).unwrap();
        parked.recv_timeout(timeout).unwrap();
    }));
    submit_fence(2);
    waiting.recv_timeout(timeout).unwrap();
    let close = node.close_for_drop(1);
    resume.send(()).unwrap();
    close.unwrap();
    let rejected = completions.recv_timeout(timeout).unwrap().response;
    assert!(
        matches!(rejected, Err(error) if error.to_string().contains("database closed for drop"))
    );
    assert_eq!(node.fence_high_water().unwrap(), 1);
    drop(node);

    served
        .host
        .set_step_hook(Some(fail_at(HostStep::DropAfterClose)));
    let error = client.drop_database(id, COORDINATOR, 1).unwrap_err();
    assert!(error.to_string().contains("injected crash"));
    assert!(served.host.database(&id).is_none());
    let catalogue = HostCatalog::load(&served.host.core.root).unwrap().unwrap();
    assert_eq!(catalogue.entry(&id).unwrap().state, HostState::Ready);
    // No in-memory route remains in this process; retry must read the ledger.
    let error = client.drop_database(id, COORDINATOR, 0).unwrap_err();
    assert!(error.to_string().contains("stale fencing generation 0 < 1"));
    served.crash();

    let served = Served::open(temp.path(), 1);
    let mut client = served.client();
    let error = client.drop_database(id, COORDINATOR, 0).unwrap_err();
    assert!(error.to_string().contains("stale fencing generation 0 < 1"));
    client.drop_database(id, COORDINATOR, 1).unwrap();
    assert_eq!(
        tombstone::load(&served.host.core.root, &id)
            .unwrap()
            .unwrap()
            .final_generation,
        1
    );
    served.crash();
}

#[test]
fn owner_events_are_separate_at_provision_restart_drop_and_shutdown() {
    use crate::distributed::r1_events::{R1EventConfig, SCHEMA_VERSION};

    let temp = tempdir::TempDir::new("host-r1-events").unwrap();
    let config = R1EventConfig {
        case_output: temp.path().join("evidence"),
        run_id: "run".into(),
        case_id: "case".into(),
        tier: "native".into(),
        policy: "r1".into(),
        retention_sha256: "0".repeat(64),
        max_events: 16,
        max_payload_bytes: 1024,
        queue: 4,
    };
    let streams = config.case_output.join("capacity/r1-events/owner_O0");
    let open = || Served::serve(try_open_fixture(temp.path(), 2, Some(config.clone())).unwrap());
    let mut served = open();
    let mut client = served.client();
    client.provision(database(1), &spec(database(1))).unwrap();
    client.provision(database(2), &spec(database(2))).unwrap();
    assert_eq!(fs::read_dir(&streams).unwrap().count(), 2);
    client.drop_database(database(1), COORDINATOR, 0).unwrap();
    assert_eq!(
        fs::read_dir(&streams)
            .unwrap()
            .filter(|entry| { entry.as_ref().unwrap().path().extension().unwrap() == "jsonl" })
            .count(),
        1
    );
    served.stop_serving();
    served.host.finish_r1_events().unwrap();
    drop(served);

    let mut served = open();
    assert_eq!(served.host.databases(), vec![database(2)]);
    served.stop_serving();
    served.host.finish_r1_events().unwrap();
    let mut databases = Vec::new();
    for entry in fs::read_dir(streams).unwrap() {
        let path = entry.unwrap().path();
        assert_eq!(path.extension().unwrap(), "jsonl");
        let stream = fs::read_to_string(path).unwrap();
        let header: serde_json::Value =
            serde_json::from_str(stream.lines().next().unwrap()).unwrap();
        let terminal: serde_json::Value =
            serde_json::from_str(stream.lines().last().unwrap()).unwrap();
        assert_eq!(header["schema_version"], SCHEMA_VERSION);
        assert_eq!(terminal["terminal"]["complete"], true);
        databases.push(header["database_id"].as_str().unwrap().to_owned());
    }
    databases.sort_unstable();
    assert_eq!(
        databases,
        [
            identity::database_hex(&database(1)),
            identity::database_hex(&database(2)),
            identity::database_hex(&database(2))
        ]
    );
}

#[test]
fn lifecycle_queue_full_is_would_block() {
    let temp = tempdir::TempDir::new("host-queue").unwrap();
    let host = open_fixture(temp.path(), 1);
    // Pause the actor inside its first job: capacity is max_databases + 1.
    let (entered, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel::<()>(0);
    let released = Mutex::new(released);
    host.set_step_hook(Some(Arc::new(move |step| {
        if step == HostStep::ProvisionBeforeReserve {
            let _ = entered.try_send(());
            let _ = released.lock().recv();
        }
        Ok(())
    })));
    let _ = host.core.serving.set((TcpLimits::default(), COORDINATOR));
    let request = |seed| ProvisionRequest {
        database: database(seed),
        spec: spec(database(seed)),
    };
    let first = host
        .actor
        .submit_provision(request(1), TcpLimits::default(), COORDINATOR)
        .unwrap();
    entered_rx.recv().unwrap();
    let queued: Vec<_> = (2..4)
        .map(|seed| {
            host.actor
                .submit_provision(request(seed), TcpLimits::default(), COORDINATOR)
                .unwrap()
        })
        .collect();
    let error = host
        .actor
        .submit_provision(request(9), TcpLimits::default(), COORDINATOR)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    drop(release);
    assert!(first.recv().unwrap().is_ok());
    for receiver in queued {
        // max_databases = 1: the later ones are rejected by the bound.
        assert!(receiver.recv().unwrap().is_err());
    }
}
