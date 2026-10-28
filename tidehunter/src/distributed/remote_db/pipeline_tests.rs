//! Bounded five-node mTLS tests for the publication boundary, not timing tests.
//! Tidehunter requires whole batches and a stable ordered checkpoint prefix;
//! independent durable batches may publish beyond an unresolved gap.
//! Existing connection guards and storage persistence hooks deterministically
//! pause append progress; tests make no assumptions about SSD speed or scheduling.

use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::GroupLimits;
use crate::distributed::tcp::{NodeConfig, StorageNode, TcpError, TcpLimits};
use crate::key_shape::KeyType;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};

#[path = "../../../examples/support/tls_material.rs"]
mod tls_material;

pub(super) struct Fixture {
    pub(super) config: RemoteDatabaseConfig,
    pub(super) shape: KeyShape,
    pub(super) nodes: Vec<Arc<StorageNode>>,
    shutdown: Arc<AtomicBool>,
    servers: Vec<JoinHandle<Result<(), TcpError>>>,
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

    fn new_with_config(
        max_batches: usize,
        shape: KeyShape,
        group_max_batches: u32,
        configure: impl FnOnce(&mut RemoteDatabaseConfig),
    ) -> Self {
        let temp = tempdir::TempDir::new("remote-pipeline-mtls").unwrap();
        let certs = tls_material::TestCertificates::generate(&temp.path().join("tls"), 5).unwrap();
        let mut config = RemoteDatabaseConfig {
            storage: LocalInventoryConfig {
                database: [71; 16],
                partitions: vec![0, 1, 2, 3, 4],
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
                max_epochs: 8,
            },
            endpoints: Vec::new(),
            tls: TlsFiles {
                transport: Default::default(),
                ca: certs.ca.clone(),
                cert: certs.authority.certificate.clone(),
                key: certs.authority.private_key.clone(),
                server_name: String::new(),
                resident_reads: super::super::tcp::ResidentReadConfig::default(),
            },
            // Five-node startup/replay shares disk and CPU with the full suite.
            // This is a bounded liveness guard, not a latency assertion.
            timeout: Duration::from_secs(30),
            max_admitted_batches: max_batches,
            max_index_bytes: 1024 * 1024,
            max_pending_batches: 64,
            max_pending_bytes: 1024 * 1024,
            pipelined_writes: true,
            read_cache_bytes: 64 * 1024,
            read_memory_allowance_bytes: 512 * 1024 * 1024,
            preparation_workers: 2,
            scan_prefetch_pages: 2,
            read_lanes: 2,
            max_pending_reads: 256,
            max_pending_read_bytes: 64 * 1024 * 1024,
            write_profile_limit: 0,
        };
        configure(&mut config);
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut nodes = Vec::new();
        let mut servers = Vec::new();
        for (index, identity) in certs.nodes.into_iter().enumerate() {
            let node = StorageNode::open(
                NodeConfig {
                    root: temp.path().join(format!("node-{index}")),
                    partition: config.partition_config(1, index as u32),
                    shape_yaml: shape.to_yaml().unwrap(),
                    max_epochs: config.storage.max_epochs,
                    read_workers: 2,
                    wal_member_read: Default::default(),
                    read_resident_first: false,
                    read_cache_bytes: 0,
                    read_memory_allowance_bytes: 512 * 1024 * 1024,
                    read_max_inflight_requests: 32,
                    read_max_inflight_bytes: 64 * 1024 * 1024,
                    read_max_device_reads: 128,
                    resident_reads: super::super::tcp::ResidentReadConfig::default(),
                },
                metrics(),
            )
            .unwrap();
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
                resident_reads: super::super::tcp::ResidentReadConfig::default(),
            };
            let serving = Arc::clone(&node);
            let stop = Arc::clone(&shutdown);
            servers.push(thread::spawn(move || {
                serving.serve(
                    listener,
                    tls,
                    TcpLimits {
                        io_timeout: Duration::from_secs(30),
                        ..TcpLimits::default()
                    },
                    stop,
                )
            }));
            nodes.push(node);
        }
        Self {
            config,
            shape,
            nodes,
            shutdown,
            servers,
            temp,
        }
    }

    pub(super) fn open(&self) -> RemoteDatabase {
        RemoteDatabase::open(
            &self.temp.path().join("authority"),
            &self.shape,
            self.config.clone(),
            metrics(),
        )
        .unwrap()
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

#[test]
fn full_groups_dispatch_before_drain_on_five_nodes_and_recover_exact_state() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    // Hold every connection before any append is enqueued. This proves a
    // bounded per-node window dispatches groups without waiting for a reply.
    let connections: Vec<_> = db
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
    let before = (
        db.pipeline_stats.early_dispatches,
        db.pipeline_stats.append_requests,
        db.pipeline_stats.peak_inflight_nodes,
        db.inflight.iter().all(|queue| !queue.is_empty()),
        db.staged.iter().map(VecDeque::len).sum::<usize>(),
        db.publication.published_prefix().next_sequence(),
        db.pending_bytes,
        db.submissions
            .iter()
            .map(|entry| entry.credit)
            .sum::<usize>(),
    );
    drop(held);
    assert_eq!((before.0, before.1, before.2), (10, 10, 5));
    assert!(before.3);
    assert_eq!((before.4, before.5), (12, 0));
    assert_eq!(before.6, before.7);
    assert!(before.6 > 0);
    db.drain().unwrap();
    assert!(db.inflight.iter().all(VecDeque::is_empty));
    assert!(db.staged.iter().all(VecDeque::is_empty));
    assert_eq!(db.publication.published_prefix().next_sequence(), 32);
    assert_eq!(db.pending_bytes, 0);
    assert_eq!(db.reserved_index_bytes, 0);
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
    let admissions = db.stats.admitted_batches;
    assert_eq!(
        db.submit(*operation, vec![mutation.clone()]).unwrap(),
        *version
    );
    assert_eq!(db.stats.admitted_batches, admissions);
    db.drain().unwrap();
    assert_eq!(value(&mut db, 0), Some(vec![99; 32]));
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 31);
    db.shutdown().unwrap();
    drop(db);
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
    assert_eq!(reopened.stats.admitted_batches, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn exhausted_publication_view_budget_rejects_before_admission_and_allows_retry() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let sentinel_operation = fixture.operation(0, 39);
    let sentinel = db.submit(sentinel_operation, vec![put(39, 39)]).unwrap();
    let max_excluded = db.config.max_pending_batches - 1;
    let mut reservations = Vec::new();
    loop {
        match db.index.read().reserve_publication_view(max_excluded) {
            Ok(reservation) => reservations.push(reservation),
            Err(super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }
    assert!(!reservations.is_empty());
    // Consume the remainder with one-exclusion reservations: one max-sized
    // reservation may leave enough slack for the smaller next-window charges.
    let mut small_reservations = Vec::new();
    loop {
        match db.index.read().reserve_publication_view(1) {
            Ok(reservation) => small_reservations.push(reservation),
            Err(super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }

    let operation = fixture.operation(0, 40);
    let id = BatchId {
        database: fixture.config.storage.database,
        operation,
    };
    let next = db.submission_next();
    let prefix = db.publication.published_prefix().next_sequence();
    let appends = db.pipeline_stats.append_requests;
    let active = db.active_identities.len();
    let submissions = db.submissions.len();
    let writers: usize = db.pending_writers.values().map(|table| table.len()).sum();
    let pending_bytes = db.pending_bytes;
    let reserved_index_bytes = db.reserved_index_bytes;
    let staged: Vec<_> = db.staged.iter().map(|queue| queue.len()).collect();
    let inflight: Vec<_> = db.inflight.iter().map(|queue| queue.len()).collect();
    let sentinel_id = BatchId {
        database: fixture.config.storage.database,
        operation: sentinel_operation,
    };
    assert!(
        !fixture.nodes[0]
            .contains_batch(db.epoch, sentinel_id)
            .unwrap()
    );
    assert!(!fixture.nodes[0].contains_batch(db.epoch, id).unwrap());
    let error = db.submit(operation, vec![put(40, 42)]).unwrap_err();
    assert!(error.uncertain_version.is_none());
    assert!(db.healthy);
    assert_eq!(db.submission_next(), next);
    assert_eq!(db.publication.published_prefix().next_sequence(), prefix);
    assert_eq!(db.active_identities.len(), active);
    assert_eq!(db.submissions.len(), submissions);
    assert_eq!(
        db.pending_writers
            .values()
            .map(|table| table.len())
            .sum::<usize>(),
        writers
    );
    assert_eq!(db.pending_bytes, pending_bytes);
    assert_eq!(db.reserved_index_bytes, reserved_index_bytes);
    assert_eq!(
        db.staged
            .iter()
            .map(|queue| queue.len())
            .collect::<Vec<_>>(),
        staged
    );
    assert_eq!(
        db.inflight
            .iter()
            .map(|queue| queue.len())
            .collect::<Vec<_>>(),
        inflight
    );
    assert_eq!(db.pipeline_stats.append_requests, appends);
    assert!(!fixture.nodes[0].contains_batch(db.epoch, id).unwrap());
    assert!(
        !fixture.nodes[0]
            .contains_batch(db.epoch, sentinel_id)
            .unwrap()
    );

    drop(reservations.pop().unwrap());
    let version = db.submit(operation, vec![put(40, 42)]).unwrap();
    assert_eq!(
        version.sequence, next,
        "rejection must not advance the clock"
    );
    db.drain().unwrap();
    assert_eq!(sentinel.sequence + 1, version.sequence);
    assert_eq!(value(&mut db, 39), Some(vec![39; 32]));
    assert_eq!(value(&mut db, 40), Some(vec![42; 32]));
    assert!(fixture.nodes[0].contains_batch(db.epoch, id).unwrap());
    drop(reservations);
    drop(small_reservations);
    db.shutdown().unwrap();
}

#[test]
fn active_checkpoint_capture_and_sparse_publication_have_separate_budgets() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.write_batch(fixture.operation(2, 50), vec![put(50, 50)])
        .unwrap();
    let max_excluded = db.config.max_pending_batches - 1;
    let mut reservations = Vec::new();
    loop {
        match db.index.read().reserve_publication_view(max_excluded) {
            Ok(reservation) => reservations.push(reservation),
            Err(super::super::native_index::NativeIndexError::Capacity) => break,
            Err(error) => panic!("unexpected reservation failure: {error}"),
        }
    }
    assert!(reservations.len() >= 3);
    let before_capture = db.index.read().stats().transient_metadata_bytes;
    let capture = db.index.read().prepare_image(IndexReadCut::Latest).unwrap();
    assert!(db.index.read().stats().transient_metadata_bytes > before_capture);
    for _ in 0..3 {
        drop(reservations.pop().unwrap());
    }

    let blocked = Arc::clone(&db.clients[0].client);
    let guard = blocked.lock();
    let first = db
        .submit(fixture.operation(0, 51), vec![put(51, 51)])
        .unwrap();
    let later = db
        .submit(fixture.operation(1, 52), vec![put(52, 52)])
        .unwrap();
    db.submit(fixture.operation(1, 53), vec![put(53, 53)])
        .unwrap();
    let group = db.inflight[1].pop_front().unwrap();
    let receipts = group
        .reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    db.accept_receipts(receipts).unwrap();
    db.publish_completed().unwrap();
    let first_published = db.publication.is_published(first);
    let later_published = db.publication.is_published(later);
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

    let first_client = Arc::clone(&db.clients[0].client);
    let first_guard = first_client.lock();
    let suffix_client = Arc::clone(&db.clients[1].client);
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
    for space in [KeySpace(0), KeySpace(1)] {
        let writers = &db.pending_writers[&space];
        assert_eq!(writers.len(), 33);
        assert!(writers.capacity() > 4);
    }
    assert_eq!(db.submissions.len(), 33);
    assert_eq!(db.inflight[1].len(), 2);
    assert_eq!(db.staged[1].len(), 28);
    drop(suffix_guard);
    while !db.inflight[1].is_empty() || !db.staged[1].is_empty() {
        db.progress_pipeline(false).unwrap();
        let Some(group) = db.inflight[1].pop_front() else {
            continue;
        };
        let receipts = group
            .reply
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        db.accept_receipts(receipts).unwrap();
        db.publish_completed().unwrap();
        db.progress_pipeline(false).unwrap();
    }
    assert!(!db.publication.is_published(sentinel));
    assert_eq!(db.publication.published_prefix().next_sequence(), 0);
    assert_eq!(db.submissions.len(), 33);
    assert_eq!(
        db.submissions
            .iter()
            .filter(|entry| entry.batch.is_some())
            .count(),
        1
    );
    for space in [KeySpace(0), KeySpace(1)] {
        let writers = &db.pending_writers[&space];
        assert_eq!(writers.len(), 1);
        assert!(writers.capacity() <= 4 * writers.len());
    }
    assert!(
        db.submit(fixture.operation(2, 200), vec![mutation(0, 100)])
            .is_err()
    );
    assert!(db.healthy);
    assert_eq!(db.submission_next(), 33);
    drop(first_guard);
    db.drain().unwrap();
    assert!(db.pending_writers.is_empty());
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
    let blocked = Arc::clone(&db.clients[0].client);
    let guard = blocked.lock();
    let first = fixture.operation(0, 0);
    let version = db.submit(first, vec![put(0, 1)]).unwrap();
    db.submit(fixture.operation(0, 1), vec![put(1, 2)]).unwrap();
    db.submit(fixture.operation(1, 2), vec![put(2, 3)]).unwrap();
    db.submit(fixture.operation(1, 3), vec![put(3, 4)]).unwrap();
    let credits = db.pending_bytes;
    let dispatches = db.pipeline_stats.early_dispatches;
    // Observe the later group's real durable reply, deliberately leaving node 0
    // blocked. No sleep or assumed persistence timing determines this ordering.
    let later = db.inflight[1].pop_front().unwrap();
    let receipts = later
        .reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    db.accept_receipts(receipts).unwrap();
    db.publish_completed().unwrap();
    let prefix = db.publication.published_prefix().next_sequence();
    let pending = db.pending_bytes;
    let retained = db.submissions.len();
    let unpublished = db
        .submissions
        .iter()
        .filter(|entry| entry.batch.is_some())
        .count();
    let later_value = db.get(KeySpace(0), &2u32.to_be_bytes());
    let retry = db.submit(first, vec![put(0, 1)]);
    let conflicting_retry = db.submit(first, vec![put(0, 9)]);
    let excess = db.submit(fixture.operation(2, 4), vec![put(4, 5)]);
    let next = db.submission_next();
    let healthy = db.healthy;
    drop(guard);
    assert_eq!(dispatches, 2);
    assert!(credits > 0);
    assert_eq!(prefix, 0);
    assert!(pending < credits);
    assert_eq!(retained, 4);
    assert_eq!(unpublished, 2);
    assert_eq!(later_value.unwrap().unwrap().as_ref(), &[3; 32]);
    assert_eq!(retry.unwrap(), version);
    assert!(conflicting_retry.is_err());
    assert!(excess.is_err());
    assert_eq!(next, 4);
    assert!(healthy);
    db.drain().unwrap();
    assert_eq!(db.publication.published_prefix().next_sequence(), 4);
    assert_eq!(db.pending_bytes, 0);
    assert_eq!(db.reserved_index_bytes, 0);
    assert!(db.submissions.is_empty());
    assert_eq!(db.publication.retained_batches(), 0);
    assert!(db.pending_writers.is_empty());
    assert_eq!(value(&mut db, 2), Some(vec![3; 32]));
    db.submit(fixture.operation(2, 4), vec![put(4, 5)]).unwrap();
    db.drain().unwrap();
    assert_eq!(value(&mut db, 4), Some(vec![5; 32]));
    db.shutdown().unwrap();
}

#[test]
fn conflicting_and_transitive_batches_wait_for_their_whole_predecessors() {
    for transitive in [false, true] {
        let fixture = Fixture::new();
        let mut db = fixture.open();
        let connection = Arc::clone(&db.clients[0].client);
        let held = connection.lock();
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
        db.submit(fixture.operation(2, 14), vec![put(11, 11)])
            .unwrap();
        for node in [1usize, 2] {
            let group = db.inflight[node].pop_front().unwrap();
            let receipts = group
                .reply
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            db.accept_receipts(receipts).unwrap();
        }
        db.publish_completed().unwrap();
        let before = (
            db.publication.is_published(first),
            db.publication.is_published(second),
            db.publication.is_published(third),
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
        assert!(db.submissions.is_empty());
        assert_eq!(db.publication.retained_batches(), 0);
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
    let connection = Arc::clone(&db.clients[0].client);
    let held = connection.lock();
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
    let group = db.inflight[1].pop_front().unwrap();
    let receipts = group
        .reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    db.accept_receipts(receipts).unwrap();
    db.publish_completed().unwrap();
    let second_published = db.publication.is_published(second);
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
    assert_eq!(db.publication.published_prefix().next_sequence(), 2);
    fixture.nodes[2].inject_drop_next_append_response();
    let operation = fixture.operation(2, 2);
    let version = db.submit(operation, vec![put(2, 3)]).unwrap();
    db.submit(fixture.operation(2, 3), vec![put(3, 4)]).unwrap();
    assert!(db.drain().is_err());
    assert!(!db.healthy);
    assert_eq!(db.publication.published_prefix().next_sequence(), 2);
    assert!(db.pending_bytes > 0);
    // A verified hot-cache record cannot let an unhealthy authority keep serving.
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.get_profiled(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.submit(operation, vec![put(2, 3)]).is_err());
    assert!(db.shutdown().is_err());
    drop(db);
    let mut reopened = fixture.open();
    for key in 0..4u32 {
        assert_eq!(value(&mut reopened, key), Some(vec![key as u8 + 1; 32]));
    }
    assert_eq!(
        reopened.submit(operation, vec![put(2, 3)]).unwrap(),
        version
    );
    assert_eq!(reopened.stats.admitted_batches, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn observed_stale_epoch_read_stops_authority_before_any_later_cache_hit() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    db.submit(fixture.operation(0, 0), vec![put(0, 1)]).unwrap();
    db.submit(fixture.operation(0, 1), vec![put(1, 2)]).unwrap();
    db.drain().unwrap();
    db.clear_read_cache();
    assert_eq!(value(&mut db, 0), Some(vec![1; 32]));
    assert_eq!(value(&mut db, 0), Some(vec![1; 32]));
    assert!(db.read_cache_stats().hits > 0);

    // Simulate a competing authority becoming known to this storage node. The
    // authority has no background lease protocol: this test begins its safety
    // assertion when an uncached RPC actually observes that its epoch is stale.
    let next_epoch = db.epoch() + 1;
    db.clients[0].client.lock().fence_to(next_epoch).unwrap();
    assert!(db.get(KeySpace(0), &1u32.to_be_bytes()).is_err());
    assert!(!db.healthy);
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
    let first_connection = Arc::clone(&db.clients[0].client);
    let later_connection = Arc::clone(&db.clients[1].client);
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
    let before = (
        !db.inflight[0].is_empty() && !db.inflight[1].is_empty(),
        db.inflight[1].len(),
        db.staged[1].is_empty(),
        db.publication.published_prefix().next_sequence(),
    );

    // A separate authenticated observer reads the storage group's durable count;
    // it does not hold or alter the authority's worker connection or replies.
    let endpoint = &fixture.config.endpoints[1];
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
    let drainer = thread::spawn(move || {
        let result = db.drain();
        assert!(finished.send((db, result)).is_ok());
    });
    drop(later_guard);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut premature = None;
    let refilled = loop {
        if let Ok(result) = completion.try_recv() {
            premature = Some(result);
            break false;
        }
        if observer.stats().unwrap().appended_groups == 2 {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        // This is a bounded progress assertion, not a latency comparison. The
        // earlier group's explicit guard remains held for the entire observation.
        thread::sleep(Duration::from_millis(1));
    };
    if premature.is_none() {
        premature = completion.try_recv().ok();
    }
    let returned_before_gap_completed = premature.is_some();
    drop(first_guard);
    let (mut db, result) = match premature {
        Some(result) => result,
        None => completion.recv_timeout(Duration::from_secs(10)).unwrap(),
    };
    drainer.join().unwrap();
    assert!(before.0 && before.2);
    assert_eq!((before.1, before.3), (2, 0));
    result.unwrap();
    assert!(
        refilled,
        "a later node must receive its next group while the earliest node is stalled"
    );
    assert!(
        !returned_before_gap_completed,
        "drain must not acknowledge beyond the unresolved prefix"
    );
    assert_eq!(db.publication.published_prefix().next_sequence(), 6);
    assert_eq!(db.pending_bytes, 0);
    for key in 0..6u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
}

#[test]
fn lone_direct_append_keeps_durability_and_unknown_response_recovery_rules() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let first_operation = fixture.operation(0, 0);
    let first_version = db.write_batch(first_operation, vec![put(0, 17)]).unwrap();
    assert_eq!(db.pipeline_stats.direct_appends, 1);
    assert_eq!(db.pipeline_stats.append_requests, 1);
    assert_eq!(db.pipeline_stats.early_dispatches, 0);
    assert!(db.inflight.iter().all(VecDeque::is_empty));
    assert!(db.staged.iter().all(VecDeque::is_empty));
    assert!(db.staged_bytes.iter().all(|bytes| *bytes == 0));
    assert_eq!(db.pending_bytes, 0);
    assert_eq!(db.reserved_index_bytes, 0);
    assert_eq!(db.publication.published_prefix().next_sequence(), 1);
    assert!(db.healthy);
    assert_eq!(value(&mut db, 0), Some(vec![17; 32]));
    assert_eq!(value(&mut db, 0), Some(vec![17; 32]));
    assert!(db.read_cache_stats().hits > 0);

    // The direct caller path reaches the exact same durable storage operation.
    // Losing its response cannot refund the batch or keep cached reads serving.
    fixture.nodes[1].inject_drop_next_append_response();
    let uncertain_operation = fixture.operation(1, 1);
    let uncertain_version = db.submit(uncertain_operation, vec![put(1, 231)]).unwrap();
    assert_eq!(db.pipeline_stats.direct_appends, 1);
    let error = db.drain().unwrap_err();
    assert_eq!(error.uncertain_version, Some(uncertain_version));
    assert_eq!(db.pipeline_stats.direct_appends, 2);
    assert_eq!(db.pipeline_stats.append_requests, 2);
    assert!(!db.healthy);
    assert_eq!(db.publication.published_prefix().next_sequence(), 1);
    assert_eq!(db.staged[1].len(), 1);
    assert!(db.staged_bytes[1] > 0);
    assert!(db.pending_bytes > 0 && db.reserved_index_bytes > 0);
    let hits = db.read_cache_stats().hits;
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert_eq!(db.read_cache_stats().hits, hits);
    assert!(db.submit(uncertain_operation, vec![put(1, 231)]).is_err());
    assert!(db.shutdown().is_err());
    drop(db);

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
    assert_eq!(reopened.stats.admitted_batches, 0);
    assert_eq!(reopened.pipeline_stats.append_requests, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn parallel_preparation_preserves_order_retries_and_complete_recovery() {
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
    assert_eq!(db.stats.admitted_batches, 4);
    assert_eq!(db.pipeline_stats.parallel_prepared_batches, 5);
    assert!(db.pipeline_stats.prepare_reserved_bytes_peak <= fixture.config.max_pending_bytes);
    db.drain().unwrap();
    assert_eq!(db.pending_bytes, 0);
    assert_eq!(db.publication.published_prefix().next_sequence(), 4);
    for key in 0..4 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8 + 7; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(reopened.submit_many(&window).unwrap(), versions);
    assert_eq!(reopened.stats.admitted_batches, 0);
    assert_eq!(reopened.pipeline_stats.append_requests, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn parallel_preparation_rejects_late_conflicts_without_admitting_a_prefix() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let known = fixture.operation(0, 0);
    let next = fixture.operation(1, 1);
    db.write_batch(known, vec![put(0, 21)]).unwrap();
    let appends = db.pipeline_stats.append_requests;
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
    assert!(db.healthy);
    assert_eq!(db.submission_next(), 1);
    assert_eq!(db.pipeline_stats.append_requests, appends);
    assert_eq!(db.pending_bytes, 0);
    assert_eq!(value(&mut db, 0), Some(vec![21; 32]));
    assert_eq!(value(&mut db, 1), None);
    assert_eq!(db.write_batch(next, vec![put(1, 22)]).unwrap().sequence, 1);
    db.shutdown().unwrap();
}

#[test]
fn preparation_budget_and_lost_reply_keep_the_existing_failure_contract() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let window = vec![
        (fixture.operation(0, 0), vec![put(0, 41)]),
        (fixture.operation(0, 1), vec![put(1, 42)]),
    ];
    db.config.max_pending_bytes = 1;
    assert!(db.submit_many(&window).is_err());
    assert_eq!(db.pipeline_stats.prepare_calls, 0);
    assert_eq!(db.submissions.len(), 0);
    db.config.max_pending_bytes = fixture.config.max_pending_bytes;
    fixture.nodes[0].inject_drop_next_append_response();
    let versions = db.submit_many(&window).unwrap();
    assert!(db.drain().is_err());
    assert!(!db.healthy);
    assert!(db.pending_bytes > 0);
    assert!(db.get(KeySpace(0), &0u32.to_be_bytes()).is_err());
    assert!(db.shutdown().is_err());
    drop(db);
    let mut reopened = fixture.open();
    assert_eq!(reopened.submit_many(&window).unwrap(), versions);
    assert_eq!(reopened.stats.admitted_batches, 0);
    assert_eq!(value(&mut reopened, 0), Some(vec![41; 32]));
    assert_eq!(value(&mut reopened, 1), Some(vec![42; 32]));
    reopened.shutdown().unwrap();
}

#[test]
fn preparation_services_previous_groups_without_admitting_invalid_next_chunk() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let connection = Arc::clone(&db.clients[0].client);
    let guard = connection.lock();
    for key in 0..6u32 {
        db.submit(
            fixture.operation(0, u64::from(key)),
            vec![put(key, key as u8)],
        )
        .unwrap();
    }
    let before = (db.inflight[0].len(), db.staged[0].is_empty());
    let (entered, entering) = mpsc::sync_channel(1);
    let (resume, resuming) = mpsc::sync_channel(1);
    db.before_preparation = Some((entered, resuming));
    // Conflict discovered only AFTER canonical preparation. No member of this
    // chunk may be admitted while older groups make storage progress.
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
        let result = db.submit_many(&next);
        assert!(finished.send((db, result)).is_ok());
    });
    let entered_in_time = entering.recv_timeout(Duration::from_secs(10)).is_ok();
    drop(guard);
    let deadline = Instant::now() + Duration::from_secs(10);
    let refilled = loop {
        if observer
            .stats()
            .map(|stats| stats.appended_groups == 3)
            .unwrap_or(false)
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        thread::sleep(Duration::from_millis(1));
    };
    // Always release the explicit gate before checking assertions.
    resume.send(()).unwrap();
    let (mut db, result) = completion.recv_timeout(Duration::from_secs(10)).unwrap();
    preparer.join().unwrap();
    assert_eq!(before.0, 2);
    assert!(!before.1);
    assert!(entered_in_time);
    assert!(
        refilled,
        "storage actors must refill while next preparation is blocked"
    );
    assert!(result.is_err());
    assert_eq!(
        db.submission_next(),
        6,
        "invalid chunk cannot allocate logical slots"
    );
    assert!(db.healthy);
    db.drain().unwrap();
    assert_eq!(value(&mut db, 7), None);
    for key in 0..6u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
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
    assert_eq!(reopened.stats.admitted_batches, 0);
    reopened.shutdown().unwrap();
}

#[test]
fn same_shard_window_appends_successor_before_prefix_is_durable() {
    let fixture = Fixture::new();
    let mut db = fixture.open();
    let (entered, release) = fixture.nodes[0].pause_next_persist(1).unwrap();
    let operations: Vec<_> = (0..4u64).map(|seed| fixture.operation(0, seed)).collect();
    for (key, operation) in operations.iter().copied().enumerate().take(2) {
        db.submit(operation, vec![put(key as u32, key as u8)])
            .unwrap();
    }
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    for (key, operation) in operations.iter().copied().enumerate().skip(2) {
        db.submit(operation, vec![put(key as u32, key as u8)])
            .unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    let later_appended = loop {
        if fixture.nodes[0]
            .contains_batch(
                1,
                BatchId {
                    database: fixture.config.storage.database,
                    operation: operations[3],
                },
            )
            .unwrap()
        {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        thread::yield_now();
    };
    assert!(
        later_appended,
        "successor must reach the appender during the stall"
    );
    assert_eq!(db.inflight[0].len(), 2);
    assert_eq!(db.publication.published_prefix().next_sequence(), 0);
    assert_eq!(
        db.publication.durable_prefix().next_sequence(),
        0,
        "the later append must not make a response while its prefix is stalled"
    );
    assert!(
        db.inflight[0]
            .iter()
            .all(|pending| matches!(pending.reply.try_recv(), Err(mpsc::TryRecvError::Empty)))
    );
    assert!((0..4u32).all(|key| {
        db.index
            .read()
            .get(KeySpace(0), &key.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .is_none()
    }));

    release.send(()).unwrap();
    db.drain().unwrap();
    assert!(db.inflight.iter().all(VecDeque::is_empty));
    assert_eq!(db.publication.published_prefix().next_sequence(), 4);
    assert_eq!(db.pending_bytes, 0);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
    }
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
    let connection = Arc::clone(&db.clients[0].client);
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
    assert_eq!(db.inflight[0].len(), 4);
    assert!(db.staged[0].is_empty());
    assert_eq!(db.pipeline_stats.append_requests, 4);
    assert_eq!(db.publication.published_prefix().next_sequence(), 0);
    let pending_bytes = db.pending_bytes;
    let next = db.submission_next();
    let error = db
        .submit(fixture.operation(0, 4), mutations(4))
        .unwrap_err();
    assert!(error.uncertain_version.is_none());
    assert!(db.healthy);
    assert_eq!(db.submission_next(), next);
    assert_eq!(db.pending_bytes, pending_bytes);
    assert_eq!(db.inflight[0].len(), 4);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, key), None);
        assert_eq!(value(&mut db, 100 + key), None);
    }
    drop(held);
    db.drain().unwrap();
    assert_eq!(db.pending_bytes, 0);
    assert_eq!(db.publication.published_prefix().next_sequence(), 4);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 33 * 1024]));
        assert_eq!(value(&mut db, 100 + key), Some(vec![key as u8; 32]));
    }
    db.shutdown().unwrap();
    drop(db);
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
    let connection = Arc::clone(&db.clients[0].client);
    let held = connection.lock();
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
    assert_eq!(db.inflight[0].len(), 2);
    assert_eq!(db.staged[0].len(), 3);
    assert_eq!(db.inflight[1].len(), 1);
    assert_eq!(db.inflight[0][0].first, versions[0]);
    assert_eq!(db.inflight[0][1].first, versions[4]);
    for &slot in &db.staged[0] {
        assert_eq!(
            db.submissions[slot - db.submission_base as usize]
                .batch
                .as_ref()
                .unwrap()
                .mutations()
                .len(),
            3
        );
    }
    let other = db.inflight[1].pop_front().unwrap();
    let receipts = other
        .reply
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    db.accept_receipts(receipts).unwrap();
    db.publish_completed().unwrap();
    assert_eq!(db.publication.published_prefix().next_sequence(), 0);
    for key in 0..4u32 {
        assert_eq!(value(&mut db, 200 + key), Some(vec![key as u8; 32]));
    }
    for key in 0..11u32 {
        assert_eq!(value(&mut db, key), None);
        assert_eq!(value(&mut db, 100 + key), None);
    }
    assert_eq!(value(&mut db, 300), None);
    drop(held);

    // Completion refills this owner even though the three staged batches are a
    // partial group. Unrelated owners' tails are not flushed by this pass.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        db.progress_pipeline(false).unwrap();
        if db.inflight[0]
            .iter()
            .any(|group| group.first == versions[8])
        {
            break;
        }
        assert!(Instant::now() < deadline, "owner did not refill");
        let _ = db.completion_wake.recv_timeout(Duration::from_millis(100));
    }
    assert!(db.staged[0].is_empty());
    assert!(db.inflight[0].len() <= OWNER_APPEND_PIPELINE_GROUPS);
    assert_eq!(db.pipeline_stats.append_requests, 4);
    // Either correlated durable reply can arrive first. Every already
    // published batch must expose both distinct keys; its whole successors
    // remain invisible until their native-key dependencies resolve.
    for key in 0..8u32 {
        let expected = db
            .publication
            .is_published(versions[key as usize])
            .then(|| vec![key as u8; 32]);
        assert_eq!(value(&mut db, key), expected);
        assert_eq!(value(&mut db, 100 + key), expected);
    }
    for key in 8..11u32 {
        assert_eq!(value(&mut db, key), None);
        assert_eq!(value(&mut db, 100 + key), None);
    }
    db.drain().unwrap();
    assert_eq!(db.publication.published_prefix().next_sequence(), 15);
    for key in 0..11u32 {
        assert_eq!(value(&mut db, key), Some(vec![key as u8; 32]));
        assert_eq!(value(&mut db, 100 + key), Some(vec![key as u8; 32]));
    }
    assert_eq!(value(&mut db, 300), Some(vec![10; 32]));
    db.shutdown().unwrap();
    drop(db);
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
fn write_profile_limit_stops_dispatch_after_one_selected_append() {
    let mut fixture = Fixture::new();
    fixture.config.write_profile_limit = 1;
    let mut db = fixture.open();
    for key in 0..4u64 {
        db.submit(fixture.operation(0, key), vec![put(key as u32, key as u8)])
            .unwrap();
    }
    db.drain().unwrap();
    db.finish_write_profile().unwrap();
    let profile = *db.write_profile.lock();
    assert_eq!(profile.sampled_requests, 1);
    assert_eq!(profile.captured_connections, 1);
    db.shutdown().unwrap();
}

#[test]
fn synchronous_append_preserves_other_correlated_completions() {
    let fixture = Fixture::new();
    let mut authority = fixture.open();
    let endpoint = &fixture.config.endpoints[0];
    let mut tls = fixture.config.tls.clone();
    tls.server_name = endpoint.server_name.clone();
    let mut client = Client::connect(
        &endpoint.address,
        &tls,
        Duration::from_secs(5),
        fixture.config.partition_config(1, endpoint.partition),
        &fixture.shape,
    )
    .unwrap();
    let first = CompleteBatch::new(
        &fixture.shape,
        BatchId {
            database: fixture.config.storage.database,
            operation: fixture.operation(0, 0),
        },
        BatchVersion {
            epoch: 1,
            sequence: 0,
        },
        vec![put(0, 1)],
        fixture.config.storage.batch_limits,
    )
    .unwrap();
    let second = CompleteBatch::new(
        &fixture.shape,
        BatchId {
            database: fixture.config.storage.database,
            operation: fixture.operation(0, 1),
        },
        BatchVersion {
            epoch: 1,
            sequence: 1,
        },
        vec![put(1, 2)],
        fixture.config.storage.batch_limits,
    )
    .unwrap();
    let first_id = client
        .submit_append_many(1, std::slice::from_ref(&first))
        .unwrap()
        .unwrap();
    let second_receipts = client
        .append_many(1, std::slice::from_ref(&second))
        .unwrap();
    assert_eq!(second_receipts[0].appended().version(), second.version());

    let deadline = Instant::now() + Duration::from_secs(5);
    let first_receipts = loop {
        if let Some(completion) = client
            .progress_appends()
            .unwrap()
            .into_iter()
            .find(|completion| completion.id == first_id)
        {
            break completion.result.unwrap();
        }
        if Instant::now() >= deadline {
            panic!("first correlated append completion was lost");
        }
        thread::yield_now();
    };
    assert_eq!(first_receipts[0].appended().version(), first.version());
    authority.shutdown().unwrap();
}
