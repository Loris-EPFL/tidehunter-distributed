use super::*;
use crate::distributed::durability::{DurabilityConfig, DurabilityTier};
use crate::distributed::nvmeof::{live_metrics::LiveNvmeMetrics, live_target::LiveTargetManager};
use crate::distributed::tcp::{self, Client, NodeConfig, StorageNode, TlsFiles, Transport};
use crate::distributed::test_bounds::PROGRESS_CAP;
use crate::distributed::write_timeline::WriteTimeline;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

pub(crate) struct Fixture {
    pub(crate) config: NodeConfig,
    pub(crate) node: Arc<StorageNode>,
    pub(crate) metrics: Arc<LiveNvmeMetrics>,
    pub(crate) endpoint: String,
    pub(crate) tls: TlsFiles,
    client_timeout: Duration,
    stop: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<()>>,
    _temp: tempdir::TempDir,
}

impl Fixture {
    pub(crate) fn new(tier: DurabilityTier, fragment: u64) -> Self {
        Self::new_with_partitions(tier, fragment, vec![2, 5])
    }

    pub(crate) fn single_partition(tier: DurabilityTier, fragment: u64) -> Self {
        Self::new_with_partitions(tier, fragment, vec![2])
    }
    pub(crate) fn new_with_partitions(t: DurabilityTier, f: u64, parts: Vec<u32>) -> Self {
        let temp = tempdir::TempDir::new("leased-coordinator").unwrap();
        let mut config = tcp::tests::config(&temp.path().join("node"));
        config.partition.partition = parts[0];
        config.partition.inventory.partitions = parts;
        config.partition.fragment_bytes = f;
        config.partition.wal_file_bytes = f * 2;
        config.partition.batch_limits.max_encoded_bytes = 8192;
        config.partition.group_limits.max_encoded_bytes = 16376.min(f as usize - 8);
        config.partition.max_batches = 4096;
        config.partition.max_reference_metadata_bytes = 64 * 1024 * 1024;
        Self::open(t, config, temp, Duration::from_secs(10))
    }

    pub(crate) fn native_geometry() -> Self {
        let temp = tempdir::TempDir::new("leased-native-profile").unwrap();
        let mut config = tcp::tests::config(&temp.path().join("node"));
        config.partition.fragment_bytes = 128 * 1024 * 1024;
        config.partition.wal_file_bytes = 10 * 1024 * 1024 * 1024;
        config.partition.batch_limits.max_encoded_bytes = 64 * 1024 + 1024;
        config.partition.group_limits.max_batches = 16;
        config.partition.group_limits.max_encoded_bytes = 2 * 1024 * 1024;
        config.partition.max_batches = 4096;
        config.partition.max_reference_metadata_bytes = 64 * 1024 * 1024;
        // Debug validation of a full 128 MiB backlog can exceed ten seconds.
        Self::open(
            DurabilityTier::Native,
            config,
            temp,
            Duration::from_secs(60),
        )
    }

    fn open(
        tier: DurabilityTier,
        config: NodeConfig,
        temp: tempdir::TempDir,
        client_timeout: Duration,
    ) -> Self {
        let node = StorageNode::open(config.clone(), tcp::tests::metrics()).unwrap();
        node.set_durability(DurabilityConfig::with_tier(tier))
            .unwrap();
        let state_dir = temp.path().join("target");
        let configfs = temp.path().join("configfs");
        std::fs::create_dir(&state_dir).unwrap();
        std::fs::create_dir(&configfs).unwrap();
        for name in ["hosts", "ports", "subsystems"] {
            std::fs::create_dir(configfs.join(name)).unwrap();
        }
        let target = LiveTargetManager::open_fixture(
            super::super::tests::live_fixture_target_config(&config.partition, state_dir),
            configfs,
        )
        .unwrap();
        node.install_live_target(target).unwrap();
        let tls = TlsFiles {
            transport: Transport::TcpTrusted,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".into(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let stop = Arc::new(AtomicBool::new(false));
        let server_node = node.clone();
        let server_stop = stop.clone();
        let server_tls = tls.clone();
        let limits = tcp::TcpLimits {
            io_timeout: client_timeout,
            ..Default::default()
        };
        let server = thread::spawn(move || {
            server_node
                .serve(listener, server_tls, limits, server_stop)
                .unwrap()
        });
        Self {
            _temp: temp,
            config,
            node,
            metrics: LiveNvmeMetrics::new(),
            endpoint,
            tls,
            client_timeout,
            stop,
            server: Some(server),
        }
    }

    pub(crate) fn client(&self) -> Client {
        let shape = crate::key_shape::KeyShape::from_yaml(&self.config.shape_yaml).unwrap();
        Client::connect(
            &self.endpoint,
            &self.tls,
            self.client_timeout,
            self.config.partition.clone(),
            &shape,
        )
        .unwrap()
    }

    pub(crate) fn writer(&self) -> Client {
        let mut client = self.client();
        client.fence_to(1).unwrap();
        self.configure_writer(&mut client, self.node.durability().tier);
        client
    }

    pub(crate) fn writer_without_ahead(&self) -> Client {
        let mut client = self.writer();
        let mut config = client.live_config.clone().unwrap();
        config.lease_ahead = false;
        client
            .configure_live_nvme(
                &config,
                crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
            )
            .unwrap();
        client
    }

    fn configure_writer(&self, client: &mut Client, tier: DurabilityTier) {
        client
            .configure_live_nvme(
                &LiveClientConfig {
                    lease_ahead: true,
                    polled_writes: false,
                    host_nqn: "nqn.2026-01.test:leased".into(),
                    host_id: [1; 16],
                    owner_identities: Default::default(),
                    leased_write_window: Some(4),
                    leased_sync: LeasedSync::PerFrameFua,
                    leased_flush_min_frame_bytes: 0,
                    max_inflight_requests: 4,
                    max_inflight_bytes: (self.config.partition.group_limits.max_encoded_bytes
                        + 4096)
                        .max(128 * 1024),
                    connect_timeout_ms: 1000,
                    io_timeout_ms: 1000,
                    leased_durability: tier,
                    inline_reads: true,
                    queue_geometry: Default::default(),
                },
                crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
            )
            .unwrap();
        client.live_write_local_fixture = true;
        client.set_live_metrics(self.metrics.clone()).unwrap();
    }

    pub(crate) fn batch(&self, sequence: u64) -> tcp::CompleteBatch {
        tcp::tests::batch(&self.config, 1, sequence)
    }

    fn big_batch(&self, sequence: u64) -> tcp::CompleteBatch {
        let original = self.batch(sequence);
        crate::distributed::codec::CompleteBatch::new(
            &crate::key_shape::KeyShape::from_yaml(&self.config.shape_yaml).unwrap(),
            original.id(),
            original.version(),
            vec![crate::distributed::codec::Mutation::Put {
                keyspace: crate::key_shape::KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![sequence as u8; 5000].into(),
            }],
            self.config.partition.batch_limits,
        )
        .unwrap()
    }
}

/// Owner-side read of `reference` at the owner's serving boundary, the
/// oracle that replaced the removed owner-RPC `Client::read_records` (now a
/// live NVMe-oF read).
///
/// A leased receipt can name bytes the owner adopted whose frontier
/// publication still lags the acknowledgement, so the production read
/// boundary persists them before it serves the record
/// (`load_serving_record_authority` → `persist_for_read`). This oracle takes
/// the same step instead of asserting the read-side frontier directly.
pub(crate) fn owner_read(
    fixture: &Fixture,
    reference: &crate::distributed::types::ValueRecordReference,
) -> Vec<crate::distributed::codec::Mutation> {
    use crate::distributed::storage::StorageError;
    let log = fixture.node.log_for_test(1);
    if let Err(error) = log.validate_durable_record_reference(reference) {
        assert!(
            matches!(error, StorageError::NeedsPersistence),
            "the owner rejects record {}: {error}",
            reference.batch_address.offset
        );
        // The read's own covering barrier, the same pair a durable-progress
        // nudge runs on the owner: adopt the leased frames, then publish
        // everything appended. Both run off the syncer thread.
        if let Err(error) = log.self_adopt_live_extent() {
            panic!(
                "the owner cannot adopt record {} before serving it: {error}",
                reference.batch_address.offset
            );
        }
        if let Err(error) = log.persist_live_progress() {
            panic!(
                "the owner cannot persist record {} before serving it: {error}",
                reference.batch_address.offset
            );
        }
    }
    if let Err(error) = log.validate_durable_record_reference(reference) {
        panic!(
            "the owner still cannot serve record {} after persisting it: {error}",
            reference.batch_address.offset
        );
    }
    vec![
        log.read_record(reference)
            .expect("the owner reads its record"),
    ]
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Release wakes the server's shutdown checks; no stream state travels.
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

#[test]
fn configured_client_installs_its_partition_identity() {
    let f = Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut client = f.writer();
    let mut config = client.live_config.clone().unwrap();
    let identity = super::LiveInitiatorIdentity {
        host_nqn: "nqn.2026-01.test:owner-specific".into(),
        host_id: [2; 16],
    };
    config
        .owner_identities
        .insert(client.expected.partition, identity.clone());
    let json = serde_json::to_value(&config).unwrap();
    let key = client.expected.partition.to_string();
    assert_eq!(
        json["owner_identities"][&key]["host_id"],
        serde_json::json!(vec![2; 16])
    );
    let decoded: LiveClientConfig = serde_json::from_value(json).unwrap();
    client
        .configure_live_nvme(
            &decoded,
            crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
        )
        .unwrap();
    let selected = client.live_config.as_ref().unwrap();
    assert_eq!(selected.host_nqn, identity.host_nqn);
    assert_eq!(selected.host_id, identity.host_id);
    assert!(selected.owner_identities.is_empty());
}

pub(crate) fn leased_latencies(
    counts: &crate::distributed::nvmeof::live_metrics::LiveNvmeCounts,
) -> [(u64, u64); 5] {
    [
        (counts.queue_samples, counts.queue_ns),
        (counts.submit_samples, counts.submit_ns),
        (counts.cq_samples, counts.cq_ns),
        (counts.leased_write_samples, counts.leased_write_ns),
        (counts.extent_switch_samples, counts.extent_switch_ns),
    ]
}

fn write(client: &mut Client, batch: &tcp::CompleteBatch) -> tcp::DurableBatch {
    client
        .submit_and_drain_leased_group(1, vec![batch.clone()], &WriteTimeline::default(), 2)
        .unwrap()[0]
}

#[test]
fn polled_leased_frame_and_skip_complete_or_report_unsupported() {
    use crate::distributed::nvmeof::live_io::{LiveIo, LiveIoError};
    use crate::distributed::nvmeof::live_namespace::{LiveNamespace, LiveNamespaceAccess};
    let unsupported = |errno| {
        assert!(
            matches!(errno, Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)),
            "unexpected polled writer errno: {errno:?}"
        );
        eprintln!("local filesystem or kernel rejects polled writes: errno={errno:?}");
    };
    let unsupported_write = |client: &mut Client, error: super::super::TcpError| {
        assert!(
            error.to_string().starts_with("leased write failed"),
            "unexpected polled writer error: {error}"
        );
        let io = &mut client.live_writer.as_mut().unwrap().io;
        unsupported(io.last_write_errno());
        assert_eq!(io.outstanding(), 0);
        assert!(io.shutdown().unwrap().is_empty());
    };
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 16384);
        let mut client = fixture.writer_without_ahead();
        let mut config = client.live_config.clone().unwrap();
        config.polled_writes = true;
        config.leased_sync = LeasedSync::GroupFlush;
        config.leased_flush_min_frame_bytes = 0;
        config.queue_geometry.poll_queues = 1;
        client
            .configure_live_nvme(
                &config,
                crate::distributed::remote_db::RemoteDatabaseConfig::MAX_OWNER_APPEND_OCCUPANCY,
            )
            .unwrap();
        let leased = client.lease_live_extent(1).unwrap();
        let path = leased.extent.segment.path.clone();
        let namespace = LiveNamespace::open_local_file(
            &path,
            leased.binding.bytes,
            leased.binding.block_size,
            LiveNamespaceAccess::ReadWrite,
        )
        .unwrap();
        client.install_leased_extent(leased).unwrap();
        // The ordinary local fixture stays interrupt-driven. Replace only
        // this test's queue so it exercises the actual leased completion path.
        client.ensure_leased_queue().unwrap();
        let io = match LiveIo::new(namespace, 4, config.max_inflight_bytes, true) {
            Ok(io) => io,
            Err(LiveIoError::Io(error)) => {
                unsupported(error.raw_os_error());
                return;
            }
            Err(error) => panic!("polled writer setup failed: {error}"),
        };
        client.live_writer.as_mut().unwrap().io = io;
        let batch = fixture.batch(0);
        if let Err(error) = client.submit_and_drain_leased_group(
            1,
            vec![batch.clone()],
            &WriteTimeline::default(),
            2,
        ) {
            unsupported_write(&mut client, error);
            return;
        }
        let cursor = client.live_leased.current.as_ref().unwrap().cursor;
        if let Err(error) = client.write_leased_skip() {
            unsupported_write(&mut client, error);
            return;
        }
        assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 0);
        assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 2);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.leased_flushes, 0);
        assert_eq!(
            counts.fua_writes,
            if tier == DurabilityTier::Synchronous {
                2
            } else {
                0
            }
        );
        let file = std::fs::File::open(path).unwrap();
        let mut marker = [0; 8];
        std::os::unix::fs::FileExt::read_exact_at(&file, &mut marker, cursor).unwrap();
        assert_eq!(&marker, crate::crc::CrcFrame::skip_marker().as_ref());
        // Local IOPOLL can reject an already-written block at close too,
        // not just ring setup or the first write. Keep the documented
        // unsupported outcome through the final closing skip as well.
        if let Err(error) = client.close_live_writer(&mut Vec::new(), None) {
            unsupported_write(&mut client, error);
            return;
        }
        let log = fixture.node.inventory.lock().active(1).unwrap();
        assert_eq!(log.receipt_count(), 1);
    }
}

#[cfg(all(target_os = "linux", feature = "test-utils"))]
#[test]
fn mapped_residency_probe_accepts_closed_leased_extent() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    write(&mut client, &fixture.batch(0));
    let log = fixture.node.inventory.lock().active(1).unwrap();
    assert_eq!(
        log.read_evidence_for_benchmark(true)
            .unwrap_err()
            .to_string(),
        "mapped residency probe unavailable during live writes"
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    assert!(log.read_evidence_for_benchmark(true).is_ok());
}

#[test]
fn leased_both_tiers_acknowledge_cqe_and_match_owner_receipts() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 16384);
        let mut client = fixture.writer();
        let mut owner_reader = fixture.client();
        for sequence in 0..3 {
            let batch = fixture.batch(sequence);
            let previous_adoptions = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
            let receipt = write(&mut client, &batch);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            let descriptor = batch.record_descriptors()[0];
            let entry = &batch.encoded()
                [descriptor.offset as usize + 8..(descriptor.offset + descriptor.len) as usize];
            let position = crate::distributed::hybrid::position::VersionPosition::member(
                batch.version(),
                receipt.appended().address(),
                0,
                descriptor,
                crc32fast::hash(entry),
            );
            // No adoption exchange is needed to acknowledge the frame.
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(
                counts.adoption_async_exchanges + counts.adoption_sync_exchanges,
                previous_adoptions
            );
            assert_eq!(owner_read(&fixture, &reference), batch.mutations());
            // Active-epoch `ResolveLiveRecords` is refused in per-owner
            // export mode; pin the owner's real WAL inode directly instead
            // (the same grant the resolution used to return).
            let mut cache = crate::distributed::tcp::live_read::LiveReadCache::default();
            crate::distributed::tcp::live_read::tests::install_owner_local(
                &mut cache,
                &fixture.node,
                position,
            );
            let member = batch.encoded()
                [descriptor.offset as usize..(descriptor.offset + descriptor.len) as usize]
                .to_vec();
            let fetched = client
                .live_read_records(
                    &mut cache,
                    1,
                    &[position],
                    Instant::now() + Duration::from_secs(2),
                    None,
                )
                .unwrap();
            assert_eq!(fetched.len(), 1);
            assert_eq!(fetched[0].as_ref(), member.as_slice());
            cache.close(&mut owner_reader, 1).unwrap();
            client.settle_leased_extent(&mut Vec::new(), None).unwrap();
            let adopted = client.live_leased.current.as_mut().unwrap().adopted;
            assert_eq!(
                adopted.end,
                client.live_leased.current.as_mut().unwrap().written_end
            );
            assert_eq!(adopted.count, sequence + 1);
        }
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(
            (
                counts.extents_leased,
                counts.leased_frames,
                counts.completed_write_ops
            ),
            (1, 3, 3)
        );
        assert_eq!(counts.lease_exchanges, 1);
        assert_eq!(
            counts.fua_writes,
            if tier == DurabilityTier::Synchronous {
                3
            } else {
                0
            }
        );
        assert_eq!(counts.leased_write_samples, 3);
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        assert!(client.live_leased.current.is_none());
    }
}

#[test]
fn leased_coordinator_crash_recovers_all_acknowledged_frames_in_both_tiers() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 16384);
        let mut client = fixture.writer();
        let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
        let receipts: Vec<_> = batches
            .iter()
            .map(|batch| write(&mut client, batch))
            .collect();
        assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
        drop(client);
        let mut recovery = fixture.client();
        recovery.fence_to(2).unwrap();
        let log = fixture
            .node
            .inventory
            .lock()
            .live_recovery_log(2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(log.receipt_count(), receipts.len());
        for (batch, receipt) in batches.iter().zip(receipts) {
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
        }
    }
}

#[test]
fn leased_duplicate_group_is_rejected_before_lease_or_submission() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    let batch = fixture.batch(0);
    assert!(
        client
            .submit_and_drain_leased_group(
                1,
                vec![batch.clone(), batch],
                &WriteTimeline::default(),
                2
            )
            .is_err()
    );
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!((counts.lease_exchanges, counts.completed_write_ops), (0, 0));
    assert!(client.live_writer.is_none());
}

#[test]
fn leased_group_limit_is_rejected_before_lease_or_buffer() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    client.expected.group_limits.max_encoded_bytes = 32;
    assert!(
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .is_err()
    );
    assert!(client.live_writer.is_none());
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!((counts.lease_exchanges, counts.completed_write_ops), (0, 0));
}

#[test]
fn leased_async_adoption_accepts_self_adopted_later_prefix() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    // Capture the same identity used by an asynchronous exchange.
    let pending = PendingAdoption {
        target: PendingTarget::Current,
        through: 4096,
        close: false,
        lease_next: false,
    };
    write(&mut client, &fixture.batch(1));
    let current = client.live_leased.current.as_ref().unwrap();
    let token = current.lease.extent.token;
    let epoch = current.lease.extent.segment.epoch;
    // Real owner self-adoption publishes the intermediate boundary.
    let reference = fixture
        .batch(1)
        .record_reference(
            crate::distributed::storage::receipt_for(
                &fixture.config.partition,
                &fixture.batch(1),
                crate::distributed::storage::member_position(
                    crate::WalPosition::new(4096, 4096),
                    16,
                    &fixture.batch(1),
                ),
            )
            .address(),
            0,
        )
        .unwrap();
    owner_read(&fixture, &reference);
    let reply = client
        .adopt_live_extent(epoch, token, 4096, false, false)
        .unwrap();
    write(&mut client, &fixture.batch(2));
    assert_eq!(reply.adoption.adopted_end, 8192);
    client
        .live_leased
        .current
        .as_mut()
        .unwrap()
        .validate_adoption(pending, &reply)
        .unwrap();
    assert_eq!(
        client.live_leased.current.as_mut().unwrap().prefixes.len(),
        1
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn leased_extent_switch_writes_skip_and_continues_at_fragment_start() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new(DurabilityTier::Synchronous, 16384);
    let mut client = fixture.writer_without_ahead();
    let first = fixture.batch(0);
    let second = fixture.big_batch(1);
    let third = fixture.big_batch(2);
    let receipts = [
        write(&mut client, &first),
        write(&mut client, &second),
        write(&mut client, &third),
    ];
    assert_eq!(
        client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .start,
        16384
    );
    assert!(receipts[2].appended().address().offset >= 16384);
    let segment = &client
        .live_leased
        .current
        .as_ref()
        .unwrap()
        .lease
        .extent
        .segment;
    let mut marker = [0u8; 4096];
    std::fs::File::open(&segment.path)
        .unwrap()
        .read_exact_at(&mut marker, 12288)
        .unwrap();
    assert_eq!(&marker[..8], &[255; 8]);
    assert!(marker[8..].iter().all(|b| *b == 0));
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(
        (
            counts.extents_leased,
            counts.leased_frames,
            counts.leased_skip_markers
        ),
        (2, 3, 1)
    );
    assert_eq!(
        (
            counts.adoption_sync_exchanges,
            counts.extent_switch_samples,
            counts.fua_writes
        ),
        (1, 1, 4)
    );
    for (batch, receipt) in [&first, &second, &third].into_iter().zip(receipts) {
        let reference = batch
            .record_reference(receipt.appended().address(), 0)
            .unwrap();
        assert_eq!(owner_read(&fixture, &reference), batch.mutations());
    }
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn leased_refused_async_submission_records_no_timing() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    let deadline = Instant::now() + Duration::from_secs(2);
    assert!(
        client
            .submit_control(&tcp::Request::Stats, deadline)
            .unwrap()
    );
    client.submit_leased_adoption().unwrap();
    assert!(!client.leased_adoption_pending());
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.adoption_async_exchanges, 0);
    assert_eq!(counts.adoption_async_samples, 0);
    loop {
        client.progress_session().unwrap();
        if let Some(reply) = client.take_control() {
            reply.unwrap();
            break;
        }
        assert!(Instant::now() < deadline);
        thread::yield_now();
    }
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn leased_bad_digest_poison_prevents_later_write_or_control() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    write(&mut client, &fixture.batch(0));
    client.live_leased.current.as_mut().unwrap().prefixes[0].digest[0] ^= 1;
    assert!(client.settle_leased_extent(&mut Vec::new(), None).is_err());
    let before = fixture.metrics.snapshot().counts;
    assert_eq!(before.adoption_async_samples, 0);
    assert_eq!(before.adoption_settle_samples, 0);
    assert_eq!(before.adoption_history_full_settles, 0);
    assert!(
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
            .is_err()
    );
    assert!(client.close_leased_extent(&mut Vec::new(), None).is_err());
    let after = fixture.metrics.snapshot().counts;
    assert_eq!(after.completed_write_ops, before.completed_write_ops);
    assert_eq!(after.metadata_requests, before.metadata_requests);
    assert_eq!(after.adoption_async_samples, 0);
    assert_eq!(after.adoption_settle_samples, 0);
}

#[test]
fn leased_adoption_rejects_shortfall_nonboundary_and_future_offsets() {
    for (through, adopted_end) in [(8192, 4096), (4096, 6144), (4096, 12288)] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        write(&mut client, &fixture.batch(1));
        let current = client.live_leased.current.as_ref().unwrap();
        let mut reply = client
            .adopt_live_extent(1, current.lease.extent.token, 8192, false, false)
            .unwrap();
        reply.adoption.adopted_end = adopted_end;
        assert!(
            client
                .check_leased_adoption(
                    PendingAdoption {
                        target: PendingTarget::Current,
                        through,
                        close: false,
                        lease_next: false
                    },
                    &reply,
                )
                .is_err()
        );
        assert!(client.live_write_failed);
        let before = fixture.metrics.snapshot().counts.completed_write_ops;
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(2)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert_eq!(
            fixture.metrics.snapshot().counts.completed_write_ops,
            before
        );
    }
}

#[test]
fn leased_coordinator_crash_recovers_acknowledged_prefix_and_drops_torn_tail() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    let first = fixture.batch(0);
    let receipt = write(&mut client, &first);
    let second = fixture.batch(1);
    let third = fixture.batch(2);
    let group = crate::distributed::group::CompleteGroup::new_live(
        &client.shape,
        vec![second],
        fixture.config.partition.batch_limits,
        fixture.config.partition.group_limits,
    )
    .unwrap();
    let (_, torn) =
        crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group).unwrap();
    let group = crate::distributed::group::CompleteGroup::new_live(
        &client.shape,
        vec![third],
        fixture.config.partition.batch_limits,
        fixture.config.partition.group_limits,
    )
    .unwrap();
    let (_, later) =
        crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group).unwrap();
    let path = client
        .live_leased
        .current
        .as_ref()
        .unwrap()
        .lease
        .extent
        .segment
        .path
        .clone();
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&torn[..100], 4096).unwrap();
    file.write_all_at(&later, 8192).unwrap();
    drop(file);
    drop(client); // No settle or close: owner must recover from its WAL.
    let mut recovered = fixture.client();
    recovered.fence_to(2).unwrap();
    let log = fixture
        .node
        .inventory
        .lock()
        .live_recovery_log(2, 1)
        .unwrap()
        .unwrap();
    assert_eq!(log.receipt_count(), 1);
    let reference = first
        .record_reference(receipt.appended().address(), 0)
        .unwrap();
    assert_eq!(log.read_record(&reference).unwrap(), first.mutations()[0]);
}

#[test]
fn leased_async_timing_counts_only_validated_completion() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    client.submit_leased_adoption().unwrap();
    client.submit_leased_adoption().unwrap();
    let submitted = fixture.metrics.snapshot().counts;
    assert_eq!(submitted.adoption_async_exchanges, 1);
    assert_eq!(submitted.adoption_async_samples, 0);
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    let completed = fixture.metrics.snapshot().counts;
    assert_eq!(completed.adoption_async_samples, 1);
    assert!(completed.adoption_async_ns > 0);
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 0);
}

#[test]
fn leased_history_full_settles_before_another_submission() {
    let fixture = Fixture::new(DurabilityTier::Native, 8 * 1024 * 1024);
    let mut client = fixture.writer_without_ahead();
    assert_eq!(client.live_leased.prefix_buffers[0].capacity(), 1024);
    for sequence in 0..1024 {
        write(&mut client, &fixture.batch(sequence));
    }
    assert_eq!(
        client.live_leased.current.as_mut().unwrap().prefixes.len(),
        1024
    );
    assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
    write(&mut client, &fixture.batch(1024));
    assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 1);
    assert_eq!(
        fixture
            .metrics
            .snapshot()
            .counts
            .adoption_history_full_settles,
        1
    );
    assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 1);
    assert_eq!(
        client.live_leased.current.as_mut().unwrap().prefixes.len(),
        1
    );
    client.settle_leased_extent(&mut Vec::new(), None).unwrap();
    assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 2);
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    assert_eq!(
        fixture
            .metrics
            .snapshot()
            .counts
            .adoption_history_full_settles,
        1
    );
    assert_eq!(fixture.metrics.snapshot().counts.adoption_settle_samples, 2);
}

#[test]
fn omitted_optional_keys_default_and_removed_write_mode_is_rejected() {
    assert!(
        LeasedWriter::default()
            .prefix_buffers
            .iter()
            .all(|buffer| buffer.capacity() == 0)
    );
    let value = serde_json::json!({
        "host_nqn": "nqn.2026-01.test:leased", "host_id": vec![1; 16],
        "max_inflight_requests": 4, "max_inflight_bytes": 65536,
        "connect_timeout_ms": 1000, "io_timeout_ms": 1000,
        "leased_durability": "native",
    });
    let config: LiveClientConfig = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(config.leased_durability, DurabilityTier::Native);
    assert!(config.inline_reads);
    assert!(config.lease_ahead);
    assert!(!config.polled_writes);
    let mut missing = value.clone();
    missing.as_object_mut().unwrap().remove("leased_durability");
    assert!(serde_json::from_value::<LiveClientConfig>(missing).is_err());
    let mut removed = value;
    removed["write_mode"] = serde_json::json!("leased");
    let error = serde_json::from_value::<LiveClientConfig>(removed)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown field"), "{error}");
}

#[test]
fn leased_traced_failed_and_short_completions_keep_unknown_write_contract() {
    use crate::distributed::write_timeline::WriteTimelineStage as Stage;
    for result in [-libc::EIO, 2048] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        client.leased_test_fail_write(result);
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        let batch = fixture.batch(0);
        let sample = timeline.begin(0).unwrap();
        timeline.operation(sample, batch.id().operation);
        timeline.batch(batch.id().operation, batch.version());
        assert!(
            client
                .submit_and_drain_leased_group(1, vec![batch], &timeline, 2)
                .is_err()
        );
        assert!(client.live_write_failed);
        let export = timeline.finish().unwrap();
        let stages: Vec<_> = export.records[0]
            .events
            .iter()
            .map(|event| event.stage)
            .collect();
        let start = stages
            .iter()
            .position(|stage| *stage == Stage::LiveIoStarted)
            .unwrap();
        assert_eq!(
            &stages[start..],
            &[
                Stage::LiveIoStarted,
                Stage::LiveFrameEncoded,
                Stage::LiveIoSubmitted,
                Stage::LiveIoCompleted,
            ]
        );
        assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
        let mut failed = client.take_failed_live_writer().unwrap();
        failed.drain_failed().unwrap();
        assert_eq!(failed.io.outstanding(), 0);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.failed_write_ops, u64::from(result < 0));
        assert_eq!(counts.short_write_ops, u64::from(result == 2048));
    }
}

#[test]
fn leased_failed_short_and_timed_out_writes_are_unknown_and_never_retried() {
    for (result, timeout) in [
        (Some(-libc::EIO), false),
        (Some(2048), false),
        (None, true),
        (Some(-libc::EIO), true),
        (Some(2048), true),
    ] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        if let Some(result) = result {
            client.leased_test_fail_write(result);
        }
        if timeout {
            client.leased_test_timeout_write();
        }
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert!(client.live_write_failed);
        let submitted = client
            .live_writer
            .as_ref()
            .unwrap()
            .io
            .stats()
            .write_requests;
        assert_eq!(submitted, 1);
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(1)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert!(client.settle_leased_extent(&mut Vec::new(), None).is_err());
        assert_eq!(
            client
                .live_writer
                .as_ref()
                .unwrap()
                .io
                .stats()
                .write_requests,
            submitted
        );
        assert_eq!(fixture.metrics.snapshot().counts.unknown_writes, 1);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
        assert_eq!(
            counts.leased_failed_writes,
            u64::from(!timeout && result.is_some_and(|n| n < 0))
        );
        assert_eq!(
            counts.leased_short_writes,
            u64::from(!timeout && result == Some(2048))
        );
        assert_eq!(counts.leased_timed_out_writes, u64::from(timeout));
        let mut failed = client.take_failed_live_writer().unwrap();
        failed.drain_failed().unwrap();
        for retire_again in [false, true] {
            if retire_again {
                failed.drain_failed().unwrap();
            }
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
            assert_eq!(
                counts.failed_write_ops,
                u64::from(result.is_some_and(|n| n < 0))
            );
            assert_eq!(counts.short_write_ops, u64::from(result == Some(2048)));
            assert_eq!(
                counts.short_write_bytes,
                if result == Some(2048) { 2048 } else { 0 }
            );
            assert_eq!(counts.completed_write_ops, u64::from(result.is_none()));
            assert_eq!(
                counts.completed_write_bytes,
                if result.is_none() { 4096 } else { 0 }
            );
        }
    }
}

#[test]
fn leased_client_drop_publishes_late_terminal_counts_without_latency() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    client.leased_test_timeout_write();
    assert!(
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2,)
            .is_err()
    );
    assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 1);
    drop(client);
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
    assert_eq!(counts.completed_write_ops, 1);
    assert_eq!(counts.completed_write_bytes, 4096);
    assert_eq!((counts.failed_write_ops, counts.short_write_ops), (0, 0));
}

#[test]
fn leased_rejects_wrong_geometry_without_submitting_a_write() {
    for bad in 0..3 {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        let mut leased = client.lease_live_extent(1).unwrap();
        match bad {
            0 => leased.extent.start += 1,
            1 => leased.binding.bytes += 4096,
            _ => leased.extent.end += 4096,
        }
        assert!(client.install_leased_extent(leased).is_err());
        assert!(client.live_write_failed);
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![fixture.batch(0)],
                    &WriteTimeline::default(),
                    2
                )
                .is_err()
        );
        assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 0);
    }
}

#[test]
fn leased_noncontiguous_next_extent_poison_stops_writes() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    client.write_leased_skip().unwrap();
    let token = client
        .live_leased
        .current
        .as_ref()
        .unwrap()
        .lease
        .extent
        .token;
    let mut reply = client
        .adopt_live_extent(1, token, 4096, true, true)
        .unwrap();
    reply.next.as_mut().unwrap().extent.start += 4096;
    assert!(
        client
            .check_leased_adoption(
                PendingAdoption {
                    target: PendingTarget::Current,
                    through: 4096,
                    close: true,
                    lease_next: true
                },
                &reply,
            )
            .is_err()
    );
    let before = fixture.metrics.snapshot().counts.completed_write_ops;
    assert!(
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
            .is_err()
    );
    assert_eq!(
        fixture.metrics.snapshot().counts.completed_write_ops,
        before
    );
}

#[test]
fn leased_checks_epoch_before_write() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    assert!(
        client
            .submit_and_drain_leased_group(2, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .is_err()
    );
    assert_eq!(fixture.metrics.snapshot().counts.metadata_requests, 0);
}

#[test]
fn polled_writes_require_a_poll_queue() {
    let mut config: LiveClientConfig = serde_json::from_value(serde_json::json!({
        "host_nqn": "nqn.2026-01.test:leased", "host_id": vec![1; 16],
        "max_inflight_requests": 4, "max_inflight_bytes": 65536,
        "connect_timeout_ms": 1000, "io_timeout_ms": 1000,
        "lease_ahead": true, "polled_writes": true, "leased_durability": "native",
    }))
    .unwrap();
    assert_eq!(
        config.validate(),
        Err("polled live writes require a poll queue")
    );
    config.queue_geometry.poll_queues = 1;
    assert!(config.validate().is_ok());
    config.polled_writes = false;
    config.queue_geometry.poll_queues = 0;
    assert!(config.validate().is_ok());
}

#[test]
fn leased_first_and_next_tier_mismatch_fail_before_another_write() {
    for actual in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let requested = if actual == DurabilityTier::Native {
            DurabilityTier::Synchronous
        } else {
            DurabilityTier::Native
        };
        let fixture = Fixture::new(actual, 16384);
        let mut client = fixture.client();
        client.fence_to(1).unwrap();
        fixture.configure_writer(&mut client, requested);
        let error = client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .unwrap_err();
        assert!(error.to_string().contains(&format!("{actual:?}")));
        assert!(error.to_string().contains(&format!("{requested:?}")));
        assert_eq!(fixture.metrics.snapshot().counts.completed_write_ops, 0);
        assert!(client.live_write_failed);
        assert!(client.live_writer.is_none());
    }
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    client.write_leased_skip().unwrap();
    let token = client
        .live_leased
        .current
        .as_ref()
        .unwrap()
        .lease
        .extent
        .token;
    let mut reply = client
        .adopt_live_extent(1, token, 4096, true, true)
        .unwrap();
    client
        .check_leased_adoption(
            PendingAdoption {
                target: PendingTarget::Current,
                through: 4096,
                close: true,
                lease_next: true,
            },
            &reply,
        )
        .unwrap();
    let mut next = reply.next.take().unwrap();
    next.tier = DurabilityTier::Synchronous;
    let before = fixture.metrics.snapshot().counts.completed_write_ops;
    assert!(client.install_leased_extent(next).is_err());
    assert!(
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(1)], &WriteTimeline::default(), 2)
            .is_err()
    );
    assert_eq!(
        fixture.metrics.snapshot().counts.completed_write_ops,
        before
    );
}

#[test]
fn leased_policy_change_after_connect_uses_frozen_lease_tier() {
    for before in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let after = if before == DurabilityTier::Native {
            DurabilityTier::Synchronous
        } else {
            DurabilityTier::Native
        };
        let fixture = Fixture::new(before, 16384);
        let mut client = fixture.client();
        fixture
            .node
            .set_durability(DurabilityConfig::with_tier(after))
            .unwrap();
        client.fence_to(1).unwrap();
        fixture.configure_writer(&mut client, after);
        write(&mut client, &fixture.batch(0));
        assert_eq!(
            client.live_leased.current.as_ref().unwrap().lease.tier,
            after
        );
        assert_eq!(
            fixture.metrics.snapshot().counts.fua_writes,
            u64::from(after == DurabilityTier::Synchronous)
        );
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }
}

#[test]
fn leased_frame_bytes_equal_the_descriptor_path() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let client = fixture.writer();
    let group = crate::distributed::group::CompleteGroup::new_live(
        &client.shape,
        vec![fixture.batch(0), fixture.big_batch(1)],
        fixture.config.partition.batch_limits,
        fixture.config.partition.group_limits,
    )
    .unwrap();
    let (_, expected) =
        crate::distributed::nvmeof::live_storage::LiveGroupDescriptor::from_group(&group).unwrap();
    assert_eq!(group.live_frame().unwrap(), expected);
}

#[test]
fn leased_full_admission_has_no_latency_sample_and_retains_dma() {
    use crate::distributed::nvmeof::live_io::{LiveSubmitOutcome, LiveWriteSync};
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    let leased = client.lease_live_extent(1).unwrap();
    client.install_leased_extent(leased).unwrap();
    client.ensure_leased_queue().unwrap();
    let io = &mut client.live_writer.as_mut().unwrap().io;
    for id in 2..6 {
        assert_eq!(
            io.submit_write(
                id,
                0,
                &[0; 4096],
                LiveWriteSync::None,
                Instant::now() + Duration::from_secs(5)
            )
            .unwrap(),
            LiveSubmitOutcome::Submitted
        );
    }
    assert!(
        client
            .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &WriteTimeline::default(), 2)
            .is_err()
    );
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.leased_full_writes, 1);
    assert_eq!(leased_latencies(&counts), [(0, 0); 5]);
    assert!(client.drain_live_writer(&mut Vec::new(), None).is_err());
    let mut failed = client.take_failed_live_writer().unwrap();
    assert_eq!(failed.io.outstanding(), 4);
    failed.drain_failed().unwrap();
    assert_eq!(failed.io.outstanding(), 0);
}

#[test]
fn leased_failed_rollover_skip_does_not_sample_any_latency_stage() {
    for result in [Some(-libc::EIO), Some(2048), None] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer();
        write(&mut client, &fixture.batch(0));
        let before = leased_latencies(&fixture.metrics.snapshot().counts);
        if let Some(result) = result {
            client.leased_test_fail_write(result);
        } else {
            client.leased_test_timeout_write();
        }
        assert!(
            client
                .submit_and_drain_leased_group(
                    1,
                    vec![
                        fixture.big_batch(1),
                        fixture.big_batch(2),
                        fixture.big_batch(3)
                    ],
                    &WriteTimeline::default(),
                    2,
                )
                .is_err()
        );
        assert!(client.live_write_failed);
        assert_eq!(leased_latencies(&fixture.metrics.snapshot().counts), before);
        let mut failed = client.take_failed_live_writer().unwrap();
        failed.drain_failed().unwrap();
        assert_eq!(leased_latencies(&fixture.metrics.snapshot().counts), before);
    }
}

#[test]
fn leased_failed_rollover_adoption_does_not_sample_extent_switch() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    client
        .live_leased
        .current
        .as_mut()
        .unwrap()
        .lease
        .extent
        .token[0] ^= 1;
    assert!(
        client
            .submit_and_drain_leased_group(
                1,
                vec![
                    fixture.big_batch(1),
                    fixture.big_batch(2),
                    fixture.big_batch(3)
                ],
                &WriteTimeline::default(),
                2,
            )
            .is_err()
    );
    let counts = fixture.metrics.snapshot().counts;
    assert!(client.live_write_failed);
    assert_eq!(
        (counts.extent_switch_samples, counts.extent_switch_ns),
        (0, 0)
    );
    // The marker itself reached a successful CQE, but the switch did not.
    assert_eq!(counts.completed_write_ops, 2);
    assert_eq!(counts.leased_write_samples, 2);
}

#[test]
fn leased_switch_preserves_a_multi_block_zero_suffix() {
    use std::os::unix::fs::FileExt;
    let fixture = Fixture::new(DurabilityTier::Synchronous, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    let old_path = client
        .live_leased
        .current
        .as_ref()
        .unwrap()
        .lease
        .extent
        .segment
        .path
        .clone();
    let group = vec![
        fixture.big_batch(1),
        fixture.big_batch(2),
        fixture.big_batch(3),
    ];
    let receipts = client
        .submit_and_drain_leased_group(1, group.clone(), &WriteTimeline::default(), 2)
        .unwrap();
    assert_eq!(
        client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .start,
        16384
    );
    let mut suffix = vec![0; 12288];
    std::fs::File::open(old_path)
        .unwrap()
        .read_exact_at(&mut suffix, 4096)
        .unwrap();
    assert_eq!(&suffix[..8], &[255; 8]);
    assert!(suffix[8..].iter().all(|byte| *byte == 0));
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 1);
    for (batch, receipt) in group.iter().zip(receipts) {
        let reference = batch
            .record_reference(receipt.appended().address(), 0)
            .unwrap();
        assert_eq!(owner_read(&fixture, &reference), batch.mutations());
    }
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn leased_lower_through_retry_uses_retired_proof() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    write(&mut client, &fixture.batch(1));
    client.settle_leased_extent(&mut Vec::new(), None).unwrap();
    assert!(
        client
            .live_leased
            .current
            .as_mut()
            .unwrap()
            .prefixes
            .is_empty()
    );
    let token = client
        .live_leased
        .current
        .as_ref()
        .unwrap()
        .lease
        .extent
        .token;
    let reply = client
        .adopt_live_extent(1, token, 4096, false, false)
        .unwrap();
    assert_eq!(
        (reply.adoption.adopted_end, reply.adoption.receipt_count),
        (8192, 2)
    );
    client
        .check_leased_adoption(
            PendingAdoption {
                target: PendingTarget::Current,
                through: 4096,
                close: false,
                lease_next: false,
            },
            &reply,
        )
        .unwrap();
    assert!(
        client
            .live_leased
            .current
            .as_mut()
            .unwrap()
            .prefixes
            .is_empty()
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn lease_ahead_switch_sends_no_synchronous_owner_request() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    write(&mut client, &fixture.batch(0));
    client.wait_closing_extent(true).unwrap();
    let before = fixture.metrics.snapshot().counts;
    let receipts = client
        .submit_and_drain_leased_group(
            1,
            vec![
                fixture.big_batch(1),
                fixture.big_batch(2),
                fixture.big_batch(3),
            ],
            &WriteTimeline::default(),
            2,
        )
        .unwrap();
    let after = fixture.metrics.snapshot().counts;
    assert_eq!(
        after.adoption_sync_exchanges,
        before.adoption_sync_exchanges
    );
    assert_eq!(after.extent_switch_waits, 0);
    assert!(client.live_leased.closing.is_some());
    assert_eq!(
        client.live_leased.current.as_ref().unwrap().adopted.count,
        0
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    for (sequence, receipt) in (1..=3).zip(receipts) {
        let batch = fixture.big_batch(sequence);
        let reference = batch
            .record_reference(receipt.appended().address(), 0)
            .unwrap();
        assert_eq!(owner_read(&fixture, &reference), batch.mutations());
    }
}

#[test]
fn lease_ahead_switch_waits_when_prelease_is_late() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    write(&mut client, &fixture.batch(0));
    assert!(client.live_leased.next.is_none());
    client
        .submit_and_drain_leased_group(
            1,
            vec![
                fixture.big_batch(1),
                fixture.big_batch(2),
                fixture.big_batch(3),
            ],
            &WriteTimeline::default(),
            2,
        )
        .unwrap();
    assert_eq!(fixture.metrics.snapshot().counts.extent_switch_waits, 1);
    assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    assert_eq!(fixture.node.log_for_test(1).receipt_count(), 4);
}

#[test]
fn lease_ahead_close_releases_unused_preleased_extent() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    write(&mut client, &fixture.batch(0));
    client.wait_closing_extent(true).unwrap();
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    assert!(client.live_leased.current.is_none());
    assert!(client.live_leased.closing.is_none());
    assert!(client.live_leased.next.is_none());
    assert!(
        client
            .live_leased
            .prefix_buffers
            .iter()
            .all(|buffer| buffer.capacity() == 1024)
    );
    let next = client.lease_live_extent(1).unwrap();
    assert_eq!(next.extent.start, 32768);
    client.install_leased_extent(next).unwrap();
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn lease_ahead_off_keeps_the_combined_close_and_lease() {
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    client
        .submit_and_drain_leased_group(
            1,
            vec![
                fixture.big_batch(1),
                fixture.big_batch(2),
                fixture.big_batch(3),
            ],
            &WriteTimeline::default(),
            2,
        )
        .unwrap();
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.lease_exchanges, 1);
    assert_eq!(counts.adoption_sync_exchanges, 1);
    assert_eq!(counts.extents_leased, 2);
    assert_eq!(counts.extent_preleases, 0);
    assert_eq!(counts.extent_switch_waits, 0);
    assert!(client.live_leased.closing.is_none());
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn leased_rotation_exports_owner_profiles_and_timeline_stages() {
    for fail in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Native, 16384);
        let mut client = fixture.writer_without_ahead();
        write(&mut client, &fixture.batch(0));
        if fail {
            client.leased_test_fail_write(-libc::EIO);
        }
        let timeline = WriteTimeline::default();
        timeline.arm(3, 1).unwrap();
        let batches: Vec<_> = (1..=3).map(|n| fixture.big_batch(n)).collect();
        let samples: Vec<_> = batches
            .iter()
            .enumerate()
            .map(|(index, batch)| {
                let sample = timeline.begin(index as u64).unwrap();
                timeline.operation(sample, batch.id().operation);
                timeline.batch(batch.id().operation, batch.version());
                sample
            })
            .collect();
        let result = client.submit_and_drain_leased_group(1, batches, &timeline, 2);
        assert_eq!(result.is_err(), fail);
        for sample in samples {
            timeline.complete(sample, None);
        }
        let export = serde_json::to_value(timeline.finish().unwrap()).unwrap();
        assert_eq!(export["dropped_events"], 0);
        for row in export["records"].as_array().unwrap() {
            let stages: Vec<_> = row["events"]
                .as_array()
                .unwrap()
                .iter()
                .map(|event| event["stage"].as_str().unwrap())
                .collect();
            assert!(stages.contains(&"live_extent_switch_started"));
            assert_eq!(stages.contains(&"live_extent_switch_completed"), !fail);
            if !fail {
                let start = stages
                    .iter()
                    .position(|s| *s == "live_extent_switch_started")
                    .unwrap();
                let end = stages
                    .iter()
                    .position(|s| *s == "live_extent_switch_completed")
                    .unwrap();
                assert!(start < end);
                let io = stages.iter().position(|s| *s == "live_io_started").unwrap();
                assert_eq!(
                    &stages[io..io + 4],
                    &[
                        "live_io_started",
                        "live_frame_encoded",
                        "live_io_submitted",
                        "live_io_completed",
                    ]
                );
            }
        }
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.lease_profile_samples, if fail { 1 } else { 2 });
        assert_eq!(counts.adopt_profile_samples, if fail { 0 } else { 1 });
        assert_eq!(counts.extent_preleases, 0);
        assert_eq!(counts.extent_switch_waits, 0);
        if fail {
            client
                .take_failed_live_writer()
                .unwrap()
                .drain_failed()
                .unwrap();
        } else {
            assert!(counts.adopt_owner_ns > 0);
            assert_eq!(counts.adopt_close_scan_bytes, 12288);
            client.close_leased_extent(&mut Vec::new(), None).unwrap();
        }
    }
}

#[test]
fn leased_switch_harvests_delayed_adoption_before_replacing_extent() {
    use crate::distributed::storage::LivePausePoint;
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    write(&mut client, &fixture.batch(0));
    let (entered, release) = fixture
        .node
        .log_for_test(1)
        .pause_live(LivePausePoint::FrameProbe);
    client.submit_leased_adoption().unwrap();
    // Shared cap for the pause-entry and skip-marker progress loops below.
    let deadline = Instant::now() + PROGRESS_CAP;
    loop {
        client.progress_session().unwrap();
        if entered.try_recv().is_ok() {
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    let group = vec![
        fixture.big_batch(1),
        fixture.big_batch(2),
        fixture.big_batch(3),
    ];
    let (mut client, result) = thread::scope(|scope| {
        let (finished, done) = std::sync::mpsc::sync_channel(1);
        let work = scope.spawn(move || {
            let result =
                client.submit_and_drain_leased_group(1, group, &WriteTimeline::default(), 2);
            let _ = finished.send(());
            (client, result)
        });
        while fixture.metrics.snapshot().counts.leased_skip_markers == 0 {
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(matches!(
            done.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert_eq!(fixture.metrics.snapshot().counts.leased_frames, 1);
        release.send(()).unwrap();
        work.join().unwrap()
    });
    result.unwrap();
    assert!(!client.leased_adoption_pending());
    assert_eq!(
        client
            .live_leased
            .current
            .as_ref()
            .unwrap()
            .lease
            .extent
            .start,
        16384
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn control_timeline_captures_leased_requests_and_owner_position_phases() {
    let fixture = Fixture::new(DurabilityTier::Native, 128 * 1024);
    let mut client = fixture.writer();
    let timeline = Arc::new(WriteTimeline::default());
    client.trace_controls(timeline.clone(), 2);
    client.owner_timeline_arm(32).unwrap();
    timeline.arm(32, 1).unwrap();
    let first = client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &timeline, 2)
        .unwrap()[0];
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    client.submit_leased_adoption().unwrap();
    client.wait_leased_control(&mut Vec::new(), None).unwrap();
    let through = first.appended().address().offset + u64::from(first.appended().address().len);
    client.checkpoint_position(1, 1, through).unwrap();
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    let (records, dropped, incomplete, error) = client.owner_timeline_fetch_partial();
    assert!(error.is_none());
    assert_eq!((dropped, incomplete), (0, 0));
    let export = timeline.finish().unwrap();
    for kind in [
        "lease",
        "prelease",
        "adoption",
        "closing_adoption",
        "position",
    ] {
        let submitted = export
            .control_events
            .iter()
            .find(|event| event.target.kind == kind && event.stage == "submit")
            .unwrap();
        let completed = export
            .control_events
            .iter()
            .find(|event| event.envelope_id == submitted.envelope_id && event.stage == "complete")
            .unwrap();
        assert_eq!(completed.success, Some(true));
        assert!(completed.elapsed_ns >= submitted.elapsed_ns);
        let owner = records
            .iter()
            .find(|record| record.envelope_id == submitted.envelope_id)
            .unwrap();
        assert!(owner.control_queue_ns.is_some());
        assert!(owner.handler_ns.is_some());
        if kind == "position" {
            assert_eq!(submitted.target.through, Some(through));
            assert!(owner.wal_file_persist_ns.is_some());
            assert!(owner.namespace_persist_ns.is_some());
            assert!(owner.append_persist_ns.is_some());
        }
    }
}
#[test]
fn leased_eventfd_registration_failure_keeps_bounded_io_wait() {
    let fixture = Fixture::new(DurabilityTier::Native, 32768);
    let mut client = fixture.writer_without_ahead();
    client.leased_test_hold_completions(&[]);
    client.set_worker_wake(-1);
    assert!(!client.live_leased.worker_wake_registered);
    assert_eq!(
        fixture.metrics.snapshot().counts.leased_eventfd_unavailable,
        1
    );
    let timeline = WriteTimeline::default();
    assert!(matches!(
        client
            .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, true)
            .unwrap(),
        LeasedSubmit::Submitted(1)
    ));
    let mut released = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while released.is_empty() {
        assert!(Instant::now() < deadline);
        client.wait_leased_flights(true).unwrap();
        client.progress_leased_flights(&mut released, None).unwrap();
    }
    assert_eq!(released[0].0, 1);
    client.clear_worker_wake();
}

fn progress_until(
    client: &mut Client,
    released: &mut Vec<(u64, Vec<tcp::DurableBatch>)>,
    timeline: &WriteTimeline,
    ready: impl Fn(&Client) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready(client) {
        assert!(
            Instant::now() < deadline,
            "leased test progress deadline expired"
        );
        client.wait_leased_flights(false).unwrap();
        client
            .progress_leased_flights(released, Some(timeline))
            .unwrap();
    }
}

#[test]
fn leased_window_deferred_attempts_do_not_consume_timeline_groups() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 32768);
        let mut client = fixture.writer_without_ahead();
        client.leased_test_set_window(1);
        let timeline = WriteTimeline::default();
        timeline.arm(1, 1).unwrap();
        assert!(matches!(
            client
                .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, true)
                .unwrap(),
            LeasedSubmit::Submitted(1)
        ));
        let batch = fixture.batch(1);
        let call = timeline.begin(1).unwrap();
        timeline.operation(call, batch.id().operation);
        timeline.batch(batch.id().operation, batch.version());
        let mut batches = vec![batch];
        for _ in 0..3 {
            let LeasedSubmit::Deferred(returned) = client
                .submit_leased_group(1, batches, &timeline, 2, true)
                .unwrap()
            else {
                panic!("full window admitted another frame");
            };
            batches = returned;
        }
        client
            .drain_leased_flights(&mut Vec::new(), Some(&timeline))
            .unwrap();
        client
            .submit_and_drain_leased_group(1, batches, &timeline, 2)
            .unwrap();
        timeline.complete(call, None);
        let export = timeline.finish().unwrap();
        assert_eq!(export.live_groups.len(), 1);
        assert_eq!(export.dropped_live_groups, 0);
        assert_eq!(export.live_groups[0].wal_position, Some(4096));
        assert!(
            export.records[0]
                .events
                .windows(2)
                .all(|events| events[0].elapsed_ns <= events[1].elapsed_ns)
        );
    }
}

#[test]
fn leased_window_releases_receipts_in_frame_order_when_completions_reorder() {
    for (tier, sync) in [
        (DurabilityTier::Native, LeasedSync::PerFrameFua),
        (DurabilityTier::Synchronous, LeasedSync::PerFrameFua),
        (DurabilityTier::Synchronous, LeasedSync::GroupFlush),
    ] {
        let fixture = Fixture::new(tier, 32768);
        let mut client = fixture.writer_without_ahead();
        client.live_config.as_mut().unwrap().leased_sync = sync;
        client.leased_test_set_window(4);
        client.leased_test_hold_completions(&[1]);
        let timeline = WriteTimeline::default();
        timeline.arm(4, 1).unwrap();
        let mut calls = Vec::new();
        for sequence in 0..4 {
            let batch = fixture.batch(sequence);
            let call = timeline.begin(sequence).unwrap();
            timeline.operation(call, batch.id().operation);
            timeline.batch(batch.id().operation, batch.version());
            calls.push(call);
            assert!(
                matches!(client.submit_leased_group(1, vec![batch], &timeline, 2, true).unwrap(),
                LeasedSubmit::Submitted(id) if id == sequence + 1)
            );
        }
        let mut released = Vec::new();
        progress_until(&mut client, &mut released, &timeline, |client| {
            client
                .live_leased
                .flights
                .iter()
                .skip(1)
                .all(|flight| flight.completed)
        });
        assert!(released.is_empty());
        let current = client.live_leased.current.as_ref().unwrap();
        assert_eq!((current.cursor, current.written_end), (16384, 0));
        assert!(
            matches!(client.submit_leased_group(1, vec![fixture.batch(4)], &timeline, 2, true).unwrap(),
            LeasedSubmit::Deferred(batches) if batches.len() == 1)
        );
        if sync == LeasedSync::GroupFlush {
            client
                .live_writer
                .as_mut()
                .unwrap()
                .io
                .hold_completions(&[5]);
            progress_until(&mut client, &mut released, &timeline, |client| {
                client.live_leased.flush_in_flight.is_some()
            });
            assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 4);
            assert!(released.len() <= 1); // The first frame alone may use FUA.
            assert!(
                client
                    .live_leased
                    .flights
                    .iter()
                    .all(|flight| flight.completed)
            );
        }
        client.leased_test_release_held();
        client
            .drain_leased_flights(&mut released, Some(&timeline))
            .unwrap();
        assert_eq!(
            released.iter().map(|entry| entry.0).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            client.live_leased.current.as_ref().unwrap().written_end,
            16384
        );
        client
            .settle_leased_extent(&mut Vec::new(), Some(&timeline))
            .unwrap();
        let owner = fixture.node.log_for_test(1);
        assert_eq!(owner.receipt_count(), 4);
        for (sequence, (_, receipts)) in released.iter().enumerate() {
            let batch = fixture.batch(sequence as u64);
            let reference = batch
                .record_reference(receipts[0].appended().address(), 0)
                .unwrap();
            assert_eq!(owner_read(&fixture, &reference), batch.mutations());
        }
        for call in calls {
            timeline.complete(call, None);
        }
        let export = timeline.finish().unwrap();
        for (sequence, group) in export.live_groups.iter().enumerate() {
            assert_eq!(group.wal_position, Some(sequence as u64 * 4096));
            assert_eq!(group.file_offset, group.wal_position);
            assert_eq!(group.frame_len, Some(4096));
            assert_eq!(group.wal_file, Some(0));
            assert_eq!(group.namespace_block_size, Some(4096));
        }
    }
}

fn group_flush_writer(fixture: &Fixture) -> Client {
    let mut client = fixture.writer_without_ahead();
    client.live_config.as_mut().unwrap().leased_sync = LeasedSync::GroupFlush;
    client
}

#[test]
fn group_flush_defaults_use_fua_for_small_frames_and_flush_large_frames() {
    let timeline = WriteTimeline::default();
    for value_bytes in [1024, 65536] {
        let temp = tempdir::TempDir::new("leased-default-sync").unwrap();
        let mut config = tcp::tests::config(&temp.path().join("node"));
        config.partition.fragment_bytes = 256 * 1024;
        config.partition.wal_file_bytes = 512 * 1024;
        config.partition.batch_limits.max_encoded_bytes = 64 * 1024 + 1024;
        config.partition.group_limits.max_encoded_bytes = 128 * 1024;
        let fixture = Fixture::open(
            DurabilityTier::Synchronous,
            config,
            temp,
            Duration::from_secs(10),
        );
        let mut client = fixture.writer_without_ahead();
        client.live_config.as_mut().unwrap().leased_sync = LeasedSync::default();
        client
            .live_config
            .as_mut()
            .unwrap()
            .leased_flush_min_frame_bytes =
            LiveClientConfig::default_leased_flush_min_frame_bytes();
        let original = fixture.batch(0);
        let batch = tcp::CompleteBatch::new(
            &crate::key_shape::KeyShape::from_yaml(&fixture.config.shape_yaml).unwrap(),
            original.id(),
            original.version(),
            vec![crate::distributed::codec::Mutation::Put {
                keyspace: crate::key_shape::KeySpace(0),
                key: b"key1"[..].into(),
                value: vec![1; value_bytes].into(),
            }],
            fixture.config.partition.batch_limits,
        )
        .unwrap();
        assert!(matches!(
            client
                .submit_leased_group(1, vec![batch], &timeline, 2, false)
                .unwrap(),
            LeasedSubmit::Submitted(_)
        ));
        assert_eq!(
            client.live_leased.flights[0].durable_at_completion,
            value_bytes < 65536
        );
        let mut released = Vec::new();
        client.drain_leased_flights(&mut released, None).unwrap();
        assert_eq!(released.len(), 1);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.fua_writes, u64::from(value_bytes < 65536));
        assert_eq!(counts.leased_flushes, u64::from(value_bytes == 65536));
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
    }
}

#[test]
fn group_flush_polled_writes_use_fua_with_queued_work() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
    let mut client = group_flush_writer(&fixture);
    let config = client.live_config.as_mut().unwrap();
    config.polled_writes = true;
    config.queue_geometry.poll_queues = 1;
    config.leased_flush_min_frame_bytes = 0;
    config.validate().unwrap();
    // The local fixture's interrupt ring makes policy coverage independent
    // of IOPOLL kernel support; the real-polled test covers the kernel route.
    client.leased_test_hold_completions(&[1, 2]);
    let timeline = WriteTimeline::default();
    for sequence in 0..2 {
        client
            .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false)
            .unwrap();
    }
    assert!(
        client
            .live_leased
            .flights
            .iter()
            .all(|flight| flight.durable_at_completion)
    );
    client.leased_test_release_held();
    let mut released = Vec::new();
    client.drain_leased_flights(&mut released, None).unwrap();
    assert_eq!(released.len(), 2);
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.fua_writes, 2);
    assert_eq!(counts.leased_flushes, 0);
}

#[test]
fn group_flush_lone_frame_uses_fua_without_namespace_flush() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
    let mut client = group_flush_writer(&fixture);
    let timeline = WriteTimeline::default();
    client
        .submit_and_drain_leased_group(1, vec![fixture.batch(0)], &timeline, 2)
        .unwrap();
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.fua_writes, 1);
    assert_eq!(counts.leased_flushes, 0);
    assert_eq!(counts.leased_frames, 1);
}

/// Synchronous leased writes: the acknowledgement of a per-frame FUA write
/// is released only after that write's completion, and never after a
/// failed one. The covering-flush form of the same property is
/// `group_flush_threshold_selects_fua_or_covering_flush_with_queued_work`
/// and `group_flush_failure_and_timeout_poison_all_unreleased_flights`.
/// (This replaced the owner-sync check of the removed Reserved route in
/// `higher_live_fence_joins_only_the_old_syncer_and_keeps_noop_fences_safe`.)
#[test]
fn synchronous_leased_ack_follows_its_fua_completion_and_never_a_failed_one() {
    for failed in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = fixture.writer_without_ahead();
        assert_eq!(
            client.live_config.as_ref().unwrap().leased_sync,
            LeasedSync::PerFrameFua
        );
        let timeline = WriteTimeline::default();
        client.leased_test_hold_completions(&[1]);
        if failed {
            client
                .live_writer
                .as_mut()
                .unwrap()
                .io
                .override_completion_result(1, -libc::EIO);
        }
        assert!(matches!(
            client
                .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, false)
                .unwrap(),
            LeasedSubmit::Submitted(1)
        ));
        assert!(client.live_leased.flights[0].durable_at_completion);
        // The write SQE itself carried the data-sync (FUA) flag; the
        // `fua_writes` metric only records the chosen policy.
        assert_eq!(
            client
                .live_writer
                .as_ref()
                .unwrap()
                .io
                .stats()
                .data_sync_writes,
            1,
            "the Synchronous leased write was not submitted with FUA"
        );
        let mut released = Vec::new();
        // Bounded: the FUA write's CQE is reaped but held, and no
        // acknowledgement may be released while it is held.
        let deadline = Instant::now() + Duration::from_secs(5);
        while client.live_writer.as_ref().unwrap().io.outstanding() != 0 {
            assert!(
                Instant::now() < deadline,
                "held FUA write CQE was not reaped within 5 s"
            );
            client.wait_leased_flights(false).unwrap();
            client
                .progress_leased_flights(&mut released, Some(&timeline))
                .unwrap();
        }
        assert!(
            released.is_empty(),
            "a Synchronous acknowledgement was released while its FUA completion was held"
        );
        assert!(!client.live_leased.flights[0].completed);
        client.leased_test_release_held();
        if failed {
            assert!(client.drain_leased_flights(&mut released, None).is_err());
            assert!(
                released.is_empty(),
                "an acknowledgement was released for a failed FUA write"
            );
            assert!(client.live_write_failed);
        } else {
            // Bounded: once the FUA completion is delivered, the
            // acknowledgement must follow without a covering flush.
            let deadline = Instant::now() + Duration::from_secs(5);
            while released.is_empty() {
                assert!(
                    Instant::now() < deadline,
                    "no acknowledgement within 5 s of the delivered FUA completion"
                );
                client.wait_leased_flights(false).unwrap();
                client
                    .progress_leased_flights(&mut released, Some(&timeline))
                    .unwrap();
            }
            assert_eq!(released.len(), 1);
            let counts = fixture.metrics.snapshot().counts;
            assert_eq!(counts.fua_writes, 1);
            assert_eq!(counts.leased_flushes, 0);
        }
    }
}

#[test]
fn group_flush_window_of_one_uses_fua_with_queued_work() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
    let mut client = group_flush_writer(&fixture);
    client.live_config.as_mut().unwrap().leased_write_window = Some(1);
    let timeline = WriteTimeline::default();
    assert!(matches!(
        client
            .submit_leased_group(1, vec![fixture.batch(0)], &timeline, 2, false)
            .unwrap(),
        LeasedSubmit::Submitted(1)
    ));
    let mut released = Vec::new();
    client
        .drain_leased_flights(&mut released, Some(&timeline))
        .unwrap();
    assert_eq!(released.len(), 1);
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.fua_writes, 1);
    assert_eq!(counts.leased_flushes, 0);
}

#[test]
fn group_flush_threshold_selects_fua_or_covering_flush_with_queued_work() {
    for (threshold, big, fua) in [
        (4097, false, true),
        (4096, false, false),
        (4095, false, false),
        (0, false, false),
        (8193, true, true),
        (8192, true, false),
    ] {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        client
            .live_config
            .as_mut()
            .unwrap()
            .leased_flush_min_frame_bytes = threshold;
        let timeline = WriteTimeline::default();
        client.leased_test_hold_completions(&[1, 2, 3]);
        for sequence in 0..2 {
            let batch = if big {
                fixture.big_batch(sequence)
            } else {
                fixture.batch(sequence)
            };
            assert!(matches!(
                client.submit_leased_group(1, vec![batch], &timeline, 2, false).unwrap(),
                LeasedSubmit::Submitted(id) if id == sequence + 1
            ));
        }
        assert_eq!(client.live_leased.flights.len(), 2);
        for flight in &client.live_leased.flights {
            assert_eq!(flight.frame_len, if big { 8192 } else { 4096 });
            assert_eq!(flight.durable_at_completion, fua);
        }
        let mut released = Vec::new();
        progress_until(&mut client, &mut released, &timeline, |client| {
            client.live_writer.as_ref().unwrap().io.outstanding() == 0
        });
        client
            .live_writer
            .as_mut()
            .unwrap()
            .io
            .hold_completions(&[3]);
        progress_until(&mut client, &mut released, &timeline, |client| {
            client
                .live_leased
                .flights
                .iter()
                .all(|flight| flight.completed)
        });
        if fua {
            assert_eq!(released.len(), 2);
            assert!(client.live_leased.flush_in_flight.is_none());
        } else {
            assert!(released.is_empty());
            assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 2);
        }
        client.leased_test_release_held();
        client.drain_leased_flights(&mut released, None).unwrap();
        assert_eq!(released.len(), 2);
        let counts = fixture.metrics.snapshot().counts;
        assert_eq!(counts.fua_writes, if fua { 2 } else { 0 });
        assert_eq!(counts.leased_flushes, if fua { 0 } else { 1 });
        assert_eq!(counts.leased_flush_covered_frames, if fua { 0 } else { 2 });
    }
}

#[test]
fn group_flush_captures_only_the_completed_prefix_and_drain_covers_later_writes() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
    let mut client = group_flush_writer(&fixture);
    let timeline = WriteTimeline::default();
    client.leased_test_hold_completions(&[2, 3, 4, 5]);
    for sequence in 0..3 {
        assert!(
            matches!(client.submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false).unwrap(),
            LeasedSubmit::Submitted(id) if id == sequence + 1)
        );
    }
    let mut released = Vec::new();
    progress_until(&mut client, &mut released, &timeline, |client| {
        client.live_leased.flush_in_flight.is_some()
    });
    assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 1);
    assert!(released.is_empty());
    client
        .live_writer
        .as_mut()
        .unwrap()
        .io
        .hold_completions(&[3, 4, 5]);
    progress_until(&mut client, &mut released, &timeline, |client| {
        client.live_leased.flights[1].completed
    });
    assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 1);
    assert!(released.is_empty());
    client
        .live_writer
        .as_mut()
        .unwrap()
        .io
        .hold_completions(&[3, 5]);
    progress_until(&mut client, &mut released, &timeline, |client| {
        client.live_leased.flushed_through_seq == 1
    });
    assert_eq!(
        released.iter().map(|entry| entry.0).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(client.live_leased.flush_in_flight.unwrap().1, 2);
    client.leased_test_release_held();
    client.drain_leased_flights(&mut released, None).unwrap();
    assert_eq!(
        released.iter().map(|entry| entry.0).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(client.live_leased.flushed_through_seq >= 3);
    assert!(client.live_leased.flush_in_flight.is_none());
    assert_eq!(client.live_writer.as_ref().unwrap().io.outstanding(), 0);
    let counts = fixture.metrics.snapshot().counts;
    assert_eq!(counts.leased_flushes, 3);
    assert_eq!(counts.leased_flush_samples, 3);
    assert_eq!(counts.leased_flush_covered_frames, 3);
    assert_eq!(counts.fua_writes, 0);
}

#[test]
fn group_flush_failure_and_timeout_poison_all_unreleased_flights() {
    for timed_out in [false, true] {
        let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
        let mut client = group_flush_writer(&fixture);
        let timeline = WriteTimeline::default();
        client.leased_test_hold_completions(&[3]);
        if !timed_out {
            client
                .live_writer
                .as_mut()
                .unwrap()
                .io
                .override_completion_result(3, -libc::EIO);
        }
        for sequence in 0..2 {
            client
                .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false)
                .unwrap();
        }
        let mut released = Vec::new();
        progress_until(&mut client, &mut released, &timeline, |client| {
            client.live_leased.flush_in_flight.is_some()
        });
        assert!(released.is_empty());
        if timed_out {
            client.live_leased.flush_in_flight.as_mut().unwrap().2 = Instant::now();
        } else {
            client.leased_test_release_held();
        }
        assert!(client.drain_leased_flights(&mut released, None).is_err());
        assert!(released.is_empty());
        assert!(client.live_write_failed);
        assert_eq!(client.live_leased.flights.len(), 2);
        assert_eq!(client.live_leased.current.as_ref().unwrap().written_end, 0);
        assert!(
            client
                .submit_leased_group(1, vec![fixture.batch(2)], &timeline, 2, true)
                .is_err()
        );
        assert!(fixture.metrics.snapshot().counts.unknown_writes > 0);
    }
}

#[test]
fn group_flush_sequence_stays_monotone_across_queue_rebind() {
    let fixture = Fixture::new(DurabilityTier::Synchronous, 32768);
    let mut client = group_flush_writer(&fixture);
    let timeline = WriteTimeline::default();
    let mut released = Vec::new();
    for sequence in 0..4 {
        let LeasedSubmit::Submitted(id) = client
            .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, false)
            .unwrap()
        else {
            panic!("unexpected deferral");
        };
        assert!(id > client.live_leased.flushed_through_seq);
        client.drain_leased_flights(&mut released, None).unwrap();
        assert!(client.live_leased.flushed_through_seq >= id);
        // Force a new queue after each durable cut, like a file rebind.
        let writer = client.live_writer.take().unwrap();
        assert_eq!(writer.io.outstanding(), 0);
        drop(writer);
    }
    assert_eq!(released.len(), 4);
    assert_eq!(fixture.metrics.snapshot().counts.leased_flushes, 4);
    client.close_leased_extent(&mut released, None).unwrap();
    assert_eq!(fixture.node.log_for_test(1).receipt_count(), 4);
}

#[test]
fn leased_window_crash_recovers_only_contiguous_prefix() {
    use std::os::unix::fs::FileExt;
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 32768);
        let mut client = fixture.writer_without_ahead();
        let timeline = WriteTimeline::default();
        client.leased_test_hold_completions(&[2]);
        client.leased_test_fail_flight(2, -libc::EIO);
        for sequence in 0..4 {
            assert!(matches!(
                client
                    .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, true)
                    .unwrap(),
                LeasedSubmit::Submitted(_)
            ));
        }
        let mut released = Vec::new();
        progress_until(&mut client, &mut released, &timeline, |client| {
            client
                .live_leased
                .flights
                .iter()
                .skip(1)
                .all(|flight| flight.completed)
                && client.live_writer.as_ref().unwrap().io.outstanding() == 0
        });
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].0, 1);
        let first = released[0].1[0];
        let current = client.live_leased.current.as_ref().unwrap();
        assert_eq!(current.written_end, 4096);
        // The injected failed CQE does not alter real DMA. Tear the retired frame's CRC.
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&current.lease.extent.segment.path)
            .unwrap();
        file.write_all_at(&[0xff; 4], 4100).unwrap();
        file.sync_data().unwrap();
        drop(file);
        drop(client); // No adoption, close, or receipt release for frames 2..4.
        let mut recovery = fixture.client();
        recovery.fence_to(2).unwrap();
        let log = fixture
            .node
            .inventory
            .lock()
            .live_recovery_log(2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(log.receipt_count(), 1);
        let batch = fixture.batch(0);
        let reference = batch
            .record_reference(first.appended().address(), 0)
            .unwrap();
        assert_eq!(log.read_record(&reference).unwrap(), batch.mutations()[0]);
    }
}

#[test]
fn leased_window_extent_switch_drains_before_skip_marker() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier, 6 * 4096);
        let mut client = fixture.writer_without_ahead();
        let timeline = WriteTimeline::default();
        let mut released = Vec::new();
        client.leased_test_hold_completions(&[1]);
        for sequence in 0..4 {
            assert!(matches!(
                client
                    .submit_leased_group(1, vec![fixture.batch(sequence)], &timeline, 2, true)
                    .unwrap(),
                LeasedSubmit::Submitted(_)
            ));
        }
        progress_until(&mut client, &mut released, &timeline, |client| {
            client
                .live_leased
                .flights
                .iter()
                .skip(1)
                .all(|flight| flight.completed)
        });
        assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
        client.leased_test_release_held();
        client.drain_leased_flights(&mut released, None).unwrap();
        // A two-block frame leaves one block. The next two-block frame needs a skip.
        for sequence in 4..7 {
            let batch = if sequence < 5 {
                fixture.batch(sequence)
            } else {
                fixture.big_batch(sequence)
            };
            loop {
                match client
                    .submit_leased_group(1, vec![batch.clone()], &timeline, 2, true)
                    .unwrap()
                {
                    LeasedSubmit::Submitted(_) => break,
                    LeasedSubmit::Deferred(_) => {
                        if sequence == 5 {
                            assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
                        }
                        client.drain_leased_flights(&mut released, None).unwrap();
                    }
                }
            }
        }
        client.drain_leased_flights(&mut released, None).unwrap();
        for sequence in 7..10 {
            let batch = fixture.big_batch(sequence);
            loop {
                match client
                    .submit_leased_group(1, vec![batch.clone()], &timeline, 2, true)
                    .unwrap()
                {
                    LeasedSubmit::Submitted(_) => break,
                    LeasedSubmit::Deferred(_) => {
                        client.drain_leased_flights(&mut released, None).unwrap()
                    }
                }
            }
        }
        client.close_leased_extent(&mut released, None).unwrap();
        assert_eq!(released.len(), 10);
        let log = fixture.node.log_for_test(1);
        assert_eq!(log.receipt_count(), 10);
        assert!(fixture.metrics.snapshot().counts.leased_skip_markers > 0);
        for (sequence, (_, receipts)) in released.into_iter().enumerate() {
            let batch = if sequence < 5 {
                fixture.batch(sequence as u64)
            } else {
                fixture.big_batch(sequence as u64)
            };
            let reference = batch
                .record_reference(receipts[0].appended().address(), 0)
                .unwrap();
            assert_eq!(owner_read(&fixture, &reference), batch.mutations());
        }
    }
}
