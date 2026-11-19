//! R0 observer on the leased ordinary-file fixture: several real file rolls,
//! one real prune (checkpoint, floor advance), and zero Value WAL unlinks.
//! Ordinary files never qualify the Linux target; this checks accounting only.
use super::pipeline_tests::{open_live_fixture_host, trusted_owner_host_config};
use super::*;
use crate::distributed::capacity::{
    Blocker, CapacityObserverConfig, R0Sample, R0Sink, SampleEvent,
};
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
use crate::distributed::tcp::{LeasedSync, LiveClientConfig, Transport};
use crate::key_shape::KeyType;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};

const MIB: u64 = 1024 * 1024;

struct Collect(Arc<Mutex<Vec<R0Sample>>>);

impl R0Sink for Collect {
    fn sample(&mut self, sample: &R0Sample) -> std::io::Result<()> {
        self.0.lock().push(sample.clone());
        Ok(())
    }
}

struct Fixture {
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    node_root: PathBuf,
    live: Arc<LiveNvmeMetrics>,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
    // Dropped last: owner WAL files live here.
    root: tempdir::TempDir,
}

impl Fixture {
    fn new(tier: DurabilityTier) -> Self {
        let root = tempdir::TempDir::new("r0-capacity-observer").unwrap();
        let shape = KeyShape::new_single(32, 1, KeyType::uniform(1));
        let tls = TlsFiles {
            transport: Transport::TcpTrusted,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".into(),
        };
        let mut config = RemoteDatabaseConfig {
            storage: LocalInventoryConfig {
                database: [0; 16],
                partitions: vec![0],
                batch_limits: BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 128 * 1024,
                },
                group_limits: GroupLimits {
                    max_batches: 32,
                    max_encoded_bytes: 2 * MIB as usize,
                },
                // Two fragments per file: every second extent rolls a file.
                fragment_bytes: 4 * MIB,
                wal_file_bytes: 8 * MIB,
                max_batches_per_partition: 8192,
                max_reference_metadata_bytes: 64 * MIB as usize,
            },
            endpoints: Vec::new(),
            tls: tls.clone(),
            timeout: Duration::from_secs(60),
            max_admitted_batches: 8192,
            max_index_bytes: 64 * MIB as usize,
            index_shards: 64,
            max_pending_batches: 64,
            max_pending_bytes: 32 * MIB as usize,
            replay_limits: ReplayLimits {
                max_batches: 65536,
                max_encoded_bytes: 1024 * MIB as usize,
            },
            owner_append_occupancy: 16,
            pipelined_writes: true,
            leased_submit_redrain: true,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: 128 * MIB as usize,
            scan_prefetch_pages: 1,
            read_lanes: Some(1),
            pin_read_lanes: false,
            max_pending_reads: 64,
            max_pending_read_bytes: 16 * MIB as usize,
            write_profile_limit: 0,
            live_nvme: LiveClientConfig {
                lease_ahead: true,
                polled_writes: false,
                host_nqn: "nqn.2026-01.test:r0-capacity".into(),
                host_id: [2; 16],
                owner_identities: Default::default(),
                leased_write_window: Some(4),
                leased_sync: LeasedSync::PerFrameFua,
                leased_flush_min_frame_bytes: 0,
                max_inflight_requests: 32,
                max_inflight_bytes: 4 * MIB as usize,
                connect_timeout_ms: 10000,
                io_timeout_ms: 30000,
                leased_durability: tier,
                inline_reads: true,
                queue_geometry: Default::default(),
            },
            durable_cut: Default::default(),
            relocation_max_reclaim_pct: 100,
            compactor_cadence: Default::default(),
        };
        let host =
            open_live_fixture_host(trusted_owner_host_config(root.path().join("owner"))).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        config.endpoints.push(RemoteEndpoint {
            partition: 0,
            address: listener.local_addr().unwrap().to_string(),
            server_name: tls.server_name.clone(),
        });
        let stop = Arc::new(AtomicBool::new(false));
        // Join the owner before removing the fixture directory.
        let serving = Arc::clone(&host);
        let stopping = stop.clone();
        let server = thread::spawn(move || {
            serving
                .serve(
                    listener,
                    tls,
                    super::super::tcp::TcpLimits::default(),
                    stopping,
                )
                .unwrap()
        });
        authority_lifecycle::provision_for_test(
            &root.path().join("authority"),
            &shape,
            &mut config,
        )
        .unwrap();
        let node_root = host
            .database(&config.storage.database)
            .unwrap()
            .config_for_test()
            .root;
        Self {
            config,
            shape,
            node_root,
            live: LiveNvmeMetrics::new(),
            stop,
            server: Some(server),
            root,
        }
    }

    fn open(&self) -> ConcurrentRemoteDatabase {
        let db = RemoteDatabase::open_with_local_live_fixture(
            &self.root.path().join("authority"),
            &self.shape,
            self.config.clone(),
            true,
        )
        .unwrap();
        for worker in &db.core.clients {
            worker
                .client
                .lock()
                .set_live_metrics(self.live.clone())
                .unwrap();
        }
        ConcurrentRemoteDatabase::new(db).unwrap()
    }

    fn wal_files(&self) -> usize {
        fs::read_dir(&self.node_root)
            .unwrap()
            .flatten()
            .flat_map(|epoch| fs::read_dir(epoch.path()).into_iter().flatten().flatten())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("wal_"))
            .count()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Release pairs with the owner service's shutdown check.
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

/// 64 hot keys overwritten with 64 KiB values: older files become dead.
fn churn(db: &ConcurrentRemoteDatabase, first: usize, count: usize) {
    let value: Bytes = vec![0x5a; 64 * 1024].into();
    for sequence in first..first + count {
        let mut key = vec![0; 32];
        key[..8].copy_from_slice(&((sequence % 64) as u64).to_be_bytes());
        let operation = (sequence as u128 + 1).to_le_bytes();
        db.write_batch(
            operation,
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key.into(),
                value: value.clone(),
            }],
        )
        .unwrap();
    }
}

#[test]
fn shadow_run_rolls_files_prunes_and_never_unlinks() {
    for tier in [DurabilityTier::Synchronous, DurabilityTier::Native] {
        let fixture = Fixture::new(tier);
        let db = fixture.open();
        let samples = Arc::new(Mutex::new(Vec::new()));
        let observer = db
            .start_capacity_observer(
                CapacityObserverConfig::default(),
                Box::new(Collect(samples.clone())),
            )
            .unwrap();
        // A second observer on the same database is refused.
        assert!(
            db.start_capacity_observer(
                CapacityObserverConfig::default(),
                Box::new(Collect(Arc::new(Mutex::new(Vec::new())))),
            )
            .is_err()
        );
        // Sampling must not settle the open leased extent: write into an open
        // extent, then take the initial inventory and compare skip markers.
        churn(&db, 0, 8);
        let skips = fixture.live.snapshot().counts.leased_skip_markers;
        observer.begin().unwrap();
        assert_eq!(fixture.live.snapshot().counts.leased_skip_markers, skips);
        churn(&db, 8, 312);
        let files_before_prune = fixture.wal_files();
        db.retire_history_before(IndexReadCut::Latest).unwrap();
        churn(&db, 320, 192);
        let summary = observer.finish().unwrap();
        let samples = samples.lock().clone();
        println!("r0 {tier:?} summary: {summary:#?}");
        assert!(summary.complete, "{tier:?}: {:?}", summary.reasons);
        assert_eq!(summary.value_wal_unlinks, Some(0));
        assert_eq!(summary.suppressed_retirement_passes, 1);
        assert_eq!(summary.prune_events, 1);
        assert!(summary.file_roll_events >= 3, "{tier:?}: {summary:?}");
        assert!(files_before_prune >= 3);
        // Shadow mode kept every file.
        assert!(fixture.wal_files() >= files_before_prune);
        let events: Vec<_> = samples.iter().map(|sample| sample.event).collect();
        assert_eq!(events.first(), Some(&SampleEvent::Begin));
        assert_eq!(events.last(), Some(&SampleEvent::End));
        assert!(events.contains(&SampleEvent::Prune));
        assert!(events.contains(&SampleEvent::Roll));
        for sample in &samples {
            assert!(sample.files.windows(2).all(|pair| (
                pair[0].partition,
                pair[0].fencing_generation,
                pair[0].file_start
            ) < (
                pair[1].partition,
                pair[1].fencing_generation,
                pair[1].file_start
            )));
            for file in &sample.files {
                assert_eq!(file.shadow_eligible, file.blockers.is_empty());
                assert!(file.allocated_bytes.is_some(), "{file:?}");
                // Every retained put member is attributed to exactly one batch set.
                assert!(file.retained_batches.unwrap_or(0) <= file.retained_put_members);
                if file.retained_put_members != 0 {
                    assert!(file.blockers.contains(&Blocker::SelectableRootRef));
                }
            }
        }
        let end = samples.last().unwrap();
        // The open file is still leased or not passed by the allocator.
        assert!(end.files.iter().any(|file| {
            file.blockers.contains(&Blocker::WriteLease)
                || file.blockers.contains(&Blocker::AllocatorNotPast)
        }));
        // The first files hold only overwritten versions after the prune.
        let reclaimed: u64 = summary
            .per_owner
            .iter()
            .map(|owner| owner.newly_shadow_reclaimed_bytes)
            .sum();
        assert!(reclaimed > 0, "{tier:?}: no shadow-eligible file: {end:#?}");
        assert!(summary.worst_owner.partition.is_some());
        db.shutdown().unwrap();
    }
}
