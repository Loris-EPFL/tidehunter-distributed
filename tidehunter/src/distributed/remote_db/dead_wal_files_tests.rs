//! R1 coordinator pass on the leased ordinary-file fixture.
//!
//! * A dead, durably sealed file of the serving fencing generation is
//!   unlinked by `retire_history_before` while a writer keeps appending into
//!   later files, with no authority restart.
//! * Crash equivalence: the GC run and a no-GC control (R0 shadow mode keeps
//!   the same logical pruning, checkpoints and floor and suppresses every
//!   physical retirement) are crashed after the same writes, including at
//!   each owner-side retirement boundary, and must reopen to the same
//!   logical state, read directly from the owner WAL. A crash freezes the
//!   on-disk image both processes leave at that instant (a process kill
//!   keeps the page cache, so file contents are the image) before any
//!   teardown I/O, then restarts both processes on that image.
//!
//! Ordinary files never qualify the Linux target; kernel namespace drain is
//! the CI kernel job's to prove.
use super::pipeline_tests::{open_live_fixture_host, trusted_owner_host_config};
use super::*;
use crate::distributed::capacity::{CapacityObserverConfig, R0Sample, R0Sink};
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
use crate::distributed::storage::CrashCut;
use crate::distributed::tcp::{
    LeasedSync, LiveClientConfig, OwnerHostConfig, StorageNode, TcpLimits, Transport,
};
use crate::key_shape::KeyType;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};

const KIB: u64 = 1024;
const KEYS: usize = 64;
const VALUE: usize = 16 * KIB as usize;
const FILE_BYTES: u64 = 1024 * KIB;
/// About four 1 MiB files before the first prune.
const INITIAL_WRITES: usize = 240;
const ROUND_WRITES: usize = 48;
const ROUNDS: usize = 6;
/// Directories that make up both processes' durable state.
const CRASH_STATE: [&str; 3] = ["owner", "owner.configfs", "authority"];

struct Discard;

impl R0Sink for Discard {
    fn sample(&mut self, _sample: &R0Sample) -> std::io::Result<()> {
        Ok(())
    }
}

/// One owner process: the node and its serving thread.
struct Owner {
    node: Arc<StorageNode>,
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
}

impl Owner {
    /// Close the listener and every session: the owner receives nothing
    /// more, as after a kill. The node itself stays alive until dropped.
    fn stop_serving(&mut self) {
        // Release pairs with the owner service's shutdown check.
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            server.join().unwrap();
        }
    }
}

/// Copy `src` to `dst` as a crash image. WAL files are hard links so their
/// inode stays the one that seals and pending retirements bind; metadata is
/// copied, so later teardown renames cannot change the image. A file renamed
/// away during the copy (a root install in flight) is skipped: either root
/// generation is a valid crash state.
fn freeze(src: &Path, dst: &Path) {
    fs::create_dir(dst).unwrap();
    for entry in fs::read_dir(src).unwrap().flatten() {
        let from = entry.path();
        let to = dst.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        let result = if kind.is_dir() {
            freeze(&from, &to);
            Ok(())
        } else if kind.is_symlink() {
            fs::read_link(&from).and_then(|target| std::os::unix::fs::symlink(target, &to))
        } else if entry.file_name().to_string_lossy().starts_with("wal_") {
            fs::hard_link(&from, &to)
        } else {
            fs::copy(&from, &to).map(drop)
        };
        match result {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("crash image of {from:?} failed: {error}"),
        }
    }
}

/// `path`'s own metadata (a symlink is not followed), if it exists.
fn entry_metadata(path: &Path) -> Option<fs::Metadata> {
    fs::symlink_metadata(path).ok()
}

/// `live` already is the frozen `image` entry: the same directory slot, the
/// same symlink target, or the same inode (WAL files are frozen as hard
/// links). Such an entry is kept, so its identity survives.
fn unchanged(image: &Path, live: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    let (Some(frozen), Some(current)) = (entry_metadata(image), entry_metadata(live)) else {
        return false;
    };
    let (frozen_kind, current_kind) = (frozen.file_type(), current.file_type());
    if frozen_kind.is_dir() || current_kind.is_dir() {
        frozen_kind.is_dir() && current_kind.is_dir()
    } else if frozen_kind.is_symlink() || current_kind.is_symlink() {
        frozen_kind.is_symlink()
            && current_kind.is_symlink()
            && fs::read_link(image).ok() == fs::read_link(live).ok()
    } else {
        (frozen.dev(), frozen.ino()) == (current.dev(), current.ino())
    }
}

/// Put the crash image `image` back into `live` in place, as a killed
/// process leaves it. Entries the image still matches (see `unchanged`)
/// keep their identity: the target journal binds its state and sealed
/// directories and every configfs directory and link by device and inode.
/// Everything else in `live` is replaced by the image, by rename.
fn restore(image: &Path, live: &Path) {
    for entry in fs::read_dir(live).unwrap().flatten() {
        let path = entry.path();
        if unchanged(&image.join(entry.file_name()), &path) {
            continue;
        }
        if entry.file_type().unwrap().is_dir() {
            fs::remove_dir_all(&path).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
        }
    }
    for entry in fs::read_dir(image).unwrap().flatten() {
        let to = live.join(entry.file_name());
        if !unchanged(&entry.path(), &to) {
            fs::rename(entry.path(), &to).unwrap();
        } else if entry.file_type().unwrap().is_dir() {
            restore(&entry.path(), &to);
        }
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

struct Fixture {
    config: RemoteDatabaseConfig,
    host_config: OwnerHostConfig,
    shape: KeyShape,
    address: String,
    live: Arc<LiveNvmeMetrics>,
    owner: Option<Owner>,
    // Dropped last: owner WAL files and the authority directory live here.
    root: tempdir::TempDir,
}

impl Fixture {
    fn new(tier: DurabilityTier) -> Self {
        let root = tempdir::TempDir::new("r1-dead-wal-files").unwrap();
        let shape = KeyShape::new_single(32, 1, KeyType::uniform(1));
        let tls = TlsFiles {
            transport: Transport::TcpTrusted,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".into(),
        };
        let address = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let config = RemoteDatabaseConfig {
            storage: LocalInventoryConfig {
                database: [0; 16],
                partitions: vec![0],
                batch_limits: BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 64 * KIB as usize,
                },
                group_limits: GroupLimits {
                    max_batches: 32,
                    max_encoded_bytes: 256 * KIB as usize,
                },
                // Two fragments per file: every second extent rolls a file.
                fragment_bytes: 512 * KIB,
                wal_file_bytes: 1024 * KIB,
                max_batches_per_partition: 8192,
                max_reference_metadata_bytes: 64 * 1024 * KIB as usize,
            },
            endpoints: vec![RemoteEndpoint {
                partition: 0,
                address: address.clone(),
                server_name: tls.server_name.clone(),
            }],
            tls,
            timeout: Duration::from_secs(60),
            max_admitted_batches: 8192,
            max_index_bytes: 64 * 1024 * KIB as usize,
            max_pending_batches: 64,
            max_pending_bytes: 32 * 1024 * KIB as usize,
            replay_limits: ReplayLimits {
                max_batches: 65536,
                max_encoded_bytes: 1024 * 1024 * KIB as usize,
            },
            owner_append_occupancy: 16,
            pipelined_writes: true,
            leased_submit_redrain: true,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: 128 * 1024 * KIB as usize,
            scan_prefetch_pages: 1,
            read_lanes: Some(1),
            pin_read_lanes: false,
            max_pending_reads: 64,
            max_pending_read_bytes: 16 * 1024 * KIB as usize,
            write_profile_limit: 0,
            live_nvme: LiveClientConfig {
                lease_ahead: true,
                polled_writes: false,
                host_nqn: "nqn.2026-01.test:r1-dead-files".into(),
                host_id: [3; 16],
                owner_identities: Default::default(),
                leased_write_window: Some(4),
                leased_sync: LeasedSync::PerFrameFua,
                leased_flush_min_frame_bytes: 0,
                max_inflight_requests: 32,
                max_inflight_bytes: 4 * 1024 * KIB as usize,
                connect_timeout_ms: 10000,
                io_timeout_ms: 30000,
                leased_durability: tier,
                inline_reads: true,
                queue_geometry: Default::default(),
            },
            durable_cut: Default::default(),
            relocation_max_reclaim_pct: 100,
            index: Default::default(),
        };
        let host_config = trusted_owner_host_config(root.path().join("owner"));
        let mut fixture = Self {
            config,
            host_config,
            shape,
            address,
            live: LiveNvmeMetrics::new(),
            owner: None,
            root,
        };
        fixture.start_owner();
        fixture
    }

    /// Start (or restart after a crash) the owner process on the same root,
    /// target journal and address. Recovery repeats any pending retirement.
    fn start_owner(&mut self) {
        let host = open_live_fixture_host(self.host_config.clone()).unwrap();
        // std sets SO_REUSEADDR on Unix listeners: the restarted owner binds
        // the address its predecessor released.
        let listener = TcpListener::bind(&self.address).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let serving = Arc::clone(&host);
        let stopping = stop.clone();
        let tls = self.config.tls.clone();
        let server = thread::spawn(move || {
            serving
                .serve(listener, tls, TcpLimits::default(), stopping)
                .unwrap()
        });
        if self.config.storage.database == [0; 16] {
            authority_lifecycle::provision_for_test(
                &self.root.path().join("authority"),
                &self.shape,
                &mut self.config,
            )
            .unwrap();
        }
        let node = host.database(&self.config.storage.database).unwrap();
        self.owner = Some(Owner {
            node,
            stop,
            server: Some(server),
        });
    }

    fn node(&self) -> &StorageNode {
        &self.owner.as_ref().unwrap().node
    }

    fn database_root(&self) -> PathBuf {
        self.host_config.root.join(format!(
            "db-{}",
            crate::distributed::tcp::host::identity::database_hex(&self.config.storage.database)
        ))
    }

    /// Open the authority with every client on the ordinary-file live fixture
    /// from the start, so a reopen's recovery reads use it too.
    fn open(&self) -> RemoteDatabase {
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
        db
    }

    /// Process kill of both sides. The owner stops serving first, so no
    /// coordinator teardown request reaches it. Then the on-disk image is
    /// frozen before any teardown I/O: in-process teardown (live writer
    /// close, owner syncer catch-up root) is not what a killed process does,
    /// and after an injected cut it could install a root from pre-publication
    /// RAM. Both processes are torn down, the frozen image is put back in
    /// place (see `restore`), and both restart on it.
    fn crash(&mut self, db: RemoteDatabase) -> RemoteDatabase {
        let mut owner = self.owner.take().unwrap();
        owner.stop_serving();
        // No grant can follow; finish the preparers' current step so the
        // image is taken at a step boundary of the target journal.
        owner.node.stop_file_preparers_for_test();
        let image = self.root.path().join("crash-image");
        if image.exists() {
            fs::remove_dir_all(&image).unwrap();
        }
        fs::create_dir(&image).unwrap();
        for name in CRASH_STATE {
            freeze(&self.root.path().join(name), &image.join(name));
        }
        db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
        drop(db);
        drop(owner);
        for name in CRASH_STATE {
            restore(&image.join(name), &self.root.path().join(name));
        }
        // Kept WAL files still have their frozen hard link in the image; the
        // target refuses a backing file with more than one link.
        fs::remove_dir_all(&image).unwrap();
        self.start_owner();
        self.open()
    }

    /// Sorted WAL file indexes of `generation` on the owner.
    fn wal_files(&self, generation: u64) -> Vec<u64> {
        let directory = self
            .database_root()
            .join(format!("epoch_{generation:016x}"));
        let mut files: Vec<u64> = fs::read_dir(directory)
            .unwrap()
            .flatten()
            .filter_map(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                u64::from_str_radix(name.strip_prefix("wal_")?, 16).ok()
            })
            .collect();
        files.sort_unstable();
        files
    }
}

fn key(sequence: usize) -> Vec<u8> {
    let mut key = vec![0; 32];
    key[..8].copy_from_slice(&((sequence % KEYS) as u64).to_be_bytes());
    key
}

/// Distinct per write: the sequence in the first bytes, then a fill byte.
fn value(sequence: usize) -> Vec<u8> {
    let mut value = vec![(sequence % 251) as u8; VALUE];
    value[..8].copy_from_slice(&(sequence as u64).to_be_bytes());
    value
}

fn churn(db: &RemoteDatabase, first: usize, count: usize) {
    for sequence in first..first + count {
        db.write_batch(
            (sequence as u128 + 1).to_le_bytes(),
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: key(sequence).into(),
                value: value(sequence).into(),
            }],
        )
        .unwrap();
    }
}

/// Final value of each hot key after `writes` acknowledged writes.
fn model(writes: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    (writes.saturating_sub(KEYS)..writes)
        .map(|sequence| (key(sequence), value(sequence)))
        .collect()
}

/// Every indexed key with its value read straight from the owner WAL, so
/// the check covers exactly the bytes retirement could have removed.
fn logical_state(db: &RemoteDatabase, node: &StorageNode) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let cut = db.published_cut().unwrap();
    let mut state = BTreeMap::new();
    let mut after: Option<Bytes> = None;
    loop {
        let page = db
            .index_page(KeySpace(0), after.as_deref(), false, cut.clone())
            .unwrap();
        let Some((last, _)) = page.last() else {
            break;
        };
        let last = last.clone();
        for (key, reference) in &page {
            let log = node.source_log_for_test(db.epoch(), reference.batch_version.epoch);
            let Mutation::Put { value, .. } = log.read_record(reference).unwrap() else {
                panic!("indexed put resolved to another record kind");
            };
            state.insert(key.to_vec(), value.to_vec());
        }
        after = Some(last);
    }
    state
}

fn retire(db: &RemoteDatabase) -> Result<(), LocalDbError> {
    db.core.retire_history_before(IndexReadCut::Latest)
}

/// No-GC control: identical writes and prune calls with every physical
/// retirement suppressed, crashed after `writes` and reopened.
fn control_after_crash(tier: DurabilityTier, writes: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut fixture = Fixture::new(tier);
    let db = fixture.open();
    let observer = CapacityObserver::start(
        &db.core,
        CapacityObserverConfig::default(),
        Box::new(Discard),
    )
    .unwrap();
    churn(&db, 0, INITIAL_WRITES.min(writes));
    let mut written = INITIAL_WRITES.min(writes);
    while written < writes {
        let count = ROUND_WRITES.min(writes - written);
        churn(&db, written, count);
        written += count;
        retire(&db).unwrap();
    }
    let files = fixture.wal_files(db.epoch());
    let expected: Vec<u64> = (0..files.len() as u64).collect();
    assert_eq!(files, expected, "{tier:?}: the no-GC control lost a file");
    assert_eq!(fixture.node().file_maintenance_stats().retired_files, 0);
    drop(observer);
    let db = fixture.crash(db);
    let state = logical_state(&db, fixture.node());
    db.shutdown().unwrap();
    state
}

#[test]
fn dead_sealed_serving_file_is_unlinked_while_writes_continue() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let mut fixture = Fixture::new(tier);
        let db = fixture.open();
        let generation = db.epoch();
        churn(&db, 0, INITIAL_WRITES);
        let mut written = INITIAL_WRITES;
        let mut retired_while_writing = None;
        for _ in 0..ROUNDS {
            // The writer appends into the open (later) file while the pass
            // pages, proves and unlinks dead sealed files.
            let retired_before = fixture.node().file_maintenance_stats().retired_files;
            thread::scope(|scope| {
                let writer = scope.spawn(|| churn(&db, written, ROUND_WRITES));
                retire(&db).unwrap();
                writer.join().unwrap();
            });
            written += ROUND_WRITES;
            let retired = fixture.node().file_maintenance_stats().retired_files;
            if retired > retired_before && retired_while_writing.is_none() {
                retired_while_writing = fixture.wal_files(generation).last().copied();
            }
        }
        // Keep writing past the file that was open at the first retirement.
        churn(&db, written, 2 * ROUND_WRITES);
        written += 2 * ROUND_WRITES;
        let highest_at_retirement = retired_while_writing
            .unwrap_or_else(|| panic!("{tier:?}: no dead sealed file was retired"));
        let files = fixture.wal_files(generation);
        assert!(
            !files.contains(&0),
            "{tier:?}: file 0 still present: {files:?}"
        );
        // Writes went on into files after the one open at the retirement.
        assert!(files.last().copied() > Some(highest_at_retirement));
        let stats = fixture.node().file_maintenance_stats();
        assert!(stats.sealed_files >= stats.retired_files && stats.retired_files >= 1);
        assert_eq!(stats.prepared_gate_waits, 0, "{tier:?}: {stats:?}");
        assert!(db.core.ensure_healthy().is_ok());
        assert_eq!(db.epoch(), generation, "no authority restart");
        // A checkpoint adopts the newest writes before reading them back.
        retire(&db).unwrap();
        let state = logical_state(&db, fixture.node());
        assert_eq!(state, model(written), "{tier:?}");
        // Crash equivalence with the no-GC control after the same writes.
        let db = fixture.crash(db);
        assert_eq!(
            logical_state(&db, fixture.node()),
            control_after_crash(tier, written),
            "{tier:?}"
        );
        // The reopened authority keeps serving after the retirement.
        churn(&db, written, ROUND_WRITES);
        retire(&db).unwrap();
        assert_eq!(
            logical_state(&db, fixture.node()),
            model(written + ROUND_WRITES)
        );
        db.shutdown().unwrap();
    }
}

/// A crash at every owner-side retirement boundary of a coordinator pass
/// (durable root not yet published, before detach, before unlink, after the
/// first unlink, after the directory sync) reopens to the no-GC state.
#[test]
fn coordinator_pass_crash_cuts_match_the_no_gc_control() {
    let cuts = [
        CrashCut::RetireAfterRoot,
        CrashCut::BeforeDetach,
        CrashCut::BeforeUnlink,
        CrashCut::AfterFirstUnlink,
        CrashCut::AfterDirectorySync,
    ];
    for cut in cuts {
        eprintln!("crash cut {cut:?}");
        let tier = DurabilityTier::Native;
        let mut fixture = Fixture::new(tier);
        let db = fixture.open();
        let generation = db.epoch();
        churn(&db, 0, INITIAL_WRITES);
        let mut written = INITIAL_WRITES;
        fixture
            .node()
            .source_log_for_test(generation, generation)
            .arm_crash_cut(cut);
        let mut fired = false;
        for _ in 0..ROUNDS {
            churn(&db, written, ROUND_WRITES);
            written += ROUND_WRITES;
            match retire(&db) {
                Ok(()) => {}
                // The owner stopped at the armed boundary. The pass error did
                // not poison the coordinator; the processes crash here.
                Err(error) => {
                    assert!(
                        error
                            .message()
                            .contains("injected seal or retirement crash"),
                        "{cut:?}: {error:?}"
                    );
                    assert!(db.core.ensure_healthy().is_ok(), "{cut:?}");
                    fired = true;
                    break;
                }
            }
        }
        assert!(fired, "{cut:?}: no retirement reached the boundary");
        let db = fixture.crash(db);
        let files = fixture.wal_files(generation);
        let state = logical_state(&db, fixture.node());
        assert_eq!(state, model(written), "{cut:?}: {files:?}");
        assert_eq!(state, control_after_crash(tier, written), "{cut:?}");
        // The first victim set holds file 0 (seals ascend; its versions are
        // all overwritten). At every boundary after the durable root the
        // restarted owner repeated the unlink from `cleanup_pending`.
        assert!(!files.contains(&0), "{cut:?}: {files:?}");
        db.shutdown().unwrap();
    }
}

/// Decision 2: a coordinator read owns its index pin until its terminal
/// completion, and a held pin keeps every version visible at its cut in the
/// installed image. The pass therefore never retires a file such a read can
/// still use; once the pin drops, a later pass retires the file.
#[test]
fn held_read_pin_keeps_its_file_until_released() {
    let fixture = Fixture::new(DurabilityTier::Native);
    let db = fixture.open();
    let generation = db.epoch();
    churn(&db, 0, KEYS);
    let cut = db.published_cut().unwrap();
    let pinned = db
        .index_reference(KeySpace(0), &key(0), cut.clone())
        .unwrap()
        .unwrap();
    assert_eq!(pinned.batch_address.offset / FILE_BYTES, 0);
    let pin = db.core.index.pin_call_floor(cut).unwrap();
    churn(&db, KEYS, INITIAL_WRITES);
    let mut written = KEYS + INITIAL_WRITES;
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        retire(&db).unwrap();
    }
    assert!(fixture.wal_files(generation).contains(&0));
    // The version visible at the pinned cut is still readable on the owner.
    let Mutation::Put {
        value: pinned_value,
        ..
    } = fixture
        .node()
        .source_log_for_test(generation, generation)
        .read_record(&pinned)
        .unwrap()
    else {
        panic!("pinned put resolved to another record kind");
    };
    assert_eq!(pinned_value.to_vec(), value(0));
    drop(pin);
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        retire(&db).unwrap();
        if !fixture.wal_files(generation).contains(&0) {
            break;
        }
    }
    assert!(!fixture.wal_files(generation).contains(&0));
    db.shutdown().unwrap();
}

/// A Fence of the owner that arrives while a serving retirement is parked
/// before its unlink waits for the retirement instead of racing it: the
/// retirement holds the active log across its syncs, and a racing fence
/// would find that handle and fail with "outstanding handles; reopen
/// required", leaving the owner unhealthy. The control scheduler orders
/// them: maintenance work counts as an active stream operation, and Fence
/// is an exclusive control that waits for those. The fence completes after
/// the release, the victim is gone, and the same owner is healthy at
/// the new generation.
#[test]
fn fence_waits_for_a_parked_serving_retirement() {
    let fixture = Fixture::new(DurabilityTier::Native);
    let db = fixture.open();
    let generation = db.epoch();
    churn(&db, 0, INITIAL_WRITES);
    let mut written = INITIAL_WRITES;
    let mut victim_path = None;
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        // Connected before the park: a connection's own Status control would
        // also wait behind the parked retirement.
        let config = &fixture.config;
        let endpoint = &config.endpoints[0];
        let mut fencer = Client::connect_shared(
            &endpoint.address,
            &config.tls,
            config.tls.client_config_with_identity().unwrap(),
            config.timeout,
            config.partition_config(1, endpoint.partition),
            &fixture.shape,
        )
        .unwrap();
        thread::scope(|scope| {
            // The test's own handle on the active log is dropped before the
            // fence: a fence requires the active log's only handle.
            let log = fixture.node().source_log_for_test(generation, generation);
            let (entered, resume) = log.pause_before_unlink();
            let retirement = scope.spawn(|| retire(&db));
            let deadline = Instant::now() + Duration::from_secs(60);
            while !retirement.is_finished() {
                if entered.recv_timeout(Duration::from_millis(10)).is_ok() {
                    break;
                }
                assert!(Instant::now() < deadline, "pass never finished");
            }
            if retirement.is_finished() {
                retirement.join().unwrap().unwrap();
                return;
            }
            let (_, pending) = log.file_retirement_state();
            let victim = *pending.first().expect("parked retirement has victims");
            let path = fixture
                .database_root()
                .join(format!("epoch_{generation:016x}"))
                .join(format!("wal_{:016x}", victim.start / FILE_BYTES));
            drop(log);
            // The second client fences the owner to the next generation
            // while the retirement is parked with the active log in hand.
            let fence = scope.spawn(move || {
                let fenced = fencer.fence_to(generation + 1);
                (fencer, fenced)
            });
            // Give the fence time to reach the owner while the retirement is
            // parked; it must still be waiting when released.
            let window = Instant::now() + Duration::from_secs(2);
            while !fence.is_finished() && Instant::now() < window {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(
                !fence.is_finished(),
                "fence did not wait for the parked retirement: {:?}",
                fence.join().unwrap().1
            );
            resume.send(()).unwrap();
            retirement.join().unwrap().unwrap();
            let (mut fencer, fenced) = fence.join().unwrap();
            let status =
                fenced.unwrap_or_else(|error| panic!("fence raced the retirement: {error:?}"));
            assert!(status.healthy && status.active_epoch == Some(generation + 1));
            // The owner keeps serving controls in the new generation without
            // a reopen.
            let status = fencer.status().unwrap();
            assert!(status.healthy && status.active_epoch == Some(generation + 1));
            victim_path = Some(path);
        });
        if victim_path.is_some() {
            break;
        }
    }
    let victim_path = victim_path.expect("no retirement reached the pause");
    assert!(fs::metadata(&victim_path).is_err(), "victim survived");
    // The fenced coordinator is stale: it is dropped without its shutdown
    // checkpoint (that coordinator's authority no longer exists).
    db.core.test_hooks.lock().skip_shutdown_checkpoint = true;
    drop(db);
}

/// The overlap itself: a real serving-stream retirement is parked after its
/// durable root and detach, before its unlink, with no lock held. Writes
/// must still be acknowledged and roll into a later file within a bounded
/// time. A retirement that stalled foreground writes fails here, where a
/// test that only interleaves writes and passes could still pass. After the
/// release, the victim inode is gone.
#[test]
fn writes_roll_into_a_later_file_while_a_retirement_is_parked_before_unlink() {
    use std::os::unix::fs::MetadataExt;
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = Fixture::new(tier);
        let db = fixture.open();
        let generation = db.epoch();
        let log = fixture.node().source_log_for_test(generation, generation);
        let (entered, resume) = log.pause_before_unlink();
        churn(&db, 0, INITIAL_WRITES);
        let mut written = INITIAL_WRITES;
        let mut parked = false;
        for _ in 0..ROUNDS {
            churn(&db, written, ROUND_WRITES);
            written += ROUND_WRITES;
            thread::scope(|scope| {
                let retirement = scope.spawn(|| retire(&db));
                let deadline = Instant::now() + Duration::from_secs(60);
                while !retirement.is_finished() {
                    if entered.recv_timeout(Duration::from_millis(10)).is_ok() {
                        parked = true;
                        break;
                    }
                    assert!(Instant::now() < deadline, "{tier:?}: pass never finished");
                }
                if !parked {
                    retirement.join().unwrap().unwrap();
                    return;
                }
                // Parked: the durable root names the victims; files remain.
                let (_, pending) = log.file_retirement_state();
                let victim = *pending.first().expect("parked retirement has victims");
                let victim_path = fixture
                    .database_root()
                    .join(format!("epoch_{generation:016x}"))
                    .join(format!("wal_{:016x}", victim.start / FILE_BYTES));
                assert_eq!(fs::metadata(&victim_path).unwrap().ino(), victim.inode);
                // The write position, not the directory: the preparer creates
                // the successor file ahead of the writes, and the file after
                // it only some time after the roll, so a listing races.
                let open = log.live_allocator_file().unwrap();
                // Foreground writes, acknowledged under a bounded wait, cross
                // into a later file while the retirement stays parked. Three
                // rounds are over 2 MiB of values: more than two 1 MiB files
                // even after an extent already leased ahead.
                let (done, acknowledged) = mpsc::sync_channel(1);
                let (first, writer_db) = (written, &db);
                scope.spawn(move || {
                    churn(writer_db, first, 3 * ROUND_WRITES);
                    let _ = done.send(());
                });
                acknowledged
                    .recv_timeout(Duration::from_secs(60))
                    .unwrap_or_else(|_| {
                        panic!("{tier:?}: writes stalled behind a parked retirement")
                    });
                written += 3 * ROUND_WRITES;
                let after = log.live_allocator_file().unwrap();
                assert!(after > open, "{tier:?}: writes stayed in file {open}");
                assert_eq!(fs::metadata(&victim_path).unwrap().ino(), victim.inode);
                resume.send(()).unwrap();
                retirement.join().unwrap().unwrap();
                assert!(
                    fs::metadata(&victim_path).is_err(),
                    "{tier:?}: victim inode survived its retirement"
                );
            });
            if parked {
                break;
            }
        }
        assert!(parked, "{tier:?}: no retirement reached the pause");
        retire(&db).unwrap();
        assert_eq!(
            logical_state(&db, fixture.node()),
            model(written),
            "{tier:?}"
        );
        db.shutdown().unwrap();
    }
}

mod r1_events;
