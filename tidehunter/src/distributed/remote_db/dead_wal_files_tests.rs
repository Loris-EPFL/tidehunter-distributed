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
use crate::Decision;
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::GroupLimits;
use crate::distributed::nvmeof::live_metrics::LiveNvmeMetrics;
use crate::distributed::storage::CrashCut;
use crate::distributed::tcp::{
    LeasedSync, LiveClientConfig, OwnerHostConfig, StorageNode, TcpLimits, Transport,
};
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
use std::fs;
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

/// The data keyspace of the fixture, plus a filtered keyspace that carries
/// the scan's Stop.
///
/// A pass with no Stop and no record at or above the scan cut does nothing, as
/// native's scan at `EndOfWal` does: no copy, no cutoff and no retirement.
/// Native's WAL-relocation tests always supply a Stop filter for that reason.
fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space_config("root", 32, 1, KeyType::uniform(1), KeySpaceConfig::new());
    builder.add_key_space_config(
        "stop",
        8,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new().with_relocation_filter(|_: &[u8], _: &[u8]| Decision::StopRelocation),
    );
    builder.build()
}

/// Appends the scan's Stop in the serving generation above the data set.
/// One marker suffices: it stays above every later write, and the pass never
/// needs to reclaim past it.
fn stop_marker(db: &ConcurrentRemoteDatabase) {
    db.write_batch(
        0x5354_4f50_0000_0000_0000_0000_0000_0001u128.to_le_bytes(),
        vec![Mutation::Put {
            keyspace: KeySpace(1),
            key: b"stop-mkr".to_vec().into(),
            value: vec![1; 64].into(),
        }],
    )
    .unwrap();
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
        let shape = shape();
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
            max_pending_batches: 64,
            max_pending_bytes: 32 * 1024 * KIB as usize,
            owner_append_occupancy: 16,
            pipelined_writes: true,
            leased_submit_redrain: true,
            read_cache_bytes: 0,
            read_memory_allowance_bytes: 128 * 1024 * KIB as usize,
            read_lanes: Some(1),
            pin_read_lanes: false,
            max_pending_reads: 64,
            max_pending_read_bytes: 16 * 1024 * KIB as usize,
            write_profile_limit: 0,
            coordinator_lease_ms: 5_000,
            resume_transient_writes: true,
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

    /// Process kill of both sides. The owner stops serving first, so no
    /// coordinator teardown request reaches it. Then the on-disk image is
    /// frozen before any teardown I/O: in-process teardown (live writer
    /// close, owner syncer catch-up root) is not what a killed process does,
    /// and after an injected cut it could install a root from pre-publication
    /// RAM. Both processes are torn down, the frozen image is put back in
    /// place (see `restore`), and both restart on it.
    fn crash(&mut self, db: ConcurrentRemoteDatabase) -> ConcurrentRemoteDatabase {
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
        db.write_core().test_hooks.lock().skip_shutdown_checkpoint = true;
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

fn churn(db: &ConcurrentRemoteDatabase, first: usize, count: usize) {
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

/// Every indexed key with its value read through the production value path,
/// from the owner WAL, so the check covers exactly the bytes retirement could
/// have removed.
fn logical_state(db: &ConcurrentRemoteDatabase) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let cut = db.hybrid_table().current();
    pipeline_tests::index_entries(db.hybrid_table(), KeySpace(0), cut)
        .into_iter()
        .filter(|(_, position)| position.is_valid())
        .map(|(key, _)| {
            let value = pipeline_tests::read_at_cut(db, KeySpace(0), &key, cut)
                .unwrap()
                .expect("an indexed put reads back");
            (key.to_vec(), value.to_vec())
        })
        .collect()
}

/// One production relocation pass: copy and re-point the survivors of the
/// oldest files, store the index, adopt the callback start, and retire every
/// dead sealed file under the retirement gate.
fn retire(db: &ConcurrentRemoteDatabase) -> Result<(), LocalDbError> {
    db.relocation_pass(false, &AtomicBool::new(false))
        .map(|_| ())
}

/// No-GC control: identical writes with no relocation pass at all, so no
/// physical retirement happens, crashed after `writes` and reopened.
fn control_after_crash(tier: DurabilityTier, writes: usize) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut fixture = Fixture::new(tier);
    let db = fixture.open();
    churn(&db, 0, INITIAL_WRITES.min(writes));
    let mut written = INITIAL_WRITES.min(writes);
    while written < writes {
        let count = ROUND_WRITES.min(writes - written);
        churn(&db, written, count);
        written += count;
    }
    let files = fixture.wal_files(db.epoch);
    let expected: Vec<u64> = (0..files.len() as u64).collect();
    assert_eq!(files, expected, "{tier:?}: the no-GC control lost a file");
    assert_eq!(fixture.node().file_maintenance_stats().retired_files, 0);
    let db = fixture.crash(db);
    let state = logical_state(&db);
    db.shutdown().unwrap();
    state
}

/// Control for `logical_state`: this suite's equality checks read every value
/// from the owner WAL, so a file unlinked behind the owner's back cannot be
/// invisible to them. The owner process keeps its files open, so the unlink
/// becomes visible when the owner restarts (`Fixture::crash`); after it, the
/// restart or the read of a value that lived in an unlinked file must fail. If
/// it did not, every `logical_state(..) == model(..)` assertion above could
/// pass without reading the files retirement removes.
///
/// The same crash and reopen succeed with every file present
/// (`control_after_crash`), so a failure here is attributable to the unlink.
#[test]
fn a_force_unlinked_live_file_cannot_pass_the_logical_state_check() {
    let mut fixture = Fixture::new(DurabilityTier::Native);
    let db = fixture.open();
    let generation = db.epoch;
    churn(&db, 0, INITIAL_WRITES);
    let files = fixture.wal_files(generation);
    assert!(
        files.len() >= 3,
        "the fixture must seal several files: {files:?}"
    );
    assert_eq!(
        logical_state(&db),
        model(INITIAL_WRITES),
        "the control's baseline"
    );
    // The newest `KEYS` writes are the live versions (1 MiB): they span the
    // newest file and the one before it. Unlink every file but the newest.
    for file in &files[..files.len() - 1] {
        let path = fixture
            .database_root()
            .join(format!("epoch_{generation:016x}"))
            .join(format!("wal_{file:016x}"));
        fs::remove_file(path).unwrap();
    }
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let db = fixture.crash(db);
        logical_state(&db)
    }));
    match outcome {
        Err(_) => {}
        Ok(state) => assert_ne!(
            state,
            model(INITIAL_WRITES),
            "values whose files were unlinked still read: the check is vacuous"
        ),
    }
}

#[test]
fn dead_sealed_serving_file_is_unlinked_while_writes_continue() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let mut fixture = Fixture::new(tier);
        let db = fixture.open();
        let generation = db.epoch;
        churn(&db, 0, INITIAL_WRITES);
        // The Stop above the initial data gives the passes a terminal: the
        // writer's later appends stay above it and are never scanned.
        stop_marker(&db);
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
        assert!(db.ensure_healthy().is_ok());
        assert_eq!(db.epoch, generation, "no authority restart");
        // A checkpoint adopts the newest writes before reading them back.
        retire(&db).unwrap();
        let state = logical_state(&db);
        assert_eq!(state, model(written), "{tier:?}");
        // Crash equivalence with the no-GC control after the same writes.
        let db = fixture.crash(db);
        assert_eq!(
            logical_state(&db),
            control_after_crash(tier, written),
            "{tier:?}"
        );
        // The reopened authority keeps serving after the retirement.
        churn(&db, written, ROUND_WRITES);
        retire(&db).unwrap();
        assert_eq!(logical_state(&db), model(written + ROUND_WRITES));
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
        let generation = db.epoch;
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
                    assert!(db.ensure_healthy().is_ok(), "{cut:?}");
                    fired = true;
                    break;
                }
            }
        }
        assert!(fired, "{cut:?}: no retirement reached the boundary");
        let db = fixture.crash(db);
        let files = fixture.wal_files(generation);
        let state = logical_state(&db);
        assert_eq!(state, model(written), "{cut:?}: {files:?}");
        assert_eq!(state, control_after_crash(tier, written), "{cut:?}");
        // The first victim set holds file 0 (seals ascend; its versions are
        // all overwritten). At every boundary after the durable root the
        // restarted owner repeated the unlink from `cleanup_pending`.
        assert!(!files.contains(&0), "{cut:?}: {files:?}");
        db.shutdown().unwrap();
    }
}

/// A held checkpoint pin keeps its VIEW readable across relocation passes, and
/// its file stays while it is held: the entry the pinned view names is an older
/// version of an overwritten key, so the pass copies its value but cannot carry
/// the re-point (the cell's latest entry is a newer version). The retirement
/// watermark is capped below that position (paper 4.4: the GC watermark tracks
/// the oldest position that may still be referenced), so file 0 is not retired
/// until the pin drops; the next pass after the release reclaims it.
#[test]
fn a_held_read_pin_keeps_its_view_across_a_retirement() {
    let fixture = Fixture::new(DurabilityTier::Native);
    let db = fixture.open();
    let generation = db.epoch;
    churn(&db, 0, KEYS);
    let (cut, pin) = db.hybrid_table().pin_checkpoint();
    let pinned = db
        .hybrid_table()
        .get(KeySpace(0), &key(0), cut)
        .unwrap()
        .unwrap();
    assert_eq!(pinned.address.offset / FILE_BYTES, 0);
    churn(&db, KEYS, INITIAL_WRITES);
    let mut written = KEYS + INITIAL_WRITES;
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        retire(&db).unwrap();
        // The pinned cut still names offset 128 in file 0 (the first write of
        // key 0), so every pass must leave that file in place.
        assert!(
            fixture.wal_files(generation).contains(&0),
            "a held pin's file was retired"
        );
    }
    assert!(
        db.retirement_pin_caps() > 0,
        "premise: the pass saw the shadowed position and capped the watermark"
    );
    let pinned_value = pipeline_tests::read_at_cut(&db, KeySpace(0), &key(0), cut)
        .unwrap()
        .expect("the pinned cut still sees the first write of key 0");
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

/// A `NoWork` pass keeps the shadow witness. The pass that first copied a
/// shadowed as-of position is not necessarily the pass that retires its file: a
/// pass with no whole file before a target merges nothing and retires nothing
/// (`targets.is_empty()` returns before the scan), and the next retirement
/// decision must still see the position. The direct retirement below is the
/// smallest way to observe that survival: it is the same function a pass calls
/// after its store.
///
/// Negative control (mutation run): drop the witness when a pass has no target
/// (`self.shadows.clear()` at the `targets.is_empty()` return in
/// `relocation_pass`); the direct retirement below then retires file 0 and the
/// assertion after it fails ("the NoWork pass dropped the witness"). Rebuilding
/// the witness only from the current pass's list is a different mutation and
/// cannot turn this test red, because a `NoWork` pass returns before the merge.
#[test]
fn a_no_work_pass_keeps_the_shadow_cap() {
    let fixture = Fixture::new(DurabilityTier::Native);
    let db = fixture.open();
    let generation = db.epoch;
    churn(&db, 0, KEYS);
    let (cut, pin) = db.hybrid_table().pin_checkpoint();
    let pinned = db
        .hybrid_table()
        .get(KeySpace(0), &key(0), cut)
        .unwrap()
        .unwrap();
    assert_eq!(
        pinned.address.offset / FILE_BYTES,
        0,
        "premise: the pinned cut names a position in WAL file 0"
    );
    churn(&db, KEYS, INITIAL_WRITES);

    // The first pass has whole files before its target: it copies the as-of
    // positions, cannot re-point the overwritten key and caps the stream.
    let first = retire_report(&db);
    assert_eq!(
        first.gate,
        crate::distributed::remote_db::relocation::RelocationGate::DidWork,
        "{first:?}"
    );
    assert!(
        db.retirement_pin_caps() > 0,
        "premise: the first pass saw the shadowed position and capped the watermark: {first:?}"
    );
    assert!(
        fixture.wal_files(generation).contains(&0),
        "the first pass retired the pinned file"
    );

    // The next pass has no whole file before its target (`target == from`), so
    // it returns before the scan and before any retirement.
    let second = retire_report(&db);
    assert_eq!(
        second.gate,
        crate::distributed::remote_db::relocation::RelocationGate::NoWork,
        "premise: pass 2 has no target: {second:?}"
    );

    // The witness outlived that pass: a retirement decision taken now still caps
    // the stream.
    {
        let core = db.write_core();
        let mut gate = core.retirement.lock();
        super::dead_wal_files::core_retire_dead_wal_files(core, &mut gate)
            .expect("retirement after the NoWork pass");
    }
    assert!(
        fixture.wal_files(generation).contains(&0),
        "the NoWork pass dropped the witness: the next retirement reclaimed the file a held cut \
         still resolves"
    );
    let pinned_value = pipeline_tests::read_at_cut(&db, KeySpace(0), &key(0), cut)
        .unwrap()
        .expect("the pinned cut still sees the first write of key 0");
    assert_eq!(pinned_value.to_vec(), value(0));

    // Release: the witness is no longer needed and the next reclaiming pass
    // retires file 0, so the cap is never a permanent stall.
    drop(pin);
    let mut written = KEYS + INITIAL_WRITES;
    for _ in 0..ROUNDS {
        churn(&db, written, ROUND_WRITES);
        written += ROUND_WRITES;
        retire(&db).unwrap();
        if !fixture.wal_files(generation).contains(&0) {
            break;
        }
    }
    assert!(
        !fixture.wal_files(generation).contains(&0),
        "file 0 never retired after the pin dropped"
    );
    db.shutdown().unwrap();
}

/// One production relocation pass with its report, for tests that assert the
/// gate rather than only the effect.
fn retire_report(db: &ConcurrentRemoteDatabase) -> crate::distributed::remote_db::RelocationReport {
    db.relocation_pass(false, &AtomicBool::new(false))
        .expect("relocation pass")
}

/// A held read cut that can name no byte of the victims does not block
/// retirement. Every key is rewritten after the file it first named, so the
/// position the pinned cut resolves is in a later file; the pass's as-of view
/// shows those positions, the live entries above the cut shadow them, and the
/// per-stream cap therefore sits above the victims. File 0 retires while the
/// checkpoint is held, and every pinned read still resolves its own value.
#[test]
fn a_latch_whose_positions_are_all_above_the_victims_does_not_block_retirement() {
    let fixture = Fixture::new(DurabilityTier::Native);
    let db = fixture.open();
    let generation = db.epoch;
    // Four rounds of the key set, then four more: every key's newest position
    // before the pin is in the second block.
    churn(&db, 0, 4 * KEYS);
    let mut written = 4 * KEYS;
    churn(&db, written, 4 * KEYS);
    written += 4 * KEYS;
    // The publication watermark may lag the acknowledged writes. Take the pin
    // and keep writing until every key resolves above file 0 at it: the
    // condition is the test's premise, read from the index, never a timing
    // premise.
    let (cut, pin) = loop {
        let (cut, pin) = db.hybrid_table().pin_checkpoint();
        let all_above = (0..KEYS).all(|sequence| {
            db.hybrid_table()
                .get(KeySpace(0), &key(sequence), cut)
                .unwrap()
                .is_some_and(|position| position.address.offset >= FILE_BYTES)
        });
        if all_above {
            break (cut, pin);
        }
        drop(pin);
        churn(&db, written, 2 * KEYS);
        written += 2 * KEYS;
        assert!(
            written < 64 * KEYS,
            "the pinned cut never reached the second block"
        );
    };
    // A newer write per key after the pin, so the pass's compare-and-set is
    // shadowed and its cap has to decide the file.
    churn(&db, written, 4 * KEYS);
    retire(&db).unwrap();
    assert!(
        !fixture.wal_files(generation).contains(&0),
        "file 0 holds no position the pinned cut can resolve: it must retire"
    );
    assert!(
        db.retirement_pin_caps() > 0,
        "premise: the pass saw shadowed re-points and capped the watermark"
    );
    for sequence in 0..KEYS {
        let pinned_value = pipeline_tests::read_at_cut(&db, KeySpace(0), &key(sequence), cut)
            .unwrap()
            .expect("the pinned cut still sees its own value");
        let bytes = pinned_value.to_vec();
        let written_at = u64::from_be_bytes(
            bytes[..8]
                .try_into()
                .expect("every value starts with its write sequence"),
        ) as usize;
        assert_eq!(
            bytes,
            value(written_at),
            "key {sequence}: the pinned value is exactly the write it names"
        );
        assert!(
            written_at < written,
            "key {sequence}: the pinned cut resolves a write below the pin"
        );
    }
    drop(pin);
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
    let generation = db.epoch;
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
    db.write_core().test_hooks.lock().skip_shutdown_checkpoint = true;
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
        let generation = db.epoch;
        let log = fixture.node().source_log_for_test(generation, generation);
        let (entered, resume) = log.pause_before_unlink();
        churn(&db, 0, INITIAL_WRITES);
        // The Stop above the initial data gives the passes a terminal.
        stop_marker(&db);
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
        assert_eq!(logical_state(&db), model(written), "{tier:?}");
        db.shutdown().unwrap();
    }
}

mod r1_events;
