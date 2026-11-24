//! R1 logical pruning through the Sui adapter on the leased ordinary-file
//! fixture (CAPACITY-R0-R1.md §5 and §7).
//!
//! The filter decides from the value's `(sequence, member)`. The tests move
//! the Stop after writing, once the WAL placement of each batch is known, so
//! the Stop lands at an exact file or owner position. The pass runs on the
//! database's relocation worker; `start_relocation_for_test` waits for it.
//!
//! Ordinary files never qualify the Linux target; namespace drain is the CI
//! kernel job's to prove.
#![cfg(feature = "test_methods")]

use super::pipeline_tests::{open_live_fixture_host, trusted_owner_host_config};
use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::GroupLimits;
use crate::distributed::sui::Db;
use crate::distributed::tcp::{LeasedSync, LiveClientConfig, TcpLimits, Transport};
use crate::key_shape::{KeyShapeBuilder, KeySpaceConfig, KeyType};
use crate::{Decision, RelocationObservation, RelocationTestError};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const KIB: u64 = 1024;
const FILE: u64 = 1024 * KIB;
const FRAGMENT: u64 = 512 * KIB;
const VALUE: usize = 8 * KIB as usize;
const NO_STOP: u64 = u64::MAX;

/// Filter decisions, read by every filtered keyspace of one database.
struct Policy {
    /// `sequence << 8 | member` of the Stop member, or `NO_STOP`. Earlier
    /// members of the Stop batch are removed: the whole batch must survive.
    stop: AtomicU64,
    /// Puts of sequences below it are removed; all others are kept.
    remove_below: AtomicU64,
    /// Parks the next filter call: sends on the first, waits on the second.
    pause: Mutex<Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>>,
}

impl Policy {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stop: AtomicU64::new(NO_STOP),
            remove_below: AtomicU64::new(0),
            pause: Mutex::new(None),
        })
    }

    fn set(&self, stop: Option<(u64, u8)>, remove_below: u64) {
        let stop = stop.map_or(NO_STOP, |(sequence, member)| {
            sequence << 8 | u64::from(member)
        });
        // Relaxed: the next pass starts after this call returns on the same
        // thread that requests it; the worker hand-off orders the stores.
        self.stop.store(stop, Ordering::Relaxed);
        self.remove_below.store(remove_below, Ordering::Relaxed);
    }

    fn decide(&self, value: &[u8]) -> Decision {
        if let Some((entered, resume)) = self.pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        let (sequence, member) = decode(value);
        let stop = self.stop.load(Ordering::Relaxed);
        if stop != NO_STOP && sequence == stop >> 8 {
            match u64::from(member).cmp(&(stop & 0xff)) {
                std::cmp::Ordering::Equal => return Decision::StopRelocation,
                std::cmp::Ordering::Less => return Decision::Remove,
                std::cmp::Ordering::Greater => {}
            }
        }
        if sequence < self.remove_below.load(Ordering::Relaxed) {
            Decision::Remove
        } else {
            Decision::Keep
        }
    }
}

fn shape(policy: &Arc<Policy>, keyspaces: &[&str]) -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    for &name in keyspaces {
        let policy = Arc::clone(policy);
        builder.add_key_space_config(
            name,
            8,
            1,
            KeyType::uniform(1),
            KeySpaceConfig::new()
                .with_relocation_filter(move |_: &[u8], value: &[u8]| policy.decide(value)),
        );
    }
    builder.build()
}

fn key(sequence: u64, member: u8) -> Vec<u8> {
    let mut key = (sequence as u32).to_be_bytes().to_vec();
    key.extend_from_slice(&u32::from(member).to_be_bytes());
    key
}

fn value(sequence: u64, member: u8) -> Vec<u8> {
    let mut value = vec![(sequence % 251) as u8; VALUE];
    value[..8].copy_from_slice(&sequence.to_be_bytes());
    value[8] = member;
    value
}

fn decode(value: &[u8]) -> (u64, u8) {
    let mut sequence = [0; 8];
    sequence.copy_from_slice(&value[..8]);
    (u64::from_be_bytes(sequence), value[8])
}

/// The serving thread holds the owner host until shutdown.
struct Owner {
    stop: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        // Release pairs with the owner service's shutdown check.
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

struct Fixture {
    config: RemoteDatabaseConfig,
    shape: KeyShape,
    node_roots: Vec<PathBuf>,
    _owners: Vec<Owner>,
    // Dropped last: owner WAL files and the authority directory live here.
    root: tempdir::TempDir,
}

impl Fixture {
    fn new(owners: u32, shape: KeyShape) -> Self {
        let root = tempdir::TempDir::new("r1-relocation").unwrap();
        let tls = TlsFiles {
            transport: Transport::TcpTrusted,
            ca: PathBuf::new(),
            cert: PathBuf::new(),
            key: PathBuf::new(),
            server_name: "trusted-test-cluster".into(),
        };
        let listeners: Vec<TcpListener> = (0..owners)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let mut config = RemoteDatabaseConfig {
            storage: LocalInventoryConfig {
                database: [0; 16],
                partitions: (0..owners).collect(),
                batch_limits: BatchLimits {
                    max_operations: 32,
                    max_encoded_bytes: 64 * KIB as usize,
                },
                group_limits: GroupLimits {
                    max_batches: 32,
                    max_encoded_bytes: 256 * KIB as usize,
                },
                fragment_bytes: FRAGMENT,
                wal_file_bytes: FILE,
                max_batches_per_partition: 8192,
                max_reference_metadata_bytes: 64 * 1024 * KIB as usize,
            },
            endpoints: listeners
                .iter()
                .zip(0..owners)
                .map(|(listener, partition)| RemoteEndpoint {
                    partition,
                    address: listener.local_addr().unwrap().to_string(),
                    server_name: tls.server_name.clone(),
                })
                .collect(),
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
            live_nvme: LiveClientConfig {
                lease_ahead: true,
                polled_writes: false,
                host_nqn: "nqn.2026-01.test:r1-relocation".into(),
                host_id: [5; 16],
                owner_identities: Default::default(),
                leased_write_window: Some(4),
                leased_sync: LeasedSync::PerFrameFua,
                leased_flush_min_frame_bytes: 0,
                max_inflight_requests: 32,
                max_inflight_bytes: 4 * 1024 * KIB as usize,
                connect_timeout_ms: 10000,
                io_timeout_ms: 30000,
                leased_durability: DurabilityTier::Native,
                inline_reads: true,
                queue_geometry: Default::default(),
            },
            durable_cut: Default::default(),
            index: Default::default(),
        };
        let mut node_roots = Vec::new();
        let mut started = Vec::new();
        for (partition, listener) in (0..owners).zip(listeners) {
            let host_config =
                trusted_owner_host_config(root.path().join(format!("owner-{partition}")));
            node_roots.push(host_config.root.clone());
            let host = open_live_fixture_host(host_config).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let serving = host;
            let stopping = Arc::clone(&stop);
            let tls = config.tls.clone();
            let server = thread::spawn(move || {
                serving
                    .serve(listener, tls, TcpLimits::default(), stopping)
                    .unwrap()
            });
            started.push(Owner {
                stop,
                server: Some(server),
            });
        }
        authority_lifecycle::provision_for_test(
            &root.path().join("authority"),
            &shape,
            &mut config,
        )
        .unwrap();
        let database =
            crate::distributed::tcp::host::identity::database_hex(&config.storage.database);
        let directory = format!("db-{database}");
        for path in &mut node_roots {
            path.push(&directory);
        }
        Self {
            config,
            shape,
            node_roots,
            _owners: started,
            root,
        }
    }

    /// Every open, first or reopen, selects the ordinary-file live fixture
    /// before open-time live recovery replays the WAL suffix.
    fn open(&self) -> Arc<Db> {
        Db::open_remote_local_fixture(
            &self.root.path().join("authority"),
            self.shape.clone(),
            self.config.clone(),
        )
        .unwrap()
    }

    /// Does WAL file `index` of `(partition, generation)` exist on its owner?
    fn wal_file_exists(&self, partition: u32, generation: u64, index: u64) -> bool {
        self.node_roots[partition as usize]
            .join(format!("epoch_{generation:016x}"))
            .join(format!("wal_{index:016x}"))
            .exists()
    }
}

/// Commit one batch whose member `m` is `key(sequence, m)` in `keyspaces[m]`.
fn write(db: &Arc<Db>, sequence: u64, keyspaces: &[KeySpace]) {
    let mut batch = db.write_batch();
    for (member, keyspace) in (0u8..).zip(keyspaces) {
        batch.write(*keyspace, key(sequence, member), value(sequence, member));
    }
    batch.commit().unwrap();
}

/// Owner, generation and frame start of the batch that holds member 0 of
/// `sequence`.
///
/// The relocation scan reports `batch.address.offset`, the frame start. A
/// hybrid position names the member frame: `VersionPosition::member` adds the
/// CRC header and the member's descriptor offset. The encoded batch starts with
/// its fixed header (`codec::HEADER_LEN`) and the first member follows it, so
/// member 0 has descriptor offset `HEADER_LEN`, not 0 (`encode_validated`
/// takes each offset from `encoded.len()`). The frame start is the member
/// offset minus the CRC header minus `HEADER_LEN`.
#[derive(Clone, Copy)]
struct BatchAddress {
    partition: u32,
    segment_generation: u64,
    offset: u64,
}

/// The owner address of the batch that holds member 0 of `sequence`.
fn address(db: &Db, keyspace: KeySpace, sequence: u64) -> BatchAddress {
    let table = db.remote_for_test().hybrid_table();
    let member = table
        .get(keyspace, &key(sequence, 0), table.current())
        .unwrap()
        .expect("the written batch is indexed");
    assert_eq!(member.operation_index, 0, "member 0 was requested");
    BatchAddress {
        partition: member.address.partition,
        segment_generation: member.address.segment_generation,
        offset: member.address.offset
            - crate::crc::CrcFrame::CRC_HEADER_LENGTH as u64
            - crate::distributed::codec::HEADER_LEN as u64,
    }
}

fn observe(db: &Db) -> RelocationObservation {
    db.start_relocation_for_test(Instant::now() + Duration::from_secs(120))
        .unwrap()
        .0
}

/// `(keyspace, sequence, member, decision)` of each callback, in order.
fn callbacks(observation: &RelocationObservation) -> Vec<(KeySpace, u64, u8, Decision)> {
    observation
        .callbacks
        .iter()
        .map(|callback| {
            let (sequence, member) = decode(&callback.value);
            assert_eq!(callback.key.as_ref(), key(sequence, member));
            (callback.keyspace, sequence, member, callback.decision)
        })
        .collect()
}

/// Expected callbacks of single-keyspace two-member batches `0..stop`, then
/// the Stop batch up to its Stop member.
fn expected_callbacks(
    keyspace: KeySpace,
    stop: (u64, u8),
    remove_below: u64,
) -> Vec<(KeySpace, u64, u8, Decision)> {
    let mut expected = Vec::new();
    for sequence in 0..stop.0 {
        for member in 0..2 {
            let decision = if sequence < remove_below {
                Decision::Remove
            } else {
                Decision::Keep
            };
            expected.push((keyspace, sequence, member, decision));
        }
    }
    for member in 0..stop.1 {
        expected.push((keyspace, stop.0, member, Decision::Remove));
    }
    expected.push((keyspace, stop.0, stop.1, Decision::StopRelocation));
    expected
}

fn exists(db: &Db, keyspace: KeySpace, sequence: u64, member: u8) -> bool {
    db.exists(keyspace, &key(sequence, member)).unwrap()
}

/// Every `(sequence, member)` of `0..writes` two-member batches still visible.
fn visible(db: &Db, keyspace: KeySpace, writes: u64) -> Vec<(u64, u8)> {
    (0..writes)
        .flat_map(|sequence| (0..2).map(move |member| (sequence, member)))
        .filter(|&(sequence, member)| exists(db, keyspace, sequence, member))
        .collect()
}

/// Callbacks follow the global logical order across owners, and the first
/// Stop ends them all: later members of its batch (in an earlier-visited
/// keyspace) and later batches of the lower owner are never called.
#[test]
fn callbacks_follow_global_version_order_across_owners_and_end_at_first_stop() {
    let policy = Policy::new();
    let fixture = Fixture::new(2, shape(&policy, &["a", "b"]));
    let db = fixture.open();
    let (a, b) = (db.ks("a"), db.ks("b"));
    const WRITES: u64 = 48;
    for sequence in 0..WRITES {
        // Member 0 in the later-visited keyspace, member 1 in the earlier.
        write(&db, sequence, &[b, a]);
    }
    // Paper 3.1/4.4: the pass scans the durable processed prefix; make the
    // writes durable before it runs.
    db.flush().unwrap();
    let partitions: Vec<u32> = (0..WRITES)
        .map(|sequence| address(&db, b, sequence).partition)
        .collect();
    let stop = (WRITES / 2..WRITES)
        .find(|&sequence| partitions[sequence as usize] == 1)
        .expect("a batch of the second owner in the second half");
    assert!(
        partitions[..stop as usize].contains(&0) && partitions[..stop as usize].contains(&1),
        "both owners must hold batches before the Stop"
    );
    assert!(
        partitions[stop as usize + 1..].contains(&0),
        "the lower owner must hold a batch after the Stop"
    );
    policy.set(Some((stop, 0)), 0);
    let observation = observe(&db);
    let mut expected = Vec::new();
    for sequence in 0..stop {
        expected.push((b, sequence, 0, Decision::Keep));
        expected.push((a, sequence, 1, Decision::Keep));
    }
    expected.push((b, stop, 0, Decision::StopRelocation));
    assert_eq!(callbacks(&observation), expected);
    let generation = address(&db, b, 0).segment_generation;
    assert_eq!(
        observation
            .streams
            .iter()
            .map(|stream| (
                stream.stream_id.partition,
                stream.stream_id.fencing_generation
            ))
            .collect::<Vec<_>>(),
        [(0, generation), (1, generation)]
    );
    assert_eq!(observation.copied_survivors, 0);
    for sequence in 0..WRITES {
        assert!(exists(&db, b, sequence, 0) && exists(&db, a, sequence, 1));
    }
    db.wait_for_background_threads_to_finish().unwrap();
}

/// One single-owner database after its first observed pass. The policy
/// stays alive inside the shape's filters.
struct StopCase {
    fixture: Fixture,
    db: Arc<Db>,
    offsets: Vec<u64>,
    stop: Option<u64>,
    observation: RelocationObservation,
}

/// Writes `writes` two-member batches to a fresh single-owner database, then
/// runs one observed pass with the Stop at member 1 of the batch `choose`
/// selects from the batch offsets (none: no Stop). Sequence 0 is removed.
fn stop_case(writes: u64, choose: impl FnOnce(&[u64]) -> Option<u64>) -> StopCase {
    let policy = Policy::new();
    let fixture = Fixture::new(1, shape(&policy, &["a"]));
    let db = fixture.open();
    let a = db.ks("a");
    for sequence in 0..writes {
        write(&db, sequence, &[a, a]);
    }
    let offsets: Vec<u64> = (0..writes)
        .map(|sequence| address(&db, a, sequence).offset)
        .collect();
    let stop = choose(&offsets);
    policy.set(stop.map(|sequence| (sequence, 1)), 1);
    // Paper 3.1: the owner fsync runs in the background, so a committed batch
    // is readable before it is durable. A relocation pass scans the processed
    // prefix `Q` = `min(durable cut, index-applied cut)` (paper 4.4: relocation
    // reclaims processed bytes only), so the fixture makes every write durable
    // before the observed pass.
    db.flush().unwrap();
    let observation = observe(&db);
    StopCase {
        fixture,
        db,
        offsets,
        stop,
        observation,
    }
}

/// The batch whose group opens WAL file 1: its Stop is at the file boundary.
fn first_in_second_file(offsets: &[u64]) -> u64 {
    offsets.iter().position(|&offset| offset >= FILE).unwrap() as u64
}

/// Design §7 fixtures (a)–(d) against native's whole-file gate: below one
/// complete file the pass is `NoWork` (filters called, nothing removed);
/// from a Stop at or after the first file boundary it removes the older
/// `Remove` rows and keeps every member of the Stop batch.
#[test]
fn stop_semantics_follow_the_native_whole_file_gate() {
    const WRITES: u64 = 130;
    // (a) Stop in the last batch of the first file.
    let StopCase {
        fixture: _a,
        db,
        offsets,
        stop,
        observation,
    } = stop_case(WRITES, |offsets| Some(first_in_second_file(offsets) - 1));
    let stop = stop.unwrap();
    assert!(offsets[stop as usize] < FILE);
    assert_eq!(observation.gate, crate::RelocationGate::NoWork);
    assert_eq!(observation.streams.len(), 1);
    assert_eq!(
        observation.streams[0].terminal_position,
        offsets[stop as usize]
    );
    assert_eq!(observation.streams[0].rounded_target, 0);
    assert_eq!(
        callbacks(&observation),
        expected_callbacks(db.ks("a"), (stop, 1), 1)
    );
    assert_eq!(visible(&db, db.ks("a"), WRITES).len(), 2 * WRITES as usize);
    db.wait_for_background_threads_to_finish().unwrap();

    // (b) No Stop; the processed bound is inside the first file.
    let StopCase {
        fixture: _b,
        db,
        offsets,
        observation,
        ..
    } = stop_case(10, |_| None);
    assert!(offsets.iter().all(|&offset| offset < FILE));
    assert_eq!(observation.gate, crate::RelocationGate::NoWork);
    assert!(observation.streams[0].rounded_target < FILE);
    assert_eq!(observation.callbacks.len(), 20);
    assert_eq!(visible(&db, db.ks("a"), 10).len(), 20);
    db.wait_for_background_threads_to_finish().unwrap();

    // (c) Stop exactly at the next-file boundary; (d) just after it.
    for after in [0, 1] {
        let StopCase {
            fixture: _c,
            db,
            offsets,
            stop,
            observation,
        } = stop_case(WRITES, |offsets| {
            Some(first_in_second_file(offsets) + after)
        });
        let stop = stop.unwrap();
        let a = db.ks("a");
        assert_eq!(
            observation.gate,
            crate::RelocationGate::DidWork,
            "after {after}"
        );
        assert_eq!(
            observation.streams[0].terminal_position,
            offsets[stop as usize]
        );
        assert_eq!(observation.streams[0].rounded_target, FILE);
        assert_eq!(callbacks(&observation), expected_callbacks(a, (stop, 1), 1));
        assert!(!exists(&db, a, 0, 0) && !exists(&db, a, 0, 1));
        // The Stop batch survives whole, although its member 0 was Remove.
        assert!(exists(&db, a, stop, 0) && exists(&db, a, stop, 1));
        assert_eq!(visible(&db, a, WRITES).len(), 2 * WRITES as usize - 2);
        db.wait_for_background_threads_to_finish().unwrap();
    }
}

/// A crash after the relocation capture is durable (the hybrid region store
/// wrote the dropped entries and the manifest raised the callback start) but
/// before physical pruning: reopen keeps the removals and the new start, and
/// the next pass starts at the file the Stop is in, without calling the
/// filters for older files.
///
/// The crash is injected at the prune boundary of the pass
/// (`relocation_prune_interruption`), the hybrid analogue of the removed Exact
/// checkpoint interruption: the observed pass returns the error instead of
/// retiring files. A production pass fail-stops the authority through
/// `background_relocation_pass`; the observed slot only reports the error.
#[test]
fn crash_after_start_persist_before_prune_keeps_removals_and_start() {
    const WRITES: u64 = 130;
    let policy = Policy::new();
    let fixture = Fixture::new(1, shape(&policy, &["a"]));
    let db = fixture.open();
    let a = db.ks("a");
    for sequence in 0..WRITES {
        write(&db, sequence, &[a, a]);
    }
    let offsets: Vec<u64> = (0..WRITES)
        .map(|sequence| address(&db, a, sequence).offset)
        .collect();
    let generation = address(&db, a, 0).segment_generation;
    let stop = first_in_second_file(&offsets);
    policy.set(Some((stop, 1)), 1);
    db.flush().unwrap();
    db.remote_for_test()
        .write_core()
        .test_hooks
        .lock()
        .relocation_prune_interruption = true;
    assert!(matches!(
        db.start_relocation_for_test(Instant::now() + Duration::from_secs(120)),
        Err(RelocationTestError::Native(_))
    ));
    // No physical pruning ran.
    assert!(fixture.wal_file_exists(0, generation, 0));
    drop(db);

    let db = fixture.open();
    assert!(!exists(&db, a, 0, 0) && !exists(&db, a, 0, 1));
    assert_eq!(visible(&db, a, WRITES).len(), 2 * WRITES as usize - 2);
    let observation = observe(&db);
    assert_eq!(observation.gate, crate::RelocationGate::NoWork);
    assert_eq!(
        callbacks(&observation),
        vec![
            (a, stop, 0, Decision::Remove),
            (a, stop, 1, Decision::StopRelocation)
        ]
    );
    assert_eq!(visible(&db, a, WRITES).len(), 2 * WRITES as usize - 2);
    db.wait_for_background_threads_to_finish().unwrap();
}

/// After a completed pass, reopening (a new fencing generation) and passing
/// again is idempotent: the persisted callback start skips the pruned file,
/// nothing reappears, and repeated passes observe the same callbacks.
#[test]
fn restart_after_completed_pass_is_idempotent() {
    const WRITES: u64 = 130;
    let StopCase {
        fixture,
        db,
        stop,
        observation,
        ..
    } = stop_case(WRITES, |offsets| Some(first_in_second_file(offsets)));
    let stop = stop.unwrap();
    let a = db.ks("a");
    assert_eq!(observation.gate, crate::RelocationGate::DidWork);
    let generation = address(&db, a, 1).segment_generation;
    let before = visible(&db, a, WRITES);
    db.wait_for_background_threads_to_finish().unwrap();

    let db = fixture.open();
    assert_eq!(visible(&db, a, WRITES), before);
    let expected = vec![
        (a, stop, 0, Decision::Remove),
        (a, stop, 1, Decision::StopRelocation),
    ];
    for _ in 0..2 {
        let observation = observe(&db);
        assert_eq!(observation.gate, crate::RelocationGate::NoWork);
        assert_eq!(callbacks(&observation), expected);
        // The older fencing generation is scanned from its adopted start.
        let old = observation
            .streams
            .iter()
            .find(|stream| stream.stream_id.fencing_generation == generation)
            .unwrap();
        assert_eq!(old.rounded_target, FILE);
        assert_eq!(visible(&db, a, WRITES), before);
    }
    db.wait_for_background_threads_to_finish().unwrap();
    drop(fixture);
}

/// Native index state before the pass, set through native's own forced flush
/// (`force_rebuild_control_region`) so the oracle never depends on when the
/// background flusher ran or whether it promoted the cell. The write path
/// queues no flush because `max_dirty_keys` exceeds the row count, so the
/// forced rebuild's flush alone decides the level state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeIndexState {
    /// Every entry flushed into L0; `l0_max_entries` exceeds the row count.
    FlushedL0,
    /// Every entry promoted into L1: with `l0_max_entries` 8 the forced flush
    /// promotes the cell.
    PromotedL1,
}

/// Filter decisions below the Stop of the one-owner parity oracle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Decisions {
    /// Sui's shape: every put before the Stop is `Remove` (Sui filters
    /// return only `Remove` or `StopRelocation`).
    RemoveOnly,
    /// The first half before the Stop is `Remove`, the rest `Keep`. Fixed
    /// native copies the Keep rows below its target to the WAL head; R1 keeps
    /// them, and their files, in place.
    KeepAfterRemoves,
}

fn native_config(state: NativeIndexState, writes: u64) -> Arc<crate::config::Config> {
    let mut config = crate::config::Config::small();
    config.frag_size = FRAGMENT;
    config.wal_file_size = FILE;
    config.relocation_max_reclaim_pct = 100;
    config.snapshot_written_bytes = u64::MAX;
    // The write path queues a background flush when a cell holds more dirty
    // rows than `max_dirty_keys` (`LargeTable::too_many_dirty`). Keep the
    // threshold above every row this test writes (two members per sequence)
    // so that only `force_rebuild_control_region` flushes, and therefore only
    // it decides the level state. Otherwise a background flush can promote
    // the cell, and a later forced flush of at most `l0_max_entries` rows
    // writes a new L0 above that L1 (`IndexFlusherThread::handle_command`,
    // the L0-or-promote decision) and leaves `[L0, L1]`, which is not a state
    // this oracle describes.
    let rows = writes.saturating_mul(2);
    config.max_dirty_keys = usize::try_from(rows.saturating_mul(4)).unwrap_or(usize::MAX / 16);
    config.l0_max_entries = Some(match state {
        NativeIndexState::FlushedL0 => 4096,
        NativeIndexState::PromotedL1 => 8,
    });
    Arc::new(config)
}

fn open_native(root: &Path, policy: &Arc<Policy>, config: &Arc<crate::config::Config>) -> Arc<Db> {
    Db::open_native(
        root,
        shape(policy, &["a"]),
        Arc::clone(config),
        Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
    .unwrap()
}

/// Native WAL offset of the record `(sequence, member)` from native's index.
fn native_position(db: &Db, keyspace: KeySpace, sequence: u64, member: u8) -> u64 {
    let native = db.native().unwrap();
    match native
        .large_table
        .get(
            native.ks_context(keyspace),
            &key(sequence, member),
            native.as_ref(),
        )
        .unwrap()
    {
        crate::large_table::GetResult::WalPosition(position, _) => position.offset(),
        _ => panic!("native index holds no WAL position for ({sequence}, {member})"),
    }
}

/// `(has L0, has L1)` of the native cell holding every row of `keyspace`.
fn native_levels(db: &Db, keyspace: KeySpace) -> (bool, bool) {
    let native = db.native().unwrap();
    let context = native.ks_context(keyspace);
    let cell = context.ks_config.cell_id(&key(0, 0));
    native
        .large_table
        .with_entry_for_test(context, &cell, |entry| {
            (entry.levels().l0().is_some(), entry.levels().l1().is_some())
        })
}

/// Every `(sequence, member)` of `0..writes` two-member batches whose value a
/// read returns.
fn readable(db: &Db, keyspace: KeySpace, writes: u64) -> Vec<(u64, u8)> {
    (0..writes)
        .flat_map(|sequence| (0..2).map(move |member| (sequence, member)))
        .filter(|&(sequence, member)| {
            db.get(keyspace, &key(sequence, member))
                .unwrap()
                .is_some_and(|read| read.as_ref() == value(sequence, member))
        })
        .collect()
}

/// Both visibility views (`exists` and value reads) on both arms equal the
/// expected surviving rows.
fn assert_state(
    case: &str,
    (leased, native): (&Db, &Db),
    keyspace: KeySpace,
    writes: u64,
    expected: &[(u64, u8)],
) {
    for (arm, db) in [("leased", leased), ("native", native)] {
        assert_eq!(
            visible(db, keyspace, writes),
            expected,
            "{case}: {arm} exists"
        );
        assert_eq!(
            readable(db, keyspace, writes),
            expected,
            "{case}: {arm} get"
        );
    }
}

/// One owner with native's geometry against fixed native Tidehunter, for each
/// forced native index state and both decision shapes: equal gates, equal
/// ordered callbacks (keyspace, key, value, decision), equal whole-file
/// targets, each arm's terminal at its own Stop position below its own
/// processed upper, and equal visible state (`exists` and value reads) right
/// after the pass and after a reopen of both arms. Byte positions differ by
/// WAL format, so terminals and uppers are checked per arm, not across.
///
/// `copied_survivors` is compared per arm; byte positions differ by WAL
/// format. Native writes a relocated batch to the WAL head; the leased pass
/// writes the copy to the serving generation and re-points the entry (module
/// doc step 4).
///
/// Under D24 both arms keep upstream's rule: for a filtered keyspace WAL
/// relocation copies nothing and drops everything below its target, so
/// `copied_survivors` is 0 on both arms. This test still asserts the fork's
/// Keep-copy parity (native copied the Keep rows below its target); the hybrid
/// D24 task rewrites it for the upstream rule and re-enables it.
#[ignore = "rewrite for D24 upstream rule: hybrid relocation will match native (no Keep copies)"]
#[test]
fn one_owner_matches_fixed_native_gate_callbacks_frontiers_and_state() {
    const WRITES: u64 = 130;
    for state in [NativeIndexState::FlushedL0, NativeIndexState::PromotedL1] {
        for decisions in [Decisions::RemoveOnly, Decisions::KeepAfterRemoves] {
            let case = format!("{state:?} {decisions:?}");
            let leased_policy = Policy::new();
            let fixture = Fixture::new(1, shape(&leased_policy, &["a"]));
            let leased = fixture.open();
            let native_policy = Policy::new();
            let native_root = tempdir::TempDir::new("r1-native-parity").unwrap();
            let config = native_config(state, WRITES);
            let native = open_native(native_root.path(), &native_policy, &config);
            let a = leased.ks("a");
            for db in [&leased, &native] {
                for sequence in 0..WRITES {
                    write(db, sequence, &[a, a]);
                }
            }
            // Paper 3.1/4.4: the leased pass scans the durable processed
            // prefix; native applies the index synchronously, the leased owner
            // fsync runs in the background.
            leased.flush().unwrap();
            native
                .native()
                .unwrap()
                .force_rebuild_control_region()
                .unwrap();
            assert_eq!(
                native_levels(&native, a),
                match state {
                    NativeIndexState::FlushedL0 => (true, false),
                    NativeIndexState::PromotedL1 => (false, true),
                },
                "{case}: forced native index state"
            );
            // Positions come from each arm's index: a pass without a Stop
            // would already reclaim on both arms (clean tail at the
            // processed upper), so no probe pass runs.
            let leased_offset = |sequence: u64| address(&leased, a, sequence).offset;
            let stop = (0..WRITES)
                .find(|&sequence| {
                    leased_offset(sequence) >= FILE
                        && native_position(&native, a, sequence, 0) >= FILE
                })
                .unwrap();
            let remove_below = match decisions {
                Decisions::RemoveOnly => stop,
                Decisions::KeepAfterRemoves => stop / 2,
            };
            // With Keep rows, every Remove lies in WAL file 0 on both arms,
            // below both targets: fixed native's Keep branch cuts at its
            // target and leaves Removes between target and terminal for a
            // later pass, while R1 removes every Remove before the Stop.
            if decisions == Decisions::KeepAfterRemoves {
                let last_remove = remove_below - 1;
                assert!(
                    leased_offset(last_remove) < FILE
                        && native_position(&native, a, last_remove, 1) < FILE,
                    "{case}"
                );
            }
            for policy in [&leased_policy, &native_policy] {
                policy.set(Some((stop, 0)), remove_below);
            }
            let native_stop = native_position(&native, a, stop, 0);
            let native_copies = (remove_below..stop)
                .flat_map(|sequence| (0..2).map(move |member| (sequence, member)))
                .filter(|&(sequence, member)| native_position(&native, a, sequence, member) < FILE)
                .count() as u64;
            // Byte positions differ by WAL format, so each arm copies the Keep
            // rows below ITS OWN file target; the counts are checked per arm.
            let leased_copies = (remove_below..stop)
                .flat_map(|sequence| (0..2).map(move |member| (sequence, member)))
                .filter(|&(sequence, _)| leased_offset(sequence) < FILE)
                .count() as u64;
            let leased_pass = observe(&leased);
            let native_pass = observe(&native);
            assert_eq!(leased_pass.gate, crate::RelocationGate::DidWork, "{case}");
            assert_eq!(native_pass.gate, leased_pass.gate, "{case}");
            let rows = |observation: &RelocationObservation| -> Vec<_> {
                observation
                    .callbacks
                    .iter()
                    .map(|callback| {
                        (
                            callback.keyspace,
                            callback.key.to_vec(),
                            callback.value.to_vec(),
                            callback.decision,
                        )
                    })
                    .collect()
            };
            assert_eq!(rows(&leased_pass), rows(&native_pass), "{case}");
            assert_eq!(
                callbacks(&leased_pass),
                expected_callbacks(a, (stop, 0), remove_below),
                "{case}"
            );
            for (arm, pass, terminal) in [
                ("leased", &leased_pass, leased_offset(stop)),
                ("native", &native_pass, native_stop),
            ] {
                assert_eq!(pass.streams.len(), 1, "{case} {arm}");
                let stream = &pass.streams[0];
                assert_eq!(stream.terminal_position, terminal, "{case} {arm}");
                assert!(
                    stream.terminal_position < stream.processed_upper,
                    "{case} {arm}"
                );
                assert_eq!(stream.rounded_target, FILE, "{case} {arm}");
            }
            assert_eq!(leased_pass.copied_survivors, leased_copies, "{case}");
            assert_eq!(native_pass.copied_survivors, native_copies, "{case}");
            if decisions == Decisions::KeepAfterRemoves {
                assert!(native_copies > 0, "{case}: the case must copy on native");
            }
            let expected: Vec<(u64, u8)> = (remove_below..WRITES)
                .flat_map(|sequence| (0..2).map(move |member| (sequence, member)))
                .collect();
            let arms = (leased.as_ref(), native.as_ref());
            assert_state(
                &format!("{case}, after the pass"),
                arms,
                a,
                WRITES,
                &expected,
            );
            leased.wait_for_background_threads_to_finish().unwrap();
            native.wait_for_background_threads_to_finish().unwrap();
            let leased = fixture.open();
            let native = open_native(native_root.path(), &native_policy, &config);
            let arms = (leased.as_ref(), native.as_ref());
            assert_state(&format!("{case}, after reopen"), arms, a, WRITES, &expected);
            leased.wait_for_background_threads_to_finish().unwrap();
            native.wait_for_background_threads_to_finish().unwrap();
        }
    }
}

/// The test slot does not coalesce: a second named pass while one runs is
/// `Busy`, an ordinary start is admitted and coalesced, and the parked pass
/// completes after release.
#[test]
fn observed_pass_slot_is_busy_while_occupied() {
    let policy = Policy::new();
    let fixture = Fixture::new(1, shape(&policy, &["a"]));
    let db = fixture.open();
    let a = db.ks("a");
    for sequence in 0..4 {
        write(&db, sequence, &[a, a]);
    }
    // Paper 3.1/4.4: the pass scans the durable processed prefix.
    db.flush().unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    *policy.pause.lock() = Some((entered, gate));
    let parked = {
        let db = Arc::clone(&db);
        thread::spawn(move || observe(&db))
    };
    waiting.recv_timeout(Duration::from_secs(60)).unwrap();
    assert!(matches!(
        db.start_relocation_for_test(Instant::now() + Duration::from_secs(1)),
        Err(RelocationTestError::Busy)
    ));
    db.start_relocation().unwrap();
    db.start_relocation().unwrap();
    resume.send(()).unwrap();
    let observation = parked.join().unwrap();
    assert_eq!(observation.gate, crate::RelocationGate::NoWork);
    assert_eq!(observation.callbacks.len(), 8);
    // The coalesced start ran or is queued; a new named pass is admitted.
    assert_eq!(observe(&db).callbacks.len(), 8);
    db.wait_for_background_threads_to_finish().unwrap();
    drop(fixture);
}

/// Rewrites both members of `sequence` with a value that still decodes as
/// `(sequence, member)` but carries `marker` in its filler bytes.
fn rewrite(db: &Arc<Db>, keyspace: KeySpace, sequence: u64, marker: u8) {
    let mut batch = db.write_batch();
    for member in 0..2u8 {
        let mut value = value(sequence, member);
        value[9] = marker;
        batch.write(keyspace, key(sequence, member), value);
    }
    batch.commit().unwrap();
}

/// A write admitted after the scan cut and before the unread drop survives
/// it: the drop is bounded by each stream's terminal, which the scan froze at
/// or below the durable end, so the newer position lies above it (paper 4.4:
/// relocation reclaims processed bytes). The scanned `Remove` rows below the
/// terminal are gone; the rewrite keeps its value.
#[test]
fn a_rewrite_after_the_scan_cut_survives_the_unread_drop() {
    const WRITES: u64 = 130;
    const MARKER: u8 = 0xA5;
    let policy = Policy::new();
    let fixture = Fixture::new(1, shape(&policy, &["a"]));
    let db = fixture.open();
    let a = db.ks("a");
    for sequence in 0..WRITES {
        write(&db, sequence, &[a, a]);
    }
    let offsets: Vec<u64> = (0..WRITES)
        .map(|sequence| address(&db, a, sequence).offset)
        .collect();
    let stop = first_in_second_file(&offsets);
    // RemoveOnly up to the Stop: every scanned record before it is Remove and
    // the Stop member ends the scan before any Keep is seen, so the pass takes
    // the unread-drop path.
    policy.set(Some((stop, 1)), stop);
    db.flush().unwrap();
    let (entered, waiting) = mpsc::sync_channel(1);
    let (resume, gate) = mpsc::sync_channel(1);
    *policy.pause.lock() = Some((entered, gate));
    let pass = {
        let db = Arc::clone(&db);
        thread::spawn(move || observe(&db))
    };
    waiting.recv_timeout(Duration::from_secs(60)).unwrap();
    // Sequence 0 was already scanned as Remove; the rewrite lands above the
    // frozen scan cut while the pass is parked at its first filter call.
    rewrite(&db, a, 0, MARKER);
    resume.send(()).unwrap();
    let observation = pass.join().unwrap();
    assert_eq!(observation.gate, crate::RelocationGate::DidWork);
    // The scanned Remove rows below the terminal are dropped...
    assert!(!exists(&db, a, 1, 0), "a scanned Remove row survives");
    assert!(!exists(&db, a, 1, 1), "a scanned Remove row survives");
    // ...and the rewrite above the scan cut keeps its value.
    let read = db
        .get(a, &key(0, 0))
        .unwrap()
        .expect("the rewrite is readable");
    let mut expected = value(0, 0);
    expected[9] = MARKER;
    assert_eq!(
        read.as_ref(),
        expected.as_slice(),
        "the rewrite was dropped"
    );
    db.wait_for_background_threads_to_finish().unwrap();
    drop(fixture);
}
