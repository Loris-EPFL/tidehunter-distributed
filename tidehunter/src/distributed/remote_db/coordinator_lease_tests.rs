//! Coordinator lease lapse end to end over two fixture owners: the renewer of
//! one owner stops, the coordinator fail-stops the database, the owner
//! write-protects that epoch's exports, and its write grants are refused.
//!
//! Every wait follows an observable event (the fail-stop flag, the fixture
//! write-protect attribute) under a bounded cap; nothing here is a sleep or a
//! latency premise.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use super::super::pipeline_tests::Fixture;
use super::super::{ConcurrentRemoteDatabase, RemoteDatabase, RemoteDatabaseConfig};
use super::SendHook;
use crate::distributed::codec::Mutation;
use crate::distributed::tcp::Client;
use crate::distributed::test_bounds::PROGRESS_CAP;
use crate::distributed::types::BatchId;
use crate::key_shape::{KeyShape, KeySpace, KeyType};

/// One operation id the coordinator routes to `partition`.
fn operation(
    config: &RemoteDatabaseConfig,
    database: [u8; 16],
    partition: u32,
    seed: u64,
) -> [u8; 16] {
    (seed * 1024..seed * 1024 + 1024)
        .map(|number| {
            let mut operation = [0u8; 16];
            operation[..8].copy_from_slice(&number.to_le_bytes());
            operation
        })
        .find(|operation| {
            config
                .inventory(1)
                .partition_for(BatchId {
                    database,
                    operation: *operation,
                })
                .expect("the fixture inventory routes every batch id")
                == partition
        })
        .expect("one operation id of a thousand routes to the partition")
}

fn put(key: u32, value: u8) -> Mutation {
    Mutation::Put {
        keyspace: KeySpace(0),
        key: key.to_be_bytes().to_vec().into(),
        value: vec![value; 32].into(),
    }
}

/// Count the namespaces of `configfs` whose fixture `write_protect` is set.
fn protected_namespaces(configfs: &Path) -> usize {
    let Ok(subsystems) = std::fs::read_dir(configfs.join("subsystems")) else {
        return 0;
    };
    let mut protected = 0;
    for subsystem in subsystems.flatten() {
        let Ok(namespaces) = std::fs::read_dir(subsystem.path().join("namespaces")) else {
            continue;
        };
        for namespace in namespaces.flatten() {
            let attribute = namespace.path().join("write_protect");
            if std::fs::read_to_string(&attribute).is_ok_and(|value| value.trim() == "1") {
                protected += 1;
            }
        }
    }
    protected
}

/// Wait for `ready` under the shared cap. The test passes because the event
/// happened, never because time passed.
fn wait_until(deadline: Instant, what: &str, mut ready: impl FnMut() -> bool) {
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::yield_now();
    }
}

#[test]
fn lease_lapse_fail_stops_the_database_and_write_protects_the_owner() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("coordinator-lease-lapse").expect("a temp directory"),
        2,
        64,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |config| config.coordinator_lease_ms = 1_000,
        |_| {},
    );
    // One switch per owner: `false` drops that owner's renewal requests.
    let renewals: Arc<Vec<AtomicBool>> = Arc::new((0..2).map(|_| AtomicBool::new(true)).collect());
    let switched = renewals.clone();
    let hook: SendHook = Arc::new(move |owner| switched[owner].load(Ordering::Acquire));
    let path = fixture.temp.path().join("authority");
    let db = RemoteDatabase::open_local_live_fixture_with_lease_hook(
        &path,
        &fixture.shape,
        fixture.config.clone(),
        hook,
    )
    .expect("the database opens once every first grant succeeds");
    let database = db.database_id();
    for partition in 0..2u32 {
        db.write_batch(
            operation(
                &fixture.config,
                database,
                partition,
                7 + u64::from(partition),
            ),
            vec![put(partition, 1)],
        )
        .expect("a write before the lapse lands");
    }
    let deadline = Instant::now() + PROGRESS_CAP;
    assert!(
        !db.core.concurrent_failure.load(Ordering::Acquire),
        "the database is healthy while every owner renews"
    );
    renewals[1].store(false, Ordering::Release);
    wait_until(deadline, "the lease of partition 1 to lapse", || {
        db.core.concurrent_failure.load(Ordering::Acquire)
    });
    let error = db
        .write_batch(operation(&fixture.config, database, 1, 31), vec![put(3, 2)])
        .expect_err("a write after the lapse must fail-stop");
    assert!(
        error
            .to_string()
            .contains("coordinator lease lapsed on owner partition 1"),
        "the write names the lapsed owner partition: {error}"
    );
    // The owner protects its exports on its own clock, from the last grant it
    // gave; the coordinator's deadline ends earlier by the owner margin.
    let protected_owner = fixture.host_configs[1].root.with_extension("configfs");
    let healthy_owner = fixture.host_configs[0].root.with_extension("configfs");
    wait_until(
        deadline,
        "partition 1 to write-protect its namespaces",
        || protected_namespaces(&protected_owner) > 0,
    );
    assert_eq!(
        protected_namespaces(&healthy_owner),
        0,
        "the owner of partition 0 keeps its lease and its writable exports"
    );
    // A write grant on the lapsed owner is refused before any file work.
    let endpoint = &fixture.config.endpoints[1];
    let mut tls = fixture.config.tls.clone();
    tls.server_name = endpoint.server_name.clone();
    let mut client = Client::connect(
        &endpoint.address,
        &tls,
        Duration::from_secs(10),
        fixture.config.partition_config(1, 1),
        &fixture.shape,
    )
    .expect("the lapsed owner still serves sessions");
    let error = client
        .lease_live_extent(db.epoch())
        .expect_err("a lapsed owner refuses write grants");
    assert!(
        error.to_string().contains("coordinator lease lapsed"),
        "the refused grant names the lapse: {error}"
    );
}

/// A clean shutdown that outlasts the lease never write-protects before its
/// seal. The shutdown is held at its check-point (the existing shutdown hook,
/// before the seal sweep) and the owner's renewals must continue while it is
/// held, so the owner neither lapses nor protects the epoch's exports.
#[test]
fn a_shutdown_slower_than_the_lease_keeps_the_owner_leased_until_the_seal() {
    let fixture = Fixture::new_with_root(
        tempdir::TempDir::new("coordinator-lease-slow-shutdown").expect("a temp directory"),
        2,
        64,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |config| config.coordinator_lease_ms = 1_000,
        |_| {},
    );
    let path = fixture.temp.path().join("authority");
    let db = RemoteDatabase::open_local_live_fixture(&path, &fixture.shape, fixture.config.clone())
        .expect("the database opens once every first grant succeeds");
    let database = db.database_id();
    let epoch = db.epoch();
    for partition in 0..2u32 {
        db.write_batch(
            operation(
                &fixture.config,
                database,
                partition,
                5 + u64::from(partition),
            ),
            vec![put(partition, 1)],
        )
        .expect("a write before the shutdown lands");
    }
    let (checkpointing_tx, checkpointing_rx) = std::sync::mpsc::sync_channel(1);
    let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
    db.core.test_hooks.lock().checkpoint_pause = Some((checkpointing_tx, resume_rx));
    // Drive the Sui close path: the concurrent facade's shutdown owns it.
    let concurrent = ConcurrentRemoteDatabase::new(db).expect("the concurrent facade opens");
    let shutdown = std::thread::spawn(move || concurrent.shutdown());
    checkpointing_rx
        .recv_timeout(PROGRESS_CAP)
        .expect("the shutdown never reached its check-point");
    let owner = Arc::clone(&fixture.nodes[0]);
    let configfs = fixture.host_configs[0].root.with_extension("configfs");
    let before = owner.coordinator_lease_state_for_test().0;
    let deadline = Instant::now() + PROGRESS_CAP;
    // Renewals are cadence-spaced at a fifth of the 1 s lease, so eleven of
    // them prove the held shutdown has outlasted the lease plus the margin.
    wait_until(
        deadline,
        "renewals to continue through the held shutdown",
        || owner.coordinator_lease_state_for_test().0 >= before + 11,
    );
    assert!(
        !owner.coordinator_lease_state_for_test().1,
        "the owner must not lapse while the coordinator renews"
    );
    assert_eq!(
        protected_namespaces(&configfs),
        0,
        "no export is write-protected before the Seal"
    );
    resume_tx.send(()).expect("the shutdown thread is live");
    shutdown
        .join()
        .expect("the shutdown thread panicked")
        .expect("the shutdown completes");
    assert!(
        !owner.coordinator_lease_state_for_test().1,
        "the sealed epoch is not lapsed"
    );
    assert!(
        protected_namespaces(&configfs) > 0,
        "the Seal itself write-protects the exports"
    );
    assert!(epoch > 0, "the fixture opened a fenced epoch");
}
