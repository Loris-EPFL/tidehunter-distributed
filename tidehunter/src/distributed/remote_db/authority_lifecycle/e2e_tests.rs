//! Coordinator lifecycle end to end over real owner hosts (mTLS loopback):
//! provision → write → reopen → drop → reprovision with a new identity;
//! every coordinator crash boundary of provision and drop, recovered
//! through `RemoteDatabase::open` / the drop entry; and a nested
//! `recovery_log` served beside its parent on the same owner hosts.
//!
//! A "crash" is the thread-local step hook failing at a boundary: the
//! operation stops exactly there and later steps never run, so the durable
//! image is the one of that boundary (every boundary follows one atomic
//! install, unlink + directory sync, or rename).

use super::super::pipeline_tests::Fixture;
use super::super::*;
use super::{
    AuthorityLifecycle, AuthorityStep, drop_tx, read_authority_manifest, read_lifecycle,
    set_step_hook,
};
use crate::distributed::tcp::host_identity;
use crate::key_shape::KeyType;
use std::io;

fn fixture() -> Fixture {
    Fixture::new_with_root(
        tempdir::TempDir::new("lifecycle-e2e").unwrap(),
        2,
        128,
        KeyShape::new_single(4, 1, KeyType::uniform(1)),
        2,
        |_| {},
        |_| {},
    )
}

/// The fixture configuration as a coordinator config document carries it:
/// without the provisioned identity.
fn fresh_config(fixture: &Fixture) -> RemoteDatabaseConfig {
    let mut config = fixture.config.clone();
    config.storage.database = [0; 16];
    config
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
        .map(|value| value.to_vec())
}

/// An operation of `database` routed to `partition` (as `Fixture::operation`).
fn operation(
    config: &RemoteDatabaseConfig,
    database: [u8; 16],
    partition: u32,
    seed: u64,
) -> [u8; 16] {
    let mut config = config.clone();
    config.storage.database = database;
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
                .unwrap()
                == partition
        })
        .unwrap()
}

fn open(fixture: &Fixture, path: &Path) -> Result<RemoteDatabase, LocalDbError> {
    RemoteDatabase::open_local_live_fixture(path, &fixture.shape, fresh_config(fixture))
}

/// Write one value through every owner and read it back.
fn serves(fixture: &Fixture, db: &mut RemoteDatabase, seed: u64) {
    let database = db.database_id();
    for partition in 0..fixture.config.endpoints.len() as u32 {
        let key = (seed * 16 + u64::from(partition)) as u32;
        db.write_batch(
            operation(
                &fixture.config,
                database,
                partition,
                seed + u64::from(partition) * 64,
            ),
            vec![put(key, partition as u8 + 1)],
        )
        .unwrap();
        assert_eq!(value(db, key), Some(vec![partition as u8 + 1; 32]));
    }
}

fn crash_at(step: AuthorityStep) {
    set_step_hook(Some(Arc::new(move |current| {
        if current == step {
            Err(io::Error::other(format!("injected crash at {current:?}")))
        } else {
            Ok(())
        }
    })));
}

fn quarantines(directory: &Path) -> Vec<String> {
    fs::read_dir(directory)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            drop_tx::is_quarantine_name(&name).then_some(name)
        })
        .collect()
}

/// Every owner host unrouted the database, removed its directory and holds
/// its terminal tombstone.
fn assert_dropped_everywhere(fixture: &Fixture, database: [u8; 16]) {
    let hex = host_identity::database_hex(&database);
    for (host, config) in fixture.hosts.iter().zip(&fixture.host_configs) {
        assert!(host.database(&database).is_none(), "{hex} still routed");
        assert!(
            !config.root.join(format!("db-{hex}")).exists(),
            "{hex} files remain"
        );
        assert!(
            config.root.join("dropped").join(&hex).is_file(),
            "{hex} has no tombstone"
        );
    }
}

#[test]
fn provision_write_reopen_drop_reprovision_gets_a_new_identity() {
    let mut fixture = fixture();
    let first = fixture.config.storage.database;
    {
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
            .unwrap();
        db.write_batch(fixture.operation(1, 2), vec![put(2, 2)])
            .unwrap();
        db.shutdown().unwrap();
    }
    {
        let mut db = fixture.open();
        assert_eq!(db.database_id(), first);
        assert_eq!(value(&mut db, 1), Some(vec![1; 32]));
        assert_eq!(value(&mut db, 2), Some(vec![2; 32]));
        db.shutdown().unwrap();
    }
    // A drop waits for every holder of the owner database handles.
    fixture.nodes.clear();
    drop_tx::drop_remote(&fixture.authority()).unwrap();
    assert!(!fixture.authority().exists());
    assert!(quarantines(fixture.temp.path()).is_empty());
    assert_dropped_everywhere(&fixture, first);
    // A dropped database is gone for good: the same path provisions anew.
    let mut db = open(&fixture, &fixture.authority()).unwrap();
    let second = db.database_id();
    assert_ne!(second, first);
    assert_eq!(value(&mut db, 1), None);
    assert_eq!(value(&mut db, 2), None);
    for host in &fixture.hosts {
        assert_eq!(host.databases(), vec![second]);
    }
    serves(&fixture, &mut db, 3);
    db.shutdown().unwrap();
}

/// Crash after every persisted coordinator drop boundary, then recover the
/// way a later caller would: a reopen of a directory holding the intent
/// completes the drop and reports `NotFound`; a drop that never wrote its
/// intent is simply retried; a durable rename leaves only a quarantine that
/// the next lifecycle action on the parent reaps. `DropAfterUnlink` is the
/// rename-not-durable image, `DropAfterRename`/`DropAfterParentSync` the
/// durable one. Owners finish exactly once, including owners that already
/// acknowledged before the crash.
#[test]
fn coordinator_drop_recovers_from_a_crash_at_every_boundary() {
    for step in [
        AuthorityStep::DropAfterNamespace,
        AuthorityStep::DropAfterIntent,
        AuthorityStep::DropAfterChildren,
        AuthorityStep::DropAfterOwner(0),
        AuthorityStep::DropAfterOwner(1),
        AuthorityStep::DropAfterUnlink,
        AuthorityStep::DropAfterRename,
        AuthorityStep::DropAfterParentSync,
        AuthorityStep::DropAfterQuarantine,
    ] {
        let mut fixture = fixture();
        let database = fixture.config.storage.database;
        {
            let db = fixture.open();
            db.write_batch(fixture.operation(0, 1), vec![put(1, 1)])
                .unwrap();
            db.shutdown().unwrap();
        }
        fixture.nodes.clear();
        crash_at(step);
        let crashed = drop_tx::drop_remote(&fixture.authority());
        set_step_hook(None);
        let error = crashed.expect_err("drop returned successfully past crash cut");
        assert_eq!(error.to_string(), format!("injected crash at {step:?}"));
        if step == AuthorityStep::DropAfterOwner(0) {
            // Owner 1 was never asked: it still serves the database.
            assert!(fixture.hosts[0].database(&database).is_none());
            assert!(fixture.hosts[1].database(&database).is_some());
        }
        let authority = fixture.authority();
        if authority.exists() {
            match read_lifecycle(&authority).unwrap() {
                Some((Some(AuthorityLifecycle::Dropping { generation }), Some(recorded))) => {
                    let manifest = read_authority_manifest(&authority.join(MANIFEST)).unwrap();
                    assert_eq!(generation, manifest.next_generation - 1, "{step:?}");
                    assert_eq!(recorded, database, "{step:?}");
                    let error = open(&fixture, &authority).err().unwrap();
                    assert_eq!(error.kind(), io::ErrorKind::NotFound, "{step:?}: {error}");
                }
                Some((Some(AuthorityLifecycle::Ready), _)) => {
                    assert_eq!(step, AuthorityStep::DropAfterNamespace);
                    drop_tx::drop_remote(&authority).unwrap();
                }
                other => panic!("{step:?}: unexpected authority state {other:?}"),
            }
        } else {
            drop_tx::reap_quarantines(fixture.temp.path()).unwrap();
        }
        assert!(!authority.exists(), "{step:?}");
        assert!(quarantines(fixture.temp.path()).is_empty(), "{step:?}");
        assert_dropped_everywhere(&fixture, database);
        // The path is free: a new open provisions a different database.
        let mut db = open(&fixture, &authority).unwrap();
        assert_ne!(db.database_id(), database, "{step:?}");
        serves(&fixture, &mut db, 2);
        db.shutdown().unwrap();
    }
}

/// Crash after every persisted coordinator provision boundary. Before the
/// `Provisioning` manifest no owner holds anything and a reopen provisions
/// a fresh identity; from the manifest on, a reopen completes exactly the
/// recorded identity on every owner (never a second one) and serves it.
#[test]
fn coordinator_provision_recovers_from_a_crash_at_every_boundary() {
    let fixture = fixture();
    let steps = [
        AuthorityStep::OpenAfterNamespace,
        AuthorityStep::ProvisionAfterPreflight,
        AuthorityStep::ProvisionAfterManifest,
        AuthorityStep::ProvisionAfterOwner(0),
        AuthorityStep::ProvisionAfterOwner(1),
        AuthorityStep::ProvisionAfterReady,
    ];
    for (index, step) in steps.into_iter().enumerate() {
        let path = fixture.temp.path().join(format!("provision-{index}"));
        crash_at(step);
        let crashed = open(&fixture, &path);
        set_step_hook(None);
        let error = crashed.err().expect("open succeeded past crash cut");
        assert!(
            error
                .to_string()
                .contains(&format!("injected crash at {step:?}")),
            "{error}"
        );
        let recorded = read_lifecycle(&path).unwrap();
        let expected = match step {
            AuthorityStep::OpenAfterNamespace | AuthorityStep::ProvisionAfterPreflight => {
                assert!(recorded.is_none(), "{step:?}: {recorded:?}");
                None
            }
            _ => {
                let Some((Some(lifecycle), Some(database))) = recorded else {
                    panic!("{step:?}: no provisioned identity recorded: {recorded:?}");
                };
                let ready = step == AuthorityStep::ProvisionAfterReady;
                assert_eq!(
                    lifecycle,
                    if ready {
                        AuthorityLifecycle::Ready
                    } else {
                        AuthorityLifecycle::Provisioning
                    },
                    "{step:?}"
                );
                let provisioned: Vec<_> = fixture
                    .hosts
                    .iter()
                    .map(|host| host.database(&database).is_some())
                    .collect();
                let expected_owners = match step {
                    AuthorityStep::ProvisionAfterManifest => vec![false, false],
                    AuthorityStep::ProvisionAfterOwner(0) => vec![true, false],
                    _ => vec![true, true],
                };
                assert_eq!(provisioned, expected_owners, "{step:?}");
                Some(database)
            }
        };
        let mut db = open(&fixture, &path).unwrap();
        if let Some(database) = expected {
            assert_eq!(db.database_id(), database, "{step:?}");
        }
        for host in &fixture.hosts {
            assert!(host.database(&db.database_id()).is_some(), "{step:?}");
            // The fixture's database plus one per recovered provision: a
            // crash never leaks an owner database.
            assert_eq!(host.databases().len(), index + 2, "{step:?}");
        }
        let database = db.database_id();
        serves(&fixture, &mut db, 1);
        db.shutdown().unwrap();
        drop(db);
        let mut reopened = open(&fixture, &path).unwrap();
        assert_eq!(reopened.database_id(), database, "{step:?}");
        assert_eq!(value(&mut reopened, 16), Some(vec![1; 32]), "{step:?}");
        reopened.shutdown().unwrap();
    }
}

/// Sui opens `epoch_N/recovery_log` while `epoch_N` serves, and prunes both.
/// The child provisions its own database on the same owner hosts; both serve
/// interleaved writes; the serving parent cannot be dropped (no intent is
/// written); dropping the child leaves the parent serving; dropping the
/// parent drops a nested child first.
#[test]
fn nested_recovery_log_serves_and_drops_beside_its_parent() {
    let mut fixture = fixture();
    let parent_id = fixture.config.storage.database;
    let mut parent = fixture.open();
    let child_path = fixture.authority().join("recovery_log");
    let mut child = open(&fixture, &child_path).unwrap();
    let child_id = child.database_id();
    assert_ne!(child_id, parent_id);
    for host in &fixture.hosts {
        assert_eq!(host.databases().len(), 2);
    }
    for seed in 1..4 {
        serves(&fixture, &mut parent, seed);
        serves(&fixture, &mut child, seed + 100);
    }
    // The serving parent holds its native lock: no intent, child intact.
    assert!(drop_tx::drop_remote(&fixture.authority()).is_err());
    assert!(matches!(
        read_lifecycle(&fixture.authority()).unwrap(),
        Some((Some(AuthorityLifecycle::Ready), _))
    ));
    serves(&fixture, &mut child, 200);
    // Dropping the child does not disturb the parent.
    child.shutdown().unwrap();
    drop(child);
    drop_tx::drop_remote(&child_path).unwrap();
    assert!(!child_path.exists());
    assert_dropped_everywhere(&fixture, child_id);
    serves(&fixture, &mut parent, 300);
    assert_eq!(value(&mut parent, 16), Some(vec![1; 32]));
    // A new nested child, then a recursive drop of the closed parent.
    let child = open(&fixture, &child_path).unwrap();
    let second_child = child.database_id();
    assert_ne!(second_child, child_id);
    child.shutdown().unwrap();
    drop(child);
    parent.shutdown().unwrap();
    drop(parent);
    fixture.nodes.clear();
    drop_tx::drop_remote(&fixture.authority()).unwrap();
    assert!(!fixture.authority().exists());
    assert_dropped_everywhere(&fixture, second_child);
    assert_dropped_everywhere(&fixture, parent_id);
    for host in &fixture.hosts {
        assert!(host.databases().is_empty());
    }
}

/// Pausing one provision does not block ordinary leased writes for a different
/// routed database. Dropping a separate database must leave that route intact.
#[test]
fn parked_provision_does_not_stop_writes_of_another_database() {
    use crate::distributed::tcp::host::HostStep;
    use std::sync::mpsc;

    let mut fixture = fixture();
    let a_id = fixture.config.storage.database;
    let mut a = fixture.open();
    let b_path = fixture.temp.path().join("b");
    let mut b = open(&fixture, &b_path).unwrap();
    let b_id = b.database_id();
    serves(&fixture, &mut a, 1);
    serves(&fixture, &mut b, 2);
    let b_nodes: Vec<_> = fixture
        .hosts
        .iter()
        .map(|host| host.database(&b_id).unwrap())
        .collect();
    let b_high_water: Vec<_> = b_nodes
        .iter()
        .map(|node| node.fence_high_water().unwrap())
        .collect();
    assert!(b_high_water.iter().all(|&generation| generation > 0));

    // Pause owner 0's lifecycle actor inside the provision of database C.
    let (entered, entered_rx) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel::<()>(0);
    let released = parking_lot::Mutex::new(released);
    fixture.hosts[0].set_step_hook(Some(Arc::new(move |step| {
        if step == HostStep::ProvisionBeforeReserve {
            let _ = entered.try_send(());
            let _ = released.lock().recv();
        }
        Ok(())
    })));
    let c_path = fixture.temp.path().join("c");
    let c_config = fresh_config(&fixture);
    let c_shape = fixture.shape.clone();
    let c_open = std::thread::spawn(move || {
        let db = RemoteDatabase::open_local_live_fixture(&c_path, &c_shape, c_config)?;
        let id = db.database_id();
        db.shutdown()?;
        Ok::<_, LocalDbError>(id)
    });
    entered_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("C's provision reached owner 0's paused lifecycle actor");

    // B's writes are acknowledged by every owner while C remains parked.
    let mut written = Vec::new();
    for seed in 3..6u64 {
        for partition in 0..fixture.config.endpoints.len() as u32 {
            let key = (seed * 16 + u64::from(partition)) as u32;
            b.write_batch(
                operation(
                    &fixture.config,
                    b_id,
                    partition,
                    seed + u64::from(partition) * 64,
                ),
                vec![put(key, seed as u8)],
            )
            .unwrap();
            written.push((key, seed as u8));
        }
    }
    assert!(!c_open.is_finished(), "C's provision must still be paused");

    for &(key, seed) in &written {
        assert_eq!(value(&mut b, key), Some(vec![seed; 32]));
    }
    drop(release);
    let c_id = c_open.join().unwrap().unwrap();
    fixture.hosts[0].set_step_hook(None);
    assert!(![a_id, b_id].contains(&c_id));

    // Dropping A leaves B's routes, fence high-water and data in place.
    a.shutdown().unwrap();
    drop(a);
    fixture.nodes.clear();
    drop_tx::drop_remote(&fixture.authority()).unwrap();
    assert_dropped_everywhere(&fixture, a_id);
    for ((host, node), high_water) in fixture.hosts.iter().zip(&b_nodes).zip(&b_high_water) {
        assert!(Arc::ptr_eq(&host.database(&b_id).unwrap(), node));
        assert!(!node.is_closed());
        assert_eq!(node.fence_high_water().unwrap(), *high_water);
    }
    serves(&fixture, &mut b, 7);
    for &(key, seed) in &written {
        assert_eq!(value(&mut b, key), Some(vec![seed; 32]));
    }
    b.shutdown().unwrap();
}
