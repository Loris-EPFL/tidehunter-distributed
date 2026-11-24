//! Coordinator lifecycle: namespace lock ordering (first-child provision
//! racing a parent drop, nested open while the parent serves, open under a
//! dropping ancestor), the identity-free storage geometry and quarantine
//! reaping of both durable rename outcomes.

use super::*;
use crate::distributed::storage::{encode_metadata, install_metadata};

#[derive(Serialize)]
struct LifecycleOnly {
    lifecycle: AuthorityLifecycle,
    storage: IdentityOnly,
}

#[derive(Serialize)]
struct IdentityOnly {
    database: [u8; 16],
}

fn write_lifecycle(directory: &Path, lifecycle: AuthorityLifecycle, database: [u8; 16]) {
    let bytes = encode_metadata(&LifecycleOnly {
        lifecycle,
        storage: IdentityOnly { database },
    })
    .unwrap();
    install_metadata(directory, MANIFEST, &bytes).unwrap();
}

#[test]
fn parent_drop_lock_excludes_a_first_child_provision() {
    let temp = tempdir::TempDir::new("namespace-parent-first").unwrap();
    let parent = temp.path().join("epoch_5");
    fs::create_dir(&parent).unwrap();
    let dropping = NamespaceGuard::acquire(&parent, NamespaceMode::Drop).unwrap();
    let child = parent.join("recovery_log");
    let error = NamespaceGuard::acquire(&child, NamespaceMode::Open)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    // The child created no state under the parent being dropped.
    assert!(!child.exists());
    drop(dropping);
    NamespaceGuard::acquire(&child, NamespaceMode::Open).unwrap();
}

#[test]
fn child_provision_in_progress_excludes_the_parent_drop() {
    let temp = tempdir::TempDir::new("namespace-child-first").unwrap();
    let parent = temp.path().join("epoch_5");
    fs::create_dir(&parent).unwrap();
    let child = NamespaceGuard::acquire(&parent.join("recovery_log"), NamespaceMode::Open).unwrap();
    let error = NamespaceGuard::acquire(&parent, NamespaceMode::Drop)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    drop(child);
    // Once the child established itself, the parent drop discovers it.
    write_lifecycle(
        &parent.join("recovery_log"),
        AuthorityLifecycle::Ready,
        [2; 16],
    );
    let guard = NamespaceGuard::acquire(&parent, NamespaceMode::Drop).unwrap();
    assert_eq!(
        drop_tx_nested(&parent),
        vec![parent.join("recovery_log").canonicalize().unwrap()]
    );
    // The parent inherits its exclusive lock when it descends.
    guard.descend(&guard.target().join("recovery_log")).unwrap();
}

fn drop_tx_nested(root: &Path) -> Vec<PathBuf> {
    let root = root.canonicalize().unwrap();
    let mut found = Vec::new();
    for entry in fs::read_dir(&root).unwrap() {
        let path = entry.unwrap().path();
        if read_lifecycle(&path).unwrap().is_some() {
            found.push(path);
        }
    }
    found
}

#[test]
fn a_serving_parent_does_not_block_a_nested_open() {
    let temp = tempdir::TempDir::new("namespace-serving").unwrap();
    let parent = temp.path().join("epoch_7");
    let guard = NamespaceGuard::acquire(&parent, NamespaceMode::Open).unwrap();
    write_lifecycle(guard.target(), AuthorityLifecycle::Ready, [7; 16]);
    // Open setup done: the serving parent keeps only its native lock.
    let serving = DbLock::acquire(guard.target(), Duration::ZERO).unwrap();
    drop(guard);
    let child = NamespaceGuard::acquire(&parent.join("recovery_log"), NamespaceMode::Open).unwrap();
    // ...while a serving child prevents the parent's deletion.
    let child_lock = DbLock::acquire(child.target(), Duration::ZERO).unwrap();
    drop(child);
    assert_eq!(
        DbLock::acquire(&parent.join("recovery_log"), Duration::ZERO)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    drop((serving, child_lock));
}

#[test]
fn open_under_a_dropping_ancestor_is_refused_before_any_state() {
    let temp = tempdir::TempDir::new("namespace-dropping").unwrap();
    let parent = temp.path().join("epoch_9");
    fs::create_dir(&parent).unwrap();
    write_lifecycle(
        &parent,
        AuthorityLifecycle::Dropping { generation: 3 },
        [9; 16],
    );
    let child = parent.join("recovery_log");
    let error = NamespaceGuard::acquire(&child, NamespaceMode::Open)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(!child.exists());
}

#[test]
fn drop_of_a_missing_path_is_not_found() {
    let temp = tempdir::TempDir::new("namespace-missing").unwrap();
    let error = drop_remote(&temp.path().join("absent")).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
}

#[test]
fn storage_geometry_never_carries_the_database() {
    #[derive(Serialize, Deserialize)]
    struct Wrapper {
        #[serde(with = "storage_geometry")]
        storage: LocalInventoryConfig,
    }
    let storage = LocalInventoryConfig {
        database: [5; 16],
        partitions: vec![0, 1],
        batch_limits: crate::distributed::codec::BatchLimits {
            max_operations: 4,
            max_encoded_bytes: 2048,
        },
        group_limits: crate::distributed::group::GroupLimits {
            max_batches: 2,
            max_encoded_bytes: 4088,
        },
        fragment_bytes: 4096,
        wal_file_bytes: 8192,
        max_batches_per_partition: 64,
        max_reference_metadata_bytes: 1 << 20,
    };
    let mut value = serde_json::to_value(Wrapper { storage }).unwrap();
    assert!(value["storage"].get("database").is_none());
    let decoded: Wrapper = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(decoded.storage.database, [0; 16]);
    value["storage"]["database"] = serde_json::json!(vec![1u8; 16]);
    let error = serde_json::from_value::<Wrapper>(value).err().unwrap();
    assert!(
        error.to_string().contains(
            "storage.database is provisioned and must not appear in remote configuration"
        ),
        "{error}"
    );
}

#[test]
fn quarantines_are_reaped_only_when_they_hold_the_matching_drop() {
    let temp = tempdir::TempDir::new("quarantine").unwrap();
    let id = [0xab; 16];
    let name = format!(".tidehunter-drop-{}", host_identity::database_hex(&id));
    // Rename durable, quarantine unlink not yet done.
    let quarantine = temp.path().join(&name);
    fs::create_dir(&quarantine).unwrap();
    fs::write(quarantine.join("LOCK"), b"").unwrap();
    write_lifecycle(
        &quarantine,
        AuthorityLifecycle::Dropping { generation: 1 },
        id,
    );
    drop_tx::reap_quarantines(temp.path()).unwrap();
    assert!(!quarantine.exists());
    // A foreign identity or extra data is corruption, never reaped.
    fs::create_dir(&quarantine).unwrap();
    write_lifecycle(
        &quarantine,
        AuthorityLifecycle::Dropping { generation: 1 },
        [1; 16],
    );
    assert_eq!(
        drop_tx::reap_quarantines(temp.path()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    // A fresh database at the original name is never touched.
    let fresh = temp.path().join("epoch_1");
    fs::create_dir(&fresh).unwrap();
    write_lifecycle(&fresh, AuthorityLifecycle::Ready, [3; 16]);
    assert!(drop_tx::is_quarantine_name(&name));
    assert!(!drop_tx::is_quarantine_name("epoch_1"));
    assert!(fresh.join(MANIFEST).exists());
}

/// Public `Db::drop_db`: an unmarked directory with a remote descendant is
/// refused, never removed natively; the descendant survives intact.
#[test]
fn public_drop_refuses_an_unmarked_parent_with_remote_descendants() {
    let temp = tempdir::TempDir::new("public-drop-unmarked").unwrap();
    let parent = temp.path().join("authorities_db");
    let child = parent.join("epoch_3").join("recovery_log");
    fs::create_dir_all(&child).unwrap();
    write_lifecycle(&child, AuthorityLifecycle::Ready, [3; 16]);
    let error = crate::distributed::sui::Db::drop_db(&parent).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "{error}");
    assert!(child.join(MANIFEST).exists());
}

/// Public `Db::drop_db` classifies under the namespace lock: while a first
/// open holds the target (and may be creating its marker), the drop neither
/// chooses native removal nor deletes anything; once no lock is held, a
/// plain directory still drops natively.
#[test]
fn public_drop_never_classifies_while_an_open_holds_the_namespace() {
    let temp = tempdir::TempDir::new("public-drop-race").unwrap();
    let path = temp.path().join("epoch_4");
    let opening = NamespaceGuard::acquire(&path, NamespaceMode::Open).unwrap();
    let error = crate::distributed::sui::Db::drop_db(&path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "{error}");
    assert!(path.exists());
    // The open finishes its marker, releases the namespace and serves.
    write_lifecycle(&path, AuthorityLifecycle::Ready, [4; 16]);
    let serving = DbLock::acquire(&path, Duration::ZERO).unwrap();
    drop(opening);
    // Now classified remote: busy, not natively removed.
    let error = crate::distributed::sui::Db::drop_db(&path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "{error}");
    assert!(path.join(MANIFEST).exists());
    drop(serving);
    // A child open in progress also blocks a native drop of its parent.
    let plain = temp.path().join("plain");
    let child = NamespaceGuard::acquire(&plain.join("child"), NamespaceMode::Open);
    assert!(child.is_err(), "parent must exist first");
    fs::create_dir(&plain).unwrap();
    let child = NamespaceGuard::acquire(&plain.join("child"), NamespaceMode::Open).unwrap();
    assert_eq!(
        crate::distributed::sui::Db::drop_db(&plain)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    drop(child);
    crate::distributed::sui::Db::drop_db(&plain).unwrap();
    assert!(!plain.exists());
}

/// The coordinator's database-lock wait is bounded by the configured
/// `open_lock_retry_timeout`, not by native's call site hard-coding zero: a
/// previous generation of this process that still holds the authority
/// directory open makes the open wait for the configured window, and a zero
/// window refuses at once.
#[test]
fn the_configured_open_lock_retry_timeout_bounds_the_authority_lock_wait() {
    use crate::distributed::hybrid::config::HybridIndexConfig;
    use std::time::Duration;

    let temp = tempdir::TempDir::new("authority-lock-window").unwrap();
    let target = temp.path().join("database");
    fs::create_dir(&target).unwrap();
    let held = DbLock::acquire(&target, Duration::ZERO).unwrap();

    // A zero window is the pre-fix behaviour: refuse while the directory is held.
    let zero = HybridIndexConfig {
        open_lock_retry_timeout: Duration::ZERO,
        ..HybridIndexConfig::default()
    };
    let error = acquire_authority_lock(&target, &zero).expect_err("held by this process");
    assert_eq!(
        error.kind(),
        io::ErrorKind::AlreadyExists,
        "a zero window refuses at once: {error}",
    );

    // A configured window waits: the same lock, released from this thread while
    // the opener retries, is granted. The opener must not have finished before
    // the release, or the configured window was not consulted.
    let windowed = HybridIndexConfig {
        open_lock_retry_timeout: Duration::from_secs(30),
        ..zero
    };
    let (started, started_rx) = std::sync::mpsc::sync_channel(1);
    let path = target.clone();
    let waiter = std::thread::spawn(move || {
        let _ = started.send(());
        acquire_authority_lock(&path, &windowed)
    });
    started_rx.recv().expect("the opener thread started");
    // Give the opener the processor without waiting on a clock: each yield lets
    // it run, and a zero-window implementation would have returned by now.
    for _ in 0..10_000 {
        if waiter.is_finished() {
            break;
        }
        std::thread::yield_now();
    }
    assert!(
        !waiter.is_finished(),
        "the wait must stay open while the directory is held",
    );
    drop(held);
    let lock = waiter
        .join()
        .expect("the opener thread did not panic")
        .expect("the handoff grants the lock");
    drop(lock);
}
