use super::super::NvmeofTransport;
use super::*;
use crate::distributed::test_bounds::PROGRESS_CAP;

fn local_resolver(dir: &std::path::Path) -> Arc<LiveNamespaceResolver> {
    let registry = LiveConnectionRegistry::open(&dir.join("resolver-registry")).unwrap();
    let target = LiveConnectTarget {
        nqn: "nqn.2026-01.test:wal".into(),
        uuid: [1; 16],
        nsid: 1,
        bytes: 4096,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: Default::default(),
    };
    LiveController::deleted_for_test(&registry, dir, &target)
        .unwrap()
        .resolver()
}

#[test]
fn shared_acquisition_uses_subsystem_identity_but_strict_acquisition_keeps_namespace_identity() {
    let dir = tempdir::TempDir::new("live-pool-shared").unwrap();
    let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:shared");
    if let Entry::Ready { identity, .. } = pool
        .state
        .lock()
        .unwrap()
        .entries
        .get_mut(&target.nqn)
        .unwrap()
    {
        identity.target.nsid = 0;
        identity.target.uuid = [0; 16];
    }
    let mut other = target.clone();
    other.nsid = 2;
    other.uuid = [3; 16];
    let deadline = Instant::now() + Duration::from_secs(10);
    let (first, how, _) = pool
        .acquire_shared(
            &target,
            "nqn.2026-01.test:host",
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline,
        )
        .unwrap();
    assert_eq!(how, LivePoolAcquisition::Reused);
    let (second, how, _) = pool
        .acquire_shared(
            &other,
            "nqn.2026-01.test:host",
            [2; 16],
            LiveNamespaceAccess::ReadShared,
            deadline,
        )
        .unwrap();
    assert_eq!(how, LivePoolAcquisition::Reused);
    assert!(Arc::ptr_eq(&first, &second));
    drop((first, second));
    pool.shutdown(deadline).unwrap();

    let registry = LiveConnectionRegistry::open(&dir.path().join("strict")).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:strict");
    let mut other = target.clone();
    other.nsid = 2;
    other.uuid = [3; 16];
    assert!(
        matches!(pool.acquire(&other, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
            Err(error) if error.kind() == io::ErrorKind::InvalidData)
    );
    pool.shutdown(deadline).unwrap();
}

#[test]
fn pending_and_capacity_are_bounded_without_connecting() {
    let dir = tempdir::TempDir::new("live-pool").unwrap();
    let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
    let pool = LiveControllerPool::new(registry, 1, 1).unwrap();
    let target = LiveConnectTarget {
        nqn: "nqn.2026-01.test:one".into(),
        uuid: [1; 16],
        nsid: 1,
        bytes: 4096,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: Default::default(),
    };
    let identity = Identity {
        target: target.clone(),
        host_nqn: "nqn.2026-01.test:host".into(),
        host_id: [2; 16],
        access: LiveNamespaceAccess::ReadWrite,
        file: None,
    };
    pool.state
        .lock()
        .unwrap()
        .entries
        .insert(target.nqn.clone(), Entry::Pending(identity));
    let mut other = target.clone();
    other.nqn = "nqn.2026-01.test:two".into();
    let deadline = Instant::now() + std::time::Duration::from_secs(1);
    let mut different_proof = target.clone();
    different_proof.bytes = 8192;
    assert!(matches!(
        pool.get_or_connect(&different_proof, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
        Err(e) if e.kind() == io::ErrorKind::InvalidData
    ));
    assert!(matches!(
        pool.get_or_connect(&target, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadOnly, deadline),
        Err(e) if e.kind() == io::ErrorKind::InvalidData
    ));
    assert!(
        matches!(pool.get_or_connect(&other, "nqn.2026-01.test:host", [2;16], LiveNamespaceAccess::ReadWrite, deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
    );
    assert!(matches!(pool.shutdown(deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
    pool.state
        .lock()
        .unwrap()
        .entries
        .insert(target.nqn.clone(), Entry::Quarantined { _export_pin: None });
    assert!(
        matches!(pool.get_or_connect(&other, "nqn.2026-01.test:host", [2;16], LiveNamespaceAccess::ReadWrite, deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
    );
    assert!(matches!(pool.shutdown(deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
    pool.state.lock().unwrap().entries.clear();
    let journal = dir.path().join("registry").join("unresolved.json");
    let intent = serde_json::json!({ "target": &target, "host_nqn": "nqn.2026-01.test:host", "host_id": vec![2;16] });
    std::fs::write(&journal, serde_json::to_vec(&intent).unwrap()).unwrap();
    assert!(
        pool.get_or_connect(
            &target,
            "nqn.2026-01.test:host",
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline
        )
        .is_err()
    );
    assert!(matches!(
        pool.state.lock().unwrap().entries.get(&target.nqn),
        Some(Entry::Quarantined { .. })
    ));
    assert!(matches!(pool.shutdown(deadline), Err(e) if e.kind() == io::ErrorKind::WouldBlock));
    std::fs::remove_file(journal).unwrap();
    pool.state.lock().unwrap().entries.clear();
    pool.shutdown(deadline).unwrap();
}
#[test]
fn idle_requires_released_lease_and_local_namespace_fd() {
    let dir = tempdir::TempDir::new("live-pool-pin").unwrap();
    let path = dir.path().join("wal");
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(4096).unwrap();
    let lease = Arc::new(LiveControllerLease {
        binding: LiveNamespaceBinding {
            namespace_path: path.clone(),
            transport: NvmeofTransport::Tcp,
            nqn: "nqn.2026-01.test:wal".into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            access: LiveNamespaceAccess::ReadWrite,
        },
        pin: Arc::new(()),
        instance: 0,
        resolver: local_resolver(dir.path()),
        namespaces: Mutex::new(HashMap::new()),
        local_fixture: false,
    });
    assert!(idle(&lease));
    let external = lease.clone();
    assert!(!idle(&lease));
    let mut namespace =
        LiveNamespace::open_local_file(&path, 4096, 4096, LiveNamespaceAccess::ReadWrite).unwrap();
    namespace.controller_lease = Some(external.pin.clone());
    drop(external);
    assert!(!idle(&lease));
    drop(namespace);
    assert!(idle(&lease));
}

fn ready_idle(pool: &LiveControllerPool, dir: &std::path::Path, nqn: &str) -> LiveConnectTarget {
    let target = LiveConnectTarget {
        nqn: nqn.into(),
        uuid: [1; 16],
        nsid: 1,
        bytes: 4096,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: Default::default(),
    };
    let lease = Arc::new(LiveControllerLease {
        binding: LiveNamespaceBinding {
            namespace_path: dir.join("wal"),
            transport: NvmeofTransport::Tcp,
            nqn: nqn.into(),
            uuid: [1; 16],
            nsid: 1,
            bytes: 4096,
            block_size: 4096,
            access: LiveNamespaceAccess::ReadWrite,
        },
        pin: Arc::new(()),
        instance: 0,
        resolver: local_resolver(dir),
        namespaces: Mutex::new(HashMap::new()),
        local_fixture: false,
    });
    let identity = Identity {
        target: target.clone(),
        host_nqn: "nqn.2026-01.test:host".into(),
        host_id: [2; 16],
        access: LiveNamespaceAccess::ReadWrite,
        file: None,
    };
    pool.state.lock().unwrap().entries.insert(
        nqn.into(),
        Entry::Ready {
            identity,
            lease,
            controller: LiveController::deleted_for_test(&pool.registry, dir, &target).unwrap(),
            usable: true,
            export_pin: None,
        },
    );
    target
}

#[test]
fn delete_runs_without_the_pool_lock_and_failure_keeps_an_unusable_entry() {
    let dir = tempdir::TempDir::new("live-pool-close").unwrap();
    let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:one");
    let deadline = Instant::now() + Duration::from_secs(5);
    let key = target.nqn.clone();

    let mut state = pool.state.lock().unwrap();
    let controller = LiveControllerPool::begin_close(&mut state, &key).unwrap();
    assert!(matches!(
        state.entries.get(&key),
        Some(Entry::Closing { .. })
    ));
    let (state, result) = pool
        .finish_close(state, &key, controller, |_| {
            // The pool lock is free while the delete runs.
            let state = pool
                .state
                .try_lock()
                .expect("delete must not hold the pool lock");
            assert!(matches!(
                state.entries.get("nqn.2026-01.test:one"),
                Some(Entry::Closing { .. })
            ));
            Err(io::Error::other("delete failed"))
        })
        .unwrap();
    // A failed delete restores the entry, unusable, with its controller.
    assert!(result.is_err());
    assert!(matches!(
        state.entries.get(&key),
        Some(Entry::Ready { usable: false, .. })
    ));
    drop(state);
    assert!(matches!(
        pool.get_or_connect(&target, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
        Err(e) if e.kind() == io::ErrorKind::InvalidData
    ));

    // Poison cleanup must not retry another lane's uncertain delete.
    pool.drain_idle(deadline).unwrap();
    assert!(matches!(
        pool.state.lock().unwrap().entries.get(&key),
        Some(Entry::Ready { usable: false, .. })
    ));

    // Retry through shutdown deletes it and removes the entry.
    pool.shutdown(deadline).unwrap();
    assert!(pool.state.lock().unwrap().entries.is_empty());
}

#[test]
fn callers_for_a_closing_target_wait_for_the_delete_to_end() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = tempdir::TempDir::new("live-pool-wait").unwrap();
    let registry = LiveConnectionRegistry::open(&dir.path().join("registry")).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:two");
    let key = target.nqn.clone();
    let deleted = AtomicBool::new(false);
    let mut state = pool.state.lock().unwrap();
    let controller = LiveControllerPool::begin_close(&mut state, &key).unwrap();
    std::thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let result = pool.get_or_connect(
                &target,
                "nqn.2026-01.test:host",
                [2; 16],
                LiveNamespaceAccess::ReadWrite,
                // The waiter follows the delete's end.
                Instant::now() + PROGRESS_CAP,
            );
            (result, deleted.load(Ordering::SeqCst))
        });
        let (state, result) = pool
            .finish_close(state, &key, controller, |_| {
                std::thread::sleep(Duration::from_millis(200));
                deleted.store(true, Ordering::SeqCst);
                Err(io::Error::other("delete failed"))
            })
            .unwrap();
        assert!(result.is_err());
        drop(state);
        // Closing is not reusable and not an error: the waiter returns only
        // after the delete ended, and then sees the unusable entry.
        let (result, delete_had_ended) = waiter.join().unwrap();
        assert!(delete_had_ended);
        assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::InvalidData));
    });
    // The delete (200 ms floor inside the callback) has already ended, so
    // this bound only needs headroom for teardown under load.
    pool.shutdown(Instant::now() + PROGRESS_CAP).unwrap();
}
