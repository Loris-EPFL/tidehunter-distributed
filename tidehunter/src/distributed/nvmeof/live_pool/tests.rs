use super::super::NvmeofTransport;
use super::*;
use crate::distributed::nvmeof::live_connect::RegistryMode;
use crate::distributed::test_bounds::PROGRESS_CAP;

fn local_resolver(dir: &std::path::Path) -> Arc<LiveNamespaceResolver> {
    let registry =
        LiveConnectionRegistry::open(&dir.join("resolver-registry"), RegistryMode::Retire).unwrap();
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
fn strict_and_shared_acquisitions_share_one_subsystem_controller() {
    const HOST: &str = "nqn.2026-01.test:host";
    let dir = tempdir::TempDir::new("live-pool-shared").unwrap();
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:shared");
    let mut other = target.clone();
    other.nsid = 2;
    other.uuid = [3; 16];
    let deadline = Instant::now() + PROGRESS_CAP;
    let (first, how, _) = pool
        .acquire_shared(
            &target,
            HOST,
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline,
        )
        .unwrap();
    assert_eq!(
        how,
        LivePoolAcquisition::Reused,
        "shared acquisition reuses the cached subsystem controller"
    );
    // A permanent export shares one subsystem NQN across many namespaces, so
    // a strict file acquisition reuses that subsystem controller.
    let (second, how, _) = pool
        .acquire_file(
            &other,
            HOST,
            [2; 16],
            LiveNamespaceAccess::ReadShared,
            ControllerAdoption::Adopt,
            deadline,
            (0, 1, 1),
        )
        .unwrap();
    assert_eq!(
        how,
        LivePoolAcquisition::Reused,
        "a file grant of the same subsystem reuses its controller"
    );
    assert!(
        Arc::ptr_eq(&first, &second),
        "both acquisitions must share one lease"
    );
    assert_eq!(
        pool.state.lock().unwrap().entries.len(),
        1,
        "one controller per subsystem NQN"
    );
    drop((first, second));
    pool.shutdown(deadline).unwrap();
}

fn fixture_target(nqn: &str, nsid: u32, uuid: [u8; 16], bytes: u64) -> LiveConnectTarget {
    LiveConnectTarget {
        nqn: nqn.into(),
        uuid,
        nsid,
        bytes,
        block_size: 4096,
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        geometry: Default::default(),
    }
}

/// A sealed subsystem spans two namespace exports. Every acquisition must
/// reuse its one controller and resolve each member through that lease.
fn acquire_two_sealed_namespaces(
    pool: &LiveControllerPool,
    host: &str,
    first: &LiveConnectTarget,
    second: &LiveConnectTarget,
    first_path: &std::path::Path,
    second_path: &std::path::Path,
) {
    let deadline = Instant::now() + PROGRESS_CAP;
    pool.cache_local_shared_namespace(first, host, [2; 16], first_path)
        .unwrap();
    pool.cache_local_shared_namespace(second, host, [2; 16], second_path)
        .unwrap();
    let (one, _, _) = pool
        .acquire_file(
            first,
            host,
            [2; 16],
            LiveNamespaceAccess::ReadOnly,
            ControllerAdoption::Adopt,
            deadline,
            (0, 1, 0),
        )
        .unwrap();
    let (two, _, _) = pool
        .acquire_file(
            second,
            host,
            [2; 16],
            LiveNamespaceAccess::ReadOnly,
            ControllerAdoption::Adopt,
            deadline,
            (0, 1, 1),
        )
        .unwrap();
    assert!(
        Arc::ptr_eq(&one, &two),
        "the second namespace must reuse the subsystem controller"
    );
    assert_eq!(
        pool.state.lock().unwrap().entries.len(),
        1,
        "one controller for the subsystem"
    );
    let (_, first_binding) = one
        .namespace(
            first.nsid,
            first.uuid,
            LiveNamespaceAccess::ReadOnly,
            deadline,
        )
        .unwrap();
    let (_, second_binding) = one
        .namespace(
            second.nsid,
            second.uuid,
            LiveNamespaceAccess::ReadOnly,
            deadline,
        )
        .unwrap();
    assert_eq!(
        first_binding.namespace_path, first_path,
        "namespace {} resolved its own export",
        first.nsid
    );
    assert_eq!(
        second_binding.namespace_path, second_path,
        "namespace {} resolved its own export",
        second.nsid
    );
    drop((one, two));
    pool.shutdown(deadline).unwrap();
}

#[test]
fn reopen_of_a_sealed_subsystem_with_two_namespaces_connects_one_controller() {
    const HOST: &str = "nqn.2026-01.test:host";
    const NQN: &str = "nqn.2026-01.test:sealed";
    const BYTES: u64 = 8192;
    let dir = tempdir::TempDir::new("live-pool-reopen").unwrap();
    let first_path = dir.path().join("wal-0");
    let second_path = dir.path().join("wal-1");
    for path in [&first_path, &second_path] {
        std::fs::File::create(path).unwrap().set_len(BYTES).unwrap();
    }
    let first = fixture_target(NQN, 1, [1; 16], BYTES);
    let second = fixture_target(NQN, 2, [3; 16], BYTES);
    {
        let registry =
            LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire)
                .unwrap();
        let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
        acquire_two_sealed_namespaces(&pool, HOST, &first, &second, &first_path, &second_path);
    }
    // Graceful close removed the controller. The reopen reuses the sealed
    // subsystem NQN and still needs exactly one controller for both members.
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    acquire_two_sealed_namespaces(&pool, HOST, &first, &second, &first_path, &second_path);
}

#[test]
fn pending_and_capacity_are_bounded_without_connecting() {
    let dir = tempdir::TempDir::new("live-pool").unwrap();
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
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
    let mut identity = Identity {
        target: target.clone(),
        host_nqn: "nqn.2026-01.test:host".into(),
        host_id: [2; 16],
        access: LiveNamespaceAccess::ReadWrite,
        file: None,
    };
    // The pool keys a subsystem, not one of its members.
    identity.target.nsid = 0;
    identity.target.uuid = [0; 16];
    pool.state
        .lock()
        .unwrap()
        .entries
        .insert(target.nqn.clone(), Entry::Pending(identity));
    let mut other = target.clone();
    other.nqn = "nqn.2026-01.test:two".into();
    let deadline = Instant::now() + PROGRESS_CAP;
    let mut different_proof = target.clone();
    different_proof.bytes = 8192;
    assert!(matches!(
        pool.get_or_connect(&different_proof, "nqn.2026-01.test:host", [2; 16], LiveNamespaceAccess::ReadWrite, deadline),
        Err(e) if e.kind() == io::ErrorKind::InvalidData
    ));
    assert!(matches!(
        pool.get_or_connect(&target, "nqn.2026-01.test:other-host", [2; 16], LiveNamespaceAccess::ReadOnly, deadline),
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
        .insert(target.nqn.clone(), Entry::Quarantined);
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
        Some(Entry::Quarantined)
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
        #[cfg(test)]
        controller_lost_override: std::sync::atomic::AtomicU8::new(0),
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
        #[cfg(test)]
        controller_lost_override: std::sync::atomic::AtomicU8::new(0),
    });
    let mut identity = Identity {
        target: target.clone(),
        host_nqn: "nqn.2026-01.test:host".into(),
        host_id: [2; 16],
        access: LiveNamespaceAccess::ReadWrite,
        file: None,
    };
    identity.target.nsid = 0;
    identity.target.uuid = [0; 16];
    pool.state.lock().unwrap().entries.insert(
        nqn.to_owned(),
        Entry::Ready {
            identity,
            lease,
            controller: LiveController::deleted_for_test(&pool.registry, dir, &target).unwrap(),
            usable: true,
        },
    );
    target
}

#[test]
fn eviction_defers_a_busy_subsystem_controller_and_retires_it_idle() {
    let dir = tempdir::TempDir::new("live-pool-exact-evict").unwrap();
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:evict");
    let deadline = Instant::now() + PROGRESS_CAP;
    let (held, how, _) = pool
        .acquire(
            &target,
            "nqn.2026-01.test:host",
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline,
        )
        .unwrap();
    assert_eq!(how, LivePoolAcquisition::Reused);
    {
        let mut state = pool.state.lock().unwrap();
        if let Some(Entry::Ready { identity, .. }) = state.entries.get_mut(&target.nqn) {
            // This controller serves subsystem 0.
            identity.file = Some((0, 1, 0));
        }
    }
    // A victim in another subsystem leaves the controller cached.
    assert_eq!(
        pool.evict_files(0, 1, 4096, &[1024 * 4096], deadline)
            .unwrap(),
        RetirementResult::Completed
    );
    assert!(
        pool.state.lock().unwrap().entries.contains_key(&target.nqn),
        "an unrelated subsystem keeps its controller"
    );
    // Its own victim defers while an external lease keeps it busy.
    assert_eq!(
        pool.evict_files(0, 1, 4096, &[0], deadline).unwrap(),
        RetirementResult::Busy
    );
    assert!(
        pool.state.lock().unwrap().entries.contains_key(&target.nqn),
        "a busy controller stays cached"
    );
    drop(held);
    assert_eq!(
        pool.evict_files(0, 1, 4096, &[0], deadline).unwrap(),
        RetirementResult::Completed
    );
    assert!(
        !pool.state.lock().unwrap().entries.contains_key(&target.nqn),
        "an idle controller of the victim subsystem is retired"
    );
    pool.shutdown(deadline).unwrap();
}

#[test]
fn delete_runs_without_the_pool_lock_and_failure_keeps_an_unusable_entry() {
    let dir = tempdir::TempDir::new("live-pool-close").unwrap();
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:one");
    let deadline = Instant::now() + PROGRESS_CAP;
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
                state.entries.get(&key),
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
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
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
                // Hold the delete until the waiter is parked on the `Closing` entry, so
                // the check below is made at a point where it cannot have returned.
                pool.wait_parked_waiters(1, Instant::now() + PROGRESS_CAP);
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
    // The delete has already ended, so this bound is only a hang guard.
    pool.shutdown(Instant::now() + PROGRESS_CAP).unwrap();
}

/// A teardown whose delete does not confirm, on a lease that is verifiably not
/// live, removes the entry instead of leaving an unusable quarantine. The CI
/// cell hit exactly this: `delete_controller` refused the store, and the entry
/// stayed unusable for the rest of the process.
#[test]
fn eviction_removes_a_lost_entry_whose_delete_does_not_confirm() {
    let dir = tempdir::TempDir::new("live-pool-lost-evict").unwrap();
    let registry_dir = dir.path().join("registry");
    let registry = LiveConnectionRegistry::open(&registry_dir, RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:lost");
    let lease = pool
        .cached_lease(&target.nqn)
        .expect("the fixture entry is cached");
    assert!(lease.controller_lost(), "the fixture controller is gone");
    // The teardown cannot remove its durable intent, so `disconnect` fails.
    std::fs::remove_dir_all(&registry_dir).unwrap();
    pool.evict_lost(&target.nqn, Instant::now() + PROGRESS_CAP)
        .expect("a lost controller is retired from the pool");
    assert!(
        pool.state.lock().unwrap().entries.is_empty(),
        "an unconfirmed delete of a lost controller keeps no unusable entry"
    );
}

/// A cached controller that is still live is never deleted: another lane may
/// have reconnected it between the break and the eviction.
#[test]
fn eviction_leaves_a_live_controller_cached() {
    const HOST: &str = "nqn.2026-01.test:host";
    let dir = tempdir::TempDir::new("live-pool-live-evict").unwrap();
    let registry_dir = dir.path().join("registry");
    let registry = LiveConnectionRegistry::open(&registry_dir, RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = fixture_target("nqn.2026-01.test:live", 1, [1; 16], 4096);
    let path = dir.path().join("wal");
    std::fs::File::create(&path).unwrap().set_len(4096).unwrap();
    pool.register_local_target(&target, LiveNamespaceAccess::ReadWrite, path)
        .unwrap();
    let deadline = Instant::now() + PROGRESS_CAP;
    let (lease, how, _) = pool
        .acquire_shared(
            &target,
            HOST,
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline,
        )
        .unwrap();
    assert_eq!(how, LivePoolAcquisition::Connected);
    assert!(
        !lease.controller_lost(),
        "the local fixture lease reports a live controller"
    );
    // A teardown would fail now, so this proves none is attempted.
    std::fs::remove_dir_all(&registry_dir).unwrap();
    pool.evict_lost(&target.nqn, deadline)
        .expect("a live controller is left alone");
    assert!(
        pool.cached_lease(&target.nqn).is_some(),
        "a live replacement stays cached"
    );
    drop(lease);
    assert!(pool.shutdown(Instant::now() + PROGRESS_CAP).is_err());
}

/// A first connect that fails while it verifies the controller it created
/// leaves a typed lost cause, not a flattened `Other`, and releases its entry so
/// the next acquire can connect.
#[test]
fn acquire_types_a_first_connect_loss_as_lost() {
    let dir = tempdir::TempDir::new("live-pool-acquire-lost").unwrap();
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = fixture_target("nqn.2026-01.test:acquire", 1, [1; 16], 4096);
    pool.fail_next_connect_lost();
    let deadline = Instant::now() + PROGRESS_CAP;
    let error = match pool.acquire_shared(
        &target,
        "nqn.2026-01.test:host",
        [2; 16],
        LiveNamespaceAccess::ReadWrite,
        deadline,
    ) {
        Ok(_) => panic!("the first connect reports the stopped controller"),
        Err(error) => error,
    };
    assert!(
        crate::distributed::nvmeof::live_namespace::controller_lost(&error),
        "the first-connect loss keeps its typed cause: {error}"
    );
    assert_eq!(
        error.kind(),
        io::ErrorKind::ConnectionAborted,
        "the loss keeps the resumable class: {error}"
    );
    assert!(
        pool.cached_lease(&target.nqn).is_none(),
        "the failed entry is released"
    );
    let path = dir.path().join("wal");
    std::fs::File::create(&path).unwrap().set_len(4096).unwrap();
    pool.register_local_target(&target, LiveNamespaceAccess::ReadWrite, path)
        .unwrap();
    let (lease, how, _) = pool
        .acquire_shared(
            &target,
            "nqn.2026-01.test:host",
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline,
        )
        .expect("the next acquire connects");
    assert_eq!(how, LivePoolAcquisition::Connected);
    drop(lease);
    pool.shutdown(deadline).unwrap();
}

/// A replay page whose cached controller the kernel has stopped evicts that
/// lease and reconnects. This is the pool half of the page retry: the entry is
/// gone, the next connect reports its own loss, and the one after it connects.
#[test]
fn a_lost_cached_lease_is_evicted_and_the_next_acquire_reconnects() {
    const HOST: &str = "nqn.2026-01.test:host";
    let dir = tempdir::TempDir::new("live-pool-replay-retry").unwrap();
    let registry =
        LiveConnectionRegistry::open(&dir.path().join("registry"), RegistryMode::Retire).unwrap();
    let pool = LiveControllerPool::new(registry, 2, 1).unwrap();
    let target = ready_idle(&pool, dir.path(), "nqn.2026-01.test:replay");
    let lost = pool
        .cached_lease(&target.nqn)
        .expect("the fixture entry is cached");
    lost.force_controller_lost(true);
    assert!(
        pool.entry_lost(&target.nqn),
        "the entry probe must name the stopped controller"
    );
    let path = dir.path().join("wal");
    std::fs::File::create(&path).unwrap().set_len(4096).unwrap();
    pool.register_local_target(&target, LiveNamespaceAccess::ReadWrite, path)
        .unwrap();
    let deadline = Instant::now() + PROGRESS_CAP;
    pool.evict_lost(&target.nqn, deadline)
        .expect("a verifiably lost lease is retired");
    assert!(
        pool.cached_lease(&target.nqn).is_none(),
        "the retry must connect instead of reusing the lost lease"
    );
    // The first reconnect verifies a controller the kernel stopped, exactly as
    // the real connect does; it must leave no entry for the retry.
    pool.fail_next_connect_lost();
    let error = match pool.acquire_shared(
        &target,
        HOST,
        [2; 16],
        LiveNamespaceAccess::ReadWrite,
        deadline,
    ) {
        Ok(_) => panic!("the fixture first connect reports the stopped controller"),
        Err(error) => error,
    };
    assert!(
        crate::distributed::nvmeof::live_namespace::controller_lost(&error),
        "the reconnect loss keeps its typed cause: {error}"
    );
    assert!(
        pool.cached_lease(&target.nqn).is_none(),
        "a lost reconnect releases its entry"
    );
    let (fresh, how, _) = pool
        .acquire_shared(
            &target,
            HOST,
            [2; 16],
            LiveNamespaceAccess::ReadWrite,
            deadline,
        )
        .expect("the retry connects a fresh controller");
    assert_eq!(
        how,
        LivePoolAcquisition::Connected,
        "the retry must connect, not reuse a cached lease"
    );
    assert!(
        !Arc::ptr_eq(&fresh, &lost),
        "the reconnect must not return the evicted lease"
    );
    assert!(
        !fresh.controller_lost(),
        "the reconnected fixture controller is live"
    );
    drop(fresh);
    pool.shutdown(deadline).unwrap();
}
