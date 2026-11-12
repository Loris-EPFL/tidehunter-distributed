use super::super::tests::{batch, config, metrics};
use super::*;

// Mirror the production ownership-preserving admission API without boxing.
#[allow(clippy::result_large_err)]
fn command(
    node: &StorageNode,
    id: u64,
    request: Request,
    charge: usize,
    completed: &mpsc::SyncSender<reactor::WorkCompletion>,
) -> Result<(), ControlWork> {
    node.control.try_submit(ControlWork {
        id,
        request,
        charge,
        limits: TcpLimits::default(),
        measured: true,
        trace_shape: None,
        queued: Some(Instant::now()),
        submitted: Some(Instant::now()),
        completed: completed.clone(),
    })
}
fn append_request(config: &NodeConfig, epoch: u64, sequence: u64) -> Request {
    Request::Append {
        epoch,
        batches: vec![batch(config, epoch, sequence).encoded().clone().into()],
    }
}
fn receive(
    completions: &mpsc::Receiver<reactor::WorkCompletion>,
    id: u64,
) -> reactor::WorkCompletion {
    let completion = completions.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(completion.id, id);
    assert_eq!(completion.profile.control_queue_samples, 1);
    assert_eq!(completion.profile.control_execution_samples, 1);
    completion
}

/// The tests below assert receipts that are already durable, which is the
/// synchronous tier's contract; the native tier is covered separately.
fn open_synchronous(config: &NodeConfig) -> Arc<StorageNode> {
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Synchronous))
        .unwrap();
    node
}

#[test]
fn isolated_append_does_not_visit_the_blocked_diagnostic_pool() {
    let temp = tempdir::TempDir::new("control-not-read-pool").unwrap();
    let config = config(&temp.path().join("node"));
    let node = open_synchronous(&config);
    node.inventory.lock().fence_to(1).unwrap();
    let (entered, entries) = mpsc::sync_channel(config.read_workers);
    let (release, released) = mpsc::sync_channel(config.read_workers);
    let released = Arc::new(Mutex::new(released));
    for _ in 0..config.read_workers {
        let entered = entered.clone();
        let released = released.clone();
        node.read_pool.spawn(move || {
            entered.send(()).unwrap();
            released
                .lock()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        });
    }
    for _ in 0..config.read_workers {
        entries.recv_timeout(Duration::from_secs(5)).unwrap();
    }
    let (completed, completions) = mpsc::sync_channel(1);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    let response = receive(&completions, 1).response;
    for _ in 0..config.read_workers {
        release.send(()).unwrap();
    }
    let Response::Appended(receipts) = response.unwrap() else {
        panic!("append failed")
    };
    assert_eq!(receipts.len(), 1);
    let log = node.inventory.lock().active(1).unwrap();
    let reference = batch(&config, 1, 0)
        .record_reference(receipts[0].address, 0)
        .unwrap();
    log.validate_durable_record_reference(&reference).unwrap();
    assert_eq!(log.stats().persistence_barriers, 1);
}

#[test]
fn appender_services_next_group_while_captured_persistence_is_paused() {
    let temp = tempdir::TempDir::new("control-captured-prefix").unwrap();
    let config = config(&temp.path().join("node"));
    let node = open_synchronous(&config);
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let (completed, completions) = mpsc::sync_channel(3);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(command(&node, 2, append_request(&config, 1, 1), 4096, &completed).is_ok());
    let second = batch(&config, 1, 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    while log.lookup(second.id()).unwrap().is_none() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(log.lookup(second.id()).unwrap().is_some());
    assert!(matches!(
        completions.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert!(command(&node, 3, Request::Fence { epoch: 2 }, 1, &completed).is_ok());
    release.send(()).unwrap();
    assert!(matches!(
        receive(&completions, 1).response.unwrap(),
        Response::Appended(_)
    ));
    assert!(matches!(
        receive(&completions, 2).response.unwrap(),
        Response::Appended(_)
    ));
    assert!(matches!(
        receive(&completions, 3).response.unwrap(),
        Response::Status(_)
    ));
    assert_eq!(log.stats().appended_groups, 2);
    assert_eq!(log.stats().persistence_barriers, 2);
}
#[test]
fn append_checkpoint_seal_and_fence_keep_fifo_and_exact_durable_receipts() {
    let temp = tempdir::TempDir::new("control-fifo").unwrap();
    let config = config(&temp.path().join("node"));
    let node = open_synchronous(&config);
    node.set_history_persistence(crate::distributed::storage::HistoryPersistence::OnDisk)
        .unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let (completed, completions) = mpsc::sync_channel(7);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let requests = [
        Request::Checkpoint {
            authority_epoch: 1,
            epoch: 1,
            prefix: 1,
        },
        Request::Seal { epoch: 1 },
        append_request(&config, 1, 1),
        Request::Fence { epoch: 2 },
        append_request(&config, 2, 0),
        Request::Seal { epoch: 2 },
    ];
    for (index, request) in requests.into_iter().enumerate() {
        assert!(command(&node, index as u64 + 2, request, 4096, &completed).is_ok());
    }
    assert!(matches!(
        completions.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    let Response::Appended(first) = receive(&completions, 1).response.unwrap() else {
        panic!("append")
    };
    assert_eq!(first[0].id, batch(&config, 1, 0).id());
    let Response::Checkpoint(proof) = receive(&completions, 2).response.unwrap() else {
        panic!("checkpoint")
    };
    assert_eq!(proof.prefix, 1);
    assert_eq!(proof.retained, 1);
    assert!(matches!(
        receive(&completions, 3).response.unwrap(),
        Response::Sealed
    ));
    assert!(
        receive(&completions, 4).response.is_err(),
        "sealed append cannot succeed"
    );
    assert!(matches!(
        receive(&completions, 5).response.unwrap(),
        Response::Status(_)
    ));
    let Response::Appended(second) = receive(&completions, 6).response.unwrap() else {
        panic!("append")
    };
    assert_eq!(second[0].id, batch(&config, 2, 0).id());
    assert!(matches!(
        receive(&completions, 7).response.unwrap(),
        Response::Sealed
    ));
    assert_eq!(log.stats().appended_groups, 1);
    assert_eq!(log.stats().persistence_barriers, 1);
}

#[test]
fn active_commands_retain_both_queue_bounds() {
    for bytes_full in [false, true] {
        let temp = tempdir::TempDir::new("control-bounds").unwrap();
        let config = config(&temp.path().join("node"));
        let node = open_synchronous(&config);
        node.inventory.lock().fence_to(1).unwrap();
        let log = node.inventory.lock().active(1).unwrap();
        let (entered, release) = log.pause_next_persist();
        let (completed, completions) = mpsc::sync_channel(MAX_COMMANDS);
        let charge = if bytes_full { MAX_COMMAND_BYTES } else { 1 };
        assert!(command(&node, 1, append_request(&config, 1, 0), charge, &completed).is_ok());
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let accepted = if bytes_full { 1 } else { MAX_COMMANDS };
        for id in 2..=accepted {
            assert!(command(&node, id as u64, Request::Stats, 1, &completed).is_ok());
        }
        let rejected = command(&node, 99, Request::Seal { epoch: 1 }, 1, &completed);
        assert!(rejected.is_err());
        let work = rejected.err().unwrap();
        assert_eq!(work.id, 99);
        assert!(matches!(work.request, Request::Seal { epoch: 1 }));
        release.send(()).unwrap();
        for id in 1..=accepted {
            assert!(receive(&completions, id as u64).response.is_ok());
        }
    }
}

#[test]
fn panic_after_append_fails_active_and_later_commands_closed() {
    let temp = tempdir::TempDir::new("control-panic").unwrap();
    let config = config(&temp.path().join("node"));
    let node = open_synchronous(&config);
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    // Hold both appends at inventory acquisition until every command has been
    // accepted, so both are in flight when the first one to finish panics.
    let inventory = node.inventory.lock();
    node.inject_panic_after_next_append();
    let (completed, completions) = mpsc::sync_channel(3);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    assert!(command(&node, 2, append_request(&config, 1, 1), 4096, &completed).is_ok());
    assert!(command(&node, 3, Request::Seal { epoch: 1 }, 1, &completed).is_ok());
    drop(inventory);
    let mut outcomes = std::collections::HashMap::new();
    let mut order = Vec::new();
    for _ in 0..3 {
        let completion = completions.recv_timeout(Duration::from_secs(5)).unwrap();
        order.push(completion.id);
        outcomes.insert(completion.id, completion.response.is_err());
    }
    // A poisoned authority fails the seal at once; an append still waiting on the
    // flusher may complete after it.
    assert_eq!(order.len(), 3);
    // The panicking append and every command behind the poisoned authority fail;
    // the overlapping append either finished before the panic or failed closed.
    assert!(outcomes[&1] || outcomes[&2]);
    assert!(outcomes[&3]);
    assert!(log.stats().appended_groups <= 2);
    // The non-panicking append may already have been synced before the failure.
    assert!(node.status().is_err());
}

#[test]
fn caller_cancellation_and_owner_drop_drain_accepted_writes() {
    let temp = tempdir::TempDir::new("control-cancel-drain").unwrap();
    let config = config(&temp.path().join("node"));
    let node = open_synchronous(&config);
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let (completed, completions) = mpsc::sync_channel(1);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(command(&node, 2, append_request(&config, 1, 1), 4096, &completed).is_ok());
    let second = batch(&config, 1, 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    while log.lookup(second.id()).unwrap().is_none() && Instant::now() < deadline {
        thread::yield_now();
    }
    let appended = log.lookup(second.id()).unwrap().unwrap();
    let reference = second.record_reference(appended.address(), 0).unwrap();
    assert!(matches!(
        log.validate_durable_record_reference(&reference),
        Err(StorageError::NeedsPersistence)
    ));
    drop(completions); // Delivery cancellation must not cancel the admitted WAL write.
    let (stopped, stopping) = mpsc::sync_channel(1);
    let dropping = thread::spawn(move || {
        drop(node);
        stopped.send(()).unwrap();
    });
    assert!(matches!(
        stopping.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    release.send(()).unwrap();
    stopping.recv_timeout(Duration::from_secs(5)).unwrap();
    dropping.join().unwrap();
    for sequence in 0..2 {
        let batch = batch(&config, 1, sequence);
        let appended = log.lookup(batch.id()).unwrap().unwrap();
        log.validate_durable_record_reference(
            &batch.record_reference(appended.address(), 0).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(log.stats().appended_groups, 2);
    assert_eq!(log.stats().persistence_barriers, 2);
}

#[test]
fn retirement_controls_require_current_authority_and_an_older_sealed_stream() {
    let temp = tempdir::TempDir::new("control-retirement-authority").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let (completed, completions) = mpsc::sync_channel(1);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    assert!(matches!(
        receive(&completions, 1).response.unwrap(),
        Response::Appended(_)
    ));
    assert!(command(&node, 2, Request::Fence { epoch: 2 }, 1, &completed).is_ok());
    assert!(matches!(
        receive(&completions, 2).response.unwrap(),
        Response::Status(_)
    ));
    assert!(
        command(
            &node,
            3,
            Request::RetirementFiles {
                authority_epoch: 2,
                epoch: 1,
                after: None,
                limit: 1
            },
            1,
            &completed,
        )
        .is_ok()
    );
    let Response::RetirementFiles(files) = receive(&completions, 3).response.unwrap() else {
        panic!("wrong retirement page response")
    };
    assert_eq!(files.len(), 1);
    let requests = [
        Request::RetirementFiles {
            authority_epoch: 1,
            epoch: 1,
            after: None,
            limit: 1,
        },
        Request::RetirementFiles {
            authority_epoch: 2,
            epoch: 2,
            after: None,
            limit: 1,
        },
        Request::RetirementFiles {
            authority_epoch: 2,
            epoch: 0,
            after: None,
            limit: 1,
        },
        Request::RetirementFiles {
            authority_epoch: 2,
            epoch: 1,
            after: None,
            limit: 257,
        },
        Request::RetireFiles {
            authority_epoch: 1,
            epoch: 1,
            prefix: 1,
            files: files.clone(),
        },
        Request::RetireFiles {
            authority_epoch: 2,
            epoch: 2,
            prefix: 1,
            files: files.clone(),
        },
        Request::RetireFiles {
            authority_epoch: 2,
            epoch: 1,
            prefix: 1,
            files: vec![files[0]; 257],
        },
    ];
    for (index, request) in requests.into_iter().enumerate() {
        let id = index as u64 + 4;
        assert!(command(&node, id, request, 4096, &completed).is_ok());
        assert!(receive(&completions, id).response.is_err());
    }
    assert!(
        node.status().is_ok(),
        "rejected requests have no retirement effects"
    );
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn live_resolution_rejects_forged_reference_and_stale_authority() {
    let temp = tempdir::TempDir::new("live-owner-reference").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let (completed, completions) = mpsc::sync_channel(1);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    let Response::Appended(receipts) = receive(&completions, 1).response.unwrap() else {
        panic!("append failed")
    };
    let reference = batch(&config, 1, 0)
        .record_reference(receipts[0].address, 0)
        .unwrap();
    let mut forged = reference;
    forged.descriptor.checksum.0 ^= 1;
    for (id, epoch, reference) in [(2, 1, forged), (3, 2, reference)] {
        assert!(
            command(
                &node,
                id,
                Request::ResolveLiveRecords {
                    authority_epoch: epoch,
                    references: vec![reference],
                },
                4096,
                &completed
            )
            .is_ok()
        );
        assert!(receive(&completions, id).response.is_err());
    }
    assert!(node.status().is_ok());
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
#[test]
fn unknown_live_pin_release_is_not_acknowledged() {
    let temp = tempdir::TempDir::new("live-unknown-pin").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config, metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let (completed, completions) = mpsc::sync_channel(1);
    assert!(
        command(
            &node,
            1,
            Request::ReleaseLiveEpochPin {
                epoch: 1,
                token: [42; 16],
            },
            1,
            &completed
        )
        .is_ok()
    );
    assert!(receive(&completions, 1).response.is_err());
    assert!(node.status().is_ok());
}

#[test]
fn synchronous_group_commit_replies_only_after_the_covering_sync() {
    let temp = tempdir::TempDir::new("control-group-commit").unwrap();
    let config = config(&temp.path().join("node"));
    let node = open_synchronous(&config);
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let (completed, completions) = mpsc::sync_channel(8);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Three more groups append while the first sync is held.
    for sequence in 1..4 {
        assert!(
            command(
                &node,
                sequence + 1,
                append_request(&config, 1, sequence),
                4096,
                &completed
            )
            .is_ok()
        );
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while log.stats().appended_groups < 4 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(log.stats().appended_groups, 4);
    // No receipt precedes the barrier that covers it.
    assert!(matches!(
        completions.recv_timeout(Duration::from_millis(200)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert_eq!(log.synced_frontier(), 0);
    release.send(()).unwrap();
    // Parallel append workers hand groups to the flusher in any order.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..4 {
        let completion = completions.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            completion.response.unwrap(),
            Response::Appended(_)
        ));
        seen.insert(completion.id);
        // Every receipt that is returned is already covered by a completed sync.
        assert!(log.synced_frontier() > 0);
    }
    assert_eq!(seen, (1..=4).collect());
    // One sync for the first group, one shared by the three queued behind it.
    assert_eq!(log.stats().persistence_barriers, 2);
    assert_eq!(log.synced_frontier(), log.appended_frontier());
}

#[test]
fn native_tier_replies_before_the_sync_and_controls_flush_first() {
    let temp = tempdir::TempDir::new("control-native-tier").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    assert_eq!(node.durability().tier, DurabilityTier::Native);
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let (completed, completions) = mpsc::sync_channel(4);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    // The reply does not wait for a sync, so it arrives with none completed.
    assert!(matches!(
        receive(&completions, 1).response.unwrap(),
        Response::Appended(_)
    ));
    assert!(log.synced_frontier() < log.appended_frontier());
    // A control flushes everything acknowledged before it runs; the held sync
    // therefore holds the control.
    assert!(command(&node, 2, Request::Fence { epoch: 2 }, 1, &completed).is_ok());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        completions.recv_timeout(Duration::from_millis(200)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release.send(()).unwrap();
    assert!(matches!(
        receive(&completions, 2).response.unwrap(),
        Response::Status(_)
    ));
    assert_eq!(log.synced_frontier(), log.appended_frontier());
}

#[test]
fn durability_cannot_change_after_the_first_append() {
    let temp = tempdir::TempDir::new("control-tier-fixed").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let (completed, completions) = mpsc::sync_channel(1);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    receive(&completions, 1).response.unwrap();
    assert!(
        node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Synchronous))
            .is_err()
    );
    assert_eq!(node.durability().tier, DurabilityTier::Native);
}

#[test]
fn native_checkpoint_proof_is_issued_only_after_its_prefix_is_synced() {
    let temp = tempdir::TempDir::new("control-native-checkpoint").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.set_history_persistence(crate::distributed::storage::HistoryPersistence::OnDisk)
        .unwrap();
    node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (completed, completions) = mpsc::sync_channel(4);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    receive(&completions, 1).response.unwrap();
    let group_end = log.appended_frontier();
    let request = Request::Checkpoint {
        authority_epoch: 1,
        epoch: 1,
        prefix: 1,
    };
    assert!(command(&node, 2, request, 1, &completed).is_ok());
    let Response::Checkpoint(proof) = receive(&completions, 2).response.unwrap() else {
        panic!("checkpoint")
    };
    assert_eq!(proof.prefix, 1);
    // The proof the coordinator needs before it commits an image exists only
    // once the owner's synced frontier covers the referenced prefix.
    assert!(log.synced_frontier() >= group_end);
}

#[test]
fn native_ack_paused_before_hand_off_fails_after_a_concurrent_persistence_failure() {
    let temp = tempdir::TempDir::new("control-native-paused-ack").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Native))
        .unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, resume) = node.control.pause_before_next_ack();
    let (completed, completions) = mpsc::sync_channel(2);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // The group is appended; a direct persistence now fails and poisons the log.
    log.fail_next_persist();
    assert!(log.persist_appended().is_err());
    resume.send(()).unwrap();
    // The held append may not be acknowledged after the failure.
    assert!(receive(&completions, 1).response.is_err());
}

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod live_fencing {
    use super::*;
    use crate::distributed::nvmeof::live_storage::{LiveGroupDescriptor, LiveReserveResult};
    use crate::distributed::nvmeof::live_target::LiveTargetManager;
    use crate::distributed::nvmeof::{LiveTargetConfig, NvmeofTransport};
    use std::os::unix::fs::FileExt;

    // Exercise the actual WAL/control lifecycle with the existing journal-only
    // target fixture. This is not evidence of kernel namespace I/O drain.
    fn open_live(root: &std::path::Path, tier: DurabilityTier) -> (NodeConfig, Arc<StorageNode>) {
        open_live_mode(root, tier, LiveExportMode::OnDemand)
    }

    fn open_live_mode(
        root: &std::path::Path,
        tier: DurabilityTier,
        mode: LiveExportMode,
    ) -> (NodeConfig, Arc<StorageNode>) {
        let mut config = config(&root.join("node"));
        // A complete 4 KiB live group stays below the native fragment trigger.
        config.partition.fragment_bytes = 16 * 1024;
        config.partition.wal_file_bytes = 32 * 1024;
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        node.set_durability(DurabilityConfig::with_tier(tier))
            .unwrap();
        let state_dir = root.join("target");
        let configfs = root.join("configfs");
        fs::create_dir(&state_dir).unwrap();
        fs::create_dir(&configfs).unwrap();
        for directory in ["hosts", "ports", "subsystems"] {
            fs::create_dir(configfs.join(directory)).unwrap();
        }
        let target = LiveTargetManager::open_fixture(
            LiveTargetConfig {
                address: "127.0.0.1".into(),
                service: 4420,
                transport: NvmeofTransport::Tcp,
                host_nqn: "nqn.2026-01.local.test:live-fence".into(),
                port_id: 42,
                state_dir,
                commissioning_port_id: 43,
                commissioning_service: 4421,
                spdk: None,
                export_mode: mode,
            },
            configfs,
        )
        .unwrap();
        {
            let mut inventory = node.inventory.lock();
            inventory.live_target = Some(target);
            inventory.fence_to(1).unwrap();
        }
        (config, node)
    }

    #[test]
    fn permanent_export_survives_read_pin_release_and_idle_reaping() {
        let temp = tempdir::TempDir::new("permanent-pin").unwrap();
        let (config, node) = open_live_mode(
            temp.path(),
            DurabilityTier::Native,
            LiveExportMode::PerOwner,
        );
        let batch = batch(&config, 1, 0);
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch.clone()],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        let Response::LiveLeased(leased) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected lease")
        };
        fs::OpenOptions::new()
            .write(true)
            .open(&leased.extent.segment.path)
            .unwrap()
            .write_all_at(&frame, leased.extent.start % leased.extent.segment.bytes)
            .unwrap();
        let mut skip = [0; 4096];
        skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
        fs::OpenOptions::new()
            .write(true)
            .open(&leased.extent.segment.path)
            .unwrap()
            .write_all_at(
                &skip,
                (leased.extent.start + frame.len() as u64) % leased.extent.segment.bytes,
            )
            .unwrap();
        extent_exchange(
            &node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: leased.extent.token,
                through: leased.extent.start + frame.len() as u64,
                close: true,
                lease_next: false,
            },
        );
        let log = node.inventory.lock().active(1).unwrap();
        let receipt = log.receipt_page(0, 1).unwrap()[0];
        let reference = batch.record_reference(receipt.address(), 0).unwrap();
        let lease = log.resolve_live_record(&reference).unwrap();
        let token = [7; 16];
        node.inventory
            .lock()
            .live_pins
            .insert(token, (1, vec![lease]));
        node.inventory.lock().release_live_pin(1, token).unwrap();
        assert!(!node.inventory.lock().live_pins.contains_key(&token));
        node.inventory.lock().reap_idle_live_segments(1).unwrap();
        assert_eq!(
            node.inventory
                .lock()
                .live_target
                .as_ref()
                .unwrap()
                .stats()
                .unwrap()
                .owned_namespace_count,
            1
        );
        shutdown(&node, 1);
    }

    #[test]
    fn open_live_exports_default_is_empty_and_permanent_exports_are_idempotent() {
        for mode in [
            LiveExportMode::OnDemand,
            LiveExportMode::PerFile,
            LiveExportMode::PerOwner,
        ] {
            let temp = tempdir::TempDir::new("open-live-exports").unwrap();
            let (_, node) = open_live_mode(temp.path(), DurabilityTier::Native, mode);
            let Response::LiveLeased(leased) =
                extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
            else {
                panic!("expected lease")
            };
            let permanent = mode != LiveExportMode::OnDemand;
            let expected_namespaces = if permanent { 2 } else { 1 };
            if permanent {
                let mut current = leased.clone();
                for _ in 0..2 {
                    let mut skip = [0; 4096];
                    skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&current.extent.segment.path)
                        .unwrap()
                        .write_all_at(&skip, current.extent.start % current.extent.segment.bytes)
                        .unwrap();
                    let Response::LiveAdopted(adopted) = extent_exchange(
                        &node,
                        Request::AdoptLiveExtent {
                            epoch: 1,
                            token: current.extent.token,
                            through: current.extent.start,
                            close: true,
                            lease_next: true,
                        },
                    ) else {
                        panic!("expected next lease")
                    };
                    current = adopted.next.unwrap();
                }
                assert_eq!(current.extent.segment.file_index, 1);
            }
            for _ in 0..2 {
                let Response::LiveExports(info) =
                    extent_exchange(&node, Request::OpenLiveExports { authority_epoch: 1 })
                else {
                    panic!("expected exports")
                };
                assert_eq!(info.mode, mode);
                assert_eq!(info.exported_files, if permanent { 2 } else { 0 });
                assert_eq!(info.wal_file_bytes, 32768);
                let inventory = node.inventory.lock();
                assert_eq!(
                    inventory
                        .live_target
                        .as_ref()
                        .unwrap()
                        .stats()
                        .unwrap()
                        .namespaces_exposed,
                    expected_namespaces
                );
                assert_eq!(
                    inventory
                        .live_target
                        .as_ref()
                        .unwrap()
                        .stats()
                        .unwrap()
                        .owned_namespace_count,
                    expected_namespaces
                );
                assert_eq!(leased.binding.permanent, mode != LiveExportMode::OnDemand);
            }
            node.inventory.lock().reap_idle_live_segments(1).unwrap();
            assert_eq!(
                node.inventory
                    .lock()
                    .live_target
                    .as_ref()
                    .unwrap()
                    .stats()
                    .unwrap()
                    .owned_namespace_count,
                expected_namespaces
            );
            shutdown(&node, 1);
        }
    }

    #[test]
    fn permanent_owner_rejects_reserved_writes_and_active_record_resolution() {
        let temp = tempdir::TempDir::new("permanent-controls").unwrap();
        let (config, node) = open_live_mode(
            temp.path(),
            DurabilityTier::Native,
            LiveExportMode::PerOwner,
        );
        let batch = batch(&config, 1, 0);
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch.clone()],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (descriptor, _) = LiveGroupDescriptor::from_group(&group).unwrap();
        let reference = batch
            .record_reference(
                PhysicalValueAddress {
                    partition: config.partition.partition,
                    segment_generation: 1,
                    offset: 0,
                    len: (batch.encoded().len() + 8) as u32,
                },
                0,
            )
            .unwrap();
        for (request, message) in [
            (
                Request::ReserveLiveGroup {
                    epoch: 1,
                    descriptor,
                },
                "permanent live export requires leased write mode",
            ),
            (
                Request::ResolveLiveRecords {
                    authority_epoch: 1,
                    references: vec![reference],
                },
                "live record resolution is disabled for the active epoch in permanent export mode",
            ),
        ] {
            let error = node.handle(request, TcpLimits::default()).err().unwrap();
            assert_eq!(error.to_string(), message);
        }
        shutdown(&node, 1);
    }

    fn reserve(
        node: &StorageNode,
        config: &NodeConfig,
        epoch: u64,
        sequence: u64,
    ) -> (LiveReserved, Vec<u8>) {
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch(config, epoch, sequence)],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (descriptor, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(
            command(
                node,
                1,
                Request::ReserveLiveGroup { epoch, descriptor },
                4096,
                &completed,
            )
            .is_ok()
        );
        let Response::LiveReserved(live_control::LiveReserveWire::Reserved(reserved)) =
            receive(&completions, 1).response.unwrap()
        else {
            panic!("expected new live reservation")
        };
        (reserved, frame)
    }

    fn complete(node: &StorageNode, reserved: &LiveReserved, frame: &[u8]) -> Receipt {
        let reservation = &reserved.reservation;
        fs::OpenOptions::new()
            .write(true)
            .open(&reservation.segment.path)
            .unwrap()
            .write_all_at(frame, reservation.frame_offset % reservation.segment.bytes)
            .unwrap();
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(
            command(
                node,
                1,
                Request::CompleteLiveGroup {
                    epoch: reservation.segment.epoch,
                    token: reservation.token,
                },
                4096,
                &completed,
            )
            .is_ok()
        );
        let Response::LiveCompleted(completion) = receive(&completions, 1).response.unwrap() else {
            panic!("expected completed live group")
        };
        assert_eq!(completion.receipts.len(), 1);
        completion.receipts[0]
    }

    fn fence(node: &StorageNode, epoch: u64) -> Result<Response, TcpError> {
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(command(node, 1, Request::Fence { epoch }, 1, &completed).is_ok());
        receive(&completions, 1).response
    }

    fn shutdown(node: &StorageNode, epoch: u64) {
        let (completed, completions) = mpsc::sync_channel(1);
        for request in [
            Request::Seal { epoch },
            Request::ShutdownLiveTarget { epoch },
        ] {
            assert!(command(node, 1, request, 1, &completed).is_ok());
            let response = receive(&completions, 1).response.unwrap();
            if let Response::LiveTargetStats(stats) = response {
                assert!(stats.fixture_only);
                assert_eq!(stats.owned_namespace_count, 0);
            } else {
                assert!(matches!(response, Response::Sealed));
            }
        }
    }

    fn assert_recovered(node: &StorageNode, config: &NodeConfig, receipts: &[Receipt]) {
        let log = node
            .inventory
            .lock()
            .live_recovery_log(2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(log.receipt_count(), receipts.len());
        for (sequence, receipt) in receipts.iter().enumerate() {
            let reference = batch(config, 1, sequence as u64)
                .record_reference(receipt.address, 0)
                .unwrap();
            log.validate_durable_record_reference(&reference).unwrap();
            assert!(matches!(
                log.read_record(&reference).unwrap(),
                crate::distributed::codec::Mutation::Put { key, value, .. }
                    if key.as_ref() == b"key1"
                        && value.as_ref() == vec![sequence as u8; 32].as_slice()
            ));
        }
    }

    #[test]
    fn higher_live_fence_joins_only_the_old_syncer_and_keeps_noop_fences_safe() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let temp = tempdir::TempDir::new("live-fence-syncer").unwrap();
            let (config, node) = open_live(temp.path(), tier);
            let (reserved, frame) = reserve(&node, &config, 1, 0);
            let first = complete(&node, &reserved, &frame);
            let log = node.inventory.lock().active(1).unwrap();
            let old = Arc::downgrade(&log);
            let frontier = log.frontier().unwrap().clone();
            if tier == DurabilityTier::Native {
                assert!(frontier.synced() < frontier.appended());
            } else {
                assert_eq!(frontier.synced(), frontier.appended());
            }
            drop(log);
            for epoch in [1, 0] {
                let response = fence(&node, epoch);
                assert_eq!(response.is_ok(), epoch == 1);
                assert_eq!(node.control.shared.syncers.lock().len(), 1);
            }
            // A later real completion proves the no-op/rejected fences did not
            // stop the old frontier. Its next control must still be able to sync.
            let (reserved, frame) = reserve(&node, &config, 1, 1);
            let second = complete(&node, &reserved, &frame);
            let Response::Status(status) = fence(&node, 2).unwrap() else {
                panic!("expected fenced status")
            };
            assert_eq!(status.active_epoch, Some(2));
            assert!(
                old.upgrade().is_none(),
                "old partition handle must be released"
            );
            assert_eq!(frontier.synced(), frontier.appended());
            assert!(node.control.shared.syncers.lock().is_empty());
            let other_tier = if tier == DurabilityTier::Native {
                DurabilityTier::Synchronous
            } else {
                DurabilityTier::Native
            };
            assert!(
                node.set_durability(DurabilityConfig::with_tier(other_tier))
                    .is_err()
            );
            assert_recovered(&node, &config, &[first, second]);
            let (reserved, frame) = reserve(&node, &config, 2, 0);
            complete(&node, &reserved, &frame);
            assert_eq!(node.control.shared.syncers.lock().len(), 1);
            shutdown(&node, 2);
        }
    }

    #[test]
    fn recoverable_live_fence_releases_used_wal_after_an_abandoned_reservation() {
        let temp = tempdir::TempDir::new("live-recoverable-syncer").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let (reserved, frame) = reserve(&node, &config, 1, 0);
        let receipt = complete(&node, &reserved, &frame);
        let log = node.inventory.lock().active(1).unwrap();
        let old = Arc::downgrade(&log);
        assert!(log.synced_frontier() < log.appended_frontier());
        let frontier = log.frontier().unwrap().clone();
        // A reserve worker may pass its preliminary flush while a completion
        // worker is still publishing the preceding group. Reserve directly at
        // the same storage boundary to reproduce that overlap deterministically:
        // the acknowledged group stays unsynced when the next frame is absent.
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch(&config, 1, 1)],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (descriptor, _) = LiveGroupDescriptor::from_group(&group).unwrap();
        let LiveReserveResult::Reserved(abandoned) = log
            .reserve_live_group(RequestAuthority { epoch: 1 }, descriptor)
            .unwrap()
        else {
            panic!("expected abandoned reservation")
        };
        let (sync_entered, resume_sync) = log.pause_next_persist();
        drop(log);
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(
            command(
                &node,
                1,
                Request::CompleteLiveGroup {
                    epoch: 1,
                    token: abandoned.token,
                },
                4096,
                &completed,
            )
            .is_ok()
        );
        assert!(receive(&completions, 1).response.is_err());
        assert!(
            node.control_context
                .recoverable_live_failure
                .load(Ordering::Acquire)
        );
        assert!(frontier.synced() < frontier.appended());
        assert!(command(&node, 2, Request::Fence { epoch: 2 }, 1, &completed).is_ok());
        sync_entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            completions.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        resume_sync.send(()).unwrap();
        let Response::Status(status) = receive(&completions, 2).response.unwrap() else {
            panic!("expected recovered authority")
        };
        assert_eq!(frontier.synced(), frontier.appended());
        assert_eq!(status.active_epoch, Some(2));
        assert!(status.healthy);
        assert!(
            old.upgrade().is_none(),
            "syncer and allocation must release old WAL"
        );
        assert!(node.control.shared.syncers.lock().is_empty());
        assert!(!node.control_context.terminal.load(Ordering::Acquire));
        assert_recovered(&node, &config, &[receipt]);
        shutdown(&node, 2);
    }

    #[test]
    fn live_fence_keeps_the_external_handle_guard_after_joining_the_syncer() {
        let temp = tempdir::TempDir::new("live-fence-external-handle").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let (reserved, frame) = reserve(&node, &config, 1, 0);
        complete(&node, &reserved, &frame);
        let retained = node.inventory.lock().active(1).unwrap();
        let error = fence(&node, 2).err().unwrap();
        assert!(
            error
                .to_string()
                .contains("old live stream still has outstanding handles")
        );
        assert!(node.control.shared.syncers.lock().is_empty());
        assert!(node.inventory.lock().status().is_err());
        assert_eq!(retained.synced_frontier(), retained.appended_frontier());
    }
    #[test]
    fn leased_control_publishes_writable_binding_and_never_waits_for_adoption_sync() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let temp = tempdir::TempDir::new("live-extent-control").unwrap();
            let (config, node) = open_live(temp.path(), tier);
            let (completed, completions) = mpsc::sync_channel(1);
            let request = Request::LeaseLiveExtent { epoch: 1 };
            assert!(is_stream_request(&request));
            let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&request).unwrap();
            let request: Request = wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap();
            assert!(matches!(request, Request::LeaseLiveExtent { epoch: 1 }));
            assert!(command(&node, 1, request, 4096, &completed).is_ok());
            let response = receive(&completions, 1).response.unwrap();
            let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&response).unwrap();
            let Response::LiveLeased(leased) =
                wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap()
            else {
                panic!("expected leased extent")
            };
            assert!(!leased.binding.readonly);
            assert_eq!(leased.tier, tier);
            assert_eq!(
                leased.extent.end - leased.extent.start,
                config.partition.fragment_bytes
            );
            let group = CompleteGroup::new_live(
                &node.control_context.append_shape,
                vec![batch(&config, 1, 0)],
                config.partition.batch_limits,
                config.partition.group_limits,
            )
            .unwrap();
            let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(&leased.extent.segment.path)
                .unwrap()
                .write_all_at(&frame, leased.extent.start % leased.extent.segment.bytes)
                .unwrap();
            let log = node.log_for_test(1);
            let (entered, resume) = log.pause_next_persist();
            let request = Request::AdoptLiveExtent {
                epoch: 1,
                token: leased.extent.token,
                through: leased.extent.start + frame.len() as u64,
                close: false,
                lease_next: false,
            };
            assert!(is_stream_request(&request));
            let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&request).unwrap();
            let request = wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap();
            assert!(command(&node, 2, request, 4096, &completed).is_ok());
            // This receive must finish even when the owner's barrier is paused.
            let response = receive(&completions, 2).response.unwrap();
            let encoded = wire_codec(HARD_FRAME_LIMIT).serialize(&response).unwrap();
            let Response::LiveAdopted(reply) =
                wire_codec(HARD_FRAME_LIMIT).deserialize(&encoded).unwrap()
            else {
                panic!("expected adoption proof")
            };
            assert_eq!(reply.adoption.receipt_count, 1);
            assert_eq!(reply.adoption.token, leased.extent.token);
            assert!(reply.next.is_none());
            if tier == DurabilityTier::Synchronous {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(log.frontier().unwrap().stats().flush_requests, 1);
                assert_eq!(log.synced_frontier(), 0);
                resume.send(()).unwrap();
            } else {
                assert_eq!(log.frontier().unwrap().stats().flush_requests, 0);
                assert_eq!(log.stats().persistence_barriers, 0);
            }
            let mut skip = [0; 4096];
            skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
            fs::OpenOptions::new()
                .write(true)
                .open(&leased.extent.segment.path)
                .unwrap()
                .write_all_at(&skip, frame.len() as u64)
                .unwrap();
            let request = Request::AdoptLiveExtent {
                epoch: 1,
                token: leased.extent.token,
                through: frame.len() as u64,
                close: true,
                lease_next: true,
            };
            assert!(command(&node, 3, request, 4096, &completed).is_ok());
            let Response::LiveAdopted(closed) = receive(&completions, 3).response.unwrap() else {
                panic!("expected close and next extent")
            };
            assert!(closed.adoption.closed);
            assert_eq!(closed.adoption.adopted_end, frame.len() as u64);
            let next = closed.next.unwrap();
            assert_eq!(next.extent.start, leased.extent.end);
            assert_eq!(next.tier, tier);
            if tier == DurabilityTier::Native {
                entered.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(log.frontier().unwrap().stats().flush_requests, 1);
                assert_eq!(log.synced_frontier(), 0);
                resume.send(()).unwrap();
            }
            drop(log);
            shutdown(&node, 1);
        }
    }
    #[test]
    fn leased_native_adoptions_defer_explicit_sync_until_extent_close() {
        let temp = tempdir::TempDir::new("live-native-adoption-policy").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let Response::LiveLeased(leased) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected native extent")
        };
        let log = node.log_for_test(1);
        let frontier = log.frontier().unwrap();
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&leased.extent.segment.path)
            .unwrap();
        for sequence in 0..3 {
            let group = CompleteGroup::new_live(
                &node.control_context.append_shape,
                vec![batch(&config, 1, sequence)],
                config.partition.batch_limits,
                config.partition.group_limits,
            )
            .unwrap();
            let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
            file.write_all_at(&frame, sequence * 4096).unwrap();
            let Response::LiveAdopted(reply) = extent_exchange(
                &node,
                Request::AdoptLiveExtent {
                    epoch: 1,
                    token: leased.extent.token,
                    through: (sequence + 1) * 4096,
                    close: false,
                    lease_next: false,
                },
            ) else {
                panic!("expected native adoption")
            };
            assert_eq!(reply.adoption.receipt_count, sequence + 1);
            assert_eq!(frontier.stats().flush_requests, 0);
            assert_eq!(frontier.stats().fsync_calls, 0);
            assert_eq!(log.stats().persistence_barriers, 0);
            assert_eq!(log.synced_frontier(), 0);
        }
        let mut marker = [0; 4096];
        marker[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
        file.write_all_at(&marker, 3 * 4096).unwrap();
        let (entered, resume) = log.pause_next_persist();
        let Response::LiveAdopted(closed) = extent_exchange(
            &node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: leased.extent.token,
                through: 3 * 4096,
                close: true,
                lease_next: false,
            },
        ) else {
            panic!("expected native close")
        };
        assert!(closed.adoption.closed);
        assert_eq!(frontier.stats().flush_requests, 1);
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(log.stats().persistence_barriers, 0);
        resume.send(()).unwrap();
        frontier
            .wait_synced(3 * 4096, Some(Instant::now() + Duration::from_secs(5)))
            .unwrap();
        assert_eq!(frontier.stats().flush_requests, 1);
        assert_eq!(log.stats().persistence_barriers, 1);
        drop(log);
        shutdown(&node, 1);
    }

    fn extent_exchange(node: &StorageNode, request: Request) -> Response {
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(command(node, 1, request, 4096, &completed).is_ok());
        receive(&completions, 1).response.unwrap()
    }

    #[test]
    fn leased_checkpoint_and_grants_tail_frames_but_recovery_stays_immutable() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let temp = tempdir::TempDir::new("live-extent-tail-controls").unwrap();
            let (config, node) = open_live(temp.path(), tier);
            node.set_history_persistence(crate::distributed::storage::HistoryPersistence::OnDisk)
                .unwrap();
            let Response::LiveLeased(leased) =
                extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
            else {
                panic!("expected extent")
            };
            let file = fs::OpenOptions::new()
                .write(true)
                .open(&leased.extent.segment.path)
                .unwrap();
            let mut groups = Vec::new();
            for n in 0..3 {
                let group = CompleteGroup::new_live(
                    &node.control_context.append_shape,
                    vec![batch(&config, 1, n)],
                    config.partition.batch_limits,
                    config.partition.group_limits,
                )
                .unwrap();
                let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
                if n < 2 {
                    file.write_all_at(&frame, n * 4096).unwrap();
                }
                groups.push((group, frame));
            }
            let (completed, completions) = mpsc::sync_channel(1);
            assert!(
                command(
                    &node,
                    1,
                    Request::RecoverLiveMetadata {
                        authority_epoch: 1,
                        source_epoch: 1,
                        cursor: 0,
                        limit: 2,
                    },
                    4096,
                    &completed
                )
                .is_ok()
            );
            let error = receive(&completions, 1).response.err().unwrap();
            assert!(
                error
                    .to_string()
                    .contains("recovery requires sealed immutable epoch")
            );
            let Response::Checkpoint(proof) = extent_exchange(
                &node,
                Request::Checkpoint {
                    authority_epoch: 1,
                    epoch: 1,
                    prefix: 2,
                },
            ) else {
                panic!("expected checkpoint proof")
            };
            assert_eq!(proof.retained, 2);
            assert_eq!(proof.prefix, 2);
            let log = node.log_for_test(1);
            assert_eq!(log.synced_frontier(), 8192);
            // The next grant must discover a frame written after the proof.
            file.write_all_at(&groups[2].1, 8192).unwrap();
            let item = &groups[2].0.batches()[0];
            let receipt = crate::distributed::storage::receipt_for(
                log.config(),
                item,
                crate::distributed::storage::member_position(
                    crate::WalPosition::new(8192, groups[2].1.len() as u32),
                    groups[2].0.batch_offsets()[0],
                    item,
                ),
            );
            let reference = item.record_reference(receipt.address(), 0).unwrap();
            let Response::LiveRecords(locations) = extent_exchange(
                &node,
                Request::ResolveLiveRecords {
                    authority_epoch: 1,
                    references: vec![reference],
                },
            ) else {
                panic!("expected read grant")
            };
            assert_eq!(locations.entries.len(), 1);
            assert_eq!(locations.entries[0].reference, reference);
            assert_eq!(log.synced_frontier(), 12288);
            extent_exchange(
                &node,
                Request::ReleaseLiveEpochPin {
                    epoch: 1,
                    token: locations.epoch_pin_token,
                },
            );
            drop(log);
            shutdown(&node, 1);
        }
    }

    #[test]
    fn leased_fence_recovers_only_after_revocation_and_never_regrants_old_generation() {
        let temp = tempdir::TempDir::new("live-extent-fence").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Synchronous);
        let Response::LiveLeased(old) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected first extent")
        };
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch(&config, 1, 0)],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&old.extent.segment.path)
            .unwrap()
            .write_all_at(&frame, 0)
            .unwrap();
        let Response::Status(status) = fence(&node, 2).unwrap() else {
            panic!("expected fenced authority")
        };
        assert_eq!(status.active_epoch, Some(2));
        let Response::LiveLeased(next) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 2 })
        else {
            panic!("expected new epoch extent")
        };
        assert_ne!(
            next.extent.segment.generation,
            old.extent.segment.generation
        );
        assert_ne!(next.extent.segment.path, old.extent.segment.path);
        assert_ne!(next.binding.uuid, old.binding.uuid);
        shutdown(&node, 2);
    }
    #[test]
    fn leased_pregrant_persistence_does_not_hold_inventory_ownership() {
        let temp = tempdir::TempDir::new("live-lease-unlocked-inventory").unwrap();
        let (_config, node) = open_live(temp.path(), DurabilityTier::Synchronous);
        let log = node.log_for_test(1);
        let (entered, resume) = log.pause_next_persist();
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(
            command(
                &node,
                1,
                Request::LeaseLiveExtent { epoch: 1 },
                4096,
                &completed
            )
            .is_ok()
        );
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        // The lookup worker owns the capacity-one reply; dropping either end
        // unblocks shutdown. It must finish before pre-grant I/O is released.
        let (sent, received) = mpsc::sync_channel(1);
        let lookup = node.clone();
        let reader = thread::spawn(move || {
            let status = lookup.status().unwrap();
            let epoch = lookup
                .inventory
                .lock()
                .active(1)
                .unwrap()
                .config()
                .inventory
                .epoch;
            sent.send((status.active_epoch, epoch)).unwrap();
        });
        assert_eq!(
            received.recv_timeout(Duration::from_secs(5)).unwrap(),
            (Some(1), 1)
        );
        reader.join().unwrap();
        assert!(matches!(
            completions.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        resume.send(()).unwrap();
        assert!(matches!(
            receive(&completions, 1).response.unwrap(),
            Response::LiveLeased(_)
        ));
        drop(log);
        shutdown(&node, 1);
    }
    #[test]
    fn leased_reply_freezes_policy_after_a_prelease_tier_change() {
        let temp = tempdir::TempDir::new("live-lease-policy").unwrap();
        let (_config, node) = open_live(temp.path(), DurabilityTier::Native);
        assert_eq!(node.durability().tier, DurabilityTier::Native);
        node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Synchronous))
            .unwrap();
        let Response::LiveLeased(leased) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected lease policy")
        };
        assert_eq!(leased.tier, DurabilityTier::Synchronous);
        assert!(
            node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Native))
                .is_err()
        );
        node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Synchronous))
            .unwrap();
        let mut marker = [0; 4096];
        marker[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
        fs::OpenOptions::new()
            .write(true)
            .open(&leased.extent.segment.path)
            .unwrap()
            .write_all_at(&marker, leased.extent.start % leased.extent.segment.bytes)
            .unwrap();
        let Response::LiveAdopted(adopted) = extent_exchange(
            &node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: leased.extent.token,
                through: leased.extent.start,
                close: true,
                lease_next: true,
            },
        ) else {
            panic!("expected next lease policy")
        };
        assert_eq!(adopted.next.unwrap().tier, DurabilityTier::Synchronous);
        shutdown(&node, 1);
    }
    #[test]
    fn leased_concurrent_requests_grant_one_extent_and_keep_its_writer_valid() {
        let temp = tempdir::TempDir::new("live-exclusive-lease").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let log = node.log_for_test(1);
        let (entered, resume) = log.pause_next_persist();
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(
            command(
                &node,
                101,
                Request::LeaseLiveExtent { epoch: 1 },
                4096,
                &completed
            )
            .is_ok()
        );
        entered.recv_timeout(Duration::from_secs(5)).unwrap();
        let (second_done, second_result) = mpsc::sync_channel(1);
        assert!(
            command(
                &node,
                102,
                Request::LeaseLiveExtent { epoch: 1 },
                4096,
                &second_done
            )
            .is_ok()
        );
        assert!(matches!(
            second_result.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        resume.send(()).unwrap();
        let Response::LiveLeased(first) = receive(&completions, 101).response.unwrap() else {
            panic!("expected the sole granted extent")
        };
        let error = receive(&second_result, 102).response.err().unwrap();
        assert!(error.to_string().contains("live extent already open"));
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch(&config, 1, 0)],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&first.extent.segment.path)
            .unwrap();
        file.write_all_at(&frame, first.extent.start % first.extent.segment.bytes)
            .unwrap();
        let mut marker = [0; 4096];
        marker[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
        file.write_all_at(
            &marker,
            (first.extent.start + 4096) % first.extent.segment.bytes,
        )
        .unwrap();
        let Response::LiveAdopted(closed) = extent_exchange(
            &node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: first.extent.token,
                through: first.extent.start + 4096,
                close: true,
                lease_next: true,
            },
        ) else {
            panic!("expected current writer's next extent")
        };
        let next = closed.next.unwrap();
        assert_eq!(next.extent.start, first.extent.end);
        // Acquire observes any terminal failure published by either worker.
        assert!(!node.control_context.poison_pending.load(Ordering::Acquire));
        assert!(node.status().unwrap().healthy);
        let group = CompleteGroup::new_live(
            &node.control_context.append_shape,
            vec![batch(&config, 1, 1)],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&next.extent.segment.path)
            .unwrap()
            .write_all_at(&frame, next.extent.start % next.extent.segment.bytes)
            .unwrap();
        let Response::LiveAdopted(reply) = extent_exchange(
            &node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: next.extent.token,
                through: next.extent.start + 4096,
                close: false,
                lease_next: false,
            },
        ) else {
            panic!("current writer must remain valid")
        };
        assert_eq!(reply.adoption.receipt_count, 1);
        assert_eq!(log.receipt_count(), 2);
        drop(log);
        shutdown(&node, 1);
    }
    #[test]
    fn leased_lost_reply_recovery_fences_an_unreported_written_prefix() {
        let temp = tempdir::TempDir::new("live-lost-lease-reply").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Synchronous);
        let Response::LiveLeased(granted) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected the sole extent")
        };
        // The fault harness retains the write address only. There is no lease
        // retry or adoption/control reply after these complete direct writes.
        let segment = granted.extent.segment;
        let start = granted.extent.start;
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&segment.path)
            .unwrap();
        for sequence in 0..3 {
            let group = CompleteGroup::new_live(
                &node.control_context.append_shape,
                vec![batch(&config, 1, sequence)],
                config.partition.batch_limits,
                config.partition.group_limits,
            )
            .unwrap();
            let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
            file.write_all_at(&frame, (start + sequence * 4096) % segment.bytes)
                .unwrap();
        }
        // Real file data sync models the synchronous target's completed writes,
        // not a hardware FUA qualification or an owner frontier barrier.
        file.sync_data().unwrap();
        drop(file);
        assert_eq!(node.log_for_test(1).receipt_count(), 0);
        assert_eq!(node.log_for_test(1).synced_frontier(), 0);
        assert!(matches!(fence(&node, 2).unwrap(), Response::Status(_)));
        let recovered = node
            .inventory
            .lock()
            .live_recovery_log(2, 1)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.receipt_count(), 3);
        assert_eq!(recovered.synced_frontier(), start + 3 * 4096);
        for (sequence, receipt) in recovered
            .receipt_page(0, 3)
            .unwrap()
            .into_iter()
            .enumerate()
        {
            assert_eq!(receipt.version().sequence, sequence as u64);
            assert_eq!(
                recovered.read_batch(receipt).unwrap().id(),
                batch(&config, 1, sequence as u64).id()
            );
        }
        drop(recovered);
        let Response::LiveLeased(next) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 2 })
        else {
            panic!("expected a new stream, never a repeated old grant")
        };
        assert_ne!(next.extent.segment.generation, segment.generation);
        assert_ne!(next.extent.segment.path, segment.path);
        shutdown(&node, 2);
    }
}

#[test]
fn protocol_rejects_the_previous_version() {
    let mut preface = std::io::Cursor::new(Vec::new());
    write_transport_mode(&mut preface, &Transport::TcpTrusted).unwrap();
    let mut preface = preface.into_inner();
    assert_eq!(PROTOCOL, 25);
    preface[4..6].copy_from_slice(&24u16.to_be_bytes());
    assert!(read_transport_mode(&mut &preface[..], &Transport::TcpTrusted).is_err());
}
