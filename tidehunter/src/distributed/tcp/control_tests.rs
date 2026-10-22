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
        queued: Some(Instant::now()),
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

#[test]
fn isolated_append_does_not_visit_the_blocked_diagnostic_pool() {
    let temp = tempdir::TempDir::new("control-not-read-pool").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
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
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
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
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
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
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
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
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    // Keep the first command at inventory acquisition until every later
    // command has been accepted, making the queued-before-panic race exact.
    let inventory = node.inventory.lock();
    node.inject_panic_after_next_append();
    let (completed, completions) = mpsc::sync_channel(3);
    assert!(command(&node, 1, append_request(&config, 1, 0), 4096, &completed).is_ok());
    assert!(command(&node, 2, append_request(&config, 1, 1), 4096, &completed).is_ok());
    assert!(command(&node, 3, Request::Seal { epoch: 1 }, 1, &completed).is_ok());
    drop(inventory);
    for id in 1..=3 {
        assert!(receive(&completions, id).response.is_err());
    }
    assert!(log.lookup(batch(&config, 1, 1).id()).unwrap().is_none());
    assert_eq!(log.stats().appended_groups, 1);
    assert_eq!(log.stats().persistence_barriers, 0);
    assert!(node.status().is_err());
}

#[test]
fn caller_cancellation_and_owner_drop_drain_accepted_writes() {
    let temp = tempdir::TempDir::new("control-cancel-drain").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
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
