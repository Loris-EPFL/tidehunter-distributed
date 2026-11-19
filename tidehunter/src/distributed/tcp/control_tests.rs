use super::super::tests::{batch, config, metrics};
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::RetirementOutcome;

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
        submitted: Some(Instant::now()),
        completed: completed.clone(),
    })
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

/// A reply is the last effect of an admitted command, so its holder must see
/// the admission released on every completion path: a successful exclusive
/// control, a failed exclusive control and a stream request. Final target
/// teardown relies on this. (The owner-RPC append hand-off paths were removed
/// with the payload route.)
#[test]
fn reply_follows_admission_release_on_every_completion_path() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let temp = tempdir::TempDir::new("control-reply-after-release").unwrap();
        let config = config(&temp.path().join("node"));
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        node.set_durability(DurabilityConfig::with_tier(tier))
            .unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let (completed, completions) = mpsc::sync_channel(1);
        let requests = [
            (Request::Fence { epoch: 2 }, true),
            // Epoch 2 is current now, so fencing back to 1 fails in the worker.
            (Request::Fence { epoch: 1 }, false),
            (
                Request::CheckpointPosition {
                    authority_epoch: 2,
                    epoch: 2,
                    through: 0,
                },
                true,
            ),
        ];
        for (id, (request, succeeds)) in (1..).zip(requests) {
            assert!(command(&node, id, request, 4096, &completed).is_ok());
            assert_eq!(receive(&completions, id).response.is_ok(), succeeds);
            assert_eq!(node.control.pending_requests_for_test(), 0);
        }
    }
}

#[test]
fn active_commands_retain_both_queue_bounds() {
    for bytes_full in [false, true] {
        let temp = tempdir::TempDir::new("control-bounds").unwrap();
        let config = config(&temp.path().join("node"));
        let node = open_synchronous(&config);
        node.inventory.lock().fence_to(1).unwrap();
        // Hold the first command (an exclusive fence) at inventory acquisition,
        // so it stays admitted while the queue fills behind it.
        let inventory = node.inventory.lock();
        let (completed, completions) = mpsc::sync_channel(MAX_COMMANDS);
        let charge = if bytes_full { MAX_COMMAND_BYTES } else { 1 };
        assert!(command(&node, 1, Request::Fence { epoch: 2 }, charge, &completed).is_ok());
        let accepted = if bytes_full { 1 } else { MAX_COMMANDS };
        for id in 2..=accepted {
            assert!(command(&node, id as u64, Request::Stats, 1, &completed).is_ok());
        }
        let rejected = command(&node, 99, Request::Seal { epoch: 1 }, 1, &completed);
        assert!(rejected.is_err());
        let work = rejected.err().unwrap();
        assert_eq!(work.id, 99);
        assert!(matches!(work.request, Request::Seal { epoch: 1 }));
        drop(inventory);
        for id in 1..=accepted {
            assert!(receive(&completions, id as u64).response.is_ok());
        }
    }
}
#[test]
fn retirement_controls_require_current_authority_and_an_older_sealed_stream() {
    let temp = tempdir::TempDir::new("control-retirement-authority").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    log.append_complete_batch(
        crate::distributed::types::RequestAuthority { epoch: 1 },
        &batch(&config, 1, 0),
    )
    .unwrap();
    log.persist_appended().unwrap();
    drop(log);
    let (completed, completions) = mpsc::sync_channel(1);
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
    let proof = |file_start| crate::distributed::storage::RetireProof {
        file_start,
        inode: None,
        seal_generation: None,
    };
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
        Request::RetireWalFiles {
            authority_epoch: 1,
            epoch: 1,
            files: files.iter().copied().map(proof).collect(),
            witness: None,
        },
        Request::RetireWalFiles {
            authority_epoch: 2,
            epoch: 2,
            files: files.iter().copied().map(proof).collect(),
            witness: None,
        },
        Request::RetireWalFiles {
            authority_epoch: 2,
            epoch: 1,
            files: vec![proof(files[0]); 257],
            witness: None,
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

#[test]
fn live_resolution_rejects_forged_reference_and_stale_authority() {
    let temp = tempdir::TempDir::new("live-owner-reference").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let (completed, completions) = mpsc::sync_channel(1);
    let appended = node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .append_complete_batch(
            crate::distributed::types::RequestAuthority { epoch: 1 },
            &batch(&config, 1, 0),
        )
        .unwrap();
    let reference = batch(&config, 1, 0)
        .record_reference(appended.address(), 0)
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
fn native_tier_controls_flush_the_appended_frontier_first() {
    let temp = tempdir::TempDir::new("control-native-tier").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    assert_eq!(node.durability().tier, DurabilityTier::Native);
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    let (entered, release) = log.pause_next_persist();
    let (completed, completions) = mpsc::sync_channel(4);
    log.append_complete_batch(
        crate::distributed::types::RequestAuthority { epoch: 1 },
        &batch(&config, 1, 0),
    )
    .unwrap();
    node.control.shared.frontier_for(&log).unwrap();
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
fn durability_cannot_change_after_the_first_stream_frontier() {
    let temp = tempdir::TempDir::new("control-tier-fixed").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let log = node.inventory.lock().active(1).unwrap();
    // The first lease (or append) attaches the stream frontier and syncer.
    node.control.shared.frontier_for(&log).unwrap();
    assert!(
        node.set_durability(DurabilityConfig::with_tier(DurabilityTier::Synchronous))
            .is_err()
    );
    assert_eq!(node.durability().tier, DurabilityTier::Native);
}

#[test]
fn position_root_does_not_replace_read_barriers() {
    use crate::distributed::storage::PartitionLog;
    use crate::distributed::types::RequestAuthority;
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let temp = tempdir::TempDir::new("position-receipt-consumers").unwrap();
        let mut config = config(&temp.path().join("node"));
        config.partition.fragment_bytes = 128 * 1024;
        config.partition.wal_file_bytes = 256 * 1024;
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        node.set_durability(DurabilityConfig::with_tier(tier))
            .unwrap();
        node.inventory.lock().fence_to(1).unwrap();
        let log = node.inventory.lock().active(1).unwrap();
        // Seed completely written bytes without a receipt barrier. The proof
        // must leave this adopted frontier behind even in synchronous policy.
        let first = batch(&config, 1, 0);
        let receipt = log
            .append_complete_batch(RequestAuthority { epoch: 1 }, &first)
            .unwrap();
        node.control.shared.frontier_for(&log).unwrap();
        let cut = log.appended_frontier();
        let reference = first.record_reference(receipt.address(), 0).unwrap();
        let (completed, completions) = mpsc::sync_channel(4);
        assert!(
            command(
                &node,
                1,
                Request::CheckpointPosition {
                    authority_epoch: 1,
                    epoch: 1,
                    through: cut
                },
                1,
                &completed
            )
            .is_ok()
        );
        let Response::CheckpointPosition { durable_end, .. } =
            receive(&completions, 1).response.unwrap()
        else {
            panic!("position proof")
        };
        assert!(durable_end >= cut);
        assert_eq!(log.position_frontier_for_test(), durable_end);
        assert!(log.synced_frontier() < durable_end);
        assert_eq!(log.stats().persistence_barriers, 0);

        // Copy the quiescent image BEFORE any read or receipt sync. Recovery
        // must therefore use the position root, not a later receipt frontier.
        let image = tempdir::TempDir::new("position-root-only-image").unwrap();
        for entry in std::fs::read_dir(config.root.join("epoch_0000000000000001")).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            std::fs::copy(entry.path(), image.path().join(entry.file_name())).unwrap();
        }
        let recovered =
            PartitionLog::recover_sealed(image.path(), &config.partition, metrics()).unwrap();
        assert_eq!(
            recovered.read_record(&reference).unwrap(),
            first.mutations()[0]
        );
        assert!(
            !recovered
                .replay_page(0, 1, 64 * 1024, durable_end)
                .unwrap()
                .groups
                .is_empty()
        );

        if tier == DurabilityTier::Synchronous {
            // The blocking owner-read boundary obtains its own covering sync.
            let (entered, release) = log.pause_next_persist();
            let read_log = log.clone();
            let (ready, result) = mpsc::sync_channel(1);
            let read = std::thread::spawn(move || {
                ready.send(read_log.read_record(&reference)).unwrap();
            });
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let pending = result.recv_timeout(Duration::from_millis(100));
            let still_behind = log.synced_frontier() < durable_end;
            release.send(()).unwrap();
            assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
            assert!(still_behind);
            assert_eq!(
                result
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
                first.mutations()[0]
            );
            read.join().unwrap();
        } else {
            assert_eq!(log.read_record(&reference).unwrap(), first.mutations()[0]);
            assert_eq!(log.stats().persistence_barriers, 0);
        }
        assert_eq!(
            log.read_batch(receipt).unwrap().mutations(),
            first.mutations()
        );
    }
}

pub(super) mod live_fencing {
    use super::*;
    use crate::distributed::group::CompleteGroup;
    use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
    use crate::distributed::nvmeof::live_target::{LiveFileIdentity, LiveTargetManager};
    use crate::distributed::nvmeof::{LiveTargetConfig, NvmeofTransport};
    use std::os::unix::fs::FileExt;

    /// The owner's key shape, which live groups are built against. The
    /// control context no longer keeps a parsed copy (the owner-RPC append
    /// handler was its only user).
    fn append_shape(node: &StorageNode) -> crate::key_shape::KeyShape {
        crate::key_shape::KeyShape::from_yaml(&node.inventory.lock().config.shape_yaml).unwrap()
    }

    // Exercise the actual WAL/control lifecycle with the existing journal-only
    // target fixture. This is not evidence of kernel namespace I/O drain.
    pub(in crate::distributed::tcp::control) fn open_live(
        root: &std::path::Path,
        tier: DurabilityTier,
    ) -> (NodeConfig, Arc<StorageNode>) {
        open_live_with(root, tier, config(&root.join("node")))
    }

    fn open_live_with(
        root: &std::path::Path,
        tier: DurabilityTier,
        mut config: NodeConfig,
    ) -> (NodeConfig, Arc<StorageNode>) {
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
            },
            configfs,
        )
        .unwrap();
        node.install_live_target(target).unwrap();
        node.with_lifecycle(|inventory, slot| {
            NodeInventory::fence(inventory, slot, 1, false, |_| Ok(()))
        })
        .unwrap();
        (config, node)
    }

    /// Lease the first live extent and return its whole WAL file.
    #[cfg(target_os = "linux")]
    fn leased_wal_file(node: &StorageNode) -> (fs::File, u64) {
        let Response::LiveLeased(leased) =
            extent_exchange(node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected lease")
        };
        let segment = &leased.extent.segment;
        (fs::File::open(&segment.path).unwrap(), segment.bytes)
    }

    // FIEMAP is Linux only.
    #[cfg(target_os = "linux")]
    #[test]
    fn default_owner_grants_prewritten_files_without_unwritten_extents() {
        let temp = tempdir::TempDir::new("default-owner-prewrite").unwrap();
        // An owner launch that omits the key gets the default.
        let mut value = serde_yaml::to_value(config(&temp.path().join("node"))).unwrap();
        value.as_mapping_mut().unwrap().remove("wal_prewrite");
        let default = serde_yaml::from_value(value).unwrap();
        let (config, node) = open_live_with(temp.path(), DurabilityTier::Synchronous, default);
        assert!(config.wal_prewrite);
        let (file, bytes) = leased_wal_file(&node);
        crate::wal::fiemap::assert_written_extents(&file, bytes);
        assert!(node.wal_prewrite_stats().0 >= bytes);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn explicit_false_owner_grants_files_with_unwritten_extents() {
        let temp = tempdir::TempDir::new("explicit-false-owner-prewrite").unwrap();
        let mut explicit = config(&temp.path().join("node"));
        explicit.wal_prewrite = false;
        let (_, node) = open_live_with(temp.path(), DurabilityTier::Synchronous, explicit);
        let (file, bytes) = leased_wal_file(&node);
        assert!(
            crate::wal::fiemap::extents(&file, bytes)
                .iter()
                .any(|&(_, _, unwritten)| unwritten),
            "an opted-out owner must keep fallocated extents unwritten"
        );
        assert_eq!(node.wal_prewrite_stats(), (0, 0, 0));
    }

    #[test]
    fn prewrite_shutdown_diagnostics_include_fenced_streams() {
        let temp = tempdir::TempDir::new("prewrite-shutdown-diagnostics").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        assert_eq!(node.wal_prewrite_stats(), (0, 0, 0));
        node.inventory.lock().config.wal_prewrite = true;
        fence(&node, 2).unwrap();
        let first = node.wal_prewrite_stats();
        assert!(first.0 >= config.partition.wal_file_bytes);
        assert!(first.1 > 0);
        assert!(first.2 > 0);
        // Fencing drops the original log and recovers a new sealed handle.
        // Costs from that original handle must remain in the owner receipt.
        fence(&node, 3).unwrap();
        node.finish_live_target_shutdown().unwrap();
        let final_stats = node.wal_prewrite_stats();
        assert!(final_stats.0 >= first.0 + config.partition.wal_file_bytes);
        assert!(final_stats.1 > first.1);
        assert!(final_stats.2 > first.2);
        assert_eq!(node.wal_prewrite_stats(), final_stats);
    }

    pub(in crate::distributed::tcp::control) fn wait_prepared(
        log: &crate::distributed::storage::PartitionLog,
        published: u64,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = log.live_file_preparation_stats();
            if stats.live_files_prepared >= 1 && stats.live_files_published >= published {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "preparation did not finish: {stats:?}"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn prepared_permanent_export_is_reused_and_identity_checked() {
        let temp = tempdir::TempDir::new("prepared-permanent").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let Response::LiveLeased(first) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("lease")
        };
        let log = node.inventory.lock().active(1).unwrap();
        wait_prepared(&log, 1);
        let segment = log.live_segment(1).unwrap();
        let (binding, created) = node
            .with_lifecycle(|inventory, slot| {
                NodeInventory::publish_permanent_segment(
                    inventory,
                    slot.as_mut().unwrap(),
                    &segment,
                )
            })
            .unwrap();
        assert!(!created);
        let published = log.live_file_preparation_stats().live_files_published;
        // A worker that loses the race to an existing export must not report
        // that it published the binding before the grant.
        node.control_context.publish_prepared_file(
            &log,
            1,
            &AtomicBool::new(false),
            &AtomicBool::new(false),
        );
        assert_eq!(
            log.live_file_preparation_stats().live_files_published,
            published
        );
        let exports = node
            .with_live_target(|target| target.stats())
            .unwrap()
            .namespaces_exposed;
        let mut changed = segment.clone();
        changed.inode += 1;
        assert_eq!(
            node.with_lifecycle(|inventory, slot| {
                NodeInventory::publish_permanent_segment(
                    inventory,
                    slot.as_mut().unwrap(),
                    &changed,
                )
            })
            .unwrap_err()
            .to_string(),
            "live segment identity or access changed"
        );
        let second = node
            .control_context
            .lease_live_published(
                1,
                DurabilityTier::Native,
                Some(first.extent.token),
                inventory::InlineExport::Prelease,
            )
            .unwrap();
        // Close both empty extents with their skip markers before crossing.
        for extent in [&first.extent, &second.extent] {
            let mut skip = [0; 4096];
            skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
            fs::OpenOptions::new()
                .write(true)
                .open(&extent.segment.path)
                .unwrap()
                .write_all_at(&skip, extent.start % extent.segment.bytes)
                .unwrap();
            extent_exchange(
                &node,
                Request::AdoptLiveExtent {
                    epoch: 1,
                    token: extent.token,
                    through: extent.start,
                    close: true,
                    lease_next: false,
                },
            );
        }
        let third = node
            .control_context
            .lease_live_published(
                1,
                DurabilityTier::Native,
                None,
                inventory::InlineExport::Lease,
            )
            .unwrap();
        assert_eq!(third.extent.segment.file_index, 1);
        assert_eq!(third.binding, binding);
        assert_eq!(third.profile.namespace_sync_ns, 0);
        // Call the same grant handler directly to exclude a later preparation
        // from the export-count assertion.
        assert_eq!(
            node.with_live_target(|target| target.stats())
                .unwrap()
                .namespaces_exposed,
            exports
        );
        drop(log);
        shutdown(&node, 1);
    }

    #[test]
    fn prepared_never_granted_exports_are_revoked_on_fence_and_seal() {
        for seal in [false, true] {
            let temp = tempdir::TempDir::new("prepared-retirement").unwrap();
            let (_, node) = open_live(temp.path(), DurabilityTier::Native);
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 });
            let log = node.inventory.lock().active(1).unwrap();
            wait_prepared(&log, 1);
            assert_eq!(
                node.with_live_target(|target| target.stats())
                    .unwrap()
                    .owned_namespace_count,
                2
            );
            drop(log);
            if seal {
                shutdown(&node, 1);
            } else {
                fence(&node, 2).unwrap();
            }
            assert_eq!(node.control.preparer_count(), 0);
            assert_eq!(
                node.with_live_target(|target| target.stats())
                    .unwrap()
                    .owned_namespace_count,
                0
            );
        }
    }

    fn wait_gate_retry(node: &StorageNode) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            // Acquire observes the worker's positive failed-lock signal.
            if node
                .control
                .shared
                .preparers
                .lock()
                .iter()
                .any(|slot| slot.waiting_for_gate.load(Ordering::Acquire))
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "worker did not reach lifecycle gate retry"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn final_target_shutdown_joins_a_preparer_parked_on_the_lifecycle_gate() {
        let temp = tempdir::TempDir::new("prepared-final-shutdown").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let log = node.inventory.lock().active(1).unwrap();
        log.lease_live_extent(RequestAuthority { epoch: 1 })
            .unwrap();
        let (entered, entry) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        *node.control.shared.before_target_shutdown.lock() = Some((entered, resume));
        let owner = node.clone();
        let shutdown = thread::spawn(move || owner.control.finish_live_target_shutdown());
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        // Teardown owns the lifecycle gate before preparation begins, so the
        // worker cannot expose any capability after teardown starts.
        node.control.shared.trigger_file_preparation(&log);
        wait_gate_retry(&node);
        release.send(()).unwrap();
        let stats = shutdown.join().unwrap().unwrap();
        assert_eq!(node.control.preparer_count(), 0);
        assert_eq!(log.live_file_preparation_stats().live_files_published, 0);
        assert_eq!(stats.namespaces_exposed, 0);
        assert_eq!(stats.owned_namespace_count, 0);
    }

    #[test]
    fn final_target_shutdown_drains_preparers_after_a_join_error() {
        let temp = tempdir::TempDir::new("prepared-shutdown-error").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let log = node.inventory.lock().active(1).unwrap();
        log.lease_live_extent(RequestAuthority { epoch: 1 })
            .unwrap();
        let (entered, entry) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        *node.control.shared.before_target_shutdown.lock() = Some((entered, resume));
        let owner = node.clone();
        let shutdown = thread::spawn(move || owner.control.finish_live_target_shutdown());
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        node.control.shared.trigger_file_preparation(&log);
        wait_gate_retry(&node);
        let remaining_stopping = {
            let mut slots = node.control.shared.preparers.lock();
            let stopping = slots[0].stopping.clone();
            let (trigger, _receiver) = mpsc::sync_channel(1);
            // Inject a failed earlier worker. The real parked worker must still
            // be stopped and joined before teardown returns that first error.
            slots.insert(
                0,
                PreparerSlot {
                    log: log.clone(),
                    trigger,
                    stopping: Arc::new(AtomicBool::new(false)),
                    waiting_for_gate: Arc::new(AtomicBool::new(false)),
                    worker: thread::spawn(|| panic!("injected preparer failure")),
                },
            );
            stopping
        };
        release.send(()).unwrap();
        let error = shutdown.join().unwrap().unwrap_err();
        assert_eq!(
            error.to_string(),
            "log file preparer panicked; restart required"
        );
        assert!(remaining_stopping.load(Ordering::Acquire));
        assert_eq!(node.control.preparer_count(), 0);
        assert_eq!(log.live_file_preparation_stats().live_files_published, 0);
        assert_eq!(
            node.with_live_target(|target| target.stats())
                .unwrap()
                .namespaces_exposed,
            0
        );
    }

    /// A grant whose export is paused inside a target journal sync holds only
    /// the lifecycle gate: an inventory reader and a position proof on another
    /// control worker complete while the sync is still paused.
    #[test]
    fn foreground_work_proceeds_while_a_target_journal_sync_is_paused() {
        use crate::distributed::nvmeof::live_target::SaveStep;
        let temp = tempdir::TempDir::new("paused-target-journal").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let (entered, paused) = mpsc::sync_channel(1);
        let (resume, resumed) = mpsc::sync_channel::<()>(1);
        let mut armed = true;
        node.with_live_target(|target| {
            target.set_save_hook(Box::new(move |step| {
                if armed && step == SaveStep::Prepared {
                    armed = false;
                    entered.send(()).map_err(io::Error::other)?;
                    // `resume` drops on test unwind, so this cannot hang.
                    resumed.recv().map_err(io::Error::other)?;
                }
                Ok(())
            }));
        });
        let leasing = {
            let node = node.clone();
            thread::spawn(move || extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 }))
        };
        paused.recv_timeout(Duration::from_secs(5)).unwrap();
        // The export holds the lifecycle gate across its journal sync...
        assert!(node.control_context.lifecycle.try_lock().is_none());
        // ...and not the inventory.
        let inventory = node
            .inventory
            .try_lock_for(Duration::from_secs(5))
            .expect("inventory is held across a target journal sync");
        assert!(inventory.active(1).is_ok());
        drop(inventory);
        let (completed, completions) = mpsc::sync_channel(1);
        let proof = Request::CheckpointPosition {
            authority_epoch: 1,
            epoch: 1,
            through: 0,
        };
        assert!(command(&node, 2, proof, 4096, &completed).is_ok());
        let response = receive(&completions, 2).response.unwrap();
        assert!(matches!(
            response,
            Response::CheckpointPosition { epoch: 1, .. }
        ));
        assert!(
            node.control_context.lifecycle.try_lock().is_none(),
            "the position proof must complete while the export is still paused"
        );
        resume.send(()).unwrap();
        let Response::LiveLeased(_) = leasing.join().unwrap() else {
            panic!("expected lease")
        };
        shutdown(&node, 1);
    }

    #[test]
    fn preparer_retirement_joins_while_the_lifecycle_gate_is_held() {
        for seal in [false, true] {
            let temp = tempdir::TempDir::new("prepared-try-lock").unwrap();
            let (_, node) = open_live(temp.path(), DurabilityTier::Native);
            let log = node.inventory.lock().active(1).unwrap();
            log.lease_live_extent(RequestAuthority { epoch: 1 })
                .unwrap();
            let lifecycle = node.control_context.lifecycle.lock();
            node.control.shared.trigger_file_preparation(&log);
            wait_prepared(&log, 0);
            wait_gate_retry(&node);
            // At this point the worker cannot acquire the gate. Retirement
            // holds the same gate in the real fence and seal paths.
            node.control.shared.retire_syncer(&log).unwrap();
            assert_eq!(node.control.preparer_count(), 0);
            assert_eq!(log.live_file_preparation_stats().live_files_published, 0);
            drop(lifecycle);
            drop(log);
            if seal {
                shutdown(&node, 1);
            } else {
                fence(&node, 2).unwrap();
            }
        }
    }

    #[test]
    fn preparer_export_failure_poison_fences() {
        let temp = tempdir::TempDir::new("prepared-publication-error").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let log = node.inventory.lock().active(1).unwrap();
        log.lease_live_extent(RequestAuthority { epoch: 1 })
            .unwrap();
        // The registry's pending path must be a regular file.
        fs::create_dir(temp.path().join("target/.live-target.pending")).unwrap();
        node.control.shared.trigger_file_preparation(&log);
        wait_prepared(&log, 0);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !node.control_context.poison_pending.load(Ordering::Acquire) {
            assert!(
                Instant::now() < deadline,
                "publication failure did not poison readers"
            );
            thread::yield_now();
        }
        node.control.shared.retire_syncer(&log).unwrap();
        assert!(node.control_context.poison_pending.load(Ordering::Acquire));
        assert!(
            log.lease_live_extent(RequestAuthority { epoch: 1 })
                .is_err()
        );
    }

    #[test]
    fn permanent_export_survives_read_pin_release_and_idle_reaping() {
        let temp = tempdir::TempDir::new("permanent-pin").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let batch = batch(&config, 1, 0);
        let group = CompleteGroup::new_live(
            &append_shape(&node),
            vec![batch.clone()],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        let log = node.inventory.lock().active(1).unwrap();
        let (preparing, resume) =
            log.pause_live(crate::distributed::storage::LivePausePoint::PrepareBeforePublish);
        let Response::LiveLeased(leased) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected lease")
        };
        preparing.recv_timeout(Duration::from_secs(5)).unwrap();
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
        let receipt = log.receipt_page(0, 1).unwrap()[0];
        let reference = batch.record_reference(receipt.address(), 0).unwrap();
        let segment = &leased.extent.segment;
        let file = LiveFileIdentity {
            path: segment.path.clone(),
            device: segment.device,
            inode: segment.inode,
            bytes: segment.bytes,
            block_size: segment.block_size,
        };
        // Pin release and idle reaping must retain this exact permanent export
        // both before and after the preparer publishes the next WAL file.
        for prepared in [false, true] {
            if prepared {
                resume.send(()).unwrap();
                wait_prepared(&log, 1);
            }
            let lease = log.resolve_live_record(&reference).unwrap();
            let token = [7; 16];
            node.with_lifecycle(|inventory, slot| {
                inventory.live_pins.insert(token, (1, vec![lease]));
                NodeInventory::release_live_pin(inventory, slot.as_mut(), 1, token).unwrap();
                assert!(!inventory.live_pins.contains_key(&token));
                let target = slot.as_mut().unwrap();
                NodeInventory::reap_idle_live_segments(inventory, target, 1).unwrap();
                assert_eq!(
                    target.binding_for_file(&file).as_ref(),
                    Some(&leased.binding)
                );
                assert_eq!(
                    target.stats().unwrap().owned_namespace_count,
                    if prepared { 2 } else { 1 }
                );
            });
        }
        shutdown(&node, 1);
    }

    #[test]
    fn open_live_exports_are_permanent_and_idempotent() {
        {
            let temp = tempdir::TempDir::new("open-live-exports").unwrap();
            let (_, node) = open_live(temp.path(), DurabilityTier::Native);
            let Response::LiveLeased(leased) =
                extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
            else {
                panic!("expected lease")
            };
            let permanent = true;
            let expected_namespaces = 2;
            let pause =
                if permanent {
                    let log = node.inventory.lock().active(1).unwrap();
                    wait_prepared(&log, 1);
                    Some(log.pause_live(
                        crate::distributed::storage::LivePausePoint::PrepareBeforePublish,
                    ))
                } else {
                    None
                };
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
                assert_eq!(info.exported_files, if permanent { 2 } else { 0 });
                assert_eq!(info.wal_file_bytes, 32768);
                let stats = node.with_live_target(|target| target.stats()).unwrap();
                assert_eq!(stats.namespaces_exposed, expected_namespaces);
                assert_eq!(stats.owned_namespace_count, expected_namespaces);
                assert!(leased.binding.permanent);
            }
            node.with_lifecycle(|inventory, slot| {
                NodeInventory::reap_idle_live_segments(inventory, slot.as_mut().unwrap(), 1)
            })
            .unwrap();
            assert_eq!(
                node.with_live_target(|target| target.stats())
                    .unwrap()
                    .owned_namespace_count,
                expected_namespaces
            );
            if let Some((_, release)) = pause {
                let _ = release.send(());
            }
            shutdown(&node, 1);
        }
    }

    #[test]
    fn prepared_namespace_is_not_advertised_as_a_granted_file() {
        let temp = tempdir::TempDir::new("prepared-export-info").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 });
        let log = node.inventory.lock().active(1).unwrap();
        wait_prepared(&log, 1);
        let Response::LiveExports(info) =
            extent_exchange(&node, Request::OpenLiveExports { authority_epoch: 1 })
        else {
            panic!("exports")
        };
        // OpenLiveExports counts files up to the allocator position, not every
        // namespace. File 1 exists on the target but has never had a lease.
        assert_eq!(info.exported_files, 1);
        assert_eq!(
            node.with_live_target(|target| target.stats())
                .unwrap()
                .owned_namespace_count,
            2
        );
        drop(log);
        shutdown(&node, 1);
    }

    #[test]
    fn permanent_owner_rejects_active_record_resolution() {
        let temp = tempdir::TempDir::new("permanent-controls").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let batch = batch(&config, 1, 0);
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
        let error = node
            .handle(
                Request::ResolveLiveRecords {
                    authority_epoch: 1,
                    references: vec![reference],
                },
                TcpLimits::default(),
            )
            .err()
            .unwrap();
        assert_eq!(
            error.to_string(),
            "live record resolution is disabled for the active epoch in permanent export mode"
        );
        shutdown(&node, 1);
    }

    /// Write one single-batch frame at `offset` of the open leased extent and
    /// adopt it without closing the extent; returns the adopted receipt.
    fn adopt_leased_frame(
        node: &StorageNode,
        config: &NodeConfig,
        leased: &LiveLeased,
        sequence: u64,
        offset: u64,
    ) -> Receipt {
        let epoch = leased.extent.segment.epoch;
        let group = CompleteGroup::new_live(
            &append_shape(node),
            vec![batch(config, epoch, sequence)],
            config.partition.batch_limits,
            config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&leased.extent.segment.path)
            .unwrap()
            .write_all_at(&frame, offset % leased.extent.segment.bytes)
            .unwrap();
        let Response::LiveAdopted(_) = extent_exchange(
            node,
            Request::AdoptLiveExtent {
                epoch,
                token: leased.extent.token,
                through: offset + frame.len() as u64,
                close: false,
                lease_next: false,
            },
        ) else {
            panic!("expected adoption")
        };
        let log = node.inventory.lock().active(epoch).unwrap();
        let appended = log.receipt_page(log.receipt_count() - 1, 1).unwrap()[0];
        Receipt {
            id: appended.id(),
            version: appended.version(),
            digest: appended.digest(),
            address: appended.address(),
        }
    }

    fn lease(node: &StorageNode, epoch: u64) -> LiveLeased {
        let Response::LiveLeased(leased) =
            extent_exchange(node, Request::LeaseLiveExtent { epoch })
        else {
            panic!("expected lease")
        };
        leased
    }

    fn fence(node: &StorageNode, epoch: u64) -> Result<Response, TcpError> {
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(command(node, 1, Request::Fence { epoch }, 1, &completed).is_ok());
        receive(&completions, 1).response
    }

    pub(in crate::distributed::tcp::control) fn shutdown(node: &StorageNode, epoch: u64) {
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

    fn leased_history(node: &StorageNode, config: &NodeConfig, extents: usize) -> u64 {
        let Response::LiveLeased(mut leased) =
            extent_exchange(node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected leased extent")
        };
        for sequence in 0..extents {
            let group = CompleteGroup::new_live(
                &append_shape(node),
                vec![batch(config, 1, sequence as u64)],
                config.partition.batch_limits,
                config.partition.group_limits,
            )
            .unwrap();
            let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
            let file = fs::OpenOptions::new()
                .write(true)
                .open(&leased.extent.segment.path)
                .unwrap();
            let offset = leased.extent.start % leased.extent.segment.bytes;
            file.write_all_at(&frame, offset).unwrap();
            let mut skip = [0; 4096];
            skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
            file.write_all_at(&skip, offset + frame.len() as u64)
                .unwrap();
            let Response::LiveAdopted(adopted) = extent_exchange(
                node,
                Request::AdoptLiveExtent {
                    epoch: 1,
                    token: leased.extent.token,
                    through: leased.extent.start + frame.len() as u64,
                    close: true,
                    lease_next: sequence + 1 < extents,
                },
            ) else {
                panic!("expected adopted history")
            };
            if let Some(next) = adopted.next {
                leased = next;
            }
        }
        let end = node.inventory.lock().active(1).unwrap().appended_frontier();
        fence(node, 2).unwrap();
        end
    }

    fn history_page(
        node: &StorageNode,
        config: &NodeConfig,
        from: u64,
        end: u64,
        sequence: u64,
    ) -> live_wire::LiveRecoveryWire {
        let Response::LiveRecovery(page) = extent_exchange(
            node,
            Request::ReplayLiveWal {
                authority_epoch: 2,
                source_epoch: 1,
                from,
                limit: 1,
                require_end: end,
            },
        ) else {
            panic!("expected WAL history page")
        };
        assert_eq!(page.entries.len(), 1);
        let entry = &page.entries[0];
        assert!(entry.binding.readonly);
        assert!(!entry.binding.permanent);
        let mut bytes = vec![0; entry.len as usize];
        fs::File::open(&entry.segment.path)
            .unwrap()
            .read_exact_at(&mut bytes, entry.offset)
            .unwrap();
        assert_eq!(&bytes[8..], batch(config, 1, sequence).encoded().as_ref());
        page
    }

    fn release_history_page(node: &StorageNode, page: &live_wire::LiveRecoveryWire) {
        assert!(matches!(
            extent_exchange(
                node,
                Request::ReleaseLiveEpochPin {
                    epoch: 2,
                    token: page.epoch_pin_token,
                },
            ),
            Response::NvmeofEpochReleased
        ));
        assert!(node.inventory.lock().live_pins.is_empty());
    }

    fn target_stats(node: &StorageNode) -> crate::distributed::nvmeof::LiveTargetStats {
        node.with_live_target(|target| target.stats()).unwrap()
    }

    #[test]
    fn trim_finishes_retired_tombstones_after_owner_restart() {
        use crate::distributed::storage::{RetireProof, RetirementOutcome};
        use crate::distributed::tcp::generation_control::TrimRequest;

        let temp = tempdir::TempDir::new("trim-retired-tombstones").unwrap();
        let root = temp.path();
        let configfs = root.join("configfs");
        let state_dir = root.join("target");
        fs::create_dir(&configfs).unwrap();
        fs::create_dir(&state_dir).unwrap();
        for directory in ["hosts", "ports", "subsystems"] {
            fs::create_dir(configfs.join(directory)).unwrap();
        }
        let mut config = config(&root.join("node"));
        config.partition.fragment_bytes = 16 * 1024;
        config.partition.wal_file_bytes = 32 * 1024;
        config.live_target = Some(LiveTargetConfig {
            address: "127.0.0.1".into(),
            service: 4420,
            transport: NvmeofTransport::Tcp,
            host_nqn: "nqn.2026-01.local.test:trim-tombstones".into(),
            port_id: 42,
            state_dir: state_dir.clone(),
            commissioning_port_id: 43,
            commissioning_service: 4421,
        });
        let reopen = || {
            StorageNode::bench_open_live_fixture(config.clone(), metrics(), configfs.clone())
                .unwrap()
        };
        let node = reopen();
        fence(&node, 1).unwrap();
        leased_history(&node, &config, 12);
        let log = node.source_log_for_test(2, 1);
        let proofs: Vec<_> = log
            .wal_file_page(None, 256)
            .unwrap()
            .into_iter()
            .map(|file_start| RetireProof {
                file_start,
                inode: None,
                seal_generation: None,
            })
            .collect();
        // Stop after the retirement root and unlink, before target cleanup.
        assert_eq!(
            log.retire_sealed_stream_files(&proofs).unwrap(),
            RetirementOutcome::Completed
        );
        let pending = log.pending_file_retirements();
        let victim = pending
            .iter()
            .filter(|file| file.start != 0)
            .map(|file| log.wal_file_path(file.start))
            .find(|path| {
                node.with_live_target(|target| target.sealed_file(path).unwrap().is_some())
            })
            .expect("a tombstone beyond the first WAL file");
        assert!(
            pending
                .iter()
                .all(|file| !log.wal_file_path(file.start).exists())
        );
        let generation_path = victim.parent().unwrap();
        let sealed = state_dir.join("sealed");
        let (marker, marker_bytes) = fs::read_dir(&sealed)
            .unwrap()
            .find_map(|entry| {
                let path = entry.unwrap().path();
                let bytes = fs::read(&path).unwrap();
                let identity: LiveFileIdentity = serde_json::from_slice(&bytes[4..]).unwrap();
                (identity.path == victim).then_some((path, bytes))
            })
            .unwrap();
        drop(log);
        drop(node);

        let node = reopen();
        fence(&node, 3).unwrap();
        let trim = |authority_generation| {
            Request::TrimGeneration(TrimRequest {
                authority_generation,
                generation: 1,
            })
        };
        fs::write(&marker, b"bad").unwrap();
        let err = node.handle(trim(3), TcpLimits::default()).err().unwrap();
        assert!(
            err.to_string().contains("truncated sealed WAL marker"),
            "{err}"
        );
        assert!(generation_path.is_dir());
        assert_eq!(
            node.source_log_for_test(3, 1).pending_file_retirements(),
            pending
        );
        fs::write(&marker, marker_bytes).unwrap();
        drop(node);

        let node = reopen();
        fence(&node, 4).unwrap();
        assert!(
            matches!(node.handle(trim(4), TcpLimits::default()).unwrap(), Response::TrimGeneration(reply) if reply.generation == 1)
        );
        assert!(!generation_path.exists());
        assert!(!marker.exists());
        assert!(node.with_live_target(|target| target.sealed_file(&victim).unwrap().is_none()));
        shutdown(&node, 4);
    }

    #[test]
    fn history_pages_reuse_retained_readonly_exports() {
        {
            let temp = tempdir::TempDir::new("retained-history-pages").unwrap();
            let (config, node) = open_live(temp.path(), DurabilityTier::Native);
            let end = leased_history(&node, &config, 2);
            assert_eq!(target_stats(&node).owned_namespace_count, 0);
            let first = history_page(&node, &config, 0, end, 0);
            let exposed = target_stats(&node).namespaces_exposed;
            release_history_page(&node, &first);
            node.with_lifecycle(|inventory, slot| {
                NodeInventory::reap_idle_live_segments(inventory, slot.as_mut().unwrap(), 2)
            })
            .unwrap();
            let retained = true;
            assert_eq!(
                target_stats(&node).owned_namespace_count,
                u64::from(retained)
            );
            let second = history_page(&node, &config, first.next.unwrap() as u64, end, 1);
            assert_eq!(first.entries[0].segment, second.entries[0].segment);
            assert_eq!(
                first.entries[0].binding == second.entries[0].binding,
                retained
            );
            assert_eq!(
                target_stats(&node).namespaces_exposed,
                exposed + u64::from(!retained)
            );
            release_history_page(&node, &second);
            // A higher-epoch reopen's replayed history remains readable, but
            // no export survives the following authority cut.
            fence(&node, 3).unwrap();
            assert_eq!(target_stats(&node).owned_namespace_count, 0);
            shutdown(&node, 3);
        }
    }

    #[test]
    fn readonly_export_connection_pin_survives_record_release_and_idle_history_eviction() {
        let temp = tempdir::TempDir::new("pinned-history-export").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let count = 2 * (super::super::inventory::MAX_IDLE_HISTORY_EXPORTS + 2);
        let end = leased_history(&node, &config, count);
        let first = history_page(&node, &config, 0, end, 0);
        let entry = &first.entries[0];
        let Response::NvmeofEpochPinned(token) = extent_exchange(
            &node,
            Request::AcquireLiveExportPin {
                epoch: 2,
                segment: entry.segment.clone(),
                binding: entry.binding.clone(),
            },
        ) else {
            panic!("expected export pin")
        };
        let file = LiveFileIdentity {
            path: entry.segment.path.clone(),
            device: entry.segment.device,
            inode: entry.segment.inode,
            bytes: entry.segment.bytes,
            block_size: entry.segment.block_size,
        };
        let mut next = first.next;
        release_history_page(&node, &first);
        for sequence in 1..count {
            let page = history_page(&node, &config, next.unwrap() as u64, end, sequence as u64);
            next = page.next;
            release_history_page(&node, &page);
        }
        assert_eq!(node.inventory.lock().live_export_pins.len(), 1);
        assert_eq!(
            node.with_live_target(|target| target.binding_for_file(&file)),
            Some(entry.binding.clone()),
        );
        assert!(matches!(
            extent_exchange(&node, Request::ReleaseLiveExportPin { epoch: 2, token }),
            Response::NvmeofEpochReleased,
        ));
        assert!(node.inventory.lock().live_export_pins.is_empty());
        fence(&node, 3).unwrap();
        shutdown(&node, 3);
    }

    #[test]
    fn retained_history_stays_readonly_and_lifecycle_revocation_allows_retirement() {
        for lifecycle in ["seal", "retire", "final"] {
            let temp = tempdir::TempDir::new("retained-history-lifecycle").unwrap();
            let (config, node) = open_live(temp.path(), DurabilityTier::Synchronous);
            let end = leased_history(&node, &config, 1);
            let page = history_page(&node, &config, 0, end, 0);
            let segment = &page.entries[0].segment;
            release_history_page(&node, &page);
            assert_eq!(target_stats(&node).owned_namespace_count, 1);
            node.with_lifecycle(|inventory, slot| {
                let target = slot.as_mut().unwrap();
                assert_eq!(
                    NodeInventory::publish_live_segment(
                        inventory,
                        target,
                        segment,
                        true,
                        inventory::InlineExport::Lease,
                    )
                    .err()
                    .unwrap()
                    .to_string(),
                    "live segment identity or access changed"
                );
                assert_eq!(
                    NodeInventory::publish_permanent_segment(inventory, target, segment)
                        .err()
                        .unwrap()
                        .to_string(),
                    "live segment identity or access changed"
                );
            });
            assert_eq!(
                fence(&node, 1).err().unwrap().to_string(),
                "fencing generation 1 already fenced or sealed (owner high-water 2)"
            );
            match lifecycle {
                "seal" => {
                    assert!(matches!(
                        extent_exchange(&node, Request::Seal { epoch: 2 }),
                        Response::Sealed
                    ));
                    assert_eq!(target_stats(&node).owned_namespace_count, 0);
                    fence(&node, 3).unwrap();
                    assert!(matches!(
                        extent_exchange(
                            &node,
                            Request::RetireWalFiles {
                                authority_epoch: 3,
                                epoch: 1,
                                files: vec![crate::distributed::storage::RetireProof {
                                    file_start: 0,
                                    inode: None,
                                    seal_generation: None,
                                }],
                                witness: None,
                            }
                        ),
                        Response::Retired(RetirementOutcome::Completed)
                    ));
                    shutdown(&node, 3);
                }
                "retire" => {
                    assert!(matches!(
                        extent_exchange(
                            &node,
                            Request::RetireWalFiles {
                                authority_epoch: 2,
                                epoch: 1,
                                files: vec![crate::distributed::storage::RetireProof {
                                    file_start: 0,
                                    inode: None,
                                    seal_generation: None,
                                }],
                                witness: None,
                            }
                        ),
                        Response::Retired(RetirementOutcome::Completed)
                    ));
                    shutdown(&node, 2);
                }
                "final" => {
                    // Exercise final teardown directly, rather than letting
                    // the preceding Seal request do the only revocation.
                    let stats = node.control.finish_live_target_shutdown().unwrap();
                    assert_eq!(stats.owned_namespace_count, 0);
                }
                _ => unreachable!(),
            }
            assert_eq!(target_stats(&node).owned_namespace_count, 0);
            if lifecycle != "final" {
                assert!(!segment.path.exists(), "retirement must unlink the WAL");
            }
        }
    }

    #[test]
    fn retained_history_cache_evicts_oldest_unpinned_export_not_an_inflight_page() {
        let temp = tempdir::TempDir::new("retained-history-bound").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let bound = inventory::MAX_IDLE_HISTORY_EXPORTS;
        let end = leased_history(&node, &config, (bound + 2) * 2);
        let mut from = 0;
        let mut first_binding = None;
        let mut second_binding = None;
        for sequence in 0..bound * 2 {
            let page = history_page(&node, &config, from, end, sequence as u64);
            from = page.next.unwrap() as u64;
            if sequence == 0 {
                first_binding = Some(page.entries[0].binding.clone());
            }
            if sequence == 2 {
                second_binding = Some(page.entries[0].binding.clone());
            }
            release_history_page(&node, &page);
            assert!(target_stats(&node).owned_namespace_count <= bound as u64);
        }
        // Repin the oldest file and keep that page in flight while a new
        // history file needs cache space. It must not be an eviction victim.
        let pinned = history_page(&node, &config, 0, end, 0);
        assert_eq!(Some(&pinned.entries[0].binding), first_binding.as_ref());
        let new = history_page(&node, &config, from, end, (bound * 2) as u64);
        from = new.next.unwrap() as u64;
        node.with_lifecycle(|inventory, slot| {
            NodeInventory::release_live_pin(inventory, slot.as_mut(), 2, new.epoch_pin_token)
        })
        .unwrap();
        assert_eq!(
            target_stats(&node).owned_namespace_count,
            (bound + 1) as u64
        );
        release_history_page(&node, &pinned);
        assert_eq!(target_stats(&node).owned_namespace_count, bound as u64);
        assert!(
            !temp
                .path()
                .join("configfs/subsystems")
                .join(&second_binding.unwrap().nqn)
                .exists(),
            "the oldest unpinned history export must be revoked"
        );
        // One more new file evicts file 2 (file 1 was evicted when the pinned
        // page joined the idle cache); the recently pinned file 0 survives.
        let skip = history_page(&node, &config, from, end, (bound * 2 + 1) as u64);
        from = skip.next.unwrap() as u64;
        release_history_page(&node, &skip);
        let next = history_page(&node, &config, from, end, (bound * 2 + 2) as u64);
        release_history_page(&node, &next);
        assert_eq!(target_stats(&node).owned_namespace_count, bound as u64);
        let reread = history_page(&node, &config, 0, end, 0);
        assert_eq!(Some(&reread.entries[0].binding), first_binding.as_ref());
        release_history_page(&node, &reread);
        fence(&node, 3).unwrap();
        assert_eq!(target_stats(&node).owned_namespace_count, 0);
        shutdown(&node, 3);
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
            let leased = lease(&node, 1);
            let first = adopt_leased_frame(&node, &config, &leased, 0, leased.extent.start);
            let log = node.inventory.lock().active(1).unwrap();
            let old = Arc::downgrade(&log);
            let frontier = log.frontier().unwrap().clone();
            // The removed Reserved completion replied only after an owner
            // covering sync in the Synchronous tier. On the leased route the
            // Synchronous proof is the coordinator's FUA/covering flush, and an
            // owner adoption without close requests no sync in either tier, so
            // the owner frontier stays behind until the fence below flushes it.
            assert!(frontier.synced() < frontier.appended(), "{tier:?}");
            drop(log);
            for epoch in [1, 0] {
                let response = fence(&node, epoch);
                assert_eq!(response.is_ok(), epoch == 1);
                assert_eq!(node.control.shared.syncers.lock().len(), 1);
            }
            // A later real completion proves the no-op/rejected fences did not
            // stop the old frontier. Its next control must still be able to sync.
            let second = adopt_leased_frame(&node, &config, &leased, 1, leased.extent.start + 4096);
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
            let leased = lease(&node, 2);
            adopt_leased_frame(&node, &config, &leased, 0, leased.extent.start);
            assert_eq!(node.control.shared.syncers.lock().len(), 1);
            shutdown(&node, 2);
        }
    }

    #[test]
    fn recoverable_live_fence_releases_used_wal_after_an_incomplete_leased_frame() {
        let temp = tempdir::TempDir::new("live-recoverable-syncer").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let leased = lease(&node, 1);
        let receipt = adopt_leased_frame(&node, &config, &leased, 0, leased.extent.start);
        let log = node.inventory.lock().active(1).unwrap();
        let old = Arc::downgrade(&log);
        assert!(log.synced_frontier() < log.appended_frontier());
        let frontier = log.frontier().unwrap().clone();
        let (sync_entered, resume_sync) = log.pause_next_persist();
        drop(log);
        let incarnation = node.inventory.lock().incarnation();
        assert!(node.inventory.lock().status().unwrap().recovery_clean);
        // Close beyond the adopted end: the next frame was never written, so the
        // acknowledged frame stays unsynced behind an incomplete live frame.
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(
            command(
                &node,
                1,
                Request::AdoptLiveExtent {
                    epoch: 1,
                    token: leased.extent.token,
                    through: leased.extent.start + 2 * 4096,
                    close: true,
                    lease_next: false,
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
        // The in-process recovery followed a persistence failure of this
        // incarnation: it cannot prove the owner kept every acknowledged write,
        // so the fence and every later status report it unclean.
        assert_eq!(status.incarnation, incarnation);
        assert!(!status.recovery_clean);
        assert_recovered(&node, &config, &[receipt]);
        shutdown(&node, 2);
    }

    #[test]
    fn live_fence_keeps_the_external_handle_guard_after_joining_the_syncer() {
        let temp = tempdir::TempDir::new("live-fence-external-handle").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let leased = lease(&node, 1);
        adopt_leased_frame(&node, &config, &leased, 0, leased.extent.start);
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
                &append_shape(&node),
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
            // Neither tier requests a sync per adoption: synchronous frames are
            // FUA- or flush-covered before acknowledgement.
            assert_eq!(log.frontier().unwrap().stats().flush_requests, 0);
            assert_eq!(log.stats().persistence_barriers, 0);
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
            // Extent close requests the fragment-boundary sync in both tiers.
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(log.frontier().unwrap().stats().flush_requests, 1);
            assert_eq!(log.synced_frontier(), 0);
            resume.send(()).unwrap();
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
                &append_shape(&node),
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

    pub(in crate::distributed::tcp::control) fn extent_exchange(
        node: &StorageNode,
        request: Request,
    ) -> Response {
        let (completed, completions) = mpsc::sync_channel(1);
        assert!(command(node, 1, request, 4096, &completed).is_ok());
        receive(&completions, 1).response.unwrap()
    }

    #[test]
    fn queued_live_fence_waits_for_unlocked_adoption_then_rejects_old_authority() {
        use crate::distributed::storage::LivePausePoint;

        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let temp = tempdir::TempDir::new("live-fence-adoption-round").unwrap();
            let (config, node) = open_live(temp.path(), tier);
            let Response::LiveLeased(leased) =
                extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
            else {
                panic!("expected leased extent")
            };
            let group = CompleteGroup::new_live(
                &append_shape(&node),
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
                .write_all_at(&frame, 0)
                .unwrap();
            let log = node.inventory.lock().active(1).unwrap();
            let (entered, resume) = log.pause_live(LivePausePoint::ValidateUnlocked);
            // No test-owned log Arc may survive the fence's recovery cut.
            drop(log);
            let (completed, completions) = mpsc::sync_channel(2);
            assert!(
                command(
                    &node,
                    1,
                    Request::AdoptLiveExtent {
                        epoch: 1,
                        token: leased.extent.token,
                        through: frame.len() as u64,
                        close: false,
                        lease_next: false,
                    },
                    4096,
                    &completed
                )
                .is_ok()
            );
            entered.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(command(&node, 2, Request::Fence { epoch: 2 }, 1, &completed).is_ok());
            {
                let queue = node.control.shared.queue.lock();
                // Positive queue state: the accepted fence waits behind the
                // active one-frame round, rather than timing a sleeping thread.
                assert!(queue.active_appends >= 1);
                assert!(queue.work.iter().any(
                    |work| work.id == 2 && matches!(work.request, Request::Fence { epoch: 2 })
                ));
            }
            assert!(matches!(
                completions.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            resume.send(()).unwrap();
            let Response::LiveAdopted(adopted) = receive(&completions, 1).response.unwrap() else {
                panic!("expected old-epoch adoption completion")
            };
            assert_eq!(adopted.adoption.receipt_count, 1);
            assert_eq!(adopted.adoption.adopted_end, frame.len() as u64);
            let Response::Status(status) = receive(&completions, 2).response.unwrap() else {
                panic!("expected higher-epoch fence")
            };
            assert_eq!(status.active_epoch, Some(2));
            let old = node
                .inventory
                .lock()
                .live_recovery_log(2, 1)
                .unwrap()
                .unwrap();
            let published = old.receipt_count();
            assert_eq!(published, 1);
            assert!(
                command(
                    &node,
                    3,
                    Request::AdoptLiveExtent {
                        epoch: 1,
                        token: leased.extent.token,
                        through: frame.len() as u64,
                        close: false,
                        lease_next: false,
                    },
                    4096,
                    &completed
                )
                .is_ok()
            );
            assert_eq!(
                receive(&completions, 3).response.err().unwrap().to_string(),
                "stale or sealed request authority"
            );
            assert_eq!(old.receipt_count(), published);
            assert_eq!(old.appended_frontier(), frame.len() as u64);
            assert_eq!(node.control.preparer_count(), 0);
            assert!(node.control.shared.syncers.lock().is_empty());
            drop(old);
            shutdown(&node, 2);
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
            &append_shape(&node),
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
            &append_shape(&node),
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
            &append_shape(&node),
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
                &append_shape(&node),
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
    assert_eq!(PROTOCOL, 32);
    preface[4..6].copy_from_slice(&31u16.to_be_bytes());
    assert!(read_transport_mode(&mut &preface[..], &Transport::TcpTrusted).is_err());
}

/// R3 removed the read executor; `Status.max_reads` and the
/// `ResolveLiveRecords` count bound must not change with it. Absent keys mean
/// async member reads with 128 device reads (Status 128); 300 device reads
/// are capped by the wire limit (256); mapped member reads report
/// `limits.max_reads` whatever the device bound. One resolution accepts
/// exactly `min(max_reads, 256)` references and rejects one more.
#[test]
fn max_reads_and_live_resolution_bound_are_unchanged() {
    use crate::distributed::storage::WalMemberReadPolicy;
    let temp = tempdir::TempDir::new("max-reads-invariance").unwrap();
    let mut document = serde_yaml::to_value(config(&temp.path().join("base"))).unwrap();
    let fields = document.as_mapping_mut().unwrap();
    fields.remove("wal_member_read");
    fields.remove("read_max_device_reads");
    let defaults: NodeConfig = serde_yaml::from_value(document).unwrap();
    assert_eq!(defaults.wal_member_read, WalMemberReadPolicy::Async);
    assert_eq!(defaults.read_max_device_reads, 128);
    let wire = TcpLimits::default();
    assert_eq!(wire.max_reads, 256);
    let narrow = TcpLimits {
        max_reads: 64,
        ..TcpLimits::default()
    };
    for (name, policy, device_reads, limits, expected) in [
        (
            "default",
            defaults.wal_member_read,
            defaults.read_max_device_reads,
            wire,
            128,
        ),
        ("device-300", WalMemberReadPolicy::Async, 300, wire, 256),
        (
            "device-300-narrow",
            WalMemberReadPolicy::Async,
            300,
            narrow,
            64,
        ),
        ("mapped", WalMemberReadPolicy::Mapped, 8, wire, 256),
        ("mapped-narrow", WalMemberReadPolicy::Mapped, 8, narrow, 64),
    ] {
        let mut config = defaults.clone();
        config.root = temp.path().join(name);
        config.wal_member_read = policy;
        config.read_max_device_reads = device_reads;
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        let Response::Status(status) = node
            .handle(Request::Status { protocol: PROTOCOL }, limits)
            .unwrap()
        else {
            panic!("{name}: status reply");
        };
        assert_eq!(status.max_reads, expected, "{name}");
        {
            node.inventory.lock().fence_to(1).unwrap();
            let appended = node
                .inventory
                .lock()
                .active(1)
                .unwrap()
                .append_complete_batch(
                    crate::distributed::types::RequestAuthority { epoch: 1 },
                    &batch(&config, 1, 0),
                )
                .unwrap();
            let reference = batch(&config, 1, 0)
                .record_reference(appended.address(), 0)
                .unwrap();
            let over_bound = |count: usize| {
                matches!(
                    node.handle(
                        Request::ResolveLiveRecords {
                            authority_epoch: 1,
                            references: vec![reference; count],
                        },
                        limits,
                    ),
                    Err(error) if error.to_string() == "live record count exceeds negotiated bound"
                )
            };
            let bound = expected.min(256);
            assert!(over_bound(0), "{name}: empty resolution");
            assert!(!over_bound(bound), "{name}: {bound} references");
            assert!(over_bound(bound + 1), "{name}: {} references", bound + 1);
        }
    }
}
