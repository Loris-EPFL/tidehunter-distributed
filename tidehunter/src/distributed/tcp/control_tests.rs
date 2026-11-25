use super::super::tests::{batch, config, metrics};
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::storage::RetirementOutcome;
use crate::distributed::test_bounds::PROGRESS_CAP;

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
        progress: None,
    })
}
fn receive(
    completions: &mpsc::Receiver<reactor::WorkCompletion>,
    id: u64,
) -> reactor::WorkCompletion {
    // The completion follows owner control progress.
    let completion = completions.recv_timeout(PROGRESS_CAP).unwrap();
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
fn live_resolution_rejects_foreign_address_and_stale_authority() {
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
    let member = crate::distributed::storage::member_address_of_reference(&reference).unwrap();
    let foreign = crate::distributed::types::PhysicalValueAddress {
        partition: member.partition + 1,
        ..member
    };
    for (id, epoch, member) in [(2, 1, foreign), (3, 2, member)] {
        assert!(
            command(
                &node,
                id,
                Request::ResolveLiveRecords {
                    authority_epoch: epoch,
                    members: vec![member],
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
                host_nqn: crate::distributed::tcp::host_identity::effective_host_nqn(
                    &config.partition.inventory.database,
                    config.partition.partition,
                ),
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
                    false,
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
                    false,
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
    fn prepared_never_granted_exports_survive_fence_and_seal() {
        for seal in [false, true] {
            let temp = tempdir::TempDir::new("prepared-retirement").unwrap();
            let (_, node) = open_live(temp.path(), DurabilityTier::Native);
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 });
            let log = node.inventory.lock().active(1).unwrap();
            wait_prepared(&log, 1);
            let owned = node
                .with_live_target(|target| target.stats())
                .unwrap()
                .owned_namespace_count;
            assert_eq!(owned, 2, "the epoch exported both live files");
            drop(log);
            if seal {
                shutdown(&node, 1);
                assert_eq!(node.control.preparer_count(), 0);
                assert_eq!(
                    node.with_live_target(|target| target.stats())
                        .unwrap()
                        .owned_namespace_count,
                    0
                );
            } else {
                fence(&node, 2).unwrap();
                // A permanent export lives as long as its file: the fence
                // write-protects it instead of removing it. The fenced epoch
                // prepares and exports its own first file, so the count grows
                // by exactly that one.
                let next = node.log_for_test(2);
                wait_prepared(&next, 1);
                drop(next);
                assert_eq!(node.control.preparer_count(), 1);
                assert_eq!(
                    node.with_live_target(|target| target.stats())
                        .unwrap()
                        .owned_namespace_count,
                    owned + FENCED_FIRST_FILE_EXPORT
                );
            }
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

    /// A grant whose export is paused inside a configfs mutation holds only
    /// the lifecycle gate: an inventory reader and a position proof on another
    /// control worker complete while the export is still paused.
    #[test]
    fn foreground_work_proceeds_while_a_target_export_is_paused() {
        let temp = tempdir::TempDir::new("paused-target-export").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let (entered, paused) = mpsc::sync_channel(1);
        let (resume, resumed) = mpsc::sync_channel::<()>(1);
        let mut armed = true;
        node.with_live_target(|target| {
            target.set_configfs_hook(Box::new(move |_index| {
                if armed {
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
            .expect("inventory is held across a target export");
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
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let log = node.inventory.lock().active(1).unwrap();
        log.lease_live_extent(RequestAuthority { epoch: 1 })
            .unwrap();
        // A regular file where the export must create its subsystem makes
        // publication fail after the lease exists.
        let name = crate::distributed::nvmeof::live_storage::wal_export_name(
            crate::distributed::nvmeof::live_storage::FILES_PER_SUBSYSTEM,
            config.partition.inventory.database,
            config.partition.partition,
            1,
            0,
        );
        fs::write(temp.path().join("configfs/subsystems").join(&name.nqn), b"").unwrap();
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
        // Pin release must retain this exact permanent export both before and
        // after the preparer publishes the next WAL file.
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

    /// A fenced epoch starts with no file and no published namespace. The
    /// owner prepares the first file (allocation, file sync, directory
    /// witness, permanent export) in the background after the fence, so the
    /// epoch's first lease is memory work instead of two syncs under its
    /// deadline.
    #[test]
    fn fence_prepares_the_first_live_file_in_the_background() {
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let temp = tempdir::TempDir::new("fence-first-file").unwrap();
            let (config, node) = open_live(temp.path(), tier);
            assert!(matches!(fence(&node, 2).unwrap(), Response::Status(_)));
            let log = node.log_for_test(2);
            wait_prepared(&log, 1);
            let (entered, resume) = log.pause_next_persist();
            let (tx, rx) = mpsc::sync_channel(1);
            let lease_node = node.clone();
            let worker = std::thread::spawn(move || {
                let leased = extent_exchange(&lease_node, Request::LeaseLiveExtent { epoch: 2 });
                tx.send(leased).expect("test receiver dropped");
            });
            let parked = entered.recv_timeout(Duration::from_millis(500)).is_ok();
            drop(resume); // Unblock a broken implementation before reporting failure.
            let response = rx.recv_timeout(PROGRESS_CAP).unwrap();
            worker.join().unwrap();
            assert!(!parked, "the first lease synced the prepared file itself");
            let Response::LiveLeased(leased) = response else {
                panic!("expected lease")
            };
            assert_eq!(leased.profile.namespace_sync_ns, 0);
            assert_eq!(leased.extent.segment.file_index, 0);
            assert_eq!(config.partition.wal_file_bytes, 32 * 1024);
            drop(log);
            shutdown(&node, 2);
        }
    }

    /// The trigger skips an owner that is sealing (`terminal`) or has a
    /// pending poison, even while its inventory still answers `active`, and
    /// starts a preparer for a healthy owner: the third call is the positive
    /// control that the first two skip because of the guard. A failed fence
    /// must not prepare either: terminal publication may land between its
    /// reply and the trigger.
    #[test]
    fn fence_time_preparation_skips_a_terminal_or_poison_pending_owner() {
        let temp = tempdir::TempDir::new("fence-prepare-guard").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let context = &node.control_context;
        context.terminal.store(true, Ordering::Release);
        node.control.shared.prepare_fenced_epoch(1);
        assert_eq!(
            node.control.preparer_count(),
            0,
            "a terminal owner started a preparer"
        );
        context.terminal.store(false, Ordering::Release);
        context.poison_pending.store(true, Ordering::Release);
        node.control.shared.prepare_fenced_epoch(1);
        assert_eq!(
            node.control.preparer_count(),
            0,
            "a poison-pending owner started a preparer"
        );
        context.poison_pending.store(false, Ordering::Release);
        node.control.shared.prepare_fenced_epoch(1);
        assert_eq!(node.control.preparer_count(), 1);
        // A poisoned owner's fence fails, and the trigger after it prepares
        // nothing for the epoch it never fenced.
        node.control_context.poison_readers();
        assert!(fence(&node, 2).is_err());
        node.control.shared.prepare_fenced_epoch(2);
        assert_eq!(
            node.control.preparer_count(),
            1,
            "a poisoned owner prepared a first file for an unfenced epoch"
        );
    }

    /// The first read on a fresh lane sends `OpenLiveExports`. It must not wait
    /// for an in-flight adoption scan, and it must not run the owner-wide flush
    /// barrier: a read would stall for the whole scan and the flush otherwise.
    #[test]
    fn live_exports_are_served_while_an_adoption_is_parked() {
        use crate::distributed::storage::LivePausePoint;
        for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
            let temp = tempdir::TempDir::new("exports-during-adoption").unwrap();
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
            let through = leased.extent.start + frame.len() as u64;
            let token = leased.extent.token;
            let log = node.log_for_test(1);
            let (entered, resume) = log.pause_live(LivePausePoint::ValidateUnlocked);
            let (adopted, adoption) = mpsc::sync_channel(1);
            let (exported, exports) = mpsc::sync_channel(1);
            std::thread::scope(|scope| {
                let adopter = node.clone();
                scope.spawn(move || {
                    let response = extent_exchange(
                        &adopter,
                        Request::AdoptLiveExtent {
                            epoch: 1,
                            token,
                            through,
                            close: false,
                            lease_next: false,
                        },
                    );
                    adopted.send(response).expect("test receiver dropped");
                });
                entered.recv_timeout(PROGRESS_CAP).unwrap();
                let barriers = node.flush_barriers_sent();
                let exporter = node.clone();
                scope.spawn(move || {
                    let response =
                        extent_exchange(&exporter, Request::OpenLiveExports { authority_epoch: 1 });
                    exported.send(response).expect("test receiver dropped");
                });
                let response = exports.recv_timeout(PROGRESS_CAP);
                resume.send(()).unwrap();
                let pending_adoption = adoption.recv_timeout(PROGRESS_CAP);
                let response = response.expect("the export waited for the parked adoption");
                let Response::LiveExports(info) = response else {
                    panic!("expected exports")
                };
                assert_eq!(info.wal_file_bytes, config.partition.wal_file_bytes);
                assert_eq!(info.exported_files, 1);
                assert_eq!(
                    node.flush_barriers_sent(),
                    barriers,
                    "the export ran the owner-wide flush barrier"
                );
                assert!(pending_adoption.is_ok());
            });
            drop(log);
            shutdown(&node, 1);
        }
    }

    /// An export in flight counts as stream work: a fence or seal waits for it
    /// instead of revoking the exports it is installing.
    #[test]
    fn a_fence_or_seal_waits_for_an_in_flight_live_export() {
        for seal in [false, true] {
            let temp = tempdir::TempDir::new("fence-during-export").unwrap();
            let (_, node) = open_live(temp.path(), DurabilityTier::Native);
            let (entered, parked) = mpsc::sync_channel(1);
            let (resume, released) = mpsc::sync_channel(1);
            *node.control_context.before_open_live_exports.lock() = Some(Box::new(move || {
                // Runs on an owner control thread, which must not panic: a
                // test that failed early dropped its ends, and that means
                // "do not park".
                if entered.send(()).is_ok() {
                    released.recv_timeout(PROGRESS_CAP).ok();
                }
            }));
            let (completed, completions) = mpsc::sync_channel(2);
            assert!(
                command(
                    &node,
                    1,
                    Request::OpenLiveExports { authority_epoch: 1 },
                    4096,
                    &completed,
                )
                .is_ok()
            );
            parked.recv_timeout(PROGRESS_CAP).unwrap();
            let barrier = if seal {
                Request::Seal { epoch: 1 }
            } else {
                Request::Fence { epoch: 2 }
            };
            assert!(command(&node, 2, barrier, 1, &completed).is_ok());
            {
                let queue = node.control.shared.queue.lock();
                // Positive queue state: the export holds the owner, and the
                // barrier waits behind it instead of racing its installation.
                assert_eq!(
                    queue.active_exports, 1,
                    "seal {seal}: the export left the owner before the barrier"
                );
                assert!(
                    queue.work.iter().any(|work| work.id == 2
                        && matches!(work.request, Request::Fence { .. } | Request::Seal { .. })),
                    "seal {seal}: the barrier started while an export was in flight"
                );
            }
            // The barrier may not be served while the export holds the owner:
            // no completion at all may arrive before the export returns. This
            // is a negative check, so a slow machine only makes it more certain.
            assert!(
                matches!(
                    completions.recv_timeout(Duration::from_millis(500)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ),
                "seal {seal}: the barrier completed while an export was in flight"
            );
            resume.send(()).unwrap();
            let Response::LiveExports(_) = receive(&completions, 1).response.unwrap() else {
                panic!("expected exports")
            };
            let response = receive(&completions, 2).response.unwrap();
            if seal {
                assert!(
                    matches!(response, Response::Sealed),
                    "seal 1: expected the seal reply, not an export"
                );
            } else {
                let Response::Status(StatusReply::Database(status)) = response else {
                    panic!("expected fence")
                };
                assert_eq!(status.active_epoch, Some(2));
            }
            shutdown(&node, if seal { 1 } else { 2 });
        }
    }

    /// The other direction of the same exclusion: a serving retirement must
    /// not start while an export is installing bindings, because the export
    /// could republish a victim the retirement is about to tombstone.
    #[test]
    fn a_serving_retirement_waits_for_an_in_flight_live_export() {
        let temp = tempdir::TempDir::new("retirement-during-export").unwrap();
        let (_, node) = open_live(temp.path(), DurabilityTier::Native);
        let (entered, parked) = mpsc::sync_channel(1);
        let (resume, released) = mpsc::sync_channel(1);
        *node.control_context.before_open_live_exports.lock() = Some(Box::new(move || {
            if entered.send(()).is_ok() {
                released.recv_timeout(PROGRESS_CAP).ok();
            }
        }));
        let (completed, completions) = mpsc::sync_channel(3);
        assert!(
            command(
                &node,
                1,
                Request::OpenLiveExports { authority_epoch: 1 },
                4096,
                &completed,
            )
            .is_ok()
        );
        parked.recv_timeout(PROGRESS_CAP).unwrap();
        // No witness: this test asserts the scheduler's class gate, not a
        // retirement's durable effect. The handler refuses it by name, and only
        // after the export it must not overlap has finished.
        assert!(
            command(
                &node,
                2,
                Request::RetireWalFiles {
                    authority_epoch: 1,
                    epoch: 1,
                    files: Vec::new(),
                    witness: None,
                },
                1,
                &completed,
            )
            .is_ok()
        );
        {
            let queue = node.control.shared.queue.lock();
            assert_eq!(
                queue.active_exports, 1,
                "the export left before the retirement was queued"
            );
            assert!(
                queue.work.iter().any(|work| work.id == 2),
                "the retirement started while an export was in flight"
            );
            assert_eq!(queue.active_retirements, 0);
        }
        // The retirement may not be served while the export holds the owner: no
        // completion at all may arrive before the export returns. This is a
        // negative check, so a slow machine only makes it more certain, and it
        // is the same window the dead-WAL park test uses.
        assert!(
            matches!(
                completions.recv_timeout(Duration::from_millis(500)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "the retirement completed while an export was in flight"
        );
        resume.send(()).unwrap();
        let Response::LiveExports(_) = receive(&completions, 1).response.unwrap() else {
            panic!("expected exports")
        };
        assert!(
            receive(&completions, 2)
                .response
                .err()
                .unwrap()
                .to_string()
                .contains("requires an Exact witness"),
            "the retirement did not reach its handler after the export"
        );
        shutdown(&node, 1);
    }

    #[test]
    fn permanent_owner_rejects_active_record_resolution() {
        let temp = tempdir::TempDir::new("permanent-controls").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        // The refusal precedes address validation: naming the active generation suffices.
        let member = PhysicalValueAddress {
            partition: config.partition.partition,
            segment_generation: 1,
            offset: 0,
            len: 64,
        };
        let error = node
            .handle(
                Request::ResolveLiveRecords {
                    authority_epoch: 1,
                    members: vec![member],
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

    pub(in crate::distributed::tcp::control) fn fence(
        node: &StorageNode,
        epoch: u64,
    ) -> Result<Response, TcpError> {
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

    pub(in crate::distributed::tcp::control) fn leased_history(
        node: &StorageNode,
        config: &NodeConfig,
        extents: usize,
    ) -> u64 {
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
        fence_prepared(node, 2);
        end
    }

    /// Exports the owner holds for a fenced epoch's first file, which the
    /// fence prepares and exports in the background. `owned_namespace_count`
    /// includes it until that epoch ends.
    const FENCED_FIRST_FILE_EXPORT: u64 = 1;

    /// Fence `epoch`, then wait until its first prepared file is published,
    /// so export counts do not depend on the background preparation's timing.
    fn fence_prepared(node: &StorageNode, epoch: u64) {
        fence(node, epoch).unwrap();
        let log = node.log_for_test(epoch);
        wait_prepared(&log, 1);
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
        assert!(entry.binding.permanent);
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
            host_nqn: crate::distributed::tcp::host_identity::effective_host_nqn(
                &config.partition.inventory.database,
                config.partition.partition,
            ),
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
            // The fence that sealed the stream write-protects its exports and
            // keeps them for the life of their files.
            assert!(target_stats(&node).owned_namespace_count > 0);
            let first = history_page(&node, &config, 0, end, 0);
            let exposed = target_stats(&node).namespaces_exposed;
            release_history_page(&node, &first);
            assert!(target_stats(&node).owned_namespace_count > 0);
            let second = history_page(&node, &config, first.next.unwrap() as u64, end, 1);
            assert_eq!(first.entries[0].segment, second.entries[0].segment);
            assert!(
                first.entries[0].binding == second.entries[0].binding,
                "the sealed page reused a different export binding"
            );
            assert_eq!(target_stats(&node).namespaces_exposed, exposed);
            release_history_page(&node, &second);
            // A higher-epoch fence write-protects the sealed exports; they
            // remain readable until their files are retired or the owner shuts
            // down. The fenced epoch adds only its own prepared first file.
            fence_prepared(&node, 3);
            assert!(target_stats(&node).owned_namespace_count > 0);
            shutdown(&node, 3);
        }
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
            assert!(target_stats(&node).owned_namespace_count > 0);
            node.with_lifecycle(|inventory, slot| {
                let target = slot.as_mut().unwrap();
                assert_eq!(
                    NodeInventory::publish_permanent_segment(inventory, target, segment, false)
                        .err()
                        .unwrap()
                        .to_string(),
                    "live segment identity or access changed"
                );
                let (binding, created) =
                    NodeInventory::publish_permanent_segment(inventory, target, segment, true)
                        .unwrap();
                assert!(!created, "a sealed page reused its existing export");
                assert!(binding.readonly, "a sealed page stayed read-only");
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
                    assert!(target_stats(&node).owned_namespace_count > 0);
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
            let Response::Status(StatusReply::Database(status)) = fence(&node, 2).unwrap() else {
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
        let Response::Status(StatusReply::Database(status)) =
            receive(&completions, 2).response.unwrap()
        else {
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
            let Response::Status(StatusReply::Database(status)) =
                receive(&completions, 2).response.unwrap()
            else {
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
            // The fence retired the old epoch's preparer and started the new
            // epoch's first-file preparation.
            let active = node.inventory.lock().active(2).unwrap();
            {
                let slots = node.control.shared.preparers.lock();
                assert_eq!(slots.len(), 1);
                assert!(Arc::ptr_eq(&slots[0].log, &active));
            }
            drop(active);
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
        let Response::Status(StatusReply::Database(status)) = fence(&node, 2).unwrap() else {
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
    assert_eq!(PROTOCOL, 37);
    preface[4..6].copy_from_slice(&u16::try_from(PROTOCOL - 1).unwrap().to_be_bytes());
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
    fields.remove("wal_member_read_policy");
    fields.remove("max_device_reads_per_status");
    let defaults: NodeConfig = serde_yaml::from_value(document).unwrap();
    assert_eq!(defaults.wal_member_read_policy, WalMemberReadPolicy::Async);
    assert_eq!(defaults.max_device_reads_per_status, 128);
    let wire = TcpLimits::default();
    assert_eq!(wire.max_reads, 256);
    let narrow = TcpLimits {
        max_reads: 64,
        ..TcpLimits::default()
    };
    for (name, policy, device_reads, limits, expected) in [
        (
            "default",
            defaults.wal_member_read_policy,
            defaults.max_device_reads_per_status,
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
        config.wal_member_read_policy = policy;
        config.max_device_reads_per_status = device_reads;
        let node = StorageNode::open(config.clone(), metrics()).unwrap();
        let Response::Status(StatusReply::Database(status)) = node
            .handle(
                Request::Status {
                    protocol: PROTOCOL,
                    database: Some(node.database()),
                },
                limits,
            )
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
            let member =
                crate::distributed::storage::member_address_of_reference(&reference).unwrap();
            let over_bound = |count: usize| {
                matches!(
                    node.handle(
                        Request::ResolveLiveRecords {
                            authority_epoch: 1,
                            members: vec![member; count],
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

#[path = "control_tests/lifetime_export_tests.rs"]
mod lifetime_export_tests;
