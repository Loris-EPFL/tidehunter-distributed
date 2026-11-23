//! Owner fencing-generation ledger, inventory pages and generation trim (L1).
//! Crash points report an error at a durable boundary; the test then reopens
//! the owner from disk, which is exactly the durable state at that boundary
//! because every ledger install and directory deletion syncs before it returns.

use super::super::super::generation_control::{
    InventoryGeneration, InventoryPageRequest, OwnerGenerationState, OwnerTrimCrash, TrimReadiness,
    check_page,
};
use super::*;
use crate::distributed::tcp::tests::{batch, config, metrics};

const FRAME: usize = 16 * 1024 * 1024;

fn fenced(root: &Path, generations: u64) -> NodeInventory {
    let mut inventory = NodeInventory::open(config(root), metrics()).unwrap();
    for generation in 1..=generations {
        inventory.fence_to(generation).unwrap();
    }
    inventory
}

fn page(
    inventory: &NodeInventory,
    after: Option<u64>,
    limit: u16,
) -> Result<super::super::super::generation_control::InventoryPageReply, TcpError> {
    let request = InventoryPageRequest {
        incarnation: inventory.incarnation(),
        revision: inventory.inventory_revision(),
        after,
        limit,
    };
    let reply = inventory.inventory_page(&request, FRAME)?;
    check_page(&request, &reply).map_err(TcpError::new)?;
    Ok(reply)
}

fn ledger(inventory: &NodeInventory) -> Vec<(u64, EpochState)> {
    inventory
        .manifest
        .epochs
        .iter()
        .map(|entry| (entry.epoch, entry.state))
        .collect()
}

fn trim(inventory: &mut NodeInventory, authority: u64, generation: u64) -> Result<(), TcpError> {
    NodeInventory::trim_generation(
        &mut &mut *inventory,
        &mut LifecycleSlot::default(),
        authority,
        generation,
    )
}

#[test]
fn inventory_pages_are_sorted_bounded_and_frozen_at_one_revision() {
    let temporary = tempdir::TempDir::new("owner-generation-pages").unwrap();
    let mut inventory = fenced(&temporary.path().join("node"), 5);
    let first = page(&inventory, None, 2).unwrap();
    assert_eq!(
        first.entries,
        vec![
            InventoryGeneration {
                generation: 1,
                state: OwnerGenerationState::Sealed,
                durable_end: 0,
            },
            InventoryGeneration {
                generation: 2,
                state: OwnerGenerationState::Sealed,
                durable_end: 0,
            },
        ]
    );
    assert_eq!(first.next, Some(2));
    let second = page(&inventory, first.next, 2).unwrap();
    assert_eq!(second.next, Some(4));
    let last = page(&inventory, second.next, 2).unwrap();
    assert_eq!(
        last.entries,
        vec![InventoryGeneration {
            generation: 5,
            state: OwnerGenerationState::Active,
            durable_end: 0,
        }]
    );
    assert_eq!(last.next, None);
    assert!(page(&inventory, None, 0).is_err());
    assert!(page(&inventory, None, u16::MAX).is_err());
    // Any ledger install raises the revision; a sweep frozen before it fails.
    let stale = InventoryPageRequest {
        incarnation: inventory.incarnation(),
        revision: inventory.inventory_revision(),
        after: None,
        limit: 2,
    };
    inventory.fence_to(6).unwrap();
    assert!(inventory.inventory_revision() > stale.revision);
    assert!(
        inventory
            .inventory_page(&stale, FRAME)
            .unwrap_err()
            .to_string()
            .contains("inventory changed during pagination")
    );
    let status = inventory.status().unwrap();
    assert_eq!(status.highest_fenced_generation, 6);
    assert_eq!(status.inventory_revision, inventory.inventory_revision());
}

#[test]
fn empty_generation_trim_is_durable_idempotent_and_never_refenced() {
    let temporary = tempdir::TempDir::new("owner-generation-trim").unwrap();
    let root = temporary.path().join("node");
    let mut inventory = fenced(&root, 3);
    let root = inventory.config.root.clone();
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 3, 1)
            .unwrap()
            .0,
        TrimReadiness::EmptySealed
    );
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 3, 3)
            .unwrap()
            .0,
        TrimReadiness::Blocked
    );
    assert!(trim(&mut inventory, 3, 3).is_err());
    assert!(trim(&mut inventory, 2, 1).is_err(), "stale authority");
    trim(&mut inventory, 3, 1).unwrap();
    assert!(!epoch_path(&root, 1).exists());
    assert_eq!(
        ledger(&inventory),
        vec![(2, EpochState::Sealed), (3, EpochState::Active)]
    );
    assert_eq!(inventory.manifest.trim_pending, None);
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 3, 1)
            .unwrap()
            .0,
        TrimReadiness::Absent
    );
    trim(&mut inventory, 3, 1).unwrap();
    assert_eq!(
        inventory.fence_to(1).unwrap_err().to_string(),
        "fencing generation 1 already fenced or sealed (owner high-water 3)"
    );
    assert!(inventory.fence_to(2).is_err());
    drop(inventory);
    let mut reopened = NodeInventory::open(config(&root), metrics()).unwrap();
    assert_eq!(
        ledger(&reopened),
        vec![(2, EpochState::Sealed), (3, EpochState::Sealed)]
    );
    assert_eq!(reopened.highest_fenced_generation(), 3);
    assert!(reopened.fence_to(3).is_err());
    reopened.fence_to(4).unwrap();
}

#[test]
fn interrupted_trim_resumes_from_the_durable_intent_at_open() {
    for point in [OwnerTrimCrash::AfterLedger, OwnerTrimCrash::AfterUnlink] {
        let temporary = tempdir::TempDir::new("owner-generation-trim-crash").unwrap();
        let mut inventory = fenced(&temporary.path().join("node"), 3);
        let root = inventory.config.root.clone();
        inventory.fail_next_trim(point);
        assert!(trim(&mut inventory, 3, 2).is_err());
        assert_eq!(inventory.manifest.trim_pending, Some(2));
        assert_eq!(
            epoch_path(&root, 2).exists(),
            point == OwnerTrimCrash::AfterLedger
        );
        drop(inventory);
        let mut reopened = NodeInventory::open(config(&root), metrics()).unwrap();
        assert!(!epoch_path(&root, 2).exists());
        assert_eq!(reopened.manifest.trim_pending, None);
        assert_eq!(
            ledger(&reopened),
            vec![(1, EpochState::Sealed), (3, EpochState::Sealed)]
        );
        // The coordinator resends the same trim under its next fence.
        reopened.fence_to(4).unwrap();
        trim(&mut reopened, 4, 2).unwrap();
        // A second reopen sees the same state.
        drop(reopened);
        let again = NodeInventory::open(config(&root), metrics()).unwrap();
        assert_eq!(
            ledger(&again),
            vec![
                (1, EpochState::Sealed),
                (3, EpochState::Sealed),
                (4, EpochState::Sealed)
            ]
        );
    }
}

#[test]
fn unregistered_generation_directory_without_intent_is_corruption() {
    let temporary = tempdir::TempDir::new("owner-generation-foreign").unwrap();
    let inventory = fenced(&temporary.path().join("node"), 2);
    let root = inventory.config.root.clone();
    drop(inventory);
    fs::create_dir(epoch_path(&root, 9)).unwrap();
    assert!(NodeInventory::open(config(&root), metrics()).is_err());
}

#[test]
fn generation_with_accepted_data_is_blocked() {
    let temporary = tempdir::TempDir::new("owner-generation-data").unwrap();
    let config = config(&temporary.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.handle(Request::Fence { epoch: 1 }, TcpLimits::default())
        .unwrap();
    node.stream_log_for_test(1)
        .append_complete_batch(RequestAuthority { epoch: 1 }, &batch(&config, 1, 0))
        .unwrap();
    node.handle(Request::Fence { epoch: 2 }, TcpLimits::default())
        .unwrap();
    let check = node
        .handle(
            Request::CheckTrimGeneration(
                super::super::super::generation_control::CheckTrimRequest {
                    authority_generation: 2,
                    generation: 1,
                },
            ),
            TcpLimits::default(),
        )
        .unwrap();
    assert!(matches!(
        check,
        Response::CheckTrimGeneration(reply)
            if reply.generation == 1 && reply.result == TrimReadiness::Blocked
    ));
    let trimmed = node.handle(
        Request::TrimGeneration(super::super::super::generation_control::TrimRequest {
            authority_generation: 2,
            generation: 1,
        }),
        TcpLimits::default(),
    );
    assert_eq!(
        trimmed.err().unwrap().to_string(),
        "owner 2 cannot trim fencing generation 1: durability proof"
    );
    assert!(
        node.inventory
            .lock()
            .manifest
            .epochs
            .iter()
            .any(|e| e.epoch == 1)
    );
}

// What these tests prove (labels):
// - The `TH_TRIM_CONTROL_*` variables are fixture-validity controls only. They
//   change the test setup, not production code, so they do not prove a rule.
// - Production rules are proved by private production mutants run on these
//   unchanged tests (evidence in the handoff, not in this patch): skipped
//   reader-map removal, skipped sole-reference check, removed retired-prefix
//   and below-end checks, unlink before marker release, removed hole check,
//   and a missing seal treated as the end.
// - The old-rule mutant (any present file blocks) is broad. It turns six
//   tests red, not only the later-frame test. The unlink-before-marker
//   mutant shows that the order is observable; its red tests fail on an I/O
//   error, so it does not prove that every crash assertion discriminates.
// - The reader snapshot and lease tests use a sole-reference proxy taken
//   after the fence. They do not prove a live-export pin.
// - The same-fragment test records SEAL end 0, no replay hole, and a direct
//   owner read of the later frame. It makes no coordinator, retained-view or
//   index claim.
#[test]
fn trim_blocks_file_below_sealed_end() {
    let temporary = tempdir::TempDir::new("owner-trim-below-end").unwrap();
    let root = temporary.path().join("node");
    let mut inventory = fenced(&root, 1);
    inventory
        .logs
        .get(&1)
        .unwrap()
        .append_complete_batch(
            RequestAuthority { epoch: 1 },
            &batch(&inventory.config, 1, 0),
        )
        .unwrap();
    inventory.fence_to(2).unwrap();
    let end = inventory.logs.get(&1).unwrap().appended_frontier();
    let below = wal_name(&epoch_path(&root, 1), 0);
    assert!(end > 0 && below.exists());
    if std::env::var_os("TH_TRIM_CONTROL_BELOW").is_some() {
        use crate::distributed::storage::{RetireProof, RetirementOutcome};
        assert_eq!(
            inventory
                .logs
                .get(&1)
                .unwrap()
                .retire_sealed_stream_files(&[RetireProof {
                    file_start: 0,
                    inode: None,
                    seal_generation: None,
                }])
                .unwrap(),
            RetirementOutcome::Completed
        );
    }
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 2, 1)
            .unwrap()
            .0,
        TrimReadiness::Blocked
    );
    assert!(trim(&mut inventory, 2, 1).is_err());
    assert!(below.exists());
    assert_eq!(inventory.manifest.trim_pending, None);
}

#[test]
fn trim_retired_generation_with_unwritten_lookahead() {
    use crate::distributed::storage::{RetireProof, RetirementOutcome};

    let temporary = tempdir::TempDir::new("owner-trim-lookahead").unwrap();
    let root = temporary.path().join("node");
    let mut inventory = fenced(&root, 1);
    inventory
        .logs
        .get(&1)
        .unwrap()
        .append_complete_batch(
            RequestAuthority { epoch: 1 },
            &batch(&inventory.config, 1, 0),
        )
        .unwrap();
    inventory.fence_to(2).unwrap();
    let log = inventory.logs.get(&1).unwrap();
    let end = log.appended_frontier();
    let size = inventory.config.partition.wal_file_bytes;
    assert!(end > 0 && end < size);
    let ahead = wal_name(&epoch_path(&root, 1), end.div_ceil(size));
    File::create(&ahead).unwrap().set_len(size).unwrap();
    let second = wal_name(&epoch_path(&root, 1), end.div_ceil(size) + 1);
    File::create(&second).unwrap().set_len(size).unwrap();
    assert_eq!(
        log.retire_sealed_stream_files(&[RetireProof {
            file_start: 0,
            inode: None,
            seal_generation: None,
        }])
        .unwrap(),
        RetirementOutcome::Completed
    );
    if std::env::var_os("TH_TRIM_CONTROL_LOOKAHEAD").is_some() {
        File::create(wal_name(&epoch_path(&root, 1), 0))
            .unwrap()
            .set_len(size)
            .unwrap();
    }
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 2, 1)
            .unwrap()
            .0,
        TrimReadiness::Retired
    );
    trim(&mut inventory, 2, 1).unwrap();
    assert!(!ahead.exists());
    assert!(!second.exists());
    assert!(!epoch_path(&root, 1).exists());
    assert_eq!(inventory.manifest.trim_pending, None);
    drop(inventory);
    let reopened = NodeInventory::open(config(&root), metrics()).unwrap();
    assert!(
        !ledger(&reopened)
            .iter()
            .any(|(generation, _)| *generation == 1)
    );
}

fn trim_live_fixture(root: &Path) -> (NodeConfig, PathBuf) {
    use crate::distributed::nvmeof::{LiveTargetConfig, NvmeofTransport};
    let configfs = root.join("configfs");
    fs::create_dir(&configfs).unwrap();
    for name in ["hosts", "ports", "subsystems"] {
        fs::create_dir(configfs.join(name)).unwrap();
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
        state_dir: root.join("target"),
        commissioning_port_id: 43,
        commissioning_service: 4421,
    });
    fs::create_dir(&config.live_target.as_ref().unwrap().state_dir).unwrap();
    (config, configfs)
}

fn trim_live_gap(next_fragment: bool) {
    use crate::distributed::group::CompleteGroup;
    use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
    use crate::distributed::storage::member_address_of_reference;
    use std::os::unix::fs::FileExt;

    let temporary = tempdir::TempDir::new("owner-trim-gap").unwrap();
    let (config, configfs) = trim_live_fixture(temporary.path());
    let open = || {
        StorageNode::bench_open_live_fixture(config.clone(), metrics(), configfs.clone()).unwrap()
    };
    let node = open();
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, 1, false, |_| Ok(()))
    })
    .unwrap();
    let log = node.inventory.lock().logs.get(&1).unwrap().clone();
    let extent = log
        .lease_live_extent(RequestAuthority { epoch: 1 })
        .unwrap()
        .0;
    let shape = crate::key_shape::KeyShape::from_yaml(&config.shape_yaml).unwrap();
    let item = batch(&config, 1, 1);
    let group = CompleteGroup::new_live(
        &shape,
        vec![item.clone()],
        config.partition.batch_limits,
        config.partition.group_limits,
    )
    .unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    let mut corrupt = frame.clone();
    corrupt[100] ^= 1;
    let later = if next_fragment { 16 * 1024 } else { 4096 };
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap();
    file.write_all_at(&corrupt, 0).unwrap();
    let control = if next_fragment {
        "TH_TRIM_CONTROL_GAP"
    } else {
        "TH_TRIM_CONTROL_LATER"
    };
    file.write_all_at(
        if std::env::var_os(control).is_some() {
            &corrupt
        } else {
            &frame
        },
        later,
    )
    .unwrap();
    file.sync_all().unwrap();
    drop(file);
    drop(log);
    drop(node);
    let node = open();
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, 2, false, |_| Ok(()))
    })
    .unwrap();
    let log = node.inventory.lock().logs.get(&1).unwrap().clone();
    assert_eq!(log.recovery_found_hole(), next_fragment);
    assert_eq!(log.appended_frontier(), 0);
    assert_eq!(
        crate::distributed::storage::trim_sealed_end(&epoch_path(&config.root, 1), 1).unwrap(),
        Some(0)
    );
    assert!(log.replay_page(0, 16, FRAME, 0).unwrap().groups.is_empty());
    let decoded = log.decode_group_at(later).unwrap();
    assert_eq!(decoded.group.batches()[0].id(), item.id());
    let reference = decoded.group.batches()[0]
        .record_reference(decoded.receipts[0].address(), 0)
        .unwrap();
    let address = member_address_of_reference(&reference).unwrap();
    let lease = log.resolve_live_member(address).unwrap();
    let mut bytes = vec![0; lease.len as usize];
    File::open(&lease.segment.path)
        .unwrap()
        .read_exact_at(&mut bytes, lease.offset)
        .unwrap();
    let start = (address.offset - later) as usize;
    assert_eq!(bytes, frame[start..start + bytes.len()]);
    drop(lease);
    drop(log);
    if next_fragment {
        assert_eq!(
            NodeInventory::trim_readiness(&mut node.inventory.lock(), 2, 1)
                .unwrap()
                .0,
            TrimReadiness::Blocked
        );
        assert!(
            node.with_lifecycle(|inventory, slot| NodeInventory::trim_generation(
                inventory, slot, 2, 1
            ))
            .is_err()
        );
        assert_eq!(node.inventory.lock().manifest.trim_pending, None);
        assert!(extent.segment.path.exists());
    } else {
        // Native replay stops at the first bad CRC; later bytes are torn tail.
        // A direct owner read can still reach them before the file is trimmed.
        assert_eq!(
            NodeInventory::trim_readiness(&mut node.inventory.lock(), 2, 1)
                .unwrap()
                .0,
            TrimReadiness::EmptySealed
        );
        node.with_lifecycle(|inventory, slot| {
            NodeInventory::trim_generation(inventory, slot, 2, 1)
        })
        .unwrap();
        assert!(!extent.segment.path.exists());
        assert_eq!(node.inventory.lock().manifest.trim_pending, None);
        drop(node);
        let reopened = open();
        assert!(!reopened.inventory.lock().logs.contains_key(&1));
        assert!(!extent.segment.path.exists());
    }
}

#[test]
fn trim_blocks_recovered_gap() {
    trim_live_gap(true);
}

#[test]
fn trim_discards_valid_frame_after_bad_checksum_in_same_fragment() {
    trim_live_gap(false);
}

#[test]
fn trim_blocks_missing_seal_with_wal_files() {
    let temporary = tempdir::TempDir::new("owner-trim-missing-seal").unwrap();
    let root = temporary.path().join("node");
    let mut inventory = fenced(&root, 1);
    inventory.fence_to(2).unwrap();
    let directory = epoch_path(&root, 1);
    let wal = wal_name(&directory, 0);
    File::create(&wal)
        .unwrap()
        .set_len(inventory.config.partition.wal_file_bytes)
        .unwrap();
    // The fence installed a zero-end seal; removing it models a lost seal.
    assert_eq!(
        crate::distributed::storage::trim_sealed_end(&directory, 1).unwrap(),
        Some(0)
    );
    if std::env::var_os("TH_TRIM_CONTROL_MISSING_SEAL").is_none() {
        fs::remove_file(directory.join("partition-seal-v5")).unwrap();
        assert_eq!(
            crate::distributed::storage::trim_sealed_end(&directory, 1).unwrap(),
            None
        );
    }
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 2, 1).unwrap(),
        (TrimReadiness::Blocked, "missing sealed end")
    );
    assert!(trim(&mut inventory, 2, 1).is_err());
    assert!(wal.exists());
    assert_eq!(inventory.manifest.trim_pending, None);
}

#[test]
fn trim_never_activated_generation_with_preallocated_files() {
    let temporary = tempdir::TempDir::new("owner-trim-abandoned").unwrap();
    let root = temporary.path().join("node");
    let mut inventory = fenced(&root, 1);
    let directory = epoch_path(&root, 1);
    let ahead = wal_name(&directory, 0);
    File::create(&ahead)
        .unwrap()
        .set_len(inventory.config.partition.wal_file_bytes)
        .unwrap();
    inventory.manifest.epochs[0].state = EpochState::Preparing;
    NodeInventory::persist_manifest(&mut &mut inventory).unwrap();
    drop(inventory);
    let mut inventory = NodeInventory::open(config(&root), metrics()).unwrap();
    assert_eq!(inventory.manifest.epochs[0].state, EpochState::Abandoned);
    inventory.fence_to(2).unwrap();
    if std::env::var_os("TH_TRIM_CONTROL_ABANDONED").is_some() {
        inventory.manifest.epochs[0].state = EpochState::Active;
    }
    assert!(ahead.exists());
    assert_eq!(
        NodeInventory::trim_readiness(&mut &mut inventory, 2, 1)
            .unwrap()
            .0,
        TrimReadiness::EmptySealed
    );
    trim(&mut inventory, 2, 1).unwrap();
    assert!(!directory.exists());
    assert_eq!(inventory.manifest.trim_pending, None);
    drop(inventory);
    let reopened = NodeInventory::open(config(&root), metrics()).unwrap();
    assert!(
        !ledger(&reopened)
            .iter()
            .any(|(generation, _)| *generation == 1)
    );
}

#[test]
fn trim_recovers_after_abandoned_zero_end_seal() {
    let temporary = tempdir::TempDir::new("owner-trim-abandoned-seal").unwrap();
    let root = temporary.path().join("node");
    let mut inventory = fenced(&root, 1);
    let directory = epoch_path(&root, 1);
    let ahead = wal_name(&directory, 0);
    File::create(&ahead)
        .unwrap()
        .set_len(inventory.config.partition.wal_file_bytes)
        .unwrap();
    inventory.manifest.epochs[0].state = EpochState::Preparing;
    NodeInventory::persist_manifest(&mut &mut inventory).unwrap();
    drop(inventory);
    let mut inventory = NodeInventory::open(config(&root), metrics()).unwrap();
    inventory.fence_to(2).unwrap();
    if std::env::var_os("TH_TRIM_CONTROL_EMPTY_SEAL").is_none() {
        inventory.fail_next_trim(OwnerTrimCrash::AfterEmptySeal);
    }
    assert!(trim(&mut inventory, 2, 1).is_err());
    assert_eq!(inventory.manifest.trim_pending, None);
    assert_eq!(
        crate::distributed::storage::trim_sealed_end(&directory, 1).unwrap(),
        Some(0)
    );
    assert!(ahead.exists());
    drop(inventory);
    let mut reopened = NodeInventory::open(config(&root), metrics()).unwrap();
    reopened.fence_to(3).unwrap();
    trim(&mut reopened, 3, 1).unwrap();
    assert!(!directory.exists());
    drop(reopened);
    let again = NodeInventory::open(config(&root), metrics()).unwrap();
    assert!(
        !ledger(&again)
            .iter()
            .any(|(generation, _)| *generation == 1)
    );
}

#[test]
fn trim_lookahead_marker_survives_each_crash_boundary() {
    for point in [
        OwnerTrimCrash::AfterLedger,
        OwnerTrimCrash::AfterLookaheadMarkerUnlink,
        OwnerTrimCrash::AfterLookaheadMarker,
        OwnerTrimCrash::AfterLookaheadFileUnlink,
        OwnerTrimCrash::AfterLookaheadUnlink,
        OwnerTrimCrash::AfterUnlink,
    ] {
        let temporary = tempdir::TempDir::new("owner-trim-marker-crash").unwrap();
        let (config, configfs) = trim_live_fixture(temporary.path());
        let open = || {
            StorageNode::bench_open_live_fixture(config.clone(), metrics(), configfs.clone())
                .unwrap()
        };
        let node = open();
        node.with_lifecycle(|inventory, slot| {
            NodeInventory::fence(inventory, slot, 1, false, |_| Ok(()))
        })
        .unwrap();
        let log = node.inventory.lock().logs.get(&1).unwrap().clone();
        let segment = log
            .lease_live_extent(RequestAuthority { epoch: 1 })
            .unwrap()
            .0
            .segment;
        if std::env::var_os("TH_TRIM_CONTROL_CRASH").is_none() {
            node.with_live_target(|target| {
                let name = crate::distributed::nvmeof::live_storage::wal_export_name(
                    crate::distributed::nvmeof::live_storage::FILES_PER_SUBSYSTEM,
                    segment.database,
                    segment.partition,
                    segment.epoch,
                    segment.file_index,
                );
                let binding = target
                    .export_permanent(&name, live_file(&segment), false)
                    .unwrap();
                target.revoke(&binding).unwrap();
            });
        }
        drop(log);
        node.with_lifecycle(|inventory, slot| {
            NodeInventory::fence(inventory, slot, 2, false, |_| Ok(()))
        })
        .unwrap();
        let directory = epoch_path(&config.root, 1);
        for id in present_wal_files(&directory).unwrap() {
            let path = wal_name(&directory, id);
            if path != segment.path {
                fs::remove_file(path).unwrap();
            }
        }
        File::open(&directory).unwrap().sync_all().unwrap();
        assert!(segment.path.exists());
        assert!(
            node.with_live_target(|target| target.sealed_file(&segment.path).unwrap().is_some())
        );
        node.fail_next_generation_trim(point);
        assert!(
            node.with_lifecycle(|inventory, slot| NodeInventory::trim_generation(
                inventory, slot, 2, 1
            ))
            .is_err()
        );
        assert_eq!(node.inventory.lock().manifest.trim_pending, Some(1));
        assert_eq!(
            node.with_live_target(|target| target.sealed_file(&segment.path).unwrap().is_some()),
            point == OwnerTrimCrash::AfterLedger,
            "marker at {point:?}"
        );
        assert_eq!(
            segment.path.exists(),
            matches!(
                point,
                OwnerTrimCrash::AfterLedger
                    | OwnerTrimCrash::AfterLookaheadMarkerUnlink
                    | OwnerTrimCrash::AfterLookaheadMarker
            ),
            "WAL at {point:?}"
        );
        drop(node);
        let reopened = open();
        assert!(!segment.path.exists());
        assert!(
            reopened
                .with_live_target(|target| target.sealed_file(&segment.path).unwrap().is_none())
        );
        assert_eq!(reopened.inventory.lock().manifest.trim_pending, None);
        assert_eq!(
            reopened.with_live_target(|target| target.stats().unwrap().namespaces_exposed),
            0
        );
        assert!(!reopened.inventory.lock().logs.contains_key(&1));
        reopened
            .with_lifecycle(|inventory, slot| {
                NodeInventory::fence(inventory, slot, 3, false, |_| Ok(()))
            })
            .unwrap();
        reopened
            .with_lifecycle(|inventory, slot| NodeInventory::trim_generation(inventory, slot, 3, 1))
            .unwrap();
        drop(reopened);
        let again = open();
        assert!(!segment.path.exists());
        assert!(
            again.with_live_target(|target| target.sealed_file(&segment.path).unwrap().is_none())
        );
        assert_eq!(
            again.with_live_target(|target| target.stats().unwrap().namespaces_exposed),
            0
        );
        assert!(!again.inventory.lock().logs.contains_key(&1));
    }
}

// Hold a snapshot and its storage lease before live-lease publication.
#[test]
fn trim_refuses_an_outstanding_generation_snapshot() {
    let temporary = tempdir::TempDir::new("owner-trim-snapshot").unwrap();
    let (config, configfs) = trim_live_fixture(temporary.path());
    let node = StorageNode::bench_open_live_fixture(config.clone(), metrics(), configfs).unwrap();
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, 1, false, |_| Ok(()))
    })
    .unwrap();
    let log = node.inventory.lock().logs.get(&1).unwrap().clone();
    let extent = log
        .lease_live_extent(RequestAuthority { epoch: 1 })
        .unwrap()
        .0;
    node.with_live_target(|target| {
        let name = crate::distributed::nvmeof::live_storage::wal_export_name(
            crate::distributed::nvmeof::live_storage::FILES_PER_SUBSYSTEM,
            extent.segment.database,
            extent.segment.partition,
            extent.segment.epoch,
            extent.segment.file_index,
        );
        let binding = target
            .export_permanent(&name, live_file(&extent.segment), false)
            .unwrap();
        target.revoke(&binding).unwrap();
    });
    drop(log);
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, 2, false, |_| Ok(()))
    })
    .unwrap();
    let address = crate::distributed::types::PhysicalValueAddress {
        partition: config.partition.partition,
        segment_generation: 1,
        offset: 0,
        len: 8,
    };
    let terminal = std::sync::atomic::AtomicBool::new(false);
    let readers = node.inventory.lock().readers.clone();
    let snapshot = readers.read().snapshot(2, &[address], &terminal).unwrap();
    let mut lease = Some(snapshot.members[0].0.resolve_live_member(address).unwrap());
    let mut snapshot = Some(snapshot);
    if std::env::var_os("TH_TRIM_CONTROL_SNAPSHOT").is_some() {
        drop(lease.take());
        drop(snapshot.take());
    }
    assert!(
        node.with_lifecycle(|inventory, slot| NodeInventory::trim_generation(
            inventory, slot, 2, 1
        ))
        .is_err()
    );
    assert!(extent.segment.path.exists());
    assert!(
        node.with_live_target(|target| target.sealed_file(&extent.segment.path).unwrap().is_some())
    );
    assert_eq!(node.inventory.lock().manifest.trim_pending, None);
    drop(snapshot);
    drop(lease);
    node.with_lifecycle(|inventory, slot| NodeInventory::trim_generation(inventory, slot, 2, 1))
        .unwrap();
    assert!(!extent.segment.path.exists());
    assert!(
        node.with_live_target(|target| target.sealed_file(&extent.segment.path).unwrap().is_none())
    );
}

#[test]
fn detached_generation_is_unavailable_to_new_reader_snapshots() {
    let temporary = tempdir::TempDir::new("owner-trim-detach").unwrap();
    let (config, configfs) = trim_live_fixture(temporary.path());
    let node = StorageNode::bench_open_live_fixture(config.clone(), metrics(), configfs).unwrap();
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, 1, false, |_| Ok(()))
    })
    .unwrap();
    let log = node.inventory.lock().logs.get(&1).unwrap().clone();
    let extent = log
        .lease_live_extent(RequestAuthority { epoch: 1 })
        .unwrap()
        .0;
    drop(log);
    node.with_lifecycle(|inventory, slot| {
        NodeInventory::fence(inventory, slot, 2, false, |_| Ok(()))
    })
    .unwrap();
    let address = crate::distributed::types::PhysicalValueAddress {
        partition: config.partition.partition,
        segment_generation: 1,
        offset: 0,
        len: 8,
    };
    let terminal = std::sync::atomic::AtomicBool::new(false);
    let readers = node.inventory.lock().readers.clone();
    let before = readers
        .read()
        .snapshot(2, &[address], &terminal)
        .err()
        .map(|error| error.kind());
    let (entered, resume) = node.inventory.lock().pause_trim_after_detach();
    std::thread::scope(|scope| {
        // Moved into the scope body: if this body panics, `resume` drops while
        // unwinding, which releases the worker before the scope joins it.
        let resume = resume;
        let worker = scope.spawn(|| {
            node.with_lifecycle(|inventory, slot| {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    NodeInventory::trim_generation(inventory, slot, 2, 1)
                }));
                // The inventory still holds the hook sender when trim ends before
                // the hook, by return or by panic. Drop it so the test side sees a
                // disconnect, not a hang. A panic is re-raised after the drop.
                drop(inventory.trim_detach_pause.take());
                result.unwrap_or_else(|payload| std::panic::resume_unwind(payload))
            })
        });
        if entered.recv().is_err() {
            panic!(
                "trim ended without reaching the detach hook: {:?}",
                worker.join()
            );
        }
        let after = readers
            .read()
            .snapshot(2, &[address], &terminal)
            .err()
            .map(|error| error.kind());
        resume.send(()).unwrap();
        worker.join().unwrap().unwrap();
        let observed = if std::env::var_os("TH_TRIM_CONTROL_DETACH").is_some() {
            before
        } else {
            after
        };
        assert_eq!(observed, Some(io::ErrorKind::NotFound));
    });
    assert!(!extent.segment.path.exists());
}

#[test]
fn previous_owner_inventory_format_is_rejected_without_rewrite() {
    let temporary = tempdir::TempDir::new("owner-generation-format").unwrap();
    let inventory = fenced(&temporary.path().join("node"), 1);
    let root = inventory.config.root.clone();
    drop(inventory);
    let path = root.join(MANIFEST);
    let mut old: serde_yaml::Value = read_metadata(&path).unwrap();
    old.as_mapping_mut()
        .unwrap()
        .insert("format".into(), 1u32.into());
    install_metadata(&root, MANIFEST, &encode_metadata(&old).unwrap()).unwrap();
    let before = fs::read(&path).unwrap();
    let error = NodeInventory::open(config(&root), metrics()).err().unwrap();
    assert_eq!(
        error.to_string(),
        "unsupported distributed owner inventory format 1 (expected 2)"
    );
    assert_eq!(fs::read(&path).unwrap(), before);
}
