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
            },
            InventoryGeneration {
                generation: 2,
                state: OwnerGenerationState::Sealed,
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
        inventory.trim_readiness(3, 1).unwrap().0,
        TrimReadiness::EmptySealed
    );
    assert_eq!(
        inventory.trim_readiness(3, 3).unwrap().0,
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
        inventory.trim_readiness(3, 1).unwrap().0,
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
