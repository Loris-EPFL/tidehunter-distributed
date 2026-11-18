//! Regression shared with the pre-fix tree; only the local-controller connector
//! is a fixture. Record resolution, grant retirement and owner export reap are real.
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::nvmeof::{
    live_connect::{LiveConnectTarget, LiveConnectionRegistry},
    live_pool::LiveControllerPool,
    live_storage::LiveGroupDescriptor,
    live_target::LiveFileIdentity,
};
use std::os::unix::fs::FileExt;

fn close_next(client: &mut Client, lease: &LiveLeased, frame: &[u8]) -> LiveLeased {
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&lease.extent.segment.path)
        .unwrap();
    let offset = lease.extent.start % lease.extent.segment.bytes;
    if !frame.is_empty() {
        file.write_all_at(frame, offset).unwrap();
    }
    let mut skip = [0; 4096];
    skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
    file.write_all_at(&skip, offset + frame.len() as u64)
        .unwrap();
    client
        .adopt_live_extent(
            1,
            lease.extent.token,
            lease.extent.start + frame.len() as u64,
            true,
            true,
        )
        .unwrap()
        .next
        .unwrap()
}

#[test]
fn retiring_read_grant_keeps_idle_controller_export_across_file_rollover() {
    let owner = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut client = owner.writer();
    let first = client.lease_live_extent(1).unwrap();
    let batch = owner.batch(0);
    let group = CompleteGroup::new_live(
        &client.shape,
        vec![batch.clone()],
        owner.config.partition.batch_limits,
        owner.config.partition.group_limits,
    )
    .unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    let next = close_next(&mut client, &first, &frame);
    let receipt = owner
        .node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .receipt_page(0, 1)
        .unwrap()[0];
    let reference = batch.record_reference(receipt.address(), 0).unwrap();
    let binding = &first.binding;
    let target = LiveConnectTarget {
        nqn: binding.nqn.clone(),
        uuid: binding.uuid,
        nsid: binding.nsid,
        bytes: binding.bytes,
        block_size: binding.block_size,
        address: binding.address.clone(),
        service: binding.service,
        transport: binding.transport,
        spdk: binding.spdk.clone(),
        geometry: Default::default(),
    };
    let temp = tempdir::TempDir::new("rollover-controller-regression").unwrap();
    let registry = LiveConnectionRegistry::open(&temp.path().join("registry")).unwrap();
    let pool = Arc::new(LiveControllerPool::new(registry, 1, 1).unwrap());
    pool.register_local_target(&target.nqn, first.extent.segment.path.clone());
    client.live_pool = Some(pool.clone());
    let mut cache = live_read::LiveReadCache::default();
    let values = client
        .live_read_records(
            &mut cache,
            1,
            &[reference],
            Instant::now() + Duration::from_secs(5),
            None,
        )
        .unwrap();
    assert_eq!(values, batch.mutations());
    cache.close(&mut client, 1).unwrap();
    assert!(owner.node.inventory.lock().live_pins.is_empty());
    let next_file = close_next(&mut client, &next, &[]);
    assert_eq!(next_file.extent.segment.file_index, 1);
    owner
        .node
        .with_lifecycle(|inventory, slot| {
            crate::distributed::tcp::NodeInventory::reap_idle_live_segments(
                inventory,
                slot.as_mut().unwrap(),
                1,
            )
        })
        .unwrap();
    let segment = &first.extent.segment;
    let file = LiveFileIdentity {
        path: segment.path.clone(),
        device: segment.device,
        inode: segment.inode,
        bytes: segment.bytes,
        block_size: segment.block_size,
    };
    let exported = || {
        owner
            .node
            .with_live_target(|target| target.binding_for_file(&file).is_some())
    };
    assert!(
        exported(),
        "idle connected controller lost its export at rollover"
    );
    pool.shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert!(!exported());
}

pub(crate) struct HistoryPool {
    pub(crate) owner: leased_tests::Fixture,
    pub(crate) client: Client,
    pub(crate) pool: Arc<LiveControllerPool>,
    pub(crate) location: LiveRecordLocation,
    _temp: tempdir::TempDir,
}

impl HistoryPool {
    pub(crate) fn new() -> Self {
        let owner = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
        owner
            .node
            .set_history_persistence(crate::distributed::storage::HistoryPersistence::WalReplay)
            .unwrap();
        let mut client = owner.writer();
        let first = client.lease_live_extent(1).unwrap();
        let batch = owner.batch(0);
        let group = CompleteGroup::new_live(
            &client.shape,
            vec![batch.clone()],
            owner.config.partition.batch_limits,
            owner.config.partition.group_limits,
        )
        .unwrap();
        let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
        let next = close_next(&mut client, &first, &frame);
        close_next(&mut client, &next, &[]);
        let receipt = owner
            .node
            .inventory
            .lock()
            .active(1)
            .unwrap()
            .receipt_page(0, 1)
            .unwrap()[0];
        let reference = batch.record_reference(receipt.address(), 0).unwrap();
        client.fence_to(2).unwrap();
        let locations = client.resolve_live_records(2, &[reference]).unwrap();
        let location = locations.entries[0].clone();
        let temp = tempdir::TempDir::new("history-controller-retirement").unwrap();
        let pool = Arc::new(
            LiveControllerPool::new(
                LiveConnectionRegistry::open(&temp.path().join("registry")).unwrap(),
                2,
                1,
            )
            .unwrap(),
        );
        pool.register_local_target(&location.binding.nqn, location.segment.path.clone());
        client.live_pool = Some(pool.clone());
        let result = Self {
            owner,
            client,
            pool,
            location,
            _temp: temp,
        };
        drop(result.acquire());
        let mut result = result;
        result
            .client
            .release_live_epoch_pin(2, locations.epoch_pin_token)
            .unwrap();
        result
    }

    pub(crate) fn poison_owner(&self) -> bool {
        self.owner.node.poison_readers();
        self.owner
            .node
            .inventory
            .lock()
            .terminal
            .load(Ordering::Acquire)
    }

    pub(crate) fn pin_count(&self) -> usize {
        self.owner.node.inventory.lock().live_export_pins.len()
    }

    fn acquire(&self) -> Arc<crate::distributed::nvmeof::live_pool::LiveControllerLease> {
        self.acquire_in(&self.pool, &self.location.binding).unwrap()
    }

    fn acquire_in(
        &self,
        pool: &LiveControllerPool,
        proof: &crate::distributed::nvmeof::live_target::LiveTargetBinding,
    ) -> io::Result<Arc<crate::distributed::nvmeof::live_pool::LiveControllerLease>> {
        let binding = &self.location.binding;
        let config = self.client.live_config.as_ref().unwrap();
        let target = LiveConnectTarget {
            nqn: binding.nqn.clone(),
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: binding.address.clone(),
            service: binding.service,
            transport: binding.transport,
            spdk: binding.spdk.clone(),
            geometry: Default::default(),
        };
        pool.acquire_pinned(
            &target,
            &config.host_nqn,
            config.host_id,
            crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess::ReadOnly,
            Instant::now() + Duration::from_secs(5),
            self.client
                .export_pin_request(2, &self.location.segment, proof),
        )
        .map(|(lease, _, _)| lease)
    }
}

#[test]
fn retirement_refuses_pinned_export_then_targeted_eviction_allows_retirement_and_shutdown() {
    let mut fixture = HistoryPool::new();
    assert!(
        fixture
            .owner
            .node
            .handle(
                Request::RetireWalFiles {
                    authority_epoch: 2,
                    epoch: 1,
                    files: vec![0],
                },
                TcpLimits::default()
            )
            .is_err()
    );
    assert!(fixture.location.segment.path.exists());
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        1
    );
    fixture.client.retire_wal_files(2, 1, vec![0]).unwrap();
    assert!(!fixture.location.segment.path.exists());
    assert!(
        fixture
            .owner
            .node
            .inventory
            .lock()
            .live_export_pins
            .is_empty()
    );
    fixture
        .pool
        .shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
}

#[test]
fn busy_retirement_is_typed_and_direct_refusal_keeps_controller_until_released() {
    use super::checkpoint::RetirementResult;
    let mut fixture = HistoryPool::new();
    let lease = fixture.acquire();
    assert!(fixture.client.retire_wal_files(2, 1, vec![0]).is_err());
    assert_eq!(
        fixture
            .client
            .retire_files_if_idle(2, 1, None, vec![0])
            .unwrap(),
        RetirementResult::Busy,
    );
    assert!(fixture.location.segment.path.exists());
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        1
    );
    drop(lease);
    assert_eq!(
        fixture
            .client
            .retire_files_if_idle(2, 1, None, vec![0])
            .unwrap(),
        RetirementResult::Completed,
    );
    assert!(!fixture.location.segment.path.exists());
    fixture
        .pool
        .shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
}

#[test]
fn only_a_token_cleared_by_its_fence_has_idempotent_release() {
    let mut fixture = HistoryPool::new();
    let token = *fixture
        .owner
        .node
        .inventory
        .lock()
        .live_export_pins
        .keys()
        .next()
        .unwrap();
    fixture.client.fence_to(3).unwrap();
    assert!(matches!(
        fixture
            .client
            .request(Request::ReleaseLiveExportPin { epoch: 2, token }),
        Ok(Response::NvmeofEpochReleased),
    ));
    assert!(
        fixture
            .client
            .request(Request::ReleaseLiveExportPin { epoch: 3, token })
            .is_err()
    );
    assert!(
        fixture
            .client
            .request(Request::ReleaseLiveExportPin {
                epoch: 2,
                token: [77; 16],
            })
            .is_err()
    );
    fixture
        .pool
        .shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
}

#[test]
fn confirmed_pin_acquire_rejection_does_not_quarantine_pool_capacity() {
    let fixture = HistoryPool::new();
    let pool = LiveControllerPool::new(
        LiveConnectionRegistry::open(&fixture._temp.path().join("rejected-registry")).unwrap(),
        1,
        1,
    )
    .unwrap();
    pool.register_local_target(
        &fixture.location.binding.nqn,
        fixture.location.segment.path.clone(),
    );
    let mut invalid = fixture.location.binding.clone();
    invalid.uuid[0] ^= 1;
    assert!(fixture.acquire_in(&pool, &invalid).is_err());
    // A definite owner rejection minted no token and left no pending slot.
    drop(
        fixture
            .acquire_in(&pool, &fixture.location.binding)
            .unwrap(),
    );
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        2
    );
    pool.shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        1
    );
    fixture
        .pool
        .shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
}

#[test]
fn lost_pin_acquire_reply_quarantines_capacity_until_fence() {
    let mut fixture = HistoryPool::new();
    let pool = LiveControllerPool::new(
        LiveConnectionRegistry::open(&fixture._temp.path().join("lost-registry")).unwrap(),
        1,
        1,
    )
    .unwrap();
    pool.register_local_target(
        &fixture.location.binding.nqn,
        fixture.location.segment.path.clone(),
    );
    pool.lose_next_pin_reply_for_test(&fixture.client.live_pin_recipe);
    assert!(
        fixture
            .acquire_in(&pool, &fixture.location.binding)
            .is_err()
    );
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        2
    );
    assert!(
        fixture
            .acquire_in(&pool, &fixture.location.binding)
            .is_err()
    );
    assert_eq!(
        pool.shutdown(Instant::now() + Duration::from_secs(5))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    fixture.client.fence_to(3).unwrap();
    assert!(
        fixture
            .owner
            .node
            .inventory
            .lock()
            .live_export_pins
            .is_empty()
    );
    fixture
        .pool
        .shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
}

#[test]
fn idle_connection_pins_do_not_delay_poison_publication_and_drain_after_disconnect() {
    let mut fixture = HistoryPool::new();
    fixture.owner.node.poison_readers();
    assert!(
        fixture
            .owner
            .node
            .inventory
            .lock()
            .terminal
            .load(Ordering::Acquire)
    );
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        1
    );
    fixture.client.record_live_write_error();
    fixture.client.drain_poisoned_live_pool().unwrap();
    assert!(
        fixture
            .owner
            .node
            .inventory
            .lock()
            .live_export_pins
            .is_empty()
    );
    fixture
        .pool
        .shutdown(Instant::now() + Duration::from_secs(5))
        .unwrap();
}
