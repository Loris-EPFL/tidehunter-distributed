//! Regression shared with the pre-fix tree; only the local-controller connector
//! is a fixture. Record resolution, grant retirement and owner export reap are real.
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::CompleteGroup;
use crate::distributed::nvmeof::{
    live_connect::{LiveConnectTarget, LiveConnectionRegistry},
    live_pool::LiveControllerPool,
    live_storage::LiveGroupDescriptor,
};
use std::os::unix::fs::FileExt;

fn close_next(client: &mut Client, lease: &LiveLeased, frame: &[u8]) -> LiveLeased {
    close_extent(client, lease, frame, true).unwrap()
}

/// Write `frame` and a skip marker into the epoch-1 extent `lease`, close it,
/// and return the next extent when `lease_next` asks the owner for one.
fn close_extent(
    client: &mut Client,
    lease: &LiveLeased,
    frame: &[u8],
    lease_next: bool,
) -> Option<LiveLeased> {
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
    let next = client
        .adopt_live_extent(
            1,
            lease.extent.token,
            lease.extent.start + frame.len() as u64,
            true,
            lease_next,
        )
        .unwrap()
        .next;
    assert_eq!(next.is_some(), lease_next);
    next
}

/// Stops and joins a second owner listener, also when a test fails.
struct ExtraListener {
    stop: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<()>>,
}

impl Drop for ExtraListener {
    fn drop(&mut self) {
        // Release wakes the server's shutdown checks; no state travels with it.
        self.stop.store(true, Ordering::Release);
        if let Some(server) = self.server.take() {
            let joined = server.join();
            assert!(joined.is_ok() || thread::panicking());
        }
    }
}

/// (namespaces exposed so far, namespaces owned now) of the owner's target.
fn namespaces(owner: &leased_tests::Fixture) -> (u64, u64) {
    let stats = owner
        .node
        .with_live_target(|target| target.stats().unwrap());
    (stats.namespaces_exposed, stats.owned_namespace_count)
}

/// Client and owner data deadline of the request held past it.
const HELD_DATA_TIMEOUT: Duration = Duration::from_secs(1);

/// Runs `request` on a second listener and client whose data deadline is
/// `HELD_DATA_TIMEOUT`, so fixture setup (10 s deadline) cannot trip it. The
/// first target-registry save of the export the request creates waits behind a
/// barrier until both data deadlines have passed, then proceeds. Returns the
/// response after checking that the request created exactly one export and
/// that the same session serves the next request.
fn hold_export_past_data_deadline(
    owner: &leased_tests::Fixture,
    shape: &crate::key_shape::KeyShape,
    request: Request,
) -> Response {
    assert_eq!(request.timeout(HELD_DATA_TIMEOUT), LIFECYCLE_TIMEOUT);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = listener.local_addr().unwrap().to_string();
    let stop = Arc::new(AtomicBool::new(false));
    let _extra = ExtraListener {
        stop: stop.clone(),
        server: Some({
            let node = owner.node.clone();
            let tls = owner.tls.clone();
            let limits = TcpLimits {
                io_timeout: HELD_DATA_TIMEOUT,
                ..TcpLimits::default()
            };
            thread::spawn(move || node.serve(listener, tls, limits, stop).unwrap())
        }),
    };
    let mut client = Client::connect(
        &endpoint,
        &owner.tls,
        HELD_DATA_TIMEOUT,
        owner.config.partition.clone(),
        shape,
    )
    .unwrap();
    let exposed = || namespaces(owner).0;
    let exposed_before = exposed();
    let (entered_tx, entered) = std::sync::mpsc::sync_channel(1);
    let (release, released) = std::sync::mpsc::sync_channel::<()>(1);
    let mut armed = true;
    owner.node.with_live_target(|target| {
        target.set_save_hook(Box::new(move |step| {
            if armed && step == crate::distributed::nvmeof::live_target::SaveStep::Prepared {
                armed = false;
                entered_tx.send(()).map_err(io::Error::other)?;
                // `release` drops on test unwind, so this cannot hang.
                released.recv().map_err(io::Error::other)?;
            }
            Ok(())
        }));
    });
    assert!(
        client
            .submit_control(&request, Instant::now() + request.timeout(client.timeout))
            .unwrap()
    );
    let bound = Instant::now() + LIFECYCLE_TIMEOUT;
    while entered.try_recv().is_err() {
        client.progress_session().unwrap();
        assert!(
            Instant::now() < bound,
            "export did not reach its first save"
        );
        std::thread::yield_now();
    }
    // The owner accepted the request before its worker entered the save, and
    // the client submitted it earlier still: both data deadlines have passed
    // when this hold ends. The owner checks the deadline again before it sends
    // the response, so the data class always closed the session here.
    let held = Instant::now();
    while held.elapsed() <= HELD_DATA_TIMEOUT {
        client.progress_session().unwrap();
        assert!(client.take_control().is_none());
        std::thread::yield_now();
    }
    release.send(()).unwrap();
    let response = loop {
        client.progress_session().unwrap();
        if let Some(response) = client.take_control() {
            break response.unwrap();
        }
        assert!(Instant::now() < bound, "export creation did not finish");
        std::thread::yield_now();
    };
    assert_eq!(exposed(), exposed_before + 1, "request created no export");
    // The same session serves the next request; Stats does no disk I/O.
    client.stats().unwrap();
    response
}

/// Every request that can create a live export inline uses the lifecycle
/// deadline class; an adoption without `lease_next` stays a data request.
fn assert_export_deadline_classes() {
    let token = [1; 16];
    let adopt = |lease_next| Request::AdoptLiveExtent {
        epoch: 1,
        token,
        through: 0,
        close: true,
        lease_next,
    };
    for (label, request, lifecycle) in [
        (
            "resolve",
            Request::ResolveLiveRecords {
                authority_epoch: 1,
                references: Vec::new(),
            },
            true,
        ),
        (
            "prelease",
            Request::PreleaseLiveExtent {
                epoch: 1,
                after: token,
            },
            true,
        ),
        ("adopt and lease next", adopt(true), true),
        ("lease", Request::LeaseLiveExtent { epoch: 1 }, true),
        ("adopt", adopt(false), false),
    ] {
        let expected = if lifecycle {
            LIFECYCLE_TIMEOUT
        } else {
            HELD_DATA_TIMEOUT
        };
        assert_eq!(request.timeout(HELD_DATA_TIMEOUT), expected, "{label}");
    }
}

/// A live record resolution that creates an export is a lifecycle operation.
/// Its registry syncs, held past the client and owner data deadline, must not
/// expire the request or close the session.
#[test]
fn export_creation_held_past_the_data_deadline_keeps_the_session() {
    let owner = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut writer = owner.writer();
    let first = writer.lease_live_extent(1).unwrap();
    let batch = owner.batch(0);
    let group = CompleteGroup::new_live(
        &writer.shape,
        vec![batch.clone()],
        owner.config.partition.batch_limits,
        owner.config.partition.group_limits,
    )
    .unwrap();
    let (_, frame) = LiveGroupDescriptor::from_group(&group).unwrap();
    assert_export_deadline_classes();
    let next = close_next(&mut writer, &first, &frame);
    close_next(&mut writer, &next, &[]);
    let receipt = owner
        .node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .receipt_page(0, 1)
        .unwrap()[0];
    let reference = batch.record_reference(receipt.address(), 0).unwrap();
    // The fence revokes every export, so the resolution must create one.
    writer.fence_to(2).unwrap();
    let before = owner.node.live_inline_export_stats();
    let response = hold_export_past_data_deadline(
        &owner,
        &writer.shape,
        Request::ResolveLiveRecords {
            authority_epoch: 2,
            references: vec![reference],
        },
    );
    let locations = match response {
        Response::LiveRecords(locations) => locations,
        other => panic!("unexpected resolution response: {}", other.as_ref()),
    };
    assert_eq!(locations.entries.len(), 1);
    assert_eq!(locations.entries[0].reference, reference);
    assert_eq!(
        owner.node.live_inline_export_stats(),
        LiveInlineExportStats {
            resolve: before.resolve + 1,
            ..before
        }
    );
}

/// The first lease after a fence creates its file's export inline (the fence
/// revoked every export). Held past the data deadline, it still completes on
/// the same session.
#[test]
fn first_lease_after_a_fence_held_past_the_data_deadline_keeps_the_session() {
    let owner = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut writer = owner.writer();
    let first = writer.lease_live_extent(1).unwrap();
    close_next(&mut writer, &first, &[]);
    writer.fence_to(2).unwrap();
    assert_eq!(namespaces(&owner).1, 0, "the fence left an export behind");
    let before = owner.node.live_inline_export_stats();
    let response = hold_export_past_data_deadline(
        &owner,
        &writer.shape,
        Request::LeaseLiveExtent { epoch: 2 },
    );
    let leased = match response {
        Response::LiveLeased(leased) => leased,
        other => panic!("unexpected lease response: {}", other.as_ref()),
    };
    assert_eq!(leased.extent.segment.epoch, 2);
    assert!(!leased.binding.readonly);
    assert_eq!(
        owner.node.live_inline_export_stats(),
        LiveInlineExportStats {
            lease: before.lease + 1,
            ..before
        }
    );
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
        self.owner.node.control_context.poison_readers();
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
        proof: &crate::distributed::nvmeof::live_storage::LiveTargetBinding,
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

/// Ported from the per-file active-epoch fixture (on-demand/per-file export
/// modes were removed; active-epoch exports are permanent and never pinned):
/// two fenced history files and a capacity-one controller pool. Pinning the
/// second file evicts only the disconnected idle entry of the first (its pin
/// is released, the second's is held), and dropping the pool releases the
/// last pin.
#[test]
fn capacity_eviction_releases_only_disconnected_entry_and_drop_releases_last() {
    let owner = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut client = owner.writer();
    let frame_of = |sequence| {
        let group = CompleteGroup::new_live(
            &client.shape,
            vec![owner.batch(sequence)],
            owner.config.partition.batch_limits,
            owner.config.partition.group_limits,
        )
        .unwrap();
        LiveGroupDescriptor::from_group(&group).unwrap().1
    };
    let (first_frame, second_frame) = (frame_of(0), frame_of(1));
    let first = client.lease_live_extent(1).unwrap();
    let next = close_next(&mut client, &first, &first_frame);
    let second = close_next(&mut client, &next, &[]);
    assert_eq!(
        second.extent.segment.file_index,
        first.extent.segment.file_index + 1
    );
    close_extent(&mut client, &second, &second_frame, false);
    let receipts = owner
        .node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .receipt_page(0, 2)
        .unwrap();
    let references: Vec<_> = receipts
        .iter()
        .zip([owner.batch(0), owner.batch(1)])
        .map(|(receipt, batch)| batch.record_reference(receipt.address(), 0).unwrap())
        .collect();
    client.fence_to(2).unwrap();
    let locations = client.resolve_live_records(2, &references).unwrap();
    assert_eq!(locations.entries.len(), 2);
    let (old, new) = (&locations.entries[0], &locations.entries[1]);
    assert_ne!(old.segment.file_index, new.segment.file_index);
    let temp = tempdir::TempDir::new("history-capacity-eviction").unwrap();
    let pool = Arc::new(
        LiveControllerPool::new(
            LiveConnectionRegistry::open(&temp.path().join("registry")).unwrap(),
            1,
            1,
        )
        .unwrap(),
    );
    for location in [old, new] {
        pool.register_local_target(&location.binding.nqn, location.segment.path.clone());
    }
    client.live_pool = Some(pool.clone());
    let config = client.live_config.clone().unwrap();
    let acquire = |location: &LiveRecordLocation| {
        let binding = &location.binding;
        let target = LiveConnectTarget {
            nqn: binding.nqn.clone(),
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: binding.address.clone(),
            service: binding.service,
            transport: binding.transport,
            geometry: Default::default(),
        };
        pool.acquire_pinned(
            &target,
            &config.host_nqn,
            config.host_id,
            crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess::ReadOnly,
            Instant::now() + Duration::from_secs(5),
            client.export_pin_request(2, &location.segment, binding),
        )
        .unwrap()
    };
    let pinned_files = || {
        let inventory = owner.node.inventory.lock();
        let mut files: Vec<_> = inventory
            .live_export_pins
            .values()
            .map(|(_, (_, file))| *file)
            .collect();
        files.sort_unstable();
        files
    };
    // The first file's controller stays connected and idle in the pool.
    drop(acquire(old).0);
    assert_eq!(pinned_files(), vec![old.segment.file_index]);
    let (lease, _, timings) = acquire(new);
    assert!(timings.disconnect.is_some());
    assert_eq!(pinned_files(), vec![new.segment.file_index]);
    drop(lease);
    client
        .release_live_epoch_pin(2, locations.epoch_pin_token)
        .unwrap();
    client.live_pool.take();
    drop(pool);
    assert!(pinned_files().is_empty());
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
        fixture.client.retire_files_if_idle(2, 1, vec![0]).unwrap(),
        RetirementResult::Busy,
    );
    assert!(fixture.location.segment.path.exists());
    assert_eq!(
        fixture.owner.node.inventory.lock().live_export_pins.len(),
        1
    );
    drop(lease);
    assert_eq!(
        fixture.client.retire_files_if_idle(2, 1, vec![0]).unwrap(),
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
    fixture.owner.node.control_context.poison_readers();
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
