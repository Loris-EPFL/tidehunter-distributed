use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::write_timeline::WriteTimeline;

#[test]
fn lease_ahead_switch_into_a_new_wal_file_rebinds_the_writer_queue() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let fixture = leased_tests::Fixture::new(tier, 16384);
        let mut client = fixture.writer();
        client.leased_test_set_window(4);
        let mut released = Vec::with_capacity(9);
        let mut first_binding = None;
        let timeline = WriteTimeline::default();
        for sequence in 0..9 {
            let mut batches = vec![fixture.batch(sequence)];
            loop {
                match client
                    .submit_leased_group(1, batches, &timeline, 2, true)
                    .unwrap()
                {
                    LeasedSubmit::Submitted(_) => break,
                    LeasedSubmit::Deferred(deferred) => {
                        batches = deferred;
                        client.drain_leased_flights(&mut released, None).unwrap();
                        client.wait_closing_extent(true).unwrap();
                    }
                }
            }
            if sequence == 0 {
                first_binding = Some(client.live_writer.as_ref().unwrap().target.uuid);
            }
        }
        client.drain_leased_flights(&mut released, None).unwrap();
        let receipts: Vec<_> = released
            .into_iter()
            .map(|(_, receipts)| receipts[0])
            .collect();
        assert_eq!(fixture.metrics.snapshot().counts.max_leased_in_flight, 4);
        let current = client.live_leased.current.as_ref().unwrap();
        assert_eq!(current.lease.extent.segment.file_index, 1);
        assert_eq!(current.lease.extent.start, 32768);
        let writer = client.live_writer.as_ref().unwrap();
        assert!(writer.matches(&current.lease.binding));
        assert_ne!(Some(writer.target.uuid), first_binding);
        let closing = client.live_leased.closing.as_ref().unwrap();
        assert_eq!(closing.lease.extent.segment.file_index, 0);
        // Old-file frames 0..8 precede the new-file frame 8 in the WAL.
        let appended: Vec<_> = receipts.iter().map(|receipt| receipt.appended()).collect();
        assert!(
            appended[..8]
                .iter()
                .all(|batch| batch.address().offset < 32768)
        );
        assert!(appended[8].address().offset >= 32768);
        // Drive the background close, not a synchronous settle. Its reply
        // must adopt every old-file frame while the new queue stays bound.
        client.wait_closing_extent(false).unwrap();
        assert!(client.live_leased.closing.is_none());
        assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 0);
        assert_eq!(fixture.metrics.snapshot().counts.extent_switch_waits, 0);
        assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
        // The close reply covered the whole old-file prefix, and the close made
        // the new-file extent the owner's open extent. The owner's opportunistic
        // self-adoption (`self_adopt_live_extent`, run by the background syncer's
        // `persist_appended` and by flush barriers) scans only that open extent,
        // so a syncer run that is late may already have adopted the complete
        // new-file frame here. One consistent page snapshot (taken under the
        // receipt authority's lock) must be the client's receipts in WAL order:
        // every old-file frame exactly once, then at most the new-file frame.
        let owner = fixture.node.log_for_test(1);
        let adopted = owner.receipt_page(0, 16).unwrap();
        assert!(
            matches!(adopted.len(), 8 | 9),
            "{tier:?}: owner adopted {} frames",
            adopted.len()
        );
        assert_eq!(adopted[..], appended[..adopted.len()], "{tier:?}");
        for (sequence, receipt) in receipts.into_iter().enumerate() {
            let batch = fixture.batch(sequence as u64);
            let reference = batch
                .record_reference(receipt.appended().address(), 0)
                .unwrap();
            assert_eq!(
                leased_tests::owner_read(&fixture, &reference),
                batch.mutations()
            );
        }
        client.close_leased_extent(&mut Vec::new(), None).unwrap();
        // Closing the new-file extent adopts its frame if the syncer or the
        // reads above did not, and never adopts it a second time.
        let owner = fixture.node.log_for_test(1);
        assert_eq!(owner.receipt_count(), 9, "{tier:?}");
        assert_eq!(owner.receipt_page(0, 16).unwrap(), appended, "{tier:?}");
    }
}

/// per_owner: the queue rebinds to WAL member 2 on the controller that was
/// connected for member 1. The writer route must name member 2, which the
/// kernel qualification's member-transition proof requires.
#[test]
fn per_owner_rebind_route_names_the_opened_member() {
    use crate::distributed::nvmeof::live_connect::{LiveConnectTarget, LiveConnectionRegistry};
    use crate::distributed::nvmeof::live_namespace::LiveNamespaceAccess;
    use crate::distributed::nvmeof::live_pool::LiveControllerPool;
    let fixture = leased_tests::Fixture::single_partition(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    client.leased_test_set_window(4);
    let pool_dir = tempdir::TempDir::new("per-owner-route-pool").unwrap();
    let pool = std::sync::Arc::new(
        LiveControllerPool::new(
            LiveConnectionRegistry::open(
                &pool_dir.path().join("registry"),
                crate::distributed::nvmeof::live_connect::RegistryMode::Retire,
            )
            .unwrap(),
            8,
            4,
        )
        .unwrap(),
    );
    client.set_live_pool(pool.clone());
    // The ordinary-file queue caches both members under one shared lease
    // and records no route; drive it into member 2.
    let timeline = WriteTimeline::default();
    let mut released = Vec::new();
    for sequence in 0..9 {
        let mut batches = vec![fixture.batch(sequence)];
        loop {
            match client
                .submit_leased_group(1, batches, &timeline, 2, true)
                .unwrap()
            {
                LeasedSubmit::Submitted(_) => break,
                LeasedSubmit::Deferred(deferred) => {
                    batches = deferred;
                    client.drain_leased_flights(&mut released, None).unwrap();
                    client.wait_closing_extent(true).unwrap();
                }
            }
        }
    }
    client.drain_leased_flights(&mut released, None).unwrap();
    assert!(fixture.metrics.snapshot().routes.is_empty());
    let lease = &client.live_leased.current.as_ref().unwrap().lease;
    assert_eq!(lease.extent.segment.file_index, 1);
    let (binding, path) = (lease.binding.clone(), lease.extent.segment.path.clone());
    let config = client.live_config.clone().unwrap();
    pool.cache_local_member_for_test(
        &LiveConnectTarget {
            nqn: binding.nqn.clone(),
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: binding.address.clone(),
            service: binding.service,
            transport: binding.transport,
            geometry: config.queue_geometry,
        },
        LiveNamespaceAccess::ReadWrite,
        &path,
    )
    .unwrap();
    // Rebind through the controller-pool path the kernel route uses.
    client.drain_live_queue().unwrap();
    client.live_write_local_fixture = false;
    client.ensure_leased_queue().unwrap();
    let routes = fixture.metrics.snapshot().routes;
    assert_eq!(routes.len(), 1);
    let writer = routes[0].writer.as_ref().unwrap();
    let identity = &routes[0].namespace_identity;
    assert_eq!(writer.file_index, 1);
    assert_eq!(binding.nsid, 2);
    assert_eq!(
        (identity.nqn.as_str(), identity.nsid),
        (binding.nqn.as_str(), 2)
    );
    assert_eq!(identity.uuid, binding.uuid);
    assert_eq!(identity.namespace_path, path);
    client.drain_live_queue().unwrap();
}

#[test]
fn leased_full_extents_roll_into_a_new_file_without_skip_or_stale_binding() {
    let fixture = leased_tests::Fixture::new(DurabilityTier::Synchronous, 16384);
    let mut client = fixture.writer_without_ahead();
    let mut receipts = Vec::with_capacity(9);
    for sequence in 0..9 {
        let batch = fixture.batch(sequence);
        receipts.push(
            client
                .submit_and_drain_leased_group(1, vec![batch], &WriteTimeline::default(), 2)
                .unwrap()[0],
        );
    }
    let current = client.live_leased.current.as_ref().unwrap();
    assert_eq!(current.lease.extent.segment.file_index, 1);
    assert_eq!(current.lease.extent.start, 32768);
    assert!(
        client
            .live_writer
            .as_ref()
            .unwrap()
            .matches(&current.lease.binding)
    );
    assert_eq!(fixture.metrics.snapshot().counts.leased_skip_markers, 0);
    assert_eq!(fixture.metrics.snapshot().counts.adoption_sync_exchanges, 2);
    for (sequence, receipt) in receipts.into_iter().enumerate() {
        let batch = fixture.batch(sequence as u64);
        let reference = batch
            .record_reference(receipt.appended().address(), 0)
            .unwrap();
        assert_eq!(
            leased_tests::owner_read(&fixture, &reference),
            batch.mutations()
        );
    }
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}

#[test]
fn leased_receipts_use_new_epoch_generation_after_clean_close_and_fence() {
    let fixture = leased_tests::Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer();
    let first = fixture.batch(0);
    client
        .submit_and_drain_leased_group(1, vec![first], &WriteTimeline::default(), 2)
        .unwrap();
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    client.fence_to(2).unwrap();
    let batch = super::super::tests::batch(&fixture.config, 2, 0);
    let receipts = client
        .submit_and_drain_leased_group(2, vec![batch.clone()], &WriteTimeline::default(), 2)
        .unwrap();
    assert_eq!(receipts[0].appended().address().segment_generation, 2);
    client.settle_leased_extent(&mut Vec::new(), None).unwrap();
    assert_eq!(
        client.live_leased.current.as_mut().unwrap().adopted.count,
        1
    );
    let reference = batch
        .record_reference(receipts[0].appended().address(), 0)
        .unwrap();
    assert_eq!(
        fixture
            .node
            .log_for_test(2)
            .read_record(&reference)
            .unwrap(),
        batch.mutations()[0]
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
}
