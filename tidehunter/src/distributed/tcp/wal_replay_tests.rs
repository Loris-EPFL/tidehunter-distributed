use super::*;

fn position_proof(
    node: &StorageNode,
    authority: u64,
    epoch: u64,
    through: u64,
) -> checkpoint::CheckpointPositionProof {
    match node
        .handle(
            Request::CheckpointPosition {
                authority_epoch: authority,
                epoch,
                through,
            },
            TcpLimits::default(),
        )
        .unwrap()
    {
        Response::CheckpointPosition {
            database,
            partition,
            epoch,
            durable_end,
        } => checkpoint::CheckpointPositionProof {
            database,
            partition,
            epoch,
            durable_end,
        },
        _ => panic!("expected position proof"),
    }
}

#[test]
fn checkpoint_position_syncs_and_advances_owner_root() {
    let temp = tempdir::TempDir::new("tcp-wal-position").unwrap();
    let config = config(&temp.path().join("node"));
    let node = StorageNode::open(config.clone(), metrics()).unwrap();
    node.inventory.lock().fence_to(1).unwrap();
    let receipt = node
        .inventory
        .lock()
        .active(1)
        .unwrap()
        .append_complete_batch(RequestAuthority { epoch: 1 }, &batch(&config, 1, 0))
        .unwrap();
    let through = receipt.address().offset + u64::from(receipt.address().len);
    let proof = position_proof(&node, 1, 1, through);
    assert_eq!(proof.durable_end, through);
    assert_eq!(position_proof(&node, 1, 1, 0).durable_end, through);
    assert!(
        config
            .root
            .join("epoch_0000000000000001/partition-wal-root-v1")
            .is_file()
    );
    drop(node);
    let node = StorageNode::open(config, metrics()).unwrap();
    node.inventory.lock().fence_to(2).unwrap();
    assert_eq!(position_proof(&node, 2, 1, 0), proof);
}

#[test]
fn replay_live_wal_page_reads_frames_through_namespace_fixture() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::tcp::live_client::leased_tests::Fixture;
    use crate::distributed::write_timeline::WriteTimeline;
    let fixture = Fixture::new(DurabilityTier::Native, 16384);
    let mut client = fixture.writer_without_ahead();
    let batches: Vec<_> = (0..3).map(|n| fixture.batch(n)).collect();
    let receipts = client
        .submit_and_drain_leased_group(1, batches.clone(), &WriteTimeline::default(), 1)
        .unwrap();
    let last = receipts.last().unwrap().appended().address();
    let through = last.offset + u64::from(last.len);
    // The position sync must not settle or close the unadopted extent.
    let before = fixture.metrics.snapshot().counts.adoption_sync_exchanges;
    assert!(
        client
            .checkpoint_position(1, 1, through)
            .unwrap()
            .durable_end
            >= through
    );
    assert_eq!(
        fixture.metrics.snapshot().counts.adoption_sync_exchanges,
        before
    );
    client.close_leased_extent(&mut Vec::new(), None).unwrap();
    client.fence_to(2).unwrap();
    let pool_dir = tempdir::TempDir::new("replay-pool").unwrap();
    let registry = crate::distributed::nvmeof::live_connect::LiveConnectionRegistry::open(
        &pool_dir.path().join("registry"),
    )
    .unwrap();
    client.set_live_pool(Arc::new(
        crate::distributed::nvmeof::live_pool::LiveControllerPool::new(registry, 8, 4).unwrap(),
    ));
    let page = client.replay_live_wal_page(2, 1, 0, 1, through).unwrap();
    assert_eq!(page.batches.len(), 3);
    for ((actual, receipt), (expected, expected_receipt)) in
        page.batches.iter().zip(batches.iter().zip(&receipts))
    {
        assert_eq!(actual.encoded(), expected.encoded());
        assert_eq!(receipt, expected_receipt);
    }
}
