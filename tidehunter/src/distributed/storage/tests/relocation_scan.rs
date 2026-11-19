use super::*;

/// Four adopted groups of three batches each.
fn grouped_log(path: &Path, config: &PartitionConfig) -> (PartitionLog, Vec<AppendedBatch>) {
    let log = PartitionLog::create(path, config.clone(), &shape(), metrics()).unwrap();
    let mut receipts = Vec::new();
    for n in (0..12).step_by(3) {
        let group = CompleteGroup::new(
            &shape(),
            vec![batch(config, n), batch(config, n + 1), batch(config, n + 2)],
            config.batch_limits,
            config.group_limits,
        )
        .unwrap();
        receipts.extend(log.append_group(authority(config), &group).unwrap());
    }
    (log, receipts)
}

fn end_of(receipt: &AppendedBatch) -> u64 {
    receipt.address().offset + u64::from(receipt.address().len)
}

/// A receipt end lies inside its group frame: the page reads each group that
/// starts below the requested end whole and names only the batches that start
/// below it, with a put reference bound to each.
#[test]
fn relocation_page_reads_whole_groups_and_names_batches_below_the_end() {
    let temp = tempdir::TempDir::new("relocation-page-groups").unwrap();
    let config = config(vec![3], 3);
    let (log, receipts) = grouped_log(&temp.path().join("stream"), &config);
    // Inside the second group (after its middle batch), and at the last batch
    // of the first group, whose frame ends after that batch.
    for named in [5, 3] {
        let end = end_of(&receipts[named - 1]);
        let page = log.relocation_page(0, end, 256, usize::MAX).unwrap();
        assert_eq!(page.next, None);
        assert_eq!(page.start, 0);
        assert!(page.available_end >= end);
        assert_eq!(
            page.batches
                .iter()
                .map(|batch| batch.address)
                .collect::<Vec<_>>(),
            receipts[..named]
                .iter()
                .map(|receipt| receipt.address())
                .collect::<Vec<_>>()
        );
        for batch in &page.batches {
            assert_eq!(batch.puts.len(), 1);
            assert_eq!(batch.puts[0].reference.batch_address, batch.address);
            assert_eq!(batch.puts[0].reduced_key, b"key1");
        }
    }
    // One group per page; every cursor stays below the end.
    let end = end_of(&receipts[7]);
    let mut cursor = 0;
    let mut versions = Vec::new();
    loop {
        let page = log.relocation_page(cursor, end, 1, usize::MAX).unwrap();
        versions.extend(page.batches.iter().map(|batch| batch.version.sequence));
        let Some(next) = page.next else { break };
        assert!(next > cursor && next < end);
        cursor = next;
    }
    assert_eq!(versions, (0..8).collect::<Vec<_>>());
    // Beyond the adopted end the owner defers with an empty page.
    let available = log.state.lock().appended_end;
    let behind = log
        .relocation_page(0, available + 8, 256, usize::MAX)
        .unwrap();
    assert!(behind.batches.is_empty());
    assert_eq!(behind.next, Some(0));
    assert_eq!(behind.available_end, available);
}
