use super::*;
fn fixture() -> Vec<Prepared> {
    let trace = crate::mainnet_trace::Trace::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap();
    let mut prepared = crate::mainnet_replay::prepare(&trace, 2).unwrap();
    for p in &mut prepared {
        for row in p
            .preload
            .iter_mut()
            .chain(p.batches.iter_mut().flat_map(|b| &mut b.entries))
            .chain(p.reads.iter_mut().flatten().map(|r| &mut r.row))
        {
            row.backend_key = physical_key(row.table, &row.key, true);
        }
    }
    prepared
}
#[test]
fn exact_framing_admits_real_large_row_singleton_and_empty_values() {
    let p = fixture();
    let receipt = admit(&p, "garnet").unwrap();
    assert!(receipt["pinned_default_server_bulk_value_limit"].is_null());
    assert!(
        receipt["maximum_encoded_request_bytes_including_transaction"]
            .as_u64()
            .unwrap()
            < MAX_REQUEST as u64
    );
    let mut singleton = false;
    let mut empty = false;
    let mut large = false;
    for row in p
        .iter()
        .flat_map(|p| p.batches.iter().flat_map(|b| &b.entries))
    {
        let mut command = redis::cmd("SET");
        command
            .arg(row.backend_key.as_ref())
            .arg(row.value.as_ref());
        assert_eq!(set_size(row).unwrap(), command.get_packed_command().len());
        if row.table == 11 {
            assert_eq!(&*row.backend_key, b"{}\0\x0b");
            singleton = true;
        }
        if row.table == 8 {
            assert!(row.value.is_empty());
            empty = true;
        }
        if row.value.len() == 134_995 {
            large = true;
        }
    }
    assert!(singleton && empty && large);
    assert_eq!(slot(b"123456789"), 0x31c3);
}
#[test]
fn table_prefix_preserves_binary_keys_and_rejects_collisions() {
    let logical = Bytes::from_static(b"arbitrary{tag}\0\xff");
    for cluster in [false, true] {
        let a = physical_key(1, &logical, cluster);
        let b = physical_key(4, &logical, cluster);
        assert_ne!(a, b);
        assert!(a.ends_with(&logical));
        assert_eq!(a.len(), logical.len() + if cluster { 4 } else { 2 });
    }
    let mut p = fixture();
    p[0].batches[0].entries[0].backend_key = p[0].batches[0].entries[1].backend_key.clone();
    assert!(admit(&p, "garnet").is_err());
}
#[test]
fn oversize_and_cross_database_batches_fail_before_sending() {
    let mut p = fixture();
    p[0].batches[0].entries[0].value = Bytes::from(vec![0; MAX_REQUEST]);
    assert!(
        admit(&p, "garnet")
            .unwrap_err()
            .to_string()
            .contains("encoded request")
    );
    let p = fixture();
    assert!(check_rows(&p[0].batches[0].entries, &tables(), Some(0)).is_err());
    assert!(bulk_size(usize::MAX).is_err());
    assert!(check_value_size(MAX_VALUE + 1).is_err());
    check_value_size(MAX_VALUE).unwrap();
}
#[test]
fn every_queued_and_exec_reply_is_required_without_rollback_claims() {
    use redis::Value::{Array, Nil, Okay, SimpleString};
    let valid = vec![
        Okay,
        SimpleString("QUEUED".into()),
        SimpleString("QUEUED".into()),
        Array(vec![Okay, Okay]),
    ];
    check_replies(&valid, 2, true).unwrap();
    for index in 0..valid.len() {
        let mut bad = valid.clone();
        bad[index] = Nil;
        assert!(check_replies(&bad, 2, true).is_err());
    }
    let mut partial = valid;
    partial[3] = Array(vec![Okay, Nil]);
    assert!(check_replies(&partial, 2, true).is_err());
    assert!(check_replies(&partial[..3], 2, true).is_err());
    assert!(check_replies(&[Okay], 2, false).is_err());
    assert!(check_replies(&[Okay, Nil], 2, false).is_err());
    check_replies(&[Okay, Okay], 2, false).unwrap();
}
#[test]
fn primary_runs_do_not_regroup_a_later_owner() {
    let mut rows = fixture().remove(0).batches.remove(0).entries;
    rows.extend(rows.clone());
    for (row, owner) in rows.iter_mut().zip([0, 0, 1, 0]) {
        row.backend_owner = owner;
    }
    let mut runs = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let length = run_length(&rows[start..]);
        runs.push((rows[start].backend_owner, start, length));
        start += length;
    }
    assert_eq!(runs, [(0, 0, 2), (1, 2, 1), (0, 3, 1)]);
}

#[test]
fn freshness_requires_a_terminal_empty_scan_not_an_empty_intermediate_page() {
    require_empty_scan(0, &[]).unwrap();
    assert!(require_empty_scan(1, &[]).is_err());
    assert!(require_empty_scan(0, &[b"existing-row".to_vec()]).is_err());
    assert!(require_empty_scan(1, &[b"existing-row".to_vec()]).is_err());
}
