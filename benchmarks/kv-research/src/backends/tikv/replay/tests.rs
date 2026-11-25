use super::*;
#[test]
fn admission_rejects_wrong_table_cross_db_and_oversize() {
    let key = Bytes::from(vec![1; 40]);
    let mut row = Row {
        table: 13,
        tx: 0,
        backend_key: physical_key(13, &key, false),
        key,
        backend_owner: 0,
        value: Bytes::new(),
    };
    assert!(encoded_pairs(&[row.clone()], 0).is_ok());
    assert!(encoded_pairs(&[row.clone()], 1).is_err());
    row.backend_key = physical_key(4, &row.key, false);
    assert!(encoded_pairs(&[row.clone()], 0).is_err());
    row.backend_key = physical_key(13, &row.key, false);
    row.value = Bytes::from(vec![0; PAYLOAD_LIMIT]);
    assert!(encoded_pairs(&[row], 0).is_err());
}

#[cfg(test)]
#[test]
fn observed_readable_byte_caps_are_admitted_without_inventing_receive_options() {
    assert_eq!(readable_bytes(&json!("8MiB")).unwrap(), 8_388_608);
    assert_eq!(readable_bytes(&json!(10485760)).unwrap(), 10_485_760);
    assert!(readable_bytes(&JsonValue::Null).is_err());
    assert!(readable_bytes(&json!("unbounded")).is_err());
}

#[cfg(test)]
#[test]
fn real_fixture_admits_exact_raw_pairs_and_retains_logical_counts() {
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
            row.backend_key = physical_key(row.table, &row.key, false);
        }
    }
    let receipt = admit(&prepared).unwrap();
    assert!(
        receipt["maximum_encoded_pairs_and_ttls_bytes"]
            .as_u64()
            .unwrap()
            < PAYLOAD_LIMIT as u64
    );
    assert_eq!(prepared.iter().flat_map(|p| &p.batches).count(), 18);
    assert_eq!(
        prepared
            .iter()
            .flat_map(|p| p.batches.iter().flat_map(|b| &b.entries))
            .count(),
        1414
    );
    let singleton = prepared[0]
        .batches
        .iter()
        .flat_map(|b| &b.entries)
        .find(|r| r.table == 11)
        .unwrap();
    assert_eq!(singleton.backend_key.as_ref(), &[0, 11]);
}
