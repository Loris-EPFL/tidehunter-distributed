use super::*;
fn row(table: u16, len: usize) -> Row {
    let key = Bytes::from(vec![1; tables()[usize::from(table)].key_bytes]);
    Row {
        table,
        tx: 0,
        backend_key: if table == 11 {
            Bytes::from_static(&[0])
        } else {
            key.clone()
        },
        key,
        backend_owner: 0,
        value: Bytes::from(vec![0; len]),
    }
}
#[test]
fn exact_encoding_singleton_empty_and_repeated_members() {
    assert_eq!(sizes(&[row(11, 0)], 0).unwrap(), (57, 70));
    assert_eq!(sizes(&[row(13, 10), row(13, 20)], 0).unwrap(), (207, 90));
    assert!(sizes(&[row(13, LIMIT)], 0).is_err());
    assert!(sizes(&[row(13, 1)], 1).is_err());
    let mut invalid = row(11, 1);
    invalid.key = Bytes::from(vec![0]);
    assert!(sizes(&[invalid], 0).is_err());
}
#[test]
fn rejects_absent_or_wrong_guardrail_receipt() {
    let options=parse_options(&json!({"nodes":["127.0.0.11:9042"],"keyspace":"kv_comparison","required_replication_factor":1})).unwrap();
    assert!(validate_receipt(&json!({}), &options).is_err());
    let mut receipt = json!({"backend":"cassandra","server_version":"5.0.9","nodes":options.nodes,"batch_size_fail_threshold_bytes":LIMIT,"batch_size_warn_threshold_bytes":5120,"effective_yaml":["batch_size_fail_threshold: 1MiB\n"],"effective_settings":[{"node":"127.0.0.11:9042","settings":{"batch_size_fail_threshold":"1MiB","batch_size_warn_threshold":"5KiB","commitlog_sync":"periodic","commitlog_sync_period":"10000ms"}}]});
    validate_receipt(&receipt, &options).unwrap();
    receipt["batch_size_fail_threshold_bytes"] = json!(51200);
    assert!(validate_receipt(&receipt, &options).is_err());
    receipt["batch_size_fail_threshold_bytes"] = json!(LIMIT);
    receipt["effective_settings"][0]["settings"]["batch_size_fail_threshold"] = json!("50KiB");
    assert!(validate_receipt(&receipt, &options).is_err());
    receipt["effective_settings"] = json!([]);
    assert!(validate_receipt(&receipt, &options).is_err());
}
