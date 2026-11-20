use super::*;

#[test]
fn redaction_removes_sensitive_fields_from_nested_objects_and_arrays() {
    let mut status = serde_json::json!({
        "Password": "hidden",
        "cluster": {
            "storage_replicas": 3,
            "processes": [{"TLS_KEY": "hidden", "version": "7.3"}],
            "authorization": {"nested": "hidden"}
        }
    });
    redact_sensitive_fields(&mut status);
    assert_eq!(
        status,
        serde_json::json!({
            "cluster": {
                "storage_replicas": 3,
                "processes": [{"version": "7.3"}]
            }
        })
    );
}

#[test]
fn client_timeout_codes_are_classified_as_timeouts() {
    for code in [1004, 1031] {
        let error = normalize_fdb_error(foundationdb::FdbBindingError::from(
            foundationdb::FdbError::from_code(code),
        ));
        let io_error = error.downcast_ref::<io::Error>().unwrap();
        assert_eq!(io_error.kind(), io::ErrorKind::TimedOut);
    }
}
#[test]
fn logical_key_embedding_preserves_order_and_spreads_leading_buckets() {
    let logical_keys: [&[u8]; 9] = [
        &[],
        &[0x00],
        &[0x00, 0x00],
        &[0x01, 0xff],
        &[0x80, 0x00],
        &[0xfe],
        &[0xfe, 0xff],
        &[0xff],
        &[0xff, 0x00],
    ];
    let physical_keys: Vec<_> = logical_keys
        .iter()
        .map(|key| FoundationDbBackend::key(key).unwrap())
        .collect();

    for pair in logical_keys.windows(2).zip(physical_keys.windows(2)) {
        let (logical, physical) = pair;
        assert_eq!(logical[0].cmp(logical[1]), physical[0].cmp(&physical[1]));
    }
    for physical in &physical_keys {
        assert!(physical[0] <= 0xfe);
    }
    assert_ne!(physical_keys[3][0], physical_keys[4][0]);
}

#[test]
fn logical_key_embedding_enforces_physical_key_limit() {
    let largest_logical = vec![0xff; FDB_MAX_KEY_BYTES - 1];
    let physical = FoundationDbBackend::key(&largest_logical).unwrap();
    assert_eq!(physical.len(), FDB_MAX_KEY_BYTES);

    let oversized_logical = vec![0xff; FDB_MAX_KEY_BYTES];
    assert!(FoundationDbBackend::key(&oversized_logical).is_err());
}
