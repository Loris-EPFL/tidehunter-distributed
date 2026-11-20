use super::*;
use std::collections::BTreeMap;
#[test]
fn descriptor_limits_and_exact_large_value() {
    assert_eq!(decode(&descriptor(134_995)).unwrap(), (134_995, 2));
    for length in [0, CHUNK, MAX_VALUE + 1] {
        assert!(decode(&descriptor(length)).is_err());
    }
    assert!(decode(b"arbitrary BCS").is_err());
    let value = vec![7u8; 134_995];
    assert_eq!(
        value.chunks(CHUNK).map(<[u8]>::len).collect::<Vec<_>>(),
        [100_000, 34_995]
    );
}
#[test]
fn fixture_admission_and_strict_limits() {
    let trace = crate::mainnet_trace::Trace::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap();
    let mut prepared = crate::mainnet_replay::prepare(&trace, 2).unwrap();
    admit(&mut prepared).unwrap();
    let mut row = prepared[0]
        .batches
        .iter()
        .flat_map(|b| &b.entries)
        .find(|r| r.value.len() == 134_995)
        .unwrap()
        .clone();
    let db = tables()[usize::from(row.table)].db_id;
    assert!(check(&[row.clone()], db).is_ok());
    assert_ne!(
        suffix(&row.backend_key, 1, 0),
        suffix(&row.backend_key, 2, 0)
    );
    row.value = Bytes::from(vec![0; MAX_VALUE + 1]);
    assert!(check(&[row.clone()], db).is_err());
    row.value = Bytes::from(vec![0; CHUNK]);
    assert!(check(&vec![row.clone(); 101], db).is_err());
    assert!(check(&[row], db + 1).is_err());
    assert!(key(11, &Bytes::from_static(b"")).is_ok());
    assert!(key(11, &Bytes::from_static(b"x")).is_err());
}
/// Run only against the empty exact-owned FDB lifecycle provided by the qualifier.
#[test]
#[ignore = "requires KV_REPLAY_FDB_CLUSTER_FILE for an empty owned server"]
fn owned_server_overwrite_and_strict_rejection() {
    let cluster = std::env::var("KV_REPLAY_FDB_CLUSTER_FILE").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let backend = runtime
        .block_on(FoundationDbReplayBackend::open(&json!({
            "cluster_file":cluster,"required_replication_factor":1,"transaction_timeout_ms":5000
        })))
        .unwrap();
    let logical = Bytes::from(vec![7; 8]);
    let mut row = Row {
        table: 10,
        tx: 0,
        key: logical.clone(),
        backend_key: key(10, &logical).unwrap(),
        backend_owner: 0,
        value: Bytes::from(vec![9; 134_995]),
    };
    for length in [100_000, 134_995, 300_001, 134_995, 0, 134_995] {
        row.value = Bytes::from(vec![9; length]);
        backend.commit(tables()[10].db_id, &[row.clone()]).unwrap();
        assert_eq!(
            backend.get(10, &row.backend_key).unwrap(),
            Some(row.value.clone())
        );
        let expected = BTreeMap::from([((10, logical.clone()), row.clone())]);
        backend
            .verify_final(
                &expected,
                Instant::now() + std::time::Duration::from_secs(10),
            )
            .unwrap();
    }
    // The raw adapter must reject the real oversized value, never truncate/fallback.
    runtime.block_on(async {
        assert!(
            backend
                .backend
                .put(Bytes::from_static(b"strict"), row.value.clone())
                .await
                .is_err()
        );
        assert_eq!(backend.backend.get(b"strict").await.unwrap(), None);
    });
    row.value = Bytes::from(vec![0; MAX_VALUE + 1]);
    assert!(backend.commit(tables()[10].db_id, &[row.clone()]).is_err());
    assert_eq!(
        backend.get(10, &row.backend_key).unwrap().unwrap().len(),
        134_995
    );
    runtime
        .block_on(backend.backend.database.run(|tr, _| async move {
            tr.clear_range(b"", b"\xff");
            Ok(())
        }))
        .unwrap();
}
