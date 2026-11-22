use super::*;
use tidehunter::index::index_format::IndexFormat;
use tidehunter::index::index_table::IndexTable;
use tidehunter::key_shape::KeyShape;

#[test]
fn production_adapter_shapes_do_not_follow_debug_build_defaults() {
    let t = crate::mainnet_trace::tables();
    for id in [4, 7, 8, 13, 14] {
        assert_eq!(table_mutexes(&t[id]), 8192);
    }
    for id in [6, 9, 12, 15] {
        assert_eq!(table_mutexes(&t[id]), 2048);
    }
    assert_eq!(table_mutexes(&t[5]), 4096);
    assert_eq!(table_mutexes(&t[0]), 1024);
    assert_eq!(table_mutexes(&t[11]), 1);
    assert_eq!(table_mutexes(&t[16]), 1);
}

#[test]
fn actual_index_serializer_confirms_geometry_rejection_before_open() {
    let tables = crate::mainnet_trace::tables();
    // All versions of one ObjectID route to the same native cell. This test
    // checks the exact whole-cell frame, not a guessed average occupancy.
    let rows: Vec<_> = (0..20_146_u64)
        .map(|version| {
            let mut key = vec![0; 40];
            key[32..].copy_from_slice(&version.to_be_bytes());
            Row {
                table: 13,
                tx: 0,
                backend_key: Bytes::from(key.clone()),
                backend_owner: 0,
                key: Bytes::from(key),
                value: Bytes::new(),
            }
        })
        .collect();
    let small = serde_json::json!({"path":"not-opened", "native_wal_geometry":"small"});
    let error = NativeReplayBackend::validate_geometry(&small, &tables, rows.iter()).unwrap_err();
    assert!(error.to_string().contains("1048632 B"));
    assert!(error.to_string().contains("objects cell Uniform(0)"));
    let large = serde_json::json!({"path":"not-opened", "native_wal_geometry":"native_default"});
    let admission =
        NativeReplayBackend::validate_geometry(&large, &tables, rows.iter().chain(rows.iter()))
            .unwrap();
    assert_eq!(admission["maximum_cell_index_keys"], 20_146);
    let shape = KeyShape::new_single(40, 8192, KeyType::uniform(1));
    let ks = shape.iter_ks().next().unwrap();
    let mut index = IndexTable::default();
    let mut position = [0; 12];
    position[7] = 1;
    position[11] = 1;
    for row in &rows {
        index.insert(
            row.key.clone(),
            tidehunter::WalPosition::from_slice(&position),
        );
    }
    let serialized = ks.index_format().serialize_index(&index, ks);
    let actual_frame = ((serialized.len() as u64 + 10).div_ceil(8)) * 8;
    assert_eq!(admission["maximum_index_frame_bytes"], actual_frame);
}

#[test]
fn geometry_admission_rejects_a_key_shorter_than_its_prefix_bits() {
    let tables = crate::mainnet_trace::tables();
    // Table 0 routes on a 48-bit sequence prefix, so a key needs six bytes.
    let row = |len: usize| Row {
        table: 0,
        tx: 0,
        backend_key: Bytes::from(vec![1; len]),
        backend_owner: 0,
        key: Bytes::from(vec![1; len]),
        value: Bytes::new(),
    };
    let options = serde_json::json!({"path":"not-opened", "native_wal_geometry":"native_default"});
    let short = row(5);
    let error = NativeReplayBackend::validate_geometry(&options, &tables, [&short].into_iter())
        .unwrap_err();
    assert!(
        error.to_string().contains("shorter than its 6-byte prefix"),
        "short key must name the prefix bound, got: {error}"
    );
    let exact = row(6);
    NativeReplayBackend::validate_geometry(&options, &tables, [&exact].into_iter())
        .expect("a key with the full prefix is admitted");
}
