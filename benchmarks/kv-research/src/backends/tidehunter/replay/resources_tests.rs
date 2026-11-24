use super::*;

/// Hand-computed example of the per-table formula: 8,000 keys of 32 B in 4
/// cells, 10,000 puts.
///   row = 32 + 48 + 1 = 81; live = 8,000 * 81 + 4 * 1,024 = 652,096;
///   average blob = ceil(652,096 / 4) = 163,024;
///   flushes = ceil(10,000 / 1,024) + 4 = 14; superseded = 14 * 163,024 =
///   2,282,336; shards per cell = 2 * ceil(163,024 / 65,536) = 6;
///   shard entry = 32 + 32 = 64; region entry = 256 + 6 * 64 = 640;
///   control region = 4 * 640 * 3 = 7,680.
#[test]
fn table_bound_matches_the_hand_computed_formula() {
    assert_eq!(HYBRID_POSITION_ENCODED_LEN, 48);
    let bound = table_bound(&TableLoad {
        puts: 10_000,
        keys: 8_000,
        cells: 4,
        reduced_key_bytes: 32,
    })
    .unwrap();
    assert_eq!(bound.live, 652_096);
    assert_eq!(bound.superseded, 2_282_336);
    assert_eq!(bound.region, 7_680);
    assert_eq!(bound.total().unwrap(), 2_942_112);
    let empty = table_bound(&TableLoad {
        puts: 0,
        keys: 0,
        cells: 0,
        reduced_key_bytes: 32,
    })
    .unwrap();
    assert_eq!(empty.total().unwrap(), 0);
}

#[test]
fn cloud_perpetual_reservation_counts_coordinator_metadata_not_external_wal() {
    let guard = tempfile::TempDir::with_prefix("replay-resource-bound-").expect("test tree");
    let root = guard.path().to_path_buf();
    let input = serde_json::json!({
        "guest_root":root.join("guest"),"addresses":["127.0.0.1:19001","127.0.0.1:19002"],
        "transport":{"kind":"tcp-trusted"},
        "owner_wal_geometry":"native_default","replay_node_db_id":0,
        "batch_limits":{"max_operations":519,"max_encoded_bytes":500000},
        "max_batches_per_partition":32768,"max_admitted_batches":1000000,
        "coordinator_cache_bytes":0,"owner_wal_member_read":"async","polling":"adaptive",
        "live_nvmeof":crate::backends::tidehunter::test_live_nvmeof(2)
    });
    let input_path = root.join("input.json");
    std::fs::write(&input_path, serde_json::to_vec(&input).unwrap()).unwrap();
    let prepared = root.join("prepared");
    crate::backends::tidehunter::prepare_external_case_file(&input_path, &prepared).unwrap();
    let remote: serde_json::Value =
        serde_json::from_slice(&std::fs::read(prepared.join("remote.json")).unwrap()).unwrap();
    let value = serde_json::json!({"databases":[{"db_id":0,"options":{
        "transport":{"kind":"tcp-trusted"},"path":root.join("db"),"owners":2,
        "external_remote_config":remote,
        "batch_limits":{"max_operations":519,"max_encoded_bytes":500000}
    }}]});
    let mut trace = Trace::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../experiments/sui-mainnet/fixture/manifest.json"),
        "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
    )
    .unwrap();
    trace.select_node_databases(&[0]).unwrap();
    let bound = reservation(&value, &trace, 4096).unwrap();
    let database = &bound["databases"][0];
    // The batch count follows the trace alone, not the index model.
    assert_eq!(database["batches"], 20480);
    assert_eq!(
        database["reservation_bytes"].as_u64().unwrap(),
        database["index_live_bytes"].as_u64().unwrap()
            + database["index_superseded_bytes"].as_u64().unwrap()
            + database["control_region_bytes"].as_u64().unwrap()
            + database["index_wal_fragment_bytes"].as_u64().unwrap()
    );
    assert!(
        database["index_wal_fragment_bytes"].as_u64().unwrap() > 0,
        "the coordinator's open allocates its index WAL writer fragment: {database}"
    );
    assert_eq!(bound["reservation_bytes"], database["reservation_bytes"]);
    // z4m-highmem-16: one 14,000 GiB local SSD; keep the owner's 50% guard.
    assert!(bound["reservation_bytes"].as_u64().unwrap() < 14000 * 1024_u64.pow(3) / 2);
    eprintln!("cloud-replay-coordinator-bound={bound}");
}
