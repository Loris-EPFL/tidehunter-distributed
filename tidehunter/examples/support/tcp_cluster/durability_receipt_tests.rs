use super::{
    DurabilityConfig, OwnerReceipt, ReceiptDurability, durability_file, publish_synced,
    read_owner_receipt,
};
use tidehunter::distributed::durability::DurabilityTier;

fn scratch(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!(
        "tcp-cluster-{name}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn receipt(partition: u32, tier: DurabilityTier) -> OwnerReceipt {
    OwnerReceipt {
        partition,
        address: "127.0.0.1:9400".to_owned(),
        launch_nonce: "nonce".to_owned(),
        pid: 1,
        boot_id: "boot".to_owned(),
        started_unix_ns: 5,
        durability: ReceiptDurability::of(DurabilityConfig::with_tier(tier)),
    }
}

#[test]
fn owner_receipt_round_trips_and_a_missing_file_fails_closed() {
    let directory = scratch("receipt");
    let receipt_path = durability_file(&directory.join("ready"));
    assert!(read_owner_receipt(&receipt_path).is_err());
    let written = receipt(2, DurabilityTier::Synchronous);
    publish_synced(&receipt_path, &serde_json::to_vec(&written).unwrap()).unwrap();
    assert_eq!(read_owner_receipt(&receipt_path).unwrap(), written);
    // The temporary name is gone: a reader sees only complete receipts.
    assert!(!receipt_path.with_extension("pending").exists());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn an_incomplete_receipt_is_never_read_as_the_native_default() {
    let directory = scratch("incomplete");
    let path = durability_file(&directory.join("ready"));
    let mut value = serde_json::to_value(receipt(0, DurabilityTier::Synchronous)).unwrap();
    let cases = [
        serde_json::json!({}),
        {
            let mut missing_tier = value.clone();
            missing_tier["durability"] = serde_json::json!({});
            missing_tier
        },
        {
            let mut missing_age = value.clone();
            missing_age["durability"]
                .as_object_mut()
                .unwrap()
                .remove("max_unsynced_age_ms");
            missing_age
        },
        {
            let mut missing_identity = value.clone();
            missing_identity
                .as_object_mut()
                .unwrap()
                .remove("partition");
            missing_identity
        },
    ];
    for case in cases {
        publish_synced(&path, &serde_json::to_vec(&case).unwrap()).unwrap();
        assert!(read_owner_receipt(&path).is_err(), "{case}");
    }
    value["unexpected"] = serde_json::json!(1);
    publish_synced(&path, &serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(read_owner_receipt(&path).is_err());
    std::fs::remove_dir_all(directory).unwrap();
}
