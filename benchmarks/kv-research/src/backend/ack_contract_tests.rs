use super::*;

#[test]
fn contracts_serialize_and_reject_unsupported_stores() {
    assert_eq!(AckContract::default(), AckContract::Synced);
    for contract in [
        AckContract::Synced,
        AckContract::OsBuffered,
        AckContract::PeriodicSync,
        AckContract::PageCache,
    ] {
        let encoded = serde_json::to_value(contract).unwrap();
        assert_eq!(encoded, contract.as_str());
        assert_eq!(
            serde_json::from_value::<AckContract>(encoded).unwrap(),
            contract
        );
        for store in ["valkey_tcp", "valkey_rdma", "kvrocks", "garnet"] {
            let expected = contract == AckContract::Synced
                || matches!(
                    (store, contract),
                    (
                        "valkey_tcp" | "valkey_rdma",
                        AckContract::OsBuffered | AckContract::PeriodicSync
                    ) | ("kvrocks", AckContract::PageCache)
                );
            assert_eq!(contract.validate_for(store).is_ok(), expected);
        }
    }
    assert!(serde_json::from_value::<AckContract>(serde_json::json!("unknown")).is_err());
    for invalid in [
        serde_json::json!({"synced": null}),
        serde_json::json!(null),
        serde_json::json!(1),
        serde_json::json!(false),
    ] {
        assert!(serde_json::from_value::<AckContract>(invalid).is_err());
    }
}

#[test]
fn relaxed_acknowledgements_match_the_campaign_contract() {
    assert_eq!(AckContract::Synced.acknowledgement(), None);
    for contract in [
        AckContract::OsBuffered,
        AckContract::PeriodicSync,
        AckContract::PageCache,
    ] {
        // These strings are a cross-language admission contract, not a
        // durability inference made by the benchmark client.
        let text = contract.acknowledgement().unwrap();
        let python = include_str!("../../../../experiments/kv-comparison/ack_contract.py");
        assert!(python.contains(&format!("return \"{text}\"")));
    }
}
