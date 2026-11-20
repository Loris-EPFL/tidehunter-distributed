use super::*;
#[test]
fn durability_requires_actual_settings() {
    let mut c = BTreeMap::from([
        ("rocksdb.write_options.sync".into(), "yes".into()),
        ("rocksdb.write_options.disable_wal".into(), "no".into()),
        ("rocksdb.disable_auto_compactions".into(), "no".into()),
        ("cluster-enabled".into(), "yes".into()),
    ]);
    assert!(check_config(Store::Kvrocks, &c, AckContract::default()).is_ok());
    c.insert("rocksdb.write_options.disable_wal".into(), "yes".into());
    assert!(check_config(Store::Kvrocks, &c, AckContract::default()).is_err());
    let mut c = BTreeMap::from([
        ("appendonly".into(), "yes".into()),
        ("aof-commit-freq".into(), "0".into()),
        ("aof-commit-wait".into(), "yes".into()),
        ("aof-null-device".into(), "no".into()),
    ]);
    assert!(check_config(Store::Garnet, &c, AckContract::default()).is_ok());
    c.insert("aof-commit-wait".into(), "no".into());
    assert!(check_config(Store::Garnet, &c, AckContract::default()).is_err());
}
#[test]
fn acknowledgement_contract_is_checked_for_the_selected_store() {
    let mut input = json!({"nodes":["a:6379","b:6379","c:6379","d:6379","e:6379"],
            "cluster":true,"required_replication_factor":1,"required_primary_nodes":5,"request_timeout_ms":1000});
    for store in [Store::Garnet, Store::Kvrocks] {
        assert_eq!(
            Options::parse(&input, store).unwrap().ack_contract,
            AckContract::Synced
        );
    }
    for contract in [
        AckContract::Synced,
        AckContract::OsBuffered,
        AckContract::PeriodicSync,
        AckContract::PageCache,
    ] {
        input["ack_contract"] = json!(contract);
        assert_eq!(
            validate_options(&input, Store::Garnet).is_ok(),
            contract == AckContract::Synced
        );
        assert_eq!(
            validate_options(&input, Store::Kvrocks).is_ok(),
            matches!(contract, AckContract::Synced | AckContract::PageCache)
        );
        #[cfg(feature = "garnet")]
        assert_eq!(
            crate::backends::validate_options("garnet", &input).is_ok(),
            contract == AckContract::Synced
        );
        #[cfg(feature = "kvrocks")]
        assert_eq!(
            crate::backends::validate_options("kvrocks", &input).is_ok(),
            matches!(contract, AckContract::Synced | AckContract::PageCache)
        );
    }
    input["ack_contract"] = json!("invalid");
    assert!(validate_options(&input, Store::Kvrocks).is_err());
    assert!(validate_options(&input, Store::Garnet).is_err());
}

#[test]
fn kvrocks_page_cache_requires_wal_and_non_sync_writes() {
    for contract in [AckContract::Synced, AckContract::PageCache] {
        for sync in ["yes", "no"] {
            let mut config = BTreeMap::from([
                ("rocksdb.write_options.sync".into(), sync.into()),
                ("rocksdb.write_options.disable_wal".into(), "no".into()),
                ("rocksdb.disable_auto_compactions".into(), "no".into()),
                ("cluster-enabled".into(), "yes".into()),
            ]);
            let result = check_config(Store::Kvrocks, &config, contract);
            assert_eq!(
                result.is_ok(),
                (sync == "no") == (contract == AckContract::PageCache)
            );
            if let Err(error) = result {
                assert!(error.to_string().contains(contract.as_str()));
            }
            for (key, value) in [
                ("rocksdb.write_options.disable_wal", "yes"),
                ("rocksdb.disable_auto_compactions", "yes"),
                ("cluster-enabled", "no"),
            ] {
                let previous = config.insert(key.into(), value.into()).unwrap();
                assert!(check_config(Store::Kvrocks, &config, contract).is_err());
                config.insert(key.into(), previous);
            }
        }
    }
}
#[test]
fn topology_rejects_holes_overlap_and_replicas() {
    let good = "a a:1@2 myself,master - 0 0 0 connected 0-3276\nb b:1@2 master - 0 0 0 connected 3277-6553\nc c:1@2 master - 0 0 0 connected 6554-9830\nd d:1@2 master - 0 0 0 connected 9831-13107\ne e:1@2 master - 0 0 0 connected 13108-16383\n";
    assert!(topology(good).is_ok());
    assert!(topology(&good.replace("13108-16383", "13109-16383")).is_err());
    assert!(topology(&good.replace("13108-16383", "13107-16383")).is_err());
    assert!(
        topology(&good.replace(
            "master - 0 0 0 connected 3277",
            "slave - 0 0 0 connected 3277"
        ))
        .is_err()
    );
}
