use super::*;

#[test]
fn config_preserves_serialized_fields_and_checks_required_values() {
    let mut values: Vec<String> = [
        "appendonly",
        "yes",
        "appendfsync",
        "always",
        "no-appendfsync-on-rewrite",
        "no",
        "maxmemory-policy",
        "noeviction",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(
        serde_json::to_value(parse_config(&values, AckContract::default()).unwrap()).unwrap(),
        json!({
            "appendonly": "yes",
            "appendfsync": "always",
            "no-appendfsync-on-rewrite": "no",
            "maxmemory-policy": "noeviction"
        })
    );
    values[3] = "everysec".to_owned();
    assert!(parse_config(&values, AckContract::default()).is_err());
    values.pop();
    assert!(parse_config(&values, AckContract::default()).is_err());
}

#[test]
fn acknowledgement_contract_defaults_and_live_settings_are_exact() {
    let mut input = json!({"nodes":["127.0.0.1:6379"],"cluster":false,
            "required_replication_factor":1,"required_primary_nodes":1,"request_timeout_ms":1000});
    assert_eq!(
        Options::parse(&input).unwrap().ack_contract,
        AckContract::Synced
    );
    for (contract, fsync) in [
        (AckContract::Synced, "always"),
        (AckContract::OsBuffered, "no"),
        (AckContract::PeriodicSync, "everysec"),
    ] {
        input["ack_contract"] = json!(contract);
        assert_eq!(Options::parse(&input).unwrap().ack_contract, contract);
        for actual in ["always", "no", "everysec"] {
            let mut values: Vec<String> = [
                "appendonly",
                "yes",
                "appendfsync",
                actual,
                "no-appendfsync-on-rewrite",
                "no",
                "maxmemory-policy",
                "noeviction",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect();
            let result = parse_config(&values, contract);
            assert_eq!(result.is_ok(), actual == fsync);
            if let Err(error) = result {
                assert!(error.to_string().contains(contract.as_str()));
            }
            for index in [1, 5, 7] {
                let previous = std::mem::replace(&mut values[index], "invalid".into());
                assert!(parse_config(&values, contract).is_err());
                values[index] = previous;
            }
        }
    }
    for invalid_contract in ["page_cache", "invalid"] {
        input["ack_contract"] = json!(invalid_contract);
        assert!(Options::parse(&input).is_err());
    }
}
