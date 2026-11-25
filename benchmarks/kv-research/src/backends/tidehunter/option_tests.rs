//! Admission of the leased-fixture index geometry option.
use super::*;

/// Fixture options with no external document; `extra` adds or replaces rows.
fn fixture_options(extra: serde_json::Value) -> serde_json::Value {
    let mut value = serde_json::json!({
        "transport": {"kind": "tcp-trusted"},
        "path": "/fixture",
        "owners": 1,
    });
    if let (Some(target), Some(source)) = (value.as_object_mut(), extra.as_object()) {
        for (key, item) in source {
            target.insert(key.clone(), item.clone());
        }
    }
    value
}

#[test]
fn external_config_and_fixture_index_are_refused_at_admission() {
    let value = fixture_options(serde_json::json!({
        "index": {"frag_size": 1048576, "wal_file_size": 4194304},
        // Deliberately incomplete: the conflict must win over nested errors,
        // because `--validate-config` is the admission gate the campaign calls.
        "external_remote_config": {}
    }));
    let error = validate_options(&value).unwrap_err().to_string();
    assert!(
        error.contains("index applies to the leased fixture only"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn invalid_index_geometry_is_refused_at_admission() {
    let value = fixture_options(serde_json::json!({
        "index": {"frag_size": 3 * 1024 * 1024, "wal_file_size": 4 * 1024 * 1024}
    }));
    let error = validate_options(&value).unwrap_err().to_string();
    assert!(
        error.contains("multiple of it"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn unaligned_index_geometry_is_refused_at_admission() {
    let value = fixture_options(serde_json::json!({
        "index": {"frag_size": 1001, "wal_file_size": 4004}
    }));
    let error = validate_options(&value).unwrap_err().to_string();
    assert!(
        error.contains("8-byte-aligned"),
        "unexpected refusal: {error}"
    );
}

#[test]
fn fixture_index_alone_is_admissible() {
    let value = fixture_options(serde_json::json!({
        "index": {"frag_size": 1048576, "wal_file_size": 4194304}
    }));
    // The trusted TCP fixture without a remote document needs Linux test-utils.
    #[cfg(all(feature = "test-utils", target_os = "linux"))]
    validate_options(&value).expect("the fixture admits the index geometry");
    #[cfg(not(all(feature = "test-utils", target_os = "linux")))]
    assert!(
        validate_options(&value).is_err(),
        "a build without the fixture must refuse the fixture route"
    );
}

/// A leased external document that deserializes, so admission reaches the
/// fixture-only row checks instead of failing on the nested document.
fn external_document() -> serde_json::Value {
    serde_json::json!({
        "storage": {
            "partitions": [0],
            "batch_limits": {"max_operations": 519, "max_encoded_bytes": 500_000},
            "group_limits": {"max_batches": 32, "max_encoded_bytes": 2_097_152},
            "fragment_bytes": 4_194_304,
            "wal_file_bytes": 16_777_216,
            "max_batches_per_partition": 8192,
            "max_reference_metadata_bytes": 67_108_864
        },
        "endpoints": [{
            "partition": 0,
            "address": "127.0.0.1:19100",
            "server_name": "fixture-admission"
        }],
        "tls": {
            "transport": {"kind": "tcp-trusted"},
            "ca": "",
            "cert": "",
            "key": "",
            "server_name": "fixture-admission"
        },
        "timeout": {"secs": 30, "nanos": 0},
        "max_admitted_batches": 8192,
        "max_pending_batches": 64,
        "max_pending_bytes": 33_554_432,
        "owner_append_occupancy": 16,
        "pipelined_writes": true,
        "read_cache_bytes": 0,
        "read_memory_allowance_bytes": 134_217_728,
        "read_lanes": 2,
        "pin_read_lanes": false,
        "max_pending_reads": 64,
        "max_pending_read_bytes": 16_777_216,
        "write_profile_limit": 0,
        "live_nvme": {
            "host_nqn": "nqn.2026-11.test:fixture-admission",
            "host_id": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            "max_inflight_requests": 16,
            "max_inflight_bytes": 4_194_304,
            "connect_timeout_ms": 10_000,
            "io_timeout_ms": 30_000,
            "leased_durability": "native",
            "leased_write_window": 1,
            "leased_sync": "per_frame_fua",
            "leased_flush_min_frame_bytes": 0,
            "inline_reads": true,
            "lease_ahead": true,
            "polled_writes": false
        }
    })
}

#[test]
fn external_config_refuses_the_fixture_durable_cut() {
    let mut extra = serde_json::json!({"external_remote_config": external_document()});
    extra["durable_cut"] = serde_json::json!({"lag_ms": 1000, "lag_bytes": 67_108_864});
    let error = validate_options(&fixture_options(extra))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("durable_cut is a local-fixture option"),
        "an external config must own its durable cut: {error}"
    );
}

#[test]
fn external_config_refuses_the_fixture_local_geometry() {
    let mut extra = serde_json::json!({"external_remote_config": external_document()});
    extra["local_geometry"] =
        serde_json::json!({"fragment_bytes": 4_194_304, "wal_file_bytes": 16_777_216});
    let error = validate_options(&fixture_options(extra))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("local_geometry is a local-fixture option"),
        "an external config must own its owner WAL geometry: {error}"
    );
}

#[test]
fn local_fixture_admits_the_durable_cut_and_local_geometry() {
    let value = fixture_options(serde_json::json!({
        "durable_cut": {"lag_ms": 1000, "lag_bytes": 67_108_864},
        "local_geometry": {"fragment_bytes": 4_194_304, "wal_file_bytes": 16_777_216}
    }));
    #[cfg(all(feature = "test-utils", target_os = "linux"))]
    {
        let options = parse_options(&value).expect("the fixture admits both rows");
        assert_eq!(
            options.durable_cut.map(|cut| (cut.lag_ms, cut.lag_bytes)),
            Some((1000, 67_108_864)),
            "durable_cut must reach the fixture unchanged"
        );
        let geometry = options.local_geometry.expect("local_geometry is parsed");
        assert_eq!(
            (geometry.fragment_bytes, geometry.wal_file_bytes),
            (4_194_304, 16_777_216),
            "local_geometry must reach the fixture unchanged"
        );
    }
    #[cfg(not(all(feature = "test-utils", target_os = "linux")))]
    assert!(
        validate_options(&value).is_err(),
        "a build without the fixture must refuse the fixture route"
    );
}

#[test]
fn ill_aligned_local_geometry_is_refused_at_admission() {
    for geometry in [
        serde_json::json!({"fragment_bytes": 3000, "wal_file_bytes": 9000}),
        serde_json::json!({"fragment_bytes": 4096, "wal_file_bytes": 5000}),
        serde_json::json!({"fragment_bytes": 0, "wal_file_bytes": 4096}),
    ] {
        let value = fixture_options(serde_json::json!({"local_geometry": geometry}));
        // The fixture route needs Linux test-utils; elsewhere the route itself is refused.
        #[cfg(all(feature = "test-utils", target_os = "linux"))]
        {
            let error = validate_options(&value).unwrap_err().to_string();
            assert!(
                error.contains("4 KiB-multiple"),
                "{geometry}: unexpected refusal: {error}"
            );
        }
        #[cfg(not(all(feature = "test-utils", target_os = "linux")))]
        assert!(
            validate_options(&value).is_err(),
            "{geometry}: a build without the fixture must refuse the fixture route"
        );
    }
}
