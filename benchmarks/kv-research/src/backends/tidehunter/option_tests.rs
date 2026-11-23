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
