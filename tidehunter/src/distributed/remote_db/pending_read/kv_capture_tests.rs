// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]

use super::*;
use crate::distributed::remote_db::pipeline_tests::Fixture;
use crate::distributed::sui::kv_capture;

#[test]
fn grouped_routes_follow_immutable_flight_references() {
    let root = tempdir::TempDir::new("sui-kv-grouped-routes").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "distributed::remote_db::pending_read::kv_capture_tests::capture_child",
            "--ignored",
            "--nocapture",
        ])
        .env("SUI_TH_KV_CAPTURE_DIR", root.path().join("trace"))
        .env("SUI_CAPTURE_TEST_ROOT", root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events = std::fs::read_to_string(root.path().join("trace/events.tsv")).unwrap();
    let routes: Vec<_> = events
        .lines()
        .skip(1)
        .map(|line| line.split('\t').collect::<Vec<_>>())
        .filter(|row| row[0] == "15")
        .collect();
    // Bulk GET and scan both move Request.references into the transport. Each
    // operation must still record all eight members from the immutable flight.
    // Route outcome 2 is the live NVMe-oF route (1 was the removed owner RPC).
    assert_eq!(routes.len(), 16);
    assert!(routes.iter().all(|row| row[15] == "2"));
    let distinct: std::collections::BTreeSet<_> = routes.iter().map(|row| row[16]).collect();
    assert_eq!(distinct.len(), 8);
    let status = std::fs::read_to_string(root.path().join("trace/final.status")).unwrap();
    assert!(status.contains("dropped_full\t0\ndropped_closed\t0\nactive\t0\n"));
}

#[test]
#[ignore = "subprocess fixture used by grouped_routes_follow_immutable_flight_references"]
fn capture_child() {
    let root = std::path::PathBuf::from(std::env::var_os("SUI_CAPTURE_TEST_ROOT").unwrap());
    let mut fixture = Fixture::new();
    fixture.config.read_cache_bytes = 0;
    kv_capture::open(
        &root.join("logical-db"),
        &fixture.shape,
        Some(&fixture.config.storage.database),
    )
    .unwrap();
    let mut db = fixture.open();
    for batch in 0..2u64 {
        let mutations = (batch * 4..batch * 4 + 4)
            .map(|key| Mutation::Put {
                keyspace: KeySpace(0),
                key: (key as u32).to_be_bytes().to_vec().into(),
                value: vec![key as u8; 32].into(),
            })
            .collect();
        db.write_batch(fixture.operation(0, batch), mutations)
            .unwrap();
    }
    let keys: Vec<_> = (0..8u32).map(|key| key.to_be_bytes().to_vec()).collect();
    let values = db.get_many(KeySpace(0), &keys).unwrap();
    assert_eq!(values.len(), 8);
    assert!(values.iter().all(Option::is_some));
    assert_eq!(db.scan(KeySpace(0)).unwrap().len(), 8);
    assert!(
        db.read_rpc_count() < 16,
        "the fixture must exercise grouped requests"
    );
    db.shutdown().unwrap();
    drop(db);
    kv_capture::finish_for_test();
}
