//! Permanent exports and sealed-member reads across an owner restart.
use super::live_fencing::*;
use super::*;
use crate::distributed::nvmeof::{LiveTargetConfig, NvmeofTransport};

#[test]
fn seal_keeps_writable_bindings_read_only() {
    let temp = tempdir::TempDir::new("seal-keeps-bindings").unwrap();
    let (_, node) = open_live(temp.path(), DurabilityTier::Native);
    let Response::LiveLeased(leased) =
        extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
    else {
        panic!("expected lease")
    };
    assert!(matches!(
        extent_exchange(&node, Request::Seal { epoch: 1 }),
        Response::Sealed
    ));
    let sealed: Vec<_> = node
        .inventory
        .lock()
        .live_binding_snapshot()
        .into_iter()
        .filter(|((epoch, _), _, _)| *epoch == 1)
        .collect();
    assert!(!sealed.is_empty(), "the sealed stream kept no binding");
    for (_, binding, writable) in sealed {
        assert!(!writable, "a sealed binding stayed writable");
        assert!(binding.readonly, "a sealed binding stayed writable");
    }
    assert!(
        !leased.binding.readonly,
        "the pre-seal lease grant must be writable"
    );
    shutdown(&node, 1);
}

#[test]
fn sealed_member_resolution_reuses_the_binding_without_an_export() {
    let temp = tempdir::TempDir::new("sealed-member-resolve").unwrap();
    let (config, node) = open_live(temp.path(), DurabilityTier::Native);
    leased_history(&node, &config, 1);
    let log = node.source_log_for_test(2, 1);
    let address = log.receipt_page(0, 1).unwrap()[0].address();
    drop(log);
    let before = node.inventory.lock().inline_exports.resolve;
    let Response::LiveRecords(records) = extent_exchange(
        &node,
        Request::ResolveLiveRecords {
            authority_epoch: 2,
            members: vec![address],
        },
    ) else {
        panic!("expected live records")
    };
    assert_eq!(records.entries.len(), 1);
    assert_eq!(records.entries[0].member, address);
    assert!(records.entries[0].binding.readonly);
    let stored = node
        .inventory
        .lock()
        .live_binding_snapshot_for((
            address.segment_generation,
            address.offset / config.partition.wal_file_bytes,
        ))
        .map(|(binding, _)| binding);
    assert_eq!(stored, Some(records.entries[0].binding.clone()));
    assert_eq!(
        node.inventory.lock().inline_exports.resolve,
        before,
        "a sealed member created an export instead of reusing its binding"
    );
    assert!(matches!(
        extent_exchange(
            &node,
            Request::ReleaseLiveEpochPin {
                epoch: 2,
                token: records.epoch_pin_token,
            }
        ),
        Response::NvmeofEpochReleased
    ));
    shutdown(&node, 2);
}

#[test]
fn owner_restart_creates_one_readonly_export_on_the_first_resolve() {
    let temp = tempdir::TempDir::new("restart-readonly-export").unwrap();
    let root = temp.path();
    let configfs = root.join("configfs");
    let state_dir = root.join("target");
    fs::create_dir(&configfs).unwrap();
    fs::create_dir(&state_dir).unwrap();
    for directory in ["hosts", "ports", "subsystems"] {
        fs::create_dir(configfs.join(directory)).unwrap();
    }
    let mut config = config(&root.join("node"));
    config.partition.fragment_bytes = 16 * 1024;
    config.partition.wal_file_bytes = 32 * 1024;
    config.live_target = Some(LiveTargetConfig {
        address: "127.0.0.1".into(),
        service: 4420,
        transport: NvmeofTransport::Tcp,
        host_nqn: crate::distributed::tcp::host_identity::effective_host_nqn(
            &config.partition.inventory.database,
            config.partition.partition,
        ),
        port_id: 42,
        state_dir: state_dir.clone(),
        commissioning_port_id: 43,
        commissioning_service: 4421,
    });
    let reopen = || {
        StorageNode::bench_open_live_fixture(config.clone(), metrics(), configfs.clone()).unwrap()
    };
    let node = reopen();
    fence(&node, 1).unwrap();
    leased_history(&node, &config, 1);
    let log = node.source_log_for_test(2, 1);
    let address = log.receipt_page(0, 1).unwrap()[0].address();
    drop(log);
    drop(node);

    // A new manager reconciles every scoped export away. The first read of
    // the sealed member recreates exactly one read-only export.
    let node = reopen();
    fence(&node, 3).unwrap();
    let before = node.inventory.lock().inline_exports.resolve;
    let Response::LiveRecords(records) = extent_exchange(
        &node,
        Request::ResolveLiveRecords {
            authority_epoch: 3,
            members: vec![address],
        },
    ) else {
        panic!("expected live records")
    };
    assert_eq!(records.entries.len(), 1);
    assert!(records.entries[0].binding.readonly);
    assert_eq!(
        node.inventory.lock().inline_exports.resolve,
        before + 1,
        "the restart did not create exactly one read-only export"
    );
    assert!(matches!(
        extent_exchange(
            &node,
            Request::ReleaseLiveEpochPin {
                epoch: 3,
                token: records.epoch_pin_token,
            }
        ),
        Response::NvmeofEpochReleased
    ));
    shutdown(&node, 3);
}
