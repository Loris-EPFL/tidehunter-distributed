//! Control-level forced pause (CAPACITY-R0-R1.md §6, decision 5): while
//! file maintenance holds the owner lifecycle gate, as a retirement revoke or
//! tombstone removal does, a prepared prelease, the extent closes and a
//! prepared rollover into the next file all complete through the control
//! handlers, and no grant records a prepared gate wait. Journal-only target
//! fixture: this is not evidence of kernel namespace I/O drain.
use super::tests::live_fencing::{extent_exchange, open_live, shutdown, wait_prepared};
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::tcp::inventory::InlineExport;
use std::os::unix::fs::FileExt;

/// Close `extent` empty: a skip marker at its start, then a closing adoption.
fn close_empty(
    node: &StorageNode,
    extent: &crate::distributed::nvmeof::live_storage::LiveWriteExtent,
) {
    let mut skip = [0; 4096];
    skip[..8].copy_from_slice(crate::crc::CrcFrame::skip_marker().as_ref());
    std::fs::OpenOptions::new()
        .write(true)
        .open(&extent.segment.path)
        .unwrap()
        .write_all_at(&skip, extent.start % extent.segment.bytes)
        .unwrap();
    assert!(matches!(
        extent_exchange(
            node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: extent.token,
                through: extent.start,
                close: true,
                lease_next: false,
            },
        ),
        Response::LiveAdopted(_)
    ));
}

#[test]
fn prepared_grants_and_rollover_complete_while_maintenance_holds_the_gate() {
    for tier in [DurabilityTier::Native, DurabilityTier::Synchronous] {
        let temp = tempdir::TempDir::new("r1-forced-pause").unwrap();
        let (_, node) = open_live(temp.path(), tier);
        // File 0 is not prepared before the first grant: the unprepared
        // residual may take the gate, as native waits for map preparation.
        let Response::LiveLeased(first) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected the first lease")
        };
        let log = node.inventory.lock().active(1).unwrap();
        // The background preparer published file 1's permanent binding.
        wait_prepared(&log, 1);
        let before = node.file_maintenance_stats();
        // Forced pause: maintenance owns the lifecycle gate from here on.
        let gate = node.control_context.lifecycle.lock();
        let (done, finished) = mpsc::sync_channel(1);
        let worker_node = node.clone();
        let worker = thread::spawn(move || {
            let context = &worker_node.control_context;
            let second = context
                .lease_live_published(1, tier, Some(first.extent.token), InlineExport::Prelease)
                .unwrap();
            assert_eq!(second.extent.segment.file_index, 0);
            close_empty(&worker_node, &first.extent);
            close_empty(&worker_node, &second.extent);
            // Rollover: the allocator crossed into the prepared file 1.
            let third = context
                .lease_live_published(1, tier, None, InlineExport::Lease)
                .unwrap();
            let _ = done.send(third.extent.segment.file_index);
        });
        let rolled_into = finished
            .recv_timeout(Duration::from_secs(10))
            .expect("a prepared grant, close or rollover waited for the maintenance gate");
        drop(gate);
        worker.join().unwrap();
        assert_eq!(rolled_into, 1, "{tier:?}");
        let after = node.file_maintenance_stats();
        assert_eq!(after.prepared_gate_waits, 0, "{tier:?}: {after:?}");
        // Neither paused grant took the gate at all.
        assert_eq!(
            after.unprepared_gate_waits, before.unprepared_gate_waits,
            "{tier:?}: {after:?}"
        );
        assert_eq!(
            after.ready_binding_grants,
            before.ready_binding_grants + 2,
            "{tier:?}: {after:?}"
        );
        drop(log);
        shutdown(&node, 1);
    }
}
