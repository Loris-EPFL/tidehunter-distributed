//! The syncer retirement transition: its final drain adopts the frames that
//! external writers left below the extent end after the frontier stopped.
use super::super::tests::batch;
use super::tests::live_fencing::{extent_exchange, open_live, shutdown};
use super::*;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::group::CompleteGroup;
use crate::distributed::nvmeof::live_storage::LiveGroupDescriptor;
use std::fs;
use std::os::unix::fs::FileExt;

fn append_shape(node: &StorageNode) -> crate::key_shape::KeyShape {
    crate::key_shape::KeyShape::from_yaml(&node.inventory.lock().config.shape_yaml).unwrap()
}

/// A retirement stops the real syncer and drains once more. That drain adopts a
/// frame that an external writer left below the extent end after the frontier
/// stopped. The retirement's own appended persist must cover the adopted frame,
/// so the seal and the fence both succeed and the value is still there after
/// the sealed stream reopens.
#[test]
fn retiring_the_syncer_adopts_a_written_frame_before_the_seal_and_fence() {
    for seal in [false, true] {
        let temp = tempdir::TempDir::new("live-retired-syncer-adoption").unwrap();
        let (config, node) = open_live(temp.path(), DurabilityTier::Native);
        let Response::LiveLeased(granted) =
            extent_exchange(&node, Request::LeaseLiveExtent { epoch: 1 })
        else {
            panic!("expected the sole extent")
        };
        let start = granted.extent.start;
        let segment = granted.extent.segment.clone();
        let file = fs::OpenOptions::new()
            .write(true)
            .open(&segment.path)
            .unwrap();
        let frame = |sequence: u64| {
            let group = CompleteGroup::new_live(
                &append_shape(&node),
                vec![batch(&config, 1, sequence)],
                config.partition.batch_limits,
                config.partition.group_limits,
            )
            .unwrap();
            LiveGroupDescriptor::from_group(&group).unwrap().1
        };
        // Publish the first frame through adoption control: the frontier
        // appends it and the extent adoption cursor moves past it.
        let first = frame(0);
        file.write_all_at(&first, start % segment.bytes).unwrap();
        let Response::LiveAdopted(adopted) = extent_exchange(
            &node,
            Request::AdoptLiveExtent {
                epoch: 1,
                token: granted.extent.token,
                through: first.len() as u64,
                close: false,
                lease_next: false,
            },
        ) else {
            panic!("expected the first frame to be adopted")
        };
        assert_eq!(
            adopted.adoption.receipt_count, 1,
            "the first frame is the only adopted one"
        );
        let log = node.inventory.lock().active(1).unwrap();
        let frontier = log.frontier().unwrap().clone();
        // Premise at the retirement: the frontier appended the first frame and
        // synced nothing. The native tier has no age bound, so the idle syncer
        // cannot drain it first (TST-9: no wall-clock premise).
        assert_eq!(
            (frontier.appended(), frontier.synced()),
            (start + first.len() as u64, start),
            "the first frame is appended and not synced"
        );
        assert_eq!(
            log.appended_frontier(),
            start + first.len() as u64,
            "premise: only the first frame is appended"
        );
        // The second frame is written, never adopted: only the drain sees it.
        let second = frame(1);
        file.write_all_at(&second, (start + first.len() as u64) % segment.bytes)
            .unwrap();
        drop(file);
        node.control.shared.retire_syncer(&log).unwrap();
        assert_eq!(
            log.receipt_count(),
            2,
            "the retirement drain adopts the written frame"
        );
        assert_eq!(
            log.appended_frontier(),
            start + first.len() as u64 + second.len() as u64,
            "the adopted frame advances the appended prefix"
        );
        drop(log);
        let values = |log: &crate::distributed::storage::PartitionLog| {
            assert_eq!(log.receipt_count(), 2, "both frames survive the seal");
            for (sequence, receipt) in log.receipt_page(0, 2).unwrap().into_iter().enumerate() {
                assert_eq!(
                    log.read_batch(receipt).unwrap().mutations(),
                    batch(&config, 1, sequence as u64).mutations(),
                    "frame {sequence} reads back"
                );
            }
        };
        if seal {
            // Seal and live target shutdown both succeed after retirement.
            shutdown(&node, 1);
            let sealed = node.inventory.lock().sealed_log_for_test(1);
            values(&sealed);
        } else {
            let Response::Status(_) = extent_exchange(&node, Request::Fence { epoch: 2 }) else {
                panic!("expected a higher-epoch fence")
            };
            // The fence reopens the retired stream from disk.
            let recovered = node
                .inventory
                .lock()
                .live_recovery_log(2, 1)
                .unwrap()
                .unwrap();
            values(&recovered);
            drop(recovered);
            shutdown(&node, 2);
        }
    }
}
