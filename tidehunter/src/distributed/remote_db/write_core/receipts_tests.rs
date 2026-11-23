use super::*;
use crate::distributed::types::{BatchId, BatchVersion, PayloadDigest};

fn receipt(operation: u8, sequence: u64) -> RecentReceipt {
    RecentReceipt {
        id: BatchId {
            database: [1; 16],
            operation: [operation; 16],
        },
        version: BatchVersion { epoch: 3, sequence },
        digest: PayloadDigest([sequence as u8; 32]),
    }
}

#[test]
fn the_probe_index_is_power_of_two_sharded() {
    for parallelism in [1, 3, 7, 64, 1000] {
        let index = ReceiptIndex::new(parallelism);
        assert!(index.shard_count().is_power_of_two());
        assert!((64..=4096).contains(&index.shard_count()));
        assert_eq!(index.len(), 0);
    }
}

#[test]
fn window_keeps_the_newest_receipts_and_evicts_the_oldest() {
    let mut window = ReceiptWindow::with_capacity(2);
    window.push(receipt(1, 1));
    window.push(receipt(2, 2));
    window.push(receipt(3, 3));
    assert_eq!(window.len(), 2);
    assert!(window.get(&[1; 16]).is_none());
    assert_eq!(window.get(&[2; 16]).unwrap().version.sequence, 2);
    assert_eq!(window.get(&[3; 16]).unwrap().version.sequence, 3);
}

#[test]
fn reapplied_operation_keeps_only_its_newer_version() {
    let mut window = ReceiptWindow::with_capacity(2);
    window.push(receipt(7, 1));
    window.push(receipt(7, 9));
    assert_eq!(window.len(), 1);
    assert_eq!(window.get(&[7; 16]).unwrap().version.sequence, 9);
    // The replacement left one entry, so the ring is not full: the next two
    // pushes fill it, and the third evicts the oldest operation by order.
    window.push(receipt(8, 10));
    assert_eq!(window.len(), 2);
    window.push(receipt(9, 11));
    assert_eq!(window.len(), 2);
    assert!(window.get(&[7; 16]).is_none());
    assert_eq!(window.get(&[8; 16]).unwrap().version.sequence, 10);
    assert_eq!(window.get(&[9; 16]).unwrap().version.sequence, 11);
}

/// The S7 negative control: a window shorter than the retry distance makes
/// the write re-apply, and a re-application is a fresh, effect-free write —
/// never an abort and never a wrong version.
#[test]
fn a_short_window_reapplies_instead_of_aborting() {
    let mut window = ReceiptWindow::with_capacity(8);
    for index in 0..8 {
        window.push(receipt(index as u8 + 1, index));
    }
    assert!(window.get(&[1; 16]).is_some());
    window.push(receipt(9, 8));
    assert!(window.get(&[1; 16]).is_none());
    assert_eq!(window.len(), 8);
    // Re-applying the forgotten operation is accepted and becomes the
    // newest entry, not a conflict.
    window.push(receipt(1, 9));
    assert_eq!(window.get(&[1; 16]).unwrap().version.sequence, 9);
}

#[test]
fn replace_rebuilds_the_index_and_honours_the_cap() {
    let mut window = ReceiptWindow::with_capacity(4);
    window.push(receipt(1, 1));
    window.replace(vec![receipt(2, 2), receipt(3, 3)]);
    assert_eq!(window.len(), 2);
    assert!(window.get(&[1; 16]).is_none());
    assert_eq!(window.get(&[3; 16]).unwrap().version.sequence, 3);
    // Distinct operation ids: a repeated id deliberately collapses to its
    // newest entry, which is not what this test measures.
    let overflow = (0..WAL_REPLAY_WINDOW_BATCHES + 8).map(|index| {
        let mut receipt = receipt(0, index as u64);
        receipt.id.operation[..8].copy_from_slice(&(index as u64).to_be_bytes());
        receipt
    });
    window.replace(overflow);
    assert_eq!(window.len(), 4);
    assert_eq!(window.iter().count(), 4);
}

#[test]
fn resident_bound_covers_the_fixed_window() {
    let mut window = ReceiptWindow::new();
    // The bound must hold at the full cap, not only below it: a full ring is
    // the production steady state.
    for index in 0..WAL_REPLAY_WINDOW_BATCHES {
        let mut operation = [0u8; 16];
        operation[..8].copy_from_slice(&(index as u64).to_be_bytes());
        let mut receipt = receipt(0, index as u64);
        receipt.id.operation = operation;
        window.push(receipt);
    }
    assert_eq!(window.len(), WAL_REPLAY_WINDOW_BATCHES);
    assert!(window.resident_bytes() <= ReceiptWindow::resident_bound());
    assert!(
        ReceiptWindow::resident_bound()
            >= WAL_REPLAY_WINDOW_BATCHES * std::mem::size_of::<RecentReceipt>()
    );
}
