use super::*;
use crate::index::pending_table::{PendingTable, Transaction};
use minibytes::Bytes;

fn id(epoch: u64, sequence: u64, operation_index: u32) -> LogicalValueId {
    LogicalValueId::new(MutationVersion {
        batch: BatchVersion { epoch, sequence },
        operation_index,
    })
}

#[test]
fn codec_preserves_full_logical_order_and_rejects_other_formats() {
    let ordered = [
        id(0, 0, 0),
        id(0, 0, u32::MAX),
        id(0, 1, 0),
        id(0, u64::MAX, u32::MAX),
        id(1, 0, 0),
        id(u64::MAX, u64::MAX, u32::MAX),
    ];
    for pair in ordered.windows(2) {
        assert!(pair[0] < pair[1]);
        assert!(pair[0].encode() < pair[1].encode());
    }
    for value in ordered {
        assert_eq!(
            LogicalValueId::decode(LOGICAL_VALUE_ID_FORMAT_VERSION, &value.encode()),
            Ok(value)
        );
    }
    let bytes = id(1, 2, 3).encode();
    assert_eq!(
        LogicalValueId::decode(0, &bytes),
        Err(ReferenceCodecError::UnsupportedFormat(0))
    );
    assert_eq!(
        LogicalValueId::decode(2, &bytes),
        Err(ReferenceCodecError::UnsupportedFormat(2))
    );
    for len in 0..LogicalValueId::ENCODED_LEN {
        assert_eq!(
            LogicalValueId::decode(LOGICAL_VALUE_ID_FORMAT_VERSION, &bytes[..len]),
            Err(ReferenceCodecError::InvalidLength(len))
        );
    }
    assert_eq!(
        LogicalValueId::decode(LOGICAL_VALUE_ID_FORMAT_VERSION, &[0; 21]),
        Err(ReferenceCodecError::InvalidLength(21))
    );
}

#[test]
fn logical_references_use_native_atomic_pending_publication() {
    let mut first = PendingTable::<LogicalValueId>::default();
    let mut second = PendingTable::<LogicalValueId>::default();
    let transaction = Transaction::default();
    let key = Bytes::from(vec![3]);
    first.insert(key.clone(), id(8, 12, 0), None, &transaction);
    first.remove(key.clone(), id(8, 12, 1), &transaction);
    second.insert(key.clone(), id(8, 12, 2), None, &transaction);
    assert!(first.take_committed().0.is_empty());
    assert!(second.take_committed().0.is_empty());
    transaction.commit();
    let (updates, removed) = first.take_committed_for(&key);
    assert_eq!(removed, 2);
    assert_eq!(updates[0].value, id(8, 12, 0));
    assert_eq!(updates[1].value, id(8, 12, 1));
    assert!(!updates[1].is_modified);
    assert_eq!(second.take_committed().0[0].value, id(8, 12, 2));
    // Payload width is the disk-format cost; Rust layout has target padding.
    assert_eq!(LogicalValueId::ENCODED_LEN, 20);
    assert_eq!(
        std::mem::size_of::<LogicalValueId>(),
        std::mem::size_of::<MutationVersion>()
    );
}
