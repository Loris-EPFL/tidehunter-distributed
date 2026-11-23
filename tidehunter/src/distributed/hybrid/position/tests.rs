//! Position ordering, flat codec and sentinel contract (plan S4 acceptance).

use super::{TaggedVersionPosition, VersionPosition};
use crate::distributed::types::{
    BatchVersion, MemberChecksum, PhysicalValueAddress, RecordDescriptor, RecordKind,
};
use crate::wal::position::{IndexCut, IndexEntryKind, IndexPosition};
use bytes::BytesMut;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn batch(epoch: u64, sequence: u64) -> BatchVersion {
    BatchVersion { epoch, sequence }
}

fn descriptor(offset: u32, len: u32) -> RecordDescriptor {
    RecordDescriptor {
        operation_index: 0,
        offset,
        len,
        keyspace: 0,
        kind: RecordKind::Put,
        key_len: 8,
        value_offset: 0,
        value_len: len.saturating_sub(8),
        checksum: MemberChecksum(0),
    }
}

fn address(partition: u32, generation: u64, offset: u64, len: u32) -> PhysicalValueAddress {
    PhysicalValueAddress {
        partition,
        segment_generation: generation,
        offset,
        len,
    }
}

fn position(epoch: u64, sequence: u64, operation: u32) -> VersionPosition {
    VersionPosition::member(
        batch(epoch, sequence),
        address(1, 2, 100, 64),
        operation,
        descriptor(12, 30),
        0xdead_beef,
    )
}

fn hash_of(value: impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[test]
fn member_points_at_the_member_frame() {
    let got = position(1, 4, 7);
    // batch address 100 + native CRC header 8 + in-batch member offset 12.
    assert_eq!(got.address, address(1, 2, 120, 30));
    assert_eq!(got.payload_crc, 0xdead_beef);
    assert_eq!(got.operation_index, 7);
    assert_eq!(got.batch, batch(1, 4));
}

#[test]
fn address_and_crc_are_payload_not_identity() {
    let first = position(3, 9, 5);
    let moved = first.moved_to(address(2, 7, 500, 30));
    let recrc = VersionPosition {
        payload_crc: 1,
        ..first
    };

    assert_eq!(first, moved);
    assert_eq!(first, recrc);
    assert_eq!(first.cmp(&moved), std::cmp::Ordering::Equal);
    assert_eq!(first.cmp(&recrc), std::cmp::Ordering::Equal);
    assert_eq!(hash_of(first), hash_of(moved));
    assert_eq!(hash_of(first), hash_of(recrc));
    // Re-pointing keeps the logical identity: a later write still wins.
    assert!(position(3, 10, 0) > moved);
}

#[test]
fn order_is_batch_then_operation() {
    let earlier_batch = position(2, 1, 9_999);
    let later_batch = position(2, 2, 0);
    assert!(earlier_batch < later_batch, "batch dominates operation");

    let first_op = position(2, 2, 3);
    let second_op = position(2, 2, 4);
    assert!(first_op < second_op, "same batch orders by operation index");

    let next_epoch = position(3, 0, 0);
    assert!(later_batch < next_epoch, "epoch dominates sequence");
}

#[test]
fn below_none_is_false_and_invalid_is_never_below_a_real_cut() {
    let real = position(4, 1, 0);
    let none = <BatchVersion as IndexCut>::NONE;

    assert!(!real.below(none));
    assert!(!VersionPosition::INVALID.below(batch(4, 1)));
    assert!(!VersionPosition::INVALID.is_valid());
    assert!(real.is_valid());

    assert!(position(4, 0, 0).below(batch(4, 1)));
    assert!(!position(4, 1, 0).below(batch(4, 1)), "cut is exclusive");
    assert_eq!(real.frontier(), batch(4, 1));
}

#[test]
fn flat_round_trip_is_48_bytes_for_every_kind() {
    assert_eq!(std::mem::size_of::<VersionPosition>(), 48);
    assert_eq!(<VersionPosition as IndexPosition>::LENGTH, 48);

    let original = position(9, u64::MAX - 1, 123_456);
    for kind in 0..4u8 {
        let mut buf = BytesMut::with_capacity(48);
        original.write_flat(kind, &mut buf);
        assert_eq!(buf.len(), 48);
        let (decoded, decoded_kind) = VersionPosition::read_flat(&mut buf.freeze());
        assert_eq!(decoded, original);
        assert_eq!(decoded.address, original.address);
        assert_eq!(decoded.payload_crc, original.payload_crc);
        assert_eq!(decoded_kind, kind);
    }
}

#[test]
fn tagged_orders_by_identity_then_kind() {
    let base = position(5, 5, 1);
    let clean = TaggedVersionPosition::new(base, IndexEntryKind::Clean);
    let modified = TaggedVersionPosition::new(base, IndexEntryKind::Modified);
    let removed = TaggedVersionPosition::new(base, IndexEntryKind::Removed);

    assert!(clean < modified);
    assert!(modified < removed);

    let moved =
        TaggedVersionPosition::new(base.moved_to(address(9, 9, 9, 9)), IndexEntryKind::Clean);
    assert_eq!(clean, moved);
    assert_eq!(hash_of(clean), hash_of(moved));
    assert_eq!(clean.cmp(&moved), std::cmp::Ordering::Equal);
    assert_eq!(
        <VersionPosition as IndexPosition>::tagged_position(clean),
        base
    );
}

#[test]
fn tombstone_is_valid_and_not_below_its_own_cut() {
    let cut = batch(6, 12);
    let tombstone = VersionPosition::tombstone(cut);
    assert!(tombstone.is_valid());
    assert_eq!(tombstone.frontier(), cut);
    assert!(!tombstone.below(cut));
}

#[test]
fn tombstone_never_ties_member_zero() {
    let cut = batch(6, 12);
    let tombstone = VersionPosition::tombstone(cut);
    assert_eq!(tombstone.operation_index, super::OPERATION_MASK);
    let member_zero = VersionPosition::from_member(cut, 0, address(1, 2, 100, 30), 1);
    assert_ne!(
        tombstone, member_zero,
        "the placeholder sorts after member 0"
    );
    // The reserved last slot is the only logical tie, and no real batch can
    // carry that many members; it is never stored.
    let last_slot =
        VersionPosition::from_member(cut, super::OPERATION_MASK, address(1, 2, 100, 30), 1);
    assert_eq!(tombstone, last_slot);
}

#[test]
fn the_identity_checksum_never_becomes_the_payload_crc() {
    let version = batch(7, 7);
    let batch_address = address(1, 2, 100, 64);
    let mut descriptor = descriptor(12, 30);
    descriptor.checksum = MemberChecksum(0xAAAA_AAAA);

    let direct = VersionPosition::member(version, batch_address, 3, descriptor, 0x1234);
    assert_eq!(direct.payload_crc, 0x1234);
    assert_ne!(direct.payload_crc, 0xAAAA_AAAA);

    let checked = VersionPosition::member_checked(version, batch_address, 3, descriptor, 0x1234)
        .expect("valid address");
    assert_eq!(checked.payload_crc, 0x1234);
    assert_ne!(checked.payload_crc, 0xAAAA_AAAA);
}

#[test]
#[should_panic(expected = "operation_index")]
fn operation_index_must_fit_below_the_kind_bits() {
    let mut buf = BytesMut::with_capacity(48);
    position(1, 1, 1 << 30).write_flat(0, &mut buf);
}

#[test]
fn invalid_sentinel_round_trips_as_an_untyped_position() {
    // `from_disk`-style normalization still needs a decodable row for the
    // tombstone sentinel; the codec must not reject the invalid value.
    let mut buf = BytesMut::with_capacity(48);
    VersionPosition::INVALID.write_flat(2, &mut buf);
    let (decoded, kind) = VersionPosition::read_flat(&mut buf.freeze());
    assert_eq!(decoded, VersionPosition::INVALID);
    assert!(!decoded.is_valid());
    assert_eq!(kind, 2);
}

#[test]
fn same_physical_detects_a_repoint_and_a_crc_change() {
    let first = position(3, 9, 5);
    let moved = first.moved_to(address(2, 7, 500, 30));
    assert_eq!(first, moved, "logical identity is unchanged");
    assert!(first.same_physical(&first));
    assert!(!first.same_physical(&moved), "the address differs");

    let recrc = VersionPosition {
        payload_crc: 7,
        ..first
    };
    assert!(!first.same_physical(&recrc), "the payload CRC differs");
}

#[test]
fn tagged_same_physical_sees_a_repoint_that_tagged_equality_hides() {
    let first = position(3, 9, 5);
    let moved = first.moved_to(address(2, 7, 500, 30));
    let a = first.tag(IndexEntryKind::Clean);
    let b = moved.tag(IndexEntryKind::Clean);
    // Negative control: the default (`==`) is blind to the re-point, which is
    // exactly why the flat branch of `apply_update` needs the override.
    assert_eq!(a, b, "tagged equality is logical");
    assert!(
        !<VersionPosition as IndexPosition>::tagged_same_physical(a, b),
        "a re-point that changes only the address is a physical change"
    );
    assert!(<VersionPosition as IndexPosition>::tagged_same_physical(
        a, a
    ));
}

#[test]
fn tagged_same_physical_distinguishes_crc_kind_and_identity() {
    let first = position(3, 9, 5);
    let a = first.tag(IndexEntryKind::Clean);
    let recrc = VersionPosition {
        payload_crc: 7,
        ..first
    }
    .tag(IndexEntryKind::Clean);
    assert!(!<VersionPosition as IndexPosition>::tagged_same_physical(
        a, recrc
    ));
    assert!(!<VersionPosition as IndexPosition>::tagged_same_physical(
        a,
        first.tag(IndexEntryKind::Modified)
    ));
    assert!(!<VersionPosition as IndexPosition>::tagged_same_physical(
        a,
        position(3, 10, 5).tag(IndexEntryKind::Clean)
    ));
}

#[test]
fn a_different_address_with_the_same_version_is_not_physically_equal() {
    let first = position(4, 1, 0);
    let other = VersionPosition {
        address: address(9, 9, 9, 9),
        ..first
    };
    assert_eq!(first, other);
    assert!(!<VersionPosition as IndexPosition>::tagged_same_physical(
        first.tag(IndexEntryKind::Modified),
        other.tag(IndexEntryKind::Modified)
    ));
}

#[test]
fn batch_start_is_a_member_zero_cutoff() {
    let version = batch(2, 2);
    let start = VersionPosition::batch_start(version, address(1, 2, 100, 64));
    assert_eq!(start.operation_index, 0, "sorts at member 0");
    assert_eq!(start.payload_crc, 0);
    assert_eq!(start.address, address(1, 2, 100, 64));

    let member_zero = VersionPosition::from_member(version, 0, address(1, 2, 120, 30), 5);
    assert_eq!(start, member_zero, "logically equal to member 0");
    assert!(
        !start.same_physical(&member_zero),
        "the anchor names the batch frame, not the member frame",
    );
    assert!(start.below(batch(3, 0)));
    assert!(!start.below(version), "the cut is exclusive");
}

#[test]
fn from_member_equals_member_for_the_same_member() {
    let version = batch(4, 4);
    let batch_address = address(3, 8, 4_096, 512);
    let descriptor = descriptor(64, 48);
    let computed = VersionPosition::member(version, batch_address, 2, descriptor, 0x1234);

    let member_address = address(
        batch_address.partition,
        batch_address.segment_generation,
        batch_address.offset + 8 + u64::from(descriptor.offset),
        descriptor.len,
    );
    let direct = VersionPosition::from_member(version, 2, member_address, 0x1234);
    assert_eq!(computed, direct);
    assert!(computed.same_physical(&direct));
}

#[test]
fn blob_round_trip_is_verbatim_for_the_tombstone() {
    let original = position(7, 8, 9);
    let mut buf = BytesMut::with_capacity(48);
    original.write_blob(&mut buf);
    assert_eq!(buf.len(), 48);
    let decoded = VersionPosition::read_blob(&mut buf.freeze());
    assert_eq!(decoded, original);
    assert!(decoded.same_physical(&original));

    let mut buf = BytesMut::with_capacity(48);
    VersionPosition::INVALID.write_blob(&mut buf);
    assert_eq!(buf.len(), 48);
    let decoded = VersionPosition::read_blob(&mut buf.freeze());
    assert_eq!(decoded, VersionPosition::INVALID);
    assert_eq!(decoded.address, VersionPosition::INVALID.address);
}

/// `IndexTable::deserialize_fixed_size` hands a tombstone-free on-disk blob
/// straight to its flat buffer, so a blob row must be byte-identical to the
/// kind-0 flat row of the same position. Negative control: the former blob
/// layout (epoch word first) differs from the flat row in its first 4 bytes and
/// every loaded version came back shifted.
#[test]
fn the_blob_row_is_byte_identical_to_a_clean_flat_row() {
    let max_operation = (1u32 << 30) - 1;
    for original in [
        position(7, 8, 9),
        position(1, 4, 0),
        position(u64::MAX - 1, u64::MAX - 1, max_operation),
        VersionPosition::INVALID,
    ] {
        let mut blob = BytesMut::with_capacity(48);
        original.write_blob(&mut blob);
        let mut flat = BytesMut::with_capacity(48);
        original.write_flat(0, &mut flat);
        assert_eq!(blob, flat, "{original:?}");

        let from_blob = VersionPosition::read_blob(&mut blob.freeze());
        let (from_flat, kind) = VersionPosition::read_flat(&mut flat.freeze());
        assert_eq!(kind, 0);
        assert!(from_blob.same_physical(&original), "{from_blob:?}");
        assert!(from_flat.same_physical(&original), "{from_flat:?}");
    }
}

#[test]
fn precedes_is_strict_logical_order() {
    assert!(position(1, 1, 0).precedes(position(1, 1, 1)));
    assert!(!position(1, 1, 1).precedes(position(1, 1, 1)));
    let moved = position(1, 1, 1).moved_to(address(9, 9, 9, 9));
    assert!(
        !position(1, 1, 1).precedes(moved),
        "a re-point is not a new write",
    );
    assert!(position(1, 1, 1).precedes(position(1, 2, 0)));
}

#[test]
fn untag_returns_the_update_position_and_kind() {
    let base = position(2, 3, 4);
    for kind in [
        IndexEntryKind::Clean,
        IndexEntryKind::Modified,
        IndexEntryKind::Removed,
    ] {
        let tagged = <VersionPosition as IndexPosition>::tag(base, kind);
        let (position, decoded) = <VersionPosition as IndexPosition>::untag(tagged);
        assert_eq!(decoded, kind);
        assert_eq!(position, base);
        assert!(position.same_physical(&base));
        assert_eq!(
            <VersionPosition as IndexPosition>::kind(tagged),
            kind,
            "the default kind accessor goes through untag",
        );
    }
}

#[test]
fn member_checked_matches_member_and_rejects_an_overflow() {
    let version = batch(4, 4);
    let batch_address = address(3, 8, 4_096, 512);
    let descriptor = descriptor(64, 48);
    let checked = VersionPosition::member_checked(version, batch_address, 2, descriptor, 0x1234)
        .expect("valid address");
    assert_eq!(
        checked,
        VersionPosition::member(version, batch_address, 2, descriptor, 0x1234),
    );
    assert!(checked.same_physical(&VersionPosition::member(
        version,
        batch_address,
        2,
        descriptor,
        0x1234
    )));

    let overflowing = address(1, 2, u64::MAX - 4, 64);
    assert!(
        VersionPosition::member_checked(version, overflowing, 2, descriptor, 1).is_err(),
        "peer-provided overflow is a bad frame",
    );
}

#[test]
fn a_zero_crc_member_is_not_an_anchor() {
    let version = batch(2, 2);
    let batch_address = address(1, 2, 100, 64);
    let member = VersionPosition::member(version, batch_address, 0, descriptor(12, 30), 0);
    let anchor = VersionPosition::batch_start(version, batch_address);

    assert!(member.is_valid());
    assert!(anchor.is_valid());
    assert_eq!(member, anchor, "logical member 0");
    assert!(
        !member.same_physical(&anchor),
        "the anchor names the batch frame, the member the member frame",
    );
}

#[test]
fn identity_is_the_batch_and_operation_without_the_address() {
    let first = position(5, 6, 7);
    let moved = first.moved_to(address(9, 9, 9, 9));
    assert_eq!(first.identity().batch, batch(5, 6));
    assert_eq!(first.identity().operation_index, 7);
    assert_eq!(first.identity(), moved.identity());
}

#[test]
fn validity_is_available_without_the_index_trait() {
    assert!(position(1, 1, 0).is_valid());
    assert!(!VersionPosition::INVALID.is_valid());
}

#[test]
fn committed_cut_is_the_maximum_unlike_the_native_plain_store() {
    let low = batch(3, 10);
    let high = batch(3, 20);
    assert_eq!(
        <VersionPosition as IndexPosition>::committed_cut(high, low),
        high,
        "a lower commit must not move a cell's frontier back"
    );
    assert_eq!(
        <VersionPosition as IndexPosition>::committed_cut(low, high),
        high
    );
    assert_eq!(
        <VersionPosition as IndexPosition>::committed_cut(high, high),
        high
    );
    // The native default stores the new cut whatever the old one was.
    assert_eq!(
        <crate::wal::position::WalPosition as IndexPosition>::committed_cut(
            crate::wal::position::LastProcessed::new(20),
            crate::wal::position::LastProcessed::new(10),
        ),
        crate::wal::position::LastProcessed::new(10)
    );
}

#[test]
fn anchor_cut_is_bounded_by_the_loader_unlike_the_native_first_frontier() {
    let first = VersionPosition::batch_start(batch(3, 30), address(1, 2, 100, 64));
    assert_eq!(
        <VersionPosition as IndexPosition>::anchor_cut(first, || batch(3, 20)),
        batch(3, 20),
        "a bound below the first write lowers the anchor"
    );
    assert_eq!(
        <VersionPosition as IndexPosition>::anchor_cut(first, || batch(3, 40)),
        batch(3, 30),
        "a bound above the first write leaves its own frontier"
    );
    // Native: the first write's frontier, and the bound is never evaluated.
    let position = crate::wal::position::WalPosition::new(30, 10);
    assert_eq!(
        <crate::wal::position::WalPosition as IndexPosition>::anchor_cut(position, || {
            unreachable!("the native anchor never reads the bound")
        }),
        crate::wal::position::LastProcessed::new(30)
    );
}

/// The row layout written out by hand, independent of `write_flat`: kind in the
/// top two bits of the first word (0 for an on-disk row), then epoch, sequence,
/// payload CRC, partition, segment generation, offset and length, all big
/// endian, 48 bytes.
fn literal_row(position: &VersionPosition) -> Vec<u8> {
    let mut row = Vec::with_capacity(48);
    row.extend_from_slice(&position.operation_index.to_be_bytes());
    row.extend_from_slice(&position.batch.epoch.to_be_bytes());
    row.extend_from_slice(&position.batch.sequence.to_be_bytes());
    row.extend_from_slice(&position.payload_crc.to_be_bytes());
    row.extend_from_slice(&position.address.partition.to_be_bytes());
    row.extend_from_slice(&position.address.segment_generation.to_be_bytes());
    row.extend_from_slice(&position.address.offset.to_be_bytes());
    row.extend_from_slice(&position.address.len.to_be_bytes());
    row
}

/// Differential test for the one blob-as-flat shortcut
/// (`IndexTable::deserialize_fixed_size`): for random positions, including
/// `operation_index` 0 and `OPERATION_MASK`, payload CRC 0 and `u32::MAX`, and
/// every width of the other fields, `write_blob` must equal `write_flat(0)` byte
/// for byte AND the hand-written literal layout (so a reordered field fails even
/// if both writers were changed together), and both readers must return the
/// same position.
///
/// Negative control (mutation run): swap two `put_*` calls in `write_flat`
/// (kind word and epoch) or give `write_blob` its own field order and the
/// literal comparison fails.
#[test]
fn random_positions_encode_identically_as_blob_and_clean_flat_rows() {
    use rand::{Rng, SeedableRng, rngs::StdRng};

    let mut rng = StdRng::seed_from_u64(0x5eed_b10b);
    let operations = [0, 1, super::OPERATION_MASK - 1, super::OPERATION_MASK];
    let crcs = [0, 1, u32::MAX - 1, u32::MAX];
    let mut cases = Vec::new();
    // The edges first: every operation index against every CRC.
    for operation_index in operations {
        for payload_crc in crcs {
            cases.push((operation_index, payload_crc));
        }
    }
    // Then random pairs in range.
    for _ in 0..2_000 {
        cases.push((rng.gen_range(0..=super::OPERATION_MASK), rng.r#gen::<u32>()));
    }
    for (operation_index, payload_crc) in cases {
        let original = VersionPosition {
            batch: batch(rng.r#gen::<u64>(), rng.r#gen::<u64>()),
            operation_index,
            payload_crc,
            address: address(
                rng.r#gen::<u32>(),
                rng.r#gen::<u64>(),
                rng.r#gen::<u64>(),
                rng.r#gen::<u32>(),
            ),
        };
        let mut blob = BytesMut::with_capacity(48);
        original.write_blob(&mut blob);
        let mut flat = BytesMut::with_capacity(48);
        original.write_flat(0, &mut flat);
        assert_eq!(blob.len(), 48);
        assert_eq!(blob, flat, "blob != clean flat for {original:?}");
        assert_eq!(
            &blob[..],
            &literal_row(&original)[..],
            "layout differs from the literal row for {original:?}"
        );
        let from_blob = VersionPosition::read_blob(&mut blob.clone().freeze());
        let (from_flat, kind) = VersionPosition::read_flat(&mut flat.freeze());
        assert_eq!(kind, 0);
        // Field by field: logical `==` ignores the address and the CRC.
        for decoded in [from_blob, from_flat] {
            assert_eq!(decoded.batch, original.batch);
            assert_eq!(decoded.operation_index, original.operation_index);
            assert_eq!(decoded.payload_crc, original.payload_crc);
            assert_eq!(decoded.address, original.address);
        }
    }
}
