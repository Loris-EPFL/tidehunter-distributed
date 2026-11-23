//! Frozen owner-protocol wire tags. Bincode encodes an enum variant as its
//! declaration index, so the ordered variant list below *is* the tag table of
//! protocol 36. Changing it requires a protocol bump.

use super::{HARD_FRAME_LIMIT, PROTOCOL, Request, Response, wire_codec};
use bincode::Options;
use serde::Deserialize;
use serde::de::{self, Deserializer, Visitor};

/// Deserializer that records the variant list serde derives for an enum (in
/// declaration order, i.e. wire-tag order) and then stops.
struct VariantNames(Option<&'static [&'static str]>);

#[derive(Debug)]
struct Captured;
impl std::fmt::Display for Captured {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("variant list captured")
    }
}
impl std::error::Error for Captured {}
impl de::Error for Captured {
    fn custom<T: std::fmt::Display>(_: T) -> Self {
        Self
    }
}

impl<'de> Deserializer<'de> for &mut VariantNames {
    type Error = Captured;
    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Captured> {
        Err(Captured)
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        variants: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Captured> {
        self.0 = Some(variants);
        Err(Captured)
    }
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct identifier ignored_any
    }
}

fn variants<T: for<'de> Deserialize<'de>>() -> &'static [&'static str] {
    let mut names = VariantNames(None);
    assert!(T::deserialize(&mut names).is_err());
    names.0.expect("an enum")
}

#[test]
fn protocol_36_request_tags_are_frozen() {
    assert_eq!(PROTOCOL, 36);
    assert_eq!(
        variants::<Request>(),
        [
            "RetirementFiles",     // 0
            "Status",              // 1
            "Fence",               // 2
            "Seal",                // 3
            "Stats",               // 4
            "StatsDetailed",       // 5
            "ConnectionProfile",   // 6
            "OwnerTimelineArm",    // 7
            "OwnerTimelineFetch",  // 8
            "CheckpointPosition",  // 9
            "RetireWalFiles",      // 10
            "DurableProgress",     // 11
            "PartitionStats",      // 12
            "ReleaseLiveEpochPin", // 13
            "ResolveLiveRecords",  // 14
            "LiveTargetStats",     // 15
            "ShutdownLiveTarget",  // 16
            "LeaseLiveExtent",     // 17
            "AdoptLiveExtent",     // 18
            "OpenLiveExports",     // 19
            "PreleaseLiveExtent",  // 20
            "ReplayLiveWal",       // 21
            "ReadEvidence",        // 22
            "ProcessResources",    // 23
            "InventoryPage",       // 24
            "CheckTrimGeneration", // 25
            "TrimGeneration",      // 26
            "FileInventoryPage",   // 27
            "RelocationPage",      // 28
            "ProvisionDatabase",   // 29
            "DropDatabase",        // 30
        ]
    );
}

#[test]
fn protocol_36_response_tags_are_frozen() {
    assert_eq!(
        variants::<Response>(),
        [
            "CheckpointPosition",  // 0
            "RetirementFiles",     // 1
            "Retired",             // 2
            "Status",              // 3
            "Sealed",              // 4
            "Stats",               // 5
            "Error",               // 6
            "StatsDetailed",       // 7
            "ConnectionProfile",   // 8
            "OwnerTimelineArmed",  // 9
            "OwnerTimelinePage",   // 10
            "DurableProgress",     // 11
            "PartitionStats",      // 12
            "NvmeofEpochReleased", // 13
            "LiveRecords",         // 14
            "LiveRecovery",        // 15
            "LiveTargetStats",     // 16
            "LiveLeased",          // 17
            "LiveAdopted",         // 18
            "LiveExports",         // 19
            "ReadEvidence",        // 20
            "ProcessResources",    // 21
            "InventoryPage",       // 22
            "CheckTrimGeneration", // 23
            "TrimGeneration",      // 24
            "FileInventoryPage",   // 25
            "RelocationPage",      // 26
            "ProvisionDatabase",   // 27
            "DropDatabase",        // 28
        ]
    );
}

/// The capture above reads serde's declaration-order list; check that bincode
/// really encodes those indices (fixed-int little-endian u32 tag first).
#[test]
fn bincode_tag_is_the_declaration_index() {
    let encode = |request: &Request| {
        wire_codec(HARD_FRAME_LIMIT)
            .serialize(request)
            .expect("encode request")
    };
    for (request, tag) in [
        (
            Request::Status {
                protocol: PROTOCOL,
                database: None,
            },
            1u32,
        ),
        (Request::PartitionStats, 12),
        (Request::LiveTargetStats, 15),
        (Request::ProcessResources { epoch: 3 }, 23),
    ] {
        assert_eq!(encode(&request)[..4], tag.to_le_bytes());
    }
    let reply = wire_codec(HARD_FRAME_LIMIT)
        .serialize(&Response::Retired(
            crate::distributed::storage::RetirementOutcome::Completed,
        ))
        .expect("encode response");
    assert_eq!(reply[..4], 2u32.to_le_bytes());
}

/// `ResolveLiveRecords` is keyed by member address alone: tag 14, the authority
/// epoch, then the addresses (`u32` partition, `u64` generation, `u64` offset,
/// `u32` length, fixed width). The tag did not change, so the table above and
/// `PROTOCOL` stay; this pins the payload so a reference-keyed request can never
/// decode as an address list by accident.
#[test]
fn resolve_live_records_carries_member_addresses() {
    use crate::distributed::types::PhysicalValueAddress;
    let members = vec![
        PhysicalValueAddress {
            partition: 1,
            segment_generation: 4,
            offset: 8192,
            len: 96,
        },
        PhysicalValueAddress {
            partition: 1,
            segment_generation: 2,
            offset: 16,
            len: 8,
        },
    ];
    let request = Request::ResolveLiveRecords {
        authority_epoch: 5,
        members: members.clone(),
    };
    let bytes = wire_codec(HARD_FRAME_LIMIT)
        .serialize(&request)
        .expect("encode request");
    assert_eq!(bytes[..4], 14u32.to_le_bytes());
    assert_eq!(bytes[4..12], 5u64.to_le_bytes());
    assert_eq!(bytes[12..20], 2u64.to_le_bytes());
    assert_eq!(bytes.len(), 4 + 8 + 8 + 24 * members.len());
    let decoded: Request = wire_codec(HARD_FRAME_LIMIT)
        .deserialize(&bytes)
        .expect("decode request");
    assert!(matches!(
        decoded,
        Request::ResolveLiveRecords { authority_epoch: 5, members: decoded } if decoded == members
    ));
}
