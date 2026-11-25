use super::*;
use crate::distributed::codec::BatchLimits;
use crate::distributed::group::{CompleteGroup, GroupLimits};
use crate::distributed::storage::{PartitionConfig, PartitionLog, StaticInventory};
use crate::distributed::types::RequestAuthority;
use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpaceConfig, KeyType};
use crate::metrics::Metrics;
use minibytes::Bytes;

const EPOCH: u64 = 6;

fn shape() -> KeyShape {
    let mut builder = KeyShapeBuilder::new();
    builder.add_key_space("plain", 4, 1, KeyType::uniform(2));
    builder.add_key_space_config_indexing(
        "reduced",
        KeyIndexing::key_reduction(8, 4..8),
        1,
        KeyType::uniform(2),
        KeySpaceConfig::new().with_value_cache_size(16),
    );
    builder.add_key_space_config_indexing(
        "prefix",
        KeyIndexing::variable_length(),
        1,
        KeyType::prefix_uniform(1, 0),
        KeySpaceConfig::new(),
    );
    builder.build()
}

fn config() -> PartitionConfig {
    PartitionConfig {
        inventory: StaticInventory {
            database: [7; 16],
            epoch: EPOCH,
            segment_generation: 9,
            partitions: vec![0],
        },
        partition: 0,
        batch_limits: BatchLimits {
            max_operations: 32,
            max_encoded_bytes: 2048,
        },
        group_limits: GroupLimits {
            max_batches: 4,
            max_encoded_bytes: 4088,
        },
        fragment_bytes: 4096,
        wal_file_bytes: 8192,
        max_batches: 16,
        max_reference_metadata_bytes: 1024 * 1024,
    }
}

fn batch(sequence: u64) -> CompleteBatch {
    let put = |space, key: &[u8], value: &[u8]| Mutation::Put {
        keyspace: KeySpace(space),
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    };
    CompleteBatch::new(
        &shape(),
        BatchId {
            database: [7; 16],
            operation: (sequence as u128).to_be_bytes(),
        },
        BatchVersion {
            epoch: EPOCH,
            sequence,
        },
        vec![
            put(0, &[0, 0, 0, 1], b"first"),
            put(0, &[255, 0, 0, 2], b"other cell"),
            put(1, &[1, 2, 3, 4, 128, 0, 0, 3], b"cached"),
            put(2, b"prefix-key", b"variable"),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: vec![0, 0, 0, 1].into(),
            },
            put(0, &[0, 0, 0, 1], b"last"),
        ],
        config().batch_limits,
    )
    .unwrap()
}

fn open(path: &std::path::Path) -> PartitionLog {
    PartitionLog::create(
        path,
        config(),
        &shape(),
        Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
    .unwrap()
}

fn durable(log: &PartitionLog, batches: &[CompleteBatch]) -> Vec<DurableBatch> {
    let group = CompleteGroup::new(
        &shape(),
        batches.to_vec(),
        config().batch_limits,
        config().group_limits,
    )
    .unwrap();
    log.append_group(RequestAuthority { epoch: EPOCH }, &group)
        .unwrap()
        .into_iter()
        .map(|receipt| log.persist_through(receipt).unwrap())
        .collect()
}

#[test]
fn unbound_routing_rejects_count_order_and_value_mismatches() {
    let shape = shape();
    let expected = batch(0);
    let validated = CompleteBatch::validated_mutations(
        &shape,
        expected.mutations(),
        BatchLimits {
            max_operations: 100,
            max_encoded_bytes: 64 * 1024,
        },
    )
    .unwrap();
    let routing = UnboundNativePublicationRouting::prepare(&shape, &validated).unwrap();
    assert_eq!(routing.keys().count(), expected.mutations().len());
    assert!(routing.owned_bytes(&shape).is_some());
    assert!(routing.bind(&expected).is_ok());
    let mut variants = Vec::with_capacity(3);
    variants.push(expected.mutations()[..expected.mutations().len() - 1].to_vec());
    let mut reordered = expected.mutations().to_vec();
    reordered.swap(0, 1);
    variants.push(reordered);
    let mut replaced = expected.mutations().to_vec();
    let Mutation::Put { value, .. } = &mut replaced[0] else {
        panic!("fixture starts with a put");
    };
    *value = Bytes::from_static(b"changed payload");
    variants.push(replaced);
    for mutations in variants {
        let other = CompleteBatch::new(
            &shape,
            expected.id(),
            expected.version(),
            mutations,
            BatchLimits {
                max_operations: 100,
                max_encoded_bytes: 64 * 1024,
            },
        )
        .unwrap();
        let routing = UnboundNativePublicationRouting::prepare(&shape, &validated).unwrap();
        assert!(matches!(
            routing.bind(&other),
            Err(NativePublicationError::ReceiptMismatch)
        ));
    }
}

#[test]
fn preflight_routing_binds_exact_receipt_and_refuses_a_substitute_before_touching_the_index() {
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::hybrid::config::HybridIndexConfig;
    use crate::distributed::hybrid::loader::CutSources;
    use crate::distributed::hybrid::table::HybridTable;
    let temp = tempdir::TempDir::new("native-routing-reuse").unwrap();
    let log = open(&temp.path().join("log"));
    let batches = [
        batch(0).into_canonical_owned(),
        batch(1).into_canonical_owned(),
    ];
    let receipts = durable(&log, &batches);
    let routing = NativePublicationRouting::prepare(&shape(), &batches[0]).unwrap();
    assert_eq!(routing.key_count(), 6);
    assert!(routing.owned_bytes(&shape()).unwrap() >= 6 * std::mem::size_of::<MemberRoute>());
    // Only the receipt/batch/routing binding is under test: the refusal
    // happens before the table is touched, so a minimal table is enough.
    let index = tempdir::TempDir::new("native-routing-index").unwrap();
    let table = HybridTable::open(
        index.path(),
        std::sync::Arc::new(KeyShape::new_single(4, 1, KeyType::uniform(1))),
        &HybridIndexConfig::default(),
        CutSources::new(EPOCH, DurabilityTier::Native),
        Metrics::new(),
    )
    .unwrap();
    assert!(matches!(
        routing.publish(&batches[0], receipts[1], &table),
        Err(NativePublicationError::ReceiptMismatch)
    ));
    assert_eq!(
        table.index_value_bytes(),
        0,
        "a refused publish touched the index"
    );
}

/// Replaces the dropped Exact-sink tests of one durable multi-cell batch
/// (`durable_group_installs_native_multicell_batch_and_resolves_values`)
/// and the "atomic visibility" half of the old preflight test. Against the
/// hybrid index: every member lands at the version-carrying position of its
/// member frame (two key spaces, two cells of one key space, a delete then
/// a put of one key), and no cut at or below the batch version sees any of
/// them, so a reader never observes half of a batch.
#[test]
fn publish_installs_every_member_and_no_cut_sees_half_of_the_batch() {
    use crate::crc::CrcFrame;
    use crate::distributed::durability::DurabilityTier;
    use crate::distributed::hybrid::config::HybridIndexConfig;
    use crate::distributed::hybrid::loader::CutSources;
    use crate::distributed::hybrid::table::HybridTable;

    let plain = {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("plain", 4, 1, KeyType::uniform(2));
        builder.add_key_space("other", 4, 1, KeyType::uniform(2));
        builder.build()
    };
    let put = |space, key: &[u8], value: &[u8]| Mutation::Put {
        keyspace: KeySpace(space),
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    };
    let batch = CompleteBatch::new(
        &plain,
        BatchId {
            database: [7; 16],
            operation: [9; 16],
        },
        BatchVersion {
            epoch: EPOCH,
            sequence: 0,
        },
        vec![
            put(0, &[0, 0, 0, 1], b"first"),
            put(0, &[255, 0, 0, 2], b"other cell"),
            put(1, &[1, 2, 3, 4], b"second key space"),
            Mutation::Delete {
                keyspace: KeySpace(0),
                key: vec![0, 0, 0, 1].into(),
            },
            put(0, &[0, 0, 0, 1], b"last"),
        ],
        config().batch_limits,
    )
    .unwrap()
    .into_canonical_owned();
    let temp = tempdir::TempDir::new("native-publish-multicell").unwrap();
    let log = PartitionLog::create(
        &temp.path().join("log"),
        config(),
        &plain,
        Metrics::new_in_enabled(&prometheus::Registry::new(), false),
    )
    .unwrap();
    let group = CompleteGroup::new(
        &plain,
        vec![batch.clone()],
        config().batch_limits,
        config().group_limits,
    )
    .unwrap();
    let receipt = log
        .append_group(RequestAuthority { epoch: EPOCH }, &group)
        .unwrap()
        .into_iter()
        .map(|receipt| log.persist_through(receipt).unwrap())
        .next()
        .unwrap();
    let address = receipt.appended().address();
    let routing = NativePublicationRouting::prepare(&plain, &batch).unwrap();
    let index = tempdir::TempDir::new("native-publish-multicell-index").unwrap();
    let table = HybridTable::open(
        index.path(),
        std::sync::Arc::new(plain.clone()),
        &HybridIndexConfig::default(),
        CutSources::new(EPOCH, DurabilityTier::Native),
        Metrics::new(),
    )
    .unwrap();
    routing.publish(&batch, receipt, &table).unwrap();
    // (key space, key, operation index of the surviving member)
    let members: [(u8, &[u8], u32); 3] = [
        (0, &[0, 0, 0, 1], 4),
        (0, &[255, 0, 0, 2], 1),
        (1, &[1, 2, 3, 4], 2),
    ];
    let above = BatchVersion {
        epoch: EPOCH,
        sequence: 1,
    };
    for (space, key, _) in members {
        assert!(
            table
                .get(KeySpace(space), key, batch.version())
                .unwrap()
                .is_none(),
            "a cut at the batch version must not see a member"
        );
    }
    table.advance_applied(above);
    for (space, key, operation) in members {
        let found = table
            .get(KeySpace(space), key, above)
            .unwrap()
            .expect("every member is indexed above the batch version");
        let reference = batch.record_reference(address, operation as usize).unwrap();
        assert_eq!(found.batch, batch.version());
        assert_eq!(found.operation_index, operation);
        assert_eq!(
            found.address.offset,
            address.offset
                + CrcFrame::CRC_HEADER_LENGTH as u64
                + u64::from(reference.descriptor.offset)
        );
        assert_eq!(found.address.len, reference.descriptor.len);
    }
}

#[test]
fn hash_routing_reserves_derived_key_owners_for_short_inputs() {
    let shape = KeyShape::new_single_config_indexing(
        KeyIndexing::Hash,
        1,
        KeyType::uniform(1),
        KeySpaceConfig::new(),
    );
    let mutations = (0..32)
        .map(|byte| Mutation::Put {
            keyspace: KeySpace(0),
            key: vec![byte].into(),
            value: Bytes::new(),
        })
        .collect::<Vec<_>>();
    let limits = BatchLimits {
        max_operations: 32,
        max_encoded_bytes: 8192,
    };
    let validated = CompleteBatch::validated_mutations(&shape, &mutations, limits).unwrap();
    let batch = validated
        .encode(
            BatchId {
                database: [7; 16],
                operation: [3; 16],
            },
            BatchVersion {
                epoch: EPOCH,
                sequence: 0,
            },
        )
        .unwrap()
        .into_canonical_owned();
    let routing = NativePublicationRouting::prepare(&shape, &batch).unwrap();
    // Hashing creates a separate Vec and minibytes Arc owner per operation;
    // unlike fixed/reduced keys, none aliases the canonical batch backing.
    let owner = std::mem::size_of::<Vec<u8>>() + 2 * std::mem::size_of::<usize>();
    let derived_bytes: usize = routing
        .routes
        .iter()
        .map(|route| route.reduced_key.len() + owner)
        .sum();
    assert!(validated.routing_heap_bound() >= derived_bytes);
    assert!(
        routing.owned_bytes(&shape).unwrap()
            >= routing.routes.capacity() * std::mem::size_of::<MemberRoute>() + derived_bytes
    );
}
