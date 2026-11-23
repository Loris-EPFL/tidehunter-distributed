use super::*;

#[test]
fn native_layout_matches_the_original_engine_defaults() {
    let native = crate::config::Config::default();
    assert_eq!(
        (
            native.frag_size,
            native.wal_file_size,
            native.max_maps,
            native.direct_io,
        ),
        (
            NATIVE_FRAGMENT_BYTES,
            NATIVE_WAL_FILE_BYTES,
            NATIVE_MAX_MAPS,
            false,
        )
    );
    let mut config = PartitionConfig {
        inventory: StaticInventory {
            database: [1; 16],
            epoch: 1,
            segment_generation: 1,
            partitions: vec![0],
        },
        partition: 0,
        batch_limits: BatchLimits {
            max_operations: 1,
            max_encoded_bytes: 1024,
        },
        group_limits: GroupLimits {
            max_batches: 1,
            max_encoded_bytes: 2048,
        },
        fragment_bytes: NATIVE_FRAGMENT_BYTES,
        wal_file_bytes: NATIVE_WAL_FILE_BYTES,
        max_batches: 1024,
        max_reference_metadata_bytes: 1024 * 1024,
    };
    assert_eq!(config.layout().max_maps, NATIVE_MAX_MAPS);
    config.fragment_bytes = 4 * 1024 * 1024;
    assert_eq!(config.layout().max_maps, RESEARCH_MAX_MAPS);
}
