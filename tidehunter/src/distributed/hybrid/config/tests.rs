//! Native configuration mapping (plan S4 acceptance, plan §5).

use super::HybridIndexConfig;
use crate::compressed_batch::BatchCodec;
use crate::config::Config;
use std::time::Duration;

#[test]
fn defaults_are_suis_production_values() {
    let config = HybridIndexConfig::default();
    assert_eq!(config.frag_size, 1024 * 1024 * 1024);
    assert_eq!(config.max_maps, 8);
    assert_eq!(config.max_index_maps, Some(3));
    assert_eq!(config.max_dirty_keys, 1024);
    assert_eq!(config.snapshot_written_bytes, 8 * 1024 * 1024 * 1024);
    assert_eq!(
        config.snapshot_unload_threshold,
        15 * 1024 * 1024 * 1024 / 2
    );
    assert_eq!(config.unload_jitter_pct, 30);
    assert_eq!(config.num_flusher_threads, 4);
    assert_eq!(config.wal_file_size, 10 * 1024 * 1024 * 1024);
    assert_eq!(config.relocation_max_reclaim_pct, 100);
    assert_eq!(config.index_min_occupancy_pct, 15);
    assert_eq!(config.open_lock_retry_timeout, Duration::from_secs(5));
    assert!(!config.direct_io);
    assert!(!config.sync_flush);
    assert!(config.metrics_enabled);
    assert!(!config.batch_compression_requested());
}

#[test]
fn to_native_config_carries_every_index_field() {
    let hybrid = HybridIndexConfig {
        frag_size: 32 * 1024 * 1024,
        max_dirty_keys: 64,
        num_flusher_threads: 2,
        index_auto_shard_threshold: Some(1024),
        snapshot_unload_threshold: 42,
        ..HybridIndexConfig::default()
    };

    let native = hybrid.to_native_config();
    assert_eq!(native.frag_size, 32 * 1024 * 1024);
    assert_eq!(native.max_dirty_keys, 64);
    assert_eq!(native.num_flusher_threads, 2);
    assert_eq!(native.index_auto_shard_threshold, Some(1024));
    assert_eq!(native.snapshot_unload_threshold, 42);
    assert_eq!(
        native.commit_pool_size, 0,
        "value-batch pool is not the index"
    );
    assert!(native.batch_codec.is_none());
    assert!(native.compressed_batch_cache_bytes.is_none());
}

#[test]
fn from_native_preserves_index_fields_and_reports_compression() {
    let mut native = Config::small();
    native.batch_codec = Some(BatchCodec::Lz4);
    native.compressed_batch_cache_bytes = Some(1024);
    native.num_flusher_threads = 3;

    let hybrid = HybridIndexConfig::from_native(&native);
    assert_eq!(hybrid.frag_size, native.frag_size);
    assert_eq!(hybrid.max_dirty_keys, native.max_dirty_keys);
    assert_eq!(hybrid.num_flusher_threads, 3);
    assert_eq!(hybrid.wal_file_size, native.wal_file_size);
    assert!(hybrid.batch_compression_requested());
    assert!(
        hybrid
            .unsupported_report("objects")
            .expect("reported")
            .contains("objects")
    );
    // The leased value route does not carry native compressed batches, so the
    // request is never applied to the index config.
    assert!(hybrid.to_native_config().batch_codec.is_none());
}

#[test]
fn serde_round_trips_and_rejects_unknown_fields() {
    let config = HybridIndexConfig {
        frag_size: 4096,
        relocation_max_reclaim_pct: 25,
        ..HybridIndexConfig::default()
    };

    let yaml = serde_yaml::to_string(&config).expect("serialize");
    let decoded: HybridIndexConfig = serde_yaml::from_str(&yaml).expect("deserialize");
    assert_eq!(decoded.frag_size, 4096);
    assert_eq!(decoded.relocation_max_reclaim_pct, 25);
    assert_eq!(decoded.max_dirty_keys, config.max_dirty_keys);

    assert!(serde_yaml::from_str::<HybridIndexConfig>("unknown_field: 1").is_err());
}

#[test]
fn an_empty_section_uses_the_defaults() {
    let decoded: HybridIndexConfig = serde_yaml::from_str("{}").expect("deserialize");
    let defaults = HybridIndexConfig::default();
    assert_eq!(decoded.frag_size, defaults.frag_size);
    assert_eq!(decoded.max_dirty_keys, defaults.max_dirty_keys);
}

#[test]
fn validate_rejects_zero_sized_fields() {
    for (name, mutate) in [
        (
            "frag_size",
            (|c: &mut HybridIndexConfig| c.frag_size = 0) as fn(&mut HybridIndexConfig),
        ),
        ("wal_file_size", |c: &mut HybridIndexConfig| {
            c.wal_file_size = 0
        }),
        ("max_maps", |c: &mut HybridIndexConfig| c.max_maps = 0),
        ("max_index_maps", |c: &mut HybridIndexConfig| {
            c.max_index_maps = Some(0)
        }),
        ("max_dirty_keys", |c: &mut HybridIndexConfig| {
            c.max_dirty_keys = 0
        }),
        ("num_flusher_threads", |c: &mut HybridIndexConfig| {
            c.num_flusher_threads = 0
        }),
    ] {
        let mut config = HybridIndexConfig::default();
        mutate(&mut config);
        let error = config.validate().expect_err(name);
        assert!(error.to_string().contains(name), "{name}: {error}");
    }
}

#[test]
fn validate_checks_the_auto_shard_fragment_ceiling() {
    let too_large = HybridIndexConfig {
        frag_size: 1024 * 1024,
        index_auto_shard_threshold: Some(1024 * 1024),
        ..HybridIndexConfig::default()
    };
    assert!(too_large.validate().is_err());

    let fits = HybridIndexConfig {
        index_auto_shard_threshold: Some(512 * 1024),
        ..too_large
    };
    fits.validate().expect("half a fragment fits");
}

#[test]
fn validate_accepts_native_small_geometry() {
    let config = HybridIndexConfig::from_native(&Config::small());
    config.validate().expect("Config::small is valid");
}

#[test]
fn native_default_is_the_native_config_default_not_suis_values() {
    let native = Config::default();
    let config = HybridIndexConfig::native_default();
    assert_eq!(config.frag_size, native.frag_size);
    assert_eq!(config.max_maps, native.max_maps);
    assert_eq!(config.max_dirty_keys, native.max_dirty_keys);
    assert_eq!(config.snapshot_written_bytes, native.snapshot_written_bytes);
    assert_eq!(
        config.snapshot_unload_threshold,
        native.snapshot_unload_threshold
    );
    assert_eq!(config.num_flusher_threads, native.num_flusher_threads);
    assert_eq!(config.wal_file_size, native.wal_file_size);
    assert_ne!(
        config.frag_size,
        HybridIndexConfig::default().frag_size,
        "Sui's production geometry differs from the native default",
    );
    config.validate().expect("native defaults are valid");
}

#[test]
fn native_auto_sharding_option_passes_through_unchanged() {
    for threshold in [None, Some(4096)] {
        let native = Config {
            index_auto_shard_threshold: threshold,
            ..Config::default()
        };
        let hybrid = HybridIndexConfig::from_native(&native);
        assert_eq!(hybrid.index_auto_shard_threshold, threshold);
        assert_eq!(
            hybrid.to_native_config().index_auto_shard_threshold,
            threshold
        );
        hybrid.validate().expect("native threshold is valid");
    }
}

#[test]
fn a_default_shard_threshold_must_fit_the_fragment_it_is_derived_from() {
    // `None` means native's half-fragment default, so it is subject to the
    // same fragment ceiling: a 32-byte fragment leaves no room for a 16-byte
    // half-fragment blob once framing and alignment are added.
    let config = HybridIndexConfig {
        frag_size: 32,
        direct_io: false,
        ..HybridIndexConfig::default()
    };
    let error = config
        .validate()
        .expect_err("a half-fragment threshold exceeds a tiny fragment's ceiling");
    let message = format!("{error:?}");
    assert!(
        message.contains("index_auto_shard_threshold 16 exceeds the WAL fragment ceiling"),
        "the error must name the resolved threshold and the ceiling: {message}"
    );
    // The explicit "disabled" spelling stays exempt.
    let disabled = HybridIndexConfig {
        frag_size: 32,
        direct_io: false,
        index_auto_shard_threshold: Some(usize::MAX),
        ..HybridIndexConfig::default()
    };
    disabled
        .validate()
        .expect("disabling auto-sharding has no ceiling");
}
