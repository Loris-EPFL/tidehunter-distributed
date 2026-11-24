//! `HybridIndexConfig`: the native index configuration, unchanged.
//!
//! The coordinator runs the native single-node index stack, so it consumes the
//! same native [`Config`] fields with the values Sui sets in
//! `tidehunter_util::thdb_config` (frag size, mapping counts, dirty-key and
//! snapshot thresholds, relocation policy, lock timeout). Every field keeps
//! its native name, type and value and is passed to `LargeTable`,
//! `IndexFlusher` and the control region unchanged; the only mechanism
//! difference is that the byte-cadence thresholds are driven by the value-byte
//! counter instead of a local WAL offset (plan §5, invariant I2b).
//!
//! Per-keyspace [`crate::key_shape::KeySpaceConfig`] is honored unchanged:
//! Sui sets `value_cache_size` on its hot keyspaces (1000 by default), and the
//! hybrid runs the native per-cell LRU as it is. The LRU is filled at
//! publication with the value the batch wrote, refilled by a live read that
//! fetched the value from its owner (native `Db::get`'s refill), and invalidated
//! by the native paths (a remove pops, a dropped cell clears). A read that hits
//! it needs no owner round trip; a checkpoint read consults it under native's
//! as-of guard and never refills. There is no shape rewrite.
//!
//! Two native rows describe value-batch compression. The leased route carries
//! whole owner frames, not native compressed batches, so the request is
//! accepted and reported once at open as unsupported (it is not silently
//! reinterpreted as index compression).
//!
//! The index blob cache rows (`index_blob_cache_bytes`,
//! `index_blob_cache_min_bytes`) pass through unchanged and drive the same
//! [`crate::index_blob_cache::IndexBlobCache`] in the hybrid loader: the
//! coordinator's index WAL is the native index WAL, so its blobs age out of
//! the mmap window in the same way.

use crate::compressed_batch::BatchCodec;
use crate::config::Config;
use crate::crc::CrcFrame;
use crate::db::WalEntry;
use crate::distributed::storage::LocalDbError;
use crate::relocation::RelocationStrategy;
use serde::{Deserialize, Serialize};
use std::io;
use std::time::Duration;

/// Index configuration of one hybrid coordinator database.
///
/// Defaults are Sui's production values from `tidehunter_util::thdb_config`
/// (1 GiB fragments, 8 index maps, 1,024 dirty keys, 8 GiB root cadence,
/// 7.5 GiB unload threshold, 4 flusher threads, 10 GiB index WAL files).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HybridIndexConfig {
    pub frag_size: u64,
    pub max_maps: usize,
    pub max_index_maps: Option<usize>,
    pub max_dirty_keys: usize,
    pub l0_max_entries: Option<usize>,
    /// Root trigger: value bytes written since the last stored snapshot.
    pub snapshot_written_bytes: u64,
    /// Force-unload dirty cells whose `flushed_at` is at least this many value
    /// bytes behind the current counter.
    pub snapshot_unload_threshold: u64,
    pub unload_jitter_pct: usize,
    pub num_flusher_threads: usize,
    pub num_pending_promotion_threads: usize,
    pub direct_io: bool,
    pub sync_flush: bool,
    pub wal_file_size: u64,
    /// Accepted for configuration parity only: the hybrid runs the WAL-based
    /// pass, so [`Self::validate`] refuses `IndexBased` instead of silently
    /// substituting `WalBased`. Sui leaves it at the default.
    pub relocation_strategy: RelocationStrategy,
    pub relocation_max_reclaim_pct: u8,
    pub relocation_batch_max_bytes: Option<usize>,
    pub index_min_occupancy_pct: u8,
    pub metrics_enabled: bool,
    /// Coordinator database-lock acquisition, before any fence or reservation
    /// (native `open_lock_retry_timeout`).
    pub open_lock_retry_timeout: Duration,
    pub index_auto_shard_threshold: Option<usize>,
    /// Unsupported request: reported once at open, never applied to the leased
    /// value route.
    pub batch_codec: Option<BatchCodec>,
    /// Unsupported request, paired with `batch_codec`.
    pub compressed_batch_cache_bytes: Option<usize>,
    /// Whole index blobs that aged out of the index WAL's mmap window, as
    /// native [`Config::index_blob_cache_bytes`]. `None` disables the cache.
    pub index_blob_cache_bytes: Option<usize>,
    /// Admission floor of the index blob cache, as native
    /// [`Config::index_blob_cache_min_bytes`].
    pub index_blob_cache_min_bytes: usize,
}

impl Default for HybridIndexConfig {
    fn default() -> Self {
        Self {
            frag_size: 1024 * 1024 * 1024,
            max_maps: 8,
            max_index_maps: Some(3),
            max_dirty_keys: 1024,
            l0_max_entries: None,
            snapshot_written_bytes: 8 * 1024 * 1024 * 1024,
            snapshot_unload_threshold: 15 * 1024 * 1024 * 1024 / 2,
            unload_jitter_pct: 30,
            num_flusher_threads: 4,
            num_pending_promotion_threads: 4,
            direct_io: false,
            sync_flush: false,
            wal_file_size: 10 * 1024 * 1024 * 1024,
            relocation_strategy: RelocationStrategy::WalBased,
            relocation_max_reclaim_pct: 100,
            relocation_batch_max_bytes: None,
            index_min_occupancy_pct: 15,
            metrics_enabled: true,
            open_lock_retry_timeout: Duration::from_secs(5),
            index_auto_shard_threshold: None,
            batch_codec: None,
            compressed_batch_cache_bytes: None,
            index_blob_cache_bytes: None,
            index_blob_cache_min_bytes: 64 * 1024 * 1024,
        }
    }
}

impl HybridIndexConfig {
    /// The exact native `Config::default()` index rows, not Sui's production
    /// values ([`Default`]). Tests use it for the native single-node geometry.
    #[cfg(test)]
    pub(crate) fn native_default() -> Self {
        Self::from_native(&Config::default())
    }

    /// Validates the native field ranges before any index artifact is opened.
    ///
    /// Native `Db::open` asserts these; the hybrid reports them as a typed
    /// error so a bad Sui configuration fails with a named message instead of
    /// a panic inside a background thread.
    pub(crate) fn validate(&self) -> Result<(), LocalDbError> {
        let invalid = |message: String| -> Result<(), LocalDbError> {
            Err(LocalDbError::with_kind(
                io::ErrorKind::InvalidInput,
                message,
            ))
        };
        if self.frag_size == 0 {
            return invalid("index frag_size must be positive".to_owned());
        }
        if self.wal_file_size == 0 {
            return invalid("index wal_file_size must be positive".to_owned());
        }
        if self.max_maps == 0 {
            return invalid("index max_maps must be positive".to_owned());
        }
        if self.max_index_maps == Some(0) {
            return invalid("index max_index_maps must be positive when set".to_owned());
        }
        if self.max_dirty_keys == 0 {
            return invalid("index max_dirty_keys must be positive".to_owned());
        }
        if self.num_flusher_threads == 0 {
            return invalid(
                "index num_flusher_threads must be positive: no flusher means dirty \
                 cells can never be flushed"
                    .to_owned(),
            );
        }
        if self.relocation_max_reclaim_pct > 100 {
            return invalid("index relocation_max_reclaim_pct must be in 0..=100".to_owned());
        }
        if matches!(self.relocation_strategy, RelocationStrategy::IndexBased(_)) {
            // The coordinator runs one WAL-based pass over the owner streams;
            // its index is a native `LargeTable` fed by that pass, not the
            // single-node cell walk `IndexBased` names. Running `WalBased`
            // instead would silently drop a requested native capability.
            return invalid(
                "index relocation_strategy IndexBased is not supported by the hybrid \
                 coordinator: use WalBased"
                    .to_owned(),
            );
        }
        if let Some(cache_bytes) = self.index_blob_cache_bytes {
            // Same two invariants, and the same text, as native `Config::validate`.
            if cache_bytes == 0 {
                return invalid(
                    "index_blob_cache_bytes must be non-zero; use None to disable the cache"
                        .to_owned(),
                );
            }
            if self.index_blob_cache_min_bytes > cache_bytes {
                return invalid(format!(
                    "index_blob_cache_min_bytes {} exceeds index_blob_cache_bytes {cache_bytes}: \
                     every blob that qualified for admission would be rejected as oversized",
                    self.index_blob_cache_min_bytes,
                ));
            }
        }
        if self.index_auto_shard_threshold != Some(usize::MAX) {
            // `Some(usize::MAX)` is native's "auto-sharding disabled" spelling
            // (`Config::disable_index_auto_sharding`), so it has no ceiling.
            //
            // Otherwise the effective threshold is native's
            // `Config::auto_shard_threshold`: half a fragment when the option
            // is `None`. Same ceiling as native `Config::validate`: one
            // serialized index blob must fit in a WAL fragment after framing
            // and worst-case alignment.
            let threshold = self
                .index_auto_shard_threshold
                .unwrap_or((self.frag_size / 2) as usize);
            let alignment = if self.direct_io { 512 } else { 8 };
            let overhead = CrcFrame::CRC_HEADER_LENGTH + WalEntry::INDEX_PREFIX_SIZE + alignment;
            let max = self.frag_size.saturating_sub(overhead as u64);
            if threshold as u64 > max {
                return invalid(format!(
                    "index_auto_shard_threshold {threshold} exceeds the WAL fragment \
                     ceiling {max} (frag_size={}, direct_io={})",
                    self.frag_size, self.direct_io,
                ));
            }
        }
        Ok(())
    }

    /// The native index [`Config`] this hybrid configuration describes.
    ///
    /// Value-route fields are neutralized: the commit pool and batch
    /// compression belong to the owners' leased write path, not to the
    /// coordinator's index.
    pub(crate) fn to_native_config(&self) -> Config {
        Config {
            frag_size: self.frag_size,
            max_maps: self.max_maps,
            max_index_maps: self.max_index_maps,
            max_dirty_keys: self.max_dirty_keys,
            l0_max_entries: self.l0_max_entries,
            snapshot_written_bytes: self.snapshot_written_bytes,
            snapshot_unload_threshold: self.snapshot_unload_threshold,
            unload_jitter_pct: self.unload_jitter_pct,
            direct_io: self.direct_io,
            num_flusher_threads: self.num_flusher_threads,
            sync_flush: self.sync_flush,
            wal_file_size: self.wal_file_size,
            relocation_strategy: self.relocation_strategy,
            relocation_max_reclaim_pct: self.relocation_max_reclaim_pct,
            relocation_batch_max_bytes: self.relocation_batch_max_bytes,
            index_min_occupancy_pct: self.index_min_occupancy_pct,
            metrics_enabled: self.metrics_enabled,
            commit_pool_size: 0,
            num_pending_promotion_threads: self.num_pending_promotion_threads,
            open_lock_retry_timeout: self.open_lock_retry_timeout,
            index_auto_shard_threshold: self.index_auto_shard_threshold,
            batch_codec: None,
            compressed_batch_cache_bytes: None,
            index_blob_cache_bytes: self.index_blob_cache_bytes,
            index_blob_cache_min_bytes: self.index_blob_cache_min_bytes,
            // Native striped relocation is the single-node `Db` path; the
            // hybrid runs its own relocation pass, so the native value is
            // never read.
            relocation_stripe_bytes: 64 * 1024 * 1024 * 1024,
        }
    }

    /// Captures the index rows of the native configuration Sui resolved. This is
    /// the one constructor the Sui adapter calls: it reads
    /// `tidehunter_util::thdb_config`'s result and hands it to
    /// `RemoteDatabaseConfig::index`, so no native value is re-derived.
    pub fn from_native(config: &Config) -> Self {
        Self {
            frag_size: config.frag_size,
            max_maps: config.max_maps,
            max_index_maps: config.max_index_maps,
            max_dirty_keys: config.max_dirty_keys,
            l0_max_entries: config.l0_max_entries,
            snapshot_written_bytes: config.snapshot_written_bytes,
            snapshot_unload_threshold: config.snapshot_unload_threshold,
            unload_jitter_pct: config.unload_jitter_pct,
            num_flusher_threads: config.num_flusher_threads,
            num_pending_promotion_threads: config.num_pending_promotion_threads,
            direct_io: config.direct_io,
            sync_flush: config.sync_flush,
            wal_file_size: config.wal_file_size,
            relocation_strategy: config.relocation_strategy,
            relocation_max_reclaim_pct: config.relocation_max_reclaim_pct,
            relocation_batch_max_bytes: config.relocation_batch_max_bytes,
            index_min_occupancy_pct: config.index_min_occupancy_pct,
            metrics_enabled: config.metrics_enabled,
            open_lock_retry_timeout: config.open_lock_retry_timeout,
            index_auto_shard_threshold: config.index_auto_shard_threshold,
            batch_codec: config.batch_codec,
            compressed_batch_cache_bytes: config.compressed_batch_cache_bytes,
            index_blob_cache_bytes: config.index_blob_cache_bytes,
            index_blob_cache_min_bytes: config.index_blob_cache_min_bytes,
        }
    }

    /// True when Sui asked for native value-batch compression.
    pub(crate) fn batch_compression_requested(&self) -> bool {
        self.batch_codec.is_some() || self.compressed_batch_cache_bytes.is_some()
    }

    /// The one-line warning the coordinator logs once at open, or `None`.
    pub(crate) fn unsupported_report(&self, database: &str) -> Option<String> {
        self.batch_compression_requested().then(|| {
            format!(
                "unsupported: WAL batch compression requested for {database}; \
                 the leased value route carries whole owner frames"
            )
        })
    }
}

#[cfg(test)]
mod tests;
