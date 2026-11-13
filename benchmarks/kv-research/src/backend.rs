//! Common application-level KV boundary; adapters must materialize returned bytes.

use serde::{Deserialize, Serialize};
use tidehunter::minibytes::Bytes;

pub type BackendResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A live leased evidence boundary could not obtain a quiescent adoption cut.
#[derive(Debug)]
pub(crate) struct LiveAdoptionBoundaryError(pub(crate) &'static str);

impl std::fmt::Display for LiveAdoptionBoundaryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for LiveAdoptionBoundaryError {}

/// Keep the inherited engine API unchanged while retaining typed I/O causes.
#[derive(Debug)]
struct NativeError(tidehunter::db::DbError);

impl std::fmt::Display for NativeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Tidehunter error: {:?}", self.0)
    }
}

impl std::error::Error for NativeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            tidehunter::db::DbError::Io(error) => Some(error),
            tidehunter::db::DbError::CorruptedIndexEntry(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

pub(crate) fn native_error(
    error: tidehunter::db::DbError,
) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(NativeError(error))
}

/// Preserves a backend transport failure's type and cause for outcome classification.
#[derive(Debug)]
pub(crate) struct BackendTransportError(Box<dyn std::error::Error + Send + Sync>);

#[cfg(any(
    feature = "valkey-tcp",
    feature = "valkey-rdma",
    feature = "garnet",
    feature = "kvrocks"
))]
impl BackendTransportError {
    pub(crate) fn new(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self(error)
    }
}

impl std::fmt::Display for BackendTransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "backend transport failure: {}", self.0)
    }
}

impl std::error::Error for BackendTransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

pub(crate) fn is_backend_transport_error(error: &(dyn std::error::Error + 'static)) -> bool {
    error.downcast_ref::<BackendTransportError>().is_some()
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchAtomicity {
    Unsupported,
    CrashRecovery,
    SerializableTransaction,
}

/// Requested acknowledgement contract; admission must verify the live server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AckContract {
    #[default]
    Synced,
    OsBuffered,
    PeriodicSync,
    PageCache,
}

impl Serialize for AckContract {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AckContract {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ContractVisitor;
        impl<'de> serde::de::Visitor<'de> for ContractVisitor {
            type Value = AckContract;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "a string ack_contract (synced, os_buffered, periodic_sync or page_cache)",
                )
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                match value {
                    "synced" => Ok(AckContract::Synced),
                    "os_buffered" => Ok(AckContract::OsBuffered),
                    "periodic_sync" => Ok(AckContract::PeriodicSync),
                    "page_cache" => Ok(AckContract::PageCache),
                    _ => Err(E::unknown_variant(
                        value,
                        &["synced", "os_buffered", "periodic_sync", "page_cache"],
                    )),
                }
            }
        }
        // A derived unit enum also accepts {"synced": null}; this option must
        // be a string. The visitor borrows input without allocating a String.
        deserializer.deserialize_str(ContractVisitor)
    }
}

impl AckContract {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::OsBuffered => "os_buffered",
            Self::PeriodicSync => "periodic_sync",
            Self::PageCache => "page_cache",
        }
    }

    pub fn validate_for(self, backend: &str) -> BackendResult<()> {
        if self == Self::Synced
            || matches!(
                (backend, self),
                (
                    "valkey_tcp" | "valkey_rdma",
                    Self::OsBuffered | Self::PeriodicSync
                ) | ("kvrocks", Self::PageCache)
            )
        {
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "ack_contract {} is not supported for {backend}",
                    self.as_str()
                ),
            )
            .into())
        }
    }

    pub fn valkey_appendfsync(self) -> BackendResult<&'static str> {
        match self {
            Self::Synced => Ok("always"),
            Self::OsBuffered => Ok("no"),
            Self::PeriodicSync => Ok("everysec"),
            Self::PageCache => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ack_contract page_cache is not supported for Valkey",
            )
            .into()),
        }
    }

    /// Relaxed receipts use the same text as the external campaign controller.
    /// Synced receipts retain each adapter's existing observed-setting text.
    pub const fn acknowledgement(self) -> Option<&'static str> {
        match self {
            Self::Synced => None,
            Self::OsBuffered => Some(
                "AOF write issued to OS in event loop before queued replies in normal path; failed/short write not fatal and replies still go out; replies from later I/O step same loop not covered; Valkey issues no fsync",
            ),
            Self::PeriodicSync => Some(
                "AOF enabled; reply after the AOF buffer write is scheduled; fsync once per second (appendfsync everysec); writes can remain in user space for up to two seconds during fsync; neither OS-page-cache-before-reply nor synced-before-acknowledgement is guaranteed",
            ),
            Self::PageCache => Some(
                "RocksDB WAL enabled; write_options.sync=no; acknowledgement without WAL fsync (OS page cache); no power-loss durability guarantee",
            ),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BackendInfo {
    pub name: String,
    pub server_version: String,
    pub client_version: String,
    pub acknowledgement: String,
    pub consistency: String,
    pub replication_factor: usize,
    pub measurement_boundary: String,
    pub get_many_strategy: String,
    pub batch_atomicity: BatchAtomicity,
    pub topology: serde_json::Value,
    pub configuration: serde_json::Value,
}

/// Generic dispatch avoids a boxed future/allocation on every local cache hit.
/// A timed-out mutation has an unknown outcome; cancellation is not rollback.
/// `get_many` returns exactly one entry per key, in caller order, or an error.
pub trait KvBackend: Send + Sync + 'static {
    fn info(&self) -> BackendInfo;
    fn get(&self, key: &[u8]) -> impl Future<Output = BackendResult<Option<Bytes>>> + Send;
    fn put(&self, key: Bytes, value: Bytes) -> impl Future<Output = BackendResult<()>> + Send;
    fn delete(&self, key: Bytes) -> impl Future<Output = BackendResult<()>> + Send;
    fn get_many(
        &self,
        keys: &[Bytes],
    ) -> impl Future<Output = BackendResult<Vec<Option<Bytes>>>> + Send;
    fn put_batch(
        &self,
        entries: &[(Bytes, Bytes)],
    ) -> impl Future<Output = BackendResult<()>> + Send;

    /// Bounded measurement-phase client diagnostics, separate from engine profiles.
    fn arm_client_profile(&self, limit: usize) -> BackendResult<()> {
        if limit == 0 {
            Ok(())
        } else {
            Err(unsupported("client operation profiling"))
        }
    }

    fn finish_client_profile(
        &self,
        _deadline: std::time::Instant,
    ) -> BackendResult<serde_json::Value> {
        Err(unsupported("client operation profiling"))
    }

    /// Observe read counters outside timed API calls. Unsupported backends report absence.
    fn read_observation(&self) -> BackendResult<serde_json::Value> {
        Ok(
            serde_json::json!({"available": false, "error": "backend does not expose read observations"}),
        )
    }
    /// Advisory mapped-WAL eviction outside timed operations, with evidence.
    fn evict_mapped_wal_before_measurement(&self) -> BackendResult<serde_json::Value> {
        Err(unsupported("pre-measurement mapped-WAL eviction"))
    }
    /// Arm bounded engine-owned read diagnostics at the measurement boundary.
    fn arm_read_profile(&self, limit: usize) -> BackendResult<()> {
        if limit == 0 {
            Ok(())
        } else {
            Err(unsupported("measurement-phase read profiling"))
        }
    }

    /// Capture and disable the selected read profile.
    fn finish_read_profile(&self) -> impl Future<Output = BackendResult<serde_json::Value>> + Send {
        async { Err(unsupported("measurement-phase read profiling")) }
    }

    /// Arm bounded write diagnostics at the measurement boundary.
    fn arm_write_profile(&self, limit: usize) -> BackendResult<()> {
        if limit == 0 {
            Ok(())
        } else {
            Err(unsupported("measurement-phase write profiling"))
        }
    }

    /// Non-destructive coordinator evidence before the completion gate, when supported.
    fn snapshot_write_profile(&self) -> BackendResult<serde_json::Value> {
        Ok(
            serde_json::json!({"available": false, "reason": "backend does not expose a write interval snapshot"}),
        )
    }

    /// Finalize a selected write profile and return its engine-owned evidence.
    fn finish_write_profile(
        &self,
    ) -> impl Future<Output = BackendResult<serde_json::Value>> + Send {
        async { Ok(serde_json::json!({"supported": false})) }
    }

    /// Independent, bounded request-linked measurement timeline.
    fn arm_write_timeline(&self, limit: usize) -> BackendResult<()> {
        if limit == 0 {
            Ok(())
        } else {
            Err(unsupported("write timeline"))
        }
    }

    fn finish_write_timeline(
        &self,
        _deadline: std::time::Instant,
    ) -> BackendResult<serde_json::Value> {
        Err(unsupported("write timeline"))
    }

    fn traced_get(
        &self,
        key: &[u8],
        _id: u64,
    ) -> impl Future<Output = BackendResult<Option<Bytes>>> + Send {
        self.get(key)
    }

    fn traced_get_many(
        &self,
        keys: &[Bytes],
        _id: u64,
    ) -> impl Future<Output = BackendResult<Vec<Option<Bytes>>>> + Send {
        self.get_many(keys)
    }

    fn traced_put(
        &self,
        key: Bytes,
        value: Bytes,
        _id: u64,
    ) -> impl Future<Output = BackendResult<()>> + Send {
        self.put(key, value)
    }

    fn traced_delete(
        &self,
        key: Bytes,
        _id: u64,
    ) -> impl Future<Output = BackendResult<()>> + Send {
        self.delete(key)
    }

    fn traced_put_batch(
        &self,
        entries: &[(Bytes, Bytes)],
        _id: u64,
    ) -> impl Future<Output = BackendResult<()>> + Send {
        self.put_batch(entries)
    }

    /// Same mutation semantics as `put`, with stage clocks only for selected diagnostics.
    fn profiled_put(
        &self,
        key: Bytes,
        value: Bytes,
    ) -> impl Future<Output = BackendResult<()>> + Send {
        self.put(key, value)
    }

    /// Same mutation semantics as `delete`, with stage clocks only for selected diagnostics.
    fn profiled_delete(&self, key: Bytes) -> impl Future<Output = BackendResult<()>> + Send {
        self.delete(key)
    }

    /// Release the owned client/embedded engine before final success publication.
    /// Backends with observable fallible teardown must override this method.
    fn shutdown(self) -> impl Future<Output = BackendResult<()>> + Send
    where
        Self: Sized,
    {
        async move {
            drop(self);
            Ok(())
        }
    }
}
#[must_use]
pub fn unsupported(operation: &str) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("unsupported backend operation: {operation}"),
    )
    .into()
}

#[cfg(test)]
mod ack_contract_tests {
    use super::*;

    #[test]
    fn contracts_serialize_and_reject_unsupported_stores() {
        assert_eq!(AckContract::default(), AckContract::Synced);
        for contract in [
            AckContract::Synced,
            AckContract::OsBuffered,
            AckContract::PeriodicSync,
            AckContract::PageCache,
        ] {
            let encoded = serde_json::to_value(contract).unwrap();
            assert_eq!(encoded, contract.as_str());
            assert_eq!(
                serde_json::from_value::<AckContract>(encoded).unwrap(),
                contract
            );
            for store in ["valkey_tcp", "valkey_rdma", "kvrocks", "garnet"] {
                let expected = contract == AckContract::Synced
                    || matches!(
                        (store, contract),
                        (
                            "valkey_tcp" | "valkey_rdma",
                            AckContract::OsBuffered | AckContract::PeriodicSync
                        ) | ("kvrocks", AckContract::PageCache)
                    );
                assert_eq!(contract.validate_for(store).is_ok(), expected);
            }
        }
        assert!(serde_json::from_value::<AckContract>(serde_json::json!("unknown")).is_err());
        for invalid in [
            serde_json::json!({"synced": null}),
            serde_json::json!(null),
            serde_json::json!(1),
            serde_json::json!(false),
        ] {
            assert!(serde_json::from_value::<AckContract>(invalid).is_err());
        }
    }

    #[test]
    fn relaxed_acknowledgements_match_the_campaign_contract() {
        assert_eq!(AckContract::Synced.acknowledgement(), None);
        for contract in [
            AckContract::OsBuffered,
            AckContract::PeriodicSync,
            AckContract::PageCache,
        ] {
            // These strings are a cross-language admission contract, not a
            // durability inference made by the benchmark client.
            let text = contract.acknowledgement().unwrap();
            let python = include_str!("../../../experiments/kv-comparison/ack_contract.py");
            assert!(python.contains(&format!("return \"{text}\"")));
        }
    }
}
