//! Common application-level KV boundary; adapters must materialize returned bytes.

use serde::{Deserialize, Serialize};
use tidehunter::minibytes::Bytes;

pub type BackendResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

#[cfg(feature = "valkey-tcp")]
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

    /// Observe read counters outside timed API calls. Unsupported backends report absence.
    fn read_observation(&self) -> serde_json::Value {
        serde_json::json!({"available": false, "error": "backend does not expose read observations"})
    }

    /// Arm bounded write diagnostics at the measurement boundary.
    fn arm_write_profile(&self, limit: usize) -> BackendResult<()> {
        if limit == 0 {
            Ok(())
        } else {
            Err(unsupported("measurement-phase write profiling"))
        }
    }

    /// Finalize a selected write profile and return its engine-owned evidence.
    fn finish_write_profile(
        &self,
    ) -> impl Future<Output = BackendResult<serde_json::Value>> + Send {
        async { Ok(serde_json::json!({"supported": false})) }
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
