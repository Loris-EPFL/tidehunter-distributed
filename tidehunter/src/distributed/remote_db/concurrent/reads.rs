//! Point and batch reads of the concurrent facade: an index position at the
//! publication watermark, then the owner read of the member frame it names.
//!
//! The reads themselves live on [`RemoteDatabase`](super::super::RemoteDatabase)
//! so the single-owner and the shared handle run one implementation; this module
//! fixes the cut (the watermark sampled at the call) and adds the call-level
//! trace.

use super::super::pending_read::ReadTraceCallGuard;
use super::ConcurrentRemoteDatabase;
use crate::distributed::storage::LocalDbError;
use crate::key_shape::KeySpace;
use minibytes::Bytes;

impl ConcurrentRemoteDatabase {
    /// Presence at the live cut. A write acknowledged before the call is below
    /// the publication watermark sampled here, so it is visible; a newer write
    /// may be too. Index-only unless the keyspace indexes a reduction of the key
    /// (then the member's own key decides, as native's `exists`).
    ///
    /// # Errors
    /// Returns key/schema validation errors or an observed authority failure.
    pub(crate) fn contains(&self, ks: KeySpace, key: &[u8]) -> Result<bool, LocalDbError> {
        self.database.exists_at_cut(ks, key, self.table.current())
    }

    /// One key: an index lookup at the publication watermark, then the owner
    /// read of the member frame the position names.
    ///
    /// The owner may retire the file between the lookup and the fetch (release
    /// is an unlink and a directory sync, with no reader pin), so the fetch gets
    /// exactly one re-resolve: the lookup at the same cut then names the
    /// relocated copy, or nothing. A second failure is the typed
    /// `value evicted during read` error.
    ///
    /// # Errors
    /// Returns key, admission, authority, transport, or value-integrity errors.
    pub fn get(&self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>, LocalDbError> {
        self.database.get_at_cut(ks, key, self.table.current())
    }

    /// Opens a call-level read trace (`None` when tracing is off); the caller
    /// finishes it. Sui's batch read path owns its own trace lifetime.
    pub(crate) fn begin_read_trace(
        &self,
        caller_id: u64,
        members: usize,
    ) -> Option<ReadTraceCallGuard> {
        let trace = self.reads.begin_trace(caller_id, members);
        if let Some(trace) = &trace {
            trace.event("api_started");
        }
        trace
    }

    /// [`Self::get`] with a call-level read trace for `caller_id`.
    pub fn traced_get(
        &self,
        ks: KeySpace,
        key: &[u8],
        caller_id: u64,
    ) -> Result<Option<Bytes>, LocalDbError> {
        let trace = self.begin_read_trace(caller_id, 1);
        let result = self.get(ks, key);
        if let Some(trace) = trace {
            trace.event("api_finished");
            trace.finish(result.is_ok());
        }
        result
    }

    /// Any number of keys at one publication cut, fetched in pages. An invalid
    /// key fails only its own slot; a failed owner page fails the call.
    ///
    /// # Errors
    /// Returns an observed authority failure or a failed owner page.
    pub(crate) fn get_many<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.traced_get_many(ks, keys, None)
    }

    /// [`Self::get_many`] with call-level trace events. Per-owner child traces
    /// belong to the read lanes, which the reader does not expose.
    pub(crate) fn traced_get_many<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        trace: Option<&ReadTraceCallGuard>,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.database
            .get_many_at_cut(ks, keys, self.table.current(), trace)
    }
}
