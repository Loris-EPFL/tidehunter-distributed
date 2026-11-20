// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit Sui adapter. Committed reads do not borrow the write-progress owner.
//! Ordinary remote writes overlap; only maintenance requires write quiescence.
//!
//! Remote pruning runs native `WalBased` relocation semantics asynchronously
//! on one background worker per database (`relocation`): it calls the filters
//! over the original WAL puts in logical order up to the first Stop, removes
//! index entries only below whole-file targets, and takes no write gate.
//! Iterators are native live iterators over the coordinator's index and a held
//! checkpoint is native's latch (`iter`): a view reads versions below its
//! pinned cut and keeps promotion from passing it. Owner WAL files are retired
//! on the native watermark, never while a retained position can name them.
use super::codec::Mutation;
use super::remote_db::{ConcurrentRemoteDatabase, RemoteDatabase, RemoteDatabaseConfig};
use super::write_timeline::{WriteTimelineExport, WriteTimelineSample, WriteTimelineStage};
use crate::config::Config;
use crate::db::{Db as NativeDb, DbError};
use crate::key_shape::{KeyShape, KeySpace};
use crate::metrics::Metrics;
use crate::minibytes::Bytes;
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

mod drop_cells;
mod index_checkpoint;
mod iter;
pub(crate) mod kv_capture;
mod relocation;
#[cfg(feature = "test_methods")]
pub use relocation::RelocationReceipt;

pub type Result<T, E = DbError> = std::result::Result<T, E>;
fn error(e: impl std::fmt::Display) -> DbError {
    DbError::Io(std::io::Error::other(e.to_string()))
}
/// Keep the typed class (busy, missing, invalid) of a remote open failure.
fn remote_error(e: super::storage::LocalDbError) -> DbError {
    DbError::Io(std::io::Error::new(e.kind(), e.to_string()))
}

/// Cumulative completed spans, including failed calls and unwinding. Nanoseconds
/// are sums across callers, not wall time or means. Recording allocates nothing.
/// Errors mean Err/unwind at that boundary.
#[derive(Default)]
struct Stage {
    ns: AtomicU64,
    calls: AtomicU64,
    errors: AtomicU64,
}
struct StageTimer<'a> {
    stage: &'a Stage,
    began: Option<Instant>,
    failed: bool,
}
impl Stage {
    fn reset(&self) {
        self.ns.store(0, Ordering::Relaxed);
        self.calls.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }
    fn record(&self, ns: Option<u64>, failed: bool) {
        if let Some(ns) = ns {
            self.ns.fetch_add(ns, Ordering::Relaxed);
        }
        if failed {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
    fn start(&self) -> StageTimer<'_> {
        self.start_if(true)
    }
    fn start_if(&self, timed: bool) -> StageTimer<'_> {
        StageTimer {
            stage: self,
            began: timed.then(Instant::now),
            failed: true,
        }
    }
    fn run<T, E>(
        &self,
        f: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E> {
        self.run_if(true, f)
    }
    fn run_if<T, E>(
        &self,
        timed: bool,
        f: impl FnOnce() -> std::result::Result<T, E>,
    ) -> std::result::Result<T, E> {
        let mut timer = self.start_if(timed);
        let result = f();
        timer.failed = result.is_err();
        result
    }
    fn measure<T>(&self, f: impl FnOnce() -> T) -> T {
        let mut timer = self.start();
        let result = f();
        timer.failed = false;
        result
    }
}
impl Drop for StageTimer<'_> {
    fn drop(&mut self) {
        let ns = self
            .began
            .map(|began| began.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        self.stage.record(ns, self.failed);
    }
}
macro_rules! visit_stages {
    ($set:ident, $owner:expr, $($field:ident),+ $(,)?) => {
        $(
            $set(concat!(stringify!($field), "_ns"), $owner.$field.ns.load(Ordering::Relaxed));
            $set(concat!(stringify!($field), "_calls"), $owner.$field.calls.load(Ordering::Relaxed));
            $set(concat!(stringify!($field), "_errors"), $owner.$field.errors.load(Ordering::Relaxed));
        )+
    };
}

fn visit_checkpoint_profile(
    set: &mut impl FnMut(&'static str, u64),
    profile: &super::remote_db::WriteProfileSnapshot,
) {
    let enabled = profile.limit_per_connection != 0;
    set("write_profile_checkpoint_available", u64::from(enabled));
    if !enabled {
        return;
    }
    for (name, value) in [
        (
            "write_profile_checkpoint_requests",
            profile.checkpoint_requests,
        ),
        (
            "write_profile_checkpoint_completed",
            profile.checkpoint_completed,
        ),
        (
            "write_profile_checkpoint_wait_ns",
            profile.checkpoint_wait_ns,
        ),
        (
            "write_profile_checkpoint_append_completions",
            profile.checkpoint_append_completions,
        ),
        (
            "write_profile_checkpoint_admission_stalls",
            profile.checkpoint_admission_stalls,
        ),
    ] {
        set(name, value);
    }
}

#[derive(Default)]
struct NativeWriteProfile {
    limit: AtomicU64,
    remaining: AtomicU64,
    barrier: Stage,
    file_calls: AtomicU64,
    file_successes: AtomicU64,
    file_ns: AtomicU64,
    namespace_calls: AtomicU64,
    namespace_successes: AtomicU64,
    namespace_ns: AtomicU64,
}
impl NativeWriteProfile {
    fn select(&self) -> bool {
        self.remaining.load(Ordering::Relaxed) != 0
            && self
                .remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
    }
    fn record(&self, profile: crate::db::ValueWalPersistenceProfile) {
        self.file_calls
            .fetch_add(profile.file_calls, Ordering::Relaxed);
        self.file_successes
            .fetch_add(profile.file_successes, Ordering::Relaxed);
        self.file_ns.fetch_add(profile.file_ns, Ordering::Relaxed);
        self.namespace_calls
            .fetch_add(profile.namespace_calls, Ordering::Relaxed);
        self.namespace_successes
            .fetch_add(profile.namespace_successes, Ordering::Relaxed);
        self.namespace_ns
            .fetch_add(profile.namespace_ns, Ordering::Relaxed);
    }
}

pub(crate) fn visit_write_connection_profile(
    set: &mut impl FnMut(&'static str, u64),
    profile: super::tcp::ConnectionProfile,
) {
    macro_rules! fields {
        ($prefix:literal, $value:expr; $($field:ident),+ $(,)?) => {
            $(set(concat!($prefix, stringify!($field)), $value.$field as u64);)+
        };
    }
    macro_rules! work {
        ($prefix:literal, $value:expr) => {
            fields!($prefix, $value; ns, calls, bytes, errors);
        };
    }
    fields!(
        "write_profile_connection_",
        profile;
        requests,
        receive_frame_including_idle_ns,
        request_decode_ns,
        handler_ns,
        response_encode_ns,
        response_send_ns,
        read_response_recheck_wait_ns,
        read_response_recheck_ns,
        read_response_recheck_samples,
        read_response_recheck_errors,
        append_inventory_wait_ns,
        append_inventory_residence_ns,
        append_decode_ns,
        append_group_build_ns,
        append_wal_ns,
        append_persist_ns,
        control_queue_ns,
        control_queue_samples,
        control_execution_ns,
        control_execution_samples,
    );
    fields!(
        "write_profile_client_",
        profile.client;
        request_encode_ns,
        exchange_ns,
        request_send_ns,
        response_header_ns,
        response_body_ns,
        response_decode_ns,
        record_verify_ns,
        requests,
    );
    fields!(
        "write_profile_client_socket_",
        profile.client.socket;
        fabric_drain_ns,
        fabric_drains,
        read_ns,
        write_ns,
        timeout_setup_ns,
        read_calls,
        write_calls,
        timeout_sets,
        read_bytes,
        write_bytes,
    );
    fields!(
        "write_profile_server_socket_",
        profile.socket;
        fabric_drain_ns,
        fabric_drains,
        read_ns,
        write_ns,
        timeout_setup_ns,
        read_calls,
        write_calls,
        timeout_sets,
        read_bytes,
        write_bytes,
    );
    fields!(
        "write_profile_client_transport_",
        profile.client_transport;
        frame_gate_stalls,
        retirement_stalls,
        frames_submitted,
        frames_retired,
        output_frames,
        peak_output_frames,
        output_bytes,
        peak_output_bytes,
        awaiting_retirement_frames,
        retained_reply_credit_bytes,
        peak_retained_reply_credit_bytes,
        cq_polls,
        cq_empty_polls,
        cq_completions,
        cq_max_poll_gap_ns,
        control_peer_checks,
        output_queue_ns,
        output_queue_samples,
        output_submit_ns,
        output_submit_samples,
        output_retire_ns,
        output_retire_samples,
        send_copy_ns,
        send_copy_calls,
        send_copy_bytes,
        receive_copy_ns,
        receive_copy_calls,
        receive_copy_bytes,
        send_post_ns,
        send_post_calls,
        send_post_bytes,
        send_post_would_block,
        send_post_errors,
        receive_post_ns,
        receive_post_calls,
        receive_post_bytes,
        receive_post_would_block,
        receive_post_errors,
        cq_call_ns,
        cq_timed_calls,
        cq_error_call_ns,
        cq_error_calls,
        cq_dispatch_ns,
        cq_dispatch_calls,
        cq_dispatch_errors,
        cq_poll_gap_ns,
        cq_poll_gap_samples,
        cq_poll_gap_max_ns,
    );
    if profile.client_transport.cq_timed_calls != 0 {
        fields!(
            "write_profile_client_transport_",
            profile.client_transport;
            tx_all_slots_pending_polls,
            rx_no_posted_slots_polls,
            rx_unread_slots_polls,
        );
    }
    work!(
        "write_profile_client_transport_frame_encode_",
        profile.client_transport.frame_encode
    );
    work!(
        "write_profile_client_transport_frame_decode_",
        profile.client_transport.frame_decode
    );
    work!(
        "write_profile_client_transport_plaintext_write_",
        profile.client_transport.plaintext_write
    );
    work!(
        "write_profile_client_transport_plaintext_read_",
        profile.client_transport.plaintext_read
    );
    work!(
        "write_profile_client_transport_plaintext_buffer_resize_",
        profile.client_transport.plaintext_buffer_resize
    );
    work!(
        "write_profile_client_transport_tls_write_",
        profile.client_transport.tls_write
    );
    work!(
        "write_profile_client_transport_tls_read_",
        profile.client_transport.tls_read
    );
    work!(
        "write_profile_client_transport_tls_process_",
        profile.client_transport.tls_process
    );
    fields!(
        "write_profile_transport_",
        profile.transport;
        frame_gate_stalls,
        retirement_stalls,
        frames_submitted,
        frames_retired,
        output_frames,
        peak_output_frames,
        output_bytes,
        peak_output_bytes,
        awaiting_retirement_frames,
        retained_reply_credit_bytes,
        peak_retained_reply_credit_bytes,
        cq_polls,
        cq_empty_polls,
        cq_completions,
        cq_max_poll_gap_ns,
        control_peer_checks,
        output_queue_ns,
        output_queue_samples,
        output_submit_ns,
        output_submit_samples,
        output_retire_ns,
        output_retire_samples,
        send_copy_ns,
        send_copy_calls,
        send_copy_bytes,
        receive_copy_ns,
        receive_copy_calls,
        receive_copy_bytes,
        send_post_ns,
        send_post_calls,
        send_post_bytes,
        send_post_would_block,
        send_post_errors,
        receive_post_ns,
        receive_post_calls,
        receive_post_bytes,
        receive_post_would_block,
        receive_post_errors,
        cq_call_ns,
        cq_timed_calls,
        cq_error_call_ns,
        cq_error_calls,
        cq_dispatch_ns,
        cq_dispatch_calls,
        cq_dispatch_errors,
        cq_poll_gap_ns,
        cq_poll_gap_samples,
        cq_poll_gap_max_ns,
    );
    if profile.transport.cq_timed_calls != 0 {
        fields!(
            "write_profile_transport_",
            profile.transport;
            tx_all_slots_pending_polls,
            rx_no_posted_slots_polls,
            rx_unread_slots_polls,
        );
    }
    work!(
        "write_profile_transport_frame_encode_",
        profile.transport.frame_encode
    );
    work!(
        "write_profile_transport_frame_decode_",
        profile.transport.frame_decode
    );
    work!(
        "write_profile_transport_plaintext_write_",
        profile.transport.plaintext_write
    );
    work!(
        "write_profile_transport_plaintext_read_",
        profile.transport.plaintext_read
    );
    work!(
        "write_profile_transport_plaintext_buffer_resize_",
        profile.transport.plaintext_buffer_resize
    );
    work!(
        "write_profile_transport_tls_write_",
        profile.transport.tls_write
    );
    work!(
        "write_profile_transport_tls_read_",
        profile.transport.tls_read
    );
    work!(
        "write_profile_transport_tls_process_",
        profile.transport.tls_process
    );
    fields!(
        "write_profile_polling_",
        profile.polling;
        progress_sweeps,
        idle_wait_calls,
        idle_wait_ns,
        scheduler_yields,
        busy_spin_hints,
        request_first_poll_gap_ns,
        request_first_poll_gap_max_ns,
        request_first_poll_samples,
    );
    fields!(
        "write_profile_storage_",
        profile.storage_write;
        append_calls,
        append_errors,
        append_retries,
        appended_groups,
        appended_receipts,
        append_total_ns,
        append_state_lock_wait_ns,
        append_validation_ns,
        append_basic_validation_ns,
        append_conflict_lookup_ns,
        append_geometry_capacity_ns,
        append_metadata_reservation_ns,
        append_descriptor_preparation_ns,
        append_wal_ns,
        append_publication_ns,
        persist_calls,
        persist_errors,
        persist_noops,
        persisted_receipts,
        persist_total_ns,
        persist_serialization_lock_wait_ns,
        persist_state_lock_wait_ns,
        persist_validation_ns,
        persist_wal_file_ns,
        persist_namespace_ns,
        persist_inventory_file_ns,
        persist_inventory_namespace_ns,
        persist_publication_ns,
        file_persist_calls,
        file_persist_successes,
        namespace_persist_calls,
        namespace_persist_successes,
        inventory_file_persist_calls,
        inventory_file_persist_successes,
        inventory_namespace_persist_calls,
        inventory_namespace_persist_successes,
        durable_publications,
    );
}

pub struct Db {
    backend: Backend,
    shape: KeyShape,
    capture_id: u64,
    writes: RwLock<()>,
    write_gate_wait: Stage,
    durable_native: AtomicBool,
    native_write_profile: NativeWriteProfile,
    operations: AtomicU64,
    pub read_calls: AtomicU64,
    pub read_keys: AtomicU64,
    /// Rows returned by remote iterators; read by the Sui metrics sampler.
    pub scan_records: Arc<AtomicU64>,
}
// One adapter is allocated per database, not per read. Keeping remote state
// inline avoids another allocation and pointer indirection on remote operations.
#[allow(clippy::large_enum_variant)]
enum Backend {
    Native(Arc<NativeDb>),
    Remote(Remote),
}
struct Remote {
    /// First field, so it drops (cancels and joins its worker) before `db`.
    relocator: relocation::RemoteRelocator,
    /// Shared only with the relocation worker, which `relocator` joins.
    db: Arc<ConcurrentRemoteDatabase>,
    epoch: u64,
    scalar_read: Stage,
    bulk_read: Stage,
    write_commit: Stage,
    profile: super::remote_db::ReadProfileMonitor,
    /// Entries skipped by held-checkpoint iterators after a bounded re-resolve
    /// (see `iter`), reported as `iterator_latched_skips`: a rise means a latch
    /// may have failed to keep a row. Tombstones are not counted.
    latched_skips: Arc<AtomicU64>,
}
impl Remote {
    /// Join the relocation worker, then take the database out of its `Arc`.
    fn into_database(self) -> Result<ConcurrentRemoteDatabase> {
        drop(self.relocator);
        Arc::try_unwrap(self.db)
            .map_err(|_| error("remote database still shared after the relocation worker joined"))
    }
}
impl Db {
    /// Live payload counters and verified namespace evidence, not configuration
    /// intent. Returns `None` when this database does not use live NVMe-oF.
    pub fn live_nvmeof_stats(&self) -> Option<super::nvmeof::live_metrics::LiveNvmeStats> {
        match &self.backend {
            Backend::Remote(remote) => remote.db.live_nvmeof_stats(),
            Backend::Native(_) => None,
        }
    }

    /// The provisioned database identity of a remote backend; `None` for the
    /// native backend.
    pub fn remote_database_id(&self) -> Option<[u8; 16]> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.database_id()),
            Backend::Native(_) => None,
        }
    }

    /// Owner persistence counters summed over owners (WAL syncs, live-frontier
    /// requests and installs). `None` for the native backend.
    pub fn owner_storage_stats(&self) -> Result<Option<super::storage::PartitionStats>> {
        match &self.backend {
            Backend::Remote(remote) => remote.db.storage_stats().map(Some).map_err(error),
            Backend::Native(_) => Ok(None),
        }
    }

    /// Boundary snapshot of the remote coordinator's durable-cut diagnostics.
    pub fn durable_cut_stats(&self) -> Option<super::remote_db::DurableCutStats> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.durable_cut_stats()),
            Backend::Native(_) => None,
        }
    }

    /// Owner persistence counters of each owner as `(partition, counters)`,
    /// in endpoint order. `None` for the native backend.
    pub fn owner_storage_stats_by_partition(
        &self,
    ) -> Result<Option<Vec<(u32, super::storage::PartitionStats)>>> {
        match &self.backend {
            Backend::Remote(remote) => remote
                .db
                .storage_stats_by_partition()
                .map(Some)
                .map_err(error),
            Backend::Native(_) => Ok(None),
        }
    }

    /// Upper mutation version of the remote published cut (the logical
    /// frontier); `None` before the first publication and for the native
    /// backend. Benchmark observation only.
    pub fn published_frontier(&self) -> Result<Option<super::types::MutationVersion>> {
        match &self.backend {
            Backend::Remote(remote) => remote.db.published_frontier().map_err(error),
            Backend::Native(_) => Ok(None),
        }
    }

    /// Final benchmark/owner teardown. Unlike ordinary close this destroys the
    /// live target's exported namespaces and is not a reopen operation.
    pub fn shutdown_live_target_final(
        self: Arc<Self>,
    ) -> Result<super::remote_db::LiveFinalShutdownEvidence> {
        let database = Arc::try_unwrap(self)
            .map_err(|_| error("Sui database still has live owners during final shutdown"))?;
        match database.backend {
            Backend::Remote(remote) => remote
                .into_database()?
                .shutdown_live_target_final()
                .map_err(error),
            Backend::Native(_) => Err(error("final live shutdown requires a live remote backend")),
        }
    }

    pub fn register_nvmeof_metrics(&self, registry: &prometheus::Registry) -> Result<()> {
        match &self.backend {
            Backend::Remote(remote) => remote.db.register_nvmeof_metrics(registry).map_err(error),
            Backend::Native(_) => Err(error("NVMe-oF requires a remote backend")),
        }
    }

    /// Native or coordinated remote drop, classified under the target's
    /// namespace lock (never from an unlocked marker check): a remote
    /// authority drops every owner and nested remote database first; an
    /// unmarked directory with remote descendants is refused (`AlreadyExists`)
    /// instead of being removed natively. Busy gives `AlreadyExists`.
    pub fn drop_db(path: &Path) -> std::io::Result<()> {
        super::remote_db::drop_db(path, NativeDb::drop_db)
    }
    pub fn open_native(
        path: &Path,
        shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        let db = NativeDb::open(path, shape, config, metrics)?;
        let shape = NativeDb::load_key_shape(path)?;
        let capture_id = kv_capture::open(path, &shape, None).map_err(error)?;
        Ok(Self::new(Backend::Native(db), shape, capture_id))
    }
    pub fn open_remote(
        path: &Path,
        shape: KeyShape,
        config: RemoteDatabaseConfig,
    ) -> Result<Arc<Self>> {
        Self::open_remote_inner(path, shape, config, |path, shape, config| {
            let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
            RemoteDatabase::open(path, shape, config, metrics)
        })
    }
    /// Ordinary-file live I/O fixture only; never a hardware qualification route.
    /// The fixture is selected before open replays the live WAL, so reopening a
    /// nonempty database never touches the real route.
    #[cfg(feature = "test-utils")]
    pub fn open_remote_with_local_live_fixture(
        path: &Path,
        shape: KeyShape,
        config: RemoteDatabaseConfig,
        buffered: bool,
    ) -> Result<Arc<Self>> {
        Self::open_remote_inner(path, shape, config, |path, shape, config| {
            RemoteDatabase::open_with_local_live_fixture(path, shape, config, buffered)
        })
    }
    /// `open_remote` over the unbuffered ordinary-file live I/O fixture;
    /// never a hardware qualification route.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn open_remote_local_fixture(
        path: &Path,
        shape: KeyShape,
        config: RemoteDatabaseConfig,
    ) -> Result<Arc<Self>> {
        Self::open_remote_inner(path, shape, config, |path, shape, config| {
            RemoteDatabase::open_with_local_live_fixture(path, shape, config, false)
        })
    }
    fn open_remote_inner(
        path: &Path,
        shape: KeyShape,
        config: RemoteDatabaseConfig,
        open: impl FnOnce(
            &Path,
            &KeyShape,
            RemoteDatabaseConfig,
        ) -> std::result::Result<RemoteDatabase, super::storage::LocalDbError>,
    ) -> Result<Arc<Self>> {
        // Before capture: a refused open must not create capture files.
        if let Some(unavailable) = super::sys::route_unavailable() {
            return Err(error(unavailable));
        }
        let mut db = open(path, &shape, config).map_err(remote_error)?;
        let capture_id = kv_capture::open(path, &shape, Some(&db.database_id())).map_err(error)?;
        db.configure_sui_read_cache();
        let epoch = db.epoch();
        let db = Arc::new(ConcurrentRemoteDatabase::new(db).map_err(error)?);
        let profile = db.read_profile_monitor();
        let relocator =
            relocation::RemoteRelocator::start_worker(Arc::clone(&db)).map_err(error)?;
        Ok(Self::new(
            Backend::Remote(Remote {
                relocator,
                db,
                epoch,
                scalar_read: Stage::default(),
                bulk_read: Stage::default(),
                write_commit: Stage::default(),
                profile,
                latched_skips: Arc::new(AtomicU64::new(0)),
            }),
            shape,
            capture_id,
        ))
    }
    fn new(backend: Backend, shape: KeyShape, capture_id: u64) -> Arc<Self> {
        Arc::new(Self {
            backend,
            shape,
            capture_id,
            writes: RwLock::new(()),
            write_gate_wait: Stage::default(),
            durable_native: AtomicBool::new(false),
            native_write_profile: NativeWriteProfile::default(),
            operations: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            read_keys: AtomicU64::new(0),
            scan_records: Arc::new(AtomicU64::new(0)),
        })
    }
    fn write_gate(&self) -> parking_lot::RwLockWriteGuard<'_, ()> {
        self.write_gate_wait.measure(|| self.writes.write())
    }
    fn shared_write_gate(&self) -> parking_lot::RwLockReadGuard<'_, ()> {
        // Hold through acknowledgement so maintenance cannot scan a cut while
        // an earlier writer is still unpublished. Independent writers overlap.
        self.write_gate_wait.measure(|| self.writes.read())
    }
    /// Select the first N native foreground durability barriers; zero adds no clocks.
    /// Configure before serving. Maintenance/audit persistence is not sampled.
    pub fn set_native_write_profile_limit(&self, limit: u64) {
        self.native_write_profile
            .limit
            .store(limit, Ordering::Relaxed);
        self.native_write_profile
            .remaining
            .store(limit, Ordering::Relaxed);
    }
    /// Arm a new bounded write sample after warm-up. Remote owners activate
    /// their append-connection profilers only after the barrier is acknowledged.
    pub fn arm_write_profile(&self, limit: usize) -> Result<()> {
        if limit > 65_536 {
            return Err(error("write profile limit exceeds 65536"));
        }
        let _gate = self.write_gate();
        match &self.backend {
            Backend::Native(_) => {
                let profile = &self.native_write_profile;
                profile.limit.store(limit as u64, Ordering::Relaxed);
                profile.remaining.store(limit as u64, Ordering::Relaxed);
                profile.barrier.reset();
                for counter in [
                    &profile.file_calls,
                    &profile.file_successes,
                    &profile.file_ns,
                    &profile.namespace_calls,
                    &profile.namespace_successes,
                    &profile.namespace_ns,
                ] {
                    counter.store(0, Ordering::Relaxed);
                }
            }
            Backend::Remote(remote) => {
                remote.db.arm_write_profile(limit).map_err(error)?;
            }
        }
        Ok(())
    }
    /// Arm request-linked coordinator diagnostics at a quiescent benchmark boundary.
    pub fn arm_write_timeline(&self, limit: usize, sample_every: usize) -> Result<()> {
        let _gate = self.write_gate();
        match &self.backend {
            Backend::Remote(remote) => remote
                .db
                .arm_write_timeline(limit, sample_every)
                .map_err(error),
            Backend::Native(_) => Err(error("write timeline requires a remote backend")),
        }
    }

    /// Export after foreground measurement; incomplete records remain explicit.
    pub fn finish_write_timeline(&self) -> Result<WriteTimelineExport> {
        let _gate = self.write_gate();
        match &self.backend {
            Backend::Remote(remote) => remote.db.finish_write_timeline().map_err(error),
            Backend::Native(_) => Err(error("write timeline requires a remote backend")),
        }
    }
    /// Finish diagnostics before an absolute monotonic deadline.
    pub fn finish_write_timeline_until(&self, deadline: Instant) -> Result<WriteTimelineExport> {
        let Backend::Remote(remote) = &self.backend else {
            return Err(error("write timeline requires a remote backend"));
        };
        let Some(_gate) = self.writes.try_write_until(deadline) else {
            remote.db.write_timeline.abort();
            return Err(error(
                "write timeline gate deadline expired; owner disarm may be incomplete",
            ));
        };
        remote
            .db
            .finish_write_timeline_until(deadline)
            .map_err(error)
    }

    /// Set before serving; zero disables extra actor/protocol diagnostic clocks.
    pub fn set_read_profile_limit(&self, limit: u64) {
        if let Backend::Remote(remote) = &self.backend {
            remote.db.set_read_profile_limit(limit);
        }
    }
    /// Snapshot best-effort caller-local traces; aggregate samples remain independent.
    pub fn read_trace_snapshot(&self) -> Option<super::remote_db::ReadTraceSnapshot> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.read_trace_snapshot()),
            Backend::Native(_) => None,
        }
    }
    /// Provider setup evidence from owner-monitor connections, not all lanes.
    #[cfg(feature = "experimental-fabric")]
    pub fn fabric_provider_evidence(
        &self,
    ) -> Option<Vec<super::remote_db::FabricProviderEvidence>> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.fabric_provider_evidence()),
            Backend::Native(_) => None,
        }
    }

    /// Snapshot sampled owner spans by configured partition without network I/O.
    /// Native backends have no remote owner partitions.
    pub fn visit_read_profile_partitions(&self, mut set: impl FnMut(u32, &'static str, u64)) {
        if let Backend::Remote(remote) = &self.backend {
            remote.profile.visit_partitions(&mut set);
        }
    }
    /// Snapshot the separate bounded coordinator-resolution sample, including
    /// cache hits and negative lookups that create no remote flight.
    pub fn coordinator_resolution_snapshot(
        &self,
    ) -> Option<super::remote_db::CoordinatorResolutionSnapshot> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.profile.coordinator_resolution_snapshot()),
            Backend::Native(_) => None,
        }
    }
    /// Drain admitted remote writes and finalize finite per-connection profile
    /// samples before a local metrics snapshot is collected.
    pub fn finish_write_profile(&self) -> Result<()> {
        let _gate = self.write_gate();
        if let Backend::Remote(remote) = &self.backend {
            remote.db.finish_write_profile().map_err(error)?;
        }
        Ok(())
    }
    /// Local counter snapshots only. Scalar/commit totals include their nested
    /// engine stages; they are not additive CPU or wall-clock fractions.
    pub fn visit_timing_metrics(&self, mut set: impl FnMut(&'static str, u64)) {
        visit_stages!(set, self, write_gate_wait);
        let profile = &self.native_write_profile;
        set(
            "native_profile_limit",
            profile.limit.load(Ordering::Relaxed),
        );
        set(
            "native_profile_available",
            u64::from(matches!(&self.backend, Backend::Native(_))),
        );
        set(
            "native_profile_barrier_calls",
            profile.barrier.calls.load(Ordering::Relaxed),
        );
        set(
            "native_profile_barrier_errors",
            profile.barrier.errors.load(Ordering::Relaxed),
        );
        set(
            "native_profile_barrier_ns",
            profile.barrier.ns.load(Ordering::Relaxed),
        );
        set(
            "native_profile_file_calls",
            profile.file_calls.load(Ordering::Relaxed),
        );
        set(
            "native_profile_file_successes",
            profile.file_successes.load(Ordering::Relaxed),
        );
        set(
            "native_profile_file_ns",
            profile.file_ns.load(Ordering::Relaxed),
        );
        set(
            "native_profile_namespace_calls",
            profile.namespace_calls.load(Ordering::Relaxed),
        );
        set(
            "native_profile_namespace_successes",
            profile.namespace_successes.load(Ordering::Relaxed),
        );
        set(
            "native_profile_namespace_ns",
            profile.namespace_ns.load(Ordering::Relaxed),
        );
        set(
            "remote_timing_available",
            u64::from(self.native().is_none()),
        );
        set("read_profile_enabled", 0);
        set("read_profile_server_available", 0);
        set("read_profile_transport_progress_available", 0);
        set("read_profile_transport_cq_available", 0);
        set("scalar_read_ns_available", 0);
        set("read_index_ns_available", 0);
        set("write_checkpoint_diagnostics_available", 0);
        if let Backend::Remote(remote) = &self.backend {
            let read_timing_available = u64::from(remote.db.read_profile_enabled());
            set("scalar_read_ns_available", read_timing_available);
            set("read_index_ns_available", read_timing_available);
            set(
                "coordinator_outcome_resident_bytes",
                remote.db.metadata_history_bytes() as u64,
            );
            visit_stages!(set, remote, scalar_read, bulk_read, write_commit);
            remote.profile.visit(&mut set);
            set(
                "iterator_latched_skips",
                remote.latched_skips.load(Ordering::Relaxed),
            );
            set(
                "snapshot_latch_deferrals",
                remote.db.snapshot_latch_deferrals(),
            );
            remote.db.visit_write_metrics(|snapshot| {
                let checkpoint = snapshot.checkpoint_diagnostics;
                set(
                    "write_checkpoint_diagnostics_available",
                    u64::from(checkpoint.available),
                );
                macro_rules! checkpoint_fields {
                    ($($field:ident),+ $(,)?) => {
                        $(set(
                            concat!("write_checkpoint_diagnostic_", stringify!($field)),
                            checkpoint.$field,
                        );)+
                    };
                }
                checkpoint_fields!(
                    records,
                    dropped_records,
                    pressure_wait_ns,
                    pressure_wait_count,
                    drain_ns,
                    drain_count,
                    existing_capture_wait_ns,
                    existing_capture_wait_count,
                    new_capture_wait_ns,
                    new_capture_wait_count,
                    staging_manifest_ns,
                    staging_manifest_count,
                    node_prefix_wait_ns,
                    node_prefix_wait_count,
                    index_prepare_ns,
                    index_prepare_count,
                    index_persist_ns,
                    index_persist_count,
                    metadata_install_ns,
                    metadata_install_count,
                    root_adoption_ns,
                    root_adoption_count,
                    owner_adoption_ns,
                    owner_adoption_count,
                    obsolete_cleanup_ns,
                    obsolete_cleanup_count,
                    tail_pressure_count,
                    known_identity_pressure_count,
                    partition_window_pressure_count,
                    index_reservation_pressure_count,
                    already_available_count,
                    after_drain_relief_count,
                    after_existing_capture_relief_count,
                    after_new_capture_relief_count,
                    capacity_error_count,
                    progress_error_count,
                );
                index_checkpoint::visit_coordinator_io(&mut set);
                let authority = snapshot.authority;
                set("write_authority_prepare_ns", authority.authority_prepare_ns);
                set("write_authority_admit_ns", authority.authority_admit_ns);
                set("write_authority_windows", authority.authority_windows);
                set("write_authority_batches", authority.authority_batches);
                set("write_storage_wait_ns", authority.storage_wait_ns);
                set("write_publication_ns", authority.publication_ns);
                set("write_dispatch_ns", authority.dispatch_ns);
                set(
                    "write_preparation_admission_ns",
                    authority.preparation_admission_ns,
                );
                set("write_checkpoint_ns", authority.checkpoint_ns);
                set("write_checkpoint_count", authority.checkpoint_count);
                set(
                    "write_background_checkpoint_suffix_batches",
                    authority.background_checkpoint_suffix_batches,
                );
                set(
                    "write_background_checkpoint_ns",
                    authority.background_checkpoint_ns,
                );
                set(
                    "write_durable_receipt_publication_blocked_batches",
                    authority.durable_receipt_publication_blocked_batches,
                );
                set(
                    "write_durable_receipt_publication_waiting_batches",
                    authority.durable_receipt_publication_waiting_batches,
                );
                set(
                    "write_durable_receipt_publication_waiting_peak",
                    authority.durable_receipt_publication_waiting_peak,
                );
                set(
                    "write_durable_receipt_publication_blocked_ns",
                    authority.durable_receipt_publication_blocked_ns,
                );
                set(
                    "write_out_of_prefix_published_batches",
                    authority.out_of_prefix_published_batches,
                );
                set(
                    "write_dependency_wait_batches",
                    authority.dependency_wait_batches,
                );
                let pipeline = snapshot.pipeline;
                set("write_append_requests", pipeline.append_requests);
                set("write_concurrent_admitted", pipeline.concurrent_admitted);
                set(
                    "write_concurrent_pending_peak",
                    pipeline.concurrent_pending_peak as u64,
                );
                set(
                    "write_concurrent_queued_peak",
                    pipeline.concurrent_queued_peak as u64,
                );
                set(
                    "write_concurrent_queue_bytes_peak",
                    pipeline.concurrent_queue_bytes_peak as u64,
                );
                let cache = snapshot.read_cache;
                set("cache_inserts", cache.inserts);
                set("cache_bypass_admission", cache.bypass_admission);
                set("cache_rejected_oversize", cache.rejected_oversize);
                set("cache_bypass_disabled", cache.bypass_disabled);
                set("cache_bypass_eviction_limit", cache.bypass_eviction_limit);
                set("cache_admitted_bytes", cache.admitted_bytes);
                set("cache_entries", cache.entries as u64);
                let write_profile = snapshot.write_profile;
                visit_checkpoint_profile(&mut set, &write_profile);
                set(
                    "write_profile_limit",
                    write_profile.limit_per_connection as u64,
                );
                set(
                    "write_profile_expected_connections",
                    write_profile.expected_connections as u64,
                );
                set(
                    "write_profile_captured_connections",
                    write_profile.captured_connections as u64,
                );
                set(
                    "write_profile_activation_errors",
                    write_profile.profile_activation_errors,
                );
                set(
                    "write_profile_capture_errors",
                    write_profile.profile_capture_errors,
                );
                set(
                    "write_profile_sampled_requests",
                    write_profile.sampled_requests,
                );
                set(
                    "write_profile_available",
                    u64::from(
                        write_profile.limit_per_connection != 0
                            && write_profile.expected_connections != 0
                            && write_profile.captured_connections
                                == write_profile.expected_connections
                            && write_profile.profile_activation_errors == 0
                            && write_profile.profile_capture_errors == 0,
                    ),
                );
                if write_profile.limit_per_connection != 0 {
                    visit_write_connection_profile(&mut set, write_profile.profile);
                }
            });
        }
    }
    pub fn try_visit_timing_metrics(&self, mut set: impl FnMut(&'static str, u64)) -> Option<()> {
        visit_stages!(set, self, write_gate_wait);
        let profile = &self.native_write_profile;
        set(
            "native_profile_limit",
            profile.limit.load(Ordering::Relaxed),
        );
        set(
            "native_profile_available",
            u64::from(matches!(&self.backend, Backend::Native(_))),
        );
        set(
            "native_profile_barrier_calls",
            profile.barrier.calls.load(Ordering::Relaxed),
        );
        set(
            "native_profile_barrier_errors",
            profile.barrier.errors.load(Ordering::Relaxed),
        );
        set(
            "native_profile_barrier_ns",
            profile.barrier.ns.load(Ordering::Relaxed),
        );
        set(
            "native_profile_file_calls",
            profile.file_calls.load(Ordering::Relaxed),
        );
        set(
            "native_profile_file_successes",
            profile.file_successes.load(Ordering::Relaxed),
        );
        set(
            "native_profile_file_ns",
            profile.file_ns.load(Ordering::Relaxed),
        );
        set(
            "native_profile_namespace_calls",
            profile.namespace_calls.load(Ordering::Relaxed),
        );
        set(
            "native_profile_namespace_successes",
            profile.namespace_successes.load(Ordering::Relaxed),
        );
        set(
            "native_profile_namespace_ns",
            profile.namespace_ns.load(Ordering::Relaxed),
        );
        set(
            "remote_timing_available",
            u64::from(self.native().is_none()),
        );
        set("read_profile_enabled", 0);
        set("read_profile_server_available", 0);
        set("read_profile_transport_progress_available", 0);
        set("read_profile_transport_cq_available", 0);
        set("scalar_read_ns_available", 0);
        set("read_index_ns_available", 0);
        set("write_checkpoint_diagnostics_available", 0);
        if let Backend::Remote(remote) = &self.backend {
            let read_timing_available = u64::from(remote.db.read_profile_enabled());
            set("scalar_read_ns_available", read_timing_available);
            set("read_index_ns_available", read_timing_available);
            set(
                "coordinator_outcome_resident_bytes",
                remote.db.try_metadata_history_bytes()? as u64,
            );
            visit_stages!(set, remote, scalar_read, bulk_read, write_commit);
            remote.profile.visit(&mut set);
            set(
                "iterator_latched_skips",
                remote.latched_skips.load(Ordering::Relaxed),
            );
            set(
                "snapshot_latch_deferrals",
                remote.db.snapshot_latch_deferrals(),
            );
            remote.db.try_visit_write_metrics(|snapshot| {
                let checkpoint = snapshot.checkpoint_diagnostics;
                set(
                    "write_checkpoint_diagnostics_available",
                    u64::from(checkpoint.available),
                );
                macro_rules! checkpoint_fields {
                    ($($field:ident),+ $(,)?) => {
                        $(set(
                            concat!("write_checkpoint_diagnostic_", stringify!($field)),
                            checkpoint.$field,
                        );)+
                    };
                }
                checkpoint_fields!(
                    records,
                    dropped_records,
                    pressure_wait_ns,
                    pressure_wait_count,
                    drain_ns,
                    drain_count,
                    existing_capture_wait_ns,
                    existing_capture_wait_count,
                    new_capture_wait_ns,
                    new_capture_wait_count,
                    staging_manifest_ns,
                    staging_manifest_count,
                    node_prefix_wait_ns,
                    node_prefix_wait_count,
                    index_prepare_ns,
                    index_prepare_count,
                    index_persist_ns,
                    index_persist_count,
                    metadata_install_ns,
                    metadata_install_count,
                    root_adoption_ns,
                    root_adoption_count,
                    owner_adoption_ns,
                    owner_adoption_count,
                    obsolete_cleanup_ns,
                    obsolete_cleanup_count,
                    tail_pressure_count,
                    known_identity_pressure_count,
                    partition_window_pressure_count,
                    index_reservation_pressure_count,
                    already_available_count,
                    after_drain_relief_count,
                    after_existing_capture_relief_count,
                    after_new_capture_relief_count,
                    capacity_error_count,
                    progress_error_count,
                );
                index_checkpoint::visit_coordinator_io(&mut set);
                let authority = snapshot.authority;
                set("write_authority_prepare_ns", authority.authority_prepare_ns);
                set("write_authority_admit_ns", authority.authority_admit_ns);
                set("write_authority_windows", authority.authority_windows);
                set("write_authority_batches", authority.authority_batches);
                set("write_storage_wait_ns", authority.storage_wait_ns);
                set("write_publication_ns", authority.publication_ns);
                set("write_dispatch_ns", authority.dispatch_ns);
                set(
                    "write_preparation_admission_ns",
                    authority.preparation_admission_ns,
                );
                set("write_checkpoint_ns", authority.checkpoint_ns);
                set("write_checkpoint_count", authority.checkpoint_count);
                set(
                    "write_background_checkpoint_suffix_batches",
                    authority.background_checkpoint_suffix_batches,
                );
                set(
                    "write_background_checkpoint_ns",
                    authority.background_checkpoint_ns,
                );
                set(
                    "write_durable_receipt_publication_blocked_batches",
                    authority.durable_receipt_publication_blocked_batches,
                );
                set(
                    "write_durable_receipt_publication_waiting_batches",
                    authority.durable_receipt_publication_waiting_batches,
                );
                set(
                    "write_durable_receipt_publication_waiting_peak",
                    authority.durable_receipt_publication_waiting_peak,
                );
                set(
                    "write_durable_receipt_publication_blocked_ns",
                    authority.durable_receipt_publication_blocked_ns,
                );
                set(
                    "write_out_of_prefix_published_batches",
                    authority.out_of_prefix_published_batches,
                );
                set(
                    "write_dependency_wait_batches",
                    authority.dependency_wait_batches,
                );
                let pipeline = snapshot.pipeline;
                set("write_append_requests", pipeline.append_requests);
                set("write_concurrent_admitted", pipeline.concurrent_admitted);
                set(
                    "write_concurrent_pending_peak",
                    pipeline.concurrent_pending_peak as u64,
                );
                set(
                    "write_concurrent_queued_peak",
                    pipeline.concurrent_queued_peak as u64,
                );
                set(
                    "write_concurrent_queue_bytes_peak",
                    pipeline.concurrent_queue_bytes_peak as u64,
                );
                let cache = snapshot.read_cache;
                set("cache_inserts", cache.inserts);
                set("cache_bypass_admission", cache.bypass_admission);
                set("cache_rejected_oversize", cache.rejected_oversize);
                set("cache_bypass_disabled", cache.bypass_disabled);
                set("cache_bypass_eviction_limit", cache.bypass_eviction_limit);
                set("cache_admitted_bytes", cache.admitted_bytes);
                set("cache_entries", cache.entries as u64);
                let write_profile = snapshot.write_profile;
                visit_checkpoint_profile(&mut set, &write_profile);
                set(
                    "write_profile_limit",
                    write_profile.limit_per_connection as u64,
                );
                set(
                    "write_profile_expected_connections",
                    write_profile.expected_connections as u64,
                );
                set(
                    "write_profile_captured_connections",
                    write_profile.captured_connections as u64,
                );
                set(
                    "write_profile_activation_errors",
                    write_profile.profile_activation_errors,
                );
                set(
                    "write_profile_capture_errors",
                    write_profile.profile_capture_errors,
                );
                set(
                    "write_profile_sampled_requests",
                    write_profile.sampled_requests,
                );
                set(
                    "write_profile_available",
                    u64::from(
                        write_profile.limit_per_connection != 0
                            && write_profile.expected_connections != 0
                            && write_profile.captured_connections
                                == write_profile.expected_connections
                            && write_profile.profile_activation_errors == 0
                            && write_profile.profile_capture_errors == 0,
                    ),
                );
                if write_profile.limit_per_connection != 0 {
                    visit_write_connection_profile(&mut set, write_profile.profile);
                }
            })?;
        }
        Some(())
    }
    pub fn native(&self) -> Option<&Arc<NativeDb>> {
        match &self.backend {
            Backend::Native(db) => Some(db),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn remote_for_test(&self) -> &ConcurrentRemoteDatabase {
        match &self.backend {
            Backend::Remote(remote) => &remote.db,
            Backend::Native(_) => panic!("test requires the remote backend"),
        }
    }
    #[cfg(test)]
    pub(crate) fn shared_write_gate_for_test(&self) -> parking_lot::RwLockReadGuard<'_, ()> {
        self.writes.read()
    }
    pub fn shape(&self) -> &KeyShape {
        &self.shape
    }
    pub fn shape_yaml(&self) -> Result<String> {
        self.shape.to_yaml().map_err(error)
    }
    pub fn set_native_durable(&self, enabled: bool) {
        self.durable_native.store(enabled, Ordering::Relaxed);
    }
    #[allow(
        clippy::expect_used,
        reason = "callers name a keyspace declared at open; the expect names the contract"
    )]
    pub fn ks(&self, name: &str) -> KeySpace {
        self.try_ks(name).expect("declared keyspace")
    }
    pub fn try_ks(&self, name: &str) -> Option<KeySpace> {
        match &self.backend {
            Backend::Native(db) => db.try_ks(name),
            Backend::Remote(_) => self
                .shape
                .iter_ks()
                .find(|ks| !ks.destroyed() && ks.name() == name)
                .map(|ks| ks.id()),
        }
    }
    pub fn validate_cached_read(&self) -> Result<()> {
        match &self.backend {
            Backend::Native(_) => Ok(()),
            Backend::Remote(remote) => remote.db.validate_cached_read().map_err(error),
        }
    }
    pub fn get(&self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>> {
        if let Some(call) = kv_capture::Call::begin(self.capture_id, ks, kv_capture::GET, 1) {
            let result = self.get(ks, key);
            kv_capture::read(&call, ks, key, &result, 0);
            call.finish(result.is_ok());
            return result;
        }
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.read_keys.fetch_add(1, Ordering::Relaxed);
        match &self.backend {
            Backend::Native(db) => db.get(ks, key),
            Backend::Remote(remote) => remote
                .scalar_read
                .run_if(remote.db.read_profile_enabled(), || {
                    remote.db.get(ks, key).map_err(error)
                }),
        }
    }
    pub fn traced_get(&self, ks: KeySpace, key: &[u8], caller_id: u64) -> Result<Option<Bytes>> {
        if let Some(call) = kv_capture::Call::begin(self.capture_id, ks, kv_capture::GET, 1) {
            let result = self.traced_get(ks, key, caller_id);
            kv_capture::read(&call, ks, key, &result, 0);
            call.finish(result.is_ok());
            return result;
        }
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.read_keys.fetch_add(1, Ordering::Relaxed);
        match &self.backend {
            Backend::Native(db) => db.get(ks, key),
            Backend::Remote(remote) => remote
                .scalar_read
                .run_if(remote.db.read_profile_enabled(), || {
                    remote.db.traced_get(ks, key, caller_id).map_err(error)
                }),
        }
    }
    pub fn exists(&self, ks: KeySpace, key: &[u8]) -> Result<bool> {
        if let Some(call) = kv_capture::Call::begin(self.capture_id, ks, kv_capture::EXISTS, 1) {
            let result = self.exists(ks, key);
            kv_capture::exists(&call, ks, key, &result, 0);
            call.finish(result.is_ok());
            return result;
        }
        match &self.backend {
            Backend::Native(db) => {
                self.read_calls.fetch_add(1, Ordering::Relaxed);
                self.read_keys.fetch_add(1, Ordering::Relaxed);
                db.exists(ks, key)
            }
            Backend::Remote(remote) if !self.shape.ks(ks).need_check_index_key() => {
                self.read_calls.fetch_add(1, Ordering::Relaxed);
                self.read_keys.fetch_add(1, Ordering::Relaxed);
                remote.db.contains(ks, key).map_err(error)
            }
            Backend::Remote(_) => self.get(ks, key).map(|v| v.is_some()),
        }
    }
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
    ) -> Vec<Result<Option<Bytes>>> {
        self.multi_get_inner(ks, keys, None)
    }

    pub fn traced_multi_get<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        caller_id: u64,
    ) -> Vec<Result<Option<Bytes>>> {
        self.multi_get_inner(ks, keys, Some(caller_id))
    }

    fn multi_get_inner<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        caller_id: Option<u64>,
    ) -> Vec<Result<Option<Bytes>>> {
        if let Some(call) =
            kv_capture::Call::begin(self.capture_id, ks, kv_capture::MULTI_GET, keys.len())
        {
            let result = self.multi_get_inner(ks, keys, caller_id);
            for (position, (key, value)) in keys.iter().zip(&result).enumerate() {
                kv_capture::read(&call, ks, key.as_ref(), value, position);
            }
            call.finish(result.iter().all(Result::is_ok));
            return result;
        }
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.read_keys
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        let remote = match &self.backend {
            Backend::Native(db) => {
                return keys.iter().map(|key| db.get(ks, key.as_ref())).collect();
            }
            Backend::Remote(remote) => remote,
        };
        let trace = caller_id.and_then(|id| remote.db.begin_read_trace(id, keys.len()));
        if keys.is_empty() {
            if let Some(trace) = trace {
                trace.event("api_finished");
                trace.finish(true);
            }
            return Vec::new();
        }
        // One call for any key count: the table pages internally at the value
        // reader's bounded window, so no separate large-read path exists.
        let values = remote.bulk_read.run(|| {
            if let Some(trace) = trace.as_ref() {
                remote.db.traced_get_many(ks, keys, Some(trace))
            } else {
                remote.db.get_many(ks, keys)
            }
        });
        let output: Vec<_> = match values {
            Ok(values) => values
                .into_iter()
                .map(|value| value.map_err(error))
                .collect(),
            Err(e) => keys.iter().map(|_| Err(error(&e))).collect(),
        };
        if let Some(trace) = trace {
            trace.event("api_finished");
            trace.finish(output.iter().all(Result::is_ok));
        }
        output
    }
    pub fn multi_exists(&self, ks: KeySpace, keys: &[Vec<u8>]) -> Vec<Result<bool>> {
        if let Some(call) =
            kv_capture::Call::begin(self.capture_id, ks, kv_capture::MULTI_EXISTS, keys.len())
        {
            let result = self.multi_exists(ks, keys);
            for (position, (key, value)) in keys.iter().zip(&result).enumerate() {
                kv_capture::exists(&call, ks, key, value, position);
            }
            call.finish(result.iter().all(Result::is_ok));
            return result;
        }
        if self.native().is_none() && self.shape.ks(ks).need_check_index_key() {
            return self
                .multi_get(ks, keys)
                .into_iter()
                .map(|value| value.map(|v| v.is_some()))
                .collect();
        }
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.read_keys
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        if keys.is_empty() {
            return Vec::new();
        }
        match &self.backend {
            Backend::Native(db) => keys.iter().map(|key| db.exists(ks, key)).collect(),
            Backend::Remote(remote) => keys
                .iter()
                .map(|key| remote.db.contains(ks, key).map_err(error))
                .collect(),
        }
    }
    pub fn insert(
        self: &Arc<Self>,
        ks: KeySpace,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<()> {
        if let Some(call) = kv_capture::Call::begin(self.capture_id, ks, kv_capture::PUT, 1) {
            let key = key.into();
            let value = value.into();
            call.member(ks, &key, Some(&value), 1, 0);
            let result = self.insert(ks, key, value);
            call.finish(result.is_ok());
            return result;
        }
        if let Backend::Native(db) = &self.backend {
            let _guard = self
                .durable_native
                .load(Ordering::Relaxed)
                .then(|| self.write_gate());
            db.insert(ks, key, value)?;
            return self.native_barrier(true);
        }
        let mut batch = self.write_batch();
        batch.write(ks, key, value);
        batch.commit()
    }
    pub fn remove(self: &Arc<Self>, ks: KeySpace, key: impl Into<Bytes>) -> Result<()> {
        if let Some(call) = kv_capture::Call::begin(self.capture_id, ks, kv_capture::DELETE, 1) {
            let key = key.into();
            call.member(ks, &key, None, 1, 0);
            let result = self.remove(ks, key);
            call.finish(result.is_ok());
            return result;
        }
        if let Backend::Native(db) = &self.backend {
            let _guard = self
                .durable_native
                .load(Ordering::Relaxed)
                .then(|| self.write_gate());
            db.remove(ks, key)?;
            return self.native_barrier(true);
        }
        let mut batch = self.write_batch();
        batch.delete(ks, key);
        batch.commit()
    }
    fn native_barrier(&self, foreground: bool) -> Result<()> {
        if self.durable_native.load(Ordering::Relaxed)
            && let Backend::Native(db) = &self.backend
        {
            if foreground && self.native_write_profile.select() {
                let mut profile = crate::db::ValueWalPersistenceProfile::default();
                let result = self
                    .native_write_profile
                    .barrier
                    .run(|| db.persist_value_wal_profiled_for_benchmark(&mut profile));
                self.native_write_profile.record(profile);
                result?;
            } else {
                db.persist_value_wal_for_benchmark()?;
            }
        }
        Ok(())
    }
    fn commit_remote(
        &self,
        mutations: Vec<Mutation>,
        sample: Option<WriteTimelineSample>,
    ) -> Result<()> {
        let Backend::Remote(remote) = &self.backend else {
            return Err(error("not a remote database"));
        };
        if mutations.is_empty() {
            return self.validate_cached_read();
        }
        let sequence = self.operations.fetch_add(1, Ordering::Relaxed);
        let mut operation = [0u8; 16];
        operation[..8].copy_from_slice(&remote.epoch.to_le_bytes());
        operation[8..].copy_from_slice(&sequence.to_le_bytes());
        if let Some(sample) = sample {
            remote.db.write_timeline.operation(sample, operation);
        }
        remote.write_commit.run(|| {
            remote
                .db
                .write_batch_traced(operation, mutations, sample)
                .map(|_| ())
                .map_err(error)
        })
    }
    pub fn write_batch(self: &Arc<Self>) -> WriteBatch {
        let native = self.native().map(|db| db.write_batch());
        WriteBatch {
            db: self.clone(),
            native,
            mutations: Vec::new(),
            payload_bytes: 0,
            capture: kv_capture::Batch::begin(self.capture_id),
        }
    }
    /// Advisory mapped-WAL eviction at a caller-enforced quiescent benchmark boundary.
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub fn evict_mapped_wal_for_benchmark(
        &self,
    ) -> Result<Vec<(u32, crate::read_probe::ReadEvidence)>> {
        let _guard = self.write_gate();
        match &self.backend {
            Backend::Remote(remote) => remote.db.evict_mapped_wal_for_benchmark().map_err(error),
            Backend::Native(_) => Err(error("mapped-WAL owner eviction requires a remote backend")),
        }
    }

    /// Native: persist the value WAL. Remote: the owners' durability barrier
    /// (`flush_durable`). Neither stores an index root: the hybrid index is
    /// persisted by its own byte-driven snapshot cadence or by
    /// `force_rebuild_control_region`, as native's `flush` leaves the control
    /// region alone.
    pub fn flush(&self) -> Result<()> {
        let _guard = self.write_gate();
        match &self.backend {
            Backend::Native(db) => db.persist_value_wal_for_benchmark(),
            Backend::Remote(remote) => remote.db.flush_durable().map_err(error),
        }
    }
    /// Native: rebuild the control region. Remote: the same forced snapshot
    /// over the coordinator's native index (every dirty cell flushed, one
    /// region stored). No write gate is taken.
    pub fn force_rebuild_control_region(&self) -> Result<()> {
        match &self.backend {
            Backend::Native(db) => db.force_rebuild_control_region().map(|_| ()),
            Backend::Remote(remote) => remote.db.force_rebuild_index().map(|_| ()).map_err(error),
        }
    }
    pub fn wait_for_background_threads_to_finish(self: Arc<Self>) -> Result<()> {
        let db = Arc::try_unwrap(self)
            .map_err(|_| error("Sui database still has live owners during shutdown"))?;
        match db.backend {
            Backend::Native(db) => {
                db.wait_for_background_threads_to_finish();
                Ok(())
            }
            Backend::Remote(remote) => remote.into_database()?.shutdown().map_err(error),
        }
    }
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        let iterator = match &self.backend {
            Backend::Native(db) => db.iterator(ks),
            Backend::Remote(remote) => {
                iter::live_iterator(&remote.db, Arc::clone(&self.scan_records), ks)
            }
        };
        kv_capture::CapturedIterator::wrap(DbIterator::Index(iterator), self.capture_id, ks)
    }
    pub fn checkpoint(self: &Arc<Self>) -> Checkpoint {
        let checkpoint = match &self.backend {
            Backend::Native(db) => Checkpoint::Native(db.checkpoint()),
            Backend::Remote(remote) => Checkpoint::Remote(iter::RemoteCheckpoint::new(
                Arc::clone(&remote.db),
                Arc::clone(&self.scan_records),
                Arc::clone(&remote.latched_skips),
            )),
        };
        if kv_capture::enabled() {
            Checkpoint::Captured(Box::new(checkpoint), self.capture_id)
        } else {
            checkpoint
        }
    }
    pub fn remote_counters(
        &self,
    ) -> Option<(
        super::remote_db::ReadCacheStats,
        super::storage::LocalDbStats,
        u64,
    )> {
        match &self.backend {
            Backend::Remote(remote) => {
                let db = &remote.db;
                Some((
                    db.read_cache_stats(),
                    db.stats(),
                    db.live_read_submission_count(),
                ))
            }
            _ => None,
        }
    }
    pub fn remote_keyspace_cache_counters(
        &self,
    ) -> Option<
        [super::remote_db::PerKeyspaceReadCacheStats; super::remote_db::KEYSPACE_COUNTER_COUNT],
    > {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.per_keyspace_read_cache_stats()),
            _ => None,
        }
    }
    pub fn try_remote_counters(
        &self,
    ) -> Option<
        Option<(
            super::remote_db::ReadCacheStats,
            super::storage::LocalDbStats,
            u64,
        )>,
    > {
        match &self.backend {
            Backend::Remote(remote) => Some(Some((
                remote.db.try_read_cache_stats()?,
                remote.db.try_stats()?,
                remote.db.live_read_submission_count(),
            ))),
            Backend::Native(_) => Some(None),
        }
    }
    pub fn try_remote_keyspace_cache_counters(
        &self,
    ) -> Option<
        Option<
            [super::remote_db::PerKeyspaceReadCacheStats; super::remote_db::KEYSPACE_COUNTER_COUNT],
        >,
    > {
        match &self.backend {
            Backend::Remote(remote) => Some(Some(remote.db.try_per_keyspace_read_cache_stats()?)),
            Backend::Native(_) => Some(None),
        }
    }
    /// Start relocation and return at once, as native does. A remote pass
    /// runs on the database's relocation worker without any write gate;
    /// requests made while one is queued or running coalesce into one pass.
    pub fn start_relocation(self: &Arc<Self>) -> Result<()> {
        let remote = match &self.backend {
            Backend::Native(db) => return db.start_relocation().map_err(error),
            Backend::Remote(remote) => remote,
        };
        let relocator = &remote.relocator;
        relocator.start().map_err(|cause| match cause {
            relocation::StartError::ShuttingDown => {
                error("relocation rejected: the database is shutting down")
            }
            relocation::StartError::WorkerStopped => {
                error("relocation worker stopped: a relocation filter panicked")
            }
        })
    }

    /// One named observed pass, waited for until `deadline` (the deadline
    /// drops only the waiter). Both backends return the native report shape
    /// (streams, ordered callbacks, copied survivors), so a native and a
    /// leased pass compare field by field. A remote pass also returns its
    /// [`RelocationReceipt`], the persistence evidence native has no analogue
    /// for; it is `None` on native.
    #[cfg(feature = "test_methods")]
    pub fn start_relocation_for_test(
        &self,
        deadline: Instant,
    ) -> std::result::Result<
        (crate::RelocationObservation, Option<RelocationReceipt>),
        crate::RelocationTestError,
    > {
        use super::remote_db::{CallbackDecision, RelocationGate};
        use crate::RelocationTestError;
        let remote = match &self.backend {
            Backend::Native(db) => {
                return db
                    .start_relocation_for_test(deadline)
                    .map(|observation| (observation, None));
            }
            Backend::Remote(remote) => remote,
        };
        let relocator = &remote.relocator;
        let report = relocator
            .run_for_test(deadline)
            .map_err(|cause| match cause {
                relocation::TestError::Busy => RelocationTestError::Busy,
                relocation::TestError::DeadlineExceeded => RelocationTestError::DeadlineExceeded,
                relocation::TestError::WorkerStopped => RelocationTestError::WorkerStopped,
                relocation::TestError::Pass(cause) => RelocationTestError::Native(error(cause)),
            })?;
        let receipt = RelocationReceipt {
            deferred: report.deferred,
            scan_cut: report.scan_cut,
            stored_target: report.stored_target,
            stored_forced: report.stored_forced,
            promotion_cut: report.promotion_cut,
            copied_survivors: report.copied_survivors,
            rejected_survivors: report.rejected_survivors,
        };
        let observation = crate::RelocationObservation {
            gate: match report.gate {
                RelocationGate::NoWork => crate::RelocationGate::NoWork,
                RelocationGate::DidWork => crate::RelocationGate::DidWork,
            },
            streams: report
                .streams
                .iter()
                .map(|stream| crate::StreamObservation {
                    stream_id: crate::RelocationStreamId {
                        partition: stream.partition,
                        fencing_generation: stream.fencing_generation,
                    },
                    processed_upper: stream.processed_upper,
                    terminal_position: stream.terminal_position,
                    rounded_target: stream.rounded_target,
                })
                .collect(),
            callbacks: report
                .callbacks
                .into_iter()
                .map(|callback| crate::RelocationCallback {
                    keyspace: callback.keyspace,
                    key: callback.key,
                    value: callback.value,
                    wal_position: callback.wal_position,
                    decision: match callback.decision {
                        CallbackDecision::Keep => crate::Decision::Keep,
                        CallbackDecision::Remove => crate::Decision::Remove,
                        CallbackDecision::Stop => crate::Decision::StopRelocation,
                    },
                })
                .collect(),
            copied_survivors: report.copied_survivors,
            witness: crate::RelocationWorkerWitness::Completed,
        };
        Ok((observation, Some(receipt)))
    }
    pub fn drop_cells_in_range(
        self: &Arc<Self>,
        ks: KeySpace,
        from: &[u8],
        to: &[u8],
    ) -> Result<()> {
        if let Some(call) =
            kv_capture::Call::begin(self.capture_id, ks, kv_capture::RANGE_DELETE, 0)
        {
            call.bound(from, false);
            call.bound(to, true);
            let result = self.drop_cells_in_range(ks, from, to);
            call.finish(result.is_ok());
            return result;
        }
        if let Backend::Native(db) = &self.backend {
            let _guard = self
                .durable_native
                .load(Ordering::Relaxed)
                .then(|| self.write_gate());
            db.drop_cells_in_range(ks, from, to)?;
            return self.native_barrier(false);
        }
        // One singleton DropCells batch under the shared gate; see
        // `drop_cells::drop_remote_cells`.
        self.drop_remote_cells(ks, from, to)
    }
}

pub struct WriteBatch {
    db: Arc<Db>,
    native: Option<crate::batch::WriteBatch>,
    mutations: Vec<Mutation>,
    payload_bytes: usize,
    capture: Option<kv_capture::Batch>,
}
impl WriteBatch {
    pub fn write(&mut self, ks: KeySpace, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        let key = key.into();
        let value = value.into();
        if let Some(capture) = &mut self.capture {
            capture.member(ks, &key, Some(&value));
        }
        self.payload_bytes = self
            .payload_bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        if let Some(batch) = &mut self.native {
            batch.write(ks, key, value);
        } else {
            self.mutations.push(Mutation::Put {
                keyspace: ks,
                key,
                value,
            });
        }
    }
    pub fn delete(&mut self, ks: KeySpace, key: impl Into<Bytes>) {
        let key = key.into();
        if let Some(capture) = &mut self.capture {
            capture.member(ks, &key, None);
        }
        self.payload_bytes = self.payload_bytes.saturating_add(key.len());
        if let Some(batch) = &mut self.native {
            batch.delete(ks, key);
        } else {
            self.mutations.push(Mutation::Delete { keyspace: ks, key });
        }
    }
    pub fn size_in_bytes(&self) -> usize {
        self.payload_bytes
    }
    pub fn commit(self) -> Result<()> {
        let call = kv_capture::Call::begin(self.db.capture_id, KeySpace(0), kv_capture::BATCH, 0);
        let result = if let Some(batch) = self.native {
            let _guard = self
                .db
                .durable_native
                .load(Ordering::Relaxed)
                .then(|| self.db.write_gate());
            batch.commit().and_then(|()| self.db.native_barrier(true))
        } else {
            let _guard = self.db.shared_write_gate();
            self.db.commit_remote(self.mutations, None)
        };
        if let Some(capture) = self.capture {
            capture.finish(result.is_ok());
        }
        if let Some(call) = call {
            call.finish(result.is_ok());
        }
        result
    }

    /// Commit with a caller-provided measurement identity, distinct from the operation ID.
    pub fn commit_traced(self, caller_sample_id: u64) -> Result<()> {
        let Backend::Remote(remote) = &self.db.backend else {
            return Err(error("write timeline requires a remote backend"));
        };
        let sample = remote.db.write_timeline.begin(caller_sample_id);
        let _guard = self.db.shared_write_gate();
        if let Some(sample) = sample {
            remote
                .db
                .write_timeline
                .event(sample, WriteTimelineStage::GateAcquired);
        }
        let result = self.db.commit_remote(self.mutations, sample);
        if let Some(sample) = sample {
            remote.db.write_timeline.complete(
                sample,
                result.as_ref().err().map(|error| format!("{error:?}")),
            );
        }
        result
    }
}

pub enum Checkpoint {
    Native(Arc<crate::checkpoint::DbCheckpoint>),
    Remote(iter::RemoteCheckpoint),
    Captured(Box<Checkpoint>, u64),
}
impl Checkpoint {
    pub fn iterator(self, ks: KeySpace) -> DbIterator {
        match self {
            Self::Native(checkpoint) => DbIterator::Index(checkpoint.iterator(ks)),
            Self::Remote(checkpoint) => DbIterator::Index(checkpoint.iterator(ks)),
            Self::Captured(checkpoint, db) => {
                kv_capture::CapturedIterator::wrap(checkpoint.iterator(ks), db, ks)
            }
        }
    }
}
pub enum DbIterator {
    /// The native iterator over either backend: a native database, or the
    /// coordinator index of a remote one (see `iter`).
    Index(crate::iterators::db_iterator::DbIterator),
    Captured(Box<kv_capture::CapturedIterator>),
}
impl DbIterator {
    pub fn set_lower_bound(&mut self, bound: impl Into<Bytes>) {
        match self {
            Self::Index(iter) => iter.set_lower_bound(bound),
            Self::Captured(iter) => {
                let bound = bound.into();
                iter.bound(&bound, false);
                iter.inner.set_lower_bound(bound);
            }
        }
    }
    pub fn set_upper_bound(&mut self, bound: impl Into<Bytes>) {
        match self {
            Self::Index(iter) => iter.set_upper_bound(bound),
            Self::Captured(iter) => {
                let bound = bound.into();
                iter.bound(&bound, true);
                iter.inner.set_upper_bound(bound);
            }
        }
    }
    pub fn reverse(&mut self) {
        match self {
            Self::Index(iter) => iter.reverse(),
            Self::Captured(iter) => {
                iter.reverse();
                iter.inner.reverse();
            }
        }
    }
}
impl Iterator for DbIterator {
    type Item = Result<(Bytes, Bytes)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Index(iter) => iter.next(),
            Self::Captured(iter) => iter.next(),
        }
    }
}

#[cfg(test)]
mod timing_tests {
    use super::*;

    #[test]
    fn acquisition_span_completes_before_guard_is_released() {
        let stage = Stage::default();
        let mutex = parking_lot::Mutex::new(());
        let guard = stage.measure(|| mutex.lock());
        assert_eq!(stage.calls.load(Ordering::Relaxed), 1);
        assert_eq!(stage.errors.load(Ordering::Relaxed), 0);
        assert!(mutex.try_lock().is_none());
        drop(guard);
        assert_eq!(stage.calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn failed_and_unwinding_stage_spans_are_not_lost() {
        let stage = Stage::default();
        assert_eq!(stage.run(|| Err::<(), _>("rejected")), Err("rejected"));
        assert_eq!(stage.calls.load(Ordering::Relaxed), 1);
        assert_eq!(stage.errors.load(Ordering::Relaxed), 1);
        let unwind = std::panic::catch_unwind(|| stage.measure(|| panic!("failed operation")));
        assert!(unwind.is_err());
        assert_eq!(stage.calls.load(Ordering::Relaxed), 2);
        assert_eq!(stage.errors.load(Ordering::Relaxed), 2);
        stage.run(|| Ok::<_, ()>(())).unwrap();
        assert_eq!(stage.calls.load(Ordering::Relaxed), 3);
        assert_eq!(stage.errors.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn native_profile_selection_is_bounded_across_callers_and_clean_at_zero() {
        let profile = NativeWriteProfile::default();
        assert!(!profile.select());
        assert_eq!(profile.barrier.calls.load(Ordering::Relaxed), 0);
        profile.remaining.store(7, Ordering::Relaxed);
        let selected = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..16 {
                let (profile, selected) = (&profile, &selected);
                scope.spawn(move || {
                    if profile.select() {
                        selected.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        assert_eq!(selected.load(Ordering::Relaxed), 7);
        assert_eq!(profile.remaining.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn native_foreground_profile_excludes_maintenance_and_stops_at_limit() {
        let temp = tempdir::TempDir::new("sui-native-profile").unwrap();
        let shape = KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1));
        let mut config = Config::small();
        config.snapshot_written_bytes = u64::MAX;
        let db = Db::open_native(
            temp.path(),
            shape,
            Arc::new(config),
            Metrics::new_in_enabled(&prometheus::Registry::new(), false),
        )
        .unwrap();
        db.set_native_durable(true);
        let ks = db.ks("root");
        db.insert(ks, 0u32.to_be_bytes().to_vec(), vec![1]).unwrap();
        assert_eq!(
            db.native_write_profile
                .barrier
                .calls
                .load(Ordering::Relaxed),
            0
        );
        db.set_native_write_profile_limit(2);
        db.native_barrier(false).unwrap();
        assert_eq!(db.native_write_profile.remaining.load(Ordering::Relaxed), 2);
        for key in 1u32..4 {
            db.insert(ks, key.to_be_bytes().to_vec(), vec![key as u8])
                .unwrap();
        }
        let mut counters = std::collections::BTreeMap::new();
        db.visit_timing_metrics(|name, value| {
            counters.insert(name, value);
        });
        assert_eq!(counters["native_profile_limit"], 2);
        assert_eq!(counters["native_profile_available"], 1);
        assert_eq!(counters["native_profile_barrier_calls"], 2);
        assert_eq!(counters["native_profile_barrier_errors"], 0);
        assert!(counters["native_profile_file_calls"] >= 2);
        assert_eq!(
            counters["native_profile_file_calls"],
            counters["native_profile_file_successes"]
        );
        db.wait_for_background_threads_to_finish().unwrap();
    }
}
