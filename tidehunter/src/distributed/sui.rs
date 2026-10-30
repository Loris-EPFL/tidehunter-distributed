// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit Sui adapter. Committed reads do not borrow the write-progress owner.
//! Ordinary remote writes overlap; only maintenance requires write quiescence.
//!
//! Remote pruning follows the native WAL filter's database-wide stop frontier,
//! expressed in logical batches (partition offsets are incomparable). Historical
//! Stop rows protect their whole atomic batch. Durable tombstones precede history
//! retirement; pinned iterator/checkpoint cuts protect their immutable backing.
//! Only wholly unreferenced WAL files are reclaimed by the authority lifecycle.
use super::codec::{CompleteBatch, HEADER_LEN, Mutation};
use super::native_index::{IndexReadCut, IndexReadPin};
use super::remote_db::{
    ConcurrentRemoteDatabase, PendingScanPage, RemoteDatabase, RemoteDatabaseConfig,
};
use super::types::{BatchVersion, MutationVersion, ValueRecordReference};
use crate::config::Config;
use crate::db::{Db as NativeDb, DbError};
use crate::key_shape::{KeyShape, KeySpace};
use crate::metrics::Metrics;
use crate::minibytes::Bytes;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

pub type Result<T, E = DbError> = std::result::Result<T, E>;
fn error(e: impl std::fmt::Display) -> DbError {
    DbError::Io(std::io::Error::other(e.to_string()))
}

/// Cumulative completed spans, including failed calls and unwinding. Nanoseconds
/// are sums across callers, not wall time or means. Recording allocates nothing.
/// Errors mean Err/unwind at that boundary: detached waits return completion
/// handles, so their transport/authority errors surface in read_finish instead.
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

fn visit_write_connection_profile(
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
        read_response_recheck_deferrals,
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
        ram_payload_verify_ns,
        ram_payload_verify_calls,
        ram_payload_verified_bytes,
        requests,
        server_handler_ns,
        server_inventory_wait_ns,
        server_authority_lookup_ns,
        server_record_read_ns,
        server_response_recheck_wait_ns,
        server_response_recheck_ns,
        server_response_recheck_samples,
        server_response_recheck_deferrals,
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
        append_history_ns,
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
    writes: RwLock<()>,
    write_gate_wait: Stage,
    durable_native: AtomicBool,
    native_write_profile: NativeWriteProfile,
    operations: AtomicU64,
    pub read_calls: AtomicU64,
    pub read_keys: AtomicU64,
    pub scan_records: AtomicU64,
}
// One adapter is allocated per database, not per read. Keeping remote state
// inline avoids another allocation and pointer indirection on remote operations.
#[allow(clippy::large_enum_variant)]
enum Backend {
    Native(Arc<NativeDb>),
    Remote(Remote),
}
struct Remote {
    db: ConcurrentRemoteDatabase,
    epoch: u64,
    timeout: std::time::Duration,
    batch_limits: super::codec::BatchLimits,
    scalar_read: Stage,
    bulk_read: Stage,
    write_commit: Stage,
    read_submit: Stage,
    read_admission_wait: Stage,
    read_detached_wait: Stage,
    read_finish: Stage,
    profile: super::remote_db::ReadProfileMonitor,
}
impl Remote {
    fn submit<T>(
        &self,
        submit: impl FnOnce(
            &ConcurrentRemoteDatabase,
        ) -> std::result::Result<T, super::local_db::LocalDbError>,
    ) -> std::result::Result<T, super::local_db::LocalDbError> {
        self.read_submit.run(|| submit(&self.db))
    }
    fn admit<T>(
        &self,
        mut submit: impl FnMut(
            &ConcurrentRemoteDatabase,
        ) -> std::result::Result<T, super::local_db::LocalDbError>,
    ) -> Result<T> {
        let deadline = Instant::now() + self.timeout;
        loop {
            // Capture before admission so completion between a failed attempt
            // and the wait cannot be lost. The read service owns credit wakeups.
            let generation = self.db.read_admission_generation();
            match self.submit(&mut submit) {
                Ok(value) => return Ok(value),
                Err(e) if RemoteDatabase::read_backpressured(&e) => {
                    self.read_admission_wait.run(|| {
                        self.db
                            .wait_read_admission(generation, deadline)
                            .map_err(error)
                    })?;
                }
                Err(e) => return Err(error(e)),
            }
        }
    }
    fn record(
        &self,
        ks: KeySpace,
        reduced_key: &[u8],
        reference: ValueRecordReference,
        pin: &IndexReadPin,
    ) -> Result<Mutation> {
        let pending = self.admit(|db| db.submit_record_at(reference, pin.clone()))?;
        let completed = self.read_detached_wait.measure(|| pending.wait());
        self.read_finish.run(|| {
            self.db
                .finish_record(completed, ks, reduced_key)
                .map_err(error)
        })
    }
}
impl Db {
    pub fn drop_db(path: &Path) -> std::io::Result<()> {
        if path.join("tcp-database-authority-v1").exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "cannot destroy only a sharded-wal coordinator: retained shard history requires coordinated retirement",
            ));
        }
        NativeDb::drop_db(path)
    }
    pub fn open_native(
        path: &Path,
        shape: KeyShape,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>> {
        let db = NativeDb::open(path, shape, config, metrics)?;
        let shape = NativeDb::load_key_shape(path)?;
        Ok(Self::new(Backend::Native(db), shape))
    }
    pub fn open_remote(
        path: &Path,
        shape: KeyShape,
        config: RemoteDatabaseConfig,
    ) -> Result<Arc<Self>> {
        let timeout = config.timeout;
        let batch_limits = config.storage.batch_limits;
        let metrics = Metrics::new_in_enabled(&prometheus::Registry::new(), false);
        let mut db = RemoteDatabase::open(path, &shape, config, metrics).map_err(error)?;
        db.configure_sui_read_cache();
        let epoch = db.epoch();
        let profile = db.read_profile_monitor();
        let db = ConcurrentRemoteDatabase::new(db).map_err(error)?;
        Ok(Self::new(
            Backend::Remote(Remote {
                db,
                epoch,
                timeout,
                batch_limits,
                scalar_read: Stage::default(),
                bulk_read: Stage::default(),
                write_commit: Stage::default(),
                read_submit: Stage::default(),
                read_admission_wait: Stage::default(),
                read_detached_wait: Stage::default(),
                read_finish: Stage::default(),
                profile,
            }),
            shape,
        ))
    }
    fn new(backend: Backend, shape: KeyShape) -> Arc<Self> {
        Arc::new(Self {
            backend,
            shape,
            writes: RwLock::new(()),
            write_gate_wait: Stage::default(),
            durable_native: AtomicBool::new(false),
            native_write_profile: NativeWriteProfile::default(),
            operations: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            read_keys: AtomicU64::new(0),
            scan_records: AtomicU64::new(0),
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
    /// Set before serving; zero disables extra actor/protocol diagnostic clocks.
    pub fn set_read_profile_limit(&self, limit: u64) {
        if let Backend::Remote(remote) = &self.backend {
            remote.db.set_read_profile_limit(limit);
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
            let index = remote.db.index_stats();
            set(
                "index_resident_metadata_bytes",
                index.charged_metadata_bytes as u64,
            );
            set(
                "index_transient_metadata_bytes",
                index.transient_metadata_bytes as u64,
            );
            set(
                "index_history_bytes",
                remote.db.index_history_bytes() as u64,
            );
            set("index_retained_versions", index.retained_versions as u64);
            set(
                "coordinator_outcome_resident_bytes",
                remote.db.metadata_history_bytes() as u64,
            );
            let floor = remote.db.retention_floor();
            let (at, epoch, sequence, operation_index) = match floor {
                IndexReadCut::At(version) => (
                    1,
                    version.batch.epoch,
                    version.batch.sequence,
                    u64::from(version.operation_index),
                ),
                _ => (0, 0, 0, 0),
            };
            set("retention_floor_at", at);
            set("retention_floor_epoch", epoch);
            set("retention_floor_sequence", sequence);
            set("retention_floor_operation_index", operation_index);
            visit_stages!(
                set,
                remote,
                scalar_read,
                bulk_read,
                write_commit,
                read_submit,
                read_admission_wait,
                read_detached_wait,
                read_finish
            );
            remote.profile.visit(&mut set);
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
                    receipt_history_ns,
                    receipt_history_count,
                    receipt_history_sync_ns,
                    receipt_history_sync_count,
                    node_prefix_wait_ns,
                    node_prefix_wait_count,
                    index_prepare_ns,
                    index_prepare_count,
                    index_persist_ns,
                    index_persist_count,
                    metadata_install_ns,
                    metadata_install_count,
                    owner_adoption_ns,
                    owner_adoption_count,
                    obsolete_cleanup_ns,
                    obsolete_cleanup_count,
                    history_io_checkpoint_count,
                    history_stream_opens,
                    history_previous_slot_reads,
                    history_receipt_write_ops,
                    history_receipt_write_bytes,
                    history_buffer_flushes,
                    history_node_write_ops,
                    history_node_write_bytes,
                    history_sync_operations,
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
                let authority = snapshot.authority;
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
                    "write_foreground_checkpoint_ns",
                    authority.foreground_checkpoint_ns,
                );
                set(
                    "write_foreground_checkpoint_count",
                    authority.foreground_checkpoint_count,
                );
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
                set("write_direct_appends", pipeline.direct_appends);
                set(
                    "write_peak_inflight_nodes",
                    pipeline.peak_inflight_nodes as u64,
                );
                set("write_durable_batches", pipeline.durable_batches);
                set("write_concurrent_admitted", pipeline.concurrent_admitted);
                set("write_concurrent_completed", pipeline.concurrent_completed);
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
                set("cache_rejected_mismatch", cache.rejected_mismatch);
                set("cache_bypass_disabled", cache.bypass_disabled);
                set("cache_bypass_eviction_limit", cache.bypass_eviction_limit);
                set("cache_admitted_bytes", cache.admitted_bytes);
                set("cache_entries", cache.entries as u64);
                let write_profile = snapshot.write_profile;
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
            let (index, history_bytes, floor) = remote.db.try_index_metrics()?;
            set(
                "index_resident_metadata_bytes",
                index.charged_metadata_bytes as u64,
            );
            set(
                "index_transient_metadata_bytes",
                index.transient_metadata_bytes as u64,
            );
            set("index_history_bytes", history_bytes as u64);
            set("index_retained_versions", index.retained_versions as u64);
            set(
                "coordinator_outcome_resident_bytes",
                remote.db.try_metadata_history_bytes()? as u64,
            );
            let (at, epoch, sequence, operation_index) = match floor {
                IndexReadCut::At(version) => (
                    1,
                    version.batch.epoch,
                    version.batch.sequence,
                    u64::from(version.operation_index),
                ),
                _ => (0, 0, 0, 0),
            };
            set("retention_floor_at", at);
            set("retention_floor_epoch", epoch);
            set("retention_floor_sequence", sequence);
            set("retention_floor_operation_index", operation_index);
            visit_stages!(
                set,
                remote,
                scalar_read,
                bulk_read,
                write_commit,
                read_submit,
                read_admission_wait,
                read_detached_wait,
                read_finish
            );
            remote.profile.visit(&mut set);
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
                    receipt_history_ns,
                    receipt_history_count,
                    receipt_history_sync_ns,
                    receipt_history_sync_count,
                    node_prefix_wait_ns,
                    node_prefix_wait_count,
                    index_prepare_ns,
                    index_prepare_count,
                    index_persist_ns,
                    index_persist_count,
                    metadata_install_ns,
                    metadata_install_count,
                    owner_adoption_ns,
                    owner_adoption_count,
                    obsolete_cleanup_ns,
                    obsolete_cleanup_count,
                    history_io_checkpoint_count,
                    history_stream_opens,
                    history_previous_slot_reads,
                    history_receipt_write_ops,
                    history_receipt_write_bytes,
                    history_buffer_flushes,
                    history_node_write_ops,
                    history_node_write_bytes,
                    history_sync_operations,
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
                let authority = snapshot.authority;
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
                    "write_foreground_checkpoint_ns",
                    authority.foreground_checkpoint_ns,
                );
                set(
                    "write_foreground_checkpoint_count",
                    authority.foreground_checkpoint_count,
                );
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
                set("write_direct_appends", pipeline.direct_appends);
                set(
                    "write_peak_inflight_nodes",
                    pipeline.peak_inflight_nodes as u64,
                );
                set("write_durable_batches", pipeline.durable_batches);
                set("write_concurrent_admitted", pipeline.concurrent_admitted);
                set("write_concurrent_completed", pipeline.concurrent_completed);
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
                set("cache_rejected_mismatch", cache.rejected_mismatch);
                set("cache_bypass_disabled", cache.bypass_disabled);
                set("cache_bypass_eviction_limit", cache.bypass_eviction_limit);
                set("cache_admitted_bytes", cache.admitted_bytes);
                set("cache_entries", cache.entries as u64);
                let write_profile = snapshot.write_profile;
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
    pub fn shape(&self) -> &KeyShape {
        &self.shape
    }
    pub fn shape_yaml(&self) -> Result<String> {
        self.shape.to_yaml().map_err(error)
    }
    pub fn set_native_durable(&self, enabled: bool) {
        self.durable_native.store(enabled, Ordering::Relaxed);
    }
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
    pub fn exists(&self, ks: KeySpace, key: &[u8]) -> Result<bool> {
        match &self.backend {
            Backend::Native(db) => {
                self.read_calls.fetch_add(1, Ordering::Relaxed);
                self.read_keys.fetch_add(1, Ordering::Relaxed);
                db.exists(ks, key)
            }
            Backend::Remote(remote) if !self.shape.ks(ks).need_check_index_key() => {
                self.read_calls.fetch_add(1, Ordering::Relaxed);
                self.read_keys.fetch_add(1, Ordering::Relaxed);
                remote
                    .db
                    .index_reference(ks, key, IndexReadCut::Latest)
                    .map(|v| v.is_some())
                    .map_err(error)
            }
            Backend::Remote(_) => self.get(ks, key).map(|v| v.is_some()),
        }
    }
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
    ) -> Vec<Result<Option<Bytes>>> {
        self.read_calls.fetch_add(1, Ordering::Relaxed);
        self.read_keys
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        if keys.is_empty() {
            return Vec::new();
        }
        let Backend::Remote(remote) = &self.backend else {
            let db = self.native().expect("native backend");
            return keys.iter().map(|key| db.get(ks, key.as_ref())).collect();
        };
        let (cut, _pin) = if keys.len() > 256 {
            match remote.db.protected_published_cut() {
                Ok((cut, pin)) => (cut, Some(pin)),
                Err(e) => return keys.iter().map(|_| Err(error(&e))).collect(),
            }
        } else {
            // One bounded window resolves under one index guard and pins only
            // misses. Cache-only bulk reads allocate no retention pin.
            (IndexReadCut::Latest, None)
        };
        let mut output = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(256) {
            match remote
                .bulk_read
                .run(|| remote.db.get_many_cut(ks, chunk, cut.clone()))
            {
                Ok(values) => output.extend(values.into_iter().map(|value| value.map_err(error))),
                Err(e) => output.extend(chunk.iter().map(|_| Err(error(&e)))),
            }
        }
        output
    }
    pub fn multi_exists(&self, ks: KeySpace, keys: &[Vec<u8>]) -> Vec<Result<bool>> {
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
            Backend::Remote(remote) => {
                let db = &remote.db;
                let (cut, _pin) = match db.protected_published_cut() {
                    Ok(protected) => protected,
                    Err(e) => return keys.iter().map(|_| Err(error(&e))).collect(),
                };
                keys.iter()
                    .map(|key| {
                        db.index_reference(ks, key, cut.clone())
                            .map(|v| v.is_some())
                            .map_err(error)
                    })
                    .collect()
            }
        }
    }
    pub fn insert(
        self: &Arc<Self>,
        ks: KeySpace,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<()> {
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
    fn commit_remote(&self, mutations: Vec<Mutation>) -> Result<()> {
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
        remote.write_commit.run(|| {
            remote
                .db
                .write_batch(operation, mutations)
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
        }
    }
    pub fn flush(&self) -> Result<()> {
        let _guard = self.write_gate();
        match &self.backend {
            Backend::Native(db) => db.persist_value_wal_for_benchmark(),
            Backend::Remote(remote) => remote.db.flush_index().map_err(error),
        }
    }
    pub fn force_rebuild_control_region(&self) -> Result<()> {
        match &self.backend {
            Backend::Native(db) => db.force_rebuild_control_region().map(|_| ()),
            Backend::Remote(_) => self.flush(),
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
            Backend::Remote(remote) => remote.db.shutdown().map_err(error),
        }
    }
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        match &self.backend {
            Backend::Native(db) => DbIterator::Native(db.iterator(ks)),
            Backend::Remote(remote) => DbIterator::Remote(RemoteIterator::new(
                self.clone(),
                ks,
                remote.db.protected_published_cut().map_err(error),
            )),
        }
    }
    pub fn checkpoint(self: &Arc<Self>) -> Checkpoint {
        match &self.backend {
            Backend::Native(db) => Checkpoint::Native(db.checkpoint()),
            Backend::Remote(remote) => Checkpoint::Remote(
                self.clone(),
                remote.db.protected_published_cut().map_err(error),
            ),
        }
    }
    /// Obtain remote read evidence for quiescent measurement boundaries.
    /// Owner samples are sequential and are not a globally consistent cut.
    pub fn read_evidence_monitor(&self) -> Option<super::remote_db::ReadEvidenceMonitor> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.read_evidence_monitor()),
            Backend::Native(_) => None,
        }
    }

    pub fn operational_read_io_monitor(
        &self,
    ) -> Option<super::remote_db::OperationalReadIoMonitor> {
        match &self.backend {
            Backend::Remote(remote) => Some(remote.db.operational_read_io_monitor()),
            Backend::Native(_) => None,
        }
    }
    pub fn remote_counters(
        &self,
    ) -> Option<(
        super::remote_db::ReadCacheStats,
        super::local_db::LocalDbStats,
        u64,
    )> {
        match &self.backend {
            Backend::Remote(remote) => {
                let db = &remote.db;
                Some((db.read_cache_stats(), db.stats(), db.read_rpc_count()))
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
            super::local_db::LocalDbStats,
            u64,
        )>,
    > {
        match &self.backend {
            Backend::Remote(remote) => Some(Some((
                remote.db.try_read_cache_stats()?,
                remote.db.try_stats()?,
                remote.db.read_rpc_count(),
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
    pub fn start_relocation(self: &Arc<Self>) -> Result<()> {
        if let Backend::Native(db) = &self.backend {
            return db.start_relocation().map_err(error);
        }
        // Tombstone current Remove rows first, then durably advance only the
        // unprotected prefix. A Stop protects its entire historical batch.
        let _guard = self.write_gate();
        let Backend::Remote(remote) = &self.backend else {
            unreachable!()
        };
        let (cut, pin) = remote.db.protected_published_cut().map_err(error)?;
        let mut cutoff: Option<BatchVersion> = None;
        // Native wal_based_relocation scans retained WAL records, not just live
        // keys. Retained index history includes overwritten Stop puts, including
        // checkpoint-backed versions above the supported-history floor.
        // Visit order is irrelevant to the minimum logical stop batch; do not
        // compare member offsets or admit any pruning before this pass completes.
        for descriptor in self.shape.iter_ks() {
            if descriptor.destroyed() {
                continue;
            }
            let Some(filter) = descriptor.relocation_filter() else {
                continue;
            };
            let mut history = remote
                .db
                .relocation_history(descriptor.id(), cut.clone())
                .map_err(error)?;
            loop {
                let row = remote
                    .db
                    .next_relocation_record(&mut history)
                    .map_err(error)?;
                let Some((key, reference)) = row else { break };
                if cutoff.is_some_and(|stop| reference.batch_version >= stop) {
                    continue;
                }
                let Mutation::Put { key, value, .. } =
                    remote.record(descriptor.id(), &key, reference, &pin)?
                else {
                    return Err(error("relocation put history resolved to a tombstone"));
                };
                if matches!(filter(&key, &value), crate::Decision::StopRelocation) {
                    cutoff = Some(reference.batch_version);
                }
            }
        }
        for descriptor in self.shape.iter_ks() {
            if descriptor.destroyed() {
                continue;
            }
            if let Some(compactor) = descriptor.compactor() {
                let Backend::Remote(remote) = &self.backend else {
                    unreachable!()
                };
                let cut = remote.db.published_cut().map_err(error)?;
                let mut after: Option<Bytes> = None;
                let mut cell = None;
                let mut rows = Vec::new();
                loop {
                    let page = remote
                        .db
                        .index_page(descriptor.id(), after.as_deref(), false, cut.clone())
                        .map_err(error)?;
                    if page.is_empty() {
                        break;
                    }
                    for (key, reference) in page {
                        let next_cell = descriptor.cell_id(&key);
                        if cell.as_ref().is_some_and(|cell| cell != &next_cell) {
                            self.compact_cell(descriptor.id(), compactor, &rows)?;
                            rows.clear();
                        }
                        cell = Some(next_cell);
                        after = Some(key.clone());
                        rows.push((key, reference));
                    }
                }
                self.compact_cell(descriptor.id(), compactor, &rows)?;
            }
            let Some(filter) = descriptor.relocation_filter() else {
                continue;
            };
            let cut = remote.db.published_cut().map_err(error)?;
            let mut after = None;
            let mut deletes =
                Vec::with_capacity((remote.batch_limits.max_operations as usize).min(256));
            let mut encoded_bytes = HEADER_LEN;
            loop {
                let page = remote
                    .db
                    .index_page(descriptor.id(), after.as_deref(), false, cut.clone())
                    .map_err(error)?;
                if page.is_empty() {
                    break;
                }
                for (key, reference) in page {
                    after = Some(key.clone());
                    if cutoff.is_some_and(|stop| reference.batch_version >= stop) {
                        continue;
                    }
                    let Mutation::Put { key, value, .. } =
                        remote.record(descriptor.id(), &key, reference, &pin)?
                    else {
                        return Err(error("live relocation row resolved to a tombstone"));
                    };
                    if matches!(filter(&key, &value), crate::Decision::Remove) {
                        self.push_pruning_delete(
                            &mut deletes,
                            &mut encoded_bytes,
                            Mutation::Delete {
                                keyspace: descriptor.id(),
                                key,
                            },
                        )?;
                    }
                }
            }
            if !deletes.is_empty() {
                self.commit_remote(deletes)?;
            }
        }
        drop(pin);
        let floor = match cutoff {
            None => cut,
            Some(stop) => {
                let before_stop = if stop.sequence != 0 {
                    IndexReadCut::At(MutationVersion {
                        batch: BatchVersion {
                            sequence: stop.sequence - 1,
                            ..stop
                        },
                        operation_index: u32::MAX,
                    })
                } else if stop.epoch != 0 {
                    IndexReadCut::At(MutationVersion {
                        batch: BatchVersion {
                            epoch: stop.epoch - 1,
                            sequence: u64::MAX,
                        },
                        operation_index: u32::MAX,
                    })
                } else {
                    IndexReadCut::BeforeAll
                };
                match (cut, before_stop) {
                    (IndexReadCut::At(cut), IndexReadCut::At(before)) => {
                        IndexReadCut::At(cut.min(before))
                    }
                    _ => IndexReadCut::BeforeAll,
                }
            }
        };
        remote.db.retire_history_before(floor).map_err(error)?;
        Ok(())
    }
    fn push_pruning_delete(
        &self,
        deletes: &mut Vec<Mutation>,
        encoded_bytes: &mut usize,
        mutation: Mutation,
    ) -> Result<()> {
        let Backend::Remote(remote) = &self.backend else {
            unreachable!()
        };
        let limits = remote.batch_limits;
        let member_bytes = CompleteBatch::encoded_len_for_mutations(
            &self.shape,
            std::slice::from_ref(&mutation),
            limits,
        )
        .map_err(error)?
            - HEADER_LEN;
        let capacity = (limits.max_operations as usize).min(256);
        if !deletes.is_empty()
            && (deletes.len() == capacity
                || *encoded_bytes + member_bytes > limits.max_encoded_bytes)
        {
            self.commit_remote(std::mem::replace(deletes, Vec::with_capacity(capacity)))?;
            *encoded_bytes = HEADER_LEN;
        }
        *encoded_bytes += member_bytes;
        deletes.push(mutation);
        Ok(())
    }

    fn compact_cell(
        &self,
        ks: KeySpace,
        compactor: &crate::key_shape::Compactor,
        rows: &[(Bytes, ValueRecordReference)],
    ) -> Result<()> {
        let Backend::Remote(remote) = &self.backend else {
            unreachable!()
        };
        let (_, pin) = remote.db.protected_published_cut().map_err(error)?;
        let retain = compactor(&mut rows.iter().map(|(key, _)| key));
        let mut deletes =
            Vec::with_capacity((remote.batch_limits.max_operations as usize).min(256));
        let mut encoded_bytes = HEADER_LEN;
        for (key, reference) in rows {
            if !retain.contains(key) {
                let Mutation::Put { key, .. } = remote.record(ks, key, *reference, &pin)? else {
                    return Err(error("live compactor row resolved to a tombstone"));
                };
                self.push_pruning_delete(
                    &mut deletes,
                    &mut encoded_bytes,
                    Mutation::Delete { keyspace: ks, key },
                )?;
            }
        }
        if !deletes.is_empty() {
            self.commit_remote(deletes)?;
        }
        Ok(())
    }
    pub fn drop_cells_in_range(
        self: &Arc<Self>,
        ks: KeySpace,
        from: &[u8],
        to: &[u8],
    ) -> Result<()> {
        if let Backend::Native(db) = &self.backend {
            let _guard = self
                .durable_native
                .load(Ordering::Relaxed)
                .then(|| self.write_gate());
            db.drop_cells_in_range(ks, from, to)?;
            return self.native_barrier(false);
        }
        let descriptor = self.shape.ks(ks);
        let reduced_from = descriptor.reduce_key(from);
        let reduced_to = descriptor.reduce_key(to);
        descriptor.map_key_range_to_cell_range(&reduced_from, &reduced_to);
        let _guard = self.write_gate();
        let Backend::Remote(remote) = &self.backend else {
            unreachable!()
        };
        let mut retained_bytes = 128usize;
        let (cut, pin) = remote.db.protected_published_cut().map_err(error)?;
        // The validated endpoints denote complete reduced cells. A full-key
        // iterator bound would wrongly exclude colliding keys whose suffix sorts
        // below `from`; include the reduced lower key and then page exclusively.
        let mut after = Bytes::copy_from_slice(&reduced_from);
        let mut page = Vec::new();
        if let Some(reference) = remote
            .db
            .index_reference(ks, from, cut.clone())
            .map_err(error)?
        {
            page.push((after.clone(), reference));
        }
        let mut deletes = Vec::with_capacity(256);
        'pages: loop {
            for (reduced, reference) in page {
                if reduced.as_ref() > reduced_to.as_ref() {
                    break 'pages;
                }
                retained_bytes =
                    retained_bytes.saturating_add(reference.descriptor.key_len as usize + 64);
                if deletes.len() >= remote.batch_limits.max_operations as usize
                    || retained_bytes > remote.batch_limits.max_encoded_bytes
                {
                    return Err(error(
                        "atomic cell-range deletion exceeds configured batch admission bound",
                    ));
                }
                let mutation = remote.record(ks, &reduced, reference, &pin)?;
                deletes.push(Mutation::Delete {
                    keyspace: ks,
                    key: mutation.key().clone(),
                });
                after = reduced;
            }
            page = remote
                .db
                .index_page(ks, Some(&after), false, cut.clone())
                .map_err(error)?;
            if page.is_empty() {
                break;
            }
        }
        if !deletes.is_empty() {
            self.commit_remote(deletes)?;
        }
        Ok(())
    }
}

pub struct WriteBatch {
    db: Arc<Db>,
    native: Option<crate::batch::WriteBatch>,
    mutations: Vec<Mutation>,
    payload_bytes: usize,
}
impl WriteBatch {
    pub fn write(&mut self, ks: KeySpace, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        let key = key.into();
        let value = value.into();
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
        if let Some(batch) = self.native {
            let _guard = self
                .db
                .durable_native
                .load(Ordering::Relaxed)
                .then(|| self.db.write_gate());
            batch.commit()?;
            self.db.native_barrier(true)
        } else {
            let _guard = self.db.shared_write_gate();
            self.db.commit_remote(self.mutations)
        }
    }
}

pub enum Checkpoint {
    Native(Arc<crate::checkpoint::DbCheckpoint>),
    Remote(Arc<Db>, Result<(IndexReadCut, IndexReadPin)>),
}
impl Checkpoint {
    pub fn iterator(self, ks: KeySpace) -> DbIterator {
        match self {
            Self::Native(checkpoint) => DbIterator::Native(checkpoint.iterator(ks)),
            Self::Remote(db, cut) => DbIterator::Remote(RemoteIterator::new(db, ks, cut)),
        }
    }
}
pub enum DbIterator {
    Native(crate::iterators::db_iterator::DbIterator),
    Remote(RemoteIterator),
}
impl DbIterator {
    pub fn set_lower_bound(&mut self, bound: impl Into<Bytes>) {
        match self {
            Self::Native(iter) => iter.set_lower_bound(bound),
            Self::Remote(iter) => {
                iter.lower = Some(bound.into());
                iter.reset();
            }
        }
    }
    pub fn set_upper_bound(&mut self, bound: impl Into<Bytes>) {
        match self {
            Self::Native(iter) => iter.set_upper_bound(bound),
            Self::Remote(iter) => {
                iter.upper = Some(bound.into());
                iter.reset();
            }
        }
    }
    pub fn reverse(&mut self) {
        match self {
            Self::Native(iter) => iter.reverse(),
            Self::Remote(iter) => {
                iter.reverse = !iter.reverse;
                iter.reset();
            }
        }
    }
}
impl Iterator for DbIterator {
    type Item = Result<(Bytes, Bytes)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Native(iter) => iter.next(),
            Self::Remote(iter) => iter.next(),
        }
    }
}
const SCAN_INITIAL_LOOKAHEAD: usize = 4;
const SCAN_MAX_LOOKAHEAD: usize = 256;

pub struct RemoteIterator {
    db: Arc<Db>,
    ks: KeySpace,
    cut: IndexReadCut,
    _pin: Option<IndexReadPin>,
    lower: Option<Bytes>,
    upper: Option<Bytes>,
    reverse: bool,
    after: Option<Bytes>,
    started: bool,
    exhausted: bool,
    done: bool,
    page: VecDeque<(Bytes, Bytes)>,
    pending: VecDeque<PendingScanPage>,
    ready: Option<Vec<(Bytes, ValueRecordReference)>>,
    lookahead: usize,
    failure: Option<DbError>,
}
impl RemoteIterator {
    fn new(db: Arc<Db>, ks: KeySpace, cut: Result<(IndexReadCut, IndexReadPin)>) -> Self {
        let (cut, pin, failure) = match cut {
            Ok((cut, pin)) => (cut, Some(pin), None),
            Err(e) => (IndexReadCut::BeforeAll, None, Some(e)),
        };
        Self {
            db,
            ks,
            cut,
            _pin: pin,
            lower: None,
            upper: None,
            reverse: false,
            after: None,
            started: false,
            exhausted: false,
            done: false,
            page: VecDeque::new(),
            pending: VecDeque::new(),
            ready: None,
            lookahead: SCAN_INITIAL_LOOKAHEAD,
            failure,
        }
    }
    fn reset(&mut self) {
        if matches!(
            self.db.shape.ks(self.ks).key_indexing(),
            crate::key_shape::KeyIndexing::Hash
        ) {
            self.failure = Some(error(
                "hash keyspaces do not support application-key bounds or reversal",
            ));
        }
        self.page.clear();
        self.pending.clear();
        self.ready = None;
        self.after = None;
        self.started = false;
        self.exhausted = false;
        self.done = false;
        self.lookahead = SCAN_INITIAL_LOOKAHEAD;
    }
    fn next_references(&mut self) -> Result<Vec<(Bytes, ValueRecordReference)>> {
        let Backend::Remote(remote) = &self.db.backend else {
            unreachable!()
        };
        let descriptor = self.db.shape.ks(self.ks);
        let target = self.lookahead;
        let mut references = Vec::with_capacity(target);
        if !self.started {
            self.started = true;
            let bound = if self.reverse {
                &self.upper
            } else {
                &self.lower
            };
            if let Some(bound) = bound {
                let reduced: Bytes = descriptor.reduce_key(bound).as_ref().to_vec().into();
                if let Some(reference) = remote
                    .db
                    .index_reference(self.ks, bound, self.cut.clone())
                    .map_err(error)?
                {
                    references.push((reduced.clone(), reference));
                }
                self.after = Some(reduced);
            }
        }
        if !self.exhausted && references.len() < target {
            let rows = remote
                .db
                .scan_index_page(
                    self.ks,
                    self.after.as_deref(),
                    self.reverse,
                    target - references.len(),
                    self.cut.clone(),
                )
                .map_err(error)?;
            if rows.is_empty() {
                self.exhausted = true;
            } else {
                references.extend(rows);
            }
        }
        if references.is_empty() {
            return Ok(references);
        }
        let mut references = remote
            .db
            .bound_scan_page(references, target)
            .map_err(error)?;
        // Reduced keys can be lossy: include the boundary candidate, but never
        // submit rows strictly past its reduced key. Full-key checks follow I/O.
        let terminal = if self.reverse {
            &self.lower
        } else {
            &self.upper
        };
        if let Some(bound) = terminal {
            let reduced = descriptor.reduce_key(bound);
            if let Some(end) = references.iter().position(|(key, _)| {
                if self.reverse {
                    key.as_ref() < reduced.as_ref()
                } else {
                    key.as_ref() > reduced.as_ref()
                }
            }) {
                references.truncate(end);
                self.exhausted = true;
            }
        }
        // Apply terminal bounds after byte bounding: a shortened page must not
        // mark the unreturned, still-in-bounds suffix exhausted.
        if let Some((reduced, _)) = references.last() {
            self.after = Some(reduced.clone());
        }
        self.lookahead = (self.lookahead * 2).min(SCAN_MAX_LOOKAHEAD);
        Ok(references)
    }

    fn fill_page(&mut self) -> Result<()> {
        let limit = match &self.db.backend {
            Backend::Remote(remote) => remote.db.scan_prefetch_pages(),
            Backend::Native(_) => unreachable!(),
        };
        while self.pending.len() < limit {
            if self
                .pending
                .back()
                .is_some_and(PendingScanPage::has_unsubmitted)
                || self.pending.front().is_some_and(PendingScanPage::oversized)
            {
                break;
            }
            let references = if let Some(ready) = self.ready.take() {
                ready
            } else if self.exhausted {
                break;
            } else {
                self.next_references()?
            };
            if references.is_empty() {
                break;
            }
            let Backend::Remote(remote) = &self.db.backend else {
                unreachable!()
            };
            // A legal oversized singleton cannot compete with another page.
            let oversized = remote.db.scan_page_oversized(&references).map_err(error)?;
            if oversized && !self.pending.is_empty() {
                self.ready = Some(references);
                break;
            }
            self.pending.push_back(
                remote
                    .db
                    .submit_scan_page(
                        references,
                        self._pin
                            .as_ref()
                            .expect("valid iterator owns its captured cut")
                            .clone(),
                    )
                    .map_err(error)?,
            );
        }
        let Some(pending) = self.pending.pop_front() else {
            return Ok(());
        };
        let Backend::Remote(remote) = &self.db.backend else {
            unreachable!()
        };
        let rows = remote
            .db
            .finish_scan_page(self.ks, pending)
            .map_err(error)?;
        for (key, value) in rows {
            if self
                .lower
                .as_ref()
                .is_some_and(|bound| key.as_ref() < bound.as_ref())
            {
                if self.reverse {
                    self.exhausted = true;
                    self.pending.clear();
                    self.ready = None;
                    break;
                }
                continue;
            }
            if self
                .upper
                .as_ref()
                .is_some_and(|bound| key.as_ref() >= bound.as_ref())
            {
                if self.reverse {
                    continue;
                }
                self.exhausted = true;
                self.pending.clear();
                self.ready = None;
                break;
            }
            self.page.push_back((key, value));
        }
        Ok(())
    }
    fn next_row(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        let Backend::Remote(remote) = &self.db.backend else {
            unreachable!()
        };
        remote.db.validate_cached_read().map_err(error)?;
        while self.page.is_empty()
            && (!self.exhausted || !self.pending.is_empty() || self.ready.is_some())
        {
            self.fill_page()?;
        }
        if self.page.is_empty() {
            return Ok(None);
        }
        self.db.scan_records.fetch_add(1, Ordering::Relaxed);
        Ok(self.page.pop_front())
    }
}
impl Iterator for RemoteIterator {
    type Item = Result<(Bytes, Bytes)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(e) = self.failure.take() {
            self.done = true;
            return Some(Err(e));
        }
        match self.next_row() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.pending.clear();
                self.ready = None;
                self.page.clear();
                self.done = true;
                Some(Err(e))
            }
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
