//! Opt-in, bounded cold-path owner events on the Linux monotonic clock.
//!
//! `a` is the file index for prepare and namespace/file syncs,
//! the WAL cut for proof/frontier, the extent start for grants, and the adopted
//! end for adoption rounds. Export/revoke use the WAL file index (u64::MAX
//! when unknown) in `a` and the backing file inode in `b`.
//! `b` is otherwise zero except for control receipt request codes:
//! 1 checkpoint, 2 retirement_files, 3 retire_files, 4 read_io_stats, 5 status,
//! 6 fence, 7 append, 8 read, 9 recover, 10 seal, 11 stats, 12 read_profiled,
//! 13 stats_detailed, 14 connection_profile, 15 operational_read_io_stats,
//! 16 owner_timeline_arm, 17 owner_timeline_fetch, 18 checkpoint_position,
//! 19 replay_wal, 20 retire_wal_files, 21 validate_record_references,
//! 22 register_nvmeof_export, 23 release_nvmeof_export, 24 register_nvmeof_epoch_pin,
//! 25 release_nvmeof_epoch_pin, 26 release_live_epoch_pin, 27 reserve_live_group,
//! 28 complete_live_group, 29 resolve_live_records, 30 recover_live_metadata,
//! 31 live_target_stats, 32 shutdown_live_target, 33 lease_live_extent,
//! 34 adopt_live_extent, 35 open_live_exports, 36 prelease_live_extent,
//! 37 replay_live_wal, 38 read_evidence, 39 process_resources,
//! 40 and 41 retired (the removed RAM probe), 42 acquire_live_export_pin,
//! 43 release_live_export_pin, 44 durable_progress, 45 partition_stats.
//! Control `a` is the envelope id.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static LOG: OnceLock<OwnerEventLog> = OnceLock::new();

/// Serialize tests that drain or assert the process-global diagnostic capture.
#[cfg(all(test, feature = "experimental-nvmeof"))]
pub(crate) static OWNER_EVENTS_TEST_LOCK: Mutex<()> = Mutex::new(());

macro_rules! event_kinds {
    ($($kind:ident => $name:literal),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum OwnerEventKind { $($kind),+ }
        impl OwnerEventKind {
            fn name(self) -> &'static str {
                match self { $(Self::$kind => $name),+ }
            }
        }
    };
}
event_kinds! {
    PrepareEnsureStart => "prepare_ensure_start", PrepareEnsureEnd => "prepare_ensure_end",
    PrepareFileSyncStart => "prepare_file_sync_start", PrepareFileSyncEnd => "prepare_file_sync_end",
    PrepareWitnessStart => "prepare_witness_start", PrepareWitnessEnd => "prepare_witness_end",
    PreparePublished => "prepare_published",
    ProofStart => "proof_start", ProofEnd => "proof_end",
    ProofFileSyncStart => "proof_file_sync_start", ProofFileSyncEnd => "proof_file_sync_end",
    ProofNamespaceSyncStart => "proof_namespace_sync_start", ProofNamespaceSyncEnd => "proof_namespace_sync_end",
    FrontierSyncStart => "frontier_sync_start", FrontierSyncEnd => "frontier_sync_end",
    LeaseGrantStart => "lease_grant_start", LeaseGrantEnd => "lease_grant_end",
    LeaseNamespaceSyncStart => "lease_namespace_sync_start", LeaseNamespaceSyncEnd => "lease_namespace_sync_end",
    ExportStart => "export_start", ExportEnd => "export_end",
    RevokeStart => "revoke_start", RevokeEnd => "revoke_end",
    AdoptionRoundStart => "adoption_round_start", AdoptionRoundEnd => "adoption_round_end",
    ControlReceived => "control_received", ControlReplied => "control_replied",
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct OwnerEvent {
    pub t_ns: u64,
    pub kind: OwnerEventKind,
    pub partition: u32,
    pub a: u64,
    pub b: u64,
}

struct OwnerEventBuffer {
    origin_instant: Instant,
    origin_monotonic_ns: u64,
    events: Vec<OwnerEvent>,
}

struct OwnerEventLog {
    capacity: usize,
    // Leaf lock: no I/O or other locks while held, only bounded pushes/copies.
    buffer: Mutex<OwnerEventBuffer>,
    dropped: AtomicU64,
}

/// Enable once per process. Later calls preserve the original log and capacity.
pub fn enable(capacity: usize) {
    LOG.get_or_init(|| OwnerEventLog::new(capacity));
}

pub(crate) fn monotonic_now_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: time is a valid stack out-pointer; the return code is checked.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) };
    assert_eq!(
        result,
        0,
        "CLOCK_MONOTONIC failed: {}",
        io::Error::last_os_error()
    );
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

impl OwnerEventLog {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            buffer: Mutex::new(OwnerEventBuffer {
                origin_instant: Instant::now(),
                origin_monotonic_ns: monotonic_now_ns(),
                events: Vec::with_capacity(capacity),
            }),
            dropped: AtomicU64::new(0),
        }
    }

    fn record(&self, kind: OwnerEventKind, partition: u32, a: u64, b: u64) {
        let mut buffer = self.buffer.lock();
        if buffer.events.len() == self.capacity {
            // Relaxed: diagnostic count only, not a publication fence.
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Rust Instant uses CLOCK_MONOTONIC on Linux. Sample under the leaf
        // lock so the exported sequence is monotonic across threads too.
        let t_ns = buffer.origin_monotonic_ns.saturating_add(
            buffer
                .origin_instant
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        );
        buffer.events.push(OwnerEvent {
            t_ns,
            kind,
            partition,
            a,
            b,
        });
    }

    fn write_json(&self, path: &Path) -> io::Result<()> {
        // Allocate before locking and release the lock before all file I/O.
        let mut snapshot = Vec::with_capacity(self.capacity);
        let (origin_monotonic_ns, dropped) = {
            let buffer = self.buffer.lock();
            snapshot.extend_from_slice(&buffer.events);
            // Relaxed: a statistic protected against restart by the buffer lock.
            (
                buffer.origin_monotonic_ns,
                self.dropped.load(Ordering::Relaxed),
            )
        };
        self.write_snapshot(path, origin_monotonic_ns, dropped, &snapshot)
    }

    fn drain_json(&self, path: &Path) -> io::Result<()> {
        let events = Vec::with_capacity(self.capacity);
        let (previous, dropped) = {
            let mut buffer = self.buffer.lock();
            let fresh = OwnerEventBuffer {
                origin_instant: Instant::now(),
                origin_monotonic_ns: monotonic_now_ns(),
                events,
            };
            let previous = std::mem::replace(&mut *buffer, fresh);
            // Relaxed: diagnostic count; all writers also hold the buffer lock.
            let dropped = self.dropped.swap(0, Ordering::Relaxed);
            (previous, dropped)
        };
        self.write_snapshot(
            path,
            previous.origin_monotonic_ns,
            dropped,
            &previous.events,
        )
    }

    fn write_snapshot(
        &self,
        path: &Path,
        origin_monotonic_ns: u64,
        dropped: u64,
        snapshot: &[OwnerEvent],
    ) -> io::Result<()> {
        let mut out = BufWriter::new(File::create(path)?);
        write!(
            out,
            "{{\"schema\":\"th-owner-events-v1\",\"origin_monotonic_ns\":{},\"capacity\":{},\"dropped\":{},\"events\":[",
            origin_monotonic_ns, self.capacity, dropped
        )?;
        for (index, event) in snapshot.iter().enumerate() {
            if index != 0 {
                out.write_all(b",")?;
            }
            write!(
                out,
                "{{\"t_ns\":{},\"kind\":\"{}\",\"partition\":{},\"a\":{},\"b\":{}}}",
                event.t_ns,
                event.kind.name(),
                event.partition,
                event.a,
                event.b
            )?;
        }
        out.write_all(b"]}")?;
        out.flush()
    }
}

/// Disabled calls only read the OnceLock and allocate nothing.
pub fn record(kind: OwnerEventKind, partition: u32, a: u64, b: u64) {
    record_into(&LOG, kind, partition, a, b);
}

/// Close a diagnostic span on success, early return, error, or unwinding.
/// An end event marks exit, not success. Disabled spans allocate nothing.
pub(crate) struct OwnerEventSpan {
    end: OwnerEventKind,
    partition: u32,
    a: u64,
    b: u64,
}

impl OwnerEventSpan {
    pub(crate) fn new(
        start: OwnerEventKind,
        end: OwnerEventKind,
        partition: u32,
        a: u64,
        b: u64,
    ) -> Self {
        record(start, partition, a, b);
        Self {
            end,
            partition,
            a,
            b,
        }
    }

    pub(crate) fn set_end_a(&mut self, a: u64) {
        self.a = a;
    }

    pub(crate) fn end_at(mut self, a: u64) {
        self.set_end_a(a);
    }
}

impl Drop for OwnerEventSpan {
    fn drop(&mut self) {
        record(self.end, self.partition, self.a, self.b);
    }
}

fn record_into(
    log: &OnceLock<OwnerEventLog>,
    kind: OwnerEventKind,
    partition: u32,
    a: u64,
    b: u64,
) {
    if let Some(log) = log.get() {
        log.record(kind, partition, a, b);
    }
}

/// Write an enabled log. No file is created when recording is disabled.
pub fn write_json(path: &Path) -> io::Result<()> {
    match LOG.get() {
        Some(log) => log.write_json(path),
        None => Err(io::Error::other("owner events are not enabled")),
    }
}

/// Detach the current events and restart with a fresh clock origin before I/O.
/// New events belong to the next capture. A write failure is returned to the
/// caller; the detached capture is not restored.
pub fn drain_json(path: &Path) -> io::Result<()> {
    match LOG.get() {
        Some(log) => log.drain_json(path),
        None => Err(io::Error::other("owner events are not enabled")),
    }
}

#[cfg(all(test, feature = "experimental-nvmeof"))]
pub(crate) fn snapshot() -> (Vec<OwnerEvent>, u64) {
    LOG.get().map_or_else(
        || (Vec::new(), 0),
        |log| {
            let buffer = log.buffer.lock();
            // Relaxed: diagnostic loss count sampled under the capture's leaf lock.
            (buffer.events.clone(), log.dropped.load(Ordering::Relaxed))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_owner_events_do_not_initialize_or_allocate_a_log() {
        let log = OnceLock::new();
        for _ in 0..100 {
            record_into(&log, OwnerEventKind::ProofStart, 0, 0, 0);
        }
        assert!(log.get().is_none());
    }

    #[test]
    fn owner_events_capacity_overflow_counts_drops() {
        let log = OwnerEventLog::new(2);
        for _ in 0..5 {
            log.record(OwnerEventKind::ProofStart, 3, 7, 0);
        }
        assert_eq!(log.buffer.lock().events.len(), 2);
        assert_eq!(log.buffer.lock().events.capacity(), 2);
        assert_eq!(log.dropped.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn owner_events_json_round_trip() {
        let log = OwnerEventLog::new(2);
        log.record(OwnerEventKind::PrepareEnsureStart, 3, 7, 0);
        let dir = tempdir::TempDir::new("owner-events").unwrap();
        let path = dir.path().join("events.json");
        log.write_json(&path).unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(json["schema"], "th-owner-events-v1");
        assert_eq!(
            json["origin_monotonic_ns"],
            log.buffer.lock().origin_monotonic_ns
        );
        assert_eq!(json["capacity"], 2);
        assert_eq!(json["dropped"], 0);
        let events: Vec<OwnerEvent> = serde_json::from_value(json["events"].clone()).unwrap();
        assert_eq!(events[0].kind, OwnerEventKind::PrepareEnsureStart);
        assert_eq!((events[0].partition, events[0].a, events[0].b), (3, 7, 0));
    }

    #[test]
    fn owner_event_times_are_monotonic_per_thread() {
        let log = OwnerEventLog::new(400);
        std::thread::scope(|scope| {
            for partition in 0..4 {
                let log = &log;
                scope.spawn(move || {
                    for a in 0..100 {
                        log.record(OwnerEventKind::ProofStart, partition, a, 0);
                    }
                });
            }
        });
        let buffer = log.buffer.lock();
        for partition in 0..4 {
            let times: Vec<_> = buffer
                .events
                .iter()
                .filter(|e| e.partition == partition)
                .map(|e| e.t_ns)
                .collect();
            assert_eq!(times.len(), 100);
            assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
        }
    }

    #[test]
    fn owner_events_drain_restarts_without_prior_events_or_drops() {
        let log = OwnerEventLog::new(1);
        log.record(OwnerEventKind::ControlReceived, 3, 1, 18);
        log.record(OwnerEventKind::ProofStart, 3, 4096, 0);
        let old_origin = log.buffer.lock().origin_monotonic_ns;
        let dir = tempdir::TempDir::new("owner-events-drain").unwrap();
        let first = dir.path().join("first.json");
        log.drain_json(&first).unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(first).unwrap()).unwrap();
        assert_eq!(json["events"].as_array().unwrap().len(), 1);
        assert_eq!(json["events"][0]["a"], 1);
        assert_eq!(json["dropped"], 1);
        assert_eq!(json["origin_monotonic_ns"], old_origin);
        assert!(log.buffer.lock().origin_monotonic_ns >= old_origin);
        assert!(log.buffer.lock().events.is_empty());
        assert_eq!(log.dropped.load(Ordering::Relaxed), 0);
        log.record(OwnerEventKind::ControlReceived, 3, 1, 33);
        let second = dir.path().join("second.json");
        log.drain_json(&second).unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(second).unwrap()).unwrap();
        assert_eq!(json["events"].as_array().unwrap().len(), 1);
        assert_eq!(json["events"][0]["b"], 33);
        assert_eq!(json["dropped"], 0);
    }
}
