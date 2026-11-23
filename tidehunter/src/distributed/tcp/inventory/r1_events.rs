//! Owner side of the ordered R1 evidence stream (see `distributed::r1_events`).
//!
//! Lives in `NodeInventory` and is called under the inventory mutex, after the
//! transition it names; `emit` only `try_send`s. `after` chains the events of
//! one exact file `(fencing generation, file start)` inside this stream. The
//! coordinator's stream is joined on the same exact file identity and seal
//! root generation; no wire field carries an event ID.
use super::*;
use crate::distributed::nvmeof::live_storage::LiveWalSegment;
use crate::distributed::r1_events::{EventId, FileRef, R1EventLog, hex};
use crate::distributed::storage::{FileSeal, PendingRetirement, UnlinkOutcome};
use serde_json::json;

/// `(fencing generation, file index, (device, inode) if known)` of a pin.
pub(in crate::distributed::tcp) type PinnedFile = (u64, u64, Option<(u64, u64)>);

pub(in crate::distributed::tcp) struct OwnerR1Events {
    log: R1EventLog,
    partition: u32,
    wal_file_bytes: u64,
    /// File of the latest live grant, per fencing generation.
    granted_file: BTreeMap<u64, u64>,
    /// Seals already reported, by `(fencing generation, file start)`.
    reported_seals: std::collections::BTreeSet<(u64, u64)>,
    /// Latest event of each exact file, by `(fencing generation, start)`.
    last: BTreeMap<(u64, u64), EventId>,
}

impl NodeInventory {
    /// Open this owner incarnation's stream (`owner_O<partition>`, named by
    /// the owner incarnation). The file is created and synced without the
    /// inventory lock; a stream already open (or opened meanwhile) refuses.
    pub(in crate::distributed::tcp) fn enable_r1_events(
        inventory: &Mutex<NodeInventory>,
        config: &crate::distributed::r1_events::R1EventConfig,
    ) -> Result<(), TcpError> {
        let (partition, incarnation, database, file_bytes) = {
            let guard = inventory.lock();
            if guard.r1_events.is_some() {
                return Err(TcpError::new("r1 event stream already enabled"));
            }
            let partition = &guard.config.partition;
            (
                partition.partition,
                guard.incarnation,
                partition.inventory.database,
                partition.wal_file_bytes,
            )
        };
        let log = R1EventLog::open(
            config,
            &format!("owner_O{partition}"),
            &hex(&incarnation),
            database,
        )
        .map_err(TcpError::component)?;
        let mut guard = inventory.lock();
        if guard.r1_events.is_some() {
            return Err(TcpError::new("r1 event stream already enabled"));
        }
        let mut events = OwnerR1Events::new(log, partition, file_bytes);
        // Recovery at owner open already repeated these unlinks and the
        // directory sync; publish them now, after the fact, in log order.
        for (&generation, log) in &guard.logs {
            let recovered = log.take_recovered_cleanup();
            events.unlinked(generation, &recovered, "recovery");
        }
        guard.r1_events = Some(events);
        Ok(())
    }
}

impl OwnerR1Events {
    pub(in crate::distributed::tcp) fn new(
        log: R1EventLog,
        partition: u32,
        wal_file_bytes: u64,
    ) -> Self {
        Self {
            log,
            partition,
            wal_file_bytes,
            granted_file: BTreeMap::new(),
            reported_seals: std::collections::BTreeSet::new(),
            last: BTreeMap::new(),
        }
    }

    pub(in crate::distributed::tcp) fn log(&self) -> &R1EventLog {
        &self.log
    }

    fn file(&self, generation: u64, start: u64) -> FileRef {
        FileRef::located(
            self.partition,
            generation,
            start,
            start.saturating_add(self.wal_file_bytes),
        )
    }

    /// Emit one event chained after the previous event of the same file.
    fn emit_file(
        &mut self,
        kind: &'static str,
        generation: u64,
        file: FileRef,
        payload: serde_json::Value,
    ) -> Option<EventId> {
        let start = file.file_start?;
        let after: Vec<EventId> = self
            .last
            .get(&(generation, start))
            .cloned()
            .into_iter()
            .collect();
        let id = self.log.emit(kind, file, None, &after, payload)?;
        self.last.insert((generation, start), id.clone());
        Some(id)
    }

    /// A live grant landed in `segment`. The first grant in a later file of
    /// the same fencing generation means the allocator passed every earlier
    /// file: one `file_rolled` for the file it left.
    pub(in crate::distributed::tcp) fn grant(&mut self, segment: &LiveWalSegment) {
        let previous = self.granted_file.insert(segment.epoch, segment.file_index);
        let Some(previous) = previous.filter(|previous| *previous < segment.file_index) else {
            return;
        };
        let start = previous.saturating_mul(self.wal_file_bytes);
        let file = self.file(segment.epoch, start);
        self.emit_file(
            "file_rolled",
            segment.epoch,
            file,
            json!({
                "to_file_start": segment.file_index.saturating_mul(self.wal_file_bytes),
                "to_device": segment.device,
                "to_inode": segment.inode,
            }),
        );
    }

    /// Report every durable seal not reported yet (seals are installed in
    /// ascending file order by the preparer thread).
    pub(in crate::distributed::tcp) fn seals(&mut self, generation: u64, seals: &[FileSeal]) {
        for seal in seals {
            if !self.reported_seals.insert((generation, seal.start)) {
                continue;
            }
            let file = FileRef::located(self.partition, generation, seal.start, seal.end)
                .with_inode(seal.device, seal.inode);
            self.emit_file(
                "seal_durable",
                generation,
                file,
                json!({
                    "root_generation": seal.root_generation,
                    "last_batch_sequence": seal.last_batch_sequence,
                }),
            );
        }
    }

    /// One retirement step for each listed victim `(start, device, inode)`.
    pub(in crate::distributed::tcp) fn victims(
        &mut self,
        kind: &'static str,
        generation: u64,
        victims: &[(u64, u64, u64)],
        payload: &serde_json::Value,
    ) {
        for &(start, device, inode) in victims {
            let file = self.file(generation, start).with_inode(device, inode);
            self.emit_file(kind, generation, file, payload.clone());
        }
    }

    /// Unlink results of finished pending entries, then one
    /// `wal_directory_synced` after all of them (the sync already happened).
    /// `phase` is `retirement` (a request) or `recovery` (owner open).
    pub(in crate::distributed::tcp) fn unlinked(
        &mut self,
        generation: u64,
        done: &[(PendingRetirement, UnlinkOutcome)],
        phase: &'static str,
    ) {
        let mut unlinks = Vec::with_capacity(done.len());
        for (entry, outcome) in done {
            let kind = match outcome {
                UnlinkOutcome::Unlinked => "wal_unlinked",
                UnlinkOutcome::AlreadyAbsent => "already_absent",
            };
            let file = self
                .file(generation, entry.start)
                .with_inode(entry.device, entry.inode);
            let payload = json!({"seal_generation": entry.seal_generation, "phase": phase});
            if let Some(id) = self.emit_file(kind, generation, file, payload) {
                unlinks.push(id);
            }
        }
        if done.is_empty() {
            return;
        }
        let files: Vec<u64> = done.iter().map(|(entry, _)| entry.start).collect();
        if let Some(id) = self.log.emit(
            "wal_directory_synced",
            FileRef {
                partition: Some(self.partition),
                fencing_generation: Some(generation),
                ..FileRef::default()
            },
            None,
            &unlinks,
            json!({"files": files, "phase": phase}),
        ) {
            for (entry, _) in done {
                self.last.insert((generation, entry.start), id.clone());
            }
        }
    }

    /// A record (`record`) or connection (`export`) pin on exact files was
    /// taken or dropped; `token` names the hold.
    pub(in crate::distributed::tcp) fn pin(
        &mut self,
        kind: &'static str,
        hold: &'static str,
        token: [u8; 16],
        files: impl IntoIterator<Item = PinnedFile>,
        reason: &'static str,
    ) {
        let token = hex(&token);
        let mut seen = std::collections::BTreeSet::new();
        for (generation, file_index, identity) in files {
            if !seen.insert((generation, file_index)) {
                continue;
            }
            let start = file_index.saturating_mul(self.wal_file_bytes);
            let mut file = self.file(generation, start);
            if let Some((device, inode)) = identity {
                file = file.with_inode(device, inode);
            }
            self.emit_file(
                kind,
                generation,
                file,
                json!({"hold": hold, "token": token, "reason": reason}),
            );
        }
    }
}

/// The files of record leases.
pub(in crate::distributed::tcp) fn lease_files(
    leases: &[crate::distributed::nvmeof::live_storage::LiveRecordLease],
) -> Vec<PinnedFile> {
    leases
        .iter()
        .map(|lease| {
            let segment = &lease.segment;
            (
                segment.epoch,
                segment.file_index,
                Some((segment.device, segment.inode)),
            )
        })
        .collect()
}
