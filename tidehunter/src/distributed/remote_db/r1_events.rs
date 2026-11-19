//! Coordinator side of the ordered R1 evidence stream (see
//! `distributed::r1_events`): one `victim_claimed`, one
//! `initiator_cq_drained` per proof, and one `retirement_reply` per request.
//!
//! Only the R1 pass emits, under the cold `WriteCore::retirement` gate; the
//! slot mutex is held for one `try_send`, never across I/O. The owner's
//! stream is joined on the exact file identity plus `seal_generation` (the
//! owner's `seal_durable.root_generation`); no wire field carries event IDs.
use super::LocalDbError;
use super::write_core::core::WriteCore;
use crate::distributed::r1_events::{EventId, FileRef, R1EventConfig, R1EventLog, new_incarnation};
use crate::distributed::storage::{RetireProof, RetirementWitness};
use crate::distributed::tcp::checkpoint::RetirementResult;
use parking_lot::Mutex;
use serde_json::json;

/// The stream and the next request number (`attempt_id`) of this process.
pub(crate) struct CoordinatorR1Events {
    log: R1EventLog,
    next_attempt: u64,
}

/// Inert until `enable`; one stream per coordinator incarnation.
#[derive(Default)]
pub(crate) struct R1EventSlot(Mutex<Option<CoordinatorR1Events>>);

impl R1EventSlot {
    /// Opening creates and syncs files, so it runs without the slot lock; a
    /// racing second `enable` is refused and its unused stream stays partial.
    pub(crate) fn enable(
        &self,
        core: &WriteCore,
        config: &R1EventConfig,
    ) -> Result<(), LocalDbError> {
        if self.0.lock().is_some() {
            return Err(LocalDbError::new("r1 event stream already enabled"));
        }
        let database = core.config.partition_config(1, 0).inventory.database;
        let log = R1EventLog::open(config, "coordinator", &new_incarnation(), database)
            .map_err(LocalDbError::component)?;
        let mut slot = self.0.lock();
        if slot.is_some() {
            return Err(LocalDbError::new("r1 event stream already enabled"));
        }
        *slot = Some(CoordinatorR1Events {
            log,
            next_attempt: 0,
        });
        Ok(())
    }

    /// Terminal line and close; the writer join runs without the slot lock.
    pub(crate) fn finish(&self) -> Result<(), LocalDbError> {
        let events = self
            .0
            .lock()
            .take()
            .ok_or_else(|| LocalDbError::new("r1 event stream is not enabled"))?;
        events.log.finish().map_err(LocalDbError::component)
    }

    /// One request is about to be sent: claim each victim, then record that
    /// no walked reference can still need it. Returns the request's
    /// `attempt_id` and the events the reply follows, or `None` if disabled.
    pub(crate) fn claimed(
        &self,
        core: &WriteCore,
        partition: u32,
        file_bytes: u64,
        proofs: &[RetireProof],
        witness: &RetirementWitness,
        install_seq: u64,
    ) -> Option<(u64, Vec<EventId>)> {
        let mut slot = self.0.lock();
        let events = slot.as_mut()?;
        let attempt = events.next_attempt;
        events.next_attempt += 1;
        let mut drained = Vec::with_capacity(proofs.len());
        for proof in proofs {
            let mut file = FileRef::located(
                partition,
                core.epoch,
                proof.file_start,
                proof.file_start.saturating_add(file_bytes),
            );
            if let Some(inode) = proof.inode {
                file = file.with_inode(inode.device, inode.inode);
            }
            let claim = events.log.emit(
                "victim_claimed",
                file,
                Some(attempt),
                &[],
                json!({
                    "seal_generation": proof.seal_generation,
                    "checkpoint_generation": witness.checkpoint_generation,
                    "metadata_digest": crate::distributed::r1_events::hex(&witness.metadata_digest),
                    "replay_from": witness.replay_from,
                    "durable_end": witness.durable_end,
                    "callback_from": witness.callback_from,
                    "covered_next": witness.covered_next,
                    "persisted_cut_next": witness.persisted_cut_next,
                    "install_seq": install_seq,
                }),
            );
            // Every remote read flight owns its `IndexReadPin` until its
            // terminal completion, and the walk that chose this victim saw
            // every reference such a pin retains (see `dead_wal_files`).
            if let Some(id) = events.log.emit(
                "initiator_cq_drained",
                file,
                Some(attempt),
                claim.as_slice(),
                json!({"basis": "no_reference_index_pins_to_terminal_cqe"}),
            ) {
                drained.push(id);
            }
        }
        Some((attempt, drained))
    }

    /// The owner answered the request `attempt`. `Busy` made no durable
    /// decision on the owner and adds no retirement edge.
    pub(crate) fn replied(
        &self,
        partition: u32,
        generation: u64,
        attempt: u64,
        after: &[EventId],
        files: &[u64],
        outcome: RetirementResult,
    ) {
        let slot = self.0.lock();
        let Some(events) = slot.as_ref() else {
            return;
        };
        let outcome = match outcome {
            RetirementResult::Completed => "completed",
            RetirementResult::Busy => "busy",
        };
        events.log.emit(
            "retirement_reply",
            FileRef {
                partition: Some(partition),
                fencing_generation: Some(generation),
                ..FileRef::default()
            },
            Some(attempt),
            after,
            json!({"outcome": outcome, "files": files}),
        );
    }
}

impl super::ConcurrentRemoteDatabase {
    /// Start this coordinator incarnation's ordered R1 evidence stream at
    /// `<case_output>/capacity/r1-events/coordinator/<incarnation>.jsonl`
    /// (`victim_claimed`, `initiator_cq_drained`, `retirement_reply`).
    pub fn enable_r1_events(&self, config: &R1EventConfig) -> Result<(), LocalDbError> {
        let core = self.write_core();
        core.r1_events.enable(core, config)
    }

    /// Write the terminal line and close the coordinator stream. Without
    /// this call the stream stays `.partial` (incomplete evidence).
    pub fn finish_r1_events(&self) -> Result<(), LocalDbError> {
        self.write_core().r1_events.finish()
    }
}
