use crate::distributed::storage::{DurableBatch, LocalDbError};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::core::WriteCore;
use std::sync::{OnceLock, Weak};

/// The weak binding prevents a worker from keeping its shutdown owner alive.
/// Durability is still decided only by the existing ordered transport release.
pub(in crate::distributed::remote_db) struct CoreCompletion {
    core: OnceLock<Weak<WriteCore>>,
}

impl CoreCompletion {
    pub(in crate::distributed::remote_db) fn unbound() -> Self {
        Self {
            core: OnceLock::new(),
        }
    }

    pub(in crate::distributed::remote_db) fn bind(&self, core: &Arc<WriteCore>) {
        // Open owns this binding and cannot admit any batch before it returns.
        assert!(
            self.core.set(Arc::downgrade(core)).is_ok(),
            "write completion bound exactly once during open"
        );
    }

    #[cfg(test)]
    fn new(core: &Arc<WriteCore>) -> Self {
        let completion = Self::unbound();
        completion.bind(core);
        completion
    }
}

impl AppendCompletion for CoreCompletion {
    fn durable(&self, owner: u32, batches: Vec<(u64, DurableBatch)>) {
        if let Some(core) = self.core.get().and_then(Weak::upgrade) {
            core.on_durable(owner, batches);
        }
    }

    fn failed(&self, owner: u32, sequences: &[u64], error: LocalDbError) {
        if let Some(core) = self.core.get().and_then(Weak::upgrade) {
            core.on_append_failed(owner, sequences, error);
        }
    }
}
/// One worker forms/releases groups. Visitors sample diagnostics only; relaxed
/// counters carry no payload, generation, publication or wakeup ordering.
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct WorkerAccounting {
    pub(crate) bytes: AtomicUsize,
    pub(crate) groups: AtomicUsize,
    pub(crate) formed: AtomicU64,
    pub(crate) peak_groups: AtomicUsize,
    /// Group drains, batches they took, and drains per [`DrainStop`] reason.
    pub(crate) drains: AtomicU64,
    pub(crate) drain_batches: AtomicU64,
    pub(crate) drain_stops: [AtomicU64; DrainStop::COUNT],
    /// Allocations the per-stream extent queue could not hold at its cap.
    ///
    /// A cap hit is not a loss: the queue publishes the lowest dropped offset as
    /// its replay floor, so recovery re-reads that extent. The counter exists to
    /// tell "the index is a whole snapshot interval behind the owners" (sustained
    /// growth) apart from a single wide burst, and S12 reads it through
    /// `PipelineStats::allocation_cap_hits`.
    pub(crate) cap_hits: AtomicU64,
    /// Test-only exact membership of every formed group, in formation order.
    /// Pushed by the forming worker only; tests read it after durability.
    #[cfg(test)]
    pub(crate) formed_groups: parking_lot::Mutex<Vec<Vec<u64>>>,
}

/// Why one drain of the READY owner queue stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainStop {
    Empty,
    Allocated,
    Fence,
    Limit,
    Credit,
}

impl DrainStop {
    pub(crate) const COUNT: usize = 5;

    pub(crate) fn live(self) -> crate::distributed::nvmeof::live_metrics::OwnerDrainStop {
        use crate::distributed::nvmeof::live_metrics::OwnerDrainStop;
        match self {
            Self::Empty => OwnerDrainStop::Empty,
            Self::Allocated => OwnerDrainStop::Allocated,
            Self::Fence => OwnerDrainStop::Fence,
            Self::Limit => OwnerDrainStop::Limit,
            Self::Credit => OwnerDrainStop::Credit,
        }
    }
}

impl WorkerAccounting {
    pub(crate) fn record_drain(&self, batches: usize, stop: DrainStop) {
        // Relaxed: worker-only diagnostics; no payload or ordering depends on them.
        self.drains.fetch_add(1, Ordering::Relaxed);
        self.drain_batches
            .fetch_add(batches as u64, Ordering::Relaxed);
        self.drain_stops[stop as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Mirrors the extent queue's own cap-hit count for readers outside the
    /// worker thread.
    ///
    /// The queue is the authority (it counts per `reserve`), so this stores the
    /// cumulative value rather than adding one: the worker republishes after
    /// every submission batch, and a repeated call for the same count must not
    /// double it. A per-hit add would instead need the worker to remember which
    /// hits it had already reported.
    pub(crate) fn record_allocation_cap_hits(&self, hits: u64) {
        // Relaxed: worker-only diagnostics; no payload or ordering depends on them.
        self.cap_hits.store(hits, Ordering::Relaxed);
    }
}

/// The sole owner-worker completion route. Production uses a Weak<WriteCore>;
/// an owner never publishes after that core has gone away.
pub(crate) trait AppendCompletion: Send + Sync {
    fn durable(&self, owner: u32, batches: Vec<(u64, DurableBatch)>);
    fn failed(&self, owner: u32, sequences: &[u64], error: LocalDbError);
}

/// Correlation retained through the transport's contiguous completion prefix.
/// This is worker-local work, never an input on the bounded control channel.
pub(crate) struct GroupCompletion {
    owner: u32,
    sequences: Vec<u64>,
    completion: Arc<dyn AppendCompletion>,
    charge: usize,
    accounting: Arc<WorkerAccounting>,
}

impl GroupCompletion {
    pub(crate) fn new(
        owner: u32,
        sequences: Vec<u64>,
        completion: Arc<dyn AppendCompletion>,
        charge: usize,
        accounting: Arc<WorkerAccounting>,
    ) -> Self {
        // These counters bound worker-local queued plus active transport work.
        // Only this worker changes them, including GroupCompletion::drop.
        accounting.bytes.fetch_add(charge, Ordering::Relaxed);
        let groups = accounting.groups.fetch_add(1, Ordering::Relaxed) + 1;
        accounting.formed.fetch_add(1, Ordering::Relaxed);
        accounting.peak_groups.fetch_max(groups, Ordering::Relaxed);
        #[cfg(test)]
        accounting.formed_groups.lock().push(sequences.clone());
        Self {
            owner,
            sequences,
            completion,
            charge,
            accounting,
        }
    }

    pub(crate) fn send(
        mut self,
        result: Result<Vec<DurableBatch>, LocalDbError>,
    ) -> Result<(), LocalDbError> {
        match result {
            Ok(receipts)
                if receipts.len() == self.sequences.len()
                    && receipts
                        .iter()
                        .zip(&self.sequences)
                        .all(|(receipt, sequence)| {
                            receipt.appended().version().sequence == *sequence
                        }) =>
            {
                self.completion
                    .durable(self.owner, self.sequences.drain(..).zip(receipts).collect());
                Ok(())
            }
            Ok(_) => {
                self.completion.failed(
                    self.owner,
                    &self.sequences,
                    LocalDbError::new("owner completion differs from queued generations"),
                );
                Err(LocalDbError::new(
                    "owner completion differs from queued generations",
                ))
            }
            Err(error) => {
                self.completion.failed(self.owner, &self.sequences, error);
                Ok(())
            }
        }
    }
}

impl Drop for GroupCompletion {
    fn drop(&mut self) {
        // Returning transport credit orders no publication data. The worker
        // cannot form its next group until this callback/drop has completed.
        self.accounting
            .bytes
            .fetch_sub(self.charge, Ordering::Relaxed);
        self.accounting.groups.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::remote_db::pipeline_tests::Fixture;
    use crate::distributed::remote_db::write_core::sequencer::Resolution;
    use crate::distributed::types::{BatchId, BatchVersion};
    use std::time::Instant;

    #[test]
    fn uncertain_append_fails_every_slot_without_reclaiming_a_generation() {
        let fixture = Fixture::new();
        let database = fixture.open();
        let core = Arc::clone(&database.core);
        let completion = CoreCompletion::new(&core);
        for sequence in 0..2 {
            assert_eq!(
                core.sequencer.allocate(BatchId {
                    database: fixture.config.storage.database,
                    operation: fixture.operation(0, sequence),
                }),
                sequence
            );
            core.sequencer
                .transition(sequence, Resolution::Allocated, Resolution::Ready)
                .unwrap();
        }
        let causal = BatchVersion {
            epoch: core.epoch,
            sequence: 1,
        };
        completion.failed(
            0,
            &[0, 1],
            LocalDbError::unknown(causal, "injected uncertain owner append"),
        );
        for sequence in 0..2 {
            let version = BatchVersion {
                epoch: core.epoch,
                sequence,
            };
            let error = core.sequencer.wait(version, Instant::now()).unwrap_err();
            assert_eq!(error.uncertain_version, Some(version));
            assert!(error.message().contains("injected uncertain owner append"));
            assert_eq!(
                core.sequencer.resolution(sequence),
                Some(Resolution::Unknown)
            );
            core.release_sequence(sequence).unwrap();
        }
        assert_eq!(core.sequencer.reuse_next(), 0);
        assert_eq!(core.publisher.lock().published_next, 0);
        assert!(core.ensure_healthy().is_err());
        assert!(core.reads.failed());
        assert!(core.first_failure.get().unwrap().contains("sequence: 1"));
        assert!(core.shutdown().is_err());
    }
}
