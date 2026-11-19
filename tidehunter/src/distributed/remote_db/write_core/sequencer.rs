use super::super::{CompleteBatch, MutationVersion, NativePublicationRouting};
use super::Dependency;
use crate::distributed::storage::{DurableBatch, LocalDbError};
use crate::distributed::types::{BatchId, BatchVersion, PayloadDigest};
use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const SHIFT: u32 = 5;
const RESOLUTION: u64 = 7;
const RELEASED: u64 = 8;
const FINALIZED: u64 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u64)]
pub(super) enum Resolution {
    Allocated = 1,
    Ready = 2,
    Durable = 3,
    Published = 4,
    Aborted = 5,
    Unknown = 6,
}

/// Prepared before READY, with no publisher or index-writer lock held. The
/// owner worker receives its own batch handle; this payload stays with the
/// logical generation until its authenticated native-index publication.
pub(super) struct SlotPublication {
    pub(super) batch: CompleteBatch,
    pub(super) routing: NativePublicationRouting,
    pub(super) metadata_charge: usize,
    pub(super) last_mutation: MutationVersion,
}

pub(super) struct SlotData {
    pub(super) id: BatchId,
    pub(super) digest: Option<PayloadDigest>,
    pub(super) predecessors: Vec<u64>,
    pub(super) keys: Vec<Dependency>,
    pub(super) receipt: Option<DurableBatch>,
    pub(super) publication: Option<SlotPublication>,
    pub(super) last_mutation: Option<MutationVersion>,
    pub(super) outcome: Option<Result<BatchVersion, LocalDbError>>,
}

pub(super) struct Slot {
    state: AtomicU64,
    pub(super) data: Mutex<SlotData>,
    pub(super) done: Condvar,
}

/// Admission covers reserved as well as allocated generations. Only a resolved,
/// caller-released AND resolver-finalized generation can return its slot credit.
/// `reuse_next` is exclusive: the first sequence not yet retired, including 0.
#[repr(align(64))]
pub(super) struct Sequencer {
    next: AtomicU64,
    slots: Box<[Slot]>,
    reuse_next: AtomicU64,
}

impl Sequencer {
    pub(super) fn new(capacity: usize) -> Self {
        Self::with_next(capacity, 0)
    }

    pub(super) fn with_next(capacity: usize, next: u64) -> Self {
        assert!(capacity != 0, "validated nonzero pending capacity");
        assert!(next < u64::MAX >> SHIFT, "valid initial sequence",);
        Self {
            next: AtomicU64::new(next),
            slots: (0..capacity)
                .map(|_| Slot {
                    state: AtomicU64::new(0),
                    data: Mutex::new(SlotData {
                        id: BatchId {
                            database: [0; 16],
                            operation: [0; 16],
                        },
                        digest: None,
                        predecessors: Vec::new(),
                        keys: Vec::new(),
                        receipt: None,
                        outcome: None,
                        publication: None,
                        last_mutation: None,
                    }),
                    done: Condvar::new(),
                })
                .collect(),
            reuse_next: AtomicU64::new(next),
        }
    }

    pub(super) fn allocate(&self, id: BatchId) -> u64 {
        // Capacity 2: H reserves early; G retires seq1; X reserves its returned
        // credit and allocates seq2; H then allocates seq3 and reuses seq1's slot.
        // G reset -> credit return (Release) -> X reservation (Acquire) ->
        // X fetch_add (Release) -> H fetch_add (Acquire) -> H reuse. Allocation
        // therefore relays retirement even for an old held reservation.
        let sequence = self.next.fetch_add(1, Ordering::AcqRel);
        assert!(
            sequence < u64::MAX >> SHIFT,
            "sequence generation exhausted"
        );
        let slot = self.slot(sequence);
        let mut data = slot.data.lock();
        data.id = id;
        data.digest = None;
        data.predecessors.clear();
        data.keys.clear();
        data.receipt = None;
        data.outcome = None;
        data.publication = None;
        data.last_mutation = None;
        // Release publishes the matching generation only after clearing all old
        // metadata. Admission plus AcqRel allocation orders the previous reuse.
        slot.state.store(
            (sequence << SHIFT) | Resolution::Allocated as u64,
            Ordering::Release,
        );
        sequence
    }

    pub(super) fn slot(&self, sequence: u64) -> &Slot {
        &self.slots[sequence as usize % self.slots.len()]
    }

    pub(super) fn next(&self) -> u64 {
        // Acquire observes allocation before control operations take a cut.
        self.next.load(Ordering::Acquire)
    }

    pub(super) fn reuse_next(&self) -> u64 {
        // Acquire observes the retiring resolver's metadata completion.
        self.reuse_next.load(Ordering::Acquire)
    }

    pub(super) fn capacity(&self) -> usize {
        self.slots.len()
    }

    pub(super) fn generation(&self, sequence: u64) -> Option<u64> {
        // Empty state 0 is not sequence 0 ALLOCATED (which is 1). Inspect the
        // full atomic word before extracting a generation, including at wrap.
        let state = self.slot(sequence).state.load(Ordering::Acquire);
        (state != 0).then_some(state >> SHIFT)
    }

    pub(super) fn resolution(&self, sequence: u64) -> Option<Resolution> {
        // The generation and resolution must be sampled from one atomic word.
        let state = self.slot(sequence).state.load(Ordering::Acquire);
        if state >> SHIFT != sequence {
            return None;
        }
        match state & RESOLUTION {
            1 => Some(Resolution::Allocated),
            2 => Some(Resolution::Ready),
            3 => Some(Resolution::Durable),
            4 => Some(Resolution::Published),
            5 => Some(Resolution::Aborted),
            6 => Some(Resolution::Unknown),
            _ => None,
        }
    }

    pub(super) fn resolved(&self, sequence: u64) -> bool {
        // Acquire pairs with the publisher's resolution transition. A newer
        // generation proves this sequence resolved before returning its credit.
        let state = self.slot(sequence).state.load(Ordering::Acquire);
        state >> SHIFT > sequence || (state >> SHIFT == sequence && resolved(state))
    }

    pub(super) fn transition(
        &self,
        sequence: u64,
        expected: Resolution,
        resolution: Resolution,
    ) -> Result<(), LocalDbError> {
        let slot = self.slot(sequence);
        // SeqCst participates in the two-completer Dekker protocol below, while
        // CAS preserves a caller release that raced with resolution. An old
        // generation or an unexpected prior resolution must never be overwritten.
        for _ in 0..3 {
            let state = slot.state.load(Ordering::SeqCst);
            if state >> SHIFT != sequence || state & RESOLUTION != expected as u64 {
                return Err(LocalDbError::new("unexpected write slot resolution"));
            }
            if slot
                .state
                .compare_exchange(
                    state,
                    (state & !RESOLUTION) | resolution as u64,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(LocalDbError::new(
            "write slot resolution changed repeatedly",
        ))
    }

    pub(super) fn complete_release(&self, sequence: u64) -> usize {
        // The caller touches no slot data after this SeqCst release. Resolution
        // alone cannot reclaim: the resolver must finish the outcome and keys.
        self.slot(sequence)
            .state
            .fetch_or(RELEASED, Ordering::SeqCst);
        self.advance()
    }

    pub(super) fn complete_resolve(&self, sequence: u64) -> usize {
        // Called only after outcome write, notify_all, key removal and dropping
        // writer/publisher. FINALIZED prevents another publisher from reclaiming
        // a released-early slot while its original resolver still uses its data.
        self.slot(sequence)
            .state
            .fetch_or(FINALIZED, Ordering::SeqCst);
        self.advance()
    }

    fn advance(&self) -> usize {
        let mut retired = 0;
        for _ in 0..self.slots.len() {
            // Dekker: the last of caller RELEASED and resolver FINALIZED either
            // sees the other bit or the other completer sees its prefix change.
            // SeqCst on both store->load pairs prevents a stranded ready prefix;
            // Release/Acquire alone permits both completers to miss each other.
            // The exclusive bound represents the empty prefix even at seq 0.
            let sequence = self.reuse_next.load(Ordering::SeqCst);
            let state = self.slot(sequence).state.load(Ordering::SeqCst);
            if state >> SHIFT != sequence
                || !resolved(state)
                || state & (RELEASED | FINALIZED) != RELEASED | FINALIZED
            {
                break;
            }
            // Exactly the CAS winner returns this generation's credit. The
            // generation tag prevents ABA across arbitrary ring wraps.
            if self
                .reuse_next
                .compare_exchange(sequence, sequence + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                retired += 1;
            }
        }
        retired
    }

    pub(super) fn wait(
        &self,
        version: BatchVersion,
        deadline: Instant,
    ) -> Result<BatchVersion, LocalDbError> {
        let slot = self.slot(version.sequence);
        let mut data = slot.data.lock();
        loop {
            // The caller's unreleased credit pins this generation during every
            // predicate check and condvar wait; the mutex orders outcome writes.
            let state = slot.state.load(Ordering::Acquire);
            if state == 0 || state >> SHIFT != version.sequence {
                return Err(LocalDbError::unknown(
                    version,
                    "write slot generation changed",
                ));
            }
            if let Some(outcome) = &data.outcome {
                return match outcome {
                    Ok(version) => Ok(*version),
                    Err(error) => {
                        let mut copy = LocalDbError::new(error.message());
                        copy.uncertain_version = error.uncertain_version;
                        Err(copy)
                    }
                };
            }
            if slot.done.wait_until(&mut data, deadline).timed_out() && data.outcome.is_none() {
                return Err(LocalDbError::unknown(
                    version,
                    "write outcome not resolved before deadline",
                ));
            }
        }
    }
}

fn resolved(state: u64) -> bool {
    matches!(state & RESOLUTION, n if n == Resolution::Published as u64 || n == Resolution::Aborted as u64)
}

#[cfg(test)]
mod tests;
