//! Admission waits by blocking reason (roadmap Phase 6 A0). Owned by the
//! admission budget mutex: every transition happens in a critical section the
//! waiting loop already takes. Waits are diagnostic; they change no policy.

use serde::{Deserialize, Serialize};

/// Granted replay tail over its hard bound (inapplicable in WAL replay).
pub(super) const WAIT_REPLAY_TAIL: u8 = 1;
pub(super) const WAIT_IDENTITY: u8 = 1 << 1;
pub(super) const WAIT_INDEX: u8 = 1 << 2;
/// Not checkpoint-bound: pending slots, bytes, partitions or FIFO order.
pub(super) const WAIT_OTHER: u8 = 1 << 3;
const HARD: u8 = WAIT_REPLAY_TAIL | WAIT_IDENTITY | WAIT_INDEX;
const REASONS: usize = 4;

/// One reason over a run. `union_ns` is wall time with at least one waiter
/// blocked by it (simultaneous reasons overlap; never add reason unions).
/// `waiter_ns`, `max_ns` cover completed waits only.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WaitReasonTotals {
    pub union_ns: u64,
    pub waiter_ns: u64,
    pub waits: u64,
    pub max_ns: u64,
    pub waiting_now: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HardWaitSnapshot {
    pub replay_tail: WaitReasonTotals,
    pub identity: WaitReasonTotals,
    pub index: WaitReasonTotals,
    pub other: WaitReasonTotals,
    /// Union over the checkpoint-hard reasons (replay tail, identity, index).
    pub any_hard_wait_ns: u64,
}

/// One waiter's current reason set, owned by its reservation.
#[derive(Default)]
pub(super) struct WaiterWait {
    bits: u8,
    since: [u64; REASONS],
}

#[derive(Default)]
pub(super) struct HardWaitTracker {
    active: [u64; REASONS],
    union_since: [u64; REASONS],
    totals: [WaitReasonTotals; REASONS],
    hard_active: u64,
    hard_since: u64,
    hard_union_ns: u64,
}

impl HardWaitTracker {
    /// Move `waiter` to `bits` at `now`. Zero ends the wait.
    pub(super) fn set(&mut self, waiter: &mut WaiterWait, bits: u8, now: u64) {
        for reason in 0..REASONS {
            let flag = 1 << reason;
            match (waiter.bits & flag != 0, bits & flag != 0) {
                (false, true) => {
                    waiter.since[reason] = now;
                    if self.active[reason] == 0 {
                        self.union_since[reason] = now;
                    }
                    self.active[reason] += 1;
                    self.totals[reason].waits += 1;
                }
                (true, false) => {
                    let waited = now.saturating_sub(waiter.since[reason]);
                    let totals = &mut self.totals[reason];
                    totals.waiter_ns = totals.waiter_ns.saturating_add(waited);
                    totals.max_ns = totals.max_ns.max(waited);
                    self.active[reason] -= 1;
                    if self.active[reason] == 0 {
                        totals.union_ns = totals
                            .union_ns
                            .saturating_add(now.saturating_sub(self.union_since[reason]));
                    }
                }
                _ => {}
            }
        }
        match (waiter.bits & HARD != 0, bits & HARD != 0) {
            (false, true) => {
                if self.hard_active == 0 {
                    self.hard_since = now;
                }
                self.hard_active += 1;
            }
            (true, false) => {
                self.hard_active -= 1;
                if self.hard_active == 0 {
                    self.hard_union_ns = self
                        .hard_union_ns
                        .saturating_add(now.saturating_sub(self.hard_since));
                }
            }
            _ => {}
        }
        waiter.bits = bits;
    }

    /// Cumulative totals; unions include intervals still open at `now`.
    pub(super) fn snapshot(&self, now: u64) -> HardWaitSnapshot {
        let reason = |index: usize| {
            let mut totals = self.totals[index];
            totals.waiting_now = self.active[index];
            if self.active[index] > 0 {
                totals.union_ns = totals
                    .union_ns
                    .saturating_add(now.saturating_sub(self.union_since[index]));
            }
            totals
        };
        HardWaitSnapshot {
            replay_tail: reason(0),
            identity: reason(1),
            index: reason(2),
            other: reason(3),
            any_hard_wait_ns: if self.hard_active > 0 {
                self.hard_union_ns
                    .saturating_add(now.saturating_sub(self.hard_since))
            } else {
                self.hard_union_ns
            },
        }
    }
}

#[cfg(test)]
mod tests;
