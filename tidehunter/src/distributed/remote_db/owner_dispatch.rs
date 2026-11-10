//! Bounded coordinator-side owner append scheduling.
//!
//! Scalar and batch workloads can need different owner occupancy limits.
//! Neither the early-coalescing limit nor the transport maximum is a fixed optimum.
//! This policy bounds already-admitted work, not durability. Like Valkey 9.1.3
//! `beforeSleep`, draining processes ready input without waiting for new input;
//! the existing durable owner receipt still precedes publication and reply.
//!
//! The caller must account for every submitted command and its byte charge,
//! including commands queued to the worker, until its reply is harvested.
//! Transport admission independently accounts for its output buffers as well.
//! Group construction must still enforce native group byte/count and
//! storage-reference limits, preserve whole batches and use one owner per group.

use super::super::tcp::APPEND_COMMAND_WINDOW;

/// Local scalar/batch qualification selects eight rather than the transport
/// maximum. Independent-NVMe deployment tuning remains an explicit setting.
pub(super) const DEFAULT_OWNER_APPEND_OCCUPANCY: usize = 8;
const EARLY_COALESCE_OCCUPANCY: usize = 2;
const EARLY_COALESCE_MIN_BYTES: usize = 32 * 1024;

/// Coordinator accounting for the transport's append charge, including queued
/// worker commands. This does not replace the reactor's retained-output credit.
#[derive(Clone, Copy, Debug)]
pub(super) struct AppendByteBudget {
    append_limit: usize,
}

impl AppendByteBudget {
    /// `transport_bytes` is the transport's actual session byte limit;
    /// `control_reserve` is the nonzero headroom reserved by this policy.
    /// `largest_group_charge` must bound one valid complete group so admission
    /// cannot permanently strand a group that is legal under native limits.
    pub(super) fn new(
        transport_bytes: usize,
        control_reserve: usize,
        largest_group_charge: usize,
    ) -> Result<Self, &'static str> {
        let append_limit = transport_bytes
            .checked_sub(control_reserve)
            .ok_or("control reserve exceeds transport byte limit")?;
        if control_reserve == 0 || largest_group_charge == 0 || largest_group_charge > append_limit
        {
            return Err("append byte budget cannot fit a complete group and control reserve");
        }
        Ok(Self { append_limit })
    }

    /// Match `TcpClient::submit_append_many`: 64 bytes per request plus each
    /// encoded batch length and 64 bytes per batch. Overflow is rejected.
    #[cfg(test)]
    fn group_charge(batch_lengths: impl IntoIterator<Item = usize>) -> Option<usize> {
        batch_lengths
            .into_iter()
            .try_fold(64usize, Self::add_batch_charge)
    }

    pub(super) fn add_batch_charge(charge: usize, encoded_len: usize) -> Option<usize> {
        charge.checked_add(encoded_len.checked_add(64)?)
    }

    /// Both arguments use the transport charge above; outstanding includes
    /// all active and worker-queued commands. Check before draining staged work.
    pub(super) fn admits(self, outstanding_charge: usize, group_charge: usize) -> bool {
        group_charge <= self.append_limit.saturating_sub(outstanding_charge)
            && outstanding_charge <= self.append_limit
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DispatchMode {
    /// Permit a full group or a refill; hold small groups at the soft limit.
    Early,
    /// Dispatch any already-ready group without waiting for future arrivals.
    DrainReady,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct OwnerDispatchPolicy {
    max_occupancy: usize,
}

impl OwnerDispatchPolicy {
    /// Reject zero and values that consume the transport's control slot.
    pub(super) fn new(max_occupancy: usize) -> Result<Self, &'static str> {
        if max_occupancy == 0 || max_occupancy > APPEND_COMMAND_WINDOW {
            return Err("owner append occupancy exceeds reserved-control window");
        }
        Ok(Self { max_occupancy })
    }

    /// Fixed maximum rounds, with at most one group per owner in each round.
    /// Early processing uses a single round; a drain stops sooner if no owner
    /// dispatches. Neither mode waits for new arrivals.
    pub(super) fn rounds(self, mode: DispatchMode) -> usize {
        match mode {
            DispatchMode::Early => 1,
            DispatchMode::DrainReady => self.max_occupancy,
        }
    }

    /// Decide whether to construct a candidate group from currently staged
    /// complete batches. `staged_bytes` excludes the group header; pass its
    /// encoded size as `group_header_bytes`. `group_ready` is the existing
    /// native group count/byte readiness predicate, and `refill` means an owner
    /// completion was harvested during this progress pass.
    // One decision over the existing queue state; no extra scheduling object or allocation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn should_form_group(
        self,
        mode: DispatchMode,
        outstanding: usize,
        staged_count: usize,
        staged_bytes: usize,
        group_header_bytes: usize,
        group_ready: bool,
        refill: bool,
    ) -> bool {
        if staged_count == 0 || outstanding >= self.max_occupancy {
            return false;
        }
        match mode {
            DispatchMode::DrainReady => true,
            DispatchMode::Early => {
                (outstanding < EARLY_COALESCE_OCCUPANCY
                    || staged_bytes.saturating_add(group_header_bytes) >= EARLY_COALESCE_MIN_BYTES)
                    && (refill || group_ready)
            }
        }
    }

    /// A group can become full while forming it even if the initial readiness
    /// predicate was false (for example, the next complete batch will not fit).
    /// Never split that batch; leave it staged for the next round.
    pub(super) fn should_dispatch_group(
        self,
        mode: DispatchMode,
        refill: bool,
        group_full: bool,
    ) -> bool {
        matches!(mode, DispatchMode::DrainReady) || refill || group_full
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_budget_counts_queued_groups_and_keeps_control_headroom() {
        let group = AppendByteBudget::group_charge([800, 800]).unwrap();
        let reserve = 128;
        let budget = AppendByteBudget::new(group * 2 + reserve, reserve, group).unwrap();
        assert!(budget.admits(group, group));
        assert!(!budget.admits(group * 2, group));
        assert!(!budget.admits(usize::MAX, group));
        assert!(AppendByteBudget::new(group, 1, group).is_err());
        assert!(AppendByteBudget::new(group * 2, 0, group).is_err());
        assert!(AppendByteBudget::group_charge([usize::MAX]).is_none());
    }

    #[test]
    fn ready_tail_does_not_wait_for_a_full_group_or_a_completion() {
        let policy = OwnerDispatchPolicy::new(4).unwrap();
        assert!(!policy.should_form_group(DispatchMode::Early, 2, 1, 128, 16, false, false));
        assert!(policy.should_form_group(DispatchMode::DrainReady, 2, 1, 128, 16, false, false));
        assert!(policy.should_dispatch_group(DispatchMode::DrainReady, false, false));
        assert!(!policy.should_form_group(DispatchMode::DrainReady, 0, 0, 0, 16, false, false));
    }

    #[test]
    fn queued_and_active_commands_share_the_same_finite_limit() {
        for limit in [2, 4, 8, APPEND_COMMAND_WINDOW] {
            let policy = OwnerDispatchPolicy::new(limit).unwrap();
            // Occupancy counts both active commands and worker-queued commands.
            let active = 1;
            let queued = limit - active;
            assert!(!policy.should_form_group(
                DispatchMode::DrainReady,
                active + queued,
                1,
                1,
                16,
                true,
                true
            ));
            assert!(policy.should_form_group(
                DispatchMode::DrainReady,
                active + queued - 1,
                1,
                1,
                16,
                true,
                true
            ));
            assert_eq!(policy.rounds(DispatchMode::DrainReady), limit);
            assert_eq!(policy.rounds(DispatchMode::Early), 1);
        }
    }

    #[test]
    fn large_groups_cannot_bypass_the_hard_limit() {
        let policy = OwnerDispatchPolicy::new(2).unwrap();
        for mode in [DispatchMode::Early, DispatchMode::DrainReady] {
            assert!(!policy.should_form_group(mode, 2, 32, usize::MAX, 16, true, true));
            assert!(policy.should_form_group(mode, 1, 32, usize::MAX, 16, true, true));
        }
    }

    #[test]
    fn control_slot_is_reserved_for_every_valid_setting() {
        assert!(OwnerDispatchPolicy::new(0).is_err());
        assert!(OwnerDispatchPolicy::new(APPEND_COMMAND_WINDOW + 1).is_err());
        assert!(OwnerDispatchPolicy::new(APPEND_COMMAND_WINDOW).is_ok());
    }

    #[test]
    fn fair_rounds_visit_each_ready_owner_once_before_revisiting() {
        let policy = OwnerDispatchPolicy::new(2).unwrap();
        let mut occupancy = [0, 0, 0];
        let mut staged = [4, 1, 4];
        let mut order = Vec::new();
        for _ in 0..policy.rounds(DispatchMode::DrainReady) {
            for owner in 0..occupancy.len() {
                if policy.should_form_group(
                    DispatchMode::DrainReady,
                    occupancy[owner],
                    staged[owner],
                    1,
                    16,
                    false,
                    false,
                ) {
                    occupancy[owner] += 1;
                    staged[owner] -= 1;
                    order.push(owner);
                }
            }
        }
        assert_eq!(order, [0, 1, 2, 0, 2]);
        assert_eq!(staged[1], 0);
    }
}
