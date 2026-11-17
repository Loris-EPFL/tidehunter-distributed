//! Transport headroom and per-owner occupancy, enforced by owner workers.

use crate::distributed::tcp::APPEND_COMMAND_WINDOW;

pub(crate) const DEFAULT_OWNER_APPEND_OCCUPANCY: usize = 8;

pub(crate) fn validate_owner_occupancy(occupancy: usize) -> Result<(), &'static str> {
    if occupancy == 0 || occupancy > APPEND_COMMAND_WINDOW {
        return Err("owner append occupancy exceeds reserved-control window");
    }
    Ok(())
}

/// Charge of active plus worker-local queued groups. The session reactor also
/// checks retained-output credit; this bound reserves headroom for controls.
#[derive(Clone, Copy, Debug)]
pub(crate) struct AppendByteBudget {
    append_limit: usize,
}

impl AppendByteBudget {
    pub(crate) fn new(
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

    /// Match TcpClient::submit_append_many: 64 bytes per command and per batch,
    /// plus canonical encoded bytes. Arithmetic overflow never grants credit.
    pub(crate) fn add_batch_charge(charge: usize, encoded_len: usize) -> Option<usize> {
        charge.checked_add(encoded_len.checked_add(64)?)
    }

    pub(crate) fn admits(self, outstanding_charge: usize, group_charge: usize) -> bool {
        group_charge <= self.append_limit.saturating_sub(outstanding_charge)
            && outstanding_charge <= self.append_limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_budget_counts_queued_groups_and_keeps_control_headroom() {
        let group = [800, 800]
            .into_iter()
            .try_fold(64usize, AppendByteBudget::add_batch_charge)
            .unwrap();
        let reserve = 128;
        let budget = AppendByteBudget::new(group * 2 + reserve, reserve, group).unwrap();
        assert!(budget.admits(group, group));
        assert!(!budget.admits(group * 2, group));
        assert!(!budget.admits(usize::MAX, group));
        assert!(AppendByteBudget::new(group, 1, group).is_err());
        assert!(AppendByteBudget::new(group * 2, 0, group).is_err());
        assert!(AppendByteBudget::add_batch_charge(64, usize::MAX).is_none());
    }

    #[test]
    fn control_slot_is_reserved_for_every_valid_setting() {
        assert!(validate_owner_occupancy(0).is_err());
        assert!(validate_owner_occupancy(APPEND_COMMAND_WINDOW + 1).is_err());
        assert!(validate_owner_occupancy(APPEND_COMMAND_WINDOW).is_ok());
    }
}
