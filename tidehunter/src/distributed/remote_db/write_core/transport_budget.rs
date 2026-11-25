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
mod tests;
