//! Publication of live read leases under the lifecycle gates.
//!
//! Moved from `control.rs` to stay under that file's length baseline; the
//! lease type and the inline-export kind are absolute and the published-lease
//! alias is parent-relative because this module is one level deeper. Lock
//! order: live-control gate, lifecycle gate, inventory; every export below
//! releases the inventory across its journal syncs and configfs work.

use super::*;

impl ControlContext {
    /// `kind` names the request (resolve, recovery or replay) that counts an
    /// export created inline here.
    pub(super) fn publish_live_leases(
        &self,
        epoch: u64,
        leases: Vec<crate::distributed::nvmeof::live_storage::LiveRecordLease>,
        kind: crate::distributed::tcp::inventory::InlineExport,
    ) -> Result<super::PublishedLiveLeases, TcpError> {
        let metadata: Vec<_> = leases
            .iter()
            .map(|lease| (lease.segment.clone(), lease.offset, u64::from(lease.len)))
            .collect();
        let gate = self.live_control_gate.lock();
        if !self.readers.read().active_valid(epoch, &self.terminal) {
            return Err(TcpError::new("stale live read authority"));
        }
        // Lock order: live-control gate, lifecycle gate, inventory. Each export below
        // releases the inventory across its journal syncs and configfs work.
        let mut lifecycle = self.lifecycle.lock();
        let mut inventory = self.inventory.lock();
        inventory.active(epoch)?;
        let target = lifecycle
            .as_mut()
            .ok_or_else(|| TcpError::new("live target not configured"))?;
        if inventory.live_pins.len() >= 1024 {
            return Err(TcpError::new("live pin capacity reached"));
        }
        let token = (0..16)
            .find_map(|_| {
                let candidate = rand::random::<[u8; 16]>();
                (candidate != [0; 16] && !inventory.live_pins.contains_key(&candidate))
                    .then_some(candidate)
            })
            .ok_or_else(|| TcpError::new("live pin token allocation failed"))?;
        inventory.live_pins.insert(token, (epoch, leases));
        if let Some(events) = inventory.r1_events.as_mut() {
            events.pin(
                "pin_held",
                "record",
                token,
                metadata.iter().map(|(segment, _, _)| {
                    (
                        segment.epoch,
                        segment.file_index,
                        Some((segment.device, segment.inode)),
                    )
                }),
                "resolve",
            );
        }
        let mut locations = Vec::with_capacity(metadata.len());
        for (segment, offset, len) in metadata {
            let binding = match inventory.live_segment_writable(epoch, &segment) {
                Ok(writable) => NodeInventory::publish_live_segment(
                    &mut inventory,
                    target,
                    &segment,
                    writable,
                    kind,
                ),
                Err(error) => Err(error),
            };
            match binding {
                Ok(binding) => locations.push((segment, binding, offset, len)),
                Err(error) => {
                    // Publication can fail after creating a kernel capability.
                    // Keep its in-memory epoch pin and file leases quarantined;
                    // the durable target registry is drained on restart.
                    drop(inventory);
                    drop(lifecycle);
                    drop(gate);
                    self.poison_readers();
                    return Err(TcpError::new(format!(
                        "live read export failed; outcome unknown: {error}"
                    )));
                }
            }
        }
        Ok((token, locations))
    }
}
