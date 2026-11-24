//! Lease-lapse protection of one authority epoch: the owner side of the
//! coordinator lease fail-stop (protocol 37).
//!
//! This module owns the sequence of gates a lapse protection takes, not the
//! target work: `NodeInventory::protect_live_targets` releases the inventory
//! across its configfs and controller calls.

use super::super::TcpError;
use super::{InventoryAccess, LifecycleSlot, NodeInventory, live_target};

/// Write-protect the writable exports of `epoch` after its coordinator lease
/// lapsed. The caller holds the live-control gate, the lifecycle gate and the
/// inventory; the target work releases the inventory.
pub(in crate::distributed::tcp) fn protect_lapsed_epoch<H: InventoryAccess>(
    inventory: &mut H,
    slot: &mut LifecycleSlot,
    epoch: u64,
) -> Result<(), TcpError> {
    if let Some(target) = live_target(inventory, slot)? {
        NodeInventory::protect_live_targets(inventory, target, epoch)?;
    }
    Ok(())
}
