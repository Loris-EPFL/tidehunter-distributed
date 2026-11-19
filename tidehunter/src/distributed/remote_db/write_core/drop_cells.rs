//! Preflight of one fresh `DropCells` slot under the publication barrier.
//!
//! The slot registered the barrier at allocation (publisher held around the
//! allocate transaction). Before READY it waits for the contiguous lower
//! prefix, walks the ordered cell directory in bounded writer steps and takes
//! a nonblocking secondary index reservation for the masks. Every failure
//! here precedes the first WAL byte and aborts the slot without effect.
//! Higher sequences keep allocating, reaching their owners and becoming
//! durable meanwhile; only their publication waits for the range.

use super::super::{LocalDbError, Mutation};
use super::core::WriteCore;
use crate::distributed::native_index::{DropCellsTarget, drop_mask_charge};
use std::time::Instant;

/// Directory entries visited per index writer hold.
const PREFLIGHT_STEP: usize = 1024;

impl WriteCore {
    /// Park a `DropCells` caller until no other fresh range holds the barrier.
    /// Holds only the publisher mutex, which `floor_changed` releases while
    /// waiting; point admission never waits here.
    pub(super) fn wait_range_barrier(&self, deadline: Instant) -> Result<(), LocalDbError> {
        let mut publisher = self.publisher.lock();
        while publisher.range_barrier.is_some() {
            self.ensure_writable()?;
            if Instant::now() >= deadline {
                return Err(LocalDbError::new(
                    "DropCells deadline expired while an earlier range held the publication barrier",
                ));
            }
            self.floor_changed.wait_until(&mut publisher, deadline);
        }
        Ok(())
    }

    /// Wait until every sequence below `sequence` resolved, then collect the
    /// occupied cells of the range and reserve their mask charge on top of
    /// `metadata_charge`. Returns the targets and the slot's new charge. No
    /// publication can create a cell meanwhile: lower ones are resolved and
    /// higher ones are deferred by the barrier. Reaping may still remove a
    /// cell; staging skips it and checks the birth of every present cell.
    pub(super) fn prepare_drop_cells(
        &self,
        mutations: &[Mutation],
        sequence: u64,
        metadata_charge: usize,
        deadline: Instant,
    ) -> Result<(Vec<DropCellsTarget>, usize), LocalDbError> {
        let [
            Mutation::DropCells {
                keyspace,
                first_cell,
                last_cell,
            },
        ] = mutations
        else {
            return Err(LocalDbError::new(
                "DropCells must be the only member in a batch",
            ));
        };
        {
            let mut publisher = self.publisher.lock();
            loop {
                self.ensure_healthy()?;
                if publisher.published_next == sequence {
                    break;
                }
                if publisher.published_next > sequence {
                    return Err(LocalDbError::new(
                        "publication passed an unresolved DropCells slot",
                    ));
                }
                if Instant::now() >= deadline {
                    return Err(LocalDbError::new(
                        "DropCells deadline expired before its lower prefix resolved",
                    ));
                }
                self.floor_changed.wait_until(&mut publisher, deadline);
            }
        }
        let mut targets = Vec::new();
        let mut after = None;
        loop {
            self.ensure_healthy()?;
            // One bounded directory step per writer hold; no I/O, owner wait
            // or callback runs under the writer.
            let mut writer = self.index.writer();
            after = writer.drop_cells_targets_step(
                *keyspace,
                first_cell,
                last_cell,
                after.as_ref(),
                PREFLIGHT_STEP,
                &mut targets,
            );
            drop(writer);
            if after.is_none() {
                break;
            }
        }
        let charge = drop_mask_charge(targets.len()).map_err(LocalDbError::component)?;
        let total = metadata_charge
            .checked_add(charge)
            .ok_or_else(|| LocalDbError::new("DropCells exceeds index metadata capacity"))?;
        self.admission.reserve_index_bytes(charge)?;
        Ok((targets, total))
    }
}
