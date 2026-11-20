//! Preflight of one fresh `DropCells` slot under the publication barrier.
//!
//! The slot registers the barrier at allocation (publisher held around the
//! allocate transaction) and waits for the contiguous lower prefix before
//! READY. Nothing is enumerated: the native index applies the range at the
//! slot's version, so a higher put into the range cannot publish ahead of the
//! drop and a lower put is already published. Higher sequences keep
//! allocating, reaching their owners and becoming durable meanwhile; only
//! their publication waits for the range.

use super::super::LocalDbError;
use super::core::WriteCore;
use std::time::Instant;

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

    /// Wait until every sequence below `sequence` resolved, so the range is
    /// published immediately after a prefix whose effects all precede it in
    /// version order. No publication can then interleave a point write inside
    /// the range between the prefix and the drop. A failure here precedes the
    /// first WAL byte and aborts the slot without effect.
    pub(super) fn wait_drop_cells_prefix(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> Result<(), LocalDbError> {
        let mut publisher = self.publisher.lock();
        loop {
            self.ensure_healthy()?;
            if publisher.published_next == sequence {
                return Ok(());
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
}
