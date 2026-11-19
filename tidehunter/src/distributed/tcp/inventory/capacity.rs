//! Owner side of the read-only R0 `FileInventoryPage` request.
//!
//! The inventory mutex is held only to resolve the stream and later to copy
//! per-file pin and binding counts; the directory scan runs with it released.
//! Nothing here installs a root, revokes a grant or unlinks a file.
use super::*;
use crate::distributed::capacity::{FileInventoryReply, MAX_FILE_INVENTORY_PAGE, TargetAccess};

impl NodeInventory {
    /// Same authentication as retirement: `authority_generation` must be the
    /// active fencing generation. `source_generation` is that generation or an
    /// older sealed one.
    pub(in crate::distributed::tcp) fn file_inventory_page(
        inventory: &Mutex<NodeInventory>,
        authority_generation: u64,
        source_generation: u64,
        after: Option<u64>,
        limit: u16,
    ) -> Result<FileInventoryReply, TcpError> {
        if limit == 0 || limit > MAX_FILE_INVENTORY_PAGE {
            return Err(TcpError::new("file inventory page exceeds bound"));
        }
        if source_generation > authority_generation {
            return Err(TcpError::new("file inventory of a future generation"));
        }
        let resolve = |guard: &NodeInventory| {
            if source_generation == authority_generation {
                guard.active(authority_generation)
            } else {
                guard.retirement_log(authority_generation, source_generation)
            }
        };
        let log = resolve(&inventory.lock())?;
        let (mut reply, log_read_pins) = log
            .file_inventory_page(after, usize::from(limit))
            .map_err(TcpError::component)?;
        let guard = inventory.lock();
        // A fence between the two holds makes this page stale, not an error
        // that an observer must distinguish from corruption.
        if resolve(&guard).is_err() {
            reply.stable = false;
        }
        reply.owner_incarnation = guard.incarnation;
        reply.value_wal_unlinks = guard.value_wal_unlinks;
        guard.fill_file_pins(&mut reply, source_generation, log_read_pins);
        Ok(reply)
    }

    fn fill_file_pins(
        &self,
        reply: &mut FileInventoryReply,
        source_generation: u64,
        log_read_pins: Option<u64>,
    ) {
        let bytes = self.config.partition.wal_file_bytes;
        let mut leases: BTreeMap<u64, u64> = BTreeMap::new();
        for lease in self.live_pins.values().flat_map(|(_, leases)| leases) {
            if lease.segment.epoch == source_generation {
                *leases.entry(lease.segment.file_index).or_default() += 1;
            }
        }
        // Record leases are the only holders of the log's read pin count. If
        // the totals differ, some pin is not attributable to a file.
        let attributed: u64 = leases.values().sum();
        let exact = log_read_pins == Some(attributed);
        let mut exports: BTreeMap<u64, u64> = BTreeMap::new();
        for (_, (source, file)) in self.live_export_pins.values() {
            if *source == source_generation {
                *exports.entry(*file).or_default() += 1;
            }
        }
        for row in &mut reply.rows {
            let file = row.file_start / bytes;
            row.read_pins = exact.then(|| leases.get(&file).copied().unwrap_or(0));
            row.export_pins = Some(exports.get(&file).copied().unwrap_or(0));
            row.target_access = match self.live_bindings.get(&(source_generation, file)) {
                Some((_, _, true)) => TargetAccess::Writable,
                Some((_, _, false)) => TargetAccess::ReadOnly,
                None => TargetAccess::None,
            };
        }
    }
}
