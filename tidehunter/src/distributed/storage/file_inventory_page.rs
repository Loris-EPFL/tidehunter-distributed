//! Read-only owner WAL file inventory, the page the coordinator's dead-file
//! retirement pass reads.
//!
//! Neither `State` nor any inventory lock is held across `readdir`/`lstat`:
//! the stream identity and cursors are snapshotted before enumeration and
//! rechecked after it, and a changed snapshot marks the page unstable. The
//! page never installs a root, unlinks, revokes or validates by rewriting.
use super::*;
use crate::distributed::remote_db::file_inventory::{
    FileInventoryReply, FileUseState, TargetAccess, WalFileObservation,
};

/// Stream state that classifies files. Compared before and after enumeration.
#[derive(Clone, Debug, Eq, PartialEq)]
struct StreamSnapshot {
    healthy: bool,
    live: bool,
    sealed: bool,
    /// Allocator position (`WalWriter::position`), if a writer is open.
    allocator: Option<u64>,
    /// Files with an open or preleased write extent.
    leased: Vec<u64>,
    /// File index and adopted end of the open extent.
    open_adopted: Option<(u64, u64)>,
    processed_end: u64,
    root: Option<(u64, u64, u64)>,
    /// Retired files of the WAL root; `None` without a root, when no file has
    /// been retired (the root is the only retirement record).
    retired: Option<RetiredFiles>,
}

impl StreamSnapshot {
    /// Same classification inputs; the allocator may advance within its file.
    fn same_classification(&self, other: &Self, wal_file_bytes: u64) -> bool {
        self.healthy == other.healthy
            && self.live == other.live
            && self.sealed == other.sealed
            && self.allocator.map(|at| at / wal_file_bytes)
                == other.allocator.map(|at| at / wal_file_bytes)
            && self.leased == other.leased
            && self.root.map(|root| root.0) == other.root.map(|root| root.0)
            && self.retired == other.retired
    }
}

impl PartitionLog {
    fn capacity_snapshot(&self) -> StreamSnapshot {
        let state = self.state.lock();
        let (live, leased, open_adopted) = {
            let mut leased = Vec::with_capacity(2);
            let extents = [&state.open_live_extent, &state.next_live_extent];
            leased.extend(
                extents
                    .iter()
                    .filter_map(|open| open.as_ref())
                    .map(|open| open.extent.segment.file_index),
            );
            leased.sort_unstable();
            leased.dedup();
            let open_adopted = state
                .open_live_extent
                .as_ref()
                .map(|open| (open.extent.segment.file_index, open.adoption.adopted_end));
            (state.live, leased, open_adopted)
        };
        StreamSnapshot {
            healthy: state.poison.is_none(),
            live,
            sealed: state.sealed,
            allocator: state.writer.as_ref().map(WalWriter::position),
            leased,
            open_adopted,
            processed_end: state.appended_end,
            root: state
                .wal_root
                .as_ref()
                .map(|root| (root.generation, root.scan_from, root.synced_end)),
            retired: state.wal_root.as_ref().map(|root| root.retired.clone()),
        }
    }

    /// One sorted page of owner WAL files after `after`, at most `limit` rows,
    /// including retired files whose directory entry remains. Pin and target
    /// fields are `None`/`Unknown` here; the node inventory fills them. Also
    /// returns the stream's live read pin total for exact pin attribution.
    pub(crate) fn file_inventory_page(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<(FileInventoryReply, Option<u64>), StorageError> {
        if limit == 0
            || limit
                > usize::from(
                    crate::distributed::remote_db::file_inventory::MAX_FILE_INVENTORY_PAGE,
                )
        {
            return Err(StorageError::Invalid(
                "invalid WAL file inventory page limit",
            ));
        }
        let config = self.config();
        let bytes = config.wal_file_bytes;
        let before = self.capacity_snapshot();
        // (offset, path) of the smallest `limit` canonical WAL names.
        let mut files: Vec<(u64, PathBuf)> = Vec::with_capacity(limit);
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let Some((id, _)) = wal_file_entry(&entry, config)? else {
                continue;
            };
            let offset = id
                .checked_mul(bytes)
                .ok_or(StorageError::Invalid("WAL offset overflow"))?;
            if after.is_some_and(|after| offset <= after) {
                continue;
            }
            match files.binary_search_by_key(&offset, |file| file.0) {
                Ok(_) => return Err(StorageError::Invalid("duplicate WAL file")),
                Err(index) if index < limit => {
                    files.insert(index, (offset, entry.path()));
                    files.truncate(limit);
                }
                Err(_) => {}
            }
        }
        // Validation and every counter come from one descriptor opened with
        // O_NOFOLLOW, so a replaced path cannot mix two inodes. O_NONBLOCK
        // keeps a substituted FIFO from blocking; the type check rejects it.
        let mut observed: Vec<(u64, u64, u64, u64, u64)> = Vec::with_capacity(files.len());
        let mut vanished = false;
        for (offset, path) in files {
            use std::os::unix::fs::OpenOptionsExt;
            let file = match OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                .open(&path)
            {
                Ok(file) => file,
                // Gone since readdir: the page is stale, not corrupt.
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    vanished = true;
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let metadata = file.metadata()?;
            if !metadata.file_type().is_file() || metadata.len() > bytes {
                return Err(StorageError::Invalid("invalid WAL file"));
            }
            let allocated = metadata
                .blocks()
                .checked_mul(512)
                .ok_or(StorageError::Invalid("WAL allocation overflow"))?;
            observed.push((
                offset,
                metadata.dev(),
                metadata.ino(),
                metadata.len(),
                allocated,
            ));
        }
        let after_snapshot = self.capacity_snapshot();
        let stable = before.same_classification(&after_snapshot, bytes) && !vanished;
        // Seals are only added between snapshots, and each is durable: an
        // exact inode match reports a valid seal even if taken later.
        let seals = self.state.lock().wal_root.clone();
        let mut rows = Vec::with_capacity(observed.len());
        for (offset, device, inode, logical_bytes, allocated_bytes) in observed {
            let retired = after_snapshot
                .retired
                .as_ref()
                .is_some_and(|retired| retired.contains(offset));
            let seal = seals.as_ref().and_then(|root| {
                root.sealed_files
                    .binary_search_by_key(&offset, |seal| seal.start)
                    .ok()
                    .map(|index| root.sealed_files[index])
                    .filter(|seal| seal.device == device && seal.inode == inode)
            });
            rows.push(classify(
                &after_snapshot,
                bytes,
                retired,
                WalFileObservation {
                    file_start: offset,
                    device,
                    inode,
                    logical_bytes,
                    allocated_bytes,
                    state: FileUseState::Unknown,
                    allocator_past: false,
                    write_lease: false,
                    validated_through: None,
                    read_pins: None,
                    target_access: TargetAccess::Unknown,
                    seal,
                },
            )?);
        }
        let read_pins = u64::try_from(self.live_read_pins.load(Ordering::Acquire)).ok();
        Ok((
            FileInventoryReply {
                partition: config.partition,
                source_generation: config.inventory.epoch,
                owner_incarnation: [0; 16],
                owner_root_generation: after_snapshot.root.map(|root| root.0),
                wal_scan_from: after_snapshot.root.map(|root| root.1),
                wal_synced_end: after_snapshot.root.map(|root| root.2),
                processed_end: after_snapshot.processed_end,
                stable,
                value_wal_unlinks: 0,
                rows,
            },
            read_pins,
        ))
    }
}

/// Classify one file from allocator and lease state, never from its length.
fn classify(
    snapshot: &StreamSnapshot,
    wal_file_bytes: u64,
    retired: bool,
    mut row: WalFileObservation,
) -> Result<WalFileObservation, StorageError> {
    let index = row.file_start / wal_file_bytes;
    let file_end = row
        .file_start
        .checked_add(wal_file_bytes)
        .ok_or(StorageError::Invalid("WAL file bound overflow"))?;
    if retired {
        row.state = FileUseState::RetiredCleanupPending;
        row.allocator_past = true;
        return Ok(row);
    }
    if !snapshot.healthy {
        return Ok(row);
    }
    row.write_lease = snapshot.leased.binary_search(&index).is_ok();
    match snapshot.allocator {
        Some(position) => {
            row.allocator_past = index < position / wal_file_bytes;
            row.state = if row.write_lease {
                FileUseState::Leased
            } else if row.file_start >= position {
                FileUseState::Lookahead
            } else {
                FileUseState::Used
            };
            // Leased adoption validates every frame below its cursor, and
            // extent close validates the remaining skip marker and zeros before
            // releasing the allocation: a passed, unleased live file is wholly
            // validated. An unleased non-live file has no such witness.
            row.validated_through = if snapshot.live && row.allocator_past && !row.write_lease {
                Some(file_end)
            } else {
                snapshot
                    .open_adopted
                    .filter(|(file, _)| *file == index)
                    .map(|(_, adopted)| adopted)
            };
        }
        None if snapshot.sealed => {
            // A sealed stream's whole physical prefix was validated by its seal
            // or recovery; nothing can be allocated in it again.
            row.allocator_past = true;
            row.state = FileUseState::Used;
            row.validated_through = Some(file_end);
        }
        None => {}
    }
    Ok(row)
}
