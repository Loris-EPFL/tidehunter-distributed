//! Repeatable target cleanup for one durably retired WAL inode.
use super::*;

impl LiveTargetManager {
    /// Finish the target side of one owner retirement whose durable
    /// `cleanup_pending` entry names exactly `(wal_path, device, inode)`. The
    /// caller verified that entry; this call never authorizes deletion. It
    /// requires no remaining export of the inode and an absent WAL path, syncs
    /// the WAL parent, removes the matching tombstone if present, and syncs
    /// the sealed-marker directory even when the tombstone was already gone,
    /// so a crash after either sync repeats safely. A marker with another
    /// identity is refused. Returns whether this call removed the tombstone
    /// (`false`: it was already gone).
    pub fn finish_retired_file(
        &mut self,
        wal_path: &Path,
        device: u64,
        inode: u64,
    ) -> io::Result<bool> {
        if self.poisoned {
            return Err(invalid("target manager is quarantined"));
        }
        if self.state.exports.iter().any(|export| {
            export.file.path == wal_path
                || (export.file.device, export.file.inode) == (device, inode)
        }) {
            return Err(invalid("retired WAL namespace is still registered"));
        }
        let marker = self.sealed_file(wal_path)?;
        if marker
            .as_ref()
            .is_some_and(|marker| (marker.device, marker.inode) != (device, inode))
        {
            return Err(invalid("sealed WAL marker names another inode"));
        }
        match fs::symlink_metadata(wal_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
            Ok(_) => return Err(invalid("retired WAL path still exists")),
        }
        let parent = wal_path
            .parent()
            .ok_or_else(|| invalid("WAL has no parent directory"))?;
        if !self.fixture {
            check_path(parent, false)?;
        }
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(parent)?
            .sync_all()?;
        let result = (|| {
            let mut removed = false;
            if marker.is_some() {
                match fs::remove_file(self.marker_path(wal_path)?) {
                    Ok(()) => removed = true,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            self.sealed_directory.sync_all()?;
            Ok(removed)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}
