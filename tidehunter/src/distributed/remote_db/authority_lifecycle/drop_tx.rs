//! Coordinator drop transaction (`Db::drop_db` of a remote directory) and
//! quarantine reaping.
//!
//! 1. Persist `Dropping { generation }` (write-ahead intent) unless present.
//! 2. Drop nested authorities deepest first, lexically within one depth,
//!    each under its own descended namespace lock and zero-timeout `DbLock`.
//!    A busy, corrupt or failing child fails the parent with `AlreadyExists`
//!    and no parent owner request.
//! 3. Re-scan: any nested authority left fails the parent.
//! 4. Send the same `DropDatabase` to every owner, including owners that
//!    already acknowledged; every owner must acknowledge.
//! 5. Unlink everything but `LOCK` and the manifest, sync the directory,
//!    rename to `.tidehunter-drop-<db hex>`, sync the parent, then remove the
//!    quarantine (`LOCK` first, manifest last) and sync the parent again.
//!
//! A crash at any point leaves either the original directory with its
//! durable intent (the next open or drop resumes) or a quarantine that the
//! next lifecycle action on its parent reaps. Neither ever touches a fresh
//! database created at the original name after the rename.

use super::{
    AuthorityLifecycle, AuthorityStep, NamespaceGuard, NamespaceMode, connect_owner,
    highest_reservation, read_lifecycle, step,
};
use super::{CoordinatorIoCategory, read_authority_manifest};
use crate::distributed::remote_db::{AuthorityManifest, MANIFEST, persist_manifest};
use crate::distributed::storage::LocalDbError;
use crate::distributed::tcp::host_identity;
use crate::lock::DbLock;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

const QUARANTINE_PREFIX: &str = ".tidehunter-drop-";

pub(super) fn is_quarantine_name(name: &str) -> bool {
    name.strip_prefix(QUARANTINE_PREFIX)
        .and_then(host_identity::parse_database_hex)
        .is_some()
}

fn with_path(error: io::Error, operation: &str, path: &Path) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("{operation} {}: {error}", path.display()),
    )
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| with_path(error, "sync directory", path))
}

fn parent_of(path: &Path) -> io::Result<&Path> {
    path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("database {} has no parent directory", path.display()),
        )
    })
}

/// `Db::drop_db` entry: classify native versus remote **under the target's
/// exclusive namespace lock**, so no concurrent open can create (or finish
/// creating) an authority marker between the check and the deletion.
/// Authority manifests are only ever created while that lock is held.
///
/// - Marker present: the coordinated remote drop (owners, nested children).
/// - No marker: the directory is native or plain. If any remote authority
///   exists below it, refuse with `AlreadyExists`: a native recursive removal
///   would bypass the children's namespace locks and owner cleanup. Otherwise
///   run `native` while the namespace lock is still held (child opens need a
///   shared lock on this directory, so none can start meanwhile).
///
/// Missing path: delegated to `native`, unchanged native semantics.
pub(crate) fn drop_db(path: &Path, native: impl FnOnce(&Path) -> io::Result<()>) -> io::Result<()> {
    let canonical = match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return native(path),
        Err(error) => return Err(error),
    };
    reap_quarantines(parent_of(&canonical)?)?;
    let namespace = NamespaceGuard::acquire(&canonical, NamespaceMode::Drop)?;
    step(AuthorityStep::DropAfterNamespace)?;
    // Recheck under the lock, without following links or hiding errors.
    let manifest_path = namespace.target().join(MANIFEST);
    match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "authority manifest is not a regular file",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(child) = nested_authorities(namespace.target())?.into_iter().next() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "cannot drop {} natively: it contains remote database {}; drop that first",
                        namespace.target().display(),
                        child.display()
                    ),
                ));
            }
            return native(namespace.target());
        }
        Err(error) => {
            return Err(with_path(
                error,
                "inspect authority manifest",
                &manifest_path,
            ));
        }
    }
    let lock = DbLock::acquire(namespace.target(), Duration::ZERO)?;
    let manifest = read_authority_manifest(&manifest_path).map_err(local_error)?;
    drop_locked(namespace.target(), lock, manifest, &namespace)
}

/// Remote-only drop (tests and tooling): a directory without an authority
/// marker is `NotFound`, never removed natively.
#[cfg(test)]
pub(crate) fn drop_remote(path: &Path) -> io::Result<()> {
    drop_db(path, |path| {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no remote database at {}", path.display()),
        ))
    })
}

fn local_error(error: LocalDbError) -> io::Error {
    io::Error::new(error.kind(), error.to_string())
}

/// The drop transaction for an authority whose namespace and `DbLock` the
/// caller holds. Consumes the lock: the directory is gone on success.
pub(super) fn drop_locked(
    path: &Path,
    lock: DbLock,
    mut manifest: AuthorityManifest,
    namespace: &NamespaceGuard,
) -> io::Result<()> {
    if manifest.format != super::AUTHORITY_FORMAT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported distributed authority format {} (expected {})",
                manifest.format,
                super::AUTHORITY_FORMAT
            ),
        ));
    }
    super::super::fencing_generations::validate(&manifest).map_err(local_error)?;
    let generation = match manifest.lifecycle {
        Some(AuthorityLifecycle::Dropping { generation }) => generation,
        Some(_) => {
            let generation = highest_reservation(&manifest);
            manifest.lifecycle = Some(AuthorityLifecycle::Dropping { generation });
            persist_manifest(path, &manifest, CoordinatorIoCategory::Open).map_err(local_error)?;
            generation
        }
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "authority manifest has no lifecycle",
            ));
        }
    };
    step(AuthorityStep::DropAfterIntent)?;
    for child in nested_authorities(path)? {
        drop_child(namespace, &child).map_err(|error| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "cannot drop {}: nested database {} is not dropped: {error}",
                    path.display(),
                    child.display()
                ),
            )
        })?;
    }
    if let Some(child) = nested_authorities(path)?.into_iter().next() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "cannot drop {}: nested database {} appeared",
                path.display(),
                child.display()
            ),
        ));
    }
    step(AuthorityStep::DropAfterChildren)?;
    drop_owners(&manifest, generation)?;
    remove_local(path, lock, &manifest)
}

/// One nested authority: its own namespace lock, zero-timeout `DbLock`,
/// manifest and intent. Missing or already completed: nothing to do.
fn drop_child(parent: &NamespaceGuard, child: &Path) -> io::Result<()> {
    let namespace = parent.descend(child)?;
    let lock = DbLock::acquire(namespace.target(), Duration::ZERO)?;
    let manifest_path = child.join(MANIFEST);
    match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "nested authority manifest is not a regular file",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    let manifest = read_authority_manifest(&manifest_path).map_err(local_error)?;
    // Its own children were dropped first (deepest-first order).
    drop_locked(child, lock, manifest, &namespace)
}

/// Nested authority directories below `root`, deepest first and lexically
/// within one depth. No symlink is followed; quarantines are skipped.
fn nested_authorities(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![(root.to_path_buf(), 0usize)];
    while let Some((directory, depth)) = pending.pop() {
        for entry in
            fs::read_dir(&directory).map_err(|error| with_path(error, "scan", &directory))?
        {
            let entry = entry?;
            // `DirEntry::file_type` does not follow symlinks.
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            if name.to_str().is_some_and(is_quarantine_name) {
                continue;
            }
            let child = entry.path();
            if read_lifecycle(&child)?.is_some() {
                found.push((depth + 1, child.clone()));
            }
            pending.push((child, depth + 1));
        }
    }
    found.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    Ok(found.into_iter().map(|(_, path)| path).collect())
}

/// Send the same terminal drop to every owner. A failed owner does not stop
/// the others; the first failure is returned and the intent stays durable.
fn drop_owners(manifest: &AuthorityManifest, generation: u64) -> io::Result<()> {
    let control = manifest.owner_control.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "dropping authority has no owner control data",
        )
    })?;
    let database = manifest.storage.database;
    let mut first_error = None;
    for (index, endpoint) in manifest.endpoints.iter().enumerate() {
        let result = (|| -> io::Result<()> {
            let principal = *control.principals.get(&endpoint.partition).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("owner {} has no recorded principal", endpoint.partition),
                )
            })?;
            let (mut client, _) = connect_owner(control, endpoint).map_err(local_error)?;
            client
                .drop_database(database, principal, generation)
                .map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("owner {} DropDatabase: {error}", endpoint.partition),
                    )
                })?;
            Ok(())
        })();
        match result {
            // A boundary failure models a crash right after this ACK: no
            // later owner is contacted.
            Ok(()) => step(AuthorityStep::DropAfterOwner(index))?,
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn remove_entry(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_dir() {
        // Does not follow symlinks inside.
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
    .map_err(|error| with_path(error, "remove", path))
}

/// Local detachment after every owner acknowledged.
fn remove_local(path: &Path, lock: DbLock, manifest: &AuthorityManifest) -> io::Result<()> {
    for entry in fs::read_dir(path).map_err(|error| with_path(error, "scan", path))? {
        let entry = entry?;
        let name = entry.file_name();
        if name == "LOCK" || name == MANIFEST {
            continue;
        }
        remove_entry(&entry.path())?;
    }
    sync_directory(path)?;
    step(AuthorityStep::DropAfterUnlink)?;
    let parent = parent_of(path)?.to_path_buf();
    let quarantine = parent.join(format!(
        "{QUARANTINE_PREFIX}{}",
        host_identity::database_hex(&manifest.storage.database)
    ));
    if fs::symlink_metadata(&quarantine).is_ok() {
        reap_one(&quarantine, false)?;
    }
    // Never unlink LOCK while the original name still denotes this database.
    fs::rename(path, &quarantine).map_err(|error| with_path(error, "quarantine", path))?;
    step(AuthorityStep::DropAfterRename)?;
    sync_directory(&parent)?;
    step(AuthorityStep::DropAfterParentSync)?;
    drop(lock);
    // Our namespace guard still locks this inode exclusively.
    reap_one(&quarantine, true)?;
    step(AuthorityStep::DropAfterQuarantine)?;
    Ok(())
}

/// Remove quarantines directly under `directory`; each is locked
/// exclusively first. A quarantine still in use is left for a later action.
pub(crate) fn reap_quarantines(directory: &Path) -> io::Result<()> {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(with_path(error, "scan", directory)),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() && entry.file_name().to_str().is_some_and(is_quarantine_name)
        {
            match reap_one(&entry.path(), false) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

/// Remove one strictly named quarantine: only `LOCK` and a manifest whose
/// lifecycle is `Dropping` with the matching identity (or nothing) may
/// remain. `LOCK` first, manifest last, then the directory; sync the parent.
fn reap_one(quarantine: &Path, already_locked: bool) -> io::Result<()> {
    let name = quarantine
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(QUARANTINE_PREFIX))
        .and_then(host_identity::parse_database_hex)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "not a quarantine name"))?;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(quarantine)
        .map_err(|error| with_path(error, "open quarantine", quarantine))?;
    match directory.try_lock() {
        Ok(()) => {}
        // The caller's own guard holds it (a different open file).
        Err(TryLockError::WouldBlock) if already_locked => {}
        Err(TryLockError::WouldBlock) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("quarantine {} is in use", quarantine.display()),
            ));
        }
        Err(TryLockError::Error(error)) => return Err(error),
    }
    for entry in fs::read_dir(quarantine)? {
        let entry = entry?;
        let file = entry.file_name();
        let allowed =
            file == "LOCK" || file == MANIFEST || file == format!(".{MANIFEST}.pending").as_str();
        if !allowed || !entry.file_type()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "quarantine {} holds unexpected {file:?}",
                    quarantine.display()
                ),
            ));
        }
    }
    if let Some((lifecycle, database)) = read_lifecycle(quarantine)?
        && (!matches!(lifecycle, Some(AuthorityLifecycle::Dropping { .. }))
            || database != Some(name))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "quarantine {} does not hold a matching drop",
                quarantine.display()
            ),
        ));
    }
    for file in ["LOCK", &format!(".{MANIFEST}.pending"), MANIFEST] {
        match fs::remove_file(quarantine.join(file)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(with_path(error, "remove", &quarantine.join(file))),
        }
    }
    fs::remove_dir(quarantine).map_err(|error| with_path(error, "remove", quarantine))?;
    drop(directory);
    sync_directory(parent_of(quarantine)?)
}
