//! `NamespaceGuard`: cold lifecycle locks over nested authority paths.
//!
//! Real directory file descriptors are locked top-down: every ancestor with
//! `File::try_lock_shared`, the target with `File::try_lock` (exclusive).
//! These are `flock` locks on open file descriptions, so two threads of one
//! process conflict exactly like two processes. They are separate from, and
//! always taken before, the target's native `DbLock`.
//!
//! - Open holds the guard through manifest/open setup only. A serving parent
//!   therefore never blocks a nested child's open, while a child's open in
//!   progress (shared on the parent) excludes the parent's drop (exclusive
//!   on the parent), and vice versa.
//! - Drop holds the guard for the whole transaction and acquires nested
//!   children with [`NamespaceGuard::descend`], inheriting its own exclusive
//!   lock instead of retaking the parent.
//! - Opening under an ancestor whose manifest is `Dropping` is refused before
//!   any state changes.
//!
//! Every directory is opened `O_NOFOLLOW` and its `(device, inode)` is
//! revalidated against the path after locking. Conflicts are `AlreadyExists`.

use super::{AuthorityLifecycle, read_lifecycle};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NamespaceMode {
    /// Create the target directory if it is missing; refuse under a
    /// dropping ancestor.
    Open,
    /// The target must exist.
    Drop,
}

/// Held namespace locks. Dropping the guard releases them.
pub(crate) struct NamespaceGuard {
    target: PathBuf,
    /// Ancestors (shared) then the target (exclusive), in lock order.
    _held: Vec<File>,
}

fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|error| io::Error::new(error.kind(), format!("open {}: {error}", path.display())))
}

/// The descriptor still names `path`: no rename or symlink swap raced us.
fn revalidate(file: &File, path: &Path) -> io::Result<()> {
    let held = file.metadata()?;
    let named = fs::symlink_metadata(path)?;
    if held.dev() != named.dev() || held.ino() != named.ino() || !named.file_type().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{} changed while it was being locked", path.display()),
        ));
    }
    Ok(())
}

fn busy(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "database namespace {} is held by another lifecycle operation",
            path.display()
        ),
    )
}

fn lock(file: &File, path: &Path, exclusive: bool) -> io::Result<()> {
    let result = if exclusive {
        file.try_lock()
    } else {
        file.try_lock_shared()
    };
    match result {
        Ok(()) => revalidate(file, path),
        Err(TryLockError::WouldBlock) => Err(busy(path)),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

impl NamespaceGuard {
    pub(crate) fn acquire(path: &Path, mode: NamespaceMode) -> io::Result<Self> {
        let name = match path.components().next_back() {
            Some(Component::Normal(name)) => name.to_owned(),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("database path {} has no final name", path.display()),
                ));
            }
        };
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        // Same path-based NotFound as native open's canonicalization.
        let parent = parent.canonicalize()?;
        let mut held = Vec::new();
        let mut prefix = PathBuf::new();
        for component in parent.components() {
            prefix.push(component);
            let file = open_directory(&prefix)?;
            lock(&file, &prefix, false)?;
            if mode == NamespaceMode::Open
                && let Some((Some(AuthorityLifecycle::Dropping { .. }), _)) =
                    read_lifecycle(&prefix)?
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("database {} is dropping", prefix.display()),
                ));
            }
            held.push(file);
        }
        let target = parent.join(&name);
        if mode == NamespaceMode::Open {
            match fs::create_dir(&target) {
                Ok(()) => {
                    // The pinned parent is the last held descriptor.
                    if let Some(parent) = held.last() {
                        parent.sync_all()?;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        let file = match open_directory(&target) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("database {} does not exist", target.display()),
                ));
            }
            result => result?,
        };
        lock(&file, &target, true)?;
        held.push(file);
        Ok(Self {
            target,
            _held: held,
        })
    }

    /// Canonical target directory.
    pub(crate) fn target(&self) -> &Path {
        &self.target
    }

    /// Lock a nested authority below this exclusively held target: shared
    /// on every intermediate directory, exclusive on `child`. The parent's
    /// own exclusive lock is inherited, never retaken.
    pub(crate) fn descend(&self, child: &Path) -> io::Result<Self> {
        let relative = child.strip_prefix(&self.target).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{} is not below {}", child.display(), self.target.display()),
            )
        })?;
        let mut held = Vec::new();
        let mut prefix = self.target.clone();
        let components: Vec<_> = relative.components().collect();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(_) = component else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("nested path {} is not canonical", child.display()),
                ));
            };
            prefix.push(component);
            let file = open_directory(&prefix)?;
            lock(&file, &prefix, index + 1 == components.len())?;
            held.push(file);
        }
        if held.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nested authority equals its parent",
            ));
        }
        Ok(Self {
            target: prefix,
            _held: held,
        })
    }
}
