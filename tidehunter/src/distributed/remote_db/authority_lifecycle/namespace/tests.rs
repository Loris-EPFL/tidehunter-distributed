//! Namespace lock release while a copy of a locked descriptor lives on. `try_clone`
//! (`dup`) shares the open file description exactly like the copy a forked child
//! inherits, so a lock released only by closing our descriptor would stay held.

use super::*;

#[test]
fn guard_drop_releases_every_lock_while_descriptor_copies_live() {
    let temp = tempdir::TempDir::new("namespace-unlock-guard").unwrap();
    let parent = temp.path().join("epoch_5");
    let child = parent.join("recovery_log");
    fs::create_dir(&parent).unwrap();
    let guard = NamespaceGuard::acquire(&child, NamespaceMode::Open).unwrap();
    // Ancestors (shared) and the target (exclusive): copy each descriptor.
    let copies = guard
        ._held
        .0
        .iter()
        .map(File::try_clone)
        .collect::<io::Result<Vec<_>>>()
        .unwrap();
    assert!(
        copies.len() >= 3,
        "{} copies of the held directories",
        copies.len()
    );
    drop(guard);

    // The exclusive lock on the parent conflicts with the old shared lock on it.
    let parent_guard = NamespaceGuard::acquire(&parent, NamespaceMode::Drop)
        .expect("the shared ancestor lock must be released by the guard drop");
    drop(parent_guard);
    // The exclusive lock on the old target must be released too.
    NamespaceGuard::acquire(&child, NamespaceMode::Drop)
        .expect("the exclusive target lock must be released by the guard drop");
    drop(copies);
}

#[test]
fn held_locks_release_a_partially_built_set_while_a_copy_lives() {
    let temp = tempdir::TempDir::new("namespace-unlock-partial").unwrap();
    let path = temp.path().join("target");
    fs::create_dir(&path).unwrap();
    // The state of an acquire that returns early: some directories pushed.
    let directory = open_directory(&path).unwrap();
    lock(&directory, &path, true).unwrap();
    let copy = directory.try_clone().unwrap();
    let mut held = HeldLocks::new();
    held.push(directory);
    drop(held);

    let probe = open_directory(&path).unwrap();
    let error = probe.try_lock().err();
    assert!(
        error.is_none(),
        "the lock outlived its HeldLocks while a copy lives: {error:?}"
    );
    drop(copy);
}
