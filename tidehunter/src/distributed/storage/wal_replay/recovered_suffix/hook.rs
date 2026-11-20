use super::*;
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fault {
    /// The next rewrite write fails with EIO.
    Write,
    /// The next rewrite sync fails with EIO.
    Sync,
    /// Before each write the file range holds zeros: a validated clean page
    /// was evicted and the device never held its content.
    EvictBeforeWrite,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Event {
    /// `bytes` rewritten at global WAL offset `offset`.
    Write { offset: u64, bytes: Vec<u8> },
    /// The file starting at global offset `file_start` is synced next.
    Sync { file_start: u64 },
}

#[derive(Default)]
pub(crate) struct Hook {
    pub(crate) fault: Option<Fault>,
    pub(crate) events: Vec<Event>,
    /// Duration of the last completed guard.
    pub(crate) elapsed: Duration,
}

thread_local! {
    static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
}

pub(crate) fn install(hook: Hook) {
    HOOK.with(|slot| *slot.borrow_mut() = Some(hook));
}

pub(crate) fn take() -> Option<Hook> {
    HOOK.with(|slot| slot.borrow_mut().take())
}

fn eio() -> StorageError {
    StorageError::Io(io::Error::from_raw_os_error(libc::EIO))
}

pub(super) fn write(
    file: &File,
    file_offset: u64,
    offset: u64,
    bytes: &[u8],
) -> Result<(), StorageError> {
    HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(hook) = slot.as_mut() else {
            return Ok(());
        };
        match hook.fault {
            Some(Fault::Write) => return Err(eio()),
            Some(Fault::EvictBeforeWrite) => {
                file.write_all_at(&vec![0; bytes.len()], file_offset)?;
            }
            _ => {}
        }
        hook.events.push(Event::Write {
            offset,
            bytes: bytes.to_vec(),
        });
        Ok(())
    })
}

pub(super) fn sync(file_start: u64) -> Result<(), StorageError> {
    HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(hook) = slot.as_mut() else {
            return Ok(());
        };
        if hook.fault == Some(Fault::Sync) {
            return Err(eio());
        }
        hook.events.push(Event::Sync { file_start });
        Ok(())
    })
}

pub(super) fn finish(elapsed: Duration) {
    HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook.elapsed = elapsed;
        }
    });
}
