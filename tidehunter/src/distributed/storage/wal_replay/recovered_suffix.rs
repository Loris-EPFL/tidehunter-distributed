//! Restart durability guard for the recovered unsynced WAL suffix.
//!
//! After a writeback error the kernel can leave page-cache pages clean although
//! their content never reached the device. A process that restarts on the same
//! kernel reads those frames from the cache, and a later fsync succeeds because
//! nothing is dirty: it would certify bytes the device does not hold. The
//! recovery scan therefore writes every frame and skip marker it accepts above
//! the persisted root position back from the bytes it validated (the write
//! dirties the pages again), and syncs each written file before any root, seal
//! or barrier certifies the stream. No byte is read again, so a page evicted
//! after validation cannot change what becomes durable. A frame is written as
//! `CrcFrame::new(payload)`: its header is the length and checksum the scan
//! checked, so the bytes equal the validated frame. A skip marker is its 8-byte
//! header followed by the zeros the scan checked. Alignment padding between
//! frames carries no data and is not written. The cost is bounded by the
//! uncheckpointed suffix and is paid once per recovery, never on the write path.
//! Any write or sync error fails the recovery: the owner stops instead of
//! reporting progress it cannot prove.

use super::*;

/// Write-back state of one recovery scan: the stream directory, its layout and
/// the WAL file written last. Both recovery paths (WAL-replay and on-disk
/// history) use it; neither needs an open `PartitionLog`.
pub(in crate::distributed::storage) struct SuffixRewrite {
    path: PathBuf,
    layout: crate::wal::layout::WalLayout,
    wal_file_bytes: u64,
    /// Start offset and writable handle of the file written last; synced when
    /// the scan moves to another file, at `sync` and at `finish`.
    file: Option<(u64, File)>,
    #[cfg(test)]
    elapsed: Duration,
}

impl SuffixRewrite {
    pub(in crate::distributed::storage) fn new(path: &Path, config: &PartitionConfig) -> Self {
        Self {
            path: path.to_path_buf(),
            layout: config.layout(),
            wal_file_bytes: config.wal_file_bytes,
            file: None,
            #[cfg(test)]
            elapsed: Duration::ZERO,
        }
    }

    /// Write back the validated group frame whose payload is `payload` at global
    /// WAL offset `offset`.
    pub(in crate::distributed::storage) fn frame(
        &mut self,
        offset: u64,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        let frame = CrcFrame::new(&payload.to_vec());
        self.write_at(offset, frame.as_ref())
    }

    /// Write back the exact frame bytes (header and payload) that the caller
    /// validated in its own buffer, at global WAL offset `offset`.
    pub(in crate::distributed::storage) fn validated_frame(
        &mut self,
        offset: u64,
        frame: &[u8],
    ) -> Result<(), StorageError> {
        self.write_at(offset, frame)
    }

    /// Write back the validated skip marker at `offset` and its zero suffix up to
    /// `end`.
    pub(in crate::distributed::storage) fn skip_marker(
        &mut self,
        offset: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        let len = usize::try_from(end - offset).map_err(|_| StorageError::Capacity)?;
        let mut bytes = vec![0; len];
        let marker = CrcFrame::skip_marker();
        let header = bytes
            .get_mut(..marker.as_ref().len())
            .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
        header.copy_from_slice(marker.as_ref());
        self.write_at(offset, &bytes)
    }

    /// Sync the file written last. Call before any barrier, root or seal that
    /// certifies a rewritten range.
    pub(in crate::distributed::storage) fn sync(&mut self) -> Result<(), StorageError> {
        #[cfg(test)]
        let started = Instant::now();
        let result = self.sync_written();
        #[cfg(test)]
        {
            self.elapsed += started.elapsed();
        }
        result
    }

    /// Sync the last written file and end the guard.
    pub(in crate::distributed::storage) fn finish(mut self) -> Result<(), StorageError> {
        self.sync()?;
        #[cfg(test)]
        hook::finish(self.elapsed);
        Ok(())
    }

    fn sync_written(&mut self) -> Result<(), StorageError> {
        if let Some((file_start, file)) = self.file.take() {
            #[cfg(test)]
            hook::sync(file_start)?;
            #[cfg(not(test))]
            let _ = file_start;
            file.sync_data()?;
        }
        Ok(())
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<(), StorageError> {
        #[cfg(test)]
        let started = Instant::now();
        let file_start = offset / self.wal_file_bytes * self.wal_file_bytes;
        if self
            .file
            .as_ref()
            .is_none_or(|(start, _)| *start != file_start)
        {
            self.sync_written()?;
            let name = self.layout.wal_file_name(
                &self.path,
                crate::wal::position::WalFileId::from_u64(file_start / self.wal_file_bytes),
            );
            let file = OpenOptions::new().write(true).open(name)?;
            self.file = Some((file_start, file));
        }
        let (_, file) = self
            .file
            .as_ref()
            .ok_or(StorageError::Invalid(FRAME_BOUNDARY))?;
        #[cfg(test)]
        hook::write(file, offset - file_start, offset, bytes)?;
        file.write_all_at(bytes, offset - file_start)?;
        #[cfg(test)]
        {
            self.elapsed += started.elapsed();
        }
        Ok(())
    }
}

/// Test hook: records every rewrite step on this thread and can inject one
/// fault. Recovery runs on the calling thread, so a thread-local hook is
/// deterministic.
#[cfg(test)]
pub(crate) mod hook {
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
        /// Right after the on-disk scan validated a frame, its file range holds
        /// zeros: the clean page was evicted between validation and use, and the
        /// device never held it. Recovery must keep using the validated bytes.
        EvictAfterValidation,
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

    /// The on-disk scan validated a frame of `len` bytes at `file_offset` in the
    /// file at `path`.
    pub(in crate::distributed::storage) fn after_validation(
        path: &Path,
        file_offset: u64,
        len: usize,
    ) -> Result<(), crate::wal::WalError> {
        let evict = HOOK.with(|slot| {
            slot.borrow()
                .as_ref()
                .is_some_and(|hook| hook.fault == Some(Fault::EvictAfterValidation))
        });
        if evict {
            OpenOptions::new()
                .write(true)
                .open(path)?
                .write_all_at(&vec![0; len], file_offset)?;
        }
        Ok(())
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
}

#[cfg(test)]
mod tests {
    use super::hook::{Event, Fault, Hook};
    use super::*;
    use crate::key_shape::{KeySpace, KeyType};
    use crate::wal::position::WalFileId;

    fn shape() -> KeyShape {
        KeyShape::new_single(4, 1, KeyType::uniform(1))
    }

    fn metrics() -> Arc<Metrics> {
        Metrics::new_in_enabled(&prometheus::Registry::new(), false)
    }

    fn config(fragment_bytes: u64, wal_file_bytes: u64) -> PartitionConfig {
        PartitionConfig {
            inventory: StaticInventory {
                database: [13; 16],
                epoch: 3,
                segment_generation: 9,
                partitions: vec![0],
            },
            partition: 0,
            batch_limits: BatchLimits {
                max_operations: 10,
                max_encoded_bytes: 500,
            },
            group_limits: GroupLimits {
                max_batches: 4,
                max_encoded_bytes: 1008,
            },
            fragment_bytes,
            wal_file_bytes,
            max_batches: 64,
            max_reference_metadata_bytes: 1024 * 1024,
        }
    }

    fn batch(config: &PartitionConfig, n: u64) -> CompleteBatch {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&n.to_be_bytes());
        CompleteBatch::new(
            &shape(),
            BatchId {
                database: config.inventory.database,
                operation,
            },
            BatchVersion {
                epoch: config.inventory.epoch,
                sequence: n,
            },
            vec![Mutation::Put {
                keyspace: KeySpace(0),
                key: (n as u32).to_be_bytes().to_vec().into(),
                value: vec![n as u8; 64].into(),
            }],
            config.batch_limits,
        )
        .unwrap()
    }

    /// A WAL-replay stream whose first `synced` batches are below its persisted
    /// root position, followed by `unsynced` batches, closed without a sync.
    /// Returns the root position and the appended end.
    fn stream(path: &Path, config: &PartitionConfig, synced: u64, unsynced: u64) -> (u64, u64) {
        let log = PartitionLog::create(path, config.clone(), &shape(), metrics()).unwrap();
        let authority = RequestAuthority {
            epoch: config.inventory.epoch,
        };
        for n in 0..synced + unsynced {
            log.append_complete_batch(authority, &batch(config, n))
                .unwrap();
            if n + 1 == synced {
                let cut = log.state.lock().writer.as_ref().unwrap().position();
                assert_eq!(log.persist_wal_through(cut).unwrap(), cut);
            }
        }
        let root = log.position_frontier_for_test();
        let appended = log.durable_progress().appended_end;
        assert_eq!(root < appended, unsynced > 0);
        (root, appended)
    }

    fn wal_bytes(path: &Path, config: &PartitionConfig, end: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        for start in (0..end).step_by(config.wal_file_bytes as usize) {
            let name = config
                .layout()
                .wal_file_name(path, WalFileId::from_u64(start / config.wal_file_bytes));
            let file = fs::read(name).unwrap();
            let len = (end - start).min(config.wal_file_bytes) as usize;
            bytes.extend_from_slice(&file[..len]);
        }
        bytes
    }

    /// Model a device that missed the unsynced suffix (the clean-but-stale cache
    /// case): it holds zeros above the synced end and takes a rewritten range
    /// only when the sync of its file follows. After recovery it must hold the
    /// recovered bytes, so a later no-op fsync can no longer certify stale data.
    #[test]
    fn restart_makes_the_recovered_unsynced_suffix_durable() {
        let temp = tempdir::TempDir::new("recovered-suffix").unwrap();
        let path = temp.path().join("stream");
        let config = config(1024, 2048);
        // The unsynced batches cross into the second WAL file.
        let (root, appended) = stream(&path, &config, 1, 24);
        assert!(appended > config.wal_file_bytes);
        let recovered_bytes = wal_bytes(&path, &config, appended);
        let mut device = recovered_bytes.clone();
        device[root as usize..].fill(0);
        hook::install(Hook::default());
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        let events = hook::take().unwrap().events;
        assert_eq!(log.receipt_count(), 25);
        let mut pending = Vec::new();
        let mut written = 0;
        for event in events {
            match event {
                Event::Write { offset, bytes } => pending.push((offset, bytes)),
                Event::Sync { file_start } => {
                    for (offset, bytes) in pending.drain(..) {
                        assert_eq!(
                            offset / config.wal_file_bytes * config.wal_file_bytes,
                            file_start
                        );
                        assert!(offset >= root);
                        // Exactly the validated bytes.
                        let start = offset as usize;
                        let end = start + bytes.len();
                        assert_eq!(bytes[..], recovered_bytes[start..end]);
                        device[start..end].copy_from_slice(&bytes);
                        written += bytes.len();
                    }
                }
            }
        }
        assert!(pending.is_empty(), "rewrite without a following sync");
        assert!(written > 0);
        drop(log);
        // The device image alone recovers every batch.
        for start in (0..appended).step_by(config.wal_file_bytes as usize) {
            let name = config
                .layout()
                .wal_file_name(&path, WalFileId::from_u64(start / config.wal_file_bytes));
            let end = (start + config.wal_file_bytes).min(appended) as usize;
            OpenOptions::new()
                .write(true)
                .open(name)
                .unwrap()
                .write_all_at(&device[start as usize..end], 0)
                .unwrap();
        }
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(log.receipt_count(), 25);
    }

    #[test]
    fn a_synced_stream_rewrites_nothing() {
        let temp = tempdir::TempDir::new("recovered-suffix-synced").unwrap();
        let path = temp.path().join("stream");
        let config = config(1024, 2048);
        stream(&path, &config, 3, 0);
        hook::install(Hook::default());
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(hook::take().unwrap().events, Vec::new());
        assert_eq!(log.receipt_count(), 3);
    }

    /// A rewrite or sync error fails recovery and leaves the stream recoverable.
    #[test]
    fn a_rewrite_or_sync_error_fails_recovery() {
        for fault in [Fault::Write, Fault::Sync] {
            let temp = tempdir::TempDir::new("recovered-suffix-fault").unwrap();
            let path = temp.path().join("stream");
            let config = config(1024, 2048);
            stream(&path, &config, 1, 2);
            hook::install(Hook {
                fault: Some(fault),
                ..Hook::default()
            });
            let error = PartitionLog::recover_sealed(&path, &config, metrics())
                .err()
                .unwrap();
            assert!(hook::take().is_some());
            assert!(matches!(error, StorageError::Io(_)), "{fault:?}: {error:?}");
            let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
            assert_eq!(log.receipt_count(), 3, "{fault:?}");
        }
    }

    /// A clean page evicted after the scan validated it no longer holds the
    /// frame. The guard writes the validated bytes, not what it would read now,
    /// so the frames still become durable.
    #[test]
    fn a_page_evicted_after_validation_still_becomes_durable() {
        let temp = tempdir::TempDir::new("recovered-suffix-evicted").unwrap();
        let path = temp.path().join("stream");
        let config = config(1024, 2048);
        stream(&path, &config, 1, 2);
        hook::install(Hook {
            fault: Some(Fault::EvictBeforeWrite),
            ..Hook::default()
        });
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert!(hook::take().is_some());
        assert_eq!(log.receipt_count(), 3);
        drop(log);
        let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
        assert_eq!(log.receipt_count(), 3);
    }

    /// An on-disk-history stream: `synced` batches made durable by a covering
    /// barrier, then `unsynced` batches closed without a sync. Returns the
    /// history root position (the durable point recovery trusts) and the
    /// appended end.
    fn on_disk_stream(
        path: &Path,
        config: &PartitionConfig,
        synced: u64,
        unsynced: u64,
    ) -> (u64, u64) {
        let log = PartitionLog::create(path, config.clone(), &shape(), metrics()).unwrap();
        log.set_history_persistence(HistoryPersistence::OnDisk)
            .unwrap();
        let authority = RequestAuthority {
            epoch: config.inventory.epoch,
        };
        for n in 0..synced + unsynced {
            log.append_complete_batch(authority, &batch(config, n))
                .unwrap();
            if n + 1 == synced {
                log.persist_appended().unwrap();
            }
        }
        let root = log.references.root().end;
        let appended = log.durable_progress().appended_end;
        assert!(root < appended);
        (root, appended)
    }

    /// On-disk history: the same device model as the WAL-replay test. Every
    /// rewrite equals the validated bytes, lies at or above the history root and
    /// is synced before recovery returns, and the device image alone recovers
    /// every batch. With `EvictAfterValidation`, each frame's file range is
    /// zeroed right after the scan validated it (a clean page evicted before use):
    /// the guard must still write the validated bytes, never a re-read copy with
    /// a fresh CRC.
    #[test]
    fn on_disk_restart_makes_the_recovered_unsynced_suffix_durable() {
        for fault in [None, Some(Fault::EvictAfterValidation)] {
            let temp = tempdir::TempDir::new("recovered-suffix-on-disk").unwrap();
            let path = temp.path().join("stream");
            let config = config(1024, 2048);
            let (root, appended) = on_disk_stream(&path, &config, 1, 24);
            assert!(appended > config.wal_file_bytes);
            let recovered_bytes = wal_bytes(&path, &config, appended);
            let mut device = recovered_bytes.clone();
            device[root as usize..].fill(0);
            hook::install(Hook {
                fault,
                ..Hook::default()
            });
            let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
            let events = hook::take().unwrap().events;
            assert_eq!(log.history_persistence(), HistoryPersistence::OnDisk);
            assert_eq!(log.receipt_count(), 25, "{fault:?}");
            let mut pending = Vec::new();
            let mut written = 0;
            for event in events {
                match event {
                    Event::Write { offset, bytes } => pending.push((offset, bytes)),
                    Event::Sync { file_start } => {
                        for (offset, bytes) in pending.drain(..) {
                            assert_eq!(
                                offset / config.wal_file_bytes * config.wal_file_bytes,
                                file_start
                            );
                            assert!(offset >= root);
                            let start = offset as usize;
                            let end = start + bytes.len();
                            assert_eq!(
                                bytes[..],
                                recovered_bytes[start..end],
                                "{fault:?} offset {offset}"
                            );
                            device[start..end].copy_from_slice(&bytes);
                            written += bytes.len();
                        }
                    }
                }
            }
            assert!(pending.is_empty(), "rewrite without a following sync");
            assert!(written > 0);
            drop(log);
            for start in (0..appended).step_by(config.wal_file_bytes as usize) {
                let name = config
                    .layout()
                    .wal_file_name(&path, WalFileId::from_u64(start / config.wal_file_bytes));
                let end = (start + config.wal_file_bytes).min(appended) as usize;
                OpenOptions::new()
                    .write(true)
                    .open(name)
                    .unwrap()
                    .write_all_at(&device[start as usize..end], 0)
                    .unwrap();
            }
            let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
            assert_eq!(log.receipt_count(), 25, "{fault:?}");
        }
    }

    /// On-disk history: a rewrite or sync error fails recovery, which installs
    /// no seal, and the stream still recovers afterwards. A sealed stream was
    /// synced whole before its seal and rewrites nothing.
    #[test]
    fn on_disk_rewrite_error_fails_recovery_and_a_sealed_stream_rewrites_nothing() {
        for fault in [Fault::Write, Fault::Sync] {
            let temp = tempdir::TempDir::new("recovered-suffix-on-disk-fault").unwrap();
            let path = temp.path().join("stream");
            let config = config(1024, 2048);
            on_disk_stream(&path, &config, 1, 2);
            hook::install(Hook {
                fault: Some(fault),
                ..Hook::default()
            });
            let error = PartitionLog::recover_sealed(&path, &config, metrics())
                .err()
                .unwrap();
            assert!(hook::take().is_some());
            assert!(matches!(error, StorageError::Io(_)), "{fault:?}: {error:?}");
            assert!(!path.join(SEAL).exists(), "{fault:?}");
            let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
            assert_eq!(log.receipt_count(), 3, "{fault:?}");
            drop(log);
            hook::install(Hook::default());
            let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
            assert_eq!(hook::take().unwrap().events, Vec::new(), "{fault:?}");
            assert_eq!(log.receipt_count(), 3, "{fault:?}");
        }
    }

    /// Restart cost of the guard; run explicitly with `--ignored --nocapture`.
    #[test]
    #[ignore = "measurement only"]
    fn measure_restart_rewrite() {
        for unsynced in [16, 256, 2048, 8192] {
            let temp = tempdir::TempDir::new("recovered-suffix-cost").unwrap();
            let path = temp.path().join("stream");
            let config = config(64 * 1024, 1024 * 1024);
            let (root, appended) = stream(&path, &config, 1, unsynced);
            hook::install(Hook::default());
            let started = Instant::now();
            let log = PartitionLog::recover_sealed(&path, &config, metrics()).unwrap();
            let recovery = started.elapsed();
            let hook = hook::take().unwrap();
            let rewritten: usize = hook
                .events
                .iter()
                .map(|event| match event {
                    Event::Write { bytes, .. } => bytes.len(),
                    Event::Sync { .. } => 0,
                })
                .sum();
            eprintln!(
                "unsynced_batches={unsynced} region_bytes={} rewritten_bytes={rewritten} \
                 guard_ms={:.3} recovery_ms={:.3} receipts={}",
                appended - root,
                hook.elapsed.as_secs_f64() * 1e3,
                recovery.as_secs_f64() * 1e3,
                log.receipt_count()
            );
        }
    }
}
