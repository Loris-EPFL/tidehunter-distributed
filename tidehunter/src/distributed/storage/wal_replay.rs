//! Owner positions are durable only after the WAL barrier and atomic root install.
use super::*;
use bincode::Options;

mod recovered_suffix;
use recovered_suffix::SuffixRewrite;

/// Recovery scans read beyond the durable frontier, where a frame can be absent or
/// torn. Only these outcomes mean "no valid frame here": a missing file, a short
/// file, or bytes that fail the frame, CRC, decode or batch checks. Any other I/O
/// error leaves the bytes unknown: recovery must fail, never read it as absent.
fn frame_missing_or_invalid(error: &StorageError) -> bool {
    match error {
        StorageError::Io(error) => matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof
        ),
        StorageError::Wal(_) | StorageError::Unknown(_) => false,
        _ => true,
    }
}

/// A recovery read beyond the durable frontier. Names the read for the test hook,
/// which can replace its result with EIO; production code passes results through.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryRead {
    /// Later-fragment probe: open, header read, body read.
    ProbeOpen,
    ProbeHeader,
    ProbeBody,
    /// Main replay scan: a frame header read.
    ScanHeader,
}

#[cfg(test)]
thread_local! {
    // Recovery runs on the calling thread, so a thread-local hook is deterministic.
    static READ_FAULT: std::cell::Cell<Option<(RecoveryRead, u64)>> =
        const { std::cell::Cell::new(None) };
}

/// Test hook: replace the result of the next `read` at WAL `offset` on this thread
/// with EIO. The faulted reads are on live streams, which need the NVMe-oF feature.
#[cfg(test)]
pub(crate) fn inject_read_fault(read: RecoveryRead, offset: u64) {
    READ_FAULT.with(|slot| slot.set(Some((read, offset))));
}

/// The read's own result, or EIO when the test hook selects it. Callers classify
/// the returned error with the production classifier either way.
#[cfg_attr(not(test), inline(always))]
fn read_result<T>(
    read: RecoveryRead,
    offset: u64,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    #[cfg(test)]
    if READ_FAULT.with(|slot| slot.get()) == Some((read, offset)) {
        READ_FAULT.with(|slot| slot.set(None));
        return Err(StorageError::Io(io::Error::from_raw_os_error(libc::EIO)));
    }
    #[cfg(not(test))]
    let _ = (read, offset);
    result
}

/// Same path for every root format; `WalRoot.format` selects the grammar.
pub(super) const ROOT: &str = "partition-wal-root-v1";
const ROOT_FORMAT: u32 = 2;
const MEMBER_MISMATCH: &str = "WAL member differs from the reference";
pub(super) const FRAME_BOUNDARY: &str = "replay position is not a frame boundary";

/// Fragment-aligned start of a replay of `offset`.
///
/// A stored stream front is fragment aligned, and an aligned offset is always a
/// frame boundary, which is what the owner's page read requires. Flooring an
/// unaligned offset widens the replay instead of narrowing it, and re-applying a
/// batch below the true start is idempotent by version, so the floor is the safe
/// direction for both the writer of a front and the reader of one.
pub(crate) fn aligned_replay_start(offset: u64, fragment_bytes: u64) -> u64 {
    if fragment_bytes == 0 {
        return offset;
    }
    offset - offset % fragment_bytes
}

/// Format 2: fixed-width bincode plus a trailing little-endian CRC32. Format 1
/// (one offset per retired file) is refused: deployments use fresh databases.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct WalRoot {
    format: u32,
    database: [u8; 16],
    partition: u32,
    epoch: u64,
    pub(super) generation: u64,
    pub(super) scan_from: u64,
    pub(super) synced_end: u64,
    pub(super) retired: RetiredFiles,
    /// Sorted by start; never retired.
    pub(super) sealed_files: Vec<FileSeal>,
    /// Sorted by start; every entry is retired.
    pub(super) cleanup_pending: Vec<PendingRetirement>,
}

impl WalRoot {
    pub(super) fn new(config: &PartitionConfig) -> Self {
        Self {
            format: ROOT_FORMAT,
            database: config.inventory.database,
            partition: config.partition,
            epoch: config.inventory.epoch,
            generation: 1,
            scan_from: 0,
            synced_end: 0,
            retired: RetiredFiles::default(),
            sealed_files: Vec::new(),
            cleanup_pending: Vec::new(),
        }
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, StorageError> {
        let mut bytes = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .serialize(self)
            .map_err(|_| StorageError::Invalid("WAL root serialization failed"))?;
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
        Ok(bytes)
    }

    pub(super) fn install(&self, path: &Path) -> Result<(), StorageError> {
        install_metadata(path, ROOT, &self.encode()?)?;
        Ok(())
    }

    /// First file start at or above which no file is sealed or retired. Seals
    /// are installed in ascending file order, so every unretired file below
    /// this bound is sealed.
    pub(super) fn seal_cursor(&self) -> u64 {
        self.sealed_files
            .last()
            .map_or(0, |seal| seal.end)
            .max(self.retired.end())
    }

    pub(super) fn open(path: &Path, config: &PartitionConfig) -> Result<Self, StorageError> {
        let file = File::open(path.join(ROOT))?;
        if file.metadata()?.len() > MAX_METADATA_BYTES {
            return Err(StorageError::Invalid("WAL root exceeds metadata limit"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_METADATA_BYTES + 1).read_to_end(&mut bytes)?;
        let body_len = bytes
            .len()
            .checked_sub(4)
            .ok_or(StorageError::Invalid("invalid WAL root CRC"))?;
        if bytes[body_len..] != crc32fast::hash(&bytes[..body_len]).to_le_bytes() {
            return Err(StorageError::Invalid("invalid WAL root CRC"));
        }
        // Fixed-width bincode writes `format` first as four little-endian bytes.
        if bytes.get(..4) != Some(&ROOT_FORMAT.to_le_bytes()[..]) {
            return Err(StorageError::Invalid(
                "unsupported WAL root format: use archive/multi-design",
            ));
        }
        let root: Self = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(body_len as u64)
            .reject_trailing_bytes()
            .deserialize(&bytes[..body_len])
            .map_err(|_| StorageError::Invalid("invalid WAL root format"))?;
        if root.database != config.inventory.database
            || root.partition != config.partition
            || root.epoch != config.inventory.epoch
            || root.generation == 0
            || root.synced_end < root.scan_from
            || !root.scan_from.is_multiple_of(config.fragment_bytes)
        {
            return Err(StorageError::Invalid(
                "WAL root differs from registered stream",
            ));
        }
        validate_root_sets(
            &root.retired,
            &root.sealed_files,
            &root.cleanup_pending,
            root.synced_end,
            root.generation,
            config.wal_file_bytes,
        )?;
        Ok(root)
    }
}

/// A whole checked outer frame. Member offsets and lengths are in `receipts`.
#[derive(Clone, Debug)]
pub struct DecodedGroup {
    pub group: CompleteGroup,
    pub receipts: Vec<AppendedBatch>,
    pub position: WalPosition,
    pub frame_end: u64,
}

#[derive(Clone, Debug)]
pub struct ReplayPage {
    pub groups: Vec<DecodedGroup>,
    pub next: Option<u64>,
}

fn position_lifecycle(state: &State) -> (bool, bool, bool) {
    (state.sealed, state.writer.is_some(), state.is_sealing())
}

impl PartitionLog {
    pub(super) fn is_retired(&self, offset: u64) -> Result<bool, StorageError> {
        Ok(self
            .state
            .lock()
            .wal_root
            .as_ref()
            .is_some_and(|root| root.retired.contains(offset)))
    }

    /// Sync the fixed acknowledged cut without blocking leases on the file barrier.
    /// Lifecycle callers keep the log alive and exclude seal/fence/retirement.
    /// The position root is separate from the adopted-receipt sync frontier:
    /// checkpoint capture/recovery use this root and its proof reply, while
    /// acknowledgements and owner reads obtain their own receipt barriers.
    pub fn persist_wal_through(&self, through: u64) -> Result<u64, StorageError> {
        self.persist_wal_through_profiled(through, None)
    }

    pub(crate) fn persist_wal_through_profiled(
        &self,
        through: u64,
        profile: Option<&mut StorageWriteProfile>,
    ) -> Result<u64, StorageError> {
        let proof_span = super::super::owner_events::OwnerEventSpan::new(
            super::super::owner_events::OwnerEventKind::ProofStart,
            super::super::owner_events::OwnerEventKind::ProofEnd,
            self.config().partition,
            through,
            0,
        );
        let mut timing = WriteTiming::new(profile, false);
        timing.stage(Some(WriteStage::PersistValidation));
        let (snapshot, lifecycle) = {
            let state = self.state.lock();
            // Fail closed: after a Fatal poison the in-memory root may lag a
            // durable root (a seal or retirement root installed without its
            // publish). Nothing may be synced or rewritten from RAM.
            if matches!(state.poison, Some(WriterPoison::Fatal)) {
                return Err(StorageError::Poisoned);
            }
            let allocated = state
                .writer
                .as_ref()
                .map_or(state.appended_end, WalWriter::position);
            if through > allocated {
                return Err(StorageError::Invalid(
                    "checkpoint position beyond allocated WAL",
                ));
            }
            let leased = state.open_live_extent.is_some();
            // Owner-RPC cuts must be below the owner's completed-write frontier.
            // An open lease has no owner-visible CQE; only its coordinator may
            // name a fully written acknowledged cut above that frontier.
            if through > state.appended_end && !leased {
                return Err(StorageError::Invalid(
                    "checkpoint position beyond completed owner writes",
                ));
            }
            let root = state
                .wal_root
                .clone()
                .ok_or(StorageError::Invalid("request requires WAL replay history"))?;
            (root, position_lifecycle(&state))
        };
        timing.stage(None);
        #[cfg(test)]
        {
            let pause = self.before_persist.lock().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.recv();
            }
        }
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::PositionBeforeData);
        #[cfg(test)]
        self.fail_position_at(4)?;
        let size = self.config().wal_file_bytes;
        // Required files cannot retire while this stream operation is active.
        // WAL bytes below through are immutable; later writes may be synced too.
        if through > snapshot.synced_end {
            for id in snapshot.scan_from / size..=through.saturating_sub(1) / size {
                let start = id * size;
                if !snapshot.retired.contains(start) {
                    timing.stage(Some(WriteStage::PersistFile));
                    timing.record(|profile| profile.file_persist_calls += 1);
                    let proof_file_sync_span = super::super::owner_events::OwnerEventSpan::new(
                        super::super::owner_events::OwnerEventKind::ProofFileSyncStart,
                        super::super::owner_events::OwnerEventKind::ProofFileSyncEnd,
                        self.config().partition,
                        id,
                        0,
                    );
                    self.wal.persist_frame_file(WalPosition::new(start, 1))?;
                    proof_file_sync_span.end_at(id);
                    timing.record(|profile| profile.file_persist_successes += 1);
                    timing.stage(None);
                }
            }
            timing.stage(Some(WriteStage::PersistNamespace));
            timing.record(|profile| profile.namespace_persist_calls += 1);
            let proof_namespace_sync_span = super::super::owner_events::OwnerEventSpan::new(
                super::super::owner_events::OwnerEventKind::ProofNamespaceSyncStart,
                super::super::owner_events::OwnerEventKind::ProofNamespaceSyncEnd,
                self.config().partition,
                0,
                0,
            );
            self.wal.persist_namespace()?;
            proof_namespace_sync_span.end_at(0);
            timing.record(|profile| profile.namespace_persist_successes += 1);
            timing.stage(None);
        }
        #[cfg(test)]
        self.fail_position_at(5)?;
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::PositionBeforeRoot);
        timing.stage(Some(WriteStage::PersistSerializationLock));
        let _persistence = self.persistence.lock();
        timing.stage(Some(WriteStage::PersistValidation));
        let current = {
            let state = self.state.lock();
            // Recheck under `persistence`: every root writer sets a Fatal
            // poison while it still holds `persistence`.
            if matches!(state.poison, Some(WriterPoison::Fatal)) {
                return Err(StorageError::Poisoned);
            }
            // The node and session stream guards must prevent these lifecycle
            // transitions. Never return a proof if a direct caller broke that rule.
            if position_lifecycle(&state) != lifecycle {
                return Err(StorageError::Invalid(
                    "position proof lifecycle changed during sync",
                ));
            }
            state
                .wal_root
                .clone()
                .ok_or(StorageError::Invalid("position root removed during sync"))?
        };
        #[cfg(test)]
        self.pause_live_at(LivePausePoint::PositionRootLocked);
        let scan_from =
            through.saturating_sub(1) / self.config().fragment_bytes * self.config().fragment_bytes;
        if through > current.synced_end || scan_from > current.scan_from {
            // Copy the root without the state lock: its seal set is O(sealed files).
            let mut root = WalRoot::clone(&current);
            root.synced_end = root.synced_end.max(through);
            root.scan_from = root.scan_from.max(scan_from);
            root.generation = root
                .generation
                .checked_add(1)
                .ok_or(StorageError::Capacity)?;
            timing.stage(Some(WriteStage::PersistPublication));
            self.install_wal_root(&root)?;
            let synced_end = root.synced_end;
            self.state.lock().wal_root = Some(Arc::new(root));
            #[cfg(test)]
            self.fail_position_at(6)?;
            #[cfg(test)]
            self.pause_live_at(LivePausePoint::PositionAfterRoot);
            timing.success(0);
            proof_span.end_at(through);
            return Ok(synced_end);
        }
        timing.success(0);
        proof_span.end_at(through);
        Ok(current.synced_end)
    }

    #[cfg(test)]
    fn fail_position_at(&self, phase: u8) -> Result<(), StorageError> {
        if self
            .fail_wal_root
            .compare_exchange(phase, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return Err(StorageError::Invalid("injected position persistence crash"));
        }
        Ok(())
    }

    pub(super) fn install_wal_root(&self, root: &WalRoot) -> Result<(), StorageError> {
        #[cfg(test)]
        {
            let fail = self
                .fail_wal_root
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |phase| {
                    (1..=3).contains(&phase).then_some(0)
                })
                .unwrap_or(0);
            if fail == 1 {
                return Err(StorageError::Invalid(
                    "injected crash before WAL root write",
                ));
            }
            if fail == 2 || fail == 3 {
                let temporary = self.path.join(format!(".{ROOT}.pending"));
                let mut file = File::create(&temporary)?;
                file.write_all(&root.encode()?)?;
                file.sync_all()?;
                if fail == 3 {
                    fs::rename(temporary, self.path.join(ROOT))?;
                }
                return Err(StorageError::Invalid(
                    "injected crash during WAL root rename",
                ));
            }
        }
        root.install(&self.path)
    }

    /// Retire whole files of this older sealed stream. The caller supplies no
    /// inode; the owner binds the inode it observes. `file_retirement` owns the
    /// durable record -> unlink -> directory sync order.
    pub fn retire_wal_files(&self, files: &[u64]) -> Result<(), StorageError> {
        let proofs: Vec<_> = files
            .iter()
            .map(|&file_start| RetireProof {
                file_start,
                inode: None,
                seal_generation: None,
            })
            .collect();
        match self.retire_proved_files(&proofs, None)? {
            RetirementOutcome::Completed => Ok(()),
            RetirementOutcome::Busy => Err(StorageError::Invalid(
                "retirement cleanup set is full; retry after cleanup",
            )),
        }
    }

    pub(super) fn open_replay_file(&self, offset: u64) -> Result<File, StorageError> {
        let id = crate::wal::position::WalFileId::from_u64(offset / self.config().wal_file_bytes);
        Ok(File::open(
            self.config().layout().wal_file_name(&self.path, id),
        )?)
    }

    #[allow(
        clippy::expect_used,
        reason = "the branch above installs the file before this reference"
    )]
    fn replay_file<'a>(
        &self,
        cached: &'a mut Option<(u64, File)>,
        offset: u64,
    ) -> Result<&'a File, StorageError> {
        let id = offset / self.config().wal_file_bytes;
        if cached.as_ref().is_none_or(|(previous, _)| *previous != id) {
            *cached = Some((id, self.open_replay_file(offset)?));
        }
        Ok(&cached.as_ref().expect("file installed above").1)
    }

    pub(super) fn frame_header(&self, file: &File, offset: u64) -> Result<[u8; 8], StorageError> {
        let mut header = [0; 8];
        file.read_exact_at(&mut header, offset % self.config().wal_file_bytes)?;
        Ok(header)
    }

    /// Whether a valid group starts at `offset`. `Ok(false)` only for an absent or
    /// invalid frame; any other I/O error is returned.
    fn probe_valid_frame(
        &self,
        cached: &mut Option<(u64, File)>,
        offset: u64,
    ) -> Result<bool, StorageError> {
        let absent = |error: StorageError| {
            if frame_missing_or_invalid(&error) {
                Ok(false)
            } else {
                Err(error)
            }
        };
        let file = match read_result(
            RecoveryRead::ProbeOpen,
            offset,
            self.replay_file(cached, offset),
        ) {
            Ok(file) => file,
            Err(error) => return absent(error),
        };
        match read_result(
            RecoveryRead::ProbeHeader,
            offset,
            self.frame_header(file, offset),
        ) {
            Ok(header) if header == [0; 8] || header == [0xff; 8] => return Ok(false),
            Ok(_) => {}
            Err(error) => return absent(error),
        }
        match read_result(
            RecoveryRead::ProbeBody,
            offset,
            self.decode_group_from_file(file, offset),
        ) {
            Ok(_) => Ok(true),
            Err(error) => absent(error),
        }
    }

    /// Decode one complete, CRC-checked group at an exact WAL frame boundary.
    pub fn decode_group_at(&self, offset: u64) -> Result<DecodedGroup, StorageError> {
        let file = self.open_replay_file(offset)?;
        self.decode_group_from_file(&file, offset)
    }

    pub(super) fn decode_group_from_file(
        &self,
        file: &File,
        offset: u64,
    ) -> Result<DecodedGroup, StorageError> {
        let header = self.frame_header(file, offset)?;
        let len = u32::from_be_bytes(
            header[..4]
                .try_into()
                .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?,
        ) as usize;
        let frame_len = len.checked_add(8).ok_or(StorageError::Capacity)?;
        if len == 0
            || frame_len > self.config().group_limits.max_encoded_bytes + 4096
            || offset % self.config().fragment_bytes + frame_len as u64
                > self.config().fragment_bytes
        {
            return Err(StorageError::Invalid(FRAME_BOUNDARY));
        }
        let bytes = self.read_checked_frame(file, offset, frame_len)?;
        let group = CompleteGroup::decode(
            &self.shape,
            bytes,
            self.config().batch_limits,
            self.config().group_limits,
        )?;
        let position = WalPosition::new(offset, frame_len as u32);
        let mut receipts = Vec::with_capacity(group.batches().len());
        for (batch, member) in group.batches().iter().zip(group.batch_offsets()) {
            validate_batch(self.config(), batch)?;
            receipts.push(receipt_for(
                self.config(),
                batch,
                member_position(position, *member, batch),
            ));
        }
        #[cfg(test)]
        self.replay_decodes.fetch_add(1, Ordering::Relaxed);
        Ok(DecodedGroup {
            group,
            receipts,
            position,
            frame_end: offset + frame_len as u64,
        })
    }

    fn read_checked_frame(
        &self,
        file: &File,
        offset: u64,
        frame_len: usize,
    ) -> Result<minibytes::Bytes, StorageError> {
        let mut frame = vec![0; frame_len];
        file.read_exact_at(&mut frame, offset % self.config().wal_file_bytes)?;
        let bytes: minibytes::Bytes = frame.into();
        let payload = CrcFrame::read_from_bytes(&bytes, 0)
            .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
        if payload.len() + 8 != frame_len {
            return Err(StorageError::Invalid(FRAME_BOUNDARY));
        }
        Ok(payload)
    }

    pub(super) fn leased_address(state: &State, address: PhysicalValueAddress) -> bool {
        if let Some(open) = &state.open_live_extent {
            let end = state
                .next_live_extent
                .as_ref()
                .map_or(open.extent.end, |next| next.extent.end);
            return address.offset >= open.extent.start
                && address
                    .offset
                    .checked_add(u64::from(address.len))
                    .is_some_and(|at| at <= end);
        }
        let _ = (state, address);
        false
    }

    pub(super) fn member_authority(
        &self,
        address: PhysicalValueAddress,
    ) -> Result<Arc<RegisteredBatch>, StorageError> {
        let file = self
            .open_replay_file(address.offset)
            .map_err(|_| StorageError::Invalid(MEMBER_MISMATCH))?;
        self.member_authority_from_file(address, &file)
    }

    /// Validate against a captured final inode, not a pathname reopened after
    /// the extent snapshot. This builds charged membership, never a WAL prefix.
    pub(super) fn member_authority_from_file(
        &self,
        address: PhysicalValueAddress,
        file: &File,
    ) -> Result<Arc<RegisteredBatch>, StorageError> {
        let invalid = || StorageError::Invalid(MEMBER_MISMATCH);
        let end = address
            .offset
            .checked_add(u64::from(address.len))
            .ok_or_else(invalid)?;
        if address.partition != self.config().partition
            || address.segment_generation != self.config().inventory.segment_generation
            || address.len <= 8
            || address.len as usize > self.config().batch_limits.max_encoded_bytes + 8
            || address.offset % self.config().fragment_bytes + u64::from(address.len)
                > self.config().fragment_bytes
        {
            return Err(invalid());
        }
        let bytes = self
            .read_checked_frame(file, address.offset, address.len as usize)
            .map_err(|_| invalid())?;
        let batch = CompleteBatch::decode(&self.shape, bytes, self.config().batch_limits)
            .map_err(|_| invalid())?;
        validate_batch(self.config(), &batch).map_err(|_| invalid())?;
        let receipt = receipt_for(
            self.config(),
            &batch,
            WalPosition::new(address.offset, address.len),
        );
        self.references.cache_registered(RegisteredBatch::new(
            batch.id(),
            batch.version(),
            batch.digest(),
            receipt.address(),
            batch.record_descriptors().into(),
            end,
        ))
    }

    /// Replay whole groups. The cursor is a WAL offset, never a receipt ordinal.
    /// `max_bytes` bounds the sum of member frame lengths plus 256 bytes per
    /// receipt. A first group larger than either page bound is returned whole.
    pub fn replay_page(
        &self,
        from: u64,
        limit: usize,
        max_bytes: usize,
        require_end: u64,
    ) -> Result<ReplayPage, StorageError> {
        let (end, retired, live, strict_end) = {
            let state = self.state.lock();
            if !state.sealed || state.writer.is_some() {
                return Err(StorageError::Invalid(
                    "replay requires sealed immutable epoch",
                ));
            }
            (
                state.appended_end,
                state.wal_root.clone(),
                state.live,
                state.wal_root.as_ref().map_or(0, |root| root.synced_end),
            )
        };
        if end < require_end {
            return Err(StorageError::Invalid(
                "sealed WAL ends below the required position",
            ));
        }
        self.replay_groups(
            from..end,
            end,
            retired.as_deref(),
            (!live).then_some(strict_end),
            limit,
            max_bytes,
        )
    }

    /// Whole groups that start in `starts`, skipping retired intervals and
    /// validated skip suffixes. Every group frame must end at or below
    /// `frame_bound` (at least `starts.end`): a group that starts below
    /// `starts.end` is read whole. An undecodable group is corruption, except
    /// at or above `tolerate_from` (a sealed non-live tail beyond its strict
    /// bound).
    pub(super) fn replay_groups(
        &self,
        starts: std::ops::Range<u64>,
        frame_bound: u64,
        retired: Option<&WalRoot>,
        tolerate_from: Option<u64>,
        limit: usize,
        max_bytes: usize,
    ) -> Result<ReplayPage, StorageError> {
        let std::ops::Range { start: from, end } = starts;
        if limit == 0 || limit > 256 {
            return Err(StorageError::Invalid("receipt page limit must be 1..=256"));
        }
        if !from.is_multiple_of(8) || from > end || end > frame_bound {
            return Err(StorageError::Invalid(FRAME_BOUNDARY));
        }
        let _admission = self.read_admission()?;
        let mut file = None;
        // A zero tail is a legal cursor only at a boundary reached from the
        // fragment's first outer frame, not inside an inner member's zero data.
        let mut boundary = from / self.config().fragment_bytes * self.config().fragment_bytes;
        // Relaxed is sufficient: this caches a scalar proof about immutable bytes.
        if self.replay_next.load(Ordering::Relaxed) == from {
            boundary = from;
        }
        while boundary < from {
            let decoded = self
                .replay_file(&mut file, boundary)
                .and_then(|file| self.decode_group_from_file(file, boundary))
                .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
            boundary = decoded
                .frame_end
                .checked_add(7)
                .ok_or(StorageError::Capacity)?
                & !7;
            if boundary > from {
                return Err(StorageError::Invalid(FRAME_BOUNDARY));
            }
        }
        let mut cursor = from;
        let mut count = 0;
        let mut bytes = 0usize;
        let mut groups = Vec::new();
        while cursor < end {
            let file_start = cursor / self.config().wal_file_bytes * self.config().wal_file_bytes;
            // Jump over a whole retired interval; never visit its files one by one.
            if let Some(skip) = retired
                .as_ref()
                .and_then(|root| root.retired.skip_end(file_start))
            {
                cursor = skip.min(end);
                continue;
            }
            let fragment_end =
                (cursor / self.config().fragment_bytes + 1) * self.config().fragment_bytes;
            let current_file = self
                .replay_file(&mut file, cursor)
                .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
            let header = self
                .frame_header(current_file, cursor)
                .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
            if header == [0xff; 8] {
                self.validate_skip_suffix(current_file, cursor, fragment_end)
                    .map_err(|_| StorageError::Invalid(FRAME_BOUNDARY))?;
                cursor = fragment_end.min(end);
                continue;
            }
            let decoded = match self.decode_group_from_file(current_file, cursor) {
                Ok(decoded) => decoded,
                Err(_) if tolerate_from.is_some_and(|strict_end| cursor >= strict_end) => {
                    cursor = fragment_end.min(end);
                    continue;
                }
                Err(_) => return Err(StorageError::Invalid(FRAME_BOUNDARY)),
            };
            if decoded.frame_end > frame_bound {
                return Err(StorageError::Invalid(FRAME_BOUNDARY));
            }
            let group_bytes = decoded.receipts.iter().try_fold(0usize, |bytes, receipt| {
                bytes
                    .checked_add(receipt.address().len as usize + 256)
                    .ok_or(StorageError::Capacity)
            })?;
            let next_bytes = bytes
                .checked_add(group_bytes)
                .ok_or(StorageError::Capacity)?;
            if count != 0 && (count + decoded.receipts.len() > limit || next_bytes > max_bytes) {
                break;
            }
            cursor = decoded
                .frame_end
                .checked_add(7)
                .ok_or(StorageError::Capacity)?
                & !7;
            count += decoded.receipts.len();
            bytes = next_bytes;
            groups.push(decoded);
            if count >= limit || bytes >= max_bytes {
                break;
            }
        }
        let next = (cursor < end).then_some(cursor);
        // This is only a boundary hint; no mutable data is published with it.
        self.replay_next
            .store(next.unwrap_or(u64::MAX), Ordering::Relaxed);
        Ok(ReplayPage { groups, next })
    }

    pub(super) fn validate_skip_suffix(
        &self,
        file: &File,
        offset: u64,
        end: u64,
    ) -> Result<(), StorageError> {
        let mut cursor = offset + 8;
        let mut buffer = [0; 65536];
        while cursor < end {
            let count = (end - cursor).min(buffer.len() as u64) as usize;
            file.read_exact_at(&mut buffer[..count], cursor % self.config().wal_file_bytes)?;
            if buffer[..count].iter().any(|byte| *byte != 0) {
                return Err(StorageError::Invalid(FRAME_BOUNDARY));
            }
            cursor += count as u64;
        }
        Ok(())
    }
}

impl PartitionLog {
    #[allow(
        clippy::expect_used,
        reason = "the header read above opened this file, so it is present"
    )]
    pub(super) fn recover_wal_replay(
        path: PathBuf,
        registration: Registration,
        shape: KeyShape,
        lock: DbLock,
        metrics: Arc<Metrics>,
        live: bool,
    ) -> Result<Self, StorageError> {
        let expected = &registration.config;
        let root = WalRoot::open(&path, expected)?;
        let mut inventory = std::collections::BTreeMap::new();
        // Retired dirents are allowed only as the exact inode of a durable
        // pending cleanup entry; anything else is not a permit to unlink.
        let mut retired_present = Vec::new();
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            if let Some((id, len)) = wal_file_entry(&entry, expected)? {
                let offset = id * expected.wal_file_bytes;
                if root.retired.contains(offset) {
                    let metadata = entry.metadata()?;
                    retired_present.push((offset, metadata.dev(), metadata.ino()));
                } else {
                    inventory.insert(offset, len);
                }
            }
        }
        retired_present.sort_unstable();
        super::file_retirement::check_retired_dirents(&root.cleanup_pending, &retired_present)?;
        if let (Some((&first, _)), Some((&last, _))) =
            (inventory.first_key_value(), inventory.last_key_value())
        {
            let mut offset = first;
            while offset < last {
                if !inventory.contains_key(&offset) {
                    // A retired interval explains its whole span at once.
                    offset = root
                        .retired
                        .skip_end(offset)
                        .ok_or(StorageError::Invalid("unexplained missing WAL file"))?;
                    continue;
                }
                offset = offset
                    .checked_add(expected.wal_file_bytes)
                    .ok_or(StorageError::Capacity)?;
            }
        }
        let scan_end = inventory
            .last_key_value()
            .map_or(root.scan_from, |(offset, len)| offset + len);
        let frontier = if live {
            let frontier: LiveFrontier = read_metadata(&path.join(LIVE_FRONTIER))?;
            let identity = LiveFrontier::new(expected);
            if frontier.database != identity.database
                || frontier.partition != identity.partition
                || frontier.epoch != identity.epoch
                || frontier.generation != identity.generation
                || (frontier.end == 0) != frontier.tail.is_none()
                || !frontier.end.is_multiple_of(4096)
                || frontier.tail.as_ref().is_some_and(|tail| {
                    !tail.offset.is_multiple_of(4096)
                        || tail.offset >= frontier.end
                        || tail.id.database != identity.database
                        || tail.version.epoch != identity.epoch
                })
            {
                return Err(StorageError::Invalid("invalid live durable frontier"));
            }
            Some(frontier)
        } else {
            None
        };
        let strict_end = root
            .synced_end
            .max(frontier.as_ref().map_or(0, |frontier| frontier.end));
        let recorded = if path.join(SEAL).exists() {
            Some(read_metadata::<SealedEnd>(&path.join(SEAL))?)
        } else {
            None
        };
        let mut physical_end = root.scan_from;
        if let Some(sealed) = &recorded {
            if sealed.format != 5
                || sealed.epoch != expected.inventory.epoch
                || sealed.generation != expected.inventory.segment_generation
            {
                return Err(StorageError::Invalid(
                    "sealed inventory no longer matches surviving frames",
                ));
            }
            let last_file = sealed.physical_end.saturating_sub(1) / expected.wal_file_bytes
                * expected.wal_file_bytes;
            if root.retired.contains(last_file) {
                physical_end = sealed.physical_end;
            }
        }
        let (references, reference_writer) = ReferenceDirectory::open(expected)?;
        let wal = Wal::open_retained(&path, expected.layout(), metrics)?;
        // Repeat the unlink of every pending dirent, then sync the directory
        // even if none remained: an earlier unlink may not be durable yet.
        // `cleanup_pending` stays in the root until target cleanup finishes.
        let present = retired_present
            .iter()
            .map(|&(offset, _, _)| {
                crate::wal::position::WalFileId::from_u64(offset / expected.wal_file_bytes)
            })
            .collect::<Vec<_>>();
        wal.retire_files(&present)?;
        if !root.cleanup_pending.is_empty() {
            File::open(&path)?.sync_all()?;
        }
        // What the repeated unlink found, for the R1 event stream: a present
        // dirent is the pending inode (checked above) and was just unlinked.
        let recovered_cleanup: Vec<_> = root
            .cleanup_pending
            .iter()
            .map(|entry| {
                let present = retired_present
                    .iter()
                    .any(|&(offset, _, _)| offset == entry.start);
                let outcome = if present {
                    super::UnlinkOutcome::Unlinked
                } else {
                    super::UnlinkOutcome::AlreadyAbsent
                };
                (*entry, outcome)
            })
            .collect();
        let mut log = Self {
            state: Mutex::new(State {
                open_live_extent: None,
                next_live_extent: None,
                last_live_adoption: None,
                live,
                lifecycle: Lifecycle::Running,
                live_tail: None,
                live_root_end: physical_end,
                writer: None,
                wal_root: Some(Arc::new(root.clone())),
                references: reference_writer,
                appended_end: physical_end,
                durable_end: physical_end,
                progress: ProgressTail::recovered(physical_end),
                namespace_synced_file: None,
                poison: None,
                sealed: true,
                stats: PartitionStats::default(),
                live_file_preparation: LiveFilePreparationStats::default(),
                #[cfg(test)]
                live_test_diagnostics: LiveTestDiagnostics::default(),
            }),
            references,
            durable_frontier: AtomicU64::new(physical_end),
            replay_next: AtomicU64::new(u64::MAX),
            #[cfg(test)]
            replay_decodes: AtomicU64::new(0),
            frontier: std::sync::OnceLock::new(),
            sync_profile: Mutex::new(StorageWriteProfile::default()),
            serve_unsynced: AtomicBool::new(false),
            persistence: Mutex::new(()),
            read_admission: parking_lot::RwLock::new(true),
            live_read_pins: std::sync::atomic::AtomicUsize::new(0),
            live_stream: live,
            // Set after the scan below.
            recovery_hole: false,
            #[cfg(test)]
            fail_inventory_witness: AtomicU8::new(0),
            #[cfg(test)]
            fail_wal_root: AtomicU8::new(0),
            #[cfg(test)]
            fail_wal_retire: AtomicBool::new(false),
            #[cfg(test)]
            r1_crash: AtomicU8::new(0),
            recovered_cleanup: Mutex::new(recovered_cleanup),
            #[cfg(test)]
            before_unlink: Mutex::new(None),
            #[cfg(test)]
            before_persist: Mutex::new(None),
            #[cfg(test)]
            fail_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_namespace_persist: AtomicBool::new(false),
            #[cfg(test)]
            fail_live_frontier: AtomicU8::new(0),
            #[cfg(test)]
            live_pause: Mutex::new(None),
            #[cfg(test)]
            fail_after_live_publish: AtomicBool::new(false),
            wal,
            registration,
            shape,
            path,
            _lock: lock,
            _local_authority: None,
        };
        let expected = log.config();
        let mut cursor = root.scan_from;
        let mut tail_seen = frontier
            .as_ref()
            .and_then(|frontier| frontier.tail.as_ref())
            .is_none_or(|tail| {
                tail.offset < root.scan_from
                    || root
                        .retired
                        .contains(tail.offset / expected.wal_file_bytes * expected.wal_file_bytes)
            });
        let mut last_tail = None;
        let mut file = None;
        // Continuity evidence: a scan that ends early (lost, torn or invalid frame, or
        // a lost skip marker) with a valid frame later left a hole below the recovered
        // tail. Unwritten space at the tail is not a hole.
        let mut gap = false;
        let mut recovery_hole = false;
        // Restart durability guard (see `recovered_suffix`): every frame and skip
        // marker accepted above the persisted root is written back from the bytes
        // this scan validated. A sealed stream was synced whole before its seal.
        let mut rewrite = recorded.is_none().then(SuffixRewrite::default);
        // Live streams stop globally at an invalid tail. Ordinary owner-RPC
        // streams scan fragments independently, including after a lost skip.
        // In both cases only present WAL files bound the uncheckpointed suffix.
        while cursor < scan_end.max(strict_end) {
            let file_start = cursor / expected.wal_file_bytes * expected.wal_file_bytes;
            if let Some(skip) = root.retired.skip_end(file_start) {
                cursor = skip;
                continue;
            }
            let fragment_end = (cursor / expected.fragment_bytes + 1) * expected.fragment_bytes;
            let current_file = log.replay_file(&mut file, cursor);
            // A real I/O error on the header read fails recovery here: a retry by
            // the decode below could read a skip marker as an invalid frame and
            // report a gap. A missing or short file falls through to the decode,
            // which classifies it.
            let header = match current_file.as_ref() {
                Ok(current) => match read_result(
                    RecoveryRead::ScanHeader,
                    cursor,
                    log.frame_header(current, cursor),
                ) {
                    Ok(header) => Some(header),
                    Err(error) if frame_missing_or_invalid(&error) => None,
                    Err(error) => return Err(error),
                },
                Err(_) => None,
            };
            if header == Some([0xff; 8]) {
                if let Err(error) = log.validate_skip_suffix(
                    current_file.as_ref().expect("header read file"),
                    cursor,
                    fragment_end,
                ) {
                    if cursor < strict_end || !frame_missing_or_invalid(&error) {
                        return Err(error);
                    }
                    gap = true;
                    if live {
                        break;
                    }
                } else if cursor >= strict_end
                    && let Some(rewrite) = rewrite.as_mut()
                {
                    log.rewrite_skip_marker(rewrite, cursor, fragment_end)?;
                }
                cursor = fragment_end;
                continue;
            }
            let decoded =
                match current_file.and_then(|file| log.decode_group_from_file(file, cursor)) {
                    Ok(decoded) => decoded,
                    Err(error) if cursor < strict_end || !frame_missing_or_invalid(&error) => {
                        return Err(error);
                    }
                    Err(_) if live => {
                        gap = true;
                        break;
                    }
                    Err(_) => {
                        gap = true;
                        cursor = fragment_end;
                        continue;
                    }
                };
            if live
                && (!cursor.is_multiple_of(4096)
                    || !decoded.position.frame_len().is_multiple_of(4096)
                    || decoded.group.encoded()[8..12] != [0, 3, 0, 12])
            {
                if cursor < strict_end {
                    return Err(StorageError::Invalid(
                        "invalid live recovery frame geometry",
                    ));
                }
                gap = true;
                break;
            }
            if let Some(frontier) = &frontier {
                if cursor < frontier.end && decoded.frame_end > frontier.end {
                    return Err(StorageError::Invalid("live frame crosses durable frontier"));
                }
                if decoded.frame_end == frontier.end {
                    let tail = frontier
                        .tail
                        .as_ref()
                        .ok_or(StorageError::Invalid("missing frontier tail"))?;
                    let last = decoded
                        .group
                        .batches()
                        .last()
                        .ok_or(StorageError::Invalid("empty live group"))?;
                    if tail.offset != cursor
                        || tail.crc != crc32fast::hash(decoded.group.encoded())
                        || tail.id != last.id()
                        || tail.version != last.version()
                        || tail.digest != last.digest()
                    {
                        return Err(StorageError::Invalid("live frontier tail mismatch"));
                    }
                    tail_seen = true;
                }
            }
            let duplicate = decoded.group.batches().iter().try_fold(
                false,
                |found, batch| -> Result<bool, StorageError> {
                    Ok(found
                        || log.references.lookup_id(batch.id())?.is_some()
                        || log.references.lookup_version(batch.version())?.is_some())
                },
            )?;
            if duplicate {
                if cursor >= strict_end && live {
                    gap = true;
                    break;
                }
                return Err(StorageError::Invalid(
                    "duplicate identity or version in replay suffix",
                ));
            }
            if let Some(batch) = decoded.group.batches().last() {
                last_tail = Some(LiveFrontierTail {
                    offset: cursor,
                    crc: crc32fast::hash(decoded.group.encoded()),
                    id: batch.id(),
                    version: batch.version(),
                    digest: batch.digest(),
                });
            }
            let metadata = decoded
                .group
                .batches()
                .iter()
                .zip(&decoded.receipts)
                .map(|(batch, receipt)| {
                    RegisteredBatch::new(
                        batch.id(),
                        batch.version(),
                        batch.digest(),
                        receipt.address(),
                        batch.record_descriptors().into(),
                        decoded.frame_end,
                    )
                })
                .collect();
            // Owner-RPC streams scan fragments independently: a group adopted after an
            // early fragment end lies above a hole.
            recovery_hole |= gap;
            log.state.lock().references.publish_group(metadata)?;
            if cursor >= strict_end
                && let Some(rewrite) = rewrite.as_mut()
            {
                log.rewrite_validated_frame(rewrite, cursor, decoded.group.encoded())?;
            }
            physical_end = physical_end.max(decoded.frame_end);
            cursor = decoded
                .frame_end
                .checked_add(7)
                .ok_or(StorageError::Capacity)?
                & !7;
        }
        // A live scan stops at its first defect; probe the first frame of every later
        // fragment (bounded by the scan end) for a valid frame above it. An I/O error
        // leaves continuity unknown, so it fails recovery instead of reading as clean.
        let mut probe = (cursor / expected.fragment_bytes + 1) * expected.fragment_bytes;
        while live && gap && !recovery_hole && probe < scan_end {
            let file_start = probe / expected.wal_file_bytes * expected.wal_file_bytes;
            if !root.retired.contains(file_start) {
                recovery_hole = log.probe_valid_frame(&mut file, probe)?;
            }
            probe = probe
                .checked_add(expected.fragment_bytes)
                .ok_or(StorageError::Capacity)?;
        }
        if cursor < strict_end {
            return Err(StorageError::Invalid("corrupt durable WAL frame"));
        }
        if !tail_seen {
            return Err(StorageError::Invalid("missing live durable frontier tail"));
        }
        // The rewritten suffix reaches the device before the barrier below may
        // certify it and before any root or seal is installed.
        if let Some(rewrite) = rewrite {
            log.finish_suffix_rewrite(rewrite)?;
        }
        log.wal.persist_registered_files()?;
        if live
            && physical_end > frontier.as_ref().map_or(0, |frontier| frontier.end)
            && let Some(tail) = last_tail
        {
            let mut adopted = LiveFrontier::new(expected);
            adopted.end = physical_end;
            adopted.tail = Some(tail);
            install_metadata(&log.path, LIVE_FRONTIER, &encode_metadata(&adopted)?)?;
        }
        if recorded
            .as_ref()
            .is_some_and(|sealed| sealed.physical_end != physical_end)
        {
            return Err(StorageError::Invalid(
                "sealed inventory no longer matches surviving frames",
            ));
        }
        let end = SealedEnd {
            format: 5,
            epoch: expected.inventory.epoch,
            generation: expected.inventory.segment_generation,
            frame_count: log.receipt_count(),
            physical_end,
        };
        install_metadata(&log.path, SEAL, &encode_metadata(&end)?)?;
        {
            let mut state = log.state.lock();
            state.appended_end = physical_end;
            state.durable_end = physical_end;
            state.progress = ProgressTail::recovered(physical_end);
            {
                state.live_root_end = physical_end;
            }
        }
        log.durable_frontier.store(physical_end, Ordering::Release);
        log.recovery_hole = recovery_hole;
        Ok(log)
    }
}
