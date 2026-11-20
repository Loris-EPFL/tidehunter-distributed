//! Lease-logical seal of one passed live WAL file inside the serving stream.
//!
//! A seal proves that the allocator permanently passed the file, that no open,
//! preleased or reserved extent owns it, and that every byte is a complete
//! validated 4 KiB group or a validated skip marker plus zeros. The writable
//! target namespace stays installed so live values in the file stay readable;
//! the owner refuses any new grant inside a sealed file. A seal never proves
//! that the file is unreachable.
//!
//! It also does not prove durability, and must not: native has no such
//! precondition either (its `Wal::gc` reclaims on
//! `min(relocation start, control-region position)` alone). A retired file's
//! durability is the coordinator's Version B term of the retirement witness
//! (the synchronous tier has no B cut because every write was already forced).
//! A crash can therefore leave a durable seal over a tail the owner never
//! synced, which loses nothing that the tier had acknowledged: at the native
//! tier that tail was only in the page cache, exactly as it would be without
//! this file's retirement.
//!
//! Steps: capture under `state` (no I/O) -> whole-file scan with no lock ->
//! `persistence` -> recheck under `state` -> root install with `state`
//! released -> publish. Seals are installed in ascending file order.
use super::file_retirement::CommitStep;
use super::*;

/// Files sealed per maintenance kick; bounds one kick's scan and root work.
const SEALS_PER_KICK: usize = 4;

struct SealCandidate {
    start: u64,
    end: u64,
    identity: WalInode,
    file: File,
}

/// First and last batch sequence of one file, in file order.
#[derive(Clone, Copy)]
struct SealSequences {
    first: u64,
    last: u64,
}

impl PartitionLog {
    /// Seal up to `SEALS_PER_KICK` passed files in ascending order. Returns
    /// how many were sealed; zero when the next file is not ready. Never waits
    /// for a grant, adoption or position proof, and adds no write-path sync.
    pub(crate) fn seal_ready_files(&self) -> Result<usize, StorageError> {
        let mut sealed = 0;
        while sealed < SEALS_PER_KICK {
            let Some(candidate) = self.capture_sealable_file()? else {
                break;
            };
            let sequences = self.validate_file_seal(&candidate)?;
            if !self.install_file_seal(&candidate, sequences)? {
                break;
            }
            sealed += 1;
        }
        Ok(sealed)
    }

    /// Next file to seal, if the stream state already allows it. No I/O
    /// under `state`; the file is opened after the lock is released.
    fn capture_sealable_file(&self) -> Result<Option<SealCandidate>, StorageError> {
        let size = self.config().wal_file_bytes;
        let (start, end) = {
            let state = self.state.lock();
            let Some(root) = state.wal_root.clone() else {
                return Ok(None);
            };
            let start = root.seal_cursor();
            let end = start.checked_add(size).ok_or(StorageError::Capacity)?;
            if !Self::seal_allowed(&state, &root, start, size) {
                return Ok(None);
            }
            (start, end)
        };
        let file = self.open_replay_file(start)?;
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() || metadata.len() != size {
            return Err(StorageError::Invalid(
                "passed live WAL file has wrong geometry",
            ));
        }
        Ok(Some(SealCandidate {
            start,
            end,
            identity: WalInode {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
            file,
        }))
    }

    /// The stream serves, is healthy, the allocator passed the file and no
    /// extent or reservation owns it. Coverage by the durable index is the
    /// coordinator's control-region frontier (the retirement witness, I8), the
    /// same term native `Wal::gc` uses; an owner-local position root is no
    /// longer a precondition, exactly as native has none.
    fn seal_allowed(state: &State, root: &WalRoot, start: u64, size: u64) -> bool {
        let index = start / size;
        let owned = |segment: &crate::distributed::nvmeof::live_storage::LiveWalSegment| {
            segment.file_index == index
        };
        state.live
            && !state.sealed
            && !state.is_sealing()
            && state.poison.is_none()
            && root.sealed_files.len() < retired_files::MAX_ROOT_ENTRIES
            && state
                .writer
                .as_ref()
                .is_some_and(|writer| index < writer.position() / size)
            && !state
                .open_live_extent
                .as_ref()
                .is_some_and(|open| owned(&open.extent.segment))
            && !state
                .next_live_extent
                .as_ref()
                .is_some_and(|open| owned(&open.extent.segment))
    }

    /// Scan the pinned inode from its first byte to its end. Every fragment
    /// must be complete live groups, then at most one validated skip marker
    /// plus zeros reaching the fragment end. Batch sequences must strictly
    /// ascend in file order (design §3 step 3); returns the first and last.
    /// The file is past the allocator and no extent owns it, so every frame in
    /// it was completed: an invalid frame, a repeated or backward sequence, or
    /// an unexplained hole is corruption, and the stream is poisoned. An I/O
    /// error is only "not sealed".
    fn validate_file_seal(
        &self,
        candidate: &SealCandidate,
    ) -> Result<Option<SealSequences>, StorageError> {
        let config = self.config();
        let mut sequences: Option<SealSequences> = None;
        let mut fragment = candidate.start;
        while fragment < candidate.end {
            let fragment_end = fragment
                .checked_add(config.fragment_bytes)
                .ok_or(StorageError::Capacity)?;
            let mut cursor = fragment;
            while cursor < fragment_end {
                let header = self
                    .frame_header(&candidate.file, cursor)
                    .map_err(|error| self.seal_scan_failure(error))?;
                if header == [0xff; 8] {
                    self.validate_skip_suffix(&candidate.file, cursor, fragment_end)
                        .map_err(|error| self.seal_scan_failure(error))?;
                    break;
                }
                let decoded = self
                    .decode_group_from_file(&candidate.file, cursor)
                    .map_err(|error| self.seal_scan_failure(error))?;
                if !cursor.is_multiple_of(4096)
                    || !decoded.position.frame_len().is_multiple_of(4096)
                    || decoded.group.encoded()[8..12] != [0, 3, 0, 12]
                {
                    return Err(self.seal_scan_failure(StorageError::Invalid(
                        "invalid live frame geometry below the durable position",
                    )));
                }
                for batch in decoded.group.batches() {
                    if batch.version().epoch != config.inventory.epoch {
                        return Err(self.seal_scan_failure(StorageError::Invalid(
                            "live frame from another fencing generation",
                        )));
                    }
                    let sequence = batch.version().sequence;
                    sequences = match sequences {
                        None => Some(SealSequences {
                            first: sequence,
                            last: sequence,
                        }),
                        Some(seen) if sequence > seen.last => Some(SealSequences {
                            last: sequence,
                            ..seen
                        }),
                        Some(_) => {
                            return Err(self.seal_scan_failure(StorageError::Invalid(
                                "live batch sequences do not strictly ascend in the file",
                            )));
                        }
                    };
                }
                cursor = decoded.frame_end;
            }
            fragment = fragment_end;
        }
        Ok(sequences)
    }

    /// Classify a scan failure. Bytes below the installed position root are
    /// certified, so anything but a real I/O error is corruption.
    fn seal_scan_failure(&self, error: StorageError) -> StorageError {
        let io_only = matches!(&error, StorageError::Io(io)
            if io.kind() != io::ErrorKind::UnexpectedEof);
        if !io_only {
            self.state.lock().poison = Some(WriterPoison::Fatal);
        }
        error
    }

    /// Recheck and persist one seal. Returns false when the stream moved on
    /// (another seal, a fence or a seal of the stream) and nothing changed.
    /// Sequences also ascend across files: the immediately preceding file's
    /// seal, while it is still recorded, must end below this file's first.
    fn install_file_seal(
        &self,
        candidate: &SealCandidate,
        sequences: Option<SealSequences>,
    ) -> Result<bool, StorageError> {
        let size = self.config().wal_file_bytes;
        // The path must still name the scanned inode (owner-only directory).
        if self.observe_wal_inode(candidate.start)? != Some(candidate.identity) {
            return Err(StorageError::Invalid(
                "WAL path changed during the file seal scan",
            ));
        }
        let _persistence = self.persistence.lock();
        let current = {
            let state = self.state.lock();
            let Some(current) = state.wal_root.clone() else {
                return Ok(false);
            };
            if current.seal_cursor() != candidate.start
                || !Self::seal_allowed(&state, &current, candidate.start, size)
            {
                return Ok(false);
            }
            current
        };
        let previous = candidate
            .start
            .checked_sub(size)
            .and_then(|start| {
                current
                    .sealed_files
                    .binary_search_by_key(&start, |seal| seal.start)
                    .ok()
            })
            .and_then(|index| current.sealed_files[index].last_batch_sequence);
        if let (Some(previous), Some(sequences)) = (previous, sequences)
            && sequences.first <= previous
        {
            return Err(self.seal_scan_failure(StorageError::Invalid(
                "live batch sequences do not ascend across sealed files",
            )));
        }
        let mut root = WalRoot::clone(&current);
        root.generation = root
            .generation
            .checked_add(1)
            .ok_or(StorageError::Capacity)?;
        root.sealed_files.push(FileSeal {
            start: candidate.start,
            end: candidate.end,
            device: candidate.identity.device,
            inode: candidate.identity.inode,
            root_generation: root.generation,
            last_batch_sequence: sequences.map(|sequences| sequences.last),
        });
        self.commit_root(root, CommitStep::Seal)?;
        Ok(true)
    }

    /// File that the next live grant allocates in (allocator position file).
    pub(crate) fn live_allocator_file(&self) -> Result<u64, StorageError> {
        let state = self.state.lock();
        Ok(state
            .writer
            .as_ref()
            .ok_or(StorageError::Sealed)?
            .position()
            / self.config().wal_file_bytes)
    }
}
