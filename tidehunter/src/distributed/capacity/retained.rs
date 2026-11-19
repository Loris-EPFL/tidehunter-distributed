//! Attribution of retained Exact-image puts to owner WAL files.
//!
//! Member counters are additive. Complete batches are counted once per
//! `batch_address`, and equal addresses must carry an equal batch identity and
//! digest. The distinct-batch set is bounded in memory; above the allowance it
//! spills sorted metadata-only runs and merges them at the end.

use super::R0Error;
use crate::distributed::types::{BatchId, BatchVersion, RecordKind, ValueRecordReference};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sorted spill runs merged at once; each run holds `max_batch_records`.
const MAX_SPILL_RUNS: usize = 1024;
const RECORD_BYTES: usize = 4 + 8 + 8 + 4 + 16 + 16 + 8 + 8 + 32;

/// One owner WAL file: partition, fencing generation (segment generation of
/// the address) and aligned file start.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct FileId {
    pub partition: u32,
    pub generation: u64,
    pub start: u64,
}

/// Retained-image counters of one file.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FileRetained {
    pub put_members: u64,
    pub member_bytes: u64,
    pub value_bytes: u64,
    pub batches: u64,
    pub batch_bytes: u64,
    pub affected_cells: u64,
}

/// Per-sample accumulator. The walk calls `add_put` for every retained version
/// of a cell, then `end_cell`.
pub(crate) struct RetainedAccumulator {
    wal_file_bytes: u64,
    files: BTreeMap<FileId, FileRetained>,
    cell_files: Vec<FileId>,
    dedup: BatchDedup,
}

impl RetainedAccumulator {
    pub(crate) fn new(
        wal_file_bytes: u64,
        max_batch_records: usize,
        scratch: Option<PathBuf>,
    ) -> Result<Self, R0Error> {
        if wal_file_bytes == 0 || max_batch_records == 0 {
            return Err(R0Error::new("invalid R0 accumulator geometry"));
        }
        Ok(Self {
            wal_file_bytes,
            files: BTreeMap::new(),
            cell_files: Vec::new(),
            dedup: BatchDedup {
                limit: max_batch_records,
                memory: Vec::new(),
                scratch,
                runs: Vec::new(),
                error: None,
            },
        })
    }

    /// Count one retained version. Deletes have no value read obligation.
    pub(crate) fn add_put(&mut self, reference: &ValueRecordReference) -> Result<(), R0Error> {
        if reference.descriptor.kind != RecordKind::Put {
            return Ok(());
        }
        let address = reference.batch_address;
        let mut file = address.offset - address.offset % self.wal_file_bytes;
        let end = address
            .offset
            .checked_add(u64::from(address.len))
            .ok_or_else(|| R0Error::new("retained batch address overflow"))?;
        // A batch never crosses a fragment, so this normally visits one file;
        // the loop keeps the conservative intersection predicate anyway.
        while file < end {
            let id = FileId {
                partition: address.partition,
                generation: address.segment_generation,
                start: file,
            };
            let counters = self.files.entry(id).or_default();
            counters.put_members += 1;
            counters.member_bytes += u64::from(reference.descriptor.len);
            counters.value_bytes += u64::from(reference.descriptor.value_len);
            self.cell_files.push(id);
            file = file
                .checked_add(self.wal_file_bytes)
                .ok_or_else(|| R0Error::new("retained file bound overflow"))?;
        }
        self.dedup.push(BatchRecord::new(reference));
        Ok(())
    }

    pub(crate) fn end_cell(&mut self) {
        self.cell_files.sort_unstable();
        self.cell_files.dedup();
        for id in self.cell_files.drain(..) {
            if let Some(counters) = self.files.get_mut(&id) {
                counters.affected_cells += 1;
            }
        }
    }

    /// Finish the distinct-batch merge. On a conflict or spill failure the
    /// member counters remain valid and the error names why batch counts are not.
    pub(crate) fn finish(mut self) -> (BTreeMap<FileId, FileRetained>, Option<R0Error>) {
        self.end_cell();
        let wal_file_bytes = self.wal_file_bytes;
        let files = &mut self.files;
        let result = self.dedup.finish(|record| {
            let end = record.offset + u64::from(record.len);
            let mut start = record.offset - record.offset % wal_file_bytes;
            while start < end {
                let id = FileId {
                    partition: record.partition,
                    generation: record.generation,
                    start,
                };
                if let Some(counters) = files.get_mut(&id) {
                    counters.batches += 1;
                    counters.batch_bytes += u64::from(record.len);
                }
                start += wal_file_bytes;
            }
        });
        (self.files, result.err())
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BatchRecord {
    // Address first: equal addresses are adjacent after sorting.
    partition: u32,
    generation: u64,
    offset: u64,
    len: u32,
    id: BatchId,
    version: BatchVersion,
    digest: [u8; 32],
}

impl BatchRecord {
    fn new(reference: &ValueRecordReference) -> Self {
        let address = reference.batch_address;
        Self {
            partition: address.partition,
            generation: address.segment_generation,
            offset: address.offset,
            len: address.len,
            id: reference.batch_id,
            version: reference.batch_version,
            digest: reference.batch_digest.0,
        }
    }

    fn same_address(&self, other: &Self) -> bool {
        (self.partition, self.generation, self.offset, self.len)
            == (other.partition, other.generation, other.offset, other.len)
    }

    fn encode(&self, bytes: &mut [u8; RECORD_BYTES]) {
        bytes[0..4].copy_from_slice(&self.partition.to_be_bytes());
        bytes[4..12].copy_from_slice(&self.generation.to_be_bytes());
        bytes[12..20].copy_from_slice(&self.offset.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.len.to_be_bytes());
        bytes[24..40].copy_from_slice(&self.id.database);
        bytes[40..56].copy_from_slice(&self.id.operation);
        bytes[56..64].copy_from_slice(&self.version.epoch.to_be_bytes());
        bytes[64..72].copy_from_slice(&self.version.sequence.to_be_bytes());
        bytes[72..104].copy_from_slice(&self.digest);
    }

    fn decode(bytes: &[u8; RECORD_BYTES]) -> Self {
        let mut u32_at = [0; 4];
        let mut u64_at = [0; 8];
        let mut read_u32 = |at: usize| {
            u32_at.copy_from_slice(&bytes[at..at + 4]);
            u32::from_be_bytes(u32_at)
        };
        let partition = read_u32(0);
        let len = read_u32(20);
        let mut read_u64 = |at: usize| {
            u64_at.copy_from_slice(&bytes[at..at + 8]);
            u64::from_be_bytes(u64_at)
        };
        let generation = read_u64(4);
        let offset = read_u64(12);
        let epoch = read_u64(56);
        let sequence = read_u64(64);
        let mut database = [0; 16];
        database.copy_from_slice(&bytes[24..40]);
        let mut operation = [0; 16];
        operation.copy_from_slice(&bytes[40..56]);
        let mut digest = [0; 32];
        digest.copy_from_slice(&bytes[72..104]);
        Self {
            partition,
            generation,
            offset,
            len,
            id: BatchId {
                database,
                operation,
            },
            version: BatchVersion { epoch, sequence },
            digest,
        }
    }
}

fn conflict() -> R0Error {
    R0Error::new("retained batch address names two batch identities")
}

/// Bounded distinct-batch set with sorted spill runs.
struct BatchDedup {
    limit: usize,
    memory: Vec<BatchRecord>,
    scratch: Option<PathBuf>,
    runs: Vec<PathBuf>,
    error: Option<R0Error>,
}

impl BatchDedup {
    fn push(&mut self, record: BatchRecord) {
        if self.error.is_some() {
            return;
        }
        self.memory.push(record);
        if self.memory.len() >= self.limit
            && let Err(error) = self.spill()
        {
            self.error = Some(error);
            self.memory = Vec::new();
        }
    }

    /// Sort, remove exact duplicates (rejecting conflicts) and write one run.
    fn sort_memory(&mut self) -> Result<(), R0Error> {
        self.memory.sort_unstable();
        if self
            .memory
            .windows(2)
            .any(|pair| pair[0].same_address(&pair[1]) && pair[0] != pair[1])
        {
            return Err(conflict());
        }
        self.memory.dedup();
        Ok(())
    }

    fn spill(&mut self) -> Result<(), R0Error> {
        self.sort_memory()?;
        // A fully deduplicated page may already fit again.
        if self.memory.len() < self.limit {
            return Ok(());
        }
        let Some(scratch) = &self.scratch else {
            return Err(R0Error::new(
                "distinct batch set exceeds the R0 allowance and no spill directory is set",
            ));
        };
        if self.runs.len() >= MAX_SPILL_RUNS {
            return Err(R0Error::new("R0 batch spill exceeds its run bound"));
        }
        static NEXT_RUN: AtomicU64 = AtomicU64::new(0);
        // Relaxed: unique file-name counter only.
        let run = NEXT_RUN.fetch_add(1, Ordering::Relaxed);
        std::fs::create_dir_all(scratch).map_err(io_error)?;
        let path = scratch.join(format!("r0-batches-{}-{run}.run", std::process::id()));
        let mut writer = BufWriter::new(File::create_new(&path).map_err(io_error)?);
        self.runs.push(path);
        let mut bytes = [0; RECORD_BYTES];
        for record in &self.memory {
            record.encode(&mut bytes);
            writer.write_all(&bytes).map_err(io_error)?;
        }
        writer.flush().map_err(io_error)?;
        self.memory.clear();
        Ok(())
    }

    fn finish(&mut self, mut visit: impl FnMut(&BatchRecord)) -> Result<(), R0Error> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        self.sort_memory()?;
        let mut readers = Vec::with_capacity(self.runs.len());
        for path in &self.runs {
            readers.push(BufReader::new(File::open(path).map_err(io_error)?));
        }
        let read = |reader: &mut BufReader<File>| -> Result<Option<BatchRecord>, R0Error> {
            let mut bytes = [0; RECORD_BYTES];
            match reader.read_exact(&mut bytes) {
                Ok(()) => Ok(Some(BatchRecord::decode(&bytes))),
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
                Err(error) => Err(io_error(error)),
            }
        };
        // Source `readers.len()` is the sorted in-memory tail.
        let memory_source = readers.len();
        let mut memory = self.memory.iter();
        let mut heap = BinaryHeap::with_capacity(readers.len() + 1);
        for (source, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = read(reader)? {
                heap.push(Reverse((record, source)));
            }
        }
        if let Some(record) = memory.next() {
            heap.push(Reverse((*record, memory_source)));
        }
        let mut previous: Option<BatchRecord> = None;
        while let Some(Reverse((record, source))) = heap.pop() {
            let next = if source == memory_source {
                memory.next().copied()
            } else {
                read(&mut readers[source])?
            };
            if let Some(next) = next {
                heap.push(Reverse((next, source)));
            }
            match &previous {
                Some(seen) if seen.same_address(&record) => {
                    if *seen != record {
                        return Err(conflict());
                    }
                }
                _ => {
                    visit(&record);
                    previous = Some(record);
                }
            }
        }
        Ok(())
    }
}

impl Drop for BatchDedup {
    fn drop(&mut self) {
        // Metadata-only scratch; a failed removal leaves a file under the run
        // result directory and changes no measurement.
        for path in &self.runs {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn io_error(error: std::io::Error) -> R0Error {
    R0Error::new(format!("R0 batch spill I/O: {error}"))
}
