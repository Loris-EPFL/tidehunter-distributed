//! Pageable exact retry authority. An immutable Patricia-tree root distinguishes
//! an absent identity from a missing/corrupt authority page. New pages append to
//! one metadata-only file; a checkpoint syncs pages before adopting its root.

use super::super::tcp::checkpoint::PrefixDigest;
use super::*;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct HistoryIoStats {
    pub stream_opens: u64,
    pub previous_slot_reads: u64,
    pub receipt_write_ops: u64,
    pub receipt_write_bytes: u64,
    pub buffer_flushes: u64,
    pub node_write_ops: u64,
    pub node_write_bytes: u64,
    pub sync_operations: u64,
}

const SLOT_BYTES: usize = 256;
const NODE_CACHE_ENTRIES: usize = 64;
type NodeCache = lru::LruCache<HistoryPointer, Node>;

fn node_digest(slot: &[u8; SLOT_BYTES]) -> [u8; 32] {
    let hash = blake2b_simd::Params::new().hash_length(32).hash(slot);
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.as_bytes());
    digest
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub(super) struct HistoryPointer {
    offset: u64,
    digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct HistoryEntry {
    receipt: CheckpointReceipt,
    ordinal: usize,
    prefix_state: [u8; 32],
}

#[derive(Clone, Copy, Serialize, Deserialize)]
enum Node {
    Leaf(HistoryEntry),
    Branch {
        bit: u8,
        left: HistoryPointer,
        right: HistoryPointer,
    },
}

#[derive(Clone)]
pub(super) struct ReceiptHistory {
    path: PathBuf,
    database: [u8; 16],
    nodes: Arc<File>,
    // Shared by frozen/live roots; bounded independently of lifetime history.
    // Only fully checksum/hash-validated or locally encoded nodes enter it.
    cache: Arc<Mutex<NodeCache>>,
    append_offset: u64,
    pub(super) root: Option<HistoryPointer>,
    pub(super) counts: BTreeMap<(u32, u64), usize>,
    pub(super) chains: BTreeMap<(u32, u64), [u8; 32]>,
    pub(super) io_stats: HistoryIoStats,
}

fn encode_slot(value: &impl Serialize) -> Result<[u8; SLOT_BYTES], LocalDbError> {
    let mut slot = [0; SLOT_BYTES];
    let mut output = std::io::Cursor::new(&mut slot[8..]);
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize_into(&mut output, value)
        .map_err(LocalDbError::component)?;
    let len = output.position() as usize;
    slot[..4].copy_from_slice(&(len as u32).to_be_bytes());
    let crc = crc32fast::hash(&slot[8..8 + len]);
    slot[4..8].copy_from_slice(&crc.to_be_bytes());
    Ok(slot)
}

fn decode_slot<T: serde::de::DeserializeOwned>(slot: &[u8; SLOT_BYTES]) -> Result<T, LocalDbError> {
    let len = u32::from_be_bytes(slot[..4].try_into().expect("fixed slot header")) as usize;
    if len > SLOT_BYTES - 8
        || slot[8 + len..].iter().any(|byte| *byte != 0)
        || crc32fast::hash(&slot[8..8 + len])
            != u32::from_be_bytes(slot[4..8].try_into().expect("fixed CRC header"))
    {
        return Err(LocalDbError::new(
            "retained authority page checksum differs",
        ));
    }
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .deserialize(&slot[8..8 + len])
        .map_err(LocalDbError::component)
}

impl ReceiptHistory {
    pub(super) fn open(path: &Path, database: [u8; 16]) -> Result<Self, LocalDbError> {
        let path = path.join("receipt-history-v2");
        match fs::create_dir(&path) {
            Ok(()) => File::open(
                path.parent()
                    .ok_or_else(|| LocalDbError::new("receipt history parent absent"))?,
            )
            .and_then(|parent| parent.sync_all())
            .map_err(LocalDbError::component)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if !fs::symlink_metadata(&path)
                    .map_err(LocalDbError::component)?
                    .file_type()
                    .is_dir()
                {
                    return Err(LocalDbError::new(
                        "receipt history is not an owned directory",
                    ));
                }
            }
            Err(error) => return Err(LocalDbError::component(error)),
        }
        let nodes = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("nodes.pages"))
            .map_err(LocalDbError::component)?;
        let append_offset = nodes
            .metadata()
            .map_err(LocalDbError::component)?
            .len()
            .div_ceil(SLOT_BYTES as u64)
            .checked_mul(SLOT_BYTES as u64)
            .ok_or_else(|| LocalDbError::new("retained authority offset exhausted"))?;
        let capacity =
            std::num::NonZeroUsize::new(NODE_CACHE_ENTRIES).expect("nonzero node cache budget");
        Ok(Self {
            path,
            database,
            nodes: Arc::new(nodes),
            append_offset,
            root: None,
            cache: Arc::new(Mutex::new(NodeCache::new(capacity))),
            counts: BTreeMap::new(),
            chains: BTreeMap::new(),
            io_stats: HistoryIoStats::default(),
        })
    }

    /// Reserved bounded node-cache charge, including duplicate hash keys,
    /// linked-list ownership and capacity slack; excludes allocator overhead.
    pub(super) fn cached_metadata_bytes(&self) -> usize {
        std::mem::size_of::<NodeCache>()
            + NODE_CACHE_ENTRIES
                * (std::mem::size_of::<Node>()
                    + 2 * std::mem::size_of::<HistoryPointer>()
                    + 8 * std::mem::size_of::<usize>())
    }

    fn read_node(&self, pointer: HistoryPointer) -> Result<Node, LocalDbError> {
        if !pointer.offset.is_multiple_of(SLOT_BYTES as u64) {
            return Err(LocalDbError::new(
                "retained retry authority page alignment differs",
            ));
        }
        if let Some(node) = { self.cache.lock().get(&pointer).copied() } {
            return Ok(node);
        }
        let mut slot = [0; SLOT_BYTES];
        self.nodes
            .read_exact_at(&mut slot, pointer.offset)
            .map_err(LocalDbError::component)?;
        let digest = node_digest(&slot);
        if digest != pointer.digest {
            return Err(LocalDbError::new(
                "retained retry authority node digest differs",
            ));
        }
        let node = decode_slot(&slot)?;
        self.cache.lock().put(pointer, node);
        Ok(node)
    }

    fn write_node(&mut self, node: &Node) -> Result<HistoryPointer, LocalDbError> {
        let slot = encode_slot(node)?;
        // One checkpoint worker owns append. Interrupted trailing pages are never
        // reused or scanned as authority; only the adopted root grants reachability.
        let offset = self.append_offset;
        self.append_offset = offset
            .checked_add(SLOT_BYTES as u64)
            .ok_or_else(|| LocalDbError::new("retained authority offset exhausted"))?;
        self.nodes
            .write_all_at(&slot, offset)
            .map_err(LocalDbError::component)?;
        self.io_stats.node_write_ops += 1;
        self.io_stats.node_write_bytes += SLOT_BYTES as u64;
        let pointer = HistoryPointer {
            offset,
            digest: node_digest(&slot),
        };
        self.cache.lock().put(pointer, *node);
        Ok(pointer)
    }

    fn bit(operation: [u8; 16], bit: u8) -> bool {
        operation[usize::from(bit) / 8] & (0x80 >> (bit % 8)) != 0
    }

    pub(super) fn lookup(&self, id: BatchId) -> Result<Option<AppendedBatch>, LocalDbError> {
        if id.database != self.database {
            return Ok(None);
        }
        let Some(mut pointer) = self.root else {
            return Ok(None);
        };
        let mut previous = None;
        for _ in 0..=128 {
            match self.read_node(pointer)? {
                Node::Leaf(entry) => {
                    return Ok((entry.receipt.id == id).then(|| entry.receipt.authenticated()));
                }
                Node::Branch { bit, left, right } => {
                    if bit >= 128 || previous.is_some_and(|old| old >= bit) {
                        return Err(LocalDbError::new("invalid retained retry authority tree"));
                    }
                    previous = Some(bit);
                    pointer = if Self::bit(id.operation, bit) {
                        right
                    } else {
                        left
                    };
                }
            }
        }
        Err(LocalDbError::new(
            "retained retry authority tree depth exceeded",
        ))
    }

    fn insert(&mut self, entry: HistoryEntry) -> Result<(), LocalDbError> {
        let Some(root) = self.root else {
            self.root = Some(self.write_node(&Node::Leaf(entry))?);
            return Ok(());
        };
        let mut pointer = root;
        let mut path =
            smallvec::SmallVec::<[(u8, HistoryPointer, HistoryPointer, bool); 16]>::new();
        let old = loop {
            if path.len() > 128 {
                return Err(LocalDbError::new(
                    "retained retry authority tree depth exceeded",
                ));
            }
            match self.read_node(pointer)? {
                Node::Leaf(old) => break old,
                Node::Branch { bit, left, right } => {
                    if bit >= 128 || path.last().is_some_and(|(old, _, _, _)| *old >= bit) {
                        return Err(LocalDbError::new("invalid retained retry authority tree"));
                    }
                    let go_right = Self::bit(entry.receipt.id.operation, bit);
                    path.push((bit, left, right, go_right));
                    pointer = if go_right { right } else { left };
                }
            }
        };
        if old.receipt.id == entry.receipt.id {
            if old.receipt.authenticated() != entry.receipt.authenticated()
                || old.ordinal != entry.ordinal
                || old.prefix_state != entry.prefix_state
            {
                return Err(LocalDbError::new(
                    "conflicting retained retry authority identity",
                ));
            }
            return Ok(());
        }
        let difference = old
            .receipt
            .id
            .operation
            .iter()
            .zip(entry.receipt.id.operation)
            .enumerate()
            .find_map(|(index, (old, new))| {
                let xor = old ^ new;
                (xor != 0).then(|| (index * 8 + xor.leading_zeros() as usize) as u8)
            })
            .ok_or_else(|| LocalDbError::new("conflicting retained retry authority database"))?;
        while path.last().is_some_and(|(bit, _, _, _)| *bit >= difference) {
            path.pop();
        }
        let subtree = if let Some((_, left, right, go_right)) = path.last() {
            if *go_right { *right } else { *left }
        } else {
            root
        };
        let leaf = self.write_node(&Node::Leaf(entry))?;
        let (left, right) = if Self::bit(entry.receipt.id.operation, difference) {
            (subtree, leaf)
        } else {
            (leaf, subtree)
        };
        let mut replacement = self.write_node(&Node::Branch {
            bit: difference,
            left,
            right,
        })?;
        for (bit, left, right, go_right) in path.into_iter().rev() {
            let node = if go_right {
                Node::Branch {
                    bit,
                    left,
                    right: replacement,
                }
            } else {
                Node::Branch {
                    bit,
                    left: replacement,
                    right,
                }
            };
            replacement = self.write_node(&node)?;
        }
        self.root = Some(replacement);
        Ok(())
    }

    fn stream_path(&self, partition: u32, epoch: u64) -> PathBuf {
        self.path.join(format!("stream-{partition}-{epoch}.pages"))
    }

    fn read_receipt(file: &File, ordinal: usize) -> Result<HistoryEntry, LocalDbError> {
        let offset = (ordinal as u64)
            .checked_mul(SLOT_BYTES as u64)
            .ok_or_else(|| LocalDbError::new("retained receipt ordinal exhausted"))?;
        let mut slot = [0; SLOT_BYTES];
        file.read_exact_at(&mut slot, offset)
            .map_err(LocalDbError::component)?;
        let entry: HistoryEntry = decode_slot(&slot)?;
        if entry.ordinal != ordinal {
            return Err(LocalDbError::new("retained receipt ordinal differs"));
        }
        Ok(entry)
    }

    pub(super) fn receipt(
        &self,
        partition: u32,
        epoch: u64,
        ordinal: usize,
    ) -> Result<CheckpointReceipt, LocalDbError> {
        let file =
            File::open(self.stream_path(partition, epoch)).map_err(LocalDbError::component)?;
        let entry = Self::read_receipt(&file, ordinal)?;
        if self.counts.get(&(partition, epoch)).copied() == ordinal.checked_add(1)
            && self.chains.get(&(partition, epoch)).copied() != Some(entry.prefix_state)
        {
            return Err(LocalDbError::new("retained receipt prefix state differs"));
        }
        let receipt = entry.receipt;
        if receipt.id.database != self.database
            || receipt.address.partition != partition
            || receipt.version.epoch != epoch
        {
            return Err(LocalDbError::new("retained receipt stream domain differs"));
        }
        Ok(receipt)
    }
    /// Append one already ordered stream as contiguous receipt slots. The
    /// coordinator processes one stream at a time, so the fixed buffer is the
    /// complete per-writer staging bound.
    pub(super) fn append_stream(
        &mut self,
        receipts: &[CheckpointReceipt],
    ) -> Result<(), LocalDbError> {
        let Some(first) = receipts.first() else {
            return Ok(());
        };
        let stream = (first.address.partition, first.version.epoch);
        let mut ordinal = self.counts.get(&stream).copied().unwrap_or(0);
        let mut state = self.state(stream.0, stream.1)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.stream_path(stream.0, stream.1))
            .map_err(LocalDbError::component)?;
        self.io_stats.stream_opens += 1;
        if ordinal != 0 {
            self.io_stats.previous_slot_reads += 1;
            let previous = Self::read_receipt(&file, ordinal - 1)?;
            if previous.prefix_state != state {
                return Err(LocalDbError::new("retained receipt terminal state differs"));
            }
        }

        const SLOTS_PER_WRITE: usize = 16;
        let mut buffer = [0u8; SLOTS_PER_WRITE * SLOT_BYTES];
        let mut buffered = 0usize;
        let mut buffer_offset = None;
        for receipt in receipts {
            if (receipt.address.partition, receipt.version.epoch) != stream {
                return Err(LocalDbError::new("receipt batch spans multiple streams"));
            }
            let next = ordinal
                .checked_add(1)
                .ok_or_else(|| LocalDbError::new("retained receipt count exhausted"))?;
            let mut digest = PrefixDigest::from_state(ordinal, state);
            digest.push(*receipt);
            let entry = HistoryEntry {
                receipt: *receipt,
                ordinal,
                prefix_state: digest.state(),
            };
            let offset = (ordinal as u64)
                .checked_mul(SLOT_BYTES as u64)
                .ok_or_else(|| LocalDbError::new("retained receipt offset exhausted"))?;
            offset
                .checked_add(SLOT_BYTES as u64)
                .ok_or_else(|| LocalDbError::new("retained receipt end exhausted"))?;
            if buffer_offset.is_none() {
                buffer_offset = Some(offset);
            }
            let slot = encode_slot(&entry)?;
            buffer[buffered..buffered + SLOT_BYTES].copy_from_slice(&slot);
            buffered += SLOT_BYTES;
            self.insert(entry)?;
            ordinal = next;
            state = entry.prefix_state;
            self.counts.insert(stream, ordinal);
            self.chains.insert(stream, state);
            if buffered == buffer.len() {
                file.write_all_at(&buffer, buffer_offset.expect("nonempty write buffer"))
                    .map_err(LocalDbError::component)?;
                self.io_stats.receipt_write_ops += 1;
                self.io_stats.receipt_write_bytes += buffer.len() as u64;
                self.io_stats.buffer_flushes += 1;
                buffered = 0;
                buffer_offset = None;
            }
        }
        if buffered != 0 {
            file.write_all_at(
                &buffer[..buffered],
                buffer_offset.expect("nonempty write buffer"),
            )
            .map_err(LocalDbError::component)?;
            self.io_stats.receipt_write_ops += 1;
            self.io_stats.receipt_write_bytes += buffered as u64;
            self.io_stats.buffer_flushes += 1;
        }
        Ok(())
    }

    pub(super) fn reset_io_stats(&mut self) {
        self.io_stats = HistoryIoStats::default();
    }

    pub(super) fn sync(&mut self) -> Result<(), LocalDbError> {
        self.nodes.sync_all().map_err(LocalDbError::component)?;
        self.io_stats.sync_operations += 1;
        for (&(partition, epoch), &count) in &self.counts {
            if count != 0 {
                self.io_stats.stream_opens += 1;
                File::open(self.stream_path(partition, epoch))
                    .and_then(|file| file.sync_all())
                    .map_err(LocalDbError::component)?;
                self.io_stats.sync_operations += 1;
            }
        }
        File::open(&self.path)
            .and_then(|directory| directory.sync_all())
            .map_err(LocalDbError::component)?;
        self.io_stats.sync_operations += 1;
        Ok(())
    }

    pub(super) fn prefix(
        &self,
        partition: u32,
        epoch: u64,
        count: usize,
    ) -> Result<[u8; 32], LocalDbError> {
        if self.counts.get(&(partition, epoch)).copied().unwrap_or(0) != count {
            return Err(LocalDbError::new("retained receipt prefix count differs"));
        }
        let state = self.state(partition, epoch)?;
        Ok(PrefixDigest::from_state(count, state).finish())
    }

    pub(super) fn state(&self, partition: u32, epoch: u64) -> Result<[u8; 32], LocalDbError> {
        let count = self.counts.get(&(partition, epoch)).copied().unwrap_or(0);
        if count == 0 {
            let genesis = PrefixDigest::new(self.database, partition, epoch, 0).state();
            if self
                .chains
                .get(&(partition, epoch))
                .is_some_and(|state| *state != genesis)
            {
                return Err(LocalDbError::new("retained empty prefix genesis differs"));
            }
            Ok(genesis)
        } else {
            self.chains
                .get(&(partition, epoch))
                .copied()
                .ok_or_else(|| LocalDbError::new("retained receipt prefix state absent"))
        }
    }

    #[cfg(test)]
    pub(super) fn replace_single_receipt_for_test(
        &mut self,
        receipt: CheckpointReceipt,
    ) -> Result<(), LocalDbError> {
        let mut digest = PrefixDigest::new(
            self.database,
            receipt.address.partition,
            receipt.version.epoch,
            0,
        );
        digest.push(receipt);
        let entry = HistoryEntry {
            receipt,
            ordinal: 0,
            prefix_state: digest.state(),
        };
        let stream = (receipt.address.partition, receipt.version.epoch);
        let file = OpenOptions::new()
            .write(true)
            .open(self.stream_path(stream.0, stream.1))
            .map_err(LocalDbError::component)?;
        file.write_all_at(&encode_slot(&entry)?, 0)
            .map_err(LocalDbError::component)?;
        self.root = Some(self.write_node(&Node::Leaf(entry))?);
        self.counts.clear();
        self.counts.insert(stream, 1);
        self.chains.clear();
        self.chains.insert(stream, entry.prefix_state);
        self.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::types::{PayloadDigest, PhysicalValueAddress};

    fn receipt(sequence: u64) -> CheckpointReceipt {
        let mut operation = [0; 16];
        operation[..8].copy_from_slice(&sequence.to_be_bytes());
        CheckpointReceipt {
            id: BatchId {
                database: [9; 16],
                operation,
            },
            version: BatchVersion { epoch: 1, sequence },
            digest: PayloadDigest([sequence as u8; 32]),
            address: PhysicalValueAddress {
                partition: 2,
                segment_generation: 1,
                offset: sequence * 256,
                len: 128,
            },
        }
    }

    #[test]
    fn stream_batch_preserves_receipt_sequence_and_exact_retry_authority() {
        let temporary = tempdir::TempDir::new("batched-receipt-history").unwrap();
        let mut history = ReceiptHistory::open(temporary.path(), [9; 16]).unwrap();
        let receipts: Vec<_> = (0..35).map(receipt).collect();
        history.append_stream(&receipts).unwrap();
        history.sync().unwrap();

        assert_eq!(
            history.prefix(2, 1, receipts.len()).unwrap(),
            crate::distributed::tcp::checkpoint::prefix_digest(
                [9; 16],
                2,
                1,
                receipts.iter().copied(),
            ),
        );
        for expected in receipts {
            assert_eq!(
                history.lookup(expected.id).unwrap(),
                Some(expected.authenticated())
            );
        }
    }

    #[test]
    fn immutable_roots_page_exact_outcomes_without_retaining_completed_maps() {
        let temporary = tempdir::TempDir::new("pageable-retry-authority").unwrap();
        let mut history = ReceiptHistory::open(temporary.path(), [9; 16]).unwrap();
        history.append_stream(&[receipt(0)]).unwrap();
        history.sync().unwrap();
        let first_root = history.clone();
        for sequence in 1..257 {
            history.append_stream(&[receipt(sequence)]).unwrap();
        }
        history.sync().unwrap();
        for sequence in 0..257 {
            assert_eq!(
                history.lookup(receipt(sequence).id).unwrap(),
                Some(receipt(sequence).authenticated())
            );
        }
        assert_eq!(
            first_root.lookup(receipt(0).id).unwrap(),
            Some(receipt(0).authenticated())
        );
        assert_eq!(first_root.lookup(receipt(1).id).unwrap(), None);
        assert_eq!(history.lookup(receipt(999).id).unwrap(), None);
        assert_eq!(
            history.counts.len(),
            1,
            "only one scalar prefix per stream is resident"
        );
        assert!(history.cache.lock().len() <= NODE_CACHE_ENTRIES);
        assert_eq!(
            history.cached_metadata_bytes(),
            first_root.cached_metadata_bytes()
        );
        assert_eq!(
            history.prefix(2, 1, 257).unwrap(),
            crate::distributed::tcp::checkpoint::prefix_digest(
                [9; 16],
                2,
                1,
                (0..257usize).map(|sequence| receipt(sequence as u64)),
            ),
        );
    }

    #[test]
    fn retained_tree_pointer_never_turns_missing_or_corrupt_pages_into_absence() {
        let temporary = tempdir::TempDir::new("missing-retry-page").unwrap();
        let mut history = ReceiptHistory::open(temporary.path(), [9; 16]).unwrap();
        history.append_stream(&[receipt(0)]).unwrap();
        history.append_stream(&[receipt(1)]).unwrap();
        history.sync().unwrap();
        let pointer = history.root.unwrap();
        history
            .nodes
            .write_all_at(&[0; SLOT_BYTES], pointer.offset)
            .unwrap();
        // Inject a cold access: live immutable cached bytes remain independently
        // proven, whereas untrusted/reopened pages must be checked from storage.
        history.cache.lock().clear();
        assert!(history.lookup(receipt(0).id).is_err());
        assert!(history.lookup(receipt(999).id).is_err());
    }

    #[test]
    fn slot_decoder_rejects_corruption_and_noncanonical_padding() {
        let mut slot = encode_slot(&HistoryEntry {
            receipt: receipt(0),
            ordinal: 0,
            prefix_state: [0; 32],
        })
        .unwrap();
        let original: HistoryEntry = decode_slot(&slot).unwrap();
        assert_eq!(original.receipt.authenticated(), receipt(0).authenticated());
        slot[SLOT_BYTES - 1] = 1;
        assert!(decode_slot::<HistoryEntry>(&slot).is_err());
        slot[SLOT_BYTES - 1] = 0;
        slot[8] ^= 1;
        assert!(decode_slot::<HistoryEntry>(&slot).is_err());
    }

    #[test]
    fn immutable_node_hits_avoid_io_and_reopen_starts_untrusted() {
        let temporary = tempdir::TempDir::new("retry-node-cache").unwrap();
        let mut history = ReceiptHistory::open(temporary.path(), [9; 16]).unwrap();
        history.append_stream(&[receipt(0)]).unwrap();
        history.sync().unwrap();
        let root = history.root.unwrap();
        history.cache.lock().clear();
        history.read_node(root).unwrap(); // validate and warm from disk
        history.nodes.set_len(0).unwrap();
        assert!(
            history.read_node(root).is_ok(),
            "validated hit must not issue a pread"
        );
        let mut wrong_digest = root;
        wrong_digest.digest[0] ^= 1;
        assert!(
            history.read_node(wrong_digest).is_err(),
            "cache keys bind the entire pointer"
        );
        let mut reopened = ReceiptHistory::open(temporary.path(), [9; 16]).unwrap();
        assert_eq!(reopened.cache.lock().len(), 0);
        reopened.root = Some(root);
        assert!(reopened.lookup(receipt(0).id).is_err());
    }

    #[test]
    fn new_metadata_hash_is_blake2b_32_not_truncated_blake2b_512() {
        use blake2::{Blake2b, Digest};
        use typenum::{U32, U64};
        let slot = [7; SLOT_BYTES];
        let expected: [u8; 32] = Blake2b::<U32>::digest(slot).into();
        let wide = Blake2b::<U64>::digest(slot);
        assert_eq!(node_digest(&slot), expected);
        assert_ne!(node_digest(&slot).as_slice(), &wide[..32]);
    }
}
