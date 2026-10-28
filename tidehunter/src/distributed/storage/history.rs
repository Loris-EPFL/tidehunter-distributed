//! CRC-protected cold authority in owned append-only metadata files.
//!
//! Native snapshot freeze/persist/adopt (Tidehunter §§3.3–3.4) binds immutable
//! pages and a bounded WAL replay suffix. Like F2 §6, completed metadata is cold,
//! not lifetime-resident. Values are never copied into this metadata history.
use super::*;
use crate::distributed::reference_directory::RegisteredBatch;
use crate::distributed::tcp::checkpoint::{CheckpointReceipt, PrefixDigest};
use crate::distributed::types::RecordDescriptor;
use bincode::Options;
use std::os::unix::fs::FileExt;

mod index;
const ROOT: &str = "storage-history-root-v2";
const DIRECTORY: &str = "storage-history-v2";
const SLOT_BYTES: usize = 256;
const DESCRIPTOR_BYTES: usize = 80;
const NODE_BUFFER_BYTES: usize = 32 * SLOT_BYTES;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct NodePointer {
    offset: u64,
    digest: [u8; 32],
}
#[derive(Clone, Copy, Serialize, Deserialize)]
struct Pointer {
    ordinal: usize,
    digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct HistoryRoot {
    format: u32,
    database: [u8; 16],
    epoch: u64,
    partition: u32,
    generation: u64,
    pub(crate) count: usize,
    pub(crate) end: u64,
    pub(crate) file_count: u64,
    pub(crate) prefix_state: Option<[u8; 32]>,
    identities: Option<NodePointer>,
    versions: Option<NodePointer>,
    addresses: Option<NodePointer>,
    ordinals: Option<NodePointer>,
    pub(crate) retired_count: usize,
    retired_index: Option<NodePointer>,
    nodes_end: u64,
    descriptors_end: u64,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(crate) struct ReceiptPage {
    ordinal: usize,
    receipt: AppendedBatch,
    group_end: u64,
    prefix_state: [u8; 32],
    descriptor_offset: u64,
    descriptor_count: u32,
    descriptor_digest: [u8; 32],
}
impl ReceiptPage {
    pub(crate) fn descriptor_count(&self) -> usize {
        self.descriptor_count as usize
    }
}

#[derive(Clone, Copy)]
pub(crate) struct HistoryRecord {
    receipt: AppendedBatch,
    pub(crate) group_end: u64,
}
impl HistoryRecord {
    pub(crate) fn receipt(&self) -> AppendedBatch {
        self.receipt
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
struct Retirement {
    ordinal: usize,
    offset: u64,
    prefix: usize,
}

/// Frozen small root plus actual file owners. No directory guard spans disk I/O.
#[derive(Clone)]
pub(crate) struct HistoryReader {
    root: HistoryRoot,
    nodes: Arc<File>,
    receipts: Arc<File>,
    descriptors: Arc<File>,
    retired: Arc<File>,
    max_metadata_bytes: usize,
}

/// Sole append capability, serialized between the history worker and lifecycle owner.
pub(crate) struct History {
    path: PathBuf,
    reader: HistoryReader,
    node_append: u64,
    node_buffer: [u8; NODE_BUFFER_BYTES],
    node_buffer_len: usize,
    node_buffer_start: u64,
    descriptor_append: u64,
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    let hash = blake2b_simd::Params::new().hash_length(32).hash(bytes);
    let mut digest = [0; 32];
    digest.copy_from_slice(hash.as_bytes());
    digest
}

fn encode_into<const N: usize>(
    slot: &mut [u8; N],
    value: &impl Serialize,
) -> Result<(), StorageError> {
    slot.fill(0);
    let mut output = io::Cursor::new(&mut slot[8..]);
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize_into(&mut output, value)
        .map_err(|_| StorageError::Invalid("history slot encoding exceeds bound"))?;
    let len = output.position() as usize;
    slot[..4].copy_from_slice(&(len as u32).to_be_bytes());
    let crc = crc32fast::hash(&slot[8..8 + len]);
    slot[4..8].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}

fn encode_slot<const N: usize>(value: &impl Serialize) -> Result<[u8; N], StorageError> {
    let mut slot = [0; N];
    encode_into(&mut slot, value)?;
    Ok(slot)
}

fn decode_slot<T: serde::de::DeserializeOwned, const N: usize>(
    slot: &[u8; N],
) -> Result<T, StorageError> {
    let len = u32::from_be_bytes([slot[0], slot[1], slot[2], slot[3]]) as usize;
    let crc = u32::from_be_bytes([slot[4], slot[5], slot[6], slot[7]]);
    if len == 0
        || len > N - 8
        || slot[8 + len..].iter().any(|byte| *byte != 0)
        || crc32fast::hash(&slot[8..8 + len]) != crc
    {
        return Err(StorageError::Invalid("history slot CRC or bounds differ"));
    }
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(len as u64)
        .reject_trailing_bytes()
        .deserialize(&slot[8..8 + len])
        .map_err(|_| StorageError::Invalid("history slot format differs"))
}

fn slot_offset(ordinal: usize) -> Result<u64, StorageError> {
    (ordinal as u64)
        .checked_mul(SLOT_BYTES as u64)
        .ok_or(StorageError::Capacity)
}
fn append_offset(file: &File, alignment: usize) -> Result<u64, StorageError> {
    file.metadata()?
        .len()
        .div_ceil(alignment as u64)
        .checked_mul(alignment as u64)
        .ok_or(StorageError::Capacity)
}

impl History {
    pub(crate) fn create(path: &Path, config: &PartitionConfig) -> Result<Self, StorageError> {
        let directory = path.join(DIRECTORY);
        fs::create_dir(&directory)?;
        let open = |name: &str| -> Result<Arc<File>, StorageError> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(directory.join(name))?;
            file.sync_all()?;
            Ok(Arc::new(file))
        };
        let root = HistoryRoot {
            format: 2,
            database: config.inventory.database,
            epoch: config.inventory.epoch,
            partition: config.partition,
            generation: config.inventory.segment_generation,
            count: 0,
            end: 0,
            file_count: 1,
            prefix_state: None,
            identities: None,
            versions: None,
            addresses: None,
            ordinals: None,
            retired_count: 0,
            retired_index: None,
            nodes_end: 0,
            descriptors_end: 0,
        };
        let reader = HistoryReader {
            root,
            nodes: open("nodes.pages")?,
            receipts: open("receipts.pages")?,
            descriptors: open("descriptors.pages")?,
            retired: open("retired.pages")?,
            max_metadata_bytes: config.max_reference_metadata_bytes,
        };
        File::open(&directory)?.sync_all()?;
        install_metadata(path, ROOT, &encode_metadata(&root)?)?;
        Ok(Self {
            path: path.to_path_buf(),
            reader,
            node_append: 0,
            node_buffer: [0; NODE_BUFFER_BYTES],
            node_buffer_len: 0,
            node_buffer_start: 0,
            descriptor_append: 0,
        })
    }

    pub(crate) fn open(path: &Path, config: &PartitionConfig) -> Result<Self, StorageError> {
        let root: HistoryRoot = read_metadata(&path.join(ROOT))?;
        if root.format != 2
            || root.database != config.inventory.database
            || root.epoch != config.inventory.epoch
            || root.partition != config.partition
            || root.generation != config.inventory.segment_generation
        {
            return Err(StorageError::Invalid(
                "history root differs from registered stream",
            ));
        }
        let directory = path.join(DIRECTORY);
        let open = |name: &str| -> Result<Arc<File>, StorageError> {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(directory.join(name))?;
            if !file.metadata()?.is_file() {
                return Err(StorageError::Invalid("history backing is not a file"));
            }
            Ok(Arc::new(file))
        };
        let reader = HistoryReader {
            root,
            nodes: open("nodes.pages")?,
            receipts: open("receipts.pages")?,
            descriptors: open("descriptors.pages")?,
            retired: open("retired.pages")?,
            max_metadata_bytes: config.max_reference_metadata_bytes,
        };
        reader.validate_root()?;
        // Immutable node/descriptor positions never reuse even unadopted tails.
        let node_append = append_offset(&reader.nodes, SLOT_BYTES)?;
        let descriptor_append = append_offset(&reader.descriptors, DESCRIPTOR_BYTES)?;
        Ok(Self {
            path: path.to_path_buf(),
            reader,
            node_append,
            node_buffer: [0; NODE_BUFFER_BYTES],
            node_buffer_len: 0,
            node_buffer_start: node_append,
            descriptor_append,
        })
    }

    pub(crate) fn reader(&self) -> HistoryReader {
        self.reader.clone()
    }

    pub(crate) fn append(
        &mut self,
        batches: &[Arc<RegisteredBatch>],
        file_count: u64,
    ) -> Result<(), StorageError> {
        let mut root = self.reader.root;
        let mut prefix = root
            .prefix_state
            .map(|state| PrefixDigest::from_state(root.count, state));
        for batch in batches {
            let receipt = batch.receipt();
            let prefix = prefix.get_or_insert_with(|| {
                PrefixDigest::new(root.database, root.partition, root.epoch, 0)
            });
            prefix.push(CheckpointReceipt::from(receipt));
            let descriptor_offset = self.descriptor_append;
            let mut descriptor_hash = blake2b_simd::Params::new().hash_length(32).to_state();
            for chunk in batch.descriptors().chunks(32) {
                let mut buffer = [0; DESCRIPTOR_BYTES * 32];
                for (index, descriptor) in chunk.iter().enumerate() {
                    let slot: &mut [u8; DESCRIPTOR_BYTES] = (&mut buffer
                        [index * DESCRIPTOR_BYTES..(index + 1) * DESCRIPTOR_BYTES])
                        .try_into()
                        .map_err(|_| StorageError::Invalid("descriptor chunk bounds differ"))?;
                    encode_into(slot, descriptor)?;
                }
                let bytes = &buffer[..chunk.len() * DESCRIPTOR_BYTES];
                self.reader
                    .descriptors
                    .write_all_at(bytes, self.descriptor_append)?;
                self.descriptor_append = self
                    .descriptor_append
                    .checked_add(bytes.len() as u64)
                    .ok_or(StorageError::Capacity)?;
                descriptor_hash.update(bytes);
            }
            let mut descriptor_digest = [0; 32];
            descriptor_digest.copy_from_slice(descriptor_hash.finalize().as_bytes());
            let page = ReceiptPage {
                ordinal: root.count,
                receipt,
                group_end: batch.group_end(),
                prefix_state: prefix.state(),
                descriptor_offset,
                descriptor_count: u32::try_from(batch.descriptors().len())
                    .map_err(|_| StorageError::Capacity)?,
                descriptor_digest,
            };
            let slot = encode_slot::<SLOT_BYTES>(&page)?;
            // Only the unadopted next ordinal can be repaired after interruption.
            // A frozen old root never grants authority for this slot.
            self.reader
                .receipts
                .write_all_at(&slot, slot_offset(root.count)?)?;
            let pointer = Pointer {
                ordinal: root.count,
                digest: hash(&slot),
            };
            root.identities = self.index_insert(
                root.identities,
                u128::from_be_bytes(receipt.id().operation),
                pointer,
            )?;
            root.versions = self.index_insert(
                root.versions,
                u128::from(receipt.version().sequence),
                pointer,
            )?;
            root.addresses = self.index_insert(
                root.addresses,
                u128::from(receipt.address().offset),
                pointer,
            )?;
            root.ordinals = self.index_insert(root.ordinals, root.count as u128, pointer)?;
            root.count = root.count.checked_add(1).ok_or(StorageError::Capacity)?;
            root.end = batch.group_end();
            root.prefix_state = Some(page.prefix_state);
        }
        root.file_count = root.file_count.max(file_count);
        if !batches.is_empty() {
            root.nodes_end = self.node_append;
            root.descriptors_end = self.descriptor_append;
            self.reader.descriptors.sync_all()?;
            self.reader.receipts.sync_all()?;
            self.flush_nodes()?;
            self.reader.nodes.sync_all()?;
        }
        install_metadata(&self.path, ROOT, &encode_metadata(&root)?)?;
        self.reader.root = root;
        Ok(())
    }

    pub(crate) fn retire(&mut self, offsets: &[u64]) -> Result<(), StorageError> {
        let mut root = self.reader.root;
        let mut changed = false;
        for &offset in offsets {
            if self.reader.is_retired(offset)? {
                continue;
            }
            let retired = Retirement {
                ordinal: root.retired_count,
                offset,
                prefix: root.count,
            };
            let slot = encode_slot::<SLOT_BYTES>(&retired)?;
            self.reader
                .retired
                .write_all_at(&slot, slot_offset(root.retired_count)?)?;
            root.retired_index = self.index_insert(
                root.retired_index,
                u128::from(offset),
                Pointer {
                    ordinal: root.retired_count,
                    digest: hash(&slot),
                },
            )?;
            root.retired_count = root
                .retired_count
                .checked_add(1)
                .ok_or(StorageError::Capacity)?;
            changed = true;
        }
        if !changed {
            return Ok(());
        }
        root.nodes_end = self.node_append;
        self.reader.retired.sync_all()?;
        self.flush_nodes()?;
        self.reader.nodes.sync_all()?;
        install_metadata(&self.path, ROOT, &encode_metadata(&root)?)?;
        self.reader.root = root;
        Ok(())
    }
}

impl HistoryReader {
    pub(crate) fn root(&self) -> &HistoryRoot {
        &self.root
    }

    fn validate_root(&self) -> Result<(), StorageError> {
        if !self.root.nodes_end.is_multiple_of(SLOT_BYTES as u64)
            || !self
                .root
                .descriptors_end
                .is_multiple_of(DESCRIPTOR_BYTES as u64)
            || self.nodes.metadata()?.len() < self.root.nodes_end
            || self.descriptors.metadata()?.len() < self.root.descriptors_end
            || self.receipts.metadata()?.len() < slot_offset(self.root.count)?
            || self.retired.metadata()?.len() < slot_offset(self.root.retired_count)?
        {
            return Err(StorageError::Invalid("history root backing is truncated"));
        }
        if self.root.count == 0 {
            if self.root.end != 0
                || self.root.prefix_state.is_some()
                || self.root.ordinals.is_some()
                || self.root.identities.is_some()
                || self.root.versions.is_some()
                || self.root.addresses.is_some()
            {
                return Err(StorageError::Invalid(
                    "nonempty authority in empty history root",
                ));
            }
        } else {
            let page = self.page(self.root.count - 1)?;
            let receipt = page.receipt;
            if page.group_end != self.root.end
                || Some(page.prefix_state) != self.root.prefix_state
                || self
                    .lookup(receipt.id())?
                    .is_none_or(|record| record.receipt != receipt)
                || self
                    .lookup_version(receipt.version())?
                    .is_none_or(|record| record.receipt != receipt)
                || self
                    .lookup_address_page(receipt.address())?
                    .is_none_or(|other| other.receipt != receipt)
            {
                return Err(StorageError::Invalid(
                    "terminal receipt differs from history root",
                ));
            }
        }
        if self.root.retired_count == 0 {
            if self.root.retired_index.is_some() {
                return Err(StorageError::Invalid("unexpected retirement root"));
            }
        } else {
            let ordinal = self.root.retired_count - 1;
            let mut slot = [0; SLOT_BYTES];
            self.retired
                .read_exact_at(&mut slot, slot_offset(ordinal)?)?;
            let retired: Retirement = decode_slot(&slot)?;
            if retired.ordinal != ordinal
                || retired.prefix > self.root.count
                || !self.is_retired(retired.offset)?
            {
                return Err(StorageError::Invalid(
                    "terminal retirement differs from root",
                ));
            }
        }
        Ok(())
    }

    fn read_receipt(&self, pointer: Pointer) -> Result<ReceiptPage, StorageError> {
        if pointer.ordinal >= self.root.count {
            return Err(StorageError::Invalid("receipt ordinal exceeds root"));
        }
        let mut slot = [0; SLOT_BYTES];
        self.receipts
            .read_exact_at(&mut slot, slot_offset(pointer.ordinal)?)?;
        if hash(&slot) != pointer.digest {
            return Err(StorageError::Invalid(
                "receipt digest differs from authority",
            ));
        }
        let page: ReceiptPage = decode_slot(&slot)?;
        if page.ordinal != pointer.ordinal
            || page.receipt.id().database != self.root.database
            || page.receipt.version().epoch != self.root.epoch
            || page.receipt.address().partition != self.root.partition
            || page.receipt.address().segment_generation != self.root.generation
        {
            return Err(StorageError::Invalid(
                "receipt domain differs from rooted stream",
            ));
        }
        Ok(page)
    }

    fn page(&self, ordinal: usize) -> Result<ReceiptPage, StorageError> {
        let pointer = self
            .index_lookup(self.root.ordinals, ordinal as u128)?
            .ok_or(StorageError::Invalid("missing receipt ordinal authority"))?;
        if pointer.ordinal != ordinal {
            return Err(StorageError::Invalid("receipt ordinal index differs"));
        }
        self.read_receipt(pointer)
    }
    pub(crate) fn at(&self, ordinal: usize) -> Result<HistoryRecord, StorageError> {
        let page = self.page(ordinal)?;
        Ok(HistoryRecord {
            receipt: page.receipt,
            group_end: page.group_end,
        })
    }
    pub(crate) fn prefix_state(&self, ordinal: usize) -> Result<[u8; 32], StorageError> {
        Ok(self.page(ordinal)?.prefix_state)
    }

    fn find(
        &self,
        root: Option<NodePointer>,
        key: u128,
        matches: impl Fn(AppendedBatch) -> bool,
    ) -> Result<Option<ReceiptPage>, StorageError> {
        let Some(pointer) = self.index_lookup(root, key)? else {
            return Ok(None);
        };
        let page = self.read_receipt(pointer)?;
        if !matches(page.receipt) {
            return Err(StorageError::Invalid(
                "receipt lookup differs from rooted key",
            ));
        }
        Ok(Some(page))
    }
    pub(crate) fn lookup(&self, id: BatchId) -> Result<Option<HistoryRecord>, StorageError> {
        Ok(self
            .find(
                self.root.identities,
                u128::from_be_bytes(id.operation),
                |receipt| receipt.id() == id,
            )?
            .map(|page| HistoryRecord {
                receipt: page.receipt,
                group_end: page.group_end,
            }))
    }
    pub(crate) fn lookup_version(
        &self,
        version: BatchVersion,
    ) -> Result<Option<HistoryRecord>, StorageError> {
        Ok(self
            .find(
                self.root.versions,
                u128::from(version.sequence),
                |receipt| receipt.version() == version,
            )?
            .map(|page| HistoryRecord {
                receipt: page.receipt,
                group_end: page.group_end,
            }))
    }
    pub(crate) fn lookup_address_page(
        &self,
        address: PhysicalValueAddress,
    ) -> Result<Option<ReceiptPage>, StorageError> {
        self.find(self.root.addresses, u128::from(address.offset), |receipt| {
            receipt.address() == address
        })
    }

    /// The caller reserves the complete descriptor heap credit before entering.
    /// Fixed-size streaming slots avoid an additional encoded-page allocation.
    pub(crate) fn read_registered(
        &self,
        page: &ReceiptPage,
    ) -> Result<RegisteredBatch, StorageError> {
        let count = page.descriptor_count();
        if count > self.max_metadata_bytes / std::mem::size_of::<RecordDescriptor>() {
            return Err(StorageError::Invalid(
                "descriptor page exceeds configured metadata bound",
            ));
        }
        let bytes = (count as u64)
            .checked_mul(DESCRIPTOR_BYTES as u64)
            .ok_or(StorageError::Capacity)?;
        if !page
            .descriptor_offset
            .is_multiple_of(DESCRIPTOR_BYTES as u64)
            || page
                .descriptor_offset
                .checked_add(bytes)
                .is_none_or(|end| end > self.root.descriptors_end)
        {
            return Err(StorageError::Invalid(
                "descriptor page exceeds rooted backing",
            ));
        }
        let mut descriptors = Vec::with_capacity(count);
        let mut digest = blake2b_simd::Params::new().hash_length(32).to_state();
        for start in (0..count).step_by(32) {
            let mut buffer = [0; DESCRIPTOR_BYTES * 32];
            let chunk_count = (count - start).min(32);
            let bytes = &mut buffer[..chunk_count * DESCRIPTOR_BYTES];
            self.descriptors.read_exact_at(
                bytes,
                page.descriptor_offset + start as u64 * DESCRIPTOR_BYTES as u64,
            )?;
            digest.update(bytes);
            for index in 0..chunk_count {
                let slot: &[u8; DESCRIPTOR_BYTES] = (&bytes
                    [index * DESCRIPTOR_BYTES..(index + 1) * DESCRIPTOR_BYTES])
                    .try_into()
                    .map_err(|_| StorageError::Invalid("descriptor chunk bounds differ"))?;
                let descriptor: RecordDescriptor = decode_slot(slot)?;
                if descriptor.operation_index as usize != start + index {
                    return Err(StorageError::Invalid("descriptor operation order differs"));
                }
                descriptors.push(descriptor);
            }
        }
        if digest.finalize().as_bytes() != page.descriptor_digest.as_slice() {
            return Err(StorageError::Invalid(
                "descriptor page digest differs from receipt authority",
            ));
        }
        Ok(RegisteredBatch::new(
            page.receipt.id(),
            page.receipt.version(),
            page.receipt.digest(),
            page.receipt.address(),
            descriptors.into_boxed_slice(),
            page.group_end,
        ))
    }

    pub(crate) fn is_retired(&self, offset: u64) -> Result<bool, StorageError> {
        let Some(pointer) = self.index_lookup(self.root.retired_index, u128::from(offset))? else {
            return Ok(false);
        };
        if pointer.ordinal >= self.root.retired_count {
            return Err(StorageError::Invalid("retirement ordinal exceeds root"));
        }
        let mut slot = [0; SLOT_BYTES];
        self.retired
            .read_exact_at(&mut slot, slot_offset(pointer.ordinal)?)?;
        if hash(&slot) != pointer.digest {
            return Err(StorageError::Invalid("retirement digest differs from root"));
        }
        let retired: Retirement = decode_slot(&slot)?;
        if retired.ordinal != pointer.ordinal
            || retired.offset != offset
            || retired.prefix > self.root.count
        {
            return Err(StorageError::Invalid(
                "retirement authority differs from requested file",
            ));
        }
        Ok(true)
    }
}
