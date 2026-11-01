//! Durable owner-side registration of file-backed Linux nvmet exports.
//!
//! Serialize registration with all authority transitions, including poisoning.
//! Persisted admissions survive a crash and remain quarantined until explicitly
//! released after the initiator has drained and closed its namespace FD.

use super::{ExportManifest, MAX_EXPORT_MANIFEST_BYTES};
use crate::distributed::storage::{StorageError, encode_metadata, read_metadata};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

const REGISTRY: &str = "nvmeof-export-leases-v1";
const CONFIGFS: &str = "/sys/kernel/config/nvmet";

/// Bounded, catalog-free identity passed through the authenticated control path.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportBinding {
    pub database_uuid: [u8; 16],
    pub epoch: u64,
    pub partition: u32,
    pub generation: u64,
    pub export_uuid: [u8; 16],
    pub nqn: String,
    pub nsid: u32,
    pub wal_file_bytes: u64,
    pub source_file_index: u64,
    pub image_len: u64,
    pub block_size: u32,
    pub source_device: u64,
    pub source_inode: u64,
    pub durable_frontier: u64,
}
impl From<&ExportManifest> for ExportBinding {
    fn from(m: &ExportManifest) -> Self {
        Self {
            database_uuid: m.database_uuid,
            epoch: m.epoch,
            partition: m.partition,
            generation: m.generation,
            export_uuid: m.export_uuid,
            nqn: m.nqn.clone(),
            nsid: m.nsid,
            wal_file_bytes: m.wal_file_bytes,
            source_file_index: m.source_file_index,
            image_len: m.image_len,
            block_size: m.block_size,
            source_device: m.source_device,
            source_inode: m.source_inode,
            durable_frontier: m.durable_frontier,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Entry {
    owner_epoch: u64,
    token: [u8; 16],
    binding: ExportBinding,
    namespace_device: u64,
    namespace_inode: u64,
    image_path: PathBuf,
    image_device: u64,
    image_inode: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct EpochPin {
    owner_epoch: u64,
    token: [u8; 16],
}
#[derive(Serialize, Deserialize)]
struct State {
    version: u32,
    entries: Vec<Entry>,
    #[serde(default)]
    epoch_pins: Vec<EpochPin>,
}

/// Caller owns the node-root lock; this registry must share the authority gate.
pub struct ExportLeaseRegistry {
    root: PathBuf,
    configfs: PathBuf,
    max_entries: usize,
    max_manifest_bytes: usize,
    entries: Vec<Entry>,
    epoch_pins: Vec<EpochPin>,
    pins: Vec<([u8; 16], File)>,
    fixture: bool,
    #[cfg(test)]
    fail_next_dir_sync: std::sync::atomic::AtomicBool,
}
fn invalid(msg: &'static str) -> StorageError {
    StorageError::Invalid(msg)
}
fn attr(path: &Path) -> Result<String, StorageError> {
    Ok(fs::read_to_string(path)?.trim().to_owned())
}
fn uuid_string(b: [u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0],
        b[1],
        b[2],
        b[3],
        b[4],
        b[5],
        b[6],
        b[7],
        b[8],
        b[9],
        b[10],
        b[11],
        b[12],
        b[13],
        b[14],
        b[15]
    )
}
fn valid_nqn(nqn: &str) -> bool {
    nqn.starts_with("nqn.")
        && nqn.len() <= 223
        && nqn.len() > 4
        && nqn
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b':' | b'-'))
}
fn verify_root_protected(path: &Path) -> Result<(), StorageError> {
    if !path.is_absolute()
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(invalid("protected path is not canonical"));
    }
    for part in path.ancestors() {
        let meta = fs::symlink_metadata(part)?;
        if meta.file_type().is_symlink() || meta.uid() != 0 || meta.mode() & 0o022 != 0 {
            return Err(invalid("path is not root protected"));
        }
    }
    Ok(())
}
impl ExportLeaseRegistry {
    /// Load registrations under the node root lock. Surviving admissions remain
    /// quarantined; target disable alone cannot prove initiator CQ retirement.
    pub fn open(
        root: &Path,
        max_entries: usize,
        max_manifest_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::open_inner(
            root,
            Path::new(CONFIGFS),
            max_entries,
            max_manifest_bytes,
            false,
        )
    }
    /// Explicit test fixture: ordinary temporary files emulate configfs control.
    #[cfg(test)]
    pub(crate) fn open_fixture(
        root: &Path,
        configfs: &Path,
        max_entries: usize,
        max_manifest_bytes: usize,
    ) -> Result<Self, StorageError> {
        Self::open_inner(root, configfs, max_entries, max_manifest_bytes, true)
    }
    fn open_inner(
        root: &Path,
        configfs: &Path,
        max_entries: usize,
        max_manifest_bytes: usize,
        fixture: bool,
    ) -> Result<Self, StorageError> {
        if max_entries == 0
            || max_entries > 4096
            || max_manifest_bytes == 0
            || max_manifest_bytes > MAX_EXPORT_MANIFEST_BYTES
        {
            return Err(invalid("invalid export registry limits"));
        }
        let path = root.join(REGISTRY);
        let (entries, epoch_pins) = match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_file() => {
                if !fixture {
                    verify_root_protected(root)?;
                    if meta.uid() != 0 || meta.mode() & 0o077 != 0 {
                        return Err(invalid("export registry must be private and root owned"));
                    }
                }
                let state: State = read_metadata(&path)?;
                if state.version != 1
                    || state
                        .entries
                        .len()
                        .checked_add(state.epoch_pins.len())
                        .is_none_or(|count| count > max_entries)
                {
                    return Err(invalid("invalid export registry"));
                }
                (state.entries, state.epoch_pins)
            }
            Ok(_) => return Err(invalid("invalid export registry file")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (Vec::new(), Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut tokens = std::collections::HashSet::with_capacity(entries.len() + epoch_pins.len());
        if entries.iter().any(|entry| {
            entry.owner_epoch == 0
                || entry.token == [0; 16]
                || !tokens.insert(entry.token)
                || !valid_nqn(&entry.binding.nqn)
        }) || epoch_pins
            .iter()
            .any(|pin| pin.owner_epoch == 0 || pin.token == [0; 16] || !tokens.insert(pin.token))
        {
            return Err(invalid("invalid persisted export admission"));
        }
        Ok(Self {
            root: root.to_path_buf(),
            configfs: configfs.to_path_buf(),
            max_entries,
            max_manifest_bytes,
            entries,
            epoch_pins,
            pins: Vec::new(),
            fixture,
            #[cfg(test)]
            fail_next_dir_sync: std::sync::atomic::AtomicBool::new(false),
        })
    }
    fn check_configfs(&self) -> Result<(), StorageError> {
        if self.fixture {
            return Ok(());
        }
        // SAFETY: geteuid has no arguments and cannot violate memory safety.
        if unsafe { libc::geteuid() } != 0 {
            return Err(invalid("export management requires root"));
        }
        use std::os::unix::ffi::OsStrExt;
        let path = std::ffi::CString::new(self.configfs.as_os_str().as_bytes())
            .map_err(|_| invalid("invalid configfs path"))?;
        let mut fs_info = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: path is NUL terminated and fs_info points to a valid output buffer.
        if unsafe { libc::statfs(path.as_ptr(), fs_info.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
        // SAFETY: a successful statfs initialized the whole result.
        let fs_info = unsafe { fs_info.assume_init() };
        if fs_info.f_type as u64 != 0x62656570 {
            return Err(invalid("nvmet is not on configfs"));
        }
        Ok(())
    }
    fn namespace(&self, binding: &ExportBinding) -> Result<PathBuf, StorageError> {
        if !valid_nqn(&binding.nqn)
            || binding.nsid == 0
            || binding.export_uuid == [0; 16]
            || binding.wal_file_bytes == 0
            || binding.image_len == 0
            || !binding.block_size.is_power_of_two()
            || binding.block_size < 512
            || !binding
                .image_len
                .is_multiple_of(u64::from(binding.block_size))
        {
            return Err(invalid("invalid namespace binding"));
        }
        Ok(self
            .configfs
            .join("subsystems")
            .join(&binding.nqn)
            .join("namespaces")
            .join(binding.nsid.to_string()))
    }
    fn trusted_image(&self, image: &Path) -> Result<File, StorageError> {
        if !image.is_absolute()
            || image.file_name().is_none_or(|name| name != "image.wal")
            || image.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            return Err(invalid("export image path is not canonical"));
        }
        if !self.fixture {
            verify_root_protected(image)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(image)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.nlink() != 1 {
            return Err(invalid("export image is not exclusive regular file"));
        }
        Ok(file)
    }
    fn inspect(&self, binding: &ExportBinding) -> Result<(Entry, File), StorageError> {
        self.check_configfs()?;
        let ns = self.namespace(binding)?;
        let meta = fs::symlink_metadata(&ns)?;
        if !meta.file_type().is_dir()
            || attr(&ns.join("device_uuid"))? != uuid_string(binding.export_uuid)
            || attr(&ns.join("enable"))? != "1"
        {
            return Err(invalid("unmatched or disabled target namespace"));
        }
        if attr(&ns.join("buffered_io"))? != "1" {
            return Err(invalid("target namespace must use buffered file I/O"));
        }
        let image = PathBuf::from(attr(&ns.join("device_path"))?);
        let file = self.trusted_image(&image)?;
        let image_meta = file.metadata()?;
        if image_meta.len() != binding.image_len {
            return Err(invalid("export image size mismatch"));
        }
        let manifest_path = image
            .parent()
            .ok_or(invalid("export image has no parent"))?
            .join("manifest.json");
        let manifest_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(manifest_path)?;
        let manifest_meta = manifest_file.metadata()?;
        if !manifest_meta.is_file()
            || manifest_meta.len() > self.max_manifest_bytes as u64
            || (!self.fixture && (manifest_meta.uid() != 0 || manifest_meta.mode() & 0o022 != 0))
        {
            return Err(invalid("invalid export manifest file"));
        }
        let mut bytes = Vec::new();
        manifest_file
            .take(self.max_manifest_bytes as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > self.max_manifest_bytes {
            return Err(invalid("export manifest too large"));
        }
        let manifest: ExportManifest =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid export manifest"))?;
        if manifest.version != 1 || ExportBinding::from(&manifest) != *binding {
            return Err(invalid("target manifest provenance mismatch"));
        }
        Ok((
            Entry {
                owner_epoch: 0,
                token: [0; 16],
                binding: binding.clone(),
                namespace_device: meta.dev(),
                namespace_inode: meta.ino(),
                image_path: image,
                image_device: image_meta.dev(),
                image_inode: image_meta.ino(),
            },
            file,
        ))
    }
    fn sync_root(&self) -> Result<(), StorageError> {
        #[cfg(test)]
        if self
            .fail_next_dir_sync
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(io::Error::other("injected directory sync failure").into());
        }
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    fn persist(&self, data: Option<&[u8]>) -> Result<(), StorageError> {
        if !self.fixture {
            verify_root_protected(&self.root)?;
        }
        if let Some(data) = data {
            let temporary = self.root.join(format!(".{REGISTRY}.pending"));
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(&temporary)?;
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
            file.write_all(data)?;
            file.sync_all()?;
            fs::rename(&temporary, self.root.join(REGISTRY))?;
            self.sync_root()?;
        } else {
            match fs::remove_file(self.root.join(REGISTRY)) {
                Ok(()) => (),
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
            // A previous unlink may have succeeded before its directory fsync
            // failed. Retry must sync even if the name is already absent.
            self.sync_root()?;
        }
        Ok(())
    }
    fn encoded_state(
        entries: &[Entry],
        epoch_pins: &[EpochPin],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        if entries.is_empty() && epoch_pins.is_empty() {
            return Ok(None);
        }
        Ok(Some(encode_metadata(&State {
            version: 1,
            entries: entries.to_vec(),
            epoch_pins: epoch_pins.to_vec(),
        })?))
    }
    fn save(&self, entries: &[Entry], epoch_pins: &[EpochPin]) -> Result<(), StorageError> {
        let data = Self::encoded_state(entries, epoch_pins)?;
        self.persist(data.as_deref())
    }
    fn check_admission(&self, owner_epoch: u64) -> Result<(), StorageError> {
        if owner_epoch == 0
            || self
                .entries
                .len()
                .checked_add(self.epoch_pins.len())
                .is_none_or(|count| count >= self.max_entries)
        {
            return Err(StorageError::Capacity);
        }
        if !self.fixture {
            // SAFETY: geteuid takes no arguments and has no memory effects.
            if unsafe { libc::geteuid() } != 0 {
                return Err(invalid("export management requires root"));
            }
            verify_root_protected(&self.root)?;
        }
        Ok(())
    }
    fn allocate_token(&self) -> Result<[u8; 16], StorageError> {
        use rand::RngCore;
        let mut entropy = rand::rngs::OsRng;
        for _ in 0..32 {
            let mut candidate = [0; 16];
            entropy.try_fill_bytes(&mut candidate).map_err(|error| {
                io::Error::other(format!("export token entropy failed: {error}"))
            })?;
            if candidate != [0; 16]
                && self.entries.iter().all(|entry| entry.token != candidate)
                && self.epoch_pins.iter().all(|pin| pin.token != candidate)
            {
                return Ok(candidate);
            }
        }
        Err(invalid("cannot allocate export token"))
    }
    /// Gate fences, seals, retirements, terminal publication and recovery.
    pub fn has_live_or_quarantined(&self) -> bool {
        !self.entries.is_empty() || !self.epoch_pins.is_empty()
    }
    /// Validates target and persists a separate token before returning it.
    pub fn register(
        &mut self,
        owner_epoch: u64,
        binding: &ExportBinding,
    ) -> Result<[u8; 16], StorageError> {
        self.check_admission(owner_epoch)?;
        let (mut entry, file) = self.inspect(binding)?;
        if self.entries.iter().any(|old| {
            old.binding.nqn == binding.nqn
                && old.binding.nsid == binding.nsid
                && (old.binding != *binding
                    || old.namespace_device != entry.namespace_device
                    || old.namespace_inode != entry.namespace_inode
                    || old.image_device != entry.image_device
                    || old.image_inode != entry.image_inode)
        }) {
            return Err(invalid("conflicting namespace registration"));
        }
        let token = self.allocate_token()?;
        entry.owner_epoch = owner_epoch;
        entry.token = token;
        // Reject deterministic limit and root failures before making an
        // unacknowledged in-memory admission or touching durable metadata.
        let mut candidate = self.entries.clone();
        candidate.push(entry.clone());
        let data = Self::encoded_state(&candidate, &self.epoch_pins)?
            .ok_or(invalid("empty export registration"))?;
        // An uncertain persist leaves the owner blocked by this in-memory pin;
        // it must not advance authority until explicitly resolved.
        self.entries.push(entry);
        self.pins.push((token, file));
        self.persist(Some(&data))?;
        Ok(token)
    }
    /// Pin current owner authority even when this owner has no exported image.
    pub fn register_epoch_pin(&mut self, owner_epoch: u64) -> Result<[u8; 16], StorageError> {
        self.check_admission(owner_epoch)?;
        let token = self.allocate_token()?;
        let pin = EpochPin { owner_epoch, token };
        let mut candidate = self.epoch_pins.clone();
        candidate.push(pin.clone());
        let data = Self::encoded_state(&self.entries, &candidate)?
            .ok_or(invalid("empty epoch registration"))?;
        // A failed persist may already have renamed the state; retain the pin.
        self.epoch_pins.push(pin);
        self.persist(Some(&data))?;
        Ok(token)
    }
    /// Caller releases only after all exported namespace FDs have been closed.
    pub fn release_epoch_pin(
        &mut self,
        owner_epoch: u64,
        token: [u8; 16],
    ) -> Result<(), StorageError> {
        let Some(index) = self.epoch_pins.iter().position(|pin| pin.token == token) else {
            if self.entries.iter().any(|entry| entry.token == token) {
                return Err(invalid("token belongs to export admission"));
            }
            return Ok(());
        };
        if self.epoch_pins[index].owner_epoch != owner_epoch {
            return Err(StorageError::Fenced);
        }
        let mut remaining = self.epoch_pins.clone();
        remaining.remove(index);
        self.save(&self.entries, &remaining)?;
        self.epoch_pins = remaining;
        Ok(())
    }
    fn disable(&self, entry: &Entry) -> Result<(), StorageError> {
        self.check_configfs()?;
        let ns = self.namespace(&entry.binding)?;
        let meta = match fs::symlink_metadata(&ns) {
            Ok(meta) => meta,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        if !meta.file_type().is_dir()
            || meta.dev() != entry.namespace_device
            || meta.ino() != entry.namespace_inode
            || attr(&ns.join("device_uuid"))? != uuid_string(entry.binding.export_uuid)
            || PathBuf::from(attr(&ns.join("device_path"))?) != entry.image_path
        {
            return Err(invalid("registered namespace identity changed"));
        }
        let file = self.trusted_image(&entry.image_path)?;
        let image = file.metadata()?;
        if image.dev() != entry.image_device
            || image.ino() != entry.image_inode
            || image.len() != entry.binding.image_len
        {
            return Err(invalid("registered export image identity changed"));
        }
        let enabled = attr(&ns.join("enable"))?;
        if enabled == "1" {
            // Linux nvmet_ns_disable removes namespace lookup, kills its
            // percpu_ref, synchronizes RCU and waits for disable_done before
            // closing the backing file. This write is intentionally blocking
            // on the owner control worker, never the read reactor.
            let mut enable = OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(ns.join("enable"))?;
            enable.write_all(b"0\n")?;
        } else if enabled != "0" {
            return Err(invalid("invalid namespace enable state"));
        }
        if attr(&ns.join("enable"))? != "0" {
            return Err(invalid("target namespace failed to disable"));
        }
        Ok(())
    }
    /// Caller must have closed reader admission, drained terminal CQEs and closed
    /// its namespace FD before acknowledging release over the authenticated path.
    pub fn release(&mut self, owner_epoch: u64, token: [u8; 16]) -> Result<(), StorageError> {
        let Some(index) = self.entries.iter().position(|entry| entry.token == token) else {
            if self.epoch_pins.iter().any(|pin| pin.token == token) {
                return Err(invalid("token belongs to epoch admission"));
            }
            return Ok(());
        };
        let entry = &self.entries[index];
        if entry.owner_epoch != owner_epoch {
            return Err(StorageError::Fenced);
        }
        let last = !self.entries.iter().enumerate().any(|(n, other)| {
            n != index
                && other.binding.nqn == entry.binding.nqn
                && other.binding.nsid == entry.binding.nsid
        });
        if last {
            self.disable(entry)?;
        }
        let mut remaining = self.entries.clone();
        remaining.remove(index);
        self.save(&remaining, &self.epoch_pins)?;
        self.entries = remaining;
        self.pins.retain(|(id, _)| *id != token);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_manifest_limit_admits_large_catalog_but_keeps_provenance_and_nofollow() {
        use super::super::CatalogEntry;
        use crate::distributed::types::{
            BatchId, BatchVersion, MemberChecksum, PayloadDigest, PhysicalValueAddress,
            RecordDescriptor, RecordKind, ValueRecordReference,
        };
        use std::os::unix::fs::symlink;
        let temp = tempdir::TempDir::new("nvmeof-large-manifest").unwrap();
        let root = temp.path().join("owner");
        let configfs = temp.path().join("configfs");
        let export = temp.path().join("export");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&export).unwrap();
        fs::write(export.join("image.wal"), vec![0; 4096]).unwrap();
        let reference = ValueRecordReference {
            batch_id: BatchId {
                database: [1; 16],
                operation: [2; 16],
            },
            batch_version: BatchVersion {
                epoch: 1,
                sequence: 0,
            },
            batch_digest: PayloadDigest([3; 32]),
            batch_address: PhysicalValueAddress {
                partition: 3,
                segment_generation: 1,
                offset: 0,
                len: 64,
            },
            descriptor: RecordDescriptor {
                operation_index: 0,
                offset: 8,
                len: 32,
                keyspace: 0,
                kind: RecordKind::Put,
                key_len: 4,
                value_offset: 16,
                value_len: 8,
                checksum: MemberChecksum(9),
            },
        };
        let manifest = ExportManifest {
            version: 1,
            database_uuid: [1; 16],
            epoch: 1,
            partition: 3,
            generation: 1,
            export_uuid: [7; 16],
            nqn: "nqn.2026-01.test:large".into(),
            nsid: 1,
            wal_file_bytes: 4096,
            source_file_index: 0,
            image_len: 4096,
            block_size: 512,
            source_device: 10,
            source_inode: 11,
            durable_frontier: 4096,
            catalog: vec![
                CatalogEntry {
                    reference,
                    offset: 16,
                    len: 32
                };
                5000
            ],
        };
        let bytes = serde_json::to_vec(&manifest).unwrap();
        assert!(bytes.len() > 1024 * 1024 && bytes.len() < MAX_EXPORT_MANIFEST_BYTES);
        let manifest_path = export.join("manifest.json");
        fs::write(&manifest_path, &bytes).unwrap();
        let binding = ExportBinding::from(&manifest);
        let ns = configfs
            .join("subsystems")
            .join(&binding.nqn)
            .join("namespaces/1");
        fs::create_dir_all(&ns).unwrap();
        fs::write(ns.join("device_uuid"), uuid_string(binding.export_uuid)).unwrap();
        fs::write(
            ns.join("device_path"),
            export.join("image.wal").to_str().unwrap(),
        )
        .unwrap();
        fs::write(ns.join("enable"), "1\n").unwrap();
        fs::write(ns.join("buffered_io"), "1\n").unwrap();
        let small = ExportLeaseRegistry::open_fixture(&root, &configfs, 8, 1024 * 1024).unwrap();
        assert!(small.inspect(&binding).is_err());
        let registry =
            ExportLeaseRegistry::open_fixture(&root, &configfs, 8, MAX_EXPORT_MANIFEST_BYTES)
                .unwrap();
        assert!(registry.inspect(&binding).is_ok());
        fs::write(ns.join("buffered_io"), "0\n").unwrap();
        assert!(registry.inspect(&binding).is_err());
        fs::write(ns.join("buffered_io"), "1\n").unwrap();
        let mut forged = binding.clone();
        forged.source_inode += 1;
        assert!(registry.inspect(&forged).is_err());
        fs::rename(&manifest_path, export.join("real.json")).unwrap();
        symlink("real.json", &manifest_path).unwrap();
        assert!(registry.inspect(&binding).is_err());
        fs::remove_file(&manifest_path).unwrap();
        let oversized = File::create(&manifest_path).unwrap();
        oversized
            .set_len(MAX_EXPORT_MANIFEST_BYTES as u64 + 1)
            .unwrap();
        assert!(registry.inspect(&binding).is_err());
        assert!(
            ExportLeaseRegistry::open_fixture(&root, &configfs, 8, MAX_EXPORT_MANIFEST_BYTES + 1)
                .is_err()
        );
    }

    #[test]
    fn admissions_survive_restart_and_only_last_drained_release_disables() {
        let temp = tempdir::TempDir::new("nvmeof-lease").unwrap();
        let root = temp.path().join("owner");
        let configfs = temp.path().join("configfs");
        let export = temp.path().join("export");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&export).unwrap();
        fs::write(export.join("image.wal"), vec![0; 4096]).unwrap();
        let manifest = ExportManifest {
            version: 1,
            database_uuid: [1; 16],
            epoch: 1,
            partition: 3,
            generation: 1,
            export_uuid: [7; 16],
            nqn: "nqn.2026-01.test:wal".into(),
            nsid: 1,
            wal_file_bytes: 4096,
            source_file_index: 0,
            image_len: 4096,
            block_size: 512,
            source_device: 10,
            source_inode: 11,
            durable_frontier: 4096,
            catalog: vec![],
        };
        fs::write(
            export.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let binding = ExportBinding::from(&manifest);
        let ns = configfs
            .join("subsystems")
            .join(&binding.nqn)
            .join("namespaces/1");
        fs::create_dir_all(&ns).unwrap();
        fs::write(ns.join("device_uuid"), uuid_string(binding.export_uuid)).unwrap();
        fs::write(
            ns.join("device_path"),
            export.join("image.wal").to_str().unwrap(),
        )
        .unwrap();
        fs::write(ns.join("enable"), "1\n").unwrap();
        fs::write(ns.join("buffered_io"), "1\n").unwrap();
        let mut registry = ExportLeaseRegistry::open_fixture(&root, &configfs, 8, 65536).unwrap();
        let mut forged = binding.clone();
        forged.source_inode += 1;
        assert!(registry.register(5, &forged).is_err());
        assert!(!registry.has_live_or_quarantined());
        assert!(!root.join(REGISTRY).exists());
        let first = registry.register(5, &binding).unwrap();
        // An oversized candidate must fail before inserting an unreachable
        // admission or changing the pin set.
        let mut bulky = registry.entries[0].clone();
        bulky.image_path = PathBuf::from("x".repeat(2048));
        let saved = std::mem::replace(&mut registry.entries, vec![bulky; 700]);
        registry.max_entries = 1024;
        let pins_before = registry.pins.len();
        assert!(registry.register(5, &binding).is_err());
        assert_eq!(registry.entries.len(), 700);
        assert_eq!(registry.pins.len(), pins_before);
        registry.entries = saved;
        registry.max_entries = 8;
        let second = registry.register(5, &binding).unwrap();
        assert_ne!(first, second);
        let epoch = registry.register_epoch_pin(5).unwrap();
        assert!(registry.release_epoch_pin(5, second).is_err());
        assert!(registry.release(5, epoch).is_err());
        registry.release_epoch_pin(5, epoch).unwrap();
        assert!(registry.has_live_or_quarantined());
        let epoch = registry.register_epoch_pin(5).unwrap();
        registry.release(5, first).unwrap();
        assert_eq!(attr(&ns.join("enable")).unwrap(), "1");
        drop(registry);
        let mut recovered = ExportLeaseRegistry::open_fixture(&root, &configfs, 8, 65536).unwrap();
        assert!(recovered.has_live_or_quarantined());
        assert_eq!(attr(&ns.join("enable")).unwrap(), "1");
        assert!(recovered.release(6, second).is_err());
        fs::write(ns.join("device_uuid"), uuid_string([9; 16])).unwrap();
        assert!(recovered.release(5, second).is_err());
        assert!(recovered.has_live_or_quarantined());
        fs::write(ns.join("device_uuid"), uuid_string(binding.export_uuid)).unwrap();
        recovered.release(5, second).unwrap();
        assert_eq!(attr(&ns.join("enable")).unwrap(), "0");
        assert!(root.join(REGISTRY).exists());
        // Removing the last export does not remove the still-live epoch pin.
        assert!(recovered.has_live_or_quarantined());
        recovered
            .fail_next_dir_sync
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(recovered.release_epoch_pin(5, epoch).is_err());
        assert!(recovered.has_live_or_quarantined());
        assert!(!root.join(REGISTRY).exists());
        // A missing file on retry must still fsync the parent, rather than
        // treating the prior uncertain unlink as durably complete.
        recovered
            .fail_next_dir_sync
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(recovered.release_epoch_pin(5, epoch).is_err());
        assert!(recovered.has_live_or_quarantined());
        recovered.release_epoch_pin(5, epoch).unwrap();
        assert!(!recovered.has_live_or_quarantined());
        assert_eq!(attr(&ns.join("enable")).unwrap(), "0");
        assert!(!root.join(REGISTRY).exists());
    }

    #[test]
    fn epoch_only_pins_are_bounded_and_survive_restart() {
        let temp = tempdir::TempDir::new("nvmeof-epoch-pin").unwrap();
        let root = temp.path().join("owner");
        fs::create_dir_all(&root).unwrap();
        let configfs = temp.path().join("unused-configfs");
        let mut registry = ExportLeaseRegistry::open_fixture(&root, &configfs, 2, 65536).unwrap();
        let first = registry.register_epoch_pin(9).unwrap();
        let second = registry.register_epoch_pin(9).unwrap();
        assert_ne!(first, second);
        assert!(registry.register_epoch_pin(9).is_err());
        drop(registry);
        let mut registry = ExportLeaseRegistry::open_fixture(&root, &configfs, 2, 65536).unwrap();
        assert!(registry.has_live_or_quarantined());
        assert!(registry.release_epoch_pin(10, first).is_err());
        registry.release_epoch_pin(9, first).unwrap();
        assert!(registry.has_live_or_quarantined());
        assert!(root.join(REGISTRY).exists());
        registry.release_epoch_pin(9, second).unwrap();
        assert!(!registry.has_live_or_quarantined());
        assert!(!root.join(REGISTRY).exists());
    }
}
