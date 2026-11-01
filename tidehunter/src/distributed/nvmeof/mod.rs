//! Sealed native WAL exports for Linux NVMe-oF readers.
//!
//! A manifest describes an owner-authorized bounded readset; its serialization
//! is not authentication. The management plane must protect its distribution.

#[cfg(target_os = "linux")]
mod namespace;
#[cfg(target_os = "linux")]
pub use namespace::{NamespaceBinding, NvmeofTransport};
#[cfg(target_os = "linux")]
pub mod lease;
#[cfg(target_os = "linux")]
mod metrics;
#[cfg(target_os = "linux")]
pub use metrics::{NvmeofMetrics, NvmeofStats, StageStats};
#[cfg(target_os = "linux")]
mod reader;
use super::types::ValueRecordReference;
#[cfg(target_os = "linux")]
pub use reader::{
    Completion, Eligibility, NvmeofError, NvmeofReader, NvmeofReaderConfig, SubmitOutcome,
};
use serde::{Deserialize, Serialize};

/// Maximum serialized manifest admitted by both publication and owner leases.
/// Separate from the conservative in-memory catalog/queue admission budgets.
pub const MAX_EXPORT_MANIFEST_BYTES: usize = 64 * 1024 * 1024;

/// One exact, owner-validated member and its original file-relative position.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntry {
    pub reference: ValueRecordReference,
    pub offset: u64,
    pub len: usize,
}

/// Persistent provenance of an independent copy of one sealed native WAL file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportManifest {
    pub version: u32,
    pub database_uuid: [u8; 16],
    pub epoch: u64,
    pub partition: u32,
    pub generation: u64,
    pub export_uuid: [u8; 16],
    pub nqn: String,
    pub wal_file_bytes: u64,
    pub source_file_index: u64,
    pub nsid: u32,
    pub image_len: u64,
    pub block_size: u32,
    pub source_device: u64,
    pub source_inode: u64,
    pub durable_frontier: u64,
    pub catalog: Vec<CatalogEntry>,
}

/// Operator-supplied namespace binding and bounded catalog budget.
#[derive(Clone, Debug)]
pub struct ExportConfig {
    pub export_uuid: [u8; 16],
    pub nqn: String,
    pub nsid: u32,
    pub block_size: u32,
    pub max_catalog_bytes: usize,
}

/// Publish only after both files and their containing directory are durable.
/// The plans retain the actual source descriptors and their storage authority.
pub(crate) fn publish_export(
    directory: &std::path::Path,
    config: &ExportConfig,
    inventory: &super::storage::StaticInventory,
    partition: u32,
    durable_frontier: u64,
    wal_file_bytes: u64,
    plans: &[(ValueRecordReference, super::storage::RecordReadPlan)],
) -> Result<ExportManifest, super::storage::StorageError> {
    use super::storage::StorageError;
    use std::io::{self, Write};
    use std::os::unix::fs::{FileExt, MetadataExt};

    if config.export_uuid == [0; 16]
        || config.nsid == 0
        || !config.nqn.starts_with("nqn.")
        || config.nqn.len() > 223
        || !config.block_size.is_power_of_two()
        || config.block_size < 512
    {
        return Err(StorageError::Invalid("invalid export namespace binding"));
    }
    let first = &plans[0].1.member;
    let source = first.file.metadata()?;
    let image_len = source.len();
    if image_len == 0 || !image_len.is_multiple_of(u64::from(config.block_size)) {
        return Err(StorageError::Invalid("WAL file is not namespace aligned"));
    }
    let file_index = plans[0].0.batch_address.offset / wal_file_bytes;
    let mut catalog = Vec::with_capacity(plans.len());
    for (reference, plan) in plans {
        let metadata = plan.member.file.metadata()?;
        let end = plan
            .member
            .offset
            .checked_add(plan.member.len as u64)
            .ok_or(StorageError::Invalid("export member range overflow"))?;
        let expected = (reference.batch_address.offset % wal_file_bytes)
            .checked_add(8)
            .and_then(|n| n.checked_add(u64::from(reference.descriptor.offset)))
            .ok_or(StorageError::Invalid("export member offset overflow"))?;
        if metadata.dev() != source.dev()
            || metadata.ino() != source.ino()
            || reference.batch_address.offset / wal_file_bytes != file_index
            || plan.member.offset != expected
            || end > image_len
            || plan.member.len != reference.descriptor.len as usize
        {
            return Err(StorageError::Invalid(
                "readset spans files or has invalid bounds",
            ));
        }
        catalog.push(CatalogEntry {
            reference: *reference,
            offset: plan.member.offset,
            len: plan.member.len,
        });
    }
    let manifest = ExportManifest {
        version: 1,
        database_uuid: inventory.database,
        epoch: inventory.epoch,
        partition,
        generation: inventory.segment_generation,
        export_uuid: config.export_uuid,
        nqn: config.nqn.clone(),
        nsid: config.nsid,
        wal_file_bytes,
        source_file_index: file_index,
        image_len,
        block_size: config.block_size,
        source_device: source.dev(),
        source_inode: source.ino(),
        durable_frontier,
        catalog,
    };
    let encoded = serde_json::to_vec(&manifest).map_err(io::Error::other)?;
    if encoded.len() > config.max_catalog_bytes || encoded.len() > MAX_EXPORT_MANIFEST_BYTES {
        return Err(StorageError::Capacity);
    }
    let parent = directory
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    // Stage in the same parent so rename is atomic. UUID-derived staging paths
    // are exclusive; a stale crash remnant must be removed by the operator.
    let suffix: String = config
        .export_uuid
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let staging = parent.join(format!(".nvmeof-export-{suffix}.partial"));
    std::fs::create_dir(&staging)?;
    struct Rollback(std::path::PathBuf, bool);
    impl Drop for Rollback {
        fn drop(&mut self) {
            if self.1 {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
    let mut rollback = Rollback(staging.clone(), true);
    let mut image = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(staging.join("image.wal"))?;
    let mut buffer = vec![0; 1024 * 1024];
    let mut offset = 0;
    while offset < image_len {
        let count = (image_len - offset).min(buffer.len() as u64) as usize;
        let read = first.file.read_at(&mut buffer[..count], offset)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sealed WAL shortened during export",
            )
            .into());
        }
        image.write_all(&buffer[..read])?;
        offset += read as u64;
    }
    // Prevent accidental writes by ordinary processes. Trusted root target
    // management still needs O_RDWR, and must enable namespace write protection.
    use std::os::unix::fs::PermissionsExt;
    image.set_permissions(std::fs::Permissions::from_mode(0o400))?;
    image.sync_all()?;
    let mut metadata = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(staging.join("manifest.json"))?;
    metadata.write_all(&encoded)?;
    metadata.set_permissions(std::fs::Permissions::from_mode(0o400))?;
    metadata.sync_all()?;
    std::fs::File::open(&staging)?.sync_all()?;
    use std::os::unix::ffi::OsStrExt;
    let from = std::ffi::CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid staging path"))?;
    let to = std::ffi::CString::new(directory.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid export path"))?;
    // SAFETY: both paths are valid NUL-terminated strings and the call borrows
    // them only for its duration. NOREPLACE prevents overwriting any user path.
    let renamed = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if renamed != 0 {
        return Err(io::Error::last_os_error().into());
    }
    rollback.0 = directory.to_path_buf();
    std::fs::File::open(parent)?.sync_all()?;
    rollback.1 = false;
    Ok(manifest)
}
