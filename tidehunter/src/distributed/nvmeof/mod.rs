//! Live native WAL data paths and sealed readsets over Linux NVMe-oF.
//!
//! A manifest describes an owner-authorized bounded readset; its serialization
//! is not authentication. The management plane must protect its distribution.

#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod namespace;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub use namespace::NamespaceBinding;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod lease;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_connect;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_io;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_metrics;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_namespace;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_pool;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub(crate) mod live_prometheus;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_storage;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub mod live_target;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod metrics;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod spdk;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub use metrics::{NvmeofMetrics, NvmeofStats, StageStats};
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
mod reader;
use super::types::ValueRecordReference;
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
pub use reader::{
    Completion, Eligibility, MemberRange, NvmeofCatalog, NvmeofError, NvmeofReader,
    NvmeofReaderConfig, ReadPlan, SubmitOutcome,
};
use serde::{Deserialize, Serialize};

/// Explicit target protection policy. A failed kernel check never selects SPDK.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExportTarget {
    #[default]
    Kernel,
    Spdk(SpdkTarget),
    /// Kernel nvmet with `buffered_io=0`, for sealed read-only image controls.
    /// This measurement control is never the default or a live WAL target.
    KernelDirect,
}

/// Owner-verified identity of the root-owned SPDK export process and AIO bdev.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpdkTarget {
    pub rpc_socket: std::path::PathBuf,
    pub pid: u32,
    pub process_start_ticks: u64,
    pub bdev_name: String,
    pub image_path: std::path::PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NvmeofTransport {
    Tcp,
    Rdma,
}

/// Externally managed SPDK daemon used for live WAL namespaces.
///
/// The caller must provision and admit the patched executable whose namespace
/// removal RPC reports pause and resume failures. Stock SPDK does not provide
/// the fence guarantee required by live WAL recovery. Campaign admission must
/// verify its source patch, build proof and binary identity before launch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveSpdkConfig {
    pub rpc_socket: std::path::PathBuf,
    pub executable: std::path::PathBuf,
}

/// Bounded live controller queues; defaults preserve the original connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveQueueGeometry {
    pub io_queues: u32,
    pub queue_size: u32,
    pub poll_queues: u32,
}

impl Default for LiveQueueGeometry {
    fn default() -> Self {
        Self {
            io_queues: 1,
            queue_size: 32,
            poll_queues: 0,
        }
    }
}

impl LiveQueueGeometry {
    pub fn validate(self) -> Result<(), &'static str> {
        if !(1..=16).contains(&self.io_queues)
            || !(16..=128).contains(&self.queue_size)
            || self.poll_queues > 4
        {
            return Err("invalid live queue geometry");
        }
        Ok(())
    }
}

/// Live WAL export scope. `OnDemand` revokes a file grant after its last pin.
/// `PerFile` and `PerOwner` export active-epoch files until seal or fencing,
/// and cache a bounded number of idle readonly immutable-history exports
/// between page pins. Seal, fencing, retirement and target shutdown revoke them.
/// Page pins still protect in-flight reads; idle exports hold no read pins.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveExportMode {
    #[default]
    OnDemand,
    PerFile,
    PerOwner,
}

impl LiveExportMode {
    pub const fn files_per_subsystem(self) -> Option<u64> {
        match self {
            Self::OnDemand => None,
            Self::PerFile => Some(1),
            Self::PerOwner => Some(1024),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTargetConfig {
    pub address: String,
    pub service: u16,
    pub transport: NvmeofTransport,
    pub host_nqn: String,
    pub port_id: u16,
    pub state_dir: std::path::PathBuf,
    pub commissioning_port_id: u16,
    pub commissioning_service: u16,
    #[serde(default)]
    pub spdk: Option<LiveSpdkConfig>,
    #[serde(default)]
    /// Permanent modes require the kernel target and leased coordinator writes.
    pub export_mode: LiveExportMode,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveTargetImplementation {
    #[default]
    Kernel,
    Spdk,
}

/// Per-process target evidence. Fixture events are not kernel qualifications.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LiveTargetStats {
    pub fixture_only: bool,
    #[serde(default)]
    pub target_implementation: LiveTargetImplementation,
    pub namespaces_exposed: u64,
    pub disable_drain_successes: u64,
    pub readonly_commission_successes: u64,
    pub errors: u64,
    pub owned_namespace_count: u64,
    pub fence_count: u64,
    pub disable_drain_time_ns: u64,
}

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
#[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
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
    struct Rollback<'a>(&'a std::path::Path, bool);
    impl Drop for Rollback<'_> {
        fn drop(&mut self) {
            if self.1 {
                let _ = std::fs::remove_dir_all(self.0);
            }
        }
    }
    let mut rollback = Rollback(&staging, true);
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
    rollback.0 = directory;
    std::fs::File::open(parent)?.sync_all()?;
    rollback.1 = false;
    Ok(manifest)
}
