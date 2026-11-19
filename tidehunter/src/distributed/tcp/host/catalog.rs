//! The owner host catalogue `<root>/tcp-owner-host-v1`: the sole authority for
//! which databases this host serves, which target slot each one owns and how
//! far its provisioning or drop got. Only the host lifecycle actor writes it.
//!
//! Every persisted transition is one CRC-framed atomic replacement
//! (`install_metadata`: pending file, file sync, rename, directory sync), so a
//! crash leaves either the old or the new catalogue, never a mixture.

use super::super::TcpError;
use super::identity;
use crate::distributed::durability::DurabilityTier;
use crate::distributed::nvmeof::{LiveTargetConfig, NvmeofTransport};
use crate::distributed::storage::{
    PartitionConfig, StorageError, encode_metadata, install_metadata, read_metadata,
};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

pub(crate) const CATALOG: &str = "tcp-owner-host-v1";
pub(crate) const CATALOG_FORMAT: u32 = 1;
/// Per-database owner root, `db-<32 lowercase hex>`.
pub(crate) const DATABASE_PREFIX: &str = "db-";
/// Target registry directory inside each database root.
pub(crate) const LIVE_TARGET_DIR: &str = "live-target";

/// Contiguous target slot ranges shared by all databases of one host. Slot `s`
/// owns public port/service `base + s` and commissioning port/service `base + s`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveTargetPool {
    pub address: String,
    pub transport: NvmeofTransport,
    pub public_port_base: u16,
    pub public_service_base: u16,
    pub commissioning_port_base: u16,
    pub commissioning_service_base: u16,
}

fn range(base: u16, slots: usize) -> Option<(u32, u32)> {
    let last = u32::from(base).checked_add(u32::try_from(slots).ok()?.checked_sub(1)?)?;
    (base != 0 && last <= u32::from(u16::MAX)).then_some((u32::from(base), last))
}

fn disjoint(a: (u32, u32), b: (u32, u32)) -> bool {
    a.1 < b.0 || b.1 < a.0
}

impl LiveTargetPool {
    /// Nonzero bases, every slot below `max_databases` fits `u16`, and the
    /// public and commissioning ranges never overlap.
    pub fn validate(&self, max_databases: usize) -> Result<(), TcpError> {
        let invalid = |reason: &str| {
            TcpError::with_kind(
                io::ErrorKind::InvalidInput,
                format!("invalid live target pool: {reason}"),
            )
        };
        if self.address.parse::<IpAddr>().is_err() {
            return Err(invalid("address must be a numeric IP"));
        }
        let ranges = [
            self.public_port_base,
            self.commissioning_port_base,
            self.public_service_base,
            self.commissioning_service_base,
        ]
        .map(|base| range(base, max_databases));
        let [
            Some(public_port),
            Some(commission_port),
            Some(public_service),
            Some(commission_service),
        ] = ranges
        else {
            return Err(invalid(
                "bases must be nonzero and base + max_databases - 1 must fit u16",
            ));
        };
        if !disjoint(public_port, commission_port) {
            return Err(invalid("public and commissioning port ranges overlap"));
        }
        if !disjoint(public_service, commission_service) {
            return Err(invalid("public and commissioning service ranges overlap"));
        }
        Ok(())
    }

    /// The actual target configuration of one slot. The host NQN is the
    /// effective initiator; the registry lives inside the database root.
    pub(crate) fn target_config(
        &self,
        slot: u32,
        database: &[u8; 16],
        partition: u32,
        database_root: &Path,
    ) -> Result<LiveTargetConfig, TcpError> {
        let offset = |base: u16| {
            u16::try_from(u32::from(base) + slot).map_err(|_| {
                TcpError::with_kind(io::ErrorKind::InvalidData, "target slot overflow")
            })
        };
        Ok(LiveTargetConfig {
            address: self.address.clone(),
            service: offset(self.public_service_base)?,
            transport: self.transport,
            host_nqn: identity::effective_host_nqn(database, partition),
            port_id: offset(self.public_port_base)?,
            state_dir: database_root.join(LIVE_TARGET_DIR),
            commissioning_port_id: offset(self.commissioning_port_base)?,
            commissioning_service: offset(self.commissioning_service_base)?,
            // Subsystem names take their DB/partition scope from this
            // effective host NQN (`live_target::subsystem_scope`).
        })
    }
}

/// Configured initiator and requested tier of one database partition. The
/// owner derives the effective initiator from these and the database ID.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseLiveTarget {
    pub initiator_nqn: String,
    pub initiator_id: [u8; 16],
    pub durability_tier: DurabilityTier,
}

impl DatabaseLiveTarget {
    pub(crate) fn validate(&self) -> Result<(), TcpError> {
        if self.initiator_id == [0; 16]
            || !self.initiator_nqn.starts_with("nqn.")
            || self.initiator_nqn.len() > 223
            || !self
                .initiator_nqn
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b',' && b != b'=')
        {
            return Err(TcpError::with_kind(
                io::ErrorKind::InvalidInput,
                "invalid live initiator identity in provision request",
            ));
        }
        Ok(())
    }
}

/// Everything a provision fixes for the life of a database on this host.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseProvisionSpec {
    pub partition: PartitionConfig,
    pub shape_yaml: String,
    pub live_target: DatabaseLiveTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DropPhase {
    /// Target journal still present: reconcile and tear down, never recommission.
    TargetCleanup,
    /// Target absence proven: only database files remain to delete.
    FilesOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum HostState {
    /// Directory and slot reserved; setup may be partial and is replayed.
    Preparing,
    /// Inventory and target registry exist; routed after startup recovery.
    Ready,
    /// Durable drop intent; never routed again.
    Dropping {
        final_generation: u64,
        phase: DropPhase,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostEntry {
    pub database: [u8; 16],
    pub principal: [u8; 32],
    pub slot: u32,
    pub spec: DatabaseProvisionSpec,
    pub state: HostState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostCatalog {
    pub format: u32,
    pub target_pool: LiveTargetPool,
    pub entries: Vec<HostEntry>,
}

fn corrupt(reason: impl Into<String>) -> TcpError {
    TcpError::with_kind(io::ErrorKind::InvalidData, reason)
}

fn storage_error(path: &Path, error: StorageError) -> TcpError {
    match error {
        StorageError::Io(error) => TcpError::with_kind(
            error.kind(),
            format!("owner host catalogue {}: {error}", path.display()),
        ),
        error => corrupt(format!("owner host catalogue {}: {error}", path.display())),
    }
}

impl HostCatalog {
    pub(crate) fn empty(target_pool: LiveTargetPool) -> Self {
        Self {
            format: CATALOG_FORMAT,
            target_pool,
            entries: Vec::new(),
        }
    }

    /// Load and validate, or `None` when the catalogue file does not exist.
    /// A non-regular file or symlink is corruption, never absence.
    pub(crate) fn load(root: &Path) -> Result<Option<Self>, TcpError> {
        let path = root.join(CATALOG);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_file() => {}
            Ok(_) => return Err(corrupt("owner host catalogue is not a regular file")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        }
        let catalog: Self = read_metadata(&path).map_err(|error| storage_error(&path, error))?;
        if catalog.format != CATALOG_FORMAT {
            return Err(corrupt(format!(
                "unsupported distributed owner host catalogue format {} (expected {CATALOG_FORMAT})",
                catalog.format
            )));
        }
        Ok(Some(catalog))
    }

    /// Structural invariants, checked on load and before every persist.
    pub(crate) fn validate(&self, max_databases: usize) -> Result<(), TcpError> {
        if self.entries.len() > max_databases {
            return Err(corrupt(format!(
                "owner host catalogue has {} entries above max_databases {max_databases}",
                self.entries.len()
            )));
        }
        if self
            .entries
            .windows(2)
            .any(|pair| pair[0].database >= pair[1].database)
        {
            return Err(corrupt(
                "owner host catalogue entries are not strictly sorted",
            ));
        }
        let mut slots: Vec<u32> = self.entries.iter().map(|entry| entry.slot).collect();
        slots.sort_unstable();
        if slots.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(corrupt("owner host catalogue reuses a target slot"));
        }
        for entry in &self.entries {
            if entry.database == [0; 16]
                || usize::try_from(entry.slot).map_or(true, |slot| slot >= max_databases)
                || entry.spec.partition.inventory.database != entry.database
            {
                return Err(corrupt(format!(
                    "owner host catalogue entry {} has an invalid identity or slot",
                    identity::database_hex(&entry.database)
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn persist(&self, root: &Path, max_databases: usize) -> Result<(), TcpError> {
        self.validate(max_databases)?;
        let path = root.join(CATALOG);
        let bytes = encode_metadata(self).map_err(|error| storage_error(&path, error))?;
        install_metadata(root, CATALOG, &bytes).map_err(|error| {
            TcpError::with_kind(
                error.kind(),
                format!("persist owner host catalogue {}: {error}", path.display()),
            )
        })
    }

    pub(crate) fn entry(&self, database: &[u8; 16]) -> Option<&HostEntry> {
        self.entries
            .binary_search_by(|entry| entry.database.cmp(database))
            .ok()
            .map(|index| &self.entries[index])
    }

    pub(crate) fn entry_mut(&mut self, database: &[u8; 16]) -> Option<&mut HostEntry> {
        self.entries
            .binary_search_by(|entry| entry.database.cmp(database))
            .ok()
            .map(|index| &mut self.entries[index])
    }

    /// Insert keeping the sort order. The caller checked absence and capacity.
    pub(crate) fn insert(&mut self, entry: HostEntry) {
        let index = self
            .entries
            .binary_search_by(|current| current.database.cmp(&entry.database))
            .unwrap_or_else(|index| index);
        self.entries.insert(index, entry);
    }

    pub(crate) fn remove(&mut self, database: &[u8; 16]) -> Option<HostEntry> {
        self.entries
            .binary_search_by(|entry| entry.database.cmp(database))
            .ok()
            .map(|index| self.entries.remove(index))
    }

    /// Least slot not held by any entry, including Preparing and Dropping:
    /// a slot is free only after its old database's terminal cleanup.
    pub(crate) fn least_free_slot(&self, max_databases: usize) -> Option<u32> {
        let mut used: Vec<u32> = self.entries.iter().map(|entry| entry.slot).collect();
        used.sort_unstable();
        let limit = u32::try_from(max_databases).ok()?;
        (0..limit).find(|slot| used.binary_search(slot).is_err())
    }
}

/// `<root>/db-<32 lowercase hex>`.
pub(crate) fn database_root(root: &Path, database: &[u8; 16]) -> PathBuf {
    root.join(format!(
        "{DATABASE_PREFIX}{}",
        identity::database_hex(database)
    ))
}
