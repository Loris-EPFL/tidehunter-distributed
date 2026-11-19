//! Append-only disk tombstones `<root>/dropped/<32 lowercase hex>`.
//!
//! A tombstone is the terminal record of a dropped (or never provisioned but
//! dropped) database. It is never loaded into the active map, removed,
//! replaced or opened as a database directory, so a delayed provision of the
//! same ID can never recreate it. Record bytes: an outer native CRC frame
//! around `be_u32 format=1 | database[16] | principal[32] | be_u64
//! final_generation`. The file name and the body must agree.

use super::super::TcpError;
use super::identity;
use crate::crc::CrcFrame;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

pub(crate) const DROPPED_DIR: &str = "dropped";
const FORMAT: u32 = 1;
const PAYLOAD_LEN: usize = 4 + 16 + 32 + 8;
const RECORD_LEN: usize = CrcFrame::CRC_HEADER_LENGTH + PAYLOAD_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DroppedDb {
    pub database: [u8; 16],
    pub principal: [u8; 32],
    pub final_generation: u64,
}

fn corrupt(path: &Path, reason: &str) -> TcpError {
    TcpError::with_kind(
        io::ErrorKind::InvalidData,
        format!("database tombstone {}: {reason}", path.display()),
    )
}

impl DroppedDb {
    fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(PAYLOAD_LEN);
        payload.extend_from_slice(&FORMAT.to_be_bytes());
        payload.extend_from_slice(&self.database);
        payload.extend_from_slice(&self.principal);
        payload.extend_from_slice(&self.final_generation.to_be_bytes());
        CrcFrame::new(&payload).as_ref().to_vec()
    }

    fn decode(path: &Path, bytes: &[u8]) -> Result<Self, TcpError> {
        if bytes.len() != RECORD_LEN {
            return Err(corrupt(path, "unexpected record length"));
        }
        let payload =
            CrcFrame::read_from_slice(bytes, 0).map_err(|_| corrupt(path, "CRC invalid"))?;
        if payload.len() != PAYLOAD_LEN {
            return Err(corrupt(path, "unexpected payload length"));
        }
        let (format, rest) = payload.split_at(4);
        let (database, rest) = rest.split_at(16);
        let (principal, generation) = rest.split_at(32);
        let format = u32::from_be_bytes(format.try_into().map_err(|_| corrupt(path, "format"))?);
        if format != FORMAT {
            return Err(corrupt(path, "unsupported tombstone format"));
        }
        Ok(Self {
            database: database.try_into().map_err(|_| corrupt(path, "database"))?,
            principal: principal
                .try_into()
                .map_err(|_| corrupt(path, "principal"))?,
            final_generation: u64::from_be_bytes(
                generation
                    .try_into()
                    .map_err(|_| corrupt(path, "generation"))?,
            ),
        })
    }
}

fn path_of(root: &Path, database: &[u8; 16]) -> PathBuf {
    root.join(DROPPED_DIR)
        .join(identity::database_hex(database))
}

/// Create `<root>/dropped` if needed and sync the host root after creation.
pub(crate) fn ensure_directory(root: &Path) -> Result<(), TcpError> {
    let directory = root.join(DROPPED_DIR);
    match fs::symlink_metadata(&directory) {
        Ok(metadata) if metadata.file_type().is_dir() => return Ok(()),
        Ok(_) => {
            return Err(corrupt(
                &directory,
                "tombstone directory is not a directory",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    fs::create_dir(&directory)?;
    File::open(root)?.sync_all()?;
    Ok(())
}

/// Read the tombstone of `database`, or `None` if it does not exist. A
/// symlink, foreign name or malformed body is corruption, never absence.
pub(crate) fn load(root: &Path, database: &[u8; 16]) -> Result<Option<DroppedDb>, TcpError> {
    let path = path_of(root, database);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err(corrupt(&path, "not a regular file")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut bytes = Vec::with_capacity(RECORD_LEN);
    File::open(&path)?
        .take(RECORD_LEN as u64 + 1)
        .read_to_end(&mut bytes)?;
    let record = DroppedDb::decode(&path, &bytes)?;
    if record.database != *database {
        return Err(corrupt(&path, "name and body disagree"));
    }
    Ok(Some(record))
}

/// Install a tombstone exactly once. An existing identical record is
/// success; a different one is never overwritten. Syncs the tombstone
/// directory before returning, so an ACK never precedes durability.
pub(crate) fn install(root: &Path, record: &DroppedDb) -> Result<(), TcpError> {
    if let Some(existing) = load(root, &record.database)? {
        if existing != *record {
            return Err(TcpError::with_kind(
                io::ErrorKind::InvalidInput,
                format!(
                    "DropDatabase {}: terminal generation differs",
                    identity::database_hex(&record.database)
                ),
            ));
        }
        // A crash after rename may predate the directory sync: sync again.
        File::open(root.join(DROPPED_DIR))?.sync_all()?;
        return Ok(());
    }
    let directory = root.join(DROPPED_DIR);
    crate::distributed::storage::install_metadata(
        &directory,
        &identity::database_hex(&record.database),
        &record.encode(),
    )
    .map_err(|error| {
        TcpError::with_kind(
            error.kind(),
            format!(
                "install database tombstone in {}: {error}",
                directory.display()
            ),
        )
    })
}

/// Names under `dropped/` are exactly 32 lowercase hex digits or a pending
/// install of one; anything else is corruption.
pub(crate) fn validate_directory(root: &Path) -> Result<(), TcpError> {
    let directory = root.join(DROPPED_DIR);
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| corrupt(&directory, "non-UTF-8 tombstone name"))?;
        let stem = name
            .strip_prefix('.')
            .and_then(|pending| pending.strip_suffix(".pending"))
            .unwrap_or(name);
        if identity::parse_database_hex(stem).is_none() || !entry.file_type()?.is_file() {
            return Err(corrupt(
                &entry.path(),
                "unexpected tombstone directory entry",
            ));
        }
    }
    Ok(())
}
