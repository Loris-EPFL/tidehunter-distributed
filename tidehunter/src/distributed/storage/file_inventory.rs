//! Once-per-used-file inventory witnesses for the bounded unrooted WAL suffix.
//! The WAL inode AND its name are durable before publishing a witness. A crash
//! before the witness is therefore an unacknowledged surviving group, not an
//! unexplained missing file. A root may remove witnesses only after adopting the
//! required highwater; directory lookahead alone is never a durable requirement.
use super::*;

const PREFIX: &str = "partition-required-wal-v1-";
const MAX_WITNESSES: usize = 1040;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct FileWitness {
    database: [u8; 16],
    epoch: u64,
    partition: u32,
    generation: u64,
    file: u64,
}

impl FileWitness {
    fn new(config: &PartitionConfig, file: u64) -> Self {
        Self {
            database: config.inventory.database,
            epoch: config.inventory.epoch,
            partition: config.partition,
            generation: config.inventory.segment_generation,
            file,
        }
    }
}

fn parse(name: &str) -> Result<Option<u64>, StorageError> {
    let Some(id) = name.strip_prefix(PREFIX) else {
        return Ok(None);
    };
    if id.len() != 16
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StorageError::Invalid("noncanonical WAL witness filename"));
    }
    Ok(Some(u64::from_str_radix(id, 16).map_err(|_| {
        StorageError::Invalid("invalid WAL witness file ID")
    })?))
}

pub(super) fn persist(
    path: &Path,
    config: &PartitionConfig,
    file: u64,
    timing: &mut WriteTiming<'_>,
) -> io::Result<bool> {
    let name = format!("{PREFIX}{file:016x}");
    let expected = FileWitness::new(config, file);
    if path.join(&name).try_exists()? {
        let actual: FileWitness = read_metadata(&path.join(&name)).map_err(io::Error::other)?;
        if actual != expected {
            return Err(io::Error::other("WAL witness differs from authority"));
        }
        return Ok(false);
    }
    let bytes = encode_metadata(&expected).map_err(io::Error::other)?;
    let temporary = path.join(format!(".{name}.pending"));
    timing.stage(Some(WriteStage::PersistInventoryFile));
    let mut output = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    output.write_all(&bytes)?;
    timing.record(|profile| {
        profile.inventory_file_persist_calls =
            profile.inventory_file_persist_calls.saturating_add(1)
    });
    output.sync_all()?;
    timing.record(|profile| {
        profile.inventory_file_persist_successes =
            profile.inventory_file_persist_successes.saturating_add(1)
    });
    timing.stage(None);
    fs::rename(temporary, path.join(name))?;
    timing.stage(Some(WriteStage::PersistInventoryNamespace));
    timing.record(|profile| {
        profile.inventory_namespace_persist_calls =
            profile.inventory_namespace_persist_calls.saturating_add(1)
    });
    File::open(path)?.sync_all()?;
    timing.record(|profile| {
        profile.inventory_namespace_persist_successes = profile
            .inventory_namespace_persist_successes
            .saturating_add(1)
    });
    timing.stage(None);
    Ok(true)
}

pub(super) fn validate(
    path: &Path,
    config: &PartitionConfig,
    file_count: u64,
) -> Result<(), StorageError> {
    let mut count = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(file) = parse(
            name.to_str()
                .ok_or(StorageError::Invalid("non-UTF8 witness filename"))?,
        )?
        else {
            continue;
        };
        count += 1;
        if count > MAX_WITNESSES || !entry.file_type()?.is_file() {
            return Err(StorageError::Invalid("WAL witness inventory exceeds bound"));
        }
        let actual: FileWitness = read_metadata(&entry.path())?;
        if actual != FileWitness::new(config, file) {
            return Err(StorageError::Invalid("WAL witness differs from authority"));
        }
        if file >= file_count && !path.join(format!("wal_{file:016x}")).is_file() {
            return Err(StorageError::Invalid("missing witnessed suffix WAL file"));
        }
    }
    Ok(())
}

pub(super) fn remove_covered(path: &Path, file_count: u64) -> Result<(), StorageError> {
    let mut removed = false;
    // At most one bounded post-root window can exist. Rescan bounded pages so
    // deleting directory entries cannot make readdir skip a remaining witness.
    for _ in 0..=MAX_WITNESSES.div_ceil(256) {
        let mut paths = Vec::with_capacity(256);
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or(StorageError::Invalid("non-UTF8 witness filename"))?;
            let authority_name = name
                .strip_prefix('.')
                .and_then(|name| name.strip_suffix(".pending"))
                .unwrap_or(name);
            if parse(authority_name)?.is_some_and(|file| file < file_count) {
                paths.push(entry.path());
                if paths.len() == 256 {
                    break;
                }
            }
        }
        if paths.is_empty() {
            if removed {
                File::open(path)?.sync_all()?;
            }
            return Ok(());
        }
        for path in paths {
            fs::remove_file(path)?;
        }
        removed = true;
    }
    Err(StorageError::Invalid(
        "covered WAL witness cleanup exceeds bound",
    ))
}
