//! A1 cold reopen in a fresh process after a crash-style coordinator stop.
//!
//! Order: validate the case; sync, evict (`POSIX_FADV_DONTNEED`) and check
//! page-cache residency of every coordinator and owner file without touching
//! pages; start a fresh owner child; time the coordinator open; capture the
//! engine recovery evidence before any other access; time the first read of
//! the lowest touched ID; reconcile the replayed tail with the stop record;
//! verify every dataset row (overwrite value if touched, build value
//! otherwise). Only this post-load cold reopen feeds the restart decision.

use super::{
    BuildReceipt, CasePaths, DatasetIdentity, RECEIPT, STOP_STATE, TOUCHED, exact_row, final_value,
    validate_route, verify_rows,
};
use crate::backend::BackendResult;
use crate::config::RunConfig;
use crate::index_a1::{IndexFixtureStage, TouchedBitmap};
use crate::leased_fixture::{OwnerChild, open_coordinator};
use crate::workload::key_bytes;
use serde_json::{Value, json};
use std::fs::{self, File};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::Instant;
use tidehunter::distributed::remote_db::diagnostics_clock_ns;
use tidehunter::key_shape::{KeyShape, KeyType};

const OUTPUT: &str = "index-a1-cold.json";
/// Residency ceiling of the selected index/root and replay-window WAL files.
const MAX_RESIDENT_FRACTION: f64 = 0.01;
/// Bounded temporary mapping per `mincore` call (a page multiple).
const RESIDENCY_CHUNK: u64 = 64 * 1024 * 1024;
/// Directory depth of the case trees (owner WAL lives two levels down).
const MAX_DEPTH: usize = 8;

/// Totals of one file category.
#[derive(Default)]
pub(super) struct Residency {
    pub(super) files: u64,
    pub(super) bytes: u64,
    pub(super) allocated_bytes: u64,
    pub(super) resident_bytes: u64,
    /// OS pages of every measured file (each file rounded up).
    pub(super) selected_pages: u64,
    pub(super) resident_pages: u64,
}

impl Residency {
    #[allow(clippy::cast_precision_loss)] // Fraction evidence only.
    pub(super) fn fraction(&self) -> f64 {
        if self.bytes == 0 {
            0.0
        } else {
            self.resident_bytes as f64 / self.bytes as f64
        }
    }

    pub(super) fn json(&self) -> Value {
        json!({
            "files": self.files,
            "bytes": self.bytes,
            "allocated_bytes": self.allocated_bytes,
            "resident_bytes": self.resident_bytes,
            "resident_fraction": self.fraction(),
            "selected_pages": self.selected_pages,
            "resident_pages": self.resident_pages,
        })
    }
}

pub(super) fn page_size() -> BackendResult<u64> {
    // SAFETY: sysconf reads no caller memory.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u64::try_from(page)
        .ok()
        .filter(|page| page.is_power_of_two() && RESIDENCY_CHUNK.is_multiple_of(*page))
        .ok_or_else(|| "invalid OS page size".into())
}

/// Sync (only so pages become evictable; not a power-loss test), evict, then
/// count resident pages with bounded read-only mappings that are never read.
fn evict_and_measure(path: &Path, page: u64, totals: &mut Residency) -> BackendResult<()> {
    let file = File::open(path)?;
    file.sync_all()?;
    let fd = file.as_raw_fd();
    // SAFETY: `fd` belongs to the live `file`; zero length means the whole
    // file. Advice changes caching only, never the file bytes.
    let status = unsafe { libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_DONTNEED) };
    if status != 0 {
        return Err(std::io::Error::from_raw_os_error(status).into());
    }
    let metadata = file.metadata()?;
    let len = metadata.len();
    let mut vector = vec![0_u8; usize::try_from(RESIDENCY_CHUNK / page)?];
    let mut resident_pages = 0_u64;
    let mut offset = 0_u64;
    while offset < len {
        let bytes = usize::try_from((len - offset).min(RESIDENCY_CHUNK))?;
        let file_offset = libc::off_t::try_from(offset)?;
        // SAFETY: maps `bytes` (> 0) of the live descriptor read-only at a
        // page-aligned offset (a multiple of RESIDENCY_CHUNK). The mapping is
        // never dereferenced, so pages are not faulted in, and it is unmapped
        // below before the next iteration.
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                bytes,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                file_offset,
            )
        };
        if map == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error().into());
        }
        let pages = bytes.div_ceil(usize::try_from(page)?);
        // SAFETY: `map` is the live page-aligned mapping of `bytes` created
        // above; `vector` holds at least `pages` writable bytes (bytes is at
        // most RESIDENCY_CHUNK). mincore does not change file data.
        let result = unsafe { libc::mincore(map, bytes, vector.as_mut_ptr()) };
        let error = (result != 0).then(std::io::Error::last_os_error);
        // SAFETY: unmaps exactly the mapping created above; no reference to it remains.
        let unmapped = unsafe { libc::munmap(map, bytes) };
        if let Some(error) = error {
            return Err(error.into());
        }
        if unmapped != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        resident_pages += vector[..pages]
            .iter()
            .filter(|&&flag| flag & 1 != 0)
            .count() as u64;
        offset += RESIDENCY_CHUNK;
    }
    totals.files += 1;
    totals.bytes += len;
    totals.allocated_bytes += metadata.blocks() * 512;
    totals.selected_pages += len.div_ceil(page);
    totals.resident_pages += resident_pages;
    totals.resident_bytes += (resident_pages * page).min(len);
    Ok(())
}

/// Every regular file below `root` (bounded depth; symlinks are not followed).
pub(super) fn evict_tree(
    root: &Path,
    page: u64,
    depth: usize,
    totals: &mut Residency,
) -> BackendResult<()> {
    if depth > MAX_DEPTH {
        return Err(format!("{}: case tree deeper than {MAX_DEPTH}", root.display()).into());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            evict_tree(&entry.path(), page, depth + 1, totals)?;
        } else if kind.is_file() {
            evict_and_measure(&entry.path(), page, totals)?;
        }
    }
    Ok(())
}

fn read_json(path: &Path) -> BackendResult<Value> {
    Ok(serde_json::from_slice(&fs::read(path).map_err(
        |error| format!("{}: {error}", path.display()),
    )?)?)
}

/// `kv-research --index-a1-cold-reopen OVERWRITE_CONFIG.json --output FRESH`.
// One ordered evidence sequence; splitting it would hide the timing order.
#[allow(clippy::too_many_lines)]
pub fn cold_reopen(config_path: &Path, output: &Path) -> BackendResult<()> {
    let config = RunConfig::from_path(config_path)?;
    if config.index_fixture_stage()? != Some(IndexFixtureStage::Overwrite) {
        return Err("cold reopen takes the case's overwrite configuration".into());
    }
    fs::create_dir_all(output)?;
    if fs::read_dir(output)?.next().is_some() {
        return Err("cold reopen output directory must be fresh".into());
    }
    let mut options = super::super::parse_options(&config.backend.options)?;
    let fixture = options
        .index_fixture
        .take()
        .ok_or("index_fixture is missing")?;
    fixture.require_case_owner_root()?;
    let replay_limits = validate_route(&options)?;
    let paths = CasePaths::new(&options, None);
    let dataset = DatasetIdentity::from_config(&config)?;
    let shape = KeyShape::new_single(32, 16, KeyType::uniform(64));
    let shape_yaml = serde_yaml::to_string(&shape)?;
    let receipt: BuildReceipt = serde_json::from_slice(&fs::read(paths.file(RECEIPT))?)?;
    receipt.check(
        &paths,
        &dataset,
        &shape_yaml,
        &fixture,
        replay_limits,
        options.durability_tier,
    )?;
    let stop = read_json(&paths.file(STOP_STATE))?;
    let touched = TouchedBitmap::from_bytes(dataset.keys, &fs::read(paths.file(TOUCHED))?)?;
    let first_id = touched
        .first()
        .ok_or("no successful overwrite: nothing to read cold")?;
    let namespace = dataset.namespace()?;
    let page = page_size()?;
    let mut coordinator = Residency::default();
    let mut owner = Residency::default();
    let eviction_started = Instant::now();
    evict_tree(&paths.coordinator, page, 0, &mut coordinator)?;
    for owner_spec in &receipt.spec.owners {
        evict_tree(
            &receipt.spec.owner_root(owner_spec.partition),
            page,
            0,
            &mut owner,
        )?;
    }
    let eviction_ns = crate::duration_ns(eviction_started.elapsed());
    // Whole-tree eviction: the first-read value block is provably
    // nonresident only when no owner page stayed resident.
    let first_value_nonresident = owner.resident_bytes == 0;
    let cold = coordinator.fraction() <= MAX_RESIDENT_FRACTION
        && owner.fraction() <= MAX_RESIDENT_FRACTION
        && first_value_nonresident;
    let owner_child = OwnerChild::spawn(&receipt.spec, 0, &paths.evidence, "owner-cold")?;
    let remote_config = receipt.spec.remote_config()?;
    let keyspace = shape
        .iter_ks()
        .next()
        .ok_or("empty fixture key shape")?
        .id();
    let open_started = Instant::now();
    let open_start_ns = diagnostics_clock_ns();
    let db = open_coordinator(&paths.coordinator, shape, remote_config)?;
    let open_ns = crate::duration_ns(open_started.elapsed());
    let open_return_ns = diagnostics_clock_ns();
    // Before any qualification access: recovery and the recovered frontier.
    let recovery = db.remote_recovery_stats();
    let recovered = db.index_checkpoint_snapshot();
    let key = key_bytes(&namespace, dataset.seed, first_id);
    let read_started = Instant::now();
    let first = db
        .get(keyspace, &key)
        .map_err(|error| format!("first cold read: {error:?}"))?;
    let first_read_ns = crate::duration_ns(read_started.elapsed());
    let first_correct = exact_row(
        &final_value(&dataset, &touched, first_id)?,
        first.as_deref(),
    ) == super::RowCheck::Correct;
    let open_to_first_read_ns = crate::duration_ns(open_started.elapsed());
    let stopped = &stop["stopped"];
    let tail_matches = recovery.as_ref().is_some_and(|stats| {
        stats.checkpoint_loaded
            && stats.cut_discarded_batches == 0
            && stopped["suffix_batches"].as_u64() == Some(stats.tail_batches)
            && stopped["suffix_bytes"].as_u64() == Some(stats.tail_bytes)
    });
    let predecessor = stopped["fencing_generation"].as_u64();
    let generation_advanced = recovered
        .as_ref()
        .zip(predecessor)
        .is_some_and(|(snapshot, stopped)| snapshot.fencing_generation > stopped);
    let (tally, verification) = verify_rows(&db, keyspace, &namespace, &dataset, |id, value| {
        Ok(exact_row(&final_value(&dataset, &touched, id)?, value))
    })?;
    let mismatched = tally.mismatched();
    // Orderly close after timing and verification; the case is complete.
    let close = db
        .wait_for_background_threads_to_finish()
        .map_err(|error| format!("{error:?}"));
    let owner_pid = owner_child.pid;
    let owner_stop = owner_child.stop().map_err(|error| error.to_string());
    #[allow(clippy::cast_precision_loss)] // Seconds for the decision input.
    let open_to_first_read_s = open_to_first_read_ns as f64 / 1e9;
    let correct = first_correct && mismatched == 0;
    let qualified = cold && correct && tail_matches && generation_advanced;
    let evidence = json!({
        "schema_version": 1,
        "case_id": config.case_id,
        "qualified": qualified,
        "correct": correct,
        "cold": cold,
        "tail_matches_stop_record": tail_matches,
        "fencing_generation_advanced": generation_advanced,
        "residency": {
            "coordinator": coordinator.json(),
            "owner_wal": owner.json(),
            "max_resident_fraction": MAX_RESIDENT_FRACTION,
            "first_value_block_nonresident": first_value_nonresident,
            "first_value_block_rule": "proved only by zero resident owner pages after whole-tree eviction",
            "eviction_ns": eviction_ns,
        },
        "open": {
            "open_start_ns": open_start_ns,
            "open_return_ns": open_return_ns,
            "open_ns": open_ns,
            "includes": "the complete engine open, including fencing-generation trim when the binary has L1",
            "recovery": recovery.as_ref().map(crate::index_checkpoint::recovery_json),
            "recovered_snapshot": recovered,
        },
        "first_read": {
            "id": first_id,
            "read_ns": first_read_ns,
            "correct": first_correct,
            "open_start_to_first_correct_read_ns": first_correct.then_some(open_to_first_read_ns),
            "open_start_to_first_correct_read_s": first_correct.then_some(open_to_first_read_s),
        },
        "stop_record": stopped,
        "verification": verification,
        "touched_count": touched.count(),
        "close_error": close.err(),
        "owner_stop_error": owner_stop.err(),
        "owner": {"pid": owner_pid, "logs": paths.evidence.join("owner-cold.stderr.log")},
    });
    fs::write(output.join(OUTPUT), serde_json::to_vec_pretty(&evidence)?)?;
    if !correct {
        return Err(format!("cold reopen read incorrect data: {mismatched} mismatched rows, first read correct={first_correct}").into());
    }
    Ok(())
}
