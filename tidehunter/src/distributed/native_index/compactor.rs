//! Native index-flush compactor at Exact capture (COMPACTOR-PARITY.md §§2–4).
//!
//! Native Tidehunter runs a keyspace's compactor when it flushes a dirty
//! cell (paper §4.3): the callback sees the sorted unique keys of the
//! candidate and returns the keys to retain. Leased Tidehunter has no
//! per-cell flush; its index flush is the Exact capture. A callback-enabled
//! cell is evaluated, at most once per capture, when one native predicate
//! holds at that capture:
//! - pressure: distinct dirty keys minus the cell's jitter exceed the
//!   keyspace's `max_dirty_keys` (unless unloading is disabled);
//! - stale: a periodic evaluation is due and the cell is dirty and has not
//!   been evaluated for `snapshot_unload_threshold` covered bytes;
//! - force: a named `force_rebuild_control_region` and the cell is dirty.
//!
//! A rejected key becomes an index-only [`PruneMark`] bound to the exact
//! selected head at the capture cut `C`. Writes after `C` stay in the live
//! suffix and win. The open-time recovery baseline never runs a callback.
//! Evaluation runs off-lock in the checkpoint worker, before the cell is
//! encoded; it takes no Sui write gate, index shard or publisher lock.

use super::prune_marks::{self, MarkBudget, PruneMark};
use super::snapshot::{FrozenCell, Rows};
use super::*;
use crate::config::Config;
use crate::key_shape::Compactor;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Native periodic snapshot observation interval (`N:db.rs:494–542`).
const PERIODIC_POLL: Duration = Duration::from_secs(60);
/// Native timed snapshot interval.
const PERIODIC_HOUR: Duration = Duration::from_secs(3600);
/// Charged scratch per eligible head: the key handle, its version and one
/// slot of the returned retain set.
const HEAD_CHARGE: usize =
    std::mem::size_of::<(Bytes, MutationVersion)>() + std::mem::size_of::<Bytes>() + 16;

/// Native cadence settings consumed by callback-enabled keyspaces. Built from
/// the same native [`Config`] fields Sui sets for the native backend; the
/// default is the native default configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompactorCadence {
    pub max_dirty_keys: usize,
    pub unload_jitter_pct: usize,
    pub snapshot_written_bytes: u64,
    pub snapshot_unload_threshold: u64,
}

impl CompactorCadence {
    pub fn from_native(config: &Config) -> Self {
        Self {
            max_dirty_keys: config.max_dirty_keys,
            unload_jitter_pct: config.unload_jitter_pct,
            snapshot_written_bytes: config.snapshot_written_bytes,
            snapshot_unload_threshold: config.snapshot_unload_threshold,
        }
    }

    /// Native jitter range: `0..max(1, max_dirty_keys * unload_jitter_pct /
    /// 100)` from the database setting, not from a keyspace override
    /// (`N:config.rs:277–283`).
    fn jitter_bound(&self) -> usize {
        (self.max_dirty_keys.saturating_mul(self.unload_jitter_pct) / 100).max(1)
    }
}

impl Default for CompactorCadence {
    fn default() -> Self {
        Self::from_native(&Config::default())
    }
}

struct PolicyKeyspace {
    keyspace: KeySpace,
    compactor: Arc<Compactor>,
    max_dirty_keys: usize,
    /// Native skips pressure unloading for keyspaces with unloading disabled.
    pressure: bool,
}

/// Process-monotonic periodic clock, initialized at open; never persisted.
struct PolicyClock {
    last_poll: Instant,
    hour_start: Instant,
}

/// Runtime compactor policy of one database: the callback keyspaces of the
/// caller's in-memory shape (closures are never persisted or sent to owners)
/// and the native cadence. Absent when no live keyspace has a callback, so a
/// callback-free database does no policy work.
pub(crate) struct CompactorPolicy {
    cadence: CompactorCadence,
    /// Ascending keyspace IDs.
    keyspaces: Vec<PolicyKeyspace>,
    /// Cold: taken once per capture, under the checkpoint freeze.
    clock: Mutex<PolicyClock>,
    #[cfg(test)]
    test_now: Mutex<Option<Instant>>,
}

impl CompactorPolicy {
    pub(crate) fn from_shape(shape: &KeyShape, cadence: CompactorCadence) -> Option<Arc<Self>> {
        let mut keyspaces: Vec<PolicyKeyspace> = shape
            .iter_ks()
            .filter(|descriptor| !descriptor.destroyed())
            .filter_map(|descriptor| {
                Some(PolicyKeyspace {
                    keyspace: descriptor.id(),
                    compactor: descriptor.compactor_handle()?,
                    max_dirty_keys: descriptor
                        .max_dirty_keys()
                        .unwrap_or(cadence.max_dirty_keys),
                    pressure: !descriptor.unloading_disabled(),
                })
            })
            .collect();
        if keyspaces.is_empty() {
            return None;
        }
        keyspaces.sort_by_key(|keyspace| keyspace.keyspace.0);
        let now = Instant::now();
        Some(Arc::new(Self {
            cadence,
            keyspaces,
            clock: Mutex::new(PolicyClock {
                last_poll: now,
                hour_start: now,
            }),
            #[cfg(test)]
            test_now: Mutex::new(None),
        }))
    }

    fn keyspace(&self, keyspace: KeySpace) -> Option<&PolicyKeyspace> {
        self.keyspaces
            .binary_search_by_key(&keyspace.0, |entry| entry.keyspace.0)
            .ok()
            .map(|at| &self.keyspaces[at])
    }

    pub(crate) fn enabled(&self, keyspace: KeySpace) -> bool {
        self.keyspace(keyspace).is_some()
    }

    fn now(&self) -> Instant {
        #[cfg(test)]
        if let Some(now) = *self.test_now.lock() {
            return now;
        }
        Instant::now()
    }

    /// Test-only fake monotonic clock.
    #[cfg(test)]
    pub(crate) fn set_test_now(&self, now: Instant) {
        *self.test_now.lock() = Some(now);
    }

    /// One periodic observation at a capture. True on the first capture at or
    /// after the next 60 s poll when the covered bytes since the last
    /// periodic evaluation exceed `snapshot_written_bytes` or the timed hour
    /// elapsed. No timer thread: a quiet database evaluates nothing.
    pub(crate) fn observe_periodic(&self, covered_bytes: u64, last_periodic_bytes: u64) -> bool {
        let now = self.now();
        let mut clock = self.clock.lock();
        if now.saturating_duration_since(clock.last_poll) < PERIODIC_POLL {
            return false;
        }
        clock.last_poll = now;
        let written =
            covered_bytes.saturating_sub(last_periodic_bytes) > self.cadence.snapshot_written_bytes;
        let hour = now.saturating_duration_since(clock.hour_start) >= PERIODIC_HOUR;
        if written || hour {
            clock.hour_start = now;
        }
        written || hour
    }

    /// Native per-birth jitter, sampled once per in-process cell birth.
    pub(super) fn sample_jitter(&self) -> u32 {
        let bound = self.cadence.jitter_bound();
        u32::try_from(rand::thread_rng().gen_range(0..bound)).unwrap_or(u32::MAX)
    }
}

/// Policy inputs frozen with one Exact capture.
#[derive(Clone)]
pub(crate) struct PolicyCapture {
    pub policy: Arc<CompactorPolicy>,
    /// Adopted covered bytes plus the canonical frame lengths of the receipts
    /// this capture newly covers.
    pub covered_bytes: u64,
    pub periodic: bool,
    pub force: bool,
    /// The open-time root after suffix replay: count dirt, never call back.
    pub recovery_baseline: bool,
}

/// Baseline of one enabled keyspace: the adopted cut and covered bytes when
/// its callback was enabled. A cell without a persisted override starts here
/// with zero dirt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KeyspaceBaseline {
    pub keyspace: KeySpace,
    pub cut: Option<MutationVersion>,
    pub bytes: u64,
}

/// Root policy block of one adopted Exact image.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct PolicyRoot {
    pub covered_bytes: u64,
    pub last_periodic_bytes: u64,
    /// Ascending keyspace IDs.
    pub keyspaces: Vec<KeyspaceBaseline>,
}

impl PolicyRoot {
    pub(super) fn baseline(&self, keyspace: KeySpace) -> Option<CellPolicy> {
        self.keyspaces
            .binary_search_by_key(&keyspace.0, |entry| entry.keyspace.0)
            .ok()
            .map(|at| CellPolicy {
                last_cut: self.keyspaces[at].cut,
                last_bytes: self.keyspaces[at].bytes,
                dirty: 0,
            })
    }

    /// Root of the next image: kept keyspace baselines, a fresh baseline at
    /// the adopted cut for a newly enabled callback, none for a disabled one.
    pub(super) fn next(
        adopted: Option<&Self>,
        adopted_cut: Option<MutationVersion>,
        capture: &PolicyCapture,
    ) -> Self {
        let adopted_bytes = adopted.map_or(0, |root| root.covered_bytes);
        let keyspaces = capture
            .policy
            .keyspaces
            .iter()
            .map(|entry| {
                adopted
                    .and_then(|root| {
                        root.keyspaces
                            .iter()
                            .find(|baseline| baseline.keyspace == entry.keyspace)
                            .copied()
                    })
                    .unwrap_or(KeyspaceBaseline {
                        keyspace: entry.keyspace,
                        cut: adopted_cut,
                        bytes: adopted_bytes,
                    })
            })
            .collect();
        Self {
            covered_bytes: capture.covered_bytes,
            last_periodic_bytes: if capture.periodic {
                capture.covered_bytes
            } else {
                adopted.map_or(0, |root| root.last_periodic_bytes)
            },
            keyspaces,
        }
    }
}

/// Policy progress of one enabled cell: the last evaluation cut `F` (`None`
/// = before all), the covered bytes at that evaluation and the distinct dirty
/// keys at the image's cut. A summary, never a key ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CellPolicy {
    pub last_cut: Option<MutationVersion>,
    pub last_bytes: u64,
    pub dirty: u32,
}

/// Per-image compactor counters (A0 interface, COMPACTOR-PARITY.md §4).
/// Reasons overlap: one evaluated cell can count under several.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactorStats {
    pub pressure_cells: u64,
    pub stale_cells: u64,
    pub force_cells: u64,
    /// Cells selected because an accepted prune plan changed them.
    pub relocation_cells: u64,
    /// Cells whose callback ran exactly once in this image.
    pub evaluated_cells: u64,
    /// Evaluated cells whose callback retained every key.
    pub keep_all_cells: u64,
    /// Selected cells deferred because their heads exceed the scratch limit
    /// or their worst-case marks do not fit the mark budget.
    pub capacity_cells: u64,
    /// Changed callback cells whose distinct dirty keys were recounted.
    pub counted_cells: u64,
    pub keys_visited: u64,
    pub keys_marked: u64,
    pub callback_ns: u64,
    pub scan_ns: u64,
    pub scratch_peak_bytes: u64,
}

impl CompactorStats {
    /// True when a callback ran in this image: the A0 `COMPACTOR` trigger bit.
    pub fn evaluated(&self) -> bool {
        self.evaluated_cells != 0
    }
}

/// Newest reference of one merged row within `cut`; versions ascend.
fn newest_in_cut(
    base: &[u8],
    overlay: &[ValueRecordReference],
    cut: &IndexReadCut,
) -> Option<ValueRecordReference> {
    let reference = |n: usize| {
        decode_reference(&base[n * REFERENCE_BYTES..(n + 1) * REFERENCE_BYTES])
            .expect("validated immutable reference")
    };
    let count = base.len() / REFERENCE_BYTES;
    let base_end = lower_bound_by(count, |n| cut.contains(&reference(n)));
    let base = (base_end > 0).then(|| reference(base_end - 1));
    let overlay_end = overlay.partition_point(|value| cut.contains(value));
    let overlay = (overlay_end > 0).then(|| overlay[overlay_end - 1]);
    newest(base, overlay)
}

/// True when the frozen cell differs from its stored base at `cut`: a sealed
/// reference within the cut, or a `DropCells` mask newer than the base.
fn changed_since_base(cell: &FrozenCell, cut: &IndexReadCut) -> bool {
    let overlay = cell.overlay.values().any(|values| {
        values
            .first()
            .is_some_and(|reference| cut.contains(reference))
    });
    let mask = cell.masks.versions.last().is_some_and(|mask| {
        cell.base
            .as_ref()
            .is_none_or(|base| !base.cut.contains_mutation(*mask))
    });
    overlay || mask
}

/// Distinct keys whose newest user reference at the cut (a Put or a real
/// Delete) is newer than `last_cut` and above the newest visible mask. A
/// `DropCells` mask therefore cancels the debt below it and adds none.
fn count_dirty(cell: &FrozenCell, cut: &IndexReadCut, last_cut: Option<MutationVersion>) -> usize {
    let mask = cell.masks.versions.last().copied();
    Rows::new(cell)
        .filter(|(_, base, overlay)| {
            newest_in_cut(base, overlay, cut).is_some_and(|newest| {
                let version = newest.mutation_version();
                mask.is_none_or(|mask| version > mask) && last_cut.is_none_or(|last| version > last)
            })
        })
        .count()
}

/// Upper bound of eligible heads: one per base or overlay row.
fn head_bound(cell: &FrozenCell) -> usize {
    cell.base
        .as_ref()
        .map_or(0, |base| base.count)
        .saturating_add(cell.overlay.len())
}

/// The selectable Put head of every key at the cut, in key order: newest
/// reference within the cut, above the newest mask, not covered by a mark
/// (adopted, folded or added by a prune plan in this image). Keys borrow the
/// immutable base block or the frozen overlay; nothing is copied.
fn collect_heads(cell: &FrozenCell, cut: &IndexReadCut, heads: &mut Vec<(Bytes, MutationVersion)>) {
    for (key, base, overlay) in Rows::new(cell) {
        let Some(newest) = newest_in_cut(base, overlay, cut) else {
            continue;
        };
        let version = newest.mutation_version();
        if newest.descriptor.kind != RecordKind::Put
            || !prune_marks::selectable_at_capture(cell, key, version, cut)
        {
            continue;
        }
        let handle = match (&cell.base, base.is_empty()) {
            (Some(block), false) => block.bytes.slice_to_bytes(key),
            _ => cell
                .overlay
                .get_key_value(key)
                .map(|(stored, _)| stored.clone())
                .expect("overlay row key"),
        };
        heads.push((handle, version));
    }
}

/// Evaluate and select one frozen cell, once, before it is encoded and after
/// the substrate folded its marks and applied any prune plan. Sets the cell's
/// next marks and policy progress.
///
/// # Errors
/// A callback that returns more keys than it was given breaks its contract
/// and fails the checkpoint (fail-stop); a callback panic unwinds to the
/// checkpoint worker's guard.
pub(super) fn prepare_cell(
    cell: &mut FrozenCell,
    capture: Option<&PolicyCapture>,
    install_epoch: u64,
    cut: &IndexReadCut,
    scratch_limit: usize,
    budget: &mut dyn MarkBudget,
    stats: &mut CompactorStats,
) -> Result<(), NativeIndexError> {
    let Some((progress, jitter)) = cell.policy else {
        return Ok(());
    };
    // Without policy inputs (a non-serving image) progress is carried and
    // only the dirty count follows the new rows.
    let enabled = match capture {
        None => None,
        Some(capture) => match capture.policy.keyspace(cell.identity.0) {
            Some(keyspace) => Some((capture, keyspace)),
            None => {
                // Disabled since the adopted image: drop the optional
                // progress. Adopted suppression stays.
                cell.policy = None;
                return Ok(());
            }
        },
    };
    let started = Instant::now();
    let dirty = if changed_since_base(cell, cut) {
        stats.counted_cells += 1;
        count_dirty(cell, cut, progress.last_cut)
    } else if cell.base.is_none() {
        0
    } else {
        progress.dirty as usize
    };
    stats.scan_ns += elapsed_ns(started);
    let dirty_count = u32::try_from(dirty).map_err(|_| NativeIndexError::Capacity)?;
    let carried = (
        CellPolicy {
            dirty: dirty_count,
            ..progress
        },
        jitter,
    );
    let Some((capture, keyspace)) = enabled else {
        cell.policy = Some(carried);
        return Ok(());
    };
    let pressure =
        keyspace.pressure && dirty.saturating_sub(jitter as usize) > keyspace.max_dirty_keys;
    let stale = capture.periodic
        && dirty != 0
        && progress.last_bytes
            <= capture
                .covered_bytes
                .saturating_sub(capture.policy.cadence.snapshot_unload_threshold);
    let force = capture.force && dirty != 0;
    // Design §2 step 6(d): an accepted prune plan (the only source of new
    // marks before this step) selects its cell, even a clean one.
    let relocation = cell.marks_changed;
    if capture.recovery_baseline || !(pressure || stale || force || relocation) {
        cell.policy = Some(carried);
        return Ok(());
    }
    stats.pressure_cells += u64::from(pressure);
    stats.stale_cells += u64::from(stale);
    stats.force_cells += u64::from(force);
    stats.relocation_cells += u64::from(relocation);
    let charge = head_bound(cell).saturating_mul(HEAD_CHARGE);
    if charge > scratch_limit {
        // Never call back on a prefix of a cell: carry the debt.
        stats.capacity_cells += 1;
        cell.policy = Some(carried);
        return Ok(());
    }
    let cut_version =
        retention::position(cut).ok_or(NativeIndexError::Invalid("compactor cut is empty"))?;
    let started = Instant::now();
    let mut heads = Vec::with_capacity(head_bound(cell));
    collect_heads(cell, cut, &mut heads);
    stats.scan_ns += elapsed_ns(started);
    // Reserve the resident charge of the worst case, every head rejected,
    // before the callback runs. A refused reservation carries the debt like
    // an oversized cell: no partial suppression is ever adopted.
    let worst = heads
        .iter()
        .map(|(key, _)| prune_marks::key_charge(key.len()))
        .sum::<usize>();
    if worst != 0 && !budget.reserve(worst) {
        stats.capacity_cells += 1;
        cell.policy = Some(carried);
        return Ok(());
    }
    stats.scratch_peak_bytes = stats
        .scratch_peak_bytes
        .max((heads.capacity() * HEAD_CHARGE) as u64);
    stats.keys_visited += heads.len() as u64;
    let started = Instant::now();
    let retain = {
        let mut keys = heads.iter().map(|(key, _)| key);
        let compactor: &Compactor = &keyspace.compactor;
        compactor(&mut keys)
    };
    stats.callback_ns += elapsed_ns(started);
    if retain.len() > heads.len() {
        return Err(NativeIndexError::Invalid(
            "compactor returned more keys than its input",
        ));
    }
    let rejected: Vec<PruneMark> = heads
        .into_iter()
        .filter(|(key, _)| !retain.contains(key))
        .map(|(key, version)| PruneMark {
            key,
            visible_from: cut_version,
            suppress_through: version,
            epoch: install_epoch,
        })
        .collect();
    drop(retain);
    // Keep only the charge of the marks this cell adds.
    budget.release(worst - rejected.iter().map(prune_marks::mark_charge).sum::<usize>());
    stats.evaluated_cells += 1;
    stats.keys_marked += rejected.len() as u64;
    if rejected.is_empty() {
        stats.keep_all_cells += 1;
    }
    prune_marks::adopt_new_marks(cell, rejected);
    cell.policy = Some((
        CellPolicy {
            last_cut: Some(cut_version),
            last_bytes: capture.covered_bytes,
            dirty: 0,
        },
        jitter,
    ));
    Ok(())
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Root policy block, after the `DropCells` mask section, present only when a
/// callback keyspace is enabled (a root without one ends at the mask
/// section): `u8 tag = 1`, `be_u64 covered_bytes, be_u64
/// last_periodic_bytes, be_u32 keyspace_count` and per ascending keyspace
/// `ks:u8, cut (21 bytes), be_u64 bytes`; then `be_u32 override_count` and
/// per stored cell in directory order with non-default progress `ks:u8,
/// native CellId, last_cut (21 bytes), be_u64 last_bytes, be_u32 dirty`.
/// Progress stays in the root, so a keep-all evaluation of a clean cell
/// reuses the cell's frame; marks are in the cell frames.
pub(super) fn encode_policy(
    root: Option<&PolicyRoot>,
    cells: &[FrozenCell],
    payload: &mut Vec<u8>,
) -> Result<(), NativeIndexError> {
    let Some(root) = root else {
        return Ok(());
    };
    payload.push(1);
    payload.extend_from_slice(&root.covered_bytes.to_be_bytes());
    payload.extend_from_slice(&root.last_periodic_bytes.to_be_bytes());
    payload.extend_from_slice(
        &u32::try_from(root.keyspaces.len())
            .map_err(|_| NativeIndexError::Capacity)?
            .to_be_bytes(),
    );
    for baseline in &root.keyspaces {
        payload.push(baseline.keyspace.0);
        encode_cut(&cut_of(baseline.cut), payload);
        payload.extend_from_slice(&baseline.bytes.to_be_bytes());
    }
    let overrides: Vec<(&FrozenCell, CellPolicy)> = cells
        .iter()
        .filter(|cell| cell.base.is_some())
        .filter_map(|cell| {
            let (progress, _) = cell.policy?;
            (Some(progress) != root.baseline(cell.identity.0)).then_some((cell, progress))
        })
        .collect();
    payload.extend_from_slice(
        &u32::try_from(overrides.len())
            .map_err(|_| NativeIndexError::Capacity)?
            .to_be_bytes(),
    );
    let mut identity = BytesMut::new();
    for (cell, progress) in overrides {
        payload.push(cell.identity.0.0);
        identity.clear();
        cell.identity.1.write_into_bytes(&mut identity);
        payload.extend_from_slice(&identity);
        encode_cut(&cut_of(progress.last_cut), payload);
        payload.extend_from_slice(&progress.last_bytes.to_be_bytes());
        payload.extend_from_slice(&progress.dirty.to_be_bytes());
        if payload.len() > MAX_IMAGE_BYTES - 8 {
            return Err(NativeIndexError::Capacity);
        }
    }
    Ok(())
}

fn cut_of(version: Option<MutationVersion>) -> IndexReadCut {
    version.map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
}

/// Bounded decoding of the policy block. Keyspaces ascend, exist and are not
/// destroyed; cuts lie within the root cut; byte baselines do not exceed the
/// covered bytes; overrides ascend, name stored cells of enabled keyspaces and
/// count at most the cell's stored keys. Never calls a callback. The caller
/// decodes it only when root bytes remain after the mask section.
pub(super) fn decode_policy(
    cursor: &mut Cursor<'_>,
    index: &mut NativeIndex,
    cut: &IndexReadCut,
) -> Result<PolicyRoot, NativeIndexError> {
    if cursor.u8()? != 1 {
        return Err(NativeIndexError::Invalid("policy block tag"));
    }
    let covered_bytes = cursor.u64()?;
    let last_periodic_bytes = cursor.u64()?;
    if last_periodic_bytes > covered_bytes {
        return Err(NativeIndexError::Invalid("policy periodic bytes"));
    }
    let bounded_cut = |decoded: IndexReadCut| -> Result<Option<MutationVersion>, NativeIndexError> {
        let version = retention::position(&decoded);
        if version.is_some_and(|version| !cut.contains_mutation(version)) {
            return Err(NativeIndexError::Invalid("policy cut beyond the root cut"));
        }
        Ok(version)
    };
    let count = cursor.u32()? as usize;
    // A baseline is 30 bytes.
    if count > 256 || count > cursor.remaining() / 30 {
        return Err(NativeIndexError::Invalid("policy keyspace count"));
    }
    let mut keyspaces: Vec<KeyspaceBaseline> = Vec::with_capacity(count);
    for _ in 0..count {
        let keyspace = KeySpace(cursor.u8()?);
        index.descriptor(keyspace)?;
        if keyspaces
            .last()
            .is_some_and(|prior| prior.keyspace.0 >= keyspace.0)
        {
            return Err(NativeIndexError::Invalid("unsorted policy keyspaces"));
        }
        let baseline_cut = bounded_cut(decode_cut(cursor)?)?;
        let bytes = cursor.u64()?;
        if bytes > covered_bytes {
            return Err(NativeIndexError::Invalid("policy baseline bytes"));
        }
        keyspaces.push(KeyspaceBaseline {
            keyspace,
            cut: baseline_cut,
            bytes,
        });
    }
    let root = PolicyRoot {
        covered_bytes,
        last_periodic_bytes,
        keyspaces,
    };
    let overrides = cursor.u32()? as usize;
    // An override is at least 1 + 9 + 21 + 8 + 4 bytes.
    if overrides > cursor.remaining() / 43 {
        return Err(NativeIndexError::Invalid("policy override count"));
    }
    let mut previous: Option<(u8, CellId)> = None;
    for _ in 0..overrides {
        let keyspace = KeySpace(cursor.u8()?);
        let cell_id = decode_cell(cursor)?;
        if root.baseline(keyspace).is_none() {
            return Err(NativeIndexError::Invalid(
                "policy override of a disabled keyspace",
            ));
        }
        let identity = (keyspace, cell_id);
        let key = (keyspace.0, identity.1.clone());
        let last_cut = bounded_cut(decode_cut(cursor)?)?;
        let last_bytes = cursor.u64()?;
        let dirty = cursor.u32()?;
        if last_bytes > covered_bytes {
            return Err(NativeIndexError::Invalid("policy cell bytes"));
        }
        let shard = index.shard_index(keyspace, &identity.1);
        let cell =
            index.shards[shard]
                .get_mut()
                .get_mut(&identity)
                .ok_or(NativeIndexError::Invalid(
                    "policy override names an absent cell",
                ))?;
        // The cell exists in `keyspace`; see `decode_masks` for the order.
        if previous.as_ref().is_some_and(|prior| *prior >= key) {
            return Err(NativeIndexError::Invalid("unsorted policy overrides"));
        }
        if cell
            .base
            .as_ref()
            .is_none_or(|base| dirty as usize > base.count)
        {
            return Err(NativeIndexError::Invalid("policy dirty count"));
        }
        cell.policy = Some(CellPolicy {
            last_cut,
            last_bytes,
            dirty,
        });
        previous = Some(key);
    }
    Ok(root)
}
