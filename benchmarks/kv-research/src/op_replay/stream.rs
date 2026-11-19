//! Replay of one database stream into both arms, with every recorded result
//! checked on each arm at its recorded point.

use super::arm::{self, Arm, FilterEvidence, KeyspaceDigest, Kind, LeasedOwner};
use super::trace::{
    DATABASE, FilterDecision, Frame, Member, Op, OpenSpec, Outcome, Reader, RelocationEvidence,
};
use super::{RowHasher, row_hash};
use crate::backend::BackendResult;
use crate::backends::replay::ReplayBackend;
use crate::mainnet_trace::Row;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::path::Path;
use tidehunter::distributed::sui;
use tidehunter::minibytes::Bytes;

/// Divergences kept per stream; the total is always counted.
const DIVERGENCE_SAMPLE: usize = 64;
/// Keys listed per removed-set difference.
const KEY_SAMPLE: usize = 16;

/// A put-only batch as shared replay rows (table = canonical keyspace ID);
/// `None` if any member is a delete, which `ReplayBackend::commit` cannot carry.
fn put_rows(members: &[Member]) -> BackendResult<Option<Vec<Row>>> {
    let mut rows = Vec::with_capacity(members.len());
    for member in members {
        let Member::Put(ks, key, value) = member else {
            return Ok(None);
        };
        let key = Bytes::from(key.clone());
        rows.push(Row {
            table: u16::try_from(*ks)?,
            tx: 0,
            backend_key: key.clone(),
            key,
            backend_owner: 0,
            value: Bytes::from(value.clone()),
        });
    }
    Ok(Some(rows))
}

/// One engine commit of the original ordered members, deletes included.
fn mixed_batch(arm: &Arm, members: &[Member]) -> BackendResult<()> {
    let db = arm.db()?;
    let mut batch = db.write_batch();
    for member in members {
        match member {
            Member::Put(ks, key, value) => {
                batch.write(arm::keyspace(db, *ks)?, key.clone(), value.clone());
            }
            Member::Delete(ks, key) => batch.delete(arm::keyspace(db, *ks)?, key.clone()),
        }
    }
    batch.commit().map_err(|error| format!("{error:?}").into())
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamStatus {
    /// Every recorded result, fence and digest matched on both arms.
    Passed,
    /// At least one recorded result or final state differs.
    Diverged,
    /// Replay stopped at an operation the leased backend does not implement yet.
    Gap,
    /// Invalid trace or replay infrastructure failure.
    Error,
}

#[derive(Debug, Serialize)]
pub struct Divergence {
    pub seq: u64,
    pub op: &'static str,
    pub arm: &'static str,
    pub detail: String,
}

/// A known difference between the leased backend and native, by kind.
#[derive(Debug, Default, Serialize)]
pub struct Gap {
    pub count: u64,
    pub first_seq: u64,
    pub detail: &'static str,
}

#[derive(Debug, Serialize)]
pub struct ArmPass {
    pub filters: FilterEvidence,
    pub removed: usize,
    pub missing_removals: Vec<String>,
    pub extra_removals: Vec<String>,
    pub removed_keys_still_readable: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct Fence {
    pub seq: u64,
    pub recorded_removed: usize,
    pub recorded_decisions: usize,
    /// Version 2 native evidence, reported but not yet compared (needs R1).
    pub recorded_evidence: Option<RelocationEvidence>,
    pub native: Option<ArmPass>,
    pub leased: Option<ArmPass>,
}

#[derive(Debug, Serialize)]
pub struct StreamReport {
    pub database: String,
    pub file: String,
    pub frames: u64,
    pub replayed_frames: u64,
    pub status: StreamStatus,
    pub error: Option<String>,
    pub divergence_count: u64,
    pub divergences: Vec<Divergence>,
    pub gaps: BTreeMap<&'static str, Gap>,
    pub fences: Vec<Fence>,
    pub op_counts: BTreeMap<&'static str, u64>,
    /// Native digests at the end of the stream (both arms matched if `Passed`).
    pub final_digests: Option<Vec<KeyspaceDigest>>,
    /// Native digests after the closing reopen of both arms.
    pub reopen_digests: Option<Vec<KeyspaceDigest>>,
}

enum Flow {
    Continue,
    /// A state-changing divergence or a gap: later results are not comparable.
    Stop,
}

struct Iter {
    raw: sui::DbIterator,
    keyspace: u32,
    count: u64,
    hasher: RowHasher,
    snapshot: bool,
    /// A live iterator with a write recorded after its begin: the leased
    /// iterator reads a cut fixed at creation, native may observe the write.
    written_since_begin: bool,
}

fn pair_written(iterators: &BTreeMap<u64, [Iter; 2]>, id: u64) -> bool {
    iterators
        .get(&id)
        .is_some_and(|pair| pair[0].written_since_begin)
}

pub(super) struct StreamReplay {
    arms: [Arm; 2],
    owner: Option<LeasedOwner>,
    /// Trace format version from `index.json`; every stream header must match.
    version: u32,
    iterators: BTreeMap<u64, [Iter; 2]>,
    fence: Option<u64>,
    last_open: Option<OpenSpec>,
    dropped: bool,
    /// Native digests at the last recorded Close (both arms matched or a
    /// divergence was recorded); the oracle for the next reopen.
    closed_digests: Option<Vec<KeyspaceDigest>>,
    report: StreamReport,
}

impl StreamReplay {
    pub(super) fn new(
        database: String,
        file: String,
        frames: u64,
        version: u32,
        dir: &Path,
    ) -> Self {
        Self {
            arms: [
                Arm::new(Kind::Native, dir.join("native")),
                Arm::new(Kind::Leased, dir.join("leased").join("coordinator")),
            ],
            owner: None,
            version,
            iterators: BTreeMap::new(),
            fence: None,
            last_open: None,
            dropped: false,
            closed_digests: None,
            report: StreamReport {
                database,
                file,
                frames,
                replayed_frames: 0,
                status: StreamStatus::Passed,
                error: None,
                divergence_count: 0,
                divergences: Vec::new(),
                gaps: BTreeMap::new(),
                fences: Vec::new(),
                op_counts: BTreeMap::new(),
                final_digests: None,
                reopen_digests: None,
            },
        }
    }

    /// Replay the stream file. Trace and engine errors end the stream with an
    /// `Error` status instead of returning.
    pub(super) fn run(mut self, path: &Path) -> StreamReport {
        let leased_dir = self.arms[1].path.parent().map(Path::to_path_buf);
        if let Some(dir) = leased_dir
            && let Err(error) = std::fs::create_dir_all(dir)
        {
            return self.fail(error.into());
        }
        let outcome = self
            .replay(path)
            .and_then(|stopped| if stopped { Ok(()) } else { self.finish() });
        if let Err(error) = outcome {
            return self.fail(error);
        }
        let shutdown = self.shutdown();
        if let Err(error) = shutdown {
            return self.fail(error);
        }
        self.report.status = if self.report.divergence_count != 0 {
            StreamStatus::Diverged
        } else if self.report.status == StreamStatus::Gap {
            StreamStatus::Gap
        } else {
            StreamStatus::Passed
        };
        self.report
    }

    fn fail(mut self, error: Box<dyn std::error::Error + Send + Sync>) -> StreamReport {
        self.report.status = StreamStatus::Error;
        self.report.error = Some(format!(
            "after {} frames: {error}",
            self.report.replayed_frames
        ));
        // Best effort: release databases and the owner before the caller reports.
        self.iterators.clear();
        for arm in &mut self.arms {
            if arm.db.is_some() {
                let _ = arm.close();
            }
        }
        if let Some(owner) = self.owner.take() {
            let _ = owner.stop();
        }
        self.report
    }

    fn shutdown(&mut self) -> BackendResult<()> {
        // A stopped replay can leave iterators that hold database references.
        self.iterators.clear();
        for arm in &mut self.arms {
            if arm.db.is_some() {
                arm.close()?;
            }
        }
        self.owner.take().map_or(Ok(()), LeasedOwner::stop)
    }

    /// Returns `true` if a divergence or gap stopped the replay early.
    fn replay(&mut self, path: &Path) -> BackendResult<bool> {
        let mut reader = Reader::open(path, self.version)?;
        for expected in 0..self.report.frames {
            let frame = reader.next_frame()?.ok_or_else(|| {
                format!("stream ends at frame {expected} of {}", self.report.frames)
            })?;
            *self.report.op_counts.entry(frame.op.name()).or_default() += 1;
            let flow = self.apply(frame)?;
            self.report.replayed_frames = expected + 1;
            if matches!(flow, Flow::Stop) {
                return Ok(true);
            }
        }
        if reader.next_frame()?.is_some() {
            return Err("frames after the indexed end of the stream".into());
        }
        if self.fence.is_some() || !self.iterators.is_empty() {
            return Err("stream ends inside a relocation fence or with live iterators".into());
        }
        Ok(false)
    }

    fn diverge(&mut self, seq: u64, op: &'static str, arm: &'static str, detail: String) {
        self.report.divergence_count += 1;
        if self.report.divergences.len() < DIVERGENCE_SAMPLE {
            self.report.divergences.push(Divergence {
                seq,
                op,
                arm,
                detail,
            });
        }
    }

    fn gap(&mut self, seq: u64, kind: &'static str, detail: &'static str) {
        let gap = self.report.gaps.entry(kind).or_insert(Gap {
            count: 0,
            first_seq: seq,
            detail,
        });
        gap.count += 1;
    }

    /// Compare one recorded result with each arm's result. Returns whether any arm differed.
    fn check<T: PartialEq + Debug>(
        &mut self,
        seq: u64,
        op: &'static str,
        expected: &Outcome<T>,
        actual: [Result<T, String>; 2],
    ) -> bool {
        let mut differed = false;
        for (kind, actual) in [Kind::Native, Kind::Leased].into_iter().zip(actual) {
            let detail = match (expected, actual) {
                (Outcome::Ok(expected), Ok(actual)) if *expected == actual => continue,
                (Outcome::Err(_), Err(_)) => continue,
                (Outcome::Ok(expected), Ok(actual)) => {
                    format!("recorded {expected:?}, replay returned {actual:?}")
                }
                (Outcome::Ok(expected), Err(error)) => {
                    format!("recorded {expected:?}, replay failed: {error}")
                }
                (Outcome::Err(error), Ok(actual)) => {
                    format!("recorded error {error}, replay returned {actual:?}")
                }
            };
            differed = true;
            self.diverge(seq, op, kind.name(), detail);
        }
        differed
    }

    fn per_arm<T>(
        &self,
        keyspace: u32,
        mut call: impl FnMut(
            &std::sync::Arc<sui::Db>,
            tidehunter::key_shape::KeySpace,
        ) -> sui::Result<T>,
    ) -> BackendResult<[Result<T, String>; 2]> {
        let mut out = Vec::with_capacity(2);
        for arm in &self.arms {
            let db = arm.db()?;
            let ks = arm::keyspace(db, keyspace)?;
            out.push(call(db, ks).map_err(|error| format!("{error:?}")));
        }
        out.try_into().map_err(|_| "two arms".into())
    }

    fn open_both(&mut self, seq: u64, spec: OpenSpec, create: bool) -> BackendResult<()> {
        if self.dropped {
            return Err("open after drop".into());
        }
        for arm in &mut self.arms {
            arm.open(&spec, create, &mut self.owner, None)?;
        }
        self.last_open = Some(spec);
        if create {
            self.closed_digests = None;
        } else if let Some(before) = self.closed_digests.take() {
            self.reopen_check(seq, "open", &before)?;
        }
        Ok(())
    }

    /// Both arms after a reopen must equal each other and the state at close.
    fn reopen_check(
        &mut self,
        seq: u64,
        op: &'static str,
        before: &[KeyspaceDigest],
    ) -> BackendResult<Vec<KeyspaceDigest>> {
        let after = self.compare_digests(seq, op)?;
        if after != before {
            let detail =
                format!("state changed across reopen: closed={before:?} reopened={after:?}");
            self.diverge(seq, op, "native", detail);
        }
        Ok(after)
    }

    #[allow(clippy::too_many_lines, reason = "one arm per recorded operation kind")]
    fn apply(&mut self, frame: Frame) -> BackendResult<Flow> {
        let Frame {
            seq,
            keyspace: ks,
            op,
        } = frame;
        let name = op.name();
        if self.fence.is_some() && !matches!(op, Op::RelocationComplete { .. }) {
            return Err(format!("{name} inside a relocation fence").into());
        }
        if op.database_wide() && ks != DATABASE {
            return Err(format!("database-wide {name} carries keyspace {ks}").into());
        }
        match op {
            Op::Create(spec) => self.open_both(seq, spec, true)?,
            Op::Open(spec) => self.open_both(seq, spec, false)?,
            Op::Close => {
                if !self.iterators.is_empty() {
                    return Err("close with live iterators".into());
                }
                self.closed_digests = Some(self.compare_digests(seq, name)?);
                for arm in &mut self.arms {
                    arm.close()?;
                }
            }
            Op::Drop { result } => {
                let actual = self.arms.each(Arm::drop_db);
                self.dropped = true;
                if self.check(seq, name, &result, actual) {
                    return Ok(Flow::Stop);
                }
            }
            Op::Put { key, value, result } => {
                let actual =
                    self.per_arm(ks, |db, ks| db.insert(ks, key.clone(), value.clone()))?;
                return Ok(self.write_flow(seq, name, &result, actual));
            }
            Op::Delete { key, result } => {
                let actual = self.per_arm(ks, |db, ks| db.remove(ks, key.clone()))?;
                return Ok(self.write_flow(seq, name, &result, actual));
            }
            Op::Batch { members, result } => {
                let actual = match put_rows(&members)? {
                    // The shared node-DB batch contract (`ReplayBackend::commit`).
                    Some(rows) => self.arms.each(|arm| ReplayBackend::commit(arm, 0, &rows)),
                    None => self.arms.each(|arm| mixed_batch(arm, &members)),
                };
                return Ok(self.write_flow(seq, name, &result, actual));
            }
            Op::Get { key, result } => {
                let table = u16::try_from(ks)?;
                let actual = [&self.arms[0], &self.arms[1]].map(|arm| {
                    ReplayBackend::get(arm, table, &key)
                        .map(|value| value.map(|bytes| bytes.to_vec()))
                        .map_err(|error| error.to_string())
                });
                self.check(seq, name, &result, actual);
            }
            Op::Exists { key, result } => {
                let actual = self.per_arm(ks, |db, ks| db.exists(ks, &key))?;
                self.check(seq, name, &result, actual);
            }
            Op::MultiGet { keys, result } => {
                let actual = self.per_arm(ks, |db, ks| Ok(db.multi_get(ks, &keys)))?;
                for (kind, actual) in [Kind::Native, Kind::Leased].into_iter().zip(actual) {
                    let actual = actual?;
                    if actual.len() != result.len() {
                        let detail = format!("{} results for {} keys", actual.len(), result.len());
                        self.diverge(seq, name, kind.name(), detail);
                        continue;
                    }
                    for (position, (expected, actual)) in result.iter().zip(actual).enumerate() {
                        let actual = actual
                            .map(|value| value.map(|bytes| bytes.to_vec()))
                            .map_err(|error| format!("{error:?}"));
                        let detail = match (expected, actual) {
                            (Outcome::Ok(expected), Ok(actual)) if *expected == actual => continue,
                            (Outcome::Err(_), Err(_)) => continue,
                            (expected, actual) => {
                                format!("key {position}: recorded {expected:?}, replay {actual:?}")
                            }
                        };
                        self.diverge(seq, name, kind.name(), detail);
                    }
                }
            }
            Op::MultiExists { keys, result } => {
                let actual = self.per_arm(ks, |db, ks| {
                    db.multi_exists(ks, &keys)
                        .into_iter()
                        .collect::<sui::Result<Vec<_>>>()
                })?;
                self.check(seq, name, &result, actual);
            }
            Op::DropCells { from, to, result } => {
                let actual = self.per_arm(ks, |db, ks| db.drop_cells_in_range(ks, &from, &to))?;
                return Ok(self.write_flow(seq, name, &result, actual));
            }
            Op::Flush { result } => {
                let actual = self.arms.each(Arm::typed_store_flush);
                self.check(seq, name, &result, actual);
            }
            Op::Checkpoint { result } => {
                // Native typed-store checkpoint copies nothing; the Sui pin's
                // typed-store returns an error on the remote backend instead.
                self.gap(seq, "typed_store_checkpoint", LEASED_CHECKPOINT);
                self.check(seq, name, &result, [Ok(()), Ok(())]);
            }
            Op::RebuildControl { result } => {
                let actual = self.arms.each(|arm| {
                    arm.db()?
                        .force_rebuild_control_region()
                        .map_err(|error| format!("{error:?}").into())
                });
                return Ok(self.write_flow(seq, name, &result, actual));
            }
            Op::IteratorBegin {
                id,
                lower,
                upper,
                reverse,
                snapshot,
            } => {
                if id != seq || self.iterators.contains_key(&id) {
                    return Err(format!("invalid iterator id {id} at sequence {seq}").into());
                }
                let mut opened = Vec::with_capacity(2);
                for arm in &self.arms {
                    let db = arm.db()?;
                    let handle = arm::keyspace(db, ks)?;
                    let mut raw = if snapshot {
                        db.checkpoint().iterator(handle)
                    } else {
                        db.iterator(handle)
                    };
                    if let Some(lower) = &lower {
                        raw.set_lower_bound(Bytes::from(lower.clone()));
                    }
                    if let Some(upper) = &upper {
                        raw.set_upper_bound(Bytes::from(upper.clone()));
                    }
                    if reverse {
                        raw.reverse();
                    }
                    opened.push(Iter {
                        raw,
                        keyspace: ks,
                        count: 0,
                        hasher: RowHasher::new(),
                        snapshot,
                        written_since_begin: false,
                    });
                }
                let pair = opened.try_into().map_err(|_| "two arms")?;
                self.iterators.insert(id, pair);
            }
            Op::IteratorStep { id, result } => {
                let pair = self
                    .iterators
                    .get_mut(&id)
                    .ok_or("step on an unknown iterator")?;
                let mut actual = Vec::with_capacity(2);
                for iter in pair.iter_mut() {
                    if iter.keyspace != ks {
                        return Err(format!("iterator {id} changed keyspace").into());
                    }
                    actual.push(match iter.raw.next() {
                        None => Ok(None),
                        Some(Ok((key, value))) => {
                            iter.count += 1;
                            iter.hasher.row(&key, &value);
                            Ok(Some(row_hash(&key, &value)))
                        }
                        Some(Err(error)) => Err(format!("{error:?}")),
                    });
                }
                let actual = actual.try_into().map_err(|_| "two arms")?;
                let label = self.iterator_label(seq, pair_written(&self.iterators, id), name);
                self.check(seq, label, &result, actual);
            }
            Op::IteratorEnd { id, count, sha256 } => {
                let pair = self
                    .iterators
                    .remove(&id)
                    .ok_or("end of an unknown iterator")?;
                let label = self.iterator_label(seq, pair[0].written_since_begin, name);
                for (kind, iter) in [Kind::Native, Kind::Leased].into_iter().zip(pair) {
                    if iter.keyspace != ks {
                        return Err(format!("iterator {id} changed keyspace").into());
                    }
                    let digest = iter.hasher.finish();
                    if iter.count != count || digest != sha256 {
                        let detail = format!(
                            "iterator {id}: recorded {count} rows sha256={}, replay {} rows sha256={}",
                            arm::hex(&sha256),
                            iter.count,
                            arm::hex(&digest)
                        );
                        self.diverge(seq, label, kind.name(), detail);
                    }
                }
            }
            Op::RelocationStart => {
                self.fence = Some(seq);
            }
            Op::RelocationComplete {
                decisions,
                removed,
                evidence,
            } => {
                let start = self
                    .fence
                    .take()
                    .ok_or("relocation completion without a start")?;
                if start + 1 != seq {
                    return Err("relocation fence frames are not adjacent".into());
                }
                return self.fence_pass(seq, &decisions, &removed, evidence);
            }
        }
        Ok(Flow::Continue)
    }

    /// A mismatching write status leaves the arms in different states.
    fn write_flow(
        &mut self,
        seq: u64,
        name: &'static str,
        expected: &Outcome<()>,
        actual: [Result<(), String>; 2],
    ) -> Flow {
        for iter in self.iterators.values_mut().flatten() {
            iter.written_since_begin |= !iter.snapshot;
        }
        if self.check(seq, name, expected, actual) {
            Flow::Stop
        } else {
            Flow::Continue
        }
    }

    /// Iterator results after an interleaved write form their own divergence class.
    fn iterator_label(&mut self, seq: u64, written: bool, name: &'static str) -> &'static str {
        if !written {
            return name;
        }
        self.gap(seq, "live_iterator_after_write", LIVE_ITERATOR);
        "live_iterator_after_write"
    }

    fn fence_pass(
        &mut self,
        seq: u64,
        decisions: &[FilterDecision],
        removed: &[(u32, Vec<u8>)],
        evidence: Option<RelocationEvidence>,
    ) -> BackendResult<Flow> {
        let recorded: arm::KeySet = removed.iter().cloned().collect();
        if recorded.len() != removed.len() {
            return Err("relocation completion lists a removed key twice".into());
        }
        let mut fence = Fence {
            seq,
            recorded_removed: recorded.len(),
            recorded_decisions: decisions.len(),
            recorded_evidence: evidence,
            native: None,
            leased: None,
        };
        let mut flow = Flow::Continue;
        for (index, kind) in [Kind::Native, Kind::Leased].into_iter().enumerate() {
            let db = std::sync::Arc::clone(self.arms[index].db()?);
            let before = arm::key_set(&db)?;
            let evidence = match self.arms[index].relocate(decisions) {
                Ok(evidence) => evidence,
                Err(error) => {
                    self.diverge(
                        seq,
                        "relocation_complete",
                        kind.name(),
                        format!("pass failed: {error}"),
                    );
                    flow = Flow::Stop;
                    continue;
                }
            };
            let after = arm::key_set(&db)?;
            let actual: arm::KeySet = before.difference(&after).cloned().collect();
            let sample = |keys: &mut dyn Iterator<Item = &(u32, Vec<u8>)>| {
                keys.take(KEY_SAMPLE)
                    .map(|(ks, key)| format!("{ks}/{}", arm::hex(key)))
                    .collect::<Vec<_>>()
            };
            let missing = sample(&mut recorded.difference(&actual));
            let extra = sample(&mut actual.difference(&recorded));
            let mut readable = Vec::new();
            for (ks, key) in &recorded {
                let handle = arm::keyspace(&db, *ks)?;
                if !matches!(db.get(handle, key), Ok(None)) && readable.len() < KEY_SAMPLE {
                    readable.push(format!("{ks}/{}", arm::hex(key)));
                }
            }
            if !missing.is_empty() || !extra.is_empty() || !readable.is_empty() {
                let detail = format!(
                    "removed set differs: missing={missing:?} extra={extra:?} still_readable={readable:?}"
                );
                self.diverge(seq, "relocation_complete", kind.name(), detail);
                flow = Flow::Stop;
            }
            if evidence.unrecorded_candidates != 0 {
                let detail = format!(
                    "{} filter candidates were not recorded by native: {:?}",
                    evidence.unrecorded_candidates, evidence.unrecorded_sample
                );
                self.diverge(seq, "relocation_complete", kind.name(), detail);
            }
            if !evidence.ordered_match {
                match kind {
                    Kind::Native => self.diverge(
                        seq,
                        "relocation_complete",
                        kind.name(),
                        "filter callback sequence differs from the recorded native order".into(),
                    ),
                    Kind::Leased => {
                        self.gap(seq, "relocation_callback_order", LEASED_CALLBACK_ORDER)
                    }
                }
            }
            let pass = ArmPass {
                filters: evidence,
                removed: actual.len(),
                missing_removals: missing,
                extra_removals: extra,
                removed_keys_still_readable: readable,
            };
            match kind {
                Kind::Native => fence.native = Some(pass),
                Kind::Leased => fence.leased = Some(pass),
            }
        }
        if fence.recorded_evidence.is_some() {
            self.gap(seq, "exact_r1_evidence", EXACT_R1_EVIDENCE);
        }
        self.report.fences.push(fence);
        Ok(flow)
    }

    /// Compare both arms' sorted digests; returns the native digests.
    fn compare_digests(
        &mut self,
        seq: u64,
        op: &'static str,
    ) -> BackendResult<Vec<KeyspaceDigest>> {
        let native = arm::digests(self.arms[0].db()?)?;
        let leased = arm::digests(self.arms[1].db()?)?;
        if native != leased {
            self.diverge(
                seq,
                op,
                "leased",
                format!("digests differ: native={native:?} leased={leased:?}"),
            );
        }
        Ok(native)
    }

    /// After the stream: the final state is the state at the last recorded
    /// Close (or, if the stream ends open, at its end, followed by a close).
    /// Reopen both arms with the last recorded open and compare against it.
    fn finish(&mut self) -> BackendResult<()> {
        if self.dropped {
            return Ok(());
        }
        let Some(spec) = self.last_open.clone() else {
            return Err("stream never opened its database".into());
        };
        let end = self.report.frames;
        let before = if self.arms[0].db.is_some() {
            let digests = self.compare_digests(end, "final")?;
            for arm in &mut self.arms {
                arm.close()?;
            }
            digests
        } else {
            self.closed_digests
                .take()
                .ok_or("closed stream without close digests")?
        };
        for arm in &mut self.arms {
            arm.open(&spec, false, &mut self.owner, None)?;
        }
        let after = self.reopen_check(end, "reopen", &before)?;
        self.report.final_digests = Some(before);
        self.report.reopen_digests = Some(after);
        // The replay contract's background-health boundary, as the mainnet replay.
        for arm in &self.arms {
            ReplayBackend::check_background_health(arm)
                .map_err(|error| format!("{}: {error}", arm.kind.name()))?;
        }
        Ok(())
    }
}

trait EachArm {
    fn each(&self, call: impl FnMut(&Arm) -> BackendResult<()>) -> [Result<(), String>; 2];
}

impl EachArm for [Arm; 2] {
    fn each(&self, mut call: impl FnMut(&Arm) -> BackendResult<()>) -> [Result<(), String>; 2] {
        [
            call(&self[0]).map_err(|e| e.to_string()),
            call(&self[1]).map_err(|e| e.to_string()),
        ]
    }
}

const LEASED_CHECKPOINT: &str = "typed-store Database::checkpoint returns Err on the sharded-wal backend at the Sui fork (typed-store/src/rocks/mod.rs:584-599); the replay applies the native no-op on both arms; roadmap Phase 3 maps it to Ok";
const LEASED_CALLBACK_ORDER: &str = "leased scans owner WAL order; the compared callback sequence differs from the recorded native pass";
const LIVE_ITERATOR: &str = "a recorded live iterator stepped after an interleaved write; leased live iterators read a protected cut taken at creation (distributed/sui.rs:1912-1919) while native ones may observe the write; divergences on such steps carry op `live_iterator_after_write` (Phase 8 iterator semantics in PARITY.md)";
const EXACT_R1_EVIDENCE: &str = "trace v2 records native gate, per-stream physical byte positions, copied survivors and completion witness. This replay compares removals and callback order; it reports but does not compare the additional native observations";
