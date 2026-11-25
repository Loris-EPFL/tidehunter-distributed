//! Bounded, independent specification of complete-batch authority.
//!
//! This deliberately does not call the implementation under test: exhaustive
//! state exploration should catch mistakes in the proposed protocol, rather than
//! restate its Rust methods. See `docs/distributed/MODEL.md` for scope and sources.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

const ALL: u8 = 0b11;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Frame {
    #[default]
    Empty,
    Torn,
    Complete,
    Durable,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Record {
    validated: bool,
    frame: Frame,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Phase {
    #[default]
    Live,
    Failed,
    Frozen,
    Recovered,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Snapshot {
    cut: u8,
    index: [u8; 4],
    // One registered stream per batch in this bounded model. A boundary of one
    // means its record is included in the cut; zero means replay that stream.
    replay_after: [u8; 2],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct History {
    records: [Record; 2],
    phase: Phase,
    available: u8,
    sealed: u8,
    // Seal fixes the complete-record inventory. It cannot subsequently grow.
    sealed_records: u8,
    holes: u8,
    published: u8,
    index: [u8; 4],
    acknowledged: u8,
    lost_reply: u8,
    snapshot: Snapshot,
}

impl Default for History {
    fn default() -> Self {
        Self {
            records: [Record::default(); 2],
            phase: Phase::Live,
            available: ALL,
            sealed: 0,
            sealed_records: 0,
            holes: 0,
            published: 0,
            index: [0; 4],
            acknowledged: 0,
            lost_reply: 0,
            snapshot: Snapshot::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Fault {
    None,
    AppendBeforeValidation,
    PublishBeforeDurable,
    PublishAcrossGap,
    PartialBatchPublication,
    TimeoutIsHole,
    RecoverResponsiveOnly,
    SnapshotAtMaximum,
    ReappendSealedHole,
    RetryBecomesNewVersion,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // Fields are retained in shortest-counterexample Debug traces.
enum Action {
    Validate(usize),
    EmitPartial(usize),
    Complete(usize),
    Persist(usize),
    Publish(usize),
    Acknowledge(usize),
    LoseReply(usize),
    Crash { lose_complete: u8, available: u8 },
    FreezeInventory,
    ReturnPartition(usize),
    SealPartition(usize),
    Recover,
    Snapshot,
    Unsafe(Fault),
}

fn write_batch(index: &mut [u8; 4], batch: usize, conflict: bool) {
    let start = if conflict { 0 } else { batch * 2 };
    index[start] = batch as u8 + 1;
    index[start + 1] = batch as u8 + 1;
}

fn reference_index(records: &[Record; 2], cut: u8, conflict: bool) -> [u8; 4] {
    let mut index = [0; 4];
    for (batch, record) in records.iter().enumerate().take(cut as usize) {
        if record.frame == Frame::Durable {
            write_batch(&mut index, batch, conflict);
        }
    }
    index
}

fn snapshot_boundaries(cut: u8) -> [u8; 2] {
    [u8::from(cut >= 1), u8::from(cut >= 2)]
}

impl History {
    fn check(self, conflict: bool) -> Result<(), &'static str> {
        for (batch, record) in self.records.iter().enumerate() {
            let bit = 1 << batch;
            if matches!(record.frame, Frame::Complete | Frame::Durable) && !record.validated {
                return Err("complete frame exists before validation/authorization");
            }
            if self.acknowledged & bit != 0 && record.frame != Frame::Durable {
                return Err("durable acknowledgement lost its complete authority");
            }
            if self.sealed & bit != 0
                && (record.frame == Frame::Durable) != (self.sealed_records & bit != 0)
            {
                return Err("a fenced stream changed after its sealed inventory");
            }
            if self.holes & bit != 0 && record.frame == Frame::Durable {
                return Err("terminal hole resurrected under the same identity");
            }
            if batch < self.published as usize
                && record.frame != Frame::Durable
                && self.holes & bit == 0
            {
                return Err("publication crossed an unresolved reservation");
            }
        }
        if self.holes != 0 && (self.phase != Phase::Recovered || self.sealed != ALL) {
            return Err("hole resolved without complete frozen and sealed inventory");
        }
        if self.phase == Phase::Recovered && self.sealed != ALL {
            return Err("recovery omitted an unavailable registered stream");
        }
        if self.index != reference_index(&self.records, self.published, conflict) {
            return Err("visible index is not one atomic logical prefix");
        }
        if self.snapshot.index != reference_index(&self.records, self.snapshot.cut, conflict)
            || self.snapshot.replay_after != snapshot_boundaries(self.snapshot.cut)
        {
            return Err("snapshot cut/index/per-stream replay boundaries disagree");
        }
        if self.phase == Phase::Live && self.snapshot.cut > self.published {
            return Err("snapshot advanced beyond the published logical prefix");
        }
        Ok(())
    }

    fn transitions(self, conflict: bool, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::with_capacity(24);
        let mut push = |action, next| {
            if next != self {
                out.push((action, next));
            }
        };
        for batch in 0..2 {
            let bit = 1 << batch;
            let record = self.records[batch];
            // Old requests can still complete after frontend failure, including
            // during partial fencing. The physical node's fence is decisive.
            if self.phase != Phase::Recovered && self.sealed & bit == 0 && self.available & bit != 0
            {
                let mut next = self;
                if !record.validated {
                    next.records[batch].validated = true;
                    push(Action::Validate(batch), next);
                } else {
                    match record.frame {
                        Frame::Empty => {
                            next.records[batch].frame = Frame::Torn;
                            push(Action::EmitPartial(batch), next);
                        }
                        Frame::Torn => {
                            next.records[batch].frame = Frame::Complete;
                            push(Action::Complete(batch), next);
                        }
                        Frame::Complete => {
                            next.records[batch].frame = Frame::Durable;
                            push(Action::Persist(batch), next);
                        }
                        Frame::Durable => {}
                    }
                }
            }
            if self.phase == Phase::Live {
                if self.published as usize == batch && record.frame == Frame::Durable {
                    let mut next = self;
                    write_batch(&mut next.index, batch, conflict);
                    next.published += 1;
                    push(Action::Publish(batch), next);
                }
                if batch < self.published as usize {
                    let mut next = self;
                    next.acknowledged |= bit;
                    push(Action::Acknowledge(batch), next);
                }
                if record.frame != Frame::Empty {
                    let mut next = self;
                    next.lost_reply |= bit;
                    push(Action::LoseReply(batch), next);
                }
            }
            if matches!(self.phase, Phase::Failed | Phase::Frozen) {
                let mut next = self;
                next.available |= bit;
                push(Action::ReturnPartition(batch), next);
                if self.phase == Phase::Frozen && self.available & bit != 0 {
                    let mut next = self;
                    next.sealed |= bit;
                    if matches!(record.frame, Frame::Complete | Frame::Durable) {
                        // A seal flushes complete surviving frames before returning
                        // its inventory. Complete but unacknowledged data is adopted.
                        next.records[batch].frame = Frame::Durable;
                        next.sealed_records |= bit;
                    }
                    push(Action::SealPartition(batch), next);
                }
            }
        }
        if self.phase == Phase::Live {
            let complete = self.records.iter().enumerate().fold(0, |mask, (i, r)| {
                mask | (u8::from(r.frame == Frame::Complete) << i)
            });
            // One frontend failure, optionally accompanied by kernel/power loss
            // of any non-fsynced complete frame, and independently unavailable
            // RF1 partitions. Durable media is never destroyed by this fault model.
            for lose_complete in 0..=ALL {
                if lose_complete & !complete != 0 {
                    continue;
                }
                for available in 0..=ALL {
                    let mut next = self;
                    next.phase = Phase::Failed;
                    next.available = available;
                    next.index = [0; 4];
                    next.published = 0;
                    for batch in 0..2 {
                        if lose_complete & (1 << batch) != 0 {
                            next.records[batch].frame = Frame::Torn;
                        }
                    }
                    push(
                        Action::Crash {
                            lose_complete,
                            available,
                        },
                        next,
                    );
                }
            }
        }
        if self.phase == Phase::Failed {
            let mut next = self;
            next.phase = Phase::Frozen;
            push(Action::FreezeInventory, next);
        }
        if self.phase == Phase::Frozen && self.sealed == ALL {
            push(Action::Recover, self.recover(conflict));
        }
        if matches!(self.phase, Phase::Live | Phase::Recovered) {
            let mut next = self;
            next.snapshot = Snapshot {
                cut: self.published,
                index: self.index,
                replay_after: snapshot_boundaries(self.published),
            };
            push(Action::Snapshot, next);
        }
        self.inject_fault(conflict, fault, &mut push);
        out
    }

    fn recover(self, conflict: bool) -> Self {
        let mut next = self;
        next.phase = Phase::Recovered;
        next.holes = ALL & !self.sealed_records;
        next.index = self.snapshot.index;
        for batch in self.snapshot.cut as usize..2 {
            if self.sealed_records & (1 << batch) != 0 {
                write_batch(&mut next.index, batch, conflict);
            }
        }
        next.published = 2;
        next
    }

    fn inject_fault(self, conflict: bool, fault: Fault, push: &mut impl FnMut(Action, Self)) {
        let mut next = self;
        match fault {
            Fault::None => return,
            Fault::AppendBeforeValidation => {
                if self.phase != Phase::Live || self.records[0].validated {
                    return;
                }
                next.records[0].frame = Frame::Complete;
            }
            Fault::PublishBeforeDurable => {
                if self.phase != Phase::Live || self.records[0].frame != Frame::Complete {
                    return;
                }
                write_batch(&mut next.index, 0, conflict);
                next.published = 1;
                next.acknowledged |= 1;
            }
            Fault::PublishAcrossGap => {
                if self.phase != Phase::Live || self.records[1].frame != Frame::Durable {
                    return;
                }
                write_batch(&mut next.index, 1, conflict);
                next.published = 2;
            }
            Fault::PartialBatchPublication => {
                if self.phase != Phase::Live || self.records[0].frame != Frame::Durable {
                    return;
                }
                next.index[0] = 1;
                next.published = 1;
            }
            Fault::TimeoutIsHole => {
                if self.phase != Phase::Live || self.lost_reply & 1 == 0 {
                    return;
                }
                next.holes |= 1;
            }
            Fault::RecoverResponsiveOnly => {
                if self.phase != Phase::Frozen || self.sealed != self.available {
                    return;
                }
                next = self.recover(conflict);
            }
            Fault::SnapshotAtMaximum => {
                if self.phase != Phase::Live || self.records[1].frame != Frame::Durable {
                    return;
                }
                next.snapshot = Snapshot {
                    cut: 2,
                    index: self.index,
                    replay_after: [1, 1],
                };
            }
            Fault::ReappendSealedHole => {
                if self.phase != Phase::Recovered || self.holes & 1 == 0 {
                    return;
                }
                next.records[0] = Record {
                    validated: true,
                    frame: Frame::Durable,
                };
            }
            Fault::RetryBecomesNewVersion => {
                if self.phase != Phase::Recovered || self.sealed_records != ALL || !conflict {
                    return;
                }
                // Retrying old BatchId with the new authority epoch must find its
                // old outcome, not make its old payload the newest mutation.
                write_batch(&mut next.index, 0, conflict);
            }
        }
        push(Action::Unsafe(fault), next);
    }
}

#[derive(Debug, Default)]
struct Coverage {
    states: usize,
    transitions: usize,
    recovered: usize,
    durable_after_hole: usize,
    lost_reply_recovered: usize,
    snapshot_recovered: usize,
    blocked_inventory: usize,
    sealed_late_requests: usize,
}

#[derive(Debug)]
struct Counterexample<A> {
    violation: &'static str,
    trace: Vec<A>,
}

// Keep predecessor links rather than a separate full trace per state. This is
// breadth-first exploration, so a failure has a shortest transition witness.
fn explore<S, A>(
    initial: S,
    mut transitions: impl FnMut(S) -> Vec<(A, S)>,
    mut check: impl FnMut(S) -> Result<(), &'static str>,
    mut visit: impl FnMut(S),
) -> Result<(usize, usize), Counterexample<A>>
where
    S: Copy + Eq + Hash,
    A: Copy + Debug,
{
    let mut known = HashMap::from([(initial, 0usize)]);
    let mut nodes: Vec<(S, Option<(usize, A)>)> = vec![(initial, None)];
    let mut cursor = 0;
    let mut edge_count = 0;
    while cursor < nodes.len() {
        let state = nodes[cursor].0;
        visit(state);
        for (action, next) in transitions(state) {
            edge_count += 1;
            if let Err(violation) = check(next) {
                let mut trace = vec![action];
                let mut predecessor = cursor;
                while let Some((parent, prior)) = nodes[predecessor].1 {
                    trace.push(prior);
                    predecessor = parent;
                }
                trace.reverse();
                return Err(Counterexample { violation, trace });
            }
            if let std::collections::hash_map::Entry::Vacant(entry) = known.entry(next) {
                entry.insert(nodes.len());
                nodes.push((next, Some((cursor, action))));
            }
        }
        cursor += 1;
    }
    Ok((nodes.len(), edge_count))
}

fn explore_history(conflict: bool, fault: Fault) -> Result<Coverage, Counterexample<Action>> {
    let mut coverage = Coverage::default();
    let (states, transitions) = explore(
        History::default(),
        |state| state.transitions(conflict, fault),
        |state| state.check(conflict),
        |state| {
            if state.phase == Phase::Recovered {
                coverage.recovered += 1;
                coverage.durable_after_hole +=
                    usize::from(state.holes == 1 && state.sealed_records == 2);
                coverage.lost_reply_recovered +=
                    usize::from(state.lost_reply & state.sealed_records != 0);
                coverage.snapshot_recovered += usize::from(state.snapshot.cut > 0);
                // Stable identities in a sealed epoch are resolved to the
                // original committed batch or a terminal not-committed outcome.
                for batch in 0..2 {
                    assert_ne!(
                        (state.holes >> batch) & 1,
                        (state.sealed_records >> batch) & 1
                    );
                }
            }
            coverage.blocked_inventory +=
                usize::from(state.phase == Phase::Frozen && state.available != ALL);
            coverage.sealed_late_requests +=
                usize::from(state.phase == Phase::Frozen && state.sealed != 0);
        },
    )?;
    coverage.states = states;
    coverage.transitions = transitions;
    Ok(coverage)
}

#[test]
fn exhaustive_complete_batch_authority_histories() {
    for conflict in [false, true] {
        let coverage = explore_history(conflict, Fault::None).unwrap();
        eprintln!("complete-batch model conflict={conflict}: {coverage:?}");
        assert!(coverage.states > 1_000, "accidentally narrowed state space");
        assert!(coverage.transitions > coverage.states);
        assert!(coverage.recovered > 0);
        assert!(coverage.durable_after_hole > 0);
        assert!(coverage.lost_reply_recovered > 0);
        assert!(coverage.snapshot_recovered > 0);
        assert!(coverage.blocked_inventory > 0);
        assert!(coverage.sealed_late_requests > 0);
    }
}

#[test]
fn authority_model_detects_unsafe_protocol_variants() {
    let mut violations = HashSet::new();
    for fault in [
        Fault::AppendBeforeValidation,
        Fault::PublishBeforeDurable,
        Fault::PublishAcrossGap,
        Fault::PartialBatchPublication,
        Fault::TimeoutIsHole,
        Fault::RecoverResponsiveOnly,
        Fault::SnapshotAtMaximum,
        Fault::ReappendSealedHole,
        Fault::RetryBecomesNewVersion,
    ] {
        let counterexample =
            explore_history(true, fault).expect_err("unsafe protocol went undetected");
        assert!(!counterexample.trace.is_empty());
        violations.insert(counterexample.violation);
        eprintln!("negative control {fault:?}: {counterexample:?}");
    }
    assert!(violations.len() >= 7);
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Location {
    #[default]
    Source,
    Relocated,
    NewUserMutation,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Reader {
    #[default]
    NotStarted,
    Protected,
    Finished,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
enum Replica {
    #[default]
    Absent,
    PayloadOnly,
    CompleteAuthority,
    DurableAuthority,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Retention {
    // Original frame is initially durable and acknowledged; replication is async.
    copy_complete: bool,
    copy_durable: bool,
    logical_version: u8,
    index: Location,
    recovery_mapping: Location,
    reader: Reader,
    capabilities_revoked: bool,
    newer_snapshot: bool,
    replica: Replica,
    replica_manifest: bool,
    deleted: bool,
    reused: bool,
    stale_address_accepted: bool,
    claims_payload_is_authority: bool,
    claims_lossless_promotion: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionFault {
    None,
    DeleteWithReader,
    DeleteWithOldRecoveryMapping,
    DeleteWithOldSnapshot,
    DeleteWhileReplicaNeedsSource,
    InstallStaleRelocation,
    PayloadOnlyAuthority,
    LosslessAsyncPromotion,
    AcceptOldGeneration,
}

#[derive(Clone, Copy, Debug)]
#[allow(dead_code)] // Used in counterexample diagnostics.
enum RetentionAction {
    AcquireReader,
    ReleaseReader,
    CopyCompleteAuthority,
    PersistCopy,
    InstallRelocation,
    NewUserMutation,
    PersistRecoveryMapping,
    ReplaceSnapshot,
    RevokeCapabilities,
    ReplicatePayload,
    ReplicateAuthorityMetadata,
    PersistReplica,
    PersistReplicaManifest,
    Delete,
    ReuseNextGeneration,
    Unsafe(RetentionFault),
}

impl Retention {
    fn can_delete(self) -> bool {
        self.index != Location::Source
            && self.recovery_mapping != Location::Source
            && self.newer_snapshot
            && self.capabilities_revoked
            && self.reader != Reader::Protected
            && self.replica == Replica::DurableAuthority
            && self.replica_manifest
    }

    fn check(self) -> Result<(), &'static str> {
        if self.copy_durable && !self.copy_complete {
            return Err("replacement durability without complete authority");
        }
        if (self.index == Location::Relocated || self.recovery_mapping == Location::Relocated)
            && !self.copy_durable
        {
            return Err("index/recovery references an unpersisted replacement");
        }
        if self.logical_version == 1 && self.index != Location::NewUserMutation {
            return Err("relocation resurrected an older user mutation");
        }
        if self.deleted && self.reader == Reader::Protected {
            return Err("old read capability lost its protected representation");
        }
        if self.deleted && self.recovery_mapping == Location::Source {
            return Err("retirement removed the persisted recovery mapping target");
        }
        if self.deleted && !self.newer_snapshot {
            return Err("retirement discarded a supported snapshot reference");
        }
        if self.deleted && (self.replica != Replica::DurableAuthority || !self.replica_manifest) {
            return Err("retirement discarded asynchronous catch-up authority");
        }
        if self.deleted && !self.can_delete() {
            return Err("retirement omitted an index/capability obligation");
        }
        if self.reused && !self.deleted {
            return Err("segment generation reused before retirement");
        }
        if self.reused && self.stale_address_accepted {
            return Err("old segment generation authorized unrelated reused bytes");
        }
        if self.claims_payload_is_authority && self.replica == Replica::PayloadOnly {
            return Err("replica payload lacks original identity/order/commit authority");
        }
        if self.claims_lossless_promotion && self.replica != Replica::DurableAuthority {
            return Err("asynchronous replica promotion claimed an unavailable acknowledged write");
        }
        Ok(())
    }

    fn transitions(self, fault: RetentionFault) -> Vec<(RetentionAction, Self)> {
        let mut out = Vec::with_capacity(16);
        let mut push = |action, next| {
            if next != self {
                out.push((action, next));
            }
        };
        if !self.deleted && !self.capabilities_revoked && self.reader == Reader::NotStarted {
            let mut next = self;
            next.reader = Reader::Protected;
            push(RetentionAction::AcquireReader, next);
        }
        if self.reader == Reader::Protected {
            let mut next = self;
            next.reader = Reader::Finished;
            push(RetentionAction::ReleaseReader, next);
        }
        if !self.deleted {
            let mut next = self;
            next.copy_complete = true;
            push(RetentionAction::CopyCompleteAuthority, next);
        }
        if self.copy_complete {
            let mut next = self;
            next.copy_durable = true;
            push(RetentionAction::PersistCopy, next);
        }
        if self.copy_durable && self.logical_version == 0 && self.index == Location::Source {
            let mut next = self;
            next.index = Location::Relocated;
            push(RetentionAction::InstallRelocation, next);
        }
        let mut next = self;
        next.logical_version = 1;
        next.index = Location::NewUserMutation;
        push(RetentionAction::NewUserMutation, next);
        let mut next = self;
        next.recovery_mapping = self.index;
        push(RetentionAction::PersistRecoveryMapping, next);
        if self.recovery_mapping != Location::Source {
            let mut next = self;
            next.newer_snapshot = true;
            push(RetentionAction::ReplaceSnapshot, next);
        }
        let mut next = self;
        next.capabilities_revoked = true;
        push(RetentionAction::RevokeCapabilities, next);
        if !self.deleted {
            let mut next = self;
            match self.replica {
                Replica::Absent => {
                    next.replica = Replica::PayloadOnly;
                    push(RetentionAction::ReplicatePayload, next);
                }
                Replica::PayloadOnly => {
                    next.replica = Replica::CompleteAuthority;
                    push(RetentionAction::ReplicateAuthorityMetadata, next);
                }
                Replica::CompleteAuthority => {
                    next.replica = Replica::DurableAuthority;
                    push(RetentionAction::PersistReplica, next);
                }
                Replica::DurableAuthority => {}
            }
        }
        if self.replica == Replica::DurableAuthority {
            let mut next = self;
            next.replica_manifest = true;
            push(RetentionAction::PersistReplicaManifest, next);
        }
        if self.can_delete() {
            let mut next = self;
            next.deleted = true;
            push(RetentionAction::Delete, next);
        }
        if self.deleted {
            let mut next = self;
            next.reused = true;
            push(RetentionAction::ReuseNextGeneration, next);
        }
        let mut next = self;
        match fault {
            RetentionFault::None => return out,
            RetentionFault::DeleteWithReader if self.reader == Reader::Protected => {
                next.deleted = true
            }
            RetentionFault::DeleteWithOldRecoveryMapping
                if self.index != Location::Source && self.recovery_mapping == Location::Source =>
            {
                next.deleted = true;
            }
            RetentionFault::DeleteWithOldSnapshot
                if self.recovery_mapping != Location::Source && !self.newer_snapshot =>
            {
                next.deleted = true;
            }
            RetentionFault::DeleteWhileReplicaNeedsSource
                if self.newer_snapshot && self.replica != Replica::DurableAuthority =>
            {
                next.deleted = true;
            }
            RetentionFault::InstallStaleRelocation
                if self.copy_durable && self.logical_version == 1 =>
            {
                next.index = Location::Relocated;
            }
            RetentionFault::PayloadOnlyAuthority if self.replica == Replica::PayloadOnly => {
                next.claims_payload_is_authority = true;
            }
            RetentionFault::LosslessAsyncPromotion => next.claims_lossless_promotion = true,
            RetentionFault::AcceptOldGeneration if self.reused => {
                next.stale_address_accepted = true;
            }
            _ => return out,
        }
        push(RetentionAction::Unsafe(fault), next);
        out
    }
}

#[test]
fn exhaustive_async_replica_and_reader_retention() {
    let mut retired_states = 0;
    let mut reused_states = 0;
    let mut acknowledged_replica_lag_states = 0;
    let (states, transitions) = explore(
        Retention::default(),
        |state| state.transitions(RetentionFault::None),
        Retention::check,
        |state| {
            retired_states += usize::from(state.deleted);
            reused_states += usize::from(state.reused);
            acknowledged_replica_lag_states +=
                usize::from(state.replica != Replica::DurableAuthority);
        },
    )
    .unwrap();
    eprintln!(
        "retention model: states={states}, transitions={transitions}, retired={retired_states}, reused={reused_states}, acknowledged_replica_lag={acknowledged_replica_lag_states}"
    );
    assert!(states > 100);
    assert!(transitions > states);
    assert!(retired_states > 0 && reused_states > 0 && acknowledged_replica_lag_states > 0);
}

#[test]
fn retention_model_detects_unsafe_protocol_variants() {
    for fault in [
        RetentionFault::DeleteWithReader,
        RetentionFault::DeleteWithOldRecoveryMapping,
        RetentionFault::DeleteWithOldSnapshot,
        RetentionFault::DeleteWhileReplicaNeedsSource,
        RetentionFault::InstallStaleRelocation,
        RetentionFault::PayloadOnlyAuthority,
        RetentionFault::LosslessAsyncPromotion,
        RetentionFault::AcceptOldGeneration,
    ] {
        let counterexample = explore(
            Retention::default(),
            |state| state.transitions(fault),
            Retention::check,
            |_| {},
        )
        .expect_err("unsafe retention protocol went undetected");
        assert!(!counterexample.trace.is_empty());
        eprintln!("negative control {fault:?}: {counterexample:?}");
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct Admission {
    foreground_used: u8,
    failed: bool,
    recovery_running: bool,
    recovered: bool,
}

#[derive(Clone, Copy, Debug)]
enum AdmissionAction {
    Admit,
    Finish,
    Fail,
    StartRecovery,
    FinishRecovery,
}

impl Admission {
    fn transitions(self, share_recovery_credits: bool) -> Vec<(AdmissionAction, Self)> {
        let mut out = Vec::new();
        if !self.failed {
            if self.foreground_used < 2 {
                let mut next = self;
                next.foreground_used += 1;
                out.push((AdmissionAction::Admit, next));
            }
            if self.foreground_used > 0 {
                let mut next = self;
                next.foreground_used -= 1;
                out.push((AdmissionAction::Finish, next));
            }
            let mut next = self;
            next.failed = true;
            out.push((AdmissionAction::Fail, next));
        } else if !self.recovered {
            if self.recovery_running {
                let mut next = self;
                next.foreground_used = 0;
                next.recovery_running = false;
                next.recovered = true;
                out.push((AdmissionAction::FinishRecovery, next));
            } else if !share_recovery_credits || self.foreground_used < 2 {
                let mut next = self;
                next.recovery_running = true;
                out.push((AdmissionAction::StartRecovery, next));
            }
        }
        out
    }

    fn check(self, share_recovery_credits: bool) -> Result<(), &'static str> {
        if self.foreground_used > 2 {
            return Err("foreground admission exceeded its bound");
        }
        if self.failed && !self.recovered && self.transitions(share_recovery_credits).is_empty() {
            return Err("foreground credits prevent the recovery needed to release them");
        }
        Ok(())
    }
}

#[test]
fn bounded_admission_reserves_recovery_progress() {
    let (states, transitions) = explore(
        Admission::default(),
        |state| state.transitions(false),
        |state| state.check(false),
        |_| {},
    )
    .unwrap();
    eprintln!("admission model: states={states}, transitions={transitions}");
    let counterexample = explore(
        Admission::default(),
        |state| state.transitions(true),
        |state| state.check(true),
        |_| {},
    )
    .expect_err("shared-credit recovery deadlock went undetected");
    eprintln!("negative shared admission control: {counterexample:?}");
    assert_eq!(counterexample.trace.len(), 3);
}
