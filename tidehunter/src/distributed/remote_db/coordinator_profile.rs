//! Independent first-N coordinator resolution observations. Snapshots are sequential,
//! not an atomic cut; drain callers before interpreting a measurement interval.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default)]
pub struct CoordinatorResolutionStage {
    pub count: u64,
    pub sum_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CoordinatorResolutionSnapshot {
    pub selected: u64,
    pub point: u64,
    pub bulk: u64,
    pub errors: u64,
    pub logical_members: u64,
    pub cache_hits: u64,
    pub index_misses: u64,
    pub remote_misses: u64,
    pub member_errors: u64,
    pub index_lock_wait: CoordinatorResolutionStage,
    pub index_lookup: CoordinatorResolutionStage,
    pub cache_lock_wait: CoordinatorResolutionStage,
    pub cache_lookup: CoordinatorResolutionStage,
    pub cut_validation: CoordinatorResolutionStage,
    pub cut_capture: CoordinatorResolutionStage,
    pub route_preparation: CoordinatorResolutionStage,
    pub pin_preparation: CoordinatorResolutionStage,
    pub batch_preparation: CoordinatorResolutionStage,
}

#[derive(Default)]
struct Stage {
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}
impl Stage {
    fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
        self.max.store(0, Ordering::Relaxed);
    }
    fn add(&self, ns: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(ns, Ordering::Relaxed);
        self.max.fetch_max(ns, Ordering::Relaxed);
    }
    fn snapshot(&self) -> CoordinatorResolutionStage {
        CoordinatorResolutionStage {
            count: self.count.load(Ordering::Relaxed),
            sum_ns: self.sum.load(Ordering::Relaxed),
            max_ns: self.max.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub(super) struct CoordinatorResolutionDiagnostics {
    remaining: AtomicU64,
    selected: AtomicU64,
    point: AtomicU64,
    bulk: AtomicU64,
    errors: AtomicU64,
    logical_members: AtomicU64,
    cache_hits: AtomicU64,
    index_misses: AtomicU64,
    remote_misses: AtomicU64,
    member_errors: AtomicU64,
    index_lock_wait: Stage,
    index_lookup: Stage,
    cache_lock_wait: Stage,
    cache_lookup: Stage,
    cut_validation: Stage,
    cut_capture: Stage,
    route_preparation: Stage,
    pin_preparation: Stage,
    batch_preparation: Stage,
}
impl CoordinatorResolutionDiagnostics {
    pub(super) fn enabled(&self) -> bool {
        self.remaining.load(Ordering::Relaxed) != 0
    }
    pub(super) fn arm(&self, limit: u64) {
        if limit != 0 {
            for counter in [
                &self.selected,
                &self.point,
                &self.bulk,
                &self.errors,
                &self.logical_members,
                &self.cache_hits,
                &self.index_misses,
                &self.remote_misses,
                &self.member_errors,
            ] {
                counter.store(0, Ordering::Relaxed);
            }
            for stage in [
                &self.index_lock_wait,
                &self.index_lookup,
                &self.cache_lock_wait,
                &self.cache_lookup,
                &self.cut_validation,
                &self.cut_capture,
                &self.route_preparation,
                &self.pin_preparation,
                &self.batch_preparation,
            ] {
                stage.reset();
            }
        }
        self.remaining.store(limit, Ordering::Relaxed);
    }
    pub(super) fn select(&self, bulk: bool, members: usize) -> Option<ResolutionSample<'_>> {
        if self.remaining.load(Ordering::Relaxed) == 0 {
            return None;
        }
        if self
            .remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_err()
        {
            return None;
        }
        self.selected.fetch_add(1, Ordering::Relaxed);
        if bulk { &self.bulk } else { &self.point }.fetch_add(1, Ordering::Relaxed);
        self.logical_members
            .fetch_add(members as u64, Ordering::Relaxed);
        Some(ResolutionSample {
            diagnostics: self,
            success: false,
        })
    }
    pub(super) fn snapshot(&self) -> CoordinatorResolutionSnapshot {
        let load = |v: &AtomicU64| v.load(Ordering::Relaxed);
        CoordinatorResolutionSnapshot {
            selected: load(&self.selected),
            point: load(&self.point),
            bulk: load(&self.bulk),
            errors: load(&self.errors),
            logical_members: load(&self.logical_members),
            cache_hits: load(&self.cache_hits),
            index_misses: load(&self.index_misses),
            remote_misses: load(&self.remote_misses),
            member_errors: load(&self.member_errors),
            index_lock_wait: self.index_lock_wait.snapshot(),
            index_lookup: self.index_lookup.snapshot(),
            cache_lock_wait: self.cache_lock_wait.snapshot(),
            cache_lookup: self.cache_lookup.snapshot(),
            cut_validation: self.cut_validation.snapshot(),
            cut_capture: self.cut_capture.snapshot(),
            route_preparation: self.route_preparation.snapshot(),
            pin_preparation: self.pin_preparation.snapshot(),
            batch_preparation: self.batch_preparation.snapshot(),
        }
    }
}

pub(super) enum ResolutionStage {
    IndexLockWait,
    IndexLookup,
    CacheLockWait,
    CacheLookup,
    CutValidation,
    CutCapture,
    RoutePreparation,
    PinPreparation,
    BatchPreparation,
}
pub(super) enum MemberOutcome {
    CacheHit,
    IndexMiss,
    RemoteMiss,
    Error,
}
pub(super) struct ResolutionSample<'a> {
    diagnostics: &'a CoordinatorResolutionDiagnostics,
    success: bool,
}
impl ResolutionSample<'_> {
    pub(super) fn start(sample: &Option<Self>) -> Option<Instant> {
        sample.as_ref().map(|_| Instant::now())
    }
    pub(super) fn stage(&self, stage: ResolutionStage, started: Option<Instant>) {
        if let Some(started) = started {
            let ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            match stage {
                ResolutionStage::IndexLockWait => &self.diagnostics.index_lock_wait,
                ResolutionStage::IndexLookup => &self.diagnostics.index_lookup,
                ResolutionStage::CacheLockWait => &self.diagnostics.cache_lock_wait,
                ResolutionStage::CacheLookup => &self.diagnostics.cache_lookup,
                ResolutionStage::CutValidation => &self.diagnostics.cut_validation,
                ResolutionStage::CutCapture => &self.diagnostics.cut_capture,
                ResolutionStage::RoutePreparation => &self.diagnostics.route_preparation,
                ResolutionStage::PinPreparation => &self.diagnostics.pin_preparation,
                ResolutionStage::BatchPreparation => &self.diagnostics.batch_preparation,
            }
            .add(ns);
        }
    }
    pub(super) fn member(&self, outcome: MemberOutcome) {
        match outcome {
            MemberOutcome::CacheHit => &self.diagnostics.cache_hits,
            MemberOutcome::IndexMiss => &self.diagnostics.index_misses,
            MemberOutcome::RemoteMiss => &self.diagnostics.remote_misses,
            MemberOutcome::Error => &self.diagnostics.member_errors,
        }
        .fetch_add(1, Ordering::Relaxed);
    }
    pub(super) fn success(&mut self) {
        self.success = true;
    }
}
impl Drop for ResolutionSample<'_> {
    fn drop(&mut self) {
        if !self.success {
            self.diagnostics.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_selection_errors_and_rearm_reset_maxima() {
        let diagnostics = CoordinatorResolutionDiagnostics::default();
        assert!(diagnostics.select(false, 1).is_none());
        diagnostics.arm(1);
        let sample = diagnostics.select(false, 1).unwrap();
        sample.member(MemberOutcome::Error);
        diagnostics.index_lookup.add(23);
        drop(sample);
        assert!(diagnostics.select(true, 2).is_none());
        let snapshot = diagnostics.snapshot();
        assert_eq!(
            (snapshot.selected, snapshot.errors, snapshot.member_errors),
            (1, 1, 1)
        );
        assert_eq!(
            (
                snapshot.index_lookup.count,
                snapshot.index_lookup.sum_ns,
                snapshot.index_lookup.max_ns
            ),
            (1, 23, 23)
        );
        diagnostics.arm(0);
        assert_eq!(diagnostics.snapshot().index_lookup.max_ns, 23);
        diagnostics.arm(1);
        let reset = diagnostics.snapshot();
        assert_eq!(
            (
                reset.selected,
                reset.errors,
                reset.index_lookup.count,
                reset.index_lookup.max_ns
            ),
            (0, 0, 0, 0)
        );
    }
}
