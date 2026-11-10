//! Sealed NVMe-oF reader state of the coordinator read path.
//!
//! Ownership (design order: share nothing, then sharded locks):
//! * Every installed reader lives in its own [`NvmeDispatchCell`]. A reader
//!   serves exactly one owner endpoint (its manifest partition), so only that
//!   endpoint's reactor thread submits to it and harvests its CQ. No lock is
//!   shared between two readers, and no caller thread touches a reader on the
//!   read path.
//! * Callers plan reads without any lock. The immutable [`NvmeofCatalog`] of a
//!   reader answers eligibility and aligned-range queries; the reader set is an
//!   `ArcSwap` snapshot replaced only by install and release (cold, serialized by
//!   `nvmeof_maintenance`, with admission paused and no read outstanding).
//! * Each cell has one `Mutex`. It is taken by the owning reactor for submit and
//!   progress, and by cold maintenance (install check, shutdown barrier,
//!   release). It is never held while verifying records or completing a request.
//!
//! Lock order: `State::retained`, `State::flights` and `ReadService::lanes` are
//! never taken while holding a cell mutex. When several cells are locked
//! together (release only), lock them in `NvmeRouting::routes` order.
use super::*;
use crate::distributed::nvmeof::{
    Eligibility, MemberRange, NvmeofCatalog, NvmeofError, NvmeofReader, ReadPlan,
};
use parking_lot::MutexGuard;

/// One member's admission facts, computed once per request or batch.
#[derive(Clone, Copy)]
pub(super) struct SealedMember {
    /// Index of the serving reader in `NvmeRouting::routes`.
    pub route: usize,
    /// Aligned device range and byte charge, computed once.
    pub range: MemberRange,
}

pub(super) struct NvmeDispatch {
    pub reader: NvmeofReader,
    pub lease_client: Arc<Mutex<Client>>,
    pub lease_epoch: u64,
    pub lease_token: [u8; 16],
    pub shape: Arc<KeyShape>,
    pub next: u64,
    pub active: HashMap<u64, Request>,
}

pub(super) struct NvmeDispatchCell {
    dispatch: Mutex<Option<NvmeDispatch>>,
    /// Accepted sealed requests not yet completed. Written only by the owning
    /// reactor, which also reads it to skip an idle reader without locking, so
    /// `Relaxed` suffices. Maintenance never relies on it: it checks `active`
    /// under the cell mutex.
    pub in_flight: AtomicUsize,
}
impl NvmeDispatchCell {
    pub fn new(dispatch: NvmeDispatch) -> Self {
        Self {
            dispatch: Mutex::new(Some(dispatch)),
            in_flight: AtomicUsize::new(0),
        }
    }
}

/// An installed reader: lock-free planning view plus its private dispatch.
#[derive(Clone)]
pub(super) struct NvmeRoute {
    pub catalog: Arc<NvmeofCatalog>,
    /// Endpoint index whose reactor owns this reader.
    pub node: usize,
    pub metadata_budget: usize,
    pub cell: Arc<NvmeDispatchCell>,
}

/// Cell guard that records contended wait and hold time in the reader's stats.
pub(super) struct DispatchGuard<'a> {
    guard: MutexGuard<'a, Option<NvmeDispatch>>,
    catalog: &'a NvmeofCatalog,
    since: Instant,
}
impl DispatchGuard<'_> {
    pub fn dispatch(&mut self) -> Option<&mut NvmeDispatch> {
        self.guard.as_mut()
    }
    pub fn take(&mut self) -> Option<NvmeDispatch> {
        self.guard.take()
    }
}
impl Drop for DispatchGuard<'_> {
    fn drop(&mut self) {
        self.catalog.stats().lock_hold.add(self.since.elapsed());
    }
}

impl NvmeRoute {
    pub fn lock(&self) -> DispatchGuard<'_> {
        let guard = if let Some(guard) = self.cell.dispatch.try_lock() {
            guard
        } else {
            let waiting = Instant::now();
            let guard = self.cell.dispatch.lock();
            self.catalog.stats().lock_wait.add(waiting.elapsed());
            guard
        };
        DispatchGuard {
            guard,
            catalog: &self.catalog,
            since: Instant::now(),
        }
    }
}

/// Immutable snapshot of the installed readers. `generation` changes only when
/// the reader set changes, never when read credit is released.
pub(super) struct NvmeRouting {
    pub generation: u64,
    pub routes: Vec<NvmeRoute>,
}
impl NvmeRouting {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            routes: Vec::new(),
        }
    }
    /// The next snapshot, with a new generation.
    pub fn successor(&self, routes: Vec<NvmeRoute>) -> Self {
        Self {
            generation: self.generation + 1,
            routes,
        }
    }
    /// Index of the reader whose catalog admits `reference`. Every reader is
    /// asked so an identity conflict with any of them is still an error.
    pub fn route_of(&self, reference: &ValueRecordReference) -> Result<Option<usize>, NvmeofError> {
        let mut selected = None;
        for (index, route) in self.routes.iter().enumerate() {
            if route.catalog.eligibility(reference)? == Eligibility::Eligible {
                selected = Some(index);
            }
        }
        Ok(selected)
    }
    pub fn sealed_plan(
        &self,
        members: &[SealedMember],
    ) -> Result<Option<Box<ReadPlan>>, LocalDbError> {
        let Some(first) = members.first() else {
            return Ok(None);
        };
        let ranges: SmallVec<[MemberRange; 32]> =
            members.iter().map(|member| member.range).collect();
        let plan = self.routes[first.route]
            .catalog
            .plan_request(&ranges)
            .map_err(LocalDbError::component)?;
        Ok(Some(Box::new(plan)))
    }
}

impl ReadService {
    /// Plan one member: one authority lookup and one alignment computation.
    pub(super) fn plan_sealed_member(
        &self,
        routing: &NvmeRouting,
        reference: &ValueRecordReference,
    ) -> Result<MemberPlan, LocalDbError> {
        let charge = member_charge(reference)?;
        let route = routing.route_of(reference).map_err(|error| {
            self.state.poison();
            LocalDbError::component(error)
        })?;
        let Some(route) = route else {
            return Ok(MemberPlan {
                charge,
                sealed: None,
            });
        };
        let catalog = &routing.routes[route].catalog;
        let range = catalog
            .plan_member(reference)
            .map_err(LocalDbError::component)?;
        if range.charge() > catalog.max_request_bytes() || catalog.max_request_members() == 0 {
            return Err(LocalDbError::new("record exceeds NVMe-oF reader capacity"));
        }
        let charge = charge
            .checked_add(range.charge())
            .ok_or_else(|| LocalDbError::new("read budget overflow"))?;
        Ok(MemberPlan {
            charge,
            sealed: Some(SealedMember { route, range }),
        })
    }

    /// Replace the reader set. The caller holds `nvmeof_maintenance`, so there
    /// is no concurrent writer of the snapshot.
    pub(super) fn publish_routing(&self, routes: Vec<NvmeRoute>) {
        let next = self.state.nvme_routing.load().successor(routes);
        let sources = next
            .routes
            .iter()
            .map(|route| route.catalog.stats().clone())
            .collect::<Vec<_>>();
        self.state.nvme_routing.store(Arc::new(next));
        for metrics in self.state.nvmeof_metrics.lock().iter() {
            metrics.replace_sources(sources.clone());
        }
    }
}

impl State {
    /// Close admission on every reader of `node`; accepted requests still
    /// complete and the reactor keeps harvesting their CQEs.
    pub(super) fn begin_nvme_shutdown(&self, node: usize) {
        for route in self
            .nvme_routing
            .load()
            .routes
            .iter()
            .filter(|route| route.node == node)
        {
            if let Some(dispatch) = route.lock().dispatch() {
                dispatch.reader.begin_shutdown();
            }
        }
    }
}
