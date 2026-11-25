//! The owner side of the coordinator lease: the time-bounded right to write
//! this database, renewed by the one coordinator that fenced the current
//! authority epoch (protocol 37).
//!
//! A lease grant is memory work: the cell takes no I/O and never holds its
//! lock across a configfs, file or controller call. The reactor decodes a
//! renewal and answers it on the queued-response path, before the control
//! queue, so an exclusive control cannot delay it. When a granted lease passes
//! its expiry plus [`OWNER_MARGIN`], the watchdog thread marks the cell lapsed
//! and write-protects the epoch's exports; no write is accepted afterwards.

use parking_lot::{Condvar, Mutex};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::TcpError;

/// Shortest lease an owner grants. A caller that asks for less gets this.
const MIN_LEASE_MS: u32 = 1_000;
/// Longest lease an owner grants: a longer grant delays failure detection.
const MAX_LEASE_MS: u32 = 30_000;
/// The owner measures validity from the grant, the coordinator from the send,
/// so the owner keeps a margin that covers the request's own flight time. The
/// coordinator's deadline ends this margin earlier (Gray & Cheriton).
const OWNER_MARGIN: Duration = Duration::from_millis(1_000);

/// The lease cell of one database on the owner.
///
/// Lock order: this mutex is a leaf. It is never taken while another lock is
/// held by this cell, and no I/O runs under it.
pub(super) struct CoordinatorLeaseCell {
    state: Mutex<LeaseState>,
    changed: Condvar,
    /// Grants this epoch granted. Test-only: a shutdown test observes renewals
    /// continuing through a held Seal.
    #[cfg(test)]
    grants: std::sync::atomic::AtomicU64,
    /// Test-only seam between the stop check and the park, where a notify that
    /// does not take the cell lock is lost.
    #[cfg(test)]
    before_park: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

#[derive(Debug, Default)]
struct LeaseState {
    /// The authority epoch whose coordinator holds the lease; 0 while sealed.
    active_epoch: u64,
    holder: Option<[u8; 16]>,
    /// `None` until the first grant of `active_epoch`.
    expires: Option<Instant>,
    /// Sticky for this epoch: set once by the expiry check, cleared only by
    /// `activate` of a new epoch.
    lapsed: bool,
    /// The owner is shutting down. Written only under the cell lock, so a
    /// watchdog that checks it and then parks cannot miss the wakeup.
    stopped: bool,
}

impl CoordinatorLeaseCell {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(LeaseState::default()),
            changed: Condvar::new(),
            #[cfg(test)]
            grants: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            before_park: Mutex::new(None),
        }
    }

    /// Grant or extend the lease of `holder` for `authority_epoch`.
    ///
    /// The duration is clamped to [`MIN_LEASE_MS`]..=[`MAX_LEASE_MS`]. A grant
    /// to another holder is refused while the current holder's grace period
    /// has not passed, so a stale coordinator that wakes up can never write
    /// beside a live one.
    pub(super) fn renew(
        &self,
        authority_epoch: u64,
        holder: [u8; 16],
        duration_ms: u32,
    ) -> Result<u32, TcpError> {
        self.renew_at(Instant::now(), authority_epoch, holder, duration_ms)
    }

    /// [`Self::renew`] with an injected clock, so tests never depend on a wall
    /// clock.
    pub(super) fn renew_at(
        &self,
        now: Instant,
        authority_epoch: u64,
        holder: [u8; 16],
        duration_ms: u32,
    ) -> Result<u32, TcpError> {
        let duration_ms = duration_ms.clamp(MIN_LEASE_MS, MAX_LEASE_MS);
        let mut state = self.state.lock();
        if authority_epoch != state.active_epoch {
            return Err(TcpError::with_kind(
                std::io::ErrorKind::PermissionDenied,
                "stale coordinator lease authority",
            ));
        }
        if state.lapsed {
            return Err(TcpError::with_kind(
                std::io::ErrorKind::PermissionDenied,
                "coordinator lease lapsed",
            ));
        }
        if let Some(other) = state.holder
            && other != holder
            && state
                .expires
                .is_some_and(|expires| now < expires + OWNER_MARGIN)
        {
            return Err(TcpError::with_kind(
                std::io::ErrorKind::PermissionDenied,
                "coordinator lease held by another incarnation",
            ));
        }
        state.holder = Some(holder);
        state.expires = Some(now + Duration::from_millis(u64::from(duration_ms)));
        #[cfg(test)]
        self.grants
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // The watchdog recomputes its deadline from the granted expiry.
        self.changed.notify_all();
        Ok(duration_ms)
    }

    /// Install the lease authority of a freshly fenced epoch. Called at the
    /// end of a successful fence install, so the epoch is active before its
    /// coordinator renews.
    pub(super) fn activate(&self, epoch: u64) {
        let mut state = self.state.lock();
        state.active_epoch = epoch;
        state.holder = None;
        state.expires = None;
        state.lapsed = false;
        self.changed.notify_all();
    }

    /// Retire the lease authority of sealed `epoch`. A stale Seal for another
    /// epoch leaves the current lease alone: a new fence owns it.
    pub(super) fn deactivate(&self, epoch: u64) {
        let mut state = self.state.lock();
        if state.active_epoch != epoch {
            return;
        }
        state.active_epoch = 0;
        state.holder = None;
        state.expires = None;
        self.changed.notify_all();
    }

    /// Refuse a write of `epoch` once its lease lapsed. Called first by every
    /// handler that writes owner files. A write of another epoch is refused by
    /// that handler's own authority check, not here.
    pub(super) fn check_writable(&self, epoch: u64) -> Result<(), TcpError> {
        let state = self.state.lock();
        if state.lapsed && epoch == state.active_epoch {
            return Err(TcpError::with_kind(
                std::io::ErrorKind::PermissionDenied,
                "coordinator lease lapsed",
            ));
        }
        Ok(())
    }

    /// Wait for the current holder's lease to expire past [`OWNER_MARGIN`].
    ///
    /// Returns the epoch to write-protect once per lapse, and `None` when the
    /// cell was stopped. The loop is designed to run until then: each iteration
    /// either waits on the cell or returns, and a lapse is reported once.
    pub(super) fn wait_lapse(&self) -> Option<u64> {
        let mut state = self.state.lock();
        loop {
            if state.stopped {
                return None;
            }
            #[cfg(test)]
            let hook = self.before_park.lock().take();
            #[cfg(test)]
            if let Some(hook) = hook {
                // Runs under the cell lock, exactly between the stop check and
                // the park that registers the waiter.
                hook();
            }
            match (state.holder, state.expires) {
                (Some(_), Some(expires)) if !state.lapsed => {
                    let deadline = expires + OWNER_MARGIN;
                    if Instant::now() >= deadline {
                        // Sticky: the watchdog protects once and parks, and no
                        // renewal is granted until a new epoch is activated.
                        state.lapsed = true;
                        return Some(state.active_epoch);
                    }
                    self.changed.wait_until(&mut state, deadline);
                }
                _ => self.changed.wait(&mut state),
            }
        }
    }

    /// Test-only: run `hook` under the cell lock between the stop check and the
    /// next park.
    #[cfg(test)]
    pub(super) fn pause_before_next_park(&self, hook: impl FnOnce() + Send + 'static) {
        *self.before_park.lock() = Some(Box::new(hook));
    }

    /// Test-only: grants of the current epoch.
    #[cfg(test)]
    pub(super) fn grants(&self) -> u64 {
        self.grants.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test-only: whether the current epoch lapsed.
    #[cfg(test)]
    pub(super) fn is_lapsed(&self) -> bool {
        self.state.lock().lapsed
    }

    /// Wake a parked [`Self::wait_lapse`] when the watchdog must stop.
    ///
    /// The stop predicate is written under the cell lock that `wait_lapse`
    /// checks it and parks under. parking_lot's `Condvar` registers a waiter
    /// only inside `wait`, so a notify issued between that check and the
    /// registration finds no waiter and is dropped; holding the same lock makes
    /// this store visible to the check or finds the waiter already parked.
    pub(super) fn stop_waiting(&self) {
        let mut state = self.state.lock();
        state.stopped = true;
        self.changed.notify_all();
    }
}

#[cfg(test)]
#[path = "coordinator_lease_tests.rs"]
mod coordinator_lease_tests;

/// The coordinator's lease client methods. They live beside the cell that
/// answers them, so the wire pair reads as one contract.
impl super::Client {
    /// Attach the coordinator's lease on this owner. Only a write worker's
    /// client gets one; every other client leaves it unset.
    pub(crate) fn set_lease_validity(
        &mut self,
        validity: Arc<super::super::remote_db::coordinator_lease::LeaseValidity>,
    ) {
        self.lease_validity = Some(validity);
    }

    /// Submission-time lease gate: the coordinator's right to write this owner
    /// ends before the owner's grant does, so a lapsed lease refuses the frame
    /// before it is encoded. One `Acquire` load, no lock.
    pub(crate) fn check_lease(&self) -> Result<(), super::TcpError> {
        match &self.lease_validity {
            Some(validity) => validity.ensure_writable(self.expected.partition),
            None => Ok(()),
        }
    }

    /// Renew the coordinator lease of `authority_epoch`. The owner clamps the
    /// duration and returns the granted value.
    pub(crate) fn renew_coordinator_lease(
        &mut self,
        authority_epoch: u64,
        holder: [u8; 16],
        duration_ms: u32,
    ) -> Result<u32, super::TcpError> {
        match self.request(super::Request::RenewCoordinatorLease {
            authority_epoch,
            holder,
            duration_ms,
        })? {
            super::Response::CoordinatorLease { granted_ms } => Ok(granted_ms),
            response => self.wrong_variant("RenewCoordinatorLease", &response, "CoordinatorLease"),
        }
    }

    /// [`Self::renew_coordinator_lease`] with a request deadline of `budget`,
    /// never more than the session's own timeout. A renewal of the cadence must
    /// not outlive the next one.
    pub(crate) fn renew_coordinator_lease_within(
        &mut self,
        authority_epoch: u64,
        holder: [u8; 16],
        duration_ms: u32,
        budget: Duration,
    ) -> Result<u32, super::TcpError> {
        let previous = self.timeout;
        self.timeout = previous.min(budget);
        let result = self.renew_coordinator_lease(authority_epoch, holder, duration_ms);
        self.timeout = previous;
        result
    }
}

/// The lease lapse watchdog of one database. It waits for the active holder's
/// lease to expire past [`OWNER_MARGIN`], then write-protects the exports of
/// that epoch, so the coordinator can no longer write owner files even if its
/// own failure detection is late.
///
/// The loop is designed to run until the control owner stops it: each
/// iteration waits on the cell or returns, and the cell reports one lapse per
/// activated epoch. It takes the live-control gate, then the lifecycle gate,
/// then the inventory; the target work releases the inventory.
pub(super) fn lease_watch(context: &super::control::ControlContext) {
    while let Some(epoch) = context.coordinator_lease.wait_lapse() {
        let _gate = context.live_control_gate.lock();
        let mut slot = context.lifecycle.lock();
        let result = super::inventory::lease::protect_lapsed_epoch(
            &mut context.inventory.lock(),
            &mut slot,
            epoch,
        );
        if let Err(error) = result {
            // `protect_live_targets` already marked the owner unhealthy on a
            // target error; the exported files may still be writable, so the
            // cause must stay visible.
            eprintln!(
                "coordinator lease lapse write-protect failed on partition {}: {error}",
                context.partition.partition
            );
        }
    }
}
