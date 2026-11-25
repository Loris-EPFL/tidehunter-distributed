//! Coordinator side of the coordinator lease (protocol 37): one renewer
//! thread per owner, and the lock-free validity stamp that every leased write
//! checks before it submits.
//!
//! A renewer owns a dedicated session to its owner and never shares the write
//! worker's client, so a renewal can never wait behind a leased frame. It
//! measures validity from the send time and the owner grants from the receive
//! time, so the coordinator's deadline always ends before the owner's grant
//! (Gray & Cheriton).
//!
//! The first grant of every owner must succeed at open. A later lapse on any
//! owner is fail-stop for the database: the coordinator fails the whole write
//! core with `ConnectionAborted`, as a disk failure stops single Tidehunter.
//! There is no degraded mode.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use super::super::storage::LocalDbError;
use super::super::tcp::{Client, TcpError, WakeFd};
use super::write_core::core::WriteCore;

/// The holder's right is valid until this owner's deadline.
const VALID: u8 = 0;
/// The last renewal failed; a write may still land inside the stamped deadline.
const SUSPECT: u8 = 1;
/// The stamped deadline passed: terminal until reopen.
const FAILED: u8 = 2;
/// The holder's deadline ends this much before the grant it measured, so the
/// owner's grant always expires later than the holder's right.
const DEADLINE_MARGIN: Duration = Duration::from_millis(500);
/// One test-only decision per renewal attempt: `true` sends the request,
/// `false` drops it, so a test can stop one owner's renewals without a clock
/// or a stopped owner.
#[cfg(test)]
pub(super) type SendHook = Arc<dyn Fn(usize) -> bool + Send + Sync>;

/// The renewer's validity stamp for one owner. Every field is read lock-free
/// by the write worker of that owner, so a submission never waits for the
/// renewer.
pub(crate) struct LeaseValidity {
    /// One coordinator-local clock for every owner of this database.
    base: Instant,
    /// `base`-relative nanoseconds at which the holder's right ends.
    valid_until_ns: AtomicU64,
    state: AtomicU8,
    /// Set by [`Self::lapse_for_test`]: a later renewal cannot repair the
    /// lapse, so a test that injects one never races the renewer thread for
    /// the failure it asserts.
    #[cfg(test)]
    terminal: AtomicBool,
}

impl LeaseValidity {
    fn new(base: Instant) -> Self {
        Self {
            base,
            valid_until_ns: AtomicU64::new(0),
            state: AtomicU8::new(VALID),
            #[cfg(test)]
            terminal: AtomicBool::new(false),
        }
    }

    /// A stamp that is already expired, for the submission-gate tests.
    #[cfg(test)]
    pub(crate) fn expired_for_test() -> Self {
        Self {
            base: Instant::now(),
            valid_until_ns: AtomicU64::new(0),
            state: AtomicU8::new(VALID),
            terminal: AtomicBool::new(false),
        }
    }

    /// A stamp that is valid for an hour, so a test can lapse it at a chosen
    /// step boundary instead of before the resume.
    #[cfg(test)]
    pub(crate) fn fresh_for_test() -> Self {
        let stamp = Self::new(Instant::now());
        // One hour, far beyond any test's own wait bounds.
        stamp
            .valid_until_ns
            .store(3_600_000_000_000, Ordering::Release);
        stamp
    }

    /// Lapse this owner's lease from a test and keep it lapsed.
    ///
    /// A resume step runs on the worker thread while the renewer of the same
    /// owner runs on its own, so a lapse that a renewal could repair would make
    /// the assertion time-dependent. `terminal` is `Release`-stored first and
    /// `Acquire`-loaded by every lease check, so the lapse is visible to the
    /// worker at its next step boundary.
    #[cfg(test)]
    pub(crate) fn lapse_for_test(&self) {
        self.terminal.store(true, Ordering::Release);
        self.state.store(FAILED, Ordering::Release);
    }

    /// The submission-time gate of a leased frame. A lapsed lease refuses the
    /// write before it is encoded.
    pub(crate) fn ensure_writable(&self, partition: u32) -> Result<(), TcpError> {
        if self.lapsed() {
            return Err(TcpError::with_kind(
                std::io::ErrorKind::ConnectionAborted,
                format!("coordinator lease lapsed on owner partition {partition}"),
            ));
        }
        Ok(())
    }

    /// True when this owner's lease has lapsed. One `Acquire` load of the
    /// stamped deadline, and one of the terminal state, in that order: a grant
    /// that lands between the two loads only makes the check more permissive.
    fn lapsed(&self) -> bool {
        #[cfg(test)]
        if self.terminal.load(Ordering::Acquire) {
            return true;
        }
        if self.state.load(Ordering::Acquire) == FAILED {
            return true;
        }
        nanos(self.base.elapsed()) >= self.valid_until_ns.load(Ordering::Acquire)
    }

    /// The instant this owner's right ends, on the coordinator's clock. A write
    /// resume never runs past it: the lease is the authority, so the budget is
    /// the earlier of this deadline and the resume's own bound.
    pub(crate) fn admitted_until(&self) -> Instant {
        self.base
            .checked_add(Duration::from_nanos(
                self.valid_until_ns.load(Ordering::Acquire),
            ))
            .unwrap_or(self.base)
    }

    /// Install a grant measured from `sent`. The deadline ends
    /// [`DEADLINE_MARGIN`] before the owner's grant does.
    fn store_grant(&self, sent: Duration, granted_ms: u32) {
        let deadline = nanos(sent)
            .saturating_add(u64::from(granted_ms).saturating_mul(1_000_000))
            .saturating_sub(nanos(DEADLINE_MARGIN));
        self.valid_until_ns.store(deadline, Ordering::Release);
        self.state.store(VALID, Ordering::Release);
    }

    fn store_state(&self, state: u8) {
        self.state.store(state, Ordering::Release);
    }
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

/// One owner's renewal session factory. The closure opens a dedicated plain
/// session, interrupted by `stop` during its handshake.
pub(super) struct RenewTarget {
    pub partition: u32,
    pub connect: Box<dyn Fn(Arc<WakeFd>) -> Result<Client, TcpError> + Send>,
}

/// The renewal session targets of one database: one dedicated plain session
/// per endpoint, over the epoch-1 inventory the owner registered with. Its
/// handshake is interruptible by the renewer's stop wake, like the durable-cut
/// poller's.
pub(super) fn targets(
    config: &super::RemoteDatabaseConfig,
    tls_config: &super::super::tcp::ClientTlsConfig,
    shape: &crate::key_shape::KeyShape,
) -> Vec<RenewTarget> {
    config
        .endpoints
        .iter()
        .map(|endpoint| {
            let mut tls = config.tls.clone();
            tls.server_name = endpoint.server_name.clone();
            let address = endpoint.address.clone();
            let client_config = tls_config.clone();
            let timeout = config.timeout;
            let expected = config.partition_config(1, endpoint.partition);
            let shape = shape.clone();
            RenewTarget {
                partition: endpoint.partition,
                connect: Box::new(move |stop| {
                    Client::connect_progress_shared(
                        &address,
                        &tls,
                        client_config.clone(),
                        timeout,
                        expected.clone(),
                        &shape,
                        stop,
                    )
                }),
            }
        })
        .collect()
}

/// A stop flag that wakes a parked renewer at once. A leaf lock: it is never
/// taken while another lock is held, and no I/O runs under it.
#[derive(Default)]
struct Stop {
    stopped: Mutex<bool>,
    changed: Condvar,
}

impl Stop {
    /// Set the stop under the guard `wait` checks it under, then notify. A
    /// parking_lot notify outside that guard can fall between the check and the
    /// park, where no waiter is registered yet, and is dropped.
    fn stop(&self) {
        let mut stopped = self.stopped.lock();
        *stopped = true;
        self.changed.notify_all();
    }

    /// Wait up to `duration` for the stop. Returns true when stopped.
    fn wait(&self, duration: Duration) -> bool {
        let mut stopped = self.stopped.lock();
        if !*stopped {
            self.changed.wait_for(&mut stopped, duration);
        }
        *stopped
    }
}

/// The renewers of one database, one per endpoint in endpoint order.
pub(super) struct CoordinatorLease {
    validity: Vec<Arc<LeaseValidity>>,
    stop: Arc<Stop>,
    /// Set when the database starts closing: renewals continue, but a renewal
    /// failure is no longer a failure of the database.
    closing: Arc<AtomicBool>,
    wakes: Vec<Arc<WakeFd>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl CoordinatorLease {
    /// Grant the lease of every owner once, then start one renewer per owner.
    ///
    /// The first grant of every owner must succeed: without it the coordinator
    /// holds no right to write, so open fails with the renewal's error. No
    /// thread is started before every first grant succeeded.
    pub(super) fn start(
        targets: Vec<RenewTarget>,
        epoch: u64,
        holder: [u8; 16],
        duration_ms: u32,
        core: Arc<WriteCore>,
        #[cfg(test)] send_hook: Option<SendHook>,
    ) -> Result<Self, LocalDbError> {
        let base = Instant::now();
        let mut wakes = Vec::with_capacity(targets.len());
        let mut grants = Vec::with_capacity(targets.len());
        for target in &targets {
            let wake = Arc::new(WakeFd::for_progress("coordinator lease stop wake").map_err(
                |error| {
                    LocalDbError::new(format!(
                        "coordinator lease wake for partition {}: {error}",
                        target.partition
                    ))
                },
            )?);
            let mut client = (target.connect)(wake.clone()).map_err(|error| {
                LocalDbError::new(format!(
                    "coordinator lease session to partition {}: {error}",
                    target.partition
                ))
            })?;
            let sent = base.elapsed();
            let granted = client
                .renew_coordinator_lease(epoch, holder, duration_ms)
                .map_err(|error| {
                    LocalDbError::new(format!(
                        "coordinator lease renewal to partition {} at open: {error}",
                        target.partition
                    ))
                })?;
            wakes.push(wake);
            grants.push((client, granted, sent));
        }
        debug_assert_eq!(
            grants.len(),
            targets.len(),
            "every target of the first grant sweep has one session"
        );
        let validity: Vec<Arc<LeaseValidity>> = targets
            .iter()
            .zip(&grants)
            .map(|(_, (_, granted, sent))| {
                let stamp = Arc::new(LeaseValidity::new(base));
                stamp.store_grant(*sent, *granted);
                stamp
            })
            .collect();
        let stop = Arc::new(Stop::default());
        let closing = Arc::new(AtomicBool::new(false));
        let mut threads = Vec::with_capacity(targets.len());
        for (index, (target, (client, granted, _sent))) in
            targets.into_iter().zip(grants).enumerate()
        {
            let validity = validity[index].clone();
            let thread_stop = stop.clone();
            let wake = wakes[index].clone();
            let core = core.clone();
            let closing = closing.clone();
            let cadence =
                Duration::from_millis(u64::from(granted) / 5).max(Duration::from_millis(1));
            #[cfg(test)]
            let hook = send_hook.clone();
            let spawned = thread::Builder::new()
                .name(format!("coordinator-lease-{}", target.partition))
                .spawn(move || {
                    renew_loop(
                        index,
                        target,
                        client,
                        epoch,
                        holder,
                        duration_ms,
                        validity,
                        base,
                        thread_stop,
                        wake,
                        cadence,
                        core,
                        closing,
                        #[cfg(test)]
                        hook,
                    );
                });
            match spawned {
                Ok(thread) => threads.push(thread),
                Err(error) => {
                    stop.stop();
                    for wake in &wakes {
                        let _ = wake.signal();
                    }
                    return Err(LocalDbError::new(format!(
                        "coordinator lease renewer thread: {error}"
                    )));
                }
            }
        }
        Ok(Self {
            validity,
            stop,
            closing,
            wakes,
            threads: Mutex::new(threads),
        })
    }

    /// The validity stamp of `owner`, in endpoint order.
    pub(super) fn validity(&self, owner: usize) -> Arc<LeaseValidity> {
        self.validity[owner].clone()
    }

    /// The database stopped accepting writes and is sealing its owners.
    /// Renewals continue until `stop`, so a Seal slower than the lease cannot
    /// lapse it; from here a renewal failure is not a database failure.
    pub(super) fn begin_close(&self) {
        self.closing.store(true, Ordering::Release);
    }

    /// Stop every renewer and join it. Safe to call twice; the second call
    /// joins nothing. Called after the owners sealed.
    pub(super) fn stop(&self) {
        self.stop.stop();
        for wake in &self.wakes {
            let _ = wake.signal();
        }
        let threads = std::mem::take(&mut *self.threads.lock());
        for thread in threads {
            let _ = thread.join();
        }
    }
}

impl Drop for CoordinatorLease {
    fn drop(&mut self) {
        self.stop();
    }
}

/// One owner's renewal loop. It is designed to run until `CoordinatorLease`
/// stops it: every iteration waits on the stop or attempts one renewal, and a
/// lapse returns after the one fail-stop.
#[allow(
    clippy::too_many_arguments,
    reason = "the loop's state is one renewer: session, authority, stamp, stop and clock"
)]
fn renew_loop(
    _index: usize,
    target: RenewTarget,
    mut client: Client,
    epoch: u64,
    holder: [u8; 16],
    duration_ms: u32,
    validity: Arc<LeaseValidity>,
    base: Instant,
    stop: Arc<Stop>,
    wake: Arc<WakeFd>,
    mut cadence: Duration,
    core: Arc<WriteCore>,
    closing: Arc<AtomicBool>,
    #[cfg(test)] send_hook: Option<SendHook>,
) {
    let RenewTarget { partition, connect } = target;
    loop {
        if stop.wait(cadence) {
            return;
        }
        // A renewal attempt whose request never reaches the owner. The test
        // hook uses it to stop one owner's renewals; production never does.
        #[cfg(test)]
        let dropped = send_hook.as_ref().is_some_and(|hook| !hook(_index));
        #[cfg(not(test))]
        let dropped = false;
        if !dropped {
            let sent = base.elapsed();
            match client.renew_coordinator_lease_within(epoch, holder, duration_ms, cadence) {
                Ok(granted) => {
                    validity.store_grant(sent, granted);
                    cadence =
                        Duration::from_millis(u64::from(granted) / 5).max(Duration::from_millis(1));
                }
                Err(_) => {
                    validity.store_state(SUSPECT);
                    // A transport failure leaves the session unusable: replace
                    // it and confirm once at once, then keep the cadence. While
                    // the database closes the owner may have sealed already, so
                    // its refusal is expected and a new session buys nothing.
                    if !closing.load(Ordering::Acquire)
                        && let Ok(mut fresh) = connect(wake.clone())
                    {
                        let sent = base.elapsed();
                        if let Ok(granted) = fresh.renew_coordinator_lease_within(
                            epoch,
                            holder,
                            duration_ms,
                            cadence,
                        ) {
                            validity.store_grant(sent, granted);
                            cadence = Duration::from_millis(u64::from(granted) / 5)
                                .max(Duration::from_millis(1));
                        }
                        client = fresh;
                    }
                }
            }
        } else {
            validity.store_state(SUSPECT);
        }
        if nanos(base.elapsed()) >= validity.valid_until_ns.load(Ordering::Acquire) {
            if closing.load(Ordering::Acquire) {
                // The database is closing: its owners seal under a live lease,
                // then this thread is stopped. A lapse is not a failure here.
                return;
            }
            // Terminal for this database: a read-only owner is a failed disk.
            validity.store_state(FAILED);
            core.fail(LocalDbError::with_kind(
                std::io::ErrorKind::ConnectionAborted,
                format!("coordinator lease lapsed on owner partition {partition}"),
            ));
            return;
        }
    }
}

#[cfg(test)]
#[path = "coordinator_lease_tests.rs"]
mod coordinator_lease_tests;

impl super::RemoteDatabase {
    /// Close the engine with the renewers alive: they run through the owners'
    /// seal sweep and stop after it, so a slow close cannot lapse the lease.
    /// `begin_close` makes a renewal failure during the close a no-op.
    pub(super) fn close_engine<T>(
        &self,
        run: impl FnOnce(&Arc<WriteCore>) -> Result<T, LocalDbError>,
    ) -> Result<T, LocalDbError> {
        self.lease.begin_close();
        let result = run(&self.core);
        self.lease.stop();
        result
    }

    /// Final campaign teardown through the same close contract as `shutdown`.
    pub(super) fn shutdown_live_target_final(
        &self,
    ) -> Result<Vec<(u32, super::super::nvmeof::LiveTargetStats)>, LocalDbError> {
        self.close_engine(|core| core.shutdown_live_target_final())
    }
}

/// Default coordinator lease duration of a configuration that omits it.
pub(super) fn default_ms() -> u32 {
    5_000
}

#[cfg(test)]
impl super::RemoteDatabase {
    /// `open_local_live_fixture` with a renewal send hook: `hook(owner)`
    /// returns `false` to drop that owner's next renewal request, so a test can
    /// stop one owner's renewals without stopping the owner.
    pub(crate) fn open_local_live_fixture_with_lease_hook(
        path: &std::path::Path,
        shape: &crate::key_shape::KeyShape,
        config: super::RemoteDatabaseConfig,
        lease_hook: SendHook,
    ) -> Result<Self, super::super::storage::LocalDbError> {
        Self::open_inner_with(
            path,
            shape,
            config,
            crate::metrics::Metrics::new(),
            None,
            None,
            Some(lease_hook),
            Some(false),
        )
    }
}
