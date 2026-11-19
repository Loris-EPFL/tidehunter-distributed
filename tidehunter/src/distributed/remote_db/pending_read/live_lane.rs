//! Live NVMe-oF lane worker: many requests in flight per lane.
//!
//! Owner: the worker and an idle scalar caller borrow `LiveReadCache` under
//! the lane's existing driver mutex, then its client mutex. The worker alone
//! moves an active cache out while sleeping; outstanding flight credits keep
//! callers out until it restores the cache. Both locks are free during sleep.
//! Input: a bounded `sync_channel` of `max_requests`; every entry holds a
//! global flight credit. The actor uses `try_send` and bounded overflow.
//! Exit: however the worker stops, it closes its inbox and completes every
//! request it holds or that is still in the channel, so no caller is left
//! waiting for it (`LaneInbox::close`). Only submitted device I/O has a
//! deadline; time in the inbox or parked for grant credit never expires.
//!
//! Requests are admitted in arrival order while the cache has room, finish in
//! any order, and each caller reads its own result slot, so per-caller result
//! order does not depend on completion order. The thread blocks on the channel
//! only when nothing is queued and nothing is in flight. With requests in
//! flight it sleeps in `LiveReadCache::wait` (CQ readiness or the waker), it
//! does not spin.
use super::*;
use crate::distributed::tcp::live_read::{
    LiveReadDone, LiveReadRequest, LiveReadSubmitError, LiveReadToken, LiveReadWaker,
};
use std::sync::OnceLock;

/// Idle time after which cached file grants are released.
const IDLE_GRANT_RELEASE: Duration = Duration::from_secs(1);
/// Longest sleep with requests in flight; the cache also stops at retry times.
const MAX_POLL_WAIT: Duration = Duration::from_secs(1);

/// The waker the actor uses after queueing a request on this lane.
pub(super) type LaneWaker = Arc<OnceLock<LiveReadWaker>>;

/// Set in a lane's `depth` once its worker takes no more requests. The rest
/// of `depth` counts requests the actor has counted and not yet uncounted or
/// handed to the worker. Both sides update `depth` with read-modify-writes,
/// so the atomic's modification order puts each actor count either before
/// the close (the worker waits for that request) or after it (the actor sees
/// this bit and fails the request itself).
pub(super) const LANE_CLOSED: usize = 1 << (usize::BITS - 1);
/// How long a closing worker waits for one racing request at a time.
const CLOSE_POLL: Duration = Duration::from_micros(100);

struct LaneInbox<'a> {
    state: &'a State,
    receiver: mpsc::Receiver<LiveLaneCommand>,
    depth: Arc<AtomicUsize>,
    /// Received, not yet admitted by the cache, oldest first. Bounded by
    /// `max_requests`: each request holds a global flight credit.
    waiting: VecDeque<Request>,
    flying: HashMap<LiveReadToken, Request>,
    drains: Vec<mpsc::SyncSender<Result<(), String>>>,
    done: Vec<LiveReadDone>,
    disconnected: bool,
}
impl LaneInbox<'_> {
    fn intake(&mut self, command: LiveLaneCommand) {
        match command {
            LiveLaneCommand::Request(request) => {
                self.depth.fetch_sub(1, Ordering::Relaxed);
                if let Some(trace) = &request.trace {
                    trace.event("live_worker_dequeued");
                }
                if self.state.failed.load(Ordering::Acquire) {
                    request.complete_nvme(Err(LocalDbError::new("read authority is poisoned")));
                } else {
                    self.waiting.push_back(request);
                }
            }
            LiveLaneCommand::Drain(reply) => self.drains.push(reply),
        }
    }
    /// Take what is already queued without waiting; at most one channel's worth.
    fn drain_channel(&mut self, bound: usize) {
        for _ in 0..bound {
            match self.receiver.try_recv() {
                Ok(command) => self.intake(command),
                Err(mpsc::TryRecvError::Empty) => return,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.disconnected = true;
                    return;
                }
            }
        }
    }
    /// Wait up to `timeout` for the next command. Returns false on timeout.
    fn wait_one(&mut self, timeout: Duration) -> bool {
        match self.receiver.recv_timeout(timeout) {
            Ok(command) => {
                self.intake(command);
                true
            }
            Err(mpsc::RecvTimeoutError::Timeout) => false,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                self.disconnected = true;
                true
            }
        }
    }
    /// Complete every request the cache returned. Returns whether any did.
    fn deliver(&mut self) -> bool {
        let delivered = !self.done.is_empty();
        for finished in self.done.drain(..) {
            let Some(request) = self.flying.remove(&finished.token) else {
                // A token the cache returned that this lane never admitted.
                self.state.poison();
                continue;
            };
            request.complete_nvme(finished.result.map_err(LocalDbError::component));
        }
        delivered
    }
    /// Fail every request still held: the lane is stopping.
    fn fail_all(&mut self, message: &str) {
        for (_, request) in self.flying.drain() {
            request.complete_nvme(Err(LocalDbError::new(message)));
        }
        self.fail_waiting(message);
    }
    /// Fail the requests not yet submitted; they own no device I/O.
    fn fail_waiting(&mut self, message: &str) {
        for request in self.waiting.drain(..) {
            request.complete_nvme(Err(LocalDbError::new(message)));
        }
    }
    /// Stop taking requests and complete every request already counted.
    ///
    /// A count that precedes the close belongs to an actor that has not yet
    /// sent or uncounted that request. Its next, non-blocking step either
    /// sends the request, which is received here, or finds the inbox full and
    /// uncounts it, so the wait is bounded by that step. Each received
    /// request is completed by `intake` (poisoned) or queued in `waiting` for
    /// `fail_waiting`. Drain commands left in the channel are dropped with it, as
    /// before; their callers see the reply channel close.
    fn close(&mut self) {
        // Relaxed: see `LANE_CLOSED`; only the modification order matters.
        self.depth.fetch_or(LANE_CLOSED, Ordering::Relaxed);
        while !self.disconnected && self.depth.load(Ordering::Relaxed) & !LANE_CLOSED != 0 {
            self.wait_one(CLOSE_POLL);
        }
    }
}

impl Drop for LaneInbox<'_> {
    /// Every exit path, including early returns and unwinding, completes the
    /// requests the worker holds or could still receive.
    fn drop(&mut self) {
        self.close();
        self.fail_all("live read lane stopped");
    }
}

pub(super) fn run(
    state: Arc<State>,
    lane: Lane,
    receiver: mpsc::Receiver<LiveLaneCommand>,
    depth: Arc<AtomicUsize>,
    waker: LaneWaker,
    epoch: u64,
    max_requests: usize,
) {
    let _poison = PoisonOnPanic(&state);
    // Built first so that its `Drop` completes queued requests on every exit,
    // including the early return below; it is dropped before `_poison`.
    let mut inbox = LaneInbox {
        state: &state,
        receiver,
        depth,
        waiting: VecDeque::new(),
        flying: HashMap::new(),
        drains: Vec::new(),
        done: Vec::new(),
        disconnected: false,
    };
    let mut driver = lane.driver.lock();
    #[cfg(test)]
    let node = driver.node;
    let cache = driver.live_cache.get_or_insert_with(Default::default);
    match cache.waker() {
        Ok(created) => {
            let _ = waker.set(created);
        }
        Err(error) => {
            super::super::record_first_failure(
                &state.first_failure,
                None,
                None,
                "live_read_waker",
                &error,
            );
            state.poison();
            return;
        }
    }
    drop(driver);
    let mut sleeping_cache = None;
    let mut pin_pending = state.pin_read_lanes;
    'lane: loop {
        if inbox.waiting.is_empty() && inbox.flying.is_empty() {
            if inbox.disconnected || state.failed.load(Ordering::Acquire) {
                break;
            }
            if !inbox.wait_one(IDLE_GRANT_RELEASE) {
                let mut driver = lane.driver.lock();
                if let Some(cache) = driver.live_cache.as_mut()
                    && cache.close(&mut lane.client.lock(), epoch).is_err()
                {
                    state.poison();
                }
            }
        }
        #[cfg(test)]
        if let Some((entered, resume)) = state.reactor_pause[node].lock().take() {
            // Reuse the reactor test hook before taking either lane mutex.
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        let mut driver = lane.driver.lock();
        let Some(mut cache) = sleeping_cache.take().or_else(|| driver.live_cache.take()) else {
            state.poison();
            break;
        };
        inbox.drain_channel(max_requests);
        if !inbox.drains.is_empty() {
            let mut client = lane.client.lock();
            let result = cache
                .drain(&mut client, epoch, &mut inbox.done)
                .and_then(|()| cache.close(&mut client, epoch))
                .map_err(|error| error.to_string());
            drop(client);
            if result.is_err() {
                state.poison();
            }
            inbox.deliver();
            for reply in inbox.drains.drain(..) {
                // Release: CQ retirement and grant close precede drain completion.
                state.live_drains.fetch_sub(1, Ordering::Release);
                let _ = reply.send(result.clone());
            }
        }
        let mut client = lane.client.lock();
        while let Some(mut request) = inbox.waiting.pop_front() {
            // Time spent in the inbox is not I/O time and never expires a
            // request: the cache bounds each member from its device submission.
            if state.failed.load(Ordering::Acquire) {
                request.complete_nvme(Err(LocalDbError::new("read authority is poisoned")));
                continue;
            }
            let live = LiveReadRequest {
                references: std::mem::take(&mut request.references),
                timeout: request.timeout,
                trace: request.trace.clone(),
            };
            match cache.submit(&mut client, epoch, live) {
                Ok(token) => {
                    if let Some(trace) = &request.trace {
                        trace.event("live_lane_admitted");
                    }
                    inbox.flying.insert(token, request);
                }
                Err(LiveReadSubmitError::Full(live)) => {
                    // The lane is at its request limit: keep order, retry after a poll.
                    request.references = live.references;
                    inbox.waiting.push_front(request);
                    break;
                }
                Err(LiveReadSubmitError::Failed(error)) => {
                    request.complete_nvme(Err(LocalDbError::component(error)));
                }
            }
        }
        // Harvest CQEs, start parked members, and collect requests that failed
        // admission (they come back from poll with their error).
        let polled = cache.poll(&mut client, epoch, &mut inbox.done);
        let pin_device = if pin_pending {
            cache.namespace_device().map(|device| {
                pin_pending = false;
                ReadLanePlacement {
                    device,
                    counters: client.read_lane_pin_counters(),
                }
            })
        } else {
            None
        };
        drop(client);
        if let Err(error) = polled {
            super::super::record_first_failure(
                &state.first_failure,
                None,
                None,
                "live_read_poll",
                &error,
            );
            state.poison();
            driver.live_cache = Some(cache);
            break 'lane;
        }
        let delivered = inbox.deliver();
        // Callers time out against the device I/O each request has
        // outstanding, not against time its members wait for grant credit.
        for (token, request) in &mut inbox.flying {
            request.set_io_deadline(cache.io_deadline(*token));
        }
        if cache.in_flight() == 0 {
            driver.live_cache = Some(cache);
            drop(driver);
            pin_live_read_lane(pin_device, lane.index);
            if !inbox.waiting.is_empty() {
                inbox.wait_one(Duration::from_micros(100));
            }
            continue;
        }
        // Only active flights move the cache out of the driver. Their global
        // credits and lane outstanding count prevent inline admission.
        drop(driver);
        pin_live_read_lane(pin_device, lane.index);
        if !delivered {
            // Sleeps until a CQE is ready, the actor wakes us, or a deadline.
            if let Err(error) = cache.wait(MAX_POLL_WAIT) {
                // Shutdown below still drains every DMA buffer after a lost wake.
                super::super::record_first_failure(
                    &state.first_failure,
                    None,
                    None,
                    "live_read_wait",
                    &error,
                );
                state.poison();
                sleeping_cache = Some(cache);
                break 'lane;
            }
        }
        sleeping_cache = Some(cache);
    }
    // Fail queued and unsubmitted requests now, before the locks and the I/O
    // shutdown below; only submitted requests wait for that shutdown. The
    // inbox's `Drop` repeats this on the early returns.
    inbox.close();
    inbox.fail_waiting("live read lane stopped");
    let mut driver = lane.driver.lock();
    let Some(mut cache) = sleeping_cache.take().or_else(|| driver.live_cache.take()) else {
        state.poison();
        return;
    };
    let mut client = lane.client.lock();
    if cache.shutdown(&mut client, epoch, &mut inbox.done).is_err() {
        state.poison();
    }
    let unreleased = cache.take_unreleased();
    drop(cache);
    drop(driver);
    // Poison publication never waits for idle connection pins. This existing
    // bounded lane worker retires them after dropping its I/O and grant leases.
    if state.failed.load(Ordering::Acquire)
        && let Err(error) = client.drain_poisoned_live_pool()
    {
        super::super::record_first_failure(
            &state.first_failure,
            None,
            None,
            "live_poison_pool_drain",
            &error,
        );
        eprintln!("live poison controller cleanup failed; pins retained until fence: {error}");
    }
    drop(client);
    inbox.deliver();
    inbox.fail_all("live read lane stopped");
    if !unreleased.is_empty() {
        state
            .live_unreleased
            .lock()
            .push((lane.client, epoch, unreleased));
        state.poison();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poisoned_live_actor_releases_idle_pool_after_terminal_publication() {
        use crate::distributed::remote_db::pipeline_tests::Fixture;
        use crate::distributed::tcp::export_pin_tests::HistoryPool;
        let mut history = HistoryPool::new();
        let base = Fixture::new_with_root(
            tempdir::TempDir::new("poison-idle-pool").unwrap(),
            1,
            128,
            KeyShape::from_yaml(&history.owner.config.shape_yaml).unwrap(),
            2,
            |_| {},
            |_| {},
        );
        let client = std::mem::replace(&mut history.client, history.owner.client());
        let mut config = base.config.clone();
        let owner = &history.owner.config.partition;
        config.storage.database = owner.inventory.database;
        config.storage.partitions = owner.inventory.partitions.clone();
        config.storage.batch_limits = owner.batch_limits;
        config.storage.group_limits = owner.group_limits;
        config.storage.fragment_bytes = owner.fragment_bytes;
        config.storage.wal_file_bytes = owner.wal_file_bytes;
        config.storage.max_batches_per_partition = owner.max_batches;
        config.storage.max_reference_metadata_bytes = owner.max_reference_metadata_bytes;
        config.endpoints[0].partition = owner.partition;
        config.endpoints[0].address = history.owner.endpoint.clone();
        config.endpoints[0].server_name = history.owner.tls.server_name.clone();
        config.tls = history.owner.tls.clone();
        config.read_lanes = Some(1);
        config.live_nvme = client.live_config_for_test().unwrap();
        let readers = ReadService::from_clients(
            &config,
            2,
            vec![client],
            &mut RecoveryStats::default(),
            Arc::new(std::sync::OnceLock::new()),
        )
        .unwrap();
        assert!(
            history.poison_owner(),
            "idle export pin must not delay terminal publication"
        );
        assert_eq!(history.pin_count(), 1);
        readers.poison();
        // Joining the existing actor is a deterministic cleanup barrier, not a
        // timeout/sleep assertion about background scheduling.
        readers.stop();
        assert_eq!(history.pin_count(), 0);
        history
            .pool
            .shutdown(Instant::now() + Duration::from_secs(5))
            .unwrap();
    }
}
