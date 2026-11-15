//! Live NVMe-oF lane worker: many requests in flight per lane.
//!
//! Owner: the worker and an idle scalar caller borrow `LiveReadCache` under
//! the lane's existing driver mutex, then its client mutex. The worker alone
//! moves an active cache out while sleeping; outstanding flight credits keep
//! callers out until it restores the cache. Both locks are free during sleep.
//! Input: a bounded `sync_channel` of `max_requests`; every entry holds a
//! global flight credit. The actor uses `try_send` and bounded overflow.
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
/// Longest sleep with requests in flight; the cache also stops at deadlines.
const MAX_POLL_WAIT: Duration = Duration::from_secs(1);

/// The waker the actor uses after queueing a request on this lane.
pub(super) type LaneWaker = Arc<OnceLock<LiveReadWaker>>;

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
            request.complete_nvme(
                finished
                    .result
                    .map(|values| (values, None))
                    .map_err(LocalDbError::component),
            );
        }
        delivered
    }
    /// Fail every request still held: the lane is stopping.
    fn fail_all(&mut self, message: &str) {
        for (_, request) in self.flying.drain() {
            request.complete_nvme(Err(LocalDbError::new(message)));
        }
        for request in self.waiting.drain(..) {
            request.complete_nvme(Err(LocalDbError::new(message)));
        }
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
    'lane: loop {
        if inbox.waiting.is_empty() && inbox.flying.is_empty() {
            if inbox.disconnected {
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
        while let Some(request) = inbox.waiting.pop_front() {
            if state.failed.load(Ordering::Acquire) || Instant::now() >= request.deadline {
                // An expired request poisons the authority, as on the transport lanes.
                let expired = !state.failed.load(Ordering::Acquire);
                if expired {
                    state.poison();
                }
                request.complete_nvme(Err(LocalDbError::new(if expired {
                    "remote read admission deadline expired"
                } else {
                    "read authority is poisoned"
                })));
                continue;
            }
            let mut request = request;
            let live = LiveReadRequest {
                references: std::mem::take(&mut request.references),
                deadline: request.deadline,
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
    let mut driver = lane.driver.lock();
    let Some(mut cache) = sleeping_cache.take().or_else(|| driver.live_cache.take()) else {
        state.poison();
        return;
    };
    let mut client = lane.client.lock();
    if cache.shutdown(&mut client, epoch, &mut inbox.done).is_err() {
        state.poison();
    }
    drop(client);
    inbox.deliver();
    inbox.fail_all("live read lane stopped");
    let unreleased = cache.take_unreleased();
    if !unreleased.is_empty() {
        state
            .live_unreleased
            .lock()
            .push((lane.client, epoch, unreleased));
        state.poison();
    }
}
