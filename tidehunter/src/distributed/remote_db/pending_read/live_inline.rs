//! Scalar cached-grant reads borrow the existing lane, never its worker queue.
use super::*;

#[cfg(test)]
pub(in crate::distributed::remote_db) struct BenchIdleLiveLane(mpsc::SyncSender<()>);

#[cfg(test)]
impl Drop for BenchIdleLiveLane {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

#[cfg(test)]
pub(in crate::distributed::remote_db) struct BenchHeldCompletion(Option<mpsc::SyncSender<()>>);

#[cfg(test)]
impl BenchHeldCompletion {
    /// Let the held terminal completion publish. Every exit path releases it,
    /// so a failing assertion cannot strand the lane worker.
    pub(in crate::distributed::remote_db) fn release(mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.try_send(());
        }
    }
}

#[cfg(test)]
impl Drop for BenchHeldCompletion {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.try_send(());
        }
    }
}

#[cfg(test)]
impl ReadService {
    /// Hold the next terminal completion after its result is stored and before
    /// its slot retires. Returns the notice it sends and the release handle.
    pub(in crate::distributed::remote_db) fn bench_hold_completion(
        &self,
    ) -> (mpsc::Receiver<()>, BenchHeldCompletion) {
        let (entered, held) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        *self.state.completion_pause.lock() = Some((entered, resume));
        (held, BenchHeldCompletion(Some(release)))
    }

    /// Park outside the driver lock before installing a cached fixture grant.
    pub(in crate::distributed::remote_db) fn bench_park_live_lane(
        &self,
        node: usize,
    ) -> BenchIdleLiveLane {
        let (entered, waiting) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        let parked = BenchIdleLiveLane(release);
        *self.state.reactor_pause[node].lock() = Some((entered, resume));
        waiting.recv_timeout(self.timeout).unwrap();
        parked
    }

    pub(in crate::distributed::remote_db) fn bench_install_owner_local(
        &self,
        node: &crate::distributed::tcp::StorageNode,
        position: VersionPosition,
    ) {
        let lanes = self.lanes.read();
        let mut driver = lanes[0][0].driver.lock();
        crate::distributed::tcp::live_read::tests::install_owner_local(
            driver.live_cache.as_mut().unwrap(),
            node,
            position,
        );
    }

    /// Register the fixture namespace of the permanent per-owner export that
    /// serves `position`, so the lane's `open_permanent` connects the owner's
    /// WAL file instead of a kernel controller. The lane must be parked so its
    /// cache is in the driver.
    pub(in crate::distributed::remote_db) fn bench_register_permanent_local(
        &self,
        node: usize,
        position: VersionPosition,
        path: &std::path::Path,
    ) {
        let lanes = self.lanes.read();
        let lane = &lanes[0][node];
        let driver = lane.driver.lock();
        assert!(
            driver.live_cache.is_some(),
            "the parked lane owns its cache"
        );
        let client = lane.client.lock();
        crate::distributed::tcp::live_read::tests::register_permanent_local(
            &client, &position, path,
        );
        drop(client);
    }

    /// Arm the next grant this lane opens as a lost controller whose read
    /// completes short.
    pub(in crate::distributed::remote_db) fn bench_arm_short_read_after_open(&self, node: usize) {
        let lanes = self.lanes.read();
        let lane = &lanes[0][node];
        let mut driver = lane.driver.lock();
        driver
            .live_cache
            .as_mut()
            .expect("the parked lane owns its cache")
            .arm_short_read_after_open();
    }
}

impl ReadService {
    fn live_inline_fallback(
        &self,
        node: usize,
        counter: fn(&ReadDiagnostics) -> &AtomicU64,
        reason: &'static str,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Option<Vec<Bytes>>, LocalDbError> {
        // Relaxed: diagnostics do not publish admission or completion state.
        counter(&self.state.diagnostics).fetch_add(1, Ordering::Relaxed);
        counter(&self.state.owner_diagnostics[node].1).fetch_add(1, Ordering::Relaxed);
        if let Some(trace) = trace {
            trace.event(reason);
        }
        Ok(None)
    }

    pub(super) fn read_live_inline(
        &self,
        node: usize,
        position: &VersionPosition,
        extra_bytes: usize,
        deadline: Instant,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Option<Vec<Bytes>>, LocalDbError> {
        let bytes = self
            .plan_member(position)?
            .charge
            .checked_add(extra_bytes)
            .and_then(|n| n.checked_add(512))
            .ok_or_else(|| LocalDbError::new("read budget overflow"))?;
        if bytes > self.max_bytes() {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let fallback = |counter, reason| self.live_inline_fallback(node, counter, reason, trace);
        let mut flights = self.state.flights.lock();
        if flights.find(node, position).is_some() {
            return fallback(
                |d| &d.live_inline_fallback_coalesced,
                "live_inline_fallback_coalesced",
            );
        }
        let Some(slot) = flights.available_slot(&self.state.flight_ready) else {
            return fallback(
                |d| &d.live_inline_fallback_credit,
                "live_inline_fallback_credit",
            );
        };
        if !flights.reserve_entries(1, &self.state.flight_ready) {
            return fallback(
                |d| &d.live_inline_fallback_credit,
                "live_inline_fallback_credit",
            );
        }
        let (lane, reservation, credit) = {
            let mut retained = self.state.retained.lock();
            // Acquire: pair with close/poison and admission-pause publication.
            if self.failed() || self.state.stopped.load(Ordering::Acquire) {
                return Err(LocalDbError::new("read authority is poisoned or closed"));
            }
            // Acquire: observe drains published before worker handoff.
            if self.state.live_drains.load(Ordering::Acquire) != 0 {
                return fallback(
                    |d| &d.live_inline_fallback_drain,
                    "live_inline_fallback_drain",
                );
            }
            if self.state.admission_paused.load(Ordering::Acquire) {
                return fallback(
                    |d| &d.live_inline_fallback_paused,
                    "live_inline_fallback_paused",
                );
            }
            if retained.0 == self.max_requests || bytes > self.max_bytes() - retained.1 {
                return fallback(
                    |d| &d.live_inline_fallback_credit,
                    "live_inline_fallback_credit",
                );
            }
            let lanes = self.lanes.read();
            let lanes = lanes
                .get(node)
                .ok_or_else(|| LocalDbError::new("read reactor stopped"))?;
            // Relaxed: round robin is only a fairness hint, not ownership.
            let next = self.next[node].fetch_add(1, Ordering::Relaxed);
            let mut selected = None;
            for offset in 0..lanes.len() {
                let lane = &lanes[next.wrapping_add(offset) % lanes.len()];
                let Some(mut scheduling) = lane.scheduling.try_lock() else {
                    continue;
                };
                // Acquire: observe retirement of all queued and active requests.
                if scheduling.inline_reserved || lane.outstanding.load(Ordering::Acquire) != 0 {
                    continue;
                }
                scheduling.inline_reserved = true;
                // AcqRel: publish ownership before releasing the scheduling lock.
                lane.outstanding.fetch_add(1, Ordering::AcqRel);
                selected = Some(lane.clone());
                break;
            }
            let Some(lane) = selected else {
                return fallback(
                    |d| &d.live_inline_fallback_busy,
                    "live_inline_fallback_busy",
                );
            };
            retained.0 += 1;
            retained.1 += bytes;
            let reservation = InlineLaneReservation(lane.scheduling.clone());
            let credit = Arc::new(Credit {
                state: self.state.clone(),
                bytes,
                requests: 1,
            });
            (lane, reservation, credit)
        };
        let lane_credit = LaneCredit {
            outstanding: lane.outstanding.clone(),
        };
        // Lock order is driver, client. Neither acquisition may wait. Outstanding
        // covers inbox, overflow and worker-local waiting/flying requests, not
        // merely the contents currently visible in the cache.
        let Some(mut driver) = lane.driver.try_lock() else {
            #[cfg(test)]
            // Relaxed: test-only diagnostics do not publish lane ownership.
            self.state
                .inline_mutex_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return fallback(
                |d| &d.live_inline_fallback_busy,
                "live_inline_fallback_busy",
            );
        };
        let Some(client) = lane.client.try_lock() else {
            #[cfg(test)]
            // Relaxed: test-only diagnostics do not publish lane ownership.
            self.state
                .inline_mutex_fallbacks
                .fetch_add(1, Ordering::Relaxed);
            return fallback(
                |d| &d.live_inline_fallback_busy,
                "live_inline_fallback_busy",
            );
        };
        let Some(cache) = driver.live_cache.as_mut() else {
            return fallback(
                |d| &d.live_inline_fallback_busy,
                "live_inline_fallback_busy",
            );
        };
        if !cache.inline_ready(position) {
            return fallback(
                |d| &d.live_inline_fallback_grant,
                "live_inline_fallback_grant",
            );
        }
        if !cache.inline_credit(position) {
            return fallback(
                |d| &d.live_inline_fallback_credit,
                "live_inline_fallback_credit",
            );
        }
        self.state.flight_ready[slot].begin();
        let flight = Arc::new(ReadFlight {
            node,
            positions: vec![*position],
            slot,
            completion: FlightCompletion::new(),
            // Set at submission below: admission locks and pauses do not count.
            io_deadline: Mutex::new(None),
            _credit: credit,
        });
        flights.insert(&flight);
        drop(flights);
        if let Some(trace) = trace {
            trace.classify_members(std::iter::empty());
            trace.lane(lane.index, slot);
            trace.event("live_inline_admitted");
        }
        // Relaxed: these counters only describe the chosen route.
        // `live_read_submissions` counts this inline live read as one remote
        // value request.
        self.state
            .live_read_submissions
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .diagnostics
            .live_inline
            .fetch_add(1, Ordering::Relaxed);
        self.state.owner_diagnostics[node]
            .1
            .live_inline
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        if let Some((entered, resume)) = self.state.inline_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        let _poison = PoisonOnPanic(&self.state);
        let sampled = self.state.diagnostics.select();
        let started = sampled.then(Instant::now);
        // The flight timer starts at transport submission and never outlives
        // the caller's absolute deadline; time in admission is not I/O time.
        *flight.io_deadline.lock() = Some(deadline);
        let result = cache
            .read_inline_cached(&client, position, deadline, trace)
            .map_err(|error| {
                let mapped = super::transport_error(error);
                if mapped.kind() == std::io::ErrorKind::TimedOut {
                    // A deadline is transient and the read is idempotent. The
                    // flight below owns its buffer until the terminal CQE, so
                    // no retry can touch it; the caller gets a typed timeout.
                    self.state.note_read_timeout(node);
                } else if mapped.kind() != std::io::ErrorKind::ConnectionAborted
                    && !super::member_read_is_retired(&mapped)
                {
                    // A retired member address is not a lane failure: the value
                    // path re-resolves it once, so poisoning here would
                    // fail-stop the engine and kill the only recovery route.
                    // A lost controller is not a lane failure either: the
                    // inline route never sets up a controller, so it declines
                    // below and the lane reconnects.
                    super::super::record_first_failure(
                        &self.state.first_failure,
                        Some(position.address.partition),
                        None,
                        "live_inline_read",
                        &mapped,
                    );
                    self.state.poison();
                }
                mapped
            });
        if let Some(started) = started {
            let ns = elapsed(started);
            for diagnostics in [
                &self.state.diagnostics,
                &self.state.owner_diagnostics[node].1,
            ] {
                diagnostics.inline_complete(ns);
                // Relaxed: live timing annotations are statistics, not handoff.
                diagnostics.nvme_completed.fetch_add(1, Ordering::Relaxed);
                diagnostics.nvme_flight_ns.fetch_add(ns, Ordering::Relaxed);
                diagnostics
                    .nvme_flight_count
                    .fetch_add(1, Ordering::Relaxed);
                diagnostics
                    .nvme_flight_max_ns
                    .fetch_max(ns, Ordering::Relaxed);
                if result.is_err() {
                    diagnostics.errors.fetch_add(1, Ordering::Relaxed);
                    diagnostics
                        .failed_owner_profile_not_applicable
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        #[cfg(test)]
        if let Some((entered, resume)) = self.state.inline_retirement_pause.lock().take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        let mut flights = self.state.flights.lock();
        // Every new joiner must upgrade the registry's weak flight under
        // this lock. No production weak upgrade occurs elsewhere, so one
        // strong owner means only this producer can still hold the result.
        // An out-of-lock upgrade/new consumer would break this invariant.
        if Arc::strong_count(&flight) == 1 {
            // Unregister in the same critical section as the decision.
            // The not-ready signal prevents slot reuse until retirement.
            flights.clear_slot(slot);
        } else {
            #[cfg(test)]
            // Relaxed: test-only allocation-path diagnostics, not handoff.
            self.state
                .inline_result_copies
                .fetch_add(1, Ordering::Relaxed);
            *flight.completion.result.lock() = Some(match &result {
                Ok(values) => Ok(values.clone()),
                Err(error) => Err(copied_read_error(error)),
            });
        }
        if let Some(trace) = trace {
            trace.event(if result.is_err() {
                "flight_error"
            } else {
                "flight_completed"
            });
        }
        drop(client);
        drop(driver);
        drop(lane_credit);
        drop(reservation);
        // A caller missing the unregistered flight must see a released lane,
        // not transient backpressure from this producer's retired I/O.
        drop(flights);
        drop(flight);
        self.state.flight_ready[slot].retire();
        self.state.notify_admission();
        match result {
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionAborted => {
                // The cached grant's controller was verifiably deleted. This
                // route never sets up a controller, so decline the member: the
                // caller submits it through the lane, which reconnects and
                // retries the idempotent read inside its own budget.
                self.live_inline_fallback(
                    node,
                    |d| &d.live_inline_fallback_grant,
                    "live_inline_fallback_controller_lost",
                    trace,
                )
            }
            other => other.map(Some),
        }
    }
}
