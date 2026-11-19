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
impl ReadService {
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
        reference: ValueRecordReference,
    ) {
        let lanes = self.lanes.read();
        let mut driver = lanes[0][0].driver.lock();
        crate::distributed::tcp::live_read::tests::install_owner_local(
            driver.live_cache.as_mut().unwrap(),
            node,
            reference,
        );
    }
}

impl ReadService {
    fn live_inline_fallback(
        &self,
        node: usize,
        counter: fn(&ReadDiagnostics) -> &AtomicU64,
        reason: &'static str,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        // Relaxed: diagnostics do not publish admission or completion state.
        counter(&self.state.diagnostics).fetch_add(1, Ordering::Relaxed);
        counter(&self.state.owner_diagnostics[node].1).fetch_add(1, Ordering::Relaxed);
        if let Some(trace) = trace {
            trace.event(reason);
        }
        Ok(None)
    }

    #[allow(clippy::too_many_arguments)] // Same scalar route/pin contract as owner inline reads.
    pub(super) fn read_live_inline(
        &self,
        node: usize,
        _epoch: u64,
        reference: &ValueRecordReference,
        extra_bytes: usize,
        pin: Option<IndexReadPin>,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Option<Vec<Mutation>>, LocalDbError> {
        let deadline = Instant::now() + self.timeout();
        let bytes = self
            .plan_member(reference)?
            .charge
            .checked_add(extra_bytes)
            .and_then(|n| n.checked_add(512))
            .ok_or_else(|| LocalDbError::new("read budget overflow"))?;
        if bytes > self.max_bytes() {
            return Err(LocalDbError::new(BYTE_BUDGET));
        }
        let fallback = |counter, reason| self.live_inline_fallback(node, counter, reason, trace);
        let mut flights = self.state.flights.lock();
        if flights.find(node, reference).is_some() {
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
        if !cache.inline_ready(reference) {
            return fallback(
                |d| &d.live_inline_fallback_grant,
                "live_inline_fallback_grant",
            );
        }
        if !cache.inline_credit(reference) {
            return fallback(
                |d| &d.live_inline_fallback_credit,
                "live_inline_fallback_credit",
            );
        }
        self.state.flight_ready[slot].begin();
        let flight = Arc::new(ReadFlight {
            node,
            references: vec![*reference],
            slot,
            completion: FlightCompletion::new(),
            _pin: pin,
            _credit: credit,
        });
        flights.insert(&flight);
        drop(flights);
        if let Some(trace) = trace {
            trace.classify_members(std::iter::empty());
            trace.lane(lane.index, slot);
            trace.event("live_inline_admitted");
        }
        // Relaxed: these counters only describe the chosen route. `read_rpcs`
        // counts this inline live read as one remote value request.
        self.state.read_rpcs.fetch_add(1, Ordering::Relaxed);
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
        let result = cache
            .read_inline_cached(&client, reference, deadline, trace)
            .map_err(|error| {
                super::super::record_first_failure(
                    &self.state.first_failure,
                    Some(reference.batch_address.partition),
                    None,
                    "live_inline_read",
                    &error,
                );
                self.state.poison();
                LocalDbError::component(error)
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
        kv_capture::routes(std::slice::from_ref(reference), true, slot as u64);
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
        result.map(Some)
    }
}
