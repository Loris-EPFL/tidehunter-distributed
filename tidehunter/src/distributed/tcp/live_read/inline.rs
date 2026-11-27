//! The inline (exclusive-lane) read path.
//!
//! Split out of `live_read.rs` to hold that file under its length ratchet;
//! the methods stay inherent on `LiveReadCache` with unchanged visibility.

use super::*;
use minibytes::Bytes;

/// One inline read failure. A lost controller is retried inside the same call;
/// every other class keeps its existing verdict and poisons the caller.
enum InlineFailure {
    Transient(TcpError),
    LostController(TcpError),
}

impl From<TcpError> for InlineFailure {
    fn from(error: TcpError) -> Self {
        Self::Transient(error)
    }
}

/// Whether this failed inline read lost its controller.
///
/// True only for a short namespace read, `EIO`/`EBADF` or the typed cause from
/// a grant whose pinned controller is verifiably not live. A live controller
/// keeps every one of those fatal. The grant is marked broken but **not**
/// poisoning, and its dead lease is evicted, so the lane retry opens a new
/// controller. A failed eviction keeps this verdict: the reconnect is still
/// possible, so a verifiably lost controller never becomes a media verdict.
fn inline_controller_lost(
    grant: &mut Grant,
    client: &Client,
    error: &LiveIoError,
    deadline: Instant,
    trace: Option<&ReadTraceChild>,
) -> bool {
    let LiveIoError::Io(error) = error else {
        return false;
    };
    if !read_controller_lost(error, deadline, || grant_controller_lost(grant)) {
        return false;
    }
    let Some(lease) = grant.lease.as_ref() else {
        return false;
    };
    // One String and no Arc clone: only the lost-controller error path.
    let nqn = lease.binding().nqn.clone();
    grant.broken = true;
    grant.lost = true;
    // Best effort: a failed teardown must not turn a verifiably lost controller
    // into a media verdict. The member is declined to the lane, whose retry
    // reconnects; the next connect reconciles an unconfirmed intent.
    if let Some(pool) = client.live_pool.as_ref() {
        let teardown = client.live_config.as_ref().map_or(deadline, |config| {
            crate::distributed::tcp::controller_teardown_deadline(config.connect_timeout_ms)
        });
        let _ = pool.evict_lost(&nqn, teardown);
    }
    if let Some(counters) = &client.live_counters {
        counters.record_read_controller_resume();
    }
    if let Some(trace) = trace {
        trace.event("live_member_requeued_lost_controller");
    }
    true
}

impl LiveReadCache {
    /// Only an exclusive idle lane may borrow its cached grant. No control
    /// exchange, controller setup, eviction or pin release runs on this path.
    pub(crate) fn inline_ready(&self, position: &VersionPosition) -> bool {
        self.requests.is_empty()
            && self.unreleased.is_empty()
            && self.grants.iter().all(|grant| grant.outstanding() == 0)
            && self.find_grant(position).is_some()
    }

    pub(crate) fn inline_credit(&self, position: &VersionPosition) -> bool {
        let Some(index) = self.find_grant(position) else {
            return false;
        };
        let grant = &self.grants[index];
        let offset = position.address.offset % grant.scope.file_bytes();
        let len = position.address.len as usize;
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > grant.scope.file_bytes())
        {
            return false;
        }
        let io = grant.inline_io();
        let (slots, bytes) = io.credit();
        let charge = match grant.scope.slot(position) {
            Some(slot) => io.read_request_charge_at(slot, &[(offset, len)]),
            None => io.read_request_charge(&[(offset, len)]),
        };
        charge.is_ok_and(|(needed_slots, charge)| needed_slots <= slots && charge <= bytes)
    }

    /// Read one member under the lane's exclusive driver/client ownership.
    /// The cached grant retains the file pin through every terminal CQE,
    /// including deadline and wait failures.
    ///
    /// The caller poisons on any error **except** a retired-member failure,
    /// which is the one error a re-resolve repairs: it is returned kind-preserved
    /// as `NotFound` and becomes a typed eviction if the retry also fails
    /// (`pending_read::member_read_is_retired` is the single rule both sides
    /// use). Today no source on this path produces that kind — a cached grant
    /// reads an already-open namespace, so an unlinked inode stays readable, and
    /// lane failures arrive as I/O or loop errors — so the exemption is
    /// defensive, not a fix for an observed fail-stop.
    ///
    /// Why skipping the poison is safe. The ring owns the in-flight DMA buffer
    /// until the terminal CQE (deadline and wait failures included), and the
    /// cached grant's pin is released only after it. Poison is the read
    /// service's fail-stop, not the buffer-lifetime mechanism, so exempting a
    /// retired member can neither free nor reuse an in-flight buffer.
    ///
    /// The exemption stays bounded. A deadline is reported as `TimedOut` and
    /// never poisons: nothing was submitted or the terminal CQE retired, and
    /// the caller may retry the idempotent read. A ring failure is `Other` and
    /// still poisons. Today `NotFound` can only come from geometry/resolve or
    /// a completed reply; if a `NotFound` ever arrives from the I/O layer
    /// itself, narrow the exemption to the geometry and resolve stages.
    ///
    /// # Lost controller
    ///
    /// This path never sets up a controller. When the cached grant's controller
    /// was verifiably deleted, it evicts the dead lease, marks the grant broken
    /// without poisoning its subsystem, and returns a `ConnectionAborted`
    /// class. The caller declines the inline route, and the lane retries the
    /// idempotent read and reconnects inside its own budget.
    pub(crate) fn read_inline_cached(
        &mut self,
        client: &Client,
        position: &VersionPosition,
        deadline: Instant,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Vec<Bytes>, TcpError> {
        let result = match self.read_inline_cached_inner(client, position, deadline, trace) {
            Ok(bytes) => Ok(bytes),
            Err(InlineFailure::Transient(error)) => Err(error),
            Err(InlineFailure::LostController(error)) => Err(TcpError::with_kind(
                std::io::ErrorKind::ConnectionAborted,
                error.to_string(),
            )),
        };
        if result.is_err()
            && let Some(counters) = &client.live_counters
        {
            counters.record_error();
        }
        result
    }

    fn read_inline_cached_inner(
        &mut self,
        client: &Client,
        position: &VersionPosition,
        deadline: Instant,
        trace: Option<&ReadTraceChild>,
    ) -> Result<Vec<Bytes>, InlineFailure> {
        let index = self
            .find_grant(position)
            .ok_or_else(|| TcpError::new("inline live grant disappeared"))?;
        let config = client
            .live_config
            .as_ref()
            .ok_or_else(|| TcpError::new("live NVMe is not configured"))?;
        let deadline = deadline.min(Instant::now() + Duration::from_millis(config.io_timeout_ms));
        self.clock += 1;
        let grant = &mut self.grants[index];
        grant.last_used = self.clock;
        let ranges = {
            let _validation =
                LiveReadTiming::start(&client.live_counters, LiveTimingStage::Validation);
            [member_range(grant, position)?]
        };
        let flight = self.next_flight;
        self.next_flight += 1;
        let _io = LiveReadTiming::start(&client.live_counters, LiveTimingStage::ReadIo);
        let slot = grant.scope.slot(position);
        let result = match slot {
            Some(slot) => grant
                .inline_io_mut()
                .submit_reads_at(flight, slot, &ranges, deadline, trace),
            None => grant
                .inline_io_mut()
                .submit_reads_traced(flight, &ranges, deadline, trace),
        };
        match result.map_err(live_error)? {
            LiveSubmitOutcome::Submitted => {}
            LiveSubmitOutcome::Full => {
                return Err(InlineFailure::Transient(TcpError::new(
                    "inline live credit disappeared",
                )));
            }
        }
        if let Some(counters) = &client.live_counters {
            counters.record_grant_hit();
            counters.record_lane_inflight(1, grant.outstanding() as u64);
        }
        // Past the deadline, retain ownership until the terminal CQE.
        // LiveIo bounds both polled waits and terminal retirement.
        let bound = deadline + DRAIN_BOUND;
        #[cfg(test)]
        if let Some((entered, resume)) = self.inline_submitted_pause.take() {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        }
        while Instant::now() < bound {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timeout = if remaining.is_zero() {
                Duration::from_millis(100)
            } else {
                remaining
            };
            let waited = grant.inline_io_mut().wait(timeout);
            if let Err(error) = waited {
                drain_failed_io(grant);
                let lost = inline_controller_lost(grant, client, &error, deadline, trace);
                if !lost {
                    grant.broken = true;
                    if let GrantScope::Subsystem {
                        generation,
                        subsystem,
                        ..
                    } = &grant.scope
                    {
                        self.failed_subsystems.insert((*generation, *subsystem));
                    }
                }
                fold_stats(client.live_counters.as_deref(), grant);
                return Err(if lost {
                    InlineFailure::LostController(live_error(error))
                } else {
                    InlineFailure::Transient(live_error(error))
                });
            }
            let completions = match grant.inline_io_mut().progress() {
                Ok(completions) => completions,
                Err(error) => {
                    drain_failed_io(grant);
                    let lost = inline_controller_lost(grant, client, &error, deadline, trace);
                    if !lost {
                        grant.broken = true;
                        if let GrantScope::Subsystem {
                            generation,
                            subsystem,
                            ..
                        } = &grant.scope
                        {
                            self.failed_subsystems.insert((*generation, *subsystem));
                        }
                    }
                    fold_stats(client.live_counters.as_deref(), grant);
                    return Err(if lost {
                        InlineFailure::LostController(live_error(error))
                    } else {
                        InlineFailure::Transient(live_error(error))
                    });
                }
            };
            fold_stats(client.live_counters.as_deref(), grant);
            if let Some(completion) = completions.into_iter().next() {
                let _delivery = client
                    .live_counters
                    .as_ref()
                    .map(|counters| LiveReadTiming {
                        counters: counters.clone(),
                        stage: LiveTimingStage::Delivery,
                        started: completion.completed_at,
                    });
                if completion.id != flight {
                    return Err(InlineFailure::Transient(TcpError::new(
                        "unexpected inline live completion",
                    )));
                }
                let result = match completion.result {
                    Ok(result) => result,
                    Err(error) => {
                        let io_error = matches!(
                            error,
                            crate::distributed::nvmeof::live_io::LiveIoError::Io(_)
                        );
                        let lost = io_error
                            && inline_controller_lost(grant, client, &error, deadline, trace);
                        if io_error
                            && !lost
                            && let GrantScope::Subsystem {
                                generation,
                                subsystem,
                                ..
                            } = &grant.scope
                        {
                            grant.broken = true;
                            self.failed_subsystems.insert((*generation, *subsystem));
                        }
                        return Err(if lost {
                            InlineFailure::LostController(live_error(error))
                        } else {
                            InlineFailure::Transient(live_error(error))
                        });
                    }
                };
                let LiveIoResult::Reads(mut records) = result else {
                    return Err(InlineFailure::Transient(TcpError::new(
                        "unexpected live write completion",
                    )));
                };
                if records.len() != 1 {
                    return Err(InlineFailure::Transient(TcpError::new(
                        "live read count mismatch",
                    )));
                }
                let bytes = records.pop().ok_or_else(|| {
                    InlineFailure::Transient(TcpError::new("missing live read result"))
                })?;
                if let Some(trace) = trace {
                    trace.event("live_member_completed");
                }
                return Ok(vec![bytes]);
            }
        }
        drain_failed_io(grant);
        grant.broken = true;
        if let GrantScope::Subsystem {
            generation,
            subsystem,
            ..
        } = &grant.scope
        {
            self.failed_subsystems.insert((*generation, *subsystem));
        }
        Err(InlineFailure::Transient(TcpError::new(
            "inline live read did not retire",
        )))
    }
}
