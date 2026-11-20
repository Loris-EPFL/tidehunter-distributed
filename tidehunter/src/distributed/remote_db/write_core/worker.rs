// The transport worker uses its enclosing database's private control types.
#[allow(clippy::wildcard_imports)]
use super::super::*;
use super::allocation::ExtentQueue;
use crate::distributed::hybrid::loader::AllocationView;

/// Move every per-batch extent the leased writer allocated since the last call
/// into this stream's allocation queue, in WAL offset order.
///
/// A frame is written contiguously by one worker, so reservation order is
/// offset order. A frame whose reservation is not yet recorded only makes the
/// next snapshot's `replay_from` over-wide (its `tail` is the end of the last
/// recorded extent, which is where the unrecorded frame starts), never narrow;
/// a reservation is what lets the queue drop an extent the snapshot's stored
/// cut already covers.
///
/// The queue's cap hits are republished here, the only place the queue grows, so
/// `PipelineStats::allocation_cap_hits` is readable without touching the worker
/// thread's state.
fn record_lease_reservations(
    client: &mut Client,
    queue: &mut ExtentQueue,
    accounting: &WorkerAccounting,
) {
    client.take_lease_reservations(|version, address| {
        let end = address.offset.saturating_add(u64::from(address.len));
        queue.reserve(version, address.offset, end);
    });
    accounting.record_allocation_cap_hits(queue.cap_hits());
}

impl ClientWorker {
    #[allow(
        clippy::expect_used,
        reason = "the queued control front just matched is owned by this loop alone"
    )]
    #[allow(
        clippy::too_many_arguments,
        reason = "construction takes the bounded control channels, the shared diagnostics and the one allocation stream; a builder would duplicate this list without removing a parameter"
    )]
    pub(in crate::distributed::remote_db) fn start(
        mut client: Client,
        queue_capacity: usize,
        failure: Arc<std::sync::atomic::AtomicBool>,
        diagnostics: WorkerDiagnostics,
        completion: Arc<dyn AppendCompletion>,
        allocations: Arc<AllocationView>,
        stream: u32,
        allocation_generation: u64,
        allocation_capacity: std::num::NonZeroUsize,
    ) -> std::io::Result<Self> {
        let WorkerDiagnostics {
            timeout,
            write_profile_limit,
            write_profile,
            write_timeline,
            first_failure,
            partition,
            policy,
        } = diagnostics;
        client.trace_controls(Arc::clone(&write_timeline), partition);
        let worker_wake = Arc::new(crate::distributed::sys::eventfd()?);
        client.set_worker_wake(std::os::fd::AsRawFd::as_raw_fd(&*worker_wake));
        // The hybrid index needs every owner allocation (position and replay
        // start); recording is opt-in so no other route retains an entry, and the
        // worker drains it after each submission.
        client.enable_lease_reservations();
        let thread_wake = Arc::clone(&worker_wake);
        let leased_write_window = client.leased_write_window();
        let client = Arc::new(Mutex::new(client));
        let worker_client = Arc::clone(&client);
        let queue = Arc::new(OwnerQueue::new(policy.max_pending_batches));
        let (sender, receiver) = OwnerSender::channel(queue_capacity, Arc::clone(&queue), timeout);
        let worker_queue = Arc::clone(&queue);
        let accounting = Arc::new(WorkerAccounting::default());
        let worker_accounting = Arc::clone(&accounting);
        #[cfg(test)]
        let caller_failure = Arc::clone(&failure);
        #[cfg(test)]
        let caller_first_failure = Arc::clone(&first_failure);
        #[cfg(test)]
        let test_allocation = Arc::clone(&allocations);
        let thread = thread::Builder::new()
            .name("value-wal-tcp".into())
            .spawn(move || {
                let _thread_wake = thread_wake;
                // The per-stream extent queue belongs to this thread alone (plan
                // Amendment 14): it reserves on submission and drops extents only
                // on a cold `DrainTo`, so it needs no lock.
                let mut allocations = ExtentQueue::new(
                    allocations,
                    stream,
                    partition,
                    allocation_generation,
                    allocation_capacity,
                );
                let mut write_profile_limit = write_profile_limit;
                let mut profile_enabled = false;
                let mut profile_requests = 0usize;
                // The configured limit arms the profile before the first frame;
                // ArmProfile replaces that attempt.
                let mut profile_attempted = false;
                let mut queued = VecDeque::new();
                let mut controls: VecDeque<OrderedCommand> =
                    VecDeque::with_capacity(queue_capacity);
                let mut disconnected = false;
                let mut leased_replies: VecDeque<(u64, LeasedMembers)> =
                    VecDeque::with_capacity(leased_write_window);
                let mut deferred: Option<(u64, Vec<CompleteBatch>, LeasedMembers)> = None;
                let mut released = Vec::with_capacity(leased_write_window);
                let prepare_leased_command =
                    |client: &mut Client,
                     command: &WorkerItem,
                     released: &mut Vec<(u64, Vec<DurableBatch>)>,
                     replies: &mut VecDeque<(u64, LeasedMembers)>| {
                        if !client.live_nvme_configured() {
                            return true;
                        }
                        // Acquire observes a fatal checkpoint or another owner's
                        // failure immediately before dispatching another command.
                        if failure.load(std::sync::atomic::Ordering::Acquire) {
                            client.record_live_write_error();
                            return false;
                        }
                        let result = match command {
                            WorkerItem::Control(Command::Seal(..))
                            | WorkerItem::Control(Command::Control(_)) => {
                                client.close_leased_extent(released, Some(&write_timeline))
                            }
                            WorkerItem::Control(Command::Stats(_))
                            | WorkerItem::Control(Command::ArmProfile(..))
                            | WorkerItem::Control(Command::OwnerTimelineArm(..))
                            | WorkerItem::Control(Command::DrainTo(..))
                            | WorkerItem::Control(Command::FinishProfile(_)) => {
                                client.wait_leased_control(released, Some(&write_timeline))
                            }
                            WorkerItem::Append(..) | WorkerItem::Control(Command::QueueReady) => {
                                Ok(())
                            }
                        };
                        let result = result.and_then(|()| {
                            release_leased_replies(released, replies, &write_timeline)
                        });
                        if let Err(error) = result {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                first_unresolved_leased(replies, None),
                                "leased_command",
                                &error,
                            );
                            // Release publishes the failure before any queued command runs.
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            return false;
                        }
                        true
                    };
                // Long-lived owner: bounded work each turn, ending on drained
                // disconnection or the first fail-stop condition.
                'worker: loop {
                    if failure.load(std::sync::atomic::Ordering::Acquire) {
                        worker_client.lock().record_live_write_error();
                        break;
                    }
                    let mut client = worker_client.lock();
                    // Failure may have arrived while a control/test holder kept
                    // this mutex. Recheck before taking any READY queue entry.
                    if failure.load(std::sync::atomic::Ordering::Acquire) {
                        client.record_live_write_error();
                        break;
                    }
                    // Snapshot allocations and all controls that fit our bounded
                    // local metadata under owner.order. A later successful send
                    // cannot fence any entry below this captured bound.
                    let (snapshot, closed) = receiver.poll(
                        &worker_queue,
                        queue_capacity.saturating_sub(queued.len() + controls.len()),
                        |ordered| {
                            if !matches!(ordered.command, Command::QueueReady) {
                                controls.push_back(ordered);
                            }
                        },
                    );
                    disconnected |= closed;
                    let mut queue_credit_blocked = false;
                    for _ in 0..queue_capacity + policy.append_occupancy {
                        if controls
                            .front()
                            .is_some_and(|control| worker_queue.head() >= control.before_owner_seq)
                        {
                            let Some(control) = controls.pop_front() else {
                                unreachable!("observed ordered control");
                            };
                            queued.push_back(WorkerItem::Control(control.command));
                            continue;
                        }
                        let control = controls.front().map(|control| control.before_owner_seq);
                        match take_ready_group(
                            &client,
                            &worker_queue,
                            control.unwrap_or(snapshot),
                            control.is_some(),
                            &policy,
                            &worker_accounting,
                            &completion,
                        ) {
                            Ok(GroupDrain::Ready(group)) => {
                                trace_worker_dequeue(&write_timeline, partition, &group);
                                queued.push_back(group);
                            }
                            Ok(GroupDrain::Empty) => {
                                // Skipping ABORTED entries may have reached the
                                // fence without forming another append group.
                                if controls.front().is_some_and(|control| {
                                    worker_queue.head() >= control.before_owner_seq
                                }) {
                                    continue;
                                }
                                break;
                            }
                            Ok(GroupDrain::CreditBlocked) => {
                                queue_credit_blocked = true;
                                break;
                            }
                            Err(error) => {
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "owner_queue",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                completion.failed(partition, &[], error);
                                break 'worker;
                            }
                        }
                    }
                    write_timeline.owner_queue(
                        partition,
                        crate::distributed::write_timeline::WriteOwnerQueue::WorkerQueued,
                        queued.len(),
                    );
                    if client.live_nvme_configured() {
                        write_timeline.owner_queue(
                            partition,
                            crate::distributed::write_timeline::WriteOwnerQueue::LeasedFlights,
                            client.leased_flights_in_flight(),
                        );
                        // Acquire observes authority failure before another control
                        // exchange or write can be admitted by this worker.
                        if failure.load(std::sync::atomic::Ordering::Acquire) {
                            client.record_live_write_error();
                            break;
                        }
                        let mut leased_failure_stage = "live_group";
                        let result = (|| {
                            client.progress_leased_flights(&mut released, Some(&write_timeline))?;
                            if let Err(error) = release_leased_replies(
                                &mut released,
                                &mut leased_replies,
                                &write_timeline,
                            ) {
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    first_unresolved_leased(&leased_replies, None),
                                    "leased_release_order",
                                    &error,
                                );
                                return Err(error);
                            }
                            leased_failure_stage = "leased_adoption";
                            if client.leased_adoption_pending() {
                                client.progress_session()?;
                                client.harvest_leased_adoption()?;
                            }
                            if matches!(queued.front(), Some(WorkerItem::Append(..)) | None) {
                                client.submit_leased_adoption()?;
                            }
                            Ok::<(), crate::distributed::tcp::TcpError>(())
                        })();
                        if let Err(error) = result {
                            client.record_live_write_error();
                            // Every frame still in flight has an unknown outcome.
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                first_unresolved_leased(
                                    &leased_replies,
                                    deferred.as_ref().map(|(_, _, members)| members),
                                ),
                                leased_failure_stage,
                                &error,
                            );
                            // Release publishes the fatal outcome to the authority.
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            break;
                        }
                        if client.leased_adoption_pending()
                            && client.leased_flights_in_flight() == 0
                            && !matches!(queued.front(), Some(WorkerItem::Append(..)))
                        {
                            // The control slot progresses only on this thread. Do
                            // not park forever on the command channel or spin on
                            // an adoption: wake within 1 ms to drive its deadline.
                            // A guard regression would wait here with a frame in flight.
                            #[cfg(test)]
                            if client.leased_flights_in_flight() != 0 {
                                client.leased_test_note_flight_adoption_wait();
                            }
                            if !worker_queue.park() && !queue_credit_blocked {
                                continue;
                            }
                            drop(client);
                            if controls.len() == queue_capacity {
                                // Receive no more controls while local metadata
                                // is full, but keep the 1 ms cadence: no reply
                                // wakes this thread, and this thread alone
                                // drives and harvests the adoption and its
                                // deadline.
                                receiver.park_full_timeout(Duration::from_millis(1));
                                worker_queue.unpark();
                                continue;
                            }
                            if disconnected || queued.len() + controls.len() >= queue_capacity {
                                // Keep the bounded FIFO intact while a command
                                // waits for the adoption slot, without a CQ spin.
                                thread::park_timeout(Duration::from_millis(1));
                                worker_queue.unpark();
                                continue;
                            }
                            match receiver.recv_timeout(Duration::from_millis(1)) {
                                Ok(ordered) if matches!(ordered.command, Command::QueueReady) => {}
                                Ok(ordered) => controls.push_back(ordered),
                                Err(mpsc::RecvTimeoutError::Timeout) => {}
                                Err(mpsc::RecvTimeoutError::Disconnected) => disconnected = true,
                            }
                            worker_queue.unpark();
                            continue;
                        }
                    }

                    if !disconnected
                        && queued.is_empty()
                        && !client.has_pending_io()
                        && client.leased_flights_in_flight() == 0
                        && deferred.is_none()
                    {
                        if !worker_queue.park() {
                            continue;
                        }
                        drop(client);
                        if controls.len() == queue_capacity {
                            receiver.park_full();
                        } else {
                            match receiver.recv() {
                                Ok(ordered) if matches!(ordered.command, Command::QueueReady) => {}
                                Ok(ordered) => controls.push_back(ordered),
                                Err(_) => disconnected = true,
                            }
                        }
                        worker_queue.unpark();
                        continue;
                    }
                    if let Some((epoch, batches, members)) = deferred.take() {
                        match client.submit_leased_group(
                            epoch,
                            batches,
                            &write_timeline,
                            partition,
                            queued.is_empty(),
                        ) {
                            Ok(crate::distributed::tcp::LeasedSubmit::Submitted(id)) => {
                                record_lease_reservations(
                                    &mut client,
                                    &mut allocations,
                                    &worker_accounting,
                                );
                                if let Some(counters) = client.live_counters() {
                                    counters.record_deferred_submit();
                                }
                                if profile_enabled {
                                    profile_requests += 1;
                                    write_profile.lock().sampled_requests += 1;
                                }
                                leased_replies.push_back((id, members));
                                continue;
                            }
                            Ok(crate::distributed::tcp::LeasedSubmit::Deferred(batches)) => {
                                deferred = Some((epoch, batches, members));
                            }
                            Err(error) => {
                                deferred = Some((epoch, Vec::new(), members));
                                client.record_live_write_error();
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "live_group",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                break;
                            }
                        }
                    }
                    if client.leased_flights_in_flight() != 0
                        && (deferred.is_some()
                            || !matches!(queued.front(), Some(WorkerItem::Append(..)))
                            || client.leased_flights_in_flight() >= client.leased_write_window())
                    {
                        if !worker_queue.park() && !queue_credit_blocked {
                            continue;
                        }
                        let waited = client.wait_leased_flights(true);
                        worker_queue.unpark();
                        if let Err(error) = waited {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                first_unresolved_leased(
                                    &leased_replies,
                                    deferred.as_ref().map(|(_, _, members)| members),
                                ),
                                "live_group",
                                &error,
                            );
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            break;
                        }
                        continue;
                    }
                    // Merge ready same-epoch appends without crossing a control
                    // barrier. Leased frames use the configured completion window.
                    if client.live_nvme_configured()
                        && deferred.is_none()
                        && client.leased_flights_in_flight() < client.leased_write_window()
                        // A sampled profile stops dispatch at its limit until
                        // the capture below has closed it.
                        && (!profile_enabled || profile_requests < write_profile_limit)
                        && let Some(WorkerItem::Append(batches, _)) = queued.front()
                    {
                        let epoch = batches[0].version().epoch;
                        if write_profile_limit > 0 && !profile_attempted {
                            profile_attempted = true;
                            if let Err(error) = settle_leased_for_control(
                                &mut client,
                                &mut released,
                                &mut leased_replies,
                                &write_timeline,
                            ) {
                                client.record_live_write_error();
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    first_unresolved_leased(&leased_replies, None),
                                    "leased_profile",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                break;
                            }
                            match client.connection_profile(true, false) {
                                Ok(_) => profile_enabled = true,
                                Err(_) => record_write_profile_error(&write_profile, true),
                            }
                        }
                        if policy.leased_submit_redrain
                            && !crate::distributed::storage::baseline_write_path()
                        {
                            // Last-moment merge: take groups that became READY
                            // after this turn's drain. Re-poll first, as at the
                            // top of the turn: the fresh allocation snapshot and
                            // every control sent before it are captured
                            // together, so a newer entry can never pass a
                            // control. An empty or ALLOCATED head ends it at
                            // once; a lone batch is never held back. Disabled,
                            // the frame carries only the top-of-turn drain.
                            let (fresh, closed) = receiver.poll(
                                &worker_queue,
                                queue_capacity.saturating_sub(queued.len() + controls.len()),
                                |ordered| {
                                    if !matches!(ordered.command, Command::QueueReady) {
                                        controls.push_back(ordered);
                                    }
                                },
                            );
                            disconnected |= closed;
                            let control = controls.front().map(|control| control.before_owner_seq);
                            for _ in 0..queue_capacity {
                                match take_ready_group(
                                    &client,
                                    &worker_queue,
                                    control.unwrap_or(fresh),
                                    control.is_some(),
                                    &policy,
                                    &worker_accounting,
                                    &completion,
                                ) {
                                    Ok(GroupDrain::Ready(group)) => {
                                        trace_worker_dequeue(&write_timeline, partition, &group);
                                        queued.push_back(group);
                                    }
                                    Ok(GroupDrain::Empty | GroupDrain::CreditBlocked) => break,
                                    Err(error) => {
                                        record_first_failure(
                                            &first_failure,
                                            Some(partition),
                                            None,
                                            "owner_queue",
                                            &error,
                                        );
                                        failure.store(true, std::sync::atomic::Ordering::Release);
                                        completion.failed(partition, &[], error);
                                        break 'worker;
                                    }
                                }
                            }
                        }
                        let budget = client.live_group_budget();
                        let mut take = 0usize;
                        let mut count = 0usize;
                        let mut frame_full = false;
                        let mut payload = crate::distributed::tcp::LiveGroupBudget::EMPTY_PAYLOAD;
                        for command in &queued {
                            let WorkerItem::Append(batches, _) = command else {
                                break;
                            };
                            if batches[0].version().epoch != epoch {
                                break;
                            }
                            match budget.extend(count, payload, batches) {
                                Some((next_count, next_payload)) => {
                                    count = next_count;
                                    payload = next_payload;
                                    take += 1;
                                }
                                // The first group is sent alone even if it exceeds the
                                // budget, so the owner reports the real limit error.
                                None if take == 0 => {
                                    take = 1;
                                    break;
                                }
                                None => {
                                    frame_full = true;
                                    break;
                                }
                            }
                        }
                        if take != 0 {
                            let mut merged = Vec::new();
                            let mut members: LeasedMembers = Vec::with_capacity(take);
                            for _ in 0..take {
                                let Some(WorkerItem::Append(batches, reply)) = queued.pop_front()
                                else {
                                    unreachable!("append queue front changed");
                                };
                                members.push((batches[0].version(), batches.len(), reply));
                                if merged.is_empty() {
                                    // Transfer the first command's allocation with its batches.
                                    merged = batches;
                                } else {
                                    merged.extend(batches);
                                }
                            }
                            let first = members[0].0;
                            // READY entries before the first ordered control.
                            let left = write_core::worker_group::ready_left(
                                &worker_queue,
                                controls
                                    .front()
                                    .map_or(u64::MAX, |control| control.before_owner_seq),
                                &policy,
                                &queued,
                            );
                            match client.submit_leased_group(
                                epoch,
                                merged,
                                &write_timeline,
                                partition,
                                queued.is_empty(),
                            ) {
                                Ok(crate::distributed::tcp::LeasedSubmit::Submitted(id)) => {
                                    record_lease_reservations(
                                        &mut client,
                                        &mut allocations,
                                        &worker_accounting,
                                    );
                                    if let Some(counters) = client.live_counters() {
                                        counters.record_submit_ready_left(left, frame_full);
                                    }
                                    // One sampled request per frame, as one owner
                                    // append request was before R3.
                                    if profile_enabled {
                                        profile_requests += 1;
                                        write_profile.lock().sampled_requests += 1;
                                    }
                                    leased_replies.push_back((id, members));
                                }
                                Ok(crate::distributed::tcp::LeasedSubmit::Deferred(batches)) => {
                                    deferred = Some((epoch, batches, members));
                                }
                                Err(error) => {
                                    deferred = Some((epoch, Vec::new(), members));
                                    client.record_live_write_error();
                                    record_first_failure(
                                        &first_failure,
                                        Some(partition),
                                        Some(first),
                                        "live_group",
                                        &error,
                                    );
                                    failure.store(true, std::sync::atomic::Ordering::Release);
                                    break;
                                }
                            }
                            continue;
                        }
                    }

                    let mut progressed = false;
                    if client.has_pending_io()
                        && let Err(error) = client.progress_session()
                    {
                        record_first_failure(
                            &first_failure,
                            Some(partition),
                            None,
                            "transport_progress",
                            &error,
                        );
                        failure.store(true, std::sync::atomic::Ordering::Release);
                        let message = error.to_string();
                        for command in queued.drain(..) {
                            match command {
                                WorkerItem::Append(batches, reply) => {
                                    let first = batches[0].version();
                                    let _ = reply.send(Err(LocalDbError::unknown(first, &message)));
                                }
                                WorkerItem::Control(Command::Stats(reply)) => {
                                    let _ = reply.send(Err(LocalDbError::new(&message)));
                                }
                                WorkerItem::Control(Command::ArmProfile(_, reply))
                                | WorkerItem::Control(Command::OwnerTimelineArm(_, reply))
                                | WorkerItem::Control(Command::Seal(_, reply))
                                | WorkerItem::Control(Command::FinishProfile(reply)) => {
                                    let _ = reply.send(Err(LocalDbError::new(&message)));
                                }
                                WorkerItem::Control(Command::Control(_)) => {}
                                WorkerItem::Control(Command::QueueReady) => {}
                                // The ack sender is dropped: the snapshot
                                // sees a disconnect and treats the drain as
                                // unperformed instead of covering a failed
                                // queue.
                                WorkerItem::Control(Command::DrainTo(..)) => {}
                            }
                        }
                        break;
                    }
                    // Acquire observes an authority failure in this pass before
                    // profile control, queued commands or close.
                    if client.live_nvme_configured()
                        && failure.load(std::sync::atomic::Ordering::Acquire)
                    {
                        client.record_live_write_error();
                        break;
                    }

                    if profile_enabled && profile_requests >= write_profile_limit {
                        // The capture is a control exchange: settle the sampled
                        // frames and their adoption first, as for FinishProfile.
                        if let Err(error) = settle_leased_for_control(
                            &mut client,
                            &mut released,
                            &mut leased_replies,
                            &write_timeline,
                        ) {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                first_unresolved_leased(&leased_replies, None),
                                "leased_profile",
                                &error,
                            );
                            failure.store(true, std::sync::atomic::Ordering::Release);
                            break;
                        }
                        match client.connection_profile(false, false) {
                            Ok(profile) => capture_write_profile(&write_profile, profile),
                            Err(_) => record_write_profile_error(&write_profile, false),
                        }
                        profile_enabled = false;
                        progressed = true;
                    }

                    match queued.front() {
                        Some(WorkerItem::Control(Command::Stats(_)))
                        | Some(WorkerItem::Control(Command::OwnerTimelineArm(_, _)))
                        | Some(WorkerItem::Control(Command::Seal(_, _)))
                        | Some(WorkerItem::Control(Command::ArmProfile(_, _)))
                        | Some(WorkerItem::Control(Command::FinishProfile(_)))
                        | Some(WorkerItem::Control(Command::DrainTo(..)))
                        | Some(WorkerItem::Control(Command::Control(_))) => {
                            if let Some(command) = queued.front()
                                && !prepare_leased_command(
                                    &mut client,
                                    command,
                                    &mut released,
                                    &mut leased_replies,
                                )
                            {
                                break;
                            }
                            match queued.pop_front().expect("front control exists") {
                                WorkerItem::Control(Command::Control(operation)) => {
                                    operation(&mut client)
                                }
                                WorkerItem::Control(Command::DrainTo(target, ack)) => {
                                    // Cold path, at most once per real snapshot
                                    // store. Reserve everything the writer has
                                    // allocated first, then treat the
                                    // snapshot's stored cut as authoritative:
                                    // every extent below it is covered by the
                                    // region and must not be replayed.
                                    record_lease_reservations(
                                        &mut client,
                                        &mut allocations,
                                        &worker_accounting,
                                    );
                                    allocations.drain_to(target);
                                    let _ = ack.send(Ok(()));
                                }
                                WorkerItem::Control(Command::Stats(reply)) => {
                                    let _ =
                                        reply.send(client.stats().map_err(LocalDbError::component));
                                }
                                WorkerItem::Control(Command::OwnerTimelineArm(limit, reply)) => {
                                    let _ = reply.send(
                                        client
                                            .owner_timeline_arm(limit)
                                            .map_err(LocalDbError::component),
                                    );
                                }
                                WorkerItem::Control(Command::Seal(epoch, reply)) => {
                                    let _ = reply
                                        .send(client.seal(epoch).map_err(LocalDbError::component));
                                }
                                WorkerItem::Control(Command::ArmProfile(limit, reply)) => {
                                    write_profile_limit = limit;
                                    profile_requests = 0;
                                    profile_attempted = limit != 0;
                                    let enabled = limit != 0;
                                    let result = client
                                        .connection_profile(enabled, true)
                                        .map(|_| profile_enabled = enabled)
                                        .map_err(LocalDbError::component);
                                    if result.is_err() {
                                        record_write_profile_error(&write_profile, true);
                                        profile_enabled = false;
                                    }
                                    let _ = reply.send(result);
                                }
                                WorkerItem::Control(Command::FinishProfile(reply)) => {
                                    let result = if profile_enabled {
                                        match client.connection_profile(false, false) {
                                            Ok(profile) => {
                                                capture_write_profile(&write_profile, profile);
                                                profile_enabled = false;
                                                Ok(())
                                            }
                                            Err(error) => {
                                                record_write_profile_error(&write_profile, false);
                                                profile_enabled = false;
                                                Err(LocalDbError::component(error))
                                            }
                                        }
                                    } else {
                                        Ok(())
                                    };
                                    let _ = reply.send(result);
                                }
                                WorkerItem::Control(Command::QueueReady) => {}
                                WorkerItem::Append(..) => {
                                    unreachable!()
                                }
                            }
                            progressed = true;
                        }
                        _ => {}
                    }
                    if disconnected && queued.is_empty() && controls.is_empty() {
                        if client.live_nvme_configured()
                            && let Err(error) =
                                client.close_leased_extent(&mut released, Some(&write_timeline))
                        {
                            client.record_live_write_error();
                            record_first_failure(
                                &first_failure,
                                Some(partition),
                                None,
                                "leased_close",
                                &error,
                            );
                            // Release publishes the fatal outcome to the authority.
                            failure.store(true, std::sync::atomic::Ordering::Release);
                        }
                        break;
                    }
                    drop(client);
                    if !progressed {
                        thread::yield_now();
                    }
                }
                {
                    let message = failure_context(
                        &first_failure,
                        "leased worker stopped with unknown write outcome",
                    );
                    for (_, members) in leased_replies.drain(..) {
                        for (version, _, reply) in members {
                            let _ = reply.send(Err(LocalDbError::unknown(version, &message)));
                        }
                    }
                    if let Some((_, _, members)) = deferred.take() {
                        for (version, _, reply) in members {
                            let _ = reply.send(Err(LocalDbError::unknown(version, &message)));
                        }
                    }
                    // Take only ownership under the lock. Terminal DMA drain
                    // stays on this worker, outside the mutex needed by shutdown.
                    let failed_writer = worker_client.lock().take_failed_live_writer();
                    if let Some(mut writer) = failed_writer
                        && let Err(error) = writer.drain_failed()
                    {
                        record_first_failure(
                            &first_failure,
                            Some(partition),
                            None,
                            "leased_drain",
                            &error,
                        );
                        // Release publishes drain failure without returning DMA.
                        failure.store(true, std::sync::atomic::Ordering::Release);
                    }
                }
                if failure.load(std::sync::atomic::Ordering::Acquire) {
                    let message = failure_context(
                        &first_failure,
                        "owner worker stopped with unresolved writes",
                    );
                    completion.failed(partition, &[], LocalDbError::new(message));
                }
                // Acquire keeps profile control off a failed authority.
                if profile_enabled && !failure.load(std::sync::atomic::Ordering::Acquire) {
                    let mut client = worker_client.lock();
                    match client.connection_profile(false, false) {
                        Ok(profile) => capture_write_profile(&write_profile, profile),
                        Err(_) => record_write_profile_error(&write_profile, false),
                    }
                }
            })?;
        sender.set_worker(thread.thread().clone());
        Ok(Self {
            client,
            sender: Some(Arc::new(sender)),
            worker_wake,
            thread: Mutex::new(Some(thread)),
            queue,
            accounting,
            #[cfg(test)]
            failure: caller_failure,
            #[cfg(test)]
            first_failure: caller_first_failure,
            #[cfg(test)]
            partition,
            #[cfg(test)]
            test_completion: None,
            #[cfg(test)]
            allocation: test_allocation,
        })
    }
}
