// The transport worker uses its enclosing database's private control types.
#[allow(clippy::wildcard_imports)]
use super::super::*;

impl ClientWorker {
    pub(in crate::distributed::remote_db) fn start(
        mut client: Client,
        queue_capacity: usize,
        failure: Arc<std::sync::atomic::AtomicBool>,
        diagnostics: WorkerDiagnostics,
        completion: Arc<dyn AppendCompletion>,
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
        let thread = thread::Builder::new()
            .name("value-wal-tcp".into())
            .spawn(move || {
                let _thread_wake = thread_wake;
                let mut write_profile_limit = write_profile_limit;
                let mut profile_enabled = false;
                let mut profile_requests = 0usize;
                // The configured limit arms the profile before the first frame;
                // ArmProfile replaces that attempt.
                let mut profile_attempted = false;
                let mut checkpoint_samples = 0usize;
                let mut checkpoint_stalls = 0u64;
                let mut checkpoint_started: Option<Instant> = None;
                let mut checkpoint_admission_deadline: Option<Instant> = None;
                let mut checkpoint_append_completions = 0u64;
                let mut queued = VecDeque::new();
                let mut controls: VecDeque<OrderedCommand> =
                    VecDeque::with_capacity(queue_capacity);
                // One fixed-cut position proof may wait outside the append FIFO.
                // Later non-append commands never pass it. Earlier covered appends
                // have already run; passing appends have later WAL positions.
                let mut parked_checkpoint: Option<WorkerItem> = None;
                let mut position: Option<(u64, u64, Reply<CheckpointPositionProof>)> = None;
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
                            WorkerItem::Control(Command::CheckpointPosition(..)) => Ok(()),
                            WorkerItem::Control(Command::Seal(..))
                            | WorkerItem::Control(Command::Control(_)) => {
                                client.close_leased_extent(released, Some(&write_timeline))
                            }
                            WorkerItem::Control(Command::Stats(_))
                            | WorkerItem::Control(Command::ArmProfile(..))
                            | WorkerItem::Control(Command::OwnerTimelineArm(..))
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
                            if parked_checkpoint.is_none()
                                && position.is_none()
                                && matches!(
                                    queued.front(),
                                    Some(WorkerItem::Control(Command::CheckpointPosition(..)))
                                )
                            {
                                parked_checkpoint = queued.pop_front();
                            }
                            if parked_checkpoint.is_some() {
                                client.prepare_leased_position()?;
                                // A fixed-through proof tolerates later appends:
                                // position persistence validates its captured offset.
                                queued.push_front(parked_checkpoint.take().unwrap());
                            }
                            if parked_checkpoint.is_none()
                                && matches!(queued.front(), Some(WorkerItem::Append(..)) | None)
                            {
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
                        if position.is_none()
                            && client.leased_adoption_pending()
                            && client.leased_flights_in_flight() == 0
                            && !matches!(
                                queued.front(),
                                Some(
                                    WorkerItem::Append(..)
                                        | WorkerItem::Control(Command::CheckpointPosition(..))
                                )
                            )
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
                        && parked_checkpoint.is_none()
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
                                if let Some(counters) = client.live_counters() {
                                    counters.record_deferred_submit();
                                }
                                if profile_enabled {
                                    profile_requests += 1;
                                    write_profile.lock().sampled_requests += 1;
                                }
                                leased_replies.push_back((id, members));
                                if position.is_none() {
                                    continue;
                                }
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
                        && !matches!(
                            queued.front(),
                            Some(WorkerItem::Control(Command::CheckpointPosition(..)))
                        )
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
                        if position.is_none() {
                            continue;
                        }
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
                        if write_profile_limit > 0
                            && !profile_attempted
                            && position.is_none()
                            && parked_checkpoint.is_none()
                        {
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
                                    let frame = (next_payload + 8 + 4095) & !4095;
                                    // Harvest a pending prelease in the worker before
                                    // a boundary write consumes it. This gives the
                                    // parked proof its first free control turn.
                                    if (position.is_some() || parked_checkpoint.is_some())
                                        && client.leased_append_needs_control(Some(frame))
                                        && client.leased_adoption_pending()
                                    {
                                        break;
                                    }
                                    count = next_count;
                                    payload = next_payload;
                                    take += 1;
                                }
                                // The first group is sent alone even if it exceeds the
                                // budget, so the owner reports the real limit error.
                                None if take == 0 => {
                                    if (position.is_none() && parked_checkpoint.is_none())
                                        || !client.leased_append_needs_control(None)
                                        || !client.leased_adoption_pending()
                                    {
                                        take = 1;
                                    }
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
                            if position.is_none() {
                                continue;
                            }
                        }
                    }

                    let mut progressed = false;
                    if parked_checkpoint.is_none()
                        && position.is_none()
                        && let Some(WorkerItem::Control(Command::CheckpointPosition(
                            authority,
                            epoch,
                            through,
                            _,
                        ))) = queued.front()
                    {
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
                        let deadline = *checkpoint_admission_deadline
                            .get_or_insert_with(|| client.position_deadline());
                        match client
                            .submit_checkpoint_position(*authority, *epoch, *through, deadline)
                        {
                            Ok(true) => {
                                let Some(WorkerItem::Control(Command::CheckpointPosition(
                                    _,
                                    epoch,
                                    through,
                                    reply,
                                ))) = queued.pop_front()
                                else {
                                    unreachable!()
                                };
                                write_timeline.checkpoint(
                                    "control_submit",
                                    Some(partition),
                                    epoch,
                                    None,
                                );
                                checkpoint_admission_deadline = None;
                                if checkpoint_samples < write_profile_limit {
                                    checkpoint_samples += 1;
                                    checkpoint_started = Some(Instant::now());
                                    let mut profile = write_profile.lock();
                                    profile.checkpoint_requests += 1;
                                    profile.checkpoint_admission_stalls = profile
                                        .checkpoint_admission_stalls
                                        .saturating_add(checkpoint_stalls);
                                }
                                checkpoint_stalls = 0;
                                position = Some((epoch, through, reply));
                                progressed = true;
                            }
                            Ok(false) => {
                                if checkpoint_samples < write_profile_limit {
                                    checkpoint_stalls = checkpoint_stalls.saturating_add(1);
                                }
                            }
                            Err(error) => {
                                let Some(WorkerItem::Control(Command::CheckpointPosition(
                                    _,
                                    _,
                                    _,
                                    reply,
                                ))) = queued.pop_front()
                                else {
                                    unreachable!()
                                };
                                checkpoint_admission_deadline = None;
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "checkpoint_position_submit",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                let _ = reply.send(Err(LocalDbError::component(error)));
                                progressed = true;
                            }
                        }
                    }
                    if client.has_pending_io() {
                        match client.progress_session() {
                            Ok(()) => {
                                if let Some((epoch, through, _)) = position.as_ref()
                                    && let Some(result) =
                                        client.take_checkpoint_position(*epoch, *through)
                                {
                                    write_timeline.checkpoint(
                                        "control_retire",
                                        Some(partition),
                                        *epoch,
                                        None,
                                    );
                                    if let Some(started) = checkpoint_started.take() {
                                        let mut profile = write_profile.lock();
                                        profile.checkpoint_completed += 1;
                                        profile.checkpoint_wait_ns = profile
                                            .checkpoint_wait_ns
                                            .saturating_add(elapsed(started));
                                        profile.checkpoint_append_completions = profile
                                            .checkpoint_append_completions
                                            .saturating_add(checkpoint_append_completions);
                                        checkpoint_append_completions = 0;
                                    }
                                    let (_, _, reply) = position.take().expect("position exists");
                                    let result = result.map_err(LocalDbError::component);
                                    if let Err(error) = &result {
                                        record_first_failure(
                                            &first_failure,
                                            Some(partition),
                                            None,
                                            "checkpoint_position_completion",
                                            error,
                                        );
                                        failure.store(true, std::sync::atomic::Ordering::Release);
                                    }
                                    let _ = reply.send(result);
                                    progressed = true;
                                }
                            }
                            Err(error) => {
                                record_first_failure(
                                    &first_failure,
                                    Some(partition),
                                    None,
                                    "transport_progress",
                                    &error,
                                );
                                failure.store(true, std::sync::atomic::Ordering::Release);
                                if let Some(started) = checkpoint_started.take() {
                                    let mut profile = write_profile.lock();
                                    profile.checkpoint_completed += 1;
                                    profile.checkpoint_wait_ns =
                                        profile.checkpoint_wait_ns.saturating_add(elapsed(started));
                                    profile.checkpoint_append_completions = profile
                                        .checkpoint_append_completions
                                        .saturating_add(checkpoint_append_completions);
                                }
                                if let Some((_, _, reply)) = position.take() {
                                    let _ = reply.send(Err(LocalDbError::new(error.to_string())));
                                }
                                let message = error.to_string();
                                for command in queued.drain(..) {
                                    match command {
                                        WorkerItem::Append(batches, reply) => {
                                            let first = batches[0].version();
                                            let _ = reply
                                                .send(Err(LocalDbError::unknown(first, &message)));
                                        }
                                        WorkerItem::Control(Command::Stats(reply)) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        WorkerItem::Control(Command::CheckpointPosition(
                                            _,
                                            _,
                                            _,
                                            reply,
                                        )) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        WorkerItem::Control(Command::ArmProfile(_, reply))
                                        | WorkerItem::Control(Command::OwnerTimelineArm(
                                            _,
                                            reply,
                                        ))
                                        | WorkerItem::Control(Command::Seal(_, reply))
                                        | WorkerItem::Control(Command::FinishProfile(reply)) => {
                                            let _ = reply.send(Err(LocalDbError::new(&message)));
                                        }
                                        WorkerItem::Control(Command::Control(_)) => {}
                                        WorkerItem::Control(Command::QueueReady) => {}
                                    }
                                }
                                break;
                            }
                        }
                    }
                    // Acquire observes checkpoint submission/completion failure in
                    // this pass before profile control, queued commands or close.
                    if client.live_nvme_configured()
                        && failure.load(std::sync::atomic::Ordering::Acquire)
                    {
                        client.record_live_write_error();
                        break;
                    }

                    if profile_enabled
                        && profile_requests >= write_profile_limit
                        && position.is_none()
                        && parked_checkpoint.is_none()
                    {
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

                    if position.is_none() && parked_checkpoint.is_none() {
                        match queued.front() {
                            Some(WorkerItem::Control(Command::Stats(_)))
                            | Some(WorkerItem::Control(Command::OwnerTimelineArm(_, _)))
                            | Some(WorkerItem::Control(Command::Seal(_, _)))
                            | Some(WorkerItem::Control(Command::ArmProfile(_, _)))
                            | Some(WorkerItem::Control(Command::FinishProfile(_)))
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
                                    WorkerItem::Control(Command::Stats(reply)) => {
                                        let _ = reply
                                            .send(client.stats().map_err(LocalDbError::component));
                                    }
                                    WorkerItem::Control(Command::OwnerTimelineArm(
                                        limit,
                                        reply,
                                    )) => {
                                        let _ = reply.send(
                                            client
                                                .owner_timeline_arm(limit)
                                                .map_err(LocalDbError::component),
                                        );
                                    }
                                    WorkerItem::Control(Command::Seal(epoch, reply)) => {
                                        let _ = reply.send(
                                            client.seal(epoch).map_err(LocalDbError::component),
                                        );
                                    }
                                    WorkerItem::Control(Command::ArmProfile(limit, reply)) => {
                                        write_profile_limit = limit;
                                        profile_requests = 0;
                                        profile_attempted = limit != 0;
                                        checkpoint_samples = 0;
                                        checkpoint_stalls = 0;
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
                                        checkpoint_samples = write_profile_limit;
                                        let result = if profile_enabled {
                                            match client.connection_profile(false, false) {
                                                Ok(profile) => {
                                                    capture_write_profile(&write_profile, profile);
                                                    profile_enabled = false;
                                                    Ok(())
                                                }
                                                Err(error) => {
                                                    record_write_profile_error(
                                                        &write_profile,
                                                        false,
                                                    );
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
                                    WorkerItem::Append(..)
                                    | WorkerItem::Control(Command::CheckpointPosition(..)) => {
                                        unreachable!()
                                    }
                                }
                                progressed = true;
                            }
                            _ => {}
                        }
                    }
                    if disconnected
                        && queued.is_empty()
                        && controls.is_empty()
                        && position.is_none()
                        && parked_checkpoint.is_none()
                    {
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
                if let Some((_, _, reply)) = position.take() {
                    let message =
                        failure_context(&first_failure, "TCP worker stopped during position proof");
                    let _ = reply.send(Err(LocalDbError::new(message)));
                }
                if let Some(command) = parked_checkpoint.take() {
                    let message = failure_context(
                        &first_failure,
                        "TCP worker stopped before proof submission",
                    );
                    match command {
                        WorkerItem::Control(Command::CheckpointPosition(_, _, _, reply)) => {
                            let _ = reply.send(Err(LocalDbError::new(message)));
                        }
                        _ => unreachable!("only fixed-cut proofs may be parked"),
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
        })
    }
}
