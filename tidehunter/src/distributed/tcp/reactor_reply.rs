//! Reply queue and reply-lease bookkeeping of an owner session.
//!
//! A control worker's completion becomes a resident reply: it waits in the
//! session's `pending` map, is encoded once the request's own deadline still
//! holds, and refunds its byte lease only when the local submission retires.
//! The replay-page stage note of an expired reply comes from
//! [`super::replay_page`].

use super::*;

/// Removes the front output frame once its completion fence has retired.
/// Check and removal share one borrow, so no unreachable `pop_front` result exists.
pub(super) fn pop_retired(output: &mut VecDeque<Output>, completed: u64) -> Option<Output> {
    if output.front()?.fence? <= completed {
        output.pop_front()
    } else {
        None
    }
}

// Stable diagnostic codes, independent of bincode enum discriminants and
// features; codes of removed requests are never reused. The table is
// documented in owner_events.rs.
pub(super) fn request_kind_code(request: &Request) -> u64 {
    match request {
        Request::RetirementFiles { .. } => 2,
        Request::Status { .. } => 5,
        Request::Fence { .. } => 6,
        Request::Seal { .. } => 10,
        Request::Stats => 11,
        Request::StatsDetailed => 13,
        Request::ConnectionProfile { .. } => 14,
        Request::OwnerTimelineArm { .. } => 16,
        Request::OwnerTimelineFetch { .. } => 17,
        Request::CheckpointPosition { .. } => 18,
        Request::RetireWalFiles { .. } => 20,
        Request::ReleaseLiveEpochPin { .. } => 26,
        Request::ResolveLiveRecords { .. } => 29,
        Request::LiveTargetStats => 31,
        Request::ShutdownLiveTarget { .. } => 32,
        Request::LeaseLiveExtent { .. } => 33,
        Request::AdoptLiveExtent { .. } => 34,
        Request::OpenLiveExports { .. } => 35,
        Request::PreleaseLiveExtent { .. } => 36,
        Request::ReplayLiveWal { .. } => 37,
        Request::ReadEvidence { .. } => 38,
        Request::ProcessResources { .. } => 39,
        Request::AcquireLiveExportPin { .. } => 42,
        Request::ReleaseLiveExportPin { .. } => 43,
        Request::DurableProgress { .. } => 44,
        Request::PartitionStats => 45,
        Request::InventoryPage(_) => 46,
        Request::CheckTrimGeneration(_) => 47,
        Request::TrimGeneration(_) => 48,
        Request::FileInventoryPage { .. } => 49,
        Request::RelocationPage { .. } => 50,
        Request::ProvisionDatabase { .. } => 51,
        Request::DropDatabase { .. } => 52,
    }
}

pub(super) fn queue_response(
    pending: &mut HashMap<u64, ServerPending>,
    id: u64,
    response: Response,
) -> Result<(), TcpError> {
    let request = pending
        .get_mut(&id)
        .ok_or_else(|| TcpError::new("response has no request owner"))?;
    if request.response_queued || request.ready_response.is_some() {
        return Err(TcpError::new("duplicate server completion"));
    }
    request.ready_response = Some(response);
    crate::distributed::owner_events::record(
        crate::distributed::owner_events::OwnerEventKind::ControlReplied,
        request.partition,
        id,
        0,
    );
    Ok(())
}

pub(super) fn flush_responses(
    stream: &mut Session,
    pending: &mut HashMap<u64, ServerPending>,
    limits: TcpLimits,
    mut diagnostics: Option<&mut ConnectionProfile>,
    replay_stages: &super::replay_page::ReplayStages,
) -> Result<bool, TcpError> {
    let mut progressed = false;
    for (&id, request) in pending.iter_mut() {
        if request.ready_response.is_some() {
            // Check the deadline while the reply is still present, so the
            // stage note can name a reply that was ready but not queued.
            let now = Instant::now();
            if now >= request.deadline {
                return Err(request.deadline_error(
                    id,
                    "had a response ready to send",
                    now,
                    replay_stages.describe(id, request, now),
                ));
            }
        }
        let Some(response) = request.ready_response.take() else {
            continue;
        };
        let began = request.measured.then(Instant::now);
        stream.queue_response(id, response, limits.max_frame_bytes)?;
        if request.measured
            && let Some(profile) = diagnostics.as_deref_mut()
        {
            profile.response_encode_ns += began.map_or(0, elapsed_ns);
        }
        request.output_started = request.measured.then(Instant::now);
        request.response_queued = true;
        stream.stats.retained_reply_credit_bytes += request.charge as u64;
        stream.stats.peak_retained_reply_credit_bytes = stream
            .stats
            .peak_retained_reply_credit_bytes
            .max(stream.stats.retained_reply_credit_bytes);
        progressed = true;
    }
    Ok(progressed)
}

pub(super) fn retire_responses(
    stream: &mut Session,
    pending: &mut HashMap<u64, ServerPending>,
    retained_bytes: &mut usize,
    mut diagnostics: Option<&mut ConnectionProfile>,
    replay_stages: &mut super::replay_page::ReplayStages,
) {
    // Completion of plaintext/TLS submission alone never refunds reply leases.
    pending.retain(|id, request| {
        let keep = !request.response_queued || stream.output_pending(*id);
        if !keep {
            // The reply left the owner: the page stage is no longer needed.
            // Until here the entry answers where a page stopped.
            replay_stages.complete(*id);
            *retained_bytes -= request.charge;
            stream.stats.retained_reply_credit_bytes -= request.charge as u64;
            if request.measured
                && let Some(profile) = diagnostics.as_deref_mut()
            {
                profile.response_send_ns += request.output_started.map_or(0, elapsed_ns);
            }
        }
        keep
    });
}
