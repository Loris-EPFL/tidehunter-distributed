//! Durable-progress poll: validate one poll and answer it now or park it.

use super::*;

/// The outcome of a validated durable-progress poll.
pub(super) enum DurablePoll {
    Ready(durable_progress::DurableProgressReport),
    Park {
        identity: PollIdentity,
        log: std::sync::Weak<crate::distributed::storage::PartitionLog>,
        wake: Wake,
        deadline: Instant,
    },
}

/// Validate a poll and answer it now when its condition holds or its wait is zero.
/// A nudge (`sync`) adopts leased frames. On a live stream it then runs one barrier
/// here that syncs everything appended and installs the strict live root over it:
/// the report covers only the root's prefix, and the bounded syncer may defer the
/// root for a whole fragment. On any other stream it asks the log syncer to cover
/// everything appended now and never syncs on this worker.
pub(super) fn durable_progress_poll(
    shared: &Shared,
    authority_epoch: u64,
    epoch: u64,
    after: u64,
    max_wait_ms: u32,
    sync: bool,
) -> Result<DurablePoll, TcpError> {
    #[cfg(not(test))]
    let wait = durable_progress::capped_wait(max_wait_ms);
    #[cfg(test)]
    let wait = Duration::from_millis(u64::from(max_wait_ms)).min(Duration::from_millis(
        // Relaxed: see `progress_cap_ms`.
        shared.progress_cap_ms.load(Ordering::Relaxed),
    ));
    let deadline = Instant::now() + wait;
    let (identity, log) = {
        // A lookup only: the guard drops before any adoption or sync below.
        let inventory = shared.context.inventory.lock();
        let identity = PollIdentity {
            database: shared.context.partition.inventory.database,
            partition: shared.context.partition.partition,
            authority_epoch,
            epoch,
            incarnation: inventory.incarnation(),
        };
        (
            identity,
            inventory.durable_progress_log(authority_epoch, epoch)?,
        )
    };
    let Some(log) = log else {
        return Ok(DurablePoll::Ready(
            identity.report(LogDurableProgress::default()),
        ));
    };
    let wake = if sync {
        log.self_adopt_live_extent().map_err(TcpError::component)?;
        let live = log.persist_live_progress().map_err(TcpError::component)?;
        if live {
            // Other parked polls of this log see the barrier's publication.
            shared.progress.advanced();
        }
        let progress = log.durable_progress();
        // Lag implies an append, which starts the frontier and fixes the tier anyway;
        // an idle log must not start a frontier here.
        let target = if !live && progress.appended_end > progress.synced_end {
            shared
                .frontier_for(&log)?
                .request_sync()
                .map_err(TcpError::component)?
        } else {
            progress.synced_end
        };
        Wake::Synced(target)
    } else {
        Wake::Generation(after)
    };
    let progress = log.durable_progress();
    if wake.ready(&progress) || Instant::now() >= deadline {
        return Ok(DurablePoll::Ready(identity.report(progress)));
    }
    Ok(DurablePoll::Park {
        identity,
        log: Arc::downgrade(&log),
        wake,
        deadline,
    })
}
