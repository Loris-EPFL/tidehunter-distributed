//! Native-style freeze/persist/install checkpoints. Only the final authority
//! manifest adopts an image; all acknowledged suffix batches remain WAL-backed.

use super::super::native_index::{PersistedIndexImage, PreparedIndexImage};
use super::*;

struct CompletedCheckpoint {
    commit: CheckpointCommit,
    manifest: AuthorityManifest,
    persisted: Option<PersistedIndexImage>,
    batches: usize,
    bytes: usize,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: VecDeque<RecentReceipt>,
    cut: Option<MutationVersion>,
    staging_manifest_ns: Option<u64>,
    node_prefix_wait_ns: Option<u64>,
    index_prepare_ns: Option<u64>,
    index_persist_ns: Option<u64>,
    metadata_install_ns: Option<u64>,
    root_adoption_ns: Option<u64>,
}

struct CapturingCheckpoint {
    commit: CheckpointCommit,
    cut: Option<MutationVersion>,
    batches: usize,
    bytes: usize,
    stream_last: BTreeMap<(u32, u64), (u64, u32)>,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: VecDeque<RecentReceipt>,
    jobs: Vec<(Arc<OwnerSender>, u32, Vec<u64>)>,
    published_next: u64,
    prepare_work_ns: Option<u64>,
}

pub(super) struct BackgroundCheckpoint {
    reply: mpsc::Receiver<Result<(), LocalDbError>>,
    thread: Option<JoinHandle<()>>,
    // Publisher-owned until capture consumes it. A post-capture request is
    // transferred atomically to one successor before this job is removed.
    requested_floor: Option<IndexReadCut>,
}

impl Drop for BackgroundCheckpoint {
    fn drop(&mut self) {
        // Never release the directory lock while persistence still owns a file.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

use super::write_core::core::{CheckpointCredits, PublisherCheckpoint, WriteCore};

pub(super) fn core_checkpoint_due(core: &WriteCore) -> bool {
    // Admission's granted tail includes the published suffix. Inspect each
    // snapshot separately, never add them or take Admission under publisher.
    let pressure = core.admission.checkpoint_pressure();
    let mut publisher = core.publisher.lock();
    let state = publisher.checkpoint_state();
    core.config
        .replay_limits
        .checkpoint_due(pressure.replay_batches, pressure.replay_bytes)
        || core
            .config
            .replay_limits
            .checkpoint_due(*state.checkpoint_tail_batches, *state.checkpoint_tail_bytes)
        || pressure.identities >= (core.config.max_admitted_batches / 2).max(1)
        || state.known.len() >= (core.config.max_admitted_batches / 2).max(1)
        || core.index.charged_metadata_bytes() >= core.config.max_index_bytes / 2
}

pub(super) fn core_request_checkpoint(core: &Arc<WriteCore>) -> Result<(), LocalDbError> {
    core.ensure_healthy()?;
    if core_checkpoint_due(core) {
        core.start_checkpoint()?;
    }
    Ok(())
}

pub(super) fn core_start_checkpoint(core: &Arc<WriteCore>) -> Result<(), LocalDbError> {
    core_start_checkpoint_with_floor(core, None)
}

fn core_start_checkpoint_with_floor(
    core: &Arc<WriteCore>,
    floor: Option<IndexReadCut>,
) -> Result<(), LocalDbError> {
    core.ensure_healthy()?;
    core_progress_checkpoint(core, false)?;
    let mut publisher = core.publisher.lock();
    let state = publisher.checkpoint_state();
    if let Some(background) = state.background_checkpoint.as_mut() {
        background.requested_floor =
            merge_checkpoint_floor(background.requested_floor.take(), floor)?;
        return Ok(());
    }
    if floor.is_none() && *state.checkpoint_tail_batches == 0 && state.manifest.checkpoint.is_some()
    {
        return Ok(());
    }
    core_launch_checkpoint(core, state, floor)
}

fn merge_checkpoint_floor(
    left: Option<IndexReadCut>,
    right: Option<IndexReadCut>,
) -> Result<Option<IndexReadCut>, LocalDbError> {
    Ok(match (left, right) {
        (Some(IndexReadCut::Sparse(_)), _) | (_, Some(IndexReadCut::Sparse(_))) => {
            return Err(LocalDbError::new(
                "checkpoint retention floor must be a scalar cut",
            ));
        }
        (None, floor) | (floor, None) => floor,
        (Some(IndexReadCut::Latest), _) | (_, Some(IndexReadCut::Latest)) => {
            Some(IndexReadCut::Latest)
        }
        (Some(IndexReadCut::BeforeAll), floor) | (floor, Some(IndexReadCut::BeforeAll)) => floor,
        (Some(IndexReadCut::At(left)), Some(IndexReadCut::At(right))) => {
            Some(IndexReadCut::At(left.max(right)))
        }
    })
}

fn core_launch_checkpoint(
    core: &Arc<WriteCore>,
    state: PublisherCheckpoint<'_>,
    floor: Option<IndexReadCut>,
) -> Result<(), LocalDbError> {
    if state.manifest.checkpoint_staging.is_some() || state.manifest.checkpoint_obsolete.is_some() {
        return Err(LocalDbError::new(
            "checkpoint cleanup must complete before another installation",
        ));
    }
    let generation = state
        .snapshot_generation
        .checked_add(1)
        .ok_or_else(|| LocalDbError::new("snapshot counter exhausted"))?;
    // One completion per checkpoint, capacity one. The worker never waits for
    // an authority poller; it captures, persists, applies and cleans up itself.
    let (sender, reply) = mpsc::sync_channel(1);
    let worker_core = Arc::clone(core);
    let thread = thread::Builder::new()
        .name("value-wal-checkpoint".to_owned())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                core_run_checkpoint(&worker_core, generation)
            }))
            .unwrap_or_else(|_| Err(LocalDbError::new("checkpoint worker panicked")));
            if let Err(error) = &result {
                worker_core.write_timeline.checkpoint_generation(
                    "worker_failed",
                    None,
                    worker_core.epoch,
                    None,
                    Some(generation),
                );
                worker_core.fail(LocalDbError::new(error.message()));
                // Fail-stop first, then drop a partial capture and its retention
                // lease. Healthy progress never reaches this cancellation path.
                worker_core.index.writer().abort_capture();
            }
            // Pair completion and notification with the waiter's predicate mutex.
            {
                let _publisher = worker_core.publisher.lock();
                let _ = sender.try_send(result);
                worker_core.floor_changed.notify_all();
            }
            // Admission is never taken under publisher. A caller still blocked
            // on a checkpoint dimension requests the successor checkpoint.
            worker_core.admission.wake_checkpoint_waiter();
        })
        .map_err(LocalDbError::component)?;
    *state.snapshot_generation = generation;
    *state.background_checkpoint = Some(BackgroundCheckpoint {
        reply,
        thread: Some(thread),
        requested_floor: floor,
    });
    Ok(())
}

pub(super) fn core_progress_checkpoint(
    core: &Arc<WriteCore>,
    wait: bool,
) -> Result<(), LocalDbError> {
    let deadline = Instant::now() + core.config.timeout;
    let mut publisher = core.publisher.lock();
    loop {
        let state = publisher.checkpoint_state();
        let Some(background) = state.background_checkpoint.as_ref() else {
            return core.ensure_healthy();
        };
        let result = match background.reply.try_recv() {
            Ok(result) => Some(result),
            Err(mpsc::TryRecvError::Disconnected) => {
                Some(Err(LocalDbError::new("checkpoint worker lost result")))
            }
            Err(mpsc::TryRecvError::Empty) => None,
        };
        if let Some(result) = result {
            let mut background = state.background_checkpoint.take();
            let requested_floor = background
                .as_mut()
                .and_then(|background| background.requested_floor.take());
            let has_successor = result.is_ok() && requested_floor.is_some();
            let successor = if has_successor {
                core_launch_checkpoint(core, state, requested_floor)
            } else {
                Ok(())
            };
            drop(publisher);
            // Join and BackgroundCheckpoint::drop must never run under publisher.
            drop(background);
            successor?;
            result?;
            if !has_successor || !wait {
                return Ok(());
            }
            publisher = core.publisher.lock();
            continue;
        }
        if !wait {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(LocalDbError::new("checkpoint completion deadline expired"));
        }
        // Recheck the completion predicate even when the wait times out: a
        // completion may have won the mutex at the deadline.
        core.floor_changed.wait_until(&mut publisher, deadline);
    }
}

pub(super) fn core_commit_checkpoint(core: &Arc<WriteCore>) -> Result<(), LocalDbError> {
    core.ensure_healthy()?;
    core.wait_published_before(core.next_sequence())?;
    core_progress_checkpoint(core, true)?;
    core_start_checkpoint(core)?;
    core_progress_checkpoint(core, true)
}

fn core_capture_checkpoint(
    core: &WriteCore,
    generation: u64,
) -> Result<
    (
        CapturingCheckpoint,
        CheckpointPersistence,
        CheckpointCredits,
    ),
    LocalDbError,
> {
    #[cfg(test)]
    let hooks = core.take_checkpoint_test_hooks();
    // Freeze the stable cut and its exact receipt set under the same locks as
    // publication. Sparse receipts beyond the contiguous floor stay in the tail.
    let mut writer = core.index.writer();
    let mut publisher = core.publisher.lock();
    let published_next = publisher.published_next;
    let state = publisher.checkpoint_state();
    // A forced request races ordinary starts only through publisher. Consume
    // it at the actual freeze, not when the worker was spawned.
    let floor = state
        .background_checkpoint
        .as_mut()
        .and_then(|background| background.requested_floor.take());
    let cut = *state.latest;
    if cut.is_some_and(|cut| cut.batch.epoch == core.epoch && cut.batch.sequence >= published_next)
    {
        return Err(LocalDbError::new(
            "checkpoint stable cut exceeds published floor",
        ));
    }
    // Partition occupancy is returned by publication, not checkpoint adoption.
    let mut credits = CheckpointCredits::default();
    let mut stream_last = BTreeMap::new();
    let mut recent: BTreeMap<_, _> = state
        .recent
        .iter()
        .map(|receipt| (receipt.version, *receipt))
        .collect();
    for receipt in state.known.values() {
        if cut.is_none_or(|cut| receipt.version() > cut.batch) {
            continue;
        }
        let address = receipt.address();
        let node = *core
            .partition_indexes
            .get(&address.partition)
            .ok_or_else(|| LocalDbError::new("checkpoint receipt names an unknown partition"))?;
        if !state.retained_epochs[node].contains(&receipt.version().epoch) {
            return Err(LocalDbError::new(
                "checkpoint receipt is outside retained inventory",
            ));
        }
        let encoded_bytes = address.len.checked_sub(8).ok_or_else(|| {
            LocalDbError::new("checkpoint receipt frame is shorter than its header")
        })? as usize;
        credits.replay_batches = credits
            .replay_batches
            .checked_add(1)
            .ok_or_else(|| LocalDbError::new("checkpoint receipt count overflow"))?;
        credits.replay_bytes = credits
            .replay_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| LocalDbError::new("checkpoint receipt byte count overflow"))?;
        stream_last
            .entry((address.partition, receipt.version().epoch))
            .and_modify(|last: &mut (u64, u32)| {
                if address.offset > last.0 {
                    *last = (address.offset, address.len);
                }
            })
            .or_insert((address.offset, address.len));
        recent.insert(
            receipt.version(),
            RecentReceipt {
                id: receipt.id(),
                version: receipt.version(),
                digest: receipt.digest(),
            },
        );
        if recent.len() > WAL_REPLAY_WINDOW_BATCHES {
            recent.pop_first();
        }
    }
    *state.stream_last = stream_last.clone();
    let jobs = core.checkpoint_jobs(state.retained_epochs)?;
    // Publication and reuse use exclusive bounds so sequence zero needs no
    // sentinel or translation into a different on-disk version.
    let diagnostics_enabled = state.checkpoint_diagnostics.is_some();
    let prepare_started = diagnostics_enabled.then(Instant::now);
    let index_cut = cut.map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
    match floor {
        Some(floor) => writer.begin_compacted_capture(index_cut, floor),
        None => writer.begin_capture(index_cut),
    }
    .map_err(LocalDbError::component)?;
    let commit = CheckpointCommit {
        epoch: core.epoch,
        generation,
        metadata_bytes: 0,
        metadata_digest: [0; 32],
        index_files: Vec::new(),
    };
    let capture = CapturingCheckpoint {
        commit,
        cut,
        batches: credits.replay_batches,
        bytes: credits.replay_bytes,
        stream_last,
        positions: state.positions.clone(),
        recent: recent.into_values().collect(),
        jobs,
        published_next,
        prepare_work_ns: prepare_started.map(elapsed),
    };
    let persistence = CheckpointPersistence {
        path: core.path.clone(),
        database: core.config.storage.database,
        fragment_bytes: core.config.storage.fragment_bytes,
        shape_yaml: state.manifest.shape_yaml.clone(),
        staging_manifest: state.manifest.clone(),
        timeline: Arc::clone(&core.write_timeline),
        diagnostics_enabled,
        #[cfg(test)]
        hooks,
    };
    Ok((capture, persistence, credits))
}

fn core_run_checkpoint(core: &Arc<WriteCore>, generation: u64) -> Result<(), LocalDbError> {
    let started = Instant::now();
    core.stats.lock().write.checkpoint_count += 1;
    core.write_timeline
        .checkpoint_generation("freeze", None, core.epoch, None, Some(generation));
    core.write_timeline.checkpoint_generation(
        "capture_start",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    let (mut capture, mut persistence, mut credits) = core_capture_checkpoint(core, generation)?;
    let prepare_started = persistence.diagnostics_enabled.then(Instant::now);
    let prepared = loop {
        // Each step releases the writer so concurrent publication can progress.
        if let Some(prepared) = core.index.writer().capture_step(32) {
            break prepared;
        }
    };
    if let (Some(total), Some(started)) = (&mut capture.prepare_work_ns, prepare_started) {
        *total = total.saturating_add(elapsed(started));
    }
    let capture_ns = elapsed(started);
    core.write_timeline.checkpoint_generation(
        "capture_end",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    let staging = CheckpointFiles {
        epoch: core.epoch,
        generation,
        index_files: prepared
            .reserved_files(&core.path.join(capture.commit.index_name()))
            .map_err(LocalDbError::component)?,
    };
    for name in staging.names() {
        match fs::symlink_metadata(core.path.join(name)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(LocalDbError::component(error)),
            Ok(_) => return Err(LocalDbError::new("checkpoint output path already exists")),
        }
    }
    persistence.staging_manifest.checkpoint_staging = Some(staging.clone());
    {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        publisher.checkpoint_state().manifest.checkpoint_staging = Some(staging);
    }
    #[cfg(test)]
    let interruption = persistence.hooks.interruption;
    #[cfg(test)]
    let cleanup_pause = persistence.hooks.cleanup_pause.take();
    let mut completed = persist_checkpoint(capture, prepared, persistence)?;
    let work_started = Instant::now();
    core.write_timeline.checkpoint_generation(
        "result_send",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    core.write_timeline
        .checkpoint_generation("adopted", None, core.epoch, None, Some(generation));
    core.write_timeline.checkpoint_generation(
        "live_install_start",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_install_before");
    {
        let mut writer = core.index.writer();
        let _publisher = core.publisher.lock();
        writer.begin_install_image(
            completed
                .persisted
                .take()
                .ok_or_else(|| LocalDbError::new("checkpoint persisted image missing"))?,
        );
    }
    loop {
        let mut writer = core.index.writer();
        let _publisher = core.publisher.lock();
        if writer.install_image_step(32) {
            break;
        }
    }
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_install_after");
    let recent = {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let batches = state
            .checkpoint_tail_batches
            .checked_sub(completed.batches)
            .ok_or_else(|| LocalDbError::new("checkpoint captured batch count exceeds tail"))?;
        let bytes = state
            .checkpoint_tail_bytes
            .checked_sub(completed.bytes)
            .ok_or_else(|| LocalDbError::new("checkpoint captured byte count exceeds tail"))?;
        *state.manifest = completed.manifest.clone();
        *state.committed_complete = completed
            .positions
            .iter()
            .filter_map(|(key, position)| position.complete.then_some(*key))
            .collect();
        *state.positions = std::mem::take(&mut completed.positions);
        *state.recent = std::mem::take(&mut completed.recent);
        state.known.retain(|_, receipt| {
            completed
                .cut
                .is_none_or(|cut| receipt.version() > cut.batch)
        });
        *state.checkpoint_tail_batches = batches;
        *state.checkpoint_tail_bytes = bytes;
        state.recent.clone()
    };
    // Identity retirement can take shard locks. It and Admission must run after
    // writer/publisher, and Active→Suffix must not resurrect a Recent identity.
    credits.identities = core.retire_checkpoint_identities(completed.cut, &recent);
    core.release_checkpoint_credits(credits)?;
    let install_ns = elapsed(work_started);
    core.write_timeline.checkpoint_generation(
        "live_install_end",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    if interruption == Some(3) {
        return Err(LocalDbError::new(
            "checkpoint committed; injected interruption before obsolete-file cleanup",
        ));
    }
    #[cfg(test)]
    if let Some((entered, resume)) = cleanup_pause {
        let _ = entered.send(());
        resume
            .recv()
            .map_err(|_| LocalDbError::new("checkpoint cleanup test pause disconnected"))?;
    }
    #[cfg(test)]
    let interrupt_cleanup = interruption == Some(7);
    #[cfg(not(test))]
    let interrupt_cleanup = false;
    core.write_timeline.checkpoint_generation(
        "cleanup_start",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    let cleanup_started = Instant::now();
    cleanup_checkpoint_artifacts(&core.path, &mut completed.manifest, interrupt_cleanup).map_err(
        |error| {
            LocalDbError::new(format!(
                "checkpoint committed; artifact cleanup incomplete: {error}"
            ))
        },
    )?;
    let cleanup_ns = elapsed(cleanup_started);
    core.write_timeline.checkpoint_generation(
        "cleanup_end",
        None,
        core.epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    if interruption == Some(8) {
        return Err(LocalDbError::new(
            "injected interruption after checkpoint cleanup",
        ));
    }
    {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        let mut state = publisher.checkpoint_state();
        *state.manifest = completed.manifest.clone();
        let diagnostic = checkpoint_diagnostic(&completed, install_ns, cleanup_ns);
        let diagnostic = core_record_checkpoint_diagnostic(&mut state, diagnostic);
        *state.last_checkpoint_diagnostic = Some(diagnostic);
    }
    {
        let mut stats = core.stats.lock();
        stats.write.checkpoint_ns += capture_ns.saturating_add(install_ns);
        stats.write.background_checkpoint_ns += elapsed(started);
    }
    core.write_timeline
        .checkpoint_generation("cleaned", None, core.epoch, None, Some(generation));
    Ok(())
}

fn core_record_checkpoint_diagnostic(
    state: &mut PublisherCheckpoint<'_>,
    mut record: CheckpointDiagnostic,
) -> CheckpointDiagnostic {
    let Some(records) = state.checkpoint_diagnostics.as_mut() else {
        return record;
    };
    record.cursor = *state.next_checkpoint_diagnostic;
    if records.len() == 128 {
        records.pop_front();
        *state.dropped_checkpoint_diagnostics =
            state.dropped_checkpoint_diagnostics.saturating_add(1);
    }
    records.push_back(record);
    *state.next_checkpoint_diagnostic = state.next_checkpoint_diagnostic.saturating_add(1);
    state.checkpoint_diagnostic_summary.record(record);
    state.checkpoint_diagnostic_summary.dropped_records = *state.dropped_checkpoint_diagnostics;
    record
}

fn checkpoint_diagnostic(
    completed: &CompletedCheckpoint,
    install_ns: u64,
    cleanup_ns: u64,
) -> CheckpointDiagnostic {
    CheckpointDiagnostic {
        epoch: completed.commit.epoch,
        generation: Some(completed.commit.generation),
        captured_cut: completed.cut,
        captured_batches: Some(completed.batches),
        captured_bytes: Some(completed.bytes),
        staging_manifest_ns: completed.staging_manifest_ns,
        node_prefix_wait_ns: completed.node_prefix_wait_ns,
        index_prepare_ns: completed.index_prepare_ns,
        index_persist_ns: completed.index_persist_ns,
        metadata_install_ns: completed.metadata_install_ns,
        root_adoption_ns: completed.root_adoption_ns,
        owner_adoption_ns: Some(install_ns),
        obsolete_cleanup_ns: Some(cleanup_ns),
        ..CheckpointDiagnostic::default()
    }
}

pub(super) fn core_retire_history_before(
    core: &Arc<WriteCore>,
    floor: IndexReadCut,
) -> Result<(), LocalDbError> {
    // Cold lifecycle gate: retirement -> writer -> publisher. Foreground
    // writes and ordinary checkpoints never take it. No caller-side Admission
    // operation may run while the gate is held, including failure handling.
    let retirement = core.retirement.lock();
    #[cfg(test)]
    if core.take_retirement_preflight_busy() {
        return Err(LocalDbError::new("injected retirement preflight busy"));
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    let _live_pause = core.reads.pause_live_for_retirement()?;
    let result = (|| {
        core.wait_published_before(core.next_sequence())?;
        core_progress_checkpoint(core, true)?;
        let floor = if floor == IndexReadCut::Latest {
            core.publisher
                .lock()
                .checkpoint_state()
                .latest
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
        } else {
            floor
        };
        let installed = core.index.retention_floor();
        let advances = match (&floor, &installed) {
            (IndexReadCut::At(requested), IndexReadCut::At(previous)) => requested > previous,
            (IndexReadCut::At(_), IndexReadCut::BeforeAll) => true,
            _ => false,
        };
        if advances {
            core_start_checkpoint_with_floor(core, Some(floor))?;
            core_progress_checkpoint(core, true)?;
        }
        core_commit_checkpoint(core)?;
        core_retire_unreachable_sealed_files(core)
    })();
    drop(retirement);
    if let Err(error) = &result {
        core.fail(LocalDbError::new(error.message()));
    }
    result
}

pub(super) fn core_retire_unreachable_sealed_files(core: &WriteCore) -> Result<(), LocalDbError> {
    let jobs = {
        let mut publisher = core.publisher.lock();
        let state = publisher.checkpoint_state();
        let mut jobs = Vec::new();
        for (node, epochs) in state.retained_epochs.iter().enumerate() {
            let partition = core.config.endpoints[node].partition;
            for &epoch in epochs {
                if epoch < core.epoch && state.committed_complete.contains(&(partition, epoch)) {
                    jobs.push((node, partition, epoch));
                }
            }
        }
        jobs
    };
    for (node, partition, epoch) in jobs {
        let mut after = None;
        let authority_epoch = core.epoch;
        loop {
            let files = core.control_owner(node, move |client| {
                client.retirement_files(authority_epoch, epoch, after, 256)
            })?;
            if files.is_empty() {
                break;
            }
            after = files.last().copied();
            let mut retired = Vec::with_capacity(files.len());
            for offset in files {
                let end = offset
                    .checked_add(core.config.storage.wal_file_bytes)
                    .ok_or_else(|| LocalDbError::new("retirement file bounds overflow"))?;
                if !core
                    .index
                    .references_wal_file(partition, epoch, offset, end)
                {
                    retired.push(offset);
                }
            }
            if !retired.is_empty() {
                let _retirement = core.control_owner(node, move |client| {
                    client.retire_files_if_idle(authority_epoch, epoch, None, retired)
                })?;
            }
        }
    }
    Ok(())
}

// Persistence owns immutable snapshots only; neither publication nor the index
// writer is held while owner controls, file writes, or syncs run.
struct CheckpointPersistence {
    path: PathBuf,
    database: [u8; 16],
    fragment_bytes: u64,
    shape_yaml: String,
    staging_manifest: AuthorityManifest,
    timeline: Arc<WriteTimeline>,
    diagnostics_enabled: bool,
    #[cfg(test)]
    hooks: super::write_core::core::CheckpointTestHooks,
}

fn persist_checkpoint(
    capture: CapturingCheckpoint,
    prepared: PreparedIndexImage,
    persistence: CheckpointPersistence,
) -> Result<CompletedCheckpoint, LocalDbError> {
    let CapturingCheckpoint {
        commit,
        cut,
        batches,
        bytes,
        stream_last,
        mut positions,
        recent,
        mut jobs,
        published_next,
        prepare_work_ns: index_prepare_ns,
        ..
    } = capture;
    let CheckpointPersistence {
        path,
        database,
        fragment_bytes,
        shape_yaml,
        mut staging_manifest,
        timeline,
        diagnostics_enabled,
        #[cfg(test)]
        hooks,
    } = persistence;
    let generation = commit.generation;
    let authority_epoch = commit.epoch;
    #[cfg(test)]
    let super::write_core::core::CheckpointTestHooks {
        interruption,
        entered,
        pause,
        prefix_pause,
        corrupt_proof: corrupt_checkpoint_proof,
        adoption_pause,
        ..
    } = hooks;
    #[cfg(test)]
    let prefix_pause = Mutex::new(prefix_pause);
    #[cfg(test)]
    if let Some(entered) = entered {
        let _ = entered.try_send(());
    }
    #[cfg(test)]
    if let Some((entered, resume)) = pause {
        let _ = entered.send(());
        resume
            .recv()
            .map_err(|_| LocalDbError::new("checkpoint test pause disconnected"))?;
    }
    // Reserve output names durably before any checkpoint artifact
    // can be written. This immutable snapshot cannot race owner
    // appends, which do not replace the authority manifest.
    let phase_started = diagnostics_enabled.then(Instant::now);
    timeline.checkpoint_generation(
        "staging_manifest_start",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_staging_before");
    persist_manifest(&path, &staging_manifest)?;
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_staging_after");
    let staging_manifest_ns = phase_started.map(elapsed);
    timeline.checkpoint_generation(
        "staging_manifest_end",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    if interruption == Some(0) {
        return Err(LocalDbError::new(
            "injected interruption after checkpoint intent",
        ));
    }
    let phase_started = diagnostics_enabled.then(Instant::now);
    let output = endpoint_map(&mut jobs, |_, (sender, partition, streams)| {
        let mut output = Vec::with_capacity(streams.len());
        for &epoch in streams.iter() {
            let key = (*partition, epoch);
            let previous = positions.get(&key);
            let Some(&(offset, len)) = stream_last.get(&key) else {
                output.push(previous.cloned().unwrap_or(StreamPosition {
                    partition: *partition,
                    epoch,
                    replay_from: 0,
                    last_offset: None,
                    durable_end: 0,
                    complete: false,
                }));
                continue;
            };
            if previous.is_some_and(|position| {
                position.complete || position.last_offset.is_some_and(|last| last >= offset)
            }) {
                output.push(previous.unwrap().clone());
                continue;
            }
            let through = offset
                .checked_add(u64::from(len))
                .ok_or_else(|| LocalDbError::new("checkpoint position overflow"))?;
            let (reply, pending) = mpsc::sync_channel(1);
            timeline.checkpoint_generation(
                "control_enqueue",
                Some(*partition),
                epoch,
                None,
                Some(generation),
            );
            sender.send(Command::CheckpointPosition(
                authority_epoch,
                epoch,
                through,
                reply,
            ))?;
            let proof = receive(pending)?;
            timeline.checkpoint_generation(
                "control_observed",
                Some(*partition),
                epoch,
                None,
                Some(generation),
            );
            #[cfg(test)]
            let proof = if corrupt_checkpoint_proof && *partition == 0 {
                let mut proof = proof;
                proof.database[0] ^= 1;
                proof
            } else {
                proof
            };
            #[cfg(test)]
            if *partition == 0
                && let Some((entered, resume)) = prefix_pause.lock().take()
            {
                let _ = entered.send(());
                resume
                    .recv()
                    .map_err(|_| LocalDbError::new("checkpoint prefix test pause disconnected"))?;
            }
            if proof.database != database
                || proof.partition != *partition
                || proof.epoch != epoch
                || proof.durable_end < through
            {
                return Err(LocalDbError::new("checkpoint position proof differs"));
            }
            output.push(StreamPosition {
                partition: *partition,
                epoch,
                replay_from: offset - offset % fragment_bytes,
                last_offset: Some(offset),
                durable_end: proof.durable_end,
                complete: previous.is_some_and(|position| position.complete),
            });
        }
        Ok(output)
    })?;
    positions = output
        .into_iter()
        .flatten()
        .map(|position| ((position.partition, position.epoch), position))
        .collect();
    #[cfg(test)]
    if interruption == Some(4) {
        return Err(LocalDbError::new(
            "injected interruption after checkpoint positions",
        ));
    }
    let node_prefix_wait_ns = phase_started.map(elapsed);
    let phase_started = diagnostics_enabled.then(Instant::now);
    timeline.checkpoint_generation(
        "index_persist_start",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_image_before");
    let persisted = prepared
        .persist(&path.join(commit.index_name()))
        .map_err(LocalDbError::component)?;
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_image_after");
    let index_persist_ns = phase_started.map(elapsed);
    timeline.checkpoint_generation(
        "index_persist_end",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    if interruption == Some(1) {
        return Err(LocalDbError::new(
            "injected interruption after checkpoint image",
        ));
    }
    timeline.checkpoint_generation(
        "metadata_encode_start",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    let encoded = encode_checkpoint(&CheckpointImageV3 {
        format: 3,
        database,
        shape_yaml,
        authority_epoch,
        generation: commit.generation,
        cut,
        published_next,
        index_bytes: persisted.metadata.bytes,
        index_digest: persisted.metadata.digest,
        positions: positions.values().cloned().collect(),
        recent: recent.iter().copied().collect(),
    })?;
    let mut commit = commit;
    commit.index_files = persisted.files();
    commit.index_files.sort_unstable();
    commit.index_files.dedup();
    commit.metadata_bytes = encoded.len() as u64;
    commit.metadata_digest = Blake2b::<U32>::digest(&encoded).into();
    timeline.checkpoint_generation(
        "metadata_encode_end",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    let phase_started = diagnostics_enabled.then(Instant::now);
    timeline.checkpoint_generation(
        "metadata_install_start",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_metadata_before");
    install_metadata(&path, &commit.metadata_name(), &encoded).map_err(LocalDbError::component)?;
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_metadata_after");
    let metadata_install_ns = phase_started.map(elapsed);
    timeline.checkpoint_generation(
        "metadata_install_end",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    if interruption == Some(2) {
        return Err(LocalDbError::new(
            "injected interruption before checkpoint adoption",
        ));
    }
    #[cfg(test)]
    if let Some((entered, resume)) = adoption_pause {
        let _ = entered.send(());
        resume
            .recv()
            .map_err(|_| LocalDbError::new("checkpoint adoption test pause disconnected"))?;
    }
    // No other serving operation replaces the authority manifest.
    // Adopt the durable root before the owner installs the image;
    // the post-cut suffix remains recoverable from the value WALs.
    staging_manifest.checkpoint_obsolete = staging_manifest.checkpoint.replace(commit.clone());
    staging_manifest.checkpoint_staging = None;
    #[cfg(test)]
    if interruption == Some(5) {
        return Err(LocalDbError::new(
            "injected interruption before checkpoint root adoption",
        ));
    }
    let phase_started = diagnostics_enabled.then(Instant::now);
    timeline.checkpoint_generation(
        "root_persist_start",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_root_before");
    persist_manifest(&path, &staging_manifest)?;
    #[cfg(test)]
    crate::failpoints::write_model::boundary("checkpoint_root_after");
    let root_adoption_ns = phase_started.map(elapsed);
    timeline.checkpoint_generation(
        "root_persist_end",
        None,
        authority_epoch,
        None,
        Some(generation),
    );
    #[cfg(test)]
    if interruption == Some(6) {
        return Err(LocalDbError::new(
            "injected interruption after checkpoint root adoption",
        ));
    }
    Ok(CompletedCheckpoint {
        commit,
        manifest: staging_manifest,
        persisted: Some(persisted),
        batches,
        bytes,
        positions,
        recent,
        cut,
        staging_manifest_ns,
        node_prefix_wait_ns,
        index_prepare_ns,
        index_persist_ns,
        metadata_install_ns,
        root_adoption_ns,
    })
}

#[cfg(test)]
mod tests {
    use super::super::pipeline_tests::Fixture;
    use super::*;

    fn put(value: u8) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(0),
            key: 1u32.to_be_bytes().to_vec().into(),
            value: vec![value; 32].into(),
        }
    }
    #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
    #[test]
    fn checkpoint_defers_busy_controller_retirement_and_retries_without_poisoning() {
        use crate::distributed::nvmeof::{
            LiveTargetConfig, NvmeofTransport,
            live_connect::{LiveConnectTarget, LiveConnectionRegistry},
            live_namespace::LiveNamespaceAccess,
            live_pool::LiveControllerPool,
            live_target::LiveTargetManager,
        };
        let fixture = Fixture::new_with_root(
            tempdir::TempDir::new("checkpoint-export-pin").unwrap(),
            1,
            128,
            KeyShape::new_single(4, 1, crate::key_shape::KeyType::uniform(1)),
            2,
            |_| {},
            |config| {
                super::super::super::tcp::StorageNode::open(
                    config,
                    super::super::tests::recovery_metrics(),
                )
                .unwrap()
            },
        );
        let first = fixture.open();
        first
            .write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        let reference = first
            .core
            .index
            .get(KeySpace(0), &1u32.to_be_bytes(), IndexReadCut::Latest)
            .unwrap()
            .unwrap();
        drop(first);
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 2), vec![put(2)])
            .unwrap();
        // The RPC fixture does not materialize a full NVMe backing allocation.
        // Write the unchanged bytes and zero tail before real export validation.
        let wal_bytes = fixture.config.storage.wal_file_bytes;
        let wal = fixture.startups[0]
            .0
            .root
            .join(format!("epoch_{:016x}", reference.batch_version.epoch))
            .join(format!(
                "wal_{:016x}",
                reference.batch_address.offset / wal_bytes
            ));
        let mut contents = fs::read(&wal).unwrap();
        assert!(contents.len() as u64 <= wal_bytes);
        contents.resize(wal_bytes as usize, 0);
        let file = fs::OpenOptions::new().write(true).open(&wal).unwrap();
        std::os::unix::fs::FileExt::write_all_at(&file, &contents, 0).unwrap();
        let root = fixture.temp.path();
        let configfs = root.join("pin-configfs");
        for name in ["hosts", "ports", "subsystems"] {
            fs::create_dir_all(configfs.join(name)).unwrap();
        }
        let state_dir = root.join("pin-target");
        fs::create_dir(&state_dir).unwrap();
        fixture.nodes[0].bench_install_live_target(
            LiveTargetManager::open_fixture(
                LiveTargetConfig {
                    address: "127.0.0.1".into(),
                    service: 4420,
                    transport: NvmeofTransport::Tcp,
                    host_nqn: "nqn.2026-01.test:checkpoint-pin".into(),
                    port_id: 42,
                    state_dir,
                    commissioning_port_id: 43,
                    commissioning_service: 4421,
                    spdk: None,
                    export_mode: Default::default(),
                },
                configfs,
            )
            .unwrap(),
        );
        let epoch = db.epoch();
        let locations = db.core.clients[0]
            .control(move |client| client.resolve_live_records(epoch, &[reference]))
            .unwrap();
        let location = locations.entries[0].clone();
        let path = location.segment.path.clone();
        let pool = Arc::new(
            LiveControllerPool::new(
                LiveConnectionRegistry::open(&root.join("pin-connections")).unwrap(),
                2,
                1,
            )
            .unwrap(),
        );
        pool.register_local_target(&location.binding.nqn, path.clone());
        let worker_pool = pool.clone();
        let lease = db.core.clients[0]
            .control(move |client| {
                client.set_live_pool(worker_pool.clone());
                let binding = &location.binding;
                let target = LiveConnectTarget {
                    nqn: binding.nqn.clone(),
                    uuid: binding.uuid,
                    nsid: binding.nsid,
                    bytes: binding.bytes,
                    block_size: binding.block_size,
                    address: binding.address.clone(),
                    service: binding.service,
                    transport: binding.transport,
                    spdk: binding.spdk.clone(),
                    geometry: Default::default(),
                };
                let (lease, _, _) = worker_pool.acquire_pinned(
                    &target,
                    "nqn.2026-01.test:checkpoint-pin",
                    [1; 16],
                    LiveNamespaceAccess::ReadOnly,
                    Instant::now() + Duration::from_secs(5),
                    client.export_pin_request(epoch, &location.segment, binding),
                )?;
                client.release_live_epoch_pin(epoch, locations.epoch_pin_token)?;
                Ok(lease)
            })
            .unwrap();
        // Retirement consults the worker client's pool (installed above), not
        // the core's immutable open-time live pool.
        db.core.retire_history_before(IndexReadCut::Latest).unwrap();
        assert!(db.core.ensure_healthy().is_ok());
        assert!(path.exists(), "busy controller must defer file retirement");
        drop(lease);
        db.core.retire_history_before(IndexReadCut::Latest).unwrap();
        assert!(db.core.ensure_healthy().is_ok());
        assert!(
            !path.exists(),
            "next checkpoint must retire the released file"
        );
    }

    #[test]
    fn request_timeline_splits_checkpoint_without_legacy_profile() {
        let fixture = Fixture::new();
        let db = fixture.open();
        assert_eq!(db.core.config.write_profile_limit, 0);
        db.core.write_timeline.arm(4, 1).unwrap();
        db.write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        db.flush_index().unwrap();
        let generation = db.core.publisher.lock().snapshot_generation;
        let export = db.core.write_timeline.finish().unwrap();
        let events: Vec<_> = export
            .checkpoint_events
            .iter()
            .filter(|event| event.generation == Some(generation))
            .collect();
        let stages = [
            "freeze",
            "capture_start",
            "capture_end",
            "staging_manifest_start",
            "staging_manifest_end",
            "index_persist_start",
            "index_persist_end",
            "metadata_encode_start",
            "metadata_encode_end",
            "metadata_install_start",
            "metadata_install_end",
            "root_persist_start",
            "root_persist_end",
            "result_send",
            "adopted",
            "live_install_start",
            "live_install_end",
            "cleanup_start",
            "cleanup_end",
            "cleaned",
        ];
        let mut previous = 0;
        for stage in stages {
            let position = events
                .iter()
                .position(|event| event.stage == stage)
                .unwrap();
            assert!(position >= previous, "out of order: {stage}");
            previous = position;
        }
        assert_eq!(export.dropped_checkpoint_events, 0);
    }

    #[test]
    fn checkpoint_install_keeps_credits_until_adoption_completes() {
        use crate::key_shape::KeyType;
        let shape = KeyShape::new_single(4, 1, KeyType::uniform(64));
        let fixture = Fixture::new_with_shape(128, shape);
        let mut db = fixture.open();
        for n in 0..40u32 {
            let mutation = Mutation::Put {
                keyspace: KeySpace(0),
                key: (n << 26).to_be_bytes().to_vec().into(),
                value: vec![n as u8; 32].into(),
            };
            db.write_batch(fixture.operation(n % 5, u64::from(n)), vec![mutation])
                .unwrap();
        }
        db.drain().unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.core.test_hooks.lock().checkpoint_adoption_pause = Some((entered, gate));
        db.core.start_checkpoint().unwrap();
        assert!(db.core.publisher.lock().background_checkpoint.is_some());
        waiting.recv().unwrap();
        db.write_batch(fixture.operation(0, 100), vec![put(99)])
            .unwrap();
        db.drain().unwrap();
        let tail = |db: &RemoteDatabase| {
            let publisher = db.core.publisher.lock();
            (
                publisher.checkpoint_tail_batches,
                publisher.checkpoint_tail_bytes,
            )
        };
        let before = tail(&db);
        let granted = db.core.admission.charges();
        // The captured image is persisted but not adopted: neither the
        // published tail nor the Admission replay/identity charges move.
        assert_eq!(before.0, 41);
        assert_eq!(
            (
                granted.replay_batches,
                granted.replay_bytes,
                granted.identities
            ),
            (41, before.1, 41)
        );
        resume.send(()).unwrap();
        db.core.progress_checkpoint(true).unwrap();
        assert_eq!(tail(&db).0, 1);
        let remaining = db.core.admission.charges();
        assert_eq!((remaining.replay_batches, remaining.identities), (1, 1));
        assert_eq!(remaining.replay_bytes, tail(&db).1);
        assert_eq!(
            db.get(KeySpace(0), &1u32.to_be_bytes())
                .unwrap()
                .unwrap()
                .as_ref(),
            &[99; 32]
        );
    }

    #[test]
    fn failed_root_never_reports_live_install_or_releases_tail() {
        let fixture = Fixture::new();
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        db.flush_index().unwrap();
        let root = db
            .core
            .publisher
            .lock()
            .manifest
            .checkpoint
            .as_ref()
            .unwrap()
            .generation;
        db.write_batch(fixture.operation(1, 2), vec![put(2)])
            .unwrap();
        db.drain().unwrap();
        let tail = |db: &RemoteDatabase| {
            let publisher = db.core.publisher.lock();
            (
                publisher.checkpoint_tail_batches,
                publisher.checkpoint_tail_bytes,
            )
        };
        let (batches, bytes) = tail(&db);
        let floor = db.core.index.retention_floor();
        db.core.write_timeline.arm(4, 1).unwrap();
        db.core.test_hooks.lock().checkpoint_interruption = Some(5);
        assert!(db.flush_index().is_err());
        let generation = db.core.publisher.lock().snapshot_generation;
        assert_eq!(tail(&db), (batches, bytes));
        assert_eq!(db.core.index.retention_floor(), floor);
        let manifest: AuthorityManifest = read_metadata(&db.core.path.join(MANIFEST)).unwrap();
        assert_eq!(manifest.checkpoint.unwrap().generation, root);
        let export = db.core.write_timeline.finish().unwrap();
        let stages: Vec<_> = export
            .checkpoint_events
            .iter()
            .filter(|event| event.generation == Some(generation))
            .map(|event| event.stage)
            .collect();
        assert!(stages.contains(&"worker_failed"));
        assert!(!stages.contains(&"root_persist_end"));
        assert!(!stages.contains(&"adopted"));
        assert!(!stages.contains(&"live_install_end"));
    }
}

#[cfg(test)]
mod core_tests {
    use super::super::pipeline_tests::Fixture;
    use super::*;

    fn put(value: u8) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(0),
            key: 1u32.to_be_bytes().to_vec().into(),
            value: vec![value; 32].into(),
        }
    }

    // The boundary has no serving core write API yet. Use real owner WAL
    // receipts and the real index, then stage the publisher's receipt handoff
    // under writer -> publisher. Admission already owns both exact charges.
    fn hold_publication(
        core: &WriteCore,
        operation: [u8; 16],
        previous: BatchVersion,
    ) -> AppendedBatch {
        let id = BatchId {
            database: core.config.storage.database,
            operation,
        };
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        publisher.published_next = previous.sequence + 1;
        let state = publisher.checkpoint_state();
        let receipt = state.known.remove(&id).unwrap();
        *state.latest = Some(MutationVersion {
            batch: previous,
            operation_index: 0,
        });
        *state.checkpoint_tail_batches -= 1;
        *state.checkpoint_tail_bytes -= (receipt.address().len - 8) as usize;
        receipt
    }

    fn publish_receipt(core: &WriteCore, receipt: AppendedBatch, operation_index: u32) {
        let _writer = core.index.writer();
        let mut publisher = core.publisher.lock();
        publisher.published_next = receipt.version().sequence + 1;
        let state = publisher.checkpoint_state();
        assert!(state.known.insert(receipt.id(), receipt).is_none());
        *state.latest = Some(MutationVersion {
            batch: receipt.version(),
            operation_index,
        });
        *state.checkpoint_tail_batches += 1;
        *state.checkpoint_tail_bytes += (receipt.address().len - 8) as usize;
        core.floor_changed.notify_all();
    }

    #[test]
    fn capture_overlaps_publication_and_apply_returns_only_captured_credits() {
        let fixture = Fixture::new();
        let db = fixture.open();
        let first = db
            .write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        let suffix_id = fixture.operation(0, 2);
        db.write_batch(suffix_id, vec![put(2), put(3)]).unwrap();
        db.drain().unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
        let core = Arc::clone(&db.core);
        let granted = core.admission.checkpoint_pressure();
        assert_eq!(granted.replay_batches, 2);
        let suffix = hold_publication(&core, suffix_id, first);
        core.start_checkpoint().unwrap();
        waiting.recv_timeout(core.config.timeout).unwrap();
        let publisher_core = Arc::clone(&core);
        thread::spawn(move || publish_receipt(&publisher_core, suffix, 1))
            .join()
            .unwrap();
        resume.send(()).unwrap();
        core.progress_checkpoint(true).unwrap();

        let suffix_bytes = (suffix.address().len - 8) as usize;
        let remaining = core.admission.checkpoint_pressure();
        assert_eq!(remaining.identities, 1);
        assert_eq!(remaining.replay_batches, 1);
        assert_eq!(remaining.replay_bytes, suffix_bytes);
        {
            let mut publisher = core.publisher.lock();
            let state = publisher.checkpoint_state();
            assert_eq!(state.known.len(), 1);
            assert_eq!(state.known.get(&suffix.id()), Some(&suffix));
            assert_eq!(*state.checkpoint_tail_batches, 1);
            assert_eq!(*state.checkpoint_tail_bytes, suffix_bytes);
            let diagnostic = state.last_checkpoint_diagnostic.unwrap();
            assert_eq!(diagnostic.captured_batches, Some(1));
            assert_eq!(
                diagnostic.captured_bytes,
                Some(granted.replay_bytes - suffix_bytes)
            );
        }
        core.commit_checkpoint().unwrap();
        let remaining = core.admission.checkpoint_pressure();
        assert_eq!(remaining.identities, 0);
        assert_eq!(remaining.replay_batches, 0);
        assert_eq!(remaining.replay_bytes, 0);
        assert!(core.publisher.lock().checkpoint_state().known.is_empty());
        // An unchanged cut cannot release any receipt a second time.
        core.commit_checkpoint().unwrap();
        assert_eq!(core.admission.checkpoint_pressure().replay_batches, 0);
    }

    #[test]
    fn capture_racing_publication_conserves_all_receipts_and_bytes() {
        let fixture = Fixture::new();
        let db = fixture.open();
        let mut operations = Vec::with_capacity(32);
        for n in 1..=32 {
            let operation = fixture.operation(0, n);
            let operation_index = ((n - 1) % 4) as u32;
            let version = db
                .write_batch(operation, vec![put(n as u8); operation_index as usize + 1])
                .unwrap();
            operations.push((operation, operation_index, version));
        }
        db.drain().unwrap();
        let core = Arc::clone(&db.core);
        let initial = core.admission.checkpoint_pressure();
        let first = operations[0].2;
        let pending: Vec<_> = operations
            .into_iter()
            .skip(1)
            .map(|(operation, operation_index, _)| {
                (hold_publication(&core, operation, first), operation_index)
            })
            .collect();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let publisher_barrier = Arc::clone(&barrier);
        let publisher_core = Arc::clone(&core);
        let publisher = thread::spawn(move || {
            publisher_barrier.wait();
            for (receipt, operation_index) in pending {
                publish_receipt(&publisher_core, receipt, operation_index);
            }
        });
        barrier.wait();
        core.start_checkpoint().unwrap();
        core.progress_checkpoint(true).unwrap();
        publisher.join().unwrap();
        let remaining = core.admission.checkpoint_pressure();
        {
            let mut publisher = core.publisher.lock();
            let state = publisher.checkpoint_state();
            let captured = state.last_checkpoint_diagnostic.unwrap();
            assert_eq!(
                captured.captured_batches.unwrap() + remaining.replay_batches,
                initial.replay_batches
            );
            assert_eq!(
                captured.captured_bytes.unwrap() + remaining.replay_bytes,
                initial.replay_bytes
            );
            assert_eq!(remaining.identities, remaining.replay_batches);
            assert_eq!(state.known.len(), remaining.replay_batches);
            assert_eq!(*state.checkpoint_tail_batches, remaining.replay_batches);
            assert_eq!(*state.checkpoint_tail_bytes, remaining.replay_bytes);
            assert_eq!(
                state
                    .known
                    .values()
                    .map(|receipt| (receipt.address().len - 8) as usize)
                    .sum::<usize>(),
                remaining.replay_bytes,
            );
        }
        core.commit_checkpoint().unwrap();
        assert_eq!(core.admission.checkpoint_pressure().replay_batches, 0);
    }

    #[test]
    fn checkpoint_due_includes_granted_but_unpublished_burst() {
        let mut fixture = Fixture::new();
        fixture.config.replay_limits.max_batches = 4;
        let db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        db.write_batch(fixture.operation(0, 2), vec![put(2)])
            .unwrap();
        db.drain().unwrap();
        let core = Arc::clone(&db.core);
        let saved = {
            let _writer = core.index.writer();
            let mut publisher = core.publisher.lock();
            let state = publisher.checkpoint_state();
            (
                std::mem::take(state.known),
                std::mem::take(state.checkpoint_tail_batches),
                std::mem::take(state.checkpoint_tail_bytes),
            )
        };
        assert_eq!(core.admission.checkpoint_pressure().replay_batches, 2);
        assert!(core_checkpoint_due(&core));
        {
            let _writer = core.index.writer();
            let mut publisher = core.publisher.lock();
            let state = publisher.checkpoint_state();
            *state.known = saved.0;
            *state.checkpoint_tail_batches = saved.1;
            *state.checkpoint_tail_bytes = saved.2;
        }
    }
    #[test]
    fn concurrent_retirements_cover_each_requested_floor() {
        let fixture = Fixture::new();
        let db = fixture.open();
        let first = db
            .write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        let second = db
            .write_batch(fixture.operation(0, 2), vec![put(2)])
            .unwrap();
        db.drain().unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
        let core = Arc::clone(&db.core);
        let lower = MutationVersion {
            batch: first,
            operation_index: 0,
        };
        let upper = MutationVersion {
            batch: second,
            operation_index: 0,
        };
        let lower_core = Arc::clone(&core);
        let lower_thread = thread::spawn(move || {
            lower_core
                .retire_history_before(IndexReadCut::At(lower))
                .unwrap();
            assert!(matches!(lower_core.index.retention_floor(),
                    IndexReadCut::At(installed) if installed >= lower));
        });
        waiting.recv_timeout(core.config.timeout).unwrap();
        assert!(core.retirement.try_lock().is_none());
        let (started, pending) = mpsc::sync_channel(1);
        let upper_core = Arc::clone(&core);
        let upper_thread = thread::spawn(move || {
            started.send(()).unwrap();
            upper_core
                .retire_history_before(IndexReadCut::At(upper))
                .unwrap();
            assert!(matches!(upper_core.index.retention_floor(),
                    IndexReadCut::At(installed) if installed >= upper));
        });
        pending.recv_timeout(core.config.timeout).unwrap();
        resume.send(()).unwrap();
        lower_thread.join().unwrap();
        upper_thread.join().unwrap();
        assert_eq!(core.index.retention_floor(), IndexReadCut::At(upper));
    }

    #[test]
    fn every_checkpoint_interruption_recovers_captured_and_concurrent_suffix() {
        for stage in 0..=8 {
            let fixture = Fixture::new();
            let db = fixture.open();
            let first_id = fixture.operation(0, 1);
            let captured_id = fixture.operation(0, 2);
            let suffix_id = fixture.operation(0, 3);
            let first = db.write_batch(first_id, vec![put(1)]).unwrap();
            db.flush_index().unwrap();
            let captured = db.write_batch(captured_id, vec![put(2)]).unwrap();
            let suffix = db.write_batch(suffix_id, vec![put(3)]).unwrap();
            db.drain().unwrap();
            let (entered, waiting) = mpsc::sync_channel(1);
            let (resume, gate) = mpsc::sync_channel(1);
            {
                let mut hooks = db.core.test_hooks.lock();
                hooks.checkpoint_pause = Some((entered, gate));
                hooks.checkpoint_interruption = Some(stage);
            }
            let core = Arc::clone(&db.core);
            let receipt = hold_publication(&core, suffix_id, captured);
            core.start_checkpoint().unwrap();
            waiting.recv_timeout(core.config.timeout).unwrap();
            publish_receipt(&core, receipt, 0);
            resume.send(()).unwrap();
            assert!(core.progress_checkpoint(true).is_err(), "stage {stage}");
            assert!(core.ensure_healthy().is_err());
            drop(core);
            drop(db);
            let mut recovered = fixture.open();
            assert_eq!(
                recovered
                    .get(KeySpace(0), &1u32.to_be_bytes())
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &[3; 32],
                "stage {stage}",
            );
            for (id, value, version) in [
                (first_id, 1, first),
                (captured_id, 2, captured),
                (suffix_id, 3, suffix),
            ] {
                assert_eq!(
                    recovered.write_batch(id, vec![put(value)]).unwrap(),
                    version
                );
            }
            recovered.shutdown().unwrap();
        }
    }
}

#[cfg(test)]
mod retirement_request_tests {
    use super::super::pipeline_tests::Fixture;
    use super::*;

    fn put(value: u8) -> Mutation {
        Mutation::Put {
            keyspace: KeySpace(0),
            key: 1u32.to_be_bytes().to_vec().into(),
            value: vec![value; 32].into(),
        }
    }

    #[test]
    fn ordinary_checkpoint_after_idle_check_preserves_forced_retirement_request() {
        let fixture = Fixture::new();
        let db = fixture.open();
        let version = db
            .write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        db.drain().unwrap();
        let (entered, waiting) = mpsc::sync_channel(1);
        let (resume, gate) = mpsc::sync_channel(1);
        db.core.test_hooks.lock().checkpoint_pause = Some((entered, gate));
        let core = Arc::clone(&db.core);
        let requested = IndexReadCut::At(MutationVersion {
            batch: version,
            operation_index: 0,
        });
        // Retirement observed idle; an ordinary checkpoint wins the start
        // before retirement records its forced floor.
        core.progress_checkpoint(true).unwrap();
        core.start_checkpoint().unwrap();
        waiting.recv_timeout(core.config.timeout).unwrap();
        core_start_checkpoint_with_floor(&core, Some(requested.clone())).unwrap();
        {
            let mut publisher = core.publisher.lock();
            let state = publisher.checkpoint_state();
            assert_eq!(
                state
                    .background_checkpoint
                    .as_ref()
                    .unwrap()
                    .requested_floor,
                Some(requested.clone())
            );
        }
        assert_eq!(core.index.retention_floor(), IndexReadCut::BeforeAll);
        resume.send(()).unwrap();
        core.progress_checkpoint(true).unwrap();
        assert_eq!(core.index.retention_floor(), requested);
        assert_eq!(
            *core.publisher.lock().checkpoint_state().snapshot_generation,
            2
        );
    }

    #[test]
    fn forced_retirement_preserves_older_pin_clamp() {
        let fixture = Fixture::new();
        let db = fixture.open();
        let first = db
            .write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        let second = db
            .write_batch(fixture.operation(0, 2), vec![put(2)])
            .unwrap();
        db.drain().unwrap();
        let core = Arc::clone(&db.core);
        let older = IndexReadCut::At(MutationVersion {
            batch: first,
            operation_index: 0,
        });
        let requested = IndexReadCut::At(MutationVersion {
            batch: second,
            operation_index: 0,
        });
        let pin = core.index.pin_cut(older.clone()).unwrap();
        core.retire_history_before(requested.clone()).unwrap();
        assert_eq!(core.index.retention_floor(), older);
        drop(pin);
        core.retire_history_before(requested.clone()).unwrap();
        assert_eq!(core.index.retention_floor(), requested);
    }
}
