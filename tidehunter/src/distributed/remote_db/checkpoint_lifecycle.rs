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
    history: Option<ReceiptHistory>,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: VecDeque<RecentReceipt>,
    cut: Option<MutationVersion>,
    staging_manifest_ns: Option<u64>,
    receipt_history_ns: Option<u64>,
    receipt_history_sync_ns: Option<u64>,
    node_prefix_wait_ns: Option<u64>,
    index_prepare_ns: Option<u64>,
    index_persist_ns: Option<u64>,
    metadata_install_ns: Option<u64>,
    root_adoption_ns: Option<u64>,
    history_io: super::receipt_history::HistoryIoStats,
}

struct CapturingCheckpoint {
    commit: CheckpointCommit,
    cut: Option<MutationVersion>,
    batches: usize,
    bytes: usize,
    receipts: Vec<CheckpointReceipt>,
    stream_last: BTreeMap<(u32, u64), (u64, u32)>,
    positions: BTreeMap<(u32, u64), StreamPosition>,
    recent: VecDeque<RecentReceipt>,
    jobs: Vec<(Arc<mpsc::SyncSender<Command>>, u32, Vec<u64>)>,
    published_next: u64,
    started: Instant,
    prepare_work_ns: Option<u64>,
}

// The channel holds one result and carries only two per checkpoint. Keep owned
// results inline rather than add separate heap allocations for both phases.
#[allow(clippy::large_enum_variant)]
enum CheckpointMessage {
    Adopted(CompletedCheckpoint),
    Cleaned(AuthorityManifest, Option<u64>),
}

pub(super) struct BackgroundCheckpoint {
    reply: mpsc::Receiver<Result<CheckpointMessage, LocalDbError>>,
    authorize_cleanup: Option<mpsc::SyncSender<()>>,
    diagnostic: Option<CheckpointDiagnostic>,
    thread: Option<JoinHandle<()>>,
    started: Instant,
    generation: u64,
    installing: Option<CompletedCheckpoint>,
    install_work_ns: Option<u64>,
    capturing: Option<CapturingCheckpoint>,
    capture_index: Option<Arc<NativeIndex>>,
}

impl Drop for BackgroundCheckpoint {
    fn drop(&mut self) {
        if self.capturing.is_some()
            && let Some(index) = &self.capture_index
        {
            index.writer().abort_capture();
        }
        // Release a worker waiting for installation authorization before join.
        self.authorize_cleanup.take();
        // Never release the directory lock while persistence still owns a file.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl RemoteDatabase {
    pub(super) fn checkpoint_due(&self) -> bool {
        // Trigger below the configured replay bounds so capture/persistence
        // can overlap serving before the finite suffix exhausts admission.
        self.config
            .replay_limits
            .checkpoint_due(self.checkpoint_tail_batches, self.checkpoint_tail_bytes)
            || self.index.charged_metadata_bytes() >= self.config.max_index_bytes / 2
            || self
                .known
                .len()
                .saturating_add(self.active_identities.len())
                >= (self.config.max_admitted_batches / 2).max(1)
    }

    pub(super) fn start_checkpoint(&mut self) -> Result<(), LocalDbError> {
        let _poison = concurrent::FailureOnPanic(self.concurrent_failure.clone());
        let result = self.start_checkpoint_with_floor(None);
        if result.is_err() {
            self.healthy = false;
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
        }
        result
    }

    fn start_checkpoint_with_floor(
        &mut self,
        floor: Option<IndexReadCut>,
    ) -> Result<(), LocalDbError> {
        if self.background_checkpoint.is_some()
            || (floor.is_none()
                && self.checkpoint_tail_batches == 0
                && self.manifest.checkpoint.is_some())
        {
            return Ok(());
        }
        if self.manifest.checkpoint_staging.is_some() || self.manifest.checkpoint_obsolete.is_some()
        {
            return Err(LocalDbError::new(
                "checkpoint cleanup must complete before another installation",
            ));
        }
        // Published suffix slots remain outside the recoverable prefix, even
        // after their value buffers and active retry entries have been released.
        let pending_batches = self.submissions.len();
        let pending_bytes = self
            .submissions
            .iter()
            .try_fold(0usize, |bytes, entry| {
                bytes.checked_add(entry.encoded_bytes)
            })
            .ok_or_else(|| LocalDbError::new("checkpoint pending byte count overflow"))?;
        let batches = self
            .checkpoint_tail_batches
            .checked_sub(pending_batches)
            .ok_or_else(|| LocalDbError::new("checkpoint pending count exceeds tail"))?;
        let bytes = self
            .checkpoint_tail_bytes
            .checked_sub(pending_bytes)
            .ok_or_else(|| LocalDbError::new("checkpoint pending bytes exceed tail"))?;
        // A gap may consume the entire admitted tail. Wait for publication rather
        // than repeatedly writing the same cut while that gap is in flight.
        if batches == 0 && floor.is_none() && self.manifest.checkpoint.is_some() {
            return Ok(());
        }
        let started = Instant::now();
        self.snapshot_generation = self
            .snapshot_generation
            .checked_add(1)
            .ok_or_else(|| LocalDbError::new("snapshot counter exhausted"))?;
        let generation = self.snapshot_generation;
        self.write_timeline.checkpoint_generation(
            "freeze",
            None,
            self.epoch,
            None,
            Some(generation),
        );
        self.write_timeline.checkpoint_generation(
            "capture_start",
            None,
            self.epoch,
            None,
            Some(generation),
        );
        let commit = CheckpointCommit {
            epoch: self.epoch,
            generation: self.snapshot_generation,
            metadata_bytes: 0,
            metadata_digest: [0; 32],
            index_files: Vec::new(),
        };
        let cut = self.latest;
        let index_cut = cut.map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        let replay = self.config.history_persistence == HistoryPersistence::WalReplay;
        let mut receipts = Vec::with_capacity(if replay { 0 } else { self.known.len() });
        self.stream_last.clear();
        let mut recent: BTreeMap<_, _> = if replay {
            self.recent
                .iter()
                .map(|receipt| (receipt.version, *receipt))
                .collect()
        } else {
            BTreeMap::new()
        };
        for receipt in self.known.values() {
            if cut.is_none_or(|cut| receipt.version() > cut.batch) {
                continue;
            }
            let node = self
                .partition_indexes
                .get(&receipt.address().partition)
                .ok_or_else(|| {
                    LocalDbError::new("checkpoint receipt names an unknown partition")
                })?;
            if !self.retained_epochs[*node].contains(&receipt.version().epoch) {
                return Err(LocalDbError::new(
                    "checkpoint receipt is outside retained inventory",
                ));
            }
            if replay {
                let address = receipt.address();
                self.stream_last
                    .entry((address.partition, receipt.version().epoch))
                    .and_modify(|last| {
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
            } else {
                receipts.push((*receipt).into());
            }
        }
        if replay {
            debug_assert!(
                self.known.values().all(|receipt| {
                    cut.is_some_and(|cut| receipt.version() <= cut.batch)
                        || self
                            .stream_last
                            .get(&(receipt.address().partition, receipt.version().epoch))
                            .is_none_or(|last| receipt.address().offset > last.0)
                }),
                "stream offsets cross the checkpoint cut"
            );
        }
        let jobs: Vec<_> = self
            .retained_epochs
            .iter()
            .enumerate()
            .map(|(node, epochs)| {
                Ok((
                    self.clients[node]
                        .sender
                        .as_ref()
                        .ok_or_else(|| LocalDbError::new("TCP worker stopped"))?
                        .clone(),
                    self.config.endpoints[node].partition,
                    epochs.clone(),
                ))
            })
            .collect::<Result<_, LocalDbError>>()?;
        let prepare_started = self.checkpoint_diagnostics.is_some().then(Instant::now);
        {
            let mut index = self.index.writer();
            match floor {
                Some(floor) => index.begin_compacted_capture(index_cut, floor),
                None => index.begin_capture(index_cut),
            }
        }
        .map_err(|error| {
            self.write_timeline.checkpoint_generation(
                "capture_failed",
                None,
                self.epoch,
                None,
                Some(generation),
            );
            LocalDbError::component(error)
        })?;
        let (_, reply) = mpsc::sync_channel(1);
        self.background_checkpoint = Some(BackgroundCheckpoint {
            reply,
            authorize_cleanup: None,
            diagnostic: None,
            thread: None,
            started,
            generation,
            installing: None,
            install_work_ns: self.checkpoint_diagnostics.as_ref().map(|_| 0),
            capturing: Some(CapturingCheckpoint {
                commit,
                cut,
                batches,
                bytes,
                receipts,
                stream_last: self.stream_last.clone(),
                positions: self.positions.clone(),
                recent: recent.into_values().collect(),
                jobs,
                published_next: self.publication.published_prefix().next_sequence(),
                started,
                prepare_work_ns: prepare_started.map(elapsed),
            }),
            capture_index: Some(self.index.clone()),
        });
        self.write_stats.checkpoint_count += 1;
        self.progress_capture(false)?;
        self.write_stats.checkpoint_ns += elapsed(started);
        Ok(())
    }

    fn progress_capture(&mut self, wait: bool) -> Result<bool, LocalDbError> {
        let Some(mut capture) = self
            .background_checkpoint
            .as_mut()
            .unwrap()
            .capturing
            .take()
        else {
            return Ok(true);
        };
        let started = self.checkpoint_diagnostics.is_some().then(Instant::now);
        loop {
            let prepared = self.index.writer().capture_step(32);
            if let Some(prepared) = prepared {
                if let (Some(total), Some(started)) = (&mut capture.prepare_work_ns, started) {
                    *total = total.saturating_add(elapsed(started));
                }
                self.write_timeline.checkpoint_generation(
                    "capture_end",
                    None,
                    capture.commit.epoch,
                    None,
                    Some(capture.commit.generation),
                );
                self.background_checkpoint.take();
                self.launch_checkpoint(capture, prepared)?;
                return Ok(true);
            }
            if !wait {
                if let (Some(total), Some(started)) = (&mut capture.prepare_work_ns, started) {
                    *total = total.saturating_add(elapsed(started));
                }
                self.background_checkpoint.as_mut().unwrap().capturing = Some(capture);
                let _ = self.preparation_wake.try_send(());
                return Ok(false);
            }
        }
    }

    fn launch_checkpoint(
        &mut self,
        capture: CapturingCheckpoint,
        prepared: PreparedIndexImage,
    ) -> Result<(), LocalDbError> {
        let CapturingCheckpoint {
            commit,
            cut,
            batches,
            bytes,
            receipts,
            stream_last,
            mut positions,
            recent,
            mut jobs,
            published_next,
            started,
            prepare_work_ns: index_prepare_ns,
        } = capture;
        let generation = commit.generation;
        let diagnostics_enabled = self.checkpoint_diagnostics.is_some();
        let staging = CheckpointFiles {
            epoch: commit.epoch,
            generation: commit.generation,
            index_files: prepared
                .reserved_files(&self.path.join(commit.index_name()))
                .map_err(LocalDbError::component)?,
        };
        for name in staging.names() {
            match fs::symlink_metadata(self.path.join(name)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(LocalDbError::component(error)),
                Ok(_) => return Err(LocalDbError::new("checkpoint output path already exists")),
            }
        }
        let mut history = self.receipt_history.clone();
        if let Some(history) = &mut history {
            history.reset_io_stats();
        }
        let fragment_bytes = self.config.storage.fragment_bytes;
        let mut staging_manifest = self.manifest.clone();
        staging_manifest.checkpoint_staging = Some(staging.clone());
        let path = self.path.clone();
        let database = self.config.storage.database;
        let shape_yaml = self.manifest.shape_yaml.clone();
        let authority_epoch = commit.epoch;
        let wake = self.preparation_wake.clone();
        let timeline = self.write_timeline.clone();
        #[cfg(test)]
        let interruption = self.checkpoint_interruption;
        #[cfg(test)]
        let entered = self.before_checkpoint_io.clone();
        #[cfg(test)]
        let pause = self.checkpoint_pause.take();
        #[cfg(test)]
        let prefix_pause = Mutex::new(self.checkpoint_prefix_pause.take());
        #[cfg(test)]
        let corrupt_checkpoint_proof = self.corrupt_checkpoint_proof;
        #[cfg(test)]
        let adoption_pause = self.checkpoint_adoption_pause.take();
        #[cfg(test)]
        let cleanup_pause = self.checkpoint_cleanup_pause.take();
        let (sender, reply) = mpsc::sync_channel(1);
        let (authorize_cleanup, cleanup_authorized) = mpsc::sync_channel(1);
        let thread =
            thread::Builder::new()
                .name("value-wal-checkpoint".to_owned())
                .spawn(move || {
                    #[cfg(test)]
                    if let Some(entered) = entered {
                        let _ = entered.try_send(());
                    }
                    let result = (|| {
                        #[cfg(test)]
                        if let Some((entered, resume)) = pause {
                            let _ = entered.send(());
                            resume.recv().map_err(|_| {
                                LocalDbError::new("checkpoint test pause disconnected")
                            })?;
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
                        persist_manifest(&path, &staging_manifest)?;
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
                        let (
                            streams,
                            receipt_history_ns,
                            receipt_history_sync_ns,
                            node_prefix_wait_ns,
                        ) = if let Some(history) = &mut history {
                            timeline.checkpoint_generation(
                                "receipt_group_start",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            let mut by_stream: BTreeMap<(u32, u64), Vec<CheckpointReceipt>> =
                                BTreeMap::new();
                            for receipt in receipts {
                                by_stream
                                    .entry((receipt.address.partition, receipt.version.epoch))
                                    .or_default()
                                    .push(receipt);
                            }
                            for receipts in by_stream.values_mut() {
                                receipts.sort_unstable_by_key(|receipt| receipt.version);
                            }
                            timeline.checkpoint_generation(
                                "receipt_group_end",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            timeline.checkpoint_generation(
                                "receipt_append_start",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            let phase_started = diagnostics_enabled.then(Instant::now);
                            for receipts in by_stream.values() {
                                history.append_stream(receipts)?;
                            }
                            let receipt_history_ns = phase_started.map(elapsed);
                            timeline.checkpoint_generation(
                                "receipt_append_end",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            #[cfg(test)]
                            if interruption == Some(4) {
                                return Err(LocalDbError::new(
                                    "injected interruption before receipt metadata sync",
                                ));
                            }
                            let phase_started = diagnostics_enabled.then(Instant::now);
                            timeline.checkpoint_generation(
                                "receipt_sync_start",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            history.sync()?;
                            let receipt_history_sync_ns = phase_started.map(elapsed);
                            timeline.checkpoint_generation(
                                "receipt_sync_end",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            let phase_started = diagnostics_enabled.then(Instant::now);
                            let streams =
                                endpoint_map(&mut jobs, |_, (sender, partition, streams)| {
                                    let mut output = Vec::with_capacity(streams.len());
                                    for epoch in std::mem::take(streams) {
                                        let count = history
                                            .counts
                                            .get(&(*partition, epoch))
                                            .copied()
                                            .unwrap_or(0);
                                        let (reply, pending) = mpsc::sync_channel(1);
                                        timeline.checkpoint_generation(
                                            "control_enqueue",
                                            Some(*partition),
                                            epoch,
                                            Some(count),
                                            Some(generation),
                                        );
                                        sender
                                            .send(Command::Checkpoint(
                                                authority_epoch,
                                                epoch,
                                                count,
                                                reply,
                                            ))
                                            .map_err(|_| {
                                                LocalDbError::new("TCP worker lost request outcome")
                                            })?;
                                        let proof = receive(pending)?;
                                        timeline.checkpoint_generation(
                                            "control_observed",
                                            Some(*partition),
                                            epoch,
                                            Some(count),
                                            Some(generation),
                                        );
                                        #[cfg(test)]
                                        let proof = if corrupt_checkpoint_proof && *partition == 0 {
                                            let mut proof = proof;
                                            proof.digest[0] ^= 1;
                                            proof
                                        } else {
                                            proof
                                        };
                                        #[cfg(test)]
                                        if *partition == 0
                                            && let Some((entered, resume)) =
                                                prefix_pause.lock().take()
                                        {
                                            let _ = entered.send(());
                                            resume.recv().map_err(|_| {
                                                LocalDbError::new(
                                                    "checkpoint prefix test pause disconnected",
                                                )
                                            })?;
                                        }
                                        // Later durable appends do not change this exact prefix.
                                        if proof.prefix != count
                                            || proof.retained < count
                                            || proof.digest
                                                != history.prefix(*partition, epoch, count)?
                                        {
                                            return Err(LocalDbError::new(
                                                "checkpoint prefix differs from published receipts",
                                            ));
                                        }
                                        output.push(CheckpointStream {
                                            proof,
                                            state: history.state(*partition, epoch)?,
                                        });
                                    }
                                    Ok(output)
                                })?
                                .into_iter()
                                .flatten()
                                .collect();
                            let node_prefix_wait_ns = phase_started.map(elapsed);
                            (
                                streams,
                                receipt_history_ns,
                                receipt_history_sync_ns,
                                node_prefix_wait_ns,
                            )
                        } else {
                            let phase_started = diagnostics_enabled.then(Instant::now);
                            let output =
                                endpoint_map(&mut jobs, |_, (sender, partition, streams)| {
                                    let mut output = Vec::with_capacity(streams.len());
                                    for &epoch in streams.iter() {
                                        let key = (*partition, epoch);
                                        let previous = positions.get(&key);
                                        let Some(&(offset, len)) = stream_last.get(&key) else {
                                            output.push(previous.cloned().unwrap_or(
                                                StreamPosition {
                                                    partition: *partition,
                                                    epoch,
                                                    replay_from: 0,
                                                    last_offset: None,
                                                    durable_end: 0,
                                                    complete: false,
                                                },
                                            ));
                                            continue;
                                        };
                                        if previous.is_some_and(|position| {
                                            position.complete
                                                || position
                                                    .last_offset
                                                    .is_some_and(|last| last >= offset)
                                        }) {
                                            output.push(previous.unwrap().clone());
                                            continue;
                                        }
                                        let through = offset
                                            .checked_add(u64::from(len))
                                            .ok_or_else(|| {
                                                LocalDbError::new("checkpoint position overflow")
                                            })?;
                                        let (reply, pending) = mpsc::sync_channel(1);
                                        timeline.checkpoint_generation(
                                            "control_enqueue",
                                            Some(*partition),
                                            epoch,
                                            None,
                                            Some(generation),
                                        );
                                        sender
                                            .send(Command::CheckpointPosition(
                                                authority_epoch,
                                                epoch,
                                                through,
                                                reply,
                                            ))
                                            .map_err(|_| {
                                                LocalDbError::new("TCP worker lost request outcome")
                                            })?;
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
                                            && let Some((entered, resume)) =
                                                prefix_pause.lock().take()
                                        {
                                            let _ = entered.send(());
                                            resume.recv().map_err(|_| {
                                                LocalDbError::new(
                                                    "checkpoint prefix test pause disconnected",
                                                )
                                            })?;
                                        }
                                        if proof.database != database
                                            || proof.partition != *partition
                                            || proof.epoch != epoch
                                            || proof.durable_end < through
                                        {
                                            return Err(LocalDbError::new(
                                                "checkpoint position proof differs",
                                            ));
                                        }
                                        output.push(StreamPosition {
                                            partition: *partition,
                                            epoch,
                                            replay_from: offset - offset % fragment_bytes,
                                            last_offset: Some(offset),
                                            durable_end: proof.durable_end,
                                            complete: previous
                                                .is_some_and(|position| position.complete),
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
                            (Vec::new(), None, None, phase_started.map(elapsed))
                        };
                        let phase_started = diagnostics_enabled.then(Instant::now);
                        timeline.checkpoint_generation(
                            "index_persist_start",
                            None,
                            authority_epoch,
                            None,
                            Some(generation),
                        );
                        let persisted = prepared
                            .persist(&path.join(commit.index_name()))
                            .map_err(LocalDbError::component)?;
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
                        let encoded = if let Some(history) = &history {
                            let image = CheckpointImage {
                                format: 2,
                                database,
                                shape_yaml,
                                authority_epoch,
                                generation: commit.generation,
                                cut,
                                published_next,
                                index_bytes: persisted.metadata.bytes,
                                index_digest: persisted.metadata.digest,
                                streams,
                                history_root: history.root,
                            };
                            encode_checkpoint(&image)?
                        } else {
                            encode_checkpoint(&CheckpointImageV3 {
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
                            })?
                        };
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
                        install_metadata(&path, &commit.metadata_name(), &encoded)
                            .map_err(LocalDbError::component)?;
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
                            resume.recv().map_err(|_| {
                                LocalDbError::new("checkpoint adoption test pause disconnected")
                            })?;
                        }
                        // No other serving operation replaces the authority manifest.
                        // Adopt the durable root before the owner installs the image;
                        // the post-cut suffix remains recoverable from the value WALs.
                        staging_manifest.checkpoint_obsolete =
                            staging_manifest.checkpoint.replace(commit.clone());
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
                        persist_manifest(&path, &staging_manifest)?;
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
                            history_io: history
                                .as_ref()
                                .map_or_else(Default::default, |history| history.io_stats),
                            history,
                            positions,
                            recent,
                            cut,
                            staging_manifest_ns,
                            receipt_history_ns,
                            receipt_history_sync_ns,
                            node_prefix_wait_ns,
                            index_prepare_ns,
                            index_persist_ns,
                            metadata_install_ns,
                            root_adoption_ns,
                        })
                    })();
                    let completed = match result {
                        Ok(completed) => completed,
                        Err(error) => {
                            timeline.checkpoint_generation(
                                "worker_failed",
                                None,
                                authority_epoch,
                                None,
                                Some(generation),
                            );
                            let _ = sender.send(Err(error));
                            let _ = wake.try_send(());
                            return;
                        }
                    };
                    let mut manifest = completed.manifest.clone();
                    timeline.checkpoint_generation(
                        "result_send",
                        None,
                        authority_epoch,
                        None,
                        Some(generation),
                    );
                    if sender
                        .send(Ok(CheckpointMessage::Adopted(completed)))
                        .is_err()
                    {
                        return;
                    }
                    let _ = wake.try_send(());
                    if cleanup_authorized.recv().is_err() {
                        return;
                    }
                    let result = (|| {
                        #[cfg(test)]
                        if let Some((entered, resume)) = cleanup_pause {
                            let _ = entered.send(());
                            resume.recv().map_err(|_| {
                                LocalDbError::new("checkpoint cleanup test pause disconnected")
                            })?;
                        }
                        #[cfg(test)]
                        let interrupt_cleanup = interruption == Some(7);
                        #[cfg(not(test))]
                        let interrupt_cleanup = false;
                        timeline.checkpoint_generation(
                            "cleanup_start",
                            None,
                            authority_epoch,
                            None,
                            Some(generation),
                        );
                        let started = diagnostics_enabled.then(Instant::now);
                        cleanup_checkpoint_artifacts(&path, &mut manifest, interrupt_cleanup)
                            .map_err(|error| {
                                LocalDbError::new(format!(
                                    "checkpoint committed; artifact cleanup incomplete: {error}"
                                ))
                            })?;
                        let cleanup_ns = started.map(elapsed);
                        timeline.checkpoint_generation(
                            "cleanup_end",
                            None,
                            authority_epoch,
                            None,
                            Some(generation),
                        );
                        #[cfg(test)]
                        if interruption == Some(8) {
                            return Err(LocalDbError::new(
                                "injected interruption after checkpoint cleanup",
                            ));
                        }
                        Ok(CheckpointMessage::Cleaned(manifest, cleanup_ns))
                    })();
                    if result.is_err() {
                        timeline.checkpoint_generation(
                            "cleanup_failed",
                            None,
                            authority_epoch,
                            None,
                            Some(generation),
                        );
                    }
                    let _ = sender.send(result);
                    let _ = wake.try_send(());
                })
                .map_err(LocalDbError::component)?;
        self.manifest.checkpoint_staging = Some(staging);
        self.background_checkpoint = Some(BackgroundCheckpoint {
            reply,
            authorize_cleanup: Some(authorize_cleanup),
            diagnostic: None,
            thread: Some(thread),
            started,
            generation,
            installing: None,
            install_work_ns: self.checkpoint_diagnostics.as_ref().map(|_| 0),
            capturing: None,
            capture_index: None,
        });
        Ok(())
    }

    pub(super) fn progress_checkpoint(&mut self, wait: bool) -> Result<(), LocalDbError> {
        if self.background_checkpoint.is_none() {
            return Ok(());
        }
        let started = Instant::now();
        let _poison = concurrent::FailureOnPanic(self.concurrent_failure.clone());
        let result = (|| {
            if !self.progress_capture(wait)? {
                return Ok(());
            }
            // A captured checkpoint produces one adoption and one cleanup result.
            for _ in 0..2 {
                let pending = self
                    .background_checkpoint
                    .as_mut()
                    .unwrap()
                    .installing
                    .take();
                let background = self.background_checkpoint.as_ref().unwrap();
                let message = if let Some(completed) = pending {
                    CheckpointMessage::Adopted(completed)
                } else if wait {
                    background
                        .reply
                        .recv()
                        .map_err(|_| LocalDbError::new("checkpoint worker lost result"))??
                } else {
                    match background.reply.try_recv() {
                        Ok(result) => result?,
                        Err(mpsc::TryRecvError::Empty) => return Ok(()),
                        Err(mpsc::TryRecvError::Disconnected) => {
                            return Err(LocalDbError::new("checkpoint worker lost result"));
                        }
                    }
                };
                match message {
                    CheckpointMessage::Adopted(mut completed) => {
                        let work_started = self.checkpoint_diagnostics.is_some().then(Instant::now);
                        if let Some(persisted) = completed.persisted.take() {
                            self.write_timeline.checkpoint_generation(
                                "adopted",
                                None,
                                completed.commit.epoch,
                                None,
                                Some(completed.commit.generation),
                            );
                            self.write_timeline.checkpoint_generation(
                                "live_install_start",
                                None,
                                completed.commit.epoch,
                                None,
                                Some(completed.commit.generation),
                            );
                            self.index.writer().begin_install_image(persisted);
                        }
                        loop {
                            if self.index.writer().install_image_step(32) {
                                break;
                            }
                            if !wait {
                                let background = self.background_checkpoint.as_mut().unwrap();
                                if let (Some(total), Some(started)) =
                                    (&mut background.install_work_ns, work_started)
                                {
                                    *total = total.saturating_add(elapsed(started));
                                }
                                background.installing = Some(completed);
                                let _ = self.preparation_wake.try_send(());
                                return Ok(());
                            }
                        }
                        let mut diagnostic = CheckpointDiagnostic {
                            epoch: completed.commit.epoch,
                            generation: Some(completed.commit.generation),
                            captured_cut: completed.cut,
                            captured_batches: Some(completed.batches),
                            captured_bytes: Some(completed.bytes),
                            staging_manifest_ns: completed.staging_manifest_ns,
                            receipt_history_ns: completed.receipt_history_ns,
                            receipt_history_sync_ns: completed.receipt_history_sync_ns,
                            history_stream_opens: Some(completed.history_io.stream_opens),
                            history_previous_slot_reads: Some(
                                completed.history_io.previous_slot_reads,
                            ),
                            history_receipt_write_ops: Some(completed.history_io.receipt_write_ops),
                            history_receipt_write_bytes: Some(
                                completed.history_io.receipt_write_bytes,
                            ),
                            history_buffer_flushes: Some(completed.history_io.buffer_flushes),
                            history_node_write_ops: Some(completed.history_io.node_write_ops),
                            history_node_write_bytes: Some(completed.history_io.node_write_bytes),
                            history_sync_operations: Some(completed.history_io.sync_operations),
                            node_prefix_wait_ns: completed.node_prefix_wait_ns,
                            index_prepare_ns: completed.index_prepare_ns,
                            index_persist_ns: completed.index_persist_ns,
                            metadata_install_ns: completed.metadata_install_ns,
                            root_adoption_ns: completed.root_adoption_ns,
                            ..CheckpointDiagnostic::default()
                        };
                        self.manifest = completed.manifest;
                        self.receipt_history = completed.history;
                        if self.config.history_persistence == HistoryPersistence::WalReplay {
                            self.committed_complete = completed
                                .positions
                                .iter()
                                .filter_map(|(key, position)| position.complete.then_some(*key))
                                .collect();
                            self.positions = completed.positions;
                            self.recent = completed.recent;
                            self.recent_ids.clear();
                            self.recent_ids.extend(
                                self.recent
                                    .iter()
                                    .map(|receipt| (receipt.id, (receipt.version, receipt.digest))),
                            );
                        }
                        self.known.retain(|_, receipt| {
                            completed
                                .cut
                                .is_none_or(|cut| receipt.version() > cut.batch)
                        });
                        self.checkpoint_tail_batches = self
                            .checkpoint_tail_batches
                            .checked_sub(completed.batches)
                            .ok_or_else(|| {
                                LocalDbError::new("checkpoint captured batch count exceeds tail")
                            })?;
                        self.checkpoint_tail_bytes = self
                            .checkpoint_tail_bytes
                            .checked_sub(completed.bytes)
                            .ok_or_else(|| {
                                LocalDbError::new("checkpoint captured byte count exceeds tail")
                            })?;
                        diagnostic.owner_adoption_ns = match (
                            self.background_checkpoint.as_ref().unwrap().install_work_ns,
                            work_started,
                        ) {
                            (Some(total), Some(started)) => {
                                Some(total.saturating_add(elapsed(started)))
                            }
                            _ => None,
                        };
                        self.write_timeline.checkpoint_generation(
                            "live_install_end",
                            None,
                            completed.commit.epoch,
                            None,
                            Some(completed.commit.generation),
                        );
                        #[cfg(test)]
                        if self.checkpoint_interruption == Some(3) {
                            return Err(LocalDbError::new(
                                "checkpoint committed; injected interruption before obsolete-file cleanup",
                            ));
                        }
                        let background = self.background_checkpoint.as_mut().unwrap();
                        background.diagnostic = Some(diagnostic);
                        background
                            .authorize_cleanup
                            .take()
                            .ok_or_else(|| {
                                LocalDbError::new("checkpoint cleanup already authorized")
                            })?
                            .send(())
                            .map_err(|_| LocalDbError::new("checkpoint cleanup worker stopped"))?;
                        if !wait {
                            return Ok(());
                        }
                    }
                    CheckpointMessage::Cleaned(manifest, cleanup_ns) => {
                        self.write_timeline.checkpoint_generation(
                            "cleaned",
                            None,
                            self.epoch,
                            None,
                            Some(self.background_checkpoint.as_ref().unwrap().generation),
                        );
                        let mut background = self.background_checkpoint.take().unwrap();
                        let mut diagnostic = background.diagnostic.take().ok_or_else(|| {
                            LocalDbError::new("checkpoint cleanup preceded installation")
                        })?;
                        self.manifest = manifest;
                        diagnostic.obsolete_cleanup_ns = cleanup_ns;
                        let diagnostic = self.record_checkpoint_diagnostic(diagnostic);
                        self.last_checkpoint_diagnostic = Some(diagnostic);
                        self.write_stats.background_checkpoint_ns += elapsed(background.started);
                        return Ok(());
                    }
                }
            }
            Err(LocalDbError::new(
                "checkpoint exceeded its two-phase progress bound",
            ))
        })();
        self.write_stats.checkpoint_ns += elapsed(started);
        if let Err(error) = &result {
            record_first_failure(
                &self.first_failure,
                None,
                error.uncertain_version,
                "checkpoint_progress",
                error,
            );
            self.healthy = false;
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
            if let Some(background) = self.background_checkpoint.take() {
                self.write_stats.background_checkpoint_ns += elapsed(background.started);
                drop(background);
            }
        }
        result
    }

    /// Complete every captured cut needed to cover the current drained prefix.
    pub(super) fn commit_checkpoint(&mut self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        if !self.submissions.is_empty() || self.inflight.iter().any(|queue| !queue.is_empty()) {
            return Err(LocalDbError::new(
                "checkpoint requires drained complete publication",
            ));
        }
        self.progress_checkpoint(true)?;
        if self.checkpoint_tail_batches != 0 || self.manifest.checkpoint.is_none() {
            self.start_checkpoint()?;
            self.progress_checkpoint(true)?;
        }
        Ok(())
    }

    /// Sui supplies a floor capped by every historical Stop batch. Index capture
    /// atomically clamps it to existing pins; only the adopted actual floor is
    /// authoritative. No live values move and no physical address is reused.
    pub(super) fn retire_history_before(
        &mut self,
        floor: IndexReadCut,
    ) -> Result<(), LocalDbError> {
        #[cfg(test)]
        if std::mem::take(&mut self.retire_preflight_busy_once) {
            return Err(LocalDbError::new("injected retirement preflight busy"));
        }
        #[cfg(all(feature = "experimental-nvmeof", target_os = "linux"))]
        let _live_pause = self.reads.pause_live_for_retirement()?;
        self.drain()?;
        self.progress_checkpoint(true)?;
        let floor = if floor == IndexReadCut::Latest {
            self.latest
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
        } else {
            floor
        };
        let installed = self.index.retention_floor();
        let advances = match (&floor, &installed) {
            (IndexReadCut::At(requested), IndexReadCut::At(previous)) => requested > previous,
            (IndexReadCut::At(_), IndexReadCut::BeforeAll) => true,
            _ => false,
        };
        let result = (|| {
            if advances {
                self.start_checkpoint_with_floor(Some(floor))?;
                self.progress_checkpoint(true)?;
                // A newly observed Stop may be an old retained anchor. Expired cuts
                // cannot be resurrected, and the durable floor must never regress.
                self.commit_checkpoint()?;
            } else {
                self.commit_checkpoint()?;
            }
            self.retire_unreachable_sealed_files()
        })();
        if result.is_err() {
            self.healthy = false;
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
        }
        result
    }

    pub(super) fn retire_unreachable_sealed_files(&mut self) -> Result<(), LocalDbError> {
        for node in 0..self.clients.len() {
            let partition = self.config.endpoints[node].partition;
            for &epoch in &self.retained_epochs[node] {
                if epoch >= self.epoch {
                    continue;
                }
                let replay = self.config.history_persistence == HistoryPersistence::WalReplay;
                let prefix = self
                    .receipt_history
                    .as_ref()
                    .and_then(|history| history.counts.get(&(partition, epoch)))
                    .copied()
                    .unwrap_or(0);
                if if replay {
                    !self.committed_complete.contains(&(partition, epoch))
                } else {
                    prefix == 0
                } {
                    continue;
                }
                let mut after = None;
                let authority_epoch = self.epoch;
                loop {
                    let files = self.clients[node].control(move |client| {
                        client.retirement_files(authority_epoch, epoch, after, 256)
                    })?;
                    if files.is_empty() {
                        break;
                    }
                    after = files.last().copied();
                    let mut retired = Vec::with_capacity(files.len());
                    for offset in files {
                        let end = offset
                            .checked_add(self.config.storage.wal_file_bytes)
                            .ok_or_else(|| LocalDbError::new("retirement file bounds overflow"))?;
                        let reachable = self
                            .index
                            .references_wal_file(partition, epoch, offset, end);
                        if !reachable {
                            retired.push(offset);
                        }
                    }
                    if !retired.is_empty() {
                        self.clients[node].control(move |client| {
                            if replay {
                                client.retire_wal_files(authority_epoch, epoch, retired)
                            } else {
                                client.retire_files(authority_epoch, epoch, prefix, retired)
                            }
                        })?;
                    }
                }
            }
        }
        Ok(())
    }
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

    #[test]
    fn request_timeline_splits_checkpoint_without_legacy_profile() {
        let fixture = Fixture::new().history(HistoryPersistence::OnDisk);
        let mut db = fixture.open();
        assert_eq!(db.config.write_profile_limit, 0);
        db.write_timeline.arm(4).unwrap();
        db.write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        db.flush_index().unwrap();
        let generation = db.snapshot_generation;
        let export = db.write_timeline.finish().unwrap();
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
            "receipt_group_start",
            "receipt_group_end",
            "receipt_append_start",
            "receipt_append_end",
            "receipt_sync_start",
            "receipt_sync_end",
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
    fn bounded_install_keeps_credits_until_all_cells_are_installed() {
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
        db.checkpoint_adoption_pause = Some((entered, gate));
        db.start_checkpoint().unwrap();
        assert!(
            db.background_checkpoint
                .as_ref()
                .unwrap()
                .capturing
                .is_some()
        );
        assert!(db.progress_capture(false).unwrap());
        waiting.recv().unwrap();
        db.write_batch(fixture.operation(0, 100), vec![put(99)])
            .unwrap();
        db.drain().unwrap();
        let before = (db.checkpoint_tail_batches, db.checkpoint_tail_bytes);
        resume.send(()).unwrap();
        let background = db.background_checkpoint.as_mut().unwrap();
        let result = background.reply.recv().unwrap();
        let (sender, receiver) = mpsc::sync_channel(1);
        let original = std::mem::replace(&mut background.reply, receiver);
        sender.send(result).unwrap();
        db.progress_checkpoint(false).unwrap();
        assert!(
            db.background_checkpoint
                .as_ref()
                .unwrap()
                .installing
                .is_some()
        );
        assert_eq!(
            (db.checkpoint_tail_batches, db.checkpoint_tail_bytes),
            before
        );
        db.background_checkpoint.as_mut().unwrap().reply = original;
        db.progress_checkpoint(true).unwrap();
        assert_eq!(db.checkpoint_tail_batches, 1);
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
        let mut db = fixture.open();
        db.write_batch(fixture.operation(0, 1), vec![put(1)])
            .unwrap();
        db.flush_index().unwrap();
        let root = db.manifest.checkpoint.as_ref().unwrap().generation;
        db.write_batch(fixture.operation(1, 2), vec![put(2)])
            .unwrap();
        db.drain().unwrap();
        let batches = db.checkpoint_tail_batches;
        let bytes = db.checkpoint_tail_bytes;
        let floor = db.index.retention_floor();
        db.write_timeline.arm(4).unwrap();
        db.checkpoint_interruption = Some(5);
        assert!(db.flush_index().is_err());
        let generation = db.snapshot_generation;
        assert_eq!(db.checkpoint_tail_batches, batches);
        assert_eq!(db.checkpoint_tail_bytes, bytes);
        assert_eq!(db.index.retention_floor(), floor);
        let manifest: AuthorityManifest = read_metadata(&db.path.join(MANIFEST)).unwrap();
        assert_eq!(manifest.checkpoint.unwrap().generation, root);
        let export = db.write_timeline.finish().unwrap();
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
