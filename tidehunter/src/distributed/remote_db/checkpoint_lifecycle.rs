//! Native-style freeze/persist/install checkpoints. Only the final authority
//! manifest adopts an image; all acknowledged suffix batches remain WAL-backed.

use super::super::native_index::PersistedIndexImage;
use super::*;

struct CompletedCheckpoint {
    commit: CheckpointCommit,
    persisted: PersistedIndexImage,
    batches: usize,
    bytes: usize,
    history: ReceiptHistory,
    cut: Option<MutationVersion>,
}

pub(super) struct BackgroundCheckpoint {
    reply: mpsc::Receiver<Result<CompletedCheckpoint, LocalDbError>>,
    thread: Option<JoinHandle<()>>,
    started: Instant,
}

impl Drop for BackgroundCheckpoint {
    fn drop(&mut self) {
        // Never release the directory lock while persistence still owns a file.
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl RemoteDatabase {
    pub(super) fn checkpoint_due(&self) -> bool {
        self.checkpoint_tail_batches >= CHECKPOINT_MAX_TAIL_BATCHES / 2
            || self.checkpoint_tail_bytes >= CHECKPOINT_MAX_TAIL_BYTES / 2
            || self.index.read().stats().charged_metadata_bytes >= self.config.max_index_bytes / 2
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

    pub(super) fn force_admission_checkpoint(&mut self) -> Result<(), LocalDbError> {
        let started = Instant::now();
        let result = self.drain().and_then(|()| self.commit_checkpoint());
        self.write_stats.foreground_checkpoint_ns += elapsed(started);
        self.write_stats.foreground_checkpoint_count += 1;
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
        let pending_batches = self.active_identities.len();
        let pending_bytes = self
            .submissions
            .iter()
            .filter_map(|entry| entry.batch.as_ref())
            .try_fold(0usize, |bytes, batch| {
                bytes.checked_add(batch.encoded().len())
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
        let commit = CheckpointCommit {
            epoch: self.epoch,
            generation: self.snapshot_generation,
            metadata_bytes: 0,
            metadata_digest: [0; 32],
            index_files: Vec::new(),
        };
        let cut = self.latest;
        let index_cut = cut.map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        let prepared = {
            let index = self.index.read();
            match floor {
                Some(floor) => index.prepare_compacted_image(index_cut, floor),
                None => index.prepare_image(index_cut),
            }
        }
        .map_err(LocalDbError::component)?;
        let staging = CheckpointFiles {
            epoch: commit.epoch,
            generation: commit.generation,
            index_files: prepared
                .new_files(&self.path.join(commit.index_name()))
                .map_err(LocalDbError::component)?,
        };
        for name in staging.names() {
            match fs::symlink_metadata(self.path.join(name)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(LocalDbError::component(error)),
                Ok(_) => return Err(LocalDbError::new("checkpoint output path already exists")),
            }
        }
        let mut by_stream: BTreeMap<(u32, u64), Vec<CheckpointReceipt>> = BTreeMap::new();
        for receipt in self.known.values() {
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
            by_stream
                .entry((receipt.address().partition, receipt.version().epoch))
                .or_default()
                .push((*receipt).into());
        }
        for receipts in by_stream.values_mut() {
            receipts.sort_unstable_by_key(|receipt| receipt.version);
        }
        let mut jobs: Vec<_> = self
            .retained_epochs
            .iter()
            .enumerate()
            .map(|(node, epochs)| {
                (
                    self.clients[node].client.clone(),
                    self.config.endpoints[node].partition,
                    epochs.clone(),
                )
            })
            .collect();
        let mut history = self.receipt_history.clone();
        let mut staging_manifest = self.manifest.clone();
        staging_manifest.checkpoint_staging = Some(staging.clone());
        let path = self.path.clone();
        let database = self.config.storage.database;
        let shape_yaml = self.manifest.shape_yaml.clone();
        let authority_epoch = self.epoch;
        let published_next = self.publication.published_prefix().next_sequence();
        let wake = self.preparation_wake.clone();
        #[cfg(test)]
        let interruption = self.checkpoint_interruption;
        #[cfg(test)]
        let entered = self.before_checkpoint_io.clone();
        #[cfg(test)]
        let pause = self.checkpoint_pause.take();
        let (sender, reply) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
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
                        resume
                            .recv()
                            .map_err(|_| LocalDbError::new("checkpoint test pause disconnected"))?;
                    }
                    // Reserve output names durably before any checkpoint artifact
                    // can be written. This immutable snapshot cannot race owner
                    // appends, which do not replace the authority manifest.
                    persist_manifest(&path, &staging_manifest)?;
                    #[cfg(test)]
                    if interruption == Some(0) {
                        return Err(LocalDbError::new(
                            "injected interruption after checkpoint intent",
                        ));
                    }
                    for (_, receipts) in by_stream {
                        for receipt in receipts {
                            history.append(receipt)?;
                        }
                    }
                    #[cfg(test)]
                    if interruption == Some(4) {
                        return Err(LocalDbError::new(
                            "injected interruption before receipt metadata sync",
                        ));
                    }
                    history.sync()?;
                    let streams = endpoint_map(&mut jobs, |_, (client, partition, streams)| {
                        let mut client = client.lock();
                        let mut output = Vec::with_capacity(streams.len());
                        for epoch in std::mem::take(streams) {
                            let count = history
                                .counts
                                .get(&(*partition, epoch))
                                .copied()
                                .unwrap_or(0);
                            let proof = client
                                .checkpoint(authority_epoch, epoch, count)
                                .map_err(LocalDbError::component)?;
                            // Later durable appends do not change this exact prefix.
                            if proof.prefix != count
                                || proof.retained < count
                                || proof.digest != history.prefix(*partition, epoch, count)?
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
                    let persisted = prepared
                        .persist(&path.join(commit.index_name()))
                        .map_err(LocalDbError::component)?;
                    #[cfg(test)]
                    if interruption == Some(1) {
                        return Err(LocalDbError::new(
                            "injected interruption after checkpoint image",
                        ));
                    }
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
                    let encoded = encode_checkpoint(&image)?;
                    let mut commit = commit;
                    commit.index_files = persisted.files();
                    commit.index_files.sort_unstable();
                    commit.index_files.dedup();
                    commit.metadata_bytes = encoded.len() as u64;
                    commit.metadata_digest = Blake2b::<U32>::digest(&encoded).into();
                    install_metadata(&path, &commit.metadata_name(), &encoded)
                        .map_err(LocalDbError::component)?;
                    #[cfg(test)]
                    if interruption == Some(2) {
                        return Err(LocalDbError::new(
                            "injected interruption before checkpoint adoption",
                        ));
                    }
                    Ok(CompletedCheckpoint {
                        commit,
                        persisted,
                        batches,
                        bytes,
                        history,
                        cut,
                    })
                })();
                let _ = sender.send(result);
                let _ = wake.try_send(());
            })
            .map_err(LocalDbError::component)?;
        self.manifest.checkpoint_staging = Some(staging);
        self.background_checkpoint = Some(BackgroundCheckpoint {
            reply,
            thread: Some(thread),
            started,
        });
        self.write_stats.checkpoint_count += 1;
        self.write_stats.checkpoint_ns += elapsed(started);
        Ok(())
    }

    pub(super) fn progress_checkpoint(&mut self, wait: bool) -> Result<(), LocalDbError> {
        let Some(background) = self.background_checkpoint.as_ref() else {
            return Ok(());
        };
        let started = Instant::now();
        let completed = if wait {
            background
                .reply
                .recv()
                .map_err(|_| LocalDbError::new("checkpoint worker lost result"))
        } else {
            match background.reply.try_recv() {
                Ok(result) => Ok(result),
                Err(mpsc::TryRecvError::Empty) => return Ok(()),
                Err(mpsc::TryRecvError::Disconnected) => {
                    Err(LocalDbError::new("checkpoint worker lost result"))
                }
            }
        };
        let _poison = concurrent::FailureOnPanic(self.concurrent_failure.clone());
        let background = self
            .background_checkpoint
            .take()
            .expect("observed checkpoint remains owned");
        let checkpoint_started = background.started;
        drop(background);
        let result = (|| {
            let completed = completed??;
            // Durable adoption precedes both live installation and reclamation.
            // The worker never changes the root while this owner accepts writes.
            self.manifest.checkpoint_obsolete = self.manifest.checkpoint.replace(completed.commit);
            self.manifest.checkpoint_staging = None;
            persist_manifest(&self.path, &self.manifest)?;
            self.index.write().install_image(completed.persisted);
            self.receipt_history = completed.history;
            self.known.retain(|_, receipt| {
                completed
                    .cut
                    .is_none_or(|cut| receipt.version() > cut.batch)
            });
            self.checkpoint_tail_batches = self
                .checkpoint_tail_batches
                .checked_sub(completed.batches)
                .ok_or_else(|| LocalDbError::new("checkpoint captured batch count exceeds tail"))?;
            self.checkpoint_tail_bytes = self
                .checkpoint_tail_bytes
                .checked_sub(completed.bytes)
                .ok_or_else(|| LocalDbError::new("checkpoint captured byte count exceeds tail"))?;
            #[cfg(test)]
            if self.checkpoint_interruption == Some(3) {
                return Err(LocalDbError::new(
                    "checkpoint committed; injected interruption before obsolete-file cleanup",
                ));
            }
            cleanup_checkpoint_artifacts(&self.path, &mut self.manifest).map_err(|error| {
                LocalDbError::new(format!(
                    "checkpoint committed; artifact cleanup incomplete: {error}"
                ))
            })
        })();
        self.write_stats.checkpoint_ns += elapsed(started);
        self.write_stats.background_checkpoint_ns += elapsed(checkpoint_started);
        if result.is_err() {
            self.healthy = false;
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
        }
        result
    }

    /// Complete every captured cut needed to cover the current drained prefix.
    pub(super) fn commit_checkpoint(&mut self) -> Result<(), LocalDbError> {
        self.ensure_healthy()?;
        if !self.submissions.is_empty() || self.inflight.iter().any(Option::is_some) {
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
        self.drain()?;
        self.progress_checkpoint(true)?;
        let floor = if floor == IndexReadCut::Latest {
            self.latest
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
        } else {
            floor
        };
        let installed = self.index.read().retention_floor();
        let advances = match (floor, installed) {
            (IndexReadCut::At(requested), IndexReadCut::At(previous)) => requested > previous,
            (IndexReadCut::At(_), IndexReadCut::BeforeAll) => true,
            _ => false,
        };
        if advances {
            self.start_checkpoint_with_floor(Some(floor))
                .inspect_err(|_| {
                    self.healthy = false;
                    self.concurrent_failure
                        .store(true, std::sync::atomic::Ordering::Release);
                    self.reads.poison();
                })?;
            self.progress_checkpoint(true)?;
        } else {
            // A newly observed Stop may be an old retained anchor. Expired cuts
            // cannot be resurrected, and the durable floor must never regress.
            self.commit_checkpoint()?;
        }
        let result = self.retire_unreachable_sealed_files();
        if result.is_err() {
            self.healthy = false;
            self.concurrent_failure
                .store(true, std::sync::atomic::Ordering::Release);
            self.reads.poison();
        }
        result
    }

    fn retire_unreachable_sealed_files(&mut self) -> Result<(), LocalDbError> {
        for node in 0..self.clients.len() {
            let partition = self.config.endpoints[node].partition;
            for &epoch in &self.retained_epochs[node] {
                if epoch >= self.epoch {
                    continue;
                }
                let prefix = self
                    .receipt_history
                    .counts
                    .get(&(partition, epoch))
                    .copied()
                    .unwrap_or(0);
                if prefix == 0 {
                    continue;
                }
                let mut after = None;
                loop {
                    let files = self.clients[node]
                        .client
                        .lock()
                        .retirement_files(self.epoch, epoch, after, 256)
                        .map_err(LocalDbError::component)?;
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
                            .read()
                            .references_wal_file(partition, epoch, offset, end);
                        if !reachable {
                            retired.push(offset);
                        }
                    }
                    if !retired.is_empty() {
                        self.clients[node]
                            .client
                            .lock()
                            .retire_files(self.epoch, epoch, prefix, retired)
                            .map_err(LocalDbError::component)?;
                    }
                }
            }
        }
        Ok(())
    }
}
