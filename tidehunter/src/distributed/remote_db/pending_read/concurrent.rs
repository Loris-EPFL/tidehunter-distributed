//! Read delegates retain exact immutable references, never a mutable writer
//! borrow. As in `TiKV`'s `ReadDelegate`, every completed/cache-only result checks
//! authority validity; an in-flight write is not an invalidation event.
#[allow(clippy::wildcard_imports)]
// Private read delegate shares its parent's bounded transport types.
use super::*;
use crate::distributed::sui::kv_capture;

// Keep misses inline; boxing would allocate on every uncached scalar read.
#[allow(clippy::large_enum_variant)]
enum GetResolution {
    Ready(Option<Bytes>),
    Miss {
        reference: ValueRecordReference,
        pin: IndexReadPin,
    },
}

impl ConcurrentRemoteDatabase {
    pub fn set_read_profile_limit(&self, limit: u64) {
        self.reads
            .state
            .diagnostics
            .limit
            .store(limit, Ordering::Relaxed);
        self.reads
            .state
            .diagnostics
            .remaining
            .store(limit, Ordering::Relaxed);
        self.reads.state.coordinator_diagnostics.arm(limit);
        self.reads.state.trace.arm(limit);
    }
    pub(crate) fn read_profile_enabled(&self) -> bool {
        self.reads.state.diagnostics.limit.load(Ordering::Relaxed) != 0
    }
    pub fn read_profile_monitor(&self) -> ReadProfileMonitor {
        ReadProfileMonitor {
            state: self.reads.state.clone(),
            fabric: self.config.tls.transport.is_fabric(),
        }
    }
    pub fn read_trace_snapshot(&self) -> ReadTraceSnapshot {
        self.reads.state.trace.snapshot()
    }

    pub fn traced_get(
        &self,
        ks: KeySpace,
        key: &[u8],
        caller_id: u64,
    ) -> Result<Option<Bytes>, LocalDbError> {
        let trace = self.reads.state.trace.begin(caller_id, 1);
        if let Some(trace) = &trace {
            trace.event("api_started");
        }
        let result = self.get_inner(ks, key, trace.as_ref());
        if let Some(trace) = trace {
            trace.event("api_finished");
            trace.finish(result.is_ok());
        }
        result
    }
    pub fn read_rpc_count(&self) -> u64 {
        self.reads.state.read_rpcs.load(Ordering::Relaxed)
    }

    /// A scalar miss first tries caller-owned progress on an idle lane. Neither
    /// the index nor cache guard survives into `read_inline` or a detached wait.
    ///
    /// # Errors
    /// Returns key, admission, authority, transport, or value-integrity errors.
    pub fn get(&self, ks: KeySpace, key: &[u8]) -> Result<Option<Bytes>, LocalDbError> {
        self.get_inner(ks, key, None)
    }

    fn get_inner(
        &self,
        ks: KeySpace,
        key: &[u8],
        trace: Option<&ReadTraceCallGuard>,
    ) -> Result<Option<Bytes>, LocalDbError> {
        if let Some(trace) = trace {
            trace.event("index_resolution_started");
        }
        let (resolution, node) = self.resolve_concurrent_get(ks, key, IndexReadCut::Latest)?;
        if let Some(trace) = trace {
            trace.event("index_resolution_finished");
        }
        let (reference, pin) = match resolution {
            GetResolution::Ready(value) => return Ok(value),
            GetResolution::Miss { reference, pin } => (reference, pin),
        };
        let node = node.expect("a miss has a route");
        let deadline = Instant::now() + self.config.timeout;
        // The member is planned once; a retry after backpressure reuses the
        // plan unless the reader set changed.
        let mut plan = None;
        loop {
            let generation = self.read_admission_generation();
            let child = trace.and_then(|trace| trace.child(node, vec![0]));
            let planned = self.reads.cached_plan(&mut plan, &reference)?;
            match self.prepare_concurrent_miss(
                ks,
                key,
                reference,
                node,
                true,
                pin.clone(),
                child,
                Some(planned),
            ) {
                Ok(pending) => return self.finish_ready_get(pending.wait()),
                Err(error) if RemoteDatabase::read_backpressured(&error) => {
                    if let Some(trace) = trace {
                        trace.event("admission_wait_started");
                    }
                    self.wait_read_admission(generation, deadline)?;
                    if let Some(trace) = trace {
                        trace.event("admission_wait_finished");
                    }
                    // A disabled cache can never answer the recheck, and probing
                    // it would only count a miss for a read that is still pending.
                    if self.cache.is_enabled()
                        && let Some(value) = self.cache.get_point(&reference, ks, key)
                    {
                        self.ensure_healthy()?;
                        return Ok(Some(value));
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// # Errors
    /// Returns key, admission, or authority errors before yielding a read handle.
    pub fn submit_get(&self, ks: KeySpace, key: &[u8]) -> Result<PendingGet, LocalDbError> {
        self.submit_get_cut(ks, key, IndexReadCut::Latest)
    }
    /// # Errors
    /// Returns key, admission, or authority errors for the requested read cut.
    pub fn submit_get_cut(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<PendingGet, LocalDbError> {
        self.prepare_concurrent_get(ks, key, cut, false)
    }

    /// Read one bounded key window at a caller-captured cut. Misses share the
    /// existing per-shard RPC batching and credit limits, not one RPC per key.
    /// Key validation errors remain per-key; authority and RPC errors fail the window.
    pub(crate) fn get_many_cut<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        cut: IndexReadCut,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.get_many_cut_inner(ks, keys, cut, None, 0)
    }

    pub(crate) fn begin_read_trace(
        &self,
        caller_id: u64,
        members: usize,
    ) -> Option<ReadTraceCallGuard> {
        let trace = self.reads.state.trace.begin(caller_id, members);
        if let Some(trace) = &trace {
            trace.event("api_started");
        }
        trace
    }

    pub(crate) fn traced_get_many_cut<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        cut: IndexReadCut,
        trace: Option<&ReadTraceCallGuard>,
        offset: usize,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.get_many_cut_inner(ks, keys, cut, trace, offset)
    }

    fn get_many_cut_inner<K: AsRef<[u8]>>(
        &self,
        ks: KeySpace,
        keys: &[K],
        cut: IndexReadCut,
        trace: Option<&ReadTraceCallGuard>,
        offset: usize,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        if let Some(trace) = trace {
            trace.event("index_resolution_started");
        }
        let mut sample = self
            .reads
            .state
            .coordinator_diagnostics
            .select(true, keys.len());
        self.ensure_healthy()?;
        if keys.len() > 256 {
            return Err(LocalDbError::new("read batch exceeds 256 keys"));
        }
        let started = self.read_profile_enabled().then(Instant::now);
        let mut values = vec![None; keys.len()];
        let mut missing = Vec::new();
        let mut groups = vec![Vec::new(); self.partition_indexes.len()];
        let (references, pin) = {
            // Resolve the cut once and use it for validation, every lookup and
            // the pin; no lock or guard is held across the batch.
            let cut = self.index.resolve_cut(cut);
            let clock = ResolutionSample::start(&sample);
            let validation = self
                .index
                .validate_cut(cut.clone())
                .map_err(LocalDbError::component);
            if let Some(sample) = &sample {
                sample.stage(ResolutionStage::CutValidation, clock);
            }
            validation?;
            let references = keys
                .iter()
                .map(|key| {
                    let clock = ResolutionSample::start(&sample);
                    let result = self
                        .index
                        .get(ks, key.as_ref(), cut.clone())
                        .map_err(LocalDbError::component);
                    if let Some(sample) = &sample {
                        sample.stage(ResolutionStage::IndexLookup, clock);
                    }
                    result
                })
                .collect::<Vec<_>>();
            // Keep lookup order, including duplicate references, and route and
            // allocate per 32-member chunk.
            for (chunk_index, chunk) in references.chunks(32).enumerate() {
                let base = chunk_index * 32;
                for (offset, reference) in chunk.iter().enumerate() {
                    let Ok(Some(reference)) = reference else {
                        continue;
                    };
                    kv_capture::reference(11, reference, Some(keys[base + offset].as_ref()), 1, 0);
                    let clock = ResolutionSample::start(&sample);
                    values[base + offset] = self.cache.get(reference);
                    if let Some(sample) = &sample {
                        sample.stage(ResolutionStage::CacheLookup, clock);
                    }
                }
                for (offset, reference) in chunk.iter().enumerate() {
                    let slot = base + offset;
                    let reference = match reference {
                        Ok(Some(reference)) => reference,
                        Ok(None) => {
                            if let Some(sample) = &sample {
                                sample.member(MemberOutcome::IndexMiss);
                            }
                            continue;
                        }
                        Err(_) => {
                            if let Some(sample) = &sample {
                                sample.member(MemberOutcome::Error);
                            }
                            continue;
                        }
                    };
                    if values[slot].is_some() {
                        if let Some(sample) = &sample {
                            sample.member(MemberOutcome::CacheHit);
                        }
                    } else {
                        if let Some(sample) = &sample {
                            sample.member(MemberOutcome::RemoteMiss);
                        }
                        let clock = ResolutionSample::start(&sample);
                        let node = self
                            .partition_indexes
                            .get(&reference.batch_address.partition)
                            .copied()
                            .ok_or_else(|| {
                                LocalDbError::new("reference names unregistered partition")
                            });
                        if let Ok(node) = node.as_ref() {
                            groups[*node].push((slot, *reference));
                            missing.push(slot);
                        }
                        if let Some(sample) = &sample {
                            sample.stage(ResolutionStage::RoutePreparation, clock);
                        }
                        node?;
                    }
                }
            }
            let pin = if missing.is_empty() {
                None
            } else {
                let clock = ResolutionSample::start(&sample);
                let pin = self.index.pin_cut(cut).map_err(LocalDbError::component);
                if let Some(sample) = &sample {
                    sample.stage(ResolutionStage::PinPreparation, clock);
                }
                Some(pin?)
            };
            (references, pin)
        };
        if let Some(trace) = trace {
            trace.event("index_resolution_finished");
        }
        let clock = ResolutionSample::start(&sample);
        let batch = ReadBatch::new(&self.reads, values, groups);
        if let Some(sample) = &sample {
            sample.stage(ResolutionStage::BatchPreparation, clock);
        }
        if let Some(started) = started {
            self.read_resolve_ns
                .fetch_add(elapsed(started), Ordering::Relaxed);
        }
        let mut batch = batch?;
        if let Some(sample) = &mut sample {
            sample.success();
        }
        batch.pin = pin;
        let clock = ResolutionSample::start(&sample);
        while !batch.complete() {
            self.ensure_healthy()?;
            let generation = self.read_admission_generation();
            batch.dispatch_traced(&self.reads, trace, offset)?;
            if batch.reads.is_empty() {
                if let Some(trace) = trace {
                    trace.event("admission_wait_started");
                }
                self.wait_read_admission(generation, batch.deadline)?;
                if let Some(trace) = trace {
                    trace.event("admission_wait_finished");
                }
            } else {
                let started = Instant::now();
                let result = batch.finish_one();
                self.value_read_ns
                    .fetch_add(elapsed(started), Ordering::Relaxed);
                result.inspect_err(|_| self.poison())?;
            }
        }
        if let Some(sample) = &sample {
            sample.stage(ResolutionStage::BatchWait, clock);
        }
        self.ensure_healthy()?;
        let clock = ResolutionSample::start(&sample);
        for slot in missing {
            let Ok(Some(reference)) = &references[slot] else {
                unreachable!("only live references were submitted");
            };
            let mutation = batch.values[slot]
                .as_ref()
                .expect("every admitted member completed");
            let prepared = self.cache.prepare_verified(*reference, mutation);
            self.cache.insert_prepared(prepared);
        }
        if let Some(sample) = &sample {
            sample.stage(ResolutionStage::PostreadCacheFill, clock);
        }
        let clock = ResolutionSample::start(&sample);
        let result = references
            .into_iter()
            .zip(batch.values)
            .zip(keys)
            .map(|((reference, mutation), key)| match reference {
                Err(error) => Err(error),
                Ok(None) => Ok(None),
                Ok(Some(_)) => RemoteDatabase::point_value(
                    ks,
                    key.as_ref(),
                    mutation.expect("every live reference was resolved"),
                )
                .inspect_err(|_| self.poison()),
            })
            .collect();
        if let Some(sample) = &sample {
            sample.stage(ResolutionStage::ResultMaterialization, clock);
        }
        self.ensure_healthy()?;
        if let Some(trace) = trace {
            trace.event("result_validation_finished");
        }
        Ok(result)
    }

    pub(crate) fn scan_index_page(
        &self,
        ks: KeySpace,
        after_reduced: Option<&[u8]>,
        reverse: bool,
        limit: usize,
        cut: IndexReadCut,
    ) -> Result<Vec<(Bytes, ValueRecordReference)>, LocalDbError> {
        self.ensure_healthy()?;
        let rows = self
            .index
            .scan_reduced(ks, after_reduced, reverse, limit, cut)
            .map_err(LocalDbError::component)?;
        self.ensure_healthy()?;
        Ok(rows)
    }
    fn resolve_concurrent_get(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
    ) -> Result<(GetResolution, Option<usize>), LocalDbError> {
        let mut sample = self.reads.state.coordinator_diagnostics.select(false, 1);
        self.ensure_healthy()?;
        let started = self.read_profile_enabled().then(Instant::now);
        let resolution = {
            // Resolve the cut once: the lookup and a miss's pin must see the
            // same cut. No lock or guard is held past each call.
            let cut = self.index.resolve_cut(cut);
            let clock = ResolutionSample::start(&sample);
            let reference = self
                .index
                .get(ks, key, cut.clone())
                .map_err(LocalDbError::component);
            if let Some(sample) = &sample {
                sample.stage(ResolutionStage::IndexLookup, clock);
            }
            match reference {
                Err(error) => {
                    if let Some(sample) = &sample {
                        sample.member(MemberOutcome::Error);
                    }
                    return Err(error);
                }
                Ok(None) => {
                    if let Some(sample) = &sample {
                        sample.member(MemberOutcome::IndexMiss);
                    }
                    GetResolution::Ready(None)
                }
                Ok(Some(reference)) => {
                    kv_capture::reference(11, &reference, Some(key), 1, 0);
                    let clock = ResolutionSample::start(&sample);
                    let cached = self.cache.get_point(&reference, ks, key);
                    if let Some(sample) = &sample {
                        sample.stage(ResolutionStage::CacheLookup, clock);
                    }
                    if let Some(value) = cached {
                        if let Some(sample) = &sample {
                            sample.member(MemberOutcome::CacheHit);
                        }
                        GetResolution::Ready(Some(value))
                    } else {
                        if let Some(sample) = &sample {
                            sample.member(MemberOutcome::RemoteMiss);
                        }
                        let clock = ResolutionSample::start(&sample);
                        let pin = self.index.pin_cut(cut).map_err(LocalDbError::component);
                        if let Some(sample) = &sample {
                            sample.stage(ResolutionStage::PinPreparation, clock);
                        }
                        GetResolution::Miss {
                            reference,
                            pin: pin?,
                        }
                    }
                }
            }
        };
        self.ensure_healthy()?;
        let node = if let GetResolution::Miss { reference, .. } = &resolution {
            let clock = ResolutionSample::start(&sample);
            let node = self
                .partition_indexes
                .get(&reference.batch_address.partition)
                .copied()
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"));
            if let Some(sample) = &sample {
                sample.stage(ResolutionStage::RoutePreparation, clock);
            }
            Some(node?)
        } else {
            None
        };
        if let Some(started) = started {
            self.read_resolve_ns
                .fetch_add(elapsed(started), Ordering::Relaxed);
        }
        if let Some(sample) = &mut sample {
            sample.success();
        }
        Ok((resolution, node))
    }

    fn prepare_concurrent_get(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
        inline: bool,
    ) -> Result<PendingGet, LocalDbError> {
        let (resolution, node) = self.resolve_concurrent_get(ks, key, cut)?;
        match resolution {
            GetResolution::Ready(value) => Ok(PendingGet {
                owner: self.reads.state.clone(),
                state: GetState::Ready(value),
            }),
            GetResolution::Miss { reference, pin } => self.prepare_concurrent_miss(
                ks,
                key,
                reference,
                node.expect("a miss has a route"),
                inline,
                pin,
                None,
                None,
            ),
        }
    }

    // Keep key, route, pin ownership and optional diagnostics explicit.
    #[allow(clippy::too_many_arguments)]
    fn prepare_concurrent_miss(
        &self,
        ks: KeySpace,
        key: &[u8],
        reference: ValueRecordReference,
        node: usize,
        inline: bool,
        pin: IndexReadPin,
        trace: Option<ReadTraceChild>,
        planned: Option<PlannedMembers<'_>>,
    ) -> Result<PendingGet, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        if let Some(trace) = &trace {
            trace.event("inline_lane_attempt");
        }
        let immediate = if inline {
            self.reads.read_inline_traced(
                node,
                self.epoch,
                &reference,
                key.len(),
                None,
                Some(pin.clone()),
                trace.as_ref(),
            )?
        } else {
            None
        };
        if let Some(trace) = &trace {
            trace.event(if immediate.is_some() {
                "inline_read_completed"
            } else {
                "inline_lane_unavailable"
            });
        }
        let state = if let Some(values) = immediate {
            self.value_read_ns
                .fetch_add(elapsed(started), Ordering::Relaxed);
            GetState::Ready(self.complete_concurrent_get(reference, ks, key, values)?)
        } else {
            let read = self.reads.submit_until_owned(
                node,
                std::slice::from_ref(&reference),
                key.len(),
                false,
                ReadContext {
                    deadline: Instant::now() + self.reads.timeout,
                    pin: Some(pin),
                    trace,
                },
                None,
                planned,
            )?;
            GetState::Pending {
                read,
                reference,
                ks,
                key: key.to_vec().into(),
            }
        };
        self.ensure_healthy()?;
        Ok(PendingGet {
            owner: self.reads.state.clone(),
            state,
        })
    }

    /// # Errors
    /// Rejects foreign handles, failed authorities, transport errors, or bad values.
    ///
    /// # Panics
    /// Panics only if an internal resolved-handle/completion invariant is broken.
    pub fn finish_ready_get(&self, completed: CompletedGet) -> Result<Option<Bytes>, LocalDbError> {
        if !Arc::ptr_eq(&self.reads.state, &completed.pending.owner) {
            return Err(LocalDbError::new(
                "read handle belongs to another authority/epoch",
            ));
        }
        self.value_read_ns
            .fetch_add(completed.wait_ns, Ordering::Relaxed);
        self.ensure_healthy()?;
        let result = match completed.pending.state {
            GetState::Ready(value) => Ok(value),
            GetState::Resolved { reference, ks, key } => {
                let (values, _) = completed
                    .result
                    .expect("resolved read has completion")
                    .inspect_err(|_| self.poison())?;
                self.complete_concurrent_get(reference, ks, &key, values)
            }
            _ => Err(LocalDbError::new("read is not a detached completion")),
        };
        self.ensure_healthy()?;
        result
    }

    fn complete_concurrent_get(
        &self,
        reference: ValueRecordReference,
        ks: KeySpace,
        key: &[u8],
        mut values: Vec<Mutation>,
    ) -> Result<Option<Bytes>, LocalDbError> {
        self.ensure_healthy()?;
        if values.len() != 1 || matches!(values.first(), Some(Mutation::Delete { .. })) {
            self.poison();
            return Err(LocalDbError::new(
                "live point reference returned an invalid record",
            ));
        }
        let mutation = values.pop().unwrap();
        let prepared = self.cache.prepare_verified(reference, &mutation);
        self.cache.insert_prepared(prepared);
        let result = RemoteDatabase::point_value(ks, key, mutation);
        self.ensure_healthy()?;
        result
    }

    /// # Errors
    /// Returns admission or authority errors for the exact immutable reference.
    pub fn submit_record(
        &self,
        reference: ValueRecordReference,
    ) -> Result<PendingRecord, LocalDbError> {
        let pin = self.pin_cut(IndexReadCut::At(MutationVersion {
            batch: reference.batch_version,
            operation_index: reference.descriptor.operation_index,
        }))?;
        self.submit_record_at(reference, pin)
    }

    pub(crate) fn submit_record_at(
        &self,
        reference: ValueRecordReference,
        pin: IndexReadPin,
    ) -> Result<PendingRecord, LocalDbError> {
        self.ensure_healthy()?;
        let ready = self.cache.get_scan(&reference);
        let read = if ready.is_none() {
            let node = *self
                .partition_indexes
                .get(&reference.batch_address.partition)
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
            Some(self.reads.submit_until(
                node,
                std::slice::from_ref(&reference),
                0,
                false,
                ReadContext {
                    deadline: Instant::now() + self.config.timeout,
                    pin: Some(pin),
                    trace: None,
                },
            )?)
        } else {
            None
        };
        self.ensure_healthy()?;
        Ok(PendingRecord {
            owner: self.reads.state.clone(),
            reference,
            ready,
            read,
        })
    }

    /// # Errors
    /// Rejects foreign handles, failed reads, or records inconsistent with the index.
    pub fn finish_record(
        &self,
        completed: CompletedRecord,
        ks: KeySpace,
        reduced_key: &[u8],
    ) -> Result<Mutation, LocalDbError> {
        if !Arc::ptr_eq(&self.reads.state, &completed.owner) {
            return Err(LocalDbError::new("record belongs to another authority"));
        }
        self.value_read_ns
            .fetch_add(completed.wait_ns, Ordering::Relaxed);
        self.ensure_healthy()?;
        let mutation = completed.result.inspect_err(|_| self.poison())?;
        if matches!(mutation, Mutation::Delete { .. })
            || mutation.keyspace() != ks
            || completed.reference.descriptor.keyspace != ks.0
            || self.shape.ks(ks).reduce_key(mutation.key()).as_ref() != reduced_key
        {
            self.poison();
            return Err(LocalDbError::new(
                "scan member does not match its live index reference",
            ));
        }
        self.ensure_healthy()?;
        Ok(mutation)
    }
}
