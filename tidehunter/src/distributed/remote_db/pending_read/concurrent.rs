//! Read delegates retain exact immutable references, never a mutable writer
//! borrow. As in `TiKV`'s `ReadDelegate`, every completed/cache-only result checks
//! authority validity; an in-flight write is not an invalidation event.
#[allow(clippy::wildcard_imports)]
// Private read delegate shares its parent's bounded transport types.
use super::*;

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
    }
    pub fn read_profile_monitor(&self) -> ReadProfileMonitor {
        ReadProfileMonitor {
            state: self.reads.state.clone(),
            fabric: self.config.tls.transport != super::super::super::tcp::Transport::Tcp,
        }
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
        let (reference, pin) = match self.resolve_concurrent_get(ks, key, IndexReadCut::Latest)? {
            GetResolution::Ready(value) => return Ok(value),
            GetResolution::Miss { reference, pin } => (reference, pin),
        };
        let deadline = Instant::now() + self.config.timeout;
        loop {
            let generation = self.read_admission_generation();
            match self.prepare_concurrent_miss(ks, key, reference, true, pin.clone()) {
                Ok(pending) => return self.finish_ready_get(pending.wait()),
                Err(error) if RemoteDatabase::read_backpressured(&error) => {
                    self.wait_read_admission(generation, deadline)?;
                    if let Some(value) = self.cache.lock().get_point(&reference, ks, key) {
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
    pub(crate) fn get_many_cut(
        &self,
        ks: KeySpace,
        keys: &[Vec<u8>],
        cut: IndexReadCut,
    ) -> Result<Vec<Result<Option<Bytes>, LocalDbError>>, LocalDbError> {
        self.ensure_healthy()?;
        if keys.len() > 256 {
            return Err(LocalDbError::new("read batch exceeds 256 keys"));
        }
        let started = Instant::now();
        let mut values = vec![None; keys.len()];
        let mut missing = Vec::new();
        let mut groups = vec![Vec::new(); self.partition_indexes.len()];
        let (references, pin) = {
            let index = self.index.read();
            index.validate_cut(cut).map_err(LocalDbError::component)?;
            let references = keys
                .iter()
                .map(|key| index.get(ks, key, cut).map_err(LocalDbError::component))
                .collect::<Vec<_>>();
            for (slot, reference) in references.iter().enumerate() {
                let Ok(Some(reference)) = reference else {
                    continue;
                };
                if let Some(value) = self.cache.lock().get(reference) {
                    values[slot] = Some(value);
                } else {
                    let node = *self
                        .partition_indexes
                        .get(&reference.batch_address.partition)
                        .ok_or_else(|| {
                            LocalDbError::new("reference names unregistered partition")
                        })?;
                    groups[node].push((slot, *reference));
                    missing.push(slot);
                }
            }
            let pin = if missing.is_empty() {
                None
            } else {
                Some(index.pin_cut(cut).map_err(LocalDbError::component)?)
            };
            (references, pin)
        };
        self.read_resolve_ns
            .fetch_add(elapsed(started), Ordering::Relaxed);
        let mut batch = ReadBatch::new(&self.reads, values, groups)?;
        batch.pin = pin;
        while !batch.complete() {
            self.ensure_healthy()?;
            let generation = self.read_admission_generation();
            batch.dispatch(&self.reads)?;
            if batch.reads.is_empty() {
                self.wait_read_admission(generation, batch.deadline)?;
            } else {
                let started = Instant::now();
                let result = batch.finish_one();
                self.value_read_ns
                    .fetch_add(elapsed(started), Ordering::Relaxed);
                result.inspect_err(|_| self.poison())?;
            }
        }
        self.ensure_healthy()?;
        for slot in missing {
            let Ok(Some(reference)) = &references[slot] else {
                unreachable!("only live references were submitted");
            };
            let mutation = batch.values[slot]
                .as_ref()
                .expect("every admitted member completed");
            let prepared = ReadCache::prepare_verified(
                &self.cache,
                self.config.read_cache_bytes,
                *reference,
                mutation,
            );
            self.cache.lock().insert_prepared(prepared);
        }
        let result = references
            .into_iter()
            .zip(batch.values)
            .zip(keys)
            .map(|((reference, mutation), key)| match reference {
                Err(error) => Err(error),
                Ok(None) => Ok(None),
                Ok(Some(_)) => RemoteDatabase::point_value(
                    ks,
                    key,
                    mutation.expect("every live reference was resolved"),
                )
                .inspect_err(|_| self.poison()),
            })
            .collect();
        self.ensure_healthy()?;
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
            .read()
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
    ) -> Result<GetResolution, LocalDbError> {
        self.ensure_healthy()?;
        let started = Instant::now();
        let resolution = {
            // A cache-only result already owns its Bytes before releasing the
            // index guard. Only asynchronous/storage work needs an allocated pin.
            let index = self.index.read();
            match index.get(ks, key, cut).map_err(LocalDbError::component)? {
                None => GetResolution::Ready(None),
                Some(reference) => match self.cache.lock().get_point(&reference, ks, key) {
                    Some(value) => GetResolution::Ready(Some(value)),
                    None => GetResolution::Miss {
                        reference,
                        pin: index.pin_cut(cut).map_err(LocalDbError::component)?,
                    },
                },
            }
        };
        self.read_resolve_ns
            .fetch_add(elapsed(started), Ordering::Relaxed);
        self.ensure_healthy()?;
        Ok(resolution)
    }

    fn prepare_concurrent_get(
        &self,
        ks: KeySpace,
        key: &[u8],
        cut: IndexReadCut,
        inline: bool,
    ) -> Result<PendingGet, LocalDbError> {
        match self.resolve_concurrent_get(ks, key, cut)? {
            GetResolution::Ready(value) => Ok(PendingGet {
                owner: self.reads.state.clone(),
                state: GetState::Ready(value),
            }),
            GetResolution::Miss { reference, pin } => {
                self.prepare_concurrent_miss(ks, key, reference, inline, pin)
            }
        }
    }

    fn prepare_concurrent_miss(
        &self,
        ks: KeySpace,
        key: &[u8],
        reference: ValueRecordReference,
        inline: bool,
        pin: IndexReadPin,
    ) -> Result<PendingGet, LocalDbError> {
        self.ensure_healthy()?;
        let node = *self
            .partition_indexes
            .get(&reference.batch_address.partition)
            .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
        let started = Instant::now();
        let immediate = if inline {
            self.reads.read_inline(
                node,
                self.epoch,
                &reference,
                key.len(),
                None,
                Some(pin.clone()),
            )?
        } else {
            None
        };
        let state = if let Some(values) = immediate {
            self.value_read_ns
                .fetch_add(elapsed(started), Ordering::Relaxed);
            GetState::Ready(self.complete_concurrent_get(reference, ks, key, values)?)
        } else {
            let read = self
                .reads
                .submit(node, vec![reference], key.len(), false, Some(pin))?;
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
        let prepared = ReadCache::prepare_verified(
            &self.cache,
            self.config.read_cache_bytes,
            reference,
            &mutation,
        );
        self.cache.lock().insert_prepared(prepared);
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
        let ready = self.cache.lock().get_scan(&reference);
        let read = if ready.is_none() {
            let node = *self
                .partition_indexes
                .get(&reference.batch_address.partition)
                .ok_or_else(|| LocalDbError::new("reference names unregistered partition"))?;
            Some(self.reads.submit_until(
                node,
                vec![reference],
                0,
                false,
                ReadContext {
                    deadline: Instant::now() + self.config.timeout,
                    class: ReadClass::ScanNoFill,
                    pin: Some(pin),
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
