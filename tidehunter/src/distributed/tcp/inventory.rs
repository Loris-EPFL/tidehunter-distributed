//! One node's durable epoch fence. Mirrors LocalInventory's preparing-before-
//! files and active-after-registration ordering, with one physical partition.
//! This is a static RF1 deployment: local filesystem locks exclude node restarts,
//! while the service rejects stale epochs. It does not elect a coordinator.

use super::*;
use crate::distributed::storage::{PartitionLog, encode_metadata, install_metadata, read_metadata};
use crate::lock::DbLock;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

const MANIFEST: &str = "tcp-node-inventory-v1";
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
enum EpochState {
    Preparing,
    Active,
    Sealed,
    Abandoned,
}
#[derive(Clone, Copy, Serialize, Deserialize)]
struct Epoch {
    epoch: u64,
    state: EpochState,
}
#[derive(Serialize, Deserialize)]
struct Manifest {
    format: u32,
    partition: PartitionConfig,
    shape_yaml: String,
    max_epochs: usize,
    epochs: Vec<Epoch>,
}

pub struct NodeInventory {
    pub(super) config: NodeConfig,
    pub(super) shape: KeyShape,
    manifest: Manifest,
    logs: BTreeMap<u64, Arc<PartitionLog>>,
    pub(super) healthy: bool,
    pub(super) terminal: Arc<AtomicBool>,
    pub(super) readers: Arc<RwLock<ReaderAuthority>>,
    metrics: Arc<Metrics>,
    // Partition handles each retain this root lock as well.
    lock: Arc<DbLock>,
}

#[derive(Default)]
pub(super) struct ReaderAuthority {
    generation: u64,
    active_epoch: Option<u64>,
    logs: BTreeMap<u64, Arc<PartitionLog>>,
    database: [u8; 16],
    partition: u32,
}

pub(super) struct ReaderSnapshot {
    pub(super) generation: u64,
    pub(super) members: Vec<(Arc<PartitionLog>, ValueRecordReference)>,
}

impl ReaderAuthority {
    pub(super) fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.active_epoch = None;
    }

    fn publish(&mut self, epoch: u64, logs: &BTreeMap<u64, Arc<PartitionLog>>) {
        self.generation = self.generation.wrapping_add(1);
        self.logs.clone_from(logs);
        self.active_epoch = Some(epoch);
    }
    pub(super) fn snapshot(
        &self,
        epoch: u64,
        references: &[ValueRecordReference],
        terminal: &AtomicBool,
    ) -> Result<ReaderSnapshot, TcpError> {
        if terminal.load(Ordering::Acquire) {
            return Err(TcpError::new("node requires restart and sealed recovery"));
        }
        if self.active_epoch != Some(epoch) {
            return Err(TcpError::new("stale or sealed request authority"));
        }
        let mut members = Vec::with_capacity(references.len());
        for reference in references {
            if reference.batch_id.database != self.database
                || reference.batch_address.partition != self.partition
                || reference.batch_address.segment_generation != reference.batch_version.epoch
            {
                return Err(TcpError::new(
                    "value reference belongs to another node/domain",
                ));
            }
            let log = self
                .logs
                .get(&reference.batch_version.epoch)
                .cloned()
                .ok_or_else(|| TcpError::new("value reference names absent epoch"))?;
            members.push((log, *reference));
        }
        Ok(ReaderSnapshot {
            generation: self.generation,
            members,
        })
    }
    pub(super) fn snapshot_valid(
        &self,
        epoch: u64,
        generation: u64,
        terminal: &AtomicBool,
    ) -> bool {
        !terminal.load(Ordering::Acquire)
            && self.generation == generation
            && self.active_epoch == Some(epoch)
            && !terminal.load(Ordering::Acquire)
    }
    pub(super) fn active_valid(&self, epoch: u64, terminal: &AtomicBool) -> bool {
        !terminal.load(Ordering::Acquire) && self.active_epoch == Some(epoch)
    }
}
impl NodeInventory {
    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub(super) fn process_resources_for_benchmark(
        &self,
        epoch: u64,
    ) -> Result<crate::read_probe::ProcessResources, TcpError> {
        self.active(epoch)?;
        crate::read_probe::ProcessResources::process().map_err(TcpError::component)
    }

    #[cfg(all(target_os = "linux", feature = "test-utils"))]
    pub(super) fn read_evidence_for_benchmark(
        &self,
        epoch: u64,
        evict: bool,
    ) -> Result<crate::read_probe::ReadEvidence, TcpError> {
        self.active(epoch)?;
        let mut evidence =
            crate::read_probe::ReadEvidence::process().map_err(TcpError::component)?;
        for log in self.logs.values() {
            let current = log
                .read_evidence_for_benchmark(evict)
                .map_err(TcpError::component)?;
            evidence.file_bytes += current.file_bytes;
            evidence.resident_bytes += current.resident_bytes;
        }
        Ok(evidence)
    }
    pub fn open(mut config: NodeConfig, metrics: Arc<Metrics>) -> Result<Self, TcpError> {
        config.resident_reads.validate()?;
        config.partition.validate().map_err(TcpError::component)?;
        if config.partition.inventory.epoch != 1
            || config.partition.inventory.segment_generation != 1
            || !(1..=1024).contains(&config.max_epochs)
        {
            return Err(TcpError::new("invalid node epoch/configuration template"));
        }
        let shape = KeyShape::from_yaml(&config.shape_yaml).map_err(TcpError::component)?;
        config.shape_yaml = shape.to_yaml().map_err(TcpError::component)?;
        match fs::create_dir(&config.root) {
            Ok(()) => {
                File::open(
                    config
                        .root
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(Path::new(".")),
                )?
                .sync_all()?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        directory(&config.root)?;
        config.root = config.root.canonicalize()?;
        let lock = Arc::new(DbLock::acquire(&config.root, Duration::ZERO)?);
        let manifest_path = config.root.join(MANIFEST);
        let mut manifest = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) if metadata.file_type().is_file() => {
                read_metadata::<Manifest>(&manifest_path).map_err(TcpError::component)?
            }
            Ok(_) => return Err(TcpError::new("node manifest is not a regular file")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                for entry in fs::read_dir(&config.root)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file()
                        || (entry.file_name() != "LOCK"
                            && entry.file_name() != format!(".{MANIFEST}.pending").as_str())
                    {
                        return Err(TcpError::new("missing node inventory beside existing data"));
                    }
                }
                let manifest = Manifest {
                    format: 1,
                    partition: config.partition.clone(),
                    shape_yaml: config.shape_yaml.clone(),
                    max_epochs: config.max_epochs,
                    epochs: Vec::new(),
                };
                persist(&config.root, &manifest)?;
                manifest
            }
            Err(error) => return Err(error.into()),
        };
        if manifest.format != 1
            || manifest.partition != config.partition
            || manifest.shape_yaml != config.shape_yaml
            || manifest.max_epochs != config.max_epochs
            || manifest.epochs.len() > config.max_epochs
            || manifest.epochs.iter().any(|e| e.epoch == 0)
            || manifest.epochs.windows(2).any(|w| w[0].epoch >= w[1].epoch)
            || manifest.epochs.iter().enumerate().any(|(index, e)| {
                matches!(e.state, EpochState::Active | EpochState::Preparing)
                    && index + 1 != manifest.epochs.len()
            })
        {
            return Err(TcpError::new("node inventory/configuration differs"));
        }
        for entry in fs::read_dir(&config.root)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(name) = name.to_str().filter(|name| name.starts_with("epoch_")) {
                let matching = manifest
                    .epochs
                    .iter()
                    .any(|e| name == format!("epoch_{:016x}", e.epoch));
                if !matching || !entry.file_type()?.is_dir() {
                    return Err(TcpError::new("unregistered node epoch directory"));
                }
            }
        }
        let mut logs = BTreeMap::new();
        for epoch in &mut manifest.epochs {
            match epoch.state {
                EpochState::Preparing => epoch.state = EpochState::Abandoned,
                EpochState::Active | EpochState::Sealed => {
                    let path = epoch_path(&config.root, epoch.epoch);
                    directory(&path)?;
                    let expected = partition_at(&config.partition, epoch.epoch);
                    let log = PartitionLog::recover_sealed(&path, &expected, metrics.clone())
                        .map_err(TcpError::component)?;
                    if log.key_shape().to_yaml().map_err(TcpError::component)? != config.shape_yaml
                    {
                        return Err(TcpError::new("node stream schema differs"));
                    }
                    logs.insert(
                        epoch.epoch,
                        Arc::new(log.retain_local_authority(lock.clone())),
                    );
                    epoch.state = EpochState::Sealed;
                }
                EpochState::Abandoned => {}
            }
        }
        // Restart always seals the old authority, even if it previously shut
        // down cleanly. It never resumes an old logical counter/writer epoch.
        persist(&config.root, &manifest)?;
        let readers = Arc::new(RwLock::new(ReaderAuthority {
            database: config.partition.inventory.database,
            partition: config.partition.partition,
            ..ReaderAuthority::default()
        }));
        Ok(Self {
            config,
            shape,
            manifest,
            logs,
            healthy: true,
            terminal: Arc::new(AtomicBool::new(false)),
            readers,
            metrics,
            lock,
        })
    }
    fn ensure_healthy(&self) -> Result<(), TcpError> {
        if self.healthy && !self.terminal.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(TcpError::new(
                "node requires restart and sealed recovery; outcome may be unknown",
            ))
        }
    }
    pub fn status(&self) -> Result<NodeStatus, TcpError> {
        self.ensure_healthy()?;
        Ok(NodeStatus {
            partition: self.config.partition.clone(),
            shape_yaml: self.config.shape_yaml.clone(),
            max_epochs: self.config.max_epochs,
            epochs: self
                .manifest
                .epochs
                .iter()
                .map(|e| NodeEpoch {
                    epoch: e.epoch,
                    sealed: e.state != EpochState::Active,
                })
                .collect(),
            active_epoch: self
                .manifest
                .epochs
                .last()
                .filter(|e| e.state == EpochState::Active)
                .map(|e| e.epoch),
            max_frame_bytes: HARD_FRAME_LIMIT,
            max_reads: 256,
            max_read_bytes: self.config.read_max_inflight_bytes,
            recovery_page_batches: 1024,
            polling: PollingPolicy::Adaptive,
        })
    }
    pub fn fence_to(&mut self, epoch: u64) -> Result<NodeStatus, TcpError> {
        self.ensure_healthy()?;
        if self
            .manifest
            .epochs
            .last()
            .is_some_and(|e| e.epoch == epoch && e.state == EpochState::Active)
        {
            return self.status();
        }
        if epoch == 0
            || self
                .manifest
                .epochs
                .last()
                .is_some_and(|e| epoch <= e.epoch)
        {
            return Err(TcpError::new(
                "epoch already fenced/sealed; cannot reactivate",
            ));
        }
        if self.manifest.epochs.len() == self.config.max_epochs {
            return Err(TcpError::new("retained node epoch capacity reached"));
        }
        if let Some(old) = self
            .manifest
            .epochs
            .last()
            .filter(|e| e.state == EpochState::Active)
            .copied()
        {
            self.seal(old.epoch)?;
        }
        self.healthy = false;
        self.readers.write().invalidate();
        self.manifest.epochs.push(Epoch {
            epoch,
            state: EpochState::Preparing,
        });
        persist(&self.config.root, &self.manifest)?;
        let log = PartitionLog::create(
            &epoch_path(&self.config.root, epoch),
            partition_at(&self.config.partition, epoch),
            &self.shape,
            self.metrics.clone(),
        )
        .map_err(TcpError::component)?;
        let log = Arc::new(log.retain_local_authority(self.lock.clone()));
        self.manifest.epochs.last_mut().unwrap().state = EpochState::Active;
        persist(&self.config.root, &self.manifest)?;
        self.logs.insert(epoch, log);
        self.readers.write().publish(epoch, &self.logs);
        self.healthy = true;
        self.status()
    }
    pub(super) fn active(&self, epoch: u64) -> Result<Arc<PartitionLog>, TcpError> {
        self.ensure_healthy()?;
        if !self
            .manifest
            .epochs
            .last()
            .is_some_and(|e| e.epoch == epoch && e.state == EpochState::Active)
        {
            return Err(TcpError::new("stale or sealed request authority"));
        }
        self.logs
            .get(&epoch)
            .cloned()
            .ok_or_else(|| TcpError::new("active stream missing"))
    }
    pub fn seal(&mut self, epoch: u64) -> Result<(), TcpError> {
        self.ensure_healthy()?;
        if self.manifest.epochs.iter().any(|e| {
            e.epoch == epoch && matches!(e.state, EpochState::Sealed | EpochState::Abandoned)
        }) {
            return Ok(());
        }
        let log = self.active(epoch)?;
        self.healthy = false;
        self.readers.write().invalidate();
        log.seal_epoch(RequestAuthority { epoch })
            .map_err(TcpError::component)?;
        self.manifest.epochs.last_mut().unwrap().state = EpochState::Sealed;
        persist(&self.config.root, &self.manifest)?;
        self.healthy = true;
        Ok(())
    }
    fn retirement_log(
        &self,
        authority_epoch: u64,
        epoch: u64,
    ) -> Result<Arc<PartitionLog>, TcpError> {
        self.active(authority_epoch)?;
        if epoch >= authority_epoch
            || !self
                .manifest
                .epochs
                .iter()
                .any(|entry| entry.epoch == epoch && entry.state == EpochState::Sealed)
        {
            return Err(TcpError::new("retirement requires an older sealed epoch"));
        }
        self.logs
            .get(&epoch)
            .cloned()
            .ok_or_else(|| TcpError::new("retirement stream absent"))
    }

    pub(super) fn retirement_files(
        &self,
        authority_epoch: u64,
        epoch: u64,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<u64>, TcpError> {
        if !(1..=256).contains(&limit) {
            return Err(TcpError::new("retirement page exceeds bound"));
        }
        self.retirement_log(authority_epoch, epoch)?
            .wal_file_page(after, limit)
            .map_err(TcpError::component)
    }

    pub(super) fn retire_files(
        &self,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
        files: &[u64],
    ) -> Result<(), TcpError> {
        if files.len() > 256 {
            return Err(TcpError::new("retirement file count exceeds bound"));
        }
        self.retirement_log(authority_epoch, epoch)?
            .retire_files(prefix, files)
            .map_err(TcpError::component)
    }

    pub(super) fn checkpoint_proof(
        &self,
        authority_epoch: u64,
        epoch: u64,
        prefix: usize,
    ) -> Result<checkpoint::CheckpointProof, TcpError> {
        self.active(authority_epoch)?;
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|entry| entry.epoch == epoch)
            .ok_or_else(|| TcpError::new("checkpoint epoch absent"))?;
        let log = match record.state {
            EpochState::Active | EpochState::Sealed => Some(
                self.logs
                    .get(&epoch)
                    .ok_or_else(|| TcpError::new("checkpoint stream absent"))?,
            ),
            EpochState::Abandoned => None,
            EpochState::Preparing => return Err(TcpError::new("checkpoint epoch incomplete")),
        };
        let retained = log.map_or(0, |log| log.receipt_count());
        if prefix > retained {
            return Err(TcpError::new("checkpoint prefix beyond retained history"));
        }
        if prefix != 0 {
            let log = log.ok_or_else(|| TcpError::new("checkpoint stream absent"))?;
            let last = log
                .receipt_page(prefix - 1, 1)
                .map_err(TcpError::component)?
                .into_iter()
                .next()
                .ok_or_else(|| TcpError::new("checkpoint receipt absent"))?;
            // Capturing a receipt is not durability evidence. Persist precisely
            // the captured prefix's last complete group before issuing proof.
            log.persist_through(last).map_err(TcpError::component)?;
        }
        let database = self.config.partition.inventory.database;
        let partition = self.config.partition.partition;
        Ok(checkpoint::CheckpointProof {
            database,
            partition,
            epoch,
            prefix,
            retained,
            digest: match log {
                Some(log) => log.checkpoint_prefix(prefix).map_err(TcpError::component)?,
                None => checkpoint::prefix_digest(database, partition, epoch, std::iter::empty()),
            },
        })
    }

    pub(super) fn recovery_page(
        &self,
        epoch: u64,
        cursor: usize,
        limit: usize,
        limits: TcpLimits,
    ) -> Result<Response, TcpError> {
        self.ensure_healthy()?;
        if limit == 0 || limit > limits.recovery_page_batches {
            return Err(TcpError::new("recovery page count exceeds bound"));
        }
        let record = self
            .manifest
            .epochs
            .iter()
            .find(|e| e.epoch == epoch)
            .ok_or_else(|| TcpError::new("recovery epoch absent"))?;
        if record.state == EpochState::Abandoned {
            return if cursor == 0 {
                Ok(Response::Recovery {
                    batches: Vec::new(),
                    next: None,
                })
            } else {
                Err(TcpError::new("invalid abandoned epoch cursor"))
            };
        }
        if record.state != EpochState::Sealed {
            return Err(TcpError::new("recovery requires sealed immutable epoch"));
        }
        let log = &self.logs[&epoch];
        let count = log.receipt_count();
        if cursor > count {
            return Err(TcpError::new("recovery cursor beyond retained history"));
        }
        let mut size = 32usize;
        let mut batches = Vec::new();
        // The wire limit is an upper bound. A bounded storage receipt page can
        // return fewer entries; `next` continues at the exact physical offset.
        for receipt in log
            .receipt_page(cursor, limit.min(256))
            .map_err(TcpError::component)?
        {
            let next = size
                .checked_add(receipt.address().len as usize + 256)
                .ok_or_else(|| TcpError::new("recovery response overflow"))?;
            if next > limits.max_frame_bytes {
                if batches.is_empty() {
                    return Err(TcpError::new("one recovery batch exceeds transport frame"));
                }
                break;
            }
            let batch = log.read_batch(receipt).map_err(TcpError::component)?;
            let durable = log.persist_through(receipt).map_err(TcpError::component)?;
            batches.push((
                batch.encoded().clone().into(),
                Receipt::from_durable(durable),
            ));
            size = next;
        }
        let end = cursor + batches.len();
        let next = (end < count).then_some(end);
        Ok(Response::Recovery { batches, next })
    }
    pub(super) fn stats(&self) -> Result<PartitionStats, TcpError> {
        self.ensure_healthy()?;
        Ok(self
            .manifest
            .epochs
            .last()
            .and_then(|e| self.logs.get(&e.epoch))
            .map_or_else(PartitionStats::default, |log| log.stats()))
    }
}
fn partition_at(template: &PartitionConfig, epoch: u64) -> PartitionConfig {
    let mut config = template.clone();
    config.inventory.epoch = epoch;
    config.inventory.segment_generation = epoch;
    config
}
fn epoch_path(root: &Path, epoch: u64) -> PathBuf {
    root.join(format!("epoch_{epoch:016x}"))
}
fn directory(path: &Path) -> Result<(), TcpError> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(TcpError::new("node inventory path is not a directory"));
    }
    Ok(())
}
fn persist(root: &Path, manifest: &Manifest) -> Result<(), TcpError> {
    install_metadata(
        root,
        MANIFEST,
        &encode_metadata(manifest).map_err(TcpError::component)?,
    )
    .map_err(TcpError::component)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::tcp::tests::{config, metrics};

    #[test]
    fn interrupted_preparing_epoch_is_retained_and_never_activated() {
        let temporary = tempdir::TempDir::new("node-preparing-fence").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        inventory.seal(1).unwrap();
        inventory.manifest.epochs.push(Epoch {
            epoch: 2,
            state: EpochState::Preparing,
        });
        persist(&config.root, &inventory.manifest).unwrap();
        // Simulate interrupted creation after the durable reservation, before
        // any registered stream or active reply could become eligible.
        fs::create_dir(epoch_path(&config.root, 2)).unwrap();
        drop(inventory);
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        assert_eq!(
            inventory.status().unwrap().epochs,
            vec![
                NodeEpoch {
                    epoch: 1,
                    sealed: true
                },
                NodeEpoch {
                    epoch: 2,
                    sealed: true
                }
            ]
        );
        assert!(inventory.fence_to(2).is_err());
        assert!(
            matches!(inventory.recovery_page(2,0,32,TcpLimits::default()).unwrap(),Response::Recovery{batches,next:None} if batches.is_empty())
        );
        inventory.fence_to(3).unwrap();
        assert!(epoch_path(&config.root, 2).is_dir());
    }

    #[test]
    fn surviving_log_retains_node_root_lock() {
        let temporary = tempdir::TempDir::new("node-root-lock").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        let log = inventory.active(1).unwrap();
        drop(inventory);
        assert!(NodeInventory::open(config.clone(), metrics()).is_err());
        drop(log);
        let inventory = NodeInventory::open(config, metrics()).unwrap();
        assert_eq!(inventory.status().unwrap().active_epoch, None);
    }

    #[test]
    fn checkpoint_proof_binds_exact_retained_prefix_and_fenced_authority() {
        let temporary = tempdir::TempDir::new("node-checkpoint-proof").unwrap();
        let config = config(&temporary.path().join("node"));
        let mut inventory = NodeInventory::open(config.clone(), metrics()).unwrap();
        inventory.fence_to(1).unwrap();
        let mut receipts = Vec::new();
        for sequence in 0..2 {
            let id = (sequence * 1024..)
                .map(|n: u64| {
                    let mut operation = [0; 16];
                    operation[..8].copy_from_slice(&n.to_be_bytes());
                    BatchId {
                        database: config.partition.inventory.database,
                        operation,
                    }
                })
                .find(|id| {
                    config.partition.inventory.partition_for(*id).unwrap()
                        == config.partition.partition
                })
                .unwrap();
            let batch = CompleteBatch::new(
                &inventory.shape,
                id,
                BatchVersion { epoch: 1, sequence },
                vec![Mutation::Put {
                    keyspace: crate::key_shape::KeySpace(0),
                    key: b"key1"[..].into(),
                    value: vec![sequence as u8; 32].into(),
                }],
                config.partition.batch_limits,
            )
            .unwrap();
            let receipt = inventory
                .active(1)
                .unwrap()
                .append_complete_batch(RequestAuthority { epoch: 1 }, &batch)
                .unwrap();
            receipts.push(checkpoint::CheckpointReceipt::from(receipt));
            // Each singleton append is a distinct group. Make room for the
            // second append, then let checkpoint_proof persist that final group.
            if sequence == 0 {
                inventory
                    .active(1)
                    .unwrap()
                    .persist_through(receipt)
                    .unwrap();
            }
        }
        let prefix = inventory.checkpoint_proof(1, 1, 1).unwrap();
        assert_eq!(prefix.retained, 2);
        assert_eq!(
            prefix.digest,
            checkpoint::prefix_digest(
                prefix.database,
                prefix.partition,
                1,
                receipts[..1].iter().copied()
            )
        );
        assert_ne!(
            prefix.digest,
            inventory.checkpoint_proof(1, 1, 2).unwrap().digest
        );
        assert!(inventory.checkpoint_proof(1, 1, 3).is_err());
        assert!(inventory.checkpoint_proof(1, 99, 0).is_err());
        inventory.fence_to(2).unwrap();
        assert!(inventory.checkpoint_proof(1, 1, 1).is_err());
        assert_eq!(inventory.checkpoint_proof(2, 1, 1).unwrap(), prefix);
        drop(inventory);
        let mut inventory = NodeInventory::open(config, metrics()).unwrap();
        inventory.fence_to(3).unwrap();
        assert_eq!(inventory.checkpoint_proof(3, 1, 1).unwrap(), prefix);
    }
}
