//! Local index root adoption and whole-unreferenced-file reclamation.
use super::super::storage::{encode_metadata, install_metadata, read_metadata};
use super::super::types::RecordKind;
use super::*;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Read;

pub(super) const ROOT: &str = "local-index-root-v2";
#[derive(Serialize, Deserialize)]
pub(super) struct RootPrefix {
    pub epoch: u64,
    pub partition: u32,
    pub count: usize,
    pub digest: [u8; 32],
}
#[derive(Serialize, Deserialize)]
pub(super) struct LocalRoot {
    format: u32,
    image: String,
    bytes: u64,
    digest: [u8; 32],
    pub latest: Option<MutationVersion>,
    pub prefixes: Vec<RootPrefix>,
}

impl LocalRoot {
    pub(super) fn load(
        path: &Path,
        shape: &KeyShape,
        config: &LocalDatabaseConfig,
    ) -> Result<Option<(Self, NativeIndex)>, LocalDbError> {
        if !path
            .join(ROOT)
            .try_exists()
            .map_err(LocalDbError::component)?
        {
            if path
                .join("local-index-root-v1")
                .try_exists()
                .map_err(LocalDbError::component)?
            {
                return Err(LocalDbError::new("obsolete local recovery root format"));
            }
            return Ok(None);
        }
        let root: Self = read_metadata(&path.join(ROOT)).map_err(LocalDbError::component)?;
        if root
            .prefixes
            .windows(2)
            .any(|pair| (pair[0].epoch, pair[0].partition) >= (pair[1].epoch, pair[1].partition))
        {
            return Err(LocalDbError::new(
                "unordered local storage recovery prefixes",
            ));
        }
        if root.format != 2 || Path::new(&root.image).components().count() != 1 {
            return Err(LocalDbError::new("invalid local recovery root"));
        }
        let mut file = File::open(path.join(&root.image)).map_err(LocalDbError::component)?;
        if file.metadata().map_err(LocalDbError::component)?.len() != root.bytes {
            return Err(LocalDbError::new(
                "local index image length differs from root",
            ));
        }
        let mut hash = blake2b_simd::Params::new().hash_length(32).to_state();
        let mut buffer = [0; 65536];
        loop {
            let count = file.read(&mut buffer).map_err(LocalDbError::component)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        let hash = hash.finalize();
        let mut digest = [0; 32];
        digest.copy_from_slice(hash.as_bytes());
        if digest != root.digest {
            return Err(LocalDbError::new("local index image differs from root"));
        }
        let index = NativeIndex::open_snapshot(
            &path.join(&root.image),
            shape,
            config.storage.database,
            config.max_index_bytes,
        )
        .map_err(LocalDbError::component)?;
        if index.latest_cut()
            != root
                .latest
                .map_or(IndexReadCut::BeforeAll, IndexReadCut::At)
        {
            return Err(LocalDbError::new("local index cut differs from root"));
        }
        Ok(Some((root, index)))
    }
}

impl LocalDatabase {
    pub(super) fn persist_index_root(&mut self, floor: IndexReadCut) -> Result<(), LocalDbError> {
        self.healthy = false;
        self.snapshot_generation = self
            .snapshot_generation
            .checked_add(1)
            .ok_or_else(|| LocalDbError::new("snapshot generation exhausted"))?;
        let name = format!(
            "index-{}-{}.snapshot",
            self.inventory.active_inventory().epoch,
            self.snapshot_generation
        );
        let image = self.path.join(&name);
        let cut = self
            .latest
            .map_or(IndexReadCut::BeforeAll, IndexReadCut::At);
        let captured = self
            .index
            .prepare_compacted_image(cut, floor)
            .map_err(LocalDbError::component)?;
        let persisted = captured.persist(&image).map_err(LocalDbError::component)?;
        let mut prefixes = Vec::with_capacity(self.logs.len());
        for log in self.logs.values() {
            let count = log.receipt_count();
            prefixes.push(RootPrefix {
                epoch: log.config().inventory.epoch,
                partition: log.config().partition,
                count,
                digest: log
                    .checkpoint_prefix(count)
                    .map_err(LocalDbError::component)?,
            });
        }
        prefixes.sort_by_key(|prefix| (prefix.epoch, prefix.partition));
        let root = LocalRoot {
            format: 2,
            image: name.clone(),
            bytes: persisted.metadata.bytes,
            digest: persisted.metadata.digest,
            latest: self.latest,
            prefixes,
        };
        let bytes = encode_metadata(&root).map_err(LocalDbError::component)?;
        install_metadata(&self.path, ROOT, &bytes).map_err(LocalDbError::component)?;
        // The durable root is installed before either history unmerge or unlink.
        self.index.install_image(persisted);
        self.retire_unreferenced_files()?;
        let reachable = self.index.reachable_files();
        for entry in fs::read_dir(&self.path).map_err(LocalDbError::component)? {
            let entry = entry.map_err(LocalDbError::component)?;
            let filename = entry.file_name();
            let Some(filename) = filename.to_str() else {
                continue;
            };
            if filename.starts_with("index-")
                && filename.contains(".snapshot")
                && filename != name
                && !reachable.iter().any(|kept| kept == filename)
            {
                fs::remove_file(entry.path()).map_err(LocalDbError::component)?;
            }
        }
        File::open(&self.path)
            .and_then(|file| file.sync_all())
            .map_err(LocalDbError::component)?;
        self.healthy = true;
        Ok(())
    }

    fn retire_unreferenced_files(&self) -> Result<(), LocalDbError> {
        let active_epoch = self.inventory.active_inventory().epoch;
        for log in self
            .logs
            .values()
            .filter(|log| log.config().inventory.epoch < active_epoch)
        {
            let mut after = None;
            loop {
                let page = log
                    .wal_file_page(after, 256)
                    .map_err(LocalDbError::component)?;
                if page.is_empty() {
                    break;
                }
                let mut dead = Vec::with_capacity(page.len());
                for &offset in &page {
                    let mut live = false;
                    for ks in self.inventory.shape().iter_ks() {
                        let mut history = self
                            .index
                            .relocation_history(ks.id(), IndexReadCut::Latest)
                            .map_err(LocalDbError::component)?;
                        while let Some((_, reference)) = history.next(&self.index) {
                            if reference.descriptor.kind == RecordKind::Put
                                && reference.batch_version.epoch == log.config().inventory.epoch
                                && reference.batch_address.partition == log.config().partition
                                && reference.batch_address.offset / log.config().wal_file_bytes
                                    * log.config().wal_file_bytes
                                    == offset
                            {
                                live = true;
                                break;
                            }
                        }
                        if live {
                            break;
                        }
                    }
                    if !live {
                        dead.push(offset);
                    }
                }
                log.retire_files(log.receipt_count(), &dead)
                    .map_err(LocalDbError::component)?;
                after = page.last().copied();
            }
        }
        Ok(())
    }
}
