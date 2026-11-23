//! Recovery scan of one owner's configfs scope.
use super::*;

/// Refuse a live WAL file longer than a WAL file may be, or shorter than the
/// durable end requires, before the target open seals it. A sealed marker
/// written from such a file would launder the truncation into an identity
/// that later matches itself. The durable root is the pre-restart witness;
/// frames still missing below it are caught by the recovery scan.
pub(crate) fn verify_live_epoch_sizes(
    directory: &Path,
    file_bytes: u64,
    synced_end: Option<u64>,
) -> io::Result<()> {
    let entries =
        fs::read_dir(directory).map_err(|error| path_error("list live epoch", directory, error))?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_str().is_some_and(|name| name.starts_with("wal_")) {
            continue;
        }
        let path = entry.path();
        let index = name
            .to_str()
            .and_then(|name| name.strip_prefix("wal_"))
            .and_then(|index| u64::from_str_radix(index, 16).ok());
        let meta = fs::symlink_metadata(&path)
            .map_err(|error| path_error("inspect live WAL", &path, error))?;
        let required = index
            .and_then(|index| {
                let start = index.saturating_mul(file_bytes);
                synced_end
                    .filter(|end| *end > start)
                    .map(|end| (end - start).min(file_bytes))
            })
            .unwrap_or(0);
        if !meta.is_file() || meta.len() > file_bytes || meta.len() < required {
            return Err(invalid(
                "WAL backing file identity, size or allocation changed",
            ));
        }
    }
    Ok(())
}

impl LiveTargetManager {
    /// Refuse a live active epoch whose backing file lost its preallocated WAL
    /// size, or one whose file was replaced (device or inode changed). This
    /// runs before the WAL scan, so a truncated or replaced file names its
    /// cause instead of failing on a short read. A sealed epoch trims its
    /// files to the used length, so only its replacement identity is checked.
    /// A marker whose path is already unlinked is a retirement tombstone and
    /// is not a live file of this directory.
    pub(crate) fn verify_live_epoch(&self, directory: &Path, active: bool) -> io::Result<()> {
        let entries = fs::read_dir(directory)
            .map_err(|error| path_error("list live epoch", directory, error))?;
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            if !name.to_str().is_some_and(|name| name.starts_with("wal_")) {
                continue;
            }
            let path = entry.path();
            let meta = fs::symlink_metadata(&path)
                .map_err(|error| path_error("inspect live WAL", &path, error))?;
            let matches_marker = match self.sealed_file(&path)? {
                Some(identity) => {
                    meta.dev() == identity.device
                        && meta.ino() == identity.inode
                        && (!active || meta.len() == identity.bytes)
                }
                None => true,
            };
            if !meta.is_file() || !matches_marker {
                return Err(invalid(
                    "WAL backing file identity, size or allocation changed",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn reconcile(&mut self) -> io::Result<()> {
        let prefix = format!("{}sub-", self.scope);
        let owned_name = |name: &[u8]| name.starts_with(prefix.as_bytes());
        let subsystems = self.configfs.join("subsystems");
        let mut owned = Vec::new();
        for entry in configfs_entries(&subsystems)? {
            if owned_name(entry.file_name().as_bytes()) {
                owned.push(PathBuf::from("subsystems").join(entry.file_name()));
            }
        }
        for sub in &owned {
            let ns_root = sub.join("namespaces");
            for ns in configfs_entries(&self.full(&ns_root)?)? {
                let ns = ns_root.join(ns.file_name());
                let enable = optional_attribute(&self.full(&ns)?.join("enable"))?;
                match enable.as_deref() {
                    Some("1") => {
                        if let Some(path) =
                            optional_attribute(&self.full(&ns)?.join("device_path"))?
                            && !path.is_empty()
                            && let Ok(meta) = fs::symlink_metadata(&path)
                            && meta.is_file()
                        {
                            let identity = LiveFileIdentity {
                                path: PathBuf::from(path),
                                device: meta.dev(),
                                inode: meta.ino(),
                                bytes: meta.len(),
                                block_size: 4096,
                            };
                            self.write_sealed(&identity)?;
                        }
                        let started = Instant::now();
                        self.put(&ns.join("enable"), "0")?;
                        if !self.fixture {
                            self.stats.disable_drain_successes =
                                self.stats.disable_drain_successes.saturating_add(1);
                            self.stats.disable_drain_time_ns =
                                self.stats.disable_drain_time_ns.saturating_add(
                                    started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                                );
                        }
                    }
                    Some("0") => {}
                    Some(_) => return Err(invalid("invalid namespace enable state")),
                    None => {}
                }
            }
        }
        let (af, addr) = match self
            .config
            .address
            .parse::<IpAddr>()
            .map_err(|_| invalid("invalid address"))?
        {
            IpAddr::V4(a) => ("ipv4", a.to_string()),
            IpAddr::V6(a) => ("ipv6", a.to_string()),
        };
        let transport = match self.config.transport {
            NvmeofTransport::Tcp => "tcp",
            NvmeofTransport::Rdma => "rdma",
        };
        for (port_id, expected) in [
            (
                self.config.port_id,
                [
                    ("addr_trtype", transport),
                    ("addr_adrfam", af),
                    ("addr_traddr", addr.as_str()),
                    ("addr_trsvcid", ""),
                ],
            ),
            (
                self.config.commissioning_port_id,
                [
                    ("addr_trtype", "tcp"),
                    ("addr_adrfam", "ipv4"),
                    ("addr_traddr", "127.0.0.1"),
                    ("addr_trsvcid", ""),
                ],
            ),
        ] {
            let service = if port_id == self.config.port_id {
                self.config.service
            } else {
                self.config.commissioning_service
            };
            let service = service.to_string();
            let port = PathBuf::from("ports").join(port_id.to_string());
            if fs::symlink_metadata(self.full(&port)?).is_err() {
                continue;
            }
            for (key, value) in expected {
                let value = if key == "addr_trsvcid" {
                    service.as_str()
                } else {
                    value
                };
                // A missing attribute is an unknown object. A default value
                // is a port whose setup stopped before it published a
                // listener: no initiator can reach it, so it is ours to
                // remove.
                let actual = optional_attribute(&self.full(&port.join(key))?)?;
                match actual {
                    None => {
                        return Err(path_error(
                            "refusing to share configfs object",
                            &self.full(&port)?,
                            invalid("refusing to share configfs object"),
                        ));
                    }
                    Some(actual) if actual.is_empty() => {}
                    Some(actual) if matches!(actual.as_str(), "unspecified" | "uninitialized") => {}
                    Some(actual) if actual == value => {}
                    Some(_) => {
                        return Err(path_error(
                            "refusing to share configfs object",
                            &self.full(&port)?,
                            invalid("refusing to share configfs object"),
                        ));
                    }
                }
            }
            let links = port.join("subsystems");
            for entry in configfs_entries(&self.full(&links)?)? {
                if !owned_name(entry.file_name().as_bytes()) {
                    return Err(path_error(
                        "refusing to share configfs object",
                        &self.full(&links.join(entry.file_name()))?,
                        invalid("refusing to share configfs object"),
                    ));
                }
                self.remove_link(&links.join(entry.file_name()))?;
            }
        }
        for sub in &owned {
            for entry in configfs_entries(&self.full(&sub.join("allowed_hosts"))?)? {
                self.remove_link(&sub.join("allowed_hosts").join(entry.file_name()))?;
            }
            for entry in configfs_entries(&self.full(&sub.join("namespaces"))?)? {
                self.remove_dir(&sub.join("namespaces").join(entry.file_name()))?;
            }
            let namespaces = sub.join("namespaces");
            if fs::symlink_metadata(self.full(&namespaces)?).is_ok() {
                self.remove_dir(&namespaces)?;
            }
            if fs::symlink_metadata(self.full(sub)?).is_ok() {
                self.remove_dir(sub)?;
            }
        }
        for port_id in [self.config.port_id, self.config.commissioning_port_id] {
            let port = PathBuf::from("ports").join(port_id.to_string());
            if fs::symlink_metadata(self.full(&port)?).is_ok() {
                self.remove_dir(&port)?;
            }
        }
        for host in [self.config.host_nqn.clone(), self.commissioning_host()] {
            let host = PathBuf::from("hosts").join(host);
            if fs::symlink_metadata(self.full(&host)?).is_ok() {
                self.remove_dir(&host)?;
            }
        }
        self.sealed_directory.sync_all()?;
        self.exports.clear();
        self.pins.clear();
        Ok(())
    }
}

/// Directory entries of one configfs (or fixture) directory; an absent
/// directory has no entries.
fn configfs_entries(root: &Path) -> io::Result<Vec<fs::DirEntry>> {
    match fs::read_dir(root) {
        Ok(entries) => entries.collect(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(path_error("list configfs directory", root, error)),
    }
}
