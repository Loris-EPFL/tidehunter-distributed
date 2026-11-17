//! SPDK live namespace ownership and conservative crash journal.
use super::*;
use serde_json::{Value, json};
use std::os::unix::fs::FileTypeExt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(super) enum Phase {
    Creating,
    Active,
    Removing,
    Drained,
    Cleaning,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Daemon {
    pid: u32,
    ticks: u64,
    socket_device: u64,
    socket_inode: u64,
}

fn start_ticks(pid: u32) -> io::Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| invalid("invalid SPDK process stat"))?;
    stat[end + 1..]
        .split_whitespace()
        .nth(19)
        .and_then(|s| s.parse().ok())
        .filter(|n| *n != 0)
        .ok_or_else(|| invalid("invalid SPDK process start time"))
}

fn discover(config: &super::super::LiveSpdkConfig) -> io::Result<Daemon> {
    check_path(&config.rpc_socket, false)?;
    check_path(&config.executable, false)?;
    let socket = fs::symlink_metadata(&config.rpc_socket)?;
    if !socket.file_type().is_socket() || config.executable.canonicalize()? != config.executable {
        return Err(invalid("invalid SPDK socket or executable"));
    }
    let stream = super::super::spdk::connect_rpc(&config.rpc_socket)?;
    let peer = super::super::spdk::peer_credentials(&stream)?;
    if peer.uid != 0 || peer.pid <= 0 {
        return Err(invalid("invalid SPDK RPC peer"));
    }
    let pid = peer.pid as u32;
    let daemon = Daemon {
        pid,
        ticks: start_ticks(pid)?,
        socket_device: socket.dev(),
        socket_inode: socket.ino(),
    };
    verify_daemon(config, &daemon)?;
    Ok(daemon)
}

fn verify_daemon(config: &super::super::LiveSpdkConfig, daemon: &Daemon) -> io::Result<()> {
    check_path(&config.rpc_socket, false)?;
    check_path(&config.executable, false)?;
    let socket = fs::symlink_metadata(&config.rpc_socket)?;
    if !socket.file_type().is_socket()
        || socket.dev() != daemon.socket_device
        || socket.ino() != daemon.socket_inode
        || start_ticks(daemon.pid)? != daemon.ticks
        || fs::metadata(format!("/proc/{}", daemon.pid))?.uid() != 0
        || fs::read_link(format!("/proc/{}/exe", daemon.pid))? != config.executable
    {
        return Err(invalid("SPDK daemon identity changed"));
    }
    Ok(())
}

impl LiveTargetManager {
    fn spdk_target(
        &self,
        bdev_name: String,
        image_path: PathBuf,
    ) -> io::Result<super::super::SpdkTarget> {
        let config = self
            .state
            .config
            .spdk
            .as_ref()
            .ok_or_else(|| invalid("missing SPDK configuration"))?;
        let daemon = self
            .state
            .spdk_daemon
            .as_ref()
            .ok_or_else(|| invalid("missing SPDK daemon identity"))?;
        verify_daemon(config, daemon)?;
        Ok(super::super::SpdkTarget {
            rpc_socket: config.rpc_socket.clone(),
            pid: daemon.pid,
            process_start_ticks: daemon.ticks,
            bdev_name,
            image_path,
        })
    }

    fn spdk_rpc(&self, method: &str, params: Value) -> io::Result<Value> {
        let target = self.spdk_target(String::new(), PathBuf::new())?;
        let value = super::super::spdk::rpc(&target, method, params)?;
        let config = self
            .state
            .config
            .spdk
            .as_ref()
            .ok_or_else(|| invalid("missing SPDK configuration"))?;
        verify_daemon(
            config,
            self.state
                .spdk_daemon
                .as_ref()
                .ok_or_else(|| invalid("missing SPDK daemon"))?,
        )?;
        Ok(value)
    }

    pub(super) fn spdk_setup(&mut self) -> io::Result<()> {
        let config = self
            .state
            .config
            .spdk
            .as_ref()
            .ok_or_else(|| invalid("missing SPDK configuration"))?;
        let found = discover(config)?;
        if let Some(old) = &self.state.spdk_daemon {
            if old.pid != found.pid
                || old.ticks != found.ticks
                || old.socket_device != found.socket_device
                || old.socket_inode != found.socket_inode
            {
                return Err(invalid("registered SPDK daemon was replaced"));
            }
        } else {
            self.state.spdk_daemon = Some(found);
            self.save()?;
        }
        for tr in [
            "TCP",
            match self.state.config.transport {
                NvmeofTransport::Tcp => "TCP",
                NvmeofTransport::Rdma => "RDMA",
            },
        ] {
            let mut transports = self.spdk_rpc("nvmf_get_transports", json!({}))?;
            let list = transports
                .as_array()
                .ok_or_else(|| invalid("invalid SPDK transport list"))?;
            if !list.iter().any(|v| {
                v.get("trtype")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s.eq_ignore_ascii_case(tr))
            }) {
                let options = if tr == "RDMA" {
                    json!({"trtype": tr, "no_srq": true, "num_cqe": 4096})
                } else {
                    json!({"trtype": tr})
                };
                if self.spdk_rpc("nvmf_create_transport", options)? != Value::Bool(true) {
                    return Err(invalid("SPDK transport creation failed"));
                }
                transports = self.spdk_rpc("nvmf_get_transports", json!({}))?;
            }
            let list = transports
                .as_array()
                .ok_or_else(|| invalid("invalid SPDK transport list"))?;
            let mut matching = list.iter().filter(|v| {
                v.get("trtype")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s.eq_ignore_ascii_case(tr))
            });
            let transport = matching
                .next()
                .ok_or_else(|| invalid("SPDK transport readback failed"))?;
            if tr == "RDMA"
                && (matching.next().is_some()
                    || transport.get("no_srq").and_then(Value::as_bool) != Some(true)
                    || transport.get("num_cqe").and_then(Value::as_u64) != Some(4096))
            {
                return Err(invalid("SPDK RDMA transport options mismatch"));
            }
        }
        Ok(())
    }

    pub(super) fn spdk_binding(
        &self,
        id: [u8; 16],
        file: &LiveFileIdentity,
    ) -> io::Result<super::super::SpdkTarget> {
        // SPDK automatically aliases a differently named bdev to its UUID.
        // Naming it by UUID avoids introducing a second usable bdev name.
        self.spdk_target(uuid(id), file.path.clone())
    }

    fn spdk_phase(&mut self, id: [u8; 16], phase: Phase) -> io::Result<()> {
        let export = self
            .state
            .exports
            .iter_mut()
            .find(|e| e.binding.uuid == id)
            .ok_or_else(|| invalid("missing SPDK journal entry"))?;
        export.spdk_phase = Some(phase);
        self.save()
    }

    fn spdk_true(&self, method: &str, params: Value) -> io::Result<()> {
        if self.spdk_rpc(method, params)? != Value::Bool(true) {
            return Err(invalid("SPDK mutation failed"));
        }
        Ok(())
    }

    fn spdk_inventory(&self, export: &Export, require_namespace: bool) -> io::Result<()> {
        let binding = &export.binding;
        let target = binding
            .spdk
            .as_ref()
            .ok_or_else(|| invalid("missing SPDK binding"))?;
        if target.as_ref() != &self.spdk_binding(binding.uuid, &export.file)? {
            return Err(invalid("SPDK descriptor changed"));
        }
        drop(self.file(&export.file)?);
        if let Some(pin) = self.pins.get(&binding.uuid) {
            let metadata = pin.metadata()?;
            if metadata.dev() != export.file.device
                || metadata.ino() != export.file.inode
                || metadata.len() != export.file.bytes
            {
                return Err(invalid("pinned SPDK WAL identity changed"));
            }
        }
        let bdevs = self.spdk_rpc("bdev_get_bdevs", json!({}))?;
        let list = bdevs
            .as_array()
            .ok_or_else(|| invalid("invalid SPDK bdev list"))?;
        let mut matches = 0;
        for b in list {
            let aio = b.pointer("/driver_specific/aio");
            if let Some(name) = aio.and_then(|v| v.get("filename")).and_then(Value::as_str)
                && name != target.image_path.to_str().unwrap_or("")
                && let Ok(meta) = fs::metadata(name)
                && meta.dev() == export.file.device
                && meta.ino() == export.file.inode
            {
                return Err(invalid("SPDK backing inode has another AIO bdev"));
            }
            if aio.and_then(|v| v.get("filename")).and_then(Value::as_str)
                == target.image_path.to_str()
            {
                matches += 1;
                if b.get("name").and_then(Value::as_str) != Some(target.bdev_name.as_str())
                    || b.get("uuid").and_then(Value::as_str) != Some(uuid(binding.uuid).as_str())
                    || b.get("aliases")
                        .and_then(Value::as_array)
                        .is_some_and(|aliases| !aliases.is_empty())
                    || aio.and_then(|v| v.get("readonly")).and_then(Value::as_bool)
                        != Some(export.readonly)
                    || b.get("block_size").and_then(Value::as_u64)
                        != Some(u64::from(binding.block_size))
                    || b.get("num_blocks")
                        .and_then(Value::as_u64)
                        .and_then(|n| n.checked_mul(u64::from(binding.block_size)))
                        != Some(binding.bytes)
                {
                    return Err(invalid("SPDK bdev mapping changed"));
                }
            } else if b.get("name").and_then(Value::as_str) == Some(target.bdev_name.as_str()) {
                return Err(invalid("SPDK bdev name was replaced"));
            }
        }
        if matches != 1 {
            return Err(invalid("SPDK backing bdev missing or aliased"));
        }
        let mut fds = 0;
        for (index, entry) in fs::read_dir(format!("/proc/{}/fd", target.pid))?.enumerate() {
            if index >= 4096 {
                return Err(invalid("SPDK FD inventory exceeds bound"));
            }
            let entry = entry?;
            let meta = match fs::metadata(entry.path()) {
                Ok(m) => m,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            if meta.dev() != export.file.device || meta.ino() != export.file.inode {
                continue;
            }
            let info = fs::read_to_string(format!(
                "/proc/{}/fdinfo/{}",
                target.pid,
                entry.file_name().to_string_lossy()
            ))?;
            let flags = info
                .lines()
                .find_map(|l| l.strip_prefix("flags:\t"))
                .and_then(|s| i32::from_str_radix(s.trim(), 8).ok())
                .ok_or_else(|| invalid("invalid SPDK FD flags"))?;
            let access = if export.readonly {
                libc::O_RDONLY
            } else {
                libc::O_RDWR
            };
            if flags & libc::O_ACCMODE != access || flags & libc::O_DIRECT == 0 {
                return Err(invalid(
                    "SPDK backing FD is not direct with required access",
                ));
            }
            fds += 1;
        }
        if fds != 1 {
            return Err(invalid("SPDK backing FD missing or aliased"));
        }
        let subsystems = self.spdk_rpc("nvmf_get_subsystems", json!({}))?;
        let list = subsystems
            .as_array()
            .ok_or_else(|| invalid("invalid SPDK subsystem list"))?;
        let mut found = 0;
        let mut owned_subsystems = 0;
        for subsystem in list {
            let nqn = subsystem.get("nqn").and_then(Value::as_str);
            if nqn == Some(binding.nqn.as_str()) {
                let serial = uuid(binding.uuid).replace('-', "");
                if subsystem.get("serial_number").and_then(Value::as_str) != Some(&serial[..20]) {
                    return Err(invalid("SPDK subsystem serial identity changed"));
                }
                owned_subsystems += 1;
                if subsystem
                    .get("namespaces")
                    .and_then(Value::as_array)
                    .is_none()
                {
                    return Err(invalid("invalid owned SPDK subsystem"));
                }
            }
            let namespaces = subsystem.get("namespaces").and_then(Value::as_array);
            if let Some(namespaces) = namespaces {
                for ns in namespaces {
                    if ns.get("bdev_name").and_then(Value::as_str)
                        == Some(target.bdev_name.as_str())
                    {
                        if nqn != Some(binding.nqn.as_str())
                            || ns.get("nsid").and_then(Value::as_u64)
                                != Some(u64::from(binding.nsid))
                            || ns.get("uuid").and_then(Value::as_str)
                                != Some(uuid(binding.uuid).as_str())
                        {
                            return Err(invalid("SPDK namespace aliased or changed"));
                        }
                        found += 1;
                    } else if nqn == Some(binding.nqn.as_str()) {
                        return Err(invalid("unexpected SPDK namespace"));
                    }
                }
            }
        }
        let expected = if require_namespace { 1 } else { 0 };
        if owned_subsystems != 1 || found != expected {
            return Err(invalid("SPDK namespace inventory mismatch"));
        }
        Ok(())
    }

    fn spdk_access(&self, binding: &LiveTargetBinding, private: bool) -> io::Result<()> {
        let inventory = self.spdk_rpc("nvmf_get_subsystems", json!({}))?;
        let subsystems = inventory
            .as_array()
            .ok_or_else(|| invalid("invalid SPDK subsystem list"))?;
        let mut matches = subsystems
            .iter()
            .filter(|s| s.get("nqn").and_then(Value::as_str) == Some(binding.nqn.as_str()));
        let sub = matches
            .next()
            .ok_or_else(|| invalid("SPDK subsystem identity is not unique"))?;
        if matches.next().is_some() {
            return Err(invalid("SPDK subsystem identity is not unique"));
        }
        let host = if private {
            &self.state.private_host
        } else {
            &self.state.config.host_nqn
        };
        let hosts = sub
            .get("hosts")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("missing SPDK host ACL"))?;
        if sub.get("allow_any_host").and_then(Value::as_bool) != Some(false)
            || hosts.len() != 1
            || hosts[0].get("nqn").and_then(Value::as_str) != Some(host.as_str())
        {
            return Err(invalid("SPDK host ACL mismatch"));
        }
        let (transport, family, address, service) = if private {
            (
                "TCP",
                "IPv4",
                "127.0.0.1",
                self.state.config.commissioning_service,
            )
        } else {
            (
                match binding.transport {
                    NvmeofTransport::Tcp => "TCP",
                    NvmeofTransport::Rdma => "RDMA",
                },
                if binding
                    .address
                    .parse::<IpAddr>()
                    .map_err(|_| invalid("invalid address"))?
                    .is_ipv4()
                {
                    "IPv4"
                } else {
                    "IPv6"
                },
                binding.address.as_str(),
                binding.service,
            )
        };
        let listeners = sub
            .get("listen_addresses")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("missing SPDK listeners"))?;
        if listeners.len() != 1
            || listeners[0].get("trtype").and_then(Value::as_str) != Some(transport)
            || listeners[0].get("adrfam").and_then(Value::as_str) != Some(family)
            || listeners[0].get("traddr").and_then(Value::as_str) != Some(address)
            || listeners[0].get("trsvcid").and_then(Value::as_str)
                != Some(service.to_string().as_str())
        {
            return Err(invalid("SPDK listener mismatch"));
        }
        Ok(())
    }

    pub(super) fn spdk_export(
        &mut self,
        binding: &LiveTargetBinding,
        file: &LiveFileIdentity,
        readonly: bool,
    ) -> io::Result<()> {
        self.save()?;
        let target = binding
            .spdk
            .as_ref()
            .ok_or_else(|| invalid("missing SPDK target"))?;
        let file_name = file
            .path
            .to_str()
            .ok_or_else(|| invalid("non-UTF8 WAL path"))?;
        let result = self.spdk_rpc("bdev_aio_create", json!({"name":target.bdev_name,"filename":file_name,"block_size":file.block_size,"readonly":readonly,"uuid":uuid(binding.uuid)}))?;
        if result.as_str() != Some(target.bdev_name.as_str()) {
            return Err(invalid("SPDK AIO creation returned wrong name"));
        }
        let serial = uuid(binding.uuid).replace('-', "");
        self.spdk_true("nvmf_create_subsystem", json!({"nqn":binding.nqn,"serial_number":&serial[..20],"allow_any_host":false,"max_namespaces":1}))?;
        self.spdk_true(
            "nvmf_subsystem_add_host",
            json!({"nqn":binding.nqn,"host":self.state.private_host}),
        )?;
        let result = self.spdk_rpc("nvmf_subsystem_add_ns", json!({"nqn":binding.nqn,"namespace":{"bdev_name":target.bdev_name,"nsid":1,"uuid":uuid(binding.uuid)}}))?;
        if result.as_u64() != Some(1) {
            return Err(invalid("SPDK namespace identity mismatch"));
        }
        let private = json!({"trtype":"TCP","adrfam":"IPv4","traddr":"127.0.0.1","trsvcid":self.state.config.commissioning_service.to_string()});
        self.spdk_true(
            "nvmf_subsystem_add_listener",
            json!({"nqn":binding.nqn,"listen_address":private}),
        )?;
        let export = self
            .state
            .exports
            .iter()
            .find(|e| e.binding.uuid == binding.uuid)
            .ok_or_else(|| invalid("missing export"))?;
        self.spdk_inventory(export, true)?;
        self.spdk_access(binding, true)?;
        let local = LiveConnectTarget {
            nqn: binding.nqn.clone(),
            uuid: binding.uuid,
            nsid: binding.nsid,
            bytes: binding.bytes,
            block_size: binding.block_size,
            address: "127.0.0.1".to_owned(),
            service: self.state.config.commissioning_service,
            transport: NvmeofTransport::Tcp,
            spdk: binding.spdk.clone(),
            geometry: Default::default(),
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        let access = if readonly {
            LiveNamespaceAccess::ReadOnly
        } else {
            LiveNamespaceAccess::ReadWrite
        };
        let (mut controller, namespace) = self
            .commissioning
            .as_mut()
            .ok_or_else(|| invalid("missing commissioning registry"))?
            .connect(
                &local,
                &self.state.private_host,
                random_id()?,
                access,
                deadline,
            )?;
        drop(namespace);
        controller.disconnect(Instant::now() + Duration::from_secs(20))?;
        if readonly {
            self.stats.readonly_commission_successes =
                self.stats.readonly_commission_successes.saturating_add(1);
        }
        self.spdk_true(
            "nvmf_subsystem_remove_listener",
            json!({"nqn":binding.nqn,"listen_address":private}),
        )?;
        self.spdk_true(
            "nvmf_subsystem_remove_host",
            json!({"nqn":binding.nqn,"host":self.state.private_host}),
        )?;
        self.spdk_true(
            "nvmf_subsystem_add_host",
            json!({"nqn":binding.nqn,"host":self.state.config.host_nqn}),
        )?;
        let family = if self
            .state
            .config
            .address
            .parse::<IpAddr>()
            .map_err(|_| invalid("invalid address"))?
            .is_ipv4()
        {
            "IPv4"
        } else {
            "IPv6"
        };
        let transport = match binding.transport {
            NvmeofTransport::Tcp => "TCP",
            NvmeofTransport::Rdma => "RDMA",
        };
        self.spdk_true("nvmf_subsystem_add_listener", json!({"nqn":binding.nqn,"listen_address":{"trtype":transport,"adrfam":family,"traddr":binding.address,"trsvcid":binding.service.to_string()}}))?;
        self.spdk_inventory(export, true)?;
        self.spdk_access(binding, false)?;
        self.spdk_phase(binding.uuid, Phase::Active)?;
        self.stats.namespaces_exposed = self.stats.namespaces_exposed.saturating_add(1);
        Ok(())
    }

    pub(super) fn spdk_fence(&mut self, groups: Option<&[[u8; 16]]>) -> io::Result<()> {
        let exports: Vec<_> = self
            .state
            .exports
            .iter()
            .filter(|e| groups.is_none_or(|g| g.contains(&e.binding.uuid)))
            .cloned()
            .collect();
        let mut sealed = false;
        for export in exports {
            match export.spdk_phase {
                Some(Phase::Active) => {
                    self.spdk_inventory(&export, true)?;
                    self.spdk_access(&export.binding, false)?;
                    self.spdk_phase(export.binding.uuid, Phase::Removing)?;
                    let start = Instant::now();
                    self.spdk_true(
                        "nvmf_subsystem_remove_ns",
                        json!({"nqn":export.binding.nqn,"nsid":export.binding.nsid}),
                    )?;
                    self.spdk_inventory(&export, false)?;
                    self.spdk_phase(export.binding.uuid, Phase::Drained)?;
                    self.stats.disable_drain_successes =
                        self.stats.disable_drain_successes.saturating_add(1);
                    self.stats.disable_drain_time_ns = self
                        .stats
                        .disable_drain_time_ns
                        .saturating_add(start.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                    self.stats.fence_count = self.stats.fence_count.saturating_add(1);
                }
                Some(Phase::Drained) => (),
                _ => return Err(invalid("uncertain SPDK operation requires quarantine")),
            }
            if !export.readonly {
                self.write_sealed(&export.file)?;
                sealed = true;
            }
        }
        // One barrier makes every marker durable before cleanup retires its export.
        if sealed {
            self.sealed_directory.sync_all()?;
        }
        Ok(())
    }

    pub(super) fn spdk_cleanup(&mut self, groups: Option<&[[u8; 16]]>) -> io::Result<()> {
        self.spdk_fence(groups)?;
        let exports: Vec<_> = self
            .state
            .exports
            .iter()
            .filter(|e| groups.is_none_or(|g| g.contains(&e.binding.uuid)))
            .cloned()
            .collect();
        for export in exports {
            self.spdk_inventory(&export, false)?;
            self.spdk_phase(export.binding.uuid, Phase::Cleaning)?;
            self.spdk_true("nvmf_delete_subsystem", json!({"nqn":export.binding.nqn}))?;
            let target = export
                .binding
                .spdk
                .as_ref()
                .ok_or_else(|| invalid("missing SPDK target"))?;
            self.spdk_true("bdev_aio_delete", json!({"name":target.bdev_name}))?;
            self.state
                .exports
                .retain(|e| e.binding.uuid != export.binding.uuid);
            self.save()?;
            self.pins.remove(&export.binding.uuid);
        }
        Ok(())
    }

    pub(super) fn spdk_stats(&self, mut stats: LiveTargetStats) -> io::Result<LiveTargetStats> {
        for export in &self.state.exports {
            if export.spdk_phase == Some(Phase::Active) {
                self.spdk_inventory(export, true)?;
                self.spdk_access(&export.binding, false)?;
                stats.owned_namespace_count = stats.owned_namespace_count.saturating_add(1);
            } else if export.spdk_phase != Some(Phase::Drained) {
                return Err(invalid("uncertain SPDK journal"));
            }
        }
        Ok(stats)
    }
}
