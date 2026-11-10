//! Stock SPDK protection uses a read-only AIO bdev, not NVMe feature 0x84.
//! This is owner control-plane admission/revocation; no RPC runs per value read.

use super::SpdkTarget;
use super::lease::{ExportBinding, uuid_string, verify_root_protected};
use serde_json::{Value, json};
use std::fs;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn process(target: &SpdkTarget) -> io::Result<()> {
    if target.pid == 0 || target.process_start_ticks == 0 {
        return Err(invalid("invalid SPDK process identity"));
    }
    let proc = std::path::PathBuf::from(format!("/proc/{}", target.pid));
    if fs::metadata(&proc)?.uid() != 0 {
        return Err(invalid("SPDK process must be root owned"));
    }
    let stat = fs::read_to_string(proc.join("stat"))?;
    let end = stat
        .rfind(')')
        .ok_or_else(|| invalid("invalid SPDK process stat"))?;
    let ticks = stat[end + 1..]
        .split_whitespace()
        .nth(19)
        .and_then(|s| s.parse::<u64>().ok());
    if ticks != Some(target.process_start_ticks) {
        return Err(invalid("SPDK process was replaced"));
    }
    let exe = fs::read_link(proc.join("exe"))?;
    if exe.file_name().is_none_or(|name| name != "nvmf_tgt") {
        return Err(invalid("unexpected SPDK executable"));
    }
    verify_root_protected(&exe).map_err(io::Error::other)?;
    Ok(())
}

pub(super) fn connect_rpc(path: &std::path::Path) -> io::Result<UnixStream> {
    let bytes = path.as_os_str().as_bytes();
    let mut address = libc::sockaddr_un {
        sun_family: libc::AF_UNIX as libc::sa_family_t,
        sun_path: [0; 108],
    };
    if bytes.is_empty() || bytes.len() >= address.sun_path.len() || bytes.contains(&0) {
        return Err(invalid("SPDK Unix socket path exceeds address bounds"));
    }
    for (destination, source) in address.sun_path.iter_mut().zip(bytes) {
        *destination = *source as libc::c_char;
    }
    // SAFETY: socket returns a new uniquely owned descriptor or -1. Nonblocking
    // connect fails closed on a full Unix listener backlog rather than hanging.
    let raw = unsafe {
        libc::socket(
            libc::AF_UNIX,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: raw is the successful fresh descriptor above; OwnedFd closes it
    // on every later error, and no second owner is constructed.
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
    // SAFETY: address is initialized and length covers its family and terminated
    // pathname without exceeding sockaddr_un. connect borrows both synchronously.
    let result = unsafe {
        libc::connect(
            fd.as_raw_fd(),
            (&raw const address).cast(),
            length as libc::socklen_t,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    let stream = UnixStream::from(fd);
    stream.set_nonblocking(false)?;
    Ok(stream)
}

pub(super) fn peer_credentials(socket: &UnixStream) -> io::Result<libc::ucred> {
    let mut peer = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = std::mem::size_of_val(&peer) as libc::socklen_t;
    // SAFETY: the live socket and correctly sized ucred/length outputs match
    // Linux SO_PEERCRED. No pointer outlives this synchronous getsockopt.
    let result = unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut peer).cast(),
            &raw mut length,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize != std::mem::size_of_val(&peer) {
        return Err(invalid("invalid SPDK RPC peer credentials"));
    }
    Ok(peer)
}

pub(super) fn rpc(target: &SpdkTarget, method: &str, params: Value) -> io::Result<Value> {
    process(target)?;
    verify_root_protected(&target.rpc_socket).map_err(io::Error::other)?;
    let mut socket = connect_rpc(&target.rpc_socket)?;
    let peer = peer_credentials(&socket)?;
    if peer.uid != 0 || peer.pid as u32 != target.pid {
        return Err(invalid("SPDK RPC peer identity mismatch"));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    socket.set_write_timeout(Some(Duration::from_secs(5)))?;
    serde_json::to_writer(
        &mut socket,
        &json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}),
    )?;
    socket.write_all(b"\n")?;
    let mut response = Vec::with_capacity(4096);
    let mut buffer = [0; 4096];
    for _ in 0..1024 {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|d| !d.is_zero())
            .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "SPDK RPC deadline"))?;
        socket.set_read_timeout(Some(remaining))?;
        let count = socket.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "SPDK RPC closed",
            ));
        }
        response.extend_from_slice(&buffer[..count]);
        match serde_json::from_slice::<Value>(&response) {
            Ok(mut value) => {
                if value.get("id") != Some(&json!(1)) || value.get("error").is_some() {
                    return Err(invalid("SPDK RPC returned an error or wrong identity"));
                }
                process(target)?;
                return value
                    .get_mut("result")
                    .map(Value::take)
                    .ok_or_else(|| invalid("SPDK RPC has no result"));
            }
            Err(error) if error.is_eof() => (),
            Err(error) => return Err(error.into()),
        }
    }
    Err(invalid("SPDK RPC exceeds bounded response size"))
}

fn namespace<'a>(subsystems: &'a Value, binding: &ExportBinding) -> io::Result<Option<&'a Value>> {
    let subsystems = subsystems
        .as_array()
        .ok_or_else(|| invalid("invalid SPDK subsystem response"))?;
    let mut found = None;
    for subsystem in subsystems {
        if subsystem.get("nqn").and_then(Value::as_str) != Some(binding.nqn.as_str()) {
            continue;
        }
        for ns in subsystem
            .get("namespaces")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("invalid SPDK namespace list"))?
        {
            if ns.get("nsid").and_then(Value::as_u64) == Some(u64::from(binding.nsid))
                && found.replace(ns).is_some()
            {
                return Err(invalid("duplicate SPDK namespace identity"));
            }
        }
    }
    Ok(found)
}

fn verify_mapping(
    target: &SpdkTarget,
    binding: &ExportBinding,
    ns: &Value,
    bdev: &Value,
) -> io::Result<()> {
    let aio = bdev
        .pointer("/driver_specific/aio")
        .ok_or_else(|| invalid("SPDK bdev is not AIO"))?;
    if ns.get("bdev_name").and_then(Value::as_str) != Some(target.bdev_name.as_str())
        || ns.get("uuid").and_then(Value::as_str) != Some(uuid_string(binding.export_uuid).as_str())
        || bdev.get("name").and_then(Value::as_str) != Some(target.bdev_name.as_str())
        || aio.get("readonly").and_then(Value::as_bool) != Some(true)
        || aio.get("filename").and_then(Value::as_str) != target.image_path.to_str()
        || bdev.get("block_size").and_then(Value::as_u64) != Some(u64::from(binding.block_size))
        || bdev
            .get("num_blocks")
            .and_then(Value::as_u64)
            .and_then(|n| n.checked_mul(u64::from(binding.block_size)))
            != Some(binding.image_len)
    {
        return Err(invalid("SPDK namespace or read-only bdev mapping mismatch"));
    }
    Ok(())
}

fn verify_image_fd(target: &SpdkTarget) -> io::Result<()> {
    verify_root_protected(&target.image_path).map_err(io::Error::other)?;
    let image = fs::metadata(&target.image_path)?;
    let directory = std::path::PathBuf::from(format!("/proc/{}/fd", target.pid));
    let mut found = false;
    for (index, row) in fs::read_dir(directory)?.enumerate() {
        if index >= 4096 {
            return Err(invalid("SPDK FD inventory exceeds bound"));
        }
        let row = row?;
        let meta = match fs::metadata(row.path()) {
            Ok(meta) => meta,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        if meta.dev() != image.dev() || meta.ino() != image.ino() {
            continue;
        }
        let info = fs::read_to_string(format!(
            "/proc/{}/fdinfo/{}",
            target.pid,
            row.file_name().to_string_lossy()
        ))?;
        let flags = info
            .lines()
            .find_map(|line| line.strip_prefix("flags:\t"))
            .and_then(|text| i32::from_str_radix(text.trim(), 8).ok())
            .ok_or_else(|| invalid("invalid SPDK backing FD flags"))?;
        if flags & libc::O_ACCMODE != libc::O_RDONLY || flags & libc::O_DIRECT == 0 {
            return Err(invalid("SPDK backing FD must be read-only direct I/O"));
        }
        found = true;
    }
    if !found {
        return Err(invalid("SPDK process has no matching backing FD"));
    }
    Ok(())
}

pub(super) fn inspect(target: &SpdkTarget, binding: &ExportBinding) -> io::Result<fs::Metadata> {
    process(target)?;
    verify_image_fd(target)?;
    let before = fs::symlink_metadata(&target.rpc_socket)?;
    if !before.file_type().is_socket() {
        return Err(invalid("SPDK RPC path is not a socket"));
    }
    let subsystems = rpc(target, "nvmf_get_subsystems", json!({}))?;
    let ns = namespace(&subsystems, binding)?.ok_or_else(|| invalid("SPDK namespace is absent"))?;
    let bdevs = rpc(target, "bdev_get_bdevs", json!({"name":target.bdev_name}))?;
    let list = bdevs
        .as_array()
        .ok_or_else(|| invalid("invalid SPDK bdev response"))?;
    if list.len() != 1 {
        return Err(invalid("SPDK bdev identity is not unique"));
    }
    verify_mapping(target, binding, ns, &list[0])?;
    let after = fs::symlink_metadata(&target.rpc_socket)?;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(invalid("SPDK RPC socket changed during admission"));
    }
    verify_image_fd(target)?;
    Ok(after)
}

pub(super) fn disable(
    target: &SpdkTarget,
    binding: &ExportBinding,
    device: u64,
    inode: u64,
) -> io::Result<()> {
    process(target)?;
    let socket = fs::symlink_metadata(&target.rpc_socket)?;
    if socket.dev() != device || socket.ino() != inode {
        return Err(invalid("registered SPDK socket was replaced"));
    }
    let subsystems = rpc(target, "nvmf_get_subsystems", json!({}))?;
    if namespace(&subsystems, binding)?.is_none() {
        return Ok(());
    }
    inspect(target, binding)?;
    if rpc(
        target,
        "nvmf_subsystem_remove_ns",
        json!({"nqn":binding.nqn,"nsid":binding.nsid}),
    )? != Value::Bool(true)
    {
        return Err(invalid("SPDK namespace removal failed"));
    }
    if namespace(&rpc(target, "nvmf_get_subsystems", json!({}))?, binding)?.is_some() {
        return Err(invalid("SPDK namespace remains after removal"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::nvmeof::ExportTarget;

    fn mapping() -> (SpdkTarget, ExportBinding, Value, Value) {
        let target = SpdkTarget {
            rpc_socket: "/run/tidehunter-spdk/test/rpc.sock".into(),
            pid: 1,
            process_start_ticks: 1,
            bdev_name: "wal_0".into(),
            image_path: "/var/lib/tidehunter/export/image.wal".into(),
        };
        let binding = ExportBinding {
            database_uuid: [1; 16],
            epoch: 1,
            partition: 0,
            generation: 1,
            export_uuid: [2; 16],
            nqn: "nqn.2026-01.test:wal".into(),
            nsid: 1,
            wal_file_bytes: 4096,
            source_file_index: 0,
            image_len: 4096,
            block_size: 512,
            source_device: 1,
            source_inode: 2,
            durable_frontier: 4096,
            target: ExportTarget::Spdk(target.clone()),
        };
        let ns = json!({"nsid":1,"bdev_name":"wal_0","uuid":uuid_string(binding.export_uuid)});
        let bdev = json!({"name":"wal_0","block_size":512,"num_blocks":8,
            "supported_io_types":{"write":true}, "claim_type":"exclusive_write",
            "driver_specific":{"aio":{"filename":target.image_path,"readonly":true}}});
        (target, binding, ns, bdev)
    }

    #[test]
    fn rejects_writable_replacement_wrong_image_and_geometry_overflow() {
        let (target, binding, ns, bdev) = mapping();
        // AIO advertises WRITE capability even when the actual bdev is RO.
        verify_mapping(&target, &binding, &ns, &bdev).unwrap();
        for (pointer, replacement) in [
            ("/driver_specific/aio/readonly", json!(false)),
            (
                "/driver_specific/aio/filename",
                json!("/var/lib/other/image.wal"),
            ),
            ("/num_blocks", json!(u64::MAX)),
        ] {
            let mut changed = bdev.clone();
            *changed.pointer_mut(pointer).unwrap() = replacement;
            assert!(
                verify_mapping(&target, &binding, &ns, &changed).is_err(),
                "{pointer}"
            );
        }
        let mut changed_ns = ns;
        changed_ns["uuid"] = json!(uuid_string([3; 16]));
        assert!(verify_mapping(&target, &binding, &changed_ns, &bdev).is_err());
    }

    #[test]
    fn duplicate_namespace_cannot_select_an_arbitrary_mapping() {
        let (_, binding, ns, _) = mapping();
        let response = json!([{"nqn":binding.nqn,"namespaces":[ns.clone(),ns]}]);
        assert!(namespace(&response, &binding).is_err());
    }
}
