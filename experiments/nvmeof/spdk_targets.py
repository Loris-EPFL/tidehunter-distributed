#!/usr/bin/env python3
"""Exact-owned, bounded SPDK immutable-file targets; no raw-SSD ownership.

Production comparisons select RDMA explicitly. TCP is only a loopback local
qualification option, never an automatic transport fallback.
"""
import argparse
import ipaddress
import json
import math
import os
from pathlib import Path
import socket
import stat
import subprocess
import sys
import time
import uuid

import campaign_targets as campaign
import manage
import spdk_artifacts

MAX_RPC = 4 << 20
TRANSPORT = {"max_queue_depth": 128, "max_io_qpairs_per_ctrlr": 4,
             "max_io_size": 131072, "io_unit_size": 131072,
             "num_shared_buffers": 4096, "buf_cache_size": 32}


def command(argv, timeout=30):
    return subprocess.check_output([str(x) for x in argv], text=True, stderr=subprocess.STDOUT,
                                   timeout=timeout).strip()


def rpc(path, method, params=None):
    request = {"jsonrpc": "2.0", "id": 1, "method": method}
    deadline = time.monotonic() + 5
    if params is not None:
        request["params"] = params
    with socket.socket(socket.AF_UNIX) as channel:
        channel.settimeout(5)
        channel.connect(str(path))
        channel.settimeout(max(.001, deadline - time.monotonic()))
        channel.sendall((json.dumps(request) + "\n").encode())
        raw = b""
        for _ in range(1024):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("SPDK RPC deadline expired: " + method)
            channel.settimeout(remaining)
            part = channel.recv(65536)
            if not part or len(raw) + len(part) > MAX_RPC:
                raise ValueError("SPDK RPC ended or exceeded its bound: " + method)
            raw += part
            try:
                result = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(result, dict) or result.get("id") != 1 or "error" in result or "result" not in result:
                raise RuntimeError("SPDK RPC " + method + ": " + str(result))
            return result["result"]
        raise ValueError("SPDK RPC response exceeded its receive bound")


def unit_status(state):
    process = subprocess.run(["systemctl", "show", state["unit_name"], "--no-pager",
        "--property=LoadState,Description,ActiveState,MainPID,Result,ExecMainStatus"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=10)
    result = dict(line.split("=", 1) for line in process.stdout.splitlines() if "=" in line)
    if process.returncode and result.get("LoadState") != "not-found":
        raise subprocess.CalledProcessError(process.returncode, process.args, output=process.stdout)
    if "LoadState" not in result:
        raise ValueError("systemd did not report SPDK unit state")
    if result.get("LoadState") != "not-found" and result.get("Description") != state["unit_description"]:
        raise ValueError("SPDK unit identity changed")
    return result


def process_identity(pid):
    path = Path("/proc") / str(pid)
    value = (path / "stat").read_text()
    fields = value[value.rfind(")") + 2:].split()
    if len(fields) < 20 or path.stat().st_uid != 0:
        raise ValueError("SPDK process identity is unavailable or not root-owned")
    exe = (path / "exe").stat()
    return {"pid": pid, "process_start_ticks": int(fields[19]),
            "executable_device": exe.st_dev, "executable_inode": exe.st_ino}


def verify_process(state):
    observed = process_identity(state["pid"])
    if any(observed[k] != state[k] for k in observed):
        raise ValueError("SPDK process or executable was replaced")
    sock = Path(state["rpc_socket"])
    info = sock.lstat()
    if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077
            or (info.st_dev, info.st_ino) != (state["rpc_device"], state["rpc_inode"])):
        raise ValueError("SPDK RPC socket identity or protection changed")
    return observed

def capture_rpc_lock(state):
    """Bind the private RPC lock inode to an FD held by the exact daemon."""
    verify_process(state)
    lock = Path(state["rpc_socket"] + ".lock")
    info = lock.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0
            or info.st_mode & 0o077 or info.st_nlink != 1):
        raise ValueError("SPDK RPC lock is not a private root-owned regular file")
    descriptors = list((Path("/proc") / str(state["pid"]) / "fd").iterdir())
    if len(descriptors) > 4096:
        raise ValueError("SPDK descriptor inspection exceeds bound")
    found = False
    for fd in descriptors:
        try:
            actual = fd.stat()
            if (actual.st_dev, actual.st_ino) == (info.st_dev, info.st_ino):
                found = True
                break
        except FileNotFoundError:
            continue
    if not found:
        raise ValueError("SPDK daemon does not hold the RPC lock inode")
    verify_process(state)
    current = lock.lstat()
    if (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino):
        raise ValueError("SPDK RPC lock changed during admission")
    return {"rpc_lock_device": info.st_dev, "rpc_lock_inode": info.st_ino}


def verify_exports(state):
    verify_process(state)
    bdevs = rpc(state["rpc_socket"], "bdev_get_bdevs")
    subsystems = rpc(state["rpc_socket"], "nvmf_get_subsystems")
    if not isinstance(bdevs, list) or not isinstance(subsystems, list):
        raise ValueError("SPDK target topology is unavailable")
    descriptors = list((Path("/proc") / str(state["pid"]) / "fd").iterdir())
    if len(descriptors) > 4096:
        raise ValueError("SPDK descriptor inspection exceeds bound")
    for entry in state["exports"]:
        m = manage.manifest(Path(entry["manifest"]))
        image = manage.image_for(Path(entry["manifest"]), m)
        info = image.stat()
        bdev = [b for b in bdevs if b.get("name") == entry["bdev_name"]]
        if (len(bdev) != 1 or bdev[0].get("block_size") != m["block_size"]
                or bdev[0].get("num_blocks", 0) * m["block_size"] != m["image_len"]
                or bdev[0].get("driver_specific", {}).get("aio", {}).get("filename") != str(image)
                or bdev[0].get("driver_specific", {}).get("aio", {}).get("readonly") is not True):
            raise ValueError("SPDK bdev image or geometry differs from export")
        subsystem = [s for s in subsystems if s.get("nqn") == entry["nqn"]]
        if len(subsystem) != 1 or subsystem[0].get("allow_any_host") is not False:
            raise ValueError("SPDK subsystem or host ACL differs from export")
        expected_listener = {"trtype": state["transport"].upper(), "adrfam": "IPv4",
                             "traddr": entry["address"], "trsvcid": str(entry["service"])}
        if subsystem[0].get("listen_addresses") != [expected_listener]:
            raise ValueError("SPDK listener differs from selected transport or owner endpoint")
        namespaces = [n for n in subsystem[0].get("namespaces", []) if n.get("nsid") == entry["nsid"]]
        if (len(namespaces) != 1 or namespaces[0].get("bdev_name") != entry["bdev_name"]
                or namespaces[0].get("uuid", "").lower() != entry["uuid"].lower()
                or {h.get("nqn") for h in subsystem[0].get("hosts", [])} != {entry["hostnqn"]}):
            raise ValueError("SPDK namespace, UUID or host mapping differs from export")
        matching = []
        for fd in descriptors:
            try:
                actual = fd.stat()
                if (actual.st_dev, actual.st_ino) != (info.st_dev, info.st_ino):
                    continue
                fields = dict(line.split(":", 1) for line in
                    (Path("/proc") / str(state["pid"]) / "fdinfo" / fd.name).read_text().splitlines() if ":" in line)
                matching.append(int(fields["flags"].strip(), 8))
            except FileNotFoundError:
                continue
        if not matching or any((flags & os.O_ACCMODE) != os.O_RDONLY or not flags & os.O_DIRECT for flags in matching):
            raise ValueError("SPDK image descriptor is not read-only direct I/O")
    return {"bdevs": bdevs, "subsystems": subsystems}


def configure(state):
    path = state["rpc_socket"]
    rpc(path, "framework_start_init")
    options = dict(TRANSPORT, trtype=state["transport"].upper())
    if state["transport"] == "rdma":
        options.update(no_srq=True, num_cqe=4096)
    rpc(path, "nvmf_create_transport", options)
    for entry in state["exports"]:
        m = manage.manifest(Path(entry["manifest"]))
        image = manage.image_for(Path(entry["manifest"]), m)
        rpc(path, "bdev_aio_create", {"name": entry["bdev_name"], "filename": str(image),
                                     "block_size": m["block_size"], "readonly": True})
        rpc(path, "nvmf_create_subsystem", {"nqn": entry["nqn"], "serial_number": entry["uuid"].replace("-", "")[:20],
                                           "allow_any_host": False, "max_namespaces": entry["nsid"]})
        rpc(path, "nvmf_subsystem_add_host", {"nqn": entry["nqn"], "host": entry["hostnqn"]})
        rpc(path, "nvmf_subsystem_add_ns", {"nqn": entry["nqn"], "namespace": {
            "bdev_name": entry["bdev_name"], "nsid": entry["nsid"], "uuid": entry["uuid"]}})
        rpc(path, "nvmf_subsystem_add_listener", {"nqn": entry["nqn"], "listen_address": {
            "trtype": state["transport"].upper(), "traddr": entry["address"],
            "trsvcid": str(entry["service"]), "adrfam": "IPv4"}})


def stop_owned(state):
    status = unit_status(state)
    if (status.get("LoadState") == "not-found" and state.get("unit_start_attempted")
            and not state.get("unit_started")):
        raise RuntimeError("SPDK start acknowledgement is ambiguous; preserve state until the exact start is reconciled")
    if status.get("LoadState") != "not-found":
        command(["systemctl", "stop", state["unit_name"]], timeout=25)
    if "pid" in state:
        try:
            actual = process_identity(state["pid"])
        except FileNotFoundError:
            actual = None
        if actual is not None and actual["process_start_ticks"] == state["process_start_ticks"]:
            raise RuntimeError("owned SPDK target is still running")
    runtime = Path(state["rpc_socket"]).parent
    if runtime.exists():
        info = runtime.lstat()
        if (info.st_dev, info.st_ino) != tuple(state["runtime_identity"]):
            raise ValueError("SPDK runtime directory was replaced")
        entries = {entry.name for entry in runtime.iterdir()}
        if entries - {"rpc.sock", "rpc.sock.lock"}:
            raise ValueError("refusing to remove unknown SPDK runtime objects")
        sock = runtime / "rpc.sock"
        lock = runtime / "rpc.sock.lock"
        if "rpc.sock" in entries:
            info = sock.lstat()
            if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != 0
                    or "rpc_inode" not in state
                    or (info.st_dev, info.st_ino) != (state["rpc_device"], state["rpc_inode"])):
                raise ValueError("refusing to remove an unrelated runtime socket")
        if "rpc.sock.lock" in entries:
            info = lock.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o077
                    or info.st_nlink != 1 or "rpc_lock_inode" not in state
                    or (info.st_dev, info.st_ino) != (state["rpc_lock_device"], state["rpc_lock_inode"])):
                raise ValueError("refusing to remove an unrelated RPC lock")
        if "rpc.sock" in entries:
            sock.unlink()
        if "rpc.sock.lock" in entries:
            lock.unlink()
        runtime.rmdir()


def capture_journal(state):
    try:
        state["journal_tail"] = command(["journalctl", "--unit", state["unit_name"],
            "--no-pager", "--output=short-monotonic", "--lines=200"], timeout=10)
    except (OSError, subprocess.SubprocessError) as error:
        state["journal_error"] = repr(error)


def setup(args):
    address = ipaddress.ip_address(args.address)
    if not (args.local_qualification and address.is_loopback and args.transport == "tcp"):
        address = ipaddress.ip_address(campaign.private_address(args.address))
        if args.transport != "rdma":
            raise ValueError("SPDK production comparison requires RDMA")
    if not math.isfinite(args.deadline_epoch):
        raise ValueError("SPDK target requires a finite absolute deadline")
    lifetime_seconds = int(args.deadline_epoch - time.time())
    if not 30 <= lifetime_seconds <= 18000:
        raise ValueError("SPDK absolute deadline must be 30..18000 seconds away")
    prefix = Path(args.spdk_prefix)
    artifact = spdk_artifacts.admit_prefix(prefix)
    exports = campaign.load_exports(args.ready, args.export_root)
    if not exports or len(exports) > 256 or any(entry["nsid"] > 256 for entry in exports):
        raise ValueError("SPDK export/namespace bound exceeded")
    if (not 1 <= args.base_service <= 65535 or args.base_service + len(exports) > 65536
            or not 1 <= args.base_port_id <= 65535 or args.base_port_id + len(exports) > 65536):
        raise ValueError("SPDK service range exceeds TCP/RDMA port bounds")
    campaign_id = str(uuid.uuid4())
    token = uuid.UUID(campaign_id).hex
    runtime = Path("/run/tidehunter-spdk") / token
    binary = prefix / "bin/nvmf_tgt"
    binary_info = binary.stat()
    state = {"version": 1, "kind": "campaign-target", "phase": "creating",
             "target_implementation": "spdk", "campaign_id": campaign_id,
             "transport": args.transport, "export_root": str(campaign.absolute(args.export_root)),
             "unit_name": "tidehunter-spdk-" + token, "unit_description": "Tidehunter SPDK " + token,
             "rpc_socket": str(runtime / "rpc.sock"), "binary_sha256": artifact["files"]["bin/nvmf_tgt"]["sha256"],
             "executable_device": binary_info.st_dev, "executable_inode": binary_info.st_ino,
             "runtime_seconds": lifetime_seconds, "deadline_epoch": args.deadline_epoch, "exports": exports}
    state_path = campaign.absolute(args.state)
    for index, entry in enumerate(exports):
        host, hostid = campaign.host_identity(campaign_id, entry["uuid"])
        entry.update(address=str(address), port_id=args.base_port_id + index, service=args.base_service + index,
                     hostnqn=host, hostid=hostid, bdev_name="wal_" + str(index), status="pending",
                     state=str(state_path / ("export-%03d" % index)))
    directory = campaign.new_campaign_state(state_path, state)
    try:
        if unit_status(state).get("LoadState") != "not-found":
            raise ValueError("SPDK unit already exists")
        runtime.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent = manage.trusted_directory(runtime.parent)
        os.close(parent)
        runtime.mkdir(mode=0o700)
        info = runtime.stat()
        state["runtime_identity"] = [info.st_dev, info.st_ino]
        manage.save(directory, state)
        cores = sorted(os.sched_getaffinity(0))[:2]
        if not cores:
            raise ValueError("SPDK requires an available CPU")
        argv = [str(binary), "--no-huge", "--no-pci", "-s", "1024", "-m", str(cores).replace(" ", ""),
                "-r", state["rpc_socket"], "--num-trace-entries", "0", "--wait-for-rpc"]
        lifetime_seconds = math.floor(args.deadline_epoch - time.time())
        if lifetime_seconds < 30:
            raise TimeoutError("SPDK target deadline expired during preparation")
        state["runtime_seconds"] = lifetime_seconds
        state["unit_start_attempted"] = True
        manage.save(directory, state)
        command(["systemd-run", "--unit=" + state["unit_name"], "--description=" + state["unit_description"],
                 "--collect", "--service-type=exec", "--property=RuntimeMaxSec=" + str(lifetime_seconds),
                 "--property=Restart=no",
                 "--property=KillMode=control-group", "--property=TimeoutStopSec=15", "--property=UMask=0077",
                 "--property=MemoryMax=2G", "--property=TasksMax=128", "--property=LimitMEMLOCK=infinity",
                 "--setenv=LD_LIBRARY_PATH=" + str(prefix / "lib"), "--", *argv])
        state["unit_started"] = True
        manage.save(directory, state)
        ready_until = time.monotonic() + 20
        for _ in range(200):
            if time.monotonic() >= ready_until:
                raise TimeoutError("SPDK target readiness deadline expired")
            status = unit_status(state)
            if status.get("ActiveState") == "failed":
                raise RuntimeError("SPDK target failed: " + str(status))
            pid = int(status.get("MainPID", "0"))
            if pid > 0 and Path(state["rpc_socket"]).exists():
                observed = process_identity(pid)
                if any(observed[k] != state[k] for k in ("executable_device", "executable_inode")):
                    raise ValueError("SPDK service executed a different binary")
                state.update(observed)
                socket_path = Path(state["rpc_socket"])
                sock = socket_path.lstat()
                if not stat.S_ISSOCK(sock.st_mode) or sock.st_uid != 0:
                    raise ValueError("SPDK service did not create an owned Unix socket")
                socket_path.chmod(0o600)
                state.update(rpc_device=sock.st_dev, rpc_inode=sock.st_ino)
                state.update(capture_rpc_lock(state))
                manage.save(directory, state)
                break
            time.sleep(.05)
        else:
            raise TimeoutError("SPDK target readiness expired")
        configure(state)
        verify_exports(state)
        state["effective_transports"] = rpc(state["rpc_socket"], "nvmf_get_transports")
        for entry in exports:
            image = manage.image_for(Path(entry["manifest"]), manage.manifest(Path(entry["manifest"])))
            entry["target"] = {"spdk": {"rpc_socket": state["rpc_socket"], "pid": state["pid"],
                "process_start_ticks": state["process_start_ticks"], "bdev_name": entry["bdev_name"], "image_path": str(image)}}
            entry["target_receipt"] = {"target_implementation": "spdk", "target_cache": "direct_file_bdev_no_OS_pagecache",
                "target_cache_verified": True, "selected_transport": args.transport,
                "direct_io_verified": True, "read_only_verified": True,
                "binary_sha256": state["binary_sha256"], "target": entry["target"]}
            entry["status"] = "published"
        state["phase"] = "published"
        manage.save(directory, state)
        return campaign.receipt(state)
    except BaseException as error:
        state["phase"] = "failed"
        state["error"] = type(error).__name__ + ": " + str(error)
        if "runtime_identity" in state:
            try:
                stop_owned(state)
                state["rollback_stopped"] = True
            except Exception as cleanup_error:
                state["rollback_error"] = repr(cleanup_error)
        if state.get("unit_start_attempted"):
            capture_journal(state)
        try:
            manage.save(directory, state)
        except Exception as journal_error:
            error.add_note("SPDK failure journal could not be saved: " + repr(journal_error))
        raise
    finally:
        directory.close()


def stats(args):
    directory, state = campaign.campaign_state(campaign.absolute(args.state), "campaign-target")
    try:
        if state.get("target_implementation") != "spdk" or state["phase"] != "published":
            raise ValueError("SPDK statistics require an owned published target")
        verify_exports(state)
        return {"kind": "spdk-target-stats", "target_implementation": "spdk",
                **{k: state[k] for k in ("campaign_id", "unit_name", "pid", "process_start_ticks", "binary_sha256")},
                "bdev_iostat": rpc(state["rpc_socket"], "bdev_get_iostat"),
                "nvmf_stats": rpc(state["rpc_socket"], "nvmf_get_stats")}
    finally:
        directory.close()


def cleanup(args):
    if not args.readers_drained:
        raise ValueError("explicit reader drain is required")
    proof = campaign.trusted_json(args.host_disconnected)
    directory, state = campaign.campaign_state(campaign.absolute(args.state), "campaign-target")
    try:
        if state.get("target_implementation") != "spdk":
            raise ValueError("not an owned SPDK target")
        campaign.validate_disconnect_proof(state, proof)
        if state["phase"] != "cleaned":
            stop_owned(state)
            for entry in state["exports"]:
                entry["status"] = "cleaned"
            state.update(phase="cleaned", readers_drained=True)
            manage.save(directory, state)
        return campaign.receipt(state)
    finally:
        directory.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    create = sub.add_parser("target-setup")
    for option in ("ready", "export-root", "state", "address", "spdk-prefix"):
        create.add_argument("--" + option, required=True)
    for option in ("base-port-id", "base-service"):
        create.add_argument("--" + option, type=int, required=True)
    create.add_argument("--deadline-epoch", type=float, required=True)
    create.add_argument("--transport", choices=("rdma", "tcp"), required=True)
    create.add_argument("--local-qualification", action="store_true")
    for name in ("stats", "status", "target-cleanup"):
        command_parser = sub.add_parser(name)
        command_parser.add_argument("--state", required=True)
        if name == "status":
            command_parser.add_argument("--kind", choices=("target",), required=True)
        if name == "target-cleanup":
            command_parser.add_argument("--readers-drained", action="store_true", required=True)
            command_parser.add_argument("--host-disconnected", required=True)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError("SPDK target management requires trusted root")
    if args.action == "status":
        directory, state = campaign.campaign_state(campaign.absolute(args.state), "campaign-target")
        try:
            if state.get("target_implementation") != "spdk":
                raise ValueError("unknown target implementation")
            result = campaign.receipt(state)
        finally:
            directory.close()
    else:
        result = {"target-setup": setup, "stats": stats, "target-cleanup": cleanup}[args.action](args)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, RuntimeError, KeyError, subprocess.SubprocessError) as error:
        print("spdk-targets: " + type(error).__name__ + ": " + str(error), file=sys.stderr)
        sys.exit(1)
