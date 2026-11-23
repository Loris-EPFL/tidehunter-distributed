"""Exact-owned local NVMe-oF helper lifecycle for the dry-run runner.

The caller owns the fixture directory, prepared exports and live owner processes.
The helpers own target and initiator identities. No global cleanup is performed.
"""
import errno
import fcntl
from itertools import islice
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import threading
import time
import uuid

ROOT = ["docker", "run", "--rm", "--privileged", "--network=host", "--pid=host",
        "--cgroupns=host", "--entrypoint", "nsenter",
        "sha256:bc4b6f355966c200c7088d8a494b04cf162cd346542b89db6262d0794ca507ed",
        "--target", "1", "--mount", "--root", "--wd", "--"]
HELPERS = Path(__file__).resolve().parent.parent / "nvmeof"


class TargetError(RuntimeError):
    pass


SYSFS_NVME = Path("/sys/class/nvme")
SYSFS_BLOCK = Path("/sys/class/block")
NVMET_CONFIGFS = Path("/sys/kernel/config/nvmet")
# Shared by all qualification workers on the host, even with different output roots.
KERNEL_PORT_STATE = Path("/run/tidehunter-kernel-live-ports")
IPV4_PORT_SYSCTLS = Path("/proc/sys/net/ipv4")
KERNEL_SERVICE_PORTS = range(20000, 65535, 2)
# The client connects with this geometry; the proof must record exactly its queue and submission counts.
KERNEL_QUEUE_GEOMETRY = {"io_queues": 1, "queue_size": 32, "poll_queues": 0}
# Bound every diagnostic listing; evidence must not grow with host state.
DIAGNOSTIC_ENTRIES = 256


def _owned_controllers(sysfs, transport, address, services, identity):
    """Initiator controllers scoped to this run's addresses, services and NQN."""
    rows = []
    services = {str(service) for service in services}
    for path in sysfs.glob("nvme*"):
        if not re.fullmatch(r"nvme[0-9]+", path.name):
            continue
        attrs = {}
        try:
            for name in ("transport", "address", "subsysnqn", "hostnqn", "hostid", "queue_count", "sqsize"):
                attrs[name] = (path / name).read_text().strip()
            fields = dict(p.split("=", 1) for p in attrs["address"].split(",") if "=" in p)
            if (attrs["transport"] == transport and fields.get("traddr") == address
                    and fields.get("trsvcid") in services
                    and (identity(attrs["hostnqn"], attrs["subsysnqn"]) if callable(identity)
                         else attrs["hostnqn"] == identity)):
                stat = path.stat()
                rows.append(dict(attrs, controller=path.name, device=stat.st_dev, inode=stat.st_ino))
        except FileNotFoundError:
            continue
    return rows


def _write_fd_bounded(fd, data, timeout):
    """Write and close `fd` on a helper thread: a sysfs write can block in the
    kernel, and must never outlive the cleanup. The thread owns the descriptor."""
    errors = []

    def write():
        try:
            if os.write(fd, data) != len(data):
                errors.append(TargetError("incomplete sysfs write"))
        except OSError as error:
            errors.append(error)
        finally:
            os.close(fd)

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TargetError(f"sysfs write did not return within {timeout:.1f}s")
    if errors and getattr(errors[0], "errno", None) not in (errno.ENOENT, errno.ENODEV):
        raise errors[0]


def _delete_enumerated_controller(sysfs, row, timeout):
    """Delete the enumerated controller object, never a replacement at its name.

    The directory descriptor pins the object; identity and attributes are
    rechecked through it, and `delete_controller` is opened relative to it
    (as `experiments/nvmeof/manage.py` `delete_controller` does).
    """
    try:
        directory = os.open(sysfs / row["controller"], os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    except OSError as error:
        if error.errno in (errno.ENOENT, errno.ENODEV):
            return
        raise
    try:
        stat = os.fstat(directory)
        if (stat.st_dev, stat.st_ino) != (row["device"], row["inode"]):
            return
        for name in ("transport", "address", "subsysnqn", "hostnqn"):
            attribute = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory)
            with os.fdopen(attribute) as stream:
                if stream.read(4097).strip() != row[name]:
                    return
        delete = os.open("delete_controller", os.O_WRONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                         dir_fd=directory)
    except OSError as error:
        if error.errno in (errno.ENOENT, errno.ENODEV):
            return
        raise
    finally:
        os.close(directory)
    _write_fd_bounded(delete, b"1", timeout)


def _delete_owned_controllers(sysfs, transport, address, services, identity, deadline):
    """Delete this run's initiator controllers and wait until they are gone.

    The client deletes them on graceful close. A client killed at its deadline
    cannot, and nothing else owns them. Returns the enumerated controller names.
    """
    rows = _owned_controllers(sysfs, transport, address, services, identity)
    for row in rows:
        _delete_enumerated_controller(sysfs, row, max(.1, deadline - time.monotonic()))
    while _owned_controllers(sysfs, transport, address, services, identity):
        if time.monotonic() >= deadline:
            raise TargetError("owned initiator controllers remain after delete")
        time.sleep(.05)
    return [row["controller"] for row in rows]


def _attributes(path, names):
    values = {}
    for name in names:
        try:
            values[name] = (path / name).read_text(errors="replace").strip()
        except OSError as error:
            values[name] = repr(error)
    return values


def _nvme_state(sysfs_nvme=SYSFS_NVME, sysfs_block=SYSFS_BLOCK, configfs=NVMET_CONFIGFS):
    """Host controllers and disks, and target namespaces and ports, for failure evidence."""
    limit = DIAGNOSTIC_ENTRIES
    disks = {}
    for path in sorted(sysfs_block.glob("nvme*"))[:limit]:
        disks[path.name] = dict(_attributes(path, ("nsid", "uuid", "hidden", "ro", "size", "dev")),
                                device=os.path.realpath(path / "device"))
    return {
        "controllers": {path.name: _attributes(path, (
            "state", "transport", "address", "subsysnqn", "hostnqn", "cntlid", "queue_count", "sqsize"))
            for path in sorted(sysfs_nvme.glob("nvme*"))[:limit]},
        "disks": disks,
        "target_namespaces": {f"{path.parent.parent.name}/{path.name}": _attributes(
            path, ("enable", "device_path", "device_uuid", "buffered_io"))
            for path in sorted(configfs.glob("subsystems/*/namespaces/*"))[:limit]},
        "target_ports": {path.name: sorted(p.name for p in path.glob("subsystems/*"))[:limit]
                         for path in sorted(configfs.glob("ports/*"))[:limit]},
    }


def _process_threads(pid, proc=Path("/proc")):
    """Per-thread state and kernel stack; root reads `stack`."""
    rows = []
    tasks = sorted((proc / str(pid) / "task").iterdir(), key=lambda p: int(p.name))
    for task in tasks[:DIAGNOSTIC_ENTRIES]:
        row = dict(_attributes(task, ("comm", "wchan", "syscall", "stack")), tid=int(task.name))
        try:
            row["state"] = next(line.split(":", 1)[1].strip() for line in
                                (task / "status").read_text().splitlines() if line.startswith("State:"))
        except (OSError, StopIteration) as error:
            row["state"] = repr(error)
        rows.append(row)
    return {"pid": pid, "task_count": len(tasks), "threads": rows}


def _gdb_backtrace(pid, timeout):
    gdb = shutil.which("gdb")
    if gdb is None:
        return "gdb unavailable\n"
    try:
        result = subprocess.run([gdb, "-nx", "-batch", "-p", str(pid), "-ex", "thread apply all bt"],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return f"gdb timed out after {timeout}s\n{exc.stdout or ''}"
    return result.stdout + result.stderr


def _journal(args, timeout):
    try:
        result = subprocess.run(["journalctl", "--no-pager", "-o", "short-monotonic", *args],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as error:
        return repr(error) + "\n"
    return result.stdout + result.stderr


def _inside(root, path):
    root, path = Path(root).resolve(strict=True), Path(path)
    if not path.is_absolute() or not path.resolve().is_relative_to(root):
        raise ValueError("path must be inside the absolute fixture directory")
    return path


def _write(path, data):
    # Do not overwrite an earlier receipt, including after a partial operation.
    with open(path, "x", encoding="utf-8") as stream:
        json.dump(data, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())

def _write_or_match(path, data):
    try:
        _write(path, data)
    except FileExistsError:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        with os.fdopen(fd, encoding="utf-8") as stream:
            existing = json.load(stream)
        if existing != data:
            raise TargetError("existing receipt differs: " + str(path))


def _run(script, args, timeout=180, *, cpu_affinity=None):
    affinity = ["taskset", "--cpu-list", ",".join(map(str, cpu_affinity))] if cpu_affinity else []
    command = ROOT + affinity + ["python3", "-B", str(HELPERS / script), *map(str, args)]
    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired as exc:
        raise TargetError(f"{script} timed out: stdout={exc.stdout!r}; stderr={exc.stderr!r}") from exc
    if result.returncode:
        raise TargetError(f"{script} failed ({result.returncode}): stdout={result.stdout!r}; stderr={result.stderr!r}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise TargetError(f"{script} returned invalid JSON: stdout={result.stdout!r}; stderr={result.stderr!r}") from exc


def select_transport(readiness):
    """Fall back only on explicit RDMA-specific missing prerequisites.

    Successful prerequisite checks are not proof of an RDMA route. A connection
    failure is not classified as transport unavailability and must fail closed.
    """
    missing = readiness.get("missing")
    if readiness.get("transport") != "rdma" or not isinstance(missing, list):
        raise ValueError("invalid RDMA prerequisite evidence")
    specific = {"active_rdma_port", "kernel_module:rdma_cm", "kernel_module:nvme_rdma",
                "kernel_module:nvmet_rdma"}
    if not missing:
        return "rdma"
    if set(missing) <= specific:
        return "tcp"
    raise TargetError("non-RDMA prerequisites are missing: " + ", ".join(missing))


class SealedTargets:
    """One prepared campaign, with explicit disconnect then target cleanup.

    `ready` and all export manifests/images must be inside `fixture`. Five
    owners may contribute exports to the same ready catalog. The runner must
    drain all readers before calling cleanup.
    """
    def __init__(self, fixture, ready, export_root, *, target, transport, address,
                 base_port_id, base_service, spdk_prefix=None, deadline_epoch=None):
        self.fixture = Path(fixture).resolve(strict=True)
        if not Path(fixture).is_absolute():
            raise ValueError("fixture must be absolute")
        self.ready = _inside(self.fixture, ready)
        self.export_root = _inside(self.fixture, export_root)
        self.target = target
        self.transport = transport
        self.address = address
        if target not in ("kernel", "spdk") or transport not in ("rdma", "tcp"):
            raise ValueError("unknown target or transport")
        # Existing SPDK helper's TCP qualification is loopback-only, while the
        # campaign initiator rejects loopback. Do not silently weaken either.
        if target == "spdk" and transport == "tcp":
            raise TargetError("SPDK TCP route cannot be admitted by the existing campaign helpers")
        if address not in ("10.99.0.1", "10.99.0.2"):
            raise ValueError("unexpected local target address")
        self.base_port_id, self.base_service = base_port_id, base_service
        self.spdk_prefix, self.deadline_epoch = spdk_prefix, deadline_epoch
        self.target_state = self.fixture / "sealed-target"
        self.host_state = self.fixture / "sealed-host"
        self.target_receipt = self.fixture / "sealed-target.json"
        self.host_receipt = self.fixture / "sealed-host.json"
        self.disconnect_receipt = self.fixture / "sealed-disconnected.json"
        self.cleanup_receipt = self.fixture / "sealed-cleaned.json"
    def _recover_failure(self, error):
        if isinstance(error, subprocess.TimeoutExpired):
            raise TargetError(f"operation timed out: {error}; fatal cleanup blocker: "
                              "helper completion and resource ownership are unknown") from error
        if not self.target_state.exists():
            return
        try:
            script = "spdk_targets.py" if self.target == "spdk" else "campaign_targets.py"
            target = _run(script, ["status", "--state", self.target_state, "--kind", "target"])
            if target.get("kind") != "campaign-target":
                raise TargetError("invalid owned target state")
            if not self.host_state.exists():
                raise TargetError("no helper-generated host disconnect proof")
            host = _run("campaign_targets.py", ["status", "--state", self.host_state, "--kind", "host"])
            if (host.get("kind") != "campaign-host" or
                    host.get("campaign_id") != target.get("campaign_id")):
                raise TargetError("host and target campaign identities differ")
            self.cleanup(readers_drained=True)
        except Exception as cleanup_error:
            raise TargetError(
                f"operation failed: {error}; fatal cleanup blocker: {cleanup_error}; "
                f"retained target state {self.target_state} and host state {self.host_state}"
            ) from error


    def setup(self):
        args = ["target-setup", "--ready", self.ready, "--export-root", self.export_root,
                "--state", self.target_state, "--address", self.address, "--transport", self.transport,
                "--base-port-id", self.base_port_id, "--base-service", self.base_service]
        script = "campaign_targets.py"
        if self.target == "spdk":
            if self.spdk_prefix is None or self.deadline_epoch is None:
                raise ValueError("SPDK prefix and absolute deadline are required")
            script = "spdk_targets.py"
            args += ["--spdk-prefix", self.spdk_prefix, "--deadline-epoch", self.deadline_epoch]
        try:
            result = _run(script, args)
            if result.get("phase") != "published" or not result.get("exports"):
                raise TargetError("target did not publish exports")
            _write_or_match(self.target_receipt, result)
            return result
        except Exception as error:
            self._recover_failure(error)
            raise

    def connect(self):
        try:
            result = _run("campaign_targets.py", ["host-connect", "--ready", self.ready,
                "--export-root", self.export_root, "--targets", self.target_receipt,
                "--state", self.host_state, "--nr-io-queues", 4, "--queue-size", 128])
            if result.get("phase") != "connected" or not result.get("bindings"):
                raise TargetError("sealed initiator did not connect all exports")
            for export in result["exports"]:
                geometry = export.get("host_receipt", {}).get("queue_geometry", {})
                if geometry.get("observed") != {"queue_count": 5, "sqsize": 127}:
                    raise TargetError("sealed controller geometry not admitted")
            _write_or_match(self.host_receipt, result)
            return result
        except Exception as error:
            self._recover_failure(error)
            raise

    def cleanup(self, *, readers_drained):
        if readers_drained is not True:
            raise ValueError("reader drain proof required")
        disconnected = _run("campaign_targets.py", ["host-disconnect", "--state", self.host_state,
                            "--readers-drained"])
        if disconnected.get("phase") != "disconnected":
            raise TargetError("initiator disconnect was not proven")
        _write_or_match(self.disconnect_receipt, disconnected)
        script = "spdk_targets.py" if self.target == "spdk" else "campaign_targets.py"
        cleaned = _run(script, ["target-cleanup", "--state", self.target_state,
                               "--readers-drained", "--host-disconnected", self.disconnect_receipt])
        if cleaned.get("phase") != "cleaned":
            raise TargetError("target cleanup was not proven")
        _write_or_match(self.cleanup_receipt, cleaned)
        return {"disconnect": disconnected, "target": cleaned}

def record_live_geometry(fixture, controller, *, nqn, address, service, transport):
    """Admit a live controller by exact sysfs identity and 1x32 readback."""
    root = Path(fixture)
    if not root.is_absolute():
        raise ValueError("fixture must be absolute")
    root = root.resolve(strict=True)
    if not re.fullmatch(r"nvme[0-9]+", controller):
        raise ValueError("invalid controller identity")
    if address not in ("10.99.0.1", "10.99.0.2") or transport not in ("rdma", "tcp"):
        raise ValueError("invalid live endpoint")
    ctrl = Path("/sys/class/nvme") / controller
    before = ctrl.stat()
    fields = dict(item.split("=", 1) for item in (ctrl / "address").read_text().strip().split(",")
                  if "=" in item)
    actual = {"nqn": (ctrl / "subsysnqn").read_text().strip(),
              "transport": (ctrl / "transport").read_text().strip(),
              "address": fields.get("traddr"), "service": fields.get("trsvcid"),
              "queue_count": int((ctrl / "queue_count").read_text().strip()),
              "sqsize": int((ctrl / "sqsize").read_text().strip())}
    after = ctrl.stat()
    expected = {"nqn": nqn, "transport": transport, "address": address,
                "service": str(service), "queue_count": 2, "sqsize": 31}
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino) or actual != expected:
        raise TargetError("live controller identity or queue geometry mismatch")
    receipt = {"controller": controller, "controller_identity": [before.st_dev, before.st_ino],
               "requested": {"nr_io_queues": 1, "queue_size": 32}, "observed": actual}
    _write(root / f"live-geometry-{controller}.json", receipt)
    return receipt


class LiveSpdkDaemon:
    """A single owner daemon; the owner creates and drains its own namespaces."""
    def __init__(self, fixture, owner_index, executable, deadline_epoch, *, socket=None):
        self.fixture = Path(fixture).resolve(strict=True)
        if not Path(fixture).is_absolute() or type(owner_index) is not int or not 0 <= owner_index < 5:
            raise ValueError("absolute fixture and owner index 0..4 required")
        self.owner_index = owner_index
        self.executable = Path(executable)
        if not self.executable.is_absolute():
            raise ValueError("SPDK executable must be absolute")
        self.deadline_epoch = deadline_epoch
        if socket is None:
            socket = Path("/run/tidehunter-spdk-live") / uuid.uuid4().hex / "rpc.sock"
        if (not isinstance(socket, (str, Path)) or
                not re.fullmatch(r"/run/tidehunter-spdk-live/[0-9a-f]{32}/rpc\.sock", str(socket))):
            raise ValueError("invalid live SPDK socket path")
        self.socket = Path(socket)
        self.token = self.socket.parent.name
        self.state = self.fixture / f"live-spdk-{owner_index}"
        self.started = self.fixture / f"live-spdk-{owner_index}-started.json"
        self.stopped = self.fixture / f"live-spdk-{owner_index}-stopped.json"

    def start(self):
        # SPDK holds exclusive core locks; cohosted owners need disjoint pairs.
        cores = sorted(os.sched_getaffinity(0))[2 * self.owner_index:2 * self.owner_index + 2]
        if len(cores) != 2:
            raise TargetError("local live SPDK requires two distinct CPUs per owner")
        result = _run("live_spdk_daemon.py", ["start", "--state", self.state,
            "--socket", self.socket, "--executable", self.executable,
            "--deadline-epoch", self.deadline_epoch], timeout=60, cpu_affinity=cores)
        if result.get("phase") != "ready":
            raise TargetError("live SPDK daemon is not ready")
        _write(self.started, result)
        return result

    def observe(self):
        return _run("live_spdk_daemon.py", ["observe", "--state", self.state])

    def stop(self, owner_proof, *, initiator_drained):
        if initiator_drained is not True:
            raise ValueError("initiator drain proof required")
        proof = _inside(self.fixture, owner_proof)
        result = _run("live_spdk_daemon.py", ["stop", "--state", self.state,
            "--owner-proof", proof, "--initiator-drained"], timeout=60)
        if result.get("phase") != "stopped":
            raise TargetError("live SPDK daemon stop not proven")
        _write(self.stopped, result)
        return result

def qualify_kernel_live(fixture, binary, *, transport="rdma",
                        deadline_seconds=600, library_path=None):
    """Run both real single-owner durability tiers in a root-owned private fixture.

    Output and temporary WALs are under the owned fixture; WALs are
    removed only after admitted shutdown.
    TCP is an explicit caller choice, never an implicit fallback.
    """
    fixture = Path(fixture)
    binary = Path(binary)
    if not fixture.is_absolute() or not binary.is_absolute() or not fixture.is_dir():
        raise ValueError("absolute existing output directory and binary required")
    if transport not in ("rdma", "tcp") or not 120 <= deadline_seconds <= 1200:
        raise ValueError("invalid transport or qualification deadline")
    container = ROOT
    if library_path is not None:
        library_path = _inside(fixture, library_path).resolve(strict=True)
        if not library_path.is_dir() or ":" in str(library_path):
            raise ValueError("library path must be a directory inside the fixture")
        container = ROOT + ["env", "LD_LIBRARY_PATH=" + str(library_path)]
    deadline_epoch = time.time() + deadline_seconds
    argv = container + ["python3", "-B", str(Path(__file__).resolve()), "--kernel-live-worker",
                        str(fixture), str(binary), transport, str(deadline_seconds), str(deadline_epoch)]
    result = subprocess.run(argv, capture_output=True, text=True, timeout=deadline_seconds)
    if result.returncode:
        raise TargetError("kernel live qualification failed; evidence retained: "
                          + result.stdout.strip() + " " + result.stderr.strip())
    return json.loads(result.stdout)


def _kernel_port_exclusions(sysctls, configfs):
    first, last = map(int, (sysctls / "ip_local_port_range").read_text().split())
    if not 1 <= first <= last <= 65535:
        raise TargetError("invalid ip_local_port_range")
    excluded = set(range(first, last + 1))
    reserved = (sysctls / "ip_local_reserved_ports").read_text().strip()
    for item in reserved.split(",") if reserved else ():
        low, sep, high = item.partition("-")
        start, end = int(low), int(high if sep else low)
        if not 1 <= start <= end <= 65535:
            raise TargetError("invalid ip_local_reserved_ports")
        excluded.update(range(start, end + 1))
    for port in (configfs / "ports").iterdir():
        # Use the service as configfs ID too. Reject either collision, even if
        # the existing port has no subsystem linked and no listener to probe.
        service = (port / "addr_trsvcid").read_text().strip()
        excluded.add(int(port.name))
        if service.isdecimal():
            excluded.add(int(service))
    return excluded


def _kernel_live_ports(address, *, commissioning_address, control_address, state, evidence,
                       deadline, sysctls=IPV4_PORT_SYSCTLS, configfs=NVMET_CONFIGFS):
    """Claim a host-wide service pair; release only after verified cleanup."""
    with (state / "allocate.lock").open("a") as lock:
        while time.monotonic() < deadline:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(.05)
        else:
            raise TimeoutError("kernel NVMe port pool lock deadline expired")

        excluded = _kernel_port_exclusions(sysctls, configfs)
        with socket.socket() as control:
            control.bind((control_address, 0))
            control_port = control.getsockname()[1]
            excluded.add(control_port)
            busy = 0
            for service in KERNEL_SERVICE_PORTS:
                if time.monotonic() >= deadline:
                    raise TimeoutError("kernel NVMe port pool allocation deadline expired")
                commissioning = service + 1
                claim = state / f"{service}.claim"
                if service in excluded or commissioning in excluded or claim.exists():
                    continue
                try:
                    # These probes detect current address conflicts only. They
                    # cannot hold the endpoints open for the kernel target.
                    with socket.socket() as public, socket.socket() as private:
                        public.bind((address, service))
                        private.bind((commissioning_address, commissioning))
                except OSError as error:
                    if error.errno != errno.EADDRINUSE:
                        raise
                    busy += 1
                    continue
                # Persistent for this boot, unlike flock: a killed worker may
                # leave a service or a target that has temporarily unbound it.
                with claim.open("x") as stream:
                    stream.write(f"{evidence}\n")
                return control_port, service, commissioning, claim
    raise TargetError(f"no free adjacent non-ephemeral kernel NVMe services ({busy} bind conflicts)")


def _admit_kernel_geometry(record, lifecycles, geometry):
    """Admit the controllers the client itself recorded, per database lifecycle.

    Each lifecycle is (step, routes, effective owner). A route must name the owner and carry exactly
    one controller record of the configured geometry. One controller serves several routes, so routes
    may repeat a (sysfs_device, sysfs_inode) pair, but all routes of one pair must agree on controller
    instance, subsystem, host and geometry. The kernel reuses instance numbers: the same name under
    another pair is a new controller and is admitted.
    """
    queue_count = geometry["io_queues"] + geometry["poll_queues"] + 1
    sqsize = geometry["queue_size"] - 1
    import cloud_campaign as campaign

    admitted = []
    for step, routes, owner in lifecycles:
        if not isinstance(routes, list) or not routes:
            raise TargetError(f"live controller geometry not proven: no route record at {step}")
        pairs = {}
        for route in routes:
            identity = route.get("namespace_identity") if isinstance(route, dict) else None
            if not campaign.live_route_matches_owner(route, owner):
                raise TargetError(f"live controller geometry not proven: route at {step} names another "
                                  "initiator or subsystem")
            controller = identity.get("controller")
            instance = identity.get("controller_instance")
            if (not isinstance(controller, dict)
                    or set(controller) != {"queue_count", "sqsize", "sysfs_device", "sysfs_inode"}
                    or any(type(value) is not int for value in controller.values())
                    or type(instance) is not int or instance < 0):
                raise TargetError(f"live controller geometry not proven: controller record missing at {step}")
            if (controller["queue_count"], controller["sqsize"]) != (queue_count, sqsize):
                raise TargetError(f"live controller geometry not proven: {step} controller nvme{instance} has "
                                  f"queue_count={controller['queue_count']}, sqsize={controller['sqsize']}; "
                                  f"expected {queue_count}, {sqsize}")
            # The sysfs device is shared by every sysfs entry: only the pair names one directory.
            pair = (controller["sysfs_device"], controller["sysfs_inode"])
            facts = (instance, identity["nqn"], route["host_nqn"], tuple(route["host_id"]),
                     controller["queue_count"], controller["sqsize"])
            if pairs.setdefault(pair, facts) != facts:
                raise TargetError(f"live controller geometry not proven: {step} controller nvme{instance} "
                                  "has routes that disagree on instance, subsystem, host or geometry")
            admitted.append({"step": step, "controller": f"nvme{instance}", "nqn": identity["nqn"],
                             "host_nqn": route["host_nqn"], "host_id": route["host_id"], **controller})
    record["controller_records"] = admitted


def _admit_kernel_durability(remote, tier):
    if (remote.get("live_nvme") or {}).get("leased_durability") != tier:
        raise TargetError("prepared kernel qualification durability differs from the requested tier")


def _kernel_public_identity(host_nqn, subsystem):
    import cloud_campaign as campaign

    match = campaign.LIVE_HOST_SCOPE.fullmatch(host_nqn)
    return (match is not None and campaign.live_database_id(match[2]) and match[3] == "00000000"
            and campaign.scoped_live_subsystem(subsystem, match[1]))


TARGET_ID_BYTES = 32


def _kernel_commissioning_identities(root, _address=None, _transport=None, _service=None,
                                     _port_id=None):
    """Bind commissioning identities to this owner's persisted target identity.

    The protocol-36 owner keeps one `target-id` per state directory: a 16-byte
    registry id followed by the little-endian device and inode of its sealed
    marker directory. The commissioning host is derived from the database
    scope, so no journal is read.
    """
    import cloud_campaign as campaign
    import nvme_campaign_owner as owner

    try:
        owner._trusted_dir(root)
    except FileNotFoundError:
        return {}
    with os.scandir(root) as entries:
        candidates = list(islice(entries, DIAGNOSTIC_ENTRIES + 1))
    if len(candidates) > DIAGNOSTIC_ENTRIES:
        raise TargetError("owner root exceeds commissioning identity scan bound")
    identities = {}
    for entry in candidates:
        database = entry.name.removeprefix("db-")
        if not entry.name.startswith("db-") or not campaign.live_database_id(database):
            continue
        state_dir = Path(entry.path) / "live-target"
        sealed_dir = state_dir / "sealed"
        try:
            owner._trusted_dir(state_dir)
            owner._trusted_dir(sealed_dir)
            raw = owner._read_bytes(state_dir / "target-id", TARGET_ID_BYTES)
        except FileNotFoundError:
            continue
        if len(raw) != TARGET_ID_BYTES or raw[:16] == bytes(16):
            raise TargetError("invalid commissioning target identity")
        sealed = sealed_dir.stat()
        device = int.from_bytes(raw[16:24], "little")
        inode = int.from_bytes(raw[24:32], "little")
        if sealed.st_dev != device or sealed.st_ino != inode:
            raise TargetError("commissioning sealed marker directory identity changed")
        scope = f"nqn.2026-01.local.tidehunter:db-{database}-p00000000-"
        private = scope + "commission"
        if private in identities:
            raise TargetError("commissioning identity is shared by two owner slots")
        identities[private] = scope
    return identities


def _admit_kernel_reuse(proof, remote, address, service, transport, endpoint):
    import cloud_campaign as campaign

    reuse = proof.get("target_reuse")
    if not isinstance(reuse, dict) or reuse.get("dropped_database") == reuse.get("database"):
        raise TargetError("kernel qualification lacks distinct database identities")
    live = remote["live_nvme"]
    seed = live.get("owner_identities", {}).get("0", live)
    writer_routes = proof.get("member_transition", {}).get("writer_routes")
    if not isinstance(writer_routes, list) or not writer_routes:
        raise TargetError("kernel qualification lacks writer routes")
    lifecycles = []
    for db_key, initiator_key, route_key in (
        ("dropped_database", "dropped_initiators", "routes_before_drop"),
        ("database", "initiators", "routes_after_reprovision"),
    ):
        database = reuse.get(db_key)
        if not campaign.live_database_id(database):
            raise TargetError("invalid kernel qualification database identity")
        effective = campaign.effective_live_owner({**seed, "address": address, "service": service}, database, 0)
        expected = {"database": database, "partition": 0, "host_nqn": effective["host_nqn"],
                    "host_id": effective["host_id"]}
        routes = reuse.get(route_key)
        if (reuse.get(initiator_key) != [expected] or not isinstance(routes, list) or not routes
                or any(not campaign.live_route_matches_owner(route, effective) or route.get("transport") != transport
                       or route.get("target_implementation", "kernel") != "kernel"
                       or type(route["namespace_identity"].get("controller_instance")) is not int
                       or route["namespace_identity"]["controller_instance"] < 0
                       for route in routes + (writer_routes if db_key == "dropped_database" else []))):
            raise TargetError("kernel target reuse route differs from the provisioned database and slot")
        lifecycles.append((route_key, routes, effective))
        if db_key == "dropped_database":
            lifecycles.append(("writer_routes", writer_routes, effective))
    shutdown = proof.get("target_shutdown")
    if not isinstance(shutdown, list) or len(shutdown) != 1 or not isinstance(shutdown[0], dict):
        raise TargetError("kernel target shutdown proof missing")
    row = shutdown[0]
    stats = row.get("stats")
    if (not campaign.live_identity_bytes(row.get("database")) or bytes(row["database"]).hex() != reuse["database"]
            or type(row.get("partition")) is not int or row["partition"] != 0 or row.get("endpoint") != endpoint
            or type(row.get("epoch")) is not int or row["epoch"] < 1
            or not isinstance(stats, dict) or stats.get("fixture_only") is not False
            or any(type(stats.get(key)) is not int or stats[key] != 0 for key in ("errors", "owned_namespace_count"))):
        raise TargetError("kernel target shutdown does not name the clean reprovisioned database")
    return reuse, lifecycles


def _kernel_live_prepare_input(guest, control_endpoint, transport, addr, service, port_id, tier,
                               owner_wal_file_bytes=None):
    prep = {"guest_root": str(guest), "addresses": [control_endpoint],
        "transport": {"kind": "tcp"}, "durability_tier": tier,
        "owner_wal_geometry": "research",
        "max_batches_per_partition": 16384, "max_admitted_batches": 16384,
        "coordinator_cache_bytes": 4 * 1024**2,
        "owner_wal_member_read": "async", "polling": "adaptive",
        # One host slot: drop -> reprovision must reuse it.
        "live_nvmeof": {"transport": transport, "addresses": [addr], "service": service,
            "port_id": port_id, "max_databases": 1, "max_inflight_requests": 32,
            "queue_geometry": dict(KERNEL_QUEUE_GEOMETRY),
            "max_inflight_bytes": 16 * 1024**2, "connect_timeout_ms": 30000, "io_timeout_ms": 30000}}
    if owner_wal_file_bytes is not None:
        prep["owner_wal_file_bytes"] = owner_wal_file_bytes
    return prep


def _kernel_live_worker(output, binary, transport, seconds, deadline_epoch, *,
                        owner_wal_file_bytes=None):
    import hashlib
    import shutil
    import signal
    import sys
    import time
    import traceback
    import yaml

    import nvme_campaign_owner as owner
    import cloud_campaign

    if os.geteuid() != 0:
        raise TargetError("kernel live worker requires root")
    output = Path(output).resolve(strict=True)
    owner._trusted_dir(output)
    KERNEL_PORT_STATE.mkdir(mode=0o700, exist_ok=True)
    owner._trusted_dir(KERNEL_PORT_STATE)
    binary = Path(binary).resolve(strict=True)
    deadline = time.monotonic() + max(0, deadline_epoch - time.time() - 5)
    with binary.open("rb") as stream:
        binary_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    token = uuid.uuid4().hex
    root = output / ("kernel-live-root-" + token)
    root.mkdir(mode=0o700)
    root_id = (root.stat().st_dev, root.stat().st_ino)
    out = output / ("kernel-live-" + token)
    out.mkdir(mode=0o700)
    overall = {"scope": "single owner kernel live NVMe-oF qualification, not performance",
               "root": str(root), "transport": transport, "binary_sha256": binary_sha256,
               "tiers": {}, "complete": False}

    def run(argv, timeout, log=None):
        argv = list(map(str, argv))
        try:
            result = subprocess.run(argv, check=False, text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            if log is not None:
                Path(log).write_text("stdout:\n" + str(exc.stdout or "") +
                                     "\nstderr:\n" + str(exc.stderr or ""))
            raise
        if log is not None:
            Path(log).write_text("stdout:\n" + result.stdout + "\nstderr:\n" + result.stderr)
        if result.returncode:
            raise TargetError(f"command failed ({result.returncode}): {argv!r}; "
                              f"stdout={result.stdout!r}; stderr={result.stderr!r}")
        return result.stdout


    def tier_run(tier):
        directory = root / tier
        directory.mkdir(mode=0o700)
        evidence = out / tier
        evidence.mkdir(mode=0o700)
        guest, prepared = directory / "guest", directory / "prepared"
        node = guest / "node-0"
        # The kernel target commissions locally, on the same loopback as control.
        control_address = "127.0.0.1"
        addr = control_address if transport == "tcp" else "10.99.0.2"
        control, service, commissioning, claim = _kernel_live_ports(
            addr, commissioning_address=control_address, control_address=control_address,
            state=KERNEL_PORT_STATE, evidence=evidence, deadline=deadline)
        port_id = service
        unit = "tidehunter-local-kernel-" + token + "-" + tier
        description = "Tidehunter local kernel " + token + " " + tier
        record = {"tier": tier, "unit": unit, "cleanup_complete": False, "errors": [],
                  "port_claim": str(claim), "public_service": service, "commissioning_service": commissioning}
        attempted = captured = False
        client = None
        launch = remote = reprovisioned_database = None

        def remaining(cap):
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError("kernel qualification deadline expired")
            return max(.1, min(cap, left))

        def status():
            text = run(["systemctl", "show", unit, "--no-pager",
                        "--property=LoadState,Description,ActiveState,SubState,MainPID,Result,ExecMainStatus"],
                       remaining(10))
            value = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
            if value.get("LoadState") != "not-found" and value.get("Description") != description:
                raise TargetError("owner unit identity changed")
            return value

        def diagnose(name, capture):
            # Failure evidence is best-effort; it never replaces the failure.
            try:
                capture()
            except Exception as error:
                record.setdefault("diagnostic_errors", []).append({"evidence": name, "error": repr(error)})

        def capture_process(label, pid):
            _write(evidence / f"{label}-threads.json", _process_threads(pid))
            (evidence / f"{label}-gdb.txt").write_text(_gdb_backtrace(pid, 10))

        try:
            prep = _kernel_live_prepare_input(
                guest, f"{control_address}:{control}", transport, addr, service, port_id, tier,
                owner_wal_file_bytes)
            prep_path = evidence / "prepare.json"
            prep_path.write_text(json.dumps(prep))
            run([binary, "--prepare-external-case", prep_path, "--output", prepared],
                remaining(90), evidence / "prepare.log")
            remote = json.loads((prepared / "remote.json").read_text())
            launch = yaml.safe_load((prepared / "node-0/launch.yaml").read_text())
            launch["launch_nonce"] = uuid.uuid4().hex
            if owner_wal_file_bytes is not None and (
                    remote["storage"]["wal_file_bytes"] != owner_wal_file_bytes):
                raise TargetError("prepared qualification WAL member size differs from request")
            host = launch["host"]
            target = host["live_target_pool"]
            _admit_kernel_durability(remote, tier)
            if (launch["listen"] != f"{control_address}:{control}" or target["address"] != addr
                    or target["transport"] != transport or host["max_databases"] != 1
                    or target["public_port_base"] != port_id
                    or target["commissioning_port_base"] != port_id + 1
                    or target["public_service_base"] != service
                    or target["commissioning_service_base"] != commissioning):
                raise TargetError("generated kernel owner identity differs from requested fixture")
            # The host creates its root and each database's target state below it.
            node.mkdir(mode=0o700, parents=True)
            for tls, tls_root in ((remote["tls"], guest), (launch["tls"], node)):
                for name in ("ca", "cert", "key"):
                    if not tls.get(name):
                        continue
                    dest = Path(tls[name])
                    if dest.parent != tls_root / "tls" or not re.fullmatch(r"[A-Za-z0-9_.-]+", dest.name):
                        raise TargetError("TLS path outside fixture")
                    source = prepared / dest.relative_to(guest)
                    data = source.read_bytes()
                    if source.is_symlink() or not data or len(data) > 65536:
                        raise TargetError("invalid TLS source")
                    dest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    if dest.exists():
                        if dest.is_symlink() or dest.read_bytes() != data:
                            raise TargetError("TLS material differs")
                    else:
                        fd = os.open(dest, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                        with os.fdopen(fd, "wb") as stream:
                            stream.write(data)
            launch_path = node / "launch.yaml"
            launch_path.write_text(yaml.safe_dump(launch, sort_keys=False))
            launch_path.chmod(0o600)
            if _owned_controllers(SYSFS_NVME, transport, addr, (service,), _kernel_public_identity):
                raise TargetError("existing controller matches fresh host identity")
            lifetime = int(deadline - time.monotonic()) - 31
            if lifetime < 1:
                raise TimeoutError("insufficient owner lifetime")
            attempted = True
            service_env = (["--setenv=LD_LIBRARY_PATH=" + os.environ["LD_LIBRARY_PATH"]]
                           if os.environ.get("LD_LIBRARY_PATH") else [])
            run(["systemd-run", "--unit=" + unit, "--description=" + description,
                 "--service-type=exec", "--property=RuntimeMaxSec=" + str(lifetime),
                 "--property=Restart=no", "--property=RemainAfterExit=yes",
                 "--property=KillMode=control-group", "--property=TimeoutStopSec=15",
                 "--property=LimitMEMLOCK=infinity", *service_env, "--", binary,
                 "--experimental-storage-node", launch_path], remaining(20))
            while True:
                current = status()
                if int(current.get("MainPID", "0")) > 0:
                    record["owner_identity"] = owner.capture_live_owner_identity(node, launch_path, current)
                    captured = True
                    break
                if current.get("ActiveState") in ("failed", "inactive"):
                    raise TargetError("owner exited before identity capture: " + repr(current))
                time.sleep(min(.05, remaining(1)))
            ready = Path(launch["ready_file"])
            while not ready.is_file():
                if status().get("ActiveState") in ("failed", "inactive"):
                    raise TargetError("owner exited before readiness")
                time.sleep(min(.1, remaining(1)))
            if ready.read_text().strip() != launch["listen"]:
                raise TargetError("owner readiness mismatch")
            receipt = cloud_campaign.read_owner_receipt(launch)
            boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
            if (receipt["durability"] != launch["durability"] or receipt["durability"]["tier"] != tier
                    or receipt["pid"] != record["owner_identity"]["process_id"]
                    or receipt["boot_id"] != boot_id):
                raise TargetError("owner durability receipt mismatch")
            record["durability_receipt"] = receipt
            input_path = directory / "qualification.json"
            input_path.write_text(json.dumps({"remote_config": remote, "coordinator_path": str(directory / "database"),
                "namespace": list(uuid.uuid4().bytes), "expected_route": transport,
                "durability_tier": tier, "value_bytes": 1024}))
            budget = remaining(seconds) - 75
            if budget <= 0:
                raise TimeoutError("insufficient cleanup reserve")
            with (evidence / "qualification.log").open("wb") as log:
                client = subprocess.Popen([str(binary), "--experimental-live-nvmeof-qualification",
                    str(input_path), "--output", str(evidence / "native")], stdin=subprocess.DEVNULL,
                    stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = client.wait(timeout=budget)
                except Exception as error:
                    if isinstance(error, subprocess.TimeoutExpired):
                        # The kill destroys the stalled stacks; capture them first.
                        diagnose("client threads", lambda: capture_process("client", client.pid))
                        diagnose("owner threads", lambda: capture_process("owner", int(status()["MainPID"])))
                    os.killpg(client.pid, signal.SIGTERM)
                    try:
                        client.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(client.pid, signal.SIGKILL)
                        client.wait(timeout=5)
                    if isinstance(error, subprocess.TimeoutExpired):
                        raise TimeoutError("qualification client timed out") from error
                    raise
                if code:
                    raise TargetError("qualification client failed: " + str(code))
            proof = json.loads((evidence / "native/qualification.json").read_text())
            checks = proof.get("checks", {})
            required = ("scalar_write_delete", "mixed_batch", "ordered_batch_missing_exists",
                        "checkpoint_iteration", "cache_miss_hit", "flush_reopen",
                        "abrupt_tail_recovery", "fence_write_protect", "target_reuse")
            if (proof.get("kind") != "live_nvmeof_qualification" or proof.get("schema_version") != 2
                    or proof.get("expected_route") != transport
                    or proof.get("durability_tier") != receipt["durability"]["tier"]
                    or any(checks.get(k) is not True for k in required)):
                raise TargetError("kernel qualification proof incomplete")
            reuse, lifecycles = _admit_kernel_reuse(proof, remote, addr, service, transport, launch["listen"])
            reprovisioned_database = reuse["database"]
            record["target_reuse"] = {key: reuse[key] for key in ("dropped_database", "database")}
            _admit_kernel_geometry(record, lifecycles, KERNEL_QUEUE_GEOMETRY)
            record["qualification_sha256"] = hashlib.sha256(
                (evidence / "native/qualification.json").read_bytes()).hexdigest()
            record["qualified"] = True
        except Exception as error:
            record["errors"].append({"phase": "qualification", "error": repr(error), "traceback": traceback.format_exc()})
        finally:
            try:
                if client is not None:
                    if client.poll() is None:
                        raise TargetError("client is still running")
                    try:
                        os.killpg(client.pid, 0)
                    except ProcessLookupError:
                        pass
                    else:
                        raise TargetError("client process group remains")
                if record["errors"]:
                    diagnose("nvme state", lambda: _write(evidence / "nvme-state.json", _nvme_state()))
                private = {}
                def private_identity(host_nqn, subsystem):
                    return (host_nqn in private
                            and cloud_campaign.scoped_live_subsystem(subsystem, private[host_nqn]))

                if captured:
                    private = _kernel_commissioning_identities(launch["host"]["root"], addr, transport, service, port_id)
                    cleanup_deadline = time.monotonic() + remaining(20)
                    deleted = _delete_owned_controllers(
                        SYSFS_NVME, transport, addr, (service,), _kernel_public_identity, cleanup_deadline)
                    if private:
                        deleted += _delete_owned_controllers(
                            SYSFS_NVME, "tcp", control_address, (commissioning,), private_identity, cleanup_deadline)
                    if deleted:
                        record["initiator_teardown"] = deleted
                        if record.pop("qualified", False):
                            record["errors"].append({"phase": "qualification", "error":
                                "client exited without deleting its initiator controllers: "
                                + ", ".join(deleted)})
                if attempted:
                    if not captured:
                        raise TargetError("owner identity unavailable; retained")
                    current = status()
                    if not owner._stopped(current):
                        Path(launch["stop_file"]).touch(exist_ok=False)
                        while True:
                            current = status()
                            if owner._stopped(current):
                                break
                            if current.get("ActiveState") in ("failed", "inactive"):
                                raise TargetError("owner did not exit gracefully")
                            time.sleep(min(.1, remaining(1)))
                    record["owner_shutdown"] = owner.admit_live_shutdown(
                        node, launch_path, launch, 0, current, expected_database=reprovisioned_database)
                    run(["systemctl", "stop", unit], remaining(20))
                    current = status()
                    if current.get("LoadState") != "not-found" and (
                            current.get("MainPID") != "0" or current.get("ActiveState") != "inactive"):
                        raise TargetError("owner unit remains active")
                if captured and (
                        _owned_controllers(SYSFS_NVME, transport, addr, (service,), _kernel_public_identity)
                        or private and _owned_controllers(
                            SYSFS_NVME, "tcp", control_address, (commissioning,), private_identity)):
                    raise TargetError("controller reappeared after owner shutdown")
                if attempted and any((NVMET_CONFIGFS / "ports" / str(port)).exists()
                                     for port in (service, commissioning)):
                    raise TargetError("configfs port remains after owner shutdown")
                claim.unlink()
                record["cleanup_complete"] = True
            except Exception as error:
                record["errors"].append({"phase": "cleanup", "error": repr(error), "traceback": traceback.format_exc()})
            if record["errors"]:
                diagnose("owner journal", lambda: (evidence / "owner-journal.txt").write_text(
                    _journal(["-u", unit, "-n", "2000"], 5)))
                diagnose("kernel journal", lambda: (evidence / "kernel-journal.txt").write_text(
                    _journal(["-k", "-n", "500"], 5)))
            _write(evidence / "proof.json", record)
        return record

    try:
        for tier in ("native", "synchronous"):
            record = tier_run(tier)
            overall["tiers"][tier] = record
            if not record["cleanup_complete"]:
                break
        overall["complete"] = len(overall["tiers"]) == 2 and all(
            r.get("qualified") and r["cleanup_complete"] for r in overall["tiers"].values())
    finally:
        if overall["tiers"] and all(r["cleanup_complete"] for r in overall["tiers"].values()):
            stat = root.lstat()
            if (stat.st_dev, stat.st_ino) != root_id or root.is_symlink():
                raise TargetError("root fixture identity changed; retained")
            shutil.rmtree(root)
            overall["fixture_removed"] = not root.exists()
        else:
            overall["fixture_removed"] = False
        _write(out / "proof.json", overall)
    return overall, out


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 7 or sys.argv[1] != "--kernel-live-worker":
        raise SystemExit("internal kernel live worker arguments required")
    result, evidence = _kernel_live_worker(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]),
                                           float(sys.argv[6]))
    print(json.dumps({"complete": result["complete"], "fixture_removed": result["fixture_removed"],
                      "binary_sha256": result["binary_sha256"], "evidence": str(evidence)}, sort_keys=True))
    if not result["complete"] or not result["fixture_removed"]:
        raise SystemExit(1)
