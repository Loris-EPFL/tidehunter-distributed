#!/usr/bin/env python3
"""Supervise the maintained kernel NVMe/TCP proofs.

The proofs own setup, admission, second-open recovery, owner layout and the
owner-restart reopen. This wrapper adds signal-to-finally delivery, evidence
and inventory receipts, not another NVMe lifecycle.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]

TRACING_ROOTS = (Path("/sys/kernel/tracing"), Path("/sys/kernel/debug/tracing"))
NVMET_EVENTS = ("nvmet_req_init", "nvmet_req_complete")
KPROBE = "p:nvmet_tcp_qr nvmet_tcp_queue_response req=%di"

# The campaign's three databases and two owner partitions. `deployment.prepare`
# gives every database process its own slot inside the partition's 64-port
# window, so six owners on one host must not share a target.
LAYOUT_OWNERS = 2
LAYOUT_NAMESPACES = ("live/checkpoints", "live/store/epoch_0", "live/store/perpetual")
LAYOUT_PORT = 24000
LAYOUT_SECONDS = 180
LAYOUT_SCOPE = "nqn.2026-01.local.tidehunter:db-"
CONFIGFS = Path("/sys/kernel/config/nvmet")
# Kernel controller class. The nvmeN ordinal comes from the kernel's
# lowest-free IDA and is recycled on delete, so it names a slot, not a
# controller; identity is the sysfs device/inode pair.
NVME_CONTROLLERS = Path("/sys/class/nvme")

# The owner-restart proof reuses the single-owner qualification geometry: 8 MiB
# WAL members in 4 MiB fragments, so the replay crosses both boundaries.
RESTART_WAL_FILE_BYTES = 8 * 1024**2
RESTART_FRAGMENT_BYTES = 4 * 1024**2
RESTART_FORBIDDEN_STDERR = ("pinned controller is not live", "short namespace read")


def write_text(path, value):
    path.write_text(value if value.endswith("\n") else value + "\n")


class NvmetTrace:
    """Failure-only nvmet trace for the single-owner abrupt-recovery proof.

    Two nvmet tracepoints plus one queue-response kprobe are armed for the
    single-owner run. Nothing is copied on success; on failure the buffer,
    event formats/listing and per-command counts go to the artifact directory.
    Diagnostic failures are themselves recorded and never mask the proof.
    """

    def __init__(self):
        self.root = None
        self.started = None
        self.errors = []

    def _record(self, error):
        self.errors.append(str(error))

    def _resolve_root(self):
        for candidate in TRACING_ROOTS:
            try:
                if (candidate / "events").is_dir():
                    return candidate
            except OSError as error:
                self._record(f"probe {candidate}: {error}")
        target = TRACING_ROOTS[0]
        try:
            target.mkdir(parents=True, exist_ok=True)
            subprocess.run(["mount", "-t", "tracefs", "tracefs", str(target)],
                           check=False, capture_output=True, text=True)
        except OSError as error:
            self._record(f"mount tracefs: {error}")
            return None
        try:
            return target if (target / "events").is_dir() else None
        except OSError as error:
            self._record(f"probe {target}: {error}")
            return None

    def arm(self):
        self.root = self._resolve_root()
        if self.root is None:
            self._record("tracefs unavailable; nvmet trace not armed")
            return
        try:
            write_text(self.root / "tracing_on", "0")
            for name, value in (("trace_clock", "mono"), ("buffer_size_kb", "4096")):
                try:
                    write_text(self.root / name, value)
                except OSError as error:
                    self._record(f"{name}: {error}")
            for event in NVMET_EVENTS:
                enable = self.root / "events" / "nvmet" / event / "enable"
                if enable.is_file():
                    try:
                        write_text(enable, "1")
                    except OSError as error:
                        self._record(f"enable {event}: {error}")
                else:
                    self._record(f"missing nvmet event {event}")
            try:
                with (self.root / "kprobe_events").open("a") as handle:
                    handle.write(KPROBE + "\n")
            except OSError as error:
                self._record(f"kprobe nvmet_tcp_queue_response: {error}")
            write_text(self.root / "tracing_on", "1")
            write_text(self.root / "trace_marker", "native-tier-start")
            self.started = time.monotonic()
        except OSError as error:
            self._record(f"arm: {error}")

    def disarm(self):
        if self.root is None:
            return
        for name, value in (("trace_marker", "native-tier-end"), ("tracing_on", "0")):
            try:
                write_text(self.root / name, value)
            except OSError as error:
                self._record(f"disarm {name}: {error}")

    def cleanup(self):
        if self.root is None:
            return
        for event in NVMET_EVENTS:
            enable = self.root / "events" / "nvmet" / event / "enable"
            if enable.is_file():
                try:
                    write_text(enable, "0")
                except OSError as error:
                    self._record(f"disable {event}: {error}")
        try:
            write_text(self.root / "kprobe_events", "-:nvmet_tcp_qr")
        except OSError as error:
            self._record(f"remove kprobe: {error}")

    @staticmethod
    def _pairs(line):
        return dict(re.findall(r"(\w+)=([^\s,]+)", line))

    def _counts(self, trace):
        completes = {}
        enqueues = {}
        for line in trace.splitlines():
            if "nvmet_req_complete:" in line:
                fields = self._pairs(line.split("nvmet_req_complete:", 1)[1])
                key = (fields.get("ctrl_id"), fields.get("qid"), fields.get("cid"))
                completes[key] = completes.get(key, 0) + 1
            elif "nvmet_tcp_qr:" in line:
                fields = self._pairs(line.split("nvmet_tcp_qr:", 1)[1])
                request = fields.get("req")
                if request is not None:
                    enqueues[request] = enqueues.get(request, 0) + 1
        return completes, enqueues

    def capture_failure(self, directory):
        """Copy the armed trace. Never raises: the proof's own verdict wins."""
        try:
            directory.mkdir(parents=True, exist_ok=True)
            trace = ""
            if self.root is not None:
                try:
                    trace = (self.root / "trace").read_text()
                except OSError as error:
                    self._record(f"read trace: {error}")
            write_text(directory / "trace.txt", trace)
            write_text(directory / "trace-errors.txt", "\n".join(self.errors))
            try:
                udev = Path("/run/udev/queue")
                write_text(directory / "udev-queue.txt",
                           "present" if udev.is_symlink() else "absent")
            except OSError as error:
                write_text(directory / "udev-queue.txt", f"unavailable: {error}")
            if self.started is not None:
                write_text(directory / "tracing-seconds.txt",
                           f"{time.monotonic() - self.started:.3f}")
            if self.root is not None:
                for source, name in (
                    ("kprobe_events", "kprobe_events.txt"),
                    ("events/nvmet/nvmet_req_complete/format", "complete-format.txt"),
                ):
                    try:
                        text = (self.root / source).read_text()
                    except OSError as error:
                        text = f"unavailable: {error}"
                    write_text(directory / name, text)
                try:
                    events = sorted(
                        path.name for path in (self.root / "events").iterdir()
                        if path.name.startswith("nvme"))
                except OSError:
                    events = []
                write_text(directory / "event-list.txt", "\n".join(events))
            completes, enqueues = self._counts(trace)
            complete_counts = {"|".join(str(part) for part in key): count
                               for key, count in completes.items()}
            duplicates = {key: count for key, count in complete_counts.items() if count > 1}
            dump(directory / "complete-counts.json", complete_counts)
            dump(directory / "duplicates.json", duplicates)
            dump(directory / "enqueue-counts.json", enqueues)
        except OSError as error:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                write_text(directory / "trace-errors.txt", f"capture: {error}")
            except OSError:
                pass


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def inventory():
    config = Path("/sys/kernel/config/nvmet")
    controllers = {}
    for path in Path("/sys/class/nvme").glob("nvme[0-9]*"):
        controllers[path.name] = {name: (path / name).read_text().strip()
                                  for name in ("subsysnqn", "transport")}
    return {"subsystems": sorted(p.name for p in (config / "subsystems").iterdir()),
            "ports": sorted(p.name for p in (config / "ports").iterdir()),
            "controllers": controllers}


def campaign_modules():
    """The Sui deployment and stage-3 spec builders of this checkout."""
    import importlib.util
    for directory in (ROOT / "experiments", ROOT / "experiments/sui-checkpoint"):
        if str(directory) not in sys.path:
            sys.path.insert(0, str(directory))
    loaded = {}
    for name, relative in (("owner_layout_deployment",
                            "experiments/sui-checkpoint/deployment.py"),
                           ("owner_layout_specs",
                            "experiments/sui-checkpoint/stage3_specs.py")):
        spec = importlib.util.spec_from_file_location(name, ROOT / relative)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        loaded[name] = module
    return loaded["owner_layout_deployment"], loaded["owner_layout_specs"]


def owner_launch(node):
    import yaml
    return yaml.safe_load(Path(node["launch"]).read_text())


def start_owner(binary, node, log):
    """Start one generated owner process; its ready file gates the next step."""
    with open(log, "ab") as stream:
        return subprocess.Popen(
            [str(binary), "--experimental-storage-node", str(node["launch"])],
            stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
            start_new_session=True)


def owner_name(node):
    return str(node["namespace"]) + "/" + str(node["partition"])


def wait_owners(nodes, processes, timeout=LAYOUT_SECONDS):
    """Wait until every owner published its ready file, or report the failure."""
    deadline = time.monotonic() + timeout
    while True:
        for node in nodes:
            process = processes[owner_name(node)]
            if process.poll() is not None:
                raise RuntimeError(f"{owner_name(node)}: owner exited "
                                   f"{process.returncode} before readiness")
        missing = [owner_name(node) for node in nodes
                   if not Path(node["ready"]).is_file()]
        if not missing:
            return
        if time.monotonic() > deadline:
            raise RuntimeError("owner readiness expired: " + ", ".join(missing))
        time.sleep(0.1)


def stop_owner(process, node, timeout=LAYOUT_SECONDS):
    """The engine's own stop: the stop file makes the owner drain its targets."""
    Path(node["stop"]).write_text("stop\n")
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=30)
    Path(node["stop"]).unlink(missing_ok=True)
    return process.returncode


def kill_owner(process, timeout=60):
    process.kill()
    process.wait(timeout=timeout)
    return process.returncode


def copy_receipts(nodes, specs, root, namespace, owners=LAYOUT_OWNERS):
    """Put the selected database's owner receipts where its spec names them.

    One receipt per partition names that partition's control address and
    process, so the receipt of another database's owner on the same host does
    not confirm this database. `owners` follows the prepared database, so a
    one-owner proof never looks for a second receipt.
    """
    for partition in range(owners):
        selected = [node for node in nodes if node["partition"] == partition
                    and node["namespace"] == namespace]
        if len(selected) != 1:
            raise RuntimeError(f"{namespace} partition {partition}: expected one owner")
        published = Path(str(selected[0]["ready"]) + ".durability.json")
        target = specs.receipt_path(Path(root), "native", partition)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(published.read_bytes())


def layout_spec(specs, document, root, index, *, reopen):
    """One kv-research run over one prepared database of the deployment."""
    case = dict(case_id=f"owner-layout-{index}" + ("-reopen" if reopen else ""),
                arm="A", mix="write" if reopen else "mixed", size=256,
                distribution="uniform", tier="native", concurrency=2,
                batch=None, cache=0, reference=False)
    # One complete write batch after the restart, so both owners of the
    # database publish their namespace.
    # No driver writes measurement releases here, so the run is not gated.
    spec = specs.run_config(case, Path(root), external={"native": {"document": document}},
                            measurement_operations=26 if reopen else 8, gated=False)
    spec["dataset"]["keys"] = 256
    if reopen:
        # The maintained second-open proof reuses the coordinator path: the
        # reopened coordinator recovers the authority the owners hold. Its live
        # writes publish the namespace again after the owner restart, because
        # setup creates only ports and hosts.
        spec["namespace"] = f"stage3-owner-layout-{index}"
        spec["backend"]["options"]["path"] = str(
            Path(root) / "stage3" / "db" / f"owner-layout-{index}" / "coordinator")
        spec["phases"].update(preload=False, warmup_operations=0)
    return spec


def negative_spec(specs, document, root, index, receipt):
    """One single-owner run of the negative case.

    The endpoint and receipt counts follow the selected database's own
    partition list; the shared builder's campaign owner count would name two
    endpoints that this deployment does not have.
    """
    spec = layout_spec(specs, document, root, index, reopen=False)
    owners = len(document["storage"]["partitions"])
    if owners != 1 or len(document.get("endpoints") or []) != owners:
        raise RuntimeError(f"a negative owner needs one partition, found {owners}")
    spec["backend"]["options"].update(
        owners=owners, external_owner_durability_receipts=[str(receipt)])
    return spec


def run_coordinator(binary, spec, root, name, timeout=240):
    """Open one prepared database: provision, export, and close with its seal."""
    path = Path(root) / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(spec, indent=2, sort_keys=True) + "\n")
    output = Path(root) / f"{name}-out"
    if output.exists():
        raise RuntimeError(f"{output}: coordinator output is not fresh")
    completed = subprocess.run(
        [str(binary), "--config", str(path), "--output", str(output)],
        capture_output=True, text=True, timeout=timeout)
    return completed, path, output


def coordinator_error(completed, name):
    if completed.returncode == 0:
        return None
    detail = (completed.stderr or completed.stdout).strip()[-3000:]
    return f"{name}: coordinator exited {completed.returncode}: {detail}"


def database_scope(nodes):
    """The one database identity each generated owner root holds."""
    scopes = {}
    for node in nodes:
        root = Path(owner_launch(node)["host"]["root"])
        databases = sorted(path.name for path in root.glob("db-*"))
        if len(databases) != 1:
            raise RuntimeError(f"{root}: expected one database root, found {databases}")
        scopes[owner_name(node)] = databases[0][3:]
    return scopes


def layout_defect(nodes, scopes):
    """Every target port holds its own database scope and nothing else."""
    expected = {}
    for node in nodes:
        pool = owner_launch(node)["host"]["live_target_pool"]
        expected[pool["public_port_base"]] = (
            f"{LAYOUT_SCOPE}{scopes[owner_name(node)]}-p{node['partition']:08x}-")
    ports = CONFIGFS / "ports"
    observed = {}
    for port in sorted(ports.iterdir(), key=lambda path: path.name):
        links = sorted(link.name for link in (port / "subsystems").iterdir())
        if not links:
            continue
        observed[int(port.name)] = links
    missing = sorted(set(expected) - set(observed))
    if missing:
        return f"target ports published no subsystem: {missing}"
    foreign = sorted(set(observed) - set(expected))
    if foreign:
        return f"unexpected published target ports: {foreign}"
    for port_id, links in observed.items():
        for link in links:
            if not link.startswith(expected[port_id]):
                return (f"port {port_id} holds another database's subsystem: {link} "
                        f"(expected the {expected[port_id]} scope)")
    return None


def layout_teardown_defect(scopes):
    """A stopped layout owns no scoped configfs object."""
    residual = []
    for scope in sorted(scopes):
        prefix = f"{LAYOUT_SCOPE}{scope}-p"
        for subsystem in (CONFIGFS / "subsystems").iterdir():
            if subsystem.name.startswith(prefix):
                residual.append(str(subsystem))
        for host in (CONFIGFS / "hosts").iterdir():
            if host.name.startswith(prefix):
                residual.append(str(host))
    for port in (CONFIGFS / "ports").iterdir():
        for link in (port / "subsystems").iterdir():
            if any(link.name.startswith(f"{LAYOUT_SCOPE}{scope}-p") for scope in scopes):
                residual.append(str(link))
    return residual


def layout_positive(deployment, specs, binary, root):
    """Three databases times two owners: export, seal, reopen and reconcile."""
    root = Path(root)
    root.mkdir(mode=0o700)
    schema = {name: {"key_spaces": []} for name in LAYOUT_NAMESPACES}
    config, nodes = deployment.prepare(
        root / "deployment", schema, root / "remote",
        shape_yamls={name: "key_spaces: []\n" for name in schema},
        hosts=["127.0.0.1"] * LAYOUT_OWNERS, owners=LAYOUT_OWNERS,
        port=LAYOUT_PORT, transport="tcp", coordinator_cache=0,
        live_transport="tcp")
    ports = [owner_launch(node)["host"]["live_target_pool"]["public_port_base"]
             for node in nodes]
    if len(set(ports)) != len(ports):
        raise RuntimeError(f"prepared owners share a target port: {sorted(ports)}")
    processes = {}
    nodes_of = {owner_name(node): node for node in nodes}
    try:
        for node in nodes:
            log = root / (owner_name(node).replace("/", "-") + ".log")
            processes[owner_name(node)] = start_owner(binary, node, log)
        wait_owners(nodes, processes)
        # Provision every database: export on its own port, then seal.
        for index, name in enumerate(LAYOUT_NAMESPACES):
            copy_receipts(nodes, specs, root, name)
            label = f"owner-layout-{index}"
            completed, _path, _output = run_coordinator(
                binary, layout_spec(specs, config["databases"][name], root, index,
                                    reopen=False), root, label)
            error = coordinator_error(completed, label)
            if error is not None:
                raise RuntimeError(error)
        # A kill leaves every scoped object behind. Only a layout that gives
        # each database its own port lets all six reopens reconcile it.
        for node in nodes:
            kill_owner(processes[owner_name(node)])
        processes = {}
        for node in nodes:
            # The ready and receipt files of the killed incarnation must not
            # satisfy the restart: the new process republishes both.
            Path(node["ready"]).unlink(missing_ok=True)
            Path(str(node["ready"]) + ".durability.json").unlink(missing_ok=True)
            log = root / (owner_name(node).replace("/", "-") + ".log")
            processes[owner_name(node)] = start_owner(binary, node, log)
        wait_owners(nodes, processes)
        # Startup reconcile removes the scoped objects and setup creates only
        # ports and hosts. The second coordinator's live writes publish each
        # namespace again on its own port; only then are the links inspected.
        for index, name in enumerate(LAYOUT_NAMESPACES):
            copy_receipts(nodes, specs, root, name)
            label = f"owner-layout-{index}-reopen"
            completed, _path, _output = run_coordinator(
                binary, layout_spec(specs, config["databases"][name], root, index,
                                    reopen=True), root, label)
            error = coordinator_error(completed, label)
            if error is not None:
                raise RuntimeError(error)
        scopes = database_scope(nodes)
        defect = layout_defect(nodes, scopes)
        if defect is not None:
            raise RuntimeError(defect)
        for node in nodes:
            code = stop_owner(processes[owner_name(node)], node)
            if code != 0:
                raise RuntimeError(f"{owner_name(node)}: owner stop exited {code}")
        processes = {}
        residual = layout_teardown_defect(scopes.values())
        if residual:
            raise RuntimeError("stopped layout left configfs objects: " + ", ".join(residual))
        return {"complete": True, "owners": len(nodes), "databases": len(LAYOUT_NAMESPACES),
                "ports": {str(port): scope for port, scope
                          in sorted(layout_observed(nodes, scopes).items())}}
    finally:
        for name, process in processes.items():
            if process.poll() is not None:
                continue
            try:
                stop_owner(process, nodes_of[name], timeout=20)
            except BaseException:
                process.kill()
                process.wait(timeout=30)


def layout_observed(nodes, scopes):
    """The published port of every generated owner, for the receipt."""
    observed = {}
    for node in nodes:
        pool = owner_launch(node)["host"]["live_target_pool"]
        observed[pool["public_port_base"]] = (
            f"{scopes[owner_name(node)]}-p{node['partition']:08x}")
    return observed


def restart_geometry(document):
    """The CI WAL geometry of the owner-restart proof: 8 MiB in 4 MiB fragments."""
    storage = document["storage"]
    storage["fragment_bytes"] = RESTART_FRAGMENT_BYTES
    storage["wal_file_bytes"] = RESTART_WAL_FILE_BYTES
    if storage["wal_file_bytes"] % storage["fragment_bytes"]:
        raise RuntimeError("restart WAL member size is not a fragment multiple")
    return document


def restart_spec(specs, document, root, *, case_id, coordinator, gated, reopen):
    """One write-only coordinator over the restart proof's database.

    The killed run stops at its measurement gate with the preloaded batches
    still unsealed in the owner WAL. The reopen uses the same coordinator root,
    writes nothing before its replay, and takes no gate because no driver
    releases one in CI.
    """
    case = dict(case_id=case_id, arm="A", mix="write", size=256,
                distribution="uniform", tier="native", concurrency=1,
                batch=None, cache=0, reference=False)
    spec = specs.run_config(case, Path(root), external={"native": {"document": document}},
                            measurement_operations=8, gated=gated)
    spec["dataset"]["keys"] = 256
    options = spec["backend"]["options"]
    # The prepared database has one partition; the shared builder defaults to
    # the campaign's two owners and would name a receipt that does not exist.
    options["owners"] = 1
    options["external_owner_durability_receipts"] = [
        str(specs.receipt_path(Path(root), "native", 0))]
    options["path"] = str(coordinator)
    spec["namespace"] = f"stage3-{case_id}"
    if reopen:
        spec["phases"].update(preload=False, warmup_operations=0)
        spec["phases"]["measurement_operations"] = 16
    return spec


def controller_identities(document, scope, partition):
    """The sysfs identity of one database's kernel controllers on this host.

    A recycled `nvmeN` ordinal is not identity: the new controller can reuse
    the slot the retired one freed. The device/inode pair of the controller
    directory names one kernel object.
    """
    prefix = f"{LAYOUT_SCOPE}{scope}-p{partition:08x}-"
    controllers = document.get("controllers") or {}
    identities = []
    for name in sorted(controllers):
        if not str((controllers[name] or {}).get("subsysnqn", "")).startswith(prefix):
            continue
        try:
            meta = (NVME_CONTROLLERS / name).stat()
        except OSError:
            # A controller the kernel is still creating or deleting has no
            # stable stat; it is not evidence of a live pre-restart object.
            continue
        identities.append({"controller": name, "sysfs_device": meta.st_dev,
                           "sysfs_inode": meta.st_ino})
    return identities


def controller_key(entity):
    """The (device, inode) pair of a controller identity, or None."""
    device = entity.get("sysfs_device")
    inode = entity.get("sysfs_inode")
    if type(device) is int and type(inode) is int:
        return (device, inode)
    return None


def reopen_defect(evidence, scope, partition, previous_controllers,
                  present_before_reopen, stderr):
    """The reason a restart reopen is unproven, or None when it is proven.

    `evidence` is the reopened coordinator's `measurement-read-evidence.json`.
    `previous_controllers` are the scoped sysfs identities captured before the
    coordinator was killed; `present_before_reopen` are those that still
    existed when the reopen started, which is what makes the check exercise
    the old adoption path rather than a controller the kernel already dropped.
    """
    start = evidence.get("start") if isinstance(evidence, dict) else None
    if not isinstance(start, dict):
        return "the reopened coordinator wrote no measurement start boundary"
    opened = start.get("coordinator_open") or {}
    if opened.get("kind") != "reopen":
        return f"the coordinator open kind is {opened.get('kind')!r}, not 'reopen'"
    recovery = opened.get("recovery") or {}
    tail = recovery.get("tail_batches")
    if type(tail) is not int or tail <= 0:
        return f"the reopen replayed no WAL batch (tail_batches={tail!r})"
    previous = {key for key in map(controller_key, previous_controllers) if key}
    present = {key for key in map(controller_key, present_before_reopen) if key}
    if not previous & present:
        return ("no pre-restart controller remained when the reopen started; "
                "the check cannot prove recovery retired it")
    prefix = f"{LAYOUT_SCOPE}{scope}-p{partition:08x}-"
    reopened = []
    for route in (start.get("live_nvmeof") or {}).get("routes") or []:
        identity = route.get("namespace_identity") or {}
        if not str(identity.get("nqn", "")).startswith(prefix):
            continue
        key = controller_key(identity.get("controller") or {})
        if key is not None:
            reopened.append(key)
    if not reopened:
        return "the reopen recorded no live controller identity for its database"
    stale = sorted(set(reopened) & previous)
    if stale:
        return f"the reopen reused the pre-restart controller {stale}"
    for marker in RESTART_FORBIDDEN_STDERR:
        if marker in stderr:
            return f"the reopen stderr names {marker!r}"
    return None


def start_coordinator(binary, spec_path, output, stdout_path, stderr_path):
    """Start one coordinator whose write gate the caller waits for."""
    with stdout_path.open("wb") as out, stderr_path.open("wb") as err:
        return subprocess.Popen(
            [str(binary), "--config", str(spec_path), "--output", str(output)],
            stdin=subprocess.DEVNULL, stdout=out, stderr=err, start_new_session=True)


def restart_positive(deployment, specs, binary, root):
    """Kill a coordinator at its write gate, restart its owner, reopen the root.

    The killed coordinator leaves its kernel controller and durable intent on
    the coordinator host. The restarted owner replaces the epoch-scoped target,
    so a recovery read that adopts the leftover controller binds a dead
    subsystem. The reopen must instead retire it, connect a new controller and
    read a non-empty replay page.
    """
    root = Path(root)
    root.mkdir(mode=0o700)
    namespace = LAYOUT_NAMESPACES[0]
    config, nodes = deployment.prepare(
        root / "deployment", {namespace: {"key_spaces": []}}, root / "remote",
        shape_yamls={namespace: "key_spaces: []\n"},
        hosts=["127.0.0.1"], owners=1, port=LAYOUT_PORT, transport="tcp",
        coordinator_cache=0, live_transport="tcp")
    if len(nodes) != 1:
        raise RuntimeError(f"the restart proof needs one owner, found {len(nodes)}")
    node = nodes[0]
    document = restart_geometry(config["databases"][namespace])
    coordinator = root / "stage3" / "db" / "owner-restart" / "coordinator"
    killed = restart_spec(specs, document, root, case_id="owner-restart",
                          coordinator=coordinator, gated=True, reopen=False)
    reopened = restart_spec(specs, document, root, case_id="owner-restart-reopen",
                            coordinator=coordinator, gated=False, reopen=True)
    gate = Path(killed["measurement_gate"]["ready_file"])
    # The binary refuses a measurement gate whose parent directory is absent.
    gate.parent.mkdir(parents=True, exist_ok=True)
    processes = {}
    first = None
    evidence = {"schema": 1, "kind": "owner_restart_reopen", "complete": False,
                "wal_file_bytes": RESTART_WAL_FILE_BYTES}
    try:
        log = root / "owner.log"
        processes[owner_name(node)] = start_owner(binary, node, log)
        wait_owners([node], processes)
        copy_receipts(nodes, specs, root, namespace, owners=1)
        # Run the coordinator until its write gate. The preloaded batches are
        # in the owner's WAL with no checkpoint and no seal.
        spec_path = root / "owner-restart.json"
        spec_path.write_text(json.dumps(killed, indent=2, sort_keys=True) + "\n")
        first_out = root / "owner-restart-out"
        if first_out.exists():
            raise RuntimeError(f"{first_out}: coordinator output is not fresh")
        first = start_coordinator(binary, spec_path, first_out,
                                  root / "owner-restart.stdout",
                                  root / "owner-restart.stderr")
        deadline = time.monotonic() + LAYOUT_SECONDS
        while not gate.is_file():
            if first.poll() is not None:
                raise RuntimeError(f"the coordinator exited {first.returncode} "
                                   "before its write gate")
            if time.monotonic() > deadline:
                raise RuntimeError("the coordinator write gate expired")
            time.sleep(0.1)
        scope = database_scope([node])[owner_name(node)]
        previous = controller_identities(inventory(), scope, node["partition"])
        if not previous:
            raise RuntimeError(f"the killed coordinator holds no controller for {scope}")
        evidence["previous_controllers"] = previous
        # No pool shutdown, no checkpoint, no seal: a real abrupt coordinator exit.
        os.killpg(first.pid, signal.SIGKILL)
        first.wait(timeout=60)
        evidence["killed_returncode"] = first.returncode
        first = None
        # The restart is a new owner incarnation: the old target and its
        # controllers no longer serve, and the ready/receipt files are stale.
        kill_owner(processes.pop(owner_name(node)))
        Path(node["ready"]).unlink(missing_ok=True)
        Path(str(node["ready"]) + ".durability.json").unlink(missing_ok=True)
        processes[owner_name(node)] = start_owner(binary, node, log)
        wait_owners([node], processes)
        copy_receipts(nodes, specs, root, namespace, owners=1)
        # The replay must find the pre-restart controller still present; if the
        # kernel already dropped it, the check cannot prove it was retired.
        present = controller_identities(inventory(), scope, node["partition"])
        # Reopen the same coordinator root: recovery must replay, not adopt.
        completed, _path, output = run_coordinator(
            binary, reopened, root, "owner-restart-reopen")
        stderr = (completed.stderr or "")
        (root / "owner-restart-reopen.stderr").write_text(stderr)
        evidence["reopen_returncode"] = completed.returncode
        raise_for = coordinator_error(completed, "owner-restart-reopen")
        if raise_for is not None:
            raise RuntimeError(raise_for)
        evidence_path = output / "measurement-read-evidence.json"
        try:
            read_evidence = json.loads(evidence_path.read_text())
        except (OSError, ValueError) as error:
            raise RuntimeError(f"the reopen wrote no read evidence: {error}") from error
        defect = reopen_defect(read_evidence, scope, node["partition"], previous,
                               present, stderr)
        if defect is not None:
            raise RuntimeError(defect)
        evidence["tail_batches"] = (
            read_evidence["start"]["coordinator_open"]["recovery"]["tail_batches"])
        remaining = controller_identities(inventory(), scope, node["partition"])
        if remaining:
            raise RuntimeError("the reopen left pre-restart controllers: "
                               + ", ".join(row["controller"] for row in remaining))
        code = stop_owner(processes.pop(owner_name(node)), node)
        if code != 0:
            raise RuntimeError(f"the restarted owner stop exited {code}")
        residual = layout_teardown_defect([scope])
        if residual:
            raise RuntimeError("the restart proof left configfs objects: "
                               + ", ".join(residual))
        evidence["complete"] = True
        return evidence
    finally:
        # A coordinator that is still running owns the database lock and its
        # controllers; it must not outlive a failed proof into the next step.
        if first is not None and first.poll() is None:
            try:
                first.kill()
                first.wait(timeout=30)
            except BaseException:
                pass
        for process in processes.values():
            if process.poll() is not None:
                continue
            try:
                stop_owner(process, node, timeout=20)
            except BaseException:
                process.kill()
                process.wait(timeout=30)


def layout_negative(deployment, specs, binary, root):
    """Two owners, one pool base, two databases: the second must refuse."""
    root = Path(root)
    root.mkdir(mode=0o700)
    prepared = {}
    for name, control in (("a", LAYOUT_PORT), ("b", LAYOUT_PORT + 10)):
        namespace = LAYOUT_NAMESPACES[0] if name == "a" else LAYOUT_NAMESPACES[1]
        config, nodes = deployment.prepare(
            root / name, {namespace: {"key_spaces": []}}, root / ("remote-" + name),
            shape_yamls={namespace: "key_spaces: []\n"},
            hosts=["127.0.0.1"], owners=1, port=control, transport="tcp",
            coordinator_cache=0, live_transport="tcp")
        prepared[name] = dict(namespace=namespace, config=config, node=nodes[0])
    # The negative case is the old layout: the second owner's launch names the
    # first owner's pool base with its own root and control port.
    first = owner_launch(prepared["a"]["node"])
    launch = owner_launch(prepared["b"]["node"])
    launch["host"]["live_target_pool"] = dict(first["host"]["live_target_pool"])
    import yaml
    Path(prepared["b"]["node"]["launch"]).write_text(yaml.safe_dump(launch, sort_keys=False))
    processes = {}
    try:
        for name in ("a", "b"):
            node = prepared[name]["node"]
            log = root / f"negative-{name}.log"
            processes[name] = start_owner(binary, node, log)
            wait_owners([node], {owner_name(node): processes[name]})
            published = str(node["ready"]) + ".durability.json"
            target = specs.receipt_path(root / ("negative-" + name), "native", 0)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(Path(published).read_bytes())
        spec = negative_spec(
            specs, prepared["a"]["config"]["databases"][prepared["a"]["namespace"]],
            root / "negative-a", 0,
            specs.receipt_path(root / "negative-a", "native", 0))
        completed, _path, _output = run_coordinator(binary, spec, root / "negative-a", "a")
        error = coordinator_error(completed, "a")
        if error is not None:
            raise RuntimeError("the first owner of the negative case failed: " + error)
        ports = database_scope([prepared["a"]["node"]])
        base = first["host"]["live_target_pool"]["public_port_base"]
        published = CONFIGFS / "ports" / str(base) / "subsystems"
        if not published.is_dir():
            raise RuntimeError(f"the first owner exported no subsystem on port {base}")
        if not sorted(link.name for link in published.iterdir()):
            raise RuntimeError(f"the first owner exported no subsystem on port {base}")
        spec = negative_spec(
            specs, prepared["b"]["config"]["databases"][prepared["b"]["namespace"]],
            root / "negative-b", 1,
            specs.receipt_path(root / "negative-b", "native", 0))
        completed, _path, _output = run_coordinator(binary, spec, root / "negative-b", "b")
        output = (completed.stdout or "") + (completed.stderr or "")
        if completed.returncode == 0:
            raise RuntimeError("the second owner shared a configfs port without a refusal")
        if "refusing to share configfs object" not in output:
            raise RuntimeError("the second owner failed for another reason: "
                               + output.strip()[-3000:])
        # The refused owner never created an object; the first owner's stop is
        # the engine's own teardown of its scoped target.
        code = stop_owner(processes.pop("a"), prepared["a"]["node"])
        if code != 0:
            raise RuntimeError(f"the negative first owner stop exited {code}")
        kill_owner(processes.pop("b"))
        residual = layout_teardown_defect(ports.values())
        if residual:
            raise RuntimeError("the negative layout left configfs objects: "
                               + ", ".join(residual))
        return {"complete": True, "refused": "refusing to share configfs object",
                "port": base, "database": ports[owner_name(prepared["a"]["node"])]}
    finally:
        for process in processes.values():
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=30)

def read_proof(state, name):
    """Return (proof, None) or (None, reason) for one proof file."""
    path = state / name / "proof.json"
    try:
        proof = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return None, f"{name} proof missing or unreadable: {error}"
    if not isinstance(proof, dict):
        return None, f"{name} proof is not a JSON object"
    return proof, None


def proof_defect(state, name):
    """Return the reason a proof is unclean, or None when it is clean."""
    proof, defect = read_proof(state, name)
    if defect is not None:
        return defect
    for key in ("complete", "cleanup_complete", "fixture_removed"):
        if proof.get(key) is not True:
            return f"{name} proof {key} is not true"
    errors = proof.get("errors")
    if not isinstance(errors, list) or errors:
        return f"{name} proof errors is not an empty list"
    return None


def teardown_defect(state, name):
    """Return the reason a proof's teardown is unconfirmed, or None when it is clean.

    The gating run() also requires `complete` and an empty `errors` list. The
    advisory cleanup action owns teardown only, so an admission or benchmark
    failure must not appear as a cleanup defect.
    """
    proof, defect = read_proof(state, name)
    if defect is not None:
        return defect
    for key in ("cleanup_complete", "fixture_removed"):
        if proof.get(key) is not True:
            return f"{name} proof {key} is not true"
    return None


def interrupted(signum, _frame):
    # The maintained proof catches this exception and executes stop_tide in finally.
    # Do not interrupt that drain a second time.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    raise RuntimeError(f"CI proof interrupted by signal {signum}")


class ControllerLossSupervisor(threading.Thread):
    """Delete the active write controller each tier's proof asks for.

    The qualification proof writes `controller-loss-ready.json` naming its own
    process, the controller, that controller's sysfs inode and its namespace.
    This thread answers every request once: it re-proves the inode and the
    coordinator's namespace descriptor, writes `delete_controller`, waits for
    the sysfs entry to disappear, and records the injection next to the
    request. A request that does not describe a live controller, or a
    controller that does not disappear, is an error, never a pass.
    """

    def __init__(self, root, deadline, *, poll=0.1):
        super().__init__(name="controller-loss-supervisor", daemon=True)
        self.root = Path(root)
        self.deadline = deadline
        self.poll = poll
        self.stop_event = threading.Event()
        self.records = []
        self.error = None

    def stop(self):
        self.stop_event.set()

    def run(self):
        seen = set()
        try:
            while not self.stop_event.wait(self.poll):
                for request in sorted(self.root.rglob("controller-loss-ready.json")):
                    if request in seen:
                        continue
                    seen.add(request)
                    self.records.append(inject_controller_loss(request))
                if time.monotonic() >= self.deadline:
                    break
        except BaseException as error:  # reported by the caller, never masked
            self.error = error


def inject_controller_loss(request_path, *, controller_root=Path("/sys/class/nvme"),
                           proc_root=Path("/proc"), delete=None, wait_seconds=30.0):
    """Delete one requested controller after proving it serves this write path.

    The roots and the delete action are injectable so a test drives the whole
    decision without a kernel controller. `delete` receives the controller's
    sysfs directory and must remove it, as the kernel does.
    """
    request = json.loads(Path(request_path).read_text())
    target = request.get("target") or {}
    controller = target.get("controller")
    inode = target.get("sysfs_inode")
    namespace = target.get("namespace_path")
    pid = request.get("pid")
    nqn = target.get("nqn")
    if request.get("kind") != "w1_controller_loss_request":
        raise RuntimeError(f"unexpected controller-loss request kind: {request.get('kind')!r}")
    if not (isinstance(controller, str) and re.fullmatch(r"nvme[0-9]+", controller)):
        raise RuntimeError(f"invalid controller name: {controller!r}")
    if target.get("state") != "live":
        raise RuntimeError(f"the requested controller is not live: {target.get('state')!r}")
    if not (isinstance(inode, int) and isinstance(pid, int) and pid > 0
            and isinstance(namespace, str)):
        raise RuntimeError("incomplete controller-loss request identity")
    path = Path(controller_root) / controller
    live_inode = path.stat().st_ino
    if live_inode != inode:
        raise RuntimeError("the requested controller inode differs from the live one")
    descriptor_dir = Path(proc_root) / str(pid) / "fd"
    try:
        descriptors = list(descriptor_dir.iterdir())
    except OSError as error:
        raise RuntimeError(
            'the coordinator holds no descriptor on the requested namespace: '
            f'{error}') from error
    held = []
    for descriptor in descriptors:
        try:
            if os.readlink(descriptor) == namespace:
                held.append(descriptor.name)
        except OSError:
            continue
    if not held:
        raise RuntimeError("the coordinator holds no descriptor on the requested namespace")
    evidence = {"schema": 1, "kind": "w1_controller_loss_injection",
                "requested": True, "injected": False,
                "controller": controller, "sysfs_inode": inode, "nqn": nqn,
                "namespace_path": namespace, "coordinator_pid": pid,
                "coordinator_descriptors": sorted(held),
                "request": str(request_path)}
    started = time.monotonic()
    if delete is None:
        (path / "delete_controller").write_text("1\n")
    else:
        delete(path)
    evidence["delete_issued"] = True
    deadline = started + wait_seconds
    while path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    evidence["removed"] = not path.exists()
    evidence["removed_seconds"] = time.monotonic() - started
    evidence["injected"] = evidence["removed"]
    Path(request_path).with_name("controller-loss-injection.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    if not evidence["removed"]:
        raise RuntimeError(f"controller {controller} did not disappear after delete_controller")
    return evidence


def run(args):
    sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
    import local_multiowner_check
    import local_dry_run_targets

    before = inventory()
    dump(args.state / "before.json", before)
    dump(args.state / "host.json", {"kernel": os.uname().release,
         "cpu_count": os.cpu_count(), "meminfo": Path("/proc/meminfo").read_text(),
         "disk_free_bytes": shutil.disk_usage(args.state).free,
         "route": "kernel NVMe/TCP; four colocated owners; one host/shared filesystem; no RDMA"})
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    # Owners prewrite by default, so the gating file pass opts out explicitly.
    sys.argv = [str(local_multiowner_check.__file__), "--binary", str(args.binary.resolve(strict=True)),
                "--libs", str(args.state / "libs"), "--owners", "4",
                "--output", str(args.state / "proof"), "--no-wal-prewrite"]
    result = 1
    passed = False
    defect = None
    final = None
    # sudo already runs this wrapper in the hosted runner's host namespace.
    # The local development Docker image is not installed on hosted runners.
    original_root = local_dry_run_targets.ROOT
    local_dry_run_targets.ROOT = []
    try:
        result = local_multiowner_check.main()
        defect = proof_defect(args.state, "proof")
        prewrite = {"attempted": False}
        if defect is None and inventory() == before:
            prewrite["attempted"] = True
            sys.argv = [str(local_multiowner_check.__file__), "--binary", str(args.binary.resolve(strict=True)),
                        "--libs", str(args.state / "libs"), "--owners", "4",
                        "--output", str(args.state / "proof-prewrite"), "--wal-prewrite"]
            try:
                prewrite["exit_code"] = local_multiowner_check.main()
            except (Exception, SystemExit) as error:
                prewrite["error"] = str(error)
            prewrite["passed"] = prewrite.get("exit_code") == 0
            defect = proof_defect(args.state, "proof-prewrite")
        elif defect is None:
            defect = "first pass left the host inventory changed"
            prewrite["reason"] = "first pass cleanup unconfirmed; second pass is unsafe"
        else:
            prewrite["reason"] = "first pass proof is unclean; second pass is unsafe"
        final = inventory()
        if defect is None and final != before:
            defect = "last pass left the host inventory changed"
        prewrite["defect"] = defect
        dump(args.state / "prewrite-result.json", prewrite)
        # Both passes carry reviewed evidence and the last pass must restore the
        # exact host inventory, so each pass gates the step.
        passed = (result == 0 and prewrite.get("attempted") is True
                  and prewrite.get("passed") is True and defect is None
                  and final == before)
        return 0 if passed else 1
    finally:
        local_dry_run_targets.ROOT = original_root
        final = inventory() if final is None else final
        dump(args.state / "after.json", final)
        dump(args.state / "result.json", {"exit_code": 0 if passed else 1, "passed": passed})


def qualify(args):
    """Run the single-owner native and synchronous WAL-replay proofs."""
    sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
    from local_dry_run_targets import _kernel_live_worker

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    root = args.state / "single-owner"
    root.mkdir(mode=0o700)
    before = inventory()
    dump(root / "before.json", before)
    output = root / "wal_replay"
    output.mkdir(mode=0o700)
    started = time.monotonic()
    seconds = 180
    trace = NvmetTrace()
    trace.arm()
    supervisor = ControllerLossSupervisor(output, time.monotonic() + seconds)
    supervisor.start()
    try:
        try:
            proof, evidence = _kernel_live_worker(
                output, args.binary, "tcp", seconds, time.time() + seconds,
                owner_wal_file_bytes=8 * 1024**2, controller_loss=True)
        except BaseException:
            # Capture while the buffer still holds the failing tier's events,
            # then let the proof's own exception carry the verdict.
            trace.disarm()
            trace.capture_failure(root / "failure-trace")
            raise
        finally:
            supervisor.stop()
            supervisor.join(timeout=10)
        trace.disarm()
        if supervisor.error is not None:
            raise RuntimeError(f"controller-loss supervision failed: {supervisor.error!r}")
        if not supervisor.records or not all(row["injected"] for row in supervisor.records):
            raise RuntimeError("controller-loss injection was not confirmed for every tier")
        qualifications = sorted(output.rglob("qualification.json"))
        for proof_path in qualifications:
            tier_proof = json.loads(proof_path.read_text())
            loss = tier_proof.get("controller_loss") or {}
            if (tier_proof.get("checks", {}).get("w1_controller_loss") is not True
                    or loss.get("resumed", {}).get("sysfs_inode") in (None, loss.get("deleted", {}).get("sysfs_inode"))):
                raise RuntimeError(f"controller-loss resume unproven in {proof_path}")
        proofs_complete = len(qualifications) == 2
        after = inventory()
        dump(output / "after.json", after)
        result = {"complete": proof["complete"], "fixture_removed": proof["fixture_removed"],
                  "inventory_restored": before == after, "evidence": str(evidence.relative_to(root)),
                  "controller_loss": {"injections": len(supervisor.records),
                                      "tiers": len(qualifications)},
                  "elapsed_seconds": round(time.monotonic() - started, 3),
                  "nvmet_trace": "armed" if trace.root is not None else "unavailable"}
        dump(root / "summary.json", {"wal_replay": result})
        print(f"wal_replay: {json.dumps(result, sort_keys=True)}", flush=True)
        failed = (not result["complete"] or not result["fixture_removed"]
                  or not result["inventory_restored"] or not proofs_complete)
        if failed:
            trace.capture_failure(root / "failure-trace")
        if not result["fixture_removed"] or not result["inventory_restored"]:
            raise RuntimeError("Single-owner kernel TCP cleanup unconfirmed")
        return 0 if result["complete"] and proofs_complete and signal.getsignal(signal.SIGTERM) != signal.SIG_IGN else 1
    finally:
        # Never leave the nvmet tracepoints or the kprobe armed for a later
        # proof on the same runner, including a failed run.
        trace.cleanup()


def restart(args):
    """Prove the coordinator reopen after a SIGKILL and an owner restart.

    One database, one owner. The coordinator is killed at its write gate so no
    clean close disconnects its controller or seals its WAL. The owner is
    killed and restarted as a new incarnation, then the same coordinator root
    is reopened. The reopen must read a non-empty replay page through a fresh
    controller and never report a lost controller or a short namespace read.
    """
    deployment, specs = campaign_modules()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    root = args.state / "owner-restart"
    root.mkdir(mode=0o700)
    binary = args.binary.resolve(strict=True)
    evidence = {"scope": __doc__, "binary": str(binary), "complete": False}
    try:
        evidence["restart"] = restart_positive(deployment, specs, binary, root)
        evidence["complete"] = True
        print("owner_restart: " + json.dumps(evidence, sort_keys=True), flush=True)
        return 0
    except BaseException as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        dump(root / "restart.json", evidence)


def layout(args):
    """Prove the Sui campaign owner layout on this host's real kernel target.

    Three databases and two partitions generate six owner processes on one
    address. Each database takes its own slot of the partition window, so
    every process exports, seals and reopens its own target. The negative pass
    gives two databases one pool base: the second process must refuse to share
    the configfs port of the first.
    """
    deployment, specs = campaign_modules()
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    root = args.state / "owner-layout"
    root.mkdir(mode=0o700)
    binary = args.binary.resolve(strict=True)
    evidence = {"scope": __doc__, "binary": str(binary), "complete": False}
    try:
        evidence["positive"] = layout_positive(deployment, specs, binary, root / "positive")
        evidence["negative"] = layout_negative(deployment, specs, binary, root / "negative")
        evidence["complete"] = True
        print("owner_layout: " + json.dumps(evidence, sort_keys=True), flush=True)
        return 0
    except BaseException as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        dump(root / "layout.json", evidence)


def cleanup(args):
    """Confirm the proofs' teardown and the independently restored inventories.

    Only teardown evidence gates this advisory action. A benchmark or admission
    failure is the run step's verdict, not a cleanup defect.
    """
    before = json.loads((args.state / "before.json").read_text())
    after = inventory()
    dump(args.state / "cleanup-inventory.json", after)
    defects = [teardown_defect(args.state, "proof")]
    prewrite_path = args.state / "proof-prewrite"
    if prewrite_path.exists():
        prewrite_defect = teardown_defect(args.state, "proof-prewrite")
        defects.append(prewrite_defect)
        dump(args.state / "prewrite-cleanup.json", {"complete": prewrite_defect is None})
    defects = [defect for defect in defects if defect is not None]
    inventory_restored = before == after
    complete = not defects and inventory_restored
    dump(args.state / "cleanup.json", {"complete": complete,
         "defects": defects, "inventory_restored": inventory_restored})
    if not complete:
        raise RuntimeError("Owned kernel TCP cleanup unconfirmed; retained proof names exact resources")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",
                        choices=("qualify", "run", "cleanup", "layout", "restart"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("requires sudo on the hosted runner")
    if not args.state.is_relative_to("/var/lib"):
        parser.error("state must be an owned /var/lib directory")
    for path in (args.state, *args.state.parents):
        stat = path.stat()
        if path.is_symlink() or stat.st_uid != 0 or stat.st_mode & 0o022:
            parser.error(f"untrusted root-owned state ancestor: {path}")
    if args.action != "cleanup" and args.binary is None:
        parser.error(f"{args.action} requires --binary")
    return {"qualify": qualify, "run": run, "cleanup": cleanup,
            "layout": layout, "restart": restart}[args.action](args)


if __name__ == "__main__":
    raise SystemExit(main())
