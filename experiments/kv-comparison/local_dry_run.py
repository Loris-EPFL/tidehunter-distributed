#!/usr/bin/env python3
"""Local process qualification for the generated comparison campaign."""
import copy
import math
import argparse
import os
from pathlib import Path
import hashlib
import json
import shutil
import threading
import subprocess
import time
from datetime import datetime, timezone
import sys
import uuid
import stat
import ack_contract as contracts


def configure_runtime_environment(runtime):
    """Apply the Soft-RoCE peer limit to controller subprocesses too."""
    os.environ["LD_LIBRARY_PATH"] = str(Path(runtime) / "lib")
    os.environ["FI_UNIVERSE_SIZE"] = "6"


def local_fabric_unit_environment(argv):
    """Pass explicit local provider diagnostics to owned transient units."""
    level = os.environ.get("FI_LOG_LEVEL")
    if level is not None and level not in ("info", "debug"):
        raise ValueError("local FI_LOG_LEVEL must be info or debug")
    argv.insert(1, "--setenv=FI_UNIVERSE_SIZE=6")
    if level is not None:
        argv.insert(1, "--setenv=FI_LOG_LEVEL=" + level)


def owner_or_nvme_quarantined(controller):
    return controller.tide_cleanup_forbidden or controller.active_nvme is not None


def scale_operations(job, fraction, *, minimum=32):
    """Scale nonzero timed phases without changing the planned dataset or zero-warmup cases."""
    if not math.isfinite(fraction) or not 0 < fraction <= 1:
        raise ValueError("fraction must be finite and in (0, 1]")
    if type(minimum) is not int or minimum < 1:
        raise ValueError("minimum must be a positive integer")
    result = copy.deepcopy(job)
    for field in ("warmup_operations", "measurement_operations"):
        count = result[field]
        if type(count) is not int or count < 0:
            raise ValueError("operation count must be a nonnegative integer")
        if count:
            result[field] = min(count, max(minimum, math.ceil(count * fraction)))
    requested = result.get("read_tier", {}).get("requested")
    if requested is not None and "warmup_operations" in requested:
        requested["warmup_operations"] = result["warmup_operations"]
    return result


def scoped_file_eviction(fixture, paths):
    """Evict only identity-checked regular files below the owned fixture."""
    root = Path(fixture).resolve(strict=True)
    records = []
    seen = set()
    for name in paths:
        path = Path(name)
        if not path.is_absolute() or path.is_symlink():
            raise ValueError("eviction requires absolute non-symlink files")
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or resolved == root:
            raise ValueError("eviction file escapes the fixture")
        before = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("eviction requires a regular file")
        identity = (before.st_dev, before.st_ino)
        if identity in seen:
            continue
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino) != identity or not stat.S_ISREG(opened.st_mode):
                raise ValueError("eviction file identity changed")
            os.fsync(fd)
            os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            records.append({"path": str(resolved), "device": opened.st_dev, "inode": opened.st_ino,
                            "bytes": opened.st_size, "fsync": True, "posix_fadvise_dontneed": True})
            seen.add(identity)
        finally:
            os.close(fd)
    return {"method": "scoped_file_eviction", "files": records,
            "scope": "owned regular files only; no global cache drop or cold-media claim"}


class LocalObjects:
    """Fixture-scoped object store with immutable generation checks."""
    def __init__(self, directory):
        self.root = Path(directory).resolve(strict=True)
        self.lock = threading.Lock()

    def _path(self, key):
        path = self.root / key
        if not isinstance(key, str) or Path(key).is_absolute() or ".." in Path(key).parts:
            raise ValueError("invalid fixture object key")
        if not path.resolve().is_relative_to(self.root):
            raise ValueError("fixture object escapes root")
        return path

    def head(self, key):
        path = self._path(key)
        if not path.is_file():
            error = FileNotFoundError(key)
            error.status = 404
            raise error
        data = path.read_bytes()
        return {"size": len(data), "generation": path.stat().st_mtime_ns,
                "metadata": {"sha256": hashlib.sha256(data).hexdigest()}}

    def put(self, key, raw, *, content_type=None, metadata=None, if_generation_match=0):
        del content_type
        if metadata and metadata.get("sha256") != hashlib.sha256(raw).hexdigest():
            raise ValueError("object checksum differs")
        with self.lock:
            path = self._path(key)
            generation = path.stat().st_mtime_ns if path.exists() else 0
            if generation != if_generation_match:
                raise ValueError("object generation changed")
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + ".pending")
            with temporary.open("xb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)

    def get(self, key, *, max_bytes):
        path = self._path(key)
        if path.stat().st_size > max_bytes:
            raise ValueError("object exceeds bounded read")
        return path.read_bytes()

    def upload(self, key, source, *, metadata, if_generation_match, deadline_check=None):
        if deadline_check:
            deadline_check()
        self.put(key, Path(source).read_bytes(), metadata=metadata,
                 if_generation_match=if_generation_match)
        if deadline_check:
            deadline_check()

    def download(self, key, destination, *, generation):
        with self.lock:
            path = self._path(key)
            if path.stat().st_mtime_ns != generation:
                raise ValueError("object generation changed")
            shutil.copyfile(path, destination)


def ordered_jobs(spec, order):
    """Bind the local run to the same generated matrix and admitted final order."""
    import cloud_campaign
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "gcp-rdma"))
    import risk_order
    if spec.get("mode") == "focused":
        import manual_campaign
        jobs = manual_campaign._reviewed_jobs(spec)
    else:
        campaign_spec = dict(spec)
        campaign_spec.pop("suite", None)
        jobs = cloud_campaign.make_matrix(campaign_spec)
    risk_order.validate_order(jobs, order)
    by_id = {job["case_id"]: job for job in jobs}
    return [by_id[case] for case in order["case_ids"]]


class LocalMode:
    """Explicit in-process adapter; it cannot be selected by a cloud spec."""
    def __init__(self, fixture, config, valkey_runtime):
        import cloud_controller
        self.cloud = cloud_controller
        self.fixture = Path(fixture).resolve(strict=True)
        self.config = config
        self.runtime = Path(valkey_runtime).resolve(strict=True)
        self.valkey = None
        self.controller = None
        self.units = set()
        self.unit_lock = threading.Lock()
        self.daemons = {}
        self.qualifications = {}
        self.layout_fleet = {
            "schema_version": 1, "run_id": config["prefix"],
            "data_root": str(self.fixture), "qualification_only": True,
            "replication_factor": 1,
            "coordinator": {"instance_id": "local-client", "address": "127.0.0.10"},
            "nodes": [{"index": i, "instance_id": f"local-{i}",
                       "address": f"127.0.0.{11 + i}"} for i in range(5)],
            "client_cpus": config["fleet"]["client_cpus"], "client_memory_mib": 8192,
            "server_cpus": config["fleet"]["server_cpus"],
            "server_memory_mib": config["fleet"]["server_memory_mib"],
        }
        self.layout_backend = None

    def validate_config(self, config):
        if config is not self.config or Path(config["fleet"]["data_root"]).resolve() != self.fixture:
            raise ValueError("local controller must use its exact owned fixture")
        if "local_mode" in config or "local_mode" in config["spec"] or config.get("platform"):
            raise ValueError("local mode is an explicit object, not a configuration selection")
        return config

    def guard(self):
        if not self.fixture.is_dir() or self.fixture.is_symlink():
            raise RuntimeError("owned local fixture is no longer available")
        return {"scope": "six logical guests sharing one local kernel", "fixture": str(self.fixture)}

    def identity(self, index):
        if type(index) is not int or index not in range(-1, 5):
            raise ValueError("invalid logical local guest")
        return {"logical_guest": index, "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
                "scope": "logical guest on shared local kernel"}

    def monitor_spec(self, results):
        return {"schema_version": 1, "run_id": self.config["prefix"],
                "state_dir": str(self.fixture / "monitor-state"), "results_root": str(results),
                "deadline_utc": self.config["deadline_utc"],
                "hosts": [{"index": i, "address": f"127.0.0.{i + 11}",
                           "role": "coordinator" if i == -1 else f"storage-{i}"} for i in range(-1, 5)],
                "ports": {"prometheus": 29090, "grafana": 23000, "benchmark": 29899,
                          "node": 29100, "host": 29898}, "allow_loopback_hosts": True}

    @staticmethod
    def root_command(argv, *, timeout):
        import local_dry_run_targets
        try:
            result = subprocess.run([*local_dry_run_targets.ROOT, *map(str, argv)],
                                    capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"owned command timed out: stdout={exc.stdout!r}; stderr={exc.stderr!r}") from exc
        if result.returncode:
            raise RuntimeError(f"owned command failed ({result.returncode}): stdout={result.stdout!r}; stderr={result.stderr!r}")
        return result.stdout

    def start_client(self, name, executable, budget, seconds):
        case = Path(executable[-1]).parent.name
        if name != self.cloud.unit_name(self.config, case, "client"):
            raise ValueError("client unit does not match case")
        with self.unit_lock:
            if self.cloud.unit_status(name).get("LoadState") != "not-found":
                raise ValueError("refusing a pre-existing unit")
            self.units.add(name)
        argv = self.cloud.systemd_argv(self.config, case, "client", budget["cpus"],
                                     budget["memory_mib"], seconds, executable)
        # RxM's 1024-peer default exceeds Soft-RoCE's CQ limit; this fleet has six hosts.
        local_fabric_unit_environment(argv)
        self.root_command(argv, timeout=30)

    def stop_client(self, name):
        if name not in self.units:
            raise ValueError("unit is not owned by this run")
        # Successful transient units may already have been collected by systemd.
        if self.cloud.unit_status(name).get("LoadState") == "not-found":
            return
        try:
            self.root_command(["systemctl", "stop", name], timeout=35)
        except RuntimeError:
            if self.cloud.unit_status(name).get("LoadState") != "not-found":
                raise
        status = self.cloud.unit_status(name)
        if status.get("LoadState") != "not-found" and (
                status.get("MainPID") != "0" or status.get("ActiveState") not in ("inactive", "failed")):
            raise RuntimeError("owned unit still active")

    def client_logs(self, name):
        if name not in self.units:
            raise ValueError("client unit is not owned by this run")
        return self.root_command(["journalctl", "--no-pager", "--unit", name,
                                  "--output=short-precise"], timeout=30)

    def snapshot_client(self, name, *, completed=False):
        status = self.cloud.unit_status(name)
        pid = status.get("MainPID", "0")
        if not pid.isdigit() or int(pid) <= 0 and not completed:
            raise RuntimeError("local workload PID is unavailable")
        result = {"unit": status, "process_io": None, "cgroup": None}
        if int(pid) > 0:
            result["process_io"] = (Path("/proc") / pid / "io").read_text()
        group = status.get("ControlGroup", "")
        if group:
            if group != "/system.slice/" + name + ".service":
                raise ValueError("local client cgroup identity differs")
            directory = Path("/sys/fs/cgroup") / group.lstrip("/")
            if directory.is_dir():
                result["cgroup"] = {field: (directory / field).read_text() for field in
                                    ("cpu.stat", "memory.current", "memory.events", "io.stat", "cpu.max", "memory.max")}
        return result

    def backend_options(self, backend, ack_contract="synced"):
        contract = contracts.selected(backend, ack_contract)
        kwargs = {"ack_contract": contract} if contract != "synced" else {}
        if self.valkey is not None and self.valkey.backend == backend:
            if getattr(self.valkey, "ack_contract", "synced") != contract:
                raise RuntimeError("local Valkey acknowledgement contract differs")
            return self.valkey.backend_options
        if self.layout_backend != backend:
            raise RuntimeError("exact local external backend is not started")
        import cloud_layout
        cfg = cloud_layout.node_config(self.layout_fleet, backend, -1, **kwargs)
        return cloud_layout.backend_options(self.layout_fleet, backend, cfg["directory"], **kwargs)

    def owned_processes(self, index, job, owner_case=None):
        role = "client" if index == -1 else f"storage-{index}"
        processes = []
        if index == -1 or job["backend"] in self.cloud.cloud_campaign.TRANSPORTS:
            name = self.cloud.unit_name(self.config, owner_case if index >= 0 and owner_case else job["case_id"], role)
            if name in self.units:
                processes.append({"role": role, "cgroup": "/system.slice/" + name + ".service"})
        if index >= 0 and job["backend"] in ("valkey_tcp", "valkey_rdma") and self.valkey is not None:
            processes.append(self.valkey.monitor_processes()[index])
        if job["backend"] == self.layout_backend:
            config = dict(self.config, fleet=self.layout_fleet)
            evidence = self.cloud.owned_monitor_processes(config, index, job, owner_case)
            # Client lifecycle belongs to self.units above, not the external layout.
            missing = [item for item in evidence["missing"]
                       if not (index == -1 and item["role"] == "client")]
            if missing:
                raise RuntimeError("local deployment process identity unavailable: " + repr(missing))
            processes.extend(p for p in evidence["processes"]
                             if not (index == -1 and p["role"] == "client"))
        return processes

    def snapshot_host(self, index, request):
        import resource_accounting
        import rdma_diagnostics
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        result = {"observed_utc": datetime.now(timezone.utc).isoformat(), "monotonic_ns": time.monotonic_ns(),
                  "nvme": {"host_identity": {"boot_id": boot}, "scope": "local shared kernel; no physical disk claim"},
                  "drop_os_caches": False, "application_caches_evicted": False,
                  "diskstats": Path("/proc/diskstats").read_text(), "vmstat": Path("/proc/vmstat").read_text(),
                  "host_cpu_stat": Path("/proc/stat").read_text(), "host_cpu_hz": os.sysconf("SC_CLK_TCK")}
        if request.get("drop"):
            job = request["resource_job"]
            case = request.get("resource_owner_case") or job["case_id"]
            roots = []
            if index == -1 and job["backend"] == "native":
                if "sealed_dataset" in job:
                    source = job["sealed_dataset"]["source_id"]
                    roots.append(Path(self.controller.shared_native[source]["database_path"]))
                else:
                    roots.append(self.fixture / "results" / "cases" / case / "database")
            elif index >= 0 and job["backend"] in self.cloud.cloud_campaign.TRANSPORTS:
                roots.append(self.fixture / "cells" / case / f"node-{index}" / "storage")
            elif index >= 0 and job["backend"] in ("valkey_tcp", "valkey_rdma") and self.valkey is not None:
                roots.append(self.valkey.directory / f"node-{index}" / "data")
            elif job["backend"] == self.layout_backend:
                role = "coordinator" if index == -1 else f"node-{index}"
                roots.append(self.fixture / job["backend"] / role / "data")
            paths = [path for root in roots if root.exists() for path in root.rglob("*")
                     if path.is_file() and not path.is_symlink()]
            result["scoped_file_eviction"] = scoped_file_eviction(self.fixture, paths)
        job = request.get("resource_job")
        if job is not None:
            processes = self.owned_processes(index, job, request.get("resource_owner_case"))
            result["resource_accounting"] = resource_accounting.capture(index, boot, processes)
            result["rdma_diagnostics"] = rdma_diagnostics.capture()
        if request.get("monitor_job") is not None:
            selected_job = request["monitor_job"]
            processes = self.owned_processes(index, selected_job, request.get("resource_owner_case"))
            selection = self.cloud.monitor_module(self.config).select_host(
                self.monitor_spec(self.controller.root), index, selected_job, processes)
            result["monitoring"] = {"complete": selection.get("complete") is not False,
                                    "selection": selection, "processes": processes, "missing": []}
        return result

    def request(self, index, request, *, case, timeout, export):
        del export
        action = request["action"]
        if action in ("cold_trace_probe", "cold_trace_start", "cold_trace_stop", "nvme_trace_start", "nvme_trace_stop"):
            raise self.cloud.SafetyError("local physical-device tracing is not permitted on the laptop")
        if action in ("monitor_start", "monitor_stop"):
            monitor = self.cloud.monitor_module(self.config)
            spec = self.monitor_spec(self.controller.root)
            return monitor.start_host(spec, index) if action == "monitor_start" else monitor.stop_host(spec, index)
        if action == "snapshot":
            return self.snapshot_host(index, request)
        if action == "layout":
            import cloud_layout
            backend, operation = request["backend"], request["operation"]
            if backend != self.layout_backend or backend not in ("tikv", "cassandra", "foundationdb", "garnet", "kvrocks"):
                raise ValueError("local deployment backend not owned")
            if operation not in ("prepare", "start", "probe", "stop", "export", "reset-data"):
                raise ValueError("invalid local deployment operation")
            argv = [sys.executable, "-B", Path(cloud_layout.__file__), operation,
                    "--fleet-json", json.dumps(self.layout_fleet), "--backend", backend,
                    "--node-index", str(index)]
            contract = contracts.selected(backend, request.get("ack_contract", "synced"))
            if contract != "synced":
                argv += ["--ack-contract", contract]
            if operation == "probe":
                argv.extend(["--timeout", str(min(900, int(timeout)))])
                if request.get("initialize"):
                    argv.append("--initialize")
                if request.get("cluster"):
                    argv.append("--cluster")
            return json.loads(self.root_command(argv, timeout=timeout))
        if action == "tide_start":
            return self.start_owner(index, request, case, timeout)
        if action == "tide_stop":
            return self.stop_owner(index, request, case)
        return self.cloud.guest_operation(self.config, request, self.controller.clients, local_mode=self)

    def start_owner(self, index, request, case, timeout):
        import yaml
        if index not in range(self.config.get("jobs", [{}])[0].get("owner_count", 5)) or case != request["case_id"]:
            raise ValueError("invalid local owner identity")
        expected_key = f"{self.config['prefix']}/deploy/cells/{case}/storage-{index}.tar.gz"
        if request["key"] != expected_key:
            raise ValueError("capsule object identity differs")
        guest = self.fixture / "cells" / case / f"node-{index}"
        guest.parent.mkdir(parents=True, exist_ok=True)
        archive = guest.parent / f"capsule-{index}.tar.gz"
        self.cloud.download_object(self.controller.clients, self.config, expected_key, archive)
        if self.cloud.digest(archive) != request["sha256"]:
            raise ValueError("capsule checksum differs")
        self.cloud.safe_extract_capsule(archive, guest, request["files"])
        archive.unlink()
        launch_path = guest / "launch.yaml"
        launch = yaml.safe_load(launch_path.read_text())
        self.cloud.validate_launch(self.config, launch, case, index)
        live = launch.get("node", {}).get("live_target")
        daemon = None
        if live is not None:
            Path(live["state_dir"]).mkdir(mode=0o700)
            if live.get("spdk") is not None:
                import local_dry_run_targets
                daemon = local_dry_run_targets.LiveSpdkDaemon(
                    self.fixture, index, live["spdk"]["executable"],
                    deadline_epoch=min(self.cloud.timestamp(self.config["deadline_utc"]).timestamp(),
                                       time.time() + 7000), socket=live["spdk"]["rpc_socket"])
                daemon.state = launch_path.parent / "live-spdk-daemon"
                daemon.started = launch_path.parent / "live-spdk-started.json"
                daemon.stopped = launch_path.parent / "live-spdk-stopped.json"
                self.daemons[(case, index)] = daemon
                try:
                    daemon_receipt = daemon.start()
                except BaseException:
                    self.controller.tide_cleanup_forbidden = True
                    raise
        name = self.cloud.unit_name(self.config, case, f"storage-{index}")
        with self.unit_lock:
            if self.cloud.unit_status(name).get("LoadState") != "not-found":
                raise ValueError("refusing a pre-existing storage unit")
            self.units.add(name)
        seconds = min(request["timeout"], int((self.cloud.timestamp(self.config["deadline_utc"])
                                               - self.cloud.utc()).total_seconds()))
        argv = self.cloud.systemd_argv(self.config, case, f"storage-{index}",
                 self.config["fleet"]["server_cpus"], self.config["fleet"]["server_memory_mib"],
                 seconds, [self.config["binary"], "--experimental-storage-node", launch_path])
        local_fabric_unit_environment(argv)
        self.root_command(argv, timeout=min(30, timeout))
        deadline = time.monotonic() + min(90, seconds, timeout)
        while time.monotonic() < deadline:
            status = self.cloud.unit_status(name)
            if status.get("SubState") == "exited" or status.get("ActiveState") in ("failed", "inactive"):
                raise RuntimeError("local owner exited before readiness")
            marker = Path(launch["ready_file"])
            if marker.is_file():
                address = marker.read_text().strip()
                if address != launch["listen"]:
                    raise ValueError("owner readiness differs from launch")
                result = {"ready": address, "launch_sha256": self.cloud.digest(launch_path),
                          "unit": status, "durability_receipt": self.cloud.owner_durability_receipt(launch)}
                if live is not None:
                    import nvme_campaign_owner
                    result["live_owner_identity"] = nvme_campaign_owner.capture_live_owner_identity(
                        launch_path.parent, launch_path, status)
                if daemon is not None:
                    result["live_spdk_daemon"] = daemon_receipt
                return result
            time.sleep(0.1)
        raise TimeoutError("local owner readiness expired")

    def stop_owner(self, index, request, case):
        import yaml
        if case != request["case_id"] or index not in range(self.config.get("jobs", [{}])[0].get("owner_count", 5)):
            raise ValueError("invalid local owner identity")
        root = self.fixture / "cells" / case
        launch_path = root / f"node-{index}" / "launch.yaml"
        launch = yaml.safe_load(launch_path.read_text())
        self.cloud.validate_launch(self.config, launch, case, index)
        name = self.cloud.unit_name(self.config, case, f"storage-{index}")
        if name not in self.units:
            raise ValueError("owner unit is not owned")
        Path(launch["stop_file"]).touch(exist_ok=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            status = self.cloud.unit_status(name)
            if status.get("MainPID") == "0" or status.get("SubState") == "exited":
                break
            time.sleep(0.1)
        self.root_command(["systemctl", "stop", name], timeout=35)
        after = self.cloud.unit_status(name)
        if after.get("LoadState") != "not-found" and (after.get("MainPID") != "0"
                or after.get("ActiveState") not in ("inactive", "failed")):
            raise RuntimeError("owned local owner did not quiesce")
        result = {"after": after, "stop_command_completed": True,
                  "logs": self.root_command(["journalctl", "--no-pager", "--unit", name,
                                              "--output=short-precise"], timeout=30)}
        if launch["node"].get("live_target") is not None:
            import nvme_campaign_owner
            result["live_target_shutdown"] = nvme_campaign_owner.admit_live_shutdown(
                launch_path.parent, launch_path, launch, index, status)
        daemon = self.daemons.get((case, index))
        if daemon is not None:
            if request.get("initiator_drained") is not True:
                raise RuntimeError("initiator drain is required before daemon cleanup")
            result["live_spdk_daemon"] = daemon.stop(
                launch_path.with_suffix(".live-target-shutdown.json"), initiator_drained=True)
            del self.daemons[(case, index)]
        database = self.cloud.guarded(launch["node"]["root"], launch_path.parent, must_exist=True)
        shutil.rmtree(database)
        result["database_removed"] = str(database)
        return result

    def admit_live_qualification(self, job):
        if job.get("nvmeof_target", "kernel") != "spdk":
            raise ValueError("five local kernel live owners are not supported")
        proof = self.qualifications.get("kernel")
        if not isinstance(proof, dict) or proof.get("complete") is not True:
            raise RuntimeError("single-owner kernel live baseline qualification is not complete")
        if proof.get("binary_sha256") != self.config["binary_sha256"]:
            raise RuntimeError("local live target qualification used a different executable")
        return proof


def failure_class(error):
    text = str(error).lower()
    if "panic" in text or "assertion failed" in text:
        return "code defect"
    if any(token in text for token in ("rxe", "rdma_cm", "soft-roce")):
        return "Soft-RoCE limitation"
    return "harness/config"


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".pending")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")
    temporary.replace(path)


def main(argv=None):
    import cloud_controller
    import local_dry_run_targets
    import local_dry_run_valkey
    from datetime import timedelta

    work = Path("/home/hybr1d/Desktop/PDM/TideHunter/checkpoint-work/spdk-matched-storage-20261105T004442Z")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True,
                        help="release package runtime containing spdk and Valkey")
    parser.add_argument("--libs", type=Path, required=True,
                        help="admitted local runtime libraries, including the pinned FDB client")
    parser.add_argument("--source-revision", required=True,
                        help="revision bound to the supplied release binary")
    parser.add_argument("--spec", type=Path, default=work / "queue-auth-final-ready/spec.json")
    parser.add_argument("--order", type=Path, default=work / "code-fix/risk-first-order.json")
    parser.add_argument("--fraction", type=float, default=0.001)
    parser.add_argument("--minimum", type=int, default=32)
    parser.add_argument("--case-timeout", type=int, default=300)
    parser.add_argument("--total-timeout", type=int, default=5400)
    parser.add_argument("--case-id", action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        raise SystemExit("run through the documented privileged Docker/nsenter host-root wrapper")
    if not 60 <= args.case_timeout <= 600 or not 3600 <= args.total_timeout <= 21600:
        raise SystemExit("case timeout must be 60..600 and total timeout 3600..21600 seconds")
    deadline = datetime.now(timezone.utc) + timedelta(seconds=args.total_timeout)
    binary = args.binary.resolve(strict=True)
    spec = json.loads(args.spec.read_text())
    order = json.loads(args.order.read_text())
    jobs = ordered_jobs(spec, order)
    if spec.get("mode") == "focused" and args.fraction != 1.0:
        raise SystemExit("focused jobs require --fraction 1.0 to preserve exact reviewed cases")
    if args.case_id:
        selected = set(args.case_id)
        if len(selected) != len(args.case_id) or not selected <= {job["case_id"] for job in jobs}:
            raise SystemExit("case ids must be unique members of the admitted matrix")
        jobs = [job for job in jobs if job["case_id"] in selected]
    jobs = [scale_operations(job, args.fraction, minimum=args.minimum) for job in jobs]
    prefix = "th-" + uuid.uuid4().hex[:16]
    default_output = work / ("local-dry-run-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    output = (args.output or default_output).absolute()
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    fixture = Path("/mnt/tidehunter-kv-comparison") / prefix
    fixture.mkdir(mode=0o700, parents=True, exist_ok=False)
    aliases, errors, mode, controller = [], [], None, None
    cleanup_verified = True
    result = {"schema_version": 1, "run_id": prefix, "complete": False, "cases": [],
              "binary_sha256": hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
              "scope": "local functional qualification; shared kernel and file-backed storage; not a performance comparison"}
    try:
        runtime = fixture / "runtime"
        (runtime / "bin").mkdir(parents=True)
        staged_binary = runtime / "bin/kv-research"
        shutil.copy2(binary, staged_binary)
        shutil.copytree(args.runtime.resolve(strict=True) / "spdk", runtime / "spdk", symlinks=True)
        shutil.copytree(args.libs.resolve(strict=True), runtime / "lib", symlinks=True)
        configure_runtime_environment(runtime)
        for suffix in range(3, 7):
            address = f"10.99.0.{suffix}/24"
            LocalMode.root_command(["ip", "address", "add", address, "dev", "thrxe1"], timeout=10)
            aliases.append(address)
        source = args.source_revision
        campaign_spec = dict(spec)
        campaign_spec.pop("suite", None)
        campaign_spec["hard_deadline_seconds"] = args.total_timeout
        campaign_spec["case_deadline_seconds"] = args.case_timeout
        campaign_spec["export_reserve_seconds"] = min(300, args.total_timeout // 4)
        addresses = [f"10.99.0.{i}" for i in range(2, 7)]
        fleet = {"run_id": prefix, "data_root": str(fixture),
                 "coordinator": {"instance_id": "local-client", "address": "10.99.0.1"},
                 "nodes": [{"instance_id": f"local-{i}", "address": address, "rdma_address": address}
                           for i, address in enumerate(addresses)],
                 "client_cpus": 4, "client_memory_mib": 4096,
                 "server_cpus": 2, "server_memory_mib": 2048}
        if datetime.now(timezone.utc) >= deadline:
            raise TimeoutError("total deadline expired during fixture setup")
        config = {"fleet": fleet, "prefix": prefix, "binary": str(staged_binary),
                  "binary_sha256": result["binary_sha256"],
                  "binary_blake2b512": hashlib.file_digest(staged_binary.open("rb"), "blake2b").hexdigest(),
                  "source_revision": source, "repository_root": str(Path(__file__).resolve().parents[2]),
                  "fdb_library_dir": str(runtime / "lib"), "deadline_utc": deadline.isoformat(), "spec": campaign_spec}
        objects = fixture / "objects"
        objects.mkdir(mode=0o700)
        mode = LocalMode(fixture, config, args.runtime)
        controller = cloud_controller.Controller(config, {"objects": LocalObjects(objects)}, local_mode=mode)
        mode.controller = controller
        controller.root.mkdir(mode=0o700)
        controller.root_owned = True
        controller.package = {}
        if any(job["backend"] == "foundationdb" for job in jobs):
            import cloud_package
            library = (runtime / "lib/libfdb_c.so").resolve(strict=True)
            library.relative_to((runtime / "lib").resolve(strict=True))
            identity = cloud_package.identity(library)
            if (identity["sha256"] != cloud_package.FDB_SHA256
                    or identity["blake2b512"] != cloud_package.FDB_BLAKE2B):
                raise ValueError("local FoundationDB client differs from pinned release")
            identity["path"] = str(library)
            controller.package["fdb"] = identity
        if any(job.get("nvmeof_route") == "live" for job in jobs):
            try:
                qualification = local_dry_run_targets.qualify_kernel_live(
                    fixture, staged_binary, transport="rdma", deadline_seconds=max(120, args.case_timeout),
                    library_path=runtime / "lib")
            except Exception as exc:
                proofs = [json.loads(path.read_text()) for path in fixture.glob("kernel-live-*/proof.json")]
                result["kernel_live_qualification_error"] = str(exc)
                if not proofs or not all(proof.get("fixture_removed") is True for proof in proofs):
                    cleanup_verified = False
                    errors.append("kernel qualification cleanup unconfirmed: " + str(exc))
                    raise
                errors.append("kernel qualification failed: " + str(exc))
                qualification = {"complete": False, "binary_sha256": result["binary_sha256"]}
            mode.qualifications["kernel"] = qualification
            result["kernel_live_qualification"] = qualification
            for evidence in fixture.glob("kernel-live-*"):
                if evidence.is_dir():
                    shutil.copytree(evidence, output / evidence.name)
        save_json(output / "matrix.json", {"order": order, "jobs": jobs, "fraction": args.fraction})
        monitor = cloud_controller.monitor_module(config)
        controller.monitoring_attempted = True
        monitor.start_local(mode.monitor_spec(controller.root))
        if not args.case_id:
            try:
                result["native_valkey_controls"] = local_dry_run_valkey.run_native_controls(
                    fixture / "valkey-controls", mode.runtime, rdma_address="10.99.0.2",
                    fraction=args.fraction, minimum=args.minimum, timeout=args.case_timeout)
            except Exception:
                cleanup_verified = False
                raise
        active_backend = None
        global_work_end = controller.work_end
        for job in jobs:
            if controller.clock() >= global_work_end:
                errors.append("overall work deadline reached before all cases")
                break
            directory = controller.root / "cases" / job["case_id"]
            if job.get("nvmeof_route") == "live" and job.get("nvmeof_target", "kernel") == "kernel":
                directory.mkdir(parents=True)
                row = {"job": job, "status": "skipped_local",
                       "reason": "kernel nvmet configfs host/port objects cannot be shared by 5 cohosted owners"}
                save_json(directory / "receipt.json", row)
                result["cases"].append(row)
                continue
            try:
                if controller.shared_tide is not None and (job["backend"] == "native" or "sealed_dataset" not in job):
                    controller.stop_tide()
                    controller.shared_tide = None
                if job.get("cohort") == "native_write_regression" and controller.shared_native:
                    for prepared in controller.shared_native.values():
                        shutil.rmtree(cloud_controller.guarded(prepared["database_path"], controller.root, must_exist=True))
                    controller.shared_native.clear()
                external = ("tikv", "cassandra", "foundationdb", "garnet", "kvrocks")
                desired = job["backend"] if job["backend"] in (*external, "valkey_tcp", "valkey_rdma") else None
                contract = contracts.selected(job["backend"], job.get("ack_contract", "synced"))
                kwargs = {"ack_contract": contract} if contract != "synced" else {}
                block = (job["backend"], job["repeat"], contract)
                if block != active_backend:
                    if mode.valkey is not None:
                        try:
                            mode.valkey.close()
                        except Exception:
                            cleanup_verified = False
                            raise
                        mode.valkey = None
                    if controller.active_backend is not None:
                        try:
                            controller.stop_backend()
                        except Exception:
                            cleanup_verified = False
                            raise
                        mode.layout_backend = None
                    if desired in ("valkey_tcp", "valkey_rdma"):
                        try:
                            mode.valkey = local_dry_run_valkey.start_cluster(
                                fixture / "valkey" / (desired + "-" + uuid.uuid4().hex),
                                mode.runtime, desired, rdma_address="10.99.0.2", **kwargs)
                        except Exception:
                            cleanup_verified = False
                            raise
                    elif desired in external:
                        mode.layout_backend = desired
                        controller.start_backend(desired, **kwargs)
                    active_backend = block
                controller.work_end = min(global_work_end, controller.clock() + args.case_timeout)
                controller.run_case(job)
            except Exception as exc:
                errors.append(job["case_id"] + ": " + type(exc).__name__ + ": " + str(exc))
            finally:
                controller.work_end = global_work_end
                path = directory / "receipt.json"
                receipt = json.loads(path.read_text()) if path.is_file() else {"job": job, "status": "failed",
                          "error": errors[-1] if errors else "case did not produce a receipt"}
                if receipt.get("status") != "completed":
                    receipt["failure_class"] = failure_class(receipt.get("error", ""))
                result["cases"].append(receipt)
                save_json(output / "progress.json", result)
            if owner_or_nvme_quarantined(controller) or not cleanup_verified:
                cleanup_verified = False
                errors.append("owner or NVMe cleanup is quarantined; refusing further cases")
                break
    except Exception as exc:
        errors.append(type(exc).__name__ + ": " + str(exc))
    finally:
        if controller is not None:
            for cleanup in (controller.stop_tide, controller.stop_backend):
                try:
                    cleanup()
                except Exception as exc:
                    cleanup_verified = False
                    errors.append("cleanup: " + type(exc).__name__ + ": " + str(exc))
        if mode is not None:
            if mode.valkey is not None:
                try:
                    mode.valkey.close()
                except Exception as exc:
                    cleanup_verified = False
                    errors.append("Valkey cleanup: " + str(exc))
            if controller is not None and controller.monitoring_attempted:
                try:
                    controller.stop_monitoring()
                    if any(not event["complete"] for event in controller.monitoring_events
                           if event["operation"].startswith(("collector_stop", "monitor_stop:", "host_stop_budget"))):
                        cleanup_verified = False
                except Exception as exc:
                    cleanup_verified = False
                    errors.append("monitor cleanup: " + str(exc))
            quarantined = controller is not None and owner_or_nvme_quarantined(controller)
            if quarantined:
                cleanup_verified = False
                errors.append("owner or NVMe teardown unconfirmed; owned units retained")
            for unit in (() if quarantined else mode.units):
                try:
                    mode.stop_client(unit)
                except Exception as exc:
                    cleanup_verified = False
                    errors.append("unit cleanup " + unit + ": " + str(exc))
            if mode.daemons:
                cleanup_verified = False
                errors.append("live SPDK daemons remain owned and require proof-driven cleanup")
        for address in (reversed(aliases) if cleanup_verified else ()):
            try:
                LocalMode.root_command(["ip", "address", "del", address, "dev", "thrxe1"], timeout=10)
            except Exception as exc:
                cleanup_verified = False
                errors.append("address cleanup " + address + ": " + str(exc))
        if controller is not None and controller.root.exists():
            shutil.copytree(controller.root, output / "results", dirs_exist_ok=True)
        for evidence in fixture.glob("kernel-live-*"):
            if evidence.is_dir():
                shutil.copytree(evidence, output / evidence.name, dirs_exist_ok=True)
        if (fixture / "valkey-controls").is_dir():
            shutil.copytree(fixture / "valkey-controls", output / "valkey-controls", dirs_exist_ok=True)
        result["errors"] = errors
        result["counts"] = {status: sum(row.get("status") == status for row in result["cases"])
                            for status in ("completed", "failed", "skipped_local")}
        result["planned"] = len(jobs)
        result["not_run"] = len(jobs) - len(result["cases"])
        result["cleanup_verified"] = cleanup_verified
        result["complete"] = (result["not_run"] == 0 and result["cleanup_verified"]
                              and not errors and not result["counts"]["failed"])
        if result["cleanup_verified"]:
            shutil.rmtree(fixture)
        else:
            result["retained_fixture"] = str(fixture)
        save_json(output / "summary.json", result)
    print(json.dumps({"output": str(output), "counts": result["counts"], "not_run": result["not_run"],
                      "errors": errors}, sort_keys=True))
    return 0 if result["complete"] and not result["counts"]["failed"] else 1


if __name__ == "__main__":
    sys.exit(main())
