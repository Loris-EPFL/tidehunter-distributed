#!/usr/bin/env python3
"""Automatic, exact-owned six-host comparison guest; never provisions resources.

`plan` and `validate` are offline. `run` requires instance-profile credentials,
an independently armed absolute guest expiry, and the admitted package on all hosts.

Usage: cloud_controller.py {plan,validate,run} --config /absolute/controller.json
The runtime config and mutable results must live outside the sealed package.
`repository_root` is <stage>/source; binary and FDB paths use manifest.paths.
Bootstrap creates /mnt/tidehunter/<run>/bootstrap.json and the existing absolute
sharded-wal-expire-<run>.timer, then atomically publishes kv-bootstrap-ready.json
there only after images/provider/package admission. Its fields are run_id,
instance_id, source_sha256, binary_sha256, deadline_utc (copied from config).
The six-host NVMe run root and a fresh results/ directory are mandatory. `guest`
is an internal exact-node SSM RPC; it is not a local qualification shortcut.
"""
import argparse
import base64
import copy
import csv
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from datetime import datetime, timezone
import hashlib
import importlib.util
from itertools import islice
import json
import math
import os
from pathlib import Path
import re
import secrets
import select
import shlex
import signal
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
import uuid
# A sealed extracted source tree must remain byte-for-byte immutable.
sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import analyze
import campaign
import cloud_campaign
import floors
import cloud_layout
import ack_contract as contracts
import local
import physical_storage
import rdma_diagnostics
import resource_accounting
import q0_campaign
from portable_transport import is_trusted, protocol_key, validate_transport
from control_execution import MAX_JSON_BYTES, execute_at_most_once

ACCOUNT = "354775971805"
TAGS = {"Project": "TideHunter", "ManagedBy": "sharded-wal-benchmark",
        "Repository": "Loris-EPFL/tidehunter-distributed", "Campaign": "kv-comparison",
        "BenchmarkBranch": "bench/aws-efa", "SourceBranch": "feat/cloud-kv-comparison"}
RAW_FILES = ("manifest.json", "summary.json", "samples.csv", "metrics.prom")
MAX_AMBIGUOUS_REPLAYS = 2


class SafetyError(RuntimeError):
    """An ownership/identity/path invariant failed: do not continue workloads."""


class InfrastructureError(RuntimeError):
    """Infrastructure or an uncertain side effect makes continuation unsafe."""


class FioFailure(RuntimeError):
    """A fio measurement failed after its process was observed."""


def core_workload_failure(result, manifest, status, gate_released, completion_observed):
    """Describe the first recorded unsuccessful sample, not a later drain error."""
    qualification_failure = manifest.get("run", {}).get("qualification_failure")
    if qualification_failure:
        return "core workload qualification failed: " + str(qualification_failure)
    if manifest["status"] == "failed":
        with (result / "samples.csv").open(newline="") as samples:
            for row in csv.DictReader(samples):
                if row.get("outcome") != "success":
                    detail = {key: row.get(key) for key in ("phase", "operation", "outcome", "error_kind")}
                    return "core workload failed; first unsuccessful recorded sample: " + json.dumps(detail)
        return "core workload failed without an unsuccessful recorded sample: " + json.dumps(
            {"unit": status, "core_status": manifest["status"]})
    if not gate_released:
        return "core exited before quiescent measurement release: " + json.dumps(status)
    if not completion_observed:
        return "core exited without the postmeasurement accounting gate"
    if status.get("Result") != "success" or status.get("ExecMainStatus") != "0":
        return "core workload failed: " + json.dumps({"unit": status, "core_status": manifest["status"]})
    return None

def admit_terminal_write_timeline(directory, job, config, manifest, receipt):
    """Keep invalid failed-run diagnostics separate from admitted evidence."""
    completed = manifest["status"] == "completed"
    try:
        timeline = cloud_campaign.admit_write_timeline(directory, job, config, completed=completed)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError) as exc:
        if completed:
            raise
        receipt["write_timeline_admission_error"] = type(exc).__name__ + ": " + str(exc)
        return
    if timeline is not None:
        receipt["write_timeline"] = timeline


def admitted_host_count(count):
    return count - 1 in cloud_layout.OWNER_HOST_COUNTS


def logical_owner_hosts(config, job):
    """Map logical WAL owners to physical fleet nodes without sharing state."""
    count = job.get("owner_count", len(config["fleet"]["nodes"]))
    hosts = config["fleet"]["nodes"]
    if job.get("owner_topology") == "colocated":
        return [hosts[0]] * count
    if count > len(hosts):
        raise SafetyError("logical owners exceed separate owner hosts")
    return hosts[:count]


def resolve_case_binary(config, job):
    """Select a staged executable and check its bytes before starting a case process."""
    import cloud_package
    binary = Path(config["binary"])
    role = job.get("binary_role")
    selection = config.get("spec", {}).get("selection")
    if selection != q0_campaign.SELECTION and role is None:
        return binary, None
    if selection != q0_campaign.SELECTION:
        raise SafetyError("case-specific binary requested outside the admitted selection")
    try:
        expected = cloud_campaign._m3_cases()[job["case_id"]]
        if expected["binary_role"] != role:
            raise ValueError("case binary role differs from reviewed job")
        stage = Path(config["repository_root"]).parent
        seals = config["spec"]["case_binaries"]
        cloud_package.admit_case_binaries(stage, seals)
        seal = seals[role]
        if role != "land4":
            binary = cloud_package.case_binary_path(stage, seal["sha256"])
        if not binary.stat().st_mode & 0o111 or cloud_package.identity(binary)["sha256"] != seal["sha256"]:
            raise ValueError("executable is not installed with the reviewed SHA-256")
        return binary, seal
    except (KeyError, OSError, ValueError, TypeError) as exc:
        raise SafetyError(f"case binary admission failed for {job.get('case_id')}: {exc}") from exc


def case_owner_job(config, case):
    if config.get("spec", {}).get("selection") in cloud_campaign.M3_SELECTIONS:
        job = cloud_campaign._m3_cases().get(case, {})
        if job and cloud_campaign.job_owner_hosts(job) == len(config["fleet"]["nodes"]):
            return job
        return {}
    return {}


def requests_archived_owner_history(job):
    """Only the sealed Q0 binaries can still produce history-maintenance evidence."""
    return job.get("selection") == cloud_campaign.Q0_SELECTION and job.get("owner_history_diagnostics", False)


def owner_process_budget(config, job, index):
    shares = sum(owner_host_index(config, job, owner) == owner_host_index(config, job, index)
                 for owner in range(job["owner_count"])) if job.get("owner_topology") else 1
    return config["fleet"]["server_cpus"], config["fleet"]["server_memory_mib"] // shares


def owner_host_index(config, job, index):
    return logical_owner_hosts(config, job)[index]["index"]


def controller_matrix(spec):
    if spec.get("selection") == q0_campaign.SELECTION:
        return cloud_campaign.q0_matrix(spec)
    if spec.get("mode") == "focused":
        jobs = cloud_campaign.make_focused_matrix(spec["selection"])
        if spec["selection"] in cloud_campaign.M3_SELECTIONS and "owner_hosts" in spec:
            jobs = [job for job in jobs if cloud_campaign.job_owner_hosts(job) == spec["owner_hosts"]]
        if spec["selection"] in cloud_campaign.WRITE_ENGINE_SELECTIONS:
            arms = spec.get("arms", ["file"])
            if arms not in (["file"], ["prewritten"]):
                raise SafetyError("write-engine fleets require exactly one file or prewritten arm")
            jobs = [job for job in jobs if job["arm"] in arms]
        return jobs
    return cloud_campaign.make_matrix(spec)


def live_trace_diagnostic(job):
    return job.get("nvmeof_route") == "live" and (
        any(job.get("diagnostics", {}).values()) or job.get("cohort") == "cold_os_cache")


def cold_trace_roles(job):
    read_tier = job.get("read_tier", {}).get("name", "")
    sealed = job.get("sealed_dataset", {}).get("profiled") is True
    native = job["backend"] == "native"
    local = read_tier.startswith("native_") or (sealed and native)
    owners = not live_trace_diagnostic(job) and (
        job.get("case_kind") == "block_trace" or read_tier == "cold_device"
        or (sealed and not native and job["backend"] not in cloud_campaign.NVME_BACKENDS))
    return local, owners


def requests_cold_trace(job):
    return live_trace_diagnostic(job) or any(cold_trace_roles(job))


def tide_owner_service_seconds(spec, job, owner_count, remaining):
    """Bound the owner lifetime without ending service before a replay client."""
    if "sealed_dataset" in job:
        return int(remaining)
    deadline = spec["case_deadline_seconds"]
    if job.get("case_kind") == "mainnet_replay":
        deadline = max(deadline, job["measurement_sizing"]["case_deadline_seconds"])
    headroom = 300 + (90 * owner_count if job.get("owner_topology") == "colocated" else 0)
    return min(int(remaining), max(deadline, 600 if job["keys"] > 65536 else 0) + headroom)


def apply_case_namespace(config, job, prefix):
    """Give each synthetic case its own key namespace. Hybrid puts share their dataset's
    keys, and replay keys come from the pinned mainnet fixture, whose typed config has no
    namespace field."""
    if job.get("case_kind") in ("hybrid_put", "mainnet_replay"):
        return
    # Write controls use a separate namespace and run after every sealed read.
    namespace = f"{prefix}:{job['repeat']}"
    if job.get("cohort") == "native_write_regression":
        namespace += ":write-control"
    config["key_namespace_hex"] = hashlib.blake2b(namespace.encode(), digest_size=16).hexdigest()


def bind_native_dataset_config(receipt, config, job):
    """Reopen the prepared database with the case's measured durability."""
    import nvme_campaign_native
    result = nvme_campaign_native.read_config(receipt, config)
    result["backend"]["options"]["durability"] = job["native_durability"]
    return result


def cleanup_native_media_dataset(path, case_directory):
    """Preserve the preparation receipt, then remove the exclusively owned dataset."""
    path = Path(path)
    if path.is_symlink() or not path.is_dir():
        raise InfrastructureError("native dataset ownership or directory is unconfirmed")
    source = path / "receipt.json"
    binding = None
    if source.exists():
        if source.is_symlink() or not source.is_file():
            raise InfrastructureError("native preparation receipt is not an ordinary file")
        target = Path(case_directory) / "native-preparation-receipt.json"
        publish_synced(target, source.read_bytes())
        binding = {"path": target.name, "sha256": digest(target)}
    shutil.rmtree(path)
    if path.exists():
        raise InfrastructureError("native dataset remains after cleanup")
    return binding


def require_hybrid_reader_drain(job, started, drain_proved, client_quiesced):
    if job.get("case_kind") == "hybrid_put" and started and not (drain_proved and client_quiesced):
        raise InfrastructureError(
            f"{job['case_id']}: measured hybrid put run failed or did not complete; "
            "reader drain could not be proven; targets remain quarantined")


def record_phase_span(spans, operation, action, *args, **kwargs):
    """Record a coordinator-clock span without changing action deadlines."""
    start = time.monotonic_ns()
    status = "failed"
    try:
        result = action(*args, **kwargs)
        status = "completed"
        return result
    finally:
        if spans is not None:
            spans.append({"operation": operation, "host_index": 0, "monotonic_start_ns": start,
                          "monotonic_end_ns": time.monotonic_ns(), "status": status})


def block_trace_evidence(case_id, scope, traces, owner_count=5):
    """Report only observed loss and leave file roles unclaimed without proof."""
    rows = traces[1:1 + owner_count] if isinstance(traces, list) and admitted_host_count(len(traces)) else []
    captured = len(rows) == owner_count and all(isinstance(row, dict) and row.get("available") is True for row in rows)
    losses = [row.get("lost") for row in rows] if captured else []
    loss_known = len(losses) == owner_count and all(type(value) is int and value >= 0 for value in losses)
    available = captured and loss_known
    identities = [row.get("device", {}).get("device") for row in rows] if captured else []
    identities_known = len(identities) == owner_count and all(isinstance(value, str) and value for value in identities)
    reasons = ["file_attribution_unproven"] if available else ["trace_capability_or_loss_unavailable"]
    return {"schema_version": 1, "case_id": case_id, "scope": scope, "available": available,
            "qualified": False, "device_identity": ",".join(identities) if identities_known else None,
            "files": [], "lost_events": sum(losses) if loss_known else None,
            "truncated": any(isinstance(row, dict) and row.get("truncated") is True for row in rows),
            "reasons": reasons}



def finalize_standalone_trace(directory, receipt):
    """Remove KV-only files after their workload validation has completed."""
    result = Path(directory) / "result"
    hashes = receipt.get("artifacts_sha256", {})
    removed = {}
    for name in ("manifest.json", "samples.csv"):
        path = result / name
        if path.is_symlink() or not path.is_file() or name not in hashes:
            raise SafetyError("validated trace workload artifact is unavailable")
        removed[name] = hashes[name]
    for name in removed:
        (result / name).unlink()
    receipt["trace_workload_validation"] = {
        "status": "validated_before_standalone_finalization",
        "removed_artifacts_sha256": removed,
        "timing_rows_by_phase": receipt.get("validated_timing_rows_by_phase"),
    }
    receipt["raw_artifacts_validated"] = False


def inspect_process_fd(directory, entry, *, strict):
    """Read a bounded, self-consistent descriptor observation."""
    def descriptor_info():
        info = (directory.parent / "fdinfo" / entry.name).read_text()
        fields = dict(line.split(":", 1) for line in info.splitlines() if ":" in line)
        return int(fields["flags"].strip(), 8), (int(fields["ino"].strip()) if "ino" in fields else None)

    for attempt in range(32):
        try:
            flags, fd_inode = descriptor_info()
            identity = entry.stat()
            target = os.readlink(entry)
            final_flags, final_inode = descriptor_info()
            final_identity = entry.stat()
            final_target = os.readlink(entry)
            if (flags != final_flags or fd_inode != final_inode
                    or (fd_inode is not None and fd_inode != identity.st_ino)
                    or (identity.st_dev, identity.st_ino) != (final_identity.st_dev, final_identity.st_ino)
                    or target != final_target):
                raise ValueError("descriptor changed during inspection")
            return flags, identity, target
        except (OSError, ValueError, KeyError) as exc:
            if not os.path.lexists(entry):
                return None
            if attempt == 31:
                if strict:
                    raise SafetyError(f"cannot inspect writable fd {entry.name}: {exc}") from exc
                return None
            time.sleep(.001)
    return None


def fio_open_flags(pid, device, inode):
    """Read the fio process fd flags for the exact opened target inode."""
    directory = Path("/proc") / str(pid) / "fd"
    try:
        entries = list(directory.iterdir())
    except OSError:
        return None
    if len(entries) > 256:
        return None
    flags = []
    for entry in entries:
        try:
            identity = entry.stat()
        except OSError:
            continue
        if (identity.st_dev, identity.st_ino) != (device, inode):
            continue
        observed = inspect_process_fd(directory, entry, strict=False)
        if observed is not None:
            value, identity, _ = observed
            if (identity.st_dev, identity.st_ino) == (device, inode):
                flags.append(value)
    if not flags:
        return None
    return {"read_only": all(value & os.O_ACCMODE == os.O_RDONLY for value in flags),
            "direct": any(value & os.O_DIRECT != 0 for value in flags)}



def provision_fio_scratch(scratch_root, case_id, size=2 * 1024**3, deadline=None, *, pattern="constant", retain_cache=False):
    """Create and sync a new bounded scratch file, never an existing data file."""
    if (not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", case_id)
            or type(size) is not int or not 0 < size <= 2 * 1024**3
            or pattern not in ("constant", "random", "sparse")):
        raise ValueError("invalid scratch identity, bound, or pattern")
    root = Path(scratch_root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    dir_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    name = case_id + ".fio-scratch"
    fd = None
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=dir_fd)
        if pattern == "sparse":
            if deadline is not None and time.monotonic() >= deadline:
                raise DeadlineError("scratch provisioning deadline exceeded")
            os.ftruncate(fd, size)
            if os.fstat(fd).st_blocks != 0:
                raise SafetyError("append scratch is not sparse")
        else:
            chunk = bytes([0xA5]) * (1024 * 1024) if pattern == "constant" else None
            remaining = size
            while remaining:
                if deadline is not None and time.monotonic() >= deadline:
                    raise DeadlineError("scratch provisioning deadline exceeded")
                data = os.urandom(min(1024 * 1024, remaining)) if pattern == "random" else memoryview(chunk)[:min(len(chunk), remaining)]
                written = os.write(fd, data)
                if written <= 0:
                    raise OSError("short scratch write")
                remaining -= written
        os.fsync(fd)
        if not retain_cache:
            os.posix_fadvise(fd, 0, size, os.POSIX_FADV_DONTNEED)
        identity = os.fstat(fd)
        return {"path": str(root / name), "size": identity.st_size,
                "device": identity.st_dev, "inode": identity.st_ino}
    except BaseException:
        if fd is not None:
            os.close(fd)
            fd = None
            os.unlink(name, dir_fd=dir_fd)
            if retain_cache:
                try:
                    os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
                except FileNotFoundError:
                    pass
                else:
                    raise InfrastructureError("partial scratch cleanup unconfirmed")
        raise
    finally:
        if fd is not None:
            os.close(fd)
        os.close(dir_fd)


def remove_fio_scratch(scratch_root, case_id, identity):
    """Remove only the file with the identity returned by provision."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", case_id):
        raise ValueError("invalid scratch identity")
    root = Path(scratch_root)
    dir_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        name = case_id + ".fio-scratch"
        current = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (identity["device"], identity["inode"]):
            raise SafetyError("scratch file identity changed; refusing cleanup")
        os.unlink(name, dir_fd=dir_fd)
    finally:
        os.close(dir_fd)


def fio_read_command(binary, target, fio, region_bytes, *, runtime_seconds=10):
    """Build only the reviewed single-job read-only fio command.

    The engine is io_uring: it is built into the fio binary and the guest image
    qualification proves it, while the guest fio package ships no libaio engine.
    """
    if (fio.get("location") not in ("local_file", "nvmeof_namespace")
            or fio.get("block_bytes") not in (4096, 65536) or fio.get("queue_depth") not in (1, 16)
            or fio.get("runtime_seconds") != runtime_seconds or fio.get("direct") is not True
            or fio.get("pattern") != "randread" or type(region_bytes) is not int or region_bytes < fio["block_bytes"]):
        raise ValueError("invalid reviewed fio dimensions")
    return [str(binary), "--name=focused-read", "--numjobs=1", "--output-format=json",
            "--readonly", "--direct=1", "--rw=randread", "--ioengine=io_uring",
            "--filename=" + str(target), "--bs=" + str(fio["block_bytes"]),
            "--iodepth=" + str(fio["queue_depth"]), "--time_based=1",
            "--runtime=" + str(runtime_seconds), "--size=" + str(region_bytes), "--thread=1"]


def run_readonly_fio(argv, device, inode, timeout):
    """Run bounded fio and require an observed read-only direct target fd."""
    if type(timeout) not in (int, float) or not 0 < timeout <= 60:
        raise ValueError("invalid fio deadline")
    start = time.monotonic()
    observed = None
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        process = subprocess.Popen(argv, stdout=stdout, stderr=stderr)
        try:
            while process.poll() is None:
                if time.monotonic() - start >= timeout:
                    raise subprocess.TimeoutExpired(argv, timeout)
                flags = fio_open_flags(process.pid, device, inode)
                if flags is not None:
                    if not flags["read_only"]:
                        raise SafetyError("fio opened the target with write access")
                    if flags["direct"]:
                        observed = flags
                time.sleep(0.02)
            if process.returncode != 0 or observed is None:
                stderr.seek(0, os.SEEK_END)
                stderr.seek(max(0, stderr.tell() - 2048))
                error_text = stderr.read(2048).decode("utf-8", errors="replace")
                stdout.seek(0)
                output_text = stdout.read(1024).decode("utf-8", errors="replace")
                reason = ("fio failed" if process.returncode != 0 else
                          "fio direct read-only target fd was not observed")
                raise FioFailure(f"{reason}; exit status {process.returncode}; "
                                 f"stderr tail: {error_text}; stdout prefix: {output_text}")
            stdout.seek(0)
            raw = stdout.read(MAX_JSON_BYTES + 1)
            if len(raw) > MAX_JSON_BYTES:
                raise ValueError("fio output exceeds the bound")
            return raw, observed
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)


def write_fio_flags(pid, device, inode, case_root, pipe_inodes):
    """Inspect every writable descriptor, rejecting unowned destinations."""
    directory = Path("/proc") / str(pid) / "fd"
    try:
        entries = list(directory.iterdir())
    except OSError:
        return None
    if len(entries) > 256:
        raise SafetyError("too many fio descriptors")
    observed = None
    for entry in entries:
        inspection = inspect_process_fd(directory, entry, strict=True)
        if inspection is None:
            continue
        flags, identity, target = inspection
        if flags & os.O_ACCMODE == os.O_RDONLY:
            continue
        if stat.S_ISREG(identity.st_mode) and (identity.st_dev, identity.st_ino) == (device, inode):
            observed = {"direct": bool(flags & os.O_DIRECT)}
        elif stat.S_ISREG(identity.st_mode) and Path(target).is_absolute() and Path(target).resolve().is_relative_to(case_root.resolve()):
            continue
        elif stat.S_ISFIFO(identity.st_mode) and identity.st_ino in pipe_inodes and entry.name in ("1", "2"):
            continue
        elif stat.S_ISCHR(identity.st_mode) and identity.st_rdev == os.makedev(1, 3):
            continue
        else:
            raise SafetyError(f"fio writable fd {entry.name} has unsafe target {target}")
    return observed


def run_write_fio(argv, identity, scratch_root, case_root, timeout):
    target = Path(identity["path"])
    if (len(argv) != 11 or argv[1] != str(scratch_root) or argv[2] != target.name
            or target.parent != Path(scratch_root) or target.is_symlink() or not target.is_file()
            or (target.stat().st_dev, target.stat().st_ino) != (identity["device"], identity["inode"])
            or not Path(argv[3]).resolve().is_relative_to(Path(case_root).resolve())
            or argv[9:] != [str(identity["device"]), str(identity["inode"])]):
        raise SafetyError("unsafe fio write argv or scratch identity")
    if type(timeout) not in (int, float) or not 0 < timeout <= 60:
        raise ValueError("invalid fio deadline")
    with open(os.devnull, "rb") as null:
        process = subprocess.Popen(argv, stdin=null, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    pipe_inodes = {os.fstat(process.stdout.fileno()).st_ino, os.fstat(process.stderr.fileno()).st_ino}
    captured = []
    collector = threading.Thread(target=lambda: captured.append(process.communicate()), daemon=True)
    collector.start()
    start = time.monotonic()
    observed = None
    try:
        while process.poll() is None:
            elapsed = time.monotonic() - start
            if elapsed >= timeout:
                raise FioFailure(f"SSD write helper timed out after {timeout} seconds")
            flags = write_fio_flags(process.pid, identity["device"], identity["inode"], case_root, pipe_inodes)
            if flags is not None:
                observed = flags
            elif observed is None and elapsed >= 2:
                raise FioFailure("fio scratch writable fd not observed")
            time.sleep(.01)
        collector.join(timeout=10)
        if collector.is_alive():
            raise InfrastructureError("fio output collection did not finish")
        raw, error = captured[0]
        if process.returncode != 0 or observed is None:
            raise FioFailure(f"write helper exit status {process.returncode}; " + error[-2048:].decode(errors="replace"))
        if len(raw) > MAX_JSON_BYTES:
            raise FioFailure("fio output exceeds bound")
        return raw, observed
    finally:
        try:
            if floors.process_group_alive(process.pid):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait(timeout=10)
            until = time.monotonic() + 2
            while floors.process_group_alive(process.pid) and time.monotonic() < until:
                time.sleep(.01)
            if floors.process_group_alive(process.pid):
                raise InfrastructureError("write helper process group cleanup unconfirmed")
            collector.join(timeout=10)
            if collector.is_alive():
                raise InfrastructureError("write helper output cleanup unconfirmed")
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise InfrastructureError("write helper process cleanup unconfirmed: " + repr(exc)) from exc


def fio_result_from_raw(job, raw_path, *, device_identity, namespace_uuid, region_bytes,
                        opened_read_only, actual_direct, quiescent, target_buffered_io):
    """Bind a single fio job to its raw JSON and the observed open policy."""
    raw = Path(raw_path).read_bytes()
    document = json.loads(raw)
    if not isinstance(document.get("jobs"), list) or len(document["jobs"]) != 1:
        raise ValueError("fio must report exactly one job")
    result = document["jobs"][0]
    read = result["read"]
    percentiles = read["clat_ns"]["percentile"]
    depth = job["fio"]["queue_depth"]
    level = result.get("iodepth_level", {}).get(str(depth))
    effective = depth if type(level) in (int, float) and math.isfinite(level) and 90 <= level <= 100 else None
    # fio's per-direction runtime is in milliseconds; job elapsed is rounded
    # to whole seconds and can materially misstate short-run throughput.
    elapsed = read["runtime"] / 1000 if "runtime" in read else result.get("elapsed")
    evidence = {
        "schema_version": 1, "case_id": job["case_id"], "location": job["fio"]["location"],
        "device_identity": device_identity, "namespace_uuid": namespace_uuid,
        "block_bytes": job["fio"]["block_bytes"], "requested_depth": depth, "effective_depth": effective,
        "actual_direct": actual_direct, "read_only": opened_read_only, "opened_read_only": opened_read_only,
        "quiescent": quiescent, "target_buffered_io": target_buffered_io, "region_bytes": region_bytes,
        "elapsed_seconds": elapsed, "read_ios": read["total_ios"], "read_bytes": read["io_bytes"],
        "write_ios": result["write"]["total_ios"], "errors": result["error"],
        "latency_ns": {"p50": percentiles["50.000000"], "p99": percentiles["99.000000"],
                       "p999": percentiles["99.900000"]},
        "fio_sha256": hashlib.sha256(raw).hexdigest(), "raw_fio_path": Path(raw_path).name,
    }
    analyze._validate_focused_artifact("fio_result", evidence, job, {})
    if analyze._verify_fio_raw(raw, evidence)[0] is not True:
        raise ValueError("fio raw output lacks required observations")
    return evidence


def media_device_snapshot(root, owner_index, diskstats):
    """Identify the physical device backing the owned data directory."""
    import cold_device_trace
    mapping = cold_device_trace.resolve_device(root)
    node = Path(mapping["sysfs_path"])
    model = (node / "device" / "model").read_text().strip()
    if not physical_storage.admitted_model("gcp", model):
        raise ValueError("backing device is not a proven GCP Local SSD")
    major, minor = map(int, mapping["device"].split(":"))
    matches = []
    for line in diskstats.splitlines():
        fields = line.split()
        if len(fields) >= 10 and fields[0] == str(major) and fields[1] == str(minor):
            matches.append(tuple(int(fields[index]) for index in (3, 5, 7, 9)))
    if len(matches) != 1 or any(value < 0 for value in matches[0]):
        raise ValueError("physical device counters unavailable")
    return dict(zip(("owner_index", "major", "minor", "read_ops", "read_sectors", "write_ops", "write_sectors"),
                    (owner_index, major, minor, *matches[0])))


def owner_case_device_snapshot(root, owner_index):
    """Record the owner Local SSD before WAL creation or after WAL removal."""
    try:
        result = media_device_snapshot(root, owner_index, Path("/proc/diskstats").read_text())
    except (OSError, ValueError, KeyError) as exc:
        # Missing observational evidence cannot turn a proven owner teardown into a failure.
        result = {"owner_index": owner_index, "device_error": f"{type(exc).__name__}: {exc}"}
    try:
        space = os.statvfs(root)
        result["wal_fs_free_bytes"] = space.f_bfree * space.f_frsize
    except OSError as exc:
        result["wal_fs_free_bytes"] = None
        result["wal_fs_free_error"] = str(exc)
    return result


def qualify_media_evidence(case_id, media, before, after, successful_reads, drained,
                           history_before=None, history_after=None, prewarm_verified=None, *, client=False):
    """Qualify matching device counters from a drained owner or client window."""
    reasons = []
    rows = []
    ratio = None
    bytes_per_read = None
    counters = ("read_ops", "read_sectors", "write_ops", "write_sectors")
    if drained is not True:
        reasons.append("measurement_not_drained")
    valid_count = type(successful_reads) is int and 0 < successful_reads <= 2**64 - 1
    if not valid_count:
        reasons.append("successful_reads_unavailable")
    def indexed(values):
        if not isinstance(values, list) or not values:
            raise ValueError("missing_devices")
        result = {}
        for row in values:
            if not isinstance(row, dict) or set(row) != {"owner_index", "major", "minor", *counters}:
                raise ValueError("invalid_device_counter")
            if (type(row["owner_index"]) is not int
                    or (row["owner_index"] != -1 if client else not 0 <= row["owner_index"] <= 2**64 - 1)
                    or any(type(row[key]) is not int or not 0 <= row[key] <= 2**64 - 1
                           for key in ("major", "minor", *counters))):
                raise ValueError("invalid_device_counter")
            identity = (row["owner_index"], row["major"], row["minor"])
            if identity in result:
                raise ValueError("duplicate_device")
            result[identity] = row
        if client and len(result) != 1:
            raise ValueError("invalid_client_device_count")
        return result
    try:
        first, last = indexed(before), indexed(after)
        if first.keys() != last.keys():
            raise ValueError("device_identity_mismatch")
        if any(last[key][counter] < first[key][counter] for key in first for counter in counters):
            raise ValueError("device_counter_decreased")
        rows = [{"owner_index": owner, "major": major, "minor": minor,
                 **{f"{counter}_{boundary}": source[(owner, major, minor)][counter]
                    for counter in counters for boundary, source in (("before", first), ("after", last))}}
                for owner, major, minor in sorted(first)]
        if valid_count:
            ratio = sum(row["read_ops_after"] - row["read_ops_before"] for row in rows) / successful_reads
        read_ops = sum(row["read_ops_after"] - row["read_ops_before"] for row in rows)
        if read_ops:
            bytes_per_read = 512 * sum(row["read_sectors_after"] - row["read_sectors_before"] for row in rows) / read_ops
    except ValueError as exc:
        reasons.append(str(exc))
    if ratio is not None:
        if media["expected"] == "cold" and ratio < 0.9:
            reasons.append("cold_read_ratio_below_threshold")
        if media["expected"] == "memory" and ratio > 0:
            reasons.append("memory_read_ratio_above_threshold")
    if media.get("history") == "prewarm":
        reasons.append("history_baseline_compared_in_report")
    if not (type(history_before) is int and type(history_after) is int
            and 0 <= history_before <= history_after <= 2**64 - 1):
        history_before = history_after = None
        if media.get("history") == "prewarm":
            reasons.append("history_counter_unavailable")
    evidence = {"schema_version": 2, "case_id": case_id, "expected": media["expected"],
                "status": "unqualified" if reasons else "qualified", "reason_codes": reasons,
                "measurement_drained": drained is True, "successful_reads": successful_reads if valid_count else 0,
                "read_ops_per_success": ratio, "device_bytes_per_read": bytes_per_read, "history_loads_before": history_before,
                "history_loads_after": history_after, "history_prewarm_verified": prewarm_verified}
    if client:
        evidence["client_devices"] = [{"client_index": row["owner_index"], **{key: value for key, value in row.items()
                                     if key != "owner_index"}} for row in rows]
    else:
        evidence["owner_devices"] = rows
    return evidence

def store_measurement_device_evidence(directory, receipt, job, media, before, after, owner_count):
    if media is not None:
        path = directory / "media_evidence.json"
        dump(path, media)
        receipt["media_evidence"] = {"path": path.name, "sha256": digest(path)}
    if job.get("device_counters"):
        native = job.get("backend") == "native"
        count = 1 if native else min(owner_count, cloud_campaign.job_owner_hosts(job))
        start = 0 if native else 1
        try:
            counters = {
                "before": [host["media_device"] for host in before[start:start + count]],
                "after": [host["media_device"] for host in after[start:start + count]],
            }
            if len(counters["before"]) != count or len(counters["after"]) != count:
                raise KeyError("missing device snapshot")
        except (KeyError, TypeError) as exc:
            raise SafetyError("client device counters unavailable" if native else "owner device counters unavailable") from exc
        path = directory / "device_counters.json"
        dump(path, counters)
        receipt["device_counters"] = {"path": path.name, "sha256": digest(path)}
    if job.get("require_media_qualified") and (not media or media["status"] != "qualified"):
        raise SafetyError("required cold canary is not device-served")



def owner_history_boundaries(evidence, owner_count=5):
    """Read existing authority counters at the benchmark's measurement gates."""
    try:
        if evidence["measurement_drained"] is not True:
            return None, None
        totals = []
        for side in ("start", "end"):
            owner = evidence[side]["owner_executor"]
            partitions = owner["partitions"]
            if owner["available"] is not True or set(partitions) != {str(i) for i in range(owner_count)}:
                return None, None
            values = [partitions[str(i)]["authority"]["history_loads"] for i in range(owner_count)]
            if any(type(v) is not int or not 0 <= v <= 2**64 - 1 for v in values):
                return None, None
            totals.append(sum(values))
        if totals[1] < totals[0] or any(v > 2**64 - 1 for v in totals):
            return None, None
        for i in range(owner_count):
            if (evidence["end"]["owner_executor"]["partitions"][str(i)]["authority"]["history_loads"]
                    < evidence["start"]["owner_executor"]["partitions"][str(i)]["authority"]["history_loads"]):
                return None, None
        return tuple(totals)
    except (KeyError, TypeError):
        return None, None


def measurement_media_evidence(job, samples, batch_size, before, ended, host_count, owner_count, drained, read_interval):
    native = job["case_kind"] == "native_media_read"
    history = owner_history_boundaries(read_interval["observed"] if read_interval else {}, owner_count)
    try:
        if len(before) != host_count + 1 or len(ended) != host_count + 1:
            raise ValueError("missing host snapshots")
        first_hosts = before[:1] if native else before[1:1 + owner_count]
        last_hosts = ended[:1] if native else ended[1:1 + owner_count]
        for first, last in zip(first_hosts, last_hosts):
            boot = first["nvme"]["host_identity"]["boot_id"]
            if not isinstance(boot, str) or not boot or boot != last["nvme"]["host_identity"]["boot_id"]:
                raise ValueError("media host boot identity changed")
        first_devices = [host["media_device"] for host in first_hosts]
        last_devices = [host["media_device"] for host in last_hosts]
    except (ValueError, KeyError, TypeError):
        first_devices = last_devices = None
    successful = analyze.successful_read_keys(samples, batch_size)
    return qualify_media_evidence(job["case_id"], job["media"], first_devices, last_devices,
                                  successful, drained, *history, client=native)


def require_supported_nvme_route(job):
    """Keep immutable sealed reads separate from live writable namespaces."""
    route = job.get("nvmeof_route")
    target = job.get("nvmeof_target", "kernel")
    if job.get("target_io") == "direct":
        cold_read = (job.get("case_kind") == "media_read" and job.get("group") in ("G1", "G10R", "G12D", "G16C")
                     and job.get("media", {}).get("expected") == "cold"
                     and job.get("mix") == cloud_campaign.mix(read=100))
        hybrid_write = (job.get("case_kind") == "hybrid_put" and job.get("group") in ("G12H", "G12D")
                        and job.get("mix") == cloud_campaign.mix(put=100))
        tcp_direct = (job.get("backend") == "tidehunter_nvme_tcp"
                      and job.get("selection") == "fabric_ab_compact" and job.get("group") == "G16C")
        if ((job.get("backend") != "tidehunter_nvme_rdma" and not tcp_direct)
                or route != "sealed_read" or target != "kernel" or not (cold_read or hybrid_write)):
            raise SafetyError("direct target I/O requires a reviewed sealed kernel case")
    if target not in ("kernel", "spdk") or target == "spdk" and (
            route not in ("sealed_read", "live") or job.get("backend") != "tidehunter_nvme_rdma"):
        raise SafetyError("SPDK requires explicit sealed or live RDMA selection")
    if job.get("backend") not in cloud_campaign.NVME_BACKENDS:
        if route is not None:
            raise SafetyError("NVMe-oF route requires an NVMe backend")
        return
    if route == "live":
        if "sealed_dataset" in job:
            raise SafetyError("live NVMe route cannot use a sealed dataset")
        return
    if route not in (None, "sealed_read") or "sealed_dataset" not in job:
        raise SafetyError("NVMe-oF execution requires an explicitly supported sealed dataset route")




class SnapshotTimeout(InfrastructureError):
    """A read-only snapshot did not produce an admitted receipt in time."""


def read_only_snapshot(request):
    # Cache eviction and monitoring selection are mutations, even though they
    # are bundled into the snapshot action.
    return (request.get("action") == "snapshot" and request.get("drop", False) is False
            and request.get("monitor_job") is None)


class DeadlineError(InfrastructureError):
    pass


class CampaignTimeLimit(DeadlineError):
    """Normal scheduling exhaustion, not an infrastructure failure."""


def utc():
    return datetime.now(timezone.utc)


def timestamp(value):
    if not isinstance(value, str):
        raise SafetyError("UTC timestamp must be a string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SafetyError("invalid absolute UTC deadline") from exc
    if result.tzinfo is None or result.utcoffset().total_seconds() != 0:
        raise SafetyError("explicit UTC timezone required")
    return result


def dump(path, value):
    path = Path(path).absolute()
    guarded(path, path.parent)
    guarded(path.with_suffix(path.suffix + ".tmp"), path.parent)
    local.dump(path, value)

def dump_compact(path, value):
    path = Path(path).absolute()
    guarded(path, path.parent)
    temporary = guarded(path.with_suffix(path.suffix + ".tmp"), path.parent)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def publish_synced(path, data):
    """Temporary file, fsync, rename, directory fsync: a reader never sees a partial file."""
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".pending")
    with temporary.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def owner_durability_receipt(launch):
    """Receipt the owner process published next to its ready file; absent or foreign means failure.

    It must name this exact owner (partition, database, address), this launch (nonce) and an
    explicit tier equal to the launch tier (omitted there means native).
    """
    try:
        return cloud_campaign.read_owner_receipt(launch)
    except FileNotFoundError as exc:
        raise InfrastructureError("owner did not publish its durability receipt") from exc
    except (KeyError, TypeError, ValueError) as exc:
        raise SafetyError("owner durability receipt differs from its launch") from exc


def confirm_owner_durability(proof, tier, index, nonce):
    receipt = proof.get("durability_receipt")
    durability = receipt.get("durability") if isinstance(receipt, dict) else None
    if (not isinstance(durability, dict) or durability.get("tier") != tier
            or receipt.get("partition") != index or receipt.get("launch_nonce") != nonce):
        raise SafetyError(f"owner {index} did not confirm durability tier {tier} for its launch")
    return receipt


def digest(path, algorithm="sha256"):
    return campaign.digest(Path(path), algorithm)


def guarded(path, root, *, must_exist=False):
    path, root = Path(path), Path(root)
    if not path.is_absolute() or str(path) != str(path.absolute()) or ".." in path.parts:
        raise SafetyError("canonical absolute path required")
    if path != root and root not in path.parents:
        raise SafetyError("path escapes exact run root")
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise SafetyError("symlink in owned path")
    if must_exist and not path.exists():
        raise SafetyError("required owned path missing: " + str(path))
    return path

def validate_read_tier_memory(jobs, fleet):
    """Reserve headroom for non-cache work within the exact guest cgroup caps."""
    for job in jobs:
        if "read_tier" not in job or job["backend"] not in cloud_campaign.TRANSPORTS:
            continue
        for prefix, cap_name in (("coordinator", "client_memory_mib"), ("owner_read", "server_memory_mib")):
            allowance = job[prefix + "_read_memory_allowance_bytes"] if prefix == "coordinator" else job["owner_read_memory_allowance_bytes"]
            cache = job["coordinator_cache_bytes"] if prefix == "coordinator" else 0
            cap = fleet[cap_name] * 1024**2 * 3 // 4
            if allowance < cache + 64 * 1024**2 or allowance > cap:
                raise ValueError(prefix + " read allowance exceeds the bounded fleet memory budget or lacks pending-read headroom")

def tracer_terminal(result):
    """Only a completed exact-owned collector result proves teardown finished."""
    rows = ([row for row in result[1:] if not (isinstance(row, dict) and row.get("inactive") is True)]
            if isinstance(result, list) and admitted_host_count(len(result)) else [result])
    return bool(rows) and all(isinstance(row, dict) and isinstance(row.get("state_dir"), str)
        and not row.get("cleanup_error") and row.get("reason") != "collector stop timed out"
        for row in rows)


def cleanup_unfinished_tracer(attempted, terminal, stop, receipt):
    if not attempted or terminal:
        return True
    try:
        result = stop()
        rows = result if isinstance(result, list) else [result]
        receipt["cold_device_cleanup"] = [
            {key: value for key, value in row.items()
             if key not in ("raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns")}
            for row in rows]
        complete = tracer_terminal(result)
    except Exception as error:
        receipt["cold_device_cleanup"] = {"available": False, "reason": repr(error)}
        complete = False
    receipt["cold_device_cleanup_complete"] = complete
    return complete


def terminal_device_evidence(job, receipt):
    """Keep node-local evidence separate and never invent missing host results."""
    evidence = receipt.get("cold_device_evidence")
    if isinstance(evidence, list) and admitted_host_count(len(evidence)):
        rows = [(index, value) for index, value in enumerate(evidence[1:1 + job.get("owner_count", 5)])]
    elif job.get("backend") == "native" and isinstance(evidence, dict):
        rows = [(-1, evidence)]
    else:
        return None
    return [{"host_index": index, "evidence": {
        key: value for key, value in row.items()
        if key not in ("raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns")}}
        for index, row in rows if isinstance(row, dict)]


def probe_trace_capability(script, wal_root, state, duration, env):
    """Attach and stop once; zero I/O is not a capability failure."""
    start = {"available": False, "reason": "probe did not start"}
    stop = {"available": False, "reason": "probe did not stop"}
    try:
        start = json.loads(command([sys.executable, "-B", script, "start", "--wal-path", wal_root,
                                    "--state-dir", state, "--duration-seconds", str(duration)],
                                   timeout=45, env=env))
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        start = {"available": False, "reason": type(error).__name__ + ": " + str(error)}
    finally:
        try:
            stop = json.loads(command([sys.executable, "-B", script, "stop", "--state-dir", state],
                                      timeout=45, env=env))
        except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
            stop = {"available": False, "reason": type(error).__name__ + ": " + str(error)}
    cleanup = tracer_terminal(stop)
    ready = (cleanup and start.get("available") is True and stop.get("available") is True
             and start.get("backend") == "bpftrace-request-identity"
             and stop.get("backend") == "bpftrace-request-identity")
    compact = {key: value for key, value in stop.items()
               if key not in ("raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns")}
    return {"identity_backend_available": ready, "cleanup_complete": cleanup,
            "start": start, "stop": compact,
            "gaps": [] if ready else ["request-identity BPF attach/stop unavailable"],
            "scope": "attachment capability only; zero block events do not indicate failure or establish device timing"}



def platform_module(config):
    if not isinstance(config, dict):
        raise SafetyError("runtime configuration must be an object")
    platform = config.get("platform")
    if platform is None:
        return None
    if not isinstance(platform, dict) or platform.get("name") != "gcp":
        raise SafetyError("unsupported explicit cloud platform")
    directory = str(Path(__file__).resolve().parents[1] / "gcp-rdma")
    if directory not in sys.path:
        sys.path.insert(0, directory)
    import gcp_runtime
    return gcp_runtime


def validate_config(config):
    if isinstance(config, dict) and ("local_mode" in config
            or isinstance(config.get("spec"), dict) and "local_mode" in config["spec"]):
        raise SafetyError("local mode is never selectable from a cloud configuration")
    platform = platform_module(config)
    if platform:
        return platform.validate_runtime(config)
    required = {"fleet", "bucket", "bucket_region", "prefix", "account", "source_revision",
                "source_sha256", "binary_sha256", "binary_blake2b512", "repository_root",
                "binary", "fdb_library_dir", "deadline_utc", "spec"}
    if not isinstance(config, dict) or set(config) != required:
        raise SafetyError("runtime config requires its exact documented schema")
    if set(config.get("spec", {}).get("backends", ())) & cloud_campaign.PORTABLE_BACKENDS:
        raise SafetyError("AWS EFA does not admit portable verbs or Valkey RDMA backends")
    fleet = cloud_layout.validate_fleet(config["fleet"])
    if fleet.get("qualification_only", False):
        raise SafetyError("cloud controller refuses nonphysical qualification fleets")
    if config["account"] != ACCOUNT or config["prefix"] != fleet["run_id"]:
        raise SafetyError("account/prefix differs from exact owned run")
    if not re.fullmatch(r"th-[a-f0-9]{16}", fleet["run_id"]):
        raise SafetyError("cloud run must use independently watched th-<16hex> identity")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", config["bucket"]):
        raise SafetyError("invalid S3 bucket")
    for region in (fleet.get("region"), config["bucket_region"]):
        if not isinstance(region, str) or not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]", region):
            raise SafetyError("explicit AWS regions required")
    for host in [fleet["coordinator"], *fleet["nodes"]]:
        if not re.fullmatch(r"i-(?:[a-f0-9]{8}|[a-f0-9]{17})", host["instance_id"]):
            raise SafetyError("exact EC2 instance IDs required")
    for name, width in (("source_sha256", 64), ("binary_sha256", 64), ("binary_blake2b512", 128)):
        if not isinstance(config[name], str) or not re.fullmatch(r"[a-f0-9]{%d}" % width, config[name]):
            raise SafetyError("invalid admitted " + name)
    if not isinstance(config["source_revision"], str) or not re.fullmatch(r"[a-f0-9]{40}", config["source_revision"]):
        raise SafetyError("full source revision required in addition to dirty source snapshot")
    for name in ("repository_root", "binary", "fdb_library_dir"):
        value = Path(config[name])
        if not value.is_absolute() or ".." in value.parts or str(value) != config[name] or value == Path("/"):
            raise SafetyError("canonical absolute installed package paths required")
    stage = Path(config["repository_root"]).parent
    if Path(config["repository_root"]).name != "source":
        raise SafetyError("repository_root must be admitted package's source directory")
    for name in ("binary", "fdb_library_dir"):
        guarded(config[name], stage)
    timestamp(config["deadline_utc"])
    validate_read_tier_memory(controller_matrix(config["spec"]), fleet)
    guarded(fleet["data_root"], Path("/mnt/tidehunter-kv-comparison"))
    return config


def imds_identity():
    base = "http://169.254.169.254/latest"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(base + "/api/token", method="PUT",
                                     headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    with opener.open(request, timeout=3) as response:
        token = response.read(4096).decode()
    request = urllib.request.Request(base + "/dynamic/instance-identity/document",
                                     headers={"X-aws-ec2-metadata-token": token})
    with opener.open(request, timeout=3) as response:
        return json.load(response)


def verify_local_identity(config, index):
    platform = platform_module(config)
    if platform:
        return platform.verify_local_identity(config, index)
    identity = imds_identity()
    host = config["fleet"]["coordinator"] if index == -1 else config["fleet"]["nodes"][index]
    if (identity.get("accountId") != config["account"] or identity.get("instanceId") != host["instance_id"]
            or identity.get("privateIp") != host["address"] or identity.get("region") != config["fleet"]["region"]):
        raise SafetyError("guest IMDS identity differs from exact configured host")
    return identity


def verify_package(config):
    import cloud_package
    stage = Path(config["repository_root"]).parent
    for key, relative in (("binary", "runtime/bin/kv-research"), ("fdb_library_dir", "runtime/lib")):
        if config[key] != str(stage / relative):
            raise SafetyError("configured runtime path differs from sealed package layout")
    try:
        options = {"runtime": True}
        if platform_module(config):
            options["provider"] = "gcp"
        admission = cloud_package.verify_directory(stage, **options)
        if config["spec"].get("selection") == q0_campaign.SELECTION:
            cloud_package.admit_case_binaries(stage, config["spec"]["case_binaries"])
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        raise SafetyError("sealed source/runtime package admission failed: " + str(exc)) from exc
    inventory = json.loads((stage / "source.json").read_text())
    if (admission["source_sha256"] != config["source_sha256"] or inventory["revision"] != config["source_revision"]
            or admission["binary_sha256"] != config["binary_sha256"]
            or admission["binary_blake2b512"] != config["binary_blake2b512"]):
        raise SafetyError("sealed package differs from externally admitted runtime hashes")
    focused = config["spec"].get("mode") == "focused"
    if focused:
        expected_profile = "focused_valkey" if config["spec"].get("selection") == "valkey_rdma_reads" else "focused"
        if admission.get("build_profile") != expected_profile:
            raise SafetyError("focused campaign requires the focused package profile")
        fdb = None
    else:
        if admission.get("build_profile") is not None:
            raise SafetyError("full campaign cannot use a focused package")
        library = stage / "runtime/lib/libfdb_c.so"
        fdb = {**json.loads((stage / "runtime/fdb-client.json").read_text()), "path": str(library)}
    return {"source": inventory, "admission": admission,
            "binary_sha256": admission["binary_sha256"], "binary_blake2b512": admission["binary_blake2b512"],
            "fdb": fdb, "manifest": json.loads((stage / "manifest.json").read_text()),
            "build": json.loads((stage / "build.json").read_text())}


def environment(config):
    result = dict(os.environ)
    for key in ("LD_PRELOAD", "LD_AUDIT", "FI_EFA_USE_DEVICE_RDMA"):
        result.pop(key, None)
    library_path = config["fdb_library_dir"]
    if not platform_module(config):
        # The sealed AWS package still links libfabric from the pinned installer.
        # Its path is linkage provenance, not an EFA transport selection.
        library_path += ":/opt/amazon/efa/lib"
    result.update(LD_LIBRARY_PATH=library_path,
                  FDB_CLIENT_LIB_DIR=config["fdb_library_dir"],
                  FDB_CLIENT_LIB_PATH=config["fdb_library_dir"])
    return result


def command(argv, *, timeout=60, env=None):
    result = subprocess.run([str(x) for x in argv], capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        raise InfrastructureError(f"command failed ({result.returncode}): {shlex.join([str(x) for x in argv])}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def admit_fabrics(config, index, clients, root):
    platform = platform_module(config)
    if platform:
        return platform.admit_fabrics(config, index, clients, root)
    host = config["fleet"]["coordinator"] if index == -1 else config["fleet"]["nodes"][index]
    receipt = {"complete": True, "run_id": config["prefix"], "instance_id": host["instance_id"],
               "source_sha256": config["source_sha256"], "binary_sha256": config["binary_sha256"],
               "scope": "TCP control; no OFI provider probes required", "probes": {}}
    dump(Path(root) / "fabric-admission.json", receipt)
    key = config["prefix"] + ("/results/fabric-admission.json" if index == -1
                              else f"/results/cells/startup/storage/{index}/fabric-admission.json")
    persist_json(clients, config, key, receipt)
    return receipt


def make_clients(config, *, guest=False):
    platform = platform_module(config)
    if platform:
        return platform.make_clients(config, guest=guest)
    # Delayed: offline plan/admission must not require boto3 or credentials.
    import boto3
    from botocore.config import Config
    session = boto3.Session(region_name=config["fleet"]["region"])
    credentials = session.get_credentials()
    if credentials is None or credentials.method != "iam-role":
        raise SafetyError("only EC2 instance-profile credentials are admitted")
    common = dict(connect_timeout=4, read_timeout=15, max_pool_connections=12)
    clients = {"s3": session.client("s3", region_name=config["bucket_region"], config=Config(
        **common, retries={"total_max_attempts": 2, "mode": "standard"}))}
    if not guest:
        for service in ("ec2", "ssm", "sts"):
            # SDK retries remain disabled for SendCommand. Only Controller.request
            # can replay an identical operation through the durable guest gate.
            clients[service] = session.client(service, config=Config(**common, retries={
                "total_max_attempts": 1 if service == "ssm" else 2, "mode": "standard"}))
    return clients


def s3_json(client, config, key, value):
    if not key.startswith(config["prefix"] + "/") or ".." in key.split("/"):
        raise SafetyError("S3 key escapes exact run prefix")
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()
    if len(raw) > MAX_JSON_BYTES:
        raise SafetyError("S3 JSON export exceeds its bounded metadata budget")
    checksum = hashlib.sha256(raw).hexdigest()
    client.put_object(Bucket=config["bucket"], Key=key, Body=raw, ExpectedBucketOwner=config["account"],
                      ServerSideEncryption="AES256", Metadata={"sha256": checksum}, ContentType="application/json")
    observed = client.head_object(Bucket=config["bucket"], Key=key, ExpectedBucketOwner=config["account"])
    if observed.get("ContentLength") != len(raw) or observed.get("Metadata", {}).get("sha256") != checksum:
        raise InfrastructureError("S3 JSON export identity not confirmed by HeadObject")
    return {"key": key, "bytes": len(raw), "sha256": checksum}


def read_s3_json(client, config, key, limit=MAX_JSON_BYTES):
    if not key.startswith(config["prefix"] + "/"):
        raise SafetyError("foreign S3 result key")
    response = client.get_object(Bucket=config["bucket"], Key=key, ExpectedBucketOwner=config["account"])
    with response["Body"] as body:
        raw = body.read(limit + 1)
    if len(raw) > limit:
        raise InfrastructureError("bounded guest result exceeded")
    return json.loads(raw)

def object_key(config, key):
    if not isinstance(key, str) or not key.startswith(config["prefix"] + "/") or ".." in key.split("/"):
        raise SafetyError("object key escapes exact run prefix")
    return key


def object_head(clients, config, key):
    object_key(config, key)
    if "objects" in clients:
        head = clients["objects"].head(key)
        return {"bytes": int(head["size"]), "sha256": head.get("metadata", {}).get("sha256"),
                "generation": head["generation"]}
    head = clients["s3"].head_object(Bucket=config["bucket"], Key=key, ExpectedBucketOwner=config["account"])
    return {"bytes": head["ContentLength"], "sha256": head.get("Metadata", {}).get("sha256")}


def put_bytes(clients, config, key, raw, *, content_type="application/octet-stream", replace=False):
    object_key(config, key)
    checksum = hashlib.sha256(raw).hexdigest()
    if "objects" in clients:
        generation = 0
        if replace:
            try:
                generation = clients["objects"].head(key)["generation"]
            except Exception as exc:
                if getattr(exc, "status", None) != 404:
                    raise
        try:
            clients["objects"].put(key, raw, content_type=content_type,
                                   metadata={"sha256": checksum}, if_generation_match=generation)
        except Exception:
            # A lost acknowledgement is reconciled by content, never a blind
            # overwrite or a new mutation identity.
            head = object_head(clients, config, key)
            if head["bytes"] != len(raw) or head["sha256"] != checksum:
                raise
    else:
        clients["s3"].put_object(Bucket=config["bucket"], Key=key, Body=raw,
            ExpectedBucketOwner=config["account"], ServerSideEncryption="AES256",
            Metadata={"sha256": checksum}, ContentType=content_type)
    head = object_head(clients, config, key)
    if head["bytes"] != len(raw) or head["sha256"] != checksum:
        raise InfrastructureError("uploaded object identity not confirmed")
    return {"key": key, "bytes": len(raw), "sha256": checksum,
            **({"generation": head["generation"]} if "generation" in head else {})}


def persist_json(clients, config, key, value, *, replace=False):
    if "objects" not in clients:
        return s3_json(clients["s3"], config, key, value)
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()
    if len(raw) > MAX_JSON_BYTES:
        raise SafetyError("JSON export exceeds bounded metadata budget")
    return put_bytes(clients, config, key, raw, content_type="application/json", replace=replace)


def read_json(clients, config, key, limit=MAX_JSON_BYTES):
    object_key(config, key)
    if "objects" not in clients:
        return read_s3_json(clients["s3"], config, key, limit)
    return json.loads(clients["objects"].get(key, max_bytes=limit))


def upload_object(clients, config, key, path, *, deadline_check=None):
    object_key(config, key)
    checksum, size = digest(path), Path(path).stat().st_size
    if deadline_check:
        deadline_check()
    if "objects" in clients:
        clients["objects"].upload(key, path, metadata={"sha256": checksum}, if_generation_match=0,
                                  deadline_check=deadline_check)
    else:
        from boto3.s3.transfer import TransferConfig
        options = {}
        if deadline_check:
            options["Callback"] = lambda count: deadline_check()
        clients["s3"].upload_file(str(path), config["bucket"], key, ExtraArgs={
            "ExpectedBucketOwner": config["account"], "ServerSideEncryption": "AES256",
            "Metadata": {"sha256": checksum}},
            Config=TransferConfig(multipart_threshold=16 << 20, multipart_chunksize=16 << 20,
                                  max_concurrency=2, use_threads=True), **options)
    head = object_head(clients, config, key)
    if head["bytes"] != size or head["sha256"] != checksum:
        raise InfrastructureError("uploaded file identity not confirmed")
    if deadline_check:
        deadline_check()
    return head


def download_object(clients, config, key, path):
    object_key(config, key)
    if "objects" in clients:
        head = object_head(clients, config, key)
        clients["objects"].download(key, path, generation=head["generation"])
        if Path(path).stat().st_size != head["bytes"] or digest(path) != head["sha256"]:
            raise InfrastructureError("downloaded generation differs from admitted object")
    else:
        clients["s3"].download_file(config["bucket"], key, str(path),
                                   ExtraArgs={"ExpectedBucketOwner": config["account"]})



def verify_instances(config, response, *, terminating=False, starting=False, now=None):
    fleet = config["fleet"]
    expected = {h["instance_id"]: h for h in [fleet["coordinator"], *fleet["nodes"]]}
    actual, expiries = {}, set()
    for reservation in response.get("Reservations", []):
        if reservation.get("OwnerId") != config["account"]:
            raise SafetyError("foreign reservation owner")
        for host in reservation.get("Instances", []):
            instance = host.get("InstanceId")
            if instance not in expected or instance in actual:
                raise SafetyError("unexpected/duplicate EC2 identity")
            tags = {t["Key"]: t["Value"] for t in host.get("Tags", [])}
            if any(tags.get(k) != v for k, v in {**TAGS, "RunId": fleet["run_id"]}.items()):
                raise SafetyError("current EC2 ownership tags differ; refusing all mutations")
            expiry = timestamp(tags.get("ExpiresAt"))
            expiries.add(expiry)
            if timestamp(config["deadline_utc"]) != expiry:
                raise SafetyError("controller deadline differs from exact independent instance expiry")
            if not terminating and expiry <= (now or utc()):
                raise DeadlineError("owned fleet has expired")
            state = host.get("State", {}).get("Name")
            if state != "terminated" and host.get("PrivateIpAddress") != expected[instance]["address"]:
                raise SafetyError("actual private address differs from fleet")
            allowed = (("running", "pending", "stopping", "stopped", "shutting-down", "terminated") if terminating
                       else ("running", "pending") if starting else ("running",))
            if state not in allowed:
                raise InfrastructureError("fleet not in an admitted lifecycle state")
            actual[instance] = host
    if set(actual) != set(expected) or len(expiries) != 1:
        raise SafetyError("exact six hosts with one immutable expiry required")
    return actual


def unit_name(config, case, role):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", case) or not re.fullmatch(r"(?:client|storage-(?:[0-9]|1[0-5]))", role):
        raise SafetyError("unsafe exact-owned unit name")
    return f"kvcloud-{config['prefix']}-{hashlib.sha256(case.encode()).hexdigest()[:16]}-{role}"


def owner_slice_limits(config, case):
    name = "kvowners" + hashlib.sha256((config["prefix"] + ":" + case).encode()).hexdigest()[:24] + ".slice"
    text = ("[Slice]\nCPUAccounting=yes\nMemoryAccounting=yes\n"
            f"CPUQuota={config['fleet']['server_cpus'] * 100}%\n"
            f"MemoryMax={config['fleet']['server_memory_mib']}M\nMemorySwapMax=0\n")
    return name, text


def configure_owner_slice(config, case):
    import fcntl
    name, text = owner_slice_limits(config, case)
    directory = Path("/run/systemd/system") / (name + ".d")
    with (Path("/run/lock") / (name + ".lock")).open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        directory.mkdir(mode=0o700, exist_ok=True)
        limits, ready = directory / "limits.conf", directory / "ready"
        if limits.exists() and limits.read_text() != text:
            raise SafetyError("owner slice limits differ from exact admitted VM budget")
        if not ready.exists():
            publish_synced(limits, text.encode())
            command(["systemctl", "daemon-reload"], timeout=20)
            publish_synced(ready, b"ready\n")
    return name


def cleanup_owner_slice(config, case, physical_index):
    job = case_owner_job(config, case)
    for owner in range(job["owner_count"]):
        if owner_host_index(config, job, owner) != physical_index:
            continue
        status = unit_status(unit_name(config, case, "storage-" + str(owner)))
        if status.get("LoadState") != "not-found" and (
                status.get("ActiveState") not in ("inactive", "failed") or status.get("MainPID") != "0"):
            raise SafetyError("owner slice cleanup requires every co-located process stopped")
    name, _ = owner_slice_limits(config, case)
    if unit_status(name).get("LoadState") != "not-found":
        command(["systemctl", "stop", name], timeout=30)
    directory = Path("/run/systemd/system") / (name + ".d")
    for leaf in ("limits.conf", "ready"):
        (directory / leaf).unlink(missing_ok=True)
    if directory.exists():
        directory.rmdir()
    command(["systemctl", "daemon-reload"], timeout=20)
    (Path("/run/lock") / (name + ".lock")).unlink(missing_ok=True)
    return {"owner_slice": name, "removed": True}


def systemd_argv(config, case, role, cpus, memory, seconds, executable, setenv=None):
    if seconds < 1 or cpus <= 0 or memory <= 0:
        raise DeadlineError("no admitted runtime/budget remains")
    env = environment(config)
    shared = role.startswith("storage-") and case_owner_job(config, case).get("owner_topology") == "colocated"
    cpu = (["--property=Slice=" + owner_slice_limits(config, case)[0]] if shared else
           ["--property=CPUQuota=" + str(cpus * 100) + "%"])
    return ["systemd-run", "--unit=" + unit_name(config, case, role), "--service-type=exec",
            *cpu, "--property=MemoryMax=" + str(int(memory)) + "M",
            "--property=MemorySwapMax=0", "--property=LimitMEMLOCK=infinity", "--property=LimitNOFILE=524288",
            "--property=RuntimeMaxSec=" + str(int(seconds)), "--property=TimeoutStopSec=25",
            "--property=KillMode=control-group", "--property=RemainAfterExit=yes",
            "--property=CPUAccounting=yes", "--property=MemoryAccounting=yes", "--property=IOAccounting=yes",
            "--property=WorkingDirectory=" + config["repository_root"],
            *["--setenv=" + key + "=" + env[key] for key in ("LD_LIBRARY_PATH", "FDB_CLIENT_LIB_DIR", "FDB_CLIENT_LIB_PATH") if key in env],
            *["--setenv=" + key + "=" + str(value) for key, value in (setenv or {}).items()],
            "--", *map(str, executable)]


def unit_status(name):
    result = subprocess.run(["systemctl", "show", name,
        "--property=LoadState,MainPID,ActiveState,SubState,Result,ExecMainStatus,ControlGroup,CPUUsageNSec,MemoryPeak,IOReadBytes,IOWriteBytes,CPUQuotaPerSecUSec,MemoryMax,LimitNOFILE,LimitNOFILESoft"],
        capture_output=True, text=True, timeout=10)
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    # systemd may garbage-collect a stopped transient unit. This is an observed
    # not-found state, not a fabricated successful process exit.
    if result.returncode and values.get("LoadState") != "not-found":
        raise InfrastructureError("cannot observe exact owned unit: " + result.stderr)
    if not values:
        raise InfrastructureError("systemd omitted unit properties")
    return values


def monitoring_spec(config, results_root):
    """Derive targets only from the same admitted fleet used by the controller."""
    fleet = config["fleet"]
    return {"schema_version": 1, "run_id": config["prefix"],
        "state_dir": "/var/lib/tidehunter-observation/" + config["prefix"],
        "results_root": str(results_root), "deadline_utc": config["deadline_utc"],
        "hosts": [{"index": index, "address": host["address"],
                   "role": "coordinator" if index == -1 else "storage-" + str(index)}
                  for index, host in enumerate([fleet["coordinator"], *fleet["nodes"]], start=-1)],
        "ports": {"prometheus": 19090, "grafana": 13000, "benchmark": 19899,
                  "node": 19100, "host": 19898}, "allow_loopback_hosts": False}


def monitoring_access(config, proofs=()):
    """Describe private operator access without asserting human IAM grants."""
    platform = platform_module(config)
    if platform:
        return platform.monitoring_access(config, proofs)
    spec = monitoring_spec(config, Path(config["fleet"]["data_root"]) / "results")
    target = config["fleet"]["coordinator"]["instance_id"]
    region = config["fleet"]["region"]
    commands = {}
    for service in ("grafana", "prometheus"):
        port = spec["ports"][service]
        argv = ["aws", "ssm", "start-session", "--region", region, "--target", target,
                "--document-name", "AWS-StartPortForwardingSession", "--parameters",
                json.dumps({"portNumber": [str(port)], "localPortNumber": [str(port)]}, separators=(",", ":"))]
        commands[service] = {"remote_loopback_port": port, "local_port": port,
            "url": f"http://127.0.0.1:{port}", "argv": argv, "command": shlex.join(argv)}
    hosts = []
    for position, host in enumerate(spec["hosts"]):
        observed = proofs[position].get("snapshot", {}).get("nvme", {}) if position < len(proofs) else {}
        filesystems = observed.get("mount", {}).get("filesystems", [])
        source = filesystems[0].get("source") if len(filesystems) == 1 else None
        record = {"index": host["index"], "address": host["address"],
            "labels": {"run_id": spec["run_id"], "host_index": str(host["index"]), "host_role": host["role"]},
            "benchmark_data_root": config["fleet"]["data_root"],
            "device_evidence": "bootstrap-proofs.json; observed mount mapping, not measured workload physical I/O"}
        if isinstance(source, str) and re.fullmatch(r"/dev/nvme[0-9]+n[0-9]+(?:p[0-9]+)?", source.split("[", 1)[0]):
            device = source.split("[", 1)[0]
            record.update({"observed_mount_source": source, "wal_device": re.sub(r"p[0-9]+$", "", Path(device).name)})
        else:
            record["device_evidence_gap"] = "bootstrap has no supported direct NVMe source mapping"
        hosts.append(record)
    return {"schema_version": 1, "run_id": spec["run_id"], "region": region, "coordinator_instance_id": target,
        "access": commands, "network": "SSM private forwarding only; UIs bind controller loopback; no public ingress",
        "operator_prerequisites": ["AWS CLI and Session Manager plugin", "valid operator AWS credentials"],
        "iam_granted": "unknown; no operator permission probe or IAM changes performed",
        "required_permissions": [
            {"action": "ssm:StartSession", "resources": [
                f"arn:aws:ec2:{region}:{config['account']}:instance/{target}",
                f"arn:aws:ssm:{region}::document/AWS-StartPortForwardingSession"]},
            {"actions": ["ssm:ResumeSession", "ssm:TerminateSession"], "resources": "operator-owned session ARNs"}],
        "grafana_authentication": "generated run-local credentials required; never embedded in exported artifacts",
        "hosts": hosts}


def monitor_module(config):
    directory = str(Path(config["repository_root"]) / "experiments/kv-comparison/observability")
    if directory not in sys.path:
        sys.path.insert(0, directory)
    import campaign_monitor
    return campaign_monitor


def docker_monitor_identity(item, proc_root=Path("/proc")):
    """Read cgroup membership for one label-admitted Docker PID."""
    pid = item.get("State", {}).get("Pid")
    identifier = item.get("Id")
    if type(pid) is not int or pid <= 0 or not item["State"].get("Running"):
        raise ValueError("exact owned container has no live PID")
    if not isinstance(identifier, str) or not re.fullmatch(r"[a-f0-9]{64}", identifier):
        raise ValueError("exact Docker container ID required")
    path = proc_root / str(pid) / "cgroup"
    if path.stat().st_size > 16384:
        raise ValueError("owned PID cgroup evidence exceeds bound")
    with path.open() as stream:
        content = stream.read(16385)
    if len(content) > 16384:
        raise ValueError("owned PID cgroup evidence exceeds bound")
    groups = [line[3:] for line in content.splitlines() if line.startswith("0::")]
    allowed = {"/system.slice/docker-" + identifier + ".scope",
               "/docker/system.slice/docker-" + identifier + ".scope"}
    if len(groups) != 1 or groups[0] not in allowed:
        raise ValueError("owned PID has no matching exact Docker cgroup v2 path")
    return {"container_id": identifier, "pid": pid, "cgroup": groups[0]}


def owned_monitor_processes(config, index, job, owner_case=None):
    """Observe known deployment PIDs only; never scan /proc for candidates."""
    processes, missing, identities = [], [], []
    if job.get("owner_topology") and index >= 0:
        roles = ["storage-" + str(owner) for owner in range(job["owner_count"])
                 if owner_host_index(config, job, owner) == index]
    else:
        roles = ["client" if index == -1 else "storage-" + str(index)] if (
            index == -1 or (job["backend"] in cloud_campaign.TRANSPORTS
                           and index < job.get("owner_count", len(config["fleet"]["nodes"])))) else []
    for role in roles:
        name = unit_name(config, owner_case if index >= 0 and owner_case is not None else job["case_id"], role) + ".service"
        try:
            status = unit_status(name)
        except (InfrastructureError, OSError, subprocess.SubprocessError) as exc:
            missing.append({"role": role, "reason": type(exc).__name__ + ": " + str(exc)})
            continue
        cgroup = status.get("ControlGroup", "")
        allowed = {"/system.slice/" + name}
        if role.startswith("storage-") and job.get("owner_topology") == "colocated":
            slice_case = owner_case if owner_case is not None else job["case_id"]
            allowed.add("/" + owner_slice_limits(config, slice_case)[0] + "/" + name)
        if cgroup not in allowed:
            missing.append({"role": role, "reason": "owned service has no admitted observed ControlGroup"})
            continue
        processes.append({"role": role, "cgroup": cgroup})
        identities.append({"role": role, "unit": name, "cgroup": cgroup})
    if job["backend"] in cloud_layout.BACKENDS:
        contract = contracts.selected(job["backend"], job.get("ack_contract", "synced"))
        kwargs = {"ack_contract": contract} if contract != "synced" else {}
        deployment = cloud_layout.node_config(config["fleet"], job["backend"], index, **kwargs)
        if deployment.get("native_valkey"):
            try:
                identity = cloud_layout.valkey_service.evidence(deployment)[0]
                processes.append({"role": "valkey" + str(index), "cgroup": identity["cgroup"]})
                identities.append({"role": "valkey" + str(index), **identity})
            except Exception as exc:
                missing.append({"role": "valkey" + str(index), "reason": type(exc).__name__ + ": " + str(exc)})
        for container in deployment["containers"]:
            try:
                item = cloud_layout.inspect_container(deployment, container)
                identity = docker_monitor_identity(item)
                processes.append({"role": container["service"], "cgroup": identity["cgroup"]})
                identities.append({"role": container["service"], **identity})
            except Exception as exc:
                missing.append({"role": container["service"], "reason": type(exc).__name__ + ": " + str(exc)})
    return {"processes": processes, "missing": missing, "identities": identities}


def tikv_diagnostics(config, index, *, since=None):
    """Best-effort, bounded endpoint snapshots outside the timed workload."""
    started = {"utc": utc().isoformat(), "monotonic_ns": time.monotonic_ns()}
    result = {"started": started, "host_index": index,
              "client_retry_counts": {"available": False, "reason": "tikv-client 0.4.0 exposes no per-request retry counter"}}

    def fetch(path, port, maximum=4 * 1024 * 1024):
        address = (config["fleet"]["coordinator"]["address"] if index == -1
                   else config["fleet"]["nodes"][index]["address"])
        deadline = time.monotonic() + 5
        chunks, total = [], 0
        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
                f"http://{address}:{port}{path}", timeout=3) as response:
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("diagnostic HTTP deadline exceeded")
                chunk = response.read1(min(65536, maximum + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > maximum:
                    raise ValueError("response exceeds diagnostic byte limit")
        return b"".join(chunks).decode("utf-8")

    def capture(fn):
        try:
            return {"available": True, "value": fn()}
        except Exception as exc:
            return {"available": False, "reason": (type(exc).__name__ + ": " + str(exc))[:500]}

    deployment = cloud_layout.node_config(config["fleet"], "tikv", index)
    def identity():
        cloud_layout.require_state(deployment)
        item = cloud_layout.inspect_container(deployment, deployment["containers"][0])
        return item["Id"]
    owned = capture(identity)
    result["container"] = owned

    def owned_fetch(path, port):
        if not owned["available"]:
            raise ValueError("owned TiKV/PD container identity unavailable")
        return fetch(path, port)

    if index == -1:
        def regions():
            value = json.loads(owned_fetch("/pd/api/v1/regions", 2379))
            if not isinstance(value, dict) or not isinstance(value.get("regions"), list):
                raise ValueError("PD regions response lacks regions")
            if value.get("count") != len(value["regions"]):
                raise ValueError("PD region count does not match returned rows")
            if any(not isinstance(region, dict) or not isinstance(region.get("peers"), list)
                   for region in value["regions"]):
                raise ValueError("PD region response lacks peer lists")
            return value
        result["pd_replication"] = capture(lambda: json.loads(owned_fetch("/pd/api/v1/config/replicate", 2379)))
        result["pd_regions"] = capture(regions)
    else:
        def metrics():
            text = owned_fetch("/metrics", 20180)
            prefixes = ("tikv_raft", "tikv_engine", "tikv_storage", "tikv_grpc", "tikv_thread", "rocksdb_")
            lines = [line for line in text.splitlines() if line and not line.startswith("#")
                     and line.startswith(prefixes)]
            if not lines:
                raise ValueError("no selected TiKV metrics returned")
            selected = "\n".join(lines) + "\n"
            if len(selected.encode()) > 1024 * 1024:
                raise ValueError("selected metrics exceed diagnostic byte limit")
            return selected
        result["metrics"] = capture(metrics)
    if since is not None:
        def logs():
            timestamp(since)
            container_id = identity()
            end = utc().isoformat()
            text = cloud_layout.command(["docker", "logs", "--timestamps", "--since", since,
                                         "--until", end, "--tail", "1000", container_id],
                                        timeout=10, include_stderr=True)
            if len(text.encode()) > 1024 * 1024:
                raise ValueError("bounded log output exceeds diagnostic byte limit")
            return {"container_id": container_id, "since": since, "until": end,
                    "tail_limit_lines": 1000, "possibly_truncated": len(text.splitlines()) >= 1000,
                    "text": text}
        result["logs"] = capture(logs)
    result["finished"] = {"utc": utc().isoformat(), "monotonic_ns": time.monotonic_ns()}
    return result


def snapshot(config, name=None, *, drop=False, completed=False):
    root = guarded(config["fleet"]["data_root"], Path("/mnt/tidehunter-kv-comparison"), must_exist=True)
    nvme = (cloud_layout.nvme_evidence(root, False, provider="gcp") if platform_module(config)
            else cloud_layout.nvme_evidence(root, False))
    if drop:
        command(["sync"], timeout=30)
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
    result = {"observed_utc": utc().isoformat(), "monotonic_ns": time.monotonic_ns(), "nvme": nvme,
              "drop_os_caches": drop, "application_caches_evicted": False,
              "diskstats": Path("/proc/diskstats").read_text(), "vmstat": Path("/proc/vmstat").read_text(),
              "host_cpu_stat": Path("/proc/stat").read_text(), "host_cpu_hz": os.sysconf("SC_CLK_TCK")}
    if name:
        status = unit_status(name)
        result["unit"] = status
        pid = status.get("MainPID", "0")
        if not pid.isdigit() or (int(pid) <= 0 and not completed):
            raise InfrastructureError("measurement snapshot requires live workload MainPID")
        result["process_io"] = Path("/proc", pid, "io").read_text() if int(pid) > 0 else None
        cgroup = status.get("ControlGroup", "")
        if cgroup and (not cgroup.startswith("/system.slice/kvcloud-") or ".." in cgroup.split("/")):
            raise SafetyError("unexpected workload cgroup")
        directory = Path("/sys/fs/cgroup") / cgroup.lstrip("/") if cgroup else None
        if directory and directory.is_dir():
            result["cgroup"] = {field: (directory / field).read_text() for field in
                                ("cpu.stat", "memory.current", "memory.events", "io.stat", "cpu.max", "memory.max")}
        elif completed:
            result["cgroup"] = None
            result["cgroup_note"] = "retired after exit; retained final systemd accounting is cumulative"
        else:
            raise InfrastructureError("live workload cgroup missing")
    return result


def host_cpu_interval(before, after):
    """Host-wide CPU, including kernel workers, not a cgroup or target attribution."""
    if not admitted_host_count(len(before)) or len(after) != len(before):
        raise ValueError("matching two or six host CPU snapshots required")
    rows = []
    for index, (start, end) in enumerate(zip(before, after), start=-1):
        row = {"host_index": index, "available": False}
        try:
            if start["nvme"]["host_identity"]["boot_id"] != end["nvme"]["host_identity"]["boot_id"]:
                raise ValueError("host rebooted")
            hz = start["host_cpu_hz"]
            if type(hz) is not int or hz <= 0 or hz != end["host_cpu_hz"]:
                raise ValueError("CPU tick frequency unavailable or changed")
            def fields(frame):
                line = frame["host_cpu_stat"].splitlines()[0].split()
                if line[0] != "cpu" or len(line) < 9:
                    raise ValueError("aggregate CPU counters unavailable")
                return [int(value) for value in line[1:9]]
            a, b = fields(start), fields(end)
            delta = [y - x for x, y in zip(a, b)]
            if any(value < 0 for value in delta):
                raise ValueError("CPU counter reset")
            row.update(available=True, user_seconds=(delta[0] + delta[1]) / hz,
                       system_seconds=(delta[2] + delta[5] + delta[6]) / hz,
                       idle_seconds=delta[3] / hz, iowait_seconds=delta[4] / hz,
                       steal_seconds=delta[7] / hz)
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            row["reason"] = str(exc)
        rows.append(row)
    return {"schema_version": 1, "hosts": rows,
            "scope": "host-wide CPU between independently sampled gates; includes unrelated work and nvmet kernel workers, not attribution to this workload"}


def safe_extract_capsule(archive, destination, expected):
    if destination.exists():
        raise SafetyError("storage capsule requires a fresh directory")
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        names = [m.name for m in members]
        if set(names) != set(expected) or len(names) != len(set(names)):
            raise SafetyError("capsule includes unexpected or duplicate files")
        if any(not m.isfile() or m.size > 4 * 1024 * 1024 or Path(m.name).is_absolute() or ".." in Path(m.name).parts for m in members):
            raise SafetyError("unsafe capsule member")
        destination.mkdir(parents=True, mode=0o700)
        for member in members:
            target = guarded(destination / member.name, destination)
            target.parent.mkdir(parents=True, exist_ok=True)
            with bundle.extractfile(member) as source, target.open("xb") as output:
                shutil.copyfileobj(source, output)
            target.chmod(0o600)


def require_zero_owner_history_waits(directory, owner_count):
    """A6 diagnostic cases require every stopped owner's epoch wait evidence."""
    for index in range(owner_count):
        path = directory / "owner-durability" / f"node-{index}.history-maintenance.json"
        try:
            evidence = json.loads(path.read_text())
            epochs = evidence["epoch_stats"]
            if evidence.get("schema_version") != 1 or not isinstance(epochs, list) or not epochs:
                raise ValueError("missing epoch history statistics")
            for epoch in epochs:
                if (not isinstance(epoch, list) or len(epoch) != 2 or type(epoch[0]) is not int
                        or not isinstance(epoch[1], dict) or type(epoch[1].get("waits")) is not int
                        or epoch[1]["waits"] < 0):
                    raise ValueError("invalid epoch history wait counter")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise RuntimeError("owner history diagnostics missing or invalid") from exc
        if any(stats["waits"] != 0 for _, stats in epochs):
            raise RuntimeError("memory-only owner waited for history")


def planned_write_batches_per_partition(job):
    """Reserve a per-owner write-batch ceiling, with fixed history slack."""
    owners = job["owner_count"]
    if job.get("case_kind") == "mainnet_replay":
        writes = job["repetitions"] * (3 + 2)  # Three commits and two preload chunks per repetition.
        return (writes + owners - 1) // owners + 4096
    writes = job["keys"]  # At most one preload batch per key.
    if any(job["mix"].get(kind, 0) for kind in ("put", "delete", "put_batch")):
        # Mixed phases reserve the all-write ceiling, not an expected fraction.
        writes += job["warmup_operations"] + job["measurement_operations"]
    return (writes + owners - 1) // owners + 4096


def owner_control_settings(job):
    """Keep owner preparation and its receipt on the same control transport."""
    backend = cloud_campaign.owner_control_backend(job)
    transport = cloud_campaign.CONTROL_TRANSPORTS[backend]
    return {"owner_polling": job.get("owner_polling", "adaptive"),
            "owner_control_backend": backend, "owner_transport": copy.deepcopy(transport),
            "owner_transport_security_label":
                "trusted_network_no_tls" if is_trusted(transport) else "mutual_tls"}


def admitted_live_campaign(config, case):
    spec = config["spec"]
    if spec.get("mode") == "standard-with-live-nvmeof":
        return True
    if (spec.get("mode") != "focused"
            or spec.get("selection") not in cloud_campaign.FOCUSED_SELECTIONS):
        return False
    return any(job["case_id"] == case and job.get("nvmeof_route") == "live"
               for job in cloud_campaign.make_focused_matrix(spec["selection"]))


def live_nvme_preparation(config, case, transport):
    if (not admitted_live_campaign(config, case)
            or transport not in ("tcp", "rdma")
            or "tidehunter_nvme_" + transport not in config["spec"]["backends"]):
        raise SafetyError("live NVMe transport is not admitted by the campaign")
    jobs = (cloud_campaign.make_focused_matrix(config["spec"]["selection"])
            if config["spec"].get("mode") == "focused" else config.get("jobs", []))
    job = next((job for job in jobs if job["case_id"] == case), {})
    requests = job.get("live_max_inflight_requests", 32)
    if type(requests) is not int or not 1 <= requests <= 1024:
        raise SafetyError("live_max_inflight_requests must be an integer in 1..1024")
    field = "rdma_address" if platform_module(config) and transport == "rdma" else "address"
    return {"addresses": [host[field] for host in logical_owner_hosts(config, job)],
            "transport": transport, "service": 4420, "port_id": 1000,
            "commissioning_service": 4421, "commissioning_port_id": 1001,
            "max_inflight_requests": requests,
            "max_inflight_bytes": min(1024**3, requests * 512 * 1024),
            "leased_write_window": None, "connect_timeout_ms": 30000, "io_timeout_ms": 30000}


def live_host_identity(host_nqn):
    prefix = "nqn.2014-08.org.nvmexpress:uuid:"
    if not isinstance(host_nqn, str) or not host_nqn.startswith(prefix):
        raise SafetyError("invalid live initiator NQN")
    try:
        identity = uuid.UUID(host_nqn[len(prefix):])
    except ValueError as error:
        raise SafetyError("invalid live initiator UUID") from error
    if identity.version != 4 or host_nqn != prefix + str(identity):
        raise SafetyError("live initiator UUID must be canonical UUIDv4")
    return list(identity.bytes)


def admit_prepared_live_client(live_client, prep, tier):
    if not isinstance(live_client, dict):
        raise SafetyError("prepared live client is missing")
    host_nqn = live_client.get("host_nqn")
    host_id = live_host_identity(host_nqn)
    expected = {key: prep[key] for key in
                ("max_inflight_requests", "max_inflight_bytes", "connect_timeout_ms", "io_timeout_ms")}
    identities = live_client.get("owner_identities", {})
    if identities:
        if set(identities) != {str(i) for i in range(len(prep["addresses"]))}:
            raise SafetyError("live owner identity map is incomplete")
        nqns = [identity.get("host_nqn") for identity in identities.values()]
        if len(set(nqns)) != len(nqns):
            raise SafetyError("live owner initiator identities are not unique")
        for identity in identities.values():
            if identity != {"host_nqn": identity.get("host_nqn"),
                            "host_id": live_host_identity(identity.get("host_nqn"))}:
                raise SafetyError("live owner UUID differs from its NQN")
        expected["owner_identities"] = identities
    expected.update(host_nqn=host_nqn, host_id=host_id, leased_durability=tier,
                    inline_reads=prep["inline_reads"], queue_geometry=prep["queue_geometry"])
    if "export_mode" in prep:
        expected["export_mode"] = "per_owner"
    for key, default in (("lease_ahead", True), ("polled_writes", False), ("leased_write_window", None)):
        if key in live_client or key in prep:
            expected[key] = prep.get(key, default)
    for key, default in (("leased_sync", "group_flush"),
                         ("leased_flush_min_frame_bytes", 65536)):
        if key in live_client or key in prep:
            expected[key] = prep.get(key, default)
    if "write_mode" in prep:  # Only an admitted archive binary requests this key.
        expected["write_mode"] = "leased"
    if live_client != expected:
        raise SafetyError("prepared live client differs from admitted configuration")
    return host_nqn


def live_target_for_owner(config, case, index, transport, host_nqn, target="kernel", export_mode="on_demand"):
    prep = live_nvme_preparation(config, case, transport)
    live_host_identity(host_nqn)
    offset = 2 * index
    result = {"address": prep["addresses"][index], "service": prep["service"] + offset,
            "transport": transport, "host_nqn": host_nqn,
            "port_id": prep["port_id"] + offset,
            "state_dir": str(Path(config["fleet"]["data_root"]) / "cells" / case
                             / f"node-{index}" / "live-nvmeof"),
            "commissioning_port_id": prep["commissioning_port_id"] + offset,
            "commissioning_service": prep["commissioning_service"] + offset}
    if target != "kernel":
        raise SafetyError("only the kernel live target is supported")
    if config["spec"].get("selection") == q0_campaign.SELECTION:
        result["export_mode"] = export_mode
    return result


def case_owner_wal_prewrite(job):
    """The case's owner WAL prewrite choice. A case that does not set it keeps the
    legacy owner behaviour (off), so historical selections reproduce exactly; new
    selections that want prewritten owners must set owner_wal_prewrite: true."""
    value = job.get("owner_wal_prewrite", False)
    if type(value) is not bool:
        raise SafetyError("owner_wal_prewrite must be boolean")
    return value


def bind_owner_wal_prewrite(job, launch):
    """Write the case's choice as an explicit boolean: owners read an absent key as on."""
    launch["node"]["wal_prewrite"] = case_owner_wal_prewrite(job)


def load_owner_launch(job, launch_path):
    """Read a prepared owner launch and bind the case's WAL prewrite choice before any validation."""
    import yaml
    launch = yaml.safe_load(Path(launch_path).read_text())
    bind_owner_wal_prewrite(job, launch)
    return launch


def validate_launch(config, launch, case, index, polling=None):
    root = Path(config["fleet"]["data_root"]) / "cells" / case
    node = root / f"node-{index}"
    if polling is not None and launch.get("limits", {}).get("polling") != polling:
        raise SafetyError("typed launch polling differs from requested owner policy")
    expected = {"ready_file": node / "ready", "stop_file": node / "stop"}
    for field, path in expected.items():
        if launch.get(field) != str(path):
            raise SafetyError("typed launch marker escapes exact case/node")
        guarded(path, root)
    if launch.get("node", {}).get("root") != str(node / "storage"):
        raise SafetyError("typed database root escapes exact case/node")
    guarded(node / "storage", root)
    job = case_owner_job(config, case)
    actual_prewrite = launch.get("node", {}).get("wal_prewrite")
    if type(actual_prewrite) is not bool:
        raise SafetyError("owner launch must carry an explicit boolean wal_prewrite")
    if "owner_wal_prewrite" in job and actual_prewrite != case_owner_wal_prewrite(job):
        raise SafetyError("owner wal_prewrite differs from the exact reviewed case")
    live_target = launch.get("node", {}).get("live_target")
    if live_target is not None:
        if not isinstance(live_target, dict):
            raise SafetyError("invalid live target launch configuration")
        expected_target = live_target_for_owner(config, case, index, live_target.get("transport"),
                                                live_target.get("host_nqn"),
                                                "spdk" if live_target.get("spdk") is not None else "kernel",
                                                live_target.get("export_mode", "on_demand"))
        actual_target = {key: value for key, value in live_target.items() if key != "spdk" or value is not None}
        if actual_target != expected_target:
            raise SafetyError("live target differs from exact owner campaign configuration")
    host = logical_owner_hosts(config, case_owner_job(config, case))[index]
    port = 9400 + index if case_owner_job(config, case).get("owner_topology") else 9400
    if launch.get("listen") != host["address"] + ":" + str(port):
        raise SafetyError("typed listener differs from exact storage endpoint")
    tls = launch.get("tls", {})
    try:
        transport = validate_transport(tls.get("transport"))
    except ValueError as error:
        raise SafetyError("invalid typed transport") from error
    expected_transport = (owner_control_settings(job)["owner_transport"]
                          if job.get("backend") in cloud_campaign.NVME_BACKENDS else None)
    if expected_transport is not None and protocol_key(transport) != protocol_key(expected_transport):
        raise SafetyError("typed transport differs from the planned leased owner control protocol")
    if protocol_key(transport) not in {
            protocol_key(value) for value in cloud_campaign.CONTROL_TRANSPORTS.values()}:
        raise SafetyError("typed transport is not admitted for leased owner control")
    if is_trusted(transport):
        if any(tls.get(field) != "" for field in ("ca", "cert", "key")):
            raise SafetyError("trusted native transport must have empty TLS paths")
    else:
        expected_tls = {"ca": "ca.der", "cert": f"tidehunter-node-{index}.der",
                        "key": f"tidehunter-node-{index}.pk8"}
        for field, name in expected_tls.items():
            if tls.get(field) != str(node / "tls" / name):
                raise SafetyError("node capsule may contain only CA certificate and its own leaf identity")
            guarded(tls[field], node)


def bounded_capture(argv, *, max_bytes, timeout):
    """Collect bounded output from one read-only diagnostic subprocess."""
    process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    data = bytearray()
    deadline = time.monotonic() + timeout
    truncated = False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout)
            ready, _, _ = select.select([process.stdout], [], [], remaining)
            if not ready:
                raise subprocess.TimeoutExpired(argv, timeout)
            chunk = os.read(process.stdout.fileno(), min(65536, max_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > max_bytes:
                truncated = True
                break
        returncode = None if truncated else process.wait(timeout=max(0.01, deadline - time.monotonic()))
        return {"available": truncated or returncode == 0,
                "text": bytes(data[:max_bytes]).decode("utf-8", errors="replace"),
                "truncated": truncated, "max_bytes": max_bytes, "returncode": returncode}
    finally:
        if process.poll() is None:
            process.kill()  # Only the collector subprocess, never the owner unit.
            process.wait()
        process.stdout.close()


def bounded_owner_journal(name):
    argv = ["journalctl", "--no-pager", "--unit", name, "--lines=512", "--output=short-precise"]
    return {**bounded_capture(argv, max_bytes=1048576, timeout=15), "max_lines": 512}


def initiator_failure_frame(config, case, since, until, active):
    """Collect only verified host/case and receipt-bound controller evidence."""
    frame = {"case_id": case, "since_utc": since, "until_utc": until,
             "scope": "verified campaign coordinator; case window; NVMe/RDMA kernel messages",
             "controllers": []}
    try:
        verify_local_identity(config, -1)
    except Exception as exc:
        frame["host_identity"] = {"available": False, "reason": type(exc).__name__}
        frame["kernel_journal"] = {"available": False, "reason": "coordinator identity not verified"}
        frame["controllers_unavailable"] = "coordinator identity not verified"
        return frame
    frame["host_identity"] = {"available": True, "instance_id": config["fleet"]["coordinator"]["instance_id"]}
    try:
        # journalctl's documented UTC form also works on older cloud images.
        bounds = [datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
                  .strftime("%Y-%m-%d %H:%M:%S.%f UTC") for value in (since, until)]
        frame["kernel_journal"] = bounded_capture(
            ["journalctl", "--no-pager", "-k", "--output=short-precise",
             "--since", bounds[0], "--until", bounds[1], "--lines=512"],
            max_bytes=32768, timeout=10)
    except Exception as exc:
        frame["kernel_journal"] = {"available": False, "reason": type(exc).__name__}
    receipts, additional = [], []
    if active and Path(active["case_directory"]).name == case:
        for host in active.get("hosts", []):
            path = Path(active["case_directory"]) / f"nvme-host-{host['index']}.json"
            try:
                data = json.loads(path.read_text())
                if data.get("phase") != "connected":
                    raise ValueError("host receipt not connected")
                entries = [(host["index"], item["host_receipt"]) for item in data["exports"]]
                receipts.extend(entries[:1])
                additional.extend(entries[1:])
            except (OSError, ValueError, KeyError, TypeError) as exc:
                frame["controllers"].append({"owner_index": host["index"], "available": False,
                                             "reason": "host receipt unavailable: " + type(exc).__name__})
    receipts.extend(additional)
    if not receipts:
        frame["controllers_unavailable"] = "no sealed-session host receipts; live or incomplete connection"
    frame["controller_limit"] = len(config["fleet"]["nodes"])
    frame["controllers_omitted"] = max(0, len(receipts) - frame["controller_limit"])

    def capture(row):
        index, receipt = row
        name = receipt.get("controller")
        result = {"owner_index": index, "controller": name}
        try:
            if not isinstance(name, str) or not re.fullmatch(r"nvme[0-9]+", name):
                raise ValueError("invalid receipt controller")
            path = Path("/sys/class/nvme") / name
            if (path.stat().st_ino != receipt["controller_inode"]
                    or (path / "subsysnqn").read_text().strip() != receipt["nqn"]
                    or (path / "transport").read_text().strip() != receipt["transport"]
                    or (path / "address").read_text().strip() != receipt["controller_address"]
                    or path.stat().st_ino != receipt["controller_inode"]):
                raise ValueError("controller identity differs from session receipt")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            result["available"] = False
            result["reason"] = "exact session identity unavailable: " + type(exc).__name__
            return result
        result["available"] = True
        for key, argv in (
            ("identity", ["nvme", "id-ctrl", "/dev/" + name, "-o", "json"]),
            ("error_log", ["nvme", "error-log", "/dev/" + name, "--log-entries=16", "-o", "json"]),
        ):
            try:
                result[key] = bounded_capture(argv, max_bytes=32768, timeout=5)
            except Exception as exc:
                result[key] = {"available": False, "reason": type(exc).__name__}
        return result

    with ThreadPoolExecutor(max_workers=5) as pool:
        frame["controllers"].extend(pool.map(capture, receipts[:frame["controller_limit"]]))
    return frame


def live_case_observation(config, case, index, launch, boundary, expected):
    """Bounded read-only host observations; ownership is validated by caller."""
    script = Path(config["repository_root"]) / "experiments/nvmeof/collect.py"
    spec = importlib.util.spec_from_file_location("nvmeof_case_collect", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    images, errors = [], []
    truncated = False
    if index >= 0:
        import cold_device_trace
        root = Path(launch["node"]["root"])
        pending = [(root, 0)]
        examined = 0
        while pending and examined < 1024 and len(images) < 256:
            directory, depth = pending.pop()
            try:
                with os.scandir(directory) as scan:
                    entries = list(islice(scan, 1025 - examined))
                if len(entries) > 1024 - examined:
                    truncated = True
                    entries = entries[:1024 - examined]
                examined += len(entries)
                for entry in entries:
                    if entry.is_dir(follow_symlinks=False):
                        if depth < 5:
                            pending.append((Path(entry.path), depth + 1))
                        else:
                            truncated = True
                    elif entry.is_file(follow_symlinks=False) and re.fullmatch(r"wal_[0-9a-f]{16}", entry.name):
                        try:
                            resolved = cold_device_trace.resolve_device(entry.path)
                            images.append({"image": entry.path, "block_device": Path(resolved["sysfs_path"]).name})
                        except (OSError, ValueError, RuntimeError) as exc:
                            errors.append((type(exc).__name__ + ": " + str(exc))[:300])
                        if len(images) >= 256:
                            truncated = True
                            break
            except OSError as exc:
                errors.append((type(exc).__name__ + ": " + str(exc))[:300])
        truncated |= bool(pending) or len(images) >= 256
    controllers = []
    if index == -1:
        try:
            with os.scandir("/sys/class/nvme") as scan:
                entries = list(islice(scan, 257))
            truncated |= len(entries) > 256
            for entry in entries[:256]:
                if not re.fullmatch(r"nvme[0-9]+", entry.name):
                    continue
                path = Path("/sys/class/nvme") / entry.name
                attrs = {key: module._attribute(path / key) for key in
                         ("subsysnqn", "hostnqn", "transport", "address")}
                if not all(value.get("available") for value in attrs.values()):
                    continue
                address = dict(part.split("=", 1) for part in attrs["address"]["value"].split(",") if "=" in part)
                candidates = expected.get("owners", {}).values() or [
                    {"host_nqn": expected["host_nqn"], "address": address, "service": expected["service"]}
                    for address in expected["addresses"]]
                if (attrs["subsysnqn"]["value"].startswith("nqn.2026-01.local.tidehunter:live-")
                        and attrs["transport"]["value"] == expected["transport"]
                        and any(attrs["hostnqn"]["value"] == owner["host_nqn"]
                                and address.get("traddr") == owner["address"]
                                and address.get("trsvcid") == str(owner["service"]) for owner in candidates)):
                    controllers.append(entry.name)
            controllers.sort()
        except OSError as exc:
            errors.append((type(exc).__name__ + ": " + str(exc))[:300])
    images.sort(key=lambda row: row["image"])
    result = module.case_capture(case, boundary, images, controllers, rdma_diagnostics)
    result["discovery"] = {"truncated": truncated, "errors": errors,
                           "controllers_scope": "host NQN, target NQN prefix, transport and configured endpoint matched; no cross-host timing",
                           "images_scope": "bounded regular wal files beneath exact owner storage root",
                           "images_available": bool(images),
                           "images_reason": None if images else "no verified WAL file at this boundary"}
    return result


def floor_binary(directory):
    source = Path(__file__).with_name("floors_tcp_rtt.c")
    binary = directory / "tcp_rtt"
    compiler = shutil.which("gcc")
    if compiler is None:
        raise FioFailure("gcc unavailable")
    version = subprocess.run([compiler, "--version"], check=True, capture_output=True, timeout=10).stdout[:4096].decode("utf-8", "replace").splitlines()[0]
    subprocess.run([compiler, "-O2", "-Wall", "-Werror", "-o", str(binary), str(source)],
                   check=True, capture_output=True, timeout=30)
    return binary, {"compiler": version, "source_sha256": digest(source), "binary_sha256": digest(binary)}


def floor_server_cleanup(directory, state):
    import signal
    results, unexpected = [], False
    for i, row in enumerate(state["servers"]):
        status_path = directory / f"server-{i}.status"
        identity = floors.process_identity(row["pid"])
        if identity is not None and identity[1] != row["start"]:
            raise InfrastructureError("floor supervisor process identity changed")
        if status_path.exists():
            end = time.monotonic() + 0.5
            while time.monotonic() < end and floors.process_group_alive(row["pid"]):
                time.sleep(0.02)
            if floors.process_group_alive(row["pid"]):
                unexpected = True
        if floors.process_group_alive(row["pid"]):
            try:
                os.killpg(row["pid"], signal.SIGKILL)
            except ProcessLookupError:
                pass
            end = time.monotonic() + 10
            while time.monotonic() < end and floors.process_group_alive(row["pid"]):
                time.sleep(0.05)
            if floors.process_group_alive(row["pid"]):
                raise InfrastructureError("floor process group cleanup unconfirmed")
        status = json.loads(status_path.read_text())["exit_status"] if status_path.exists() else None
        path = directory / f"server-{i}.out"
        raw = path.read_bytes() if path.exists() else b""
        if len(raw) > 16384:
            raise SafetyError("floor server output exceeds bound")
        results.append({"exit_status": status, "output": raw.decode("utf-8", "ignore"), "argv": row["argv"]})
    shutil.rmtree(directory)
    if unexpected:
        raise InfrastructureError("network floor process remained after server completion")
    return results


def floor_client_command(argv, timeout):
    import signal
    process = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + timeout
    raw, completed = bytearray(), False
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout)
            readable, _, _ = select.select([process.stdout], [], [], remaining)
            if not readable:
                raise subprocess.TimeoutExpired(argv, timeout)
            chunk = os.read(process.stdout.fileno(), 16385 - len(raw))
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > 16384:
                raise FioFailure("floor client output exceeds bound")
        status = process.wait(timeout=max(0.001, deadline - time.monotonic()))
        completed = True
        return status, raw.decode("utf-8", "ignore")
    finally:
        alive = floors.process_group_alive(process.pid)
        if alive:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait(timeout=10)
        process.stdout.close()
        if floors.process_group_alive(process.pid):
            raise InfrastructureError("floor client process group cleanup unconfirmed")
        if alive and completed:
            raise InfrastructureError("network floor process remained after client completion")


def floor_client_attempts(argv, deadline, *, perftest):
    """Retry only quick perftest connection failures during listener startup."""
    for attempt in range(1, 4):
        remaining = deadline - time.monotonic()
        if remaining <= 1:
            raise DeadlineError("network floor client deadline exceeded")
        start = time.monotonic()
        status, raw = floor_client_command(argv, remaining)
        elapsed = time.monotonic() - start
        connection_error = re.search(
            r"connection\s+(?:refused|unreachable)|network is unreachable|host is unreachable|"
            r"no route to host|rdma[_ -]cm.{0,100}(?:connect|unreachable|refused|failure|failed)|"
            r"(?:connect|connection).{0,100}rdma[_ -]cm",
            raw, re.IGNORECASE | re.DOTALL)
        if not (perftest and status and elapsed < 3 and connection_error and attempt < 3):
            return status, raw, attempt
        remaining = deadline - time.monotonic()
        if remaining <= 1:
            raise DeadlineError("network floor client deadline exceeded")
        time.sleep(min(0.1, remaining - 1))
    raise AssertionError("bounded floor attempts exhausted")


def native_wal_snapshot(directory, label):
    """Capture the paused client's cumulative logical-WAL counter at a gate."""
    from decimal import Decimal
    source = Path(directory) / "result/live.prom"
    try:
        with source.open("rb") as stream:
            data = stream.read(16 * 1024**2 + 1)
        if len(data) > 16 * 1024**2:
            raise ValueError("native metrics snapshot exceeds bound")
        series = {}
        for line in data.decode().splitlines():
            if not line.startswith("wal_written_bytes_type"):
                continue
            match = re.fullmatch(r'(wal_written_bytes_type(?:_total)?(?:\{.*\})?)\s+([0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)\s*', line)
            if match is None:
                raise ValueError("invalid native logical-WAL counter")
            key, number = match.groups()
            value = Decimal(number)
            if value < 0 or value > 2**53 or value != value.to_integral_value() or key in series:
                raise ValueError("native logical-WAL counter is not a unique nonnegative integer")
            series[key] = int(value)
            if len(series) > 4096:
                raise ValueError("native logical-WAL metric population exceeds bound")
        if not series:
            raise ValueError("native logical-WAL counter unavailable")
        target = Path(directory) / ("native-wal-" + label + ".prom")
        target.write_bytes(data)
        return {"available": True, "series": series, "path": target.name,
                "sha256": hashlib.sha256(data).hexdigest()}
    except (OSError, UnicodeError, ValueError, ArithmeticError) as exc:
        return {"available": False, "reason": str(exc)}


def native_wal_interval(start, end):
    evidence = {"available": False, "start": start, "end": end,
                "counter": "wal_written_bytes_type",
                "scope": "post-warmup pre-release to completion-observed; includes background logical WAL records, excludes preload/warmup; not physical device bytes"}
    if not start or not end or not start.get("available") or not end.get("available"):
        evidence["reason"] = "native WAL boundary unavailable"
        return evidence
    before, after = start["series"], end["series"]
    if before.keys() != after.keys() or any(after[key] < value for key, value in before.items()):
        evidence["reason"] = "native WAL counter population changed or reset"
        return evidence
    evidence.update(available=True, logical_wal_bytes=sum(after[key] - value for key, value in before.items()))
    return evidence


def store_thread_cpu(directory, traces):
    artifact = {"coordinator": traces[0] if traces and not traces[0].get("inactive") else {"available": False},
                "owners": [trace for trace in traces[1:] if not trace.get("inactive")]}
    dump(Path(directory) / "thread-cpu.json", artifact)
    return artifact


def store_owner_ftrace(directory, traces):
    """Keep owner trace payloads beside the case, not only in guest receipts."""
    for index, trace in enumerate(traces):
        if index == 0 or trace.get("inactive"):
            continue  # all_hosts starts with the coordinator.
        trace = dict(trace)
        text = trace.pop("trace", "")
        cpu = trace.pop("cpu_samples", {"available": False, "reason": "owner CPU samples unavailable"})
        target = Path(directory) if index == 1 else Path(directory) / "owner-ftrace" / f"node-{index - 1}"
        target.mkdir(parents=True, exist_ok=True)
        (target / "owner-ftrace.txt").write_text(text)
        dump(target / "owner-cpu.json", cpu)
        dump(target / "owner-ftrace.json", trace)


def guest_operation(config, request, clients, *, local_mode=None):
    index = request.get("owner_index", request["index"])
    physical_index = request["index"]
    if type(physical_index) is not int or physical_index not in range(-1, len(config["fleet"]["nodes"])):
        raise SafetyError("exact guest index required")
    if "owner_index" in request:
        job = case_owner_job(config, request.get("case_id"))
        if (type(index) is not int or index < 0
                or index >= job.get("owner_count", 0)
                or owner_host_index(config, job, index) != physical_index):
            raise SafetyError("logical owner does not belong to the addressed host")
    identity = (verify_local_identity(config, physical_index) if local_mode is None
                else local_mode.identity(physical_index))
    root = guarded(config["fleet"]["data_root"], Path("/mnt/tidehunter-kv-comparison"), must_exist=True)
    action = request["action"]
    if local_mode is not None and action in ("cold_trace_probe", "cold_trace_start", "cold_trace_stop",
                                             "nvme_trace_start", "nvme_trace_stop", "owner_ftrace_start", "owner_ftrace_stop"):
        raise SafetyError("local physical-device tracing is not permitted")
    if local_mode is not None and action in ("monitor_start", "monitor_stop", "snapshot", "tide_start", "tide_stop"):
        return local_mode.request(index, request, case=request.get("case_id", "startup"),
                                  timeout=request.get("timeout", 180), export=False)
    if action in ("valkey_native_start", "valkey_native_snapshot", "valkey_native_stop"):
        import valkey_cloud
        return valkey_cloud.guest(config, request)
    if action in ("fleet_tools_target_setup", "fleet_tools_target_cleanup",
                  "fleet_tools_cpu_start", "fleet_tools_cpu_stop"):
        if local_mode is not None:
            raise SafetyError("F9 tools require fleet hardware")
        import fleet_tools
        return fleet_tools.guest(config, request, sys.modules[__name__])
    if action == "bootstrap" and platform_module(config):
        return platform_module(config).bootstrap_guest(config, index, clients)
    if action == "bootstrap":
        ready_path = guarded(Path("/mnt/tidehunter") / config["prefix"] / "kv-bootstrap-ready.json",
                             Path("/mnt/tidehunter"), must_exist=True)
        ready = json.loads(ready_path.read_text())
        expected_ready = {"run_id": config["prefix"], "instance_id": identity["instanceId"],
                          "source_sha256": config["source_sha256"], "binary_sha256": config["binary_sha256"]}
        if (any(ready.get(key) != value for key, value in expected_ready.items())
                or timestamp(ready.get("deadline_utc")) != timestamp(config["deadline_utc"])):
            raise SafetyError("final bootstrap readiness receipt differs from admitted source/host/deadline")
        package = verify_package(config)
        bootstrap = guarded(Path("/mnt/tidehunter") / config["prefix"] / "bootstrap.json", Path("/mnt/tidehunter"), must_exist=True)
        receipt = json.loads(bootstrap.read_text())
        if receipt.get("run_id") != config["prefix"] or receipt.get("identity", {}).get("instanceId") != identity["instanceId"]:
            raise SafetyError("independent bootstrap receipt differs from guest")
        timer = "sharded-wal-expire-" + config["prefix"] + ".timer"
        command(["systemctl", "is-active", "--quiet", timer], timeout=10)
        if timestamp(receipt["deadline_utc"]) < timestamp(config["deadline_utc"]):
            raise SafetyError("guest independent TTL ends before campaign deadline")
        images = {name: json.loads(command(["docker", "image", "inspect", image], timeout=30)) for name, image in local.IMAGES.items()}
        monitor_images_path = Path("/mnt/tidehunter") / config["prefix"] / "monitor-image-proof.json"
        monitor_images = (json.loads(monitor_images_path.read_text()) if monitor_images_path.is_file()
                          else {"complete": False, "gaps": ["bootstrap monitoring image receipt missing"]})
        trace_tool = None
        if config["spec"].get("mode", "standard") in ("focused", "read-tier", "read-tier-focused"):
            binary = shutil.which("bpftrace")
            trace_tool = {"requested": True, "binary": binary, "binary_sha256": None,
                          "version": None, "package_version": None, "kernel_release": os.uname().release,
                          "kernel_btf_readable": os.access("/sys/kernel/btf/vmlinux", os.R_OK),
                          "permission_probe": "deferred to per-case collector; binary/BTF presence does not prove capability"}
            if binary:
                try:
                    trace_tool["binary_sha256"] = digest(binary)
                    trace_tool["version"] = command([binary, "--version"], timeout=10).strip()
                    trace_tool["package_version"] = command(
                        ["dpkg-query", "-W", "-f=${Version}", "bpftrace"], timeout=10).strip()
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                    trace_tool["probe_error"] = type(exc).__name__ + ": " + str(exc)
        return {"identity": identity, "package": package, "bootstrap": receipt, "ready": ready, "monitor_images": monitor_images,
                "read_tier_trace_tool": trace_tool,
                "expiry_timer": command(["systemctl", "show", timer], timeout=10), "images": images,
                "fabric_admission": admit_fabrics(config, index, clients, root),
                "ldd": command(["ldd", config["binary"]], timeout=30, env=environment(config)),
                "snapshot": snapshot(config)}
    if action == "cold_trace_probe":
        if config["spec"].get("mode", "standard") not in ("focused", "read-tier", "read-tier-focused"):
            raise SafetyError("trace capability probe is restricted to focused mode")
        control_root = guarded(Path("/var/lib/tidehunter-kv-control") / config["prefix"],
                               Path("/var/lib/tidehunter-kv-control"))
        control_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        state = guarded(control_root / ("trace-capability-" + str(index + 1)), control_root)
        script = Path(config["repository_root"]) / "experiments/kv-comparison/cold_device_trace.py"
        duration = min(45, int((timestamp(config["deadline_utc"]) - utc()).total_seconds()))
        if duration < 1:
            return {"identity_backend_available": False, "cleanup_complete": True,
                    "gaps": ["campaign deadline reached before probe"]}
        return probe_trace_capability(script, root, state, duration, environment(config))
    if action in ("monitor_start", "monitor_stop"):
        observer = monitor_module(config)
        spec = monitoring_spec(config, root / "results")
        return observer.start_host(spec, index) if action == "monitor_start" else observer.stop_host(spec, index)
    if action in ("net_floor_serve", "net_floor_run", "net_floor_collect"):
        job = request.get("job")
        expected_index = 0 if action != "net_floor_run" else -1
        if (local_mode is not None or index != expected_index or config["spec"].get("mode") != "focused"
                or not isinstance(job, dict) or job.get("case_kind") != "net_floor"
                or request.get("quiescent") is not True or job.get("case_id") != request.get("case_id")
                or set(request) != {"index", "action", "case_id", "job", "quiescent", "timeout"}):
            raise SafetyError("network floor requires owned quiescent focused hosts")
        cloud_campaign.validate_focused_job(job)
        duration = request["timeout"]
        if type(duration) is not int or not 1 <= duration <= 600:
            raise SafetyError("invalid network floor deadline")
        deadline = min(time.monotonic() + duration,
                       time.monotonic() + (timestamp(config["deadline_utc"]) - utc()).total_seconds())
        net = job["net"]
        field = "rdma_address" if net["interface"] == "rdma0" else "address"
        client_address = config["fleet"]["coordinator"][field]
        server_address = config["fleet"]["nodes"][0][field]
        base = guarded(root / "floors", root)
        base.mkdir(mode=0o700, parents=True, exist_ok=True)
        directory = guarded(base / (job["case_id"] + ("-client" if action == "net_floor_run" else "-server")), base)
        ports = [floors.port_for(job["case_id"], net["tool"], i) for i in range(len(net["sizes"]))]
        if action == "net_floor_collect":
            state = json.loads((directory / "state.json").read_text())
            if state.get("case_id") != job["case_id"] or state.get("net") != net:
                raise SafetyError("network floor server identity differs")
            end = min(deadline, time.monotonic() + 5)
            while time.monotonic() < end and any(not (directory / f"server-{i}.status").exists()
                                                   for i in range(len(state["servers"]))):
                time.sleep(0.05)
            try:
                return {"servers": floor_server_cleanup(directory, state)}
            except Exception as exc:
                raise InfrastructureError("network floor cleanup unconfirmed: " + repr(exc)) from exc
        directory.mkdir(mode=0o700, exist_ok=False)
        if action == "net_floor_serve":
            state = {"case_id": job["case_id"], "net": net, "servers": []}
            try:
                binary, build = floor_binary(directory) if net["tool"] == "tcp_rtt" else (None, None)
                for i, size in enumerate(net["sizes"]):
                    argv = floors.server_argv(net, size, ports[i], server_address, binary)
                    proc = subprocess.Popen([sys.executable, str(Path(floors.__file__)), str(directory), str(i), json.dumps(argv)],
                                            start_new_session=True, stdin=subprocess.DEVNULL,
                                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    identity = floors.process_identity(proc.pid)
                    if identity is None:
                        if floors.process_group_alive(proc.pid):
                            import signal
                            try:
                                os.killpg(proc.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            end = time.monotonic() + 10
                            while time.monotonic() < end and floors.process_group_alive(proc.pid):
                                time.sleep(0.05)
                            if floors.process_group_alive(proc.pid):
                                raise InfrastructureError("floor supervisor vanished with a live process group")
                        raise FioFailure("network floor supervisor exited at startup")
                    state["servers"].append({"pid": proc.pid, "start": identity[1], "argv": argv})
                    dump(directory / "state.json", state)
                while True:
                    ready = []
                    for i, row in enumerate(state["servers"]):
                        if (directory / f"server-{i}.status").exists():
                            raise FioFailure("network floor server exited before listener readiness")
                        identity = floors.process_identity(row["pid"])
                        if identity is None or identity[0] == "Z":
                            raise FioFailure("network floor supervisor exited before listener readiness")
                        pid_path = directory / f"server-{i}.pid"
                        if not pid_path.exists():
                            ready.append(False)
                            continue
                        pid = int(pid_path.read_text())
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise DeadlineError("network floor listener readiness deadline exceeded")
                        ready.append(floors.rdma_listening(pid, ports[i], min(5, remaining),
                                                           directory / f"server-{i}.out")
                                     if net["tool"].startswith("ib_") else floors.tcp_listening(pid, ports[i]))
                    if all(ready):
                        return {"ready": True, "port_base": ports[0], "build": build}
                    if time.monotonic() >= deadline:
                        raise DeadlineError("network floor listener readiness deadline exceeded")
                    time.sleep(0.05)
            except Exception as failure:
                try:
                    floor_server_cleanup(directory, state)
                except Exception as exc:
                    raise InfrastructureError("network floor startup cleanup unconfirmed: " + repr(exc)) from exc
                if isinstance(failure, (FioFailure, subprocess.CalledProcessError, FileNotFoundError)):
                    return {"net_failed": (type(failure).__name__ + ": " + str(failure))[:4096]}
                raise
        try:
            binary, build = floor_binary(directory) if net["tool"] == "tcp_rtt" else (None, None)
            runs = []
            for i, size in enumerate(net["sizes"]):
                argv = floors.client_argv(net, size, ports[i], client_address, server_address, binary)
                semantics = "round_trip" if net["tool"] in ("ib_read_lat", "tcp_rtt") else "half_round_trip"
                row = {"size_bytes": size, "iterations": net["iterations"], "status": "failed",
                       "argv_client": argv, "argv_server": None, "latency_semantics": semantics,
                       "latency_us": dict.fromkeys(floors.LATENCY_FIELDS), "setsockopt": None,
                       "raw_client_output": "", "raw_server_output": "", "raw_sha256": None, "error": None}
                try:
                    status, raw, attempts = floor_client_attempts(
                        argv, deadline, perftest=net["tool"].startswith("ib_"))
                    row["attempts"] = attempts
                    row["raw_client_output"] = raw
                    if status:
                        raise FioFailure("network floor client exited " + str(status))
                    if net["tool"].startswith("ib_"):
                        row["latency_us"] = floors.parse_perftest(raw, size, net["iterations"])
                    elif net["tool"] == "fi_pingpong":
                        row["latency_us"] = floors.parse_fi_pingpong(raw, size)
                    else:
                        value = json.loads(raw)
                        if (value.get("size"), value.get("iterations"), value.get("warmup")) != (size, net["iterations"], net["warmup"]):
                            raise ValueError("TCP helper dimensions differ")
                        row["latency_us"] = {key: value[field_name] / 1000 for key, field_name in
                                             (("min", "min_ns"), ("p50", "p50_ns"), ("avg", "mean_ns"),
                                              ("p90", "p90_ns"), ("p99", "p99_ns"), ("p999", "p999_ns"),
                                              ("max", "max_ns"), ("stdev", "stdev_ns"))}
                        row["setsockopt"] = value["setsockopt"]
                    row["status"] = "completed"
                except (FioFailure, ValueError, KeyError, OSError) as exc:
                    row["error"] = (type(exc).__name__ + ": " + str(exc))[:4096]
                row["raw_sha256"] = hashlib.sha256(row["raw_client_output"].encode()).hexdigest()
                runs.append(row)
            return {"runs": runs, "build": build}
        finally:
            try:
                shutil.rmtree(directory)
            except Exception as exc:
                raise InfrastructureError("network client cleanup unconfirmed: " + repr(exc)) from exc
    if action == "ssd_write_floor":
        job = request.get("job")
        spec = job.get("ssd_write", {}) if isinstance(job, dict) else {}
        append = spec.get("mode") in ("append_pwrite", "append_mmap", "append_mmap_fsync")
        host = spec.get("host", "owner0")
        expected_index = {"client": -1, "owner0": 0}.get(host) if isinstance(host, str) else None
        if (local_mode is not None or expected_index is None
                or index != expected_index or config["spec"].get("mode") != "focused"
                or not isinstance(job, dict) or job.get("case_kind") != action
                or request.get("quiescent") is not True or job.get("case_id") != request.get("case_id")
                or set(request) != {"index", "action", "case_id", "job", "quiescent", "timeout"}):
            raise SafetyError("SSD write floor requires an owned quiescent focused host")
        cloud_campaign.validate_focused_job(job)
        duration = request["timeout"]
        if type(duration) is not int or not 15 <= duration <= 600:
            raise SafetyError("invalid SSD write floor deadline")
        deadline = min(time.monotonic() + duration,
                       time.monotonic() + (timestamp(config["deadline_utc"]) - utc()).total_seconds())
        scratch_root = guarded(root / "fio-scratch", root)
        case_root = guarded(root / "floors" / job["case_id"], root)
        case_root.mkdir(mode=0o700, parents=True, exist_ok=False)
        identity = None
        try:
            spec = job["ssd_write"]
            device = media_device_snapshot(root, index, Path("/proc/diskstats").read_text())
            identity = provision_fio_scratch(scratch_root, job["case_id"], size=spec["region_bytes"],
                                             deadline=deadline, pattern="sparse" if append else "random", retain_cache=True)
            if identity["device"] != root.stat().st_dev:
                raise SafetyError("scratch is not on owner data filesystem")
            compiler = shutil.which("gcc")
            if compiler is None:
                raise FioFailure("gcc unavailable")
            source = Path(__file__).with_name("floors_ssd_write.c")
            binary = case_root / "ssd_write"
            try:
                subprocess.run([compiler, "-O2", "-Wall", "-Werror", "-o", str(binary), str(source)],
                               check=True, capture_output=True, timeout=30)
            except subprocess.CalledProcessError as exc:
                raise FioFailure("write helper build failed") from exc
            argv = floors.write_argv(binary, identity["path"], case_root / "lat.log", spec, identity)
            remaining = deadline - time.monotonic()
            if remaining < (6 if append else spec["runtime_seconds"]) + 2:
                raise DeadlineError("insufficient time for SSD write measurement")
            raw, flags = run_write_fio(argv, identity, scratch_root, case_root, min(60, remaining))
            log = case_root / "lat.log"
            if log.is_symlink() or not log.is_file() or log.stat().st_size > floors.MAX_LOG_BYTES:
                raise FioFailure("write latency log missing or exceeds bound")
            data = log.read_bytes()
            floors.parse_write_log(data, spec)
            return {"raw_base64": base64.b64encode(raw).decode("ascii"),
                    "lat_base64": base64.b64encode(data).decode("ascii"), "argv": argv,
                    "build": {"source_sha256": digest(source), "binary_sha256": digest(binary)},
                    "scratch_identity": {"device": identity["device"], "inode": identity["inode"]},
                    "device_identity": f"{device['major']}:{device['minor']}", "actual_direct": flags["direct"],
                    **({"host_index": index} if "host" in spec else {})}
        except (FioFailure, DeadlineError, subprocess.TimeoutExpired, ValueError, KeyError) as exc:
            return {"fio_failed": str(exc)[:4096]}
        finally:
            try:
                if identity is not None:
                    remove_fio_scratch(scratch_root, job["case_id"], identity)
                    if os.path.lexists(identity["path"]):
                        raise SafetyError("scratch remains after cleanup")
                shutil.rmtree(case_root)
                if os.path.lexists(case_root):
                    raise SafetyError("case directory remains after cleanup")
            except Exception as exc:
                raise InfrastructureError("SSD write floor cleanup unconfirmed: " + repr(exc)) from exc
    if action == "ssd_floor":
        job = request.get("job")
        ssd = job.get("ssd", {}) if isinstance(job, dict) else {}
        expected_index = {"client": -1, "owner0": 0}.get(ssd.get("host", "owner0"))
        if (local_mode is not None or expected_index is None or index != expected_index
                or config["spec"].get("mode") != "focused"
                or not isinstance(job, dict) or job.get("case_kind") != "ssd_floor"
                or request.get("quiescent") is not True or job.get("case_id") != request.get("case_id")
                or set(request) != {"index", "action", "case_id", "job", "quiescent", "timeout"}):
            raise SafetyError("SSD floor requires an owned quiescent focused host")
        cloud_campaign.validate_focused_job(job)
        duration = request["timeout"]
        if type(duration) is not int or not 15 <= duration <= 600:
            raise SafetyError("invalid SSD floor deadline")
        deadline = min(time.monotonic() + duration,
                       time.monotonic() + (timestamp(config["deadline_utc"]) - utc()).total_seconds())
        scratch_root = guarded(root / "fio-scratch", root)
        case_root = guarded(root / "floors" / job["case_id"], root)
        case_root.mkdir(mode=0o700, parents=True, exist_ok=False)
        identity = None
        try:
            ssd = job["ssd"]
            device = media_device_snapshot(root, index, Path("/proc/diskstats").read_text())
            identity = provision_fio_scratch(scratch_root, job["case_id"], size=ssd["region_bytes"],
                                             deadline=deadline, pattern="random")
            if deadline - time.monotonic() < ssd["age_seconds"] + ssd["runtime_seconds"] + 2:
                raise DeadlineError("insufficient time for SSD aging and measurement")
            time.sleep(ssd["age_seconds"])
            binary = shutil.which("fio")
            if binary is None:
                raise FioFailure("fio binary unavailable")
            fio = {**ssd, "location": "local_file"}
            argv = fio_read_command(binary, identity["path"], fio, identity["size"],
                                    runtime_seconds=ssd["runtime_seconds"])
            argv += ["--write_lat_log=" + str(case_root / "lat"), "--log_offset=1", "--lat_percentiles=1"]
            remaining = deadline - time.monotonic()
            if remaining < ssd["runtime_seconds"] + 2:
                raise DeadlineError("insufficient time for SSD measurement")
            raw, flags = run_readonly_fio(argv, identity["device"], identity["inode"], min(60, remaining))
            log = case_root / "lat_clat.1.log"
            if log.is_symlink() or not log.is_file() or log.stat().st_size > floors.MAX_LOG_BYTES:
                raise FioFailure("fio completion latency log missing or exceeds bound")
            data = log.read_bytes()
            floors.parse_fio_log(data, block_bytes=ssd["block_bytes"], region_bytes=ssd["region_bytes"])
            return {"raw_base64": base64.b64encode(raw).decode("ascii"),
                    "lat_base64": base64.b64encode(data).decode("ascii"), "argv": argv,
                    "device_identity": f"{device['major']}:{device['minor']}", "scratch": identity,
                    "opened_read_only": flags["read_only"], "actual_direct": flags["direct"],
                    **({"host_index": index} if "host" in ssd else {})}
        except FioFailure as exc:
            return {"fio_failed": str(exc)[:4096]}
        finally:
            try:
                if identity is not None:
                    remove_fio_scratch(scratch_root, job["case_id"], identity)
                shutil.rmtree(case_root)
            except Exception as exc:
                raise InfrastructureError("SSD floor cleanup unconfirmed: " + repr(exc)) from exc
    if action == "fio_scratch":
        job = request.get("job")
        if (local_mode is not None or index != 0 or config["spec"].get("mode") != "focused"
                or not isinstance(job, dict) or job.get("case_kind") != "fio_floor"
                or job.get("fio", {}).get("location") != "local_file"
                or request.get("quiescent") is not True):
            raise SafetyError("fio scratch requires an owned quiescent focused owner")
        cloud_campaign.validate_focused_job(job)
        if job["case_id"] != request.get("case_id"):
            raise SafetyError("fio scratch case identity differs")
        duration = request.get("timeout")
        if type(duration) is not int or not 15 <= duration <= 600:
            raise SafetyError("invalid fio scratch deadline")
        deadline = min(time.monotonic() + duration,
                       time.monotonic() + (timestamp(config["deadline_utc"]) - utc()).total_seconds())
        device = media_device_snapshot(root, index, Path("/proc/diskstats").read_text())
        scratch_root = guarded(root / "fio-scratch", root)
        identity = None
        try:
            identity = provision_fio_scratch(scratch_root, job["case_id"], deadline=deadline)
            binary = shutil.which("fio")
            if binary is None:
                raise RuntimeError("fio binary unavailable")
            argv = fio_read_command(binary, identity["path"], job["fio"], identity["size"])
            remaining = deadline - time.monotonic()
            if remaining < 12:
                raise DeadlineError("insufficient time for fio measurement")
            raw, flags = run_readonly_fio(argv, identity["device"], identity["inode"], min(60, remaining))
            return {"raw_base64": base64.b64encode(raw).decode("ascii"), "argv": argv,
                    "device_identity": f"{device['major']}:{device['minor']}", "scratch": identity,
                    "opened_read_only": flags["read_only"], "actual_direct": flags["direct"]}
        except FioFailure as exc:
            return {"fio_failed": str(exc)[:4096]}
        finally:
            if identity is not None:
                try:
                    remove_fio_scratch(scratch_root, job["case_id"], identity)
                except Exception as exc:
                    raise InfrastructureError("fio scratch cleanup unconfirmed: " + repr(exc)) from exc
    if action == "snapshot":
        observed = snapshot(config, request.get("unit"), drop=request.get("drop", False))
        resource_job = request.get("resource_job")
        if (isinstance(resource_job, dict) and resource_job.get("owner_count", len(config["fleet"]["nodes"])) == 1
                and resource_job.get("backend") in cloud_campaign.TRANSPORTS and index >= 1):
            name = unit_name(config, resource_job["case_id"], "storage-" + str(index))
            status = unit_status(name)
            if status.get("LoadState") != "not-found" and (
                    status.get("ActiveState") not in ("inactive", "failed") or status.get("MainPID") != "0"):
                raise SafetyError("inactive owner host has a running case owner")
            observed["owner_activity"] = {"active": False, "unit": status}
        if (index >= 0 and isinstance(resource_job, dict)
                and (resource_job.get("case_kind") == "media_read"
                     or (resource_job.get("case_kind") == "live_read" and isinstance(resource_job.get("media"), dict))
                     or (resource_job.get("device_counters") and resource_job.get("backend") in cloud_campaign.TRANSPORTS))
                and index < resource_job.get("owner_count", len(config["fleet"]["nodes"]))):
            try:
                owner_case = request.get("resource_owner_case", resource_job["case_id"])
                owner_root = guarded(root / "cells" / owner_case / ("node-" + str(index)), root, must_exist=True)
                observed["media_device"] = media_device_snapshot(owner_root, index, observed["diskstats"])
            except (OSError, ValueError, KeyError) as exc:
                observed["media_device_error"] = type(exc).__name__ + ": " + str(exc)
        if (index == -1 and isinstance(resource_job, dict) and resource_job.get("backend") == "native"
                and resource_job.get("device_counters")):
            try:
                native_root = guarded(root / "results" / "cases" / resource_job["case_id"] / "database",
                                      root, must_exist=True)
                observed["media_device"] = media_device_snapshot(native_root, index, observed["diskstats"])
            except (OSError, ValueError, KeyError) as exc:
                observed["media_device_error"] = type(exc).__name__ + ": " + str(exc)
        if index == -1 and isinstance(resource_job, dict) and resource_job.get("case_kind") == "native_media_read":
            try:
                cloud_campaign.validate_focused_job(resource_job)
                if "sealed_dataset" in resource_job:
                    source = resource_job["sealed_dataset"]["source_id"]
                    native_root = root / "results" / "datasets" / (source + "-native") / "database"
                else:
                    native_root = root / "results" / "cases" / resource_job["case_id"] / "database"
                native_root = guarded(native_root, root, must_exist=True)
                observed["media_device"] = media_device_snapshot(native_root, index, observed["diskstats"])
            except (OSError, ValueError, KeyError) as exc:
                observed["media_device_error"] = type(exc).__name__ + ": " + str(exc)
        if resource_job is not None:
            if request.get("monitor_job") is not None and request["monitor_job"] != resource_job:
                raise SafetyError("resource and monitoring selections disagree")
            accounting = owned_monitor_processes(config, index, resource_job, request.get("resource_owner_case"))
            observed["resource_accounting"] = resource_accounting.capture(
                index, observed["nvme"]["host_identity"]["boot_id"], accounting["processes"])
            for missing in accounting["missing"]:
                observed["resource_accounting"]["processes"].append({
                    "process_role": missing["role"], "available": False, "reason": missing["reason"][:300]})
            observed["rdma_diagnostics"] = rdma_diagnostics.capture()
        if resource_job is not None and resource_job.get("backend") == "tikv":
            since = request.get("tikv_diagnostic_since")
            if since is not None:
                if not isinstance(since, list) or len(since) != 6:
                    raise SafetyError("TiKV diagnostic boundaries require six host timestamps")
                since = since[index + 1]
                if since is not None:
                    timestamp(since)
            observed["tikv_diagnostics"] = tikv_diagnostics(config, index, since=since)
        if request.get("monitor_job") is not None:
            started = time.monotonic()
            try:
                evidence = owned_monitor_processes(config, index, request["monitor_job"], request.get("resource_owner_case"))
                selected = monitor_module(config).select_host(monitoring_spec(config, root / "results"),
                    index, request["monitor_job"], evidence["processes"])
                observed["monitoring"] = {"complete": not evidence["missing"] and selected.get("complete") is not False,
                                          "selection": selected, **evidence}
            except Exception as exc:
                observed["monitoring"] = {"complete": False, "error": type(exc).__name__ + ": " + str(exc)}
            observed["monitoring"]["elapsed_seconds"] = time.monotonic() - started
        return observed
    if action == "layout":
        backend, operation = request["backend"], request["operation"]
        if backend not in cloud_layout.BACKENDS or operation not in ("prepare", "start", "probe", "stop", "export", "reset-data"):
            raise SafetyError("invalid deployment operation")
        script = Path(config["repository_root"]) / "experiments/kv-comparison/cloud_layout.py"
        argv = [sys.executable, "-B", script, operation, "--fleet-json", json.dumps(config["fleet"]),
                "--backend", backend, "--node-index", str(index)]
        contract = contracts.selected(backend, request.get("ack_contract", "synced"))
        if contract != "synced":
            argv += ["--ack-contract", contract]
        if operation == "probe":
            argv += ["--timeout", str(request["timeout"])]
            if request.get("initialize"):
                argv.append("--initialize")
            if request.get("cluster"):
                argv.append("--cluster")
        return json.loads(command(argv, timeout=request["operation_timeout"], env=environment(config)))
    if action in ("nvme_owner_stop", "nvme_owner_restart", "nvme_owner_export",
                  "nvme_owner_target_setup", "nvme_owner_target_cleanup", "nvme_owner_cleanup_export",
                  "nvme_owner_target_snapshot"):
        if config["spec"].get("mode") not in ("focused", "nvmeof-focused", "standard-with-nvmeof", "standard-with-live-nvmeof") or index < 0:
            raise SafetyError("NVMe owner operations require an admitted NVMe fleet owner")
        if action == "nvme_owner_restart" and config["spec"].get("selection") == q0_campaign.SELECTION:
            raise SafetyError("Q0 does not admit owner restarts")
        if action == "nvme_owner_target_cleanup":
            expected = f"{config['prefix']}/results/cells/{request['case_id']}/storage/{index}/nvme-disconnect/{request['generation']}.json"
            if request.get("host_disconnected_key") != expected:
                raise SafetyError("unexpected disconnect proof location")
            request = {**request, "host_disconnected": read_json(clients, config, expected)}
        import nvme_campaign_owner
        result = nvme_campaign_owner.execute(config, request, command=command,
            validate_launch=validate_launch, unit_status=unit_status, unit_name=unit_name,
            local_qualification=local_mode is not None)
        if action == "nvme_owner_export" and result.get("complete") is True:
            for entry in result["ready"]["exports"]:
                raw = nvme_campaign_owner.read_export_manifest(
                    config, request["case_id"], index, entry["manifest_path"], validate_launch=validate_launch)
                checksum = hashlib.sha256(raw).hexdigest()
                if checksum != entry["manifest_sha256"]:
                    raise SafetyError("owner manifest changed before upload")
                ident = entry["export_uuid"]
                if not isinstance(ident, list) or len(ident) != 16 or any(type(b) is not int or not 0 <= b <= 255 for b in ident):
                    raise SafetyError("invalid export UUID")
                key = f"{config['prefix']}/results/cells/{request['case_id']}/storage/{index}/nvme-manifests/{bytes(ident).hex()}.json"
                put_bytes(clients, config, key, raw, content_type="application/json")
                entry["manifest_key"] = key
                entry["manifest_bytes"] = len(raw)
        return result
    if action in ("nvme_trace_start", "nvme_trace_stop"):
        if config["spec"].get("mode") not in ("focused", "nvmeof-focused", "standard-with-nvmeof", "standard-with-live-nvmeof") or index < 0:
            raise SafetyError("NVMe trace requires an admitted NVMe fleet owner")
        import nvme_campaign_trace
        return nvme_campaign_trace.execute(config, request, clients, command=command)
    if action in ("cgroup_memory_start", "cgroup_memory_stop"):
        if not platform_module(config) or local_mode is not None:
            raise SafetyError("cgroup memory sampling requires an admitted GCP guest")
        import owner_ftrace
        case = request["case_id"]
        role = "client" if index < 0 else "storage-" + str(index)
        name = unit_name(config, case, role)
        directory = guarded(root / "cells" / case / "cgroup-memory" / role, root)
        state_path = directory / "state.json"
        if action == "cgroup_memory_stop":
            state = json.loads(state_path.read_text()) if state_path.is_file() else None
            return {"role": role, "physical_host_index": physical_index, **owner_ftrace.stop_memory(state)}
        status = unit_status(name)
        cgroup = status.get("ControlGroup")
        parent = (owner_slice_limits(config, case)[0]
                  if index >= 0 and case_owner_job(config, case).get("owner_topology") == "colocated"
                  else "system.slice")
        if cgroup != f"/{parent}/{name}.service":
            raise InfrastructureError(f"CGROUP_MEMORY_UNAVAILABLE: no exact owned cgroup for {role}: {cgroup!r}")
        duration = min(owner_ftrace.CPU_MAX_SAMPLES,
                       (timestamp(config["deadline_utc"]) - utc()).total_seconds())
        state = owner_ftrace.start_memory(directory, Path("/sys/fs/cgroup") / cgroup.lstrip("/"), duration)
        dump(state_path, state)
        return {"role": role, **state}
    if action in ("thread_cpu_start", "thread_cpu_stop"):
        import owner_ftrace
        case = request["case_id"]
        state_root = guarded(root / "cells" / case /
                             ("coordinator-cpu" if index < 0 else "node-" + str(index)) / "thread-cpu", root)
        state_root.mkdir(parents=True, exist_ok=True)
        state_path = state_root / "state.json"
        if action == "thread_cpu_start":
            role = "client" if index < 0 else "storage-" + str(index)
            status = unit_status(unit_name(config, case, role))
            pid = int(status.get("MainPID", "0"))
            duration = min(owner_ftrace.CPU_MAX_SAMPLES,
                           (timestamp(config["deadline_utc"]) - utc()).total_seconds())
            state = owner_ftrace.start_threads(state_root, pid, duration)
            job = case_owner_job(config, case)
            if job.get("owner_topology"):
                state["owner_index"] = index
                state["physical_host_index"] = physical_index
                first = index < 0 or all(owner_host_index(config, job, other) != physical_index
                                         for other in range(index))
                if first:
                    state["host_cpu_sampler"] = owner_ftrace.start_cpu(state_root, duration)
            dump(state_path, state)
            return state
        state = json.loads(state_path.read_text()) if state_path.exists() else None
        result = owner_ftrace.stop_threads(state)
        if isinstance(state, dict) and "owner_index" in state:
            result.update(owner_index=state["owner_index"], physical_host_index=state["physical_host_index"])
            if "host_cpu_sampler" in state:
                result["host_cpu"] = owner_ftrace.stop_cpu(state["host_cpu_sampler"])
        return result
    case = request["case_id"]
    if action == "tide_slice_cleanup":
        return cleanup_owner_slice(config, case, physical_index)
    guest_root = guarded(root / "cells" / case, root)
    capsule_root = guarded(guest_root / ("node-" + str(index)), root)
    if action == "live_spdk_snapshot":
        if index < 0 or not admitted_live_campaign(config, case):
            raise SafetyError("live SPDK snapshot requires an admitted owner")
        import yaml
        launch = yaml.safe_load((capsule_root / "launch.yaml").read_text())
        validate_launch(config, launch, case, index)
        if not (launch["node"].get("live_target") or {}).get("spdk"):
            raise SafetyError("live SPDK snapshot requires the exact SPDK owner")
        script = Path(config["repository_root"]) / "experiments/nvmeof/live_spdk_daemon.py"
        target = json.loads(command([sys.executable, "-B", script, "observe", "--state",
                                     capsule_root / "live-spdk-daemon"], timeout=30))
        return {"case_id": case, "index": index, "target": target}
    if action in ("tide_evidence", "tide_provider_facts", "live_case_snapshot"):
        if not isinstance(case, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", case):
            raise SafetyError("unsafe evidence case identity")
        if action in ("tide_evidence", "tide_provider_facts") and index < 0:
            raise SafetyError("owner evidence requires a storage index")
        launch = None
        if index >= 0:
            import yaml
            launch_path = guarded(capsule_root / "launch.yaml", capsule_root, must_exist=True)
            launch = yaml.safe_load(launch_path.read_text())
            validate_launch(config, launch, case, index)
        if action == "tide_provider_facts":
            name = unit_name(config, case, "storage-" + str(index))
            capture = bounded_capture(
                ["journalctl", "--no-pager", "--unit", name, "--output=cat",
                 "--grep=TIDEHUNTER_FABRIC_PROVIDER_FACTS "],
                max_bytes=1048576, timeout=15)
            facts = []
            if capture["available"]:
                for line in capture["text"].splitlines():
                    prefix = "TIDEHUNTER_FABRIC_PROVIDER_FACTS "
                    if line.startswith(prefix):
                        facts.append(json.loads(line[len(prefix):]))
            return {"case_id": case, "index": index, "unit_name": name,
                    "available": capture["available"] and not capture["truncated"],
                    "truncated": capture["truncated"], "records": facts,
                    "scope": "exact owner session journal through collection; may include earlier shared-owner cases"}
        if action == "live_case_snapshot":
            if not admitted_live_campaign(config, case):
                raise SafetyError("live boundary observation requires live campaign")
            if index >= 0 and launch["node"].get("live_target") is None:
                raise SafetyError("live boundary requires exact live owner")
            expected = request.get("expected")
            if not isinstance(expected, dict) or set(expected) not in ({"host_nqn", "transport"}, {"host_nqn", "transport", "owners"}):
                raise SafetyError("exact live initiator identity and transport required")
            live_host_identity(expected["host_nqn"])
            if expected["transport"] not in ("tcp", "rdma"):
                raise SafetyError("invalid live transport")
            prep = live_nvme_preparation(config, case, expected["transport"])
            expected = {**expected, "addresses": prep["addresses"], "service": prep["service"]}
            return live_case_observation(config, case, index, launch, request.get("boundary"), expected)
        name = unit_name(config, case, "storage-" + str(index))
        result = {"case_id": case, "index": index, "unit_name": name,
                  "capture": "read-only failure-time exact owner evidence"}
        try:
            result["unit"] = {"available": True, "value": unit_status(name)}
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            result["unit"] = {"available": False, "reason": (type(exc).__name__ + ": " + str(exc))[:300]}
        try:
            result["journal"] = bounded_owner_journal(name)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            result["journal"] = {"available": False, "reason": (type(exc).__name__ + ": " + str(exc))[:300]}
        if (launch["node"].get("live_target") or {}).get("spdk") is not None:
            try:
                import nvme_campaign_owner
                state = nvme_campaign_owner._read_json(capsule_root / "live-spdk-daemon/state.json")
                result["live_spdk_state"] = {"available": True, "value": state}
                result["live_spdk_journal"] = bounded_owner_journal(state["unit_name"])
            except (OSError, RuntimeError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
                result["live_spdk_state"] = {"available": False, "reason": type(exc).__name__ + ": " + str(exc)}
        result["kernel_errors"] = {"available": False, "reason": "no safe exact-owner kernel log filter"}
        return result
    if action in ("owner_ftrace_start", "owner_ftrace_stop"):
        import owner_ftrace
        if index < 0:
            return {"available": False, "reason": "coordinator has no owner WAL"}
        state_path = capsule_root / "owner-ftrace-state.json"
        instance = Path("/sys/kernel/tracing/instances") / ("th-" + case)
        if action == "owner_ftrace_start":
            duration = min(owner_ftrace.CPU_MAX_SAMPLES,
                           (timestamp(config["deadline_utc"]) - utc()).total_seconds())
            instance = owner_ftrace.start(case, cpu_directory=capsule_root, duration_seconds=duration)
            state = owner_ftrace.START_STATES.pop(str(instance))
            dump(state_path, state)
            return state
        state = json.loads(state_path.read_text()) if state_path.exists() else None
        result = owner_ftrace.stop(instance, capsule_root, state)
        result["trace"] = ((capsule_root / "owner-ftrace.txt").read_text(errors="replace")
                           if (capsule_root / "owner-ftrace.txt").exists() else "")
        cpu_path = capsule_root / "owner-cpu.json"
        result["cpu_samples"] = (json.loads(cpu_path.read_text())
                                 if cpu_path.exists() and cpu_path.stat().st_size <= owner_ftrace.CPU_MAX_BYTES
                                 else {"available": False, "reason": "owner CPU artifact unavailable or exceeds bound"})
        return result
    if action in ("cold_trace_start", "cold_trace_stop"):
        if index < 0:
            return {"available": False, "reason": "coordinator has no owner WAL"}
        import yaml
        launch = yaml.safe_load((capsule_root / "launch.yaml").read_text())
        validate_launch(config, launch, case, index)
        wal_root = guarded(launch["node"]["root"], capsule_root, must_exist=True)
        trace_id = request.get("trace_id", case)
        if not isinstance(trace_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", trace_id):
            raise SafetyError("invalid exact trace identity")
        state = guarded(Path("/var/lib/tidehunter-kv-control") / config["prefix"] /
                        ("cold-" + hashlib.sha256(trace_id.encode()).hexdigest()[:16] + "-" + str(index)),
                        Path("/var/lib/tidehunter-kv-control") / config["prefix"])
        script = Path(config["repository_root"]) / "experiments/kv-comparison/cold_device_trace.py"
        argv = [sys.executable, "-B", script, "start" if action == "cold_trace_start" else "stop",
                "--state-dir", state]
        if action == "cold_trace_start":
            duration = min(int(request["duration_seconds"]), int((timestamp(config["deadline_utc"]) - utc()).total_seconds()))
            if duration < 1:
                return {"available": False, "reason": "campaign deadline reached"}
            argv += ["--wal-path", wal_root, "--duration-seconds", str(duration)]
        try:
            return json.loads(command(argv, timeout=45, env=environment(config)))
        except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
            return {"available": False, "reason": type(exc).__name__ + ": " + str(exc)}
    name = unit_name(config, case, "storage-" + str(index))
    if action == "tide_start":
        executable, binary_seal = resolve_case_binary(config, case_owner_job(config, case))
        if request.get("case_binary") != binary_seal:
            raise SafetyError("owner and coordinator binary seals differ")
        expected_key = f"{config['prefix']}/deploy/cells/{case}/storage-{index}.tar.gz"
        if request["key"] != expected_key:
            raise SafetyError("unexpected capsule S3 path")
        archive = guarded(root / ("capsule-" + hashlib.sha256(case.encode()).hexdigest()[:16] + "-" + str(index) + ".tar.gz"), root)
        download_object(clients, config, expected_key, archive)
        if digest(archive) != request["sha256"]:
            raise SafetyError("node capsule bytes differ from admission")
        safe_extract_capsule(archive, capsule_root, request["files"])
        archive.unlink()
        import yaml
        launch = yaml.safe_load((capsule_root / "launch.yaml").read_text())
        validate_launch(config, launch, case, index)
        before_wal = (owner_case_device_snapshot(capsule_root, index)
                      if config["spec"].get("selection") == cloud_campaign.REDRAIN_AB_SELECTION else None)
        if launch["node"].get("live_target") is not None:
            live_state = guarded(launch["node"]["live_target"]["state_dir"], capsule_root)
            live_state.mkdir(mode=0o700, exist_ok=False)
        live_spdk = (launch["node"].get("live_target") or {}).get("spdk")
        daemon = None
        if live_spdk is not None:
            script = Path(config["repository_root"]) / "experiments/nvmeof/live_spdk_daemon.py"
            deadline_epoch = min(timestamp(config["deadline_utc"]).timestamp(), time.time() + 17990)
            daemon = json.loads(command([sys.executable, "-B", script, "start", "--state",
                capsule_root / "live-spdk-daemon", "--socket", live_spdk["rpc_socket"],
                "--executable", live_spdk["executable"], "--deadline-epoch", str(deadline_epoch)],
                timeout=45))
        seconds = min(request["timeout"], int((timestamp(config["deadline_utc"]) - utc()).total_seconds()))
        cpus, memory = owner_process_budget(config, case_owner_job(config, case), index)
        shared_slice = (configure_owner_slice(config, case)
                        if case_owner_job(config, case).get("owner_topology") == "colocated" else None)
        command(systemd_argv(config, case, "storage-" + str(index), cpus,
                             memory, seconds,
                             [str(executable), "--experimental-storage-node", capsule_root / "launch.yaml"],
                             setenv={**({"TIDEHUNTER_HISTORY_DIAGNOSTICS": "1"} if request.get("owner_history_diagnostics") else {}),
                                     **({"TIDEHUNTER_OWNER_EVENTS": "1"} if request.get("owner_trace") else {})}), timeout=20)
        deadline = time.monotonic() + min(90, seconds)
        while time.monotonic() < deadline:
            ready = Path(launch["ready_file"])
            status = unit_status(name)
            if status.get("SubState") == "exited" or status.get("ActiveState") in ("failed", "inactive"):
                raise InfrastructureError("storage provider exited before readiness: " + json.dumps(status))
            if ready.is_file():
                address = ready.read_text().strip()
                if address != launch["listen"]:
                    raise SafetyError("storage readiness endpoint differs from typed launch")
                result = {"ready": address, "launch_sha256": digest(capsule_root / "launch.yaml"), "unit": status}
                if binary_seal is not None:
                    result["case_binary"] = binary_seal
                result["durability_receipt"] = owner_durability_receipt(launch)
                if before_wal is not None:
                    result["case_device"] = before_wal
                if shared_slice is not None:
                    result["owner_slice"] = {"name": shared_slice, "limits": owner_slice_limits(config, case)[1],
                                             "observed": unit_status(shared_slice)}
                if daemon is not None:
                    result["live_spdk_daemon"] = daemon
                if launch["node"].get("live_target") is not None:
                    import nvme_campaign_owner
                    result["live_owner_identity"] = nvme_campaign_owner.capture_live_owner_identity(
                        capsule_root, capsule_root / "launch.yaml", status)
                return result
            time.sleep(0.2)
        raise InfrastructureError("storage provider readiness expired")
    if action == "tide_stop":
        import yaml
        launch = yaml.safe_load((capsule_root / "launch.yaml").read_text())
        validate_launch(config, launch, case, index)
        guarded(launch["stop_file"], guest_root).touch(exist_ok=True)
        status = unit_status(name)
        # Let the engine observe its typed stop marker and drain before asking
        # systemd to reap the exact unit. Forced cleanup is still bounded.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            stopped = unit_status(name)
            if stopped.get("MainPID") == "0" or stopped.get("SubState") == "exited":
                break
            time.sleep(0.2)
        command(["systemctl", "stop", name], timeout=35)
        logs = command(["journalctl", "--no-pager", "--unit", name, "--output=short-precise"], timeout=30)
        after = unit_status(name)
        if after.get("LoadState") != "not-found" and (
                after.get("ActiveState") not in ("inactive", "failed") or after.get("MainPID") != "0"):
            raise InfrastructureError("owned storage unit did not quiesce")
        events = Path("/sys/fs/cgroup/system.slice") / (name + ".service") / "cgroup.events"
        cgroup_events = events.read_text() if events.exists() else None
        if cgroup_events and dict(line.split() for line in cgroup_events.splitlines()).get("populated") != "0":
            raise InfrastructureError("owned storage cgroup remains populated after stop")
        result = {"before": status, "after": after, "logs": logs,
                  "stop_command_completed": True, "cgroup_events_after_stop": cgroup_events}
        if request.get("owner_history_diagnostics"):
            history_path = (capsule_root / "launch.yaml").with_suffix(".history-maintenance.json")
            result["history_maintenance"] = json.loads(guarded(history_path, guest_root, must_exist=True).read_text())
        if request.get("owner_trace"):
            owner_path = (capsule_root / "launch.yaml").with_suffix(".owner-events.json")
            result["owner_events"] = json.loads(guarded(owner_path, guest_root, must_exist=True).read_text())
        if launch["node"].get("live_target") is not None:
            import nvme_campaign_owner
            try:
                result["live_target_shutdown"] = nvme_campaign_owner.admit_live_shutdown(
                    capsule_root, capsule_root / "launch.yaml", launch, index, stopped)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise InfrastructureError("live target shutdown unconfirmed; owner database retained: " + str(exc)) from exc
        if (launch["node"].get("live_target") or {}).get("spdk") is not None:
            if request.get("initiator_drained") is not True:
                raise InfrastructureError("live SPDK initiator drain unconfirmed; owner and daemon retained")
            script = Path(config["repository_root"]) / "experiments/nvmeof/live_spdk_daemon.py"
            result["live_spdk_daemon"] = json.loads(command([sys.executable, "-B", script, "stop",
                "--state", capsule_root / "live-spdk-daemon", "--owner-proof",
                (capsule_root / "launch.yaml").with_suffix(".live-target-shutdown.json"),
                "--initiator-drained"], timeout=45))
        database = guarded(launch["node"]["root"], capsule_root, must_exist=True)
        shutil.rmtree(database)
        result["database_removed"] = str(database)
        if config["spec"].get("selection") == cloud_campaign.REDRAIN_AB_SELECTION:
            result["case_device"] = owner_case_device_snapshot(capsule_root, index)
        return result
    raise SafetyError("unsupported guest action")


class Controller:
    def __init__(self, config, clients, *, clock=time.monotonic, local_mode=None):
        self.local_mode = local_mode
        self.config = validate_config(config) if local_mode is None else local_mode.validate_config(config)
        self.clients = clients
        self.clock = clock
        deadline = timestamp(config["deadline_utc"])
        if platform_module(config):
            campaign_deadline = timestamp(config["platform"]["runtime"]["campaign_deadline_utc"])
            if campaign_deadline >= deadline:
                raise SafetyError("campaign must leave time for retrieval before VM deletion")
            deadline = campaign_deadline
        seconds = (deadline - utc()).total_seconds()
        self.end = clock() + min(seconds, config["spec"]["hard_deadline_seconds"])
        self.work_end = self.end - config["spec"]["export_reserve_seconds"]
        self.root = guarded(Path(config["fleet"]["data_root"]) / "results", Path(config["fleet"]["data_root"]))
        self.ids = [h["instance_id"] for h in [config["fleet"]["coordinator"], *config["fleet"]["nodes"]]]
        self.sequence = 0
        self.sequence_lock = threading.Lock()
        self.active_backend = None
        self.active_tide = None
        self.tide_cleanup_forbidden = False
        self.shared_tide = None
        self.shared_native = {}
        self.active_nvme = None
        self.package = None
        self.root_owned = False
        self.expiry = None
        self.monitoring_attempted = False
        self.monitoring_events = []
        self.monitoring_complete = True
        self.resource_io_complete = None

    def remaining(self, *, export=False):
        remaining = (self.end if export else self.work_end) - self.clock()
        if remaining <= 0:
            raise DeadlineError("absolute deadline/export reserve reached")
        return remaining

    def client_runtime_seconds(self, limit, *, minimum=1):
        try:
            available = self.remaining()
        except DeadlineError as exc:
            raise CampaignTimeLimit("campaign time limit reached") from exc
        seconds = min(limit, int(available))
        if seconds < minimum:
            raise CampaignTimeLimit("campaign time limit reached")
        return seconds

    def guard(self, *, terminating=False, starting=False):
        if self.local_mode is not None:
            return self.local_mode.guard()
        platform = platform_module(self.config)
        if platform:
            return platform.verify_fleet(self.config, self.clients, terminating=terminating, starting=starting)
        response = self.clients["ec2"].describe_instances(InstanceIds=self.ids)
        actual = verify_instances(self.config, response, terminating=terminating, starting=starting)
        tags = {item["Key"]: item["Value"] for item in actual[self.ids[0]]["Tags"]}
        expiry = timestamp(tags["ExpiresAt"])
        if self.expiry is None:
            self.expiry = expiry
        elif self.expiry != expiry:
            raise SafetyError("current instance expiry differs from immutable startup admission")
        return actual

    def persist(self, name, value):
        dump(self.root / name, value)
        return persist_json(self.clients, self.config, self.config["prefix"] + "/results/" + name, value, replace=True)

    def request(self, index, request, *, case="startup", timeout=180, export=False):
        available = self.remaining(export=export)
        if available < 1:
            raise DeadlineError("insufficient whole-second control-operation budget")
        timeout = max(1, min(int(timeout), int(available)))
        logical_index = index
        action = request.get("action", "")
        job = case_owner_job(self.config, request.get("case_id", case))
        owner_action = (action.startswith("tide_") or action.startswith("live_")
                        or action.startswith("nvme_owner_") or action.startswith("thread_cpu_")
                        or action.startswith("cgroup_memory_"))
        if index >= 0 and job.get("owner_topology") and owner_action:
            index = owner_host_index(self.config, job, logical_index)
            request = {**request, "owner_index": logical_index}
        request = {**request, "index": index}
        if self.local_mode is not None:
            return self.local_mode.request(index, request, case=case, timeout=timeout, export=export)
        if request.get("action") == "layout":
            request["operation_timeout"] = timeout
        if request.get("action") == "layout" and request.get("operation") == "probe":
            request["timeout"] = min(request["timeout"], timeout)
        platform = platform_module(self.config)
        if platform and index != -1:
            return platform.request_remote(self, index, request, case=case, timeout=timeout, export=export)
        if index == -1:
            self.guard()
            return guest_operation(self.config, request, self.clients)
        with self.sequence_lock:
            self.sequence += 1
            number = self.sequence
        key = f"{self.config['prefix']}/results/cells/{case}/storage/{index}/{number:06d}-{request['action']}.json"
        deadline = min(self.end if export else self.work_end, self.clock() + timeout)
        deadline_epoch = min(timestamp(self.config["deadline_utc"]).timestamp(),
                             utc().timestamp() + max(0, deadline - self.clock()))
        payload = {"execution_schema_version": 1, "deadline_epoch": deadline_epoch,
                   "request": request, "key": key}
        encode = lambda v: base64.b64encode(json.dumps(v).encode()).decode()
        script = Path(self.config["repository_root"]) / "experiments/kv-comparison/cloud_controller.py"
        argv = ["python3", str(script), "guest", "--config-base64", encode(self.config), "--request-base64", encode(payload)]
        shell = "exec " + shlex.join(argv)
        if request["action"] == "bootstrap":
            marker = Path("/mnt/tidehunter") / self.config["prefix"] / "kv-bootstrap-ready.json"
            waiting = "while [ ! -f " + shlex.quote(str(marker)) + " ]; do sleep 1; done"
            shell = shlex.join(["timeout", str(max(1, timeout - 60)), "sh", "-c", waiting]) + " && " + shell
        intent = {"instance_id": self.ids[index + 1], "request": request, "key": key, "sent_utc": utc().isoformat()}
        journal = self.root / "commands" / f"{number:06d}.json"
        attempts = []
        command_id = None

        def record(status, **evidence):
            dump(journal, {**intent, "status": status, "attempts": attempts, **evidence})

        def receipt_if_present():
            try:
                result = read_json(self.clients, self.config, key)
                if not isinstance(result, dict):
                    raise SafetyError("guest response is not a control-operation receipt")
                return result
            except Exception as exc:
                response = getattr(exc, "response", {})
                code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
                if code in ("NoSuchKey", "404", "NotFound"):
                    return None
                raise

        def admit(result, *, reconciled):
            if (not isinstance(result, dict) or result.get("request") != request
                    or result.get("instance_id") != self.ids[index + 1]):
                raise SafetyError("guest response not bound to exact request/host")
            dump(self.root / "guest" / case / f"storage-{index}-{number:06d}.json", result)
            if result.get("status") != "completed":
                raise InfrastructureError("guest operation failed: " + str(result.get("error")))
            if "result" not in result:
                raise SafetyError("completed guest response has no operation result")
            record("completed", command_id=command_id, reconciled=reconciled)
            return result["result"]

        record("intent")
        ambiguous = 0
        # Explicit bootstrap nonacceptance can wait for agent registration.
        # Ambiguous replay is permitted only for a read-only snapshot; it uses
        # the identical payload, key and guest execution gate.
        for _ in range(timeout + 1):
            if self.clock() >= deadline:
                break
            self.remaining(export=export)
            self.guard()
            if self.clock() >= deadline:
                break
            attempts.append({"number": len(attempts) + 1, "status": "intent"})
            record("sending")
            try:
                response = self.clients["ssm"].send_command(InstanceIds=[self.ids[index + 1]], DocumentName="AWS-RunShellScript",
                    Parameters={"commands": [shell], "executionTimeout": [str(timeout)]},
                    TimeoutSeconds=max(30, min(2592000, timeout)), Comment="TideHunter exact-owned comparison")
                command_id = response["Command"]["CommandId"]
                if not isinstance(command_id, str) or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", command_id):
                    raise ValueError("missing/invalid command identity")
                attempts[-1].update(status="sent", command_id=command_id)
                record("sent", command_id=command_id)
                break
            except self.clients["ssm"].exceptions.InvalidInstanceId as exc:
                attempts[-1].update(status="known_not_accepted", error=repr(exc))
                record("known_not_accepted")
                if request["action"] != "bootstrap":
                    raise InfrastructureError("SSM rejected exact guest; no command was accepted") from exc
                time.sleep(min(2, max(0, deadline - self.clock())))
            except Exception as exc:
                command_id = None
                attempts[-1].update(status="unknown_send_response", error=repr(exc))
                record("unknown_send_response")
                self.remaining(export=export)
                if self.clock() >= deadline:
                    break
                result = receipt_if_present()
                if result is not None:
                    return admit(result, reconciled=True)
                if not read_only_snapshot(request) or ambiguous == MAX_AMBIGUOUS_REPLAYS:
                    break
                ambiguous += 1
                time.sleep(min(0.5, max(0, deadline - self.clock())))
        for _ in range(2 * timeout + 1):
            if self.clock() >= deadline:
                break
            self.remaining(export=export)
            if command_id is None:
                result = receipt_if_present()
                if result is not None:
                    return admit(result, reconciled=True)
            else:
                try:
                    invocation = self.clients["ssm"].get_command_invocation(
                        CommandId=command_id, InstanceId=self.ids[index + 1])
                except self.clients["ssm"].exceptions.InvocationDoesNotExist:
                    time.sleep(min(0.5, max(0, deadline - self.clock())))
                    continue
                status = invocation.get("Status")
                if status not in ("Pending", "InProgress", "Delayed"):
                    record("invocation_terminal", command_id=command_id, invocation=invocation)
                    if status in ("TimedOut", "DeliveryTimedOut", "ExecutionTimedOut") and read_only_snapshot(request):
                        result = receipt_if_present()
                        if result is not None:
                            return admit(result, reconciled=True)
                        raise SnapshotTimeout("read-only snapshot SSM invocation timed out")
                    if status != "Success" or invocation.get("ResponseCode") != 0:
                        raise InfrastructureError("SSM command failed/unknown terminal status: " + str(status))
                    return admit(read_json(self.clients, self.config, key), reconciled=False)
            time.sleep(min(0.5, max(0, deadline - self.clock())))
        record("deadline_exceeded", command_id=command_id)
        if read_only_snapshot(request):
            raise SnapshotTimeout("SSM read-only snapshot deadline exceeded")
        raise InfrastructureError("SSM exact operation deadline exceeded; state may still be live")

    def all_hosts(self, request, *, case="startup", timeout=180, export=False, owner_count=None):
        # A retry gets a fresh operation key only for a genuinely read-only
        # snapshot. The measurement gate remains closed until all hosts return.
        cancelled = threading.Event()
        def capture(index):
            if index == -1 or not read_only_snapshot(request) or timeout < 4:
                return self.request(index, request, case=case, timeout=timeout, export=export)
            end = min(self.end if export else self.work_end, self.clock() + timeout)
            first = min(60, max(1, timeout - 10))
            try:
                return self.request(index, request, case=case, timeout=first, export=export)
            except SnapshotTimeout:
                if cancelled.is_set():
                    raise
                remaining = int(end - self.clock())
                if remaining < 1:
                    raise
                return self.request(index, request, case=case, timeout=remaining, export=export)

        pool = ThreadPoolExecutor(max_workers=6)
        host_count = len(self.config["fleet"]["nodes"])
        active_count = host_count if owner_count is None else owner_count
        job = case_owner_job(self.config, request.get("case_id", case))
        if job.get("owner_topology") and request.get("action") in (
                "snapshot", "owner_ftrace_start", "owner_ftrace_stop", "cold_trace_start", "cold_trace_stop"):
            active_count = min(active_count, cloud_campaign.job_owner_hosts(job))
        indices = range(-1, active_count)
        futures = [pool.submit(capture, index) for index in indices]
        try:
            done, _ = wait(futures, return_when=FIRST_EXCEPTION)
            for future in futures:
                if future in done and future.exception() is not None:
                    cancelled.set()
                    for pending in futures:
                        pending.cancel()
                    raise future.exception()
            return [future.result() for future in futures] + [
                {"inactive": True} for _ in range(active_count, host_count)]
        finally:
            # A read-only snapshot may still be finishing on another host, but
            # it cannot release the case gate or mutate the measured state.
            # Mutations must settle before cleanup can safely begin.
            pool.shutdown(wait=not (cancelled.is_set() and read_only_snapshot(request)),
                          cancel_futures=cancelled.is_set())

    def bootstrap(self):
        self.root.mkdir(parents=True, exist_ok=False)
        self.root_owned = True
        monitor_module(self.config).validate_spec(monitoring_spec(self.config, self.root))
        identity = verify_local_identity(self.config, -1)
        platform = platform_module(self.config)
        if platform:
            return platform.bootstrap_controller(self)
        account = self.clients["sts"].get_caller_identity()
        if account.get("Account") != self.config["account"]:
            raise SafetyError("caller account differs from exact campaign account")
        self.guard(starting=True)
        # First export happens before package inspection or any remote wait.
        self.persist("startup.json", {"status": "starting", "observed_utc": utc().isoformat(), "identity": identity,
            "config": self.config, "caller": account})
        self.package = verify_package(self.config)
        deadline = self.clock() + min(600, self.remaining())
        while self.clock() < deadline:
            current = self.guard(starting=True)
            if all(host["State"]["Name"] == "running" for host in current.values()):
                break
            time.sleep(2)
        else:
            raise InfrastructureError("exact owned fleet never reached running state")
        proofs = self.all_hosts({"action": "bootstrap"}, timeout=300)
        validate_physical_proofs([x["snapshot"]["nvme"] for x in proofs])
        dump(self.root / "bootstrap-proofs.json", proofs)
        self.persist("startup.json", {"status": "admitted", "identity": identity, "config": self.config,
            "package": self.package, "bootstrap_proofs_sha256": digest(self.root / "bootstrap-proofs.json")})
        self.start_monitoring()
        self.observe("access_receipt", lambda: dump(self.root / "observability/access.json",
                                                  monitoring_access(self.config, proofs)), retain_receipt=False)


    def observe(self, operation, function, *, retain_receipt=True):
        """Observer failure is evidence, never a reason to discard raw results."""
        started = self.clock()
        try:
            value = function()
            complete = not isinstance(value, dict) or value.get("complete") is not False
            event = {"operation": operation, "complete": complete}
            if retain_receipt:
                event["receipt"] = value
        except Exception as exc:
            event = {"operation": operation, "complete": False,
                     "error": type(exc).__name__ + ": " + str(exc)}
            value = None
        event.update({"elapsed_seconds": max(0, self.clock() - started), "observed_utc": utc().isoformat()})
        with self.sequence_lock:
            self.monitoring_events.append(event)
            self.monitoring_complete = self.monitoring_complete and event["complete"]
        return value

    def observe_resource_interval(self, case_id, resources):
        coverage = resource_accounting.coverage(resources)
        with self.sequence_lock:
            self.resource_io_complete = (coverage["io_complete"] if self.resource_io_complete is None
                                         else self.resource_io_complete and coverage["io_complete"])
        self.observe("resource_interval:" + case_id, lambda: {
            "complete": coverage["core_complete"], "core_complete": coverage["core_complete"],
            "io_complete": coverage["io_complete"], "resource_complete": coverage["complete"],
            "path": "resource-interval.json", "scope": resource_accounting.SCOPE})
        return coverage

    def observer_hosts(self, action, *, export=False, timeout=30):
        def host(index):
            return self.observe(f"{action}:{index}", lambda: self.request(index,
                {"action": action}, timeout=timeout, export=export))
        with ThreadPoolExecutor(max_workers=6) as pool:
            return list(pool.map(host, range(-1, len(self.config["fleet"]["nodes"]))))

    def monitor_spec(self):
        if self.local_mode is not None:
            return self.local_mode.monitor_spec(self.root)
        return monitoring_spec(self.config, self.root)

    def start_monitoring(self):
        self.monitoring_attempted = True  # Partial starts remain exact-owned cleanup candidates.
        self.observer_hosts("monitor_start", timeout=90)
        if not self.monitoring_complete:
            raise InfrastructureError("monitoring host readiness failed; refusing unobserved benchmark")
        self.observe("collector_start", lambda: monitor_module(self.config).start_collector(
            self.monitor_spec(), timeout_seconds=min(120, self.remaining())))
        if not self.monitoring_complete:
            raise InfrastructureError("Prometheus/Grafana readiness failed; refusing unobserved benchmark")

    def select_monitoring(self, job, directory):
        if not self.monitoring_attempted:
            return
        self.observe("case_select:" + job["case_id"], lambda: monitor_module(self.config).select_case(
            self.monitor_spec(), job, directory / "result"))

    def finish_monitoring(self, job, directory, receipt):
        if not self.monitoring_attempted:
            return
        summary = self.observe("case_summary:" + job["case_id"],
            lambda: cloud_campaign.summarize_case(directory, local_mode=self.local_mode is not None), retain_receipt=False) if receipt["raw_artifacts_validated"] else None
        def finish():
            evidence = receipt
            for key in ("counter_evidence", "observed", "effective_configuration", "client", "write_profile_limit"):
                evidence = evidence.get(key) if isinstance(evidence, dict) else None
            observed = {"write_profile_enabled": evidence > 0} if type(evidence) is int and evidence >= 0 else {}
            read_evidence = receipt.get("counter_evidence", {}).get("observed")
            if isinstance(read_evidence, dict):
                observed["read_evidence"] = read_evidence
            interval = receipt.get("measurement_read_evidence", {}).get("observed")
            if isinstance(interval, dict):
                observed["measurement_read_evidence"] = interval
            overhead = receipt.get("q0_overhead")
            if overhead is not None and overhead.get("available") is not False:
                observed["measurement_read_evidence"] = {**interval, "q0_overhead": overhead}
            resources = receipt.get("resource_evidence", {}).get("observed")
            if isinstance(resources, dict):
                observed["resource_evidence"] = resources
            diagnostics = receipt.get("write_diagnostics", {}).get("observed")
            if isinstance(diagnostics, dict):
                observed["write_diagnostics"] = diagnostics
                observed["write_profile_enabled"] = diagnostics["runtime_enabled"]
            diagnostics = receipt.get("read_diagnostics", {}).get("observed")
            if isinstance(diagnostics, dict):
                monitor_diagnostics = copy.deepcopy(diagnostics)
                profile = monitor_diagnostics.get("profile")
                if isinstance(profile, dict):
                    profile.pop("caller_correlation", None)
                observed["read_diagnostics"] = monitor_diagnostics
                observed["read_profile_enabled"] = diagnostics["runtime_enabled"]
            correlation_summaries = []
            summary_errors = []
            exporter_path = str(Path(__file__).resolve().parent / "observability")
            if exporter_path not in sys.path:
                sys.path.insert(0, exporter_path)
            import campaign_exporter
            for kind, key in (("write", "write_timeline"), ("read", "read_diagnostics"),
                              ("valkey", "valkey_diagnostics")):
                binding = receipt.get(key)
                if not isinstance(binding, dict):
                    continue
                try:
                    if kind == "write":
                        path = guarded(directory / binding["path"], directory, must_exist=True)
                        if digest(path) != binding["sha256"]:
                            raise ValueError("timeline changed after admission")
                        artifact = json.loads(path.read_text())
                    else:
                        artifact = binding["observed"]
                    correlation_summaries.append(campaign_exporter.summarize_correlated_timings(
                        kind, artifact, cloud_campaign.diagnostic_backend(job)))
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                    summary_errors.append({"kind": kind, "error": type(exc).__name__ + ": " + str(exc)})
            if summary_errors:
                receipt["correlation_summary_errors"] = summary_errors
            observed["correlation_summaries"] = correlation_summaries
            return monitor_module(self.config).finish_case(self.monitor_spec(),
                job, directory / "result", metrics=summary.get("metrics") if summary else None,
                status=receipt["status"], timeout_seconds=min(2, self.remaining(export=True)),
                read_tier_effective=receipt.get("read_tier_effective"),
                cold_device_evidence=terminal_device_evidence(job, receipt),
                spdk_target_io=receipt.get("spdk_target_io"), **observed)
        self.observe("case_finish:" + job["case_id"], finish)

    def export_monitoring(self):
        if not self.monitoring_attempted:
            return
        def capture():
            timeout = self.observer_cleanup_budget(240, following_seconds=40)
            destination = guarded(self.root / "observability/monitoring.tar.gz", self.root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            return monitor_module(self.config).export_session(self.monitor_spec(),
                destination, timeout_seconds=timeout)
        self.observe("snapshot_export", capture)

    def stop_monitoring(self):
        if not self.monitoring_attempted:
            return
        self.observe("collector_stop", lambda: monitor_module(self.config).stop_collector(
            self.monitor_spec(),
            timeout_seconds=self.observer_cleanup_budget(20, following_seconds=20)))
        try:
            timeout = self.observer_cleanup_budget(20)
        except DeadlineError:
            self.observe("host_stop_budget", lambda: self.observer_cleanup_budget(20))
            return
        self.observer_hosts("monitor_stop", export=True, timeout=timeout)

    def observer_cleanup_budget(self, maximum, *, following_seconds=0):
        # The monitoring snapshot and exact-owned Docker shutdowns may consume
        # only their share; raw CSV/config archive and upload keep a floor.
        raw_floor = min(150, self.config["spec"]["export_reserve_seconds"] / 4)
        available = self.remaining(export=True) - raw_floor - following_seconds
        if available < 1:
            raise DeadlineError("observer cleanup budget exhausted; preserving raw export reserve")
        return min(maximum, available)

    def monitoring_receipt(self):
        return {"schema_version": 1, "attempted": self.monitoring_attempted,
            "complete": self.monitoring_attempted and self.monitoring_complete,
            "resource_io_complete": self.resource_io_complete,
            "placement": "controller-cohosted observer on root volume; capped, not proven non-perturbing",
            "spec": self.monitor_spec(), "events": self.monitoring_events}

    def layout(self, backend, operation, index, **kwargs):
        contract = getattr(self, "active_ack_contract", "synced")
        if contract != "synced":
            kwargs.setdefault("ack_contract", contract)
        return self.request(index, {"action": "layout", "backend": backend, "operation": operation, **kwargs},
                            case="startup", timeout=kwargs.get("timeout", 180))

    def start_backend(self, backend, ack_contract="synced"):
        self.active_ack_contract = contracts.selected(backend, ack_contract)
        self.active_backend = backend  # Retain partial start for finally cleanup.
        proofs = []
        for index in range(-1, len(self.config["fleet"]["nodes"])):
            self.layout(backend, "prepare", index)
            self.layout(backend, "start", index)
            if backend == "cassandra" and index >= 0:
                self.layout(backend, "probe", index, timeout=180)
        standalone = backend == "valkey_rdma" and len(self.config["fleet"]["nodes"]) == 1
        proofs.append(self.layout(backend, "probe", -1, timeout=180, initialize=not standalone, cluster=not standalone))
        for index in range(len(self.config["fleet"]["nodes"])):
            proofs.append(self.layout(backend, "probe", index, timeout=180, cluster=not standalone))
        if self.local_mode is None:
            provider = "gcp" if platform_module(self.config) else "aws"
            hosts = [self.config["fleet"]["coordinator"], *self.config["fleet"]["nodes"]] if provider == "gcp" else []
            validate_physical_proofs([x["proof"]["nvme"] for x in proofs], provider=provider,
                                     expected_instance_ids=[host["numeric_id"] for host in hosts] if hosts else None)
        elif any(x["proof"]["nvme"].get("independent_nvme_admitted") is not False for x in proofs):
            raise SafetyError("local layout must not claim independent NVMe")
        dump(self.root / "deployments" / (backend + "-" + str(self.sequence)) / "proofs.json", proofs)

    def cleanup_deadline(self):
        # Half the independent export reserve cannot be consumed by a wedged
        # store shutdown. Exact-fleet termination remains the final backstop.
        reserve = self.config["spec"]["export_reserve_seconds"] / 2
        return min(self.clock() + min(300, reserve), self.end - reserve)

    def cleanup_timeout(self, deadline, requested=90):
        remaining = min(deadline - self.clock(), self.remaining(export=True), requested)
        if remaining < 1:
            raise DeadlineError("cleanup budget exhausted; preserving raw export reserve")
        return int(remaining)

    def stop_backend(self):
        backend = self.active_backend
        if not backend:
            return
        errors = []
        deadline = self.cleanup_deadline()
        deployment = self.root / "deployments" / f"{backend}-stop-{self.sequence}"
        # No host is stopped before all six export attempts finish. Even if
        # export fails, stop every exact-owned host, but preserve its data.
        for index in range(-1, len(self.config["fleet"]["nodes"])):
            try:
                evidence = self.request(index, {"action": "layout", "backend": backend, "operation": "export",
                                        **({"ack_contract": self.active_ack_contract}
                                           if getattr(self, "active_ack_contract", "synced") != "synced" else {})},
                                        timeout=self.cleanup_timeout(deadline), export=True)
                dump(deployment / f"host-{index}-export.json", evidence)
            except Exception as exc:
                errors.append(f"host {index} export: {exc!r}")
        for index in range(-1, len(self.config["fleet"]["nodes"])):
            try:
                stopped = self.request(index, {"action": "layout", "backend": backend, "operation": "stop",
                                       **({"ack_contract": self.active_ack_contract}
                                          if getattr(self, "active_ack_contract", "synced") != "synced" else {})},
                                       timeout=self.cleanup_timeout(deadline), export=True)
                dump(deployment / f"host-{index}-stop.json", stopped)
            except Exception as exc:
                errors.append(f"host {index} stop: {exc!r}")
        self.active_backend = None
        if errors:
            dump(deployment / "cleanup.json", {"backend": backend, "reset_skipped": True, "errors": errors})
            raise InfrastructureError("backend export/stop incomplete; data reset forbidden: " + "; ".join(errors))
        # Fleet-wide stop admission is a prerequisite for the node-local
        # reset helper, which can prove only that its own containers are absent.
        for index in range(-1, len(self.config["fleet"]["nodes"])):
            try:
                reset = self.request(index, {"action": "layout", "backend": backend, "operation": "reset-data",
                                     **({"ack_contract": self.active_ack_contract}
                                        if getattr(self, "active_ack_contract", "synced") != "synced" else {})},
                                     timeout=self.cleanup_timeout(deadline), export=True)
                dump(deployment / f"host-{index}-reset.json", reset)
                if (reset.get("status") != "reset" or reset.get("data_empty") is not True
                        or reset.get("initialization_removed") is not True):
                    raise InfrastructureError("reset helper did not confirm an empty, uninitialized owned data bind")
            except Exception as exc:
                errors.append(f"host {index} reset: {exc!r}")
        dump(deployment / "cleanup.json", {"backend": backend, "reset_skipped": False,
                                          "reset_complete": not errors, "errors": errors})
        if errors:
            raise InfrastructureError("backend fresh-data reset incomplete: " + "; ".join(errors))

    def prepare_tide(self, job, directory):
        import yaml
        case = job["case_id"]
        guest_root = str(Path(self.config["fleet"]["data_root"]) / "cells" / case)
        if (job.get("nvmeof_route") != "live" or job.get("nvmeof_target", "kernel") != "kernel"
                or job.get("target_io", "buffered") != "buffered"):
            raise SafetyError("Tidehunter owner cases require leased NVMe-oF with a buffered kernel target")
        owner_count = job.get("owner_count", len(self.config["fleet"]["nodes"]))
        polling = job.get("owner_polling", "adaptive")
        if polling not in ("adaptive", "busy"):
            raise ValueError("owner_polling must be adaptive or busy")
        storage_batches = (planned_write_batches_per_partition(job)
                           if job.get("selection") in ("nvmeof_f7_highmem32", *cloud_campaign.M3_SELECTIONS)
                           else job["keys"] + job["warmup_operations"] + job["measurement_operations"])
        owner_hosts = logical_owner_hosts(self.config, job)
        prep = {"guest_root": guest_root, "addresses": [h["address"] + ":" + str(9400 + (i if job.get("owner_topology") else 0)) for i, h in enumerate(owner_hosts)],
                "transport": owner_control_settings(job)["owner_transport"],
                "max_batches_per_partition": max(16384, storage_batches),
                "max_admitted_batches": 1_000_000, "max_index_bytes": 512 * 1024**2,
                "coordinator_cache_bytes": job["coordinator_cache_bytes"],
                "owner_wal_member_read": "async",
                "polling": polling, "owner_wal_geometry": job.get("owner_wal_geometry", "research")}
        if "owner_wal_file_bytes" in job:
            prep["owner_wal_file_bytes"] = job["owner_wal_file_bytes"]
        tier = cloud_campaign.durability_tier(job)
        prep["durability_tier"] = tier
        if job.get("case_kind") == "mainnet_replay":
            prep.update(replay_node_db_id=0,
                        database=list(hashlib.blake2b(case.encode(), digest_size=16).digest()),
                        batch_limits={"max_operations": 519, "max_encoded_bytes": 500000})
        receipts, nonces = {}, {}
        executable, binary_seal = resolve_case_binary(self.config, job)
        if job.get("nvmeof_route") == "live":
            live_transport = {"tidehunter_nvme_tcp": "tcp", "tidehunter_nvme_rdma": "rdma"}.get(job["backend"])
            prep["live_nvmeof"] = live_nvme_preparation(self.config, case, live_transport)
            prep["live_nvmeof"].update(inline_reads=job.get("live_inline_reads", True),
                                       lease_ahead=job.get("live_lease_ahead", True),
                                       polled_writes=job.get("live_polled_writes", False),
                                       leased_write_window=job.get("live_leased_write_window"),
                                       queue_geometry={"io_queues": job.get("live_io_queues", 4),
                                                       "queue_size": job.get("live_io_queue_size", 128),
                                                       "poll_queues": job.get("live_poll_queues", 0)})
            for key in ("leased_sync", "leased_flush_min_frame_bytes", "leased_write_lanes"):
                if "live_" + key in job:
                    prep["live_nvmeof"][key] = job["live_" + key]
            if binary_seal is not None:
                owner_memory = (owner_process_budget(self.config, job, 0)[1] if job.get("owner_topology")
                                else self.config["fleet"]["server_memory_mib"])
                allowance = job.get("owner_read_memory_allowance_bytes",
                    min(512 * 1024**2, int(owner_memory * 1024**2 // 4)))
                if allowance + 64 * 1024**2 > owner_memory * 1024**2:
                    raise SafetyError("owner read allowance exceeds its co-located process share")
                prep.update(owner_read_resident_first=job.get("owner_read_resident_first", False),
                            owner_read_memory_allowance_bytes=allowance)
                prep["history_persistence"] = "wal_replay"
                prep["live_nvmeof"].update(write_mode="leased", export_mode="per_owner")
        if "live_pin_read_lanes" in job:
            prep["pin_read_lanes"] = job["live_pin_read_lanes"]
        if "read_lanes" in job:
            prep["read_lanes"] = job["read_lanes"]
        if "coordinator_read_memory_allowance_bytes" in job:
            memory = job["coordinator_read_memory_allowance_bytes"]
            if (job.get("selection") in cloud_campaign.NVMEOF_PATH_SELECTIONS
                    and (type(memory) is not int or not 512 * 1024**2 <= memory <= 16 * 1024**3)):
                raise SafetyError("coordinator_read_memory_allowance_bytes must be an integer in 512 MiB..16 GiB")
            prep["coordinator_read_memory_allowance_bytes"] = memory
        if platform_module(self.config):
            prep = platform_module(self.config).prepare_transport(self.config, prep)
        dump(directory / "prepare-input.json", prep)
        prepared = directory / "prepared"
        command([str(executable), "--prepare-external-case", directory / "prepare-input.json", "--output", prepared],
                timeout=min(60, self.remaining()), env=environment(self.config))
        geometry = {"native_default": (134217728, 10737418240), "research": (4194304, 1073741824),
                    "sui_harness": (16777216, 268435456)}.get(prep["owner_wal_geometry"])
        if geometry is not None and "owner_wal_file_bytes" in prep:
            size = prep["owner_wal_file_bytes"]
            if type(size) is not int or size < 2 * geometry[0] or size > geometry[1] or size % geometry[0] or size % 4096:
                raise SafetyError("invalid owner WAL file size override")
            geometry = (geometry[0], size)
        for index in range(owner_count):
            launch = yaml.safe_load((prepared / f"node-{index}" / "launch.yaml").read_text())
            partition = (launch or {}).get("node", {}).get("partition", {})
            if geometry is None or (partition.get("fragment_bytes"), partition.get("wal_file_bytes")) != geometry:
                raise SafetyError("prepared owner WAL geometry differs from the planned case")
        self.active_tide = (job, directory, [])
        remote = json.loads((prepared / "remote.json").read_text())
        if (remote.get("storage", {}).get("fragment_bytes"), remote.get("storage", {}).get("wal_file_bytes")) != geometry:
            raise SafetyError("prepared coordinator WAL geometry differs from the planned case")
        if job.get("nvmeof_route") == "live":
            host_nqn = admit_prepared_live_client(remote.get("live_nvme"), prep["live_nvmeof"], tier)
            owner_identities = remote["live_nvme"].get("owner_identities", {})
        elif remote.get("live_nvme") is not None:
            raise SafetyError("unexpected live client in non-live campaign case")
        if "read_lanes" in job:
            remote["read_lanes"] = job["read_lanes"]
        if "live_pin_read_lanes" in job:
            if remote.get("pin_read_lanes") != job["live_pin_read_lanes"]:
                raise SafetyError("prepared coordinator read pinning differs from planned case")
        # The typed remote config references guest_root on the coordinator too.
        # Stage just its client identity there without rebasing engine serde.
        for field in ("ca", "cert", "key"):
            if not remote["tls"].get(field):
                continue
            target = guarded(remote["tls"][field], Path(guest_root))
            source = guarded(prepared / "tls" / target.name, prepared, must_exist=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            with source.open("rb") as incoming, target.open("xb") as outgoing:
                shutil.copyfileobj(incoming, outgoing)
            target.chmod(0o600)
        def start(index):
            launch_path = prepared / f"node-{index}" / "launch.yaml"
            launch = load_owner_launch(job, launch_path)
            validate_launch(self.config, launch, case, index, polling)
            if job.get("nvmeof_route") == "live":
                selected = launch.get("node", {}).get("live_target") or {}
                if selected != live_target_for_owner(
                        self.config, case, index, live_transport,
                        owner_identities.get(str(index), {}).get("host_nqn", host_nqn), job.get("nvmeof_target", "kernel"),
                        "per_owner"):
                    raise SafetyError("prepared live target is missing or differs from selected transport")
            elif launch.get("node", {}).get("live_target") is not None:
                raise SafetyError("unexpected live target in non-live campaign case")
            launch["launch_nonce"] = nonces[index] = secrets.token_hex(16)
            launch_path.write_text(yaml.safe_dump(launch, sort_keys=False))
            files = {"launch.yaml": launch_path}
            # Typed helper already embeds final guest paths. Only these three TLS
            # files may cross the machine boundary; never its CA signing key.
            tls = launch["tls"]
            for field in ("ca", "cert", "key"):
                if not tls.get(field):
                    continue
                target = Path(tls[field])
                relative = target.relative_to(Path(guest_root) / f"node-{index}")
                source = guarded(prepared / f"node-{index}" / relative, prepared, must_exist=True)
                files[str(relative)] = source
            archive = directory / f"storage-{index}.tar.gz"
            with tarfile.open(archive, "w:gz") as bundle:
                for name, source in files.items():
                    bundle.add(source, arcname=name, recursive=False)
            key = f"{self.config['prefix']}/deploy/cells/{case}/storage-{index}.tar.gz"
            upload_object(self.clients, self.config, key, archive)
            # Mark before send; even an unknown response must be cleaned up.
            with self.sequence_lock:
                self.active_tide[2].append(index)
            service_seconds = tide_owner_service_seconds(self.config["spec"], job, owner_count, self.remaining())
            proof = self.request(index, {"action": "tide_start", "case_id": case, "key": key,
                "sha256": digest(archive), "files": list(files), "timeout": service_seconds,
                "owner_history_diagnostics": requests_archived_owner_history(job),
                **({"case_binary": binary_seal} if binary_seal is not None else {}),
                "owner_trace": job.get("owner_trace", False)}, case=case, timeout=120)
            dump(directory / "server-proofs" / f"start-{index}.json", proof)
            if proof.get("case_binary") != binary_seal:
                raise SafetyError("owner reported a different executable seal")
            receipts[index] = confirm_owner_durability(proof, tier, index, nonces[index])
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(start, index) for index in range(owner_count)]
            for future in futures:
                future.result()
        receipt_paths = []
        for index in range(owner_count):
            path = (directory / "owner-durability" / f"node-{index}.durability.json").absolute()
            publish_synced(path, (json.dumps(receipts[index], sort_keys=True) + "\n").encode())
            receipt_paths.append(str(path))
        options = {"path": str(directory / "database"), "evidence": str(directory / "read-evidence"), "owners": owner_count,
                "key_count": max(1048576, job["keys"]), "max_batches_per_partition": prep["max_batches_per_partition"],
                "coordinator_read_cache_bytes": job["coordinator_cache_bytes"],
                "external_remote_config": remote,
                "durability_tier": tier,
                "external_owner_durability_receipts": receipt_paths}
        if binary_seal is not None:
            options.update(owner_read_resident_first=prep["owner_read_resident_first"],
                           external_owner_read_config=json.loads(
                               (prepared / "external_owner_read_config.json").read_text()))
        if job.get("selection") in cloud_campaign.M3_SELECTIONS and job.get("case_kind") != "mainnet_replay":
            self.admit_logical_owners(job, directory, options)
        return options

    def admit_logical_owners(self, job, directory, options):
        """Admit all owners concurrently through one full-inventory authority."""
        cell = directory / "owner-admission"
        cell.mkdir(parents=True, exist_ok=False)
        selected = copy.deepcopy(options)
        selected.update(path=str(directory / "database"), evidence=str(cell / "read-evidence"),
                        coordinator_read_cache_bytes=0)
        _, binary_seal = resolve_case_binary(self.config, job)
        revision = binary_seal["revision"] if binary_seal is not None else self.config["source_revision"]
        config = cloud_campaign.make_run_config(job, cell, selected, revision,
                                                cloud_campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS)
        return self.run_logical_owner_admission(job, directory, config)

    def run_logical_owner_admission(self, job, directory, config):
        """Run a typed full-topology admission, shared with local TCP checks."""
        import importlib.util
        cell = directory / "owner-admission"
        config = cloud_campaign.make_owner_admission_config(config, job["owner_count"])
        remote = config["backend"]["options"]["external_remote_config"]
        identities = remote["live_nvme"]["owner_identities"]
        expected_nqns = {identities[str(index)]["host_nqn"] for index in range(job["owner_count"])}
        keys = 16 * job["owner_count"]
        dump(cell / "config.json", config)
        began = self.clock()
        executable, binary_seal = resolve_case_binary(self.config, job)
        command([str(executable), "--config", cell / "config.json", "--output", cell / "result"],
                timeout=min(cloud_campaign.M3_OWNER_ADMISSION_TIMEOUT_SECONDS, self.remaining()), env=environment(self.config))
        elapsed = self.clock() - began
        summary = json.loads((cell / "result/summary.json").read_text())
        try:
            cloud_campaign.admit_redrain_arm(job, summary.get("backend"))
        except (KeyError, TypeError, ValueError) as exc:
            raise SafetyError("owner admission did not confirm its reviewed redrain arm") from exc
        evidence = json.loads((cell / "result/measurement-read-evidence.json").read_text())
        live = (evidence.get("end") or {}).get("live_nvmeof") or {}
        routes = live.get("routes", [])
        read_nqns = {route.get("host_nqn") for route in routes if route.get("writer") is None}
        write_nqns = {route.get("host_nqn") for route in routes if route.get("writer") is not None}
        with (cell / "result/samples.csv").open() as stream:
            samples = list(csv.DictReader(stream))
        preloads = [sample for sample in samples if sample.get("phase") == "preload"]
        reads = [sample for sample in samples if sample.get("phase") == "measurement"]
        if (summary.get("status") != "completed" or len(samples) != 2 * keys
                or len(preloads) != keys or len(reads) != keys
                or any(sample.get("operation") != "put" for sample in preloads)
                or any(sample.get("operation") != "read" for sample in reads)
                or any(sample.get("outcome") not in ("ok", "success") for sample in samples)
                or live.get("completed_read_ops", 0) < keys or live.get("completed_write_ops", 0) <= 0
                or live.get("errors") != 0
                or (binary_seal is not None and live.get("payload_rpc_requests") != 0)
                or live.get("route_evidence_dropped", 0) != 0
                or read_nqns != expected_nqns or write_nqns != expected_nqns):
            raise SafetyError("full-inventory logical owner write/read admission failed")
        # The blocking command has reaped the only admission coordinator. Its
        # complete verified route inventory must also be drained before another
        # coordinator can open the same owner authority.
        script = Path(self.config["repository_root"]) / "experiments/nvmeof/manage.py"
        spec = importlib.util.spec_from_file_location("nvmeof_admission_manage", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        controllers = module.controller_snapshot()
        remaining = [name for name, identity in controllers.items() if any(
            module.controller_matches(identity, route["namespace_identity"]["nqn"],
                                      route["transport"], route["address"], route["service"])
            for route in routes)]
        if remaining:
            raise SafetyError("admission coordinator left live controllers: " + ", ".join(remaining))
        # Owners retain the granted epochs. The measured coordinator must reopen
        # this authority, not create a new manifest with no knowledge of them.
        proofs = [{"owner_index": index, "host_index": owner_host_index(self.config, job, index),
                   "host_nqn": identities[str(index)]["host_nqn"], "endpoint": remote["endpoints"][index],
                   "config": str((cell / "config.json").relative_to(directory)),
                   "summary_sha256": digest(cell / "result/summary.json")}
                  for index in range(job["owner_count"])]
        dump(directory / "owner-admission.json", {"owners": proofs, "elapsed_seconds": elapsed,
             "preload_puts": keys, "verified_reads": keys, "concurrency": job["owner_count"],
             "coordinator_exited": True, "coordinator_database_retained": (directory / "database").is_dir(),
             "authority_database": "database",
             "remaining_controllers": remaining,
             "scope": "one full-inventory authority; parallel verified cache-disabled reads of every preloaded key; every owner has read and write routes; crash qualification is per physical VM"})

    def stop_tide(self):
        if self.active_nvme is not None:
            raise InfrastructureError("NVMe ownership remains live or quarantined; owner removal forbidden")
        if self.tide_cleanup_forbidden:
            raise InfrastructureError("client quiescence unconfirmed; shared owner data retained")
        if not self.active_tide:
            return
        job, directory, indices = self.active_tide
        history_diagnostics = requests_archived_owner_history(job)
        if self.shared_tide is not None and self.shared_tide.get("ready") is not None:
            cleanup = self.owner_batch("nvme_owner_cleanup_export", job["case_id"], timeout=120,
                                       export=True, no_targets_published=not self.shared_tide.get("target_attempted", False))
            dump(directory / "owner-export-cleanup.json", cleanup)
            self.shared_tide["ready"] = None
        errors = []
        deadline = self.cleanup_deadline()
        def stop(index):
            result = self.request(index, {"action": "tide_stop", "case_id": job["case_id"],
                                          "initiator_drained": True,
                                          "owner_history_diagnostics": history_diagnostics,
                                          "owner_trace": job.get("owner_trace", False)},
                                  case=job["case_id"], timeout=self.cleanup_timeout(deadline), export=True)
            dump(directory / "server-proofs" / f"stop-{index}.json", result)
            if job.get("owner_trace"):
                dump(directory / "owner-events" / f"node-{index}.json", result["owner_events"])
            if history_diagnostics:
                dump(directory / "owner-durability" / f"node-{index}.history-maintenance.json", result["history_maintenance"])

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(stop, index) for index in indices]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    errors.append(repr(exc))
        if errors:
            self.tide_cleanup_forbidden = True
            raise InfrastructureError("Tidehunter exact unit cleanup incomplete: " + "; ".join(errors))
        if job.get("owner_topology") == "colocated":
            cleaned = self.request(0, {"action": "tide_slice_cleanup", "case_id": job["case_id"]},
                case=job["case_id"], timeout=self.cleanup_timeout(deadline), export=True)
            dump(directory / "server-proofs" / "slice-stop.json", cleaned)
        if job.get("selection") in cloud_campaign.M3_SELECTIONS:
            # The authority is still needed while any owner can report its
            # epochs. Only confirmed fleet-wide stop permits its removal.
            database = guarded(directory / "database", self.root)
            if database.exists():
                shutil.rmtree(database)
        if self.shared_tide is not None:
            database = guarded(directory / "database", self.root)
            if database.exists():
                shutil.rmtree(database)
            control = Path("/var/lib/tidehunter-kv-control") / self.config["prefix"]
            for mirror in self.shared_tide.get("mirror_roots", []):
                shutil.rmtree(guarded(mirror, control, must_exist=True))
        self.active_tide = None

    def owner_batch(self, action, case, *, timeout=120, export=False, **fields):
        timeout = max(1, min(timeout, int(self.remaining(export=export))))
        def execute(index):
            request = {"action": action, "case_id": case, "timeout": timeout, **fields}
            if action == "nvme_owner_target_setup":
                host = self.config["fleet"]["nodes"][index]
                field = "rdma_address" if platform_module(self.config) and fields.get("transport") == "rdma" else "address"
                request["address"] = host[field]
                if self.local_mode is not None:
                    request["base_port_id"] += index * 1000
                    request["base_service"] += index * 1000
            result = self.request(index, request, case=case, timeout=timeout, export=export)
            if result.get("complete") is not True:
                raise InfrastructureError("owner lifecycle failed: " + json.dumps(result))
            return result
        with ThreadPoolExecutor(max_workers=5) as pool:
            return list(pool.map(execute, sorted(self.active_tide[2]) if self.active_tide else range(len(self.config["fleet"]["nodes"]))))

    def refresh_owner_receipts(self, options, rows, tier):
        """After an owner restart, replace each receipt copy with the new process's own receipt.

        A fresh receipt names the same owner and launch but a later start by a different process
        incarnation; a copy left from the earlier process can never confirm the restarted owner.
        """
        paths = options["external_owner_durability_receipts"]
        if len(rows) != len(paths):
            raise SafetyError("restart returned a different number of owners than receipts")
        fresh = []
        for index, (path, row) in enumerate(zip(paths, rows)):
            previous = json.loads(Path(path).read_text())
            receipt = row.get("durability_receipt")
            if (not cloud_campaign.owner_receipt_is_fresh(previous, receipt)
                    or receipt["durability"]["tier"] != tier or receipt["partition"] != index):
                raise SafetyError(f"owner {index} did not publish a fresh receipt after restart")
            fresh.append((path, receipt))
        for path, receipt in fresh:
            publish_synced(path, (json.dumps(receipt, sort_keys=True) + "\n").encode())

    def stage_nvme_manifests(self, case, exports):
        control = guarded(Path("/var/lib/tidehunter-kv-control") / self.config["prefix"],
                          Path("/var/lib/tidehunter-kv-control"))
        control.mkdir(mode=0o700, parents=True, exist_ok=True)
        mirrors = []
        for index, owner in enumerate(exports):
            source = Path(self.config["fleet"]["data_root"]) / "cells" / case / f"node-{index}" / "nvmeof/export"
            if Path(owner["work_dir"]) / "export" != source:
                raise SafetyError("manifest source differs from exact owner export")
            mirror = control / ("source-" + hashlib.sha256(case.encode()).hexdigest()[:16] + "-" + str(index))
            mirror.mkdir(mode=0o700)
            mirrors.append(str(mirror))
            for entry in owner["ready"]["exports"]:
                original = Path(entry["manifest_path"])
                if not original.is_absolute() or ".." in original.parts or not original.is_relative_to(source) or original.name != "manifest.json":
                    raise SafetyError("manifest path escapes owner export")
                ident = entry["export_uuid"]
                if not isinstance(ident, list) or len(ident) != 16 or any(type(b) is not int or not 0 <= b <= 255 for b in ident):
                    raise SafetyError("invalid export UUID")
                key = f"{self.config['prefix']}/results/cells/{case}/storage/{index}/nvme-manifests/{bytes(ident).hex()}.json"
                checksum, size = entry.get("manifest_sha256"), entry.get("manifest_bytes")
                if (entry.get("manifest_key") != key or not isinstance(checksum, str)
                        or not re.fullmatch(r"[a-f0-9]{64}", checksum)
                        or type(size) is not int or not 0 < size <= 64 * 1024**2):
                    raise SafetyError("manifest S3 identity or size differs from admission")
                head = object_head(self.clients, self.config, key)
                if head["bytes"] != size or head["sha256"] != checksum:
                    raise InfrastructureError("source manifest HEAD differs from owner receipt")
                destination = mirror / original.relative_to(source)
                guarded(destination, mirror)
                destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                download_object(self.clients, self.config, key, destination)
                destination.chmod(0o600)
                if destination.stat().st_size != size or digest(destination) != checksum:
                    raise SafetyError("downloaded source manifest differs from owner receipt")
        return mirrors

    def prepared_tide_options(self, job, directory, spans=None):
        def shared_identity(candidate):
            source = candidate["sealed_dataset"]["source_id"]
            focused_hybrid = (candidate.get("case_kind") is not None
                              and candidate["backend"] in cloud_campaign.NVME_BACKENDS
                              and candidate.get("nvmeof_route") == "sealed_read")
            transport = ("tidehunter_verbs_native" if focused_hybrid else
                         "tcp" if candidate["backend"] == "tidehunter_tcp"
                         or candidate["backend"] in cloud_campaign.NVME_BACKENDS
                         else candidate["backend"])
            return (source, transport, cloud_campaign.durability_tier(candidate),
                    candidate.get("owner_polling", "adaptive"))

        identity = shared_identity(job)
        transport, polling = identity[1], identity[3]
        if polling not in ("adaptive", "busy"):
            raise ValueError("owner_polling must be adaptive or busy")
        if self.shared_tide is not None and self.shared_tide["identity"] != identity:
            if self.active_nvme is not None:
                raise SafetyError("cannot replace a dataset with outstanding NVMe namespaces")
            self.stop_tide()
            self.shared_tide = None
        if self.shared_tide is None:
            name = "-".join(identity[:3])
            if job.get("selection") == "fabric_ab_compact":
                name += "-" + polling + "-" + job["case_id"]
            block = guarded(self.root / "datasets" / name, self.root)
            block.mkdir(parents=True, exist_ok=False)
            group = [job] + [
                candidate for candidate in controller_matrix(self.config["spec"])
                if "sealed_dataset" in candidate
                and shared_identity(candidate) == identity
                and candidate["case_id"] != job["case_id"]
            ]
            # Match RemoteDatabaseConfig::MAX_AUTO_READ_LANES. Missing/None
            # stays automatic on each reader, but owners reserve for its cap.
            read_lanes = max(64 if candidate.get("read_lanes") is None
                             else candidate["read_lanes"] for candidate in group)
            dump(block / "preparation-sizing.json", {
                "read_lanes": read_lanes,
                "job_ids": [candidate["case_id"] for candidate in group],
            })
            preparation_job = dict(job, read_lanes=read_lanes)
            options = record_phase_span(
                spans, "backend_deploy", self.prepare_tide, preparation_job, block)
            prep_config = cloud_campaign.make_run_config(job, block, options, self.config["source_revision"],
                                                        min(600, int(self.remaining())))
            if job.get("case_kind") == "hybrid_put":
                prep_config["dataset"]["value_size"] = 1024
                prep_config["phases"]["preload"] = True
                prep_config["phases"]["verify_existing_dataset"] = False
            prep_config["key_namespace_hex"] = hashlib.blake2b(
                f"{self.config['prefix']}:{job['repeat']}".encode(), digest_size=16).hexdigest()
            dump(block / "dataset-config.json", prep_config)
            record_phase_span(spans, "dataset_preload", command,
                [self.config["binary"], "prepare-nvmeof-dataset", block / "dataset-config.json",
                 block / "dataset-prepared.json"], timeout=min(600, self.remaining()), env=environment(self.config))
            prepared = json.loads((block / "dataset-prepared.json").read_text())
            if (prepared.get("schema_version") != 1 or prepared.get("keys") != job["keys"]
                    or prepared.get("value_bytes") != prep_config["dataset"]["value_size"]
                    or prepared.get("seed") != prep_config["seed"]
                    or prepared.get("preload_recipe") != "kv-research-native-sealed-batch-v2"
                    or type(prepared.get("preload_batch_max_members")) is not int
                    or not 1 <= prepared["preload_batch_max_members"] <= 32
                    or prepared.get("namespace") != list(bytes.fromhex(prep_config["key_namespace_hex"]))):
                raise SafetyError("sealed dataset preparation identity differs from requested source")
            ready, mirrors = None, []
            if transport == "tcp" or job.get("case_kind") is not None:
                case = job["case_id"]
                # Preserve the authoritative WAL if any owner transition is uncertain.
                self.tide_cleanup_forbidden = True
                stopped = self.owner_batch("nvme_owner_stop", case, timeout=120)
                dump(block / "owner-stopped.json", stopped)
                exports = record_phase_span(spans, "export_copy", self.owner_batch,
                                            "nvme_owner_export", case, timeout=600, prepared=prepared)
                dump(block / "owner-exports.json", exports)
                ready = [row["ready"] for row in exports]
                if (len(ready) != len(prepared["partitions"])
                        or {row.get("partition") for row in ready} != set(prepared["partitions"])
                        or sum(row.get("members", -1) for row in ready) != job["keys"]
                        or any(any(row.get(key) != prepared[key] for key in
                                   ("namespace", "seed", "keys", "value_bytes", "entropy", "source_epoch",
                                    "preload_recipe", "preload_batch_max_members"))
                               or not isinstance(row.get("exports"), list)
                               or sum(item.get("members", -1) for item in row["exports"]) != row.get("members")
                               for row in ready)):
                    raise SafetyError("owner exports do not cover the exact sealed dataset")
                mirrors = self.stage_nvme_manifests(case, exports)
                restarted = self.owner_batch("nvme_owner_restart", case, timeout=120,
                                             runtime_seconds=int(self.remaining()))
                dump(block / "owner-restarted.json", restarted)
                self.refresh_owner_receipts(options, restarted, cloud_campaign.durability_tier(job))
                self.tide_cleanup_forbidden = False
            self.shared_tide = {"identity": identity, "job": job, "block": block,
                                "options": options, "prepared": prepared, "ready": ready, "mirror_roots": mirrors}
        options = copy.deepcopy(self.shared_tide["options"])
        options["evidence"] = str(directory / "read-evidence")
        if "read_lanes" in job:
            options["external_remote_config"]["read_lanes"] = job["read_lanes"]
        else:
            options["external_remote_config"].pop("read_lanes", None)
        if "live_pin_read_lanes" in job:
            options["external_remote_config"]["pin_read_lanes"] = job["live_pin_read_lanes"]
        else:
            options["external_remote_config"].pop("pin_read_lanes", None)
        return options

    def connect_nvme(self, job, directory):
        require_supported_nvme_route(job)
        if job.get("nvmeof_route") == "live":
            raise SafetyError("live NVMe route cannot use sealed namespace connection")
        shared = self.shared_tide
        if not shared or self.active_nvme is not None:
            raise SafetyError("NVMe connection requires one prepared, idle dataset")
        transport = {"tidehunter_nvme_tcp": "tcp", "tidehunter_nvme_rdma": "rdma"}[job["backend"]]
        generation = hashlib.sha256(job["case_id"].encode()).hexdigest()[:16]
        case = shared["job"]["case_id"]
        active = {"generation": generation, "case": case, "case_directory": str(directory),
                  "transport": transport, "target_implementation": (
                      "kernel_direct" if job.get("target_io") == "direct" else job.get("nvmeof_target", "kernel")),
                  "hosts": []}
        # One deadline covers target setup, five sequential host connections,
        # typed admission, the client case, cleanup and a final two-minute margin.
        target_seconds = self.config["spec"]["case_deadline_seconds"] + 120 + 5 * 120 + 120 + 300 + 120
        active["target_deadline_utc"] = datetime.fromtimestamp(min(
            timestamp(self.config["deadline_utc"]).timestamp(), utc().timestamp() + target_seconds),
            timezone.utc).isoformat()
        self.active_nvme = active
        shared["target_attempted"] = True
        targets = self.owner_batch("nvme_owner_target_setup", case, timeout=120,
                                   generation=generation, base_port_id=1000, base_service=4420,
                                   transport=transport, target_implementation=active["target_implementation"],
                                   target_deadline_utc=active["target_deadline_utc"])
        dump(directory / "nvme-targets.json", targets)
        active["targets"] = targets
        script = Path(self.config["repository_root"]) / "experiments/nvmeof/campaign_targets.py"
        control = Path("/var/lib/tidehunter-kv-control") / self.config["prefix"]
        control.mkdir(mode=0o700, parents=True, exist_ok=True)
        readers = []
        for index, (owner, ready) in enumerate(zip(targets, shared["ready"])):
            target = owner["target"]
            if target.get("transport") != transport or not target.get("exports"):
                raise SafetyError("target transport or exported namespace set differs from admission")
            expected_target = active["target_implementation"]
            expected_cache = ("direct_file_bdev_no_OS_pagecache" if expected_target == "spdk"
                              else "direct_file_no_OS_pagecache" if expected_target == "kernel_direct"
                              else "buffered_file_page_cache")
            for entry in target["exports"]:
                policy = entry.get("target_receipt", {})
                if (policy.get("selected_transport") != transport
                        or policy.get("target_cache") != expected_cache
                        or policy.get("target_cache_verified") is not True
                        or expected_target != "spdk" and (
                            type(policy.get("buffered_io")) is not int
                            or policy["buffered_io"] != (0 if expected_target == "kernel_direct" else 1))
                        or expected_target == "spdk" and (
                            policy.get("target_implementation") != "spdk"
                            or policy.get("direct_io_verified") is not True
                            or policy.get("read_only_verified") is not True)):
                    raise SafetyError("actual target implementation, open mode or selected transport is unconfirmed")
            root = Path(shared["mirror_roots"][index])
            remote_root = Path(target["export_root"])
            expected_root = Path(self.config["fleet"]["data_root"]) / "cells" / case / f"node-{index}" / "nvmeof/export"
            if remote_root != expected_root:
                raise SafetyError("target export root differs from prepared owner")
            ready_path = control / ("ready-" + generation + "-" + str(index) + ".json")
            target_path = control / ("target-" + generation + "-" + str(index) + ".json")
            dump_compact(ready_path, ready)
            dump_compact(target_path, target)
            host_state = control / ("host-" + generation + "-" + str(index))
            active["hosts"].append({"index": index, "state": str(host_state), "target": target,
                                    "ready_path": str(ready_path), "target_path": str(target_path)})
            result = json.loads(command([sys.executable, "-B", script, "host-connect",
                "--ready", ready_path, "--export-root", root, "--state", host_state,
                "--targets", target_path], timeout=min(120, self.remaining())))
            if result.get("phase") != "connected" or result.get("transport") != transport:
                raise InfrastructureError("namespace connection/protection incomplete")
            dump(directory / f"nvme-host-{index}.json", result)
            active["hosts"][-1]["controllers"] = []
            for item in result["exports"]:
                controller = item["host_receipt"]["controller"]
                if not re.fullmatch(r"nvme[0-9]+", controller):
                    raise SafetyError("invalid protected controller identity")
                identify = json.loads(command(["nvme", "id-ctrl", "/dev/" + controller, "-o", "json"], timeout=20))
                raw_id = identify.get("cntlid")
                ctrl_id = int(raw_id, 0) if isinstance(raw_id, str) else raw_id
                if type(ctrl_id) is not int or not 1 <= ctrl_id <= 65535:
                    raise SafetyError("controller Identify did not provide a usable target controller ID")
                active["hosts"][-1]["controllers"].append({"export_uuid": item["export_uuid"], "ctrl_id": ctrl_id})
            bindings = {tuple(item["export_uuid"]): item for item in result["bindings"]}
            for entry in target["exports"]:
                binding = bindings.pop(tuple(entry["export_uuid"]), None)
                if binding is None:
                    raise SafetyError("namespace binding missing for protected export")
                if binding.get("transport") != transport:
                    raise SafetyError("namespace binding transport differs from selected transport")
                target_policy = entry.get("target", "kernel")
                if expected_target != "spdk" and target_policy != expected_target:
                    raise SafetyError("namespace target policy differs from requested file I/O")
                if binding.get("target", "kernel") != target_policy:
                    raise SafetyError("namespace binding target policy differs from protected export")
                manifest_path = guarded(root / Path(entry["manifest"]).relative_to(remote_root), root, must_exist=True)
                if digest(manifest_path) != entry.get("manifest_sha256"):
                    raise SafetyError("protected target manifest differs from source bytes")
                manifest = json.loads(manifest_path.read_text())
                catalog = manifest.get("catalog")
                if not isinstance(catalog, list) or not catalog:
                    raise SafetyError("protected export has no bounded catalog")
                charged = len(catalog) * 4096 + len(manifest["nqn"].encode()) + 32 * 4096 + 128 * 8192 + 4096
                catalog_budget = ((charged + 1024**2 - 1) // 1024**2) * 1024**2
                readers.append({"binding": {"namespace_path": binding["namespace_path"],
                    "transport": transport, "target": target_policy},
                    "manifest": manifest, "max_inflight_requests": 32,
                    "max_inflight_members": 128, "max_inflight_bytes": 16 * 1024**2,
                    "max_catalog_bytes": catalog_budget})
            if bindings:
                raise SafetyError("unexpected namespace binding")
        if not readers:
            raise SafetyError("sealed dataset produced no namespace readers")
        remote = shared["options"]["external_remote_config"]
        allowance = remote["read_memory_allowance_bytes"] - remote["read_cache_bytes"] - remote["max_pending_read_bytes"]
        reserved = sum(reader["max_catalog_bytes"] + reader["max_inflight_bytes"]
                       + reader["max_inflight_members"] * 2048 + reader["max_inflight_requests"] * 1024
                       for reader in readers)
        if allowance < 0 or reserved > allowance:
            raise SafetyError("namespace catalog and inflight budgets exceed coordinator read allowance")
        dump(directory / "nvme-reader-budget.json", {"allowance_bytes": allowance,
             "reserved_bytes": reserved, "reader_count": len(readers),
             "scope": "conservative sum of catalog, reader metadata and maximum inflight bytes"})
        return readers

    def spdk_boundary(self, directory, boundary, job):
        if boundary not in ("start", "end"):
            raise SafetyError("SPDK counters require a known boundary")
        if job.get("nvmeof_route") == "live":
            def capture(index):
                try:
                    return self.request(index, {"action": "live_spdk_snapshot", "case_id": job["case_id"]},
                                        case=job["case_id"], timeout=30)
                except Exception as exc:
                    return {"case_id": job["case_id"], "index": index, "available": False,
                            "reason": type(exc).__name__ + ": " + str(exc)}
            with ThreadPoolExecutor(max_workers=5) as pool:
                rows = list(pool.map(capture, range(len(self.config["fleet"]["nodes"]))))
        else:
            active = self.active_nvme
            if not active or active.get("target_implementation") != "spdk":
                raise SafetyError("SPDK counters require a protected target")
            rows = self.owner_batch("nvme_owner_target_snapshot", active["case"], timeout=30,
                                    generation=active["generation"], target_implementation="spdk")
        path = directory / ("spdk-target-" + boundary + ".json")
        dump(path, rows)
        return {"path": path.name, "sha256": digest(path)}

    def nvme_trace(self, action, duration=None, *, export=False):
        active = self.active_nvme
        if not active or len(active["hosts"]) != len(self.config["fleet"]["nodes"]):
            raise SafetyError("target trace requires every protected host binding")
        def capture(host):
            if action == "stop" and host.get("trace_already_stopped") is not None:
                return host["trace_already_stopped"]
            request = {"action": "nvme_trace_" + action, "case_id": active["case"],
                       "generation": active["generation"], "controllers": host["controllers"],
                       "timeout": 90}
            if action == "start":
                request["duration_seconds"] = duration
            result = self.request(host["index"], request, case=active["case"], timeout=90, export=export)
            if action == "start" and result.get("complete") is not True and result.get("cleanup_complete") is True:
                host["trace_already_stopped"] = result
            return result
        with ThreadPoolExecutor(max_workers=5) as pool:
            return list(pool.map(capture, active["hosts"]))

    def disconnect_nvme(self, *, drained):
        active = self.active_nvme
        if active is None:
            return
        if not drained or len(active["hosts"]) != len(self.config["fleet"]["nodes"]):
            raise InfrastructureError("reader drain or complete namespace ownership is unconfirmed; quarantine retained")
        script = Path(self.config["repository_root"]) / "experiments/nvmeof/campaign_targets.py"
        deadline = self.cleanup_deadline()
        def disconnect(host):
            timeout = self.cleanup_timeout(deadline, 60)
            proof = json.loads(command([sys.executable, "-B", script, "host-disconnect",
                "--state", host["state"], "--readers-drained"], timeout=timeout))
            key = f"{self.config['prefix']}/results/cells/{active['case']}/storage/{host['index']}/nvme-disconnect/{active['generation']}.json"
            persist_json(self.clients, self.config, key, proof)
            timeout = self.cleanup_timeout(deadline, 60)
            result = self.request(host["index"], {"action": "nvme_owner_target_cleanup",
                "case_id": active["case"], "timeout": timeout, "generation": active["generation"],
                "readers_drained": True, "host_disconnected_key": key,
                "target_implementation": active["target_implementation"]},
                case=active["case"], timeout=timeout, export=True)
            if result.get("complete") is not True:
                raise InfrastructureError("target cleanup unconfirmed: " + json.dumps(result))
            dump(Path(active["case_directory"]) / f"nvme-cleanup-{host['index']}.json",
                 {"host_disconnect_s3": key, "target_cleanup": result, "readers_drained": True})
            control = Path("/var/lib/tidehunter-kv-control") / self.config["prefix"]
            shutil.rmtree(guarded(host["state"], control, must_exist=True))
            guarded(host["ready_path"], control, must_exist=True).unlink()
            guarded(host["target_path"], control, must_exist=True).unlink()
            return result
        errors = []
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(disconnect, host) for host in active["hosts"]]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    errors.append(exc)
        if errors:
            raise InfrastructureError("NVMe target cleanup incomplete: " + "; ".join(map(str, errors))) from errors[0]
        self.active_nvme = None

    def failure_owner_evidence(self, directory):
        """Read exact owner journals even if teardown is forbidden."""
        if not self.active_tide:
            return []
        owner_job, _, indices = self.active_tide
        owner_case = owner_job["case_id"]
        deadline = self.cleanup_deadline()
        def capture(index):
            try:
                row = self.request(index, {"action": "tide_evidence", "case_id": owner_case},
                                   case=owner_case, timeout=self.cleanup_timeout(deadline, 35), export=True)
            except Exception as exc:
                row = {"case_id": owner_case, "index": index, "available": False,
                       "reason": type(exc).__name__ + ": " + str(exc)}
            path = directory / "server-proofs" / f"failure-{index}.json"
            dump(path, row)
            return {"index": index, "path": str(path.relative_to(directory)), "sha256": digest(path),
                    "available": row.get("available", True), "owner_case": owner_case}
        with ThreadPoolExecutor(max_workers=5) as pool:
            return list(pool.map(capture, sorted(set(indices))))

    def provider_facts(self, directory):
        """Capture bounded provider facts from the exact owned sessions."""
        if not self.active_tide:
            return None
        owner_job, _, indices = self.active_tide
        owner_case = owner_job["case_id"]
        deadline = self.cleanup_deadline()
        def capture(index):
            try:
                row = self.request(index, {"action": "tide_provider_facts", "case_id": owner_case},
                                   case=owner_case, timeout=self.cleanup_timeout(deadline, 35), export=True)
                if not isinstance(row, dict) or row.get("case_id") != owner_case or row.get("index") != index:
                    raise ValueError("provider facts owner identity differs")
                return row
            except Exception as exc:
                return {"case_id": owner_case, "index": index, "available": False,
                        "reason": (type(exc).__name__ + ": " + str(exc))[:300]}
        with ThreadPoolExecutor(max_workers=5) as pool:
            rows = list(pool.map(capture, sorted(set(indices))))
        path = directory / "owner-provider-facts.json"
        dump(path, {"owners": rows})
        return {"path": path.name, "sha256": digest(path)}

    def live_boundaries(self, case, boundary, directory, expected, *, export=False):
        """Every host has an independent best-effort frame and receipt."""
        def capture(index):
            try:
                frame = self.request(index, {"action": "live_case_snapshot", "case_id": case,
                                            "boundary": boundary, "expected": expected},
                                     case=case, timeout=45, export=export)
            except Exception as exc:
                frame = {"available": False, "reason": type(exc).__name__ + ": " + str(exc)}
            path = directory / "live-observation" / f"{boundary}-{index}.json"
            dump(path, frame)
            return {"index": index, "path": str(path.relative_to(directory)), "sha256": digest(path),
                    "available": frame.get("available", True)}
        with ThreadPoolExecutor(max_workers=6) as pool:
            count = self.active_tide[0].get("owner_count", len(self.config["fleet"]["nodes"])) if self.active_tide else len(self.config["fleet"]["nodes"])
            return list(pool.map(capture, range(-1, count)))

    def live_boundary_deltas(self, directory):
        script = Path(self.config["repository_root"]) / "experiments/nvmeof/collect.py"
        spec = importlib.util.spec_from_file_location("nvmeof_case_compare", script)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        results = []
        count = self.active_tide[0].get("owner_count", len(self.config["fleet"]["nodes"])) if self.active_tide else len(self.config["fleet"]["nodes"])
        for index in range(-1, count):
            try:
                base = directory / "live-observation"
                first = json.loads((base / f"begin-{index}.json").read_text())
                last = json.loads((base / f"end-{index}.json").read_text())
                if first.get("available") is False or last.get("available") is False:
                    raise ValueError("boundary capture unavailable")
                value = module.case_compare(first, last, rdma_diagnostics)
            except Exception as exc:
                value = {"available": False, "reason": type(exc).__name__ + ": " + str(exc)}
            path = directory / "live-observation" / f"delta-{index}.json"
            dump(path, value)
            results.append({"index": index, "path": str(path.relative_to(directory)),
                            "sha256": digest(path), "available": value.get("available", False)})
        return results

    def run_fio_case(self, job):
        """Run one read-only fio job without a KV workload."""
        directory = guarded(self.root / "cases" / job["case_id"], self.root)
        directory.mkdir(parents=True, exist_ok=False)
        receipt = {"job": job, "status": "failed", "binary_blake2b512": self.config["binary_blake2b512"],
                   "artifacts_sha256": {}, "measurement_evidence": {}, "counter_evidence": {},
                   "raw_artifacts_validated": False}
        spans, failure = [], None
        self.select_monitoring(job, directory)
        try:
            cloud_campaign.validate_focused_job(job)
            require_supported_nvme_route(job)
            self.guard()
            if self.active_nvme is not None:
                raise SafetyError("fio requires a quiescent initiator")
            if job["fio"]["location"] == "local_file":
                self.stop_tide()
                self.shared_tide = None
                if self.active_tide is not None or self.active_backend is not None:
                    raise SafetyError("fio owner is not quiescent")
                timeout = min(600, int(self.remaining()))
                if timeout < 15:
                    raise DeadlineError("insufficient time for fio scratch")
                try:
                    result = self.request(0, {"action": "fio_scratch", "case_id": job["case_id"],
                        "job": job, "quiescent": True, "timeout": timeout}, case=job["case_id"], timeout=timeout)
                except Exception as exc:
                    raise InfrastructureError("fio scratch outcome or cleanup is uncertain: " + repr(exc)) from exc
                if "fio_failed" in result:
                    message = result["fio_failed"]
                    if not isinstance(message, str) or len(message) > 4096:
                        raise SafetyError("invalid fio failure receipt")
                    raise FioFailure(message)
                raw = base64.b64decode(result["raw_base64"], validate=True)
                argv = result["argv"]
                device = result["device_identity"]
                namespace, size, buffered = "owned-scratch", result["scratch"]["size"], None
                flags = {"read_only": result["opened_read_only"], "direct": result["actual_direct"]}
            else:
                self.prepared_tide_options(job, directory, spans)
                readers = record_phase_span(spans, "target_setup", self.connect_nvme, job, directory)
                selected = max(readers, key=lambda row: row["manifest"]["image_len"])
                manifest = selected["manifest"]
                namespace = str(uuid.UUID(bytes=bytes(manifest["export_uuid"])))
                target = selected["binding"]["namespace_path"]
                size = manifest["image_len"]
                target_entry = next(entry for owner in self.active_nvme["targets"]
                                    for entry in owner["target"]["exports"]
                                    if entry["export_uuid"] == manifest["export_uuid"])
                policy = target_entry["target_receipt"]
                if (policy.get("target_cache_verified") is not True
                        or policy.get("target_cache") != "buffered_file_page_cache"):
                    raise SafetyError("kernel target buffered_io readback is unavailable")
                buffered = 1
                binary = shutil.which("fio")
                if binary is None:
                    raise RuntimeError("fio binary unavailable")
                fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
                try:
                    identity = os.fstat(fd)
                    argv = fio_read_command(binary, target, job["fio"], size)
                    raw, flags = run_readonly_fio(argv, identity.st_dev, identity.st_ino,
                                                  min(60, self.remaining()))
                    device = f"{os.major(identity.st_rdev)}:{os.minor(identity.st_rdev)}"
                finally:
                    os.close(fd)
            if len(raw) > MAX_JSON_BYTES:
                raise SafetyError("fio output exceeds the bound")
            raw_path = directory / "fio_raw.json"
            raw_path.write_bytes(raw)
            command_path = directory / "fio_command.json"
            dump(command_path, argv)
            receipt["fio_command"] = {"path": command_path.name, "sha256": digest(command_path)}
            evidence = fio_result_from_raw(job, raw_path, device_identity=device, namespace_uuid=namespace,
                                           region_bytes=size, opened_read_only=flags["read_only"],
                                           actual_direct=flags["direct"], quiescent=True,
                                           target_buffered_io=buffered)
            result_path = directory / "fio_result.json"
            dump(result_path, evidence)
            receipt["fio_result"] = {"path": result_path.name, "sha256": digest(result_path)}
            receipt["status"] = "completed"
        except Exception as exc:
            failure = exc
            receipt["error"] = type(exc).__name__ + ": " + str(exc)
        finally:
            if self.active_nvme is not None:
                try:
                    record_phase_span(spans, "target_teardown", self.disconnect_nvme, drained=True)
                except Exception as exc:
                    failure = InfrastructureError("fio namespace cleanup failed: " + repr(exc))
                    receipt["status"] = "failed"
                    receipt["error"] = str(failure)
            phase = directory / "phase_timing.json"
            dump(phase, {"schema_version": 1, "case_id": job["case_id"], "spans": spans})
            receipt["phase_timing"] = {"path": phase.name, "sha256": digest(phase)}
            dump(directory / "receipt.json", receipt)
            self.finish_monitoring(job, directory, receipt)
            receipt["monitoring"] = {"attempted": self.monitoring_attempted,
                "events": [event for event in self.monitoring_events if event["operation"].endswith(":" + job["case_id"])]}
            dump(directory / "receipt.json", receipt)
        if isinstance(failure, (SafetyError, InfrastructureError, OSError, subprocess.TimeoutExpired)):
            raise failure
        return receipt


    def run_floor_case(self, job):
        """Run a network or SSD floor without starting a KV workload."""
        directory = guarded(self.root / "cases" / job["case_id"], self.root)
        directory.mkdir(parents=True, exist_ok=False)
        receipt = {"job": job, "status": "failed", "binary_blake2b512": self.config["binary_blake2b512"],
                   "artifacts_sha256": {}, "measurement_evidence": {}, "counter_evidence": {},
                   "raw_artifacts_validated": False}
        spans, failure = [], None
        self.select_monitoring(job, directory)
        try:
            cloud_campaign.validate_focused_job(job)
            require_supported_nvme_route(job)
            self.guard()
            if self.active_nvme is not None:
                raise SafetyError("floor requires a quiescent initiator")
            self.stop_tide()
            self.shared_tide = None
            if self.active_tide is not None or self.active_backend is not None:
                raise SafetyError("floor owner is not quiescent")
            timeout = min(600, int(self.remaining()))
            if timeout < 15:
                raise DeadlineError("insufficient time for floor case")
            request = {"case_id": job["case_id"], "job": job, "quiescent": True, "timeout": timeout}
            if job["case_kind"] == "ssd_write_floor":
                spec = job["ssd_write"]
                host_index = {"client": -1, "owner0": 0}[spec.get("host", "owner0")]
                try:
                    result = self.request(host_index, {**request, "action": "ssd_write_floor"}, case=job["case_id"], timeout=timeout)
                except Exception as exc:
                    raise InfrastructureError("SSD write floor outcome or cleanup is uncertain: " + repr(exc)) from exc
                if "fio_failed" in result:
                    message = result["fio_failed"]
                    if not isinstance(message, str) or len(message) > 4096:
                        raise SafetyError("invalid SSD write floor failure receipt")
                    raise FioFailure(message)
                if "host" in spec and (type(result.get("host_index")) is not int or result["host_index"] != host_index):
                    raise SafetyError("SSD write floor host identity differs")
                if "host" in spec:
                    receipt["host_index"] = host_index
                raw = base64.b64decode(result["raw_base64"], validate=True)
                log = base64.b64decode(result["lat_base64"], validate=True)
                evidence = floors.write_result(job, raw, log, result["device_identity"], result["actual_direct"])
                raw_path = directory / "ssd_write_raw.json"
                raw_path.write_bytes(raw)
                log_path = directory / "ssd_write_floor_lat.log"
                log_path.write_bytes(log)
                receipt["ssd_write_floor_lat_log"] = {"path": log_path.name, "sha256": digest(log_path)}
                command_path = directory / "ssd_write_command.json"
                dump(command_path, {"argv": result["argv"], "build": result["build"],
                                    "scratch_identity": result["scratch_identity"]})
                receipt["ssd_write_command"] = {"path": command_path.name, "sha256": digest(command_path)}
                evidence["raw"] = {"path": raw_path.name, "sha256": digest(raw_path)}
                evidence["lat_log"] = {**receipt["ssd_write_floor_lat_log"], "rows": evidence["logged_operations"],
                                       "columns": ["write_ns", "sync_ns", "total_ns", "offset"]}
                result_path = directory / "ssd_write_floor_result.json"
                dump(result_path, evidence)
                receipt["ssd_write_floor_result"] = {"path": result_path.name, "sha256": digest(result_path)}
            elif job["case_kind"] == "ssd_floor":
                ssd = job["ssd"]
                host_index = {"client": -1, "owner0": 0}[ssd.get("host", "owner0")]
                try:
                    result = self.request(host_index, {**request, "action": "ssd_floor"}, case=job["case_id"], timeout=timeout)
                except Exception as exc:
                    raise InfrastructureError("SSD floor outcome or cleanup is uncertain: " + repr(exc)) from exc
                if "fio_failed" in result:
                    message = result["fio_failed"]
                    if not isinstance(message, str) or len(message) > 4096:
                        raise SafetyError("invalid SSD floor failure receipt")
                    raise FioFailure(message)
                if "host" in ssd and (type(result.get("host_index")) is not int or result["host_index"] != host_index):
                    raise SafetyError("SSD floor host identity differs")
                if "host" in ssd:
                    receipt["host_index"] = host_index
                raw = base64.b64decode(result["raw_base64"], validate=True)
                log = base64.b64decode(result["lat_base64"], validate=True)
                ssd = job["ssd"]
                rows = floors.parse_fio_log(log, block_bytes=ssd["block_bytes"], region_bytes=ssd["region_bytes"])
                raw_path = directory / "fio_raw.json"
                raw_path.write_bytes(raw)
                log_path = directory / "ssd_floor_lat.log"
                log_path.write_bytes(log)
                receipt["ssd_floor_lat_log"] = {"path": log_path.name, "sha256": digest(log_path)}
                command_path = directory / "fio_command.json"
                dump(command_path, result["argv"])
                receipt["fio_command"] = {"path": command_path.name, "sha256": digest(command_path)}
                fio_job = {**job, "fio": {**ssd, "location": "local_file"}}
                evidence = fio_result_from_raw(fio_job, raw_path, device_identity=result["device_identity"],
                    namespace_uuid="owned-scratch", region_bytes=ssd["region_bytes"],
                    opened_read_only=result["opened_read_only"], actual_direct=result["actual_direct"],
                    quiescent=True, target_buffered_io=None)
                if len(rows) != evidence["read_ios"]:
                    raise FioFailure("fio latency log does not contain every read")
                evidence.update(state=ssd["state"], age_seconds=ssd["age_seconds"],
                    lat_log={**receipt["ssd_floor_lat_log"], "rows": len(rows),
                             "columns": ["time_ms", "latency_ns", "direction", "block_bytes", "offset"]})
                if "host" in ssd:
                    evidence["host_index"] = host_index
                result_path = directory / "ssd_floor_result.json"
                dump(result_path, evidence)
                receipt["ssd_floor_result"] = {"path": result_path.name, "sha256": digest(result_path)}
            else:
                server_started = False
                client = None
                servers = None
                try:
                    server_started = True
                    serve = self.request(0, {**request, "action": "net_floor_serve"}, case=job["case_id"], timeout=timeout)
                    if "net_failed" in serve:
                        server_started = False
                        raise FioFailure(serve["net_failed"])
                    if serve.get("ready") is not True:
                        raise SafetyError("network floor listener readiness not confirmed")
                    client = self.request(-1, {**request, "action": "net_floor_run"}, case=job["case_id"], timeout=timeout)
                finally:
                    if server_started:
                        try:
                            deadline = self.cleanup_deadline()
                            servers = self.request(0, {**request, "action": "net_floor_collect",
                                "timeout": self.cleanup_timeout(deadline, 30)},
                                case=job["case_id"], timeout=self.cleanup_timeout(deadline, 30), export=True)["servers"]
                        except Exception as exc:
                            raise InfrastructureError("network floor server cleanup unconfirmed: " + repr(exc)) from exc
                net = job["net"]
                if len(client["runs"]) != len(net["sizes"]) or len(servers) != len(net["sizes"]):
                    raise SafetyError("network floor run count differs")
                for row, server in zip(client["runs"], servers):
                    row["argv_server"] = server["argv"]
                    row["raw_server_output"] = server["output"]
                    if server["exit_status"] != 0:
                        row["status"] = "failed"
                        row["error"] = row["error"] or "server did not complete successfully"
                field = "rdma_address" if net["interface"] == "rdma0" else "address"
                evidence = {"schema_version": 1, "case_id": job["case_id"], **{k: net[k] for k in
                    ("tool", "mode", "interface", "provider", "endpoint")}, "client_host": 0, "server_host": 1,
                    "client_address": self.config["fleet"]["coordinator"][field],
                    "server_address": self.config["fleet"]["nodes"][0][field],
                    "device": "irdma0" if net["interface"] == "rdma0" else None,
                    "build": client["build"], "runs": client["runs"]}
                result_path = directory / "net_floor_result.json"
                dump(result_path, evidence)
                receipt["net_floor_result"] = {"path": result_path.name, "sha256": digest(result_path)}
                if any(row["status"] != "completed" for row in client["runs"]):
                    raise FioFailure("network floor measurement failed")
            receipt["status"] = "completed"
        except Exception as exc:
            failure = exc
            receipt["error"] = (type(exc).__name__ + ": " + str(exc))[:4096]
        finally:
            phase = directory / "phase_timing.json"
            dump(phase, {"schema_version": 1, "case_id": job["case_id"], "spans": spans})
            receipt["phase_timing"] = {"path": phase.name, "sha256": digest(phase)}
            dump(directory / "receipt.json", receipt)
            self.finish_monitoring(job, directory, receipt)
            receipt["monitoring"] = {"attempted": self.monitoring_attempted,
                "events": [event for event in self.monitoring_events if event["operation"].endswith(":" + job["case_id"])]}
            dump(directory / "receipt.json", receipt)
        if isinstance(failure, (SafetyError, InfrastructureError, OSError, subprocess.TimeoutExpired)):
            raise failure
        return receipt


    def run_case(self, job):
        if job.get("selection") in cloud_campaign.TOOL_SELECTIONS:
            import fleet_tools
            if self.config["spec"].get("selection") != job["selection"]:
                raise SafetyError("F9 tool selection differs from admitted fleet")
            return fleet_tools.run_case(self, job, sys.modules[__name__])
        host_count = len(self.config["fleet"]["nodes"])
        owner_count = job.get("owner_count", host_count)
        if host_count == 1:
            selection = self.config["spec"].get("selection")
            if (self.config["spec"].get("mode") != "focused"
                    or (cloud_campaign.selection_owner_hosts(selection) != 1
                        and selection not in ('nvmeof_m3_t1', 'nvmeof_m3_t2_2', 'nvmeof_m3_t2_5'))):
                raise ValueError("compact fleet requires a compact focused selection")
            expected = next((candidate for candidate in cloud_campaign.make_focused_matrix(selection)
                             if candidate["case_id"] == job.get("case_id")), None)
            if (expected is None or not cloud_campaign._focused_exact(job, expected)
                    or cloud_campaign.job_owner_hosts(job) > host_count):
                raise ValueError("case is not in the reviewed compact selection")
        if type(owner_count) is not int or owner_count not in cloud_layout.LOGICAL_OWNER_COUNTS:
            raise ValueError("unsupported logical owner_count")
        if ("owner_count" in job or owner_count == 1 and job.get("backend") in cloud_campaign.NVME_BACKENDS):
            cloud_campaign.active_owner_count(job)
        if job.get("backend") in cloud_campaign.NVME_BACKENDS and job.get("nvmeof_route") != "live":
            raise ValueError("Tidehunter NVMe-oF cases require the leased live route")
        if job.get("case_kind") in ("net_floor", "ssd_floor", "ssd_write_floor"):
            return self.run_floor_case(job)
        if job.get("case_kind") == "fio_floor":
            return self.run_fio_case(job)
        directory = guarded(self.root / "cases" / job["case_id"], self.root)
        case_binary, binary_seal = resolve_case_binary(self.config, job)
        revision = binary_seal["revision"] if binary_seal is not None else self.config["source_revision"]
        directory.mkdir(parents=True, exist_ok=False)
        case_started_utc = utc().isoformat()
        receipt = {"job": job, "status": "failed", "binary_blake2b512": self.config["binary_blake2b512"],
                   "artifacts_sha256": {}, "measurement_evidence": {}, "counter_evidence": {},
                   "raw_artifacts_validated": False}
        if binary_seal is not None:
            import cloud_package
            receipt["case_binary"] = binary_seal
            receipt["binary_blake2b512"] = cloud_package.identity(case_binary)["blake2b512"]
        if job.get("backend") in cloud_campaign.TRANSPORTS:
            receipt.update(owner_control_settings(job))
        client = unit_name(self.config, job["case_id"], "client")
        started = False
        client_quiesced = False
        completion_observed = False
        drain_proved = False
        gate_released = False
        status = {}
        focused_spans = []
        tracer_attempted = False
        tracer_stopped = False
        tracer_clean = True
        owner_ftrace_active = False
        thread_cpu_active = False
        cgroup_memory_active = False
        replay_platform = (platform_module(self.config) if self.local_mode is None
                           and job.get("case_kind") == "mainnet_replay" else None)
        native_wal_start = None
        nvme_trace_attempted = False
        nvme_trace_stopped = False
        live_observation_attempted = False
        live_trace_attempted = False
        live_case = job.get("nvmeof_route") == "live"
        live_diagnostic = live_trace_diagnostic(job)
        sealed_diagnostic = job.get("sealed_dataset", {}).get("profiled") is True
        physical_trace = self.local_mode is None
        local_trace, owner_trace = cold_trace_roles(job)
        local_trace = physical_trace and local_trace
        owner_trace = physical_trace and owner_trace
        if not physical_trace:
            receipt["physical_device_trace"] = {
                "available": False, "reason": "local physical-device tracing is not permitted on the laptop"}
        trace_case = job["case_id"]
        failure = None
        native_dataset_path = None
        native_cleanup_safe = True
        def native_trace(action, duration=None):
            if self.local_mode is not None:
                return {"available": False, "reason": "local physical-device tracing is not permitted on the laptop"}
            control_root = guarded(Path("/var/lib/tidehunter-kv-control") / self.config["prefix"],
                                   Path("/var/lib/tidehunter-kv-control"))
            control_root.mkdir(mode=0o700, parents=True, exist_ok=True)
            state = guarded(control_root / ("native-cold-" + hashlib.sha256(job["case_id"].encode()).hexdigest()[:16]), control_root)
            script = Path(self.config["repository_root"]) / "experiments/kv-comparison/cold_device_trace.py"
            argv = [sys.executable, "-B", script, action, "--state-dir", state]
            if action == "start":
                wal = Path(config["backend"]["options"]["path"]) if "sealed_dataset" in job else directory / "database"
                argv += ["--wal-path", guarded(wal, self.root, must_exist=True),
                         "--duration-seconds", str(duration)]
            try:
                return json.loads(command(argv, timeout=45, env=environment(self.config)))
            except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
                return {"available": False, "reason": type(exc).__name__ + ": " + str(exc)}
        def collect_owner_ftrace():
            nonlocal owner_ftrace_active
            traces = self.all_hosts({"action": "owner_ftrace_stop", "case_id": job["case_id"]},
                case=job["case_id"], owner_count=owner_count, timeout=60, export=True)
            store_owner_ftrace(directory, traces)
            owner_ftrace_active = False
        def collect_thread_cpu():
            nonlocal thread_cpu_active
            traces = self.all_hosts({"action": "thread_cpu_stop", "case_id": job["case_id"]},
                case=job["case_id"], owner_count=0 if job["backend"] == "native" else owner_count, timeout=60, export=True)
            store_thread_cpu(directory, traces)
            receipt["thread_cpu_evidence"] = {"path": "thread-cpu.json",
                                              "sha256": digest(directory / "thread-cpu.json")}
            thread_cpu_active = False
        def collect_cgroup_memory():
            nonlocal cgroup_memory_active
            evidence = self.all_hosts({"action": "cgroup_memory_stop", "case_id": job["case_id"]},
                case=job["case_id"], owner_count=0 if job["backend"] == "native" else owner_count,
                timeout=60, export=True)
            measurements = [row for row in evidence if not row.get("inactive")]
            artifact = {"client": measurements[0], "owners": measurements[1:]}
            path = directory / "cgroup-memory.json"
            dump(path, artifact)
            receipt["cgroup_memory_evidence"] = {"path": path.name, "sha256": digest(path)}
            cgroup_memory_active = False
            return [row for row in measurements if row.get("available") is not True]
        self.select_monitoring(job, directory)
        try:
            require_supported_nvme_route(job)
            self.guard()
            if "case_kind" in job:
                cloud_campaign.validate_focused_job(job)
            if job.get("nvmeof_route") == "live":
                if self.local_mode is not None:
                    self.local_mode.admit_live_qualification(job)
                else:
                    platform = platform_module(self.config)
                    if platform is None:
                        raise SafetyError("live NVMe pair qualification is unavailable on this platform")
                    try:
                        platform.admit_live_qualification(self.config["platform"]["runtime"])
                    except Exception as exc:
                        raise SafetyError("live NVMe pair qualification failed: " + str(exc)) from exc
            if not self.monitoring_complete:
                raise InfrastructureError("case monitoring selection failed; refusing measurement")
            backend = job["backend"]
            contract = contracts.selected(backend, job.get("ack_contract", "synced"))
            contract_options = {"ack_contract": contract} if contract != "synced" else {}
            budget = {"cpus": self.config["fleet"]["client_cpus"], "memory_mib": self.config["fleet"]["client_memory_mib"]}
            if backend in cloud_layout.BACKENDS:
                if self.local_mode is not None:
                    options = self.local_mode.backend_options(backend, **contract_options)
                    if backend == "foundationdb":
                        cfg = cloud_layout.node_config(self.local_mode.layout_fleet, backend, -1, **contract_options)
                        budget = cfg["client_budget"]
                else:
                    cfg = cloud_layout.node_config(self.config["fleet"], backend, -1, **contract_options)
                    options = cloud_layout.backend_options(self.config["fleet"], backend, cfg["directory"], **contract_options)
                    budget = cfg["client_budget"]
                    if backend == "foundationdb" and not Path(options["cluster_file"]).is_file():
                        raise InfrastructureError("generated local FoundationDB cluster file missing")
            elif backend == "native":
                options = {"path": str(directory / "database"), "key_count": max(1048576, job["keys"]), "durability": job["native_durability"]}
            else:
                if "sealed_dataset" in job:
                    options = (self.prepared_tide_options(job, directory, focused_spans)
                               if "case_kind" in job else self.prepared_tide_options(job, directory))
                else:
                    options = record_phase_span(focused_spans if "case_kind" in job else None,
                                                "backend_deploy", self.prepare_tide, job, directory)
                if job.get("nvmeof_route") == "live":
                    transport = {"tidehunter_nvme_tcp": "tcp", "tidehunter_nvme_rdma": "rdma"}[backend]
                    prep = live_nvme_preparation(self.config, job["case_id"], transport)
                    identity = options["external_remote_config"]["live_nvme"]
                    live_expected = {"addresses": prep["addresses"], "transport": transport,
                                     "service": prep["service"], "host_nqn": identity["host_nqn"],
                                     "host_id": identity["host_id"]}
                    live_expected["owners"] = {
                        str(index): {"address": address, "service": prep["service"] + 2 * index,
                                     **identity["owner_identities"][str(index)]}
                        for index, address in enumerate(prep["addresses"])}
                    dump(directory / "live-nvme-expected.json", live_expected)
                    receipt["live_nvme_expected"] = {"path": "live-nvme-expected.json",
                                                     "sha256": digest(directory / "live-nvme-expected.json")}
            owner_receipts = options.get("external_owner_durability_receipts")
            if owner_receipts is not None:
                bindings = []
                for index, source in enumerate(owner_receipts):
                    source = guarded(source, self.root, must_exist=True)
                    target = directory / "owner-durability" / f"node-{index}.durability.json"
                    if source != target:
                        publish_synced(target, source.read_bytes())
                    bindings.append({"path": str(target.relative_to(directory)), "sha256": digest(target)})
                receipt["owner_durability_receipts"] = bindings
            if contract != "synced":
                options = dict(options, ack_contract=contract)
            client_deadline = (job["measurement_sizing"]["case_deadline_seconds"]
                               if job.get("selection") in cloud_campaign.M3_SELECTIONS
                               else self.config["spec"]["case_deadline_seconds"])
            minimum_deadline = (job["interference"]["duration_seconds"] + 1
                                if job.get("case_kind") == "interference" else 1)
            seconds = self.client_runtime_seconds(client_deadline, minimum=minimum_deadline)
            config = cloud_campaign.make_run_config(job, directory, options, revision, seconds)
            apply_case_namespace(config, job, self.config["prefix"])
            if backend == "native" and "sealed_dataset" in job:
                import nvme_campaign_native
                source = job["sealed_dataset"]["source_id"]
                if source not in self.shared_native:
                    path = guarded(self.root / "datasets" / (source + "-native"), self.root)
                    prep_config = config
                    prep_timeout = min(600, self.remaining())
                    if job.get("case_kind") == "native_media_read":
                        # Preparation is outside the measured durability contract.
                        # Use bounded parallel page-cache writes and verify after reopen.
                        prep_config = copy.deepcopy(config)
                        prep_config["backend"]["options"]["durability"] = "page_cache"
                        prep_config["execution"]["concurrency"] = 16
                        required = 2 * job["keys"] * job["value_size"] + 256 * 1024**2
                        if shutil.disk_usage(self.root).free < required:
                            raise InfrastructureError("insufficient client Local SSD space for native dataset")
                        prep_timeout = min(180, self.remaining())
                        path.mkdir(parents=True, exist_ok=False)
                        native_dataset_path = path
                    try:
                        self.shared_native[source] = record_phase_span(
                            focused_spans if "case_kind" in job else None, "dataset_preload",
                            nvme_campaign_native.prepare_native, self.config["binary"], prep_config, path,
                            prep_timeout, environment=environment(self.config))
                    except nvme_campaign_native.PreparationCleanupUnconfirmed:
                        native_cleanup_safe = False
                        raise
                    except nvme_campaign_native.PreparationTimeout:
                        self.remaining()
                        raise
                if job.get("case_kind") == "native_media_read":
                    if native_dataset_path is None:
                        native_dataset_path = guarded(Path(self.shared_native[source]["database_path"]).parent, self.root, must_exist=True)
                config = bind_native_dataset_config(self.shared_native[source], config, job)
            if "sealed_dataset" in job:
                if backend == "native":
                    receipt["dataset_source"] = {"logical_id": job["sealed_dataset"]["data_id"],
                        "physical_id": source, "physical_scope": "native local dataset", "preparation_receipt":
                            str(Path(self.shared_native[source]["database_path"]).parent / "receipt.json")}
                else:
                    shared = self.shared_tide
                    receipt["dataset_source"] = {"logical_id": job["sealed_dataset"]["data_id"],
                        "physical_id": job["sealed_dataset"]["source_id"],
                        "physical_scope": "shared owner sealed WAL block",
                        "block": str(shared["block"]), "owner_transport": shared["identity"][1],
                        "read_transport": (self.active_nvme["transport"] if backend in cloud_campaign.NVME_BACKENDS
                                           else shared["identity"][1]),
                        "source_epoch": shared["prepared"]["source_epoch"],
                        "preload_recipe": shared["prepared"]["preload_recipe"],
                        "preload_batch_max_members": shared["prepared"]["preload_batch_max_members"],
                        "database": shared["prepared"]["database"]}
            config["limits"]["deadline_seconds"] = self.client_runtime_seconds(
                config["limits"]["deadline_seconds"], minimum=minimum_deadline)
            (dump_compact if backend in cloud_campaign.NVME_BACKENDS else dump)(directory / "config.json", config)
            try:
                admission_timeout = min(120 if backend in cloud_campaign.NVME_BACKENDS else 30, self.remaining())
                admitted = campaign.admit_config(case_binary, directory / "config.json", directory,
                                                 environment(self.config), timeout=admission_timeout)
                analyze.verify_normalization(config, admitted)
            except (RuntimeError, ValueError, KeyError) as exc:
                raise SafetyError("typed configuration admission failed: " + str(exc)) from exc
            if replay_platform:
                budget["memory_mib"] = replay_platform.replay_client_memory_mib(self.config["fleet"]["instance_type"])
            dump(directory / "client-budget.json", budget)
            if live_case:
                live_observation_attempted = True
                try:
                    expected_route = {"host_nqn": live_expected["host_nqn"], "transport": live_expected["transport"],
                                      "owners": live_expected["owners"]}
                    receipt["live_observation_begin"] = self.live_boundaries(job["case_id"], "begin", directory, expected_route)
                except Exception as exc:
                    receipt["live_observation_begin"] = {"available": False, "reason": repr(exc)}
            if live_diagnostic and physical_trace:
                live_trace_attempted = True
                try:
                    receipt["live_block_trace_start"] = self.all_hosts(
                        {"action": "cold_trace_start", "case_id": job["case_id"],
                         "trace_id": job["case_id"], "duration_seconds": min(3600, max(1, int(self.remaining())))},
                        case=job["case_id"], timeout=60)
                except Exception as exc:
                    receipt["live_block_trace_start"] = {"available": False, "reason": repr(exc)}
            seconds = self.client_runtime_seconds(
                config["limits"]["deadline_seconds"] + 30, minimum=minimum_deadline)
            self.guard()
            # Record possible ownership before the systemd request: a timeout
            # may have started the service despite losing the response.
            started = True
            executable = [str(case_binary), "--config", directory / "config.json", "--output", directory / "result"]
            if self.local_mode is None:
                command(systemd_argv(self.config, job["case_id"], "client", budget["cpus"], budget["memory_mib"], seconds,
                                     executable), timeout=20)
            else:
                self.local_mode.start_client(client, executable, budget, seconds)
            end = self.clock() + seconds
            ready = directory / "measurement.ready"
            gate_released = False
            while not ready.exists():
                self.remaining()
                status = unit_status(client)
                if status.get("SubState") == "exited" or status.get("ActiveState") in ("failed", "inactive"):
                    break
                if self.clock() >= end:
                    raise DeadlineError("measurement ready deadline exceeded")
                time.sleep(0.1)
            status = unit_status(client)
            if ready.exists() and status.get("SubState") != "exited" and status.get("ActiveState") not in ("failed", "inactive"):
                before = self.all_hosts({"action": "snapshot", "drop": job["drop_os_caches"] and
                    job.get("media", {}).get("history") != "prewarm",
                    "resource_job": job,
                    **({"resource_owner_case": self.shared_tide["job"]["case_id"]}
                       if "sealed_dataset" in job and backend != "native" else {}),
                    **({"monitor_job": job} if self.monitoring_attempted else {})}, case=job["case_id"], timeout=60)
                if self.monitoring_attempted:
                    for index, host in enumerate(before, start=-1):
                        self.observe(f"host_select:{index}:" + job["case_id"],
                            lambda host=host: host.get("monitoring", {"complete": False, "error": "guest omitted host selector evidence"}))
                process_before = (snapshot(self.config, client) if self.local_mode is None
                                  else self.local_mode.snapshot_client(client))
                dump(directory / "measurement-before.json", {"hosts": before, "client": process_before})
                receipt["measurement_evidence"].update({"gate_released": True,
                    "before": "measurement-before.json", "before_sha256": digest(directory / "measurement-before.json")})
                if owner_trace:
                    if "sealed_dataset" in job:
                        trace_case = self.shared_tide["job"]["case_id"]
                    tracer_attempted = True
                    try:
                        receipt["cold_device_start"] = self.all_hosts(
                            {"action": "cold_trace_start", "case_id": trace_case, "duration_seconds": min(seconds, int(self.remaining())),
                             **({"trace_id": job["case_id"]} if "sealed_dataset" in job else {})},
                            case=job["case_id"], timeout=60, owner_count=owner_count)
                    except Exception as exc:
                        receipt["cold_device_start"] = {"available": False, "reason": repr(exc)}
                if local_trace:
                    tracer_attempted = True
                    receipt["cold_device_start"] = native_trace("start", min(seconds, int(self.remaining())))
                if physical_trace and sealed_diagnostic and backend in cloud_campaign.NVME_BACKENDS:
                    nvme_trace_attempted = True
                    try:
                        receipt["nvmeof_trace_start"] = self.nvme_trace("start", min(seconds, int(self.remaining())))
                    except Exception as exc:
                        receipt["nvmeof_trace_start"] = {"available": False, "reason": repr(exc)}
                if job.get("nvmeof_target") == "spdk":
                    receipt["spdk_target_measurement"] = {"start": self.spdk_boundary(directory, "start", job),
                        "scope": "owner-local bdev counters around the drained measurement; not pure fabric or NAND latency"}
                if job.get("owner_trace"):
                    owner_ftrace_active = True
                    receipt["owner_ftrace_start"] = self.all_hosts(
                        {"action": "owner_ftrace_start", "case_id": job["case_id"]},
                        case=job["case_id"], owner_count=owner_count, timeout=60)
                if job.get("native_wal_sampling"):
                    native_wal_start = native_wal_snapshot(directory, "start")
                if job.get("thread_cpu_sampling") and self.local_mode is None:
                    thread_cpu_active = True
                    receipt["thread_cpu_start"] = self.all_hosts(
                        {"action": "thread_cpu_start", "case_id": job["case_id"]},
                        case=job["case_id"], owner_count=0 if job["backend"] == "native" else owner_count, timeout=60)
                if replay_platform:
                    cgroup_memory_active = True
                    samplers = self.all_hosts({"action": "cgroup_memory_start", "case_id": job["case_id"]},
                        case=job["case_id"], owner_count=0 if backend == "native" else owner_count, timeout=60)
                    missing = [row for row in samplers if not row.get("inactive") and "directory" not in row]
                    if missing:
                        raise InfrastructureError(f"CGROUP_MEMORY_UNAVAILABLE: {missing}")
                (directory / "measurement.release").touch(exist_ok=False)
                gate_released = True
                while True:
                    self.remaining()
                    completed = directory / "measurement.complete"
                    if completed.exists() and not completion_observed:
                        if job.get("native_wal_sampling"):
                            native_wal = native_wal_interval(native_wal_start, native_wal_snapshot(directory, "end"))
                            dump(directory / "native-wal-evidence.json", native_wal)
                            receipt["native_wal_evidence"] = {"path": "native-wal-evidence.json",
                                "sha256": digest(directory / "native-wal-evidence.json")}
                        if cgroup_memory_active:
                            missing = collect_cgroup_memory()
                            if missing:
                                reasons = [(row["role"], row.get("reason")) for row in missing]
                                raise InfrastructureError(f"CGROUP_MEMORY_INCOMPLETE: {reasons}")
                        if thread_cpu_active:
                            collect_thread_cpu()
                        if owner_ftrace_active:
                            collect_owner_ftrace()
                        ended = self.all_hosts({"action": "snapshot", "drop": False,
                            "resource_job": job, **({"resource_owner_case": self.shared_tide["job"]["case_id"]}
                            if "sealed_dataset" in job and backend != "native" else {}),
                            **({"tikv_diagnostic_since": [
                                host.get("tikv_diagnostics", {}).get("started", {}).get("utc")
                                for host in before]} if backend == "tikv" else {})}, case=job["case_id"], timeout=110)
                        dump(directory / "measurement-end.json", {"hosts": ended,
                            "client": (snapshot(self.config, client) if self.local_mode is None
                                       else self.local_mode.snapshot_client(client))})
                        if job.get("nvmeof_target") == "spdk":
                            receipt["spdk_target_measurement"]["end"] = self.spdk_boundary(directory, "end", job)
                        if owner_trace:
                            try:
                                traces = self.all_hosts(
                                    {"action": "cold_trace_stop", "case_id": trace_case,
                                     **({"trace_id": job["case_id"]} if "sealed_dataset" in job else {})}, case=job["case_id"], timeout=60, owner_count=owner_count)
                                tracer_stopped = tracer_terminal(traces)
                                # Full bounded raw traces remain in the individual guest receipts.
                                # Do not duplicate five large event streams in the case receipt.
                                receipt["cold_device_evidence"] = [
                                    {k: v for k, v in trace.items() if k not in ("raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns")}
                                    for trace in traces]
                                receipt["cold_device_raw_location"] = "guest/<case>/storage-<index>-<sequence>.json (request.action=cold_trace_stop)"
                            except Exception as exc:
                                receipt["cold_device_evidence"] = {"available": False, "reason": repr(exc)}
                        if local_trace:
                            trace = native_trace("stop")
                            tracer_stopped = tracer_terminal(trace)
                            dump(directory / "native-device-trace.json", trace)
                            receipt["cold_device_evidence"] = {k: v for k, v in trace.items()
                                if k not in ("raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns")}
                            receipt["cold_device_raw_location"] = "native-device-trace.json"
                        if nvme_trace_attempted:
                            try:
                                traces = self.nvme_trace("stop")
                                nvme_trace_stopped = all(row.get("cleanup_complete") is True for row in traces)
                                receipt["nvmeof_trace_evidence"] = traces
                                uploaded = all(row.get("upload_complete") is True for row in traces)
                                receipt["nvmeof_trace_upload_complete"] = uploaded
                                receipt["nvmeof_trace_raw_location"] = ("S3 keys in nvmeof_trace_evidence; exact owner and case generation"
                                    if uploaded and all(row.get("complete") is True for row in traces) else None)
                            except Exception as exc:
                                receipt["nvmeof_trace_evidence"] = {"available": False, "reason": repr(exc)}
                        resources = resource_accounting.interval(
                            [host["resource_accounting"] for host in before],
                            [host["resource_accounting"] for host in ended])
                        dump(directory / "resource-interval.json", resources)
                        host_cpu = host_cpu_interval(before, ended)
                        dump(directory / "host-cpu-interval.json", host_cpu)
                        receipt["host_cpu_evidence"] = {"path": "host-cpu-interval.json",
                            "sha256": digest(directory / "host-cpu-interval.json"), "observed": host_cpu}
                        coverage = self.observe_resource_interval(job["case_id"], resources)
                        receipt["resource_evidence"] = {"path": "resource-interval.json",
                            "sha256": digest(directory / "resource-interval.json"), "observed": resources,
                            **coverage,
                            "scope": "host-local premeasurement through completion-gate accounting; includes driver, oracle, gate overlap and background work; available IO totals sum observed cgroup device entries, which may overlap"}
                        receipt["measurement_evidence"].update({"completion_gate_observed": True,
                            "end": "measurement-end.json", "end_sha256": digest(directory / "measurement-end.json")})
                        completion_observed = True
                        (directory / "measurement.observed").touch(exist_ok=False)
                    status = unit_status(client)
                    if status.get("SubState") == "exited" or status.get("ActiveState") in ("failed", "inactive"):
                        break
                    if self.clock() >= end:
                        raise DeadlineError("case absolute runtime exceeded")
                    time.sleep(0.2)
            dump(directory / "client-final-unit.json", status)
            after = self.all_hosts({"action": "snapshot", "drop": False}, case=job["case_id"], timeout=60)
            dump(directory / "measurement-after.json", {"hosts": after, "client":
                 (snapshot(self.config, client, completed=True) if self.local_mode is None
                  else self.local_mode.snapshot_client(client, completed=True))})
            receipt["measurement_evidence"].update({"gate_released": gate_released,
                "scope": "quiescent gate release through process exit; includes post-workload checks and drain",
                "before": "measurement-before.json" if gate_released else None, "after": "measurement-after.json",
                "before_sha256": digest(directory / "measurement-before.json") if gate_released else None,
                "after_sha256": digest(directory / "measurement-after.json"),
                "os_caches_dropped": bool(job["drop_os_caches"] and job.get("media", {}).get("history") != "prewarm"
                                          and gate_released and self.local_mode is None),
                "engine_caches_evicted": None if job.get("evict_mapped_wal_before_measurement") else False})
            if self.local_mode is not None and gate_released and job["drop_os_caches"]:
                receipt["measurement_evidence"]["scoped_file_eviction"] = [
                    host.get("scoped_file_eviction") for host in before]
            if "sealed_dataset" in job:
                receipt["measurement_evidence"]["mapped_wal_eviction"] = (
                    "completed_before_gate" if gate_released and job.get("evict_mapped_wal_before_measurement")
                    else "not_requested" if not job.get("evict_mapped_wal_before_measurement")
                    else "unconfirmed")
            receipt["measurement_evidence"].update({
                "completion_gate_observed": completion_observed,
                "end": "measurement-end.json" if completion_observed else None,
                "end_sha256": digest(directory / "measurement-end.json") if completion_observed else None})
            preliminary_core_error = None
            try:
                preliminary_manifest = json.loads((directory / "result/manifest.json").read_text())
                if preliminary_manifest.get("status") == "failed":
                    preliminary_core_error = core_workload_failure(
                        directory / "result", preliminary_manifest, status, gate_released, completion_observed)
            except (OSError, ValueError, KeyError, TypeError):
                pass
            # A failed runner can still have complete, provenance-bound raw
            # samples for its observed phases. Admit those bytes before judging
            # exit/status, but never treat malformed or missing rows as metrics.
            try:
                campaign.verify_run_identity(directory / "result", receipt, admitted,
                    self.package["fdb"] if backend == "foundationdb" else None, allow_failed=True)
                manifest = json.loads((directory / "result/manifest.json").read_text())
                summary = json.loads((directory / "result/summary.json").read_text())
                if (manifest.get("schema_version") != 1 or manifest.get("status") not in ("completed", "failed")
                        or summary.get("status") != manifest["status"]
                        or not isinstance(manifest.get("backend_info"), dict)
                        or summary.get("backend") != manifest.get("backend_info")
                        or manifest.get("raw_samples_complete") is not True):
                    raise ValueError("terminal manifest/summary schema, backend, status or raw writer completeness differs")
                analyze.admit_ack_contract(backend, summary["backend"], job.get("ack_contract", "synced"))
                if manifest["status"] == "completed":
                    cloud_campaign.admit_redrain_arm(job, summary["backend"])
                cloud_campaign.admit_replay_summary(admitted, summary)
                q0_ab = job.get("selection") == q0_campaign.SELECTION and job.get("arm") in q0_campaign.AB_ROLES
                phase_windows = ({} if q0_ab or job.get("measurement_sizing", {}).get("minimum_phase_seconds") else None)
                observed_counts = campaign.verify_timing_result(
                    directory / "result", admitted, allow_partial=True, phase_windows=phase_windows)
                if phase_windows is not None:
                    receipt["timing_windows_by_phase"] = phase_windows
                receipt["validated_timing_rows_by_phase"] = observed_counts
                receipt["planned_timing_rows_by_phase"] = campaign.planned_timing_rows(admitted)
                if job.get("case_kind") == "interference":
                    artifact = directory / "result" / "interference_result.json"
                    data = json.loads(artifact.read_text())
                    analyze._validate_focused_artifact("interference_result", data, job, receipt)
                    analyze.reconcile_interference_samples(analyze.read_samples(directory / "result" / "samples.csv"), data)
                    receipt["interference_result"] = {"path": "result/interference_result.json", "sha256": digest(artifact)}
                    receipt["planned_timing_rows_by_phase"]["measurement"] = observed_counts["measurement"]
                if job.get("write_wait_diagnostics", {}).get("enabled") is True:
                    artifact = directory / "result" / "write_wait_metrics.json"
                    data = json.loads(artifact.read_text())
                    analyze._validate_focused_artifact("write_wait_metrics", data, job, receipt)
                    receipt["write_wait_metrics"] = {"path": "result/write_wait_metrics.json", "sha256": digest(artifact)}
                receipt["artifacts_sha256"] = {name: digest(directory / "result" / name) for name in RAW_FILES}
                receipt["core_manifest_status"] = manifest["status"]
                receipt["core_summary_status"] = summary["status"]
                eviction = cloud_campaign.admit_wal_eviction_evidence(
                    directory, job, completed=manifest["status"] == "completed")
                if eviction is not None:
                    receipt["wal_eviction_evidence"] = eviction
                diagnostics = cloud_campaign.admit_write_diagnostics(
                    directory, job, completed=manifest["status"] == "completed")
                if diagnostics is not None:
                    receipt["write_diagnostics"] = diagnostics
                diagnostics = cloud_campaign.admit_read_diagnostics(
                    directory, job, completed=manifest["status"] == "completed")
                if diagnostics is not None:
                    receipt["read_diagnostics"] = diagnostics
                diagnostics = cloud_campaign.admit_valkey_diagnostics(
                    directory, job, config=admitted, completed=manifest["status"] == "completed")
                if diagnostics is not None:
                    receipt["valkey_diagnostics"] = diagnostics
                admit_terminal_write_timeline(directory, job, admitted, manifest, receipt)
                read_interval = cloud_campaign.admit_measurement_read_evidence(directory)
                if read_interval is not None:
                    receipt["measurement_read_evidence"] = read_interval
                spdk_io = cloud_campaign.admit_spdk_target_measurement(
                    directory, job, receipt, completed=manifest["status"] == "completed")
                if spdk_io is not None:
                    receipt["spdk_target_io"] = spdk_io
                receipt["raw_artifacts_validated"] = True
            except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError) as exc:
                if preliminary_core_error is not None:
                    receipt["raw_admission_error"] = type(exc).__name__ + ": " + str(exc)
                    raise RuntimeError(preliminary_core_error) from exc
                raise SafetyError("terminal source/config/binary/schema/raw-row admission failed: " + str(exc)) from exc
            drain_proved = (completion_observed and status.get("Result") == "success"
                            and status.get("ExecMainStatus") == "0" and manifest["status"] == "completed")
            qualification_error = None
            if job.get("case_kind") == "hybrid_put":
                samples = list(analyze.read_samples(directory / "result" / "samples.csv"))
                qualification = analyze.qualify_hybrid_put(
                    job, summary["backend"], read_interval["observed"] if read_interval else None,
                    self.active_nvme["targets"], config["backend"]["options"]["nvmeof_readers"],
                    samples, drain_proved, admitted, receipt.get("spdk_target_io"))
                receipt["hybrid_put_qualification"] = qualification
            elif backend in cloud_campaign.NVME_BACKENDS and job.get("nvmeof_route") == "live":
                qualification = cloud_campaign.qualify_live_nvme_route(
                    read_interval["observed"] if read_interval is not None else None, job, live_expected, summary["backend"])
                cloud_campaign.admit_live_write_contract(job, summary["backend"])
                receipt["live_nvme_measurement_qualification"] = qualification
                if not qualification["qualified"]:
                    qualification_error = "live NVMe measurement path was not qualified: " + "; ".join(qualification["reasons"])
            elif backend in cloud_campaign.NVME_BACKENDS:
                qualification = cloud_campaign.qualify_nvme_measurement(
                    read_interval["observed"] if read_interval is not None else None,
                    len(config["backend"]["options"]["nvmeof_readers"]),
                    self.active_nvme["transport"], self.active_nvme["targets"],
                    expected_target=("kernel_direct" if job.get("target_io") == "direct"
                                     else job.get("nvmeof_target", "kernel")))
                receipt["nvmeof_measurement_qualification"] = qualification
                if not qualification["qualified"]:
                    qualification_error = "NVMe measurement path was not qualified: " + "; ".join(qualification["reasons"])
            window_qualification = cloud_campaign.qualify_m3_phase_windows(
                job, receipt.get("timing_windows_by_phase", {}))
            if window_qualification is not None:
                receipt["measurement_window_qualification"] = window_qualification
                if not window_qualification["qualified"]:
                    qualification_error = "; ".join(filter(None, [
                        qualification_error, "read phase windows were not qualified: "
                        + "; ".join(window_qualification["reasons"])]))
            if q0_ab and manifest["status"] == "completed":
                overhead = q0_campaign.qualify_overhead(
                    read_interval, phase_windows.get("measurement"), job["durability_tier"])
                receipt["q0_overhead"] = overhead
                if overhead.get("available") is False:
                    qualification_error = "; ".join(filter(None, [
                        qualification_error, "Q0 overhead counters unavailable: " + overhead["reason"]]))
            core_error = core_workload_failure(directory / "result", manifest, status, gate_released, completion_observed)
            if core_error is not None:
                if manifest.get("run", {}).get("qualification_failure"):
                    raise InfrastructureError(core_error)
                raise RuntimeError(core_error)
            if qualification_error is not None:
                raise RuntimeError(qualification_error)
            counters_missing = False
            if backend in cloud_campaign.TRANSPORTS:
                evidence = directory / "read-evidence/read-evidence.json"
                if evidence.is_file():
                    try:
                        observed = json.loads(evidence.read_text())
                        verify_counter_evidence(job, observed, owner_count)
                        receipt["counter_evidence"] = {"path": str(evidence.relative_to(directory)), "sha256": digest(evidence),
                                                       "coordinator_scope": "cumulative_adapter_lifetime_not_measurement_interval",
                                                       **({"owner_io_scope": "latest_passive_sample_may_be_stale_not_measurement_interval"}
                                                          if "owner_io" in observed else {}),
                                                       "observed": observed}
                        if "read_tier" in job and "owner_read_configuration" in observed:
                            receipt["read_tier_effective"] = {
                                "coordinator_cache_bytes": observed["coordinator"]["cache"]["budget_bytes"],
                                "coordinator_read_memory_allowance_bytes": observed.get("effective_configuration", {}).get("coordinator", {}).get("read_memory_allowance_bytes"),
                                "owner_read_memory_allowance_bytes": observed["owner_read_configuration"][0].get("read_memory_allowance_bytes"),
                                "owner_read_resident_first": observed["owner_read_configuration"][0].get("read_resident_first"),
                                "page_cache_policy": ("scoped_file_eviction" if self.local_mode is not None
                                    and receipt["measurement_evidence"].get("scoped_file_eviction") is not None
                                    else "drop_at_measurement_gate" if receipt["measurement_evidence"].get("os_caches_dropped") is True
                                    else "not_dropped" if receipt["measurement_evidence"].get("os_caches_dropped") is False else None),
                                "warmup_operations": admitted["phases"]["warmup_operations"],
                                "warmup_key_sweep": admitted["phases"].get("warmup_key_sweep", False),
                                "measurement_key_sweep": admitted["phases"].get("measurement_key_sweep", False),
                                "read_lanes": job["read_lanes"], "concurrency": admitted["execution"]["concurrency"],
                                "sessions": None}
                    except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError) as exc:
                        raise SafetyError("actual coordinator/owner counter schema admission failed: " + str(exc)) from exc
                else:
                    counters_missing = True
            if observed_counts != receipt["planned_timing_rows_by_phase"]:
                raise SafetyError("completed core does not contain every planned raw timing row")
            if counters_missing:
                raise RuntimeError("coordinator actual cache/RPC evidence missing")
            media = None
            if (job.get("case_kind") in ("media_read", "native_media_read")
                    or (job.get("case_kind") == "live_read" and isinstance(job.get("media"), dict))):
                samples = analyze.read_samples(directory / "result" / "samples.csv")
                media = measurement_media_evidence(job, samples, config["workload"]["batch_size"],
                                                   before, ended, host_count, owner_count, drain_proved, read_interval)
            store_measurement_device_evidence(directory, receipt, job, media, before, ended, owner_count)
            if job.get("case_kind") == "block_trace":
                evidence = block_trace_evidence(job["case_id"], job["trace"]["scope"],
                                                receipt.get("cold_device_evidence"), owner_count)
                path = directory / "block_trace_evidence.json"
                dump(path, evidence)
                receipt["block_trace_evidence"] = {"path": path.name, "sha256": digest(path)}
            receipt["status"] = "completed"
        except Exception as exc:
            primary = None
            if started:
                try:
                    failed_manifest = json.loads((directory / "result/manifest.json").read_text())
                    if failed_manifest.get("status") == "failed":
                        primary = core_workload_failure(directory / "result", failed_manifest, status,
                                                        gate_released, completion_observed)
                except (OSError, ValueError, KeyError, TypeError):
                    pass
            if primary and primary not in str(exc):
                receipt["secondary_error"] = type(exc).__name__ + ": " + str(exc)
                failure = (InfrastructureError(primary + "; secondary failure: " + str(exc))
                           if failed_manifest.get("run", {}).get("qualification_failure")
                           or isinstance(exc, (SafetyError, InfrastructureError, OSError, subprocess.TimeoutExpired))
                           else RuntimeError(primary))
                receipt["error"] = type(failure).__name__ + ": " + primary
            else:
                failure = exc
                receipt["error"] = type(exc).__name__ + ": " + str(exc)
            if self.tide_cleanup_forbidden and not isinstance(failure, InfrastructureError):
                failure = InfrastructureError("owner preparation or client quiescence unconfirmed; source retained: " + repr(failure))
            if isinstance(exc, SafetyError):
                receipt["raw_artifacts_validated"] = False
        finally:
            def record_cleanup_error(phase, error):
                nonlocal failure
                message = type(error).__name__ + ": " + str(error)
                receipt.setdefault("cleanup_errors", []).append({
                    "phase": phase, "type": type(error).__name__, "message": str(error)})
                primary = receipt.get("error")
                receipt.setdefault("error", message)
                if primary is not None and primary != message:
                    failure = InfrastructureError(primary + "; cleanup " + phase + ": " + message)
                    failure.__cause__ = error
                else:
                    failure = error
            if cgroup_memory_active:
                try:
                    collect_cgroup_memory()
                except Exception as exc:
                    record_cleanup_error("cgroup_memory_stop", exc)
            if thread_cpu_active:
                try:
                    collect_thread_cpu()
                except Exception as exc:
                    record_cleanup_error("thread_cpu_stop", exc)
            if owner_ftrace_active:
                try:
                    collect_owner_ftrace()
                except Exception as exc:
                    record_cleanup_error("owner_ftrace_stop", exc)
            if failure is not None and job.get("backend") in cloud_campaign.NVME_BACKENDS:
                try:
                    frame = initiator_failure_frame(self.config, job["case_id"], case_started_utc,
                                                    utc().isoformat(), self.active_nvme)
                    path = directory / "nvme-initiator-failure.json"
                    dump(path, frame)
                    receipt["failure_initiator_evidence"] = {"path": path.name, "sha256": digest(path)}
                except Exception as exc:
                    receipt["failure_initiator_evidence"] = {"available": False, "reason": type(exc).__name__}
            if failure is not None and self.active_tide:
                try:
                    receipt["failure_owner_evidence"] = self.failure_owner_evidence(directory)
                except Exception as exc:
                    receipt["failure_owner_evidence"] = {"available": False, "reason": repr(exc)}
            if live_observation_attempted:
                try:
                    receipt["live_observation_end"] = self.live_boundaries(job["case_id"], "end", directory, expected_route, export=True)
                    receipt["live_observation_delta"] = self.live_boundary_deltas(directory)
                except Exception as exc:
                    receipt["live_observation_end_error"] = {"available": False, "reason": repr(exc)}
            if live_trace_attempted:
                try:
                    traces = self.all_hosts({"action": "cold_trace_stop", "case_id": job["case_id"],
                                             "trace_id": job["case_id"]},
                                            case=job["case_id"], timeout=60, export=True)
                    receipt["live_block_trace_cleanup"] = [
                        {k: v for k, v in row.items()
                         if k not in ("raw_events", "raw_intervals_ns", "candidate_raw_intervals_ns")}
                        for row in traces]
                    receipt["live_block_trace_raw_location"] = "guest operation receipts for cold_trace_stop"
                    receipt["live_block_trace_cleanup_complete"] = tracer_terminal(traces)
                    if not receipt["live_block_trace_cleanup_complete"]:
                        tracer_clean = False
                except Exception as exc:
                    receipt["live_block_trace_cleanup"] = {"available": False, "reason": repr(exc)}
                    tracer_clean = False

            def stop_after_failure():
                if owner_trace:
                    return self.all_hosts({"action": "cold_trace_stop", "case_id": trace_case,
                        **({"trace_id": job["case_id"]} if "sealed_dataset" in job else {})},
                                          case=job["case_id"], timeout=60, export=True, owner_count=owner_count)
                result = native_trace("stop")
                dump(directory / "native-device-trace-cleanup.json", result)
                return result
            tracer_clean = cleanup_unfinished_tracer(tracer_attempted, tracer_stopped, stop_after_failure, receipt) and tracer_clean
            if nvme_trace_attempted and not nvme_trace_stopped:
                try:
                    traces = self.nvme_trace("stop", export=True)
                    nvme_trace_stopped = all(row.get("cleanup_complete") is True for row in traces)
                    receipt["nvmeof_trace_cleanup"] = traces
                except Exception as exc:
                    receipt["nvmeof_trace_cleanup"] = {"available": False, "reason": repr(exc)}
                if not nvme_trace_stopped:
                    tracer_clean = False
            if (directory / "result/write-timeline.json").is_file() and "write_timeline" not in receipt:
                try:
                    admitted_path = directory / "normalized-config.json"
                    admitted = json.loads(admitted_path.read_text())
                    timeline = cloud_campaign.admit_write_timeline(directory, job, admitted, completed=False)
                    if timeline is not None:
                        receipt["write_timeline"] = timeline
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    record_cleanup_error("partial_write_timeline", SafetyError("partial write timeline admission failed: " + str(exc)))
            if (directory / "result/read-diagnostics.json").is_file() and "read_diagnostics" not in receipt:
                try:
                    diagnostics = cloud_campaign.admit_read_diagnostics(directory, job, completed=False)
                    if diagnostics is not None:
                        receipt["read_diagnostics"] = diagnostics
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                    receipt["partial_read_diagnostics_error"] = type(exc).__name__ + ": " + str(exc)
            if (directory / "result/valkey-timings.json").is_file() and "valkey_diagnostics" not in receipt:
                try:
                    normalized = directory / "normalized-config.json"
                    parsed = json.loads(normalized.read_text()) if normalized.is_file() else None
                    diagnostics = cloud_campaign.admit_valkey_diagnostics(directory, job, config=parsed, completed=False)
                    if diagnostics is not None:
                        receipt["valkey_diagnostics"] = diagnostics
                except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
                    receipt["partial_valkey_diagnostics_error"] = type(exc).__name__ + ": " + str(exc)
            if self.active_tide:
                try:
                    receipt["owner_provider_facts"] = self.provider_facts(directory)
                except (OSError, ValueError, RuntimeError) as exc:
                    receipt["owner_provider_facts"] = {"available": False, "reason": str(exc)[:300]}
            # Publish the terminal snapshot while exact-owned cgroups still
            # exist. The scrape barrier and CSV reduction are outside API timers.
            if "case_kind" in job:
                phase_path = directory / "phase_timing.json"
                dump(phase_path, {"schema_version": 1, "case_id": job["case_id"], "spans": focused_spans})
                receipt["phase_timing"] = {"path": phase_path.name, "sha256": digest(phase_path)}
            self.observe("case_terminal_receipt:" + job["case_id"], lambda: dump(directory / "receipt.json", receipt))
            self.finish_monitoring(job, directory, receipt)
            if started:
                try:
                    if self.local_mode is None:
                        command(["systemctl", "stop", client], timeout=35)
                    else:
                        self.local_mode.stop_client(client)
                    stopped = unit_status(client)
                    if stopped.get("LoadState") != "not-found" and (
                            stopped.get("ActiveState") not in ("inactive", "failed") or stopped.get("MainPID") != "0"):
                        raise InfrastructureError("client unit did not quiesce")
                    client_quiesced = True
                    logs = (command(["journalctl", "--no-pager", "--unit", client, "--output=short-precise"], timeout=30)
                            if self.local_mode is None else self.local_mode.client_logs(client))
                    (directory / "client.log").write_text(logs)
                except Exception as exc:
                    record_cleanup_error("client_stop", InfrastructureError("client cleanup/export failed: " + repr(exc)))
            if started and not client_quiesced and self.shared_tide is not None:
                self.tide_cleanup_forbidden = True
            if job.get("nvmeof_route") == "live" and started and not (client_quiesced and drain_proved):
                self.tide_cleanup_forbidden = True
            if native_dataset_path is not None:
                if not native_cleanup_safe or (started and not client_quiesced):
                    record_cleanup_error("native_dataset", InfrastructureError(
                        "native dataset cleanup refused because process termination is unconfirmed"))
                else:
                    try:
                        binding = cleanup_native_media_dataset(native_dataset_path, directory)
                        if binding is not None:
                            receipt["native_preparation_receipt"] = binding
                            if "dataset_source" in receipt:
                                receipt["dataset_source"]["preparation_receipt"] = binding["path"]
                        self.shared_native.pop(job["sealed_dataset"]["source_id"], None)
                        receipt["native_dataset_cleanup"] = "completed"
                    except Exception as exc:
                        record_cleanup_error("native_dataset", InfrastructureError(
                            "native dataset cleanup unconfirmed: " + repr(exc)))
            database = guarded(directory / "database", directory)
            receipt["database_files_retained"] = database.exists()
            if (job.get("selection") not in cloud_campaign.M3_SELECTIONS
                    and database.exists() and (not started or client_quiesced) and not (
                        job.get("nvmeof_route") == "live" and self.tide_cleanup_forbidden)):
                try:
                    shutil.rmtree(database)
                    receipt["database_files_retained"] = False
                except OSError as exc:
                    record_cleanup_error("coordinator_database", InfrastructureError("owned coordinator database cleanup failed: " + repr(exc)))
            try:
                if self.active_nvme is not None:
                    target_started = time.monotonic_ns()
                    target_status = "failed"
                    try:
                        require_hybrid_reader_drain(job, started, drain_proved, client_quiesced)
                        self.disconnect_nvme(drained=(not started or (drain_proved and client_quiesced)))
                        target_status = "completed"
                    finally:
                        if "case_kind" in job:
                            focused_spans.append({"operation": "target_teardown", "host_index": 0,
                                "monotonic_start_ns": target_started, "monotonic_end_ns": time.monotonic_ns(),
                                "status": target_status})
                if "sealed_dataset" not in job:
                    self.stop_tide()
                    receipt["database_files_retained"] = database.exists()
            except Exception as exc:
                record_cleanup_error("owner_storage", exc)
            if requests_archived_owner_history(job):
                try:
                    require_zero_owner_history_waits(directory, owner_count)
                except RuntimeError as exc:
                    receipt["owner_history_error"] = str(exc)
                    if failure is None:
                        failure = exc
            if not tracer_clean and not isinstance(failure, InfrastructureError):
                record_cleanup_error("tracer", InfrastructureError("exact-owned trace cleanup unconfirmed; refusing next case"))
            if (job.get("case_kind") == "block_trace" and receipt["status"] == "completed"
                    and failure is None and drain_proved and client_quiesced):
                try:
                    finalize_standalone_trace(directory, receipt)
                except (OSError, SafetyError) as exc:
                    record_cleanup_error("standalone_trace", InfrastructureError(
                        "trace standalone finalization failed: " + repr(exc)))
            receipt["artifacts_sha256"] = {name: digest(directory / "result" / name)
                for name in RAW_FILES if (directory / "result" / name).is_file()}
            if failure:
                receipt["status"] = "not_started" if isinstance(failure, CampaignTimeLimit) and not started else "failed"
                receipt.setdefault("error", type(failure).__name__ + ": " + str(failure))
            receipt["monitoring"] = {"attempted": self.monitoring_attempted,
                "host_case_scope": "selection begins at premeasurement gate; retained until next gate selection (may include cleanup/next preload)",
                "events": [event for event in self.monitoring_events
                           if event["operation"].endswith(":" + job["case_id"])]}
            if "case_kind" in job:
                phase_path = directory / "phase_timing.json"
                dump(phase_path, {"schema_version": 1, "case_id": job["case_id"], "spans": focused_spans})
                receipt["phase_timing"] = {"path": phase_path.name, "sha256": digest(phase_path)}
            dump(directory / "receipt.json", receipt)
        if isinstance(failure, (SafetyError, InfrastructureError, OSError, subprocess.TimeoutExpired)):
            raise failure
        if failure is not None and not isinstance(failure, (RuntimeError, ValueError)):
            raise InfrastructureError("unexpected infrastructure/API failure: " + repr(failure)) from failure
        return receipt

    def checkpoint_case(self, job, receipt):
        """Retain a finished or failed case independently of the final fleet archive."""
        case = job["case_id"]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", case):
            raise SafetyError("unsafe checkpoint case identity")
        directory = guarded(self.root / "cases" / case, self.root)
        if not directory.is_dir():
            return None
        staging = self.root.parent / "case-checkpoints"
        staging.mkdir(exist_ok=True)
        archive = staging / (case + ".tar.gz")
        check = lambda: self.remaining(export=True)
        records = archive_results(directory, archive, deadline_check=check)
        key = self.config["prefix"] + "/results/case-checkpoints/" + case + ".tar.gz"
        stored = upload_object(self.clients, self.config, key, archive, deadline_check=check)
        manifest = {"schema_version": 1, "case_id": case, "status": receipt["status"],
                    "source_sha256": self.config["source_sha256"], "archive": {"key": key, **stored},
                    "files": records, "observed_utc": utc().isoformat()}
        return persist_json(self.clients, self.config, key.removesuffix(".tar.gz") + ".json", manifest)

    def export(self, error):
        if not self.root_owned:
            persist_json(self.clients, self.config, self.config["prefix"] + "/results/receipt.json",
                    {"status": "incomplete", "error": error, "no_owned_result_directory": True})
            return {"complete": False}
        dump(self.root / "controller-outcome.json", {"error": error, "observed_utc": utc().isoformat()})
        if (self.root / "matrix.json").exists():
            try:
                report = cloud_campaign.report(self.root)
            except Exception as exc:
                dump(self.root / "report-error.json", {"error": repr(exc)})
                report = {"complete": False, "report_error": repr(exc)}
        else:
            report = {"complete": False}
        profiles = self.config["spec"].get("valkey_reproduction_profiles", [])
        if profiles:
            native_path = self.root / "valkey-native/report.json"
            native = json.loads(native_path.read_text()) if native_path.is_file() else {"complete": False, "reason": "native control receipt missing"}
            report["native_valkey_reproduction"] = native
            report["complete"] = report["complete"] and native.get("complete") is True
        # Each observer step is independently contained: failed snapshot/stop
        # must never skip the authoritative raw-result archive below.
        for operation, action in (("session_export", self.export_monitoring), ("session_stop", self.stop_monitoring)):
            try:
                action()
            except BaseException as exc:
                self.monitoring_complete = False
                self.monitoring_events.append({"operation": operation, "complete": False, "error": repr(exc)})
        monitoring = self.monitoring_receipt()
        try:
            dump(self.root / "observability/receipt.json", monitoring)
        except OSError as exc:
            monitoring["complete"] = False
            monitoring["receipt_error"] = repr(exc)
        report["monitoring_complete"] = monitoring["complete"]
        report["complete"] = report["complete"] and monitoring["complete"]
        if (self.root / "comparison.json").exists():
            try:
                dump(self.root / "comparison.json", report)
            except OSError as exc:
                report["comparison_monitoring_update_error"] = repr(exc)
        self.remaining(export=True)
        archive = self.root.parent / "controller-export.tar.gz"
        records = archive_results(self.root, archive, deadline_check=lambda: self.remaining(export=True))
        checksum = digest(archive)
        size = archive.stat().st_size
        key = self.config["prefix"] + "/results/controller.tar.gz"
        upload_object(self.clients, self.config, key, archive,
                      deadline_check=lambda: self.remaining(export=True))
        # File inventories grow with the matrix. Keep bulk metadata out of the
        # launcher's independently bounded, frequently polled control receipt.
        inventory = persist_json(self.clients, self.config, self.config["prefix"] + "/results/controller-inventory.json", records)
        self.persist("receipt.json", {"schema_version": 2,
            "status": "completed" if report["complete"] and not error else "incomplete",
            "error": error, "key": key, "bytes": size, "sha256": checksum, "inventory": inventory,
            "source_sha256": self.config["source_sha256"], "binary_blake2b512": self.config["binary_blake2b512"],
            "monitoring_complete": monitoring["complete"]})
        return report

    def terminate(self):
        platform = platform_module(self.config)
        if platform:
            return platform.terminate(self.config, self.clients)
        # This is intentionally independent of the workload/export deadline.
        # Never terminate a subset from a malformed or foreign response.
        current = self.guard(terminating=True)
        ids = [instance for instance in self.ids if current[instance]["State"]["Name"] != "terminated"]
        if not ids:
            return {"already_terminated": self.ids}
        response = self.clients["ec2"].terminate_instances(InstanceIds=ids)
        observed = response.get("TerminatingInstances", [])
        if (len(observed) != len(ids) or {x.get("InstanceId") for x in observed} != set(ids)
                or any(x.get("CurrentState", {}).get("Name") not in ("shutting-down", "terminated") for x in observed)):
            raise InfrastructureError("termination response unknown/incomplete; independent watchdog remains armed")
        return response

    def run(self):
        error = None
        case_finalization_error = None
        export_error = None
        report = {"complete": False}
        time_limit_reached = False
        try:
            self.bootstrap()
            jobs = controller_matrix(self.config["spec"])
            archive_limit = cloud_campaign.WRITE_ENGINE_ARCHIVE_LIMIT_BYTES.get(
                self.config["spec"].get("selection"))
            if archive_limit is not None:
                # Keep space for raw results, case checkpoints and the final archive.
                # The monitoring archive can also be copied into the final archive.
                free = shutil.disk_usage(self.root).free
                required = 3 * archive_limit + 2 * monitor_module(self.config).MAX_BYTES
                dump(self.root / "archive-staging.json",
                     {"available_bytes": free, "required_bytes": required, "path": str(self.root)})
                if free < required:
                    raise InfrastructureError(
                        f"archive staging on {self.root} needs {required} bytes; {free} available")
            dump(self.root / "matrix.json", {"schema_version": 1, "spec": self.config["spec"], "jobs": jobs})
            if platform_module(self.config):
                persist_json(self.clients, self.config, self.config["prefix"] + "/results/matrix.json",
                             {"schema_version": 1, "spec": self.config["spec"], "jobs": jobs})
            if self.config["spec"].get("valkey_reproduction_profiles"):
                import valkey_cloud
                valkey_cloud.run(self)
            if self.config["spec"].get("mode", "standard") in ("focused", "read-tier", "read-tier-focused"):
                probes = self.all_hosts({"action": "cold_trace_probe"}, timeout=120)
                if not isinstance(probes, list) or len(probes) != len(self.config["fleet"]["nodes"]) + 1:
                    raise InfrastructureError("trace capability probe returned incomplete fleet evidence")
                capability = {"schema_version": 1, "hosts": probes,
                    "identity_backend_available": all(p.get("identity_backend_available") is True for p in probes),
                    "gaps": [{"host_index": index, "gaps": proof.get("gaps", [])}
                             for index, proof in enumerate(probes, start=-1)
                             if proof.get("identity_backend_available") is not True],
                    "scope": "premeasurement attach/stop only; no device-I/O or timing claim"}
                dump(self.root / "trace-capability.json", capability)
                if any(proof.get("cleanup_complete") is not True for proof in probes):
                    raise InfrastructureError("trace capability probe teardown unconfirmed")
            active_block = None
            for position, job in enumerate(jobs):
                self.client_runtime_seconds(self.config["spec"]["case_deadline_seconds"])
                if not self.monitoring_complete:
                    raise InfrastructureError("monitoring failed; refusing the next benchmark case")
                backend = job["backend"]
                if self.shared_tide is not None and (backend == "native" or "sealed_dataset" not in job):
                    self.stop_tide()
                    self.shared_tide = None
                if job.get("cohort") == "native_write_regression" and self.shared_native:
                    for prepared in self.shared_native.values():
                        database = guarded(prepared["database_path"], self.root, must_exist=True)
                        shutil.rmtree(database)
                    self.shared_native.clear()
                desired = backend if backend in cloud_layout.BACKENDS else None
                contract = contracts.selected(backend, job.get("ack_contract", "synced"))
                block = (backend, job["repeat"], contract)
                if block != active_block:
                    self.stop_backend()
                    if desired:
                        self.start_backend(desired, **({"ack_contract": contract} if contract != "synced" else {}))
                    active_block = block
                case_failed = False
                try:
                    self.run_case(job)
                except BaseException:
                    case_failed = True
                    raise
                finally:
                    try:
                        path = self.root / "cases" / job["case_id"] / "receipt.json"
                        receipt = json.loads(path.read_text()) if path.is_file() else {"status": "failed"}
                        checkpoint = self.checkpoint_case(job, receipt) if platform_module(self.config) else None
                        self.persist("progress.json", {"observed_utc": utc().isoformat(), "case_index": position,
                            "total_cases": len(jobs), "case_id": job["case_id"], "status": receipt["status"],
                            "remaining_work_seconds": max(0, self.work_end - self.clock()), "checkpoint": checkpoint})
                    except BaseException as exc:
                        if not case_failed:
                            raise
                        case_finalization_error = type(exc).__name__ + ": " + str(exc)
        except CampaignTimeLimit:
            time_limit_reached = True
            self.persist("campaign-outcome.json", {"outcome": "campaign time limit reached"})
            if case_finalization_error:
                error = "case finalization: " + case_finalization_error
        except BaseException as exc:
            error = type(exc).__name__ + ": " + str(exc)
            if case_finalization_error:
                error += "; case finalization: " + case_finalization_error
        finally:
            for cleanup in (self.stop_tide, self.stop_backend):
                try:
                    cleanup()
                except BaseException as exc:
                    error = (error + "; " if error else "") + "cleanup: " + repr(exc)
            try:
                report = self.export(error)
            except BaseException as exc:
                export_error = repr(exc)
                try:
                    value = {"status": "export_failed", "error": error, "export_error": export_error}
                    if self.root_owned:
                        self.persist("receipt.json", value)
                    else:
                        persist_json(self.clients, self.config, self.config["prefix"] + "/results/receipt.json", value)
                except BaseException:
                    pass  # Original export failure is surfaced; termination is unconditional.
            finally:
                termination = self.terminate()
                # Local evidence may disappear when self termination takes effect.
                if self.root_owned:
                    name = "lifecycle-owner.json" if termination.get("external_lifecycle_owner") else "termination-request.json"
                    dump(self.root / name, termination)
        if error or export_error or (not report.get("complete") and not time_limit_reached):
            raise InfrastructureError(json.dumps({"controller_error": error, "export_error": export_error,
                                                  "complete": report.get("complete", False)}))
        return report


def validate_physical_proofs(proofs, *, provider="aws", expected_instance_ids=None):
    if not admitted_host_count(len(proofs)):
        raise SafetyError("two or six physical host proofs required")
    if provider not in ("aws", "gcp"):
        raise SafetyError("unknown physical storage provider")
    if expected_instance_ids is not None and (provider != "gcp" or len(expected_instance_ids) != len(proofs)):
        raise SafetyError("invalid expected physical instance identities")
    boot_ids, serials, instance_ids = set(), set(), set()
    for index, proof in enumerate(proofs):
        if proof.get("qualification_only") is not False or proof.get("independent_nvme_admitted") is not True:
            raise SafetyError("physical device proof was not admitted")
        host = proof.get("host_identity", {})
        boot = host.get("boot_id")
        devices = proof.get("block_devices", {}).get("blockdevices", [])
        selected = [x for x in devices if physical_storage.admitted_model(provider, x.get("model"))
                    and isinstance(x.get("serial"), str) and x["serial"].strip()]
        if not isinstance(boot, str) or not boot.strip() or boot in boot_ids or not selected:
            raise SafetyError("physical independence not proven by unique boot IDs and instance NVMe serials")
        if provider == "gcp":
            instance_id = host.get("instance_id")
            if (not isinstance(instance_id, str) or not re.fullmatch(r"[1-9][0-9]*", instance_id)
                    or instance_id in instance_ids or len(selected) != 1
                    or (expected_instance_ids is not None and instance_id != expected_instance_ids[index])):
                raise SafetyError("physical independence not proven by exact unique Google instance IDs")
            instance_ids.add(instance_id)
        else:
            host_serials = {x["serial"] for x in selected}
            if serials & host_serials:
                raise SafetyError("duplicate physical NVMe serial across hosts")
            serials |= host_serials
        boot_ids.add(boot)
    return True


def verify_counter_evidence(job, evidence, owner_count=None):
    coordinator = evidence.get("coordinator")
    expected_backend = cloud_campaign.diagnostic_backend(job)
    if evidence.get("schema_version") != 1 or evidence.get("backend") != expected_backend or not isinstance(coordinator, dict):
        raise SafetyError("actual coordinator counter evidence has wrong backend/schema")
    count = coordinator.get("read_rpc_count")
    if type(count) is not int or count < 0 or not isinstance(coordinator.get("cache"), dict):
        raise SafetyError("actual RPC/cache counters are missing")
    if coordinator["cache"].get("budget_bytes") != job["coordinator_cache_bytes"]:
        raise SafetyError("actual coordinator cache budget differs from declared cohort")
    if "coordinator_read_memory_allowance_bytes" in job:
        actual = evidence.get("effective_configuration", {}).get("coordinator", {}).get("read_memory_allowance_bytes")
        if actual != job["coordinator_read_memory_allowance_bytes"]:
            raise SafetyError("actual coordinator read allowance differs from declared cohort")
    # Frozen job metadata may still describe the retired owner read settings.
    # Only archived evidence carries that population.
    if "owner_read_configuration" in evidence or job.get("selection") == cloud_campaign.Q0_SELECTION:
        owners = evidence.get("owner_read_configuration")
        owner_count = job.get("owner_count", 5) if owner_count is None else owner_count
        if (not isinstance(owners, list) or len(owners) != owner_count
                or {owner.get("partition") for owner in owners} != set(range(owner_count))):
            raise SafetyError("actual owner configurations differ from declared partitions")
        if "owner_read_memory_allowance_bytes" in job and any(
                owner.get("read_memory_allowance_bytes") != job["owner_read_memory_allowance_bytes"]
                for owner in owners):
            raise SafetyError("actual owner read allowance differs from declared cohort")
        if "owner_read_resident_first" in job and any(
                owner.get("read_resident_first") != job["owner_read_resident_first"] for owner in owners):
            raise SafetyError("actual owner NOWAIT policy differs from declared cohort")
    # Hit counts and passive sample availability are observations, not admission
    # requirements. Configuring a cache must never manufacture a hit population.
    return evidence


def archive_results(root, destination, *, deadline_check=None):
    records = {}
    root = Path(root)
    guarded(destination, Path(destination).parent)
    denied = {"database", "owners", "data", "wal", "tls", "coordinator-db"}
    retained = {".json", ".jsonl", ".yaml", ".yml", ".csv", ".prom", ".log", ".txt", ".md", ".ready", ".release"}
    # Prune databases before walking them, rather than enumerating a potentially
    # enormous volume only to filter its files afterwards. File payloads stream.
    with Path(destination).open("xb") as output, tarfile.open(fileobj=output, mode="w|gz") as bundle:
        for directory, children, names in os.walk(root, topdown=True, followlinks=False):
            for child in children:
                if (Path(directory) / child).is_symlink():
                    raise SafetyError("symlink directory in raw evidence tree")
            children[:] = sorted(child for child in children if child not in denied)
            for name in sorted(names):
                if deadline_check:
                    deadline_check()
                path = Path(directory) / name
                relative = path.relative_to(root)
                if path.is_symlink() or not path.is_file():
                    raise SafetyError("nonregular raw evidence file")
                monitoring_archive = relative == Path("observability/monitoring.tar.gz")
                if not monitoring_archive and (path.suffix not in retained or path.name.endswith(".tar.gz") or "private" in path.name.lower()):
                    continue
                if path.stat().st_size > 4 * 1024**3:
                    raise SafetyError("individual raw evidence file exceeds export bound")
                records[str(relative)] = {"bytes": path.stat().st_size, "sha256": digest(path)}
                bundle.add(path, arcname=str(relative), recursive=False)
    return records


def guest_main(config, payload):
    validate_config(config)
    if (not isinstance(payload, dict)
            or set(payload) != {"execution_schema_version", "deadline_epoch", "request", "key"}
            or type(payload["execution_schema_version"]) is not int
            or payload["execution_schema_version"] != 1):
        raise SafetyError("guest requires the guarded control-execution schema")
    deadline_epoch = payload["deadline_epoch"]
    if (type(deadline_epoch) not in (int, float) or not math.isfinite(deadline_epoch)
            or deadline_epoch > timestamp(config["deadline_utc"]).timestamp()):
        raise SafetyError("guest operation deadline differs from bounded fleet expiry")
    request, key = payload["request"], payload["key"]
    index = request["index"]
    if type(index) is not int or index not in range(len(config["fleet"]["nodes"])):
        raise SafetyError("SSM guest action is restricted to storage indices")
    expected = f"{config['prefix']}/results/cells/"
    if not key.startswith(expected) or f"/storage/{index}/" not in key or ".." in key.split("/"):
        raise SafetyError("guest result key is outside exact node prefix")
    verify_local_identity(config, index)
    guarded(config["fleet"]["data_root"], Path("/mnt/tidehunter-kv-comparison"), must_exist=True)
    # Keep durable control receipts off the measured owner NVMe data roots.
    root = guarded(Path("/var/lib/tidehunter-kv-control") / config["prefix"],
                   Path("/var/lib/tidehunter-kv-control"))
    for directory in (root.parent, root):
        directory.mkdir(mode=0o700, exist_ok=True)
        descriptor = os.open(directory.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    clients = make_clients(config, guest=True)
    result = {"request": request, "instance_id": config["fleet"]["nodes"][index]["instance_id"]}
    try:
        result["result"] = execute_at_most_once(root, key, request,
            lambda: guest_operation(config, request, clients), deadline_epoch=deadline_epoch)
        result["status"] = "completed"
    except Exception as exc:
        result.update(status="failed", error=type(exc).__name__ + ": " + str(exc))
    exported = persist_json(clients, config, key, result)
    print(json.dumps(exported))
    # A successfully exported failure is a successful RPC transport. The caller
    # admits status separately; returning nonzero would hide its detailed proof.
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "validate", "run", "guest"))
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--config", type=Path)
    source.add_argument("--config-base64")
    parser.add_argument("--request-base64")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.config.read_text() if args.config else base64.b64decode(args.config_base64, validate=True))
        validate_config(config)
        if args.action == "guest":
            if not args.request_base64:
                raise SafetyError("guest requires its exact bounded operation")
            return guest_main(config, json.loads(base64.b64decode(args.request_base64, validate=True)))
        if args.action in ("plan", "validate"):
            jobs = controller_matrix(config["spec"])
            value = {"schema_version": 1, "config": config, "jobs": jobs,
                "workload_stop_utc": datetime.fromtimestamp(timestamp(config["deadline_utc"]).timestamp()
                    - config["spec"]["export_reserve_seconds"], timezone.utc).isoformat(),
                "owned_instance_ids": [h["instance_id"] for h in [config["fleet"]["coordinator"], *config["fleet"]["nodes"]]],
                "scope": "offline schema/plan only; no cloud health or package-byte claim"}
            if args.output:
                dump(args.output, value)
            print(json.dumps({"status": "valid", "planned_cases": len(jobs), "scope": value["scope"]}))
            return 0
        Controller(config, make_clients(config)).run()
        return 0
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": type(exc).__name__ + ": " + str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
