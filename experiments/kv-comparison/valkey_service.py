"""Exact-owned systemd Valkey process for a packaged matched TCP/RDMA pair."""
import hashlib
import json
import os
from pathlib import Path
import subprocess


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def command(argv, timeout=30):
    return subprocess.check_output([str(x) for x in argv], text=True, stderr=subprocess.STDOUT, timeout=timeout).strip()


def identity(runtime):
    runtime = Path(runtime).resolve(strict=True)
    metadata = json.loads((runtime / "valkey-rdma.json").read_text())
    if metadata.get("source_commit") != "fe8d406c0b5a64ec897f6455e4de684629883764":
        raise ValueError("Valkey package source commit differs")
    for name in ("valkey-server", "valkey-cli"):
        path = runtime / "bin" / name
        if not path.is_file() or path.is_symlink() or digest(path) != metadata["artifacts"]["src/" + name]["sha256"]:
            raise ValueError("Valkey package executable identity differs")
    return metadata


def status(unit):
    result = subprocess.run(["systemctl", "show", unit,
        "--property=LoadState,ActiveState,MainPID,ControlGroup,CPUQuotaPerSecUSec,MemoryMax,LimitMEMLOCK"],
        capture_output=True, text=True, timeout=10)
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if result.returncode and values.get("LoadState") != "not-found":
        raise RuntimeError("cannot observe exact owned Valkey unit: " + result.stderr)
    if not values:
        raise RuntimeError("systemd omitted Valkey unit properties")
    return values


def admit_unit(service):
    metadata = identity(service["runtime"])
    if (metadata["source_commit"] != service["source_commit"] or
            metadata["artifacts"]["src/valkey-server"]["sha256"] != service["server_sha256"]):
        raise RuntimeError("Valkey unit source or executable identity differs")
    server = str(Path(service["runtime"]) / "bin/valkey-server")
    if service["args"][0] != server:
        raise RuntimeError("Valkey unit executable path differs")
    show = command(["systemctl", "show", service["unit"], "--property=ExecStart"])
    expected = "argv[]=" + " ".join(service["args"]) + " ;"
    if expected not in show:
        raise RuntimeError("Valkey service arguments differ")


def evidence(config):
    service = config.get("native_valkey")
    if not service:
        return []
    identity(service["runtime"])
    state = status(service["unit"])
    pid = state.get("MainPID", "")
    expected_cgroup = "/system.slice/" + service["unit"] + ".service"
    if state.get("ActiveState") != "active" or not pid.isdigit() or int(pid) <= 0 or state.get("ControlGroup") != expected_cgroup:
        raise RuntimeError("Valkey unit is not the expected active service")
    proc = Path("/proc") / pid
    actual = (proc / "cmdline").read_bytes().rstrip(b"\0").split(b"\0")
    actual = [os.fsdecode(x) for x in actual]
    # Valkey replaces argv with its process title; bind source to /proc/exe and
    # command configuration to systemd's ExecStart rather than trusting title.
    exe = (proc / "exe").resolve(strict=True)
    server = (Path(service["runtime"]) / "bin/valkey-server").resolve(strict=True)
    if exe != server or digest(exe) != service["server_sha256"]:
        raise RuntimeError("Valkey process executable identity differs")
    admit_unit(service)
    cgroup = Path("/sys/fs/cgroup") / expected_cgroup.lstrip("/")
    if int((cgroup / "memory.max").read_text().strip()) != service["memory_mib"] * 1024**2:
        raise RuntimeError("Valkey MemoryMax differs")
    if (cgroup / "memory.swap.max").read_text().strip() != "0":
        raise RuntimeError("Valkey MemorySwapMax differs")
    quota, period = (cgroup / "cpu.max").read_text().split()
    if quota == "max" or int(quota) != int(float(service["cpus"]) * int(period)):
        raise RuntimeError("Valkey CPUQuota differs")
    limits = (proc / "limits").read_text().splitlines()
    if not any(line.startswith("Max locked memory") and "unlimited" in line for line in limits):
        raise RuntimeError("Valkey memlock limit differs")
    return [{"unit": service["unit"], "pid": int(pid), "cgroup": expected_cgroup,
             "systemd": state, "argv": actual, "source": service["source_commit"], "server_sha256": service["server_sha256"]}]


def start(config):
    service = config.get("native_valkey")
    if not service:
        return []
    identity(service["runtime"])
    state = status(service["unit"])
    if state.get("LoadState") != "not-found":
        return evidence(config)
    command(["systemd-run", "--unit=" + service["unit"], "--service-type=exec",
             "--property=CPUQuota=" + str(service["cpus"] * 100) + "%",
             "--property=MemoryMax=" + str(service["memory_mib"]) + "M",
             "--property=MemorySwapMax=0", "--property=LimitMEMLOCK=infinity",
             "--property=TimeoutStopSec=25", *service["args"]], timeout=60)
    return evidence(config)


def stop(config):
    service = config.get("native_valkey")
    if not service:
        return []
    state = status(service["unit"])
    if state.get("LoadState") == "not-found":
        return []
    if state.get("ActiveState") in ("inactive", "failed") and state.get("MainPID") == "0":
        admit_unit(service)
        if state["ActiveState"] == "failed":
            command(["systemctl", "reset-failed", service["unit"]], timeout=15)
            after = status(service["unit"])
            if after.get("LoadState") != "not-found" and after.get("ActiveState") not in ("inactive", "failed"):
                raise RuntimeError("Valkey failed unit did not quiesce")
        return [{"unit": service["unit"], "systemd": state, "already_stopped": True}]
    before = evidence(config)
    command(["systemctl", "stop", service["unit"]], timeout=35)
    after = status(service["unit"])
    if after.get("LoadState") != "not-found" and (after.get("ActiveState") not in ("inactive", "failed") or after.get("MainPID") != "0"):
        raise RuntimeError("Valkey service did not stop")
    return before
