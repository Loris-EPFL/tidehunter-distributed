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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request
# A sealed extracted source tree must remain byte-for-byte immutable.
sys.dont_write_bytecode = True

import analyze
import campaign
import cloud_campaign
import cloud_layout
import local

ACCOUNT = "354775971805"
TAGS = {"Project": "TideHunter", "ManagedBy": "sharded-wal-benchmark",
        "Repository": "Loris-EPFL/tidehunter-distributed", "Campaign": "kv-comparison",
        "BenchmarkBranch": "bench/aws-efa", "SourceBranch": "feat/cloud-kv-comparison"}
RAW_FILES = ("manifest.json", "summary.json", "samples.csv", "metrics.prom")
DISABLED_RESIDENT_READS = {"source_bytes": 0, "source_regions": 0, "destination_bytes": 0,
    "destination_slots": 0, "max_read_bytes": 0, "max_handles": 0, "require_device_read": False}


class SafetyError(RuntimeError):
    """An ownership/identity/path invariant failed: do not continue workloads."""


class InfrastructureError(RuntimeError):
    """Infrastructure or an uncertain side effect makes continuation unsafe."""


class DeadlineError(InfrastructureError):
    pass


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


def validate_config(config):
    required = {"fleet", "bucket", "bucket_region", "prefix", "account", "source_revision",
                "source_sha256", "binary_sha256", "binary_blake2b512", "repository_root",
                "binary", "fdb_library_dir", "deadline_utc", "spec"}
    if not isinstance(config, dict) or set(config) != required:
        raise SafetyError("runtime config requires its exact documented schema")
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
    cloud_campaign.make_matrix(config["spec"])
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
        admission = cloud_package.verify_directory(stage, runtime=True)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        raise SafetyError("sealed source/runtime package admission failed: " + str(exc)) from exc
    inventory = json.loads((stage / "source.json").read_text())
    if (admission["source_sha256"] != config["source_sha256"] or inventory["revision"] != config["source_revision"]
            or admission["binary_sha256"] != config["binary_sha256"]
            or admission["binary_blake2b512"] != config["binary_blake2b512"]):
        raise SafetyError("sealed package differs from externally admitted runtime hashes")
    library = stage / "runtime/lib/libfdb_c.so"
    fdb = json.loads((stage / "runtime/fdb-client.json").read_text())
    return {"source": inventory, "admission": admission,
            "binary_sha256": admission["binary_sha256"], "binary_blake2b512": admission["binary_blake2b512"],
            "fdb": {**fdb, "path": str(library)}, "manifest": json.loads((stage / "manifest.json").read_text()),
            "build": json.loads((stage / "build.json").read_text())}


def environment(config):
    result = dict(os.environ)
    for key in ("LD_PRELOAD", "LD_AUDIT"):
        result.pop(key, None)
    result.update(LD_LIBRARY_PATH=config["fdb_library_dir"] + ":/opt/amazon/efa/lib",
                  FDB_CLIENT_LIB_DIR=config["fdb_library_dir"], FDB_CLIENT_LIB_PATH=config["fdb_library_dir"],
                  FI_EFA_USE_DEVICE_RDMA="1")
    return result


def command(argv, *, timeout=60, env=None):
    result = subprocess.run([str(x) for x in argv], capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        raise InfrastructureError(f"command failed ({result.returncode}): {shlex.join([str(x) for x in argv])}\n{result.stdout}\n{result.stderr}")
    return result.stdout


def admit_fabrics(config, index, clients, root):
    # Pinned util/info.c maps -p to prov_name, -f to fabric name, and -c/-m
    # to caps/mode. efa-direct is a fabric of provider efa, not a provider:
    # https://github.com/ofiwg/libfabric/blob/3cc1095272b07ebeb5cc8f44851af6aa3eb75e41/util/info.c
    # Match resources.rs::Info::hints. Direct TLS/native messages need no RMA;
    # only registered-RAM adds read caps and RX_CQ_DATA. Rust still checks
    # endpoint geometry and FI_OPT_EFA_EMULATED_READ after endpoint enable.
    profiles = (
        ("efa_message", "efa", "FI_MSG|FI_DIRECTED_RECV", "FI_CONTEXT"),
        ("efa_direct_message", "efa-direct", "FI_MSG", "FI_CONTEXT2"),
        ("efa_direct_read", "efa-direct", "FI_MSG|FI_RMA|FI_READ|FI_REMOTE_READ", "FI_CONTEXT2|FI_RX_CQ_DATA"),
    )
    host = config["fleet"]["coordinator"] if index == -1 else config["fleet"]["nodes"][index]
    receipt = {"complete": False, "run_id": config["prefix"], "instance_id": host["instance_id"],
               "source_sha256": config["source_sha256"], "binary_sha256": config["binary_sha256"],
               "scope": "fi_getinfo discovery only; not endpoint/native-read or workload qualification", "probes": {}}
    failure = None
    for name, fabric, caps, mode in profiles:
        argv = ["/opt/amazon/efa/bin/fi_info", "-p", "efa", "-f", fabric,
                "-t", "FI_EP_RDM", "-c", caps, "-m", mode, "-v"]
        probe = {"argv": argv}
        receipt["probes"][name] = probe
        try:
            probe["stdout"] = command(argv, timeout=30, env=environment(config))
            probe["status"] = "discovered"
        except Exception as exc:
            probe.update(status="failed", error=type(exc).__name__ + ": " + str(exc))
            failure = exc
            break
    receipt["complete"] = failure is None
    dump(Path(root) / "fabric-admission.json", receipt)
    key = config["prefix"] + ("/results/fabric-admission.json" if index == -1
                              else f"/results/cells/startup/storage/{index}/fabric-admission.json")
    try:
        s3_json(clients["s3"], config, key, receipt)
    except Exception as exc:
        if failure is not None:
            raise failure from exc
        raise
    if failure is not None:
        raise failure
    return receipt


def make_clients(config, *, guest=False):
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
            # SendCommand is deliberately never retried: an unknown response can
            # already have started the command. Poll only a known command ID.
            clients[service] = session.client(service, config=Config(**common, retries={
                "total_max_attempts": 1 if service == "ssm" else 2, "mode": "standard"}))
    return clients


def s3_json(client, config, key, value):
    if not key.startswith(config["prefix"] + "/") or ".." in key.split("/"):
        raise SafetyError("S3 key escapes exact run prefix")
    raw = (json.dumps(value, sort_keys=True, indent=2, default=str) + "\n").encode()
    checksum = hashlib.sha256(raw).hexdigest()
    client.put_object(Bucket=config["bucket"], Key=key, Body=raw, ExpectedBucketOwner=config["account"],
                      ServerSideEncryption="AES256", Metadata={"sha256": checksum}, ContentType="application/json")
    observed = client.head_object(Bucket=config["bucket"], Key=key, ExpectedBucketOwner=config["account"])
    if observed.get("ContentLength") != len(raw) or observed.get("Metadata", {}).get("sha256") != checksum:
        raise InfrastructureError("S3 JSON export identity not confirmed by HeadObject")
    return {"key": key, "bytes": len(raw), "sha256": checksum}


def read_s3_json(client, config, key, limit=32 * 1024 * 1024):
    if not key.startswith(config["prefix"] + "/"):
        raise SafetyError("foreign S3 result key")
    response = client.get_object(Bucket=config["bucket"], Key=key, ExpectedBucketOwner=config["account"])
    with response["Body"] as body:
        raw = body.read(limit + 1)
    if len(raw) > limit:
        raise InfrastructureError("bounded guest result exceeded")
    return json.loads(raw)


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
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", case) or not re.fullmatch(r"(?:client|storage-[0-4])", role):
        raise SafetyError("unsafe exact-owned unit name")
    return f"kvcloud-{config['prefix']}-{hashlib.sha256(case.encode()).hexdigest()[:16]}-{role}"


def systemd_argv(config, case, role, cpus, memory, seconds, executable):
    if seconds < 1 or cpus <= 0 or memory <= 0:
        raise DeadlineError("no admitted runtime/budget remains")
    env = environment(config)
    return ["systemd-run", "--unit=" + unit_name(config, case, role), "--service-type=exec",
            "--property=CPUQuota=" + str(cpus * 100) + "%", "--property=MemoryMax=" + str(int(memory)) + "M",
            "--property=MemorySwapMax=0", "--property=LimitMEMLOCK=infinity",
            "--property=RuntimeMaxSec=" + str(int(seconds)), "--property=TimeoutStopSec=25",
            "--property=KillMode=control-group", "--property=RemainAfterExit=yes",
            "--property=CPUAccounting=yes", "--property=MemoryAccounting=yes", "--property=IOAccounting=yes",
            "--property=WorkingDirectory=" + config["repository_root"],
            *["--setenv=" + key + "=" + env[key] for key in ("LD_LIBRARY_PATH", "FDB_CLIENT_LIB_DIR", "FDB_CLIENT_LIB_PATH", "FI_EFA_USE_DEVICE_RDMA")],
            "--", *map(str, executable)]


def unit_status(name):
    result = subprocess.run(["systemctl", "show", name,
        "--property=LoadState,MainPID,ActiveState,SubState,Result,ExecMainStatus,ControlGroup,CPUUsageNSec,MemoryPeak,IOReadBytes,IOWriteBytes,CPUQuotaPerSecUSec,MemoryMax"],
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
                  for index, host in zip(range(-1, 5), [fleet["coordinator"], *fleet["nodes"]])],
        "ports": {"prometheus": 19090, "grafana": 13000, "benchmark": 19899,
                  "node": 19100, "host": 19898}, "allow_loopback_hosts": False}


def monitoring_access(config, proofs=()):
    """Describe private operator access without asserting human IAM grants."""
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


def owned_monitor_processes(config, index, job):
    """Observe known deployment PIDs only; never scan /proc for candidates."""
    processes, missing, identities = [], [], []
    role = "client" if index == -1 else "storage-" + str(index)
    if index == -1 or job["backend"] in cloud_campaign.TRANSPORTS:
        name = unit_name(config, job["case_id"], role) + ".service"
        processes.append({"role": role, "cgroup": "/system.slice/" + name})
    if job["backend"] in cloud_layout.BACKENDS:
        deployment = cloud_layout.node_config(config["fleet"], job["backend"], index)
        for container in deployment["containers"]:
            try:
                item = cloud_layout.inspect_container(deployment, container)
                identity = docker_monitor_identity(item)
                processes.append({"role": container["service"], "cgroup": identity["cgroup"]})
                identities.append({"role": container["service"], **identity})
            except Exception as exc:
                missing.append({"role": container["service"], "reason": type(exc).__name__ + ": " + str(exc)})
    return {"processes": processes, "missing": missing, "identities": identities}


def snapshot(config, name=None, *, drop=False, completed=False):
    root = guarded(config["fleet"]["data_root"], Path("/mnt/tidehunter-kv-comparison"), must_exist=True)
    nvme = cloud_layout.nvme_evidence(root, False)
    if drop:
        command(["sync"], timeout=30)
        Path("/proc/sys/vm/drop_caches").write_text("3\n")
    result = {"observed_utc": utc().isoformat(), "monotonic_ns": time.monotonic_ns(), "nvme": nvme,
              "drop_os_caches": drop, "application_caches_evicted": False,
              "diskstats": Path("/proc/diskstats").read_text(), "vmstat": Path("/proc/vmstat").read_text()}
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


def validate_launch(config, launch, case, index):
    root = Path(config["fleet"]["data_root"]) / "cells" / case
    node = root / f"node-{index}"
    expected = {"ready_file": node / "ready", "stop_file": node / "stop"}
    for field, path in expected.items():
        if launch.get(field) != str(path):
            raise SafetyError("typed launch marker escapes exact case/node")
        guarded(path, root)
    if launch.get("node", {}).get("root") != str(node / "storage"):
        raise SafetyError("typed database root escapes exact case/node")
    guarded(node / "storage", root)
    if launch.get("listen") != config["fleet"]["nodes"][index]["address"] + ":9400":
        raise SafetyError("typed listener differs from exact storage endpoint")
    tls = launch.get("tls", {})
    if tls.get("transport") not in cloud_campaign.TRANSPORTS.values():
        raise SafetyError("unknown typed transport")
    if tls["transport"] == "efa-native":
        if any(tls.get(field) for field in ("ca", "cert", "key")):
            raise SafetyError("trusted native transport must have empty TLS paths")
    else:
        expected_tls = {"ca": "ca.der", "cert": f"tidehunter-node-{index}.der",
                        "key": f"tidehunter-node-{index}.pk8"}
        for field, name in expected_tls.items():
            if tls.get(field) != str(root / "tls" / name):
                raise SafetyError("node capsule may contain only CA certificate and its own leaf identity")
            guarded(tls[field], root)


def guest_operation(config, request, clients):
    index = request["index"]
    if type(index) is not int or index not in range(-1, 5):
        raise SafetyError("exact guest index required")
    identity = verify_local_identity(config, index)
    root = guarded(config["fleet"]["data_root"], Path("/mnt/tidehunter-kv-comparison"), must_exist=True)
    action = request["action"]
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
        return {"identity": identity, "package": package, "bootstrap": receipt, "ready": ready, "monitor_images": monitor_images,
                "expiry_timer": command(["systemctl", "show", timer], timeout=10), "images": images,
                "fabric_admission": admit_fabrics(config, index, clients, root),
                "ldd": command(["ldd", config["binary"]], timeout=30, env=environment(config)),
                "snapshot": snapshot(config)}
    if action in ("monitor_start", "monitor_stop"):
        observer = monitor_module(config)
        spec = monitoring_spec(config, root / "results")
        return observer.start_host(spec, index) if action == "monitor_start" else observer.stop_host(spec, index)
    if action == "snapshot":
        observed = snapshot(config, request.get("unit"), drop=request.get("drop", False))
        if request.get("monitor_job") is not None:
            started = time.monotonic()
            try:
                evidence = owned_monitor_processes(config, index, request["monitor_job"])
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
        if operation == "probe":
            argv += ["--timeout", str(request["timeout"])]
            if request.get("initialize"):
                argv.append("--initialize")
            if request.get("cluster"):
                argv.append("--cluster")
        return json.loads(command(argv, timeout=request["operation_timeout"], env=environment(config)))
    case = request["case_id"]
    name = unit_name(config, case, "storage-" + str(index))
    guest_root = guarded(root / "cells" / case, root)
    capsule_root = guarded(guest_root / ("node-" + str(index)), root)
    if action == "tide_start":
        expected_key = f"{config['prefix']}/deploy/cells/{case}/storage-{index}.tar.gz"
        if request["key"] != expected_key:
            raise SafetyError("unexpected capsule S3 path")
        archive = guarded(root / ("capsule-" + hashlib.sha256(case.encode()).hexdigest()[:16] + "-" + str(index) + ".tar.gz"), root)
        clients["s3"].download_file(config["bucket"], expected_key, str(archive), ExtraArgs={"ExpectedBucketOwner": config["account"]})
        if digest(archive) != request["sha256"]:
            raise SafetyError("node capsule bytes differ from admission")
        safe_extract_capsule(archive, guest_root, request["files"])
        archive.unlink()
        import yaml
        launch = yaml.safe_load((capsule_root / "launch.yaml").read_text())
        validate_launch(config, launch, case, index)
        seconds = min(request["timeout"], int((timestamp(config["deadline_utc"]) - utc()).total_seconds()))
        command(systemd_argv(config, case, "storage-" + str(index), config["fleet"]["server_cpus"],
                             config["fleet"]["server_memory_mib"], seconds,
                             [config["binary"], "--experimental-storage-node", capsule_root / "launch.yaml"]), timeout=20)
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
                return {"ready": address, "launch_sha256": digest(capsule_root / "launch.yaml"), "unit": status}
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
        database = guarded(launch["node"]["root"], capsule_root, must_exist=True)
        shutil.rmtree(database)
        result["database_removed"] = str(database)
        return result
    raise SafetyError("unsupported guest action")


class Controller:
    def __init__(self, config, clients, *, clock=time.monotonic):
        self.config = validate_config(config)
        self.clients = clients
        self.clock = clock
        seconds = (timestamp(config["deadline_utc"]) - utc()).total_seconds()
        self.end = clock() + min(seconds, config["spec"]["hard_deadline_seconds"])
        self.work_end = self.end - config["spec"]["export_reserve_seconds"]
        self.root = guarded(Path(config["fleet"]["data_root"]) / "results", Path(config["fleet"]["data_root"]))
        self.ids = [h["instance_id"] for h in [config["fleet"]["coordinator"], *config["fleet"]["nodes"]]]
        self.sequence = 0
        self.sequence_lock = threading.Lock()
        self.active_backend = None
        self.active_tide = None
        self.package = None
        self.root_owned = False
        self.expiry = None
        self.monitoring_attempted = False
        self.monitoring_events = []
        self.monitoring_complete = True

    def remaining(self, *, export=False):
        remaining = (self.end if export else self.work_end) - self.clock()
        if remaining <= 0:
            raise DeadlineError("absolute deadline/export reserve reached")
        return remaining

    def guard(self, *, terminating=False, starting=False):
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
        return s3_json(self.clients["s3"], self.config, self.config["prefix"] + "/results/" + name, value)

    def request(self, index, request, *, case="startup", timeout=180, export=False):
        timeout = max(1, min(int(timeout), int(self.remaining(export=export))))
        request = {**request, "index": index}
        if request.get("action") == "layout":
            request["operation_timeout"] = timeout
        if request.get("action") == "layout" and request.get("operation") == "probe":
            request["timeout"] = min(request["timeout"], timeout)
        self.guard()
        if index == -1:
            return guest_operation(self.config, request, self.clients)
        with self.sequence_lock:
            self.sequence += 1
            number = self.sequence
        key = f"{self.config['prefix']}/results/cells/{case}/storage/{index}/{number:06d}-{request['action']}.json"
        payload = {"request": request, "key": key}
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
        dump(journal, {**intent, "status": "intent"})
        deadline = self.clock() + timeout
        while self.clock() < deadline:
            self.remaining(export=export)
            try:
                response = self.clients["ssm"].send_command(InstanceIds=[self.ids[index + 1]], DocumentName="AWS-RunShellScript",
                    Parameters={"commands": [shell], "executionTimeout": [str(timeout)]},
                    TimeoutSeconds=max(30, min(2592000, timeout)), Comment="TideHunter exact-owned comparison")
                command_id = response["Command"]["CommandId"]
                if not isinstance(command_id, str) or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", command_id):
                    raise ValueError("missing/invalid command identity")
                break
            except self.clients["ssm"].exceptions.InvalidInstanceId as exc:
                # This typed rejection guarantees nonacceptance. It is distinct
                # from a lost response, which must never trigger another send.
                dump(journal, {**intent, "status": "known_not_accepted", "error": repr(exc)})
                if request["action"] != "bootstrap":
                    raise InfrastructureError("SSM rejected exact guest; no command was accepted") from exc
                time.sleep(min(2, max(0, deadline - self.clock())))
                self.guard()
            except Exception as exc:
                dump(journal, {**intent, "status": "unknown_send_response", "error": repr(exc)})
                raise InfrastructureError("SSM SendCommand outcome unknown; never blindly retry") from exc
        else:
            raise InfrastructureError("exact guest never accepted bounded bootstrap command")
        dump(journal, {**intent, "status": "sent", "command_id": command_id})
        while self.clock() < deadline:
            self.remaining(export=export)
            try:
                invocation = self.clients["ssm"].get_command_invocation(CommandId=command_id, InstanceId=self.ids[index + 1])
            except self.clients["ssm"].exceptions.InvocationDoesNotExist:
                time.sleep(0.5)
                continue
            status = invocation.get("Status")
            if status in ("Pending", "InProgress", "Delayed"):
                time.sleep(0.5)
                continue
            dump(journal, {**intent, "command_id": command_id, "invocation": invocation})
            if status != "Success" or invocation.get("ResponseCode") != 0:
                raise InfrastructureError("SSM command failed/unknown terminal status: " + str(status))
            result = read_s3_json(self.clients["s3"], self.config, key)
            if result.get("request") != request or result.get("instance_id") != self.ids[index + 1]:
                raise SafetyError("guest response not bound to exact request/host")
            dump(self.root / "guest" / case / f"storage-{index}-{number:06d}.json", result)
            if result.get("status") != "completed":
                raise InfrastructureError("guest operation failed: " + str(result.get("error")))
            return result["result"]
        raise InfrastructureError("SSM known command deadline exceeded; state may still be live")

    def all_hosts(self, request, *, case="startup", timeout=180, export=False):
        # Calls are bounded and each mutating SSM request rechecks all six tags.
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(self.request, index, request, case=case, timeout=timeout, export=export) for index in range(-1, 5)]
            return [future.result() for future in futures]

    def bootstrap(self):
        self.root.mkdir(parents=True, exist_ok=False)
        self.root_owned = True
        identity = verify_local_identity(self.config, -1)
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

    def observer_hosts(self, action, *, export=False, timeout=30):
        def host(index):
            return self.observe(f"{action}:{index}", lambda: self.request(index,
                {"action": action}, timeout=timeout, export=export))
        with ThreadPoolExecutor(max_workers=6) as pool:
            return list(pool.map(host, range(-1, 5)))

    def start_monitoring(self):
        self.monitoring_attempted = True  # Partial starts remain exact-owned cleanup candidates.
        self.observer_hosts("monitor_start", timeout=90)
        self.observe("collector_start", lambda: monitor_module(self.config).start_collector(
            monitoring_spec(self.config, self.root), timeout_seconds=min(120, self.remaining())))

    def select_monitoring(self, job, directory):
        if not self.monitoring_attempted:
            return
        self.observe("case_select:" + job["case_id"], lambda: monitor_module(self.config).select_case(
            monitoring_spec(self.config, self.root), job, directory / "result"))

    def finish_monitoring(self, job, directory, receipt):
        if not self.monitoring_attempted:
            return
        summary = self.observe("case_summary:" + job["case_id"],
            lambda: cloud_campaign.summarize_case(directory), retain_receipt=False) if receipt["raw_artifacts_validated"] else None
        def finish():
            evidence = receipt
            for key in ("counter_evidence", "observed", "effective_configuration", "client", "write_profile_limit"):
                evidence = evidence.get(key) if isinstance(evidence, dict) else None
            observed = {"write_profile_enabled": evidence > 0} if type(evidence) is int and evidence >= 0 else {}
            read_evidence = receipt.get("counter_evidence", {}).get("observed")
            if isinstance(read_evidence, dict):
                observed["read_evidence"] = read_evidence
            return monitor_module(self.config).finish_case(monitoring_spec(self.config, self.root),
                job, directory / "result", metrics=summary.get("metrics") if summary else None,
                status=receipt["status"], timeout_seconds=min(2, self.remaining(export=True)), **observed)
        self.observe("case_finish:" + job["case_id"], finish)

    def export_monitoring(self):
        if not self.monitoring_attempted:
            return
        def capture():
            timeout = self.observer_cleanup_budget(90, following_seconds=40)
            destination = guarded(self.root / "observability/monitoring.tar.gz", self.root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            return monitor_module(self.config).export_session(monitoring_spec(self.config, self.root),
                destination, timeout_seconds=timeout)
        self.observe("snapshot_export", capture)

    def stop_monitoring(self):
        if not self.monitoring_attempted:
            return
        self.observe("collector_stop", lambda: monitor_module(self.config).stop_collector(
            monitoring_spec(self.config, self.root),
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
            "placement": "controller-cohosted observer on root volume; capped, not proven non-perturbing",
            "spec": monitoring_spec(self.config, self.root), "events": self.monitoring_events}

    def layout(self, backend, operation, index, **kwargs):
        return self.request(index, {"action": "layout", "backend": backend, "operation": operation, **kwargs},
                            case="startup", timeout=kwargs.get("timeout", 180))

    def start_backend(self, backend):
        self.active_backend = backend  # Retain partial start for finally cleanup.
        proofs = []
        for index in range(-1, 5):
            self.layout(backend, "prepare", index)
            self.layout(backend, "start", index)
            if backend == "cassandra" and index >= 0:
                self.layout(backend, "probe", index, timeout=180)
        proofs.append(self.layout(backend, "probe", -1, timeout=180, initialize=True, cluster=True))
        for index in range(5):
            proofs.append(self.layout(backend, "probe", index, timeout=180, cluster=True))
        validate_physical_proofs([x["proof"]["nvme"] for x in proofs])
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
        for index in range(-1, 5):
            try:
                evidence = self.request(index, {"action": "layout", "backend": backend, "operation": "export"},
                                        timeout=self.cleanup_timeout(deadline), export=True)
                dump(deployment / f"host-{index}-export.json", evidence)
            except Exception as exc:
                errors.append(f"host {index} export: {exc!r}")
        for index in range(-1, 5):
            try:
                stopped = self.request(index, {"action": "layout", "backend": backend, "operation": "stop"},
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
        for index in range(-1, 5):
            try:
                reset = self.request(index, {"action": "layout", "backend": backend, "operation": "reset-data"},
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
        allowance = min(512 * 1024**2, int(self.config["fleet"]["server_memory_mib"] * 1024**2 // 4))
        prep = {"guest_root": guest_root, "addresses": [h["address"] + ":9400" for h in self.config["fleet"]["nodes"]],
                "transport": cloud_campaign.TRANSPORTS[job["backend"]],
                "max_batches_per_partition": max(16384, job["keys"] + job["warmup_operations"] + job["measurement_operations"]),
                "max_admitted_batches": 1_000_000, "max_index_bytes": 512 * 1024**2,
                "coordinator_cache_bytes": job["coordinator_cache_bytes"], "owner_read_workers": 4,
                "owner_wal_member_read": "async", "owner_read_cache_bytes": job["owner_read_cache_bytes"],
                "owner_read_memory_allowance_bytes": allowance,
                "owner_read_resident_first": "resident_reads" in job,
                "resident_reads": job.get("resident_reads", DISABLED_RESIDENT_READS),
                "polling": "adaptive"}
        dump(directory / "prepare-input.json", prep)
        prepared = directory / "prepared"
        command([self.config["binary"], "--prepare-external-case", directory / "prepare-input.json", "--output", prepared],
                timeout=min(60, self.remaining()), env=environment(self.config))
        self.active_tide = (job, directory, [])
        remote = json.loads((prepared / "remote.json").read_text())
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
            launch = yaml.safe_load(launch_path.read_text())
            validate_launch(self.config, launch, case, index)
            files = {f"node-{index}/launch.yaml": launch_path}
            # Typed helper already embeds final guest paths. Only these three TLS
            # files may cross the machine boundary; never its CA signing key.
            tls = launch["tls"]
            for field in ("ca", "cert", "key"):
                if not tls.get(field):
                    continue
                target = Path(tls[field])
                relative = target.relative_to(Path(guest_root))
                source = guarded(prepared / "tls" / target.name, prepared, must_exist=True)
                files[str(relative)] = source
            archive = directory / f"storage-{index}.tar.gz"
            with tarfile.open(archive, "w:gz") as bundle:
                for name, source in files.items():
                    bundle.add(source, arcname=name, recursive=False)
            key = f"{self.config['prefix']}/deploy/cells/{case}/storage-{index}.tar.gz"
            self.clients["s3"].upload_file(str(archive), self.config["bucket"], key, ExtraArgs={
                "ExpectedBucketOwner": self.config["account"], "ServerSideEncryption": "AES256", "Metadata": {"sha256": digest(archive)}})
            # Mark before send; even an unknown response must be cleaned up.
            with self.sequence_lock:
                self.active_tide[2].append(index)
            service_seconds = min(int(self.remaining()), max(self.config["spec"]["case_deadline_seconds"],
                600 if job["keys"] > 65536 else 0) + 300)
            proof = self.request(index, {"action": "tide_start", "case_id": case, "key": key,
                "sha256": digest(archive), "files": list(files), "timeout": service_seconds}, case=case, timeout=120)
            dump(directory / "server-proofs" / f"start-{index}.json", proof)
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(start, index) for index in range(5)]
            for future in futures:
                future.result()
        return {"path": str(directory / "database"), "evidence": str(directory / "read-evidence"), "owners": 5,
                "key_count": max(1048576, job["keys"]), "max_batches_per_partition": prep["max_batches_per_partition"],
                "coordinator_read_cache_bytes": job["coordinator_cache_bytes"],
                "external_remote_config": remote,
                "external_owner_read_config": json.loads((prepared / "external_owner_read_config.json").read_text())}

    def stop_tide(self):
        if not self.active_tide:
            return
        job, directory, indices = self.active_tide
        errors = []
        deadline = self.cleanup_deadline()
        def stop(index):
            result = self.request(index, {"action": "tide_stop", "case_id": job["case_id"]},
                                  case=job["case_id"], timeout=self.cleanup_timeout(deadline), export=True)
            dump(directory / "server-proofs" / f"stop-{index}.json", result)

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(stop, index) for index in indices]
            for future in futures:
                try:
                    future.result()
                except Exception as exc:
                    errors.append(repr(exc))
        self.active_tide = None
        if errors:
            raise InfrastructureError("Tidehunter exact unit cleanup incomplete: " + "; ".join(errors))

    def run_case(self, job):
        directory = guarded(self.root / "cases" / job["case_id"], self.root)
        directory.mkdir(parents=True, exist_ok=False)
        receipt = {"job": job, "status": "failed", "binary_blake2b512": self.config["binary_blake2b512"],
                   "artifacts_sha256": {}, "measurement_evidence": {}, "counter_evidence": {},
                   "raw_artifacts_validated": False}
        client = unit_name(self.config, job["case_id"], "client")
        started = False
        client_quiesced = False
        failure = None
        self.select_monitoring(job, directory)
        try:
            self.guard()
            backend = job["backend"]
            budget = {"cpus": self.config["fleet"]["client_cpus"], "memory_mib": self.config["fleet"]["client_memory_mib"]}
            if backend in cloud_layout.BACKENDS:
                cfg = cloud_layout.node_config(self.config["fleet"], backend, -1)
                options = cloud_layout.backend_options(self.config["fleet"], backend, cfg["directory"])
                budget = cfg["client_budget"]
                if backend == "foundationdb" and not Path(options["cluster_file"]).is_file():
                    raise InfrastructureError("generated local FoundationDB cluster file missing")
            elif backend == "native":
                options = {"path": str(directory / "database"), "key_count": max(1048576, job["keys"]), "durability": job["native_durability"]}
            else:
                options = self.prepare_tide(job, directory)
            seconds = min(self.config["spec"]["case_deadline_seconds"], int(self.remaining()))
            config = cloud_campaign.make_run_config(job, directory, options, self.config["source_revision"], seconds)
            # Fully overwrite the same bounded logical keyset across all stores
            # in a repetition; backend identity must not change physical keys.
            config["key_namespace_hex"] = hashlib.blake2b(
                f"{self.config['prefix']}:{job['repeat']}".encode(), digest_size=16).hexdigest()
            config["limits"]["deadline_seconds"] = min(config["limits"]["deadline_seconds"], int(self.remaining()))
            dump(directory / "config.json", config)
            try:
                admitted = campaign.admit_config(Path(self.config["binary"]), directory / "config.json", directory, environment(self.config))
                analyze.verify_normalization(config, admitted)
            except (RuntimeError, ValueError, KeyError) as exc:
                raise SafetyError("typed configuration admission failed: " + str(exc)) from exc
            dump(directory / "client-budget.json", budget)
            seconds = min(config["limits"]["deadline_seconds"] + 30, int(self.remaining()))
            self.guard()
            # Record possible ownership before the systemd request: a timeout
            # may have started the service despite losing the response.
            started = True
            command(systemd_argv(self.config, job["case_id"], "client", budget["cpus"], budget["memory_mib"], seconds,
                                 [self.config["binary"], "--config", directory / "config.json", "--output", directory / "result"]), timeout=20)
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
                before = self.all_hosts({"action": "snapshot", "drop": job["drop_os_caches"],
                    **({"monitor_job": job} if self.monitoring_attempted else {})}, case=job["case_id"], timeout=60)
                if self.monitoring_attempted:
                    for index, host in zip(range(-1, 5), before):
                        self.observe(f"host_select:{index}:" + job["case_id"],
                            lambda host=host: host.get("monitoring", {"complete": False, "error": "guest omitted host selector evidence"}))
                process_before = snapshot(self.config, client)
                dump(directory / "measurement-before.json", {"hosts": before, "client": process_before})
                (directory / "measurement.release").touch(exist_ok=False)
                gate_released = True
                while True:
                    self.remaining()
                    status = unit_status(client)
                    if status.get("SubState") == "exited" or status.get("ActiveState") in ("failed", "inactive"):
                        break
                    if self.clock() >= end:
                        raise DeadlineError("case absolute runtime exceeded")
                    time.sleep(0.2)
            dump(directory / "client-final-unit.json", status)
            after = self.all_hosts({"action": "snapshot", "drop": False}, case=job["case_id"], timeout=60)
            dump(directory / "measurement-after.json", {"hosts": after, "client": snapshot(self.config, client, completed=True)})
            receipt["measurement_evidence"] = {"gate_released": gate_released,
                "scope": "quiescent gate release through process exit; includes post-workload checks and drain",
                "before": "measurement-before.json" if gate_released else None, "after": "measurement-after.json",
                "before_sha256": digest(directory / "measurement-before.json") if gate_released else None,
                "after_sha256": digest(directory / "measurement-after.json"),
                "os_caches_dropped": job["drop_os_caches"] if gate_released else False, "engine_caches_evicted": False}
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
                observed_counts = campaign.verify_timing_result(directory / "result", admitted, allow_partial=True)
                receipt["validated_timing_rows_by_phase"] = observed_counts
                receipt["planned_timing_rows_by_phase"] = {
                    "preload": admitted["dataset"]["keys"] if admitted["phases"]["preload"] else 0,
                    "warmup": admitted["phases"]["warmup_operations"],
                    "aging": admitted["phases"]["aging_operations"],
                    "measurement": admitted["phases"]["measurement_operations"]}
                receipt["artifacts_sha256"] = {name: digest(directory / "result" / name) for name in RAW_FILES}
                receipt["core_manifest_status"] = manifest["status"]
                receipt["core_summary_status"] = summary["status"]
                receipt["raw_artifacts_validated"] = True
            except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError) as exc:
                raise SafetyError("terminal source/config/binary/schema/raw-row admission failed: " + str(exc)) from exc
            counters_missing = False
            if backend in cloud_campaign.TRANSPORTS:
                evidence = directory / "read-evidence/read-evidence.json"
                if evidence.is_file():
                    try:
                        observed = json.loads(evidence.read_text())
                        verify_counter_evidence(job, observed)
                        receipt["counter_evidence"] = {"path": str(evidence.relative_to(directory)), "sha256": digest(evidence),
                                                       "coordinator_scope": "cumulative_adapter_lifetime_not_measurement_interval",
                                                       "owner_io_scope": "latest_passive_sample_may_be_stale_not_measurement_interval",
                                                       "observed": observed}
                    except (OSError, RuntimeError, ValueError, KeyError, TypeError, AttributeError) as exc:
                        raise SafetyError("actual coordinator/owner counter schema admission failed: " + str(exc)) from exc
                else:
                    counters_missing = True
            if not gate_released:
                raise RuntimeError("core exited before quiescent measurement release: " + json.dumps(status))
            if status.get("Result") != "success" or status.get("ExecMainStatus") != "0" or manifest["status"] != "completed":
                raise RuntimeError("core workload failed: " + json.dumps({"unit": status, "core_status": manifest["status"]}))
            if observed_counts != receipt["planned_timing_rows_by_phase"]:
                raise SafetyError("completed core does not contain every planned raw timing row")
            if counters_missing:
                raise RuntimeError("coordinator actual cache/RPC evidence missing")
            receipt["status"] = "completed"
        except Exception as exc:
            failure = exc
            if isinstance(exc, SafetyError):
                receipt["raw_artifacts_validated"] = False
            receipt["error"] = type(exc).__name__ + ": " + str(exc)
        finally:
            # Publish the terminal snapshot while exact-owned cgroups still
            # exist. The scrape barrier and CSV reduction are outside API timers.
            self.observe("case_terminal_receipt:" + job["case_id"], lambda: dump(directory / "receipt.json", receipt))
            self.finish_monitoring(job, directory, receipt)
            if started:
                try:
                    command(["systemctl", "stop", client], timeout=35)
                    client_quiesced = True
                    (directory / "client.log").write_text(command(["journalctl", "--no-pager", "--unit", client, "--output=short-precise"], timeout=30))
                except Exception as exc:
                    failure = InfrastructureError("client cleanup/export failed: " + repr(exc))
            database = guarded(directory / "database", directory)
            receipt["database_files_retained"] = database.exists()
            if database.exists() and (not started or client_quiesced):
                try:
                    shutil.rmtree(database)
                    receipt["database_files_retained"] = False
                except OSError as exc:
                    failure = InfrastructureError("owned coordinator database cleanup failed: " + repr(exc))
            try:
                self.stop_tide()
            except Exception as exc:
                failure = exc
            receipt["artifacts_sha256"] = {name: digest(directory / "result" / name)
                for name in RAW_FILES if (directory / "result" / name).is_file()}
            if failure:
                receipt["status"] = "failed"
                receipt["error"] = type(failure).__name__ + ": " + str(failure)
            receipt["monitoring"] = {"attempted": self.monitoring_attempted,
                "host_case_scope": "selection begins at premeasurement gate; retained until next gate selection (may include cleanup/next preload)",
                "events": [event for event in self.monitoring_events
                           if event["operation"].endswith(":" + job["case_id"])]}
            dump(directory / "receipt.json", receipt)
        if isinstance(failure, (SafetyError, InfrastructureError, OSError, subprocess.TimeoutExpired)):
            raise failure
        if failure is not None and not isinstance(failure, (RuntimeError, ValueError)):
            raise InfrastructureError("unexpected infrastructure/API failure: " + repr(failure)) from failure
        return receipt

    def export(self, error):
        if not self.root_owned:
            s3_json(self.clients["s3"], self.config, self.config["prefix"] + "/results/receipt.json",
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
        from boto3.s3.transfer import TransferConfig
        key = self.config["prefix"] + "/results/controller.tar.gz"
        self.clients["s3"].upload_file(str(archive), self.config["bucket"], key, ExtraArgs={
            "ExpectedBucketOwner": self.config["account"], "ServerSideEncryption": "AES256", "Metadata": {"sha256": checksum}},
            Config=TransferConfig(multipart_threshold=16 << 20, multipart_chunksize=16 << 20, max_concurrency=2, use_threads=True),
            Callback=lambda count: self.remaining(export=True))
        head = self.clients["s3"].head_object(Bucket=self.config["bucket"], Key=key, ExpectedBucketOwner=self.config["account"])
        if head.get("ContentLength") != size or head.get("Metadata", {}).get("sha256") != checksum:
            raise InfrastructureError("raw archive HEAD differs from exact uploaded bytes")
        self.persist("receipt.json", {"status": "completed" if report["complete"] and not error else "incomplete",
            "error": error, "key": key, "bytes": size, "sha256": checksum, "files": records,
            "source_sha256": self.config["source_sha256"], "binary_blake2b512": self.config["binary_blake2b512"],
            "monitoring_complete": monitoring["complete"]})
        return report

    def terminate(self):
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
        export_error = None
        report = {"complete": False}
        try:
            self.bootstrap()
            jobs = cloud_campaign.make_matrix(self.config["spec"])
            dump(self.root / "matrix.json", {"schema_version": 1, "spec": self.config["spec"], "jobs": jobs})
            active_block = None
            for position, job in enumerate(jobs):
                self.remaining()
                backend = job["backend"]
                desired = backend if backend in cloud_layout.BACKENDS else None
                block = (backend, job["repeat"])
                if block != active_block:
                    self.stop_backend()
                    if desired:
                        self.start_backend(desired)
                    active_block = block
                try:
                    self.run_case(job)
                finally:
                    path = self.root / "cases" / job["case_id"] / "receipt.json"
                    receipt = json.loads(path.read_text()) if path.is_file() else {"status": "failed"}
                    self.persist("progress.json", {"observed_utc": utc().isoformat(), "case_index": position,
                        "total_cases": len(jobs), "case_id": job["case_id"], "status": receipt["status"],
                        "remaining_work_seconds": max(0, self.work_end - self.clock())})
        except BaseException as exc:
            error = type(exc).__name__ + ": " + str(exc)
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
                        s3_json(self.clients["s3"], self.config, self.config["prefix"] + "/results/receipt.json", value)
                except BaseException:
                    pass  # Original export failure is surfaced; termination is unconditional.
            finally:
                termination = self.terminate()
                # Local evidence may disappear when self termination takes effect.
                if self.root_owned:
                    dump(self.root / "termination-request.json", termination)
        if error or export_error or not report.get("complete"):
            raise InfrastructureError(json.dumps({"controller_error": error, "export_error": export_error,
                                                  "complete": report.get("complete", False)}))
        return report


def validate_physical_proofs(proofs):
    if len(proofs) != 6:
        raise SafetyError("six physical host proofs required")
    boot_ids, serials = set(), set()
    for proof in proofs:
        if proof.get("qualification_only") is not False or proof.get("independent_nvme_admitted") is not True:
            raise SafetyError("physical device proof was not admitted")
        boot = proof.get("host_identity", {}).get("boot_id")
        devices = proof.get("block_devices", {}).get("blockdevices", [])
        selected = [x for x in devices if "instance storage" in str(x.get("model", "")).lower() and x.get("serial")]
        if not boot or boot in boot_ids or not selected:
            raise SafetyError("physical independence not proven by unique boot IDs and instance NVMe serials")
        host_serials = {x["serial"] for x in selected}
        if serials & host_serials:
            raise SafetyError("duplicate physical NVMe serial across hosts")
        boot_ids.add(boot)
        serials |= host_serials
    return True


def verify_counter_evidence(job, evidence):
    coordinator = evidence.get("coordinator")
    owners = evidence.get("owner_read_configuration")
    if evidence.get("schema_version") != 1 or evidence.get("backend") != job["backend"] or not isinstance(coordinator, dict):
        raise SafetyError("actual coordinator counter evidence has wrong backend/schema")
    count = coordinator.get("read_rpc_count")
    if type(count) is not int or count < 0 or not isinstance(coordinator.get("cache"), dict):
        raise SafetyError("actual RPC/cache counters are missing")
    if coordinator["cache"].get("budget_bytes") != job["coordinator_cache_bytes"]:
        raise SafetyError("actual coordinator cache budget differs from declared cohort")
    if (not isinstance(owners, list) or len(owners) != 5 or
            {owner.get("partition") for owner in owners} != set(range(5)) or
            any(owner.get("read_cache_bytes") != job["owner_read_cache_bytes"] for owner in owners)):
        raise SafetyError("actual owner cache configurations differ from declared cohort")
    expected_resident = job.get("resident_reads", DISABLED_RESIDENT_READS)
    if not isinstance(expected_resident, dict) or type(expected_resident.get("require_device_read")) is not bool:
        raise SafetyError("job must declare a typed resident-read policy")
    for owner in owners:
        resident = owner.get("resident_reads")
        if (not isinstance(resident, dict) or resident != expected_resident
                or type(resident.get("require_device_read")) is not bool
                or resident["require_device_read"] is not expected_resident["require_device_read"]):
            raise SafetyError("actual owner resident-read/RMA policy differs from declared job")
    # Hit counts and passive sample availability are observations, not admission
    # requirements. Configuring a cache must never manufacture a hit population.
    return evidence


def archive_results(root, destination, *, deadline_check=None):
    records = {}
    root = Path(root)
    guarded(destination, Path(destination).parent)
    denied = {"database", "owners", "data", "wal", "tls", "coordinator-db"}
    retained = {".json", ".yaml", ".yml", ".csv", ".prom", ".log", ".txt", ".md", ".ready", ".release"}
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
    request, key = payload["request"], payload["key"]
    index = request["index"]
    if type(index) is not int or index not in range(5):
        raise SafetyError("SSM guest action is restricted to storage indices")
    expected = f"{config['prefix']}/results/cells/"
    if not key.startswith(expected) or f"/storage/{index}/" not in key or ".." in key.split("/"):
        raise SafetyError("guest result key is outside exact node prefix")
    clients = make_clients(config, guest=True)
    result = {"request": request, "instance_id": config["fleet"]["nodes"][index]["instance_id"]}
    try:
        result["result"] = guest_operation(config, request, clients)
        result["status"] = "completed"
    except Exception as exc:
        result.update(status="failed", error=type(exc).__name__ + ": " + str(exc))
    exported = s3_json(clients["s3"], config, key, result)
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
            jobs = cloud_campaign.make_matrix(config["spec"])
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
