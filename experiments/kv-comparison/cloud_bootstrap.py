#!/usr/bin/env python3
"""Render MCP-launched EC2 user data; install the sealed runtime and start its controller."""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import lzma
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import sys
import tarfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent
MAX_BUNDLE = 512 * 1024 * 1024


def validate_plan(plan):
    required = {"run_id", "account", "region", "bucket", "bucket_region", "bundle_key",
                "bundle_sha256", "bundle_bytes", "source_sha256", "binary_sha256",
                "deadline_utc", "ttl_seconds", "instance_type"}
    if (not isinstance(plan, dict) or not required <= set(plan)
            or set(plan) - required - {"read_tier_trace"}
            or ("read_tier_trace" in plan and type(plan["read_tier_trace"]) is not bool)):
        raise ValueError("exact bootstrap plan schema required")
    if plan["account"] != "354775971805" or not re.fullmatch(r"th-[a-f0-9]{16}", plan["run_id"]):
        raise ValueError("wrong account or run ID")
    if plan["region"] not in ("us-east-1", "us-east-2", "us-west-2", "eu-central-1", "eu-west-1"):
        raise ValueError("region not covered by independent watchdog")
    if plan["bucket_region"] != "us-east-1" or plan["bucket"] != "tidehunter-ci-354775971805-us-east-1":
        raise ValueError("wrong approved source/result bucket")
    if plan["bundle_key"] != plan["run_id"] + "/inputs/bundle.tar.gz":
        raise ValueError("bundle must be under exact owned input prefix")
    for key in ("bundle_sha256", "source_sha256", "binary_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", plan[key]):
            raise ValueError("invalid " + key)
    if type(plan["bundle_bytes"]) is not int or not 0 < plan["bundle_bytes"] <= MAX_BUNDLE:
        raise ValueError("bundle outside bounded input size")
    if type(plan["ttl_seconds"]) is not int or not 3600 <= plan["ttl_seconds"] <= 10800:
        raise ValueError("invalid bootstrap lifetime")
    deadline = datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00"))
    if deadline.tzinfo is None or deadline.utcoffset().total_seconds() != 0:
        raise ValueError("UTC absolute deadline required")
    if plan["instance_type"] not in ("g6.8xlarge", "g6.16xlarge", "g6e.8xlarge", "c6id.32xlarge"):
        raise ValueError("unsupported exact EFA/NVMe instance type")
    return plan


def activation_record(plan, config=None):
    """Build the explicit activation envelope; never contacts AWS."""
    validate_plan(plan)
    value = {"schema_version": 1, "state": "pending" if config is None else "ready",
             "run_id": plan["run_id"], "source_sha256": plan["source_sha256"]}
    if config is not None:
        value["config"] = config
    activation_config(plan, value)
    return value


def activation_config(plan, value):
    """Return a bound raw config, or None only for a valid pending envelope."""
    validate_plan(plan)
    if not isinstance(value, dict) or type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise ValueError("activation requires integer schema_version=1")
    state = value.get("state")
    if not isinstance(state, str) or state not in ("pending", "ready"):
        raise ValueError("unknown activation state")
    expected = {"schema_version", "state", "run_id", "source_sha256"}
    if state == "ready":
        expected.add("config")
    if set(value) != expected:
        raise ValueError("exact pending/ready activation envelope required")
    if value["run_id"] != plan["run_id"] or value["source_sha256"] != plan["source_sha256"]:
        raise ValueError("activation run/source differs from admitted bootstrap plan")
    if state == "pending":
        return None
    config = value["config"]
    if not isinstance(config, dict):
        raise ValueError("ready activation requires the raw controller config object")
    import cloud_controller
    try:
        cloud_controller.validate_config(config)
        json.dumps(config, allow_nan=False)
    except (RuntimeError, ValueError, KeyError, TypeError) as exc:
        raise ValueError("invalid ready controller config: " + str(exc)) from exc
    bindings = {"prefix": "run_id", "account": "account", "bucket": "bucket", "bucket_region": "bucket_region",
                "source_sha256": "source_sha256", "binary_sha256": "binary_sha256"}
    if any(config[field] != plan[planned] for field, planned in bindings.items()):
        raise ValueError("ready controller config differs from admitted run/source/binary/account/bucket")
    if config["fleet"]["run_id"] != plan["run_id"] or config["fleet"]["region"] != plan["region"]:
        raise ValueError("ready fleet differs from admitted run/region")
    if plan.get("read_tier_trace", False) != (config["spec"].get("mode", "standard") in ("read-tier", "read-tier-focused")):
        raise ValueError("read-tier trace preparation differs from admitted campaign mode")
    configured_deadline = datetime.fromisoformat(config["deadline_utc"].replace("Z", "+00:00"))
    planned_deadline = datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00"))
    if configured_deadline != planned_deadline:
        raise ValueError("ready controller deadline differs from immutable bootstrap deadline")
    stage = Path("/mnt/tidehunter") / plan["run_id"] / "package"
    for field, relative in (("repository_root", "source"), ("binary", "runtime/bin/kv-research"),
                            ("fdb_library_dir", "runtime/lib")):
        if config[field] != str(stage / relative):
            raise ValueError("ready controller paths differ from the admitted package layout")
    return config


def render_userdata(plan):
    validate_plan(plan)
    # Refuse a shortened TTL that cannot reach the fixed expiry before paying
    # for guests; the guest also enforces expiry <= pendingTime + TTL.
    remaining = (datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00"))
                 - datetime.now(timezone.utc)).total_seconds()
    if not 0 < remaining <= plan["ttl_seconds"]:
        raise ValueError("absolute deadline must be future and reachable within the unchanged launch TTL")
    old = HERE.parent / "aws-sharded-wal"
    # Reuse the reviewed disk/absolute-expiry code and pinned provider installer.
    sys.path.insert(0, str(old))
    try:
        specification = importlib.util.spec_from_file_location("kv_existing_control", old / "control.py")
        module = importlib.util.module_from_spec(specification)
        specification.loader.exec_module(module)
        efa = module.efa_install_script(plan)
    finally:
        sys.path.pop(0)
    trace_install = ("if ! timeout 180 apt-get install -y bpftrace; then "
                     "echo 'optional bpftrace unavailable; collector will report capability gap' >&2; fi"
                     if plan.get("read_tier_trace", False) else "")
    capsule = base64.b85encode(lzma.compress(json.dumps({
        "bootstrap": (old / "bootstrap.py").read_text(),
        "install": Path(__file__).read_text(), "plan": plan}).encode())).decode()
    unpack = ("import base64,json,lzma,pathlib;"
        f"c=json.loads(lzma.decompress(base64.b85decode({capsule!r})));"
        "pathlib.Path('/root/kv-disk-bootstrap.py').write_text(c['bootstrap']);"
        "pathlib.Path('/root/kv-install.py').write_text(c['install']);"
        "pathlib.Path('/root/kv-bootstrap-plan.json').write_text(json.dumps(c['plan']))")
    value = f'''#!/bin/bash
set -euo pipefail
/usr/sbin/shutdown -h +{plan['ttl_seconds'] // 60}
trap '/usr/sbin/poweroff' ERR
umask 077
/usr/bin/python3 -c {shlex.quote(unpack)}
/usr/bin/python3 /root/kv-disk-bootstrap.py --account {plan['account']} --run-id {plan['run_id']} --instance-type {plan['instance_type']} --ttl-seconds {plan['ttl_seconds']} --deadline-utc {shlex.quote(plan['deadline_utc'])}
export DEBIAN_FRONTEND=noninteractive
export PYTHONDONTWRITEBYTECODE=1
timeout 600 apt-get update -o Acquire::Retries=2
timeout 900 apt-get install -y python3-boto3 python3-yaml docker.io binutils curl ca-certificates openssl pkg-config libclang-dev rdma-core
{trace_install}
systemctl enable --now docker
{efa}
if systemctl cat snap.amazon-ssm-agent.amazon-ssm-agent.service >/dev/null 2>&1; then
  systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service
elif systemctl cat amazon-ssm-agent.service >/dev/null 2>&1; then
  systemctl enable --now amazon-ssm-agent.service
else
  curl --proto '=https' --tlsv1.2 --connect-timeout 10 --max-time 120 -fsS https://s3.{plan['region']}.amazonaws.com/amazon-ssm-{plan['region']}/latest/debian_amd64/amazon-ssm-agent.deb -o /root/amazon-ssm-agent.deb
  timeout 120 dpkg -i /root/amazon-ssm-agent.deb
  systemctl enable --now amazon-ssm-agent.service
fi
/usr/bin/python3 -B /root/kv-install.py install --plan /root/kv-bootstrap-plan.json
'''
    if len(value.encode()) > 16384:
        raise ValueError("user-data exceeds the checked EC2 16KiB limit")
    return value


def instance_launch_index():
    base = "http://169.254.169.254/latest"
    request = urllib.request.Request(base + "/api/token", method="PUT", headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"})
    with urllib.request.urlopen(request, timeout=3) as response:
        token = response.read(4096).decode()
    request = urllib.request.Request(base + "/meta-data/ami-launch-index", headers={"X-aws-ec2-metadata-token": token})
    with urllib.request.urlopen(request, timeout=3) as response:
        value = response.read(8).decode().strip()
    if value not in {str(i) for i in range(6)}:
        raise ValueError("unexpected atomic-fleet launch index")
    return int(value)


def safe_unpack(archive_path, root):
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    with tarfile.open(archive_path, "r:gz") as archive:
        seen, size = set(), 0
        for member in archive:
            name = PurePosixPath(member.name)
            size += member.size
            if (not member.isfile() or name.is_absolute() or ".." in name.parts
                    or str(name) != member.name or member.name in seen or size > MAX_BUNDLE or len(seen) >= 20000):
                raise ValueError("unsafe/oversized bootstrap archive")
            seen.add(member.name)
            target = root.joinpath(*name.parts)
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with archive.extractfile(member) as source, target.open("xb") as output:
                import shutil
                shutil.copyfileobj(source, output)
            target.chmod(0o755 if member.mode & 0o111 else 0o644)


def monitor_image_plan(source, index):
    """Use only the sealed lock, and cache collector images only on its host."""
    if type(index) is not int or index not in range(6):
        raise ValueError("exact admitted bootstrap launch index required")
    lock = Path(source) / "experiments/kv-comparison/observability/images.lock.json"
    pins = json.loads(lock.read_text())["images"]
    names = ["library/python", "prom/node-exporter"]
    if index == 5:
        names += ["prom/prometheus", "grafana/grafana"]
    result = {name: pins[name]["image"] for name in names}
    if any(not re.fullmatch(re.escape(name) + r":[A-Za-z0-9_.-]+@sha256:[a-f0-9]{64}", image)
           for name, image in result.items()):
        raise ValueError("monitoring images must use their sealed digest identities")
    return result


def cache_monitor_images(source, index, deadline):
    started = time.monotonic()
    stop = min(started + 180, started + (deadline - datetime.now(timezone.utc)).total_seconds() - 30)
    evidence = {"complete": True, "images": {}, "gaps": []}
    try:
        pins = monitor_image_plan(source, index)
    except Exception as exc:
        return {"complete": False, "images": {}, "gaps": [type(exc).__name__ + ": invalid sealed monitoring lock"],
                "elapsed_seconds": time.monotonic() - started}
    for name, image in pins.items():
        try:
            remaining = stop - time.monotonic()
            if remaining < 1:
                raise TimeoutError("monitor image cache deadline")
            subprocess.run(["docker", "pull", image], check=True, capture_output=True, text=True,
                           timeout=min(60, remaining))
            remaining = stop - time.monotonic()
            if remaining < 1:
                raise TimeoutError("monitor image inspection deadline")
            observed = json.loads(subprocess.check_output(["docker", "image", "inspect", image],
                text=True, timeout=min(10, remaining)))
            if len(observed) != 1 or not observed[0].get("Id"):
                raise ValueError("pinned image did not resolve to one local identity")
            evidence["images"][name] = {"image": image, "id": observed[0]["Id"],
                                       "repo_digests": observed[0].get("RepoDigests")}
        except Exception as exc:
            evidence["complete"] = False
            evidence["gaps"].append(name + ": " + type(exc).__name__)
    evidence["elapsed_seconds"] = time.monotonic() - started
    return evidence


def restart_ssm_after_profile():
    """Reload the profile attached after atomic allocation before command readiness."""
    # Match the Sui atomic bootstrap: enable --now leaves the AMI-started
    # agent running. AWS requires role discovery or a restart after attachment:
    # https://repost.aws/knowledge-center/assign-iam-role-ec2-instance
    for unit in ("amazon-ssm-agent.service", "snap.amazon-ssm-agent.amazon-ssm-agent.service"):
        installed = subprocess.run(["systemctl", "cat", unit], capture_output=True, text=True, timeout=10)
        if installed.returncode:
            continue
        subprocess.run(["systemctl", "restart", unit], check=True, timeout=30)
        subprocess.run(["systemctl", "is-active", "--quiet", unit], check=True, timeout=10)
        return unit
    raise RuntimeError("bootstrap installed no discoverable SSM Agent service")


def install(plan):
    validate_plan(plan)
    if os.geteuid() != 0:
        raise ValueError("fresh owned EC2 root required")
    import boto3
    from botocore.config import Config
    root = Path("/mnt/tidehunter") / plan["run_id"]
    bootstrap = json.loads((root / "bootstrap.json").read_text())
    observed_deadline = datetime.fromisoformat(bootstrap["deadline_utc"].replace("Z", "+00:00"))
    planned_deadline = datetime.fromisoformat(plan["deadline_utc"].replace("Z", "+00:00"))
    if (bootstrap["run_id"] != plan["run_id"]
            or bootstrap["identity"]["accountId"] != plan["account"]
            or observed_deadline != planned_deadline):
        raise ValueError("disk/account/absolute deadline receipt differs from admitted launch")
    client = boto3.client("s3", region_name=plan["bucket_region"], config=Config(
        connect_timeout=5, read_timeout=30, retries={"total_max_attempts": 3, "mode": "standard"}))
    archive = root / "bundle.tar.gz"
    client.download_file(plan["bucket"], plan["bundle_key"], str(archive), ExtraArgs={"ExpectedBucketOwner": plan["account"]})
    with archive.open("rb") as stream:
        checksum = hashlib.file_digest(stream, "sha256").hexdigest()
    if archive.stat().st_size != plan["bundle_bytes"] or checksum != plan["bundle_sha256"]:
        raise ValueError("downloaded runtime bundle differs from exact staged bytes")
    stage = root / "package"
    safe_unpack(archive, stage)
    sys.path.insert(0, str(stage / "source/experiments/kv-comparison"))
    import cloud_package
    import local
    manifest = json.loads((stage / "manifest.json").read_text())
    if manifest["source_sha256"] != plan["source_sha256"] or manifest["binary_sha256"] != plan["binary_sha256"]:
        raise ValueError("source/binary identities differ from startup contract")
    os.environ["LD_LIBRARY_PATH"] = str(stage / "runtime/lib") + ":/opt/amazon/efa/lib"
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    os.environ["FI_EFA_USE_DEVICE_RDMA"] = "1"
    cloud_package.verify_directory(stage, runtime=True)
    # The successful private bundle download proves the attached profile is usable.
    # Give the boot-started agent a fresh credential-discovery cycle.
    ssm_refresh = {"unit": restart_ssm_after_profile(), "process_active": True,
        "observed_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "post-profile agent restart only; managed-node command acceptance remains mandatory"}
    (root / "ssm-agent-profile-refresh.json").write_text(json.dumps(ssm_refresh, indent=2) + "\n")
    alias = Path("/mnt/tidehunter-kv-comparison")
    alias.mkdir(mode=0o700, exist_ok=False)
    subprocess.run(["mount", "--bind", "/mnt/tidehunter", str(alias)], check=True, timeout=30)
    images = {}
    for name, image in local.IMAGES.items():
        subprocess.run(["docker", "pull", image], check=True, timeout=600)
        observed = subprocess.check_output(["docker", "image", "inspect", image], text=True, timeout=30)
        images[name] = json.loads(observed)
    (root / "image-proof.json").write_text(json.dumps(images, indent=2) + "\n")
    index = instance_launch_index()
    # Monitoring is useful but never an activation prerequisite for raw work.
    # Pulls are bounded, digest-locked, and failures remain explicit evidence.
    monitor_images = cache_monitor_images(stage / "source", index, planned_deadline)
    (root / "monitor-image-proof.json").write_text(json.dumps(monitor_images, indent=2) + "\n")
    ready = {"run_id": plan["run_id"], "instance_id": bootstrap["identity"]["instanceId"],
        "source_sha256": plan["source_sha256"], "binary_sha256": plan["binary_sha256"], "deadline_utc": plan["deadline_utc"]}
    marker = root / "kv-bootstrap-ready.json"
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(json.dumps(ready, indent=2) + "\n")
    temporary.replace(marker)
    ready_key = plan["run_id"] + ("/results/controller-bootstrap.json" if index == 5 else f"/results/cells/startup/storage/{index}/bootstrap.json")
    refresh_key = ready_key.rsplit("/", 1)[0] + "/ssm-agent-profile-refresh.json"
    client.put_object(Bucket=plan["bucket"], Key=refresh_key, Body=json.dumps(ssm_refresh).encode(),
        ExpectedBucketOwner=plan["account"], ServerSideEncryption="AES256")
    client.put_object(Bucket=plan["bucket"], Key=ready_key, Body=json.dumps(ready).encode(),
        ExpectedBucketOwner=plan["account"], ServerSideEncryption="AES256")
    if index != 5:
        return
    # The operator precreates pending before EC2, then uses an ETag CAS to
    # publish ready only after exact profiles, policies and private IPs are bound.
    config_path = root / "controller.json"
    deadline = datetime.fromisoformat(plan["deadline_utc"]).timestamp()
    for _ in range(900):
        if time.time() >= deadline - 600:
            raise TimeoutError("controller config never arrived before export reserve")
        response = client.get_object(Bucket=plan["bucket"], Key=plan["run_id"] + "/inputs/controller.json",
                                     ExpectedBucketOwner=plan["account"])
        with response["Body"] as body:
            raw = body.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            raise ValueError("oversized controller activation record")
        config = activation_config(plan, json.loads(raw))
        if config is None:
            time.sleep(1)
            continue
        if config["fleet"]["coordinator"]["instance_id"] != ready["instance_id"]:
            raise ValueError("ready controller assignment differs from own bootstrap")
        config_path.write_text(json.dumps(config, sort_keys=True, indent=2) + "\n")
        break
    else:
        raise TimeoutError("bounded controller startup config wait expired")
    import cloud_controller
    config = json.loads(config_path.read_text())
    cloud_controller.validate_config(config)
    if config["fleet"]["coordinator"]["instance_id"] != ready["instance_id"] or config["source_sha256"] != plan["source_sha256"]:
        raise ValueError("controller assignment or source binding differs from own bootstrap")
    remaining = int(deadline - time.time())
    unit = "kvcloud-controller-" + plan["run_id"]
    subprocess.run(["systemd-run", "--unit=" + unit, "--service-type=exec", "--property=RuntimeMaxSec=" + str(remaining),
        "--property=LimitMEMLOCK=infinity", "--setenv=PYTHONDONTWRITEBYTECODE=1", "--setenv=FI_EFA_USE_DEVICE_RDMA=1",
        "--setenv=LD_LIBRARY_PATH=" + os.environ["LD_LIBRARY_PATH"],
        "--property=StandardOutput=append:" + str(root / "controller-autostart.log"),
        "--property=StandardError=append:" + str(root / "controller-autostart.log"),
        "/usr/bin/python3", "-B", str(stage / "source/experiments/kv-comparison/cloud_controller.py"),
        "run", "--config", str(config_path)], check=True, timeout=30)
    subprocess.run(["systemctl", "is-active", "--quiet", unit], check=True, timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("render", "install"))
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    if args.action == "render":
        if args.output is None:
            parser.error("render requires --output")
        args.output.write_text(render_userdata(plan))
        print(json.dumps({"user_data_bytes": args.output.stat().st_size}))
    else:
        install(plan)
    return 0


if __name__ == "__main__":
    sys.exit(main())
