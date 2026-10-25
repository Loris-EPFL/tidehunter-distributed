#!/usr/bin/env python3
"""Node-local RF1 external stores. No AWS access or remote process orchestration."""
import argparse
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
import time
import urllib.request

import local

BACKENDS = ("tikv", "cassandra", "foundationdb")
RUN_LABEL = "io.tidehunter.kv-comparison.run"
NODE_LABEL = "io.tidehunter.kv-comparison.node"
BACKEND_LABEL = "io.tidehunter.kv-comparison.backend"
MAX_OUTPUT = 32 * 1024 * 1024


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_fleet(fleet):
    if not isinstance(fleet, dict) or type(fleet.get("schema_version")) is not int or fleet["schema_version"] != 1 or not isinstance(fleet.get("run_id"), str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", fleet["run_id"]):
        raise ValueError("schema_version=1 and safe run_id required")
    qualification = fleet.get("qualification_only", False)
    if not isinstance(qualification, bool) or type(fleet.get("replication_factor")) is not int or fleet["replication_factor"] != 1:
        raise ValueError("boolean qualification_only and replication_factor=1 required")
    if not isinstance(fleet.get("data_root"), str):
        raise ValueError("data_root must be an absolute string")
    root = Path(fleet["data_root"])
    allowed = [Path("/mnt/tidehunter-kv-comparison")]
    if qualification:
        allowed.append(Path("/tmp/tidehunter-kv-comparison"))
    if root.parent not in allowed or root.name != fleet["run_id"] or str(root) != fleet["data_root"]:
        raise ValueError("data_root must be canonical /mnt/tidehunter-kv-comparison/<run_id> (or /tmp for qualification)")
    nodes = fleet.get("nodes", [])
    if not isinstance(nodes, list) or len(nodes) != 5 or any(not isinstance(x, dict) or type(x.get("index")) is not int for x in nodes) or [x["index"] for x in nodes] != list(range(5)):
        raise ValueError("exact ordered node indices 0..4 required")
    hosts = [fleet.get("coordinator", {})] + nodes
    addresses, instances = set(), set()
    for host in hosts:
        if not isinstance(host, dict) or not isinstance(host.get("address"), str):
            raise ValueError("each host must have a string IPv4 address")
        address = ipaddress.IPv4Address(host.get("address", ""))
        private = any(address in ipaddress.IPv4Network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"))
        if not (address.is_loopback if qualification else private):
            raise ValueError("qualification requires loopback; physical deployment requires RFC1918 IPv4")
        identity = host.get("instance_id", "")
        if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", identity) or str(address) in addresses or identity in instances:
            raise ValueError("six distinct valid IPs and machine IDs required")
        addresses.add(str(address))
        instances.add(identity)
    for key, minimum, maximum in (("server_cpus", 2, 256), ("client_cpus", 2, 256),
                                  ("server_memory_mib", 2048, 1048576), ("client_memory_mib", 4096, 1048576)):
        value = fleet.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not minimum <= value <= maximum:
            raise ValueError(f"{key} must be in {minimum}..{maximum}")
        if key.endswith("mib") and int(value) != value:
            raise ValueError("memory MiB must be integral")
    return fleet


def backend_options(fleet, backend, root):
    validate_fleet(fleet)
    pd = fleet["coordinator"]["address"]
    if backend == "tikv":
        return {"pd_endpoints": [pd + ":2379"], "pd_api_endpoints": ["http://" + pd + ":2379"],
                "required_replication_factor": 1, "request_timeout_ms": 5000}
    if backend == "cassandra":
        return {"nodes": [x["address"] + ":9042" for x in fleet["nodes"]], "keyspace": "kv_comparison",
                "required_replication_factor": 1, "consistency": "quorum", "request_timeout_ms": 5000,
                "get_many_concurrency": 16}
    if backend == "foundationdb":
        if not Path(root).is_absolute():
            raise ValueError("cluster file root must be absolute")
        return {"cluster_file": str(Path(root) / "fdb.cluster"), "required_replication_factor": 1,
                "transaction_timeout_ms": 5000}
    raise ValueError("unsupported backend")


def node_config(fleet, backend, index):
    validate_fleet(fleet)
    if backend not in BACKENDS or type(index) is not int or index not in range(-1, 5):
        raise ValueError("unsupported backend or node index")
    if backend == "foundationdb" and fleet["client_memory_mib"] < 8192:
        raise ValueError("FoundationDB requires 6 GiB for control roles and at least 2 GiB for the client")
    coordinator = index == -1
    host = fleet["coordinator"] if coordinator else fleet["nodes"][index]
    address, pd = host["address"], fleet["coordinator"]["address"]
    role = "coordinator" if coordinator else f"node-{index}"
    directory = Path(fleet["data_root"]) / backend / role
    owner = digest({"fleet": fleet, "backend": backend})
    labels = {local.OWNER_LABEL: owner, RUN_LABEL: fleet["run_id"], NODE_LABEL: role, BACKEND_LABEL: backend}
    files, containers = {}, []
    # Keep the 8-GiB host total; reserve memory for FDB's transaction/log roles.
    control_cpu, control_memory = {"tikv": (1, 1024), "cassandra": (0, 0), "foundationdb": (1, 6144)}[backend]
    cpus = control_cpu if coordinator else fleet["server_cpus"]
    memory = control_memory if coordinator else int(fleet["server_memory_mib"])

    def add(service, image, args, target="/data", entrypoint=None, environment=None):
        name = "kvcloud-" + owner[:16] + "-" + service
        data = str(directory / "data")
        argv = ["docker", "run", "--detach", "--name", name, "--network", "host", "--restart", "no",
                "--cpus", str(cpus), "--memory", f"{memory}m", "--memory-swap", f"{memory}m", "--pids-limit", "1024",
                "--log-opt", "max-size=8m", "--log-opt", "max-file=2"]
        if image == "tikv":
            argv += ["--ulimit", f"nofile={local.TIKV_NOFILE}:{local.TIKV_NOFILE}"]
        for key, value in labels.items():
            argv += ["--label", key + "=" + value]
        # Bind rather than -v: Docker must never silently create an absent host path.
        argv += ["--mount", f"type=bind,src={data},dst={target}"]
        for filename in files:
            readonly = "" if filename == "fdb.cluster" else ",readonly"
            argv += ["--mount", f"type=bind,src={directory / filename},dst=/config/{filename}{readonly}"]
        for key, value in (environment or {}).items():
            argv += ["--env", key + "=" + str(value)]
        if entrypoint:
            argv += ["--entrypoint", entrypoint]
        argv += [local.IMAGES[image], *args]
        containers.append({"name": name, "image": local.IMAGES[image], "run_argv": argv,
                           "data_bind": {"source": data, "destination": target}, "cpus": cpus, "memory_mib": memory,
                           "service": service})

    if backend == "tikv":
        if coordinator:
            # PD 8.5.4 scatter's allowLeader requires an actual placement-rule fit.
            score_version = "v1" if fleet.get("qualification_only") else "v2"
            files["pd.toml"] = ('[replication]\nmax-replicas = 1\nenable-placement-rules = true\n'
                                'location-labels = ["host"]\n[schedule]\nmerge-schedule-limit = 0\n'
                                f'region-score-formula-version = "{score_version}"\n')
            add("pd", "pd", ["--name=pd0", "--data-dir=/data", f"--client-urls=http://{pd}:2379",
                f"--advertise-client-urls=http://{pd}:2379", f"--peer-urls=http://{pd}:2380",
                f"--advertise-peer-urls=http://{pd}:2380", f"--initial-cluster=pd0=http://{pd}:2380", "--config=/config/pd.toml"])
        else:
            files["tikv.toml"] = (f'memory-usage-limit = "{int(memory * .75)}MB"\n'
                '[readpool.unified]\nmin-thread-count = 1\nmax-thread-count = 2\n[server]\ngrpc-concurrency = 2\n'
                '[storage]\nreserve-space = "64MB"\nscheduler-worker-pool-size = 2\n[storage.block-cache]\n'
                f'capacity = "{int(memory / 4)}MB"\n[raftstore]\nmax-apply-unpersisted-log-limit = 0\n'
                'store-io-pool-size = 1\nstore-pool-size = 1\napply-pool-size = 1\n'
                '[raft-engine]\nenable = true\ndir = "/data/raft-engine"\n'
                '[rocksdb]\nmax-background-jobs = 2\n[rocksdb.titan]\nenabled = true\n[raftdb]\nmax-background-jobs = 2\n')
            args = ["--addr", address + ":20160", "--advertise-addr", address + ":20160",
                    "--status-addr", address + ":20180", "--pd", pd + ":2379", "--data-dir", "/data",
                    "--config", "/config/tikv.toml", "--labels", "host=" + host["instance_id"]]
            if fleet.get("qualification_only", False):
                # A mostly full shared laptop disk must not make these tiny
                # functional stores ineligible for PD placement. Cloud runs
                # retain the actual instance-store capacity and free-space gate.
                args += ["--capacity", "8GB"]
            add(f"tikv{index}", "tikv", args)
    elif backend == "cassandra" and not coordinator:
        # Explicit stable Murmur3 tokens give five equal RF1 ranges, not random-vnode imbalance.
        token = -(2**63) + index * (2**64 // 5)
        files["cassandra.yaml"] = (f"cluster_name: 'kv-{owner[:24]}'\nnum_tokens: 1\ninitial_token: '{token}'\n"
            "partitioner: org.apache.cassandra.dht.Murmur3Partitioner\n"
            "endpoint_snitch: GossipingPropertyFileSnitch\nauthenticator: AllowAllAuthenticator\nauthorizer: AllowAllAuthorizer\n"
            "seed_provider:\n  - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n    parameters:\n"
            f"      - seeds: '{fleet['nodes'][0]['address']}:7000'\nlisten_address: {address}\nbroadcast_address: {address}\n"
            f"rpc_address: {address}\nbroadcast_rpc_address: {address}\nstart_native_transport: true\nnative_transport_port: 9042\n"
            "storage_port: 7000\ncommitlog_sync: batch\ncommitlog_segment_size: 32MiB\n"
            "data_file_directories:\n  - /var/lib/cassandra/data\ncommitlog_directory: /var/lib/cassandra/commitlog\n"
            "saved_caches_directory: /var/lib/cassandra/saved_caches\nhints_directory: /var/lib/cassandra/hints\n"
            "cdc_raw_directory: /var/lib/cassandra/cdc_raw\n")
        env = {"CASSANDRA_CLUSTER_NAME": "kv-" + owner[:24], "CASSANDRA_LISTEN_ADDRESS": address,
               "CASSANDRA_BROADCAST_ADDRESS": address, "CASSANDRA_RPC_ADDRESS": address,
               "CASSANDRA_BROADCAST_RPC_ADDRESS": address, "CASSANDRA_SEEDS": fleet["nodes"][0]["address"],
               "CASSANDRA_ENDPOINT_SNITCH": "GossipingPropertyFileSnitch", "CASSANDRA_DC": "dc1",
               "CASSANDRA_RACK": host["instance_id"], "CASSANDRA_NUM_TOKENS": "1", "LOCAL_JMX": "yes",
               "MAX_HEAP_SIZE": f"{memory // 2}M", "MAX_DIRECT_MEMORY_SIZE": f"{memory // 8}M",
               "JVM_EXTRA_OPTS": f"-Dcassandra.jmx.local.port={7199 + index} -Dcassandra.available_processors={int(cpus)} "
                                 f"-XX:ActiveProcessorCount={int(cpus)} -XX:ParallelGCThreads=2 -XX:ConcGCThreads=2 "
                                 "-Dcassandra.logdir=/var/lib/cassandra/logs"}
        add(f"cassandra{index}", "cassandra", ["-ec", "cp /config/cassandra.yaml /etc/cassandra/cassandra.yaml; "
            "exec /usr/local/bin/docker-entrypoint.sh cassandra -f"], target="/var/lib/cassandra", entrypoint="/bin/bash", environment=env)
    elif backend == "foundationdb":
        files["fdb.cluster"] = f"kvcomparison:{owner[:32]}@{pd}:4500\n"
        args = ["--cluster-file", "/config/fdb.cluster",
                "--public-address", address + ":4500", "--listen-address", address + ":4500", "--datadir", "/data",
                "--logdir", "/data/logs", "--memory", f"{int(memory * .75)}MiB",
                "--cache-memory", f"{512 if coordinator else memory // 4}MiB",
                "--locality-zoneid", host["instance_id"], "--locality-machineid", host["instance_id"],
                "--locality-dcid", "dc1", "--class", "transaction" if coordinator else "storage"]
        if fleet.get("qualification_only"):
            # A tiny functional fixture may share a nearly full host filesystem.
            # Physical deployments retain FDB's default 5% floor.
            args += ["--knob-min_available_space_ratio", "0.01"]
        add("fdb-control" if coordinator else f"fdb{index}", "foundationdb", args, entrypoint="/usr/bin/fdbserver")
    invocation = [sys.executable, str(Path(__file__).resolve())]
    arguments = ["--fleet-json", json.dumps(fleet, sort_keys=True, separators=(",", ":")),
                 "--backend", backend, "--node-index", str(index)]
    actions = {action: [*invocation, action, *arguments]
               for action in ("prepare", "start", "probe", "stop", "export", "reset-data")}
    actions["bootstrap-probe"] = [*actions["probe"], "--timeout", "300"]
    actions["probe"] += ["--timeout", "300", "--initialize" if coordinator else "--cluster"]
    return {"schema_version": 1, "run_id": fleet["run_id"], "backend": backend, "index": index,
            "address": address, "instance_id": host["instance_id"], "directory": str(directory), "owner": owner,
            "labels": labels, "files": files, "containers": containers, "actions": actions,
            "qualification_only": fleet.get("qualification_only", False),
            "budget": {"cpus": cpus, "memory_mib": memory},
            "client_budget": {"cpus": fleet["client_cpus"] - control_cpu,
                              "memory_mib": fleet["client_memory_mib"] - control_memory} if coordinator else None}


def build_plan(fleet, backend):
    nodes = [node_config(fleet, backend, i) for i in range(-1, 5)]
    return {"schema_version": 1, "backend": backend, "replication_factor": 1,
            "qualification_only": fleet.get("qualification_only", False),
            "physical_storage_hosts": None if fleet.get("qualification_only") else 5,
            "nodes": nodes, "backend_options": backend_options(fleet, backend, nodes[0]["directory"]),
            "lifecycle": {"order": "prepare all; start coordinator; start stores (Cassandra sequential with local probes); coordinator probe --initialize; probe all; export all; stop all; optional reset-data only after every host stopped",
                          "timeout_seconds": 300, "cleanup": "exact label-checked containers only; reset-data is separate and retains configs/exports"},
            "ack_policy": {"tikv": "RF1 raft-kv/Raft Engine, apply-unpersisted limit=0; v8.5.4 source synchronizes raft writes before persistence notification",
                           "cassandra": "commitlog_sync=batch, durable_writes=true, QUORUM at RF1",
                           "foundationdb": "single SSD, transaction log durable commit; serializable transaction semantics"}[backend]}


def command(argv, timeout=30, include_stderr=False):
    result = subprocess.run([str(x) for x in argv], text=True, capture_output=True, timeout=timeout)
    if len(result.stdout) + len(result.stderr) > MAX_OUTPUT:
        raise RuntimeError("command output exceeded evidence bound")
    if result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {argv}: {result.stderr[-4000:]} {result.stdout[-4000:]}")
    return result.stdout + result.stderr if include_stderr else result.stdout


def ensure_image(image):
    """Resolve the exact pinned digest locally before contacting the registry."""
    if image not in local.IMAGES.values():
        raise ValueError("image is not in the admitted digest-pinned set")
    try:
        observed = json.loads(command(["docker", "image", "inspect", image]))
    except RuntimeError:
        command(["docker", "pull", image], timeout=300)
        observed = json.loads(command(["docker", "image", "inspect", image]))
    if len(observed) != 1 or not observed[0].get("Id"):
        raise RuntimeError("pinned image did not resolve to one local image identity")
    return observed[0]


def guarded(path):
    path = Path(path)
    if not path.is_absolute() or path != path.resolve():
        raise ValueError("noncanonical/symlink path refused")
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError("symlink path refused")
    return path


def save(path, value):
    path = guarded(path)
    if path.exists() and not path.is_file():
        raise ValueError("evidence target is not a regular file")
    # Atomic replacement, without following an existing temporary-file symlink.
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
    temporary.replace(path)


def nvme_evidence(directory, qualification):
    existing = Path(directory)
    while not existing.exists():
        existing = existing.parent
    mount = json.loads(command(["findmnt", "--json", "--target", str(existing), "--output", "SOURCE,TARGET,FSTYPE,OPTIONS"]))
    filesystems = mount.get("filesystems", [])
    if len(filesystems) != 1:
        raise RuntimeError("unambiguous data filesystem required")
    source = filesystems[0]["source"].split("[")[0]
    evidence = {"mount": mount, "qualification_only": qualification, "independent_nvme_admitted": False,
                "host_identity": {"hostname": socket.gethostname(),
                                  "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}}
    if qualification:
        return evidence
    if not re.fullmatch(r"/dev/nvme[0-9]+n[0-9]+(?:p[0-9]+)?", source):
        raise RuntimeError("direct instance-store NVMe filesystem required; root/EBS/overlay/RAID not admitted")
    device = re.sub(r"p[0-9]+$", "", source)
    blocks = json.loads(command(["lsblk", "--json", "--nodeps", "--paths", "--output", "NAME,TYPE,MODEL,SERIAL", device]))
    devices = blocks.get("blockdevices", [])
    if len(devices) != 1 or "instance storage" not in (devices[0].get("model") or "").lower():
        raise RuntimeError("NVMe is not identified as EC2 instance storage (EBS is not admitted)")
    if "rw" not in filesystems[0].get("options", "").split(","):
        raise RuntimeError("data mount is not writable")
    evidence.update({"block_devices": blocks, "independent_nvme_admitted": True})
    return evidence


def require_state(config):
    directory = guarded(config["directory"])
    role = "coordinator" if config["index"] == -1 else f"node-{config['index']}"
    roots = [Path("/mnt/tidehunter-kv-comparison")]
    if config["qualification_only"]:
        roots.append(Path("/tmp/tidehunter-kv-comparison"))
    if (config["backend"] not in BACKENDS or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", config["run_id"]) or
            directory not in [root / config["run_id"] / config["backend"] / role for root in roots]):
        raise ValueError("owned directory escapes admitted run root")
    saved = json.loads(guarded(directory / "owner.json").read_text())
    if saved != config:
        raise ValueError("exact fleet/config ownership receipt mismatch")
    guarded(directory / "data")
    for filename in config["files"]:
        guarded(directory / filename)
    return directory


def prepare(config):
    directory = guarded(config["directory"])
    if (directory / "owner.json").exists():
        require_state(config)
        return {"reused": True, "nvme": nvme_evidence(directory, config["qualification_only"])}
    if directory.exists() and any(directory.iterdir()):
        raise ValueError("refusing nonempty unowned directory")
    # Prove the requested address belongs to this host, without listening on a service port.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((config["address"], 0))
    evidence = nvme_evidence(directory, config["qualification_only"])
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "data").mkdir()
    if config["backend"] == "foundationdb":
        (directory / "data" / "logs").mkdir()
    for filename, content in config["files"].items():
        (directory / filename).write_text(content)
    save(directory / "owner.json", config)
    save(directory / "nvme.json", evidence)
    return {"prepared": True, "nvme": evidence}


def inspect_container(config, container, required=True):
    ids = command(["docker", "ps", "-aq", "--filter", "name=^/" + container["name"] + "$"]).split()
    if not ids:
        if required:
            raise RuntimeError("owned container absent: " + container["name"])
        return None
    if len(ids) != 1:
        raise RuntimeError("ambiguous container identity")
    item = json.loads(command(["docker", "inspect", ids[0]]))[0]
    if item["Name"] != "/" + container["name"] or any((item["Config"].get("Labels") or {}).get(k) != v for k, v in config["labels"].items()):
        raise RuntimeError("foreign container labels/name; refusing action")
    return item


def runtime_evidence(config):
    evidence = []
    for container in config["containers"]:
        item = inspect_container(config, container)
        if not item["State"]["Running"]:
            raise RuntimeError("container not running: " + container["name"])
        host = item["HostConfig"]
        if (host.get("NanoCpus") != int(container["cpus"] * 1e9) or host.get("Memory") != container["memory_mib"] * 1024**2 or
                host.get("MemorySwap") != container["memory_mib"] * 1024**2):
            raise RuntimeError("effective CPU/memory cgroup cap mismatch")
        mounts = item.get("Mounts", [])
        bind = container["data_bind"]
        if not any(x.get("Type") == "bind" and x.get("Source") == bind["source"] and x.get("Destination") == bind["destination"] and x.get("RW") for x in mounts):
            raise RuntimeError("effective writable NVMe data bind mismatch")
        image = json.loads(command(["docker", "image", "inspect", container["image"]]))[0]
        if item["Image"] != image["Id"]:
            raise RuntimeError("running image differs from digest-pinned image")
        evidence.append({"id": item["Id"], "image_id": item["Image"], "repo_digests": image.get("RepoDigests"),
                         "state": item["State"], "cgroup_limits": host, "mounts": mounts,
                         "top": command(["docker", "top", item["Id"], "-eo", "pid,ppid,pcpu,rss,args"]),
                         "stats": command(["docker", "stats", "--no-stream", "--format", "{{json .}}", item["Id"]])})
    return evidence


def start(config):
    directory = require_state(config)
    nvme_evidence(directory, config["qualification_only"])
    if config["backend"] == "foundationdb":
        # fdbserver opens trace files but does not create --logdir. Reset
        # deliberately empties data/, so recreate it on every fresh start.
        guarded(directory / "data" / "logs").mkdir(exist_ok=True)
    if config["backend"] == "cassandra" and config["index"] == -1:
        ensure_image(local.IMAGES["cassandra"])
    for container in config["containers"]:
        existing = inspect_container(config, container, required=False)
        if existing:
            if not existing["State"]["Running"]:
                raise RuntimeError("owned container stopped; explicitly stop before restart")
            continue
        ensure_image(container["image"])
        command(container["run_argv"], timeout=60)
    return {"started": True, "runtime": runtime_evidence(config)}


def execute(config, *args):
    container = config["containers"][0]
    item = inspect_container(config, container)
    if not item["State"]["Running"]:
        raise RuntimeError("owned container is not running")
    return command(["docker", "exec", item["Id"], *args], timeout=20)


def http(fleet, path, body=None, address=None):
    base = "http://" + (address or fleet["coordinator"]["address"]) + (":20180" if address else ":2379/pd/api/v1")
    request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
    # Do not route private/loopback administrative traffic through ambient HTTP proxies.
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=10) as response:
        content = response.read(MAX_OUTPUT + 1)
    if len(content) > MAX_OUTPUT:
        raise RuntimeError("HTTP evidence exceeded bound")
    return json.loads(content)


def json_rows(text):
    return [json.loads(line.strip()) for line in text.splitlines() if line.strip().startswith("{")]


def cql(fleet, config, query):
    address = config["address"] if config["index"] >= 0 else fleet["nodes"][0]["address"]
    args = ["cqlsh", address, "9042", "--connect-timeout=5", "--request-timeout=10", "-e", query]
    if config["containers"]:
        return execute(config, *args)
    # The coordinator has no resident Cassandra server; this bounded owned tool uses its client budget.
    name = "kvcloud-" + config["owner"][:16] + "-cql-tool"
    argv = ["docker", "run", "--rm", "--name", name, "--network", "host", "--cpus", "1", "--memory", "512m", "--pids-limit", "128"]
    for key, value in config["labels"].items():
        argv += ["--label", key + "=" + value]
    try:
        return command([*argv, "--entrypoint", "cqlsh", local.IMAGES["cassandra"], *args[1:]], timeout=20)
    except subprocess.TimeoutExpired:
        item = inspect_container(config, {"name": name}, required=False)
        if item:
            command(["docker", "rm", "--force", item["Id"]])
        raise


def admit_tikv(fleet, stores, regions, replication, configs):
    up = [x for x in stores.get("stores", []) if x["store"].get("state_name") == "Up"]
    expected = {x["address"] + ":20160" for x in fleet["nodes"]}
    if len(up) != 5 or {x["store"]["address"] for x in up} != expected or replication.get("max-replicas") != 1 or replication.get("enable-placement-rules") != "true":
        raise RuntimeError("TiKV observed five-store/RF1/placement contract mismatch")
    ids = {x["store"]["id"] for x in up}
    if any(not x["store"].get("version", "").startswith("8.5.4") for x in up):
        raise RuntimeError("TiKV actual version mismatch")
    leaders, peers = set(), set()
    listing = regions.get("regions", [])
    for region in listing:
        members = region.get("peers", [])
        leader = region.get("leader", {}).get("store_id")
        if len(members) != 1 or leader != members[0].get("store_id") or leader not in ids or region.get("pending_peers") or region.get("down_peers"):
            raise RuntimeError("TiKV region RF1/healthy leader mismatch")
        leaders.add(leader)
        peers.add(members[0]["store_id"])
    if len(listing) < 20 or leaders != ids or peers != ids:
        raise RuntimeError("TiKV split/scatter has not given every storage host actual region/leader coverage")
    if len(configs) != 5:
        raise RuntimeError("TiKV effective configurations missing")
    acknowledgements = [admit_tikv_ack(settings) for settings in configs]
    return {"region_count": len(listing), "leader_store_ids": sorted(leaders), "peer_store_ids": sorted(peers),
            "acknowledgements": acknowledgements}


def admit_tikv_rules(rules):
    if (not isinstance(rules, list) or len(rules) != 1 or rules[0].get("group_id") != "pd" or
            rules[0].get("id") != "default" or rules[0].get("start_key") != "" or rules[0].get("end_key") != "" or
            rules[0].get("count") != 1 or rules[0].get("role") != "voter" or rules[0].get("is_witness", False) or
            rules[0].get("label_constraints") or rules[0].get("location_labels") != ["host"]):
        raise RuntimeError("TiKV effective placement rules must be exactly one whole-keyspace RF1 voter rule")
    return rules[0]


def admit_tikv_ack(settings):
    raftstore, engine, storage = settings.get("raftstore", {}), settings.get("raft-engine", {}), settings.get("storage", {})
    if (type(raftstore.get("max-apply-unpersisted-log-limit")) is not int or raftstore["max-apply-unpersisted-log-limit"] != 0 or
            type(raftstore.get("store-io-pool-size")) is not int or raftstore["store-io-pool-size"] != 1 or
            engine.get("enable") is not True or engine.get("dir") != "/data/raft-engine" or
            storage.get("engine") != "raft-kv" or type(storage.get("api-version")) is not int or
            storage["api-version"] != 1 or storage.get("data-dir") != "/data"):
        raise RuntimeError("TiKV actual raft engine/persist-before-apply controls or data paths do not match durable RF1 contract")
    return {"max_apply_unpersisted_log_limit": 0, "store_io_pool_size": 1, "raft_engine": True,
            "sync_evidence": "TiKV v8.5.4 async_io/write.rs consume_and_shrink(sync=true) before callbacks",
            "evidence_basis": "observed runtime configuration plus version-pinned source; not a power-loss test"}


def admit_fdb(fleet, status):
    cluster = status.get("cluster", {})
    config = cluster.get("configuration", {})
    if not cluster.get("database_available") or not cluster.get("full_replication") or config.get("redundancy_mode") != "single":
        raise RuntimeError("FDB unavailable/not fully replicated/not single")
    if config.get("storage_replicas", 1) != 1 or config.get("log_replicas", 1) != 1:
        raise RuntimeError("FDB effective custom replication differs from RF1")
    expected = {x["instance_id"]: x["address"] + ":4500" for x in [fleet["coordinator"], *fleet["nodes"]]}
    processes = cluster.get("processes", {})
    if len(processes) != 6:
        raise RuntimeError("FDB must report five storage processes plus coordinator control process")
    seen, storage, roles = set(), set(), set()
    for process in processes.values():
        machine = process.get("locality", {}).get("machineid")
        if machine in seen or machine not in expected or process.get("address") != expected[machine] or process.get("version") != "7.3.77":
            raise RuntimeError("FDB observed identity/address/version mismatch")
        if any(message.get("name") in ("trace_log_could_not_create_file", "file_open_error")
               for message in process.get("messages", [])):
            raise RuntimeError("FDB process reports file/trace creation errors on admitted data paths")
        seen.add(machine)
        recruited = {x.get("role") for x in process.get("roles", [])}
        roles |= recruited
        if "storage" in recruited:
            storage.add(machine)
    if storage != {x["instance_id"] for x in fleet["nodes"]} or not {"log", "commit_proxy", "grv_proxy", "resolver", "master", "cluster_controller"} <= roles:
        raise RuntimeError("FDB actual recruited storage/control roles do not cover requested topology")
    return {"storage_machine_ids": sorted(storage), "actual_roles": sorted(roles)}


def initialize(fleet, config):
    if config["index"] != -1:
        raise ValueError("initialize is coordinator-only")
    directory = require_state(config)
    marker = guarded(directory / "initialization.json")
    if marker.exists():
        return json.loads(marker.read_text())
    backend = config["backend"]
    evidence = {"backend": backend, "observed_at_unix": time.time()}
    if backend == "tikv":
        stores = http(fleet, "/stores")
        if len([x for x in stores.get("stores", []) if x["store"].get("state_name") == "Up"]) != 5:
            raise RuntimeError("all five TiKV stores must be Up before splitting")
        # Comparator keys are uniformly hashed 32-byte RawKV keys (API v1, no transactional prefix).
        keys = [f"{i * 256 // 20:02X}" for i in range(1, 20)]
        regions = http(fleet, "/regions")
        boundaries = {row.get("start_key", "").upper() for row in regions.get("regions", [])}
        missing = [key for key in keys if key not in boundaries]
        evidence["split_keys_hex"] = keys
        evidence["requested_missing_splits"] = missing
        if missing:
            evidence["split_response"] = http(fleet, "/regions/split", {"split_keys": missing, "retry_limit": 3})
            regions = http(fleet, "/regions")
        evidence["regions_after_split"] = regions
        save(directory / "initialization-attempt.json", evidence)
        boundaries = {row.get("start_key", "").upper() for row in regions.get("regions", [])}
        if not set(keys) <= boundaries:
            raise RuntimeError("PD has not yet published every requested RawKV split boundary")
        evidence["scatter_response"] = http(fleet, "/regions/scatter", {"regions_id": [x["id"] for x in regions.get("regions", [])],
                                                                          "group": config["owner"][:24], "retry_limit": 3})
        observed = http(fleet, "/regions")
        evidence["regions_after_scatter"] = observed
        save(directory / "initialization-attempt.json", evidence)
        # Split/scatter acknowledgements can report zero for already completed
        # work. Admit the actual desired state, not a non-idempotent percentage.
        store_ids = {row["store"]["id"] for row in stores["stores"] if row["store"].get("state_name") == "Up"}
        if {row.get("leader", {}).get("store_id") for row in observed.get("regions", [])} != store_ids:
            raise RuntimeError("PD scatter has not yet placed leaders on all five stores")
    elif backend == "cassandra":
        evidence["create_keyspace"] = cql(fleet, config, "CREATE KEYSPACE IF NOT EXISTS kv_comparison WITH replication = "
            "{'class':'SimpleStrategy','replication_factor':1} AND durable_writes = true;")
    else:
        evidence["configure"] = execute(config, "/usr/bin/fdbcli", "-C", "/config/fdb.cluster", "--timeout", "10", "--exec",
            "configure new single ssd logs=1 commit_proxies=1 grv_proxies=1 resolvers=1")
        if "ERROR" in evidence["configure"]:
            raise RuntimeError("FDB configure rejected: " + evidence["configure"])
    save(marker, evidence)
    return evidence


def probe_once(fleet, config):
    directory = require_state(config)
    proof = {"observed_at_unix": time.time(), "qualification_only": config["qualification_only"],
             "nvme": nvme_evidence(directory, config["qualification_only"]), "runtime": runtime_evidence(config)}
    backend = config["backend"]
    if backend == "tikv":
        if config["index"] >= 0:
            settings = http(fleet, "/config", address=config["address"])
            proof["acknowledgement"] = admit_tikv_ack(settings)
            proof["effective_config"] = settings
        else:
            stores, regions, replication = http(fleet, "/stores"), http(fleet, "/regions"), http(fleet, "/config/replicate")
            configs = [http(fleet, "/config", address=x["address"]) for x in fleet["nodes"]]
            rules = http(fleet, "/config/rules")
            schedule = http(fleet, "/config/schedule")
            proof.update({"stores": stores, "regions": regions, "replication": replication, "effective_configs": configs,
                          "placement_rules": rules, "schedule": schedule, "pd_version": http(fleet, "/version")})
            save(directory / "probe-attempt.json", proof)
            admit_tikv_rules(rules)
            if schedule.get("region-score-formula-version") != ("v1" if config["qualification_only"] else "v2"):
                raise RuntimeError("TiKV observed scheduling formula does not match the declared physical/qualification layout")
            proof["coverage"] = admit_tikv(fleet, stores, regions, replication, configs)
    elif backend == "cassandra":
        rows = json_rows(cql(fleet, config, "SELECT JSON release_version, data_center, rack, host_id, tokens FROM system.local;"))
        if len(rows) != 1 or rows[0].get("release_version") != "5.0.9" or rows[0].get("data_center") != "dc1":
            raise RuntimeError("Cassandra local CQL version/DC mismatch")
        proof["system_local"] = rows[0]
        local_index = max(0, config["index"])
        expected_node = fleet["nodes"][local_index]
        expected_token = str(-(2**63) + local_index * (2**64 // 5))
        if rows[0].get("rack") != expected_node["instance_id"] or rows[0].get("tokens") != [expected_token] or not rows[0].get("host_id"):
            raise RuntimeError("Cassandra observed rack/token/host identity mismatch")
        if config["index"] >= 0:
            status = execute(config, "nodetool", "-p", str(7199 + config["index"]), "status")
            yaml = execute(config, "cat", "/etc/cassandra/cassandra.yaml")
            settings = dict(re.findall(r"^([a-z_]+):\s*([^#\n]+)", yaml, re.MULTILINE))
            names = ("commitlog_sync", "data_file_directories", "commitlog_directory", "hints_directory",
                     "saved_caches_directory", "cdc_raw_directory")
            effective = json_rows(cql(fleet, config, "SELECT JSON name, value FROM system_views.settings WHERE name IN (" +
                                      ",".join("'" + name + "'" for name in names) + ");"))
            values = {row["name"]: row.get("value") for row in effective}
            if values.get("commitlog_sync") not in ("batch", "group") or settings.get("commitlog_sync", "").strip() != values["commitlog_sync"]:
                raise RuntimeError("Cassandra running commitlog acknowledgement is not verified batch/group")
            expected_paths = {"commitlog_directory": "/var/lib/cassandra/commitlog", "hints_directory": "/var/lib/cassandra/hints",
                              "saved_caches_directory": "/var/lib/cassandra/saved_caches", "cdc_raw_directory": "/var/lib/cassandra/cdc_raw"}
            if any(values.get(name) != path for name, path in expected_paths.items()) or str(values.get("data_file_directories")).replace('"', "").replace(" ", "") != "[/var/lib/cassandra/data]":
                raise RuntimeError("Cassandra actual persistent directories escape admitted NVMe bind")
            proof.update({"ring": status, "effective_yaml": yaml, "effective_settings": effective,
                          "commitlog_sync": values["commitlog_sync"]})
            # Local readiness is suitable for sequential bootstrap; final admission requires --cluster.
            proof["ring_un"] = re.findall(r"^UN\s+(\S+)", status, re.MULTILINE)
            proof["ring_members"] = re.findall(r"^([UD][NLJM])\s+(\S+)", status, re.MULTILINE)
        else:
            peers = json_rows(cql(fleet, config, "SELECT JSON peer, native_address, host_id, data_center, rack FROM system.peers_v2;"))
            if {x.get("peer") for x in peers} != {x["address"] for x in fleet["nodes"][1:]}:
                raise RuntimeError("Cassandra CQL peers do not cover all five storage nodes")
            expected_peers = {x["address"]: x for x in fleet["nodes"][1:]}
            if len(peers) != 4 or any(x.get("native_address") != x["peer"] or x.get("data_center") != "dc1" or
                                     x.get("rack") != expected_peers[x["peer"]]["instance_id"] for x in peers):
                raise RuntimeError("Cassandra effective peer address/DC/rack mismatch")
            host_ids = [proof["system_local"]["host_id"], *(x.get("host_id") for x in peers)]
            if None in host_ids or len(set(host_ids)) != 5:
                raise RuntimeError("Cassandra host IDs are not five distinct actual identities")
            rows = json_rows(cql(fleet, config, "SELECT JSON replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name='kv_comparison';"))
            if len(rows) != 1 or rows[0].get("durable_writes") is not True or rows[0].get("replication", {}).get("replication_factor") != "1" or not rows[0]["replication"].get("class", "").endswith("SimpleStrategy"):
                raise RuntimeError("Cassandra effective RF1/durable keyspace mismatch")
            proof.update({"peers": peers, "keyspace": rows[0], "storage_node_receipts_required": True})
    else:
        status = json.loads(execute(config, "/usr/bin/fdbcli", "-C", "/config/fdb.cluster", "--timeout", "10", "--exec", "status json"))
        proof["status"] = status
        container = config["containers"][0]
        item = inspect_container(config, container)
        expected_command = container["run_argv"][container["run_argv"].index(container["image"]) + 1:]
        actual_command = item["Config"].get("Cmd")
        if actual_command != expected_command:
            raise RuntimeError("FDB actual server argv differs from the admitted physical/qualification knob policy")
        filesystem = os.statvfs(directory / "data")
        total, available = filesystem.f_blocks * filesystem.f_frsize, filesystem.f_bavail * filesystem.f_frsize
        proof["disk_admission"] = {
            "observed_server_argv": actual_command,
            "min_available_space_ratio": 0.01 if config["qualification_only"] else 0.05,
            "min_available_space_ratio_basis": "explicit qualification-only argv override" if config["qualification_only"] else "unmodified pinned server default",
            "absolute_min_available_space_bytes": 100000000, "default_safety_buffer_ratio": 0.01,
            "local_filesystem_total_bytes": total, "local_filesystem_available_bytes": available,
            "local_filesystem_available_ratio": available / total if total else None,
            "ratekeeper_source": "FoundationDB 7.3.77 Ratekeeper.actor.cpp reason 7: storage_server_min_free_space_ratio"}
        save(directory / "probe-attempt.json", proof)
        proof["coverage"] = admit_fdb(fleet, status)
    return proof


def probe(fleet, config, timeout=120, initialize_cluster=False, cluster=False):
    if type(timeout) is not int or not 1 <= timeout <= 900:
        raise ValueError("probe timeout must be 1..900 seconds")
    deadline, error = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            initialization = initialize(fleet, config) if initialize_cluster else None
            proof = probe_once(fleet, config)
            if cluster and config["backend"] == "cassandra" and config["index"] >= 0:
                if (set(proof["ring_un"]) != {x["address"] for x in fleet["nodes"]} or len(proof["ring_un"]) != 5 or
                        len(proof["ring_members"]) != 5 or any(state != "UN" for state, _ in proof["ring_members"])):
                    raise RuntimeError("Cassandra exact five-node UN ring not reached")
            proof["initialization"] = initialization
            proof["scope"] = "cluster" if cluster or config["index"] == -1 else "node"
            if config["backend"] == "cassandra" and config["index"] == -1:
                proof["scope"] = "cluster_metadata_requires_all_storage_cluster_receipts"
            save(Path(config["directory"]) / "deployment.json", proof)
            return {"status": "ready", "proof": proof}
        except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
            error = str(exc)
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"readiness deadline exceeded: {error}")


def reset_container(config):
    role = config["labels"][NODE_LABEL]
    return {"name": "kvcloud-" + config["owner"][:16] + "-" + role + "-reset"}


def reset_data(config):
    """Empty one admitted data bind; caller must have stopped the entire fleet."""
    directory = require_state(config)
    data = guarded(directory / "data")
    marker = guarded(directory / "initialization.json")
    if not data.is_dir() or (marker.exists() and not marker.is_file()):
        raise ValueError("reset requires the owned data directory and a regular initialization marker")
    owned = command(["docker", "ps", "-aq", "--filter", "label=" + local.OWNER_LABEL + "=" + config["owner"]]).split()
    if owned:
        raise RuntimeError("reset requires all owned backend containers absent, including stopped containers")
    # Refuse data borrowed by any other local container, including a parent bind.
    ids = command(["docker", "ps", "-aq"]).split()
    for item in json.loads(command(["docker", "inspect", *ids])) if ids else []:
        for mount in item.get("Mounts", []):
            source = Path(mount["Source"]).resolve() if mount.get("Source") else None
            if source is not None and (source == data or data in source.parents or source in data.parents):
                raise RuntimeError("reset data is mounted by another container; refusing deletion")
    # find -xdev does not follow symlinks, but refuse nested mounts rather than
    # partially emptying a tree containing an unexpected filesystem.
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        field = line.split()[4]
        mount = Path(re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), field))
        if mount == data or data in mount.parents:
            raise RuntimeError("reset data contains a mountpoint; refusing deletion")
    nvme = nvme_evidence(data, config["qualification_only"])
    # The pinned image's find handles files owned by container root/Cassandra.
    # The only writable host bind is this exact owned data path.
    ensure_image(local.IMAGES["cassandra"])
    if command(["docker", "ps", "-aq", "--filter", "label=" + local.OWNER_LABEL + "=" + config["owner"]]).split():
        raise RuntimeError("owned backend appeared before reset; refusing deletion")
    tool = reset_container(config)
    argv = ["docker", "run", "--rm", "--name", tool["name"], "--network", "none", "--read-only", "--user", "0",
            "--cpus", "1", "--memory", "256m", "--memory-swap", "256m", "--pids-limit", "64",
            "--cap-drop", "ALL", "--cap-add", "DAC_OVERRIDE", "--cap-add", "FOWNER"]
    for key, value in config["labels"].items():
        argv += ["--label", key + "=" + value]
    argv += ["--mount", f"type=bind,src={data},dst=/data", "--entrypoint", "/usr/bin/find",
             local.IMAGES["cassandra"], "/data", "-mindepth", "1", "-xdev", "-delete"]
    save(directory / "reset.json", {"status": "resetting", "reset_at_unix": time.time(),
                                    "data_directory": str(data), "prior_deployment_receipt_invalidated": True})
    marker.unlink(missing_ok=True)
    try:
        command(argv, timeout=300)
    finally:
        item = inspect_container(config, tool, required=False)
        if item:
            command(["docker", "rm", "--force", item["Id"]])
    if any(data.iterdir()):
        raise RuntimeError("reset left data entries; no fresh deployment admitted")
    result = {"status": "reset", "reset_at_unix": time.time(), "data_directory": str(data), "data_empty": True,
              "initialization_removed": True, "config_and_exports_retained": True,
              "prior_deployment_receipt_invalidated": True, "nvme": nvme,
              "maintenance_budget": {"cpus": 1, "memory_mib": 256}}
    save(directory / "reset.json", result)
    return result


def stop(config):
    require_state(config)
    removed = []
    for container in config["containers"]:
        item = inspect_container(config, container, required=False)
        if item:
            command(["docker", "stop", "--time", "20", item["Id"]], timeout=30)
            # Reinspect immediately before removal; never remove volumes or data trees.
            item = inspect_container(config, container)
            command(["docker", "rm", item["Id"]])
            removed.append(item["Id"])
    tools = [reset_container(config)]
    if config["backend"] == "cassandra" and config["index"] == -1:
        tools.append({"name": "kvcloud-" + config["owner"][:16] + "-cql-tool"})
    for tool in tools:
        item = inspect_container(config, tool, required=False)
        if item:
            command(["docker", "rm", "--force", item["Id"]])
            removed.append(item["Id"])
    result = {"removed_container_ids": removed, "data_retained": True}
    save(Path(config["directory"]) / "stop.json", result)
    return result


def export(config):
    directory = require_state(config)
    output = directory / "export"
    guarded(output)
    output.mkdir(exist_ok=True)
    if any(x.is_symlink() for x in output.iterdir()):
        raise ValueError("symlink in evidence output refused")
    evidence = {"config": config, "exported_at_unix": time.time(), "containers": [], "receipts": {}}
    for name in ("nvme.json", "initialization-attempt.json", "initialization.json", "probe-attempt.json", "deployment.json", "stop.json", "reset.json"):
        path = guarded(directory / name)
        if path.exists():
            evidence["receipts"][name] = json.loads(path.read_text())
    for container in config["containers"]:
        item = inspect_container(config, container, required=False)
        if item:
            evidence["containers"].append({"id": item["Id"], "image_id": item["Image"], "state": item["State"],
                                           "host_config": item["HostConfig"], "mounts": item.get("Mounts"),
                                           "logs": command(["docker", "logs", "--tail", "1000", item["Id"]], include_stderr=True)})
    save(output / "evidence.json", evidence)
    return {"export_path": str(output / "evidence.json"), "evidence": evidence}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("plan", "backend-options", "prepare", "start", "probe", "stop", "export", "reset-data"))
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fleet", type=Path)
    source.add_argument("--fleet-json", help="inline fleet object for self-contained plan action argv")
    parser.add_argument("--backend", choices=BACKENDS, required=True)
    parser.add_argument("--node-index", type=int, default=-1)
    parser.add_argument("--root", type=Path, help="backend-options cluster-file directory (defaults to coordinator directory)")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--initialize", action="store_true", help="coordinator probe only, after all storage nodes started")
    parser.add_argument("--cluster", action="store_true", help="require full ring for Cassandra storage probes")
    args = parser.parse_args(argv)
    fleet = json.loads(args.fleet.read_text() if args.fleet else args.fleet_json)
    config = node_config(fleet, args.backend, args.node_index)
    if args.action == "plan":
        result = build_plan(fleet, args.backend)
    elif args.action == "backend-options":
        root = args.root or node_config(fleet, args.backend, -1)["directory"]
        result = backend_options(fleet, args.backend, root)
    elif args.action == "probe":
        result = probe(fleet, config, args.timeout, args.initialize, args.cluster)
    else:
        result = {"prepare": prepare, "start": start, "stop": stop, "export": export, "reset-data": reset_data}[args.action](config)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        sys.exit(1)
