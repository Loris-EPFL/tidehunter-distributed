#!/usr/bin/env python3
"""Exact-owned, Linux-only local comparator lifecycle; no cloud dependencies."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import uuid

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
OWNER_LABEL = "io.tidehunter.kv-comparison.owner"
IMAGES = {
    "pd": "pingcap/pd:v8.5.4@sha256:672f499607a9f378f01ed0f78b2aa073d689b67ca1a820d58176660191db769b",
    "tikv": "pingcap/tikv:v8.5.4@sha256:12f547570dd2bfe56f20fd349c6b80335520a2ff7b0fd82c98071a507a9d202c",
    "cassandra": "library/cassandra:5.0.9@sha256:3ea9333712c5692f3fe91d6c4dcc262df9e36338c4fdd1bc00a5e53f1137a1a0",
    "foundationdb": "foundationdb/foundationdb:7.3.77@sha256:559b178a137bbe09272ad6e3752531aa04e2ddacab8ccfa4ffa86467db033bea",
    "valkey_tcp": "valkey/valkey:9.0.7@sha256:7e1deda01b64ece3167d370ab5be3452d29734feec22cbcbc69b3a64f6859d6d",
}
# TiKV 8.5.4 rejects the hosted runner's inherited 65,536-descriptor limit.
TIKV_NOFILE = 1_048_576


def dump(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def command(args, timeout=30, check=True):
    result = subprocess.run([str(x) for x in args], capture_output=True, text=True,
                            timeout=timeout, cwd=ROOT)
    if len(result.stdout) + len(result.stderr) > 32 * 1024 * 1024:
        raise RuntimeError("command output exceeded evidence bound")
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args}\n{result.stderr[-8000:]}\n{result.stdout[-8000:]}")
    return result.stdout if check else result.stdout + result.stderr


def compose(state, *args, timeout=60):
    return command(["docker", "compose", "--project-name", state["project"], "--file",
                    state["compose_file"], "--profile", state["backend"], *args], timeout)


def load(directory):
    directory = Path(directory).resolve()
    state = json.loads((directory / "owner.json").read_text())
    if state["directory"] != str(directory) or not re.fullmatch(r"[a-f0-9]{32}", state["owner"]):
        raise ValueError("state owner/path mismatch; refusing lifecycle action")
    if state["project"] != "kv-" + state["owner"][:16] or state["compose_file"] != str(directory / "compose.json"):
        raise ValueError("state project/compose identity mismatch; refusing action")
    if state["backend"] not in ("tikv", "cassandra", "foundationdb", "valkey_tcp") or state["layout"] not in ("distributed", "qualification"):
        raise ValueError("unsupported saved deployment contract")
    return state


def valkey_server_args(address, port, bus_port, cluster, maxmemory_mib):
    # Keep Valkey's snapshot/rewrite maintenance defaults; never substitute a cache.
    args = ["valkey-server", "--bind", address, "--port", str(port), "--protected-mode", "no",
            "--dir", "/data", "--appendonly", "yes", "--appendfsync", "always",
            "--no-appendfsync-on-rewrite", "no", "--maxmemory-policy", "noeviction",
            "--maxmemory", str(maxmemory_mib * 1024**2)]
    if cluster:
        args += ["--cluster-enabled", "yes", "--cluster-config-file", "/data/nodes.conf",
                 "--cluster-node-timeout", "5000", "--cluster-announce-ip", address,
                 "--cluster-announce-port", str(port), "--cluster-port", str(bus_port),
                 "--cluster-announce-bus-port", str(bus_port)]
    return args


def valkey_info(text):
    result = {}
    for line in text.splitlines():
        if line and not line.startswith("#") and ":" in line:
            name, value = line.split(":", 1)
            if name in result:
                raise RuntimeError("duplicate Valkey INFO field")
            result[name] = value
    return result


def valkey_cli(run, endpoint, *args, json_output=False):
    host, port = endpoint.rsplit(":", 1)
    text = run(endpoint, "valkey-cli", "--json" if json_output else "--raw",
               "-h", host, "-p", port, *args)
    return json.loads(text) if json_output else text


def valkey_evidence(run, endpoints, cluster):
    nodes = []
    for endpoint in endpoints:
        settings = valkey_cli(run, endpoint, "CONFIG", "GET", "*", json_output=True)
        if isinstance(settings, list) and len(settings) % 2 == 0:
            settings = dict(zip(settings[::2], settings[1::2]))
        if not isinstance(settings, dict):
            raise RuntimeError("Valkey CONFIG GET did not return effective settings")
        node = {"endpoint": endpoint, "effective_config": settings}
        for section in ("server", "persistence", "replication", "cluster"):
            text = valkey_cli(run, endpoint, "INFO", section)
            node["info_" + section] = valkey_info(text)
        if cluster:
            node["cluster_info"] = valkey_info(valkey_cli(run, endpoint, "CLUSTER", "INFO"))
            node["cluster_nodes"] = valkey_cli(run, endpoint, "CLUSTER", "NODES")
            node["cluster_links"] = valkey_cli(run, endpoint, "CLUSTER", "LINKS", json_output=True)
        nodes.append(node)
    return {"nodes": nodes, "transport": "TCP", "authentication": "unauthenticated private network only",
            "read_path": "in-memory; not an NVMe cold-read or value-WAL equivalent",
            "image": IMAGES["valkey_tcp"]}


def admit_valkey(nodes, endpoints, cluster, complete=True):
    expected = set(endpoints)
    if len(nodes) != len(expected) or len(expected) != (5 if cluster and complete else 1) or {x["endpoint"] for x in nodes} != expected:
        raise RuntimeError("Valkey requested primary endpoint inventory mismatch")
    views, identities = [], {}
    for node in nodes:
        config, server = node["effective_config"], node["info_server"]
        persistence, replication = node["info_persistence"], node["info_replication"]
        required = {"appendonly": "yes", "appendfsync": "always", "no-appendfsync-on-rewrite": "no",
                    "maxmemory-policy": "noeviction", "dir": "/data", "cluster-enabled": "yes" if cluster else "no"}
        if any(config.get(k) != v for k, v in required.items()):
            raise RuntimeError("Valkey effective durable/no-eviction/cluster configuration mismatch")
        for field in ("appenddirname", "appendfilename"):
            value = config.get(field, "")
            if not value or value in (".", "..") or "/" in value:
                raise RuntimeError("Valkey AOF path escapes owned data bind")
        mode = server.get("server_mode", server.get("redis_mode"))
        if server.get("valkey_version") != "9.0.7" or mode != ("cluster" if cluster else "standalone"):
            raise RuntimeError("Valkey observed version/mode mismatch")
        if (persistence.get("loading") != "0" or persistence.get("aof_enabled") != "1" or
                persistence.get("aof_last_write_status") != "ok" or persistence.get("aof_last_bgrewrite_status") != "ok"):
            raise RuntimeError("Valkey AOF is disabled, loading or unhealthy")
        if replication.get("role") != "master" or replication.get("connected_slaves") != "0":
            raise RuntimeError("Valkey RF1 requires primaries with zero replicas")
        if node["info_cluster"].get("cluster_enabled") != ("1" if cluster else "0"):
            raise RuntimeError("Valkey INFO cluster mode mismatch")
        if not cluster or not complete:
            continue
        info = node["cluster_info"]
        required_info = {"cluster_state": "ok", "cluster_slots_assigned": "16384", "cluster_slots_ok": "16384",
                         "cluster_slots_pfail": "0", "cluster_slots_fail": "0",
                         "cluster_known_nodes": "5", "cluster_size": "5"}
        if any(info.get(k) != v for k, v in required_info.items()):
            raise RuntimeError("Valkey incomplete/unhealthy cluster slot or primary inventory")
        owners, members, self_ids = {}, {}, []
        for line in node["cluster_nodes"].splitlines():
            fields = line.split()
            if len(fields) < 8:
                raise RuntimeError("Valkey malformed CLUSTER NODES evidence")
            identity, advertised, flags, master, _, _, _, link, *slots = fields
            flags = set(flags.split(","))
            endpoint, _, bus = advertised.split(",")[0].partition("@")
            if (not re.fullmatch(r"[a-f0-9]{40}", identity) or identity in members or endpoint not in expected or
                    not bus.isdigit() or int(bus) != int(endpoint.rsplit(":", 1)[1]) + 10000 or
                    flags - {"myself", "master"} or "master" not in flags or master != "-" or link != "connected"):
                raise RuntimeError("Valkey requires exact healthy primary membership and connected cluster links")
            members[identity] = endpoint
            if "myself" in flags:
                if endpoint != node["endpoint"]:
                    raise RuntimeError("Valkey self identity advertises wrong endpoint")
                self_ids.append(identity)
            if not slots:
                raise RuntimeError("Valkey requested primary owns no slots")
            for token in slots:
                if not re.fullmatch(r"\d+(?:-\d+)?", token):
                    raise RuntimeError("Valkey slot migration/import or malformed ownership is not admitted")
                bounds = token.split("-")
                start, end = int(bounds[0]), int(bounds[-1])
                if not 0 <= start <= end < 16384:
                    raise RuntimeError("Valkey slot outside legal range")
                for slot in range(start, end + 1):
                    if slot in owners:
                        raise RuntimeError("Valkey duplicate slot ownership")
                    owners[slot] = identity
        if len(members) != 5 or set(members.values()) != expected or len(self_ids) != 1 or len(owners) != 16384:
            raise RuntimeError("Valkey incomplete unique 16384-slot primary ownership")
        links = node["cluster_links"]
        if not isinstance(links, list):
            raise RuntimeError("Valkey cluster link evidence missing")
        links = [dict(zip(link[::2], link[1::2])) if isinstance(link, list) and len(link) % 2 == 0 else link for link in links]
        others = set(members) - set(self_ids)
        if len(links) != 8 or any(not isinstance(link, dict) or "r" not in link.get("events", "") for link in links) or any(
                {link.get("node") for link in links if link.get("direction") == direction} != others
                for direction in ("to", "from")):
            raise RuntimeError("Valkey not all requested primary cluster links are active")
        identities[node["endpoint"]] = self_ids[0]
        views.append((members, owners))
    if cluster and complete and (len(set(identities.values())) != 5 or any(view != views[0] for view in views[1:])):
        raise RuntimeError("Valkey primary views disagree about identities or slot ownership")
    return {"required_primary_nodes": 5 if cluster else 1, "replication_factor": 1, "replica_count": 0,
            "primary_endpoints": sorted(expected), "slot_count": (16384 if cluster else 0) if complete else None,
            "primary_ids": identities, "durability": "AOF appendfsync always; no-appendfsync-on-rewrite no"}


def initialize_valkey(run, endpoints, directory, timeout):
    deadline = time.monotonic() + timeout
    while True:
        try:
            if any(valkey_cli(run, endpoint, "PING").strip() != "PONG" for endpoint in endpoints):
                raise RuntimeError("Valkey primary PING failed")
            infos = [valkey_info(valkey_cli(run, endpoint, "CLUSTER", "INFO")) for endpoint in endpoints]
            break
        except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired):
            if time.monotonic() >= deadline:
                raise RuntimeError("Valkey primary startup deadline exceeded")
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    evidence = {"observed_at_unix": time.time(), "initial_cluster_info": infos}
    # Only create a pristine cluster. Never replay a partially completed mutation:
    # final admission, not the create command's exit status, proves readiness.
    if all(info.get("cluster_known_nodes") == "1" and info.get("cluster_slots_assigned") == "0" for info in infos):
        evidence["create"] = valkey_cli(run, endpoints[0], "--cluster", "create", *endpoints,
                                       "--cluster-replicas", "0", "--cluster-yes", "--cluster-timeout", "5000")
    else:
        evidence["create"] = "existing cluster membership retained; actual topology admission required"
    dump(Path(directory) / "initialization.json", evidence)
    return evidence


def topology(backend, layout, directory, owner):
    """Compose limits are hard cgroup budgets, not reserved physical resources."""
    count = 1 if layout == "qualification" else (5 if backend == "valkey_tcp" else 3)
    services, volumes = {}, {}

    def add(name, image, cpus, memory, args=None, **extra):
        volume = name + "-data"
        volumes[volume] = {"labels": {OWNER_LABEL: owner}}
        service = {"image": IMAGES[image], "profiles": [backend], "network_mode": "host",
                   "cpus": cpus, "mem_limit": memory, "pids_limit": 512,
                   "labels": {OWNER_LABEL: owner}, "restart": "no",
                   "logging": {"driver": "json-file", "options": {"max-size": "8m", "max-file": "2"}},
                   "volumes": [volume + ":/data"], "stop_grace_period": "20s"}
        if args is not None:
            service["command"] = args
        service.update(extra)
        services[name] = service

    if backend == "tikv":
        pd_args = ["--name=pd0", "--data-dir=/data", "--client-urls=http://127.0.0.1:2379",
                   "--advertise-client-urls=http://127.0.0.1:2379", "--peer-urls=http://127.0.0.1:2380",
                   "--advertise-peer-urls=http://127.0.0.1:2380", "--initial-cluster=pd0=http://127.0.0.1:2380",
                   "--config=/config/pd.toml"]
        add("pd0", "pd", 1.0, "512m", pd_args)
        services["pd0"]["volumes"].append(str(directory / "pd.toml") + ":/config/pd.toml:ro")
        for i in range(count):
            add(f"tikv{i}", "tikv", 2.0, "1536m", ["--addr", f"127.0.0.1:{20160+i}",
                "--advertise-addr", f"127.0.0.1:{20160+i}", "--status-addr", f"127.0.0.1:{20180+i}",
                "--pd", "127.0.0.1:2379", "--data-dir", "/data", "--config", "/config/tikv.toml",
                "--labels", f"zone=local-process-{i}"],
                ulimits={"nofile": {"soft": TIKV_NOFILE, "hard": TIKV_NOFILE}})
            services[f"tikv{i}"]["volumes"].append(str(directory / "tikv.toml") + ":/config/tikv.toml:ro")
    elif backend == "cassandra":
        for i in range(count):
            address = f"127.0.0.{11+i}"
            add(f"cassandra{i}", "cassandra", 2.0, "1536m", environment={
                "CASSANDRA_CLUSTER_NAME": "kv-" + owner,
                "CASSANDRA_LISTEN_ADDRESS": address, "CASSANDRA_BROADCAST_ADDRESS": address,
                "CASSANDRA_RPC_ADDRESS": address, "CASSANDRA_BROADCAST_RPC_ADDRESS": address,
                "CASSANDRA_SEEDS": "127.0.0.11", "CASSANDRA_ENDPOINT_SNITCH": "GossipingPropertyFileSnitch",
                "CASSANDRA_DC": "dc1", "CASSANDRA_RACK": f"local-process-{i}",
                "CASSANDRA_NUM_TOKENS": "16", "MAX_HEAP_SIZE": "512M", "MAX_DIRECT_MEMORY_SIZE": "256M",
                "LOCAL_JMX": "yes",
                # The pinned cassandra-env.sh resets JMX_PORT=7199; its trailing
                # JVM_EXTRA_OPTS is the supported final system-property override.
                "JVM_EXTRA_OPTS": f"-Dcassandra.jmx.local.port={7199+i} -Dcassandra.ring_delay_ms=1000 "
                                  "-Dcassandra.available_processors=2 -XX:ActiveProcessorCount=2 "
                                  "-XX:ParallelGCThreads=2 -XX:ConcGCThreads=2"})
            services[f"cassandra{i}"]["volumes"] = [f"cassandra{i}-data:/var/lib/cassandra"]
            # Avoid simultaneous bootstrap races; actual UN/CQL readiness gates each addition.
    elif backend == "foundationdb":
        for i in range(count):
            add(f"fdb{i}", "foundationdb", 1.0, "768m", ["--cluster-file", "/config/fdb.cluster",
                "--public-address", f"127.0.0.1:{4500+i}", "--listen-address", f"127.0.0.1:{4500+i}",
                "--datadir", "/data", "--logdir", "/data/logs", "--memory", "512MiB", "--cache-memory", "128MiB",
                "--locality-zoneid", f"local-process-{i}", "--locality-machineid", f"local-process-{i}",
                "--locality-dcid", "local-dc", "--class", "unset"], entrypoint=["/usr/bin/fdbserver"])
            services[f"fdb{i}"]["volumes"].append(str(directory / "fdb.cluster") + ":/config/fdb.cluster")
    elif backend == "valkey_tcp":
        for i in range(count):
            add(f"valkey{i}", "valkey_tcp", 2.0, "1536m",
                valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, count > 1, 1024))
    else:
        raise ValueError("unknown backend")
    return {"services": services, "volumes": volumes}, count


def inspect_owned(state):
    ids = command(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project=" + state["project"]]).split()
    if not ids:
        return []
    items = json.loads(command(["docker", "inspect", *ids]))
    for item in items:
        if item["Config"]["Labels"].get(OWNER_LABEL) != state["owner"]:
            raise RuntimeError("foreign container in owned project; refusing action")
    return items


def container(state, service):
    items = inspect_owned(state)
    matches = [x for x in items if x["Config"]["Labels"].get("com.docker.compose.service") == service]
    if len(matches) != 1 or not matches[0]["State"]["Running"]:
        raise RuntimeError(f"requested service {service} is absent or not running")
    return matches[0]["Id"]


def execute(state, service, *args):
    return command(["docker", "exec", container(state, service), *args], timeout=15)


def http(path):
    with urllib.request.urlopen("http://127.0.0.1:2379" + path, timeout=3) as response:
        return json.load(response)


def probe(state):
    backend, count = state["backend"], state["nodes"]
    items = inspect_owned(state)
    if len(items) != count + (backend == "tikv") or any(not x["State"]["Running"] for x in items):
        raise RuntimeError("not all requested containers are running")
    if backend == "tikv":
        stores = http("/pd/api/v1/stores")
        regions = http("/pd/api/v1/regions")
        replication = http("/pd/api/v1/config/replicate")
        up = [x for x in stores.get("stores", []) if x["store"].get("state_name") == "Up"]
        if len(up) != count or replication.get("max-replicas") != count:
            raise RuntimeError("PD effective store/RF mismatch")
        region_list = regions.get("regions", [])
        if not region_list or any(len(x.get("peers", [])) != count or not x.get("leader")
                                  or x.get("pending_peers") or x.get("down_peers") for x in region_list):
            raise RuntimeError("TiKV regions have not reached requested ownership/replication")
        configs = []
        for i in range(count):
            with urllib.request.urlopen(f"http://127.0.0.1:{20180+i}/config", timeout=3) as response:
                configs.append(json.load(response))
        return {"stores": stores, "regions": regions, "replication": replication, "effective_configs": configs,
                "pd_version": http("/pd/api/v1/version")}
    if backend == "cassandra":
        records = []
        for i in range(count):
            status = execute(state, f"cassandra{i}", "nodetool", "-p", str(7199+i), "status", "kv_comparison")
            live = re.findall(r"^UN\s+(\S+)", status, re.MULTILINE)
            if set(live) != {f"127.0.0.{11+j}" for j in range(count)}:
                raise RuntimeError("Cassandra gossip membership not fully UN")
            schema = execute(state, f"cassandra{i}", "cqlsh", f"127.0.0.{11+i}", "9042", "-e",
                             "SELECT JSON release_version, data_center, rack, host_id, tokens FROM system.local;")
            rows = [json.loads(x.strip()) for x in schema.splitlines() if x.strip().startswith("{")]
            if len(rows) != 1 or rows[0]["release_version"] != "5.0.9" or rows[0]["data_center"] != "dc1":
                raise RuntimeError("Cassandra actual version/DC mismatch")
            records.append({"status": status, "local": rows[0],
                            "effective_yaml": execute(state, f"cassandra{i}", "cat", "/etc/cassandra/cassandra.yaml")})
        return {"nodes": records, "keyspace_replication": "validated by adapter after isolated keyspace creation"}
    if backend == "valkey_tcp":
        endpoints = state["backend_options"]["nodes"]
        proof = valkey_evidence(
            lambda endpoint, *args: execute(state, f"valkey{endpoints.index(endpoint)}", *args),
            endpoints, count > 1)
        for item in items:
            if (item["Config"]["Image"] != IMAGES["valkey_tcp"] or
                    item["HostConfig"]["Memory"] != 1536 * 1024**2 or item["HostConfig"]["NanoCpus"] != 2_000_000_000):
                raise RuntimeError("Valkey running image or CPU/memory caps differ from owned deployment")
        proof["runtime"] = [{"id": item["Id"], "image_id": item["Image"], "state": item["State"],
                             "memory_bytes": item["HostConfig"]["Memory"], "nano_cpus": item["HostConfig"]["NanoCpus"],
                             "runtime_configuration_sha256": runtime_configuration_hash(item)} for item in items]
        dump(Path(state["directory"]) / "probe-attempt.json", proof)
        proof["coverage"] = admit_valkey(proof["nodes"], endpoints, count > 1)
        return proof
    text = execute(state, "fdb0", "/usr/bin/fdbcli", "-C", "/config/fdb.cluster", "--timeout", "8", "--exec", "status json")
    status = json.loads(text)
    cluster = status.get("cluster", {})
    config = cluster.get("configuration", {})
    expected = "single" if count == 1 else "double"
    processes = cluster.get("processes", {})
    if not cluster.get("database_available") or not cluster.get("full_replication") or config.get("redundancy_mode") != expected or len(processes) != count:
        raise RuntimeError("FDB database/process/redundancy not ready")
    zones = {x.get("locality", {}).get("zoneid") for x in processes.values()}
    if len(zones) != count or None in zones or any(x.get("version") != "7.3.77" for x in processes.values()):
        raise RuntimeError("FDB observed version/locality mismatch")
    return status


def ready(state, timeout):
    deadline = time.monotonic() + timeout
    error = None
    while time.monotonic() < deadline:
        try:
            proof = probe(state)
            dump(Path(state["directory"]) / "deployment.json", {"observed_at_unix": time.time(), "topology": state, "observed": proof})
            return proof
        except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
            error = str(exc)
            time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise RuntimeError(f"requested {state['backend']} readiness failed: {error}")


def runtime_configuration_hash(item):
    # Bind actual argv/entrypoint/environment without exporting secret values.
    config = item["Config"]
    environment = {}
    for entry in config.get("Env") or []:
        name, _, value = entry.partition("=")
        environment[name] = "<per-run-identity>" if name in {"HOSTNAME", "CASSANDRA_CLUSTER_NAME"} else value
    identity = {"command": config.get("Cmd"), "entrypoint": config.get("Entrypoint"), "environment": environment}
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def export(state, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    items = inspect_owned(state)
    # Never export container environment: it can contain secrets in future custom deployments.
    dump(output / "containers.json", [{"id": x["Id"], "image_id": x["Image"], "state": x["State"],
         "service": x["Config"]["Labels"].get("com.docker.compose.service"),
         "memory_bytes": x["HostConfig"]["Memory"], "nano_cpus": x["HostConfig"]["NanoCpus"],
         "runtime_configuration_sha256": runtime_configuration_hash(x)} for x in items])
    for item in items:
        name = item["Config"]["Labels"]["com.docker.compose.service"]
        (output / (name + ".log")).write_text(command(["docker", "logs", "--tail", "300", item["Id"]], check=False))
    for name in ("owner.json", "compose.json", "deployment.json", "probe-attempt.json", "initialization.json", "images.json", "pd.toml", "tikv.toml", "fdb.cluster"):
        source = Path(state["directory"]) / name
        if source.exists() and source.resolve() != (output / name).resolve():
            shutil.copyfile(source, output / name)


def stop(state):
    inspect_owned(state)
    names = command(["docker", "volume", "ls", "-q", "--filter", "label=com.docker.compose.project=" + state["project"]]).split()
    if names:
        for item in json.loads(command(["docker", "volume", "inspect", *names])):
            if item.get("Labels", {}).get(OWNER_LABEL) != state["owner"]:
                raise RuntimeError("foreign volume in owned project; refusing cleanup")
    compose(state, "down", "--volumes", "--remove-orphans", timeout=90)
    dump(Path(state["directory"]) / "cleanup.json", {"completed_at_unix": time.time(), "project": state["project"], "owner": state["owner"]})


def start(args):
    if platform.system() != "Linux":
        raise RuntimeError("host-loopback topology requires Linux Docker Engine (not Docker Desktop)")
    directory = args.state.resolve()
    if (directory / "owner.json").exists():
        state = load(directory)
        if state["backend"] != args.backend or state["layout"] != args.layout:
            raise RuntimeError("existing state backend/layout mismatch")
        ready(state, args.timeout)
        return
    if directory.exists() and any(directory.iterdir()):
        raise RuntimeError("new state directory must be empty; refusing to overwrite unowned files")
    directory.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(directory).free < 4 * 1024**3:
        raise RuntimeError("at least 4 GiB free disk required before image/data allocation")
    owner = uuid.uuid4().hex
    spec, count = topology(args.backend, args.layout, directory, owner)
    if args.backend == "tikv":
        addresses = [("127.0.0.1", p) for p in [2379, 2380] + [20160+i for i in range(count)] + [20180+i for i in range(count)]]
    elif args.backend == "cassandra":
        addresses = [(f"127.0.0.{11+i}", p) for i in range(count) for p in (7000, 7001, 9042)]
        addresses += [("127.0.0.1", 7199+i) for i in range(count)]
    elif args.backend == "valkey_tcp":
        addresses = [(f"127.0.0.{21+i}", p) for i in range(count) for p in (6379+i, 16379+i)]
    else:
        addresses = [("127.0.0.1", 4500+i) for i in range(count)]
    for address in addresses:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(address)
            except OSError as exc:
                raise RuntimeError(f"requested local endpoint {address} is not free; no unowned service was stopped") from exc
    state = {"owner": owner, "project": "kv-" + owner[:16], "backend": args.backend,
             "layout": args.layout, "nodes": count, "directory": str(directory),
             "compose_file": str(directory / "compose.json"), "physical_hosts": 1, "independent_disks": False,
             "placement": "all local processes share host SSD; logical zones are not failure domains",
             "created_at_unix": time.time()}
    if args.backend == "valkey_tcp":
        state["backend_options"] = {"nodes": [f"127.0.0.{21+i}:{6379+i}" for i in range(count)],
                                    "cluster": count > 1, "required_replication_factor": 1,
                                    "required_primary_nodes": count, "request_timeout_ms": 5000}
    dump(directory / "owner.json", state)
    dump(directory / "compose.json", spec)
    (directory / "pd.toml").write_text(f'[replication]\nmax-replicas = {count}\nlocation-labels = ["zone"]\n')
    (directory / "tikv.toml").write_text('memory-usage-limit = "1GB"\n[readpool.unified]\nmin-thread-count = 1\nmax-thread-count = 2\n[server]\ngrpc-concurrency = 2\n[storage]\nreserve-space = "64MB"\nscheduler-worker-pool-size = 2\n[storage.block-cache]\ncapacity = "256MB"\n[raftstore]\nstore-pool-size = 1\napply-pool-size = 1\n[rocksdb]\nmax-background-jobs = 2\n[rocksdb.titan]\nenabled = true\n[raftdb]\nmax-background-jobs = 2\n')
    coordinators = ",".join(f"127.0.0.1:{4500+i}" for i in range(count))
    (directory / "fdb.cluster").write_text(f"kvcomparison:{owner}@{coordinators}\n")
    try:
        compose(state, "pull", timeout=300)
        used = sorted({x["image"] for x in spec["services"].values()})
        dump(directory / "images.json", [{"requested": image, "resolved": json.loads(command(["docker", "image", "inspect", image]))[0]["RepoDigests"],
              "image_id": json.loads(command(["docker", "image", "inspect", image]))[0]["Id"]} for image in used])
        if args.backend == "cassandra":
            for i in range(count):
                compose(state, "up", "-d", f"cassandra{i}")
                deadline = time.monotonic() + args.timeout
                while True:
                    try:
                        execute(state, f"cassandra{i}", "cqlsh", f"127.0.0.{11+i}", "9042", "-e", "SELECT release_version FROM system.local;")
                        break
                    except (RuntimeError, OSError, subprocess.TimeoutExpired):
                        for item in inspect_owned(state):
                            if (item["Config"]["Labels"].get("com.docker.compose.service") == f"cassandra{i}"
                                    and item["State"]["Status"] in {"exited", "dead"}):
                                raise RuntimeError(f"Cassandra node {i} exited before CQL readiness: "
                                                   f"exit={item['State']['ExitCode']} oom={item['State']['OOMKilled']}")
                        if time.monotonic() >= deadline:
                            raise RuntimeError(f"Cassandra node {i} CQL startup deadline")
                        time.sleep(1)
        if args.backend == "cassandra":
            execute(state, "cassandra0", "cqlsh", "127.0.0.11", "9042", "-e",
                    "CREATE KEYSPACE IF NOT EXISTS kv_comparison WITH replication = "
                    "{'class':'SimpleStrategy','replication_factor':" + str(count) + "} AND durable_writes = true;")
        else:
            compose(state, "up", "-d")
        if args.backend == "foundationdb":
            # This command blocks on the real coordinator handshake, not an arbitrary startup sleep.
            execute(state, "fdb0", "/usr/bin/fdbcli", "-C", "/config/fdb.cluster", "--timeout", "10", "--exec",
                    "configure new " + ("single" if count == 1 else "double") + " ssd")
        if args.backend == "valkey_tcp" and count > 1:
            initialize_valkey(
                lambda endpoint, *argv: execute(state, f"valkey{state['backend_options']['nodes'].index(endpoint)}", *argv),
                state["backend_options"]["nodes"], directory, args.timeout)
        ready(state, args.timeout)
    except BaseException:
        export(state, directory / "failure")
        raise


def cleanup_extraction(receipt):
    state = json.loads(Path(receipt).read_text())
    if not re.fullmatch(r"kv-fdb-client-[a-f0-9]{32}", state["name"]):
        raise ValueError("invalid client extraction owner name")
    ids = command(["docker", "ps", "-aq", "--filter", "name=^/" + state["name"] + "$"]).split()
    for item in json.loads(command(["docker", "inspect", *ids])) if ids else []:
        if item["Config"]["Labels"].get(OWNER_LABEL) != state["owner"]:
            raise RuntimeError("foreign extraction container; refusing cleanup")
        command(["docker", "rm", "-fv", item["Id"]])


def prepare_fdb(args):
    """Copy exact pinned image library, without installing a package on the host."""
    prefix = args.prefix.resolve()
    prefix.mkdir(parents=True, exist_ok=True)
    library = prefix / "lib" / "libfdb_c.so"
    library.parent.mkdir(exist_ok=True)
    if library.exists():
        raise RuntimeError("prefix already contains libfdb_c.so; use a fresh explicit prefix")
    command(["docker", "pull", IMAGES["foundationdb"]], timeout=300)
    owner = uuid.uuid4().hex
    name = "kv-fdb-client-" + owner
    dump(prefix / "fdb-extract.json", {"owner": owner, "name": name})
    try:
        ident = command(["docker", "create", "--name", name, "--label", OWNER_LABEL + "=" + owner,
                         "--entrypoint", "/usr/bin/true", IMAGES["foundationdb"]]).strip()
        command(["docker", "cp", ident + ":/usr/lib/libfdb_c.so", library])
        dump(prefix / "client.json", {"version": "7.3.77", "api": 730, "image": IMAGES["foundationdb"],
                                     "path": str(library.resolve()),
                                     "sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
                                     "blake2b512": hashlib.blake2b(library.read_bytes()).hexdigest()})
    finally:
        cleanup_extraction(prefix / "fdb-extract.json")
    print(f"export FDB_CLIENT_LIB_DIR={library.parent}\nexport FDB_CLIENT_LIB_PATH={library.parent}\nexport LD_LIBRARY_PATH={library.parent}:\"${{LD_LIBRARY_PATH:-}}\"\nexport LIBRARY_PATH={library.parent}:\"${{LIBRARY_PATH:-}}\"")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    launch = sub.add_parser("start")
    launch.add_argument("--backend", required=True, choices=["tikv", "cassandra", "foundationdb", "valkey_tcp"])
    launch.add_argument("--layout", choices=["distributed", "qualification"], default="distributed")
    launch.add_argument("--state", type=Path, required=True)
    launch.add_argument("--timeout", type=int, default=180)
    for action in ("ready", "export", "stop"):
        p = sub.add_parser(action)
        p.add_argument("--state", type=Path, required=True)
        if action == "ready":
            p.add_argument("--timeout", type=int, default=180)
        if action == "export":
            p.add_argument("--output", type=Path, required=True)
    p = sub.add_parser("prepare-fdb")
    p.add_argument("--prefix", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "start":
        start(args)
    elif args.action == "prepare-fdb":
        prepare_fdb(args)
    else:
        state = load(args.state)
        if args.action == "ready":
            ready(state, args.timeout)
        elif args.action == "stop":
            stop(state)
        else:
            export(state, args.output)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"kv-comparison local error: {exc}", file=sys.stderr)
        sys.exit(1)
