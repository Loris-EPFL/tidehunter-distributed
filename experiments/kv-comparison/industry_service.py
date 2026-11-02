"""RESP control and strict admission for the pinned five-primary industry stores."""
import hashlib
import socket
import re
import time

SOURCES = {
    "garnet": {"version": "2.2.5", "commit": "8d007138208ce030eb10a50c80e636b1678b83e4", "url": "https://github.com/microsoft/garnet/tree/v2.2.5"},
    "kvrocks": {"version": "2.17.0", "commit": "185d4221df20af8ed3416e4dec4de8917312a2e9", "url": "https://github.com/apache/kvrocks/tree/v2.17.0", "image_version_label": "v2.17.0-rc2"},
}


def local_endpoints():
    return [f"127.0.0.{31+i}:6379" for i in range(5)]


def options(backend, nodes):
    if backend not in SOURCES or len(nodes) != 5 or len(set(nodes)) != 5:
        raise ValueError("five distinct industry endpoints required")
    return {"nodes": list(nodes), "cluster": True, "required_replication_factor": 1,
            "required_primary_nodes": 5, "request_timeout_ms": 5000}


def garnet_args(address, port, memory_mib):
    return ["--bind", address, "--port", str(port), "--cluster-announce-ip", address,
            "--cluster-announce-port", str(port), "--cluster", "--storage-tier", "--logdir", "/data",
            "--checkpointdir", "/data", "--recover", "--fail-on-recovery-error",
            "--compaction-type", "Lookup", "--compaction-freq", "300", "--checkpoint-freq", "600",
            "--aof", "--aof-commit-freq", "0", "--aof-commit-wait",
            "--memory", f"{memory_mib // 4}m", "--page", "4m", "--segment", "1g",
            "--index", "64m", "--index-max-size", "64m", "--readcache", "--readcache-memory", "128m",
            "--readcache-page", "4m", "--aof-memory", "64m", "--aof-page-size", "8m",
            "--metrics-sampling-freq", "1"]


def kvrocks_config(address, port, memory_mib):
    return (f"bind {address}\nport {port}\ndir /data\ncluster-enabled yes\n"
            "persist-cluster-nodes-enabled yes\nworkers 2\n"
            f"rocksdb.block_cache_size {memory_mib // 4}\n"
            "rocksdb.subkey_block_cache_size 128\nrocksdb.metadata_block_cache_size 128\n"
            "rocksdb.disable_auto_compactions no\n"
            "rocksdb.write_buffer_size 32\nrocksdb.max_write_buffer_number 2\n"
            "rocksdb.max_background_jobs 2\nrocksdb.write_options.sync yes\n"
            "rocksdb.write_options.disable_wal no\n")


def request(endpoint, *args):
    host, port = endpoint.rsplit(":", 1)
    chunks = [str(arg).encode() for arg in args]
    message = b"*%d\r\n" % len(chunks) + b"".join(b"$%d\r\n" % len(x) + x + b"\r\n" for x in chunks)
    with socket.create_connection((host, int(port)), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(message)
        stream = sock.makefile("rb")
        def read(depth=0):
            if depth > 8:
                raise RuntimeError("RESP nesting limit exceeded")
            line = stream.readline(1024 * 1024)
            if not line.endswith(b"\r\n"):
                raise RuntimeError("truncated RESP response")
            kind, payload = line[:1], line[1:-2]
            if kind == b"-":
                raise RuntimeError("server rejected command: " + payload.decode("utf-8", "replace"))
            if kind == b"+":
                return payload.decode()
            if kind == b":":
                return int(payload)
            if kind == b"$":
                length = int(payload)
                if length == -1:
                    return None
                if not 0 <= length <= 16 * 1024 * 1024:
                    raise RuntimeError("RESP bulk length out of bounds")
                value = stream.read(length + 2)
                if len(value) != length + 2 or value[-2:] != b"\r\n":
                    raise RuntimeError("truncated RESP bulk")
                return value[:-2].decode()
            if kind == b"*":
                length = int(payload)
                if not 0 <= length <= 20000:
                    raise RuntimeError("RESP array length out of bounds")
                return [read(depth + 1) for _ in range(length)]
            raise RuntimeError("unsupported RESP response")
        return read()


def info(text):
    if not isinstance(text, str):
        raise RuntimeError("invalid INFO response")
    result = {}
    for line in text.splitlines():
        if line and not line.startswith("#") and ":" in line:
            key, value = line.split(":", 1)
            if key in result:
                raise RuntimeError("duplicate INFO field")
            result[key] = value
    return result


def settings(endpoint, backend):
    keys = (["appendonly", "aof-commit-freq", "aof-commit-wait", "logdir", "cluster-enabled"] if backend == "garnet" else
            ["dir", "cluster-enabled", "persist-cluster-nodes-enabled", "rocksdb.block_cache_size",
             "rocksdb.subkey_block_cache_size", "rocksdb.metadata_block_cache_size", "rocksdb.disable_auto_compactions",
             "rocksdb.write_buffer_size", "rocksdb.max_write_buffer_number", "rocksdb.max_background_jobs",
             "rocksdb.write_options.sync", "rocksdb.write_options.disable_wal"])
    result = {}
    for key in keys:
        value = request(endpoint, "CONFIG", "GET", key)
        if not isinstance(value, list) or len(value) != 2 or value[0] != key:
            raise RuntimeError("missing effective setting: " + key)
        result[key] = value[1]
    return result


def evidence(backend, endpoints, complete=True):
    nodes = []
    for endpoint in endpoints:
        node = {"endpoint": endpoint, "effective_config": settings(endpoint, backend)}
        for section in ("server", "replication", "cluster", "persistence", "memory"):
            node["info_" + section] = info(request(endpoint, "INFO", section))
        if complete:
            node["cluster_info"] = info(request(endpoint, "CLUSTER", "INFO"))
            node["cluster_nodes"] = request(endpoint, "CLUSTER", "NODES")
            node["cluster_slots"] = request(endpoint, "CLUSTER", "SLOTS")
        nodes.append(node)
    return {"nodes": nodes, "source": SOURCES[backend], "transport": "TCP",
            "authentication": "unauthenticated private network only",
            "read_path": "storage tier plus bounded engine cache; OS cache eviction does not guarantee device reads"}


def admit_runtime(backend, runtime, config):
    """Validate an observed cloud container receipt, not a caller-supplied tier assertion."""
    images = {
        "garnet": "ghcr.io/microsoft/garnet:2.2.5@sha256:ef84a729cc054b048d8ed17113475890dc6389a8ea8725c34771efe785841137",
        "kvrocks": "apache/kvrocks:2.17.0@sha256:a1e99b3653fb900d5a17b80a9487f74079edc6d5e9b70e3103c01cbfc0d5146a",
    }
    if backend not in images or config.get("backend") != backend or config.get("index") not in range(5):
        raise RuntimeError("invalid industry runtime receipt context")
    if len(runtime) != 1:
        raise RuntimeError("expected exactly one observed store container")
    item = runtime[0]
    address, memory = config["address"], config["budget"]["memory_mib"]
    args = garnet_args(address, 6379, memory) if backend == "garnet" else ["-c", "/config/kvrocks.conf"]
    entrypoint = ["/app/GarnetServer"] if backend == "garnet" else ["kvrocks"]
    if (item.get("image") != images[backend] or item.get("cmd") != args or
            item.get("entrypoint") != entrypoint or not item.get("state", {}).get("Running") or
            not any(x.endswith(images[backend].split("@")[1]) for x in item.get("repo_digests", []))):
        raise RuntimeError("industry image, running state, entrypoint or server arguments differ")
    limits = item.get("cgroup_limits", {})
    if (item.get("name") != "/" + config["containers"][0]["name"] or
            any(item.get("labels", {}).get(k) != v for k, v in config["labels"].items()) or
            limits.get("Memory") != memory * 1024**2 or limits.get("MemorySwap") != memory * 1024**2 or
            limits.get("NanoCpus") != int(config["budget"]["cpus"] * 1e9)):
        raise RuntimeError("industry container ownership or resource limits differ")
    source = config["directory"] + "/data"
    if not any(x.get("Type") == "bind" and x.get("Source") == source and x.get("Destination") == "/data" and x.get("RW") for x in item.get("mounts", [])):
        raise RuntimeError("industry data bind escapes the owned directory")
    if backend == "kvrocks":
        if (item.get("config_text") != kvrocks_config(address, 6379, memory) or
                not any(x.get("Type") == "bind" and x.get("Source") == config["directory"] + "/kvrocks.conf" and
                        x.get("Destination") == "/config/kvrocks.conf" and not x.get("RW") for x in item.get("mounts", []))):
            raise RuntimeError("Kvrocks effective configuration bind differs")
    return {"image": images[backend], "data_bind": source,
            "storage_tier_evidence": "observed pinned container argv and owned data bind" if backend == "garnet" else "observed pinned RocksDB configuration and owned data bind"}


def admit_local_runtime(backend, runtime, state):
    import local
    if backend not in SOURCES or state.get("backend") != backend or len(runtime) != 5:
        raise RuntimeError("invalid local industry runtime inventory")
    project, owner = state["project"], state["owner"]
    seen = set()
    for item in runtime:
        name = item.get("service", "")
        if name not in {f"{backend}{i}" for i in range(5)} or name in seen:
            raise RuntimeError("invalid local industry service identity")
        seen.add(name)
        index = int(name[len(backend):])
        args = garnet_args(f"127.0.0.{31+index}", 6379, 1536) if backend == "garnet" else ["-c", "/config/kvrocks.conf"]
        entrypoint = ["/app/GarnetServer"] if backend == "garnet" else ["kvrocks"]
        if (item.get("image") != local.IMAGES[backend] or item.get("cmd") != args or item.get("entrypoint") != entrypoint or
                not item.get("state", {}).get("Running") or item.get("memory_bytes") != 1536 * 1024**2 or
                item.get("memory_swap_bytes") != 1536 * 1024**2 or item.get("nano_cpus") != 2_000_000_000 or
                not any(x.endswith(local.IMAGES[backend].split("@")[1]) for x in item.get("repo_digests", []))):
            raise RuntimeError("local industry observed image, argv or resource limits mismatch")
        volume_name = project + "_" + name + "-data"
        if (not any(x.get("Type") == "volume" and x.get("Name") == volume_name and
                    x.get("Destination") == "/data" and x.get("RW") for x in item.get("mounts", [])) or
                item.get("volume_labels", {}).get(local.OWNER_LABEL) != owner):
            raise RuntimeError("local industry data volume is not exact-owned")
        if backend == "kvrocks":
            config_path = state["directory"] + "/" + name + ".conf"
            if (item.get("config_text") != kvrocks_config(f"127.0.0.{31+index}", 6379, 1536) or
                    not any(x.get("Type") == "bind" and x.get("Source") == config_path and
                            x.get("Destination") == "/config/kvrocks.conf" and not x.get("RW") for x in item.get("mounts", []))):
                raise RuntimeError("local Kvrocks configuration bind mismatch")
    return {"containers": 5, "image": local.IMAGES[backend], "owned_volumes": True,
            "storage_tier_evidence": "observed pinned container argv and exact-owned data volumes"}


def admit(backend, nodes, endpoints, complete=True):
    expected = set(endpoints)
    if len(nodes) != len(expected) or {n.get("endpoint") for n in nodes} != expected or len(expected) != (5 if complete else 1):
        raise RuntimeError("industry primary endpoint inventory mismatch")
    views = []
    for node in nodes:
        config = node["effective_config"]
        required = ({"appendonly": "yes", "aof-commit-freq": "0", "aof-commit-wait": "yes", "logdir": "/data", "cluster-enabled": "yes"} if backend == "garnet" else
                    {"dir": "/data", "cluster-enabled": "yes", "persist-cluster-nodes-enabled": "yes", "rocksdb.write_options.sync": "yes", "rocksdb.write_options.disable_wal": "no", "rocksdb.disable_auto_compactions": "no", "rocksdb.write_buffer_size": "32", "rocksdb.max_write_buffer_number": "2", "rocksdb.max_background_jobs": "2", "rocksdb.subkey_block_cache_size": "128", "rocksdb.metadata_block_cache_size": "128"})
        if any(str(config.get(k)) != v for k, v in required.items()):
            raise RuntimeError("industry effective durability, data path or cluster setting mismatch")
        if backend == "kvrocks" and not 0 < int(config.get("rocksdb.block_cache_size", "0")) <= 262144:
            raise RuntimeError("Kvrocks block cache limit invalid")
        if node["info_replication"].get("role") not in ("master", "primary"):
            raise RuntimeError("industry node is not primary")
        version = node["info_server"].get("garnet_version" if backend == "garnet" else "kvrocks_version", "")
        if not version.startswith(SOURCES[backend]["version"]):
            raise RuntimeError("industry server version mismatch")
        if node["info_replication"].get("connected_slaves") not in (None, "0"):
            raise RuntimeError("industry RF1 primary has replicas")
        if not complete:
            continue
        slots = node["cluster_slots"]
        if not isinstance(slots, list):
            raise RuntimeError("invalid cluster slots response")
        owners, identities = {}, {}
        for row in slots:
            if not isinstance(row, list) or len(row) != 3 or not isinstance(row[2], list) or len(row[2]) < 3:
                raise RuntimeError("invalid slot range or replica present")
            start, end, primary = row
            endpoint = str(primary[0]) + ":" + str(primary[1])
            identity = primary[2]
            if endpoint not in expected or not isinstance(identity, str) or len(identity) != 40 or not isinstance(start, int) or not isinstance(end, int) or not 0 <= start <= end < 16384:
                raise RuntimeError("invalid slot ownership")
            if endpoint in identities and identities[endpoint] != identity:
                raise RuntimeError("conflicting node identity")
            identities[endpoint] = identity
            for slot in range(start, end + 1):
                if slot in owners:
                    raise RuntimeError("duplicate slot ownership")
                owners[slot] = endpoint
        if len(owners) != 16384 or set(identities) != expected or len(set(identities.values())) != 5:
            raise RuntimeError("incomplete five-primary slot coverage")
        health = node["cluster_info"]
        if any(health.get(k) != v for k, v in {"cluster_state": "ok", "cluster_slots_assigned": "16384",
                "cluster_slots_ok": "16384", "cluster_slots_pfail": "0", "cluster_slots_fail": "0", "cluster_known_nodes": "5"}.items()):
            raise RuntimeError("cluster reports unhealthy or incomplete membership")
        members, self_count, reported = {}, 0, {}
        for line in node["cluster_nodes"].splitlines():
            fields = line.split()
            if len(fields) < 8:
                raise RuntimeError("malformed cluster member")
            identity, address, flags, master, _, _, _, link, *ranges = fields
            endpoint = address.split("@")[0].split(",")[0]
            flags = set(flags.split(","))
            if (endpoint not in expected or identity != identities.get(endpoint) or
                    flags - {"myself", "master"} or "master" not in flags or master != "-" or link != "connected"):
                raise RuntimeError("invalid or unhealthy primary membership")
            if endpoint in members or not ranges:
                raise RuntimeError("duplicate or slotless cluster member")
            members[endpoint] = identity
            for token in ranges:
                if not re.fullmatch(r"\d+(?:-\d+)?", token):
                    raise RuntimeError("invalid or migrating cluster slot range")
                bounds = token.split("-")
                start, end = int(bounds[0]), int(bounds[-1])
                if not 0 <= start <= end < 16384:
                    raise RuntimeError("invalid cluster node slot range")
                for slot in range(start, end + 1):
                    if slot in reported:
                        raise RuntimeError("duplicate cluster node slot range")
                    reported[slot] = endpoint
            if "myself" in flags:
                self_count += 1
                if endpoint != node["endpoint"]:
                    raise RuntimeError("cluster self identity mismatch")
        if set(members) != expected or self_count != 1 or reported != owners:
            raise RuntimeError("incomplete primary membership")
        views.append((owners, identities))
    if complete and any(view != views[0] for view in views[1:]):
        raise RuntimeError("cluster views disagree")
    return {"required_primary_nodes": 5, "replication_factor": 1, "replica_count": 0,
            "primary_endpoints": sorted(expected), "slot_count": 16384 if complete else None,
            "primary_ids": views[0][1] if complete else {},
            "durability": "AOF commit per operation and commit-wait" if backend == "garnet" else "RocksDB WAL enabled and write_options.sync=yes"}


def initialize(backend, endpoints, owner, timeout):
    deadline = time.monotonic() + timeout
    while True:
        try:
            if all(request(endpoint, "PING") == "PONG" for endpoint in endpoints):
                break
        except (OSError, RuntimeError, ValueError):
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError("industry primary startup deadline exceeded")
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    if backend == "kvrocks":
        ids = [hashlib.sha1((owner + endpoint).encode()).hexdigest() for endpoint in endpoints]
        topology = "\n".join(f"{ids[i]} {endpoint.rsplit(':', 1)[0]} {endpoint.rsplit(':', 1)[1]} master - {i * 16384 // 5}-{(i + 1) * 16384 // 5 - 1}" for i, endpoint in enumerate(endpoints))
        for endpoint, identity in zip(endpoints, ids):
            version = request(endpoint, "CLUSTERX", "VERSION")
            if int(version) < 0:
                request(endpoint, "CLUSTERX", "SETNODEID", identity)
                request(endpoint, "CLUSTERX", "SETNODES", topology, "1")
            elif int(version) != 1:
                raise RuntimeError("existing Kvrocks topology version is not owned bootstrap")
        return {"topology": topology, "version": 1}
    for i, endpoint in enumerate(endpoints):
        current = info(request(endpoint, "CLUSTER", "INFO"))
        if current.get("cluster_known_nodes") == "1" and current.get("cluster_slots_assigned") == "0":
            request(endpoint, "CLUSTER", "SET-CONFIG-EPOCH", str(i + 1))
            request(endpoint, "CLUSTER", "ADDSLOTSRANGE", str(i * 16384 // 5), str((i + 1) * 16384 // 5 - 1))
    for endpoint in endpoints[1:]:
        host, port = endpoint.rsplit(":", 1)
        request(endpoints[0], "CLUSTER", "MEET", host, port)
    return {"bootstrap": "assigned disjoint slot ranges and issued CLUSTER MEET"}
