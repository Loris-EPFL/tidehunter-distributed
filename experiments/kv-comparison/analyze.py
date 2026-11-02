#!/usr/bin/env python3
"""Pool raw observations only within an exact admitted comparison boundary."""
import argparse
import csv
import hashlib
import ipaddress
import json
import math
import re
from pathlib import Path
import sys

import local


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def quantile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def read_samples(path, *, measurement_only=True):
    with Path(path).open(newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"phase", "operation", "outcome", "scheduled_ns", "dispatched_ns", "dispatch_observed", "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes", "error_kind"}
        if not required <= set(reader.fieldnames or []):
            raise ValueError("raw samples lack the required timing/accounting schema")
        for row in reader:
            if measurement_only and row["phase"] != "measurement":
                continue
            if None in row or row["dispatch_observed"] not in ("true", "false"):
                raise ValueError("invalid raw timing row or dispatch identity")
            for key in ("scheduled_ns", "dispatched_ns", "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes"):
                text = row[key]
                if text is None or not text.isascii() or not text.isdigit():
                    raise ValueError("missing or invalid timing/count in raw sample")
                row[key] = int(text)
            if row["logical_keys"] == 0 or row["completed_ns"] < row["scheduled_ns"]:
                raise ValueError("invalid logical-key count or terminal clock ordering")
            if row["response_ns"] != row["completed_ns"] - row["scheduled_ns"]:
                raise ValueError("response latency disagrees with terminal clocks")
            dispatched = row["dispatch_observed"] == "true"
            if dispatched:
                if not row["scheduled_ns"] <= row["dispatched_ns"] <= row["completed_ns"]:
                    raise ValueError("invalid dispatch clock ordering")
                if (row["api_ns"] != row["completed_ns"] - row["dispatched_ns"]
                        or row["scheduling_ns"] != row["dispatched_ns"] - row["scheduled_ns"]):
                    raise ValueError("API/scheduling latency disagrees with monotonic clocks")
                if row["outcome"] == "rejected":
                    raise ValueError("rejected arrival cannot have an observed API dispatch")
            else:
                if row["dispatched_ns"] or row["api_ns"] or row["scheduling_ns"]:
                    raise ValueError("undispatched call has invented API clocks")
                row["api_ns"] = row["scheduling_ns"] = None
            yield row


def aggregate(samples):
    groups = {}
    offered = completed = failures = 0
    for row in samples:
        offered += 1
        outcome = row["outcome"]
        if outcome in ("ok", "success"):
            completed += 1
        else:
            failures += 1
        key = (row["operation"], outcome)
        bucket = groups.setdefault(key, {"count": 0, "logical_keys": 0, "api_ns": [], "response_ns": [], "scheduling_ns": []})
        bucket["count"] += 1
        bucket["logical_keys"] += row["logical_keys"]
        for field in ("api_ns", "response_ns", "scheduling_ns"):
            if row[field] is not None:
                bucket[field].append(row[field])
    if offered == 0:
        raise ValueError("zero measurement samples are not a successful performance run")
    result = []
    for (operation, outcome), bucket in sorted(groups.items()):
        item = {"operation": operation, "outcome": outcome, "count": bucket["count"], "logical_keys": bucket["logical_keys"]}
        for field in ("api_ns", "response_ns", "scheduling_ns"):
            values = bucket[field]
            item[field] = {"sample_count": len(values), "p50": quantile(values, .50), "p95": quantile(values, .95),
                           "p99": quantile(values, .99), "p999": quantile(values, .999), "max": max(values) if values else None}
        result.append(item)
    return {"offered_calls": offered, "success_calls": completed, "unsuccessful_calls": failures,
            "success_fraction": completed / offered, "populations": result,
            "tail_caveat": "Quantiles are conditional on operation/outcome and timed samples. Rejected/unknown/timeout calls remain in denominator; no overall successful-tail or HA claim."}

def verify_normalization(raw, admitted):
    """Defaults may be added; explicitly supplied values may not be changed."""
    if isinstance(raw, dict):
        if not isinstance(admitted, dict) or any(key not in admitted for key in raw):
            raise ValueError("normalized configuration dropped supplied fields")
        for key, value in raw.items():
            verify_normalization(value, admitted[key])
    elif isinstance(raw, list):
        if not isinstance(admitted, list) or len(raw) != len(admitted):
            raise ValueError("normalized configuration changed a supplied list")
        for value, normalized in zip(raw, admitted):
            verify_normalization(value, normalized)
    elif (isinstance(raw, bool) != isinstance(admitted, bool)) or raw != admitted:
        raise ValueError("normalized configuration changed an explicitly supplied value")


def verify_binding(run, config, receipt, manifest, summary):
    admitted_path = run / "normalized-config.json"
    admitted = json.loads(admitted_path.read_text())
    verify_normalization(config, admitted)
    if hashlib.sha256(admitted_path.read_bytes()).hexdigest() != receipt["normalized_config_sha256"]:
        raise ValueError(f"{run}: normalized admission changed after execution")
    executed = manifest.get("run", {})
    if executed.get("config") != admitted:
        raise ValueError(f"{run}: result configuration differs from admitted executable input")
    if executed.get("binary", {}).get("blake2b512") != receipt["binary_blake2b512"]:
        raise ValueError(f"{run}: result executable differs from admitted binary")
    if manifest.get("backend_info") != summary.get("backend"):
        raise ValueError(f"{run}: manifest and summary disagree about the backend contract")
    hashes = receipt.get("artifacts_sha256", {})
    for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom"):
        file = run / "result" / name
        digest = hashlib.sha256()
        with file.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if hashes.get(name) != digest.hexdigest():
            raise ValueError(f"{run}: result artifact {name} is missing its original integrity binding")
    marker = manifest.get("run", {}).get("measurement_read_evidence_schema_version")
    if marker is not None and (type(marker) is not int or marker != 1):
        raise ValueError(f"{run}: unsupported measurement read evidence capability")
    if marker == 1 and receipt.get("measurement_read_evidence") is None:
        raise ValueError(f"{run}: advertised measurement read evidence is missing its integrity binding")
    evidence = receipt.get("measurement_read_evidence")
    if evidence is not None:
        if (not isinstance(evidence, dict) or evidence.get("path") != "result/measurement-read-evidence.json"
                or hashlib.sha256((run / evidence["path"]).read_bytes()).hexdigest() != evidence.get("sha256")):
            raise ValueError(f"{run}: measurement read evidence integrity binding differs")
    diagnostics = receipt.get("read_diagnostics")
    requested = config.get("diagnostics", {}).get("read_profile_limit", 0)
    path = run / "result/read-diagnostics.json"
    if diagnostics is None:
        if requested or path.exists():
            raise ValueError(f"{run}: read diagnostics are missing their integrity binding")
    elif (not isinstance(diagnostics, dict) or diagnostics.get("path") != "result/read-diagnostics.json"
          or hashlib.sha256(path.read_bytes()).hexdigest() != diagnostics.get("sha256")
          or json.loads(path.read_text()) != diagnostics.get("observed")):
        raise ValueError(f"{run}: read diagnostics integrity binding differs")


def valkey_view(text, identities, self_id):
    """Reduce a validated cluster view to advertised endpoint and slot ownership."""
    owners, seen, own = {}, set(), []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 9:
            raise ValueError("malformed Valkey cluster view")
        node_id, address, flags, master, _, _, _, link, *ranges = fields
        flags = set(flags.split(","))
        endpoint, _, bus = address.split(",")[0].partition("@")
        if (node_id not in identities or node_id in seen or
                endpoint != identities[node_id] or not bus.isdigit() or
                int(bus) != int(endpoint.rsplit(":", 1)[1]) + 10000 or
                flags - {"myself", "master"} or "master" not in flags or
                master != "-" or link != "connected"):
            raise ValueError("invalid Valkey cluster membership")
        seen.add(node_id)
        if "myself" in flags:
            own.append(node_id)
        for token in ranges:
            if not re.fullmatch(r"\d+(?:-\d+)?", token):
                raise ValueError("invalid Valkey slot range")
            parts = token.split("-")
            start, end = int(parts[0]), int(parts[-1])
            if not 0 <= start <= end < 16384:
                raise ValueError("invalid Valkey slot range")
            for slot in range(start, end + 1):
                if slot in owners:
                    raise ValueError("duplicate Valkey slot")
                owners[slot] = identities[node_id]
    if seen != set(identities) or own != [self_id] or len(owners) != 16384:
        raise ValueError("incomplete Valkey cluster view")
    return [owners[i] for i in range(16384)]


def admit_native_valkey(run, deployment, backend, config):
    """Admit the local native exporter, including its independently captured proof."""
    import local_valkey_rdma

    state = deployment["topology"]
    owner = json.loads((run / "deployment/owner.json").read_text())
    services = json.loads((run / "deployment/services.json").read_text())
    observed = deployment["observed"]
    count = state["nodes"]
    if owner != state or state.get("paired_valkey") is not True or state["backend"] != backend["name"]:
        raise ValueError("native Valkey owner/deployment mismatch")
    controls, rdma = state["control_endpoints"], state["rdma_endpoints"]
    cluster = state["layout"] != "qualification"
    if (count != (5 if cluster else 1) or len(controls) != count or len(rdma) != count or
            len(set(controls)) != count or len(set(rdma)) != count or
            len(state["services"]) != count or len(services) != count):
        raise ValueError("native Valkey inventory mismatch")
    options = local_valkey_rdma.options_for_backend(state["backend"], count, rdma)
    if (state["backend_options"] != options or
            config["backend"].get("options") != options or
            controls != [f"127.0.0.{21+i}:{6379+i}" for i in range(count)]):
        raise ValueError("native Valkey client endpoint contract differs")
    address = rdma[0].rsplit(":", 1)[0]
    if (ipaddress.IPv4Address(address).is_loopback or
            rdma != [f"{address}:{6380+i}" for i in range(count)]):
        raise ValueError("native Valkey RDMA endpoints differ")
    source = "fe8d406c0b5a64ec897f6455e4de684629883764"
    client = backend["configuration"]
    expected_transport = ("upstream libvalkey VALKEY_CONN_RDMA, RDMA-CM RC"
                          if state["backend"] == "valkey_rdma" else "upstream libvalkey VALKEY_CONN_TCP")
    if (observed.get("source_commit") != source or
            observed.get("transport") != ("RDMA-CM RC" if state["backend"] == "valkey_rdma" else "TCP") or
            client.get("source_commit") != source or client.get("transport") != expected_transport or
            "bundled libvalkey 0.2.1" not in backend.get("client_version", "") or
            backend.get("server_version") != "9.0.7"):
        raise ValueError("native Valkey source or transport differs")
    qualification = observed.get("rdma_qualification" if state["backend"] == "valkey_rdma" else "tcp_qualification", {})
    if qualification != {"status": "passed", "endpoints": options["nodes"], "operations": ["PING", "SET", "GET", "DEL"]}:
        raise ValueError("native Valkey client qualification missing")
    runtime = observed.get("runtime")
    if not isinstance(runtime, list) or len(runtime) != count:
        raise ValueError("native Valkey readiness runtime missing")
    roles = []
    for i, (service, exported, ready) in enumerate(zip(state["services"], services, runtime)):
        data = str(Path(state["directory"]) / f"node-{i}" / "data")
        argv = local.valkey_server_args(f"127.0.0.{21+i}", 6379+i, 16379+i, cluster, 1024)
        argv = [str(Path(state["runtime"]) / "bin/valkey-server") if x == "valkey-server"
                else x.replace("/data", data) for x in argv]
        argv += ["--rdma-bind", address, "--rdma-port", str(6380+i)]
        expected = {"runtime": state["runtime"], "unit": f"kvcloud-valkey-{state['owner'][:20]}-{i}",
                    "args": argv, "cpus": 2, "memory_mib": 1536, "source_commit": source,
                    "server_sha256": service.get("server_sha256")}
        digest = service.get("server_sha256")
        if service != expected or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("native Valkey service contract differs")
        if not isinstance(exported, list) or len(exported) != 1:
            raise ValueError("native Valkey exported runtime missing")
        for record in (ready, exported[0]):
            systemd = record.get("systemd", {})
            unit = service["unit"]
            cgroup = "/system.slice/" + unit + ".service"
            files = record.get("cgroup_files")
            limits = record.get("proc_limits")
            if not isinstance(files, dict) or not isinstance(limits, list):
                raise ValueError("native Valkey raw resource proof missing")
            cpu = files.get("cpu.max")
            match = re.fullmatch(r"([0-9]+) ([0-9]+)", cpu) if isinstance(cpu, str) else None
            if (files.get("memory.max") != str(service["memory_mib"] * 1024**2) or
                    files.get("memory.swap.max") != "0" or not match or int(match[2]) <= 0 or
                    int(match[1]) != service["cpus"] * int(match[2]) or
                    not any(isinstance(line, str) and line.startswith("Max locked memory") and
                            line.split()[3:5] == ["unlimited", "unlimited"] for line in limits)):
                raise ValueError("native Valkey cgroup or memlock proof differs")
            if (record.get("unit") != unit or record.get("cgroup") != cgroup or
                    type(record.get("pid")) is not int or record["pid"] <= 0 or
                    record.get("source") != source or record.get("server_sha256") != digest or
                    systemd.get("LoadState") != "loaded" or systemd.get("ActiveState") != "active" or
                    systemd.get("MainPID") != str(record["pid"]) or systemd.get("ControlGroup") != cgroup or
                    systemd.get("MemoryMax") != str(service["memory_mib"] * 1024**2) or
                    systemd.get("LimitMEMLOCK") not in ("infinity", "18446744073709551615") or
                    systemd.get("CPUQuotaPerSecUSec") not in ("2s", "2000000us", "2000000") or
                    "argv[]=" + " ".join(service["args"]) + " ;" not in systemd.get("ExecStart", "") or
                    not record.get("argv")):
                raise ValueError("native Valkey systemd runtime proof differs")
        if ready["pid"] != exported[0]["pid"] or ready["systemd"]["CPUQuotaPerSecUSec"] != exported[0]["systemd"]["CPUQuotaPerSecUSec"]:
            raise ValueError("native Valkey runtime changed")
        roles.append({"server_sha256": digest, "cpu_quota_cpus": service["cpus"],
                      "memory_max_bytes": service["memory_mib"] * 1024**2,
                      "memory_swap_max_bytes": 0, "memlock": "unlimited"})
    dirs = {endpoint: str(Path(state["directory"]) / f"node-{i}" / "data") for i, endpoint in enumerate(controls)}
    try:
        coverage = local.admit_valkey(observed["nodes"], controls, cluster, data_dir=dirs)
        if coverage != observed["coverage"]:
            raise ValueError("native Valkey recorded coverage differs")
        core_settings = client["node_config"]
        if len(core_settings) != count:
            raise ValueError("native Valkey client settings missing")
        persistence = client["persistence"]
        if len(persistence) != count or any(
                item.get("loading") != "0" or item.get("aof_enabled") != "1" or
                item.get("aof_last_write_status") != "ok" or item.get("aof_last_bgrewrite_status") != "ok"
                for item in persistence):
            raise ValueError("native Valkey client persistence health differs")
        keys = ("appendonly", "appendfsync", "no-appendfsync-on-rewrite", "maxmemory-policy")
        for node, settings in zip(observed["nodes"], core_settings):
            if any(settings.get(key) != node["effective_config"].get(key) for key in keys):
                raise ValueError("native Valkey client durability settings differ")
            cluster_file = node["effective_config"].get("cluster-config-file")
            if cluster and cluster_file != str(Path(dirs[node["endpoint"]]) / "nodes.conf"):
                raise ValueError("native Valkey cluster configuration path differs")
        topology = backend["topology"]
        ids = topology["node_ids"]
        views = topology["cluster_nodes_views"]
        if (topology["cluster"] is not cluster or topology["primary_count"] != count or
                topology["slot_count"] != (16384 if cluster else None) or topology["endpoints"] != options["nodes"]):
            raise ValueError("native Valkey backend topology differs")
        expected_slots = None
        if cluster:
            if (len(ids) != count or len(set(ids)) != count or len(views) != count or
                    ids != [coverage["primary_ids"][endpoint] for endpoint in controls]):
                raise ValueError("native Valkey backend identity differs")
            identities = dict(zip(ids, controls))
            expected_slots = valkey_view(observed["nodes"][0]["cluster_nodes"], identities, ids[0])
            for i, view in enumerate(views):
                if valkey_view(view, identities, ids[i]) != expected_slots:
                    raise ValueError("native Valkey backend ownership differs")
        elif ids != [] or views != []:
            raise ValueError("standalone Valkey cluster proof differs")
    except (KeyError, TypeError, IndexError, RuntimeError) as exc:
        raise ValueError(f"invalid native Valkey evidence: {exc}") from exc
    # Slots map to advertised control endpoints; selected client endpoints are retained separately.
    stable = {key: value for key, value in topology.items() if key not in ("node_ids", "cluster_nodes_views")}
    stable["slot_owners"] = expected_slots
    settings = []
    for node in observed["nodes"]:
        omitted = ("dir", "cluster-config-file") if cluster else ("dir",)
        settings.append({key: value for key, value in node["effective_config"].items() if key not in omitted})
    return ({"kind": "native_systemd", "layout": state["layout"], "nodes": count,
             "physical_hosts": state["physical_hosts"], "independent_disks": state["independent_disks"],
             "roles": roles, "source_commit": source,
             "resource_evidence": "observed systemd, cgroup files and process limits"},
            stable, settings)


def stable_topology(name, original):
    topology = dict(original)
    for field in ("path", "process_id", "cluster_file", "cluster_name", "coordinator_process_id", "owner_process_evidence", "owner_processes"):
        topology.pop(field, None)
    if name == "tidehunter_tcp" and "endpoints" in topology:
        topology["endpoints"] = sorted(
            ({"partition": item["partition"]} for item in topology["endpoints"]),
            key=lambda item: item["partition"])
    if name in ("valkey_tcp", "valkey_rdma", "garnet", "kvrocks") and isinstance(topology.get("primaries"), list):
        topology["primaries"] = sorted(
            ({key: value for key, value in item.items() if key != "node_id"}
             for item in topology["primaries"]), key=canonical)
    if name == "tikv_raw" and "store_metadata" in topology:
        # IDs, capacity/current load, start/heartbeat times are observations,
        # not independent-run configuration. Preserve the raw original artifact.
        topology["store_metadata"] = sorted((
            {key: row["store"].get(key) for key in ("address", "status_address", "version", "git_hash", "state_name")}
            | {"labels": sorted(row["store"].get("labels", []), key=canonical)}
            for row in topology["store_metadata"].get("stores", [])), key=canonical)
    if name == "foundationdb" and "cluster_status" in topology:
        cluster = topology["cluster_status"].get("cluster", {})
        # Role assignments/IDs/loads can change across legitimate repetitions.
        # Keep configured placement/classes and health, not volatile recruitment.
        topology["cluster_status"] = {
            "configuration": cluster.get("configuration"),
            "database_available": cluster.get("database_available"),
            "full_replication": cluster.get("full_replication"),
            "protocol_version": cluster.get("protocol_version"),
            "processes": sorted(({
                "address": item.get("address"), "version": item.get("version"),
                "class_type": item.get("class_type"),
                "locality": {key: item.get("locality", {}).get(key) for key in ("dcid", "machineid", "zoneid")}
            } for item in cluster.get("processes", {}).values()), key=canonical),
        }
    return topology


def stable_configuration(original, name=None):
    config = {key: value for key, value in original.items() if key not in ("coordinator_path", "cluster_file")}
    if isinstance(config.get("loaded_native_library"), dict):
        config["loaded_native_library"] = {
            key: value for key, value in config["loaded_native_library"].items() if key != "path"}
    if name in ("valkey_tcp", "valkey_rdma") and isinstance(config.get("persistence"), list):
        # Retain health/guarantee evidence, not changing AOF sizes, timestamps,
        # rewrite progress or counters. Original artifacts retain every field.
        fields = ("aof_enabled", "loading", "async_loading", "aof_last_write_status", "aof_last_bgrewrite_status")
        config["persistence"] = [{key: item[key] for key in fields if key in item}
                                 for item in config["persistence"]]
    if name in ("garnet", "kvrocks"):
        # Raw INFO snapshots include changing counters, sizes and timestamps;
        # effective CONFIG and the storage policy remain part of the contract.
        config.pop("node_info", None)
    return config


def host_capacity(meminfo):
    values = {}
    for line in meminfo.splitlines():
        key, separator, value = line.partition(":")
        if separator and key in ("MemTotal", "SwapTotal"):
            amount, unit = value.split()
            if unit != "kB" or not amount.isdigit():
                raise ValueError("invalid host memory-capacity evidence")
            values[key] = int(amount) * 1024
    if set(values) != {"MemTotal", "SwapTotal"} or values["MemTotal"] <= 0:
        raise ValueError("missing host RAM/swap capacity evidence")
    return values



def admit_industry_observation(kind, deployment, backend):
    """Reconstruct RF1 slot and durable-setting proof from the raw observations."""
    import industry_service

    topology = deployment.get("topology", {})
    observed = deployment.get("observed", {})
    if (topology.get("nodes") != 5 or backend.get("name") != kind
            or backend.get("replication_factor") != 1):
        raise ValueError("industry deployment must have five actual RF1 primaries")
    nodes = observed.get("nodes")
    coverage = observed.get("coverage")
    if not isinstance(nodes, list) or not isinstance(coverage, dict):
        raise ValueError("missing industry primary/slot observations")
    endpoints = coverage.get("primary_endpoints")
    if not isinstance(endpoints, list) or len(endpoints) != 5 or len(set(endpoints)) != 5:
        raise ValueError("missing industry primary endpoint inventory")
    try:
        reconstructed = industry_service.admit(kind, nodes, endpoints)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise ValueError(f"invalid industry primary/slot/durability evidence: {exc}") from exc
    if reconstructed != coverage:
        raise ValueError("industry recorded coverage disagrees with raw observations")
    core = backend.get("topology", {})
    primaries = core.get("primaries")
    if (core.get("cluster") is not True or core.get("primary_count") != 5
            or core.get("slot_count") != 16384 or not isinstance(primaries, list)
            or len(primaries) != 5):
        raise ValueError("missing industry backend primary/slot observation")
    try:
        core_slots = {}
        for primary in primaries:
            endpoint = primary["address"]
            if reconstructed["primary_ids"].get(endpoint) != primary["node_id"]:
                raise ValueError("industry backend primary identities disagree with deployment")
            for start, end in primary["slot_ranges"]:
                if type(start) is not int or type(end) is not int or not 0 <= start <= end < 16384:
                    raise ValueError("invalid industry backend slot range")
                for slot in range(start, end + 1):
                    if slot in core_slots:
                        raise ValueError("duplicate industry backend slot")
                    core_slots[slot] = endpoint
        actual_slots = {}
        for start, end, primary in nodes[0]["cluster_slots"]:
            endpoint = f"{primary[0]}:{primary[1]}"
            actual_slots.update((slot, endpoint) for slot in range(start, end + 1))
        if core_slots != actual_slots:
            raise ValueError("industry backend slot ownership disagrees with deployment")
    except (KeyError, TypeError, IndexError) as exc:
        raise ValueError("missing industry backend slot observations") from exc
    return reconstructed


def admit_industry_runtime(kind, deployment, containers):
    import industry_service

    runtime = deployment.get("observed", {}).get("runtime")
    if not isinstance(runtime, list) or not isinstance(containers, list):
        raise ValueError("missing actual industry launch and storage-tier proof")
    try:
        proof = industry_service.admit_local_runtime(kind, runtime, deployment["topology"])
        observed = {(item["service"], item["id"], item["image_id"]) for item in runtime}
        exported = {(item["service"], item["id"], item["image_id"]) for item in containers}
        if len(observed) != 5 or observed != exported:
            raise ValueError("industry launch receipt disagrees with exported containers")
        return proof
    except (KeyError, TypeError, RuntimeError) as exc:
        raise ValueError(f"invalid industry launch and storage-tier proof: {exc}") from exc


def boundary(run):
    receipt = json.loads((run / "receipt.json").read_text())
    if receipt.get("status") != "completed" or receipt.get("claim_boundary") == "API qualification only":
        raise ValueError(f"{run}: failed/qualification run cannot enter performance comparison")
    observation = receipt.get("observation")
    if not isinstance(observation, dict) or observation.get("state") not in ("exact_project_present", "exact_project_absent_other_observers_unknown"):
        raise ValueError(f"{run}: unknown/missing actual observer-project presence")
    if receipt.get("observation_after") != observation:
        raise ValueError(f"{run}: observer observation contract changed or lacks final verification")
    config = json.loads((run / "config.json").read_text())
    manifest = json.loads((run / "result/manifest.json").read_text())
    # Core records actual backend metadata; a config echo cannot substitute for it.
    summary = json.loads((run / "result/summary.json").read_text())
    if manifest.get("status") not in ("completed", "success") or summary.get("status") not in ("completed", "success"):
        raise ValueError(f"{run}: core result did not complete successfully")
    if hashlib.sha256((run / "config.json").read_bytes()).hexdigest() != receipt["config_sha256"]:
        raise ValueError(f"{run}: effective config hash changed after execution")
    verify_binding(run, config, receipt, manifest, summary)
    backend = summary.get("backend", manifest.get("backend_info", manifest.get("backend")))
    required = {"name", "server_version", "client_version", "acknowledgement", "consistency", "replication_factor", "measurement_boundary", "get_many_strategy", "batch_atomicity", "topology", "configuration"}
    if not isinstance(backend, dict) or not required <= set(backend):
        raise ValueError(f"{run}: manifest lacks observed effective BackendInfo")
    deployment_path = run / "deployment/deployment.json"
    deployment = json.loads(deployment_path.read_text()) if deployment_path.exists() else None
    if config["backend"]["kind"] in ("tikv", "cassandra", "foundationdb", "valkey_tcp", "valkey_rdma", "garnet", "kvrocks"):
        if not deployment:
            raise ValueError(f"{run}: missing actual external deployment/replication proof")
        observed, nodes = deployment["observed"], deployment["topology"]["nodes"]
        expected_rf = (1 if config["backend"]["kind"] in ("valkey_tcp", "valkey_rdma", "garnet", "kvrocks") else
                       (1 if nodes == 1 else 2) if config["backend"]["kind"] == "foundationdb" else nodes)
        if backend["replication_factor"] != expected_rf:
            raise ValueError(f"{run}: observed backend RF does not match admitted deployment")
        if config["backend"]["kind"] == "tikv":
            regions = observed["regions"]["regions"]
            if not regions or any(len(region.get("peers", [])) != nodes or not region.get("leader") or region.get("down_peers") or region.get("pending_peers") for region in regions):
                raise ValueError(f"{run}: no complete TiKV region ownership proof")
        elif config["backend"]["kind"] == "foundationdb":
            if not observed["cluster"].get("full_replication") or not observed["cluster"].get("database_available"):
                raise ValueError(f"{run}: missing available/full-replication FoundationDB observation")
        elif config["backend"]["kind"] in ("valkey_tcp", "valkey_rdma"):
            expected_primaries = 1 if deployment["topology"]["layout"] == "qualification" else 5
            coverage = observed.get("coverage", {})
            if (nodes != expected_primaries or coverage.get("required_primary_nodes") != nodes
                    or coverage.get("replication_factor") != 1 or coverage.get("replica_count") != 0
                    or len(observed.get("nodes", [])) != nodes
                    or len(set(coverage.get("primary_endpoints", []))) != nodes
                    or coverage.get("slot_count") != (0 if nodes == 1 else 16384)):
                raise ValueError(f"{run}: incomplete Valkey RF1 primary/slot ownership proof")
        elif config["backend"]["kind"] in ("garnet", "kvrocks"):
            admit_industry_observation(config["backend"]["kind"], deployment, backend)
        elif len(observed["nodes"]) != nodes:
            raise ValueError(f"{run}: incomplete Cassandra member observations")
    # Dynamic per-run namespaces/storage roots are not compared. Retain raw originals and
    # compare all semantic info plus the admitted topology/resource budget instead.
    guarantee = {key: backend[key] for key in required - {"configuration", "topology"}}
    if config["backend"]["kind"] == "foundationdb":
        guarantee["native_client_runtime"] = {
            key: value for key, value in json.loads((run.parent / "fdb-client.json").read_text()).items()
            if key != "path"}
    guarantee["effective_configuration"] = stable_configuration(backend["configuration"], backend["name"])
    native = (deployment and config["backend"]["kind"] in ("valkey_tcp", "valkey_rdma")
              and deployment["topology"].get("paired_valkey") is True)
    if native:
        deployment_budget, stable_valkey, valkey_settings = admit_native_valkey(run, deployment, backend, config)
        guarantee["observed_topology"] = stable_valkey
        guarantee["deployment_effective_configuration"] = valkey_settings
    else:
        guarantee["observed_topology"] = stable_topology(backend["name"], backend["topology"])
    if config["backend"]["kind"] in ("garnet", "kvrocks"):
        guarantee["deployment_effective_configuration"] = sorted(
            (node["effective_config"] for node in deployment["observed"]["nodes"]), key=canonical)
    hardware = receipt["hardware"]
    capacity = host_capacity(hardware["memory"])
    hardware = {key: hardware[key] for key in ("machine", "cpu", "kernel", "affinity", "block_devices")}
    hardware["memory_capacity_bytes"] = capacity
    first_cpu = dict(line.split(":", 1) for line in hardware["cpu"].split("\n\n", 1)[0].splitlines() if ":" in line)
    hardware["cpu"] = {key.strip(): value.strip() for key, value in first_cpu.items()
                       if key.strip() in ("vendor_id", "cpu family", "model", "model name", "stepping", "flags", "CPU implementer", "CPU architecture", "CPU part")}
    if not native:
        deployment_budget = None
    if deployment and not native:
        topology = deployment["topology"]
        compose = json.loads((run / "deployment/compose.json").read_text())
        actual = json.loads((run / "deployment/containers.json").read_text())
        resolved_images = {item["requested"]: item["image_id"] for item in json.loads((run / "deployment/images.json").read_text())}
        if config["backend"]["kind"] in ("garnet", "kvrocks"):
            admit_industry_runtime(config["backend"]["kind"], deployment, actual)
        if {item["service"] for item in actual} != set(compose["services"]):
            raise ValueError(f"{run}: actual role/container inventory differs from deployment")
        for item in actual:
            limits = compose["services"][item["service"]]
            if item["image_id"] != resolved_images.get(limits["image"]):
                raise ValueError(f"{run}: actual server image differs from the admitted digest-pinned image")
            memory = limits["mem_limit"].lower()
            expected_bytes = int(memory[:-1]) * {"m": 1024**2, "g": 1024**3}[memory[-1]]
            if item["memory_bytes"] != expected_bytes or item["nano_cpus"] != int(limits["cpus"] * 1e9):
                raise ValueError(f"{run}: actual server resource caps differ from declared budgets")
            if not isinstance(item.get("runtime_configuration_sha256"), str) or len(item["runtime_configuration_sha256"]) != 64:
                raise ValueError(f"{run}: missing actual server runtime-configuration identity")
        role_identities = {item["service"]: item["runtime_configuration_sha256"] for item in actual}
        deployment_budget = {"layout": topology["layout"], "nodes": topology["nodes"],
                             "physical_hosts": topology["physical_hosts"], "independent_disks": topology["independent_disks"],
                             "roles": {name: {**{key: service[key] for key in ("cpus", "mem_limit", "image")},
                                             "runtime_configuration_sha256": role_identities[name]}
                                       for name, service in compose["services"].items()},
                             "mounted_configuration_sha256": {name: hashlib.sha256((run / "deployment" / name).read_bytes()).hexdigest()
                                                               for name in ("pd.toml", "tikv.toml")
                                                               if config["backend"]["kind"] == "tikv"}}
    return {"backend": guarantee, "workload": {key: config[key] for key in ("dataset", "workload", "execution", "phases", "limits", "seed")},
            "hardware": hardware, "deployment": deployment_budget, "client_budget": receipt["client_budget"],
            "binary_sha256": receipt["binary_sha256"], "claim_boundary": receipt["claim_boundary"],
            "observation": receipt["observation"],
            "diagnostics": {"write_profile_limit": 0, "read_profile_limit": 0, **config.get("diagnostics", {})},
            "completion_accounting_gate": config.get("measurement_completion_gate") is not None,
            "storage_population": receipt["storage_population"]}


def independent_run_identity(run):
    config = json.loads((run / "config.json").read_text())
    receipt = json.loads((run / "receipt.json").read_text())
    identity = (config.get("namespace"), receipt.get("started_at_unix"), receipt.get("binary_sha256"))
    if (not isinstance(identity[0], str) or not identity[0]
            or type(identity[1]) not in (int, float) or not math.isfinite(identity[1]) or identity[1] <= 0
            or not isinstance(identity[2], str) or not identity[2]):
        raise ValueError("missing independent-run identity")
    return identity


def analyze(runs):
    if not runs:
        raise ValueError("at least one result directory required")
    runs = [path.resolve(strict=True) for path in runs]
    if len(set(runs)) != len(runs):
        raise ValueError("duplicate result directory")
    boundaries = [boundary(path) for path in runs]
    identity = canonical(boundaries[0])
    if any(canonical(item) != identity for item in boundaries[1:]):
        raise ValueError("different workload/hardware/guarantee/version/resource/observation contracts; analyze separately, never average p99 or assert speedup")
    identities = [independent_run_identity(path) for path in runs]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicated independent-run identity; copied results are not repetitions")
    samples, per_run = [], []
    for run in runs:
        rows = list(read_samples(run / "result/samples.csv"))
        if not rows:
            raise ValueError(f"{run}: no measured arrivals")
        config = json.loads((run / "config.json").read_text())
        requested = config["phases"]["measurement_operations"]
        if len(rows) != requested:
            raise ValueError(f"{run}: {len(rows)} raw arrivals do not account for all {requested} requested arrivals")
        offered_rate = config["execution"].get("offered_ops_per_second") if config["execution"]["mode"] == "open_loop" else None
        start = min(row["scheduled_ns"] for row in rows)
        end = max(row["completed_ns"] or row["scheduled_ns"] for row in rows)
        elapsed = (end - start) / 1e9
        offered_window = requested / offered_rate if offered_rate else None
        exposure = max(elapsed, offered_window or 0)
        success = sum(row["outcome"] in ("ok", "success") for row in rows)
        per_run.append({"run": str(run), "offered_calls": requested, "success_calls": success,
                        "offered_calls_per_second": offered_rate, "offered_window_seconds": offered_window,
                        "response_window_seconds": elapsed, "achieved_success_calls_per_second": success / exposure if exposure else None})
        samples.extend(rows)
    return {"schema_version": 1, "runs": [str(x) for x in runs], "boundary_sha256": hashlib.sha256(identity.encode()).hexdigest(),
            "boundary": boundaries[0], "aggregation": "nearest-rank quantiles of pooled raw samples, never average run p99",
            "per_run_accounting": per_run, "statistics": aggregate(samples)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    local.dump(args.output, analyze(args.runs))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError) as exc:
        print(f"kv-comparison analysis rejected: {exc}", file=sys.stderr)
        sys.exit(1)
