#!/usr/bin/env python3
"""Pool raw observations only within an exact admitted comparison boundary."""
import argparse
import csv
import hashlib
import json
import math
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


def stable_topology(name, original):
    topology = dict(original)
    for field in ("path", "process_id", "cluster_file", "cluster_name", "coordinator_process_id", "owner_process_evidence", "owner_processes"):
        topology.pop(field, None)
    if name == "tidehunter_tcp" and "endpoints" in topology:
        topology["endpoints"] = sorted(
            ({"partition": item["partition"]} for item in topology["endpoints"]),
            key=lambda item: item["partition"])
    if name == "valkey_tcp" and isinstance(topology.get("primaries"), list):
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
    if name == "valkey_tcp" and isinstance(config.get("persistence"), list):
        # Retain health/guarantee evidence, not changing AOF sizes, timestamps,
        # rewrite progress or counters. Original artifacts retain every field.
        fields = ("aof_enabled", "loading", "async_loading", "aof_last_write_status", "aof_last_bgrewrite_status")
        config["persistence"] = [{key: item[key] for key in fields if key in item}
                                 for item in config["persistence"]]
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
    if config["backend"]["kind"] in ("tikv", "cassandra", "foundationdb", "valkey_tcp"):
        if not deployment:
            raise ValueError(f"{run}: missing actual external deployment/replication proof")
        observed, nodes = deployment["observed"], deployment["topology"]["nodes"]
        expected_rf = (1 if config["backend"]["kind"] == "valkey_tcp" else
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
        elif config["backend"]["kind"] == "valkey_tcp":
            expected_primaries = 1 if deployment["topology"]["layout"] == "qualification" else 5
            coverage = observed.get("coverage", {})
            if (nodes != expected_primaries or coverage.get("required_primary_nodes") != nodes
                    or coverage.get("replication_factor") != 1 or coverage.get("replica_count") != 0
                    or len(observed.get("nodes", [])) != nodes
                    or len(set(coverage.get("primary_endpoints", []))) != nodes
                    or coverage.get("slot_count") != (0 if nodes == 1 else 16384)):
                raise ValueError(f"{run}: incomplete Valkey RF1 primary/slot ownership proof")
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
    guarantee["observed_topology"] = stable_topology(backend["name"], backend["topology"])
    hardware = receipt["hardware"]
    capacity = host_capacity(hardware["memory"])
    hardware = {key: hardware[key] for key in ("machine", "cpu", "kernel", "affinity", "block_devices")}
    hardware["memory_capacity_bytes"] = capacity
    first_cpu = dict(line.split(":", 1) for line in hardware["cpu"].split("\n\n", 1)[0].splitlines() if ":" in line)
    hardware["cpu"] = {key.strip(): value.strip() for key, value in first_cpu.items()
                       if key.strip() in ("vendor_id", "cpu family", "model", "model name", "stepping", "flags", "CPU implementer", "CPU architecture", "CPU part")}
    deployment_budget = None
    if deployment:
        topology = deployment["topology"]
        compose = json.loads((run / "deployment/compose.json").read_text())
        actual = json.loads((run / "deployment/containers.json").read_text())
        resolved_images = {item["requested"]: item["image_id"] for item in json.loads((run / "deployment/images.json").read_text())}
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
            "diagnostics": config.get("diagnostics", {"write_profile_limit": 0}),
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
