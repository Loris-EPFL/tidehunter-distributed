#!/usr/bin/env python3
"""Explicit multi-host comparison inputs and raw-sample reports; never allocates AWS."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import random
import statistics
import sys

import analyze
import local
import resource_accounting

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from portable_transport import runtime_backend, transport_for_case, validate_transport, protocol_key, select_backends

BACKENDS = ("native", "tidehunter_tcp", "tidehunter_efa", "tidehunter_efa_direct",
            "tidehunter_efa_native", "tikv", "cassandra", "foundationdb", "valkey_tcp")
LEGACY_BACKENDS = BACKENDS[:-1]
TRANSPORTS = {name: transport_for_case(name) for name in BACKENDS if name.startswith("tidehunter_")}
MIX_FIELDS = ("read", "put", "delete", "missing_read", "get_many", "put_batch")


def mix(**values):
    return {name: values.get(name, 0) for name in MIX_FIELDS}


def make_matrix(spec):
    """Block by backend to amortize server deployment; shuffle within each repeat."""
    required = {"schema_version", "seed", "repeats", "value_sizes", "backends",
                "keys", "max_cases", "offered_rates", "offered_seconds",
                "case_deadline_seconds", "hard_deadline_seconds", "export_reserve_seconds"}
    if not required <= set(spec) or set(spec) - required - {"diagnostics"}:
        raise ValueError("cloud matrix requires its explicit schema")
    if spec["schema_version"] != 1 or isinstance(spec["seed"], bool) or not isinstance(spec["seed"], int):
        raise ValueError("invalid cloud schema/seed")
    for name, lower, upper in (("repeats", 1, 10), ("keys", 1024, 65536), ("max_cases", 1, 2000),
                              ("offered_seconds", 2, 60), ("case_deadline_seconds", 60, 600),
                              ("hard_deadline_seconds", 3600, 10800), ("export_reserve_seconds", 300, 1800)):
        value = spec[name]
        if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
            raise ValueError(f"invalid {name}")
    if spec["export_reserve_seconds"] >= spec["hard_deadline_seconds"] // 3:
        raise ValueError("export reserve consumes the campaign")
    sizes, backends, rates = spec["value_sizes"], spec["backends"], spec["offered_rates"]
    if set(sizes) != {64, 256, 1024, 8192, 65536} or len(sizes) != 5:
        raise ValueError("full campaign requires all five value sizes without duplicates")
    backends = select_backends(None, backends, set(BACKENDS))
    rates = spec["offered_rates"]
    if set(rates) != {1000, 10000, 50000} or len(rates) != 3 or any(type(rate) is not int for rate in rates):
        raise ValueError("full campaign requires all three offered rates without duplicates")
    diagnostics = spec.get("diagnostics", {"write_profile_limit": 0})
    if (not isinstance(diagnostics, dict) or set(diagnostics) != {"write_profile_limit"}
            or type(diagnostics["write_profile_limit"]) is not int
            or not 0 <= diagnostics["write_profile_limit"] <= 65536):
        raise ValueError("diagnostics requires an explicit write_profile_limit between 0 and 65536")
    profiles = [
        ("point-mixed", mix(read=45, put=45, delete=5, missing_read=5), 1, 8192),
        ("read-concurrent", mix(read=100), 16, 8192),
        ("write-concurrent", mix(put=100), 16, 4096),
        ("read-many32", mix(get_many=100), 16, 2048),
    ]
    jobs = []
    for repeat in range(spec["repeats"]):
        order = list(backends)
        random.Random(spec["seed"] + repeat).shuffle(order)
        for backend in order:
            block = []
            def add(label, size, operation_mix, concurrency, count, *, cohort="primary_rf1",
                    rate=None, distribution="uniform", keys=None, cache=0, owner_cache=0, warmup=512):
                block.append({"backend": backend, "repeat": repeat, "profile": label,
                    "cohort": cohort, "value_size": size, "keys": keys or spec["keys"],
                    "mix": operation_mix, "concurrency": concurrency, "measurement_operations": count,
                    "warmup_operations": warmup, "offered_ops_per_second": rate,
                    "distribution": distribution, "coordinator_cache_bytes": cache,
                    "owner_read_cache_bytes": owner_cache, "drop_os_caches": cohort == "cold_os_cache",
                    "native_durability": "page_cache" if cohort == "native_pagecache" else "value_wal_barrier"})
            for size in sizes:
                for label, operation_mix, concurrency, count in profiles:
                    add(label, size, operation_mix, concurrency, count)
            for rate in rates:
                add(f"offered-{rate}", 1024, mix(read=70, put=30), 32,
                    rate * spec["offered_seconds"], rate=rate)
            add("zipf-read", 1024, mix(read=100), 16, 8192, distribution={"zipf": {"exponent": 0.99}})
            # A large finite dataset plus a quiescent OS-cache drop is not called
            # an all-NVMe population: engine block caches still exist and I/O is observed.
            add("cold-os-large", 65536, mix(read=100), 16, 4096,
                cohort="cold_os_cache", keys=131072, warmup=0)
            if backend in TRANSPORTS:
                for size in (256, 8192):
                    add("coordinator-cache", size, mix(read=100), 1, 8192,
                        cohort="coordinator_cache_configured", cache=128 * 1024**2, warmup=1024)
                    add("owner-cache", size, mix(read=100), 1, 8192,
                        cohort="owner_cache_configured", owner_cache=128 * 1024**2, warmup=1024)
            if backend == "tidehunter_efa_native":
                for size in (256, 8192):
                    add("registered-ram", size, mix(read=100), 1, 8192,
                        cohort="registered_ram_configured", warmup=1024)
                    block[-1]["resident_reads"] = {
                        "source_bytes": 16 * 1024**2, "source_regions": 4096,
                        "destination_bytes": 20 * 1024**2, "destination_slots": 640,
                        "max_read_bytes": 32768, "max_handles": 4096,
                        "require_device_read": True}
            if backend == "native":
                for size in sizes:
                    for label, operation_mix, concurrency, count in (profiles[0], profiles[3]):
                        add(label, size, operation_mix, concurrency, count, cohort="native_pagecache")
            primary = [job for job in block if job["cohort"] == "primary_rf1"]
            extra = [job for job in block if job["cohort"] not in ("primary_rf1", "cold_os_cache")]
            cold = [job for job in block if job["cohort"] == "cold_os_cache"]
            # Match the common workload/overwrite history exactly across stores.
            random.Random(spec["seed"] ^ (repeat << 8)).shuffle(primary)
            random.Random(spec["seed"] ^ (repeat << 8)).shuffle(extra)
            diagnostic_jobs = []
            if diagnostics["write_profile_limit"] and backend in ("native", *TRANSPORTS):
                diagnostic_jobs = [job | {"cohort": "write_stage_diagnostics", "diagnostics": dict(diagnostics)}
                    for job in primary if job["value_size"] == 1024 and job["profile"] == "write-concurrent"]
            jobs.extend(primary + extra + diagnostic_jobs + cold)
    if len(jobs) > spec["max_cases"]:
        raise ValueError(f"matrix contains {len(jobs)} cases, exceeding explicit max_cases")
    for number, job in enumerate(jobs):
        job["case_id"] = f"{number:04d}-{job['backend']}-{job['value_size']}-{job['profile']}-{job['cohort']}-r{job['repeat']}"
    return jobs


def make_run_config(job, case_root, options, source_revision, deadline_seconds=180):
    case_root = Path(case_root)
    if not case_root.is_absolute():
        raise ValueError("absolute case root required")
    maximum = job["keys"] + job["warmup_operations"] + job["measurement_operations"] + 4096
    options = dict(options)
    if job["backend"] in TRANSPORTS:
        expected = TRANSPORTS[job["backend"]]
        selected = validate_transport(options.get("transport", expected))
        if protocol_key(selected) != protocol_key(expected):
            raise ValueError("runtime transport differs from research case provider/security")
        options["transport"] = selected
    config = {"schema_version": 1, "backend": {"kind": runtime_backend(job["backend"]), "options": options},
        "seed": 1775 + job["repeat"],
        "namespace": "cloud-" + hashlib.blake2b(job["case_id"].encode(), digest_size=12).hexdigest(),
        "dataset": {"keys": job["keys"], "value_size": job["value_size"], "entropy": 1.0},
        "workload": {"distribution": job["distribution"], "mix": job["mix"], "batch_size": 32},
        "execution": {"mode": "open_loop" if job["offered_ops_per_second"] else "closed_loop",
            "concurrency": job["concurrency"], "queue_capacity": 128,
            "request_timeout_ms": 10000, "offered_ops_per_second": job["offered_ops_per_second"]},
        "phases": {"preload": True, "warmup_operations": job["warmup_operations"],
            "aging_operations": 0, "measurement_operations": job["measurement_operations"]},
        "qualification_only": False, "source_revision": source_revision,
        "measurement_gate": {"ready_file": str(case_root / "measurement.ready"),
            "release_file": str(case_root / "measurement.release"), "timeout_seconds": 120},
        "measurement_completion_gate": {"ready_file": str(case_root / "measurement.complete"),
            "release_file": str(case_root / "measurement.observed"), "timeout_seconds": 120},
        "limits": {"max_keys": max(job["keys"], 1000000), "max_bytes": 16 * 1024**3,
            "max_operations": maximum, "deadline_seconds": max(deadline_seconds, 600 if job["keys"] > 65536 else deadline_seconds),
            "shutdown_grace_seconds": 20}}
    if "diagnostics" in job:
        config["diagnostics"] = job["diagnostics"]
    return config


def admit_measurement_read_evidence(directory):
    result = Path(directory) / "result"
    path = result / "measurement-read-evidence.json"
    manifest_path = result / "manifest.json"
    marker = None
    if manifest_path.is_file():
        marker = json.loads(manifest_path.read_text()).get("run", {}).get("measurement_read_evidence_schema_version")
    if marker is not None and (type(marker) is not int or marker != 1):
        raise ValueError("unsupported measurement read evidence capability")
    if not path.is_file():
        if marker == 1:
            raise ValueError("advertised measurement read evidence artifact is missing")
        return None  # Older source revisions did not advertise this artifact.
    if path.stat().st_size > 256 * 1024:
        raise ValueError("measurement read evidence exceeds bounded artifact size")
    data = json.loads(path.read_text())
    if (not isinstance(data, dict) or type(data.get("schema_version")) is not int
            or data["schema_version"] != 1 or data.get("scope") != "measurement_boundary"
            or type(data.get("measurement_attempted")) is not bool
            or type(data.get("measurement_drained")) is not bool
            or not isinstance(data.get("deltas"), dict)
            or not all(isinstance(data.get(k), (dict, type(None))) for k in ("start", "end"))):
        raise ValueError("invalid measurement read evidence schema")
    observer_path = str(Path(__file__).resolve().parent / "observability")
    if observer_path not in sys.path:
        sys.path.insert(0, observer_path)
    import campaign_exporter
    campaign_exporter.measurement_read_metrics(data, {})
    return {"path": "result/measurement-read-evidence.json",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "observed": data}


def admit_write_diagnostics(directory, job, *, completed):
    path = Path(directory) / "result/write-diagnostics.json"
    requested = job.get("diagnostics", {}).get("write_profile_limit", 0)
    if not path.is_file():
        if completed and requested:
            raise ValueError("requested write-stage diagnostics artifact is missing")
        return None
    if path.stat().st_size > 64 * 1024:
        raise ValueError("write-stage diagnostics exceed the bounded artifact size")
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or not isinstance(data.get("selection"), dict):
        raise ValueError("write-stage diagnostics require object and selection evidence")
    selection = data.get("selection", {})
    if (type(data.get("schema_version")) is not int or data["schema_version"] != 1
            or data.get("backend") != job["backend"] or data.get("phase") != "measurement"
            or selection.get("requested_limit") != requested
            or type(data.get("phase_attempted")) is not bool or type(data.get("runtime_enabled")) is not bool
            or type(selection.get("observed_samples")) is not int or selection["observed_samples"] < 0):
        raise ValueError("write-stage diagnostics schema, backend, phase or requested limit differs")
    if completed and requested and (not data["phase_attempted"] or not data["runtime_enabled"]
            or selection["observed_samples"] == 0 or not isinstance(data.get("profile"), dict) or data.get("error")):
        raise ValueError("requested write-stage diagnostics did not observe real measurement samples")
    observer_path = str(Path(__file__).resolve().parent / "observability")
    if observer_path not in sys.path:
        sys.path.insert(0, observer_path)
    import campaign_exporter
    campaign_exporter.write_diagnostic_metrics(data, {})
    return {"path": "result/write-diagnostics.json", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "observed": data}


def summarize_case(directory):
    directory = Path(directory)
    receipt = json.loads((directory / "receipt.json").read_text())
    job = receipt["job"]
    value = {"case_id": job["case_id"], "job": job, "status": receipt.get("status"),
             "error": receipt.get("error"), "measurement_evidence": receipt.get("measurement_evidence"),
             "counter_evidence": receipt.get("counter_evidence")}
    read_interval = admit_measurement_read_evidence(directory)
    if read_interval is not None:
        if read_interval != receipt.get("measurement_read_evidence"):
            raise ValueError("measurement read evidence differs from its receipt integrity binding")
        value["measurement_read_evidence"] = read_interval
    elif receipt.get("measurement_read_evidence") is not None:
        raise ValueError("measurement read evidence artifact missing")
    resource_binding = receipt.get("resource_evidence")
    if resource_binding is not None:
        if resource_binding.get("path") != "resource-interval.json":
            raise ValueError("resource evidence path differs from the owned interval artifact")
        artifact = directory / "resource-interval.json"
        resources = json.loads(artifact.read_text())
        if (hashlib.sha256(artifact.read_bytes()).hexdigest() != resource_binding.get("sha256")
                or resources != resource_binding.get("observed")):
            raise ValueError("resource interval evidence differs from its integrity binding")
        frames = []
        for field, filename in (("before", "measurement-before.json"), ("end", "measurement-end.json")):
            path = directory / filename
            boundary = receipt.get("measurement_evidence", {})
            if (boundary.get(field) != filename
                    or hashlib.sha256(path.read_bytes()).hexdigest() != boundary.get(field + "_sha256")):
                raise ValueError("resource boundary evidence differs from its integrity binding")
            frames.append([host["resource_accounting"] for host in json.loads(path.read_text())["hosts"]])
        if resource_accounting.interval(*frames) != resources:
            raise ValueError("resource interval cannot be reproduced from owned boundary counters")
        value["resource_evidence"] = resource_binding
    diagnostics = admit_write_diagnostics(directory, job, completed=receipt.get("status") == "completed")
    if diagnostics is not None:
        if diagnostics != receipt.get("write_diagnostics"):
            raise ValueError("write-stage diagnostics differ from their receipt integrity binding")
        value["write_diagnostics"] = diagnostics
    names = ("manifest.json", "summary.json", "samples.csv", "metrics.prom")
    if receipt.get("status") != "completed" and not receipt.get("raw_artifacts_validated"):
        return {**value, "metrics_unavailable": "failed before trustworthy raw artifact admission"}
    if any(name not in receipt.get("artifacts_sha256", {}) for name in names):
        raise ValueError("admitted raw artifacts lack their complete integrity binding")
    result = directory / "result"
    config = json.loads((directory / "config.json").read_text())
    normalized = json.loads((directory / "normalized-config.json").read_text())
    analyze.verify_normalization(config, normalized)
    manifest = json.loads((result / "manifest.json").read_text())
    summary = json.loads((result / "summary.json").read_text())
    if manifest.get("status") not in ("completed", "failed") or summary.get("status") != manifest.get("status"):
        raise ValueError("manifest and summary disagree about the actual core status")
    if receipt.get("status") == "completed" and manifest.get("status") != "completed":
        raise ValueError("completed receipt contradicts failed core result")
    if manifest["run"]["config"] != normalized:
        raise ValueError("executed configuration differs from admission")
    if manifest["run"]["binary"]["blake2b512"] != receipt["binary_blake2b512"]:
        raise ValueError("actual executable differs from admitted release binary")
    if manifest.get("backend_info") != summary.get("backend"):
        raise ValueError("manifest and summary disagree about the executed backend")
    for name in names:
        if hashlib.sha256((result / name).read_bytes()).hexdigest() != receipt["artifacts_sha256"][name]:
            raise ValueError("raw result checksum differs from its original admission")
    samples = list(analyze.read_samples(result / "samples.csv"))
    if not samples:
        return {**value, "backend": summary["backend"],
                "metrics_unavailable": "validated execution has no observed measurement calls"}
    totals = analyze.aggregate(samples)
    elapsed = (max(row["completed_ns"] for row in samples) - min(row["scheduled_ns"] for row in samples)) / 1e9
    if elapsed <= 0:
        raise ValueError("measurement exposure must be positive")
    good = [row for row in samples if row["outcome"] in ("ok", "success")]
    totals.update({"elapsed_seconds": elapsed, "exposure": "observed raw measurement span",
        "planned_measurement_calls": config["phases"]["measurement_operations"],
        "measurement_rows_complete": len(samples) == config["phases"]["measurement_operations"],
        "success_calls_per_second": len(good) / elapsed,
        "success_logical_keys_per_second": sum(row["logical_keys"] for row in good) / elapsed,
        "success_value_mib_per_second": sum(row["bytes"] for row in good) / elapsed / 1024**2})
    for population in totals["populations"]:
        selected = [row for row in samples if row["operation"] == population["operation"]
                    and row["outcome"] == population["outcome"]]
        for kind in ("api", "response", "scheduling"):
            values = [row[kind + "_ns"] for row in selected if row[kind + "_ns"] is not None]
            population[kind + "_mean_ns"] = statistics.mean(values) if values else None
            population[kind + "_min_ns"] = min(values) if values else None
            population[kind + "_stddev_ns"] = statistics.pstdev(values) if values else None
        population["tail_warning"] = "p99 descriptive below 10000 calls; p999 descriptive below 100000 calls"
    return {**value, "backend": summary["backend"], "metrics": totals}



def write_csv_report(root, results):
    planned_repeats = sorted({row["job"]["repeat"] for row in results})
    repetition_mode = "single" if len(planned_repeats) == 1 else "multiple"
    fields = ["case_id", "status", "backend", "cohort", "profile", "repeat",
              "planned_independent_repetitions", "repetition_mode", "timing_stddev_scope", "value_bytes", "keys",
              "concurrency", "offered_ops_per_second", "observed_calls", "planned_calls",
              "measurement_rows_complete", "successful_calls", "unsuccessful_calls", "elapsed_seconds",
              "case_success_calls_per_second", "case_success_logical_keys_per_second", "case_success_value_mib_per_second",
              "operation", "outcome", "population_calls", "population_logical_keys",
              "acknowledgement", "consistency", "replication_factor", "measurement_boundary",
              "get_many_strategy", "batch_atomicity", "observed_topology", "effective_configuration", "resource_evidence"]
    fields += [f"{kind}_{stat}_us" for kind in ("api", "response", "scheduling")
               for stat in ("mean", "min", "stddev")]
    fields += [f"{kind}_{stat}_us" for kind in ("api", "response", "scheduling")
               for stat in ("p50", "p95", "p99", "p999", "max")]
    fields += ["error", "metrics_unavailable"]
    with (root / "comparison.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for result in results:
            job, metrics = result["job"], result.get("metrics", {})
            row = {key: job.get(key) for key in ("backend", "cohort", "profile", "repeat", "keys", "concurrency", "offered_ops_per_second")}
            row.update(case_id=result["case_id"], status=result["status"], value_bytes=job["value_size"],
                       error=result.get("error"), metrics_unavailable=result.get("metrics_unavailable"))
            row.update(planned_independent_repetitions=len(planned_repeats), repetition_mode=repetition_mode,
                       timing_stddev_scope="within_case_population")
            backend = result.get("backend", {})
            row.update({key: backend.get(key) for key in ("acknowledgement", "consistency",
                       "replication_factor", "measurement_boundary", "get_many_strategy", "batch_atomicity")})
            row.update(observed_topology=analyze.canonical(backend.get("topology")),
                       effective_configuration=analyze.canonical(backend.get("configuration")))
            row["resource_evidence"] = analyze.canonical(result.get("resource_evidence"))
            row.update({target: metrics.get(source) for target, source in (
                ("observed_calls", "offered_calls"), ("planned_calls", "planned_measurement_calls"),
                ("measurement_rows_complete", "measurement_rows_complete"), ("successful_calls", "success_calls"),
                ("unsuccessful_calls", "unsuccessful_calls"), ("elapsed_seconds", "elapsed_seconds"),
                ("case_success_calls_per_second", "success_calls_per_second"),
                ("case_success_logical_keys_per_second", "success_logical_keys_per_second"),
                ("case_success_value_mib_per_second", "success_value_mib_per_second"))})
            for population in metrics.get("populations", [None]):
                flat = dict(row)
                if population is not None:
                    flat.update(operation=population["operation"], outcome=population["outcome"],
                                population_calls=population["count"], population_logical_keys=population["logical_keys"])
                    for kind in ("api", "response", "scheduling"):
                        for stat in ("mean", "min", "stddev"):
                            number = population[kind + "_" + stat + "_ns"]
                            flat[f"{kind}_{stat}_us"] = number / 1000 if number is not None else None
                    for kind in ("api", "response", "scheduling"):
                        for stat in ("p50", "p95", "p99", "p999", "max"):
                            number = population[kind + "_ns"][stat]
                            flat[f"{kind}_{stat}_us"] = number / 1000 if number is not None else None
                writer.writerow(flat)


def report(root):
    root = Path(root)
    planned = json.loads((root / "matrix.json").read_text())
    results = []
    for job in planned["jobs"]:
        directory = root / "cases" / job["case_id"]
        if (directory / "receipt.json").exists():
            results.append(summarize_case(directory))
        else:
            results.append({"case_id": job["case_id"], "job": job, "status": "not_run"})
    planned_repeats = sorted({job["repeat"] for job in planned["jobs"]})
    repetition_mode = "single" if len(planned_repeats) == 1 else "multiple"
    repetition_note = ("One planned independent repetition; no run-to-run variability estimate or confidence/significance claim."
                       if repetition_mode == "single" else
                       f"{len(planned_repeats)} planned independent repetitions; results below remain per-case sample statistics, not pooled run quantiles.")
    value = {"schema_version": 1, "complete": all(row["status"] == "completed" for row in results),
        "planned_cases": len(results), "completed_cases": sum(row["status"] == "completed" for row in results),
        "cases": results,
        "planned_repeats": planned_repeats, "planned_independent_repetitions": len(planned_repeats),
        "repetition_mode": repetition_mode,
        "planned_backends": sorted({job["backend"] for job in planned["jobs"]}),
        "current_backend_coverage": set(BACKENDS) <= {job["backend"] for job in planned["jobs"]},
        "interpretation": ["Common RF1 API experiment, not equivalent HA/transaction/security guarantees.",
            "API-call latency for get_many is the complete ordered group, not latency per key.",
            "OS-cache eviction does not evict engine caches; actual device-I/O intervals are retained.",
            "Valkey GET is memory-resident even after OS-cache eviction; AOF always is write acknowledgment, not a value-WAL-backed read.",
            "No Sui checkpoint/application or sustainable infinite-aging claim."]}
    local.dump(root / "comparison.json", value)
    write_csv_report(root, results)
    lines = ["# Cloud distributed-KV comparison", "", f"Completed {value['completed_cases']}/{len(results)} planned cases.", "",
             "Raw per-operation API, response and scheduling latency are preserved; failed/rejected arrivals remain in denominators.", "",
             repetition_note, "Timing standard deviations describe within-case populations, not run-to-run variability.", "",
             "| Case/status | Backend | Value B | Cohort | Operation/outcome | N | Case calls/s | API mean µs | API p50 µs | API p95 µs | API p99 µs | Response p99 µs |", "|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for row in results:
        if "metrics" not in row:
            lines.append(f"| {row['case_id']} | {row['job']['backend']} | {row['job']['value_size']} | {row['job']['cohort']} | {row['status']} | — | — | — | — | — | — | — |")
            continue
        for population in row["metrics"]["populations"]:
            def us(value):
                return f"{value / 1000:.3f}" if value is not None else "—"
            lines.append(f"| {row['case_id']} ({row['status']}) | {row['job']['backend']} | {row['job']['value_size']} | {row['job']['cohort']} | {population['operation']}/{population['outcome']} | {population['count']} | {row['metrics']['success_calls_per_second']:.2f} | {us(population['api_mean_ns'])} | {us(population['api_ns']['p50'])} | {us(population['api_ns']['p95'])} | {us(population['api_ns']['p99'])} | {us(population['response_ns']['p99'])} |")
    lines.extend(["", "Quantiles are exact nearest-rank raw-sample quantiles, not histogram bucket bounds. Tail precision depends on population size. Compare only matching size/workload/cohort/acknowledgement; consult comparison.json for full configuration, per-repeat logical-key throughput, bandwidth, errors and additional quantiles.", ""])
    (root / "comparison.md").write_text("\n".join(lines))
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--spec", type=Path, default=Path(__file__).with_name("cloud-campaign.json"))
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--backends", help="comma-separated nonempty subset of tracked backend IDs")
    plan.add_argument("--write-profile-limit", type=int,
                      help="diagnostic-only 1KiB write cohort for native/Tidehunter; leaves normal plans unprofiled")
    summary = commands.add_parser("report")
    summary.add_argument("--root", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "plan":
        spec = json.loads(args.spec.read_text())
        if args.write_profile_limit is not None:
            spec["diagnostics"] = {"write_profile_limit": args.write_profile_limit}
        if args.backends is not None:
            spec["backends"] = select_backends(args.backends, spec.get("backends"), set(BACKENDS))
        else:
            spec["backends"] = select_backends(None, spec.get("backends"), set(BACKENDS))
        value = {"schema_version": 1, "spec": spec, "jobs": make_matrix(spec)}
        local.dump(args.output, value)
        print(json.dumps({"planned_cases": len(value["jobs"]),
                          "hard_deadline_seconds": spec["hard_deadline_seconds"]}))
    else:
        value = report(args.root)
        print(json.dumps({key: value[key] for key in ("complete", "planned_cases", "completed_cases")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
