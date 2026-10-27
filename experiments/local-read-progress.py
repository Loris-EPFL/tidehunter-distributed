#!/usr/bin/env python3
"""Compare bounded read progress and cold WAL-member I/O locally; no AWS calls."""

import argparse
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil


import sys


sys.path.insert(0, str(Path(__file__).resolve().parent))
import portable_transport


HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "local_compare", HERE / "aws-sharded-wal/local_compare.py"
)
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)
MIB = 1024 * 1024
READ_COUNTERS = ("cache_hits", "cache_misses", "cache_evictions", "wal_reads", "wal_read_bytes",
                 "resident_read_attempts", "resident_read_successes", "resident_read_fallbacks",
                 "resident_read_unsupported", "resident_read_partial", "resident_read_deferred",
                 "async_read_submissions")


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def configurations(suite):
    cases = []
    if suite in ("all", "cold"):
        cases.append(dict(label="cold-native", backend="native-sync", cold=True,
                          cache=0, read_window=32, native_workers=32))
        for policy in ("mapped", "positional"):
            for label, lanes, workers, window in (("serial", 1, 1, 1),
                                                   ("window", 4, 1, 32),
                                                   ("parallel", 4, 4, 32)):
                cases.append(dict(label=f"cold-{policy}-{label}", backend="tcp",
                                  cold=True, cache=0, read_window=window,
                                  lanes=lanes, workers=workers, policy=policy))
        for window in (1, 32):
            cases.append(dict(label=f"cold-async-window{window}", backend="tcp",
                              cold=True, cache=0, read_window=window,
                              lanes=1, workers=2, policy="async"))
    if suite in ("all", "warm"):
        for backend in ("native-sync", "tcp"):
            for cache in (0, 8 * MIB):
                cases.append(dict(label=f"warm-{backend}-cache{cache}", backend=backend,
                                  cold=False, cache=cache, read_window=32,
                                  native_workers=32, lanes=1, workers=2, policy="async"))
    if suite in ("all", "resident"):
        for policy in ("mapped", "positional", "async"):
            cases.append(dict(label=f"resident-{policy}", backend="tcp",
                              cold=False, cache=0, read_window=32,
                              lanes=1, workers=2, policy=policy))
    if suite in ("all", "pressure"):
        for frontend, shard in ((128 * 1024, 0), (0, 64 * 1024),
                                (128 * 1024, 64 * 1024)):
            cases.append(dict(label=f"pressure-front{frontend}-shard{shard}",
                              backend="tcp", cold=False, cache=frontend,
                              shard_cache=shard, pressure=True, read_window=32,
                              lanes=1, workers=2, policy="async"))
    return cases


def read_path_evidence(path, case):
    if case["backend"] != "tcp":
        return {}
    with (path / "read_path_evidence.csv").open() as stream:
        snapshots = list(csv.DictReader(stream))
    phases = {}
    expected = {("frontend", "")} | {("storage", str(n)) for n in range(5)}
    for row in snapshots:
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"malformed read-path evidence: {path}")
        identity = (row["role"], row["partition"])
        if identity not in expected or row["boundary"] not in ("before", "after"):
            raise ValueError(f"unknown read-path boundary: {path}")
        snapshot = phases.setdefault(row["phase"], {}).setdefault(row["boundary"], {})
        if identity in snapshot:
            raise ValueError(f"duplicate read-path process: {path}")
        snapshot[identity] = row
        budget = int(row["cache_budget_bytes"])
        configured = case["cache"] if row["role"] == "frontend" else case.get("shard_cache", 0)
        if budget != configured or not 0 <= int(row["cache_retained_bytes"]) <= budget:
            raise ValueError(f"cache exceeds its configured budget: {path}")
        if not 0 <= int(row["cache_metadata_bytes"]) <= int(row["cache_retained_bytes"]):
            raise ValueError(f"cache metadata is not accounted: {path}")
        if budget == 0 and int(row["cache_entries"]) != 0:
            raise ValueError(f"disabled cache retains values: {path}")
        for field, limit in (("peak_inflight_requests", 32),
                             ("peak_inflight_bytes", 64 * MIB),
                             ("peak_device_reads", 128)):
            if row["role"] == "storage" and not 0 <= int(row[field]) <= limit:
                raise ValueError(f"shard read admission bound exceeded: {path}: {field}")
    output = {}
    counters = READ_COUNTERS[:5] + tuple(
        counter for counter in READ_COUNTERS[5:] if snapshots and counter in snapshots[0])
    for phase, boundaries in phases.items():
        if set(boundaries) != {"before", "after"} or any(set(rows) != expected for rows in boundaries.values()):
            raise ValueError(f"incomplete read-path evidence: {path}: {phase}")
        totals = {f"{role}_{counter}": 0 for role in ("frontend", "storage") for counter in counters}
        for identity in expected:
            before, after = (boundaries[boundary][identity] for boundary in ("before", "after"))
            for counter in counters:
                delta = int(after[counter]) - int(before[counter])
                if delta < 0:
                    raise ValueError(f"read-path counter regressed: {path}: {phase}: {counter}")
                totals[f"{identity[0]}_{counter}"] += delta
        if case.get("pressure") and phase in ("point_get", "point_get_window"):
            if totals["storage_wal_reads"] <= 0 or totals["storage_wal_read_bytes"] <= 0:
                raise ValueError(f"cache-pressure phase did not fetch the WAL: {path}: {phase}")
            for role, budget in (("frontend", case["cache"]), ("storage", case.get("shard_cache", 0))):
                if budget and (totals[f"{role}_cache_misses"] <= 0 or totals[f"{role}_cache_evictions"] <= 0):
                    raise ValueError(f"configured cache was not pressured: {path}: {phase}: {role}")
        output[phase] = totals
    if not {"point_get", "point_get_window"} <= output.keys():
        raise ValueError(f"missing scalar/windowed read-path evidence: {path}")
    return output


def collect(path, case, value_bytes, repetition, reads):
    if not (path / "COMPLETE").is_file():
        raise ValueError(f"missing value/reopen completion: {path}")
    with (path / "summary.csv").open() as stream:
        summaries = list(csv.DictReader(stream))
    phases = {row["phase"] for row in summaries}
    if "reopen_get_audit" not in phases or "point_get_window" not in phases:
        raise ValueError(f"missing window or reopen evidence: {path}")
    paths = read_path_evidence(path, case)
    io = {}
    if case["cold"]:
        with (path / "read_io.csv").open() as stream:
            for row in csv.DictReader(stream):
                phase = io.setdefault(row["phase"], {})
                partition = int(row["partition"])
                if partition in phase:
                    raise ValueError(f"duplicate read-I/O partition: {path}")
                phase[partition] = {key: int(row[key]) for key in
                                    ("physical_read_bytes", "minor_faults", "major_faults")}
        expected = set(range(5)) if case["backend"] == "tcp" else {0}
        for phase in ("cache_evicted_get", "cache_evicted_get_many", "cache_evicted_get_window"):
            if phase not in phases or set(io.get(phase, {})) != expected:
                raise ValueError(f"incomplete cold phase evidence: {path}: {phase}")
            if sum(row["physical_read_bytes"] for row in io[phase].values()) <= 0:
                raise ValueError(f"no physical reads in cold phase: {path}: {phase}")
    rows = []
    for row in summaries:
        if not (row["phase"].startswith("point_get") or row["phase"].startswith("cache_evicted_get")):
            continue
        if (int(row["value_bytes"]), int(row["batch_size"]), int(row["window"]),
                int(row["repetition"])) != (value_bytes, 32, 32, 0):
            raise ValueError(f"unexpected benchmark dimensions: {path}")
        if int(row["logical_operations"]) != reads:
            raise ValueError(f"read count differs from requested workload: {path}: {row['phase']}")
        evidence = io.get(row["phase"], {})
        rows.append(dict(case=case["label"], value_bytes=value_bytes, repetition=repetition,
                         backend=row["backend"], phase=row["phase"],
                         cache_bytes=case["cache"], read_window=case["read_window"],
                         wal_member_read=case.get("policy") if case["backend"] == "tcp" else "native",
                         resident_first=case.get("resident_first") if case["backend"] == "tcp" else None,
                         shard_cache_bytes=case.get("shard_cache", 0),
                         native_read_workers=case.get("native_workers", 1),
                         read_lanes=case.get("lanes") if case["backend"] == "tcp" else None,
                         server_read_workers=case.get("workers") if case["backend"] == "tcp" else None,
                         samples=int(row["samples"]), logical_operations=int(row["logical_operations"]),
                         phase_wall_ns=int(row["phase_wall_ns"]), p50_ns=float(row["p50_ns"]),
                         p99_ns=float(row["p99_ns"]) if row["p99_ns"] else None,
                         **{key: sum(item[key] for item in evidence.values()) if evidence else None
                            for key in ("physical_read_bytes", "minor_faults", "major_faults")},
                         **{f"{role}_{counter}": paths.get(row["phase"], {}).get(f"{role}_{counter}")
                            for role in ("frontend", "storage")
                            for counter in READ_COUNTERS}))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suite", choices=("all", "cold", "warm", "pressure", "resident"), default="all")
    parser.add_argument("--transport", choices=("tcp", "fabric-tcp", "fabric-tcp-native"), default="tcp")
    parser.add_argument("--resident-first", choices=("true", "false"), default="true")
    parser.add_argument("--values", default="1024,8192")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--batches", type=int, default=320)
    parser.add_argument("--reads", type=int, default=4096)
    args = parser.parse_args()
    values = [int(value) for value in args.values.split(",")]
    if not values or len(set(values)) != len(values) or any(value not in (1024, 8192) for value in values):
        raise ValueError("values must be a unique subset of 1024,8192")
    if not (1 <= args.repetitions <= 5 and 32 <= args.batches <= 512 and 32 <= args.reads <= 8192):
        raise ValueError("repetitions, batches or reads exceed the bounded campaign")
    timeout = shutil.which("timeout")
    if timeout is None:
        raise RuntimeError("GNU timeout is required for the per-case process deadline")
    binary = args.candidate.resolve(strict=True)
    identity = digest(binary)
    cases = configurations(args.suite)
    for case in cases:
        if case["backend"] == "tcp":
            case["resident_first"] = args.resident_first == "true" and case["policy"] == "async"
        combined_cache = case["cache"] + 5 * case.get("shard_cache", 0)
        if case.get("pressure") and min(values) * args.batches * 32 <= 4 * combined_cache:
            raise ValueError("cache-pressure dataset must exceed combined application caches by 4x")
        if case.get("pressure") and args.reads < 1024:
            raise ValueError("cache pressure needs at least 1024 reads across the larger working set")
    args.output.mkdir()
    transport_config = args.output / "requested-configs" / "transport.json"
    transport_config.parent.mkdir()
    portable_transport.write_transport_config(
        transport_config, portable_transport.transport_for_case(args.transport)
    )
    plan = dict(binary=str(binary), binary_sha256=identity, configurations=cases,
                values=values, repetitions=args.repetitions, batches=args.batches, reads=args.reads,
                transport=args.transport, transport_config=str(transport_config),
                case_count=len(cases) * len(values) * args.repetitions,
                case_timeout_seconds=600,
                topology="five loopback storage processes on one shared SSD; no hardware RDMA",
                limits="bounded cold-start comparison, not NVMe saturation or larger-than-RAM scaling",
                cache_policy="finite on-demand caches; cold caches disabled; pressure working set "
                             "exceeds combined frontend and five shard budgets by more than 4x; "
                             "native nominal values exclude metadata",
                latency_scope="scalar/window samples are per-request; windows include queue/retirement delay; "
                              "multi-get samples cover complete calls; native windows use up to 32 shared-Db workers")
    (args.output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    rows = []
    for repetition in range(args.repetitions):
        offset = repetition % len(cases)
        order = cases[offset:] + cases[:offset]
        if repetition % 2:
            order.reverse()
        for value in values:
            for case in order:
                if digest(binary) != identity:
                    raise ValueError("benchmark executable changed during campaign")
                output = args.output / f'{case["label"]}-v{value}-r{repetition}'
                flags = ["--backend", case["backend"], "--transport-config", str(transport_config),
                         "--values", str(value), "--batch-sizes", "32", "--windows", "32",
                         "--batches", str(args.batches), "--mixed", str(min(64, args.batches // 4)),
                         "--reads", str(args.reads), "--repetitions", "1",
                         "--read-window", str(case["read_window"]),
                         "--tcp-read-cache-bytes", str(case["cache"]),
                         "--native-read-cache-bytes",
                         str(case["cache"] if case["backend"] == "native-sync" else 0),
                         "--native-read-workers", str(case.get("native_workers", 1)),
                         "--read-resource-evidence", "true",
                         "--hot-read-pass", str(not case["cold"]).lower(),
                         "--cold-read-pass", str(case["cold"]).lower()]
                if case["backend"] == "tcp":
                    flags += ["--tcp-read-lanes", str(case["lanes"]),
                              "--tcp-server-read-workers", str(case["workers"]),
                              "--tcp-wal-member-read", case["policy"],
                              "--tcp-shard-cache-bytes", str(case.get("shard_cache", 0)),
                              "--tcp-resident-first", str(case["resident_first"]).lower()]
                local.run([timeout, "--signal=TERM", "--kill-after=5s", "600s",
                           str(binary), "--output", str(output), *flags], output)
                rows.extend(collect(output, case, value, repetition, args.reads))
                with (args.output / "comparison.csv").open("w", newline="") as stream:
                    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
                    writer.writeheader()
                    writer.writerows(rows)
    if digest(binary) != identity:
        raise ValueError("benchmark executable changed at campaign completion")
    (args.output / "COMPLETE").write_text("all requested read comparisons and value/reopen audits passed\n")


if __name__ == "__main__":
    main()
