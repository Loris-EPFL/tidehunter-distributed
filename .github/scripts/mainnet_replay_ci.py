#!/usr/bin/env python3
"""Run and audit the committed native replay; never gate on timing values."""
import argparse
import collections
import csv
import json
import math
import os
from pathlib import Path
import subprocess

LABEL = ("GitHub-hosted runner, informational, archive-derivable node KV subset, "
         "not decision data")
MANIFEST_SHA256 = "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa"
PER_REPEAT = dict(store_gets=507, overlay_demands=116, put_members=707,
                  write_calls=9, checkpoints=1, transactions=45)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def audit(result, repetitions):
    summary = json.loads((result / "summary.json").read_text())
    manifest = json.loads((result / "manifest.json").read_text())
    phase = summary["phase_counts"]
    profile = phase["profile"]
    require(summary["status"] == phase["run_status"] == manifest["status"] == "completed",
            "replay did not complete")
    require(phase["error"] is None, "replay reported an error")
    for key in ("unknown_outcomes", "unresolved_workers"):
        require(phase[key] == 0, f"nonzero {key}")
    for key in ("rejected_calls", "timed_out_calls"):
        require(profile[key] == 0, f"nonzero {key}")
    for key in ("errors_or_wrong_values", "timeouts_or_unknown_mutations"):
        require(manifest[key] == 0, f"nonzero {key}")
    require(manifest["raw_samples_complete"] is True
            and manifest["raw_sample_writer_error"] is None, "incomplete raw samples")
    require(profile["scope"] == "archive-derivable node KV subset", "wrong replay scope")
    for key, count in PER_REPEAT.items():
        require(phase["counts"][key] == repetitions * count, f"wrong total {key}")
    calls = repetitions * 516
    for key in ("offered_calls", "successful_calls"):
        require(profile[key] == calls, f"wrong {key}")
    for key in ("completed_operations", "succeeded_operations"):
        require(manifest[key] == calls, f"wrong {key}")
    table = profile["tables"]["get:objects"]
    require(table["members"] == repetitions * 507
            and table["value_bytes"] == repetitions * 233123, "wrong checked read bytes")
    per_rep = collections.defaultdict(collections.Counter)
    with (result / "samples.csv").open(newline="") as stream:
        for row in csv.DictReader(stream):
            require(row["phase"] == "measurement" and row["outcome"] == "success",
                    "unsuccessful or nonmeasurement sample")
            require(row["source_checkpoint"] == "345683547", "wrong source checkpoint")
            rep = int(row["repetition"])
            require(0 <= rep < repetitions, "unexpected repetition")
            operation, members = row["operation"], int(row["members"])
            if operation == "get":
                require(members == 1, "get must have one member")
                per_rep[rep]["store_gets"] += 1
                per_rep[rep]["checked_value_bytes"] += int(row["bytes"])
            elif operation == "commit":
                per_rep[rep]["write_calls"] += 1
                per_rep[rep]["put_members"] += members
            else:
                raise ValueError(f"unexpected operation: {operation}")
    expected = dict(store_gets=507, write_calls=9, put_members=707, checked_value_bytes=233123)
    require(set(per_rep) == set(range(repetitions)), "missing repetition samples")
    for rep, counts in per_rep.items():
        require(dict(counts) == expected, f"wrong counts/bytes in repetition {rep}: {counts}")
    elapsed = profile["measurement_elapsed_seconds"]
    require(math.isfinite(elapsed) and elapsed > 0, "invalid elapsed time")
    latencies = {}
    for operation in ("get", "commit"):
        api = profile["latency"][f"operation:{operation}:success"]["api"]
        latencies[operation] = {key: api[key] for key in ("count", "p50_ns", "p99_ns")}
    return dict(status="completed", label=LABEL, repetitions=repetitions,
                measurement_elapsed_seconds=elapsed,
                checkpoints_per_second=profile["checkpoints_per_second"],
                logical_ops_per_second=profile["logical_ops_per_second"],
                api_calls_per_second=profile["api_calls_per_second"],
                latency=latencies, counts=phase["counts"], resources=phase["resources"],
                per_repetition_api_counts_and_bytes="verified from every raw sample",
                overlay_demands="116 per repetition inferred from admitted trace and exact total; no API samples",
                duration_qualification="finite 512 repetitions; not sustained/30 s")


def run(args):
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    benchmark = args.mode == "benchmark"
    repetitions = 512 if benchmark else 2
    config = dict(
        schema_version=1, case_id=f"mainnet-native-{args.mode}",
        backend=dict(kind="native", options=dict(
            path=str(root / "db"), native_wal_geometry="native_default" if benchmark else "small",
            native_shards=1, durability="page_cache")),
        workload=dict(kind="sui_mainnet_replay",
                      manifest_path=str(Path("experiments/sui-mainnet/fixture/manifest.json").resolve()),
                      manifest_sha256=MANIFEST_SHA256, repetitions=repetitions,
                      minimum_measurement_seconds=0.0),
        execution=dict(mode="closed_loop", concurrency=1 if benchmark else 4,
                       client_shards=1, queue_capacity=1 if benchmark else 4,
                       request_timeout_ms=30000 if benchmark else 10000),
        limits=dict(max_keys=600000 if benchmark else 10000,
                    max_bytes=1073741824 if benchmark else 67108864,
                    max_operations=1000000 if benchmark else 10000,
                    deadline_seconds=120 if benchmark else 60, shutdown_grace_seconds=10 if benchmark else 5))
    (root / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    report = dict(status="failed", label=LABEL,
                  duration_qualification="finite 512 repetitions; not sustained/30 s" if benchmark else "correctness only")
    try:
        with (root / "run.log").open("w") as log:
            subprocess.run([str(args.binary.resolve()), "--config", str(root / "config.json"),
                            "--output", str(root / "result")], stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=180 if benchmark else 90)
        report = audit(root / "result", repetitions)
        if not benchmark:
            report["duration_qualification"] = "correctness only"
    except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError) as error:
        report["error"] = str(error)
        raise
    finally:
        rendered = json.dumps(report, indent=2, allow_nan=False) + "\n"
        (root / "ci-summary.json").write_text(rendered)
        print(rendered)
        if benchmark and os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
                stream.write(f"### Mainnet replay benchmark\n\n{LABEL}\n\n"
                             "Finite R=512; actual elapsed time only, **not sustained/30 s**. "
                             "Rates are informational and never build thresholds.\n\n```json\n"
                             + rendered + "```\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("correctness", "benchmark"))
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())
