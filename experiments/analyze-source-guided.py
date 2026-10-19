#!/usr/bin/env python3
"""Recompute source-guided campaign summaries from raw samples and audit cleanup."""

import argparse
import csv
import hashlib
import json
import math
import re
import statistics
from collections import defaultdict
from itertools import product
from pathlib import Path


SHAPE = ("value_bytes", "batch_size", "window")
METRICS = ("phase_wall_ns", "measured_api_ns", "mean_ns", "p50_ns", "p95_ns", "p99_ns", "max_ns")


def rows(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


def shape(row):
    return tuple(int(row[key]) for key in SHAPE)


def save_csv(path, data):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(dict.fromkeys(key for row in data for key in row)))
        writer.writeheader()
        writer.writerows(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.input.resolve()
    assert (source / "COMPLETE").is_file(), "campaign incomplete"
    plan = json.loads((source / "plan.json").read_text())
    repetitions = plan["repetitions"]
    configs = [entry[0] for entry in plan["configs"]]
    expected_shapes = plan["case_count"] // repetitions // len(configs)
    common = dict(zip(plan["common"][::2], plan["common"][1::2]))
    selected_shapes = set(product(*(map(int, common[flag].split(",")) for flag in ("--values", "--batch-sizes", "--windows"))))
    assert len(selected_shapes) == expected_shapes
    for name, binary in plan["binaries"].items():
        assert hashlib.sha256(Path(binary).read_bytes()).hexdigest() == plan["binary_sha256"][name]
    candidate = Path(plan["binaries"]["candidate"]).parent
    manifest = json.loads((candidate / "source-manifest.json").read_text())
    assert manifest["binary_sha256"] == plan["binary_sha256"]["candidate"]
    assert manifest["source_sha256"] == json.loads((candidate / "source-before.json").read_text())
    summary, stages, profiles = defaultdict(list), defaultdict(list), defaultdict(list)
    resources, known_nodes = [], set()
    sample_count = phase_count = case_count = 0
    for rep in range(repetitions):
        for config in configs:
            label = f"{config}-r{rep}"
            folder = source / label
            assert (folder / "COMPLETE").is_file(), label
            resource = json.loads((source / f"{label}.resources.json").read_text())
            assert resource["exit_code"] == 0 and not resource["remaining_sampled_pids"], label
            resources.append({"name": label, **resource})
            raw = defaultdict(list)
            for row in rows(folder / "samples.csv"):
                raw[shape(row), row["phase"]].append(row)
                sample_count += 1
            shapes = set()
            for row in rows(folder / "summary.csv"):
                case, phase = shape(row), row["phase"]
                shapes.add(case)
                measurements = raw.pop((case, phase))
                times = sorted(int(sample["latency_ns"]) for sample in measurements)
                operations = sum(int(sample["logical_operations"]) for sample in measurements)
                assert int(row["samples"]) == len(times)
                assert int(row["logical_operations"]) == operations
                assert int(row["measured_api_ns"]) == sum(times)
                assert math.isclose(float(row["mean_ns"]), statistics.mean(times), rel_tol=1e-12, abs_tol=1e-8)
                assert {int(sample["sample"]) for sample in measurements} == set(range(len(times)))
                for percentile in (50, 95, 99):
                    assert int(row[f"p{percentile}_ns"]) == times[math.ceil(len(times) * percentile / 100) - 1]
                assert int(row["max_ns"]) == times[-1]
                rate = operations * 1e9 / max(1, int(row["phase_wall_ns"]))
                assert math.isclose(float(row["instrumented_phase_ops_per_sec"]), rate, rel_tol=1e-12, abs_tol=1e-8)
                for metric in METRICS:
                    summary[config, case, phase, metric].append(float(row[metric]))
                phase_count += 1
            assert not raw and shapes == selected_shapes, label
            case_count += len(shapes)
            assert (source / f"{label}.stdout.log").read_text().count("all_reopened_keys_verified=true") == expected_shapes
            for path in folder.glob("*/node-*/start-*.process-status.txt"):
                text = path.read_text()
                known_nodes.add(int(re.search(r"^Pid:\s+(\d+)", text, re.M)[1]))
                assert int(re.search(r"^VmSwap:\s+(\d+)\s+kB", text, re.M)[1]) == 0
            for row in rows(folder / "stages.csv"):
                if row["counter"] in ("cache_hits", "cache_inserts"):
                    assert int(row["delta"]) == 0, (label, "unexpected value cache use")
                stages[config, shape(row), row["phase"], row["counter"]].append(int(row["delta"]))
            for row in rows(folder / "remote_read_profiles.csv"):
                profiles[config, shape(row)].append(row)
    assert case_count == plan["case_count"]
    assert all(len(values) == repetitions for values in summary.values())
    assert not [pid for pid in known_nodes if Path(f"/proc/{pid}").exists()], "known storage PID still exists"
    medians = {key: statistics.median(values) for key, values in summary.items()}
    comparison = []
    pairs = [
        ("serial-control-tcp", "previous-tcp"),
        ("parallel-preparation-tcp", "serial-control-tcp"),
        ("prefetch-tcp", "serial-control-tcp"),
        ("combined-tcp", "previous-tcp"),
        ("combined-tcp", "current-native"),
        ("current-native", "previous-native"),
    ]
    for numerator, denominator in pairs:
        for (config, case, phase, metric), value in medians.items():
            baseline = medians.get((denominator, case, phase, metric))
            if config == numerator and baseline is not None and baseline > 0:
                comparison.append({
                    "numerator": numerator, "denominator": denominator, **dict(zip(SHAPE, case)),
                    "phase": phase, "metric": metric, "numerator_median": value,
                    "denominator_median": baseline, "ratio": value / baseline,
                    "change_percent": 100 * (value / baseline - 1),
                })
    median_rows = [{"config": config, **dict(zip(SHAPE, case)), "phase": phase, "metric": metric,
                    "median": value, "min": min(summary[config, case, phase, metric]),
                    "max": max(summary[config, case, phase, metric])}
                   for (config, case, phase, metric), value in medians.items()]
    stage_rows = [{"config": config, **dict(zip(SHAPE, case)), "phase": phase, "counter": counter,
                   "median": statistics.median(values), "repetition_values": json.dumps(values)}
                  for (config, case, phase, counter), values in stages.items()]
    profile_rows = []
    for (config, case), values in profiles.items():
        row = {"config": config, **dict(zip(SHAPE, case)), "samples": len(values)}
        for metric in values[0]:
            if metric.endswith("_ns") or metric in ("cache_hits", "cache_misses"):
                row[metric + "_mean"] = statistics.mean(int(value[metric]) for value in values)
        profile_rows.append(row)
    audit = {
        "complete": True, "campaigns": repetitions * len(configs), "cases": case_count,
        "raw_samples": sample_count, "summary_phases": phase_count,
        "full_byte_reopen_audits": case_count, "known_storage_pids_exited": len(known_nodes),
        "peak_sampled_aggregate_rss_kib": max(resource["peak_sampled_process_tree_rss_kib"] for resource in resources),
        "swap_in_pages": sum(resource["swap_delta_pages"]["pswpin"] for resource in resources),
        "swap_out_pages": sum(resource["swap_delta_pages"]["pswpout"] for resource in resources),
        "binary_sha256": plan["binary_sha256"],
        "candidate_build": {key: value for key, value in manifest.items() if key != "source_sha256"},
        "comparison": "Median of per-run metrics per matched shape; no pooled percentiles or significance claim.",
        "limits": ["Five processes share one SSD; no independent-NVMe claim.",
                   "Application value cache is disabled for every TCP variant; native reference is unchanged.",
                   "No page-cache eviction or physical-read counter isolation; read phases follow writes and audits.",
                   "Diagnostic stages overlap; worker elapsed sums are not CPU time.",
                   "Sampled RSS sums shared mappings and may miss peaks; laptop is not a dedicated host."],
    }
    args.output.mkdir(parents=True, exist_ok=False)
    save_csv(args.output / "medians.csv", median_rows)
    save_csv(args.output / "comparison.csv", comparison)
    save_csv(args.output / "stages.csv", stage_rows)
    save_csv(args.output / "profiles.csv", profile_rows)
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    (args.output / "COMPLETE").write_text(f"{case_count} cases and raw distributions verified\n")
    print(json.dumps(audit, indent=2))


if __name__ == "__main__":
    main()
