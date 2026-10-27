#!/usr/bin/env python3
"""Retain failed coverage and compare explicit cohorts without mixing contracts."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import sys

import analyze
import local

MAX_RUNS = 10000
MIN_REPETITIONS = 5
BOOTSTRAP_RESAMPLES = 2000


def interval(values, estimator, seed=1775):
    """Percentile bootstrap over independent RUNS, never over correlated calls."""
    if not values or any(not math.isfinite(value) or value < 0 for value in values):
        raise ValueError("uncertainty requires finite nonnegative run-level observations")
    result = {"runs": len(values), "estimate": estimator(values), "minimum": min(values),
              "maximum": max(values), "confidence": None, "interval": None,
              "method": "run-level percentile bootstrap; approximate, not a guarantee"}
    if len(values) < MIN_REPETITIONS:
        result["status"] = "insufficient_independent_repetitions"
        return result
    rng = random.Random(seed)
    estimates = sorted(estimator(rng.choices(values, k=len(values))) for _ in range(BOOTSTRAP_RESAMPLES))
    result.update(status="estimated", confidence=0.95,
                  interval=[analyze.quantile(estimates, .025), analyze.quantile(estimates, .975)],
                  resamples=BOOTSTRAP_RESAMPLES, bootstrap_seed=seed)
    return result


def repetition_uncertainty(paths, accounting):
    throughput = [row["achieved_success_calls_per_second"] for row in accounting]
    if any(value is None for value in throughput):
        raise ValueError("run has no measured throughput exposure")
    populations = []
    per_run = []
    identities = set()
    for path in paths:
        identity = analyze.independent_run_identity(path)
        if identity in identities:
            raise ValueError("duplicated independent-run identity; copied results are not repetitions")
        identities.add(identity)
        groups = {}
        for row in analyze.read_samples(path / "result/samples.csv"):
            groups.setdefault((row["operation"], row["outcome"]), []).append(row)
        per_run.append(groups)
    keys = sorted(set().union(*(set(groups) for groups in per_run)))
    for operation, outcome in keys:
        groups = [run.get((operation, outcome), []) for run in per_run]
        for metric in ("api_ns", "response_ns", "scheduling_ns"):
            observations = [[row[metric] for row in group if row[metric] is not None] for group in groups]
            counts = [len(values) for values in observations]
            for percentile in (.50, .95, .99, .999):
                values = [analyze.quantile(run, percentile) for run in observations if run]
                item = {"operation": operation, "outcome": outcome, "metric": metric,
                        "percentile": percentile, "total_runs": len(paths), "sample_counts": counts,
                        "estimand": "median of independent per-run conditional percentiles; NOT the pooled percentile"}
                item["observed_run_percentiles"] = values
                if not values or any(count == 0 for count in counts):
                    item.update(status="population_missing_in_one_or_more_runs", interval=None)
                elif percentile >= .95 and min(counts) * (1 - percentile) < 10:
                    item.update(status="insufficient_tail_population", interval=None,
                                observed_run_percentiles=values,
                                policy="at least ten expected observations beyond each requested tail per run")
                else:
                    item.update(interval(values, statistics.median))
                populations.append(item)
    return {"throughput_mean_calls_per_second": interval(throughput, statistics.mean),
            "latency_between_run_variation": populations,
            "caveat": "Independent deployments/runs are the resampling units. This does not establish HA, steady state, or workload representativeness."}


def discover(roots):
    paths, seen = [], set()
    for root in roots:
        root = root.resolve(strict=True)
        candidates = [root] if (root / "receipt.json").is_file() else root.iterdir()
        for candidate in candidates:
            if candidate.is_symlink():
                raise ValueError("symlink result directories are not admitted")
            if not candidate.is_dir() or not (candidate / "receipt.json").is_file():
                continue
            candidate = candidate.resolve(strict=True)
            if candidate in seen:
                raise ValueError("duplicate result directory")
            seen.add(candidate)
            paths.append(candidate)
            if len(paths) > MAX_RUNS:
                raise ValueError("report exceeds bounded run population")
    if not paths:
        raise ValueError("no campaign receipts found")
    return sorted(paths)


def resource_peak(paths):
    peak = None
    for path in paths:
        file = path / "client-resources.jsonl"
        if not file.is_file():
            continue
        with file.open() as stream:
            for line in stream:
                row = json.loads(line)
                value = row.get("aggregate_rss_bytes")
                if value is not None:
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        raise ValueError("invalid sampled RSS")
                    peak = max(peak or 0, value)
    return peak


def make_report(paths):
    coverage, cohorts = [], {}
    independent = set()
    for path in paths:
        receipt = json.loads((path / "receipt.json").read_text())
        item = {"run": str(path), "status": receipt.get("status"), "job": receipt.get("job"),
                "claim_boundary": receipt.get("claim_boundary"), "error": receipt.get("error")}
        if item["status"] != "completed":
            item["classification"] = "failed_or_incomplete"
        elif item["claim_boundary"] == "API qualification only":
            item["classification"] = "qualification_only"
        else:
            try:
                config = json.loads((path / "config.json").read_text())
                identity = (config.get("namespace"), receipt.get("started_at_unix"), receipt.get("binary_sha256"))
                if not all(value is not None for value in identity) or identity in independent:
                    raise ValueError("missing or duplicated independent-run identity")
                boundary = analyze.boundary(path)
                key = hashlib.sha256(analyze.canonical(boundary).encode()).hexdigest()
                # Validate all raw accounting even for a single-run cohort.
                analyze.analyze([path])
                independent.add(identity)
                cohorts.setdefault(key, []).append(path)
                item.update(classification="performance", cohort=key)
            except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError) as error:
                item.update(classification="invalid_artifacts", error=str(error))
        coverage.append(item)
    results = []
    for key, runs in sorted(cohorts.items()):
        analysis = analyze.analyze(runs)
        analysis["cohort"] = key
        analysis["uncertainty"] = repetition_uncertainty(runs, analysis["per_run_accounting"])
        analysis["peak_sampled_aggregate_client_rss_bytes"] = resource_peak(runs)
        results.append(analysis)
    return {"schema_version": 1, "coverage": coverage, "cohorts": results,
            "comparability": "Rows are separate admitted contracts, NOT pooled across databases or an automatic same-guarantee ranking.",
            "limits": "Local shared SSD; finite aging; no HA, independent-NVMe, EFA or sustainable steady-state claim.",
            "complete": all(row["classification"] in ("performance", "qualification_only") for row in coverage)}


def rows(report):
    for cohort in report["cohorts"]:
        boundary = cohort["boundary"]
        backend = boundary["backend"]
        workload = boundary["workload"]
        means = cohort["uncertainty"]["throughput_mean_calls_per_second"]
        case = hashlib.sha256(analyze.canonical(workload).encode()).hexdigest()[:12]
        embedded = backend["name"] in ("native", "rocksdb", "blobdb")
        server_cpu = sum(role["cpus"] for role in (boundary.get("deployment") or {}).get("roles", {}).values())
        for population in cohort["statistics"]["populations"]:
            yield {"cohort": cohort["cohort"], "case": case, "backend": backend["name"],
                   "tier": "Embedded references" if embedded else "Distributed application APIs",
                   "boundary": backend["measurement_boundary"], "acknowledgement": backend["acknowledgement"],
                   "consistency": backend["consistency"], "replication_factor": backend["replication_factor"],
                   "batch_atomicity": backend["batch_atomicity"], "get_many_strategy": backend["get_many_strategy"],
                   "server_roles": analyze.canonical((boundary.get("deployment") or {}).get("roles", {})),
                   "observed_topology": analyze.canonical(backend.get("observed_topology")),
                   "effective_configuration": analyze.canonical(backend.get("effective_configuration")),
                   "value_bytes": workload["dataset"]["value_size"], "dataset_keys": workload["dataset"]["keys"],
                   "distribution": analyze.canonical(workload["workload"]["distribution"]),
                   "operation_mix": analyze.canonical(workload["workload"]["mix"]),
                   "aging_operations": workload["phases"]["aging_operations"],
                   "execution_mode": workload["execution"]["mode"],
                   "offered_ops_per_second": workload["execution"].get("offered_ops_per_second"),
                   "concurrency": workload["execution"]["concurrency"],
                   "allocated_cpu_ceiling": boundary["client_budget"]["cpus"] + server_cpu,
                   "independent_runs": len(cohort["runs"]), "operation": population["operation"],
                   "outcome": population["outcome"], "samples": population["count"],
                   "success_fraction": cohort["statistics"]["success_fraction"],
                   "mean_achieved_success_calls_per_second": means["estimate"],
                   "throughput_ci95_low": means["interval"][0] if means["interval"] else None,
                   "throughput_ci95_high": means["interval"][1] if means["interval"] else None,
                   "api_p50_ns": population["api_ns"]["p50"], "api_p99_ns": population["api_ns"]["p99"],
                   "response_p50_ns": population["response_ns"]["p50"], "response_p99_ns": population["response_ns"]["p99"],
                   "peak_sampled_client_rss_bytes": cohort["peak_sampled_aggregate_client_rss_bytes"]}


def markdown(report, csv_rows):
    lines = ["# Local distributed-KV research report", "", report["comparability"], "", report["limits"], "",
             "## Coverage", "", "| Run | Classification | Status |", "|---|---|---|"]
    for row in report["coverage"]:
        lines.append(f"| {Path(row['run']).name} | {row['classification']} | {row['status']} |")
    for tier in sorted({row["tier"] for row in csv_rows}):
        lines.extend(["", "## " + tier, "",
                      "Rows can differ in acknowledgment, consistency, RF and resource budgets. This is not a same-guarantee ranking. Whole-workload QPS is repeated beside each conditional latency population, not operation-specific throughput.", "",
                      "| Case | Backend | Operation / outcome | RF | Runs | Calls | Success fraction | Workload success calls/s | API p50 µs | Response p99 µs |", "|---|---|---|---:|---:|---:|---:|---:|---:|---:|"])
        for row in csv_rows:
            if row["tier"] != tier:
                continue
            def micros(value):
                return "unavailable" if value is None else f"{value / 1000:.3f}"
            lines.append(f"| {row['case']} | {row['backend']} | {row['operation']} / {row['outcome']} | {row['replication_factor']} | {row['independent_runs']} | {row['samples']} | {row['success_fraction']:.6f} | {row['mean_achieved_success_calls_per_second']:.3f} | {micros(row['api_p50_ns'])} | {micros(row['response_p99_ns'])} |")
        lines.extend(["", "### Cases and contract details", ""])
        emitted = set()
        for row in csv_rows:
            if row["tier"] == tier and row["cohort"] not in emitted:
                emitted.add(row["cohort"])
                rate = row["offered_ops_per_second"] if row["execution_mode"] == "open_loop" else f"{row['concurrency']} closed-loop workers"
                lines.append(f"- Case `{row['case']}`, cohort `{row['cohort'][:12]}`, {row['backend']}: {row['value_bytes']} B values, {row['dataset_keys']} keys, {rate} offered calls/s, {row['aging_operations']} aging calls, CPU ceiling {row['allocated_cpu_ceiling']}. Ack `{row['acknowledgement']}`; consistency `{row['consistency']}`; atomic batch `{row['batch_atomicity']}`; GET-many `{row['get_many_strategy']}`. Boundary: {row['boundary']}.")
    lines.extend(["", "## Interpretation", "",
                  "Raw pooled quantiles are not averages of run p99 values. JSON retains per-run throughput and conditional tail variability. Confidence intervals require at least five independent runs; high tails also require a declared sample-count threshold. Missing intervals are not zero uncertainty.", "",
                  "Valkey TCP uses RF1 AOF appendfsync always for write acknowledgment, not a value-WAL-backed read path. GET remains RAM-resident even in an OS-cache-drop cohort; no cross-shard atomic PUT-batch guarantee is implied.", "",
                  "A batch's elapsed time is not individual-key latency. Failure/rejection populations remain visible. Qualification-only runs deliberately contribute no performance samples. Sampled aggregate RSS can double-count shared pages and is not total physical memory/cache usage.", ""])
    return "\n".join(lines)


def slo_frontier(report, p99_us, minimum_success, operation):
    if not math.isfinite(p99_us) or p99_us <= 0 or not 0 < minimum_success <= 1:
        raise ValueError("SLO requires positive finite p99 microseconds and success fraction in (0,1]")
    groups = {}
    for cohort in report["cohorts"]:
        boundary = cohort["boundary"]
        if boundary["workload"]["execution"]["mode"] != "open_loop":
            continue
        # A rate sweep changes only offered rate; never pool its raw latencies.
        case = json.loads(analyze.canonical(boundary))
        case["workload"]["execution"].pop("offered_ops_per_second", None)
        identity = hashlib.sha256(analyze.canonical(case).encode()).hexdigest()
        group = groups.setdefault(identity, {"backend": boundary["backend"]["name"],
            "acknowledgement": boundary["backend"]["acknowledgement"], "rates": []})
        tails = [row for row in cohort["uncertainty"]["latency_between_run_variation"]
                 if row["operation"] == operation and row["outcome"] == "success"
                 and row["metric"] == "response_ns" and row["percentile"] == .99]
        per_run = cohort["per_run_accounting"]
        enough = (bool(tails) and len(tails[0]["observed_run_percentiles"]) == len(cohort["runs"])
                  and min(tails[0]["sample_counts"], default=0) * .01 >= 10)
        fraction_pass = all(row["success_calls"] / row["offered_calls"] >= minimum_success for row in per_run)
        tail_pass = enough and all(value <= p99_us * 1000 for value in tails[0]["observed_run_percentiles"])
        group["rates"].append({
            "cohort": cohort["cohort"],
            "offered_ops_per_second": boundary["workload"]["execution"]["offered_ops_per_second"],
            "achieved_success_calls_per_second": cohort["uncertainty"]["throughput_mean_calls_per_second"]["estimate"],
            "passes_all_observed_runs": bool(fraction_pass and tail_pass),
            "tail_evidence_status": tails[0]["status"] if tails else "population_missing",
            "independent_runs": len(cohort["runs"]),
            "inferential_status": "repeated_observations" if len(cohort["runs"]) >= MIN_REPETITIONS else "insufficient_repetitions",
        })
    for group in groups.values():
        group["rates"].sort(key=lambda row: row["offered_ops_per_second"])
        passing = [row for row in group["rates"] if row["passes_all_observed_runs"]]
        group["maximum_observed_admissible_success_calls_per_second"] = max(
            (row["achieved_success_calls_per_second"] for row in passing), default=None)
    return {"p99_response_us": p99_us, "minimum_success_fraction": minimum_success, "operation": operation,
            "groups": groups,
            "claim": "Finite observed rate-sweep points, NOT proof of sustained capacity, future-tail bounds or HA; inspect sample counts and run-level uncertainty."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("roots", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path, help="Fresh report directory")
    parser.add_argument("--allow-incomplete", action="store_true", help="Retain failed coverage without exit failure; does not turn failures into results")
    parser.add_argument("--p99-slo-us", type=float, help="Predeclared response-p99 objective; no automatic default objective")
    parser.add_argument("--minimum-success-fraction", type=float, default=.999)
    parser.add_argument("--slo-operation", choices=["read", "put", "delete", "missing_read", "get_many", "put_batch"], default="read")
    args = parser.parse_args()
    report = make_report(discover(args.roots))
    if args.p99_slo_us is not None:
        report["slo_frontier"] = slo_frontier(report, args.p99_slo_us, args.minimum_success_fraction, args.slo_operation)
    args.output.mkdir(parents=True, exist_ok=False)
    local.dump(args.output / "comparison.json", report)
    output_rows = list(rows(report))
    with (args.output / "comparison.csv").open("w", newline="") as stream:
        if output_rows:
            writer = csv.DictWriter(stream, fieldnames=list(output_rows[0]))
            writer.writeheader()
            writer.writerows(output_rows)
    (args.output / "comparison.md").write_text(markdown(report, output_rows))
    print(f"Retained {len(report['coverage'])} run receipts and {len(report['cohorts'])} distinct contracts in {args.output}")
    return 0 if report["complete"] or args.allow_incomplete else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, KeyError, TypeError, OSError, json.JSONDecodeError) as error:
        print(f"research report rejected: {error}", file=sys.stderr)
        sys.exit(1)
