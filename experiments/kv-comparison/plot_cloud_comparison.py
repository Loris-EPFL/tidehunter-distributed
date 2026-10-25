#!/usr/bin/env python3
"""Offline, matched-run figures from the closed cloud comparison export.

No raw samples are pooled. Latency points are medians of per-run conditional
quantiles, and vertical whiskers are observed run minima/maxima, NOT CIs.
Matplotlib is imported only when rendering; data validation needs stdlib only.
"""
import argparse
from collections import Counter
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import textwrap


# One style per backend, including when only a subset is present in a cohort.
STYLES = {
    "native": ("Native", "#333333", "o", "-"),
    "tidehunter_tcp": ("TH TCP", "#0072B2", "s", "-"),
    "tidehunter_efa": ("TH EFA", "#56B4E9", "^", "--"),
    "tidehunter_efa_direct": ("TH EFA direct", "#009E73", "D", "-.") ,
    "tidehunter_efa_native": ("TH EFA native", "#D55E00", "v", "-"),
    "tikv": ("TiKV", "#CC79A7", "P", "--"),
    "cassandra": ("Cassandra", "#8C6500", "X", ":"),
    "foundationdb": ("FoundationDB", "#7854A3", "h", "-."),
}
BACKENDS = tuple(STYLES)
OBSERVED_NAMES = {"tikv": "tikv_raw", "cassandra": "cassandra_cql_vendor_unverified"}
TH = tuple(name for name in BACKENDS if name.startswith("tidehunter_"))
SIZES = (64, 256, 1024, 8192, 65536)
TIMINGS = ("api_ns", "response_ns", "scheduling_ns")
GOODPUT = ("success_calls_per_second", "success_logical_keys_per_second",
           "success_value_mib_per_second")
BASE_CAPTION = (
    "Points: median of matched per-repeat values; whiskers: observed run min/max, NOT confidence intervals. "
    "No pooled quantiles; few repetitions support no confidence claim. Successful latency is conditional on operation/outcome. "
    "RF1 does not equalize consistency, durability, security, or transaction semantics. "
    "Primary RF1 only: native uses value_wal_barrier and TH explicit caches are off; other cohorts use their stated settings. "
    "Warm OS is not guaranteed cache residency. Tail quantiles are descriptive, including small populations."
)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def nonnegative(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name}: expected finite nonnegative number")
    return value


def integer(value, name, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name}: expected integer >= {minimum}")
    return value


def close(left, right):
    return math.isclose(left, right, rel_tol=1e-6, abs_tol=1e-7)


def validate_metrics(case):
    """Reject inconsistent denominators and duplicate operation/outcome groups."""
    metrics = case.get("metrics")
    if not isinstance(metrics, dict) or metrics.get("measurement_rows_complete") is not True:
        raise ValueError(f"{case['case_id']}: completed case lacks complete measurement metrics")
    offered = integer(metrics.get("offered_calls"), "offered_calls", 1)
    success = integer(metrics.get("success_calls"), "success_calls")
    failed = integer(metrics.get("unsuccessful_calls"), "unsuccessful_calls")
    if offered != success + failed or offered != metrics.get("planned_measurement_calls"):
        raise ValueError("inconsistent offered/success/unsuccessful/planned accounting")
    if offered != case["job"]["measurement_operations"]:
        raise ValueError("measurement count differs from job")
    if not close(nonnegative(metrics.get("success_fraction"), "success_fraction"), success / offered):
        raise ValueError("success fraction differs from offered denominator")
    elapsed = nonnegative(metrics.get("elapsed_seconds"), "elapsed_seconds")
    if not elapsed or not close(nonnegative(metrics.get(GOODPUT[0]), GOODPUT[0]), success / elapsed):
        raise ValueError("goodput differs from observed elapsed measurement span")
    seen, total, successful, keys = set(), 0, 0, 0
    for population in metrics.get("populations", []):
        identity = population.get("operation"), population.get("outcome")
        if None in identity or identity in seen:
            raise ValueError("missing or duplicate operation/outcome population")
        seen.add(identity)
        count = integer(population.get("count"), "population count", 1)
        logical = integer(population.get("logical_keys"), "logical_keys", 1)
        total += count
        if identity[1] in ("success", "ok"):
            successful += count
            keys += logical
        for timing in TIMINGS:
            summary = population.get(timing)
            if not isinstance(summary, dict):
                raise ValueError("missing timing summary")
            samples = integer(summary.get("sample_count"), "sample_count")
            if samples > count:
                raise ValueError("timed samples exceed population")
            values = [summary.get(key) for key in ("p50", "p95", "p99", "p999", "max")]
            if not samples:
                if any(value is not None for value in values):
                    raise ValueError("zero timed samples cannot have latency")
            else:
                for value in values:
                    nonnegative(value, timing)
                if values != sorted(values):
                    raise ValueError("quantiles are not monotone")
            if identity[1] == "rejected" and timing != "response_ns" and samples:
                raise ValueError("rejected arrivals have no API/scheduling clocks")
    if total != offered or successful != success:
        raise ValueError("population counts differ from workload accounting")
    if not close(nonnegative(metrics.get(GOODPUT[1]), GOODPUT[1]), keys / elapsed):
        raise ValueError("logical-key goodput differs from successful populations")
    nonnegative(metrics.get(GOODPUT[2]), GOODPUT[2])


class Dataset:
    def __init__(self, document):
        if document.get("schema_version") != 1 or not isinstance(document.get("cases"), list):
            raise ValueError("expected comparison.json schema_version 1 with cases")
        self.cases = document["cases"]
        if document.get("planned_cases") != len(self.cases):
            raise ValueError("planned_cases does not match case coverage")
        self.index = {}
        ids, contracts = set(), {}
        for case in self.cases:
            job = case["job"]
            case_id = case["case_id"]
            backend = job["backend"]
            if backend not in STYLES:
                raise ValueError(f"unknown backend {backend}; add an explicit style")
            integer(job["repeat"], "repeat")
            integer(job["value_size"], "value_size", 1)
            if case_id in ids or job.get("case_id") != case_id:
                raise ValueError("duplicate or inconsistent case identity")
            ids.add(case_id)
            key = (backend, job["cohort"], job["profile"], job["value_size"], job["repeat"])
            if key in self.index:
                raise ValueError("duplicate backend/cohort/profile/size/repeat identity")
            self.index[key] = case
            if case["status"] not in ("completed", "failed", "not_run"):
                raise ValueError("unknown case status")
            if case["status"] != "completed":
                # Failed measurements remain diagnostics, never performance points.
                if case["status"] == "not_run" and case.get("metrics") is not None:
                    raise ValueError("not-run case must not carry performance metrics")
                continue
            if case.get("backend", {}).get("name") != OBSERVED_NAMES.get(backend, backend):
                raise ValueError("job and observed backend differ")
            contract = {name: value for name, value in job.items()
                        if name not in ("backend", "case_id", "repeat")}
            cohort_cell = (job["cohort"], job["profile"], job["value_size"])
            if cohort_cell in contracts and contracts[cohort_cell] != contract:
                raise ValueError("workload contract changed between repetitions or backends")
            contracts[cohort_cell] = contract
            validate_metrics(case)
        completed = sum(case["status"] == "completed" for case in self.cases)
        if document.get("completed_cases") != completed:
            raise ValueError("completed_cases does not match coverage")

    def completed_repeats(self, cell):
        return {key[-1] for key, case in self.index.items()
                if key[:-1] == cell and case["status"] == "completed"}

    def matched_repeats(self, cells):
        """No per-backend downselection: every point in a figure shares repeats."""
        sets = [self.completed_repeats(cell) for cell in cells]
        common = sorted(set.intersection(*sets)) if sets else []
        if len(common) < 2:
            raise ValueError("fewer than two common completed repeats across every requested cell; figure omitted")
        return common


def metric_value(case, metric, operation=None, outcome="success", percentile="p99"):
    metrics = case["metrics"]
    if metric in GOODPUT:
        return metrics[metric], None
    if metric == "rejection_fraction":
        count = sum(row["count"] for row in metrics["populations"] if row["outcome"] == "rejected")
        return count / metrics["offered_calls"], metrics["offered_calls"]
    if metric in TIMINGS:
        rows = [row for row in metrics["populations"]
                if row["operation"] == operation and row["outcome"] == outcome]
        if not rows or not rows[0][metric]["sample_count"]:
            return None, 0
        return rows[0][metric][percentile], rows[0][metric]["sample_count"]
    raise ValueError(f"unknown metric {metric}")


def aggregate_point(dataset, cell, repeats, metric, operation=None, outcome="success", percentile="p99"):
    observations = []
    for repeat in repeats:
        case = dataset.index.get(cell + (repeat,))
        if case is None or case["status"] != "completed":
            return None, "failed or missing matched case"
        value, samples = metric_value(case, metric, operation, outcome, percentile)
        if value is None:
            return None, f"{operation}/{outcome}/{metric} absent or untimed in repeat {repeat}; not zero"
        observations.append({"repeat": repeat, "case_id": case["case_id"], "value": value,
                             "timed_sample_count_or_denominator": samples})
    values = [item["value"] for item in observations]
    if len(values) < 2:
        return None, "fewer than two matched observations; not a comparative point"
    return {"backend": cell[0], "cohort": cell[1], "profile": cell[2], "value_bytes": cell[3],
            "operation": operation, "outcome": outcome if operation else "whole_workload",
            "metric": metric, "percentile": percentile if metric in TIMINGS else None,
            "repeats": repeats, "observations": observations, "median": statistics.median(values),
            "observed_min": min(values), "observed_max": max(values)}, None


def panel(title, metric, operation=None, percentile="p99", outcome="success", **kwargs):
    return dict(title=title, metric=metric, operation=operation, percentile=percentile,
                outcome=outcome, **kwargs)


def size_series(backends, cohort, profile, sizes=SIZES, suffix=""):
    return [{"label": STYLES[backend][0] + suffix, "backend": backend,
             "cells": [(backend, cohort, profile, size) for size in sizes], "x": list(sizes)}
            for backend in backends]


def goodput_panels():
    return [panel("Whole-workload successful calls", GOODPUT[0]),
            panel("Successful logical keys", GOODPUT[1]),
            panel("Successful value payload", GOODPUT[2])]


def figure_specs():
    specs = []

    def add(name, title, series, panels, columns, caption, xkind="size", xticks=None):
        specs.append(dict(name=name, title=title, series=series, panels=panels, columns=columns,
                          caption=caption, xkind=xkind, xticks=xticks))

    for profile, operation, title in (("read-concurrent", "read", "Pure reads"),
                                      ("write-concurrent", "put", "Pure writes"),
                                      ("read-many32", "get_many", "Read-many32")):
        add("primary-" + profile, title + " — primary RF1, c16",
            size_series(BACKENDS, "primary_rf1", profile),
            [panel("API p50", "api_ns", operation, "p50"), panel("API p99", "api_ns", operation),
             panel("Response p99", "response_ns", operation)] + goodput_panels(), 3,
            "Closed-loop c16; all five value sizes. " +
            ("Get-many latency is PER CALL of 32 keys, not per key. FoundationDB performs 32 sequential transaction gets. "
             if operation == "get_many" else "One key per API call. "))
    mixed = size_series(BACKENDS, "primary_rf1", "point-mixed")
    add("primary-point-mixed-latency", "Point-mixed — primary RF1, c1", mixed,
        [panel(f"{operation} API {quantile}", "api_ns", operation, quantile)
         for quantile in ("p50", "p99") for operation in ("read", "put", "delete", "missing_read")], 4,
        "Closed-loop c1: 45% read, 45% put, 5% delete, 5% missing_read. Missing-read success is a successful absence result, not a hit. "
        "Each operation/outcome remains separate; delete/missing tails have small populations. ")
    add("primary-point-mixed-goodput", "Point-mixed — whole-workload goodput, c1", mixed,
        goodput_panels(), 3, "Goodput covers the ENTIRE mixed workload, not the adjacent operation-specific latency populations. "
        "Value MiB/s uses successful value payload accounting, not wire traffic or delete/missing bytes. ")
    offered = [{"label": STYLES[backend][0], "backend": backend,
                "cells": [(backend, "primary_rf1", f"offered-{rate}", 1024) for rate in (1000, 10000, 50000)],
                "x": [1000, 10000, 50000]} for backend in BACKENDS]
    add("offered-load-accounting", "Offered load — 1 KiB, c32", offered,
        goodput_panels() + [panel("Rejected arrivals / offered arrivals", "rejection_fraction")], 2,
        "Open-loop 70% read / 30% put; c32 dispatch bound; configured arrival rates, not achieved throughput, on x. "
        "Rejection fractions retain ALL offered arrivals in the denominator. Goodput uses observed measurement span. ", "offered")
    add("offered-load-tails", "Offered-load conditional p99 — 1 KiB, c32", offered,
        [panel(f"{operation} {metric.removesuffix('_ns')} p99 / success", metric, operation)
         for operation in ("read", "put") for metric in TIMINGS] +
        [panel(f"{operation} response p99 / rejected", "response_ns", operation, outcome="rejected")
         for operation in ("read", "put")], 3,
        "Success and rejected populations are NEVER mixed. API is dispatch-to-completion; response is scheduled-arrival-to-completion; "
        "scheduling is arrival-to-dispatch. Rejected arrivals have response only (API/scheduling undefined). "
        "Rejected-response points require a timed rejection in every matched repeat; missing populations are gaps, not zeros. "
        "Read with offered-load-accounting; fast conditional tails alone do not imply accepting offered load. ", "offered")
    zipf = []
    for backend in BACKENDS:
        zipf.append({"label": STYLES[backend][0], "backend": backend,
                     "cells": [(backend, "primary_rf1", profile, 1024) for profile in ("read-concurrent", "zipf-read")],
                     "x": [0, 1]})
    add("zipf-versus-uniform", "Uniform versus Zipf(0.99) — 1 KiB reads, c16", zipf,
        [panel("API p50", "api_ns", "read", "p50"), panel("API p99", "api_ns", "read"),
         panel("Successful calls", GOODPUT[0])], 3,
        "Same 1 KiB c16 pure-read profile; access frequency differs, not key-bit distribution. No explicit coordinator/owner cache in either primary cohort. ",
        "category", ["Uniform", "Zipf 0.99"])
    cache_series, cache_panels = [], []
    # Separate panels, not a pooled cache-mode curve or an unmatched c16 baseline.
    for cohort, profile, backends, label in (
            ("coordinator_cache_configured", "coordinator-cache", TH, "Coordinator cache"),
            ("owner_cache_configured", "owner-cache", TH, "Owner cache"),
            ("registered_ram_configured", "registered-ram", ("tidehunter_efa_native",), "Registered RAM")):
        rows = size_series(backends, cohort, profile, (256, 8192))
        cache_series.extend(rows)
        for metric, quantile, heading in (("api_ns", "p50", "API p50"), ("api_ns", "p99", "API p99"),
                                          (GOODPUT[0], "p99", "successful calls")):
            cache_panels.append(panel(f"{label}: {heading}", metric, "read" if metric in TIMINGS else None,
                                      quantile, cohort_filter=cohort))
    add("cache-cohorts", "Separate configured-cache cohorts — c1, 256 B and 8 KiB", cache_series,
        cache_panels, 3, "Every row is a distinct c1 cohort; no comparison against primary c16 pure reads. "
        "Coordinator and owner cache budgets are 128 MiB where configured; Linux page cache also remains. "
        "The owner-cache cohort includes cache misses/WAL fallbacks; it is not isolated owner-cache-hit latency. "
        "Registered RAM is TH EFA native only and is mixed RMA plus message fallback, NOT isolated RDMA latency. "
        "Cumulative path counters include preload/warmup/qualification and are not timing fractions. ")
    cold = [{"label": STYLES[backend][0], "backend": backend,
             "cells": [(backend, "cold_os_cache", "cold-os-large", 65536)], "x": [i]}
            for i, backend in enumerate(BACKENDS)]
    add("cold-os-cache", "Cold-OS-cache protocol — 64 KiB reads, c16", cold,
        [panel("API p50", "api_ns", "read", "p50"), panel("API p99", "api_ns", "read")] + goodput_panels(), 3,
        "8 GiB value preload, no warmup, cache-drop gate. Cold OS does NOT mean isolated NVMe/device latency: engine and controller caches, "
        "network, scheduling and metadata may contribute. Systems need at least two completed cold repeats to enter this separate cohort; "
        "all plotted systems share matched repeats and exclusions remain explicit. Missing results are never plotted as zero. ",
        "backend", [STYLES[name][0] for name in BACKENDS])
    native = []
    for cohort, suffix in (("primary_rf1", " / WAL barrier"), ("native_pagecache", " / pagecache")):
        for profile, short in (("point-mixed", "mixed c1"), ("read-many32", "many32 c16")):
            rows = size_series(("native",), cohort, profile, suffix=suffix + " / " + short)
            rows[0]["variant"] = len(native)
            native.extend(rows)
    add("native-pagecache-latency", "Native pagecache versus value-WAL barrier — distinct durability", native,
        [panel(f"{operation} API {quantile}", "api_ns", operation, quantile,
               profile_filter="read-many32" if operation == "get_many" else "point-mixed")
         for quantile in ("p50", "p99") for operation in ("read", "put", "delete", "missing_read", "get_many")], 5,
        "Native-only durability extras; pagecache acknowledgment is not equivalent to value_wal_barrier. "
        "Point-mixed c1 operations are separate; get_many c16 is one 32-key call. Both modes can use Linux page cache for reads. ")
    add("native-pagecache-goodput", "Native durability extras — whole-workload goodput", native,
        goodput_panels(), 3, "Four labeled series: mixed c1 and read-many32 c16, each with barrier/pagecache durability. "
        "Do not interpret different profiles/concurrency as a durability-only speedup. ")
    return specs


def build_figures(dataset):
    figures, omitted = [], []
    for spec in figure_specs():
        unavailable = []
        if spec["name"] == "cold-os-cache":
            available = []
            for series in spec["series"]:
                cell = series["cells"][0]
                if len(dataset.completed_repeats(cell)) >= 2:
                    available.append(series)
                else:
                    unavailable.append({
                        "backend": series["backend"], "reason": "Fewer than two completed cold-OS repeats",
                        "case_ids": [case["case_id"] for key, case in dataset.index.items() if key[:-1] == cell]})
            spec = dict(spec, series=available)
        cells = list(dict.fromkeys(cell for series in spec["series"] for cell in series["cells"]))
        try:
            repeats = dataset.matched_repeats(cells)
        except ValueError as error:
            omitted.append({"figure": spec["name"], "reason": str(error)})
            continue
        result = {key: value for key, value in spec.items() if key not in ("series", "panels")}
        result.update(repeats=repeats, panels=[], files=[spec["name"] + extension for extension in (".png", ".pdf")])
        used = set()
        for number, request in enumerate(spec["panels"]):
            plotted = dict(request, id=f"{spec['name']}:{number}", series=[], unavailable=[])
            for source in spec["series"]:
                points = []
                for x, cell in zip(source["x"], source["cells"]):
                    if request.get("cohort_filter", cell[1]) != cell[1] or request.get("profile_filter", cell[2]) != cell[2]:
                        continue
                    point, reason = aggregate_point(dataset, cell, repeats, request["metric"],
                                                    request["operation"], request["outcome"], request["percentile"])
                    if reason:
                        plotted["unavailable"].append({"cell": cell, "x": x, "reason": reason})
                    else:
                        point["x"] = x
                        points.append(point)
                        used.update(item["case_id"] for item in point["observations"])
                if points:
                    plotted["series"].append({key: value for key, value in source.items() if key not in ("cells", "x")} | {"points": points})
            plotted["status"] = "available" if plotted["series"] else "unavailable"
            if not plotted["series"]:
                plotted["reason"] = "No complete matched operation/outcome timing population; not plotted as zero"
            result["panels"].append(plotted)
        result["case_ids"] = sorted(used)
        result["caption"] += BASE_CAPTION
        if spec["name"] == "cold-os-cache":
            result["unavailable_systems"] = unavailable
        figures.append(result)
    return figures, omitted


def coverage(dataset):
    result = {"total": dict(Counter(case["status"] for case in dataset.cases)), "by_backend": {}, "by_cohort": {}}
    for field, destination in (("backend", "by_backend"), ("cohort", "by_cohort")):
        for name in sorted({case["job"][field] for case in dataset.cases}):
            result[destination][name] = dict(Counter(case["status"] for case in dataset.cases if case["job"][field] == name))
    return result


def render(figures, coverage_data, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    plt.rcParams.update({"font.size": 10, "axes.titlesize": 11, "axes.labelsize": 10,
                         "legend.fontsize": 9, "pdf.fonttype": 42, "savefig.dpi": 200})
    units = {GOODPUT[0]: "Successful API calls/s", GOODPUT[1]: "Successful logical keys/s",
             GOODPUT[2]: "Successful value MiB/s", "rejection_fraction": "Rejected / offered arrivals"}
    for figure in figures:
        columns = min(3, figure["columns"])
        rows = math.ceil(len(figure["panels"]) / columns)
        fig, axes = plt.subplots(rows, columns, figsize=(5.0 * columns, 3.7 * rows + 3.3), squeeze=False)
        labels = {}
        for axis, item in zip(axes.flat, figure["panels"]):
            axis.set_title(item["title"])
            axis.set_ylabel("Latency (µs per API call)" if item["metric"] in TIMINGS else units[item["metric"]])
            if item["status"] == "unavailable":
                axis.text(.5, .5, "Unavailable\n(no matched timing population)", ha="center", transform=axis.transAxes)
                continue
            positive, zero = False, False
            for series in item["series"]:
                label, color, marker, line = STYLES[series["backend"]]
                variant = series.get("variant")
                if variant is not None:
                    line = ("-", "--", "-.", ":")[variant]
                    marker = ("o", "s", "^", "D")[variant]
                points = series["points"]
                scale = 1 / 1000 if item["metric"] in TIMINGS else 1
                x = [point["x"] for point in points]
                y = [point["median"] * scale for point in points]
                error = [[(point["median"] - point["observed_min"]) * scale for point in points],
                         [(point["observed_max"] - point["median"]) * scale for point in points]]
                positive |= any(value > 0 for value in y)
                zero |= any(point["observed_min"] == 0 for point in points)
                # Do not connect across an unavailable x: missing values are explicit NaN gaps.
                unavailable_x = {entry["x"] for entry in item["unavailable"] if entry["cell"][0] == series["backend"]}
                if unavailable_x:
                    lookup = dict(zip(x, y))
                    joined = sorted(set(x) | unavailable_x)
                    axis.plot(joined, [lookup.get(value, math.nan) for value in joined], color=color, linestyle=line)
                    line_for_error = "none"
                else:
                    line_for_error = line if figure["xkind"] != "backend" else "none"
                axis.errorbar(x, y, yerr=error, color=color, marker=marker, linestyle=line_for_error,
                              capsize=3, markersize=5, linewidth=1.3)
                labels[series["label"]] = Line2D([], [], color=color, marker=marker, linestyle=line)
            if item["metric"] == "rejection_fraction":
                axis.set_ylim(-.02, 1.02)
            elif positive and not zero:
                axis.set_yscale("log")
            elif positive:
                axis.set_yscale("symlog", linthresh=1)
            if figure["xkind"] == "size":
                axis.set_xscale("log", base=2)
                ticks = sorted({point["x"] for source in item["series"] for point in source["points"]})
                axis.set_xticks(ticks, [str(value) for value in ticks])
                axis.set_xlabel("Value size (bytes)")
            elif figure["xkind"] == "offered":
                axis.set_xscale("log")
                axis.set_xticks([1000, 10000, 50000], ["1k", "10k", "50k"])
                axis.set_xlabel("Configured offered API calls/s")
            else:
                axis.set_xticks(range(len(figure["xticks"])), figure["xticks"],
                                rotation=45 if figure["xkind"] == "backend" else 0,
                                ha="right" if figure["xkind"] == "backend" else "center")
                if figure["xkind"] == "backend":
                    axis.text(.98, .97, "FDB unavailable", transform=axis.transAxes, ha="right", va="top", fontsize=9)
            axis.grid(True, which="major", alpha=.25)
        for axis in list(axes.flat)[len(figure["panels"]):]:
            axis.set_visible(False)
        fig.suptitle(figure["title"] + "\nMatched repeats " + ", ".join("r" + str(value) for value in figure["repeats"]), fontsize=15, y=.995)
        fig.legend(list(labels.values()), list(labels), loc="upper center", bbox_to_anchor=(.5, .947), ncol=min(4, len(labels)))
        caption = "\n".join(textwrap.wrap(figure["caption"], width=columns * 49))
        fig.text(.025, .015, caption, va="bottom", fontsize=8)
        fig.tight_layout(rect=(0, .19 if rows > 1 else .30, 1, .88))
        for name in figure["files"]:
            fig.savefig(output / name, bbox_inches="tight")
        plt.close(fig)
    fig, axes = plt.subplots(1, 2, figsize=(16, 7.5))
    for axis, key, title in zip(axes, ("by_backend", "by_cohort"), ("By backend", "By cohort")):
        names = list(coverage_data[key])
        bottom = [0] * len(names)
        for status, color in (("completed", "#009E73"), ("failed", "#D55E00"), ("not_run", "#999999")):
            counts = [coverage_data[key][name].get(status, 0) for name in names]
            bars = axis.bar(range(len(names)), counts, bottom=bottom, color=color, label=status)
            axis.bar_label(bars, labels=[str(count) if count else "" for count in counts], label_type="center", fontsize=9)
            bottom = [a + b for a, b in zip(bottom, counts)]
        axis.set_xticks(range(len(names)), [STYLES[name][0] if key == "by_backend" else name for name in names], rotation=40, ha="right")
        axis.set_ylabel("Planned cases (count)")
        axis.set_title(title)
        axis.legend()
    fig.suptitle("Campaign coverage — all planned repeats, not the matched performance subset")
    fig.text(.03, .025, "Coverage counts are not performance zeros. FDB r2 primary extras are retained here but excluded from matched main curves.\n"
             "Completed/failed/not_run are original campaign states, not reconstructed successes; no new runs or Sui replay.", fontsize=10)
    fig.tight_layout(rect=(0, .11, 1, .94))
    for extension in ("png", "pdf"):
        fig.savefig(output / ("coverage." + extension), bbox_inches="tight")
    plt.close(fig)


def export_data(figures, coverage_data, output):
    fields = ["figure", "panel", "series", "backend", "cohort", "profile", "value_bytes", "x", "operation", "outcome",
              "metric", "percentile", "unit", "median", "observed_min", "observed_max", "repeat", "case_id", "value",
              "timed_sample_count_or_denominator"]
    with (output / "plot-data.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for figure in figures:
            for item in figure["panels"]:
                for series in item["series"]:
                    for point in series["points"]:
                        base = {key: point.get(key) for key in fields if key in point}
                        base.update(figure=figure["name"], panel=item["id"], series=series["label"],
                                    unit="ns" if point["metric"] in TIMINGS else
                                    {GOODPUT[0]: "calls/s", GOODPUT[1]: "keys/s", GOODPUT[2]: "value MiB/s", "rejection_fraction": "fraction"}[point["metric"]])
                        for observation in point["observations"]:
                            writer.writerow(base | observation)
        for grouping in ("total", "by_backend", "by_cohort"):
            groups = {"all": coverage_data["total"]} if grouping == "total" else coverage_data[grouping]
            for name, counts in groups.items():
                for status in ("completed", "failed", "not_run"):
                    writer.writerow(dict(figure="coverage", panel=grouping, series=name, outcome=status,
                                         metric="case_count", unit="cases", value=counts.get(status, 0)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    dataset = Dataset(json.loads(raw))
    figures, omitted = build_figures(dataset)
    coverage_data = coverage(dataset)
    # Never permit outputs to overwrite immutable source evidence.
    source_root = args.input.resolve().parent
    destination = args.output.resolve()
    if destination == source_root or source_root in destination.parents:
        parser.error("output must be outside the immutable input directory")
    args.output.mkdir(parents=True, exist_ok=True)
    render(figures, coverage_data, args.output)
    export_data(figures, coverage_data, args.output)
    used = set().union(*(set(figure["case_ids"]) for figure in figures))
    manifest = {"schema_version": 1, "input": str(args.input), "input_sha256": hashlib.sha256(raw).hexdigest(),
                "aggregation": "median of per-repeat scalar metrics/conditional quantiles, never pooled samples",
                "variation": "observed min/max across matched repeats, NOT CI; no confidence claim",
                "repeat_policy": "intersection of completed repeat identities across EVERY cell in each figure; minimum two; no one-repeat fallback",
                "units": "plot-data.csv retains exact source ns; latency figures convert ns / 1000 to µs; MiB = 2^20 bytes",
                "figures": figures, "omitted_figures": omitted, "coverage": coverage_data,
                "coverage_figure": {"files": ["coverage.png", "coverage.pdf"], "title": "Campaign coverage",
                                    "caption": "All planned statuses, all repeats; not performance points; counts in coverage and plot-data.csv"},
                "completed_cases_not_in_figures": [case["case_id"] for case in dataset.cases if case["status"] == "completed" and case["case_id"] not in used],
                "limitations": ["Unmatched repetitions remain in the raw appendix and are not pooled into matched curves.",
                                "No raw-sample verification or independence proof beyond exported case/repeat identity; source receipts remain authoritative.",
                                "No rejection API/scheduling latency: undispatched arrivals have no such clocks.",
                                "No Sui checkpoint replay, isolated disk/RDMA microbenchmark, or confidence inference; rendering is offline."]}
    (args.output / "plots.json").write_text(json.dumps(manifest, indent=2, allow_nan=False) + "\n")
    print(f"Wrote {len(figures) + 1} figure pairs, plot-data.csv and plots.json to {args.output}")


if __name__ == "__main__":
    main()
