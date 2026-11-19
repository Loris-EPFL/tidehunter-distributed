"""Per-case A1 evidence extracted from kv-research result directories.

Inputs per case: the overwrite result (`samples.csv` or `samples.csv.gz`,
`summary.json`, `write-diagnostics.json`, `index-a1-stage.json`) and the cold
reopen result (`index-a1-cold.json`). Headline latency uses successful scalar
puts of the measurement phase only, nearest rank.
"""

from __future__ import annotations

import csv
import gzip
import json
import math
from array import array
from pathlib import Path

NS = 1_000_000_000


def read_json(path: Path) -> dict:
    with path.open() as handle:
        return json.load(handle)


def _open_samples(result: Path):
    plain = result / "samples.csv"
    if plain.exists():
        return plain.open(newline="")
    return gzip.open(result / "samples.csv.gz", "rt", newline="")


def nearest_rank(sorted_values, quantile: float) -> int | None:
    if not sorted_values:
        return None
    rank = max(1, math.ceil(quantile * len(sorted_values)))
    return sorted_values[rank - 1]


def median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2


def measurement_samples(result: Path, skip_seconds: int = 5) -> dict:
    """Streaming pass over measurement rows: outcome counts, nearest-rank
    latency of successful puts, scheduler tails, mean in-flight, and median
    successful puts/s over complete one-second buckets after `skip_seconds`."""
    outcomes: dict[str, int] = {}
    api = array("Q")
    scheduling = array("Q")
    api_total = 0
    first_dispatch = None
    last_complete = 0
    first_scheduled = None
    buckets: dict[int, int] = {}
    with _open_samples(result) as handle:
        for row in csv.DictReader(handle):
            if row["phase"] != "measurement":
                continue
            outcome = row["outcome"]
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
            scheduled = int(row["scheduled_ns"])
            dispatched = int(row["dispatched_ns"])
            completed = int(row["completed_ns"])
            api_ns = int(row["api_ns"])
            first_scheduled = scheduled if first_scheduled is None else min(first_scheduled, scheduled)
            if row["dispatch_observed"] == "true":
                first_dispatch = dispatched if first_dispatch is None else min(first_dispatch, dispatched)
                api_total += api_ns
            last_complete = max(last_complete, completed)
            if outcome == "success" and row["operation"] == "put":
                api.append(api_ns)
                scheduling.append(int(row["scheduling_ns"]))
                buckets[completed // NS] = buckets.get(completed // NS, 0) + 1
    api_sorted = sorted(api)
    scheduling_sorted = sorted(scheduling)
    window = last_complete - first_dispatch if first_dispatch is not None else 0
    start_bucket = (first_scheduled or 0) // NS + skip_seconds
    last_bucket = last_complete // NS
    # Complete buckets only: the last (partial) second is excluded.
    rates = [buckets.get(second, 0) for second in range(start_bucket, last_bucket)]
    successes = outcomes.get("success", 0)
    return {
        "outcomes": outcomes,
        "successful_puts": len(api_sorted),
        "failed_or_unknown": sum(count for name, count in outcomes.items() if name != "success"),
        "rejected": outcomes.get("rejected", 0),
        "unknown": outcomes.get("unknown_mutation", 0) + outcomes.get("timeout", 0),
        "latency_ns": {q: nearest_rank(api_sorted, value) for q, value in (("p50", 0.50), ("p99", 0.99), ("p999", 0.999))},
        "p999_qualified": len(api_sorted) >= 100_000,
        "scheduling_ns": {q: nearest_rank(scheduling_sorted, value) for q, value in (("p50", 0.50), ("p99", 0.99), ("p999", 0.999))},
        "mean_in_flight": api_total / window if window > 0 else None,
        "achieved_success_per_s": successes * NS / window if window > 0 else None,
        "complete_buckets_after_skip": len(rates),
        "median_puts_per_s": median(rates) if rates else None,
    }


def thirds_delta(values: list[float]) -> float | None:
    """Median of the last third minus median of the first third; None when
    three nonempty groups cannot be formed."""
    count = len(values)
    if count < 3:
        return None
    first = values[: count // 3]
    last = values[2 * count // 3 :]
    return median(last) - median(first)


def debt_deltas(write_diagnostics: dict, db: str = "coordinator") -> dict:
    """Suffix bytes, batches and oldest age trends over the timed window."""
    details = write_diagnostics.get("index_checkpoint") or {}
    window = next((w for w in details.get("windows", []) if w.get("name") == "timed"), None)
    if window is None:
        return {"available": False, "reason": "no timed window"}
    start, end = window.get("start_ns"), window.get("end_ns")
    series: dict[str, list[float]] = {"suffix_bytes": [], "suffix_batches": [], "oldest_age_ns": []}
    for sample in details.get("samples_1s", []):
        if not (start <= sample.get("at_ns", -1) < end):
            continue
        for database in sample.get("databases", []):
            if database.get("db") == db:
                for name, values in series.items():
                    values.append(database[name])
    deltas = {name: thirds_delta(values) for name, values in series.items()}
    return {
        "available": all(value is not None for value in deltas.values()),
        "samples": len(series["suffix_bytes"]),
        "bytes": deltas["suffix_bytes"],
        "batches": deltas["suffix_batches"],
        "age_ns": deltas["oldest_age_ns"],
    }


def ring_complete(write_diagnostics: dict) -> bool:
    details = write_diagnostics.get("index_checkpoint") or {}
    return (
        details.get("ring", {}).get("lost_unread_rows", 1) == 0
        and details.get("spool", {}).get("overflow_rows", 1) == 0
        and details.get("sample_overflow", 1) == 0
    )


def case_metrics(overwrite: Path, cold: Path | None, resources: dict | None) -> dict:
    """One case's decision inputs and context; `valid` gates the evidence screen."""
    stage = read_json(overwrite / "index-a1-stage.json")
    summary = read_json(overwrite / "summary.json")
    diagnostics = read_json(overwrite / "write-diagnostics.json")
    timed = stage.get("timed", {})
    samples = measurement_samples(overwrite)
    cold_evidence = read_json(cold / "index-a1-cold.json") if cold else None
    coordinator = timed.get("coordinator_file_bytes")
    wal = timed.get("owner_wal_data_frame_bytes")
    puts = timed.get("successful_puts")
    ratio = coordinator / wal if coordinator is not None and wal else None
    per_put = coordinator / puts if coordinator is not None and puts else None
    cold_s = None
    cold_qualified = False
    if cold_evidence:
        cold_s = cold_evidence.get("first_read", {}).get("open_start_to_first_correct_read_s")
        cold_qualified = bool(cold_evidence.get("qualified"))
    breach = bool(resources and resources.get("breach"))
    reasons = []
    if summary.get("status") != "completed":
        reasons.append("run not completed")
    if samples["failed_or_unknown"]:
        reasons.append("rejected, unknown or failed calls")
    if not timed.get("complete") or not timed.get("ack_matches_publication"):
        reasons.append("timed counters incomplete or acknowledgements differ from publication")
    if not ring_complete(diagnostics):
        reasons.append("ring loss or collector overflow")
    if not cold_qualified:
        reasons.append("cold reopen not qualified")
    if breach:
        reasons.append("resource ceiling breach")
    debt = debt_deltas(diagnostics)
    if not debt["available"]:
        reasons.append("insufficient suffix samples for thirds")
    return {
        "valid": not reasons,
        "invalid_reasons": reasons,
        "ratio": ratio,
        "cold_open_to_first_read_s": cold_s,
        "coordinator_bytes_per_put": per_put,
        "debt": debt,
        "samples": samples,
        "timed": timed,
        "skip_markers": timed.get("leased_skip_markers"),
        "resources": resources,
    }
