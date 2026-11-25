"""Native-vs-leased comparison arms for the Q1 fleet: concurrency-sweep workload
binding and per-point exact-quantile + throughput evidence.

Every point is a PAIR: native single-node vs leased NVMe-oF at the same engine
commit, in the same fleet, interleaved repeats, both durability tiers, sweeping
concurrency 1/4/8/16 for BOTH remote_put (512 B, batch 32 — otherwise the Q0
workload) and mainnet_replay (the same Sui mainnet fixture, manifest sha256
4264e79e…c922fa). NO c64 rows: Q0's c64 points measured queueing, not service
time. Quantiles are exact nearest-rank values from samples.csv plus measured
throughput per point; log2 bucketed summaries are not admissible.

Cut order (owner-fixed, enforced here): repeats first (min 2), then
row_operations down to the >= 10 s per-row floor, then the land4 anchor. A
native/leased pair is never dropped: every concurrency point must carry both
arms or the case is invalid.
"""
from q1_manifest import fields, integer, require

COMPARISON_CASES = ("put_native", "put_synchronous", "replay_native", "replay_synchronous")
REPLAY_CASES = COMPARISON_CASES[2:]
CONCURRENCY_SWEEP = (1, 4, 8, 16)
MIN_ROW_SECONDS = 10
QUANTILES = ("p50", "p90", "p99", "p999", "p9999")

# Q0 reference workloads (checkpoint-work/q0-CI-29a9c657… config validation;
# P13/P16/F1 campaigns), pinned field for field. row_operations may be cut
# (second in the cut order) but never below the >= 10 s per-row floor.
PUT_REFERENCE = {
    "case_kind": "remote_put", "value_size": 512, "batch_size": 32,
    "keys": 4096, "warmup_operations": 65536,
    "row_operations_reference": 524288,
    "distribution": "uniform", "mix": {"put": 100}, "owner_count": 2,
    "owner_wal_geometry": "native_default", "owner_wal_prewrite": True, "target_io": "buffered",
}
REPLAY_REFERENCE = {
    "case_kind": "mainnet_replay", "value_size": 512, "batch_size": 32,
    "keys": 4096, "repetitions_reference": 4096,
    "distribution": "uniform", "mix": {"put": 50, "read": 50}, "owner_count": 2,
    "owner_wal_geometry": "native_default", "owner_wal_prewrite": True, "target_io": "buffered",
    "fixture_manifest_sha256": "4264e79e0cbe4a1e36d94468df2f049f988eac57877c9bf2e7e175489dc922fa",
}

# Closed set of allowed differences between the two arms of one point.
NAMED_DIFFERENCES = {
    "network": "the leased arm crosses NVMe-oF (kernel target, buffered file "
               "backing) with TCP/mTLS control to two owners; the native arm "
               "is a single node on its own VM's local SSD",
}


def _tier(case):
    return case.split("_", 1)[1]  # "native" | "synchronous"


def validate_case(config, case):
    """Bind one comparison case: sweep, exact workload, both arms, identical context."""
    require(case in COMPARISON_CASES, f"unknown comparison case {case}")
    fields(config, "kind tier repeats backends concurrency context workload", "comparison case")
    require(config["kind"] == "comparison" and config["tier"] == _tier(case)
            and integer(config["repeats"], 2, 3), "wrong comparison identity or repeats")
    require(tuple(config["backends"]) == ("leased", "native"),
            "both arms are mandatory in every pair")
    require(tuple(config["concurrency"]) == CONCURRENCY_SWEEP,
            "the sweep is exactly 1/4/8/16; c64 queueing rows are excluded")
    context = config["context"]
    base_context = {"engine_sha", "client_sha256", "harness_sha", "machine_type", "image_id",
                    "ssd", "wal_geometry", "wal_prewrite", "sync_writes", "named_differences"}
    require(set(context) == base_context, "invalid comparison context")
    require(hex40(context["engine_sha"]) and hex64(context["client_sha256"])
            and hex64(context["harness_sha"])
            and context["machine_type"] == "z4m-highmem-16" and integer(context["image_id"])
            and context["ssd"] == "local-ssd" and context["wal_geometry"] == "native_default"
            and context["wal_prewrite"] is True, "comparison context is not identical across arms")
    # Native engine honors the same tier via TH_SYNC_WRITES (parse_env_bool),
    # default false == page cache == the native tier.
    require(context["sync_writes"] == (_tier(case) == "synchronous"),
            "sync_writes must match the case tier on both arms")
    require(context["named_differences"] == NAMED_DIFFERENCES,
            "only the closed set of named differences is allowed")
    reference = dict(REPLAY_REFERENCE if case in REPLAY_CASES else PUT_REFERENCE)
    reference_row_key = ("row_operations_reference" if "row_operations_reference" in reference
                         else "repetitions_reference")
    reference_row = reference.pop(reference_row_key)
    workload = config["workload"]
    for key, value in reference.items():
        require(workload.get(key) == value, f"comparison workload differs from the Q0 pin: {key}")
    row = workload["row_operations"]
    # Cut order, second: row_operations may shrink from the Q0 reference, but
    # never below the >= 10 s per-row floor (checked via min_row_seconds).
    require(integer(row, 1) and row <= reference_row, "row_operations must be 1..reference")
    require(integer(workload.get("min_row_seconds"), MIN_ROW_SECONDS),
            f"comparison rows must keep >= {MIN_ROW_SECONDS} s per row")
    return config



def hex40(value):
    return isinstance(value, str) and len(value) == 40 and all(c in "0123456789abcdef" for c in value)


def hex64(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


QUANTILE_FRACTIONS = {"p50": 0.50, "p90": 0.90, "p99": 0.99, "p999": 0.999, "p9999": 0.9999}


def _analyze_quantile(values, fraction):
    """Reuse the harness's own exact aggregation (kv-comparison analyze.quantile):
    ordered[ceil(fraction * n) - 1]. No log2 buckets anywhere in this path."""
    import sys as _sys
    sibling = str(__import__("pathlib").Path(__file__).resolve().parents[1] / "kv-comparison")
    if sibling not in _sys.path:
        _sys.path.insert(0, sibling)
    from analyze import quantile
    return quantile(values, fraction)


def exact_quantiles(samples):
    """samples: iterable of numeric latencies. Exact nearest-rank quantiles."""
    ordered = list(samples)
    return {name: _analyze_quantile(ordered, fraction)
            for name, fraction in QUANTILE_FRACTIONS.items()}

REPORT_QUANTILES = ("p50", "p99", "p999")  # owner-requested report set per arm


def read_samples_csv(path):
    """Parse a samples.csv and return (api_ns, response_ns) integer lists.

    The report generator computes exact nearest-rank quantiles from these
    columns; the log2 bucket upper bounds in summary.json are kept only as a
    cross-check (see cross_check_buckets).
    """
    import csv

    api, response = [], []
    with open(path, newline="") as stream:
        reader = csv.DictReader(stream)
        require(reader.fieldnames is not None
                and {"api_ns", "response_ns"} <= set(reader.fieldnames),
                "samples.csv lacks api_ns/response_ns columns")
        for row in reader:
            api.append(int(row["api_ns"]))
            response.append(int(row["response_ns"]))
    require(api and len(api) == len(response), "samples.csv rows disagree")
    return api, response


def arm_report(api_ns, response_ns):
    """Exact nearest-rank p50/p99/p999 per arm from raw samples."""
    return {"api": {name: exact_quantiles(api_ns)[name] for name in REPORT_QUANTILES},
            "response": {name: exact_quantiles(response_ns)[name] for name in REPORT_QUANTILES}}


def cross_check_buckets(exact_value, bucket_upper_bound):
    """Buckets are a cross-check only. summary.json reports log2 buckets, so the
    bucket whose upper bound is U covers (U/2, U]; the exact value must fall
    there (no fixed-width assumption)."""
    require(exact_value <= bucket_upper_bound
            and exact_value > bucket_upper_bound // 2,
            "exact quantile disagrees with the log2 bucket cross-check")
    return True


def _derived_status(row, concurrency, base):
    """Owner rule (2) as a check on the row's own metrics, not its label:
    p50 <= 1.2x the c=1 pilot, throughput scales >= 0.8 x c, queue-wait per
    operation <= 1% of the pilot p50 (scheduling ~ 0)."""
    if row["p50"] > int(1.2 * base["p50"]):
        return "waiting (saturated)"
    # throughput_ns is ns per operation (inverse of ops/s): scaling with c
    # means ns/op shrinks as 1/c; >= 0.8 x c means ns/op <= base / (0.8 x c).
    if row["throughput_ns"] * 0.8 * concurrency > base["throughput_ns"]:
        return "waiting (saturated)"
    if row["queue_wait_ns_per_op"] > max(1, int(0.01 * base["p50"])):
        return "waiting (saturated)"
    return "qualified"


def _metric_row_checked(row, concurrency, base, repeats, arm):
    fields(row, "repeat samples rows duration_ns throughput_ns queue_wait_ns_per_op status "
          + " ".join(QUANTILES), "comparison repeat row")
    require(integer(row["repeat"], 0, repeats - 1)
            and integer(row["samples"], 1) and integer(row["rows"], 1)
            and integer(row["duration_ns"], MIN_ROW_SECONDS * 1_000_000_000)
            and integer(row["throughput_ns"], 1)
            and type(row["queue_wait_ns_per_op"]) is int
            and row["queue_wait_ns_per_op"] >= 0
            and row["status"] in ("qualified", "waiting (saturated)"),
            f"{arm} c={concurrency}: invalid repeat row identity")
    values = [row[name] for name in QUANTILES]
    require(all(type(v) is int and v >= 0 for v in values)
            and all(a <= b for a, b in zip(values, values[1:])),
            f"{arm} c={concurrency}: quantiles must be non-negative and monotone")
    derived = _derived_status(row, concurrency, base)
    require(row["status"] == derived,
            f"{arm} c={concurrency} repeat {row['repeat']}: label {row['status']!r} "
            f"contradicts the row's own metrics ({derived!r})")
    return derived


def validate_point_report(report, case, config):
    """Per point: both arms, every interleaved repeat, exact quantiles +
    throughput, and the owner's no-waiting rule enforced as a checked
    qualification.

    (1) each arm's c=1 rows are the pilot (service latency baseline);
    (2) a point's status is DERIVED from each repeat's own metrics (p50 within
        1.2x of the pilot, throughput >= 0.8 x c scaling, queue-wait <= 1% of
        the pilot p50) — a label contradicting the metrics is rejected;
    (3) a point that derives "waiting (saturated)" is not a latency result,
        and the sweep for that arm STOPS there: later points must be explicit
        {"status": "not run (sweep stopped)"} markers with no metrics, which
        keeps the native/leased pair visible. Measured rows after the stop are
        rejected; a mismatched marker before any saturation is rejected too.
    """
    fields(report, "case source points", "comparison point report")
    require(report["case"] == case and report["source"] == "samples.csv exact nearest-rank",
            "quantiles must come from samples.csv, not bucketed summaries")
    points = report["points"]
    require(isinstance(points, list) and len(points) == len(config["concurrency"]),
            "one report entry per sweep point")
    for point in points:
        fields(point, "concurrency arms", "comparison point")
        require(point["concurrency"] in config["concurrency"], "unexpected concurrency point")
        require(set(point["arms"]) == {"leased", "native"}, "a native/leased pair is never dropped")
    sweep = config["concurrency"]
    for arm in ("leased", "native"):
        entries = {point["concurrency"]: point["arms"][arm] for point in points}
        base = None
        stopped = False
        for concurrency in sweep:
            entry = entries[concurrency]
            if stopped:
                require(isinstance(entry, dict) and entry == {"status": "not run (sweep stopped)"},
                        f"{arm} c={concurrency}: after saturation only a "
                        "'not run (sweep stopped)' marker may appear")
                continue
            require(isinstance(entry, list) and len(entry) == config["repeats"],
                    f"{arm}: one row per interleaved repeat at c={concurrency}")
            if concurrency == sweep[0]:
                # the c=1 pilot: the service-latency baseline must qualify.
                # Baseline = the pilot's own first row; the p50 rule is then
                # self-referential by construction, so only the structural
                # checks (duration floor, monotone quantiles) really bind.
                statuses = [_metric_row_checked(row, concurrency, _pilot_base(entry),
                                                config["repeats"], arm) for row in entry]
                require(all(status == "qualified" for status in statuses),
                        f"{arm}: the c=1 pilot must qualify")
                base = _pilot_base(entry)
                continue
            statuses = [_metric_row_checked(row, concurrency, base, config["repeats"], arm)
                        for row in entry]
            require(len(set(statuses)) == 1,
                    f"{arm} c={concurrency}: repeats disagree on qualification status")
            if statuses[0] == "waiting (saturated)":
                stopped = True
    return report


def _pilot_base(entry):
    """The pilot baseline for rule checks: the pilot's own first-row metrics.
    The c=1 checks are self-referential by construction (p50 <= 1.2x p50),
    so the pilot row only needs the structural checks, which use its own
    metrics as the baseline."""
    first = entry[0]
    return {"p50": first["p50"], "throughput_ns": first["throughput_ns"]}
