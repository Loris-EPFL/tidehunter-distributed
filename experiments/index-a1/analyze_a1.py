#!/usr/bin/env python3
"""A1 decision: is Exact sufficient? [INFERENCE; not validator qualification]

Rule (INDEX-A0-A1.md, "Decision: is Exact sufficient?"):
- For E4 and E16, per tier, take the median of the five case values at
  1M/5M/10M for (1) timed coordinator file bytes / timed accepted owner WAL
  data-frame bytes and (2) cold open start to first correct read (s).
- Fit OLS to (ln N, ln median); predict exp(a + b ln N*) with unrounded
  coefficients: N* = 709,000,000 for (1), 100,000,000 for (2).
- Evidence screen: five successful cases per size and tier, zero
  rejected/unknown/incorrect, no resource breach, qualified cold reopens,
  complete counters, no ring loss. Debt test on per-second suffix samples:
  disqualify when at least four of five cases of any size/tier show a positive
  age difference with a positive bytes or batches difference, or a byte
  difference at least the setting's soft byte envelope.
- Fit quality: refit five times dropping the same paired repeat at all sizes;
  an absolute log residual above 0.20 in any fit, a nonpositive input, or a
  pass/fail flip across the six predictions makes the metric unqualified.
- Pass: qualified ratio prediction <= 1.0 and qualified cold prediction <= 60 s
  in both tiers on the same setting. Fail: a qualified prediction over its
  bound or a measured disqualification. Otherwise unqualified.
- Any setting passes: "Exact sufficient (projected)". Both fail: "Exact
  insufficient". Else "inconclusive". C-scaled never enters the decision.
- Selection when both pass: minimize the tier sum of fitted coordinator bytes
  per successful put at 100M; ties and an unqualified tie-break pick E4.
- A0 V6 gate first: without a V6 pass receipt for the same tree and binary
  as the run, the verdict is "unqualified: A0 V6 device-reconciliation gate
  not passed" and every setting is unqualified, never pass or fail.

V6 receipt schema (JSON): {"schema_version": 1, "gate":
"a0_v6_device_reconciliation", "passed": true, "tree": "<run tree id>",
"binary_sha256": "<hex>", "max_abs_residual_ratio": <= 0.10, ...}. "tree"
and "binary_sha256" must equal RESULTS_DIR/run-manifest.json.

Usage: analyze_a1.py RESULTS_DIR --v6-receipt V6.json [--output decision.json]
RESULTS_DIR holds run-manifest.json and <case>/case-metrics.json from run_a1.py.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import a1_cases  # noqa: E402
from a1_metrics import median  # noqa: E402

RATIO_TARGET_N = 709_000_000
COLD_TARGET_N = 100_000_000
TIEBREAK_TARGET_N = 100_000_000
RATIO_BOUND = 1.0
COLD_BOUND_S = 60.0
RESIDUAL_LIMIT = 0.20
METRICS = {
    "ratio": ("ratio", RATIO_TARGET_N, RATIO_BOUND),
    "cold": ("cold_open_to_first_read_s", COLD_TARGET_N, COLD_BOUND_S),
}


def ols(points: list[tuple[float, float]]) -> tuple[float, float]:
    """Least squares y = a + b x."""
    count = len(points)
    mean_x = sum(x for x, _ in points) / count
    mean_y = sum(y for _, y in points) / count
    sxx = sum((x - mean_x) ** 2 for x, _ in points)
    sxy = sum((x - mean_x) * (y - mean_y) for x, y in points)
    slope = sxy / sxx
    return mean_y - slope * mean_x, slope


def fit(medians: dict[int, float], target: int) -> dict:
    """ln-ln OLS of medians by size; residuals in natural-log units."""
    if any(value is None or value <= 0 for value in medians.values()):
        return {"qualified_input": False, "reason": "nonpositive or missing fit input", "medians": medians}
    points = [(math.log(size), math.log(value)) for size, value in sorted(medians.items())]
    intercept, slope = ols(points)
    residuals = [y - (intercept + slope * x) for x, y in points]
    return {
        "qualified_input": True,
        "medians": medians,
        "intercept": intercept,
        "slope": slope,
        "log_residuals": residuals,
        "max_abs_residual": max(abs(value) for value in residuals),
        "prediction": math.exp(intercept + slope * math.log(target)),
        "target_n": target,
    }


def metric_fit(cases: dict[tuple[int, int], dict], field: str, target: int, bound: float) -> dict:
    """Full fit plus the five reduced fits that drop one paired repeat."""
    def medians(drop: int | None) -> dict[int, float | None]:
        out = {}
        for size in a1_cases.SIZES:
            values = [cases[(size, repeat)].get(field) for repeat in range(a1_cases.REPEATS) if repeat != drop]
            out[size] = None if any(value is None for value in values) else median(values)
        return out

    full = fit(medians(None), target)
    reduced = [fit(medians(repeat), target) for repeat in range(a1_cases.REPEATS)]
    fits = [full, *reduced]
    reasons = []
    # Two distinct sizes fit any two points exactly: the 0.20 residual screen
    # and the drop-one-repeat flip can never fire, so a verdict would rest on
    # unscreened noise. Refuse to qualify such a metric.
    if len(set(a1_cases.SIZES)) < 3:
        reasons.append("fewer than three sizes: a two-point log-log fit is exact")
    if not all(item["qualified_input"] for item in fits):
        reasons.append("nonpositive or missing fit input")
    else:
        if any(item["max_abs_residual"] > RESIDUAL_LIMIT for item in fits):
            reasons.append(f"absolute log residual above {RESIDUAL_LIMIT}")
        verdicts = {item["prediction"] <= bound for item in fits}
        if len(verdicts) > 1:
            reasons.append("pass/fail flips across the six predictions")
    qualified = not reasons
    return {
        "qualified": qualified,
        "unqualified_reasons": reasons,
        "bound": bound,
        "passes": qualified and full["prediction"] <= bound,
        "exceeds": qualified and full["prediction"] > bound,
        "full": full,
        "drop_one_repeat": reduced,
    }


def debt_screen(cases: dict[tuple[int, int], dict], soft_bytes: int) -> dict:
    """Measured debt disqualification per size from valid cases only;
    retains every case delta."""
    disqualified = []
    deltas = {}
    for size in a1_cases.SIZES:
        rows = [cases[(size, repeat)] for repeat in range(a1_cases.REPEATS)]
        deltas[size] = [row.get("debt") for row in rows]
        valid = [row["debt"] for row in rows if row.get("valid") and row["debt"].get("available")]
        growing = sum(1 for row in valid if row["age_ns"] > 0 and (row["bytes"] > 0 or row["batches"] > 0))
        large = sum(1 for row in valid if row["bytes"] >= soft_bytes)
        if growing >= 4:
            disqualified.append({"size": size, "rule": "age and bytes or batches grow in at least 4 of 5 cases", "cases": growing})
        if large >= 4:
            disqualified.append({"size": size, "rule": f"byte difference >= soft envelope {soft_bytes} in at least 4 of 5 cases", "cases": large})
    return {"disqualified": disqualified, "deltas": deltas}


def evaluate_setting(arm: str, by_tier: dict[str, dict[tuple[int, int], dict]]) -> dict:
    tiers = {}
    missing = []
    disqualified = []
    for tier in a1_cases.TIERS:
        cases = by_tier.get(tier, {})
        absent = [
            {"size": size, "repeat": repeat, "reasons": cases.get((size, repeat), {}).get("invalid_reasons", ["missing"])}
            for size in a1_cases.SIZES
            for repeat in range(a1_cases.REPEATS)
            if not cases.get((size, repeat), {}).get("valid")
        ]
        missing.extend({"tier": tier, **row} for row in absent)
        complete = {
            (size, repeat): cases.get((size, repeat), {})
            for size in a1_cases.SIZES
            for repeat in range(a1_cases.REPEATS)
        }
        debt = debt_screen(complete, a1_cases.SOFT_BYTES[arm])
        disqualified.extend({"tier": tier, **row} for row in debt["disqualified"])
        fits = {}
        if not absent:
            for name, (field, target, bound) in METRICS.items():
                fits[name] = metric_fit(complete, field, target, bound)
            fits["bytes_per_put_tiebreak"] = metric_fit(
                complete, "coordinator_bytes_per_put", TIEBREAK_TARGET_N, math.inf
            )
        tiers[tier] = {"debt": debt, "fits": fits}
    decision_fits = [tiers[tier]["fits"].get(name) for tier in a1_cases.TIERS for name in METRICS]
    exceeds = [fit for fit in decision_fits if fit and fit["exceeds"]]
    if disqualified or exceeds:
        status = "fail"
    elif not missing and all(fit and fit["passes"] for fit in decision_fits):
        status = "pass"
    else:
        status = "unqualified"
    return {
        "arm": arm,
        "status": status,
        "missing_or_invalid_cases": missing,
        "measured_disqualifications": disqualified,
        "exceeding_predictions": [
            {"tier": tier, "metric": name, "prediction": tiers[tier]["fits"][name]["full"]["prediction"]}
            for tier in a1_cases.TIERS
            for name in METRICS
            if tiers[tier]["fits"].get(name, {}).get("exceeds")
        ],
        "tiers": tiers,
    }


def decide(settings: dict[str, dict]) -> dict:
    statuses = {arm: settings[arm]["status"] for arm in a1_cases.DECISION_ARMS}
    passing = [arm for arm in a1_cases.DECISION_ARMS if statuses[arm] == "pass"]
    if passing:
        verdict = "Exact sufficient (projected)"
    elif all(status == "fail" for status in statuses.values()):
        verdict = "Exact insufficient"
    else:
        verdict = "inconclusive"
    selected = "e4"
    selection = "E4 provisional"
    if len(passing) == 1:
        selected = passing[0]
        selection = f"only {passing[0].upper()} passes"
    elif len(passing) == 2:
        sums = {}
        for arm in passing:
            fits = [settings[arm]["tiers"][tier]["fits"]["bytes_per_put_tiebreak"] for tier in a1_cases.TIERS]
            if all(fit["qualified"] for fit in fits):
                sums[arm] = sum(fit["full"]["prediction"] for fit in fits)
        if len(sums) == 2 and sums["e16"] < sums["e4"]:
            selected = "e16"
            selection = "E16: lower tier-sum coordinator bytes per put at 100M"
        else:
            selection = "E4: lower or tied tie-break, or tie-break unqualified"
    return {
        "verdict": verdict,
        "setting_status": statuses,
        "selected_setting": selected,
        "selection": selection,
        "next_step": {
            "Exact sufficient (projected)": "confirm restart and stable operation at 100M in Q1",
            "Exact insufficient": "separate owner-approved PerCell design track (identity/cadence coupling); does not nominate C or authorize C7",
            "inconclusive": "E4 stays provisional; report the evidence",
        }[verdict],
        "scope": "[INFERENCE] local single-host extrapolation; not validator qualification; 709M is an extrapolation reference",
    }


def load(results: Path) -> dict[str, dict[str, dict[tuple[int, int], dict]]]:
    """arm -> tier -> (size, repeat) -> case metrics."""
    out: dict[str, dict[str, dict[tuple[int, int], dict]]] = {}
    for path in sorted(results.glob("*/case-metrics.json")):
        with path.open() as handle:
            metrics = json.load(handle)
        identity = metrics["identity"]
        if identity["arm"] not in a1_cases.DECISION_ARMS or not isinstance(identity["repeat"], int):
            continue
        out.setdefault(identity["arm"], {}).setdefault(identity["tier"], {})[
            (identity["size"], identity["repeat"])
        ] = metrics
    return out


V6_GATE = "a0_v6_device_reconciliation"
V6_RESIDUAL_LIMIT = 0.10
V6_UNQUALIFIED = "unqualified: A0 V6 device-reconciliation gate not passed"


def v6_gate(receipt: Path | None, manifest: dict) -> dict:
    """Pass only with a schema-valid passing receipt for this run's tree and binary."""
    if receipt is None or not receipt.is_file():
        return {"passed": False, "reason": "V6 pass receipt missing"}
    try:
        value = json.loads(receipt.read_text())
    except (OSError, ValueError) as error:
        return {"passed": False, "reason": f"V6 receipt unreadable: {error}"}
    ratio = value.get("max_abs_residual_ratio") if isinstance(value, dict) else None
    if not (
        isinstance(value, dict)
        and value.get("schema_version") == 1
        and value.get("gate") == V6_GATE
        and value.get("passed") is True
        and isinstance(ratio, (int, float))
        and not isinstance(ratio, bool)
        and 0 <= ratio <= V6_RESIDUAL_LIMIT
    ):
        return {"passed": False, "reason": "V6 receipt failed or malformed", "receipt": str(receipt)}
    for field in ("tree", "binary_sha256"):
        if not manifest.get(field) or value.get(field) != manifest.get(field):
            return {
                "passed": False,
                "reason": f"V6 receipt {field} does not match the run",
                "receipt": value.get(field),
                "run": manifest.get(field),
            }
    return {"passed": True, "receipt": str(receipt), "tree": value["tree"], "max_abs_residual_ratio": ratio}


def analyze(results: Path, v6_receipt: Path | None) -> dict:
    manifest_path = results / "run-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text()) if manifest_path.is_file() else {}
    except (OSError, ValueError):
        manifest = {}
    gate = v6_gate(v6_receipt, manifest)
    if not gate["passed"]:
        # Gate first: no case evidence is read or evaluated, so a damaged
        # case file cannot replace this verdict and no measured status leaks.
        return {
            "schema_version": 1,
            "decision": {
                "verdict": V6_UNQUALIFIED,
                "setting_status": {arm: "unqualified" for arm in a1_cases.DECISION_ARMS},
                "selected_setting": "e4",
                "selection": "E4 provisional",
                "next_step": "pass the A0 V6 device-reconciliation gate on this tree and binary, then rerun the analysis",
                "scope": "[INFERENCE] local single-host extrapolation; not validator qualification",
                "v6_gate": gate,
            },
            "settings": {},
        }
    loaded = load(results)
    settings = {arm: evaluate_setting(arm, loaded.get(arm, {})) for arm in a1_cases.DECISION_ARMS}
    decision = decide(settings)
    decision["v6_gate"] = gate
    return {"schema_version": 1, "decision": decision, "settings": settings}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("results", type=Path)
    parser.add_argument("--v6-receipt", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    report = analyze(arguments.results, arguments.v6_receipt)
    text = json.dumps(report, indent=2, default=str)
    if arguments.output:
        arguments.output.write_text(text + "\n")
    print(json.dumps(report["decision"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
