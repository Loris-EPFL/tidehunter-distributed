#!/usr/bin/env python3
"""A0 V6 device-reconciliation pass receipt (roadmap Phase 6, V6).

Each argument is one kv-research run output directory of the V6 procedure
(`diagnostics.index_checkpoint` on): `write-diagnostics.json`, `manifest.json`
and `summary.json`, all written by that run. The receipt is the one
`experiments/index-a1/analyze_a1.py --v6-receipt` requires:

    {"schema_version": 1, "gate": "a0_v6_device_reconciliation", "passed": bool,
     "tree": <run tree id>, "binary_sha256": <hex>, "max_abs_residual_ratio": <number>}

`passed` is true only when all of these hold; every failure is listed:
  * every run was produced by the measured binary: the run manifest's
    `binary.blake2b512` (kv-research self-hash) equals BLAKE2b-512 of `--binary`;
  * every run was built from the measured tree: `config.source_revision`
    equals `--tree` byte for byte (set it to run_a1.py's `--tree`);
  * every run completed, with a tier receipt of `native` or `synchronous`, and
    both prescribed tiers are covered;
  * every run's fail-closed `device_reconciliation` gate is eligible (post-sync
    D, no ring loss, sync, counters, allocation complete);
  * the largest abs(D-A)/D over all runs is at most 0.10;
  * the run controller reported no rejection (`--reject`: idle gate, foreign
    device traffic).
`binary_sha256` is SHA-256 of `--binary`, the digest run_a1.py records.

Usage: v6_receipt.py --tree ID --binary KV_RESEARCH --output V6.json [--reject R]... RUN_DIR...
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

GATE = "a0_v6_device_reconciliation"
RESIDUAL_LIMIT = 0.10
TIERS = ("native", "synchronous")


def digest(path: Path, algorithm: str) -> str:
    hasher = hashlib.new(algorithm)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def sha256(path: Path) -> str:
    """Same digest as run_a1.py: SHA-256 hex of the file bytes."""
    return digest(path, "sha256")


def read_json(path: Path) -> tuple[dict | None, str | None]:
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return None, f"{path.name} unreadable: {error}"
    if not isinstance(value, dict):
        return None, f"{path.name} is not an object"
    return value, None


def run_result(run: Path, tree: str, binary_blake2b512: str) -> dict:
    """One run's evidence: identity, tier, gate eligibility, ratio, reasons."""
    reasons: list[str] = []
    files = {}
    for name in ("write-diagnostics.json", "manifest.json", "summary.json"):
        value, error = read_json(run / name)
        files[name] = value or {}
        if error:
            reasons.append(error)
    diagnostics, manifest, summary = (
        files["write-diagnostics.json"],
        files["manifest.json"],
        files["summary.json"],
    )
    # kv-research writes the run's binary identity and config under "run"
    # (report.rs: manifest["run"] = manifest_extra) for synthetic and replay runs.
    run_extra = manifest.get("run") if isinstance(manifest.get("run"), dict) else {}
    binary = (run_extra.get("binary") or {}).get("blake2b512")
    if binary != binary_blake2b512:
        reasons.append("run binary differs from --binary")
    revision = (run_extra.get("config") or {}).get("source_revision")
    if revision != tree:
        reasons.append("run config.source_revision differs from --tree")
    if summary.get("status") != "completed":
        reasons.append("run did not complete")
    tier = (summary.get("durability") or {}).get("tier")
    if tier not in TIERS:
        reasons.append("run has no native or synchronous tier receipt")
    gate = (diagnostics.get("index_checkpoint") or {}).get("device_reconciliation")
    if not isinstance(gate, dict):
        gate = {}
        reasons.append("no device_reconciliation gate")
    elif gate.get("eligible") is not True:
        reasons.extend(f"gate: {reason}" for reason in gate.get("reasons") or ["not eligible"])
    ratio = (gate.get("reconciliation") or {}).get("abs_residual_ratio")
    if isinstance(ratio, bool) or not isinstance(ratio, (int, float)):
        ratio = None
        reasons.append("no residual ratio")
    diagnostics_path = run / "write-diagnostics.json"
    return {
        "run": str(run),
        "tier": tier,
        "ratio": ratio,
        "diagnostics_sha256": sha256(diagnostics_path) if diagnostics_path.is_file() else None,
        "valid": not reasons,
        "reasons": reasons,
    }


def build_receipt(tree: str, binary: Path, run_dirs: list[Path], rejections: list[str]) -> dict:
    binary_blake2b512 = digest(binary, "blake2b")
    runs = [run_result(run, tree, binary_blake2b512) for run in run_dirs]
    failures = [f"controller: {reason}" for reason in rejections]
    if not runs:
        failures.append("no V6 runs")
    failures.extend(f"{run['run']}: {reason}" for run in runs for reason in run["reasons"])
    covered = sorted({run["tier"] for run in runs if run["valid"]})
    missing = [tier for tier in TIERS if tier not in covered]
    if missing:
        failures.append(f"tiers without a valid run: {', '.join(missing)}")
    ratios = [run["ratio"] for run in runs]
    max_ratio = max(ratios) if runs and all(ratio is not None for ratio in ratios) else None
    if max_ratio is not None and max_ratio > RESIDUAL_LIMIT:
        failures.append(f"max abs(D-A)/D {max_ratio} above {RESIDUAL_LIMIT}")
    return {
        "schema_version": 1,
        "gate": GATE,
        "passed": not failures,
        "tree": tree,
        "binary_sha256": sha256(binary),
        "binary_blake2b512": binary_blake2b512,
        "max_abs_residual_ratio": max_ratio,
        "residual_limit": RESIDUAL_LIMIT,
        "tiers_covered": covered,
        "runs": runs,
        "failures": failures,
    }


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tree", required=True, help="tree identity, byte-equal to run_a1.py --tree and each run's config.source_revision")
    parser.add_argument("--binary", type=Path, required=True, help="kv-research binary used for V6 and A1")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reject", action="append", default=[], help="controller rejection reason (idle gate, foreign traffic)")
    parser.add_argument("runs", type=Path, nargs="+", help="kv-research output directory of each V6 run")
    arguments = parser.parse_args(argv)
    receipt = build_receipt(arguments.tree, arguments.binary, arguments.runs, arguments.reject)
    arguments.output.write_text(json.dumps(receipt, indent=2) + "\n")
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
