#!/usr/bin/env python3
"""A0 V6 controller: run kv-research once per tier and issue the V6 receipt.

For each tier the caller gives a complete kv-research config (the V6 Sui
replay fixture case with that tier). The controller writes a copy with:
  * `source_revision` = `--tree`, the same string given to run_a1.py --tree,
    so v6_receipt.py can bind every run to the measured tree;
  * `diagnostics.index_checkpoint` = true;
then runs `BINARY --config CONFIG --output RUN_DIR` with a fresh output
directory and finally writes the receipt from all run directories
(v6_receipt.build_receipt). A failed kv-research run is kept and makes the
receipt fail; the controller never retries or drops a run.

Not done here (procedure duties stay with the operator, who reports them with
`--reject`): tmpfs placement and allocation ceiling, the equal-duration idle
gate, and rejection of unrelated device traffic.

Usage: run_v6.py --tree ID --binary KV_RESEARCH --work DIR \
           --config-native N.json --config-synchronous S.json [--reject R]...
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v6_receipt  # noqa: E402


def prepare_config(base: Path, tree: str, destination: Path) -> Path:
    config = json.loads(base.read_text())
    if not isinstance(config, dict):
        raise ValueError(f"{base}: config is not an object")
    config["source_revision"] = tree
    diagnostics = config.setdefault("diagnostics", {})
    if not isinstance(diagnostics, dict):
        raise ValueError(f"{base}: diagnostics is not an object")
    diagnostics["index_checkpoint"] = True
    destination.write_text(json.dumps(config, indent=2) + "\n")
    return destination


def run_tier(binary: Path, tree: str, tier: str, base: Path, work: Path) -> dict:
    tier_dir = work / tier
    tier_dir.mkdir(parents=True, exist_ok=False)
    config = prepare_config(base, tree, tier_dir / "config.json")
    output = tier_dir / "output"
    completed = subprocess.run(
        [str(binary), "--config", str(config), "--output", str(output)],
        capture_output=True,
        text=True,
        check=False,
    )
    (tier_dir / "stderr.log").write_text(completed.stderr[-65536:])
    return {"tier": tier, "output": str(output), "exit_status": completed.returncode}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tree", required=True, help="tree identity, byte-equal to run_a1.py --tree")
    parser.add_argument("--binary", type=Path, required=True, help="kv-research binary used for V6 and A1")
    parser.add_argument("--work", type=Path, required=True, help="fresh controller directory")
    parser.add_argument("--config-native", type=Path, required=True)
    parser.add_argument("--config-synchronous", type=Path, required=True)
    parser.add_argument("--reject", action="append", default=[], help="procedure rejection (idle gate, foreign traffic, tmpfs ceiling)")
    parser.add_argument("--output", type=Path, help="receipt path (default WORK/v6-receipt.json)")
    arguments = parser.parse_args(argv)
    if not arguments.tree:
        parser.error("--tree must not be empty")
    arguments.work.mkdir(parents=True, exist_ok=False)
    runs = [
        run_tier(arguments.binary, arguments.tree, "native", arguments.config_native, arguments.work),
        run_tier(arguments.binary, arguments.tree, "synchronous", arguments.config_synchronous, arguments.work),
    ]
    rejections = list(arguments.reject) + [
        f"{run['tier']} run exited with status {run['exit_status']}" for run in runs if run["exit_status"] != 0
    ]
    receipt = v6_receipt.build_receipt(
        arguments.tree, arguments.binary, [Path(run["output"]) for run in runs], rejections
    )
    receipt["controller_runs"] = runs
    output = arguments.output or arguments.work / "v6-receipt.json"
    output.write_text(json.dumps(receipt, indent=2) + "\n")
    return 0 if receipt["passed"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
