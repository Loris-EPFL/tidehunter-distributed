#!/usr/bin/env python3
"""Run unchanged cached-image replay qualifiers, never overlapping lifecycles."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
import local


def test_binary(messages):
    candidates = {item["executable"] for line in messages.splitlines()
                  if (item := json.loads(line)).get("reason") == "compiler-artifact"
                  and item.get("target", {}).get("name") == "kv_research"
                  and item.get("profile", {}).get("test") and item.get("executable")
                  and "lib" in item["target"]["kind"]}
    if len(candidates) != 1:
        raise ValueError("expected exactly one kv-research library test executable")
    return Path(candidates.pop())


def run(binary, messages, output):
    output.mkdir(parents=True, exist_ok=False)
    receipt = {"status": "starting", "rdma_execution": "not executed; TCP only, no RDMA hardware route",
               "sequential_lifecycles": True, "qualifiers": {}}
    local.dump(output / "receipt.json", receipt)
    deadline = time.monotonic() + 1440

    def budget(limit):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("sequential replay CI deadline exceeded")
        return min(limit, remaining)

    try:
        executable = test_binary(messages.read_text())
        # Qualifiers deliberately never pull: admit the exact lifecycle image pins first.
        for image in ("pd", "tikv", "cassandra", "foundationdb", "valkey_tcp"):
            with (output / f"pull-{image}.log").open("w") as log:
                subprocess.run(["docker", "pull", local.IMAGES[image]], check=True,
                               stdout=log, stderr=subprocess.STDOUT, timeout=budget(180))
        for name, extra in (("tikv_cql", []), ("fdb_rdma", ["--test-binary", str(executable)])):
            started = time.monotonic()
            entry = {"status": "running"}
            receipt["qualifiers"][name] = entry
            local.dump(output / "receipt.json", receipt)
            try:
                with (output / f"{name}.log").open("w") as log:
                    subprocess.run([sys.executable, "-B", str(local.HERE / f"mainnet_replay_{name}.py"),
                                    "--binary", str(binary), "--output", str(output / name), *extra],
                                   check=True, stdout=log, stderr=subprocess.STDOUT, timeout=budget(900))
                entry["status"] = "completed"
            finally:
                entry["elapsed_seconds"] = time.monotonic() - started
                local.dump(output / "receipt.json", receipt)
        receipt["status"] = "completed"
    except BaseException as error:
        receipt["status"] = "failed"
        receipt["error"] = str(error)
        raise
    finally:
        local.dump(output / "receipt.json", receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--test-messages", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    run(args.binary.resolve(strict=True), args.test_messages, args.output.resolve())


if __name__ == "__main__":
    main()
