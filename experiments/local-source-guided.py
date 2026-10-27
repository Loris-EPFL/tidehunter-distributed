#!/usr/bin/env python3
"""Matched local preparation/read-ahead ablations; no cloud or RDMA operations."""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path


import sys


HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import portable_transport


spec = importlib.util.spec_from_file_location(
    "local_compare", HERE / "aws-sharded-wal/local_compare.py"
)
local = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--windows", default="1,8,32")
    args = parser.parse_args()
    if not 1 <= args.repetitions <= 5:
        raise ValueError("bounded repetition count required")
    windows = args.windows.split(",")
    if len(set(windows)) != len(windows) or any(w not in {"1", "8", "32"} for w in windows):
        raise ValueError("windows must be a unique subset of 1,8,32")
    binaries = {
        name: getattr(args, name).resolve() for name in ("baseline", "candidate")
    }
    hashes = {name: sha256(path) for name, path in binaries.items()}
    args.output.mkdir()
    transport_config = args.output / "requested-configs" / "transport.json"
    transport_config.parent.mkdir()
    portable_transport.write_transport_config(
        transport_config, portable_transport.transport_for_case("tcp")
    )
    # Every TCP comparison uses zero application value cache. This avoids
    # attributing previous cache-hit advantages to preparation/read-ahead work.
    tcp = ["--backend", "tcp", "--tcp-pipeline", "true", "--tcp-read-cache-bytes", "0"]
    configs = [("previous-tcp", "baseline", tcp)]
    for label, workers, pages in [
        ("serial-control-tcp", 1, 1),
        ("parallel-preparation-tcp", 2, 1),
        ("prefetch-tcp", 1, 2),
        ("combined-tcp", 2, 2),
    ]:
        configs.append(
            (
                label,
                "candidate",
                tcp
                + ["--tcp-preparation-workers", str(workers)]
                + ["--tcp-scan-prefetch-pages", str(pages)],
            )
        )
    configs.extend(
        [
            ("previous-native", "baseline", ["--backend", "native-sync"]),
            ("current-native", "candidate", ["--backend", "native-sync"]),
        ]
    )
    common = [
        "--repetitions", "1", "--windows", args.windows,
        "--values", "1024,8192", "--batch-sizes", "1,32",
        "--batches", "128", "--mixed", "32", "--reads", "512",
    ]
    plan = {
        "binary_sha256": hashes,
        "binaries": {name: str(path) for name, path in binaries.items()},
        "transport_config": str(transport_config),
        "configs": configs,
        "common": common,
        "repetitions": args.repetitions,
        "case_count": args.repetitions * len(configs) * 4 * len(windows),
        "topology": "five loopback storage processes on one shared SSD; no RDMA",
        "cache_policy": "zero application value-cache bytes for every TCP case; native unchanged",
        "durability": "native file/directory barrier and distributed durable acknowledgements",
        "timing_scope": "complete submitted windows, exact scalar/multi-get/scan bytes, reopen audits",
        "limits": "local software comparison only; no independent-NVMe scaling claim",
    }
    (args.output / "plan.json").write_text(json.dumps(plan, indent=2) + "\n")
    for repetition in range(args.repetitions):
        offset = repetition % len(configs)
        order = configs[offset:] + configs[:offset]
        if repetition % 2:
            order.reverse()
        for label, name, flags in order:
            if sha256(binaries[name]) != hashes[name]:
                raise ValueError("benchmark executable changed during campaign")
            output = args.output / f"{label}-r{repetition}"
            runtime_transport = (
                ["--transport-config", str(transport_config)] if name == "candidate" else []
            )
            local.run(
                [str(binaries[name]), "--output", str(output), *common,
                 *runtime_transport, *flags],
                output,
            )
            if not (output / "COMPLETE").exists():
                raise ValueError(f"missing full-value/reopen verification: {label}")
    if any(sha256(binaries[name]) != hashes[name] for name in binaries):
        raise ValueError("benchmark executable changed at campaign completion")
    (args.output / "COMPLETE").write_text(
        "all source-guided matched ablations and full-value/reopen audits passed\n"
    )


if __name__ == "__main__":
    main()
