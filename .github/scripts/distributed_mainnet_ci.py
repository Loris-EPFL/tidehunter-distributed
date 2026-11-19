#!/usr/bin/env python3
"""Exact distributed replay correctness selection; local-file routes, not RDMA."""
import os
import re

from checks import cargo


CASES = (
    (("--lib", "backends::tidehunter::replay"), None),
    (("--test", "distributed_mainnet"), 1),
)


def main():
    environment = os.environ.copy()
    environment["RUST_TEST_THREADS"] = "1"
    for selection, expected in CASES:
        output = cargo(environment, "test", "-p", "kv-research", "--features",
                       "nvmeof,test-utils", *selection, "--", "--nocapture", capture=True)
        populations = re.findall(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;", output)
        if len(populations) != 1 or int(populations[0]) < 1:
            raise RuntimeError(f"Replay selection {selection} did not execute any tests")
        if expected is not None and int(populations[0]) != expected:
            raise RuntimeError(f"Replay selection {selection} did not execute exactly {expected} tests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
