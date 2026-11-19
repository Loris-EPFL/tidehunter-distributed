#!/usr/bin/env python3
"""Gate leased distributed replay on ordinary files; not a CLI or RDMA proof."""
import os
import re

from checks import cargo


REPLAY_MODULE = "backends::tidehunter::replay"


def main():
    environment = os.environ.copy()
    environment["RUST_TEST_THREADS"] = "1"
    output = cargo(environment, "test", "-p", "kv-research", "--features",
                   "nvmeof,test-utils", "--lib", REPLAY_MODULE, "--", "--nocapture", capture=True)
    populations = re.findall(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;", output)
    if len(populations) != 1 or int(populations[0]) < 1:
        raise RuntimeError(f"Replay selection {REPLAY_MODULE} did not execute a nonempty passing suite")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
