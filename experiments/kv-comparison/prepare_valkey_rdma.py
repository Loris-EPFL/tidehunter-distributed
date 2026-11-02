#!/usr/bin/env python3
"""Build the pinned upstream Valkey server, CLI, and standalone RDMA client library.

Requires git, make, a C toolchain, and RDMA-CM/verbs development headers and
libraries. Creates a new checkout; never modifies an existing source tree.
"""
import argparse
import subprocess
from pathlib import Path

import time
COMMIT = "fe8d406c0b5a64ec897f6455e4de684629883764"
URL = "https://github.com/valkey-io/valkey.git"


def run(argv, cwd=None, deadline=None):
    remaining = deadline - time.monotonic() if deadline is not None else 3600
    if remaining <= 0:
        raise TimeoutError("Valkey preparation deadline exceeded")
    return subprocess.run([str(x) for x in argv], cwd=cwd, check=True, timeout=remaining,
                          stdout=subprocess.PIPE if argv[:3] == ["git", "rev-parse", "HEAD"] else None,
                          text=True)


def prepare(destination):
    destination = destination.absolute()
    if destination.exists():
        raise ValueError("destination must not already exist")
    deadline = time.monotonic() + 3600
    run(["git", "clone", "--depth", "1", "--branch", "9.0.7", "--single-branch", URL, destination], deadline=deadline)
    observed = run(["git", "rev-parse", "HEAD"], destination, deadline).stdout.strip()
    if observed != COMMIT:
        raise ValueError(f"Valkey tag does not resolve to pinned commit: {observed}")
    # Services use systemd's exec protocol, not sd_notify. Disable optional
    # autodetected libsystemd linkage so identical source yields a fixed ABI.
    run(["make", "BUILD_RDMA=yes", "USE_SYSTEMD=no", "-j2"], destination, deadline)
    # Server's bundled libvalkey uses server-provided SDS/DICT. Rebuild the
    # standalone library only after server and CLI linking has completed.
    client = destination / "deps/libvalkey"
    run(["make", "clean"], client, deadline)
    run(["make", "USE_RDMA=1", "static", "-j2"], client, deadline)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    prepare(parser.parse_args().output)


if __name__ == "__main__":
    main()
