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

# The hook changes only the standalone client, after server/CLI linking.
# Exact one-occurrence replacements fail closed on any source mismatch.
HOOKS = (
    ("static valkeyContextFuncs valkeyContextRdmaFuncs;",
     """extern void kv_diag_poll_begin(void);
extern void kv_diag_poll_end(int);
extern void kv_diag_cq_poll_begin(void);
extern void kv_diag_cq_poll_end(int);
static valkeyContextFuncs valkeyContextRdmaFuncs;"""),
    ("    ret = ibv_poll_cq(ctx->cq, 1, &wc);",
     """    kv_diag_cq_poll_begin();
    ret = ibv_poll_cq(ctx->cq, 1, &wc);
    kv_diag_cq_poll_end(ret);"""),
    ("    ret = poll(pfd, VALKEY_RDMA_POLLFD_MAX, timed - now);",
     """    kv_diag_poll_begin();
    ret = poll(pfd, VALKEY_RDMA_POLLFD_MAX, timed - now);
    int kv_diag_errno = errno;
    kv_diag_poll_end(ret > 0 && (pfd[VALKEY_RDMA_POLLFD_CQ].revents & POLLIN));
    errno = kv_diag_errno;"""),
)


def hooked_source(source):
    for original, replacement in HOOKS:
        if source.count(original) != 1:
            raise ValueError("pinned libvalkey CQ hook does not match exactly")
        source = source.replace(original, replacement, 1)
    return source


def verify_hook(destination):
    relative = "deps/libvalkey/src/rdma.c"
    original = subprocess.run(
        ["git", "-C", str(destination), "show", f"HEAD:{relative}"],
        check=True, stdout=subprocess.PIPE, text=True,
    ).stdout
    actual = (destination / relative).read_text()
    if actual != hooked_source(original):
        raise ValueError("libvalkey CQ hook differs from the pinned diagnostic patch")


def install_hook(destination):
    source = destination / "deps/libvalkey/src/rdma.c"
    source.write_text(hooked_source(source.read_text()))
    verify_hook(destination)



def run(argv, cwd=None, deadline=None):
    remaining = deadline - time.monotonic() if deadline is not None else 3600
    if remaining <= 0:
        raise TimeoutError("Valkey preparation deadline exceeded")
    return subprocess.run([str(x) for x in argv], cwd=cwd, check=True, timeout=remaining,
                          stdout=subprocess.PIPE if argv[:3] == ["git", "rev-parse", "HEAD"] else None,
                          text=True)


def prepare(destination, cq_diagnostics=False, source=None):
    destination = destination.absolute()
    if destination.exists():
        raise ValueError("destination must not already exist")
    deadline = time.monotonic() + 3600
    if source is None:
        run(["git", "clone", "--depth", "1", "--branch", "9.0.7", "--single-branch", URL, destination], deadline=deadline)
    else:
        source = source.resolve(strict=True)
        run(["git", "clone", "--no-hardlinks", "--branch", "9.0.7", "--single-branch", source, destination], deadline=deadline)
    observed = run(["git", "rev-parse", "HEAD"], destination, deadline).stdout.strip()
    if observed != COMMIT:
        raise ValueError(f"Valkey tag does not resolve to pinned commit: {observed}")
    # Services use systemd's exec protocol, not sd_notify. Disable optional
    # autodetected libsystemd linkage so identical source yields a fixed ABI.
    run(["make", "BUILD_RDMA=yes", "USE_SYSTEMD=no", "-j2"], destination, deadline)
    if cq_diagnostics:
        install_hook(destination)
    # Server's bundled libvalkey uses server-provided SDS/DICT. Rebuild the
    # standalone library only after server and CLI linking has completed.
    client = destination / "deps/libvalkey"
    run(["make", "clean"], client, deadline)
    run(["make", "USE_RDMA=1", "static", "-j2"], client, deadline)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--source", type=Path, help="clone an existing pinned checkout without network access")
    parser.add_argument("--cq-diagnostics", action="store_true",
                        help="instrument standalone client CQ polling (measurement only)")
    parser.add_argument("--verify-hook", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.verify_hook:
        if args.output or args.cq_diagnostics or args.source:
            parser.error("--verify-hook cannot be combined with build options")
        verify_hook(args.verify_hook)
    elif args.output:
        prepare(args.output, args.cq_diagnostics, args.source)
    else:
        parser.error("--output is required")


if __name__ == "__main__":
    main()
