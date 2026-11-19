#!/usr/bin/env python3
"""Supervise the maintained single- and four-owner kernel TCP proofs.

The proof owns setup, admission, second-open recovery and teardown. This wrapper
adds signal-to-finally delivery and inventory receipts, not another NVMe lifecycle.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[2]


def dump(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def inventory():
    config = Path("/sys/kernel/config/nvmet")
    controllers = {}
    for path in Path("/sys/class/nvme").glob("nvme[0-9]*"):
        controllers[path.name] = {name: (path / name).read_text().strip()
                                  for name in ("subsysnqn", "transport")}
    return {"subsystems": sorted(p.name for p in (config / "subsystems").iterdir()),
            "ports": sorted(p.name for p in (config / "ports").iterdir()),
            "controllers": controllers}


def interrupted(signum, _frame):
    # The maintained proof catches this exception and executes stop_tide in finally.
    # Do not interrupt that drain a second time.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    raise RuntimeError(f"CI proof interrupted by signal {signum}")


def run(args):
    sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
    import local_multiowner_check
    import local_dry_run_targets

    dump(args.state / "before.json", inventory())
    dump(args.state / "host.json", {"kernel": os.uname().release,
         "cpu_count": os.cpu_count(), "meminfo": Path("/proc/meminfo").read_text(),
         "disk_free_bytes": shutil.disk_usage(args.state).free,
         "route": "kernel NVMe/TCP; four colocated owners; one host/shared filesystem; no RDMA"})
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    # Owners prewrite by default, so the gating file pass opts out explicitly.
    sys.argv = [str(local_multiowner_check.__file__), "--binary", str(args.binary.resolve(strict=True)),
                "--libs", str(args.state / "libs"), "--owners", "4",
                "--output", str(args.state / "proof"), "--no-wal-prewrite"]
    result = 1
    # sudo already runs this wrapper in the hosted runner's host namespace.
    # The local development Docker image is not installed on hosted runners.
    original_root = local_dry_run_targets.ROOT
    local_dry_run_targets.ROOT = []
    try:
        result = local_multiowner_check.main()
        first = json.loads((args.state / "proof/proof.json").read_text())
        prewrite = {"attempted": False}
        if (first.get("cleanup_complete") is True and first.get("fixture_removed") is True
                and json.loads((args.state / "before.json").read_text()) == inventory()):
            prewrite["attempted"] = True
            sys.argv = [str(local_multiowner_check.__file__), "--binary", str(args.binary.resolve(strict=True)),
                        "--libs", str(args.state / "libs"), "--owners", "4",
                        "--output", str(args.state / "proof-prewrite"), "--wal-prewrite"]
            try:
                prewrite["exit_code"] = local_multiowner_check.main()
            except (Exception, SystemExit) as error:
                prewrite["error"] = str(error)
            prewrite["passed"] = prewrite.get("exit_code") == 0
        else:
            prewrite["reason"] = "first pass cleanup unconfirmed; second pass is unsafe"
        dump(args.state / "prewrite-result.json", prewrite)
        return result
    finally:
        local_dry_run_targets.ROOT = original_root
        dump(args.state / "after.json", inventory())
        dump(args.state / "result.json", {"exit_code": result, "passed": result == 0})


def qualify(args):
    """Run the four single-owner cells with independent cleanup checks."""
    sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
    from local_dry_run_targets import KERNEL_LIVE_HISTORIES, _kernel_live_worker

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    root = args.state / "single-owner"
    root.mkdir(mode=0o700)
    before = inventory()
    dump(root / "before.json", before)
    results = {}
    for history in KERNEL_LIVE_HISTORIES:
        output = root / history
        output.mkdir(mode=0o700)
        started = time.monotonic()
        # Each history gets its own deadline. Sequential runs bound disk
        # use and let the existing worker remove each clean fixture first.
        seconds = 180
        proof, evidence = _kernel_live_worker(
            output, args.binary, "tcp", seconds, time.time() + seconds,
            history_persistence=history, owner_wal_file_bytes=8 * 1024**2)
        after = inventory()
        dump(output / "after.json", after)
        result = {"complete": proof["complete"], "fixture_removed": proof["fixture_removed"],
                  "inventory_restored": before == after, "evidence": str(evidence.relative_to(root)),
                  "elapsed_seconds": round(time.monotonic() - started, 3)}
        results[history] = result
        dump(root / "summary.json", results)
        print(f"{history}: {json.dumps(result, sort_keys=True)}", flush=True)
        if not result["fixture_removed"] or not result["inventory_restored"]:
            raise RuntimeError("Single-owner kernel TCP cleanup unconfirmed; refusing another proof")
        if signal.getsignal(signal.SIGTERM) == signal.SIG_IGN:
            return 1
    return 0 if all(result["complete"] for result in results.values()) else 1


def cleanup(args):
    """Require the proof's finally cleanup and independently restored inventories."""
    before = json.loads((args.state / "before.json").read_text())
    after = inventory()
    dump(args.state / "cleanup-inventory.json", after)
    proof = json.loads((args.state / "proof/proof.json").read_text())
    complete = (proof.get("cleanup_complete") is True and proof.get("fixture_removed") is True
                and before == after)
    prewrite_path = args.state / "proof-prewrite"
    if prewrite_path.exists():
        prewrite = json.loads((prewrite_path / "proof.json").read_text())
        prewrite_clean = (prewrite.get("cleanup_complete") is True
                          and prewrite.get("fixture_removed") is True)
        complete = complete and prewrite_clean
        dump(args.state / "prewrite-cleanup.json", {"complete": prewrite_clean})
    dump(args.state / "cleanup.json", {"complete": complete,
         "proof_cleanup_complete": proof.get("cleanup_complete"),
         "fixture_removed": proof.get("fixture_removed"), "inventory_restored": before == after})
    if not complete:
        raise RuntimeError("Owned kernel TCP cleanup unconfirmed; retained proof names exact resources")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("qualify", "run", "cleanup"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("requires sudo on the hosted runner")
    if not args.state.is_relative_to("/var/lib"):
        parser.error("state must be an owned /var/lib directory")
    for path in (args.state, *args.state.parents):
        stat = path.stat()
        if path.is_symlink() or stat.st_uid != 0 or stat.st_mode & 0o022:
            parser.error(f"untrusted root-owned state ancestor: {path}")
    if args.action != "cleanup" and args.binary is None:
        parser.error(f"{args.action} requires --binary")
    return {"qualify": qualify, "run": run, "cleanup": cleanup}[args.action](args)


if __name__ == "__main__":
    raise SystemExit(main())
