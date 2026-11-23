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


def read_proof(state, name):
    """Return (proof, None) or (None, reason) for one proof file."""
    path = state / name / "proof.json"
    try:
        proof = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        return None, f"{name} proof missing or unreadable: {error}"
    if not isinstance(proof, dict):
        return None, f"{name} proof is not a JSON object"
    return proof, None


def proof_defect(state, name):
    """Return the reason a proof is unclean, or None when it is clean."""
    proof, defect = read_proof(state, name)
    if defect is not None:
        return defect
    for key in ("complete", "cleanup_complete", "fixture_removed"):
        if proof.get(key) is not True:
            return f"{name} proof {key} is not true"
    errors = proof.get("errors")
    if not isinstance(errors, list) or errors:
        return f"{name} proof errors is not an empty list"
    return None


def teardown_defect(state, name):
    """Return the reason a proof's teardown is unconfirmed, or None when it is clean.

    The gating run() also requires `complete` and an empty `errors` list. The
    advisory cleanup action owns teardown only, so an admission or benchmark
    failure must not appear as a cleanup defect.
    """
    proof, defect = read_proof(state, name)
    if defect is not None:
        return defect
    for key in ("cleanup_complete", "fixture_removed"):
        if proof.get(key) is not True:
            return f"{name} proof {key} is not true"
    return None


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

    before = inventory()
    dump(args.state / "before.json", before)
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
    passed = False
    defect = None
    final = None
    # sudo already runs this wrapper in the hosted runner's host namespace.
    # The local development Docker image is not installed on hosted runners.
    original_root = local_dry_run_targets.ROOT
    local_dry_run_targets.ROOT = []
    try:
        result = local_multiowner_check.main()
        defect = proof_defect(args.state, "proof")
        prewrite = {"attempted": False}
        if defect is None and inventory() == before:
            prewrite["attempted"] = True
            sys.argv = [str(local_multiowner_check.__file__), "--binary", str(args.binary.resolve(strict=True)),
                        "--libs", str(args.state / "libs"), "--owners", "4",
                        "--output", str(args.state / "proof-prewrite"), "--wal-prewrite"]
            try:
                prewrite["exit_code"] = local_multiowner_check.main()
            except (Exception, SystemExit) as error:
                prewrite["error"] = str(error)
            prewrite["passed"] = prewrite.get("exit_code") == 0
            defect = proof_defect(args.state, "proof-prewrite")
        elif defect is None:
            defect = "first pass left the host inventory changed"
            prewrite["reason"] = "first pass cleanup unconfirmed; second pass is unsafe"
        else:
            prewrite["reason"] = "first pass proof is unclean; second pass is unsafe"
        final = inventory()
        if defect is None and final != before:
            defect = "last pass left the host inventory changed"
        prewrite["defect"] = defect
        dump(args.state / "prewrite-result.json", prewrite)
        # Both passes carry reviewed evidence and the last pass must restore the
        # exact host inventory, so each pass gates the step.
        passed = (result == 0 and prewrite.get("attempted") is True
                  and prewrite.get("passed") is True and defect is None
                  and final == before)
        return 0 if passed else 1
    finally:
        local_dry_run_targets.ROOT = original_root
        final = inventory() if final is None else final
        dump(args.state / "after.json", final)
        dump(args.state / "result.json", {"exit_code": 0 if passed else 1, "passed": passed})


def qualify(args):
    """Run the single-owner native and synchronous WAL-replay proofs."""
    sys.path.insert(0, str(ROOT / "experiments/kv-comparison"))
    from local_dry_run_targets import _kernel_live_worker

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    root = args.state / "single-owner"
    root.mkdir(mode=0o700)
    before = inventory()
    dump(root / "before.json", before)
    output = root / "wal_replay"
    output.mkdir(mode=0o700)
    started = time.monotonic()
    seconds = 180
    proof, evidence = _kernel_live_worker(
        output, args.binary, "tcp", seconds, time.time() + seconds,
        owner_wal_file_bytes=8 * 1024**2)
    after = inventory()
    dump(output / "after.json", after)
    result = {"complete": proof["complete"], "fixture_removed": proof["fixture_removed"],
              "inventory_restored": before == after, "evidence": str(evidence.relative_to(root)),
              "elapsed_seconds": round(time.monotonic() - started, 3)}
    dump(root / "summary.json", {"wal_replay": result})
    print(f"wal_replay: {json.dumps(result, sort_keys=True)}", flush=True)
    if not result["fixture_removed"] or not result["inventory_restored"]:
        raise RuntimeError("Single-owner kernel TCP cleanup unconfirmed")
    return 0 if result["complete"] and signal.getsignal(signal.SIGTERM) != signal.SIG_IGN else 1


def cleanup(args):
    """Confirm the proofs' teardown and the independently restored inventories.

    Only teardown evidence gates this advisory action. A benchmark or admission
    failure is the run step's verdict, not a cleanup defect.
    """
    before = json.loads((args.state / "before.json").read_text())
    after = inventory()
    dump(args.state / "cleanup-inventory.json", after)
    defects = [teardown_defect(args.state, "proof")]
    prewrite_path = args.state / "proof-prewrite"
    if prewrite_path.exists():
        prewrite_defect = teardown_defect(args.state, "proof-prewrite")
        defects.append(prewrite_defect)
        dump(args.state / "prewrite-cleanup.json", {"complete": prewrite_defect is None})
    defects = [defect for defect in defects if defect is not None]
    inventory_restored = before == after
    complete = not defects and inventory_restored
    dump(args.state / "cleanup.json", {"complete": complete,
         "defects": defects, "inventory_restored": inventory_restored})
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
