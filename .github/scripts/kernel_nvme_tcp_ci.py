#!/usr/bin/env python3
"""Supervise the maintained single- and four-owner kernel TCP proofs.

The proof owns setup, admission, second-open recovery and teardown. This wrapper
adds signal-to-finally delivery and inventory receipts, not another NVMe lifecycle.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]

TRACING_ROOTS = (Path("/sys/kernel/tracing"), Path("/sys/kernel/debug/tracing"))
NVMET_EVENTS = ("nvmet_req_init", "nvmet_req_complete")
KPROBE = "p:nvmet_tcp_qr nvmet_tcp_queue_response req=%di"


def write_text(path, value):
    path.write_text(value if value.endswith("\n") else value + "\n")


class NvmetTrace:
    """Failure-only nvmet trace for the single-owner abrupt-recovery proof.

    Two nvmet tracepoints plus one queue-response kprobe are armed for the
    single-owner run. Nothing is copied on success; on failure the buffer,
    event formats/listing and per-command counts go to the artifact directory.
    Diagnostic failures are themselves recorded and never mask the proof.
    """

    def __init__(self):
        self.root = None
        self.started = None
        self.errors = []

    def _record(self, error):
        self.errors.append(str(error))

    def _resolve_root(self):
        for candidate in TRACING_ROOTS:
            try:
                if (candidate / "events").is_dir():
                    return candidate
            except OSError as error:
                self._record(f"probe {candidate}: {error}")
        target = TRACING_ROOTS[0]
        try:
            target.mkdir(parents=True, exist_ok=True)
            subprocess.run(["mount", "-t", "tracefs", "tracefs", str(target)],
                           check=False, capture_output=True, text=True)
        except OSError as error:
            self._record(f"mount tracefs: {error}")
            return None
        try:
            return target if (target / "events").is_dir() else None
        except OSError as error:
            self._record(f"probe {target}: {error}")
            return None

    def arm(self):
        self.root = self._resolve_root()
        if self.root is None:
            self._record("tracefs unavailable; nvmet trace not armed")
            return
        try:
            write_text(self.root / "tracing_on", "0")
            for name, value in (("trace_clock", "mono"), ("buffer_size_kb", "4096")):
                try:
                    write_text(self.root / name, value)
                except OSError as error:
                    self._record(f"{name}: {error}")
            for event in NVMET_EVENTS:
                enable = self.root / "events" / "nvmet" / event / "enable"
                if enable.is_file():
                    try:
                        write_text(enable, "1")
                    except OSError as error:
                        self._record(f"enable {event}: {error}")
                else:
                    self._record(f"missing nvmet event {event}")
            try:
                with (self.root / "kprobe_events").open("a") as handle:
                    handle.write(KPROBE + "\n")
            except OSError as error:
                self._record(f"kprobe nvmet_tcp_queue_response: {error}")
            write_text(self.root / "tracing_on", "1")
            write_text(self.root / "trace_marker", "native-tier-start")
            self.started = time.monotonic()
        except OSError as error:
            self._record(f"arm: {error}")

    def disarm(self):
        if self.root is None:
            return
        for name, value in (("trace_marker", "native-tier-end"), ("tracing_on", "0")):
            try:
                write_text(self.root / name, value)
            except OSError as error:
                self._record(f"disarm {name}: {error}")

    def cleanup(self):
        if self.root is None:
            return
        for event in NVMET_EVENTS:
            enable = self.root / "events" / "nvmet" / event / "enable"
            if enable.is_file():
                try:
                    write_text(enable, "0")
                except OSError as error:
                    self._record(f"disable {event}: {error}")
        try:
            write_text(self.root / "kprobe_events", "-:nvmet_tcp_qr")
        except OSError as error:
            self._record(f"remove kprobe: {error}")

    @staticmethod
    def _pairs(line):
        return dict(re.findall(r"(\w+)=([^\s,]+)", line))

    def _counts(self, trace):
        completes = {}
        enqueues = {}
        for line in trace.splitlines():
            if "nvmet_req_complete:" in line:
                fields = self._pairs(line.split("nvmet_req_complete:", 1)[1])
                key = (fields.get("ctrl_id"), fields.get("qid"), fields.get("cid"))
                completes[key] = completes.get(key, 0) + 1
            elif "nvmet_tcp_qr:" in line:
                fields = self._pairs(line.split("nvmet_tcp_qr:", 1)[1])
                request = fields.get("req")
                if request is not None:
                    enqueues[request] = enqueues.get(request, 0) + 1
        return completes, enqueues

    def capture_failure(self, directory):
        """Copy the armed trace. Never raises: the proof's own verdict wins."""
        try:
            directory.mkdir(parents=True, exist_ok=True)
            trace = ""
            if self.root is not None:
                try:
                    trace = (self.root / "trace").read_text()
                except OSError as error:
                    self._record(f"read trace: {error}")
            write_text(directory / "trace.txt", trace)
            write_text(directory / "trace-errors.txt", "\n".join(self.errors))
            try:
                udev = Path("/run/udev/queue")
                write_text(directory / "udev-queue.txt",
                           "present" if udev.is_symlink() else "absent")
            except OSError as error:
                write_text(directory / "udev-queue.txt", f"unavailable: {error}")
            if self.started is not None:
                write_text(directory / "tracing-seconds.txt",
                           f"{time.monotonic() - self.started:.3f}")
            if self.root is not None:
                for source, name in (
                    ("kprobe_events", "kprobe_events.txt"),
                    ("events/nvmet/nvmet_req_complete/format", "complete-format.txt"),
                ):
                    try:
                        text = (self.root / source).read_text()
                    except OSError as error:
                        text = f"unavailable: {error}"
                    write_text(directory / name, text)
                try:
                    events = sorted(
                        path.name for path in (self.root / "events").iterdir()
                        if path.name.startswith("nvme"))
                except OSError:
                    events = []
                write_text(directory / "event-list.txt", "\n".join(events))
            completes, enqueues = self._counts(trace)
            complete_counts = {"|".join(str(part) for part in key): count
                               for key, count in completes.items()}
            duplicates = {key: count for key, count in complete_counts.items() if count > 1}
            dump(directory / "complete-counts.json", complete_counts)
            dump(directory / "duplicates.json", duplicates)
            dump(directory / "enqueue-counts.json", enqueues)
        except OSError as error:
            try:
                directory.mkdir(parents=True, exist_ok=True)
                write_text(directory / "trace-errors.txt", f"capture: {error}")
            except OSError:
                pass


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


class ControllerLossSupervisor(threading.Thread):
    """Delete the active write controller each tier's proof asks for.

    The qualification proof writes `controller-loss-ready.json` naming its own
    process, the controller, that controller's sysfs inode and its namespace.
    This thread answers every request once: it re-proves the inode and the
    coordinator's namespace descriptor, writes `delete_controller`, waits for
    the sysfs entry to disappear, and records the injection next to the
    request. A request that does not describe a live controller, or a
    controller that does not disappear, is an error, never a pass.
    """

    def __init__(self, root, deadline, *, poll=0.1):
        super().__init__(name="controller-loss-supervisor", daemon=True)
        self.root = Path(root)
        self.deadline = deadline
        self.poll = poll
        self.stop_event = threading.Event()
        self.records = []
        self.error = None

    def stop(self):
        self.stop_event.set()

    def run(self):
        seen = set()
        try:
            while not self.stop_event.wait(self.poll):
                for request in sorted(self.root.rglob("controller-loss-ready.json")):
                    if request in seen:
                        continue
                    seen.add(request)
                    self.records.append(inject_controller_loss(request))
                if time.monotonic() >= self.deadline:
                    break
        except BaseException as error:  # reported by the caller, never masked
            self.error = error


def inject_controller_loss(request_path, *, controller_root=Path("/sys/class/nvme"),
                           proc_root=Path("/proc"), delete=None, wait_seconds=30.0):
    """Delete one requested controller after proving it serves this write path.

    The roots and the delete action are injectable so a test drives the whole
    decision without a kernel controller. `delete` receives the controller's
    sysfs directory and must remove it, as the kernel does.
    """
    request = json.loads(Path(request_path).read_text())
    target = request.get("target") or {}
    controller = target.get("controller")
    inode = target.get("sysfs_inode")
    namespace = target.get("namespace_path")
    pid = request.get("pid")
    nqn = target.get("nqn")
    if request.get("kind") != "w1_controller_loss_request":
        raise RuntimeError(f"unexpected controller-loss request kind: {request.get('kind')!r}")
    if not (isinstance(controller, str) and re.fullmatch(r"nvme[0-9]+", controller)):
        raise RuntimeError(f"invalid controller name: {controller!r}")
    if target.get("state") != "live":
        raise RuntimeError(f"the requested controller is not live: {target.get('state')!r}")
    if not (isinstance(inode, int) and isinstance(pid, int) and pid > 0
            and isinstance(namespace, str)):
        raise RuntimeError("incomplete controller-loss request identity")
    path = Path(controller_root) / controller
    live_inode = path.stat().st_ino
    if live_inode != inode:
        raise RuntimeError("the requested controller inode differs from the live one")
    descriptor_dir = Path(proc_root) / str(pid) / "fd"
    try:
        descriptors = list(descriptor_dir.iterdir())
    except OSError as error:
        raise RuntimeError(
            'the coordinator holds no descriptor on the requested namespace: '
            f'{error}') from error
    held = []
    for descriptor in descriptors:
        try:
            if os.readlink(descriptor) == namespace:
                held.append(descriptor.name)
        except OSError:
            continue
    if not held:
        raise RuntimeError("the coordinator holds no descriptor on the requested namespace")
    evidence = {"schema": 1, "kind": "w1_controller_loss_injection",
                "requested": True, "injected": False,
                "controller": controller, "sysfs_inode": inode, "nqn": nqn,
                "namespace_path": namespace, "coordinator_pid": pid,
                "coordinator_descriptors": sorted(held),
                "request": str(request_path)}
    started = time.monotonic()
    if delete is None:
        (path / "delete_controller").write_text("1\n")
    else:
        delete(path)
    evidence["delete_issued"] = True
    deadline = started + wait_seconds
    while path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    evidence["removed"] = not path.exists()
    evidence["removed_seconds"] = time.monotonic() - started
    evidence["injected"] = evidence["removed"]
    Path(request_path).with_name("controller-loss-injection.json").write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    if not evidence["removed"]:
        raise RuntimeError(f"controller {controller} did not disappear after delete_controller")
    return evidence


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
    trace = NvmetTrace()
    trace.arm()
    supervisor = ControllerLossSupervisor(output, time.monotonic() + seconds)
    supervisor.start()
    try:
        try:
            proof, evidence = _kernel_live_worker(
                output, args.binary, "tcp", seconds, time.time() + seconds,
                owner_wal_file_bytes=8 * 1024**2, controller_loss=True)
        except BaseException:
            # Capture while the buffer still holds the failing tier's events,
            # then let the proof's own exception carry the verdict.
            trace.disarm()
            trace.capture_failure(root / "failure-trace")
            raise
        finally:
            supervisor.stop()
            supervisor.join(timeout=10)
        trace.disarm()
        if supervisor.error is not None:
            raise RuntimeError(f"controller-loss supervision failed: {supervisor.error!r}")
        if not supervisor.records or not all(row["injected"] for row in supervisor.records):
            raise RuntimeError("controller-loss injection was not confirmed for every tier")
        qualifications = sorted(output.rglob("qualification.json"))
        if len(qualifications) != 2:
            raise RuntimeError(f"expected two tier proofs, found {len(qualifications)}")
        for proof_path in qualifications:
            tier_proof = json.loads(proof_path.read_text())
            loss = tier_proof.get("controller_loss") or {}
            if (tier_proof.get("checks", {}).get("w1_controller_loss") is not True
                    or loss.get("resumed", {}).get("sysfs_inode") in (None, loss.get("deleted", {}).get("sysfs_inode"))):
                raise RuntimeError(f"controller-loss resume unproven in {proof_path}")
        after = inventory()
        dump(output / "after.json", after)
        result = {"complete": proof["complete"], "fixture_removed": proof["fixture_removed"],
                  "inventory_restored": before == after, "evidence": str(evidence.relative_to(root)),
                  "controller_loss": {"injections": len(supervisor.records),
                                      "tiers": len(qualifications)},
                  "elapsed_seconds": round(time.monotonic() - started, 3),
                  "nvmet_trace": "armed" if trace.root is not None else "unavailable"}
        dump(root / "summary.json", {"wal_replay": result})
        print(f"wal_replay: {json.dumps(result, sort_keys=True)}", flush=True)
        failed = (not result["complete"] or not result["fixture_removed"]
                  or not result["inventory_restored"])
        if failed:
            trace.capture_failure(root / "failure-trace")
        if not result["fixture_removed"] or not result["inventory_restored"]:
            raise RuntimeError("Single-owner kernel TCP cleanup unconfirmed")
        return 0 if result["complete"] and signal.getsignal(signal.SIGTERM) != signal.SIG_IGN else 1
    finally:
        # Never leave the nvmet tracepoints or the kprobe armed for a later
        # proof on the same runner, including a failed run.
        trace.cleanup()


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
