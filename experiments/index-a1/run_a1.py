#!/usr/bin/env python3
"""Roadmap Phase 6 A1 local calibration controller (INDEX-A0-A1.md, "A1").

Runs only inside a Main-granted quiet window; never in parallel with source
builds or other cases. Every case uses a fresh prepared database:
build (N scalar puts, verified, explicit root, clean close, receipt) ->
overwrite (receipt-bound reopen, timed load, crash-style coordinator stop) ->
cold reopen (fresh process, eviction + residency check, timed open to the
first correct read, exact final rows). Runs are serial.

Commands (all take --workspace, the repository checkout root):
  plan      print the ordered case list and the host budget check; runs nothing
  emit      write the exact generated configurations of every case and stage
  pilot     per tier: E4 closed-loop pilots at 1M/5M/10M, rate selection
            floor(0.70 x min median), open-loop confirmation (0.8x retry once)
  preflight bounded C-scaled cadence preflight at 1M/5M per tier
  main      five interleaved blocks over the eligible cells
  analyze   decision (analyze_a1.py)

Resource ceilings per case [INFERENCE, design step 4]: owner 80 GiB,
coordinator 16 GiB, output 4 GiB, >= 25 GiB host free, 20 GiB memory
(private systemd user scope when available, else RSS + MemAvailable >= 6 GiB),
16 CPUs pinned identically for every arm. A breach kills the stage's process
group and keeps the evidence; the case is failed, never rerun as warm.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import a1_cases  # noqa: E402
import a1_metrics  # noqa: E402

GIB = 1024**3
FREE_RESERVE = 25 * GIB
OWNER_CEILING = 80 * GIB
COORDINATOR_CEILING = 16 * GIB
OUTPUT_CEILING = 4 * GIB
MEMORY_CEILING = 20 * GIB
MEM_AVAILABLE_FLOOR = 6 * GIB
CPU_LIMIT = 16
BUILD_PROCESSES = {"cargo", "rustc", "cc1", "cc1plus", "ld", "ld.lld", "mold"}
BUILD_WAIT_S = 600
PILOT_MIN_BUCKETS = 5
PILOT_MIN_OCCUPANCY = 8.0  # half of c16 [INFERENCE: pilot validity criterion]
RATE_FRACTION = 0.70
RETRY_FRACTION = 0.8


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_json(path: Path, value) -> None:
    pending = path.with_suffix(path.suffix + ".pending")
    pending.write_text(json.dumps(value, indent=2, default=str) + "\n")
    os.replace(pending, path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_usage(root: Path) -> tuple[int, int]:
    """(file length, st_blocks x 512) of every regular file below root."""
    length = allocated = 0
    if not root.exists():
        return 0, 0
    for directory, _, files in os.walk(root):
        for name in files:
            try:
                info = os.lstat(os.path.join(directory, name))
            except FileNotFoundError:
                continue
            length += info.st_size
            allocated += info.st_blocks * 512
    return length, allocated


def free_bytes(path: Path) -> int:
    info = os.statvfs(path)
    return info.f_bavail * info.f_frsize


def mem_available() -> int:
    with open("/proc/meminfo") as handle:
        for line in handle:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable missing")


def process_tree_rss(root_pid: int) -> int:
    """Sum of VmRSS over root_pid and its descendants (one /proc scan)."""
    children: dict[int, list[int]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as handle:
                stat = handle.read()
        except OSError:
            continue
        parent = int(stat.rsplit(")", 1)[1].split()[1])
        children.setdefault(parent, []).append(int(entry))
    total = 0
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        stack.extend(children.get(pid, []))
        try:
            with open(f"/proc/{pid}/status") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        total += int(line.split()[1]) * 1024
        except OSError:
            continue
    return total


def build_processes() -> list[str]:
    found = []
    for entry in os.listdir("/proc"):
        if entry.isdigit():
            try:
                with open(f"/proc/{entry}/comm") as handle:
                    name = handle.read().strip()
            except OSError:
                continue
            if name in BUILD_PROCESSES:
                found.append(f"{entry}:{name}")
    return found


def wait_for_quiet_host() -> None:
    deadline = time.monotonic() + BUILD_WAIT_S
    while True:
        found = build_processes()
        if not found:
            return
        if time.monotonic() >= deadline:
            raise SystemExit(f"source build processes still running {found}; A1 needs the quiet window")
        time.sleep(10)


def pinned_cpus() -> list[int]:
    return sorted(os.sched_getaffinity(0))[:CPU_LIMIT]


def memory_scope_available() -> bool:
    if shutil.which("systemd-run") is None:
        return False
    probe = subprocess.run(
        ["systemd-run", "--user", "--scope", "--quiet", "-p", f"MemoryMax={MEMORY_CEILING}", "true"],
        capture_output=True,
        check=False,
    )
    return probe.returncode == 0


class Stage:
    """One kv-research process with 1 s resource monitoring and ceilings."""

    def __init__(self, controller: "Controller", case_dir: Path, name: str, argv: list[str], data: Path):
        self.controller = controller
        self.case_dir = case_dir
        self.name = name
        self.argv = argv
        self.data = data
        self.breach: str | None = None
        self.peaks = {"owner_allocated": 0, "coordinator_allocated": 0, "output_allocated": 0, "rss": 0, "min_free": None, "min_mem_available": None}

    def _sample(self, pid: int, output: Path, log) -> None:
        owner = tree_usage(self.data / "fixture")
        coordinator = tree_usage(self.data / "coordinator")
        output_usage = tree_usage(output)
        free = free_bytes(self.controller.workspace)
        available = mem_available()
        rss = process_tree_rss(pid)
        row = {"t": time.time(), "owner": owner, "coordinator": coordinator, "output": output_usage, "free": free, "mem_available": available, "rss": rss}
        log.write(json.dumps(row) + "\n")
        peaks = self.peaks
        peaks["owner_allocated"] = max(peaks["owner_allocated"], owner[1])
        peaks["coordinator_allocated"] = max(peaks["coordinator_allocated"], coordinator[1])
        peaks["output_allocated"] = max(peaks["output_allocated"], output_usage[1])
        peaks["rss"] = max(peaks["rss"], rss)
        peaks["min_free"] = free if peaks["min_free"] is None else min(peaks["min_free"], free)
        peaks["min_mem_available"] = available if peaks["min_mem_available"] is None else min(peaks["min_mem_available"], available)
        for value, ceiling, label in (
            (owner[1], OWNER_CEILING, "owner allocation above 80 GiB"),
            (coordinator[1], COORDINATOR_CEILING, "coordinator allocation above 16 GiB"),
            (output_usage[1], OUTPUT_CEILING, "output allocation above 4 GiB"),
            (owner[1] + coordinator[1] + output_usage[1], 100 * GIB, "case allocation above 100 GiB"),
            (rss, MEMORY_CEILING, "process-tree RSS above 20 GiB"),
        ):
            if value > ceiling and self.breach is None:
                self.breach = label
        if free < FREE_RESERVE and self.breach is None:
            self.breach = "host free space below 25 GiB"
        if available < MEM_AVAILABLE_FLOOR and self.breach is None:
            self.breach = "MemAvailable below 6 GiB"

    def run(self, output: Path) -> dict:
        command = list(self.argv)
        if self.controller.memory_scope:
            command = ["systemd-run", "--user", "--scope", "--quiet", "-p", f"MemoryMax={MEMORY_CEILING}", "-p", "MemorySwapMax=0", "--", *command]
        cpus = self.controller.cpus
        stdout = (self.case_dir / f"{self.name}.stdout.log").open("wb")
        stderr = (self.case_dir / f"{self.name}.stderr.log").open("wb")
        started = time.time()
        process = subprocess.Popen(
            command,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            preexec_fn=lambda: os.sched_setaffinity(0, cpus),
        )
        stop = threading.Event()

        def monitor() -> None:
            with (self.case_dir / f"{self.name}.resources.jsonl").open("w") as log:
                while not stop.is_set():
                    self._sample(process.pid, output, log)
                    if self.breach is not None:
                        os.killpg(process.pid, signal.SIGKILL)
                        return
                    stop.wait(1.0)

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        code = process.wait()
        stop.set()
        thread.join()
        stdout.close()
        stderr.close()
        return {
            "stage": self.name,
            "argv": command,
            "exit_code": code,
            "breach": self.breach,
            "peaks": self.peaks,
            "elapsed_s": time.time() - started,
            "stderr_tail": (self.case_dir / f"{self.name}.stderr.log").read_text(errors="replace")[-4000:],
        }


class Controller:
    def __init__(self, arguments):
        self.workspace = arguments.workspace.resolve()
        self.results = (arguments.results or self.workspace / "scratch/IndexA1/results").resolve()
        self.binary = arguments.binary.resolve() if getattr(arguments, "binary", None) else None
        self.tree = getattr(arguments, "tree", None)
        self.cpus = pinned_cpus()
        self.memory_scope = False
        self.state_path = self.results / "state.json"

    def prepare(self) -> None:
        if self.binary is None or not self.binary.is_file():
            raise SystemExit("--binary must name the Linux kv-research build (feature test-utils)")
        self.results.mkdir(parents=True, exist_ok=True)
        self.memory_scope = memory_scope_available()
        manifest = self.results / "run-manifest.json"
        binary_sha256 = sha256(self.binary)
        if manifest.exists():
            recorded = json.loads(manifest.read_text())
            if recorded.get("tree") != self.tree or recorded.get("binary_sha256") != binary_sha256:
                raise SystemExit("results directory belongs to another tree or binary; use a fresh --results")
            return
        write_json(manifest, {
            "schema_version": 1,
            "created": now(),
            # Binds the A0 V6 pass receipt to this run (analyze_a1.py).
            "tree": self.tree,
            "binary": str(self.binary),
            "binary_sha256": binary_sha256,
            "cpus": self.cpus,
            "memory_scope": self.memory_scope,
            "host": host_resources(self.workspace),
            "generator": {"a1_cases.py": sha256(Path(a1_cases.__file__))},
        })

    def state(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text())
        return {"cases": {}, "rates": {}, "c_scaled_ready": []}

    def save(self, state: dict) -> None:
        write_json(self.state_path, state)

    def stage(self, case: str, name: str, config: dict | None, extra: list[str] | None = None) -> dict:
        """Write the exact config and run one stage; returns its record."""
        case_dir = self.results / case
        case_dir.mkdir(parents=True, exist_ok=True)
        data = Path(a1_cases.case_path(str(self.workspace), case))
        output = case_dir / f"{name}-result"
        if config is not None:
            config_path = case_dir / f"{name}.json"
            write_json(config_path, config)
            argv = [str(self.binary), "--config", str(config_path), "--output", str(output)]
        else:
            argv = [str(self.binary), *(extra or []), "--output", str(output)]
        wait_for_quiet_host()
        need = stage_need(name, config, data)
        free = free_bytes(self.workspace)
        if free - FREE_RESERVE < need:
            raise SystemExit(f"{case} {name}: host free {free / GIB:.1f} GiB leaves less than the stage worst case {need / GIB:.1f} GiB above the 25 GiB reserve")
        record = Stage(self, case_dir, name, argv, data).run(output)
        write_json(case_dir / f"{name}.record.json", record)
        compress_samples(output)
        return record

    def finish_case(self, case: str, identity: dict, records: list[dict]) -> dict:
        case_dir = self.results / case
        data = Path(a1_cases.case_path(str(self.workspace), case))
        resources = {"breach": any(r["breach"] for r in records), "stages": records}
        metrics = None
        overwrite = case_dir / "overwrite-result"
        cold = case_dir / "cold-result"
        if (overwrite / "index-a1-stage.json").exists() and (overwrite / "summary.json").exists():
            try:
                metrics = a1_metrics.case_metrics(overwrite, cold if (cold / "index-a1-cold.json").exists() else None, resources)
            except (OSError, KeyError, ValueError) as error:
                metrics = {"valid": False, "invalid_reasons": [f"metrics extraction failed: {error}"]}
        if metrics is None:
            metrics = {"valid": False, "invalid_reasons": ["stage failed: " + "; ".join(f"{r['stage']} exit {r['exit_code']} {r['breach'] or ''}" for r in records)], "resources": resources}
        metrics["identity"] = identity
        write_json(case_dir / "case-metrics.json", metrics)
        archive_case(data, case_dir)
        return metrics

    def run_case(self, size: int, arm: str, tier: str, repeat, overwrite_ops: int | None, rate: int | None, cold: bool = True) -> dict:
        case = a1_cases.case_name(size, arm, tier, repeat)
        identity = {"case": case, "size": size, "arm": arm, "tier": tier, "repeat": repeat, "rate": rate, "overwrite_operations": overwrite_ops}
        case_dir = self.results / case
        if case_dir.exists():
            # Resume: a finished case keeps its evidence; an interrupted one
            # stays failed and its label is never reused for a new run.
            existing = case_dir / "case-metrics.json"
            if existing.exists():
                return json.loads(existing.read_text())
            return {"valid": False, "invalid_reasons": ["interrupted earlier; label not reused"], "identity": identity}
        records = []
        build = self.stage(case, "build", a1_cases.config(str(self.workspace), case, size, arm, tier, "build"))
        records.append(build)
        if build["exit_code"] == 0 and not build["breach"]:
            config = a1_cases.config(str(self.workspace), case, size, arm, tier, "overwrite", operations=overwrite_ops, rate=rate)
            overwrite = self.stage(case, "overwrite", config)
            records.append(overwrite)
            if cold and overwrite["exit_code"] == 0 and not overwrite["breach"]:
                records.append(self.stage(case, "cold", None, ["--index-a1-cold-reopen", str(self.results / case / "overwrite.json")]))
        return self.finish_case(case, identity, records)


def stage_need(name: str, config: dict | None, data: Path) -> int:
    """Worst-case additional allocation of one stage (4 KiB per put before
    grouping) plus the coordinator ceiling not yet used and the output ceiling."""
    coordinator = tree_usage(data / "coordinator")[1]
    remaining_coordinator = max(0, COORDINATOR_CEILING - coordinator)
    if name == "cold" or config is None:
        return remaining_coordinator + OUTPUT_CEILING
    puts = config["phases"]["measurement_operations"]
    return puts * 4096 + remaining_coordinator + OUTPUT_CEILING


def compress_samples(output: Path) -> None:
    raw = output / "samples.csv"
    if not raw.exists():
        return
    with raw.open("rb") as source, gzip.open(output / "samples.csv.gz", "wb", compresslevel=6) as target:
        shutil.copyfileobj(source, target, 1 << 20)
    raw.unlink()


def archive_case(data: Path, case_dir: Path) -> None:
    """Keep receipts, stop record, touched bitmap and owner evidence; then
    remove only this validated case's data root."""
    keep = case_dir / "fixture-evidence"
    keep.mkdir(exist_ok=True)
    for name in ("fixture-receipt.json", "overwrite-stop.json", "touched.bin"):
        if (data / name).exists():
            shutil.copy2(data / name, keep / name)
    if (data / "evidence").exists():
        shutil.copytree(data / "evidence", keep / "evidence", dirs_exist_ok=True)
    case_root = data.parent
    if case_root.exists() and case_root.parent.name == "cases":
        shutil.rmtree(case_root)


def host_resources(workspace: Path) -> dict:
    with open("/proc/meminfo") as handle:
        meminfo = {line.split(":")[0]: int(line.split()[1]) * 1024 for line in handle if line.split(":")[0] in ("MemTotal", "MemAvailable")}
    mount = subprocess.run(["findmnt", "-T", str(workspace), "-o", "SOURCE,MAJ:MIN,FSTYPE,OPTIONS", "-n"], capture_output=True, text=True, check=False).stdout.strip()
    return {"logical_cpus": os.cpu_count(), "affinity": sorted(os.sched_getaffinity(0)), "meminfo": meminfo, "free_bytes": free_bytes(workspace), "mount": mount}


def plan(arguments) -> int:
    workspace = arguments.workspace.resolve()
    host = host_resources(workspace)
    ready = frozenset(tuple(item) for item in arguments.c_scaled_ready or [])
    cells = a1_cases.local_cells(ready)
    order = [(repeat, *cell) for repeat in range(a1_cases.REPEATS) for cell in a1_cases.block_order(repeat, cells)]
    budget = {str(size): a1_cases.case_budget(size) for size in a1_cases.SIZES}
    worst = max(item["owner_worst_bytes"] for item in budget.values()) + COORDINATOR_CEILING + OUTPUT_CEILING
    report = {
        "host": host,
        "budget_per_case": budget,
        "worst_case_stage_sum_bytes": worst,
        "worst_case_fits_now": host["free_bytes"] - FREE_RESERVE >= worst,
        "per_stage_gate": "each stage starts only if free - 25 GiB >= its own worst case (puts x 4 KiB + unused coordinator ceiling + 4 GiB output)",
        "memory": {"ceiling": MEMORY_CEILING, "mem_total": host["meminfo"].get("MemTotal"), "fits": host["meminfo"].get("MemTotal", 0) >= MEMORY_CEILING + MEM_AVAILABLE_FLOOR},
        "cpus": pinned_cpus(),
        "pilot_cases": [a1_cases.case_name(size, "e4", tier, "pilot") for tier in a1_cases.TIERS for size in a1_cases.SIZES],
        "confirm_cases": [a1_cases.case_name(size, "e4", tier, "confirm") for tier in a1_cases.TIERS for size in a1_cases.SIZES],
        "c_scaled_preflight_cases": [a1_cases.case_name(size, "c-scaled", tier, "preflight") for tier in a1_cases.TIERS for size in a1_cases.C_SCALED_SIZES],
        "main_cases": [a1_cases.case_name(size, arm, tier, repeat) for repeat, size, tier, arm in order],
    }
    print(json.dumps(report, indent=2, default=str))
    return 0


def emit(arguments) -> int:
    workspace = str(arguments.workspace.resolve())
    out = arguments.out
    out.mkdir(parents=True, exist_ok=True)
    count = 0
    for size in a1_cases.SIZES:
        for tier in a1_cases.TIERS:
            for arm in a1_cases.ARM_ORDER:
                if arm == "c-scaled" and size not in a1_cases.C_SCALED_SIZES:
                    continue
                for repeat in range(a1_cases.REPEATS):
                    case = a1_cases.case_name(size, arm, tier, repeat)
                    write_json(out / f"{case}-build.json", a1_cases.config(workspace, case, size, arm, tier, "build"))
                    overwrite = a1_cases.config(workspace, case, size, arm, tier, "overwrite", rate=arguments.rate)
                    write_json(out / f"{case}-overwrite.json", overwrite)
                    count += 2
    print(f"wrote {count} configurations to {out} (overwrite rate {arguments.rate})")
    return 0


def pilot(arguments) -> int:
    controller = Controller(arguments)
    controller.prepare()
    state = controller.state()
    for tier in a1_cases.TIERS:
        if tier in state["rates"]:
            continue
        medians = {}
        notes = []
        for size in a1_cases.SIZES:
            metrics = controller.run_case(size, "e4", tier, "pilot", a1_cases.PILOT_OPERATIONS, None, cold=False)
            samples = metrics.get("samples") or {}
            if (samples.get("complete_buckets_after_skip") or 0) < PILOT_MIN_BUCKETS:
                metrics = controller.run_case(size, "e4", tier, "pilot2", a1_cases.PILOT_EXTENDED_OPERATIONS, None, cold=False)
                samples = metrics.get("samples") or {}
            occupancy = samples.get("mean_in_flight")
            if (samples.get("complete_buckets_after_skip") or 0) < PILOT_MIN_BUCKETS or samples.get("median_puts_per_s") is None:
                notes.append(f"{size}: fewer than {PILOT_MIN_BUCKETS} complete buckets after 5 s")
            elif occupancy is None or occupancy < PILOT_MIN_OCCUPANCY:
                notes.append(f"{size}: closed-loop mean API occupancy {occupancy} below half of c16")
            medians[size] = samples.get("median_puts_per_s")
        record = {"pilot_medians": medians, "notes": notes, "eligible": False}
        if notes or any(value is None for value in medians.values()):
            state["rates"][tier] = record
            controller.save(state)
            continue
        rate = math.floor(RATE_FRACTION * min(medians.values()))
        for attempt, label in ((rate, "confirm"), (math.floor(RETRY_FRACTION * rate), "confirm-r08")):
            confirms = {}
            for size in a1_cases.SIZES:
                metrics = controller.run_case(size, "e4", tier, label, a1_cases.CONFIRM_OPERATIONS, attempt, cold=False)
                samples = metrics.get("samples") or {}
                debt = metrics.get("debt") or {}
                stable = (
                    samples.get("failed_or_unknown", 1) == 0
                    and debt.get("available")
                    and debt["bytes"] <= 0
                    and debt["batches"] <= 0
                )
                confirms[size] = {"stable": bool(stable), "failed_or_unknown": samples.get("failed_or_unknown"), "debt": debt, "achieved": samples.get("achieved_success_per_s")}
            record.setdefault("confirmations", {})[label] = {"rate": attempt, "sizes": confirms}
            if all(item["stable"] for item in confirms.values()):
                record.update({"rate": attempt, "eligible": True})
                break
        state["rates"][tier] = record
        controller.save(state)
    print(json.dumps(state["rates"], indent=2, default=str))
    return 0


def preflight(arguments) -> int:
    """Bounded C-scaled cadence preflight: build, then at most 1M closed-loop
    overwrites. Ready only when the test-branch engine reports a natural
    cadence adoption (generation trigger `per_cell_cadence`); on this base the
    PerCell arm is rejected at open and recorded unavailable."""
    controller = Controller(arguments)
    controller.prepare()
    state = controller.state()
    ready = set(map(tuple, state["c_scaled_ready"]))
    outcomes = state.setdefault("c_scaled_preflight", {})
    for tier in a1_cases.TIERS:
        for size in a1_cases.C_SCALED_SIZES:
            case = a1_cases.case_name(size, "c-scaled", tier, "preflight")
            if case in outcomes:
                continue
            metrics = controller.run_case(size, "c-scaled", tier, "preflight", a1_cases.CONFIRM_OPERATIONS, None, cold=False)
            build = json.loads((controller.results / case / "build.record.json").read_text())
            if build["exit_code"] != 0:
                unavailable = "per_cell is unavailable" in build["stderr_tail"]
                outcomes[case] = {"ready": False, "reason": "PerCell unavailable in this revision" if unavailable else "build failed", "stderr_tail": build["stderr_tail"][-800:]}
                continue
            diagnostics_path = controller.results / case / "overwrite-result/write-diagnostics.json"
            generations = []
            if diagnostics_path.exists():
                generations = a1_metrics.read_json(diagnostics_path).get("index_checkpoint", {}).get("generations", [])
            cadence = [row for row in generations if "per_cell_cadence" in (row.get("trigger_flags") or [])]
            outcomes[case] = {"ready": bool(cadence), "cadence_adoptions": len(cadence), "limiting_counters": metrics.get("timed"), "debt": metrics.get("debt")}
            if cadence:
                ready.add((size, tier))
    state["c_scaled_ready"] = sorted(ready)
    controller.save(state)
    print(json.dumps(outcomes, indent=2, default=str))
    return 0


def main_blocks(arguments) -> int:
    controller = Controller(arguments)
    controller.prepare()
    state = controller.state()
    cells = a1_cases.local_cells(frozenset(map(tuple, state["c_scaled_ready"])))
    for repeat in range(a1_cases.REPEATS):
        for size, tier, arm in a1_cases.block_order(repeat, cells):
            case = a1_cases.case_name(size, arm, tier, repeat)
            if case in state["cases"]:
                continue
            tier_rate = state["rates"].get(tier, {})
            if not tier_rate.get("eligible"):
                state["cases"][case] = {"skipped": "tier ineligible: pilot or confirmation failed"}
                controller.save(state)
                continue
            metrics = controller.run_case(size, arm, tier, repeat, a1_cases.TIMED_OPERATIONS, tier_rate["rate"])
            state["cases"][case] = {"valid": metrics.get("valid"), "invalid_reasons": metrics.get("invalid_reasons"), "finished": now()}
            controller.save(state)
    return 0


def analyze(arguments) -> int:
    import analyze_a1

    results = (arguments.results or arguments.workspace / "scratch/IndexA1/results").resolve()
    report = analyze_a1.analyze(results, arguments.v6_receipt)
    write_json(results / "decision.json", report)
    print(json.dumps(report["decision"], indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="A1 local calibration controller")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "emit", "pilot", "preflight", "main", "analyze"):
        command = sub.add_parser(name)
        command.add_argument("--workspace", type=Path, required=True)
        command.add_argument("--results", type=Path)
        if name in ("pilot", "preflight", "main"):
            command.add_argument("--binary", type=Path, required=True)
            command.add_argument("--tree", required=True, help="source tree identity (base + patch SHA-256s); must match the V6 receipt")
        if name == "analyze":
            command.add_argument("--v6-receipt", type=Path, help="A0 V6 pass receipt; without it the verdict is unqualified")
        if name == "emit":
            command.add_argument("--out", type=Path, required=True)
            command.add_argument("--rate", type=int, required=True)
        if name == "plan":
            command.add_argument("--c-scaled-ready", type=json.loads, default=[])
    arguments = parser.parse_args()
    return {"plan": plan, "emit": emit, "pilot": pilot, "preflight": preflight, "main": main_blocks, "analyze": analyze}[arguments.command](arguments)


if __name__ == "__main__":
    raise SystemExit(main())
