"""Shared bounded Sui checkpoint RPC, configuration and offline audit helpers.

Extracted from the worktree preserved at
project:archive/middleware-legacy/experiments/sui, HEAD 1969983 plus its uncommitted
changes. Original files: run_local_checkpoint.py, checkpoint_experiment.py,
audit_checkpoint.py and analyze_checkpoint.py.
Only their checkpoint support dependency closure is retained here.
"""

from concurrent.futures import ThreadPoolExecutor
import hashlib
from datetime import datetime
import json
import os
from pathlib import Path
import math
import re
import time
import urllib.request

import yaml


class CheckpointBound(int):
    """Sui's pinned serde_yaml 0.8 externally tagged checkpoint bound."""


class ExperimentDumper(yaml.SafeDumper):
    pass


ExperimentDumper.add_representer(
    CheckpointBound,
    lambda dumper, value: dumper.represent_dict({"Checkpoint": int(value)}),
)


def completed_checkpoint(metrics):
    values = [int(float(line.split()[1])) for line in metrics.splitlines()
              if line.startswith("last_executed_checkpoint ")]
    if len(values) != 1 or values[0] < 0:
        raise ValueError("one completed checkpoint gauge is required")
    return values[0]


RPC_INDEX_PIPELINES = frozenset(("object_by_owner", "object_by_type", "balance", "epochs",
    "object_version_by_checkpoint", "package_versions", "tx_seq_by_digest", "tx_metadata_by_seq",
    "transaction_bitmap", "event_bitmap"))


def pipeline_series(metrics, family):
    """One non-negative integer per RPC pipeline for a `family{pipeline=...}` series."""
    values = {}
    for line in metrics.splitlines():
        if not line.startswith(family + "{"):
            continue
        metric, value = line.rsplit(" ", 1)
        name = re.search(r'(?:\{|,)pipeline="([^"]+)"(?:,|\})', metric)
        number = float(value)
        if name is None or name[1] not in RPC_INDEX_PIPELINES or name[1] in values or not number.is_integer() or number < 0:
            raise ValueError(f"invalid or duplicate {family} pipeline")
        values[name[1]] = int(number)
    return values


def rpc_index_frontiers(metrics, checkpoint):
    frontiers = pipeline_series(metrics, "rpc_store_indexer_watermark_checkpoint_in_db")
    if any(value > checkpoint for value in frontiers.values()):
        raise ValueError("out-of-range committed RPC frontier")
    return frontiers


def rpc_committer_attempts(metrics):
    """Batches each committer attempted in this process; absent means none were attempted.

    The committer counts an attempt before it writes, and it is the only writer
    of a pipeline's watermark, so a zero total is this process committing
    nothing at all. This counter is the observable for "did the reopened node
    commit", because the frontier family's presence carries no such meaning (see
    `reopened_index_state`).
    """
    return pipeline_series(metrics, "rpc_store_indexer_total_committer_batches_attempted")


RPC_INDEX_RESUME = re.compile(r"Ingestion range cohort=(\d+) start=(\d+) pipelines=\[(.*)\]")
RPC_INDEX_BOOTSTRAP = re.compile(
    r"bootstrapping embedded rpc-store action=(\w+) lowest_available=(\d+) "
    r"highest_committed=(None|Some\((\d+)\))")


def rpc_index_bootstrap(log):
    """The rpc-store bootstrap action the node logged, or None before it states one."""
    stated = None
    for line in log.splitlines():
        match = RPC_INDEX_BOOTSTRAP.search(line)
        if match is None:
            continue
        if stated is not None:
            raise ValueError("the node stated its rpc-store bootstrap twice")
        action, lowest, _, number = match.groups()
        if action not in ("Resume", "SeedHistory", "Restore"):
            raise ValueError("unknown rpc-store bootstrap action")
        stated = {"action": action, "lowest_available": int(lowest),
                  "highest_committed": None if number is None else int(number)}
    return stated


def rpc_index_resume(log, checkpoint):
    """Where the framework resumes its ten pipelines, or None before the node states it.

    `Indexer::run` reads every pipeline's persisted watermark and logs one
    `Ingestion range` per cohort with the smallest `watermark + 1` it will
    ingest from. A clean reopen at the frozen tip therefore states
    `start = checkpoint + 1` for all ten pipelines while committing nothing.
    """
    cohorts, names = [], []
    for line in log.splitlines():
        match = RPC_INDEX_RESUME.search(line)
        if match is None:
            continue
        cohort, start, listed = int(match[1]), int(match[2]), [
            name.strip().strip('"') for name in match[3].split(",") if name.strip()]
        if not listed or any(name not in RPC_INDEX_PIPELINES for name in listed):
            raise ValueError("the node resumed an unknown index pipeline")
        cohorts.append({"cohort": cohort, "start": start, "pipelines": listed})
        names.extend(listed)
    if not cohorts:
        return None
    if len({row["cohort"] for row in cohorts}) != len(cohorts) or sorted(names) != sorted(RPC_INDEX_PIPELINES):
        raise ValueError("the stated ingestion cohorts do not cover each index pipeline once")
    start = min(row["start"] for row in cohorts)
    if start != checkpoint + 1:
        raise ValueError(f"the node resumed from checkpoint {start - 1}, not the frozen tip {checkpoint}")
    return {"start": start, "checkpoint": checkpoint, "cohorts": cohorts,
            "scope": "the persisted per-pipeline watermark the framework read at startup"}


def reopened_index_state(metrics, log, checkpoint):
    """How a reopened node's ten indexers stand against the frozen tip.

    A clean reopen resumes every pipeline from its persisted watermark, so the
    framework schedules from `checkpoint + 1` and ingests nothing. Whether this
    process committed is read from `rpc_store_indexer_total_committer_batches_attempted`:
    the committer increments it before every batch write and it is the only
    writer of a pipeline's committed watermark.

    The committed-frontier gauge cannot stand in for the counter. Each committer
    binds one `rpc_store_indexer_watermark_checkpoint_in_db{pipeline=...}` child
    at construction, through `CheckpointLagMetricReporter::new_for_pipeline`
    (`pipeline/{concurrent/commit_watermark,sequential/committer}.rs`), and
    Prometheus publishes every bound child at its zero value. A reopened node
    therefore publishes the whole family at 0 without committing anything; only
    a non-zero value is a commit this process made. Returns None while the node
    has not yet stated its resume frontier.
    """
    frontiers = rpc_index_frontiers(metrics, checkpoint)
    attempts = rpc_committer_attempts(metrics)
    if sum(attempts.values()):
        missing = sorted(RPC_INDEX_PIPELINES - set(frontiers))
        behind = sorted(f"{name}@{value}" for name, value in frontiers.items()
                        if value != checkpoint)
        if missing or behind:
            raise RuntimeError(
                f"the reopened node attempted batches without committing all ten "
                f"pipelines at the frozen tip {checkpoint}: "
                + ", ".join(missing + behind))
        return {"status": "all-ten-committed-at-frozen-tip", "checkpoint": checkpoint,
                "frontiers": frontiers, "committer_batches": attempts,
                "scope": "the reopened node re-executed checkpoints"}
    # This process attempted no batch, so any bound child still reads zero. A
    # non-zero frontier under a zero attempt total is a watermark this process
    # cannot have written; treat it as an inconsistency rather than a commit.
    published = {name: value for name, value in frontiers.items() if value}
    if published:
        raise RuntimeError(
            "the reopened node published a committed frontier without attempting a batch: "
            + ", ".join(sorted(f"{name}@{value}" for name, value in published.items())))
    resume = rpc_index_resume(log, checkpoint)
    if resume is None:
        return None
    bootstrap = rpc_index_bootstrap(log)
    if bootstrap is None:
        raise RuntimeError("the reopened node did not state its rpc-store bootstrap action")
    if bootstrap["action"] != "Resume":
        raise RuntimeError(f"the reopened rpc-store bootstrapped with {bootstrap['action']} "
                           "instead of resuming the persisted indexes")
    if bootstrap["highest_committed"] != checkpoint:
        raise RuntimeError("the reopened rpc-store restored to checkpoint "
                           f"{bootstrap['highest_committed']}, not the frozen tip {checkpoint}")
    return {"status": "persisted-watermark-at-frozen-tip", "checkpoint": checkpoint,
            "frontiers": frontiers, "committer_batches": attempts, "resume": resume,
            "bootstrap": bootstrap,
            "scope": "the persisted watermark each pipeline resumes from; the committer "
                     "attempted no batch and left every bound frontier child at zero"}


def wait_for_reopened_indexes(child, address, log, checkpoint, metrics):
    """Wait until the reopened node states where its indexers resume.

    Unlike the replay wait, a clean reopen commits nothing: the committed-frontier
    gauge is bound at zero by each committer and the attempt counter stays at
    zero, so the observable is the framework's resume frontier read back from
    the persisted watermarks.
    """
    start = time.monotonic()
    observations = []
    for _ in range(480):
        state = reopened_index_state(metrics, log.read_text() if log.exists() else "", checkpoint)
        observations.append({"seconds": time.monotonic() - start,
                             "status": None if state is None else state["status"]})
        if state is not None:
            state.update({"seconds": time.monotonic() - start, "observations": observations,
                          "note": "read before the tail read-back; excluded from the foreground replay"})
            return metrics, state
        if child.poll() is not None or time.monotonic() - start >= 120:
            raise RuntimeError("the reopened node did not state its indexer resume frontier at the frozen tip")
        time.sleep(0.25)
        with urllib.request.urlopen(f"http://{address}/metrics", timeout=1) as response:
            metrics = response.read(32 << 20).decode()
    raise RuntimeError("reopened indexer wait exceeded fixed sample budget")


def wait_for_rpc_indexes(child, address, checkpoint, metrics):
    start = time.monotonic()
    observations = []
    for _ in range(480):
        frontiers = rpc_index_frontiers(metrics, checkpoint)
        observations.append({"seconds": time.monotonic() - start, "frontiers": frontiers})
        if set(frontiers) == RPC_INDEX_PIPELINES and all(value == checkpoint for value in frontiers.values()):
            return metrics, {"status": "all-ten-committed-at-frozen-tip", "seconds": time.monotonic() - start,
                             "checkpoint": checkpoint, "observations": observations,
                             "scope": "after foreground replay timer; excluded from replay elapsed"}
        if child.poll() is not None or time.monotonic() - start >= 120:
            raise RuntimeError("RPC indexers did not commit all ten pipelines at frozen tip")
        time.sleep(0.25)
        with urllib.request.urlopen(f"http://{address}/metrics", timeout=1) as response:
            metrics = response.read(32 << 20).decode()
    raise RuntimeError("RPC indexer wait exceeded fixed sample budget")


def completion_window(progress):
    advanced = []
    for sample in progress:
        if advanced and sample["completed_checkpoint"] < advanced[-1]["completed_checkpoint"]:
            raise ValueError("completed checkpoint regressed")
        if not advanced or sample["completed_checkpoint"] != advanced[-1]["completed_checkpoint"]:
            advanced.append(sample)
    if len(advanced) < 2:
        return {"status": "insufficient completed progress samples"}
    first, last = advanced[0], advanced[-1]
    duration = last["seconds"] - first["seconds"]
    if duration <= 0:
        raise ValueError("nonpositive completion window")
    count = last["completed_checkpoint"] - first["completed_checkpoint"]
    return {"seconds": duration, "completed_checkpoint_delta": count,
            "sampled_checkpoints_per_second": count / duration,
            "status": "short window; not steady state" if duration < 5 else "sampled completed window",
            "note": "250 ms observation resolution; not individual checkpoint latency"}


def verify_source_range(address, tip):
    # The pinned producer retains the most recent 5,000 full checkpoint contents.
    # Summary RPCs detect holes, not availability under failure; replay/audit remain required.
    if not 0 < tip < 5000:
        raise ValueError("frozen source must retain genesis: checkpoint tip must be 1..4999")

    def checkpoint(sequence):
        result = rpc(address, "sui_getCheckpoint", [str(sequence)])
        if int(result["sequenceNumber"]) != sequence:
            raise ValueError("source checkpoint sequence mismatch")
        return result["digest"]

    with ThreadPoolExecutor(max_workers=8) as pool:
        digests = list(pool.map(checkpoint, range(tip + 1)))
    return {"checkpoint_count": len(digests), "summary_range_verified": True,
            "full_contents_retention_guard": "tip below pinned producer 5000-checkpoint retention",
            "range_sha256": hashlib.sha256(json.dumps(digests).encode()).hexdigest()}


def rpc(address, method, params=(), timeout=15):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}).encode()
    request = urllib.request.Request(f"http://{address}", payload, {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        result = json.load(response)
    if "error" in result:
        raise RuntimeError(f"{method}: {result['error']}")
    return result["result"]


def configuration(root, name):
    with (root / f"{name}.yaml").open() as stream:
        config = yaml.safe_load(stream)
    bound = config.get("run-with-range")
    if isinstance(bound, dict) and set(bound) == {"Checkpoint"}:
        checkpoint = bound["Checkpoint"]
        if type(checkpoint) is not int or not 0 <= checkpoint < 1 << 64:
            raise ValueError("checkpoint bound must be a u64")
        config["run-with-range"] = CheckpointBound(checkpoint)
    return config


def wait_rpc(child, address):
    deadline = time.monotonic() + 300
    for _ in range(1200):
        if time.monotonic() >= deadline:
            break
        if child.poll() is not None:
            raise RuntimeError(f"Sui exited before RPC readiness: {child.returncode}")
        try:
            return rpc(address, "sui_getLatestCheckpointSequenceNumber", timeout=1)
        except (OSError, RuntimeError):
            time.sleep(0.25)
    raise TimeoutError("Sui RPC readiness exceeded five minutes")


def source_environment(previous):
    # Frozen sources predate these fields and used 32 MiB asynchronous native WALs.
    # Do not reinterpret their existing files with a new fragment size.
    return {"TH_FRAG_SIZE": str(previous.get("fragment_mib", 32) << 20),
            "TH_SYNC_WRITES": str(previous.get("durable", False)).lower()}


def compare_local_store(relative, left, right):
    for audit in (left, right):
        if audit.get("wal_recovery") != "AbsoluteConsistency":
            raise RuntimeError("local audit requires strict WAL recovery")
        if set(audit["tables"]) != set(audit.get("semantic_tables", {})):
            raise RuntimeError("local semantic audit must cover every raw table")
        for name, raw in audit["tables"].items():
            if raw["records"] != audit["semantic_tables"][name]["records"]:
                raise RuntimeError("local semantic audit must retain every raw row")
        if relative == "indexes":
            if audit.get("sequence_digest_bijection") is not True:
                raise RuntimeError("legacy index comparison requires validated sequence/digest bijection")
        elif audit["semantic_tables"] != audit["tables"]:
            raise RuntimeError("only legacy indexes allow local sequence normalization")
    return {"equal": left["semantic_tables"] == right["semantic_tables"],
            "raw_equal": left["tables"] == right["tables"],
            "native": left["tables"], "distributed": right["tables"],
            "native_semantic": left["semantic_tables"], "distributed_semantic": right["semantic_tables"],
            "native_schema": left["schema"], "distributed_schema": right["schema"],
            "comparison": "validated sequence/digest bijection and all semantic rows" if relative == "indexes" else "all raw rows"}


def local_rocksdb_paths(live, audited_roots=()):
    """Inventory RocksDB stores outside exact roots already audited by their engine."""
    excluded = {Path(name) for name in audited_roots}
    if len(excluded) > 32 or any(
            name.is_absolute() or not name.parts or any(part in (".", "..") for part in name.parts)
            for name in excluded):
        raise RuntimeError("invalid separately audited database roots")
    for name in excluded:
        target = live / name
        if target.is_symlink() or not target.is_dir() or (target / "CURRENT").exists():
            raise RuntimeError("separately audited database root is absent, unsafe or RocksDB")
    databases, visited = {}, 0
    for path, directories, files in os.walk(live, followlinks=False):
        visited += 1
        current = Path(path)
        if visited > 256 or len(current.relative_to(live).parts) > 8 or len(directories) + len(files) > 4096:
            raise RuntimeError("local store audit inventory exceeds bounds")
        if any((current / name).is_symlink() for name in directories + files):
            raise RuntimeError("local store audit refuses symlinks")
        if ".distributed-tidehunter" in directories:
            directories.clear()
        elif "CURRENT" in files:
            databases[str(current.relative_to(live))] = current
            directories.clear()
        if len(databases) > 32:
            raise RuntimeError("local store audit exceeds 32 databases")
        # Prune at the parent before enumerating immutable cells. Exact engine
        # state was audited separately; unknown stores must remain in inventory.
        directories[:] = [name for name in directories
                          if (current / name).relative_to(live) not in excluded]
    return databases


EXECUTION = re.compile(r'^(\S+) .*execute_checkpoint\{seq=(\d+)\}.*: executing checkpoint$')


def log_timing(text):
    lines = text.splitlines()
    first = next((datetime.fromisoformat(line.split()[0]) for line in lines if re.match(r'^\d{4}-', line)), None)
    events = [(datetime.fromisoformat(match[1]), int(match[2])) for line in lines
              if (match := EXECUTION.fullmatch(line))]
    if not events or first is None:
        raise ValueError("checkpoint execution timestamps absent")
    intervals = sorted((right[0] - left[0]).total_seconds() * 1000 for left, right in zip(events, events[1:]))
    return {"logged_execution_starts": len(events), "first_checkpoint": events[0][1], "last_checkpoint": events[-1][1],
            "first_log_to_first_execution_seconds": (events[0][0] - first).total_seconds(),
            "first_to_last_execution_start_seconds": (events[-1][0] - events[0][0]).total_seconds(),
            "start_spacing_ms": {name: intervals[min(len(intervals) - 1, math.ceil(q * len(intervals)) - 1)]
                                 for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))} if intervals else {},
            "caveat": "execution START spacing, not checkpoint completion/service latency; includes scheduling/waiting"}


def replay_phases(text, elapsed_seconds):
    """Separate logged startup from an upper bound that still includes shutdown.

    The first log follows process launch. Subtracting its startup interval from
    total process elapsed therefore overestimates, not underestimates, time after
    the first execution start. This is not a steady-state throughput estimate.
    """
    timing = log_timing(text)
    startup = timing["first_log_to_first_execution_seconds"]
    span = timing["first_to_last_execution_start_seconds"]
    if not math.isfinite(elapsed_seconds) or min(startup, span) < 0 or elapsed_seconds < startup + span:
        raise ValueError("process elapsed and logged execution timeline disagree")
    return {**timing, "after_first_execution_to_exit_upper_seconds": elapsed_seconds - startup,
            "phase_note": "upper bound includes final execution, shutdown and exit polling; not completion latency or steady state"}

