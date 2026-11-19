"""Roadmap Phase 6 A1 case matrix, exact run configurations and run order.

Source of truth: checkpoint-work/research/design/INDEX-A0-A1.md ("A1: fixture
and exact run inputs", "A1: execution, resource budget and decision").

Every configuration is generated here deterministically; the controller writes
the exact generated JSON next to each case's results. Only the absolute data
path and, for overwrite stages, the selected per-tier integer rate vary.

Design-document correction: the engine field of the replay envelope is
`max_encoded_bytes` (`ReplayLimits`, deny_unknown_fields); the design's
canonical example spells it `max_bytes`, which the engine rejects.
"""

from __future__ import annotations

import copy

SIZES = (1_000_000, 5_000_000, 10_000_000)
TIERS = ("native", "synchronous")
# Arm indices of the design's interleaving: [E4, E16, C, C-scaled].
ARM_ORDER = ("e4", "e16", "c", "c-scaled")
# Production C needs an 8 GiB cadence that the local 1 GiB identity runway
# cannot reach: never run locally (design, "Cadence preflight is a hard gate").
LOCAL_ARMS = ("e4", "e16", "c-scaled")
DECISION_ARMS = ("e4", "e16")
REPEATS = 5
SEED = 20261119
NAMESPACE = "a1a0a1a0a1a0a1a0a1a0a1a0a1a0a1a0"
VALUE_SIZE = 1024
TIMED_OPERATIONS = 10_000_000
PILOT_OPERATIONS = 1_000_000
PILOT_EXTENDED_OPERATIONS = 2_000_000
CONFIRM_OPERATIONS = 1_000_000
E4 = {"max_batches": 4096, "max_encoded_bytes": 268_435_456}
E16 = {"max_batches": 16_384, "max_encoded_bytes": 1_073_741_824}
# Soft triggers are one quarter of the envelope (remote_db/replay_limits.rs).
SOFT_BYTES = {"e4": E4["max_encoded_bytes"] // 4, "e16": E16["max_encoded_bytes"] // 4}
PRODUCTION_C = {"dirty_keys": 1024, "snapshot_written_bytes": 8_589_934_592, "stale_wal_bytes": 8_053_063_680}
# Config::small() at 1M, scaled by N / 1M for all three inputs.
C_SCALED = {
    1_000_000: {"dirty_keys": 32, "snapshot_written_bytes": 134_217_728, "stale_wal_bytes": 268_435_456},
    5_000_000: {"dirty_keys": 160, "snapshot_written_bytes": 671_088_640, "stale_wal_bytes": 1_342_177_280},
    10_000_000: {"dirty_keys": 320, "snapshot_written_bytes": 1_342_177_280, "stale_wal_bytes": 2_684_354_560},
}
# C-scaled 10M cannot reach its 1.25 GiB cadence before a safety root.
C_SCALED_SIZES = (1_000_000, 5_000_000)
FIXTURE = {
    "max_admitted_batches": 262_144,
    "max_index_bytes": 536_870_912,
    "fragment_bytes": 4_194_304,
    "wal_file_bytes": 1_073_741_824,
}


def case_name(size: int, arm: str, tier: str, repeat: int | str) -> str:
    suffix = repeat if isinstance(repeat, str) else f"r{repeat}"
    return f"n{size}-{arm}-{tier}-{suffix}"


def case_path(workspace: str, case: str) -> str:
    return f"{workspace}/scratch/IndexA1/cases/{case}/db"


def _arm_fixture(arm: str, size: int) -> tuple[str, dict, dict | None]:
    if arm == "e4":
        return "exact", E4, None
    if arm == "e16":
        return "exact", E16, None
    if arm == "c":
        return "per_cell", E16, PRODUCTION_C
    if arm == "c-scaled":
        return "per_cell", E16, C_SCALED[size]
    raise ValueError(f"unknown arm {arm}")


def config(
    workspace: str,
    case: str,
    size: int,
    arm: str,
    tier: str,
    stage: str,
    *,
    operations: int | None = None,
    rate: int | None = None,
) -> dict:
    """Exact kv-research run configuration of one stage.

    build: closed loop, `new` IDs 0..N, measurement = N.
    overwrite: `overwrite` policy; open loop at `rate`, or closed loop
    (pilot) when `rate` is None; measurement = `operations` (default 10M).
    """
    if tier not in TIERS or size not in SIZES or stage not in ("build", "overwrite"):
        raise ValueError(f"invalid case input {size} {tier} {stage}")
    kind, replay, per_cell = _arm_fixture(arm, size)
    fixture = {"stage": stage, "arm": kind, **FIXTURE}
    if per_cell is not None:
        fixture["per_cell"] = dict(per_cell)
    execution = {
        "mode": "closed_loop",
        "concurrency": 16,
        "client_shards": 1,
        "queue_capacity": 64,
        "request_timeout_ms": 30000,
    }
    if stage == "build":
        measurement = size
        policy = "new"
        if operations is not None or rate is not None:
            raise ValueError("build stages take no operations or rate")
    else:
        measurement = TIMED_OPERATIONS if operations is None else operations
        policy = "overwrite"
        if rate is not None:
            if not isinstance(rate, int) or rate <= 0:
                raise ValueError("open-loop rate must be a positive integer")
            execution["mode"] = "open_loop"
            execution["offered_ops_per_second"] = rate
    return {
        "schema_version": 1,
        "case_id": f"{case}-{stage}",
        "backend": {
            "kind": "tidehunter",
            "options": {
                "path": case_path(workspace, case),
                "owners": 1,
                "transport": {"kind": "tcp-trusted"},
                "coordinator_read_cache_bytes": 0,
                "durability_tier": tier,
                "replay_limits": copy.deepcopy(replay),
                "index_fixture": fixture,
            },
        },
        "seed": SEED,
        "key_namespace_hex": NAMESPACE,
        "dataset": {"keys": size, "value_size": VALUE_SIZE, "entropy": 1.0},
        "workload": {
            "kind": "synthetic",
            "distribution": "uniform",
            "put_key_policy": policy,
            "mix": {"read": 0, "put": 100, "delete": 0, "missing_read": 0, "get_many": 0, "put_batch": 0},
            "batch_size": 32,
        },
        "execution": execution,
        "phases": {
            "preload": False,
            "verify_existing_dataset": False,
            "warmup_operations": 0,
            "aging_operations": 0,
            "measurement_operations": measurement,
        },
        "qualification_only": False,
        "diagnostics": {
            "index_checkpoint": True,
            "write_profile_limit": 0,
            "read_profile_limit": 0,
            "write_timeline_limit": 0,
        },
        "limits": {
            "max_keys": 10_000_000,
            "max_bytes": 21_474_836_480,
            "max_operations": 10_000_000,
            "deadline_seconds": 21600,
            "shutdown_grace_seconds": 300,
        },
    }


def _rotate(items: list, count: int) -> list:
    count %= len(items)
    return items[count:] + items[:count]


def block_order(repeat: int, available: frozenset[tuple[int, str, str]]) -> list[tuple[int, str, str]]:
    """Design run order of block `repeat`: sizes rotated left by R mod 3; tier
    order alternates by (R + size_index) mod 2; arms [E4, E16, C, C-scaled]
    rotated left by (R + size_index + tier_index) mod 4 and reversed for odd R.
    Unavailable (size, tier, arm) cells are skipped without substitution."""
    order = []
    for size in _rotate(list(SIZES), repeat % 3):
        size_index = SIZES.index(size)
        tiers = list(TIERS) if (repeat + size_index) % 2 == 0 else list(reversed(TIERS))
        for tier in tiers:
            tier_index = TIERS.index(tier)
            arms = _rotate(list(ARM_ORDER), repeat + size_index + tier_index)
            if repeat % 2 == 1:
                arms.reverse()
            order.extend((size, tier, arm) for arm in arms if (size, tier, arm) in available)
    return order


def local_cells(c_scaled_ready: frozenset[tuple[int, str]]) -> frozenset[tuple[int, str, str]]:
    """(size, tier, arm) cells run locally. C-scaled only where its bounded
    preflight reached a natural cadence adoption."""
    cells = {(size, tier, arm) for size in SIZES for tier in TIERS for arm in DECISION_ARMS}
    cells |= {(size, tier, "c-scaled") for size, tier in c_scaled_ready if size in C_SCALED_SIZES}
    return frozenset(cells)


GIB = 1024**3


def case_budget(size: int, operations: int = TIMED_OPERATIONS) -> dict:
    """Worst-case allocation of one case (design step 4, [INFERENCE]): one 4 KiB
    leased frame per scalar put before grouping, no reduction budgeted."""
    owner = (size + operations) * 4096
    return {
        "owner_worst_bytes": owner,
        "owner_ceiling_bytes": 80 * GIB,
        "coordinator_ceiling_bytes": 16 * GIB,
        "output_ceiling_bytes": 4 * GIB,
        "case_ceiling_bytes": 100 * GIB,
        "free_reserve_bytes": 25 * GIB,
        "fits_owner_ceiling": owner <= 80 * GIB,
    }
