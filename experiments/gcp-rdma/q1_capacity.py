"""Capacity R3 admission and matched physical evidence checks."""

import json

from q1_manifest import fields, integer
from q1_package import hex_string, require


def _unique_fields(items):
    result = dict(items)
    require(len(result) == len(items), "duplicate field in capacity evidence")
    return result


def read_r0(path, *, run_id, workload, input_digest, min_rolls):
    """Read R0 format 1 after the `sample_count` correction; no inferred zeroes."""
    with path.open() as stream:
        report = json.load(stream, object_pairs_hook=_unique_fields)
    require(integer(report["format_version"], 1, 1)
            and (report["run_id"], report["workload"], report["input_digest"])
            == (run_id, workload, input_digest)
            and workload in ("sui_mainnet_retention", "synthetic_churn")
            and hex_string(report["database"], 32) and integer(report["wal_file_bytes"])
            and report["complete"] is True and not report["reasons"]
            and integer(report["value_wal_unlinks"], 0, 0) and integer(report["missed_events"], 0, 0)
            and integer(report["elapsed_ns"]), "R0 shadow report is stale or incomplete")
    samples, owners = report["samples"], report["owners"]
    require(isinstance(samples, list) and integer(report["sample_count"], 2, len(samples))
            and report["sample_count"] == len(samples)
            and samples[0]["event"] == "begin" and samples[-1]["event"] == "end"
            and isinstance(owners, list) and owners == sorted(set(owners)) and owners
            and all(integer(partition, 0) for partition in owners), "R0 missing complete sample window")
    rolls = {partition: set() for partition in owners}
    prune_events = roll_events = complete_prunes = 0
    previous_end = 0
    for i, sample in enumerate(samples):
        event = sample["event"]
        require(integer(sample["sample_id"], i, i) and (i in (0, len(samples) - 1) or event in ("prune", "roll"))
                and type(sample["complete"]) is bool
                and (sample["reason"] is None if sample["complete"] else bool(sample["reason"]))
                and integer(sample["observed_start_ns"], previous_end)
                and integer(sample["observed_end_ns"], sample["observed_start_ns"]),
                "R0 sample has gaps or invalid measurements")
        previous_end = sample["observed_end_ns"]
        prune_events += event == "prune"
        roll_events += event == "roll"
        require(bool(sample["rolled_files"]) == (event == "roll"), "R0 roll has no crossing")
        if not sample["complete"]:
            require(i not in (0, len(samples) - 1), "R0 boundary inventory incomplete")
            # As in the engine ledger, an uncertain intermediate observation
            # is preserved for audit but does not qualify a prune or rollover.
            continue
        complete_prunes += event == "prune"
        for row in sample["rolled_files"]:
            fields(row, "partition fencing_generation file_index", "R0 roll")
            require(row["partition"] in rolls and integer(row["fencing_generation"])
                    and integer(row["file_index"]), "R0 roll has invalid identity")
            rolls[row["partition"]].add((row["fencing_generation"], row["file_index"]))
        keys = set()
        for file in sample["files"]:
            key = file["partition"], file["fencing_generation"], file["file_start"]
            require(key[0] in rolls and integer(key[1]) and integer(key[2], 0)
                    and key[2] % report["wal_file_bytes"] == 0 and key not in keys
                    and file["file_end"] == key[2] + report["wal_file_bytes"]
                    and integer(file["allocated_bytes"], 0) and file["allocated_bytes"] % 512 == 0,
                    "R0 file identity or allocated bytes unavailable")
            keys.add(key)
    require(prune_events == report["prune_events"] and (complete_prunes > 0 or workload == "synthetic_churn")
            and roll_events == report["file_roll_events"] and integer(min_rolls, 3)
            and all(len(crossings) >= min_rolls for crossings in rolls.values()),
            "R0 did not observe the required prune and full-file rolls")
    growth = report["per_owner"]
    require([row["partition"] for row in growth] == owners
            and all(type(row["net_allocated_growth_bytes_per_day"]) is int and row["reason"] is None for row in growth),
            "R0 owner growth unqualified")
    worst = max(growth, key=lambda row: (row["net_allocated_growth_bytes_per_day"], -row["partition"]))
    require(report["worst_owner"] == {key: worst[key] for key in
                                      ("partition", "net_allocated_growth_bytes_per_day", "reason")},
            "R0 worst owner does not match per-owner slopes")
    return report


def verify_r1_retirement(seal, proof, outcome, *, file_start, file_bytes):
    """Check the existing active-stream R1 types, not an ordered cleanup trace."""
    fields(seal, "start end device inode root_generation last_batch_sequence", "R1 FileSeal")
    fields(proof, "file_start inode seal_generation", "R1 RetireProof")
    require(integer(file_start, 0) and integer(file_bytes)
            and seal["start"] == proof["file_start"] == file_start and seal["end"] == file_start + file_bytes
            and integer(seal["device"], 0) and integer(seal["inode"]) and integer(seal["root_generation"])
            and (seal["last_batch_sequence"] is None or integer(seal["last_batch_sequence"], 0))
            and proof["inode"] == {"device": seal["device"], "inode": seal["inode"]}
            and proof["seal_generation"] == seal["root_generation"] and outcome in ("Busy", "Completed"),
            "R1 retirement does not name the sealed WAL inode")
    return outcome

OWNERS = ("O0", "O1")


def validate_admission(admission):
    fields(admission, "retention_sha trigger policy r2_proof tiers cycles deadline_seconds owners", "R3 admission")
    require(hex_string(admission["retention_sha"], 64)
            and admission["trigger"] in ("space_uncertainty", "owner_skew_or_pin_lifetime")
            and admission["policy"] in ("dead_only", "evacuation")
            and (hex_string(admission["r2_proof"], 64) if admission["policy"] == "evacuation"
                 else admission["r2_proof"] is None)
            and admission["tiers"] in (["native"], ["synchronous"], ["native", "synchronous"])
            and integer(admission["cycles"], 3) and integer(admission["deadline_seconds"], maximum=1160)
            and set(admission["owners"]) == set(OWNERS), "invalid R3 trigger or matched matrix")
    for owner in OWNERS:
        limits = admission["owners"][owner]
        fields(limits, "reserve low high usable write_bytes cpu_ns", "R3 owner budget")
        require(all(integer(n) for n in limits.values())
                and limits["reserve"] < limits["low"] < limits["high"] < limits["usable"], "invalid owner budget")


def verify_capacity(admission, evidence, event_inputs):
    """Qualify the matched results with scoped R1 streams for each arm."""
    from q1_r1_events import collect_r1_events

    validate_admission(admission)
    require(set(evidence) == set(event_inputs) == set(admission["tiers"]), "partial R3 tier matrix")
    manifests = {}
    for tier, pair in evidence.items():
        fields(pair, "control policy", "R3 matched pair")
        control, selected = pair["control"], pair["policy"]
        inputs = event_inputs[tier]
        require(set(inputs) == {"control", "policy"}, "missing R1 stream inputs for capacity arm")
        manifests[tier] = {}
        for label, arm, kind in (("control", control, "no_gc"), ("policy", selected, admission["policy"])):
            fields(arm, "policy tier retention_sha state_sha older_cut_sha route key_partitions same_cell "
                        "goodput p99 p999 rejected unknown tails_qualified owner_evidence", "R3 arm")
            require(arm["policy"] == kind and arm["tier"] == tier and arm["retention_sha"] == admission["retention_sha"]
                    and hex_string(arm["state_sha"], 64) and hex_string(arm["older_cut_sha"], 64)
                    and arm["route"] == "tcp_mtls+nvme_rdma_kernel_buffered_per_owner"
                    and arm["key_partitions"] in ([0, 1], [1, 0]) and arm["same_cell"] is True
                    and arm["tails_qualified"] is True and arm["rejected"] == arm["unknown"] == 0
                    and all(integer(arm[k]) for k in ("goodput", "p99", "p999"))
                    and set(arm["owner_evidence"]) == set(OWNERS), "invalid R3 route, oracle or throughput evidence")
            source = inputs[label]
            scope = source["scope"]
            require((scope["tier"], scope["policy"], scope["retention_sha256"]) == (tier, kind, arm["retention_sha"]),
                    "R1 evidence belongs to another capacity arm")
            manifests[tier][label], _ = collect_r1_events(**source)
            for owner, data in arm["owner_evidence"].items():
                verify_owner(admission, owner, data, reclaim=kind != "no_gc")
        require(all(control[k] == selected[k] for k in ("state_sha", "older_cut_sha", "key_partitions"))
                and selected["goodput"] * 100 >= control["goodput"] * 95
                and selected["p99"] * 100 <= control["p99"] * 105
                and selected["p999"] * 100 <= control["p999"] * 110, "R3 oracle or foreground regression")
    return manifests


def verify_owner(admission, owner, data, *, reclaim):
    limits = admission["owners"][owner]
    fields(data, "min_free final_free written_bytes cpu_ns roll_ids held release drain target_removed retire "
                 "lag_start resumed generation_before generation_after reclaimed_blocks ledger_bytes fs_bytes "
                 "post_release_backlog read_after_retire dma_after_retire", "R3 owner evidence")
    require(integer(data["min_free"], limits["reserve"], limits["usable"])
            and integer(data["final_free"], data["min_free"], limits["usable"])
            and integer(data["written_bytes"], 1, limits["write_bytes"])
            and integer(data["cpu_ns"], 1, limits["cpu_ns"]), "R3 exceeded disk or resource bounds")
    rolls = data["roll_ids"]
    require(len(rolls) >= admission["cycles"] and all(integer(x) for x in rolls)
            and all(a < b for a, b in zip(rolls, rolls[1:])), "R3 actual roll evidence missing")
    require(integer(data["held"]) and integer(data["release"], data["held"] + 1)
            and integer(data["lag_start"]) and integer(data["resumed"], data["lag_start"] + 1)
            and integer(data["generation_before"]) and data["generation_before"] == data["generation_after"],
            "R3 missing pin/lag window or authority restarted")
    require(all(integer(data[k], 0, 0) for k in ("read_after_retire", "dma_after_retire")), "R3 I/O after retire")
    if reclaim:
        require(integer(data["drain"], data["release"]) and integer(data["target_removed"], data["drain"])
                and integer(data["retire"], data["target_removed"])
                and integer(data["reclaimed_blocks"]) and integer(data["post_release_backlog"], 0, 0)
                and data["reclaimed_blocks"] * 512 == data["ledger_bytes"] == data["fs_bytes"]
                and data["final_free"] > limits["high"], "R3 premature reclaim, residue or space mismatch")
    else:
        require(all(integer(data[k], 0, 0) for k in ("retire", "reclaimed_blocks", "ledger_bytes", "fs_bytes")),
                "R3 no-GC control reclaimed data")
