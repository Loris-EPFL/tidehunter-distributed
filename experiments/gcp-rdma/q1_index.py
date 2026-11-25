"""Admission and evidence gates for the Q1 index stage."""

from statistics import median

from q1_manifest import fields, integer
from q1_package import hex_string, require

GIB = 1 << 30
FLEET_KEYS = 100_000_000
FLEET_MINIMUM_PUTS = 10_000_000
EXACT_SOFT_BYTES = {"E4": 64 << 20, "E16": 256 << 20}


def validate_stage(stage, *, fixture=False):
    fields(stage, "version owner_roots tier exact keys minimum_puts max_puts queue read_cache "
                  "rss_bytes cgroup_bytes owner_allocation_bytes", "index input")
    require(type(stage["version"]) is int and stage["version"] == 1
            and len(stage["owner_roots"]) == 2 and len(set(stage["owner_roots"])) == 2
            and all(hex_string(root, 64) for root in stage["owner_roots"])
            and stage["tier"] in ("native", "synchronous") and stage["exact"] in EXACT_SOFT_BYTES,
            "invalid index identity, tier or Exact setting")
    require(integer(stage["keys"], maximum=FLEET_KEYS) and integer(stage["minimum_puts"])
            and (fixture or (stage["keys"], stage["minimum_puts"]) == (FLEET_KEYS, FLEET_MINIMUM_PUTS))
            and integer(stage["max_puts"], stage["minimum_puts"])
            and integer(stage["queue"]) and stage["read_cache"] is False
            and stage["rss_bytes"] == 64 * GIB and stage["cgroup_bytes"] == 96 * GIB
            and len(stage["owner_allocation_bytes"]) == 2
            and all(integer(b) for b in stage["owner_allocation_bytes"]), "invalid index sizing or resource bounds")


def validate_build(stage, receipt, database_id):
    require(hex_string(database_id, 32) and receipt["database_id"] == database_id
            and receipt["owner_roots"] == stage["owner_roots"]
            and receipt["distinct_rows"] == receipt["successful"] == stage["keys"]
            and receipt["iterated_rows"] == stage["keys"] and integer(receipt["normal_generations"])
            and all(receipt[k] is True for k in ("normally_adopted", "persistence_covered", "clean_close")),
            "incomplete or mismatched index build receipt")


def pilot_rate(successful_per_second, *, fallback):
    require(integer(successful_per_second) and type(fallback) is bool, "invalid pilot rate")
    return successful_per_second * (56 if fallback else 70) // 100


class SuccessDomains:
    """Two bitmaps: timed ACKs take precedence over pilot ACKs."""

    def __init__(self, keys):
        require(integer(keys, maximum=FLEET_KEYS), "invalid bitmap size")
        self.keys = keys
        self.maps = {phase: bytearray((keys + 7) // 8) for phase in ("pilot", "overwrite")}

    def ack(self, key, phase):
        require(integer(key, 0, self.keys - 1) and phase in self.maps, "invalid overwrite ACK")
        i, bit = divmod(key, 8)
        self.maps[phase][i] |= 1 << bit

    def domain(self, key):
        require(integer(key, 0, self.keys - 1), "invalid oracle key")
        i, bit = divmod(key, 8)
        for phase in ("overwrite", "pilot"):
            if self.maps[phase][i] & (1 << bit):
                return phase
        return "build"


def validate_timed(stage, proof, database_id, allocation_peaks):
    begin, end = proof["start_ns"], proof["end_ns"]
    require(hex_string(database_id, 32) and proof["database_id"] == database_id
            and integer(begin) and integer(end, begin + 1)
            and integer(proof["offered"], stage["minimum_puts"], stage["max_puts"])
            and proof["offered"] == proof["accepted"]
            and all(integer(proof[k], 0, 0) for k in
                    ("rejected", "unknown", "wrong", "errors", "lost_generations", "cap_events"))
            and integer(proof["rss_peak"], maximum=stage["rss_bytes"])
            and integer(proof["cgroup_peak"], maximum=stage["cgroup_bytes"] - 1)
            and len(allocation_peaks) == 2
            and all(integer(a, maximum=b) for a, b in zip(allocation_peaks, stage["owner_allocation_bytes"])),
            "index overwrite counters or resources failed")
    generations = set()
    for row in proof["generations"]:
        fields(row, "database_id fencing_generation generation kind trigger_names adopted_ns installed_ns credited_ns",
               "generation")
        ident = row["fencing_generation"], row["generation"]
        require(row["database_id"] == database_id and all(integer(n) for n in ident)
                and row["kind"] in ("normal", "requested", "failed"), "wrong generation identity")
        times = [row[key] for key in ("adopted_ns", "installed_ns", "credited_ns")]
        if (row["kind"] == "normal" and all(integer(value, begin, end) for value in times)
                and times[0] <= times[1] <= times[2]):
            generations.add(ident)
    require(len(generations) >= 20 and proof["normal_generations_in_window"] == len(generations),
            "fewer than 20 eligible Exact generations or duplicate generation evidence")
    seconds = proof["seconds"]
    require(proof["missing_seconds"] == [] and len(seconds) == (end - begin) // 1_000_000_000
            and len(seconds) >= 3, "missing complete-second evidence")
    names = ("age_ns", "bytes", "batches")
    n = len(seconds)
    for i, row in enumerate(seconds):
        fields(row, "second age_ns bytes batches", "suffix sample")
        require(row["second"] == i and all(integer(row[k], 0) for k in names), "invalid suffix sample")
    thirds = [seconds[i*n//3:(i+1)*n//3] for i in range(3)]
    medians = {key: [median(row[key] for row in third) for third in thirds] for key in names}
    rising = {key for key, (a, b, c) in medians.items() if a < b < c}
    require(not ("age_ns" in rising and rising & {"bytes", "batches"})
            and medians["bytes"][2] - medians["bytes"][0] < EXACT_SOFT_BYTES[stage["exact"]],
            "index suffix debt is growing")
    return medians


def validate_cold(stage, stop, recovery):
    for selected in (recovery["index_pages"], recovery["wal_pages"]):
        require(selected is not None, "unqualified cold residency: " + str(recovery["cold_unqualified_reason"]))
        fields(selected, "selected resident", "residency proof")
        require(integer(selected["selected"]) and integer(selected["resident"], 0, 0),
                "cold residency requires zero resident pages in each nonempty whole-tree selection")
    require(recovery["cold"] is True and recovery["cold_unqualified_reason"] is None, "unqualified cold residency")
    frontier = stop["frontier"]
    fields(frontier, "fencing_generation sequence operation_index", "index frontier")
    require(integer(frontier["fencing_generation"]) and all(integer(frontier[key], 0) for key in
            ("sequence", "operation_index")) and recovery["frontier"] == recovery["stop_frontier"] == frontier
            and all(integer(stop[k], 0) and recovery[k] == stop[k]
                    for k in ("suffix_batches", "suffix_bytes", "touched_key"))
            and stop["touched_key"] < stage["keys"] and stop["forced_root"] is False
            and all(recovery[key] is True for key in ("frontier_matches", "tail_matches", "first_correct",
                    "fresh_owner_processes", "same_database", "fencing_generation_advanced"))
            and recovery["warmup"] is False and recovery["first_block_resident"] is False
            and integer(recovery["discarded"], 0, 0) and recovery["close_error"] is None
            and recovery["owner_stop_error"] is None and integer(recovery["open_to_read_ns"], maximum=60_000_000_000),
            "cold frontier, first read or recovery bound failed")


def validate_a1_selection(v6, manifest, report, exact):
    """Admit only the V6 receipt and A1 decision for the same local binary/tree."""
    require(all(integer(row["schema_version"], 1, 1) for row in (v6, manifest, report)),
            "unsupported A0/A1 output schema")
    ratio = v6["max_abs_residual_ratio"]
    observed = v6["runs"]
    require(v6["gate"] == "a0_v6_device_reconciliation" and v6["passed"] is True
            and not v6["failures"] and v6["tiers_covered"] == ["native", "synchronous"]
            and type(ratio) in (int, float) and 0 <= ratio <= 0.10
            and {row["tier"] for row in observed} == {"native", "synchronous"}
            and all(row["valid"] is True and not row["reasons"] and hex_string(row["diagnostics_sha256"], 64)
                    and type(row["ratio"]) in (int, float) and 0 <= row["ratio"] <= ratio for row in observed)
            and max(row["ratio"] for row in observed) == ratio
            and isinstance(manifest["tree"], str) and manifest["tree"]
            and hex_string(manifest["binary_sha256"], 64)
            and all(v6[key] == manifest[key] for key in ("tree", "binary_sha256")),
            "A0 V6 did not qualify this A1 run")
    decision = report["decision"]
    selected, gate = decision["selected_setting"], decision["v6_gate"]
    setting = report["settings"].get(selected, {})
    require(exact in EXACT_SOFT_BYTES and selected == exact.lower()
            and decision["verdict"] == "Exact sufficient (projected)"
            and decision["setting_status"].get(selected) == "pass"
            and setting.get("status") == "pass" and setting.get("arm") == selected
            and set(setting.get("tiers", {})) == {"native", "synchronous"}
            and gate["passed"] is True and gate["tree"] == v6["tree"]
            and gate["max_abs_residual_ratio"] == ratio,
            "A1 did not select the sealed Exact setting")
    return selected


def a1_local_command(binary, config_path, output, config, phase):
    """Run A1's existing local calibration interface, never the two-owner fleet cell."""
    options = config["backend"]["options"]
    stage = options["index_fixture"]["stage"]
    require(config["backend"]["kind"] == "tidehunter" and options["owners"] == 1
            and options["transport"] == {"kind": "tcp-trusted"}
            and integer(config["dataset"]["keys"], maximum=10_000_000), "not an A1 local fixture")
    require((phase, stage) in {("build", "build"), ("overwrite", "overwrite"),
                              ("cold-reopen", "overwrite")}, "wrong A1 fixture stage")
    command = "--index-a1-cold-reopen" if phase == "cold-reopen" else "--config"
    return [str(binary), command, str(config_path), "--output", str(output)]


def _dataset(config):
    return {"key_namespace_hex": config["key_namespace_hex"], "seed": config["seed"],
            **{key: config["dataset"][key] for key in ("keys", "value_size", "entropy")}}


def validate_a1_local_build(config, receipt):
    options = config["backend"]["options"]
    verification = receipt["verification"]
    keys = config["dataset"]["keys"]
    require(integer(receipt["schema_version"], 1, 1) and receipt["build_case_id"] == config["case_id"]
            and receipt["path"] == options["path"] and receipt["dataset"] == _dataset(config)
            and receipt["fixture"] == options["index_fixture"]
            and receipt["replay_limits"] == options["replay_limits"]
            and receipt["spec"]["tier"] == options["durability_tier"]
            and verification["dataset_rows"]["rows"] == verification["successful_puts"] == keys
            and integer(verification["dataset_rows"]["mismatched"], 0, 0)
            and verification["iterated_rows"] == keys + verification["other_rows"]
            and bool(verification["checkpoint_after_explicit_root"]) and bool(verification["owner_progress"]),
            "incomplete or stale local A1 build receipt")


def validate_a1_local_reopen(config, stop, cold):
    """Verify local A1's real stop and cold-reopen evidence without upgrading its scope."""
    require(all(integer(row["schema_version"], 1, 1) and row["case_id"] == config["case_id"]
                for row in (stop, cold)) and stop["stage"] == "overwrite"
            and stop["dataset"] == _dataset(config) and stop["measurement_ok"] is True
            and stop["timed"]["complete"] is True and stop["timed"]["ack_matches_publication"] is True
            and cold["stop_record"] == stop["stopped"]
            and cold["touched_count"] == stop["touched"]["count"] and integer(cold["touched_count"])
            and all(cold[key] is True for key in ("qualified", "cold", "correct", "tail_matches_stop_record",
                                                  "fencing_generation_advanced"))
            and cold["close_error"] is None and cold["owner_stop_error"] is None,
            "incomplete or stale local A1 overwrite/reopen evidence")
    first, residency = cold["first_read"], cold["residency"]
    recovered = cold["open"]["recovery"]
    require(recovered["tail_batches"] == stop["stopped"]["suffix_batches"]
            and recovered["tail_bytes"] == stop["stopped"]["suffix_bytes"]
            and integer(recovered["cut"]["discarded_batches"], 0, 0), "local A1 replay tail changed")
    require(first["id"] == stop["touched"]["first_read_id"] and first["correct"] is True
            and integer(first["open_start_to_first_correct_read_ns"])
            and residency["first_value_block_nonresident"] is True
            and 0 <= residency["coordinator"]["resident_fraction"] <= 0.01
            and integer(residency["owner_wal"]["resident_bytes"], 0, 0)
            and cold["verification"]["rows"] == config["dataset"]["keys"]
            and integer(cold["verification"]["mismatched"], 0, 0), "local A1 cold oracle failed")
    return first["open_start_to_first_correct_read_ns"]
