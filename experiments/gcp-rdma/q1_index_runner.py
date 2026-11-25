"""Offline adapter for the staged kv-research Q1 index producer (receipt v1)."""

import json
import math
from pathlib import PurePosixPath

from q1_index import EXACT_SOFT_BYTES, pilot_rate, validate_build, validate_cold, validate_stage, validate_timed
from q1_manifest import fields, integer
from q1_package import file_digest, hex_string, relative_path, require

PHASES = ("build", "pilot", "overwrite", "cold-reopen", "verify")
REQUIRED = {"pilot": {"pilot-touched.bin", "pilot-uncertain.bin"},
            "overwrite": {"timed-touched.bin", "timed-uncertain.bin", "overwrite-stop.json", "write-diagnostics.json"}}
FAILURE_COUNTS = ("rejected", "unknown", "wrong", "errors")


def stage_document(index, *, case_id, dataset, case_root, owners, workload, remote):
    """Wrap sealed index policy in the final placement-bound runner input."""
    validate_stage(index)
    require(case_id == f"index_{index['tier']}", "index case tier differs from the seal")
    fields(dataset, "key_namespace_hex seed value_size entropy", "index dataset")
    require(hex_string(dataset["key_namespace_hex"], 32) and integer(dataset["seed"], 0, 2**64 - 1)
            and integer(dataset["value_size"], 1024, 1024) and type(dataset["entropy"]) in (float, int)
            and 0 <= dataset["entropy"] <= 1, "invalid index dataset")
    require(PurePosixPath(case_root).is_absolute() and len(owners) == 2, "invalid index case and owner roots")
    for partition, owner in enumerate(owners):
        fields(owner, "partition address server_name root", "index owner endpoint")
        require(integer(owner["partition"], partition, partition) and owner["server_name"]
                and PurePosixPath(owner["root"]).is_absolute(), "invalid index owner endpoint")
    require(owners[0]["address"] != owners[1]["address"], "index owners must use separate endpoints")
    fields(workload, "clients request_timeout_ms deadline_seconds shutdown_grace_seconds pilot overwrite_operations",
           "index workload")
    pilot = workload["pilot"]
    base_pilot = {"operations", "confirm_seconds", "skip_seconds", "min_complete_seconds"}
    require(set(pilot) in (base_pilot, base_pilot | {"qualify"}), "invalid index pilot workload")
    if "qualify" in pilot:
        # The sealed Rust stage accepts an optional pilot.qualify (default
        # true; false is the CI local-proof route: floor(0.70 x median)
        # without confirmations or a debt rule). Fleet qualification stages
        # must keep it absent or true.
        require(pilot["qualify"] is True, "fleet index stages must run pilot qualification")
    require(integer(workload["clients"], maximum=index["queue"]) and integer(index["queue"], maximum=65536)
            and integer(workload["request_timeout_ms"]) and integer(workload["deadline_seconds"], maximum=86400)
            and integer(workload["shutdown_grace_seconds"], maximum=600)
            and integer(workload["overwrite_operations"], index["minimum_puts"], index["max_puts"])
            and integer(pilot["operations"]) and integer(pilot["confirm_seconds"], 9)
            and integer(pilot["skip_seconds"], 0) and integer(pilot["min_complete_seconds"]), "invalid index workload")
    live, storage = remote["live_nvme"], remote["storage"]
    require("database" not in storage and storage["partitions"] == [0, 1]
            and remote["endpoints"] == [{key: owner[key] for key in ("partition", "address", "server_name")}
                                        for owner in owners]
            and remote["tls"]["transport"] == {"kind": "tcp"} and integer(remote["read_cache_bytes"], 0, 0)
            and all(PurePosixPath(remote["tls"][key]).is_absolute() for key in ("ca", "cert", "key"))
            and live["export_mode"] == "per_owner" and live["leased_durability"] == index["tier"]
            and remote["replay_limits"] == {"max_batches": 4096 if index["exact"] == "E4" else 16384,
                                            "max_encoded_bytes": 4 * EXACT_SOFT_BYTES[index["exact"]]},
            "index requires provisioned two-owner TCP/mTLS and leased Exact route")
    return {"schema_version": 1, "kind": "q1_index_stage", "case_id": case_id, "mode": "fleet", "index": index,
            "dataset": dataset, "case_root": str(case_root), "owners": owners, "remote": remote, "workload": workload}


def stage_command(binary, config, phase, output, *, partition=None):
    require((phase in PHASES and partition is None) or (phase == "owner-observe" and integer(partition, 0, 1)),
            "invalid Q1 index command phase/partition")
    argv = [str(binary), "--q1-index-stage", str(config), "--phase", phase]
    if partition is not None:
        argv += ["--partition", str(partition)]
    return argv + ["--output", str(output)]


def _listed(root, row, remaining):
    fields(row, "file bytes sha256", "index artifact")
    path = root / relative_path(row["file"])
    require(path.is_file() and path.resolve() == path and integer(row["bytes"], 0, remaining)
            and path.stat().st_size == row["bytes"] and hex_string(row["sha256"], 64)
            and file_digest(path) == row["sha256"], f"invalid index artifact {row['file']}")
    return path


def _receipt(path, doc, phase, input_sha, binary_sha):
    require(path.is_absolute() and path.resolve() == path and path.is_file(), "missing or unsafe index receipt")
    row = json.loads(path.read_text())
    fields(row, "schema_version kind phase mode case_id input_sha256 executable_sha256 predecessors database_id "
                "owner_roots owners status error terminal artifacts recorded_unix_ns", "index stage receipt")
    require(integer(row["schema_version"], 1, 1) and row["kind"] == "q1_index_stage_receipt"
            and (row["phase"], row["mode"], row["case_id"], row["input_sha256"], row["executable_sha256"])
            == (phase, "fleet", doc["case_id"], input_sha, binary_sha) and row["owner_roots"] == doc["index"]["owner_roots"]
            and integer(row["recorded_unix_ns"]), "index receipt does not bind the sealed phase, input and binary")
    require(row["status"] == "complete" and row["error"] is None, f"index {phase} did not complete")
    return row


def _starts(doc, phase, row, admitted):
    require(len(row["owners"]) == 2 and len(admitted) == 2, "missing index owner start receipts")
    starts = []
    for owner, expected, saved in zip(doc["owners"], row["owners"], admitted):
        # Saved producer inputs are the exact owner receipts already checked by the guest.
        partition = owner["partition"]
        require(expected["partition"] == partition and expected["address"] == owner["address"]
                and expected["root"] == owner["root"] and expected["route"] == "external L2 OwnerHost (host launch)"
                and expected["start_receipt_file"] == f"{doc['case_root']}/owners/p{partition}.receipt.json"
                and expected["start_receipt_sha256"] == file_digest(saved)
                and expected["start_receipt_bytes"] == saved.stat().st_size,
                f"index {phase} owner inputs differ from saved receipt bytes")
        start = json.loads(saved.read_text())
        require(start == expected["start_receipt"] and start["partition"] == partition
                and start["address"] == owner["address"] and start["durability"]["tier"] == doc["index"]["tier"]
                and start["launch_nonce"] and start["boot_id"] and integer(start["pid"]) and integer(start["started_unix_ns"]),
                "invalid Q1 index OwnerHost start receipt")
        starts.append(start)
    return starts


def _measurement(row):
    return (integer(row["offered"]) and row["offered"] == row["accepted"]
            and all(integer(row[key], 0, 0) for key in FAILURE_COUNTS))


def _pilot(doc, terminal):
    closed, confirms = terminal["closed_loop"], terminal["confirmations"]
    rate, workload = closed["qualified_rate"], doc["workload"]
    measured = closed["measurement"]
    require(terminal["counts_toward_qualification"] is False and integer(rate) and _measurement(measured)
            and type(measured["median_puts_per_s"]) in (float, int) and math.isfinite(measured["median_puts_per_s"])
            and math.floor(measured["median_puts_per_s"]) == rate
            and len(measured["complete_seconds"]) >= workload["pilot"]["min_complete_seconds"]
            and type(measured["mean_in_flight"]) in (float, int) and math.isfinite(measured["mean_in_flight"])
            and measured["mean_in_flight"] >= workload["clients"] / 2 and len(confirms) in (1, 2),
            "index closed-loop pilot did not qualify")
    for i, confirm in enumerate(confirms):
        fallback = i == 1
        expected_attempt = "fallback_0_8" if fallback else "primary"
        require(confirm["attempt"] == expected_attempt and confirm["rate"] == pilot_rate(rate, fallback=fallback)
                and confirm["stable"] is (i == len(confirms) - 1), "invalid index pilot confirmation sequence")
    chosen = confirms[-1]
    require(_measurement(chosen["measurement"]) and chosen["debt"]["growing"] is False
            and chosen["measurement"]["offered"] == chosen["rate"] * workload["pilot"]["confirm_seconds"]
            and len(chosen["seconds"]) >= 8 and terminal["selected_rate"] == chosen["rate"]
            and terminal["selected_attempt"] == chosen["attempt"] and terminal["fallback_used"] is (len(confirms) == 2),
            "index pilot did not confirm the selected rate")
    return chosen["rate"]


def _observe(doc, path, owner, *, input_sha, binary_sha, database, peak):
    receipt = _receipt(path, doc, "owner-observe", input_sha, binary_sha)
    terminal, partition = receipt["terminal"], owner["partition"]
    require(receipt["database_id"] is None and receipt["owners"] is None and receipt["predecessors"] == []
            and receipt["artifacts"] == [] and terminal["partition"] == partition and terminal["root"] == owner["root"]
            and terminal["root_fingerprint"] == doc["index"]["owner_roots"][partition]
            and terminal["databases"] == [database] and terminal["evicted"] is True
            and integer(terminal["allocated_bytes"], 1, peak)
            and terminal["allocation"]["allocated_bytes"] == terminal["allocated_bytes"],
            "owner observation is not the stopped tree of the admitted index database")
    return terminal["residency"]


def _bitmaps(stage, paths, terminals):
    expected_bytes = 8 * ((stage["keys"] + 63) // 64)
    for stem, labels in (("pilot", ("pilot", "pilot_uncertain")), ("timed", ("timed", "timed_uncertain"))):
        for suffix, label in zip(("touched", "uncertain"), labels):
            name = f"{stem}-{suffix}.bin"
            path = paths[name]
            count = terminals["verify"]["domains"][label]
            require(path.stat().st_size == expected_bytes and integer(count, 0, stage["keys"]), "invalid index bitmap")
            bits = int.from_bytes(path.read_bytes(), "little")
            require(bits.bit_length() <= stage["keys"] and bits.bit_count() == count
                    and (suffix != "uncertain" or count == 0), "index bitmap differs from exact row oracle")
            if name == "timed-touched.bin":
                stop = terminals["overwrite"]["stop"]
                require(stop["touched_count"] == count and bits and (bits & -bits).bit_length() - 1 == stop["touched_key"],
                        "cold read did not nominate a timed-acknowledged row")


def collect_index_stages(stage_file, phase_dirs, owner_starts, observations, owner_shutdowns, allocation_peaks, *,
                         input_sha, binary_sha, max_bytes):
    """Check verified guest artifacts. Peak and stop provenance come from owner supervision."""
    require(stage_file.is_absolute() and stage_file.resolve() == stage_file and file_digest(stage_file) == input_sha,
            "index stage bytes differ from the placement binding")
    doc = json.loads(stage_file.read_text())
    require(doc == stage_document(**{
        key: doc[key] for key in ("index", "case_id", "dataset", "case_root", "owners", "workload", "remote")
    }), "wrong staged fleet index document")
    stage = doc["index"]
    require(doc["mode"] == "fleet" and set(phase_dirs) == set(owner_starts) == set(PHASES)
            and set(observations) == set(owner_shutdowns) == set(allocation_peaks) == {0, 1}
            and hex_string(input_sha, 64) and hex_string(binary_sha, 64) and integer(max_bytes), "incomplete index cell")
    predecessors, terminals, paths, starts = [], {}, {}, {}
    database = None
    for phase in PHASES:
        root = phase_dirs[phase]
        name = f"{phase}-receipt.json"
        row = _receipt(root / name, doc, phase, input_sha, binary_sha)
        if phase == "build":
            database = row["database_id"]
        require(hex_string(database, 32) and row["database_id"] == row["terminal"]["database_id"] == database
                and row["predecessors"] == predecessors, "index receipt chain changed database or predecessor")
        starts[phase] = _starts(doc, phase, row, owner_starts[phase])
        predecessors.append({"phase": phase, "file": name, "sha256": file_digest(root / name)})
        terminals[phase] = row["terminal"]
        seen = set()
        for item in row["artifacts"]:
            path = _listed(root, item, max_bytes)
            require(item["file"] not in seen, "duplicate index artifact")
            max_bytes -= item["bytes"]
            seen.add(item["file"])
            if item["file"] in REQUIRED.get(phase, ()):
                paths[item["file"]] = path
        require(REQUIRED.get(phase, set()) <= seen, f"missing required index {phase} artifact")
    validate_build(stage, terminals["build"], database)
    rate = _pilot(doc, terminals["pilot"])
    overwrite, cold, verify = (terminals[phase] for phase in PHASES[2:])
    require({row["launch_nonce"] for row in starts["overwrite"]}.isdisjoint(
                row["launch_nonce"] for row in starts["cold-reopen"])
            and all((old["pid"], old["boot_id"]) != (new["pid"], new["boot_id"])
                    and old["started_unix_ns"] < new["started_unix_ns"]
                    for old, new in zip(starts["overwrite"], starts["cold-reopen"])),
            "index cold recovery did not restart both owners")
    require(integer(rate) and overwrite["rate"] == rate and overwrite["owner_allocation_peaks"] is None
            and overwrite["offered"] == doc["workload"]["overwrite_operations"], "index timed run differs from admitted pilot")
    peaks = [allocation_peaks[partition] for partition in range(2)]
    debt = validate_timed(stage, overwrite, database, peaks)
    require(overwrite["debt"] == {"medians_by_third": debt, "soft_bytes": EXACT_SOFT_BYTES[stage["exact"]],
                                  "growing": False}, "index debt disagrees with timed evidence")
    stop = overwrite["stop"]
    require([row["partition"] for row in stop["owner_progress"]] == [0, 1]
            and all(isinstance(row["stats"], dict) and row["stats"] for row in stop["owner_progress"]),
            "index stop lacks per-partition owner progress")
    require(json.loads(paths["overwrite-stop.json"].read_text()) == stop, "index stop artifact differs from receipt")
    validate_cold(stage, stop, cold)
    resident = cold["residency"]["owner_observations"]
    require(len(resident) == 2, "missing cold owner observation references")
    selected_pages = 0
    for owner, evidence, peak in zip(doc["owners"], resident, peaks):
        partition = owner["partition"]
        path = observations[partition]
        residency = _observe(doc, path, owner, input_sha=input_sha, binary_sha=binary_sha, database=database, peak=peak)
        require(evidence["partition"] == partition and evidence["residency"] == residency
                and evidence["receipt"] == f"{doc['case_root']}/owners/p{partition}.observe.json"
                and evidence["receipt_sha256"] == file_digest(path) and evidence["receipt_bytes"] == path.stat().st_size
                and integer(residency["selected_pages"]) and integer(residency["resident_pages"], 0, 0),
                "cold owner pages do not bind the evicted tree")
        selected_pages += residency["selected_pages"]
    require(cold["wal_pages"] == {"selected": selected_pages, "resident": 0}, "cold owner page totals differ")
    require(verify["rows"] == verify["correct"] == verify["iterated_rows"] == stage["keys"]
            and all(integer(verify[key], 0, 0) for key in ("ambiguous_accepted", "missing", "wrong", "extra_rows"))
            and verify["close_error"] is None and verify["owner_stop_error"] is None
            and verify["owner_inventory"]["foreign_target_databases"] == [], "index full row oracle failed")
    inventory = {item["file"]: item for item in verify["owner_inventory"]["files"]}
    require(len(inventory) == len(verify["owner_inventory"]["files"]), "duplicate index owner inventory path")
    for owner in doc["owners"]:
        # The caller supplies the previously validated schema-2 shutdown bytes
        # and the path under which the producer saw them on the guest.
        guest_name, path = owner_shutdowns[owner["partition"]]
        item = inventory.get(guest_name)
        require(item is not None and item["scope"] == "kernel_live_target_shutdown"
                and item["schema_version"] == 2 and item["complete"] is True
                and item["target_databases"] == [database] and item["bytes"] == path.stat().st_size
                and item["sha256"] == file_digest(path), "index shutdown differs from owner inventory")
        proof = json.loads(path.read_text())
        require(proof["owner"] == {"partition": owner["partition"], "root": owner["root"]}
                and proof["complete"] is True and proof["schema_version"] == 2
                and len(proof["targets"]) == 1 and bytes(proof["targets"][0]["database"]).hex() == database,
                "index shutdown proof names another owner or database")
    _bitmaps(stage, paths, terminals)
    return {"database_id": database, "suffix_medians": debt, "open_to_read_ns": cold["open_to_read_ns"]}
