"""Read and join the R1 v1 producer streams without comparing guest clocks."""

import hashlib
import json

from q1_capacity import _unique_fields
from q1_manifest import fields, integer
from q1_package import hex_string, relative_path, require

ROLES = {"coordinator", "owner_O0", "owner_O1"}
COORDINATOR = {"victim_claimed", "initiator_cq_drained", "retirement_reply"}
OWNER = {"file_rolled", "seal_durable", "target_io_drained", "target_removed", "retirement_root_durable",
         "wal_unlinked", "already_absent", "wal_directory_synced", "target_tombstone_synced",
         "cleanup_root_durable", "pin_held", "pin_released"}
FILE_ID = ("partition", "fencing_generation", "file_start", "file_end", "device", "inode")


def _file(row):
    result = tuple(row[key] for key in FILE_ID)
    require(all(integer(value, 0) for value in result) and row["file_start"] < row["file_end"]
            and integer(row["inode"]), "R1 transition needs an exact file identity")
    return result


def _read(root, name, scope, process, max_events, max_bytes, max_line):
    relative = relative_path(name)
    role, incarnation = process["producer_role"], process["incarnation"]
    require(role in ROLES and hex_string(incarnation, 32)
            and relative.parts == ("capacity", "r1-events", role, incarnation + ".jsonl"),
            "unexpected or partial R1 stream path")
    path = root / relative
    require(path.resolve() == path and path.is_file(), "R1 stream missing or outside case root")
    sha = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        def line():
            nonlocal size
            data = stream.readline(max_line + 1)
            size += len(data)
            require(size <= max_bytes and len(data) <= max_line and data.endswith(b"\n"),
                    "R1 stream truncated or over its sealed byte/line bound")
            sha.update(data)
            return json.loads(data, object_pairs_hook=_unique_fields)

        header = line()
        fields(header, "schema_version run_id case_id tier policy retention_sha256 database_id producer_role "
                       "incarnation boot_id process_id process_start_ticks mono_clock", "R1 stream header")
        require(header == {**scope, **process, "schema_version": 1, "mono_clock": "monotonic_ns"}
                and integer(header["schema_version"], 1, 1) and integer(header["process_id"])
                and integer(header["process_start_ticks"]) and header["boot_id"], "R1 stream identity mismatch")
        records = []
        for seq in range(max_events + 1):
            row = line()
            if set(row) == {"terminal"}:
                terminal = row["terminal"]
                fields(terminal, "complete events lost", "R1 terminal")
                require(terminal["complete"] is True and integer(terminal["events"], seq, seq)
                        and integer(terminal["lost"], 0, 0) and not stream.read(1),
                        "R1 stream is not complete and lossless")
                return records, {"path": name, "bytes": size, "sha256": sha.hexdigest(),
                                 "producer_role": role, "incarnation": incarnation,
                                 "first_seq": 0 if records else None, "last_seq": seq - 1 if records else None,
                                 "complete": True, "lost": 0}
            fields(row, "seq monotonic_ns kind after partition fencing_generation file_start file_end device inode "
                        "attempt_id payload", "R1 event")
            require(seq < max_events and integer(row["seq"], seq, seq) and integer(row["monotonic_ns"], 0)
                    and (not records or records[-1]["monotonic_ns"] <= row["monotonic_ns"])
                    and row["kind"] in (COORDINATOR if role == "coordinator" else OWNER)
                    and integer(row["partition"], 0, 1) and integer(row["fencing_generation"])
                    and (integer(row["attempt_id"], 0) if role == "coordinator"
                         else row["attempt_id"] is None and role == f"owner_O{row['partition']}")
                    and isinstance(row["payload"], dict) and isinstance(row["after"], list), "invalid R1 event")
            for edge in row["after"]:
                fields(edge, "role incarnation seq", "R1 predecessor")
                require(edge["role"] == role and edge["incarnation"] == incarnation
                        and integer(edge["seq"], 0, seq - 1), "R1 predecessor is not earlier in the same stream")
            # Capture the causal DAG, not a cross-host join by timestamp.
            records.append(row)
    raise ValueError("Q1: R1 stream exceeds its sealed event bound")


def _after(row):
    return {edge["seq"] for edge in row["after"]}


def _completed(records):
    completed = set()
    replies = set()
    for row in records:
        if row["kind"] != "retirement_reply":
            continue
        attempt, payload = row["attempt_id"], row["payload"]
        require(attempt not in replies and payload["outcome"] in ("busy", "completed"), "invalid R1 reply")
        replies.add(attempt)
        keys = set()
        for seq in _after(row):
            drained = records[seq]
            require(drained["kind"] == "initiator_cq_drained" and drained["attempt_id"] == attempt
                    and drained["payload"] == {"basis": "no_reference_index_pins_to_terminal_cqe"}
                    and len(_after(drained)) == 1, "R1 reply lacks initiator drain")
            claim = records[next(iter(_after(drained)))]
            file = _file(claim)
            generation = claim["payload"]["seal_generation"]
            require(claim["kind"] == "victim_claimed" and claim["attempt_id"] == attempt and _file(drained) == file
                    and file[:2] == (row["partition"], row["fencing_generation"]) and integer(generation),
                    "R1 coordinator claim differs from drain or reply")
            keys.add((file, generation))
        require(sorted(file[2] for file, _ in keys) == payload["files"] and keys,
                "R1 reply victim list differs from claims")
        if payload["outcome"] == "completed":
            completed.update(keys)
    return completed


def _owner_proofs(records):
    qualified = set()
    for row in records:
        if row["kind"] != "cleanup_root_durable":
            continue
        fields(row["payload"], "root_generation stream", "R1 cleanup root")
        require(integer(row["payload"]["root_generation"]) and row["payload"]["stream"] in (None, "older_sealed"),
                "invalid R1 cleanup root")
        if row["payload"]["stream"] == "older_sealed":
            continue
        file = _file(row)

        def predecessor(current, kinds):
            require(len(_after(current)) == 1, "R1 owner step lacks its predecessor")
            before = records[next(iter(_after(current)))]
            require(before["kind"] in kinds, "R1 owner cleanup steps out of order")
            if before["kind"] != "wal_directory_synced":
                require(_file(before) == file, "R1 cleanup switched inode")
            return before

        tombstone = predecessor(row, {"target_tombstone_synced"})
        fields(tombstone["payload"], "tombstone_removed stream", "R1 target tombstone")
        require(type(tombstone["payload"]["tombstone_removed"]) is bool and tombstone["payload"]["stream"] is None,
                "invalid R1 target tombstone")
        synced = predecessor(tombstone, {"wal_directory_synced"})
        require((synced["partition"], synced["fencing_generation"]) == file[:2]
                and file[2] in synced["payload"]["files"], "R1 WAL directory sync did not name the victim")
        unlinks = [records[seq] for seq in _after(synced) if records[seq]["file_start"] == file[2]]
        require(len(unlinks) == 1 and unlinks[0]["kind"] in {"wal_unlinked", "already_absent"}
                and _file(unlinks[0]) == file, "R1 WAL directory sync lacks the exact unlink")
        unlink = unlinks[0]
        if unlink["payload"]["phase"] == "recovery":
            # A replayed unlink does not prove the live retirement of that file.
            continue
        root = predecessor(unlink, {"retirement_root_durable"})
        # Only a call that actually revoked a binding emits the drain/removal
        # pair. A previous attempt can also leave such a pair in the chain.
        current = root
        steps = {"target_removed", "seal_durable", "pin_held", "pin_released"}
        for _ in range(len(records)):
            before = predecessor(current, steps)
            if before["kind"] == "seal_durable":
                break
            current = before
            if before["kind"] == "target_removed":
                drained = predecessor(before, {"target_io_drained"})
                require(before["payload"] == {"tombstone_durable": True}
                        and drained["payload"] == {"record_pins": 0, "export_pins": 0, "drain": "namespace_enable_0"},
                        "R1 target drain evidence missing")
                current = drained
        seal = before
        require(seal["kind"] == "seal_durable", "R1 owner seal missing")
        generation = seal["payload"]["root_generation"]
        require(integer(generation) and generation == root["payload"]["seal_generation"]
                == unlink["payload"]["seal_generation"]
                and integer(root["payload"]["root_generation"], generation + 1)
                and integer(row["payload"]["root_generation"], root["payload"]["root_generation"] + 1),
                "R1 seal and retirement generations differ")
        qualified.add((file, generation))
    return qualified


def collect_r1_events(root, scope, streams, *, max_events, max_bytes, max_line):
    """Return a harness manifest and qualified same-generation retirements.

    Streams map admitted relative paths to independently observed process identities.
    The caller archives only after these records and the case-level gates pass.
    """
    fields(scope, "run_id case_id tier policy retention_sha256 database_id", "R1 stream scope")
    require(root.is_absolute() and root.resolve() == root and integer(max_events) and integer(max_bytes)
            and integer(max_line, maximum=max_bytes) and hex_string(scope["database_id"], 32)
            and hex_string(scope["retention_sha256"], 64), "invalid R1 stream collection scope")
    require(scope["policy"] != "evacuation", "R2 evacuation is not qualified by R1 retirement evidence")
    require(scope["tier"] in ("native", "synchronous") and scope["policy"] in ("dead_only", "no_gc"),
            "unsupported R1 tier or policy")
    manifest, completed, owner_proofs = [], set(), set()
    for name, process in sorted(streams.items()):
        fields(process, "producer_role incarnation boot_id process_id process_start_ticks", "R1 process identity")
        records, descriptor = _read(root, name, scope, process, max_events,
                                    max_bytes - sum(row["bytes"] for row in manifest), max_line)
        manifest.append(descriptor)
        if scope["policy"] == "no_gc":
            require(not any(row["kind"] in {"retirement_root_durable", "wal_unlinked", "already_absent",
                                           "wal_directory_synced", "cleanup_root_durable"} for row in records),
                    "R1 no-GC control retired files")
        if process["producer_role"] == "coordinator":
            completed.update(_completed(records))
        else:
            owner_proofs.update(_owner_proofs(records))
    require({row["producer_role"] for row in manifest} == ROLES, "missing R1 producer role")
    joined = completed & owner_proofs
    if scope["policy"] == "dead_only":
        require({file[0] for file, _ in joined} == {0, 1} and owner_proofs == completed,
                "R1 same-generation owner retirement lacks a completed matching coordinator proof")
    else:
        require(not completed and not owner_proofs, "R1 no-GC control retired files")
    return {"schema_version": 1, **scope, "streams": manifest}, joined
