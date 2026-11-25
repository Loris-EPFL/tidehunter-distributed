"""Q1 bindings for the L2 multi-database kv-research owner launcher."""

from hashlib import blake2b
from pathlib import PurePosixPath

from q1_manifest import fields, integer
from q1_package import require


def bind_launch(prepared, owner, tier, unit_dir, nonce):
    """Bind a prepared L2 launch (including its TLS files and read limits) to one role."""
    require("host" in prepared and "node" not in prepared, "Q1 requires OwnerHost, not NodeConfig")
    host = prepared["host"]
    require(not ({"partition", "shape_yaml", "live_target", "trusted_coordinator_id"} & host.keys()),
            "Q1 owner must provision authenticated namespaces dynamically")
    tls = prepared["tls"]
    require(tls["transport"] == {"kind": "tcp"}
            and all(PurePosixPath(tls[key]).is_absolute() for key in ("ca", "cert", "key")),
            "Q1 owner requires TCP/mTLS")
    require(tier in ("native", "synchronous") and prepared["durability"]["tier"] == tier,
            "Q1 owner launch tier differs")
    require(PurePosixPath(unit_dir).is_absolute() and isinstance(nonce, str) and nonce,
            "Q1 owner needs a scoped unit directory and launch nonce")
    return prepared | {"partition": owner["partition"], "listen": owner["control"],
                       "host": host | {key: owner[key] for key in ("root", "max_databases", "live_target_pool")},
                       "ready_file": str(PurePosixPath(unit_dir, "ready")),
                       "stop_file": str(PurePosixPath(unit_dir, "stop")), "launch_nonce": nonce,
                       "drop_first_append_response": False}


def owner_command(binary, launch_file):
    return [str(binary), "--experimental-storage-node", str(launch_file)]


def validate_config_command(binary, coordinator_config, owner_launches):
    require(len(owner_launches) == 2, "Q1 validates both OwnerHost launches")
    return [str(binary), "--experimental-validate-config", str(coordinator_config), *map(str, owner_launches)]


def verify_ready(receipt, launch, expected_boot_id, *, previous=None):
    """Admit readiness for this incarnation, not a claim about a provisioned data route."""
    names = "partition address launch_nonce pid boot_id started_unix_ns durability"
    if "history_persistence" in launch:  # L2 before the R3 cutover
        names += " history_persistence"
    fields(receipt, names, "OwnerHost receipt")
    fields(receipt["durability"], "tier max_unsynced_age_ms max_pending_completions", "OwnerHost durability")
    require(receipt.get("history_persistence") == launch.get("history_persistence")
            and integer(receipt["partition"], 0) and receipt["partition"] == launch["partition"]
            and receipt["address"] == launch["listen"] and receipt["launch_nonce"] == launch["launch_nonce"]
            and all(receipt["durability"][key] == value for key, value in launch["durability"].items())
            and integer(receipt["durability"]["max_pending_completions"]) and integer(receipt["pid"])
            and isinstance(expected_boot_id, str) and expected_boot_id
            and receipt["boot_id"] == expected_boot_id and integer(receipt["started_unix_ns"]),
            "OwnerHost receipt differs from admitted launch")
    if previous is not None:
        require(all(receipt[key] == previous[key] for key in ("partition", "address", "launch_nonce", "durability"))
                and receipt["started_unix_ns"] > previous["started_unix_ns"]
                and (receipt["pid"], receipt["boot_id"]) != (previous["pid"], previous["boot_id"]),
                "OwnerHost restart reused or changed the launch")
    return receipt


def verify_shutdown(proof, launch, launch_bytes, identity, ready, unit_status, expected_databases):
    """Bind schema 2 to the exact launch, process and independently observed target set."""
    fields(proof, "schema_version scope complete owner targets process_id process_start_ticks "
                  "launch_blake2b512 serve_error cleanup_error preparation inline_exports owner_threads",
           "OwnerHost shutdown proof")
    stopped = {"MainPID": "0", "SubState": "exited", "Result": "success", "ExecMainStatus": "0"}
    require(all(unit_status.get(key) == value for key, value in stopped.items())
            and integer(proof["schema_version"], 2, 2) and proof["scope"] == "kernel_live_target_shutdown"
            and proof["complete"] is True and proof["serve_error"] is None and proof["cleanup_error"] is None
            and proof["owner"] == {"partition": launch["partition"], "root": launch["host"]["root"]}
            and proof["process_id"] == ready["pid"] and integer(proof["process_start_ticks"])
            and proof["launch_blake2b512"] == blake2b(launch_bytes).hexdigest()
            and all(proof[key] == identity[key] for key in
                    ("process_id", "process_start_ticks", "launch_blake2b512")),
            "OwnerHost did not shut down the admitted process cleanly")
    seen = set()
    for entry in proof["targets"]:
        fields(entry, "database target", "OwnerHost shutdown target")
        database, target = entry["database"], entry["target"]
        require(isinstance(database, list) and len(database) == 16 and any(database)
                and all(integer(byte, 0, 255) for byte in database)
                and target["fixture_only"] is False and target["target_implementation"] == "kernel"
                and integer(target["errors"], 0, 0)
                and integer(target["owned_namespace_count"], 0, 0), "unclean OwnerHost target")
        key = bytes(database).hex()
        require(key not in seen, "duplicate OwnerHost shutdown database")
        seen.add(key)
    require(seen and seen == set(expected_databases), "OwnerHost shutdown target set differs from inventory")
    return proof
