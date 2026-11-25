"""Q1 golden topology and qualification of typed Rust exports.

The Rust tool owns genesis signing, workload execution and canonical digesting.
"""

import json
from pathlib import PurePosixPath

from q1_manifest import MANDATORY, SLOTS, STATUSES, fields, integer, roles
from q1_package import artifact, hex_string, require

EPOCH_MS = 30_000
RETENTION = {"kind": "retain_all"}
DATABASES = ("authorities_db/live/checkpoints", "authorities_db/live/store/perpetual")
TEMPLATES = ("authorities_db/live/store/epoch_{n}", "consensus_db/{n}")


def topology(placement, case, peak_namespaces):
    require(case in MANDATORY[:2] and integer(peak_namespaces, 10, 32), "invalid golden topology")
    base = PurePosixPath("/mnt/tidehunter-q1", placement["run_id"], case)
    validators, owners = {}, {}
    for i, slot in enumerate(SLOTS[:4]):
        ip = placement["slots"][slot]["ordinary_ip"]
        remote = roles(case)[f"validator_{slot}"][1] == "leased"
        root = base / slot / "sui"
        capsule = base / slot / f"validator-{i}"
        env = {"USE_TIDEHUNTER": "1", "TH_SYNC_WRITES": "false"}
        if remote:
            env["SUI_SHARDED_WAL_CONFIG"] = str(capsule / "sharded-wal.json")
            env["SUI_SHARDED_WAL_SCHEMA_OUTPUT"] = str(root / "golden-schema")
        validators[slot] = {"host": ip, "port_base": 17000, "root": str(root), "env": env,
                            "capsule": str(capsule / "capsule.json"),
                            "backend": "remote" if remote else "native"}
        if not remote:
            continue
        for partition, owner in enumerate(SLOTS[4:]):
            host = placement["slots"][owner]
            beginning = 18000 + i * 128
            owners[f"owner_{slot}_{owner}"] = {
                "partition": partition, "root": str(base / owner / slot),
                "control": f"{host['ordinary_ip']}:{17100 + i}", "rdma_ip": host["rdma_ip"],
                "live_target_pool": {
                    "address": host["rdma_ip"], "transport": "rdma",
                    "public_port_base": beginning, "public_service_base": beginning,
                    "commissioning_port_base": beginning + peak_namespaces,
                    "commissioning_service_base": beginning + peak_namespaces,
                },
                "max_databases": peak_namespaces,
            }
    driver = {"slot": "V0", "host": placement["slots"]["V0"]["ordinary_ip"], "port_base": 17020,
              "root": str(base / "V0" / "fullnode"), "capsule": str(base / "V0" / "driver" / "capsule.json")}
    return {"epoch_duration_ms": EPOCH_MS, "retention": RETENTION.copy(), "validators": validators,
            "owners": owners, "driver": driver}


def genesis_spec(placement, case, layout, routes):
    """Build the post-L2 CLI input from prepared, private coordinator routes."""
    validators = []
    require(set(routes) == {s for s, v in layout["validators"].items() if v["backend"] == "remote"},
            "incorrect golden route roles")
    for slot in SLOTS[:4]:
        row = layout["validators"][slot]
        backend = {"kind": row["backend"]}
        if slot in routes:
            route = routes[slot]
            fields(route, "databases templates", "golden route groups")
            require(set(route["databases"]) == set(DATABASES) and set(route["templates"]) == set(TEMPLATES),
                    "incorrect L2 exact routes or template patterns")
            expected = [layout["owners"][f"owner_{slot}_{owner}"]["control"] for owner in SLOTS[4:]]
            for config in (*route["databases"].values(), *route["templates"].values()):
                require("local_live_fixture" not in config and "database" not in config["storage"]
                        and config["tls"]["transport"] == {"kind": "tcp"}
                        and config["live_nvme"]["leased_durability"] == "native"
                        and config["live_nvme"]["export_mode"] == "per_owner"
                        and [(endpoint["partition"], endpoint["address"]) for endpoint in config["endpoints"]]
                        == list(enumerate(expected)),
                        "route must provision on the validator's two Native owner hosts")
            backend.update(route)
        validators.append({key: row[key] for key in ("host", "port_base", "root")} | {"backend": backend})
    return {"format": 1, "run_id": placement["run_id"], "case_id": case,
            "epoch_duration_ms": EPOCH_MS, "restart_validator": 3, "sync_writes": False,
            "retention": RETENTION.copy(), "validators": validators,
            "fullnode": {key: layout["driver"][key] for key in ("host", "port_base", "root")}}


def arm_index(genesis, genesis_receipt, spec, drive, captures, reopens):
    """Render the verify input from the already admitted genesis and four process pairs."""
    process_indices = set(range(4))
    require(set(captures) == set(reopens) == process_indices
            and genesis_receipt["phase"] == "genesis" and genesis_receipt["status"] == "passed"
            and all(genesis_receipt[key] == spec[key] for key in ("run_id", "case_id")),
            "wrong genesis or incomplete validator outputs")
    sealed = {row["path"]: row["sha256"] for row in genesis_receipt["artifacts"]}
    committee = json.loads(artifact(genesis, "committee.json", sealed["committee.json"]).read_text())
    members = committee["validators"]
    require(len(members) == 4 and {m["index"] for m in members} == process_indices
            and len({m["name"] for m in members}) == 4, "wrong four-member committee")
    validators = []
    for row in sorted(members, key=lambda member: member["index"]):
        index = row["index"]
        node = spec["validators"][index]
        kind = node["backend"]["kind"]
        require(row["backend"] == kind and all(row[key] == node[key] for key in ("host", "port_base", "root"))
                and row["sharded_routes"] == ({"databases": list(DATABASES), "templates": list(TEMPLATES)}
                                               if kind == "remote" else None),
                "committee differs from admitted genesis routes")
        validators.append({"index": index, "name": row["name"], "backend": kind,
                           "pre_stop": str(captures[index]), "reopened": str(reopens[index])})
    return {"format": 1, "run_id": spec["run_id"], "case_id": spec["case_id"], "genesis": str(genesis),
            "drive": str(drive), "validators": validators}


def producer_command(binary, phase, source, *, output=None, control=None, cut=None):
    """Use argument vectors, never shell interpolation. Capture uses ctl, not a CLI phase."""
    argv = [str(binary), phase]
    if phase == "ctl":
        require(control is not None, "ctl requires the private socket")
        return argv + ["--socket", str(control), "--request", json.dumps(source, separators=(",", ":"))]
    require(phase in ("genesis", "verify", "validator", "drive", "reopen"), "unknown Sui fleet command")
    argv += ["--input" if phase in ("genesis", "verify") else "--capsule", str(source)]
    if phase in ("validator", "drive"):
        require(control is not None, "missing Sui fleet control socket")
        argv += ["--control", str(control)]
    if phase == "reopen":
        require(integer(cut, 0), "missing certified cut")
        argv += ["--cut", str(cut)]
    if phase != "validator":
        require(output is not None, "missing Sui fleet phase output")
        argv += ["--output", str(output)]
    return argv


def common_cut(responses):
    require(set(responses) == set(SLOTS[:4]), "missing validator cut")
    cuts = set()
    for response in responses.values():
        require(response["ok"] is True and response["error"] is None, "wait_cut failed")
        row = response["body"]
        require(row["qualified"] is True and integer(row["cut"], 0)
                and row["epoch_end_checkpoint"] == row["cut"]
                and integer(row["pending_executions"], 0, 0) and isinstance(row["digest"], str) and row["digest"],
                "unqualified wait_cut")
        cuts.add((row["cut"], row["digest"]))
    require(len(cuts) == 1, "validators disagree on the terminal certified cut")
    return next(iter(cuts))

def verify_restart_prefix(before, after):
    """The restarted V3 must catch up to and certify the previously saved prefix."""
    for response in (before, after):
        require(response["ok"] is True and response["error"] is None, "V3 certified_prefix failed")
        row = response["body"]
        require(integer(row["sequence_number"], 0) and integer(row["highest_executed"], row["sequence_number"])
                and integer(row["epoch"], 0, 2) and isinstance(row["digest"], str) and row["digest"],
                "invalid V3 certified_prefix")
    require(all(before["body"][key] == after["body"][key] for key in ("sequence_number", "epoch", "digest")),
            "V3 certified prefix changed across restart")



PRODUCER_ROLES = {"genesis": "genesis", "drive": "driver", "capture": "validator",
                  "reopen": "audit", "verify": "verifier"}
PUBLIC_FILES = {
    "genesis": frozenset({"genesis.blob", "committee.json"}),
    "drive": frozenset({"workload.json"}),
    "capture": frozenset({"dump.json"}),
    "reopen": frozenset({"dump.json"}),
    "verify": frozenset({"workload.json", "pre-stop.json", "reopened.json", "restart-only-keyspaces.json"}),
}


def _process_files(case, index):
    if index is None:
        return {"capsule": "driver/capsule.json", "fullnode_config": "driver/fullnode.yaml"}
    directory = f"validator-{index}"
    result = {"capsule": f"{directory}/capsule.json", "node_config": f"{directory}/node.yaml"}
    if roles(case)[f"validator_V{index}"][1] == "leased":
        result["sharded_wal_config"] = f"{directory}/sharded-wal.json"
    return result


def genesis_process_inputs(verified_receipt, case, index):
    """Get private input digests from a previously verified passing genesis."""
    require(case in MANDATORY[:2] and (index is None or integer(index, 0, 3))
            and verified_receipt["phase"] == "genesis" and verified_receipt["case_id"] == case
            and verified_receipt["status"] == "passed", "wrong genesis for process inputs")
    sealed = {row["path"]: row["sha256"] for row in verified_receipt["artifacts"]}
    return {key: sealed[path] for key, path in _process_files(case, index).items()}


def producer_export_paths(phase, verified_receipt):
    """List archival paths only; genesis capsule distribution is a private operation.

    Archive the validated receipt itself, never recursively upload its directory
    or interpret its list of hashes as a request to upload every listed file.
    """
    require(phase in PUBLIC_FILES and verified_receipt["phase"] == phase, "wrong golden export phase")
    return ("receipt.json", *sorted(PUBLIC_FILES[phase].intersection(
        row["path"] for row in verified_receipt["artifacts"]
    )))


def verify_producer_receipt(receipt, root, *, run, case, phase, validator, input_sha, start_ms, deadline_ms):
    """Check the Rust receipt before attaching an instance ID and GCS generations.

    `validator` and `input_sha` come from admitted build and input artifacts,
    not the receipt being checked. For drive/capture/reopen the expected
    capsule digest comes from genesis. Rust `verify` owns the committee oracle.
    """
    fields(receipt, "format run_id case_id phase role validator inputs started_at_ms ended_at_ms "
                    "status error artifacts details", "golden producer receipt")
    require(case in MANDATORY[:2] and phase in PRODUCER_ROLES
            and receipt["format"] == "sui-golden-fleet-receipt-v1"
            and (receipt["run_id"], receipt["case_id"], receipt["phase"], receipt["role"])
            == (run, case, phase, PRODUCER_ROLES[phase]), "stale golden producer receipt")
    if phase in ("capture", "reopen"):
        fields(validator, "index name", "admitted validator")
        require(integer(validator["index"], 0, 3) and isinstance(validator["name"], str)
                and validator["name"], "validator must be in the four-member committee")
    else:
        require(validator is None, "non-validator phase has committee identity")
    require(receipt["validator"] == validator, "wrong golden producer identity")
    status, error = receipt["status"], receipt["error"]
    require(status in STATUSES and (error is None if status == "passed" else isinstance(error, str) and error),
            "invalid golden producer outcome")
    if phase in ("drive", "capture", "reopen"):
        index = validator["index"] if validator is not None else None
        required_inputs = {"executable", *_process_files(case, index)}
        require(input_sha.keys() == required_inputs, "wrong genesis-sealed process inputs")
    else:
        required_inputs = ({"spec"} if phase == "genesis" else {"index", "committee", "workload"}) | {"executable"}
    if phase == "verify" and status in ("passed", "failed"):
        required_inputs.update(f"{kind}/{index}" for kind in ("pre_stop", "reopened") for index in range(4))
    observed = receipt["inputs"]
    matching = observed == input_sha
    if phase == "verify" and status == "unqualified":
        # The CLI omits absent files when reporting an incomplete arm. Its own
        # executable and the index still have to match the admitted invocation.
        matching = {"index", "executable"} <= observed.keys() <= input_sha.keys() and all(
            value == input_sha[key] for key, value in observed.items()
        )
    require(required_inputs <= input_sha.keys() and matching
            and all(hex_string(value, 64) for value in input_sha.values()), "golden producer input changed")
    require(integer(receipt["started_at_ms"], start_ms, deadline_ms)
            and integer(receipt["ended_at_ms"], receipt["started_at_ms"], deadline_ms),
            "golden producer receipt outside phase deadline")
    require(root.is_absolute() and root.resolve() == root, "invalid golden artifact root")
    seen = set()
    for row in receipt["artifacts"]:
        fields(row, "path bytes sha256", "golden producer artifact")
        path = artifact(root, row["path"], row["sha256"])
        require(path not in seen and integer(row["bytes"], 0) and path.stat().st_size == row["bytes"],
                "duplicate golden artifact or byte count mismatch")
        seen.add(path)
    required_files = PUBLIC_FILES[phase]
    if phase == "genesis":
        required_files = required_files | {
            path for index in (None, 0, 1, 2, 3) for path in _process_files(case, index).values()
        }
        unexpected = {root / f"validator-{index}/sharded-wal.json" for index in range(4)
                      if "sharded_wal_config" not in _process_files(case, index)}
        require(not (seen & unexpected), "native validator has a sharded config")
    require(status != "passed" or {root / name for name in required_files} <= seen,
            "missing golden producer artifact")
    return status
