"""Validate the frozen logical Q1 seal and its separate physical binding."""

import ipaddress
import re

from q1_package import EXECUTABLES, PROFILE, digest, hex_string, relative_path, require

SLOTS = ("V0", "V1", "V2", "V3", "O0", "O1")
MANDATORY = ("golden_mixed", "golden_leased", "index_native", "index_synchronous")
COMPARISON = ("put_native", "put_synchronous", "replay_native", "replay_synchronous")
CAPACITY = "capacity_r3"
# Proof families gated on the case families actually sealed: a comparison-only
# seal carries no index or golden proofs (those fleets moved out of Q1).
INDEX_PROOFS = {"a0", "a1", "index_feasibility"}
GOLDEN_PROOFS = {"native_golden", "local_golden", "split_process", "sync_tier"}
STATUSES = {"passed", "failed", "unqualified", "aborted"}
PROOFS = {"sync_tier", "fencing", "lifecycle", "range_drop", "same_generation_retire",
          "native_clean_tail", "compactor", "native_golden", "op_replay", "local_golden",
          "split_process", "a0", "a1", "index_feasibility", "package_abi", "timing"}


def fields(value, names, context):
    require(isinstance(value, dict) and set(value) == set(names.split()), f"invalid {context} fields")


def integer(value, minimum=1, maximum=None):
    return type(value) is int and minimum <= value and (maximum is None or value <= maximum)


def roles(case):
    if case in MANDATORY[:2]:
        backends = ("native", "native", "leased", "leased") if case == MANDATORY[0] else ("leased",) * 4
        result = {f"validator_{slot}": (slot, backend) for slot, backend in zip(SLOTS[:4], backends)}
        result["driver"] = ("V0", "native")
        for slot, backend in zip(SLOTS[:4], backends):
            if backend == "leased":
                for owner in SLOTS[4:]:
                    result[f"owner_{slot}_{owner}"] = (owner, "leased")
        return result
    if case in COMPARISON:
        # every point is a native/leased pair: the leased arm runs from the
        # coordinator VM with two owners; the native arm is a single node on
        # the native host VM's local SSD (slot V1 in this fleet).
        return {"coordinator": ("V0", "leased"), "owner_O0": ("O0", "leased"),
                "owner_O1": ("O1", "leased"), "native_host": ("V1", "native")}
    require(case in (*MANDATORY[2:], CAPACITY), "unknown case")
    return {"coordinator": ("V0", "leased"), "owner_O0": ("O0", "leased"), "owner_O1": ("O1", "leased")}


def validate_seal(spec):
    fields(spec, "version profile plan_sha engine_sha sui_sha image_id executables inputs proofs ci cases exact "
                 "ceiling_cents artifact_bytes", "seal")
    require(type(spec["version"]) is int and spec["version"] == 1 and spec["profile"] == PROFILE,
            "wrong seal version or profile")
    for name in ("engine_sha", "sui_sha", "plan_sha"):
        require(hex_string(spec[name], 64 if name == "plan_sha" else 40), f"invalid {name}")
    require(integer(spec["image_id"]) and spec["ceiling_cents"] == 9668
            and integer(spec["artifact_bytes"], maximum=16_000_000_000), "wrong image or budget")
    has_index_cases = any(case["id"] in MANDATORY[2:] for case in spec["cases"])
    require((spec["exact"] in ("E4", "E16")) if has_index_cases else spec["exact"] is None,
            "A1 must nominate Exact for both tiers (and only when index cases are sealed)")
    cases = spec["cases"]
    ids = tuple(c["id"] for c in cases)
    comparison = any(case in COMPARISON for case in ids)
    require(ids in (MANDATORY, (*MANDATORY, CAPACITY), (*MANDATORY, *COMPARISON),
                    (*MANDATORY, *COMPARISON, CAPACITY), COMPARISON),
            "incorrect or partial Q1 matrix")
    require(not comparison or tuple(case for case in ids if case in COMPARISON) == COMPARISON,
            "comparison cases must form the fixed four-case set in order")
    optional = CAPACITY in ids
    has_index = any(case in MANDATORY[2:] for case in ids)
    has_golden = any(case in MANDATORY[:2] for case in ids)
    expected_proofs = PROOFS \
        | ({"capacity_admission", "capacity_local"} if optional else set()) \
        | ({"comparison_admission", "comparison_local"} if comparison else set())
    if not has_index:
        expected_proofs -= INDEX_PROOFS
    if not has_golden:
        expected_proofs -= GOLDEN_PROOFS
    require(set(spec["proofs"]) == expected_proofs and all(hex_string(v, 64) for v in spec["proofs"].values()),
            "missing or invalid local proof digests")
    ci = spec["ci"]
    fields(ci, "engine_sha run attempt kernel_artifact workflow_sha kernel_sha", "CI proof")
    require(ci["engine_sha"] == spec["engine_sha"] and integer(ci["run"]) and integer(ci["attempt"]),
            "kernel CI proof revision or attempt mismatch")
    require(ci["kernel_artifact"] == f"kernel-nvme-tcp-{ci['run']}-{ci['attempt']}"
            and all(hex_string(ci[k], 64) for k in ("workflow_sha", "kernel_sha")),
            "CI proof must bind workflow and same-attempt kernel artifact")
    require(set(spec["executables"]) == EXECUTABLES, "missing executable role")
    for entry in spec["executables"].values():
        fields(entry, "path sha256", "executable")
        relative_path(entry["path"])
        require(hex_string(entry["sha256"], 64), "invalid executable digest")
    require({"q1-workload.json", "q1-build-receipt.json"} <= spec["inputs"].keys(), "missing frozen inputs")
    for path, sha in spec["inputs"].items():
        relative_path(path)
        require(hex_string(sha, 64), "invalid frozen input digest")
    total = 3300
    for case in cases:
        fields(case, "id seconds inputs_sha", "case")
        require(integer(case["seconds"], maximum=600 if case["id"] in MANDATORY[:2]
                        else 3400 if case["id"] in COMPARISON else 9000)
                and hex_string(case["inputs_sha"], 64), "invalid case budget or digest")
        total += case["seconds"]
    require(total <= 12300, "fleet time exceeds the 12,300 s envelope")
    return digest(spec)


def validate_binding(spec, seal_sha, binding):
    require(validate_seal(spec) == seal_sha, "seal bytes changed")
    fields(binding, "version seal_sha run_id earliest_creation slots case_configs", "placement binding")
    require(type(binding["version"]) is int and binding["version"] == 1 and binding["seal_sha"] == seal_sha
            and isinstance(binding["run_id"], str) and re.fullmatch(r"[a-z][a-z0-9-]{0,39}", binding["run_id"])
            and integer(binding["earliest_creation"]), "invalid binding identity")
    slots = binding["slots"]
    require(set(slots) == set(SLOTS), "placement must bind six logical slots")
    ids, ips = set(), set()
    for slot in SLOTS:
        host = slots[slot]
        fields(host, "index id ordinary_ip rdma_ip", "slot")
        require(integer(host["index"], 0) and host["index"] == SLOTS.index(slot)
                and integer(host["id"]) and host["id"] not in ids, "misplaced or duplicate host")
        ids.add(host["id"])
        for key in ("ordinary_ip", "rdma_ip"):
            ip = ipaddress.IPv4Address(host[key])
            require(str(ip) == host[key] and any(ip in network for network in PRIVATE_NETWORKS)
                    and ip not in ips, "non-private or duplicate NIC address")
            ips.add(ip)
    configs = binding["case_configs"]
    require(set(configs) == {c["id"] for c in spec["cases"]}, "placement case matrix mismatch")
    for case, config in configs.items():
        fields(config, "genesis_sha roles", "case config")
        require(hex_string(config["genesis_sha"], 64) if case in MANDATORY[:2]
                else config["genesis_sha"] is None, "incorrect genesis digest")
        require(set(config["roles"]) == set(roles(case)), "missing or additional workload role")
        validators, certificates = set(), set()
        for role, entry in config["roles"].items():
            fields(entry, "config_sha certificate_sha validator_id", "role config")
            require(hex_string(entry["config_sha"], 64), "invalid config digest")
            _, backend = roles(case)[role]
            cert = entry["certificate_sha"]
            require((hex_string(cert, 64) and cert not in certificates) if backend == "leased"
                    else cert is None, "incorrect or reused mTLS identity")
            if cert is not None:
                certificates.add(cert)
            identity = entry["validator_id"]
            require((hex_string(identity, 64) and identity not in validators) if role.startswith("validator_")
                    else identity is None, "incorrect or reused validator identity")
            if identity is not None:
                validators.add(identity)
    return digest(binding)


PRIVATE_NETWORKS = tuple(map(ipaddress.IPv4Network, ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")))
