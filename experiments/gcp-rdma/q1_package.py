"""Q1 package provenance checks; no network or build side effects."""

import hashlib
import json
import re
import tomllib
from pathlib import Path, PurePosixPath

PROFILE = "nvmeof_q1"
SUI_ROLES = {"sui_golden_native": False, "sui_golden_sharded": True}
EXECUTABLES = {*SUI_ROLES, "kv_research"}


def require(condition, message):
    if not condition:
        raise ValueError(f"Q1: {message}")


def hex_string(value, width):
    return isinstance(value, str) and re.fullmatch(f"[a-f0-9]{{{width}}}", value) is not None


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def file_digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def relative_path(value):
    require(isinstance(value, str) and value, "missing relative path")
    path = PurePosixPath(value)
    require(not path.is_absolute() and path.as_posix() == value and value != "."
            and ".." not in path.parts and "\\" not in value and "\0" not in value,
            f"unsafe relative path: {value!r}")
    return path


def artifact(root, name, sha):
    path = root / relative_path(name)
    require(path.is_file() and path.resolve() == path.absolute(), f"invalid artifact path: {name}")
    require(hex_string(sha, 64) and file_digest(path) == sha, f"artifact digest mismatch: {name}")
    return path


def verify_sui_resolution(manifest, lock, metadata, engine_sha, *, leased):
    """Verify original locked metadata before an optional offline source replacement."""
    require(hex_string(engine_sha, 40), "engine SHA must be full and lowercase")
    dep = tomllib.loads(manifest).get("dependencies", {}).get("tidehunter", {})
    require(isinstance(dep, dict) and dep.get("rev") == engine_sha and "path" not in dep
            and isinstance(dep.get("git"), str), "typed-store TideHunter git pin mismatch")
    lock_entries = [p for p in tomllib.loads(lock).get("package", []) if p["name"] == "tidehunter"]
    source = f"git+{dep['git']}?rev={engine_sha}#{engine_sha}"
    require(len(lock_entries) == 1 and lock_entries[0].get("source") == source,
            "Cargo.lock TideHunter source mismatch")
    packages = metadata["packages"]
    engines = [p for p in packages if p["name"] == "tidehunter"]
    stores = [p for p in packages if p["name"] == "typed-store"]
    require(len(engines) == len(stores) == 1 and engines[0].get("source") == source
            and engines[0]["version"] == lock_entries[0]["version"], "locked metadata source mismatch")
    nodes = {n["id"]: n for n in metadata["resolve"]["nodes"]}
    engine_node = nodes.get(engines[0]["id"], {})
    store_node = nodes.get(stores[0]["id"], {})
    engine_features = set(engine_node.get("features", []))
    store_features = set(store_node.get("features", []))
    require(any(d["pkg"] == engines[0]["id"] and d["name"] == "tidehunter"
                for d in store_node.get("deps", [])), "typed-store does not resolve the sealed engine")
    require("tidehunter" in store_features and ("sharded-wal" in store_features) == leased
            and ("experimental-distributed" in engine_features) == leased
            and "experimental-nvmeof" not in engine_features, "Sui resolved feature graph mismatch")
    return {"source": source, "version": engines[0]["version"],
            "typed_store_features": sorted(store_features), "engine_features": sorted(engine_features)}


def verify_package(root: Path, spec):
    """Verify bytes staged under the admitted root against a separately sealed spec."""
    require(root.is_absolute() and root.resolve() == root, "package root must be canonical and absolute")
    roles = spec["executables"]
    require(set(roles) == EXECUTABLES, "incorrect Q1 executable roles")
    require(len({entry["path"] for entry in roles.values()}) == len(roles), "executable role path reused")
    for role, entry in roles.items():
        require(relative_path(entry["path"]).parts[:2] == ("runtime", "bin"), "binary outside runtime/bin")
        path = artifact(root, entry["path"], entry["sha256"])
        require(path.stat().st_mode & 0o111, f"{role}: binary not executable")
        with path.open("rb") as stream:
            header = stream.read(20)
            require(len(header) == 20 and header[:7] == b"\x7fELF\x02\x01\x01"
                    and header[16:20] in (b"\x02\x00\x3e\x00", b"\x03\x00\x3e\x00"),
                    f"{role}: not Linux x86-64 ELF")
    require("q1-build-receipt.json" in spec["inputs"], "unsealed build receipt")
    for name, sha in spec["inputs"].items():
        artifact(root, name, sha)
    receipt = json.loads((root / "q1-build-receipt.json").read_text())
    require(set(receipt) == {"version", "engine_sha", "sui_sha", "abi", "roles"}
            and type(receipt["version"]) is int and receipt["version"] == 1
            and receipt["engine_sha"] == spec["engine_sha"] and receipt["sui_sha"] == spec["sui_sha"]
            and receipt["abi"] == "rocky-linux-x86_64"
            and set(receipt["roles"]) == EXECUTABLES, "invalid Q1 build receipt")
    for role, entry in receipt["roles"].items():
        common = {"sha256", "engine_sha", "offline", "locked"}
        extra = {"metadata", "manifest", "lock", "use_tidehunter"} if role in SUI_ROLES else set()
        require(set(entry) == common | extra and entry["sha256"] == roles[role]["sha256"]
                and entry["engine_sha"] == spec["engine_sha"]
                and entry["offline"] is True and entry["locked"] is True, f"{role}: build receipt mismatch")
        if role in SUI_ROLES:
            require(entry["use_tidehunter"] == "1", f"{role}: USE_TIDEHUNTER must be 1")
            paths = [entry[key] for key in ("manifest", "lock", "metadata")]
            require(all(name in spec["inputs"] for name in paths), f"{role}: unsealed build metadata")
            manifest, lock, metadata = ((root / name).read_text() for name in paths)
            graph = json.loads(metadata)
            leased = SUI_ROLES[role]
            verify_sui_resolution(manifest, lock, graph, spec["engine_sha"], leased=leased)
            tool_ids = [package["id"] for package in graph["packages"] if package["name"] == "sui-golden-fleet"]
            tool_nodes = [node for node in graph["resolve"]["nodes"] if node["id"] in tool_ids]
            features = {"golden", "sharded"} if leased else {"golden"}
            require(len(tool_ids) == len(tool_nodes) == 1 and set(tool_nodes[0]["features"]) == features,
                    f"{role}: wrong golden fleet build features")
