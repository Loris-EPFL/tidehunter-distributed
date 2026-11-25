import copy
import json
import mmap
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from q1_capacity import validate_admission, verify_owner
from q1_dispatch import Scope, run_cases, stable_empty
from q1_golden import genesis_process_inputs, producer_export_paths, topology, verify_producer_receipt
from q1_index import GIB, SuccessDomains, validate_build, validate_cold, validate_stage, validate_timed
from q1_manifest import CAPACITY, MANDATORY, PROOFS, SLOTS, roles, validate_binding, validate_seal
from q1_package import EXECUTABLES, SUI_ROLES, artifact, digest, file_digest, verify_package, verify_sui_resolution
from q1_residency import measure

SHA = "1" * 64
ENGINE = "a" * 40


def fixture():
    seal = {"version": 1, "profile": "nvmeof_q1", "plan_sha": SHA, "engine_sha": ENGINE, "sui_sha": "b" * 40,
            "image_id": 91, "executables": {r: {"path": f"runtime/bin/{r}", "sha256": SHA} for r in EXECUTABLES},
            "inputs": {"q1-workload.json": SHA, "q1-build-receipt.json": SHA}, "proofs": dict.fromkeys(PROOFS, SHA),
            "ci": {"engine_sha": ENGINE, "run": 20, "attempt": 2, "kernel_artifact": "kernel-nvme-tcp-20-2",
                   "workflow_sha": SHA, "kernel_sha": SHA},
            "cases": [{"id": case, "seconds": 300, "inputs_sha": SHA} for case in MANDATORY],
            "exact": "E4", "ceiling_cents": 9668, "artifact_bytes": 1000}
    placement = {"version": 1, "seal_sha": digest(seal), "run_id": "q-fleet", "earliest_creation": 100,
                 "slots": {slot: {"index": i, "id": 200 + i, "ordinary_ip": f"10.0.1.{i+1}",
                                    "rdma_ip": f"10.0.2.{i+1}"} for i, slot in enumerate(SLOTS)},
                 "case_configs": {case: {"genesis_sha": SHA if case in MANDATORY[:2] else None,
                                          "roles": {r: {"config_sha": SHA,
                                                        "certificate_sha": digest(r) if b == "leased" else None,
                                                        "validator_id": digest(r) if r.startswith("validator_") else None}
                                                    for r, (_, b) in roles(case).items()}} for case in MANDATORY}}
    return seal, placement


class Fleet(unittest.TestCase):
    def setUp(self):
        self.seal, self.placement = fixture()
        self.scope = Scope(self.seal, digest(self.seal), self.placement, digest(self.placement))

    def test_duplicate_and_misplaced_ids(self):
        for key, value in (("id", 200), ("index", 3), ("ordinary_ip", "127.1.2.3")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                binding = copy.deepcopy(self.placement)
                binding["slots"]["O0"][key] = value
                validate_binding(self.seal, digest(self.seal), binding)

    def test_partial_capacity_refused(self):
        self.seal["cases"].append({"id": CAPACITY, "seconds": 100, "inputs_sha": SHA})
        with self.assertRaisesRegex(ValueError, "proof"):
            validate_seal(self.seal)

    def test_same_attempt(self):
        self.seal["ci"]["attempt"] = 3
        with self.assertRaisesRegex(ValueError, "same-attempt"):
            validate_seal(self.seal)

    def test_request_scope(self):
        request = self.scope.request(MANDATORY[0], "prepare", "validator_V3", "start", 150)
        self.assertTrue(self.scope.validate_request(request, 203, 120).startswith("th-q1-"))
        with self.assertRaisesRegex(ValueError, "deadline"):
            self.scope.validate_request(request, 203, 150)
        with self.assertRaisesRegex(ValueError, "scope"):
            self.scope.validate_request(request, 204, 120)
        with self.assertRaisesRegex(ValueError, "scope"):
            self.scope.validate_request({**request, "input_sha": {}}, 203, 120)

    def test_uncertain_start_still_cleaned(self):
        traffic, saved = [], {}

        def execute(request):
            traffic.append(request)
            if request["verb"] == "start":
                raise TimeoutError("reply lost")
            return {k: v for k, v in request.items() if k not in {"verb", "deadline"}} | {
                "started": 100, "ended": 101, "status": "passed", "artifacts": []}

        def body(case, send):
            send("prepare", "validator_V3", "start")

        with self.assertRaisesRegex(TimeoutError, "reply lost"):
            run_cases(self.scope, lambda: 100, execute, body, saved.__setitem__, lambda case: ())
        self.assertEqual([(r["verb"], r["instance_id"]) for r in traffic], [("start", 203), ("stop", 203)])
        self.assertEqual(saved["q1-terminal"]["outcomes"], dict.fromkeys(MANDATORY, "aborted"))

    def test_missing_upload_and_unsafe_path(self):
        request = self.scope.request(MANDATORY[0], "collect", "validator_V3", "collect", 150)
        receipt = {k: v for k, v in request.items() if k not in {"verb", "deadline"}} | {
            "started": 110, "ended": 115, "status": "passed", "artifacts": []}
        with self.assertRaisesRegex(ValueError, "missing contributor"):
            self.scope.validate_receipt(request, receipt)
        receipt["artifacts"] = [{"path": "../secrets", "sha256": SHA, "generation": 1, "bytes": 1}]
        with self.assertRaisesRegex(ValueError, "unsafe"):
            self.scope.validate_receipt(request, receipt)

    def test_stable_inventory(self):
        snapshots = iter([(), None, (), ()])
        stable_empty(lambda: next(snapshots))
        with self.assertRaisesRegex(ValueError, "residue"):
            stable_empty(lambda: ("owned-nqn",))

    def test_topology_matches_roles(self):
        mixed = topology(self.placement, MANDATORY[0], 10)
        self.assertEqual(set(mixed["owners"]), {f"owner_V{i}_O{j}" for i in range(2, 4) for j in range(2)})
        self.assertNotIn("SUI_SHARDED_WAL_CONFIG", mixed["validators"]["V0"]["env"])
        v3 = mixed["validators"]["V3"]
        self.assertEqual(v3["host"], "10.0.1.4")
        self.assertEqual(v3["env"]["SUI_SHARDED_WAL_SCHEMA_OUTPUT"], v3["root"] + "/golden-schema")
        self.assertEqual(v3["env"]["SUI_SHARDED_WAL_CONFIG"], str(Path(v3["capsule"]).with_name("sharded-wal.json")))
        self.assertGreaterEqual(mixed["driver"]["port_base"], mixed["validators"]["V0"]["port_base"] + 9)
        all_leased = topology(self.placement, MANDATORY[1], 10)
        for j in range(2):
            ports = []
            for role, row in all_leased["owners"].items():
                if role.endswith(f"O{j}"):
                    for prefix in ("public", "commissioning"):
                        base = row["live_target_pool"][f"{prefix}_port_base"]
                        ports.extend(range(base, base + row["max_databases"]))
            self.assertEqual(len(ports), len(set(ports)))


class Package(unittest.TestCase):
    def test_package_binds_receipt_and_role_paths(self):
        spec, _ = fixture()
        source = f"git+https://example.invalid/th?rev={ENGINE}#{ENGINE}"
        with TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            (root / "runtime/bin").mkdir(parents=True)
            spec["inputs"] = {}

            def seal_input(name, content):
                path = root / name
                path.write_text(content)
                spec["inputs"][name] = file_digest(path)

            seal_input("Cargo.toml", f'[dependencies.tidehunter]\ngit = "https://example.invalid/th"\nrev = "{ENGINE}"')
            seal_input("Cargo.lock", f'[[package]]\nname = "tidehunter"\nversion = "1"\nsource = "{source}"')
            receipt = {"version": 1, "engine_sha": ENGINE, "sui_sha": spec["sui_sha"],
                       "abi": "rocky-linux-x86_64", "roles": {}}
            for role, entry in spec["executables"].items():
                path = root / entry["path"]
                path.write_bytes(bytes.fromhex("7f454c4602010100000000000000000003003e00"))
                path.chmod(0o700)
                entry["sha256"] = file_digest(path)
                row = receipt["roles"][role] = {"engine_sha": ENGINE, "sha256": entry["sha256"],
                                               "offline": True, "locked": True}
                if role in SUI_ROLES:
                    name = f"{role}.json"
                    leased = SUI_ROLES[role]
                    seal_input(name, json.dumps({
                        "packages": [{"name": "typed-store", "id": "store"},
                                     {"name": "tidehunter", "id": "th", "source": source, "version": "1"},
                                     {"name": "sui-golden-fleet", "id": "tool"}],
                        "resolve": {"nodes": [{"id": "store", "features": ["tidehunter"] + (
                            ["sharded-wal"] if leased else []), "deps": [{"name": "tidehunter", "pkg": "th"}]},
                            {"id": "th", "features": ["experimental-distributed"] if leased else []},
                            {"id": "tool", "features": ["golden", "sharded"] if leased else ["golden"]}]},
                    }))
                    row.update(manifest="Cargo.toml", lock="Cargo.lock", metadata=name, use_tidehunter="1")
            name = "q1-build-receipt.json"
            seal_input(name, json.dumps(receipt))
            verify_package(root, spec)
            for role, features in (
                ("sui_golden_native", ["golden", "sharded"]),
                ("sui_golden_sharded", ["golden", "sharded", "local-live-fixture"]),
                ("sui_golden_native", None),
            ):
                metadata = receipt["roles"][role]["metadata"]
                original = (root / metadata).read_text()
                graph = json.loads(original)
                nodes = graph["resolve"]["nodes"]
                tool = next(node for node in nodes if node["id"] == "tool")
                if features is None:
                    nodes.remove(tool)
                else:
                    tool["features"] = features
                seal_input(metadata, json.dumps(graph))
                with self.subTest(role=role, features=features), self.assertRaisesRegex(
                    ValueError, "wrong golden fleet build features"
                ):
                    verify_package(root, spec)
                seal_input(metadata, original)
            sha = spec["inputs"].pop(name)
            with self.assertRaisesRegex(ValueError, "unsealed build receipt"):
                verify_package(root, spec)
            spec["inputs"][name] = sha
            (root / name).write_text("{}")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                verify_package(root, spec)
            receipt["engine_sha"] = "c" * 40
            seal_input(name, json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "invalid Q1 build receipt"):
                verify_package(root, spec)
            spec["executables"]["kv_research"]["path"] = "other/kv"
            with self.assertRaisesRegex(ValueError, "outside runtime/bin"):
                verify_package(root, spec)

    def test_artifact_bytes_are_bound_and_symlinks_rejected(self):
        with TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            path = root / "workload"
            path.write_bytes(b"sealed bytes")
            sha = file_digest(path)
            self.assertEqual(artifact(root, "workload", sha), path)
            (root / "link").symlink_to(path)
            with self.assertRaisesRegex(ValueError, "invalid artifact path"):
                artifact(root, "link", sha)
            path.write_bytes(b"changed bytes")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                artifact(root, "workload", sha)

    def test_q1_is_not_a_kv_matrix(self):
        import manual_campaign

        with self.assertRaisesRegex(ValueError, "offline-only"):
            manual_campaign._reviewed_jobs({"selection": "nvmeof_q1"})

    def test_exact_pin_lock_and_metadata(self):
        url = "https://example.invalid/tidehunter"
        source = f"git+{url}?rev={ENGINE}#{ENGINE}"
        manifest = f'[dependencies.tidehunter]\ngit = "{url}"\nrev = "{ENGINE}"\n'
        lock = f'[[package]]\nname = "tidehunter"\nversion = "1.0.0"\nsource = "{source}"\n'
        metadata = {"packages": [{"name": "typed-store", "id": "store"},
                                 {"name": "tidehunter", "id": "engine", "version": "1.0.0", "source": source}],
                    "resolve": {"nodes": [{"id": "store", "features": ["tidehunter", "sharded-wal"],
                                           "deps": [{"name": "tidehunter", "pkg": "engine"}]},
                                          {"id": "engine", "features": ["experimental-distributed"]}]}}
        self.assertEqual(verify_sui_resolution(manifest, lock, metadata, ENGINE, leased=True)["source"], source)
        for bad in (manifest.replace(ENGINE, "9076518"), manifest + 'path = "engine"\n'):
            with self.assertRaisesRegex(ValueError, "pin mismatch"):
                verify_sui_resolution(bad, lock, metadata, ENGINE, leased=True)
        with self.assertRaisesRegex(ValueError, "Cargo.lock"):
            verify_sui_resolution(manifest, lock.replace(ENGINE, "b" * 40), metadata, ENGINE, leased=True)
        with self.assertRaisesRegex(ValueError, "feature graph"):
            verify_sui_resolution(manifest, lock, metadata, ENGINE, leased=False)
        metadata["packages"][1]["source"] = None
        with self.assertRaisesRegex(ValueError, "metadata source"):
            verify_sui_resolution(manifest, lock, metadata, ENGINE, leased=True)


class Index(unittest.TestCase):
    def setUp(self):
        self.database_id = "a" * 32
        self.stage = {"version": 1, "owner_roots": [SHA, "2" * 64], "tier": "native",
                      "exact": "E4", "keys": 10, "minimum_puts": 11, "max_puts": 30, "queue": 4,
                      "read_cache": False, "rss_bytes": 64 * GIB, "cgroup_bytes": 96 * GIB,
                      "owner_allocation_bytes": [10, 10]}

    def test_build_and_oracle(self):
        validate_stage(self.stage, fixture=True)
        with self.assertRaises(ValueError):
            validate_stage(self.stage)
        receipt = {"database_id": self.database_id, "owner_roots": self.stage["owner_roots"],
            "distinct_rows": 10, "successful": 10, "normally_adopted": True, "persistence_covered": True,
            "clean_close": True, "iterated_rows": 10, "normal_generations": 1}
        validate_build(self.stage, receipt, self.database_id)
        with self.assertRaisesRegex(ValueError, "build receipt"):
            validate_build(self.stage, {**receipt, "distinct_rows": 9}, self.database_id)
        oracle = SuccessDomains(10)
        oracle.ack(1, "overwrite")
        oracle.ack(1, "pilot")
        oracle.ack(9, "pilot")
        self.assertEqual([oracle.domain(i) for i in (0, 1, 9)], ["build", "overwrite", "pilot"])

    def test_drain_checkpoint_does_not_qualify(self):
        generations = [{"database_id": self.database_id, "fencing_generation": 7, "generation": i+1,
                        "kind": "normal", "trigger_names": [], "adopted_ns": i+1, "installed_ns": i+2, "credited_ns": i+3}
                       for i in range(20)]
        proof = {"database_id": self.database_id, "start_ns": 1, "end_ns": 3_000_000_001,
                 "offered": 15, "accepted": 15, "rejected": 0, "unknown": 0, "wrong": 0, "errors": 0,
                 "lost_generations": 0, "normal_generations_in_window": 20, "missing_seconds": [],
                 "generations": generations, "rss_peak": 4, "cgroup_peak": 5, "cap_events": 0,
                 "owner_allocation_peaks": None, "seconds": [{"second": i, "bytes": 1, "batches": 1, "age_ns": 1}
                                                            for i in range(3)]}
        validate_timed(self.stage, proof, self.database_id, [5, 6])
        generations[-1]["kind"] = "requested"
        with self.assertRaisesRegex(ValueError, "20 eligible"):
            validate_timed(self.stage, proof, self.database_id, [5, 6])

    def test_cold_without_warm_fallback(self):
        frontier = {"fencing_generation": 7, "sequence": 13, "operation_index": 0}
        common = {"frontier": frontier, "suffix_batches": 1, "suffix_bytes": 4096, "touched_key": 2}
        recovery = {**common, "stop_frontier": frontier, "open_to_read_ns": 1, "first_correct": True,
                    "discarded": 0, "warmup": False, "first_block_resident": False,
                    "frontier_matches": True, "tail_matches": True, "cold": True, "fresh_owner_processes": True,
                    "same_database": True, "fencing_generation_advanced": True, "close_error": None,
                    "owner_stop_error": None, "cold_unqualified_reason": None,
                    "index_pages": {"selected": 100, "resident": 0}, "wal_pages": {"selected": 99, "resident": 0}}
        validate_cold(self.stage, {**common, "forced_root": False}, recovery)
        with self.assertRaisesRegex(ValueError, "first read"):
            validate_cold(self.stage, {**common, "forced_root": False}, {**recovery, "first_correct": False})
        recovery["index_pages"]["resident"] = 1  # Even 1% of the superset might include every replay page.
        with self.assertRaisesRegex(ValueError, "residency"):
            validate_cold(self.stage, {**common, "forced_root": False}, recovery)

    def test_null_cold_residency_preserves_unqualified_reason(self):
        receipt = {"index_pages": None, "wal_pages": {"selected": 99, "resident": 0}, "cold": False,
                   "cold_unqualified_reason": "a resident page remained in the measured superset"}
        with self.assertRaisesRegex(ValueError, "resident page remained"):
            validate_cold(self.stage, {}, receipt)


class Golden(unittest.TestCase):
    def test_genesis_export_does_not_include_private_capsules_or_configs(self):
        paths = ("genesis.blob", "committee.json", "driver/fullnode.yaml", "driver/capsule.json",
                 "validator-0/node.yaml", "validator-0/capsule.json", "validator-3/sharded-wal.json")
        receipt = {"phase": "genesis", "artifacts": [{"path": path} for path in paths]}
        self.assertEqual(producer_export_paths("genesis", receipt), ("receipt.json", "committee.json", "genesis.blob"))

    def test_capture_receipt_must_match_admitted_identity_and_hashes(self):
        with TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            dump = root / "dump.json"
            dump.write_bytes(b"{}")
            validator = {"index": 3, "name": "protocol-key"}
            configs = {"validator-3/capsule.json": SHA, "validator-3/node.yaml": "2" * 64,
                       "validator-3/sharded-wal.json": "3" * 64, "driver/capsule.json": SHA,
                       "driver/fullnode.yaml": "4" * 64, "validator-0/capsule.json": SHA,
                       "validator-0/node.yaml": "5" * 64}
            genesis = {"phase": "genesis", "status": "passed", "case_id": MANDATORY[0],
                       "artifacts": [{"path": path, "sha256": sha} for path, sha in configs.items()]}
            inputs = {"executable": SHA, **genesis_process_inputs(genesis, MANDATORY[0], 3)}
            self.assertEqual(inputs["sharded_wal_config"], "3" * 64)
            self.assertEqual(genesis_process_inputs(genesis, MANDATORY[0], None)["fullnode_config"], "4" * 64)
            native = genesis_process_inputs(genesis, MANDATORY[0], 0)
            self.assertNotIn("sharded_wal_config", native)
            receipt = {"format": "sui-golden-fleet-receipt-v1", "run_id": "q-fleet", "case_id": MANDATORY[0],
                       "phase": "capture", "role": "validator", "validator": validator.copy(),
                       "inputs": inputs.copy(), "started_at_ms": 50, "ended_at_ms": 100, "status": "passed",
                       "error": None, "artifacts": [{"path": "dump.json", "bytes": 2, "sha256": file_digest(dump)}],
                       "details": {}}
            expected = {"run": "q-fleet", "case": MANDATORY[0], "phase": "capture", "validator": validator,
                        "input_sha": inputs, "start_ms": 50, "deadline_ms": 100}
            self.assertEqual(verify_producer_receipt(receipt, root, **expected), "passed")
            no_capsule = {k: v for k, v in inputs.items() if k != "capsule"}
            with self.assertRaisesRegex(ValueError, "wrong genesis-sealed process inputs"):
                verify_producer_receipt({**receipt, "inputs": no_capsule}, root,
                                        **{**expected, "input_sha": no_capsule})
            for key in ("node_config", "sharded_wal_config"):
                with self.subTest(key=key), self.assertRaisesRegex(ValueError, "producer input changed"):
                    verify_producer_receipt({**receipt, "inputs": {**inputs, key: "9" * 64}}, root, **expected)
            native_validator = {"index": 0, "name": "native"}
            with self.assertRaisesRegex(ValueError, "wrong genesis-sealed process inputs"):
                verify_producer_receipt({**receipt, "validator": native_validator, "inputs": inputs}, root,
                                        **{**expected, "validator": native_validator})
            for status in ("failed", "unqualified", "aborted"):
                outcome = {**receipt, "status": status, "error": f"reason for {status}"}
                with self.subTest(status=status):
                    self.assertEqual(verify_producer_receipt(outcome, root, **expected), status)
            for key, value in (("validator", {"index": 4, "name": "driver"}), ("run_id", "other"),
                               ("status", "unqualified"), ("inputs", {"executable": SHA}),
                               ("ended_at_ms", 101), ("artifacts", [])):
                with self.subTest(key=key), self.assertRaises(ValueError):
                    verify_producer_receipt({**receipt, key: value}, root, **expected)
            verifier_inputs = {"executable": SHA, "index": SHA, "workload": SHA}
            verifier = {**receipt, "phase": "verify", "role": "verifier", "validator": None,
                        "status": "unqualified", "error": "wrong roster", "artifacts": []}
            verifier_args = {**expected, "phase": "verify", "validator": None, "input_sha": verifier_inputs}
            with self.assertRaisesRegex(ValueError, "producer input changed"):
                verify_producer_receipt({**verifier, "inputs": verifier_inputs}, root,
                                        **verifier_args)
            verifier_inputs["committee"] = SHA
            self.assertEqual(verify_producer_receipt({**verifier, "inputs": verifier_inputs}, root, **verifier_args),
                             "unqualified")
            dump.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                verify_producer_receipt(receipt, root, **expected)


class Capacity(unittest.TestCase):
    def test_retirement_requires_pin_release_and_kernel_drain(self):
        limits = {"reserve": 200, "low": 300, "high": 500, "usable": 1000, "write_bytes": 2000, "cpu_ns": 100}
        admission = {"retention_sha": SHA, "trigger": "space_uncertainty", "policy": "dead_only",
                     "r2_proof": None, "tiers": ["native"], "cycles": 3, "deadline_seconds": 100,
                     "owners": {"O0": limits, "O1": limits}}
        validate_admission(admission)
        row = {"min_free": 250, "final_free": 800, "written_bytes": 1000, "cpu_ns": 1,
               "roll_ids": [17, 18, 19], "held": 4, "release": 12, "drain": 13, "target_removed": 14,
               "retire": 15, "lag_start": 7, "resumed": 8, "generation_before": 3, "generation_after": 3,
               "reclaimed_blocks": 1, "ledger_bytes": 512, "fs_bytes": 512, "post_release_backlog": 0,
               "read_after_retire": 0, "dma_after_retire": 0}
        verify_owner(admission, "O0", row, reclaim=True)
        for key, value in (("drain", 11), ("roll_ids", [1, 2]), ("ledger_bytes", 511), ("dma_after_retire", 1)):
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify_owner(admission, "O0", {**row, key: value}, reclaim=True)


class Residency(unittest.TestCase):
    def test_probes_file_without_mutating_it(self):
        with TemporaryDirectory(dir=Path.cwd()) as temp:
            root = Path(temp)
            payload = b"x" * mmap.PAGESIZE
            with (root / "selected").open("wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            self.assertEqual(measure(root, "selected", 0, mmap.PAGESIZE), {"selected": 1, "resident": 1})
            proof = measure(root, "selected", 0, mmap.PAGESIZE, evict=True)
            self.assertEqual(proof["selected"], 1)
            self.assertEqual((root / "selected").read_bytes(), payload)
            with self.assertRaisesRegex(ValueError, "unsafe"):
                measure(root, "../other", 0, mmap.PAGESIZE)


if __name__ == "__main__":
    unittest.main()
