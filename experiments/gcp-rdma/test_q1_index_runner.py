"""Receipt-v1 fixtures from index_fixture/q1.rs, including separate owner observations."""

from copy import deepcopy
import unittest

from q1_index import FLEET_KEYS, FLEET_MINIMUM_PUTS, GIB
from q1_index_runner import PHASES, collect_index_stages, stage_command, stage_document
from q1_package import file_digest
from test_q1_producers import temporary_directory, write_json

BINARY = "9" * 64
DB = "12" * 16


class Q1IndexRunner(unittest.TestCase):
    def setUp(self):
        self.root = temporary_directory(self)
        index = {"version": 1, "owner_roots": ["1" * 64, "2" * 64], "tier": "native", "exact": "E4",
                 "keys": FLEET_KEYS, "minimum_puts": FLEET_MINIMUM_PUTS, "max_puts": FLEET_MINIMUM_PUTS,
                 "queue": 16, "read_cache": False, "rss_bytes": 64 * GIB, "cgroup_bytes": 96 * GIB,
                 "owner_allocation_bytes": [100_000, 100_000]}
        owners = [{"partition": i, "address": f"10.0.0.{10+i}:9400", "server_name": f"owner-{i}",
                   "root": f"/mnt/owner-{i}"} for i in range(2)]
        remote = {"storage": {"partitions": [0, 1], "max_epochs": 16}, "read_cache_bytes": 0,
                  "tls": {"transport": {"kind": "tcp"}, "cert": "/tls/cert", "ca": "/tls/ca", "key": "/tls/key"},
                  "endpoints": [{k: v for k, v in owner.items() if k != "root"} for owner in owners],
                  "replay_limits": {"max_batches": 4096, "max_encoded_bytes": 256 << 20},
                  "live_nvme": {"export_mode": "per_owner", "leased_durability": "native"}}
        self.doc = stage_document(index, case_id="index_native", case_root="/mnt/index", owners=owners, remote=remote,
            dataset={"key_namespace_hex": "0" * 32, "seed": 3, "value_size": 1024, "entropy": 1.0},
            workload={"clients": 8, "request_timeout_ms": 1000, "deadline_seconds": 30, "shutdown_grace_seconds": 10,
                      "pilot": {"operations": 1000, "confirm_seconds": 9, "skip_seconds": 0, "min_complete_seconds": 1},
                      "overwrite_operations": FLEET_MINIMUM_PUTS})
        self.stage = self.root / "stage.json"
        self.input_sha = write_json(self.stage, self.doc)
        self.phase_dirs = {phase: self.root / phase for phase in PHASES}
        self.starts, self.observes, self.shutdowns = {}, {}, {}
        self.terminals = self.build_terminals()

    @staticmethod
    def measurement(n):
        return {"offered": n, "accepted": n, "errors": 0, "wrong": 0, "rejected": 0, "unknown": 0}

    def envelope(self, phase, terminal, *, owners=None, artifacts=None, predecessors=None):
        return {"schema_version": 1, "kind": "q1_index_stage_receipt", "phase": phase, "mode": "fleet",
                "case_id": self.doc["case_id"], "input_sha256": self.input_sha, "executable_sha256": BINARY,
                "predecessors": predecessors or [], "database_id": None if phase == "owner-observe" else DB,
                "owner_roots": self.doc["index"]["owner_roots"], "owners": owners, "status": "complete", "error": None,
                "terminal": terminal, "artifacts": artifacts or [], "recorded_unix_ns": 1000}

    def build_terminals(self):
        build = {"database_id": DB, "owner_roots": self.doc["index"]["owner_roots"], "distinct_rows": FLEET_KEYS,
                 "successful": FLEET_KEYS, "iterated_rows": FLEET_KEYS, "normally_adopted": True,
                 "persistence_covered": True, "clean_close": True, "normal_generations": 1}
        pilot = {"database_id": DB, "counts_toward_qualification": False, "selected_rate": 70,
                 "selected_attempt": "primary", "fallback_used": False, "closed_loop": {
                     "qualified_rate": 100, "measurement": self.measurement(1000) | {
                         "median_puts_per_s": 100.0, "complete_seconds": [100], "mean_in_flight": 5.0}},
                 "confirmations": [{"attempt": "primary", "rate": 70, "stable": True,
                                    "measurement": self.measurement(630), "debt": {"growing": False}, "seconds": [0, 1, 2, 3, 4, 5, 6, 7]}]}
        frontier = {"fencing_generation": 6, "sequence": 800, "operation_index": 0}
        stop = {"frontier": frontier, "touched_key": 3, "touched_count": 1, "forced_root": False,
                "suffix_batches": 12, "suffix_bytes": 4096,
                "owner_progress": [{"partition": i, "stats": {"durable_end": 100}} for i in range(2)]}
        seconds = [{"second": i, "age_ns": 1, "bytes": 1, "batches": 1} for i in range(3)]
        timed = self.measurement(FLEET_MINIMUM_PUTS) | {"database_id": DB, "rate": 70,
            "start_ns": 1, "end_ns": 3_000_000_001, "lost_generations": 0,
            "generations": [{"database_id": DB, "fencing_generation": 6, "generation": i+1,
                             "kind": "normal", "trigger_names": ["soft_suffix"], "adopted_ns": i+1,
                             "installed_ns": i+2, "credited_ns": i+3} for i in range(20)],
            "normal_generations_in_window": 20, "rss_peak": 1000, "cgroup_peak": 2000, "cap_events": 0,
            "owner_allocation_peaks": None, "seconds": seconds, "missing_seconds": [], "stop": stop,
            "debt": {"growing": False, "soft_bytes": 64 << 20, "medians_by_third": {k: [1, 1, 1] for k in
                      ("age_ns", "bytes", "batches")}}}
        cold = {"database_id": DB, "frontier": frontier, "stop_frontier": frontier, "frontier_matches": True,
                "tail_matches": True, "first_correct": True, "cold": True, "warmup": False,
                "suffix_batches": 12, "suffix_bytes": 4096, "touched_key": 3, "open_to_read_ns": 20, "discarded": 0,
                "index_pages": {"selected": 100, "resident": 0}, "wal_pages": {"selected": 100, "resident": 0},
                "cold_unqualified_reason": None,
                "first_block_resident": False, "fresh_owner_processes": True, "same_database": True,
                "fencing_generation_advanced": True, "close_error": None, "owner_stop_error": None, "rows": None,
                "residency": {"owner_observations": []}}
        verify = {"database_id": DB, "rows": FLEET_KEYS, "correct": FLEET_KEYS, "iterated_rows": FLEET_KEYS,
                  "ambiguous_accepted": 0, "missing": 0, "wrong": 0, "extra_rows": 0, "close_error": None,
                  "owner_stop_error": None, "domains": {"pilot": 1, "pilot_uncertain": 0, "timed": 1, "timed_uncertain": 0},
                  "owner_inventory": {"files": [], "foreign_target_databases": []}}
        for owner in self.doc["owners"]:
            i = owner["partition"]
            residency = {"files": 1, "bytes": 50 * 4096, "allocated_bytes": 4096, "resident_bytes": 0,
                         "resident_fraction": 0.0, "selected_pages": 50, "resident_pages": 0}
            obs = {"partition": i, "root": owner["root"], "root_fingerprint": self.doc["index"]["owner_roots"][i],
                   "allocation": {"files": 1, "bytes": 50 * 4096, "allocated_bytes": 4096}, "allocated_bytes": 4096,
                   "databases": [DB], "evicted": True, "residency": residency}
            path = self.root / f"observe-{i}.json"
            self.observes[i] = path
            sha = write_json(path, self.envelope("owner-observe", obs))
            cold["residency"]["owner_observations"].append({"partition": i, "residency": residency,
                "receipt": f"/mnt/index/owners/p{i}.observe.json", "receipt_bytes": path.stat().st_size,
                "receipt_sha256": sha})
            path = self.root / f"shutdown-{i}.json"
            guest = f"/mnt/index/owners/p{i}.live-target-shutdown.json"
            self.shutdowns[i] = (guest, path)
            proof = {"owner": {"partition": i, "root": owner["root"]}, "complete": True,
                     "schema_version": 2, "targets": [{"database": [0x12] * 16}]}
            sha = write_json(path, proof)
            verify["owner_inventory"]["files"].append({"file": guest, "bytes": path.stat().st_size, "sha256": sha,
                "scope": "kernel_live_target_shutdown", "schema_version": 2, "complete": True, "target_databases": [DB]})
        return dict(zip(PHASES, (build, pilot, timed, cold, verify)))

    def emit(self):
        predecessors = []
        for phase in PHASES:
            root = self.phase_dirs[phase]
            root.mkdir(exist_ok=True)
            refs = []
            self.starts[phase] = []
            restarted = int(phase in ("cold-reopen", "verify"))
            for owner in self.doc["owners"]:
                i = owner["partition"]
                start = {"partition": i, "address": owner["address"], "pid": i+10+restarted,
                         "boot_id": f"boot-{i}", "started_unix_ns": 20+restarted,
                         "launch_nonce": f"nonce-{i}-{restarted}", "durability": {"tier": "native"}}
                saved = self.root / f"{phase}-start-{i}.json"
                self.starts[phase].append(saved)
                sha = write_json(saved, start)
                refs.append({"partition": i, "address": owner["address"], "root": owner["root"],
                    "route": "external L2 OwnerHost (host launch)", "start_receipt": start,
                    "start_receipt_bytes": saved.stat().st_size, "start_receipt_sha256": sha,
                    "start_receipt_file": f"/mnt/index/owners/p{i}.receipt.json"})
            artifacts = []
            if phase in ("pilot", "overwrite"):
                stem = "pilot" if phase == "pilot" else "timed"
                for name, bit in ((f"{stem}-touched.bin", b"\x08"), (f"{stem}-uncertain.bin", b"\0")):
                    with (root / name).open("wb") as stream:
                        stream.write(bit)
                        stream.truncate(8 * ((FLEET_KEYS + 63) // 64))
                    artifacts.append(name)
            if phase == "overwrite":
                write_json(root / "overwrite-stop.json", self.terminals[phase]["stop"])
                write_json(root / "write-diagnostics.json", {"index_checkpoint": {}})
                artifacts += ["overwrite-stop.json", "write-diagnostics.json"]
            listed = [{"file": name, "bytes": (root / name).stat().st_size, "sha256": file_digest(root / name)}
                      for name in artifacts]
            name = f"{phase}-receipt.json"
            sha = write_json(root / name, self.envelope(phase, self.terminals[phase], owners=refs, artifacts=listed,
                                                        predecessors=predecessors))
            predecessors.append({"phase": phase, "file": name, "sha256": sha})

    def collect(self, *, peaks=None):
        return collect_index_stages(self.stage, self.phase_dirs, self.starts, self.observes, self.shutdowns,
            {0: 8192, 1: 8192} if peaks is None else peaks, input_sha=self.input_sha, binary_sha=BINARY,
            max_bytes=60_000_000)

    def test_phase_commands_and_fleet_bound_stage(self):
        self.assertEqual(stage_command("kv", "stage", "owner-observe", "fresh", partition=1),
            ["kv", "--q1-index-stage", "stage", "--phase", "owner-observe", "--partition", "1", "--output", "fresh"])
        with self.assertRaisesRegex(ValueError, "phase/partition"):
            stage_command("kv", "stage", "cold-reopen", "fresh", partition=0)
        config = deepcopy(self.doc)
        config["remote"]["storage"]["database"] = [1] * 16
        with self.assertRaisesRegex(ValueError, "provisioned two-owner"):
            stage_document(**{k: v for k, v in config.items() if k not in {"kind", "mode", "schema_version"}})


    def test_optional_pilot_qualify_bound(self):
        qualify_true = deepcopy(self.doc)
        qualify_true["workload"]["pilot"]["qualify"] = True
        stage_document(**{k: v for k, v in qualify_true.items()
                          if k not in {"kind", "mode", "schema_version"}})
        qualify_false = deepcopy(self.doc)
        qualify_false["workload"]["pilot"]["qualify"] = False
        with self.assertRaisesRegex(ValueError, "must run pilot qualification"):
            stage_document(**{k: v for k, v in qualify_false.items()
                              if k not in {"kind", "mode", "schema_version"}})

    def test_actual_receipt_chain_and_independent_allocation(self):
        self.emit()
        self.assertEqual(self.collect()["open_to_read_ns"], 20)
        with self.assertRaisesRegex(ValueError, "stopped tree"):
            self.collect(peaks={0: 4095, 1: 8192})
        with (self.phase_dirs["pilot"] / "pilot-receipt.json").open("ab") as stream:
            stream.write(b"\n")
        with self.assertRaisesRegex(ValueError, "predecessor"):
            self.collect()

    def test_verify_receipt_rejects_wrong_rows_even_with_a_passing_cold_reopen(self):
        self.terminals["verify"]["ambiguous_accepted"] = 1
        self.emit()
        with self.assertRaisesRegex(ValueError, "full row oracle"):
            self.collect()
