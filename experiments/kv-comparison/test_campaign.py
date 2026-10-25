import copy
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest

import analyze
import campaign
import local


class AdmissionTests(unittest.TestCase):
    def spec(self):
        return {"schema_version": 1, "seed": 17, "repeats": 2, "layout": "distributed", "backends": ["tikv", "cassandra"],
                "profiles": [{"name": "fresh", "keys": 64, "aging_operations": 0}, {"name": "aged", "keys": 64, "aging_operations": 128}],
                "rates": [100, 1000], "base_config": "base.json", "max_runs": 16}

    def test_random_order_repeatable_and_complete(self):
        spec = self.spec()
        jobs = campaign.matrix(spec)
        self.assertEqual(jobs, campaign.matrix(copy.deepcopy(spec)))
        identities = {(x["repeat"], x["backend"], x["profile"]["name"], x["rate"]) for x in jobs}
        self.assertEqual(len(identities), 16)
        spec["seed"] = 18
        self.assertNotEqual(jobs, campaign.matrix(spec))

    def test_matrix_bound_precedes_expansion(self):
        spec = self.spec()
        spec["max_runs"] = 15
        with self.assertRaises(ValueError):
            campaign.matrix(spec)
        spec = self.spec()
        spec["rates"] = [100, 100]
        with self.assertRaises(ValueError):
            campaign.matrix(spec)

    def test_finite_aging_and_unknown_fields_required(self):
        spec = self.spec()
        spec["profiles"][1]["aging_operations"] = -1
        with self.assertRaises(ValueError):
            campaign.matrix(spec)
        spec = self.spec()
        spec["aws"] = True
        with self.assertRaises(ValueError):
            campaign.matrix(spec)

    def test_tcp_retention_accounts_worst_case_and_rejects_over_limit(self):
        base = {"dataset": {}, "phases": {"preload": True, "warmup_operations": 10,
                                         "aging_operations": 0, "measurement_operations": 100},
                "execution": {}, "limits": {}}
        job = {"backend": "tidehunter_tcp", "profile": {"keys": 64, "aging_operations": 5}, "rate": 100}
        config = campaign.build_config(base, job, None, Path("/tmp/unused"), 17, False)
        self.assertEqual(config["backend"]["options"]["max_batches_per_partition"], 64 + 10 + 5 + 100 + 1024)
        self.assertEqual(base["phases"]["aging_operations"], 0)
        qualified = campaign.build_config(base, job, None, Path("/tmp/unused"), 17, True)
        self.assertEqual(qualified["backend"]["options"]["max_batches_per_partition"], 1024)
        base["phases"]["measurement_operations"] = 1_000_000
        with self.assertRaisesRegex(ValueError, "exceeds supported"):
            campaign.build_config(base, job, None, Path("/tmp/unused"), 17, False)

    def test_owned_state_cannot_be_relocated(self):
        with tempfile.TemporaryDirectory() as directory:
            local.dump(Path(directory) / "owner.json", {"directory": "/wrong", "owner": "a" * 32})
            with self.assertRaises(ValueError):
                local.load(directory)

    def test_snapshot_retains_new_observer_sources_not_runtime_or_unrelated_files(self):
        import subprocess
        import zipfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
            git("init", "-q")
            (root / "tracked.rs").write_text("fn reference() {}\n")
            git("add", "tracked.rs")
            git("-c", "user.name=Loris-EPFL", "-c", "user.email=91122257+Loris-EPFL@users.noreply.github.com", "commit", "-qm", "fixture")
            here = root / "experiments/kv-comparison"
            observer = here / "observability"
            (observer / ".state").mkdir(parents=True)
            (observer / "exporter.py").write_text("SOURCE = True\n")
            (observer / "prometheus.yaml").write_text("scrape_configs: []\n")
            (observer / ".state/secret.json").write_text('{"password":"not-source"}\n')
            (root / "unrelated-user-file.json").write_text('{"private":true}\n')
            output = root / "output"
            output.mkdir()
            with patch.object(local, "ROOT", root), patch.object(local, "HERE", here):
                campaign.source_snapshot(output)
            with zipfile.ZipFile(output / "source.zip") as archive:
                names = set(archive.namelist())
            self.assertIn("tracked.rs", names)
            self.assertIn("experiments/kv-comparison/observability/exporter.py", names)
            self.assertIn("experiments/kv-comparison/observability/prometheus.yaml", names)
            self.assertNotIn("experiments/kv-comparison/observability/.state/secret.json", names)
            self.assertNotIn("unrelated-user-file.json", names)

    def test_unrequested_external_observer_is_recorded_present(self):
        from unittest.mock import patch
        item = {"Config": {"Labels": {"com.docker.compose.project": "kv-comparison-observability",
                                      "com.docker.compose.service": "prometheus"}, "Image": "pinned:image"},
                "Image": "sha256:observed", "HostConfig": {"Memory": 1024, "NanoCpus": 0},
                "State": {"Health": {"Status": "healthy"}}}
        with patch.object(local, "command", side_effect=["owned-id\n", json.dumps([item])]):
            observed = campaign.observer_snapshot(False)
        self.assertFalse(observed["requested"])
        self.assertEqual(observed["state"], "exact_project_present")

    def test_unavailable_observer_inventory_is_unknown_not_off(self):
        from unittest.mock import patch
        with patch.object(local, "command", side_effect=FileNotFoundError("Docker absent")):
            self.assertEqual(campaign.observer_snapshot(False)["state"], "unknown")


class AccountingTests(unittest.TestCase):
    def row(self, latency, outcome="ok"):
        return {"operation": "read", "outcome": outcome, "logical_keys": 1,
                "api_ns": latency, "response_ns": latency, "scheduling_ns": 0}

    def test_pool_raw_samples_not_percentiles(self):
        rows = [self.row(1)] * 1000 + [self.row(1000)]
        report = analyze.aggregate(rows)
        self.assertEqual(report["populations"][0]["api_ns"]["p99"], 1)
        self.assertEqual(report["offered_calls"], 1001)

    def test_rejected_timeout_unknown_never_disappear(self):
        rows = [self.row(10), self.row(None, "rejected"), self.row(100, "timeout"), self.row(100, "unknown_mutation")]
        report = analyze.aggregate(rows)
        self.assertEqual(report["offered_calls"], 4)
        self.assertEqual(report["success_fraction"], .25)
        populations = {x["outcome"]: x for x in report["populations"]}
        self.assertEqual(populations["rejected"]["api_ns"]["sample_count"], 0)
        self.assertIsNone(populations["rejected"]["api_ns"]["p99"])
        self.assertEqual(populations["timeout"]["count"], 1)

    def test_zero_samples_not_green(self):
        with self.assertRaises(ValueError):
            analyze.aggregate([])

    def test_negative_timing_rejected(self):
        fields = ["phase", "operation", "outcome", "scheduled_ns", "dispatched_ns", "dispatch_observed", "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes", "error_kind"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "samples.csv"
            with path.open("w", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                writer.writerow(dict(zip(fields, ["measurement", "read", "ok", 0, 0, "true", 1, -1, 1, 0, 1, 10, ""])))
            with self.assertRaises(ValueError):
                list(analyze.read_samples(path))

    def test_different_contracts_rejected_before_raw_pool(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as folder:
            paths = [Path(folder) / name for name in ("a", "b")]
            for path in paths:
                path.mkdir()
            with patch.object(analyze, "boundary", side_effect=[{"rf": 1}, {"rf": 3}]):
                with self.assertRaisesRegex(ValueError, "different workload"):
                    analyze.analyze(paths)


class ControllerIntegrityTests(unittest.TestCase):

    def test_fdb_loader_environment_is_exact_and_rechecks_admitted_bytes(self):
        import os
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            library = Path(directory) / "libfdb_c.so"
            library.write_bytes(b"native-library-admission-fixture")
            identity = {"path": str(library), "sha256": campaign.digest(library),
                        "blake2b512": campaign.digest(library, "blake2b")}
            with patch.dict(os.environ, {"LD_LIBRARY_PATH": "/ambient", "LD_PRELOAD": "/inject.so", "LD_AUDIT": "/audit.so"}):
                environment = campaign.admitted_fdb_environment(identity)
            self.assertEqual(environment["LD_LIBRARY_PATH"], directory)
            self.assertNotIn("LD_PRELOAD", environment)
            self.assertNotIn("LD_AUDIT", environment)
            library.write_bytes(b"changed after admission")
            with self.assertRaisesRegex(RuntimeError, "bytes changed"):
                campaign.admitted_fdb_environment(identity)

    def test_cleanup_failures_cannot_leave_completed_receipt(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / "server"
            state.mkdir()
            owner = "a" * 32
            local.dump(state / "owner.json", {"directory": str(state), "owner": owner,
                       "project": "kv-" + owner[:16], "compose_file": str(state / "compose.json"),
                       "backend": "tikv", "layout": "qualification"})
            receipt = {"status": "completed"}
            with patch.object(local, "export", side_effect=RuntimeError("export failed")), \
                    patch.object(local, "stop", side_effect=RuntimeError("stop failed")):
                with self.assertRaisesRegex(RuntimeError, "receipt marked failed"):
                    campaign.finalize_run(root, state, True, False, receipt)
            retained = json.loads((root / "receipt.json").read_text())
            self.assertEqual(retained["status"], "failed")
            self.assertEqual({item["phase"] for item in retained["cleanup_errors"]},
                             {"deployment_export", "exact_owned_stop"})

    def test_native_cleanup_removes_only_generated_database(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "database").mkdir()
            (root / "database/value.wal").write_bytes(b"owned fixture")
            (root / "tcp-evidence").mkdir()
            (root / "tcp-evidence/owner.log").write_text("retained evidence\n")
            (root / "unrelated.txt").write_text("keep\n")
            receipt = {"status": "completed"}
            campaign.finalize_run(root, root / "server", False, False, receipt)
            self.assertFalse((root / "database").exists())
            self.assertEqual((root / "tcp-evidence/owner.log").read_text(), "retained evidence\n")
            self.assertEqual((root / "unrelated.txt").read_text(), "keep\n")
            self.assertFalse(json.loads((root / "receipt.json").read_text())["database_files_retained"])

    def test_loaded_native_library_must_match_path_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            library = root / "libfdb_c.so"
            library.write_bytes(b"native-library-fixture")
            identity = {"path": str(library), "sha256": campaign.digest(library),
                        "blake2b512": campaign.digest(library, "blake2b")}
            loaded = {"path": "/wrong/libfdb_c.so", "blake2b512": identity["blake2b512"], "api_max_version": 730}
            manifest = {"status": "completed", "run": {"config": {"seed": 17}, "binary": {"blake2b512": "binary-identity"}},
                        "backend_info": {"configuration": {"loaded_native_library": loaded}}}
            local.dump(root / "manifest.json", manifest)
            receipt = {"binary_blake2b512": "binary-identity"}
            with self.assertRaisesRegex(RuntimeError, "path/hash differs"):
                campaign.verify_run_identity(root, receipt, {"seed": 17}, identity)
            loaded["path"] = identity["path"]
            local.dump(root / "manifest.json", manifest)
            campaign.verify_run_identity(root, receipt, {"seed": 17}, identity)
            self.assertEqual(receipt["loaded_native_library"], loaded)


class SupervisorTests(unittest.TestCase):
    def test_real_child_exit_preserved(self):
        import os
        import sys
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (root / "run.log").open("w") as log:
                code = campaign.supervise([sys.executable, "-c", "raise SystemExit(7)"], log,
                                          root / "resources.jsonl", sorted(os.sched_getaffinity(0))[:1], 256, 5, 1)
            self.assertEqual(code, 7)

    def test_native_blocked_child_independently_terminated(self):
        import os
        import sys
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (root / "run.log").open("w") as log:
                with self.assertRaisesRegex(RuntimeError, "wall deadline"):
                    campaign.supervise([sys.executable, "-c", "import time; time.sleep(60)"], log,
                                       root / "resources.jsonl", sorted(os.sched_getaffinity(0))[:1], 256, .05, .05)


if __name__ == "__main__":
    unittest.main()
