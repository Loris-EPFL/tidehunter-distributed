import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import cloud_campaign as cloud


class CloudReports(unittest.TestCase):
    def failure_fixture(self, root):
        spec = json.loads(Path(cloud.__file__).with_name("cloud-campaign.json").read_text())
        job = dict(cloud.make_matrix(spec)[0], backend="native", keys=4, warmup_operations=0,
                   measurement_operations=4, value_size=256)
        directory = root / "cases" / job["case_id"]
        result = directory / "result"
        result.mkdir(parents=True)
        config = cloud.make_run_config(job, directory, {"path": str(directory / "database")}, None)
        config["phases"]["preload"] = False
        backend = {"name": "native", "acknowledgement": "fixture", "replication_factor": 1}
        binary = "a" * 128
        (directory / "config.json").write_text(json.dumps(config))
        (directory / "normalized-config.json").write_text(json.dumps(config))
        (result / "manifest.json").write_text(json.dumps({"status": "failed", "backend_info": backend,
            "run": {"config": config, "binary": {"blake2b512": binary}}}))
        (result / "summary.json").write_text(json.dumps({"status": "failed", "backend": backend}))
        (result / "metrics.prom").write_text("# unit-test fixture\n")
        fields = ["phase", "operation", "outcome", "scheduled_ns", "dispatched_ns", "dispatch_observed",
                  "completed_ns", "api_ns", "response_ns", "scheduling_ns", "logical_keys", "bytes", "error_kind"]
        with (result / "samples.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            for outcome, scheduled, dispatched, completed, size, error in (
                ("ok", 100, 110, 150, 256, ""),
                ("timeout", 200, 220, 1220, 0, "timeout"),
                ("ok", 300, 320, 380, 256, "")):
                writer.writerow({"phase": "measurement", "operation": "get", "outcome": outcome,
                    "scheduled_ns": scheduled, "dispatched_ns": dispatched, "dispatch_observed": "true",
                    "completed_ns": completed, "api_ns": completed - dispatched,
                    "response_ns": completed - scheduled, "scheduling_ns": dispatched - scheduled,
                    "logical_keys": 1, "bytes": size, "error_kind": error})
        receipt = {"job": job, "status": "failed", "raw_artifacts_validated": True,
            "binary_blake2b512": binary, "error": "one timed-out call; fourth arrival not issued",
            "artifacts_sha256": {name: hashlib.sha256((result / name).read_bytes()).hexdigest()
                for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}}
        (directory / "receipt.json").write_text(json.dumps(receipt))
        (root / "matrix.json").write_text(json.dumps({"jobs": [job]}))
        return directory, receipt

    def test_failed_partial_cell_preserves_timeout_denominator_and_observed_quantiles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.failure_fixture(root)
            value = cloud.report(root)
            self.assertFalse(value["complete"])
            row = value["cases"][0]
            self.assertEqual(row["status"], "failed")
            metrics = row["metrics"]
            self.assertEqual((metrics["offered_calls"], metrics["success_calls"], metrics["unsuccessful_calls"]), (3, 2, 1))
            self.assertEqual(metrics["success_fraction"], 2 / 3)
            self.assertEqual(metrics["planned_measurement_calls"], 4)
            self.assertFalse(metrics["measurement_rows_complete"])
            populations = {item["outcome"]: item for item in metrics["populations"]}
            self.assertEqual(populations["ok"]["api_ns"]["p50"], 40)
            self.assertEqual(populations["timeout"]["count"], 1)
            with (root / "comparison.csv").open() as stream:
                output = {item["outcome"]: item for item in csv.DictReader(stream)}
            self.assertEqual(output["timeout"]["status"], "failed")
            self.assertEqual(output["timeout"]["unsuccessful_calls"], "1")

    def test_unvalidated_failure_never_becomes_a_performance_observation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, receipt = self.failure_fixture(root)
            receipt["raw_artifacts_validated"] = False
            (directory / "receipt.json").write_text(json.dumps(receipt))
            row = cloud.summarize_case(directory)
            self.assertNotIn("metrics", row)
            self.assertEqual(row["status"], "failed")

    def test_common_operation_order_is_identical_across_backends_with_cold_last(self):
        spec = json.loads(Path(cloud.__file__).with_name("cloud-campaign.json").read_text())
        jobs = cloud.make_matrix(spec)
        for repeat in range(spec["repeats"]):
            expected = None
            for backend in cloud.BACKENDS:
                block = [job for job in jobs if job["repeat"] == repeat and job["backend"] == backend]
                common = [(job["profile"], job["value_size"], job["offered_ops_per_second"])
                          for job in block if job["cohort"] == "primary_rf1"]
                if expected is None:
                    expected = common
                self.assertEqual(common, expected)
                self.assertEqual(block[-1]["cohort"], "cold_os_cache")
                for job in block:
                    config = cloud.make_run_config(job, Path("/tmp/owned-case"), {}, None)
                    self.assertLessEqual(len(config["namespace"].encode()), 64)


if __name__ == "__main__":
    unittest.main()
