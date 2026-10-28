import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import cloud_campaign as cloud


class CloudMatrixTests(unittest.TestCase):
    def test_advertised_read_evidence_cannot_be_missing(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            result.mkdir()
            manifest = result / "manifest.json"
            manifest.write_text(json.dumps({"run": {}}))
            self.assertIsNone(cloud.admit_measurement_read_evidence(directory))
            manifest.write_text(json.dumps({"run": {"measurement_read_evidence_schema_version": 1}}))
            with self.assertRaisesRegex(ValueError, "advertised measurement read evidence"):
                cloud.admit_measurement_read_evidence(directory)

    def spec(self, repeats):
        return {
            "schema_version": 1, "seed": 1775, "repeats": repeats,
            "value_sizes": [64, 256, 1024, 8192, 65536],
            "backends": ["native", "tidehunter_tcp", "tidehunter_efa", "tidehunter_efa_direct",
                         "tidehunter_efa_native", "tikv", "cassandra", "foundationdb", "valkey_tcp"],
            "keys": 4096, "max_cases": 774, "offered_rates": [1000, 10000, 50000],
            "offered_seconds": 8, "case_deadline_seconds": 180,
            "hard_deadline_seconds": 10800, "export_reserve_seconds": 600,
        }

    def test_one_repeat_preserves_every_first_repeat_job_and_run_configuration(self):
        single = cloud.make_matrix(self.spec(1))
        historical = cloud.make_matrix(self.spec(3))
        self.assertEqual((len(single), len(historical)), (253, 759))
        first = [job for job in historical if job["repeat"] == 0]
        self.assertEqual(single, first)
        for job, old_job in zip(single, first):
            self.assertEqual(
                cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source"),
                cloud.make_run_config(old_job, Path("/tmp/owned-case"), {}, "source"))

    def test_write_diagnostics_are_a_distinct_cohort_without_reducing_any_work(self):
        normal = cloud.make_matrix(self.spec(1))
        diagnostic = cloud.make_matrix(self.spec(1) | {"diagnostics": {"write_profile_limit": 256}})
        self.assertEqual((len(normal), len(diagnostic)), (253, 258))
        def identity(job):
            return job["backend"], job["repeat"], job["profile"], job["cohort"], job["value_size"]
        retained = {identity(job): job for job in diagnostic if job["cohort"] != "write_stage_diagnostics"}
        self.assertEqual(len(retained), len(normal))
        for before in normal:
            after = retained[identity(before)]
            self.assertEqual({key: value for key, value in before.items() if key != "case_id"},
                             {key: value for key, value in after.items() if key != "case_id"})
        selected = [job for job in diagnostic if job["cohort"] == "write_stage_diagnostics"]
        self.assertEqual({job["backend"] for job in selected}, {"native", *cloud.TRANSPORTS})
        for after in selected:
            before = retained[(*identity(after)[:3], "primary_rf1", after["value_size"])]
            self.assertEqual(after["diagnostics"]["write_profile_limit"], 256)
            for field in ("mix", "concurrency", "keys", "measurement_operations", "warmup_operations",
                          "offered_ops_per_second", "distribution", "coordinator_cache_bytes",
                          "owner_read_cache_bytes", "native_durability", "drop_os_caches"):
                self.assertEqual(before[field], after[field])

    def test_invalid_diagnostic_budget_is_rejected_before_launch(self):
        for value in (True, -1, 65537, {}, {"write_profile_limit": 16, "unknown": 1}):
            diagnostics = value if isinstance(value, dict) else {"write_profile_limit": value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud.make_matrix(self.spec(1) | {"diagnostics": diagnostics})

    def test_full_profile_cohort_and_per_case_phase_contract(self):
        profiles = {
            "point-mixed": ({"read": 45, "put": 45, "delete": 5, "missing_read": 5}, 1, 8192),
            "read-concurrent": ({"read": 100}, 16, 8192),
            "write-concurrent": ({"put": 100}, 16, 4096),
            "read-many32": ({"get_many": 100}, 16, 2048),
        }
        spec = self.spec(1)
        jobs = cloud.make_matrix(spec)
        identities = {(job["backend"], job["cohort"], job["profile"], job["value_size"]) for job in jobs}
        expected = set()
        for backend in spec["backends"]:
            expected.update((backend, "primary_rf1", profile, size)
                            for profile in profiles for size in spec["value_sizes"])
            expected.update((backend, "primary_rf1", f"offered-{rate}", 1024)
                            for rate in spec["offered_rates"])
            expected.add((backend, "primary_rf1", "zipf-read", 1024))
            expected.add((backend, "cold_os_cache", "cold-os-large", 65536))
            if backend in ("tidehunter_tcp", "tidehunter_efa", "tidehunter_efa_direct", "tidehunter_efa_native"):
                expected.update((backend, cohort, profile, size) for size in (256, 8192)
                                for cohort, profile in (("coordinator_cache_configured", "coordinator-cache"),
                                                        ("owner_cache_configured", "owner-cache")))
            if backend == "tidehunter_efa_native":
                expected.update((backend, "registered_ram_configured", "registered-ram", size)
                                for size in (256, 8192))
            if backend == "native":
                expected.update((backend, "native_pagecache", profile, size)
                                for profile in ("point-mixed", "read-many32") for size in spec["value_sizes"])
        self.assertEqual(identities, expected)
        self.assertEqual(len(jobs), len(expected))
        for job in jobs:
            with self.subTest(case=job["case_id"]):
                profile, cohort = job["profile"], job["cohort"]
                if profile in profiles:
                    operation_mix, concurrency, operations = profiles[profile]
                elif job["offered_ops_per_second"] is not None:
                    operation_mix, concurrency = {"read": 70, "put": 30}, 32
                    operations = job["offered_ops_per_second"] * 8
                else:
                    operation_mix = {"read": 100}
                    concurrency = 16 if profile in ("zipf-read", "cold-os-large") else 1
                    operations = 4096 if profile == "cold-os-large" else 8192
                self.assertEqual({key: value for key, value in job["mix"].items() if value}, operation_mix)
                self.assertEqual((job["concurrency"], job["measurement_operations"]), (concurrency, operations))
                warmup = 0 if cohort == "cold_os_cache" else 1024 if cohort.endswith("_configured") else 512
                self.assertEqual(job["warmup_operations"], warmup)
                self.assertEqual(job["keys"], 131072 if cohort == "cold_os_cache" else 4096)
                self.assertEqual(job["drop_os_caches"], cohort == "cold_os_cache")
                self.assertEqual(job["coordinator_cache_bytes"], 128 * 1024**2 if cohort == "coordinator_cache_configured" else 0)
                self.assertEqual(job["owner_read_cache_bytes"], 128 * 1024**2 if cohort == "owner_cache_configured" else 0)
                self.assertEqual(job["native_durability"], "page_cache" if cohort == "native_pagecache" else "value_wal_barrier")
                self.assertEqual(job["distribution"], {"zipf": {"exponent": 0.99}} if profile == "zipf-read" else "uniform")
                if cohort == "registered_ram_configured":
                    self.assertEqual(job["resident_reads"], {
                        "source_bytes": 16 * 1024**2, "source_regions": 4096,
                        "destination_bytes": 20 * 1024**2, "destination_slots": 640,
                        "max_read_bytes": 32768, "max_handles": 4096, "require_device_read": True})
                else:
                    self.assertNotIn("resident_reads", job)
                config = cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source", 180)
                self.assertEqual(config["phases"], {"preload": True, "warmup_operations": warmup,
                    "aging_operations": 0, "measurement_operations": operations})
                self.assertFalse(config["qualification_only"])
                self.assertEqual(config["workload"]["batch_size"], 32)
                self.assertEqual(config["limits"]["max_operations"], job["keys"] + warmup + operations + 4096)
                self.assertEqual(config["limits"]["deadline_seconds"], 600 if cohort == "cold_os_cache" else 180)

    def test_exact_case_bound_admits_all_jobs_but_never_truncates(self):
        for repeats, count in ((1, 253), (3, 759)):
            with self.subTest(repeats=repeats):
                spec = self.spec(repeats) | {"max_cases": count}
                self.assertEqual(len(cloud.make_matrix(spec)), count)
                with self.assertRaisesRegex(ValueError, "exceeding explicit max_cases"):
                    cloud.make_matrix(spec | {"max_cases": count - 1})

    def test_selection_accepts_supported_subsets_and_keeps_complete_selected_cases(self):
        full = self.spec(1)
        full_jobs = cloud.make_matrix(full)
        subset = full | {"backends": ["valkey_tcp"], "max_cases": 25}
        selected = cloud.make_matrix(subset)
        self.assertEqual(len(selected), 25)
        self.assertEqual({job["backend"] for job in selected}, {"valkey_tcp"})
        valkey = [job for job in full_jobs if job['backend'] == 'valkey_tcp']
        common = [job for job in full_jobs if job['backend'] == 'tikv']
        ignored = {'backend', 'case_id'}
        self.assertEqual(
            sorted((cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in valkey)),
            sorted((cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in common)))
        self.assertEqual(len(valkey), 25)
        expected = [job for job in full_jobs if job["backend"] == "valkey_tcp"]
        self.assertEqual(
            [cloud.analyze.canonical({k: v for k, v in job.items() if k != "case_id"}) for job in selected],
            [cloud.analyze.canonical({k: v for k, v in job.items() if k != "case_id"}) for job in expected])
        for invalid in ([], ["native", "native"], ["not-a-backend"]):
            with self.subTest(backends=invalid), self.assertRaises(ValueError):
                cloud.make_matrix(full | {"backends": invalid})
        self.assertEqual(len(cloud.make_matrix(self.spec(3) | {
            'diagnostics': {'write_profile_limit': 256}})), 774)

    def test_historical_frozen_jobs_report_without_current_completeness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = [job for job in cloud.make_matrix(self.spec(1))
                    if job["backend"] != "valkey_tcp"]
            frozen = {"schema_version": 1, "spec": self.spec(1) | {
                "backends": list(cloud.LEGACY_BACKENDS)}, "jobs": jobs}
            original = json.dumps(frozen)
            (root / "matrix.json").write_text(original)
            report = cloud.report(root)
            self.assertEqual(report["planned_cases"], 228)
            self.assertEqual(report["planned_backends"], sorted(cloud.LEGACY_BACKENDS))
            self.assertFalse(report["current_backend_coverage"])
            self.assertEqual((root / "matrix.json").read_text(), original)
            self.assertEqual([row["job"] for row in report["cases"]], jobs)


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

    def test_resource_interval_must_match_both_observed_boundary_counters(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            before = {"schema_version": 1, "host_index": -1, "boot_id": "same-boot", "processes": [{
                "process_role": "client", "available": True, "cgroup": "/system.slice/owned.service",
                "inode": 12, "monotonic_ns": 1000000000, "cpu": {"usage_usec": 1000}}]}
            after = {**before, "processes": [{**before["processes"][0],
                "monotonic_ns": 2000000000, "cpu": {"usage_usec": 5000}}]}
            resources = cloud.resource_accounting.interval([before], [after])
            receipt["measurement_evidence"] = {}
            for field, name, frame in (("before", "measurement-before.json", before),
                                      ("end", "measurement-end.json", after)):
                path = directory / name
                path.write_text(json.dumps({"hosts": [{"resource_accounting": frame}]}))
                receipt["measurement_evidence"][field] = name
                receipt["measurement_evidence"][field + "_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            path = directory / "resource-interval.json"
            path.write_text(json.dumps(resources))
            receipt["resource_evidence"] = {"path": path.name, "observed": resources,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            (directory / "receipt.json").write_text(json.dumps(receipt))
            row = cloud.summarize_case(directory)["resource_evidence"]["observed"]["processes"][0]
            self.assertEqual(row["cpu_seconds"], .004)
            resources["processes"][0]["cpu_seconds"] = 5
            path.write_text(json.dumps(resources))
            receipt["resource_evidence"]["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            (directory / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "cannot be reproduced"):
                cloud.summarize_case(directory)

    def test_completed_requested_profile_requires_real_measurement_samples(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "result").mkdir()
            job = {"backend": "native", "diagnostics": {"write_profile_limit": 16}}
            with self.assertRaisesRegex(ValueError, "artifact is missing"):
                cloud.admit_write_diagnostics(directory, job, completed=True)
            profile = {"schema_version": 1, "backend": "native", "phase": "measurement",
                "selection": {"requested_limit": 16, "observed_samples": 0},
                "phase_attempted": True, "runtime_enabled": False, "profile": {}, "error": None}
            path = directory / "result/write-diagnostics.json"
            path.write_text(json.dumps(profile))
            with self.assertRaisesRegex(ValueError, "real measurement samples"):
                cloud.admit_write_diagnostics(directory, job, completed=True)
            profile["phase"] = "warmup"
            path.write_text(json.dumps(profile))
            with self.assertRaisesRegex(ValueError, "phase"):
                cloud.admit_write_diagnostics(directory, job, completed=False)

    def test_failed_partial_cell_preserves_timeout_denominator_and_observed_quantiles(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.failure_fixture(root)
            value = cloud.report(root)
            self.assertFalse(value["complete"])
            self.assertEqual(value["planned_repeats"], [0])
            self.assertEqual(value["planned_independent_repetitions"], 1)
            self.assertEqual(value["repetition_mode"], "single")
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
            self.assertEqual(populations["ok"]["api_mean_ns"], 50)
            self.assertEqual(populations["ok"]["api_stddev_ns"], 10)
            self.assertEqual(populations["ok"]["response_mean_ns"], 65)
            self.assertEqual(populations["ok"]["response_stddev_ns"], 15)
            self.assertEqual(populations["ok"]["scheduling_mean_ns"], 15)
            self.assertEqual(populations["ok"]["scheduling_stddev_ns"], 5)
            with (root / "comparison.csv").open() as stream:
                output = {item["outcome"]: item for item in csv.DictReader(stream)}
            self.assertEqual(output["timeout"]["status"], "failed")
            self.assertEqual(output["timeout"]["unsuccessful_calls"], "1")
            self.assertEqual(output["ok"]["response_mean_us"], "0.065")
            self.assertEqual(output["ok"]["response_stddev_us"], "0.015")
            self.assertEqual(output["ok"]["scheduling_mean_us"], "0.015")
            self.assertEqual(output["ok"]["scheduling_stddev_us"], "0.005")
            self.assertEqual(output["ok"]["planned_independent_repetitions"], "1")
            self.assertEqual(output["ok"]["repetition_mode"], "single")
            self.assertEqual(output["ok"]["timing_stddev_scope"], "within_case_population")

    def test_csv_retains_actual_acknowledgement_roles_and_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory, _ = self.failure_fixture(root)
            row = cloud.summarize_case(directory)
            row["backend"] = {"name": "valkey_tcp", "acknowledgement": "observed AOF always",
                "consistency": "primary TCP", "replication_factor": 1,
                "measurement_boundary": "application API; memory-resident GET",
                "get_many_strategy": "ordered bounded per-key GET", "batch_atomicity": "unsupported",
                "topology": {"primary_count": 5, "replica_count": 0},
                "configuration": {"appendfsync": "always", "maxmemory-policy": "noeviction"}}
            row["resource_evidence"] = {"observed": {"processes": [{"process_role": "valkey0", "cpu_seconds": 1.25}]}}
            cloud.write_csv_report(root, [row])
            with (root / "comparison.csv").open() as stream:
                exported = list(csv.DictReader(stream))
            for item in exported:
                self.assertEqual(item["acknowledgement"], row["backend"]["acknowledgement"])
                self.assertEqual(item["batch_atomicity"], "unsupported")
                self.assertEqual(json.loads(item["observed_topology"]), row["backend"]["topology"])
                self.assertEqual(json.loads(item["effective_configuration"]), row["backend"]["configuration"])
                self.assertEqual(json.loads(item["resource_evidence"]), row["resource_evidence"])

    def test_undispatched_rejection_retains_response_statistics_without_api_clocks(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            samples = directory / "result/samples.csv"
            with samples.open() as stream:
                fields = csv.DictReader(stream).fieldnames
            with samples.open("a", newline="") as stream:
                csv.DictWriter(stream, fieldnames=fields).writerow({
                    "phase": "measurement", "operation": "get", "outcome": "rejected",
                    "scheduled_ns": 400, "dispatched_ns": 0, "dispatch_observed": "false",
                    "completed_ns": 430, "api_ns": 0, "response_ns": 30, "scheduling_ns": 0,
                    "logical_keys": 1, "bytes": 0, "error_kind": "capacity"})
            receipt["artifacts_sha256"]["samples.csv"] = hashlib.sha256(samples.read_bytes()).hexdigest()
            (directory / "receipt.json").write_text(json.dumps(receipt))
            row = cloud.summarize_case(directory)
            rejected = next(item for item in row["metrics"]["populations"] if item["outcome"] == "rejected")
            self.assertEqual(rejected["response_ns"]["sample_count"], 1)
            self.assertEqual(rejected["response_mean_ns"], 30)
            self.assertEqual(rejected["response_stddev_ns"], 0)
            for kind in ("api", "scheduling"):
                self.assertEqual(rejected[kind + "_ns"]["sample_count"], 0)
                for stat in ("mean", "min", "stddev"):
                    self.assertIsNone(rejected[kind + "_" + stat + "_ns"])

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
