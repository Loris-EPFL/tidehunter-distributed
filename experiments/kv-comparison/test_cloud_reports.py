import copy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_campaign as cloud
import cloud_controller as controller


class WriteTimelineAdmissionTests(unittest.TestCase):
    def fixture(self, root):
        result = root / "result"
        result.mkdir()
        config = {"backend": {"kind": "tidehunter"}, "diagnostics": {"write_timeline_limit": 1}}
        job = {"case_id": "trace", "backend": "tidehunter_tcp",
               "diagnostics": {"write_timeline_limit": 1}}
        (root / "normalized-config.json").write_text(json.dumps(config))
        (result / "manifest.json").write_text(json.dumps({
            "run": {"config": config}, "backend_info": {"name": "tidehunter_tcp"}}))
        (result / "samples.csv").write_text(
            "phase,operation,outcome,scheduled_ns,dispatched_ns,dispatch_observed,completed_ns,api_ns,response_ns,scheduling_ns,logical_keys,bytes,error_kind,caller_sample_id\n"
            "measurement,put,success,0,1,true,11,10,11,1,1,8,,0\n")
        event = lambda stage, ns: {"stage": stage, "elapsed_ns": ns, "owner": 0, "envelope_id": 7}
        trace = {"limit": 1, "records": [{"caller_sample_id": 0, "completed": True, "error": None,
                  "operation": [0] * 16, "batch": {"epoch": 1, "sequence": 0},
                  "events": [event("tx_queued", 1), event("completion_harvested", 9)]}],
                 "owner_records": [{"owner": 0, "envelope_id": 7, "success": True}],
                 "checkpoint_events": [], "owner_errors": [], "dropped_calls": 0, "dropped_events": 0,
                 "incomplete_calls": 0, "dropped_checkpoint_events": 0, "owner_dropped_records": 0,
                 "owner_incomplete_records": 0, "owner_expected_connections": 1,
                 "owner_captured_connections": 1, "deadline_exceeded": False,
                 "owner_storage_durations_available": True}
        data = {"schema_version": 1, "requested_limit": 1, "measurement_attempted": True,
                "measurement_drained": True, "available": True, "status": "complete",
                "error": None, "timeline": trace}
        path = result / "write-timeline.json"
        path.write_text(json.dumps(data))
        return job, config, data, path

    def test_completed_timeline_requires_real_matched_case_and_bounded_records(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            binding = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertEqual(binding["coverage"]["joined_records"], 1)
            self.assertNotIn("timeline", binding)
            for change in ("limit", "backend", "missing_frame", "missing_sample", "duplicate", "wrong_phase"):
                with self.subTest(change=change):
                    altered = copy.deepcopy(data)
                    altered_job = copy.deepcopy(job)
                    samples = root / "result/samples.csv"
                    original = samples.read_text()
                    if change == "limit":
                        altered["requested_limit"] = 2
                    elif change == "backend":
                        altered_job["backend"] = "tidehunter_efa"
                    elif change == "missing_frame":
                        altered["timeline"]["owner_records"] = []
                    elif change == "missing_sample":
                        samples.write_text(original.replace(",,0\n", ",,\n"))
                    elif change == "duplicate":
                        altered["timeline"]["records"].append(copy.deepcopy(altered["timeline"]["records"][0]))
                    else:
                        altered["measurement_attempted"] = False
                    path.write_text(json.dumps(altered))
                    with self.assertRaises(ValueError):
                        cloud.admit_write_timeline(root, altered_job, config, completed=True)
                    samples.write_text(original)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "missing"):
                cloud.admit_write_timeline(root, job, config, completed=True)

    def test_extent_switch_stages_are_admitted(self):
        # Run 13 first launch: the engine emits these stages on a call whose group switched extents.
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            events = data["timeline"]["records"][0]["events"]
            events[1:1] = [{"stage": "live_extent_switch_started", "elapsed_ns": 2, "owner": 0, "envelope_id": None},
                           {"stage": "live_extent_switch_completed", "elapsed_ns": 3, "owner": 0, "envelope_id": None}]
            path.write_text(json.dumps(data))
            self.assertEqual(cloud.admit_write_timeline(root, job, config, completed=True)["coverage"]["joined_records"], 1)
            events[1]["stage"] = "live_extent_switch_unknown"
            path.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                cloud.admit_write_timeline(root, job, config, completed=True)

    def test_sparse_write_ids_join_with_mixed_and_unprofiled_measurement_calls(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            data["timeline"]["records"][0]["caller_sample_id"] = 17
            samples = root / "result/samples.csv"
            samples.write_text(samples.read_text().replace(",,0\n", ",,17\n") +
                "measurement,read,success,20,21,true,31,10,11,1,1,8,,18\n" +
                "measurement,put,success,40,41,true,51,10,11,1,1,8,,19\n")
            path.write_text(json.dumps(data))
            admitted = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertEqual(admitted["coverage"]["joined_records"], 1)
            for caller in (18, 20, 2**64):
                with self.subTest(caller=caller):
                    data["timeline"]["records"][0]["caller_sample_id"] = caller
                    path.write_text(json.dumps(data))
                    with self.assertRaises(ValueError):
                        cloud.admit_write_timeline(root, job, config, completed=True)

    def test_checkpoint_generation_is_optional_validated_and_hash_bound(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            event = {"stage": "adopted", "elapsed_ns": 3, "epoch": 1}
            data["timeline"]["checkpoint_events"] = [event]
            path.write_text(json.dumps(data))
            old_binding = cloud.admit_write_timeline(root, job, config, completed=True)
            event["generation"] = 7
            path.write_text(json.dumps(data))
            new_binding = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertNotEqual(old_binding["sha256"], new_binding["sha256"])
            for invalid in (-1, True, 2**64):
                event["generation"] = invalid
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, "checkpoint timeline"):
                    cloud.admit_write_timeline(root, job, config, completed=True)
            event["generation"] = 7
            event["pressure"] = {"reason": "bytes", "batches": 10, "encoded_bytes": 100,
                                 "max_batches": 20, "max_encoded_bytes": 120}
            path.write_text(json.dumps(data))
            pressure_binding = cloud.admit_write_timeline(root, job, config, completed=True)
            self.assertNotEqual(new_binding["sha256"], pressure_binding["sha256"])
            event["pressure"]["encoded_bytes"] = -1
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "checkpoint pressure"):
                cloud.admit_write_timeline(root, job, config, completed=True)
            event["pressure"]["encoded_bytes"] = 100
            for invalid in (-1, True, 2**64):
                event["pressure"]["incoming_encoded_bytes"] = invalid
                path.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, "checkpoint pressure"):
                    cloud.admit_write_timeline(root, job, config, completed=True)

    def test_local_consumer_checks_timeline_receipt_binding(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, _, path = self.fixture(root)
            result = root / "result"
            backend = {"name": "tidehunter_tcp"}
            binary = "a" * 128
            manifest = {"run": {"config": config, "binary": {"blake2b512": binary}},
                        "backend_info": backend}
            summary = {"backend": backend}
            (result / "manifest.json").write_text(json.dumps(manifest))
            (result / "summary.json").write_text(json.dumps(summary))
            (result / "metrics.prom").write_text("")
            receipt = {"job": job, "status": "completed", "binary_blake2b512": binary,
                       "normalized_config_sha256": hashlib.sha256((root / "normalized-config.json").read_bytes()).hexdigest(),
                       "artifacts_sha256": {name: hashlib.sha256((result / name).read_bytes()).hexdigest()
                                            for name in ("manifest.json", "summary.json", "samples.csv", "metrics.prom")}}
            receipt["write_timeline"] = cloud.admit_write_timeline(root, job, config, completed=True)
            cloud.analyze.verify_binding(root, config, receipt, manifest, summary)
            path.write_text(path.read_text() + " ")
            with self.assertRaisesRegex(ValueError, "integrity"):
                cloud.analyze.verify_binding(root, config, receipt, manifest, summary)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "missing"):
                cloud.analyze.verify_binding(root, config, receipt, manifest, summary)

    def test_partial_receipt_is_bound_and_tampering_or_missing_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job, config, data, path = self.fixture(root)
            data.update(status="failed", available=False, error="deadline exceeded")
            data["timeline"].update(deadline_exceeded=True, owner_captured_connections=0,
                                    owner_storage_durations_available=False)
            path.write_text(json.dumps(data))
            binding = cloud.admit_write_timeline(root, job, config, completed=False)
            receipt = {"job": job, "status": "failed", "raw_artifacts_validated": False,
                       "write_timeline": binding}
            (root / "receipt.json").write_text(json.dumps(receipt))
            case = cloud.summarize_case(root)
            self.assertEqual(case["write_timeline"]["coverage"]["status"], "failed")
            self.assertFalse(case["write_timeline"]["coverage"]["available"])
            with self.assertRaises(ValueError):
                cloud.admit_write_timeline(root, job, config, completed=True)
            path.write_text(path.read_text() + " ")
            with self.assertRaisesRegex(ValueError, "integrity"):
                cloud.summarize_case(root)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "integrity"):
                cloud.summarize_case(root)

    def test_report_retains_failed_invalid_partial_and_rejects_completed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            case = root / "cases" / "trace"
            case.mkdir(parents=True)
            job, config, data, path = self.fixture(case)
            job.update(repeat=0, value_size=8, cohort="write_timeline_diagnostics")
            data["timeline"]["live_groups"] = [{
                "owner": 0, "reservation_token": None, "owner_validation_ns": None,
                "owner_persistence": {"unexpected": 1},
                "batches": [{"epoch": 1, "sequence": 0}]}]
            data["timeline"]["dropped_live_groups"] = 0
            path.write_text(json.dumps(data))
            receipt = {"job": job, "status": "failed", "error": "deadline exceeded",
                       "raw_artifacts_validated": False}
            (case / "receipt.json").write_text(json.dumps(receipt))
            (root / "matrix.json").write_text(json.dumps({"jobs": [job]}))
            row = cloud.report(root)["cases"][0]
            self.assertEqual(row["status"], "failed")
            self.assertIn("deadline exceeded", row["error"])
            self.assertIn("invalid live group storage persistence profile", row["error"])
            self.assertTrue((root / "comparison.csv").is_file())
            with self.assertRaisesRegex(ValueError, "invalid live group storage persistence profile"):
                cloud.admit_write_timeline(case, job, config, completed=True)
            receipt["status"] = "completed"
            (case / "receipt.json").write_text(json.dumps(receipt))
            with patch.object(cloud, "summarize_case", side_effect=ValueError(
                    "invalid live group storage persistence profile")):
                with self.assertRaisesRegex(ValueError, "invalid live group storage persistence profile"):
                    cloud.report(root)


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
            "backends": list(cloud.BACKENDS),
            "keys": 4096, "max_cases": 924, "offered_rates": [1000, 10000, 50000],
            "offered_seconds": 8, "case_deadline_seconds": 180,
            "hard_deadline_seconds": 10800, "export_reserve_seconds": 600,
        }
    def test_combined_matrix_preserves_standard_and_sealed_populations(self):
        backends = ["native", "tidehunter_tcp", "tidehunter_verbs_native",
                    "tidehunter_nvme_rdma", "tikv", "cassandra", "foundationdb",
                    "valkey_tcp", "valkey_rdma", "garnet", "kvrocks"]
        diagnostics = {"write_profile_limit": 256, "read_profile_limit": 256,
                       "write_timeline_limit": 8192}
        spec = self.spec(1) | {"mode": "standard-with-nvmeof", "backends": backends,
                               "diagnostics": diagnostics, "hard_deadline_seconds": 14400}
        jobs = cloud.make_matrix(spec)
        standard = cloud.make_matrix(spec | {"mode": "standard",
                                    "backends": [b for b in backends if b not in cloud.NVME_BACKENDS]})
        focused = cloud.make_matrix(spec | {"mode": "nvmeof-focused",
                                   "backends": backends[:4]})
        self.assertEqual(len(standard), 280)
        self.assertEqual(len(focused), 129)
        self.assertEqual(len(jobs), 409)
        self.assertEqual(jobs[:len(standard)], standard)
        self.assertEqual([{k: v for k, v in j.items() if k != "case_id"}
                          for j in jobs[len(standard):]],
                         [{k: v for k, v in j.items() if k != "case_id"} for j in focused])
        self.assertEqual(len({j["case_id"] for j in jobs}), len(jobs))
        expected = {"native": 70, "tidehunter_tcp": 69, "tidehunter_verbs_native": 69,
                    "tidehunter_nvme_rdma": 26, "tikv": 25, "cassandra": 25,
                    "foundationdb": 25, "valkey_tcp": 25, "valkey_rdma": 25,
                    "garnet": 25, "kvrocks": 25}
        self.assertEqual({b: sum(j["backend"] == b for j in jobs) for b in backends}, expected)
        seen, previous = set(), None
        for job in jobs[len(standard):]:
            if "sealed_dataset" not in job:
                continue
            source = job["sealed_dataset"]["source_id"]
            if source != previous:
                self.assertNotIn(source, seen)
                seen.add(source)
                previous = source
            self.assertEqual(job["mix"]["read"] + job["mix"]["get_many"], 100)
            config = cloud.make_run_config(job, Path("/tmp/case"), {}, "source")
            self.assertFalse(config["phases"]["preload"])
            self.assertTrue(config["phases"]["verify_existing_dataset"])
        self.assertTrue(all("sealed_dataset" in j for j in jobs if j["backend"] in cloud.NVME_BACKENDS))
        with self.assertRaisesRegex(ValueError, "409 cases"):
            cloud.make_matrix(spec | {"max_cases": 408})
        with self.assertRaises(ValueError):
            cloud.make_matrix(spec | {"repeats": 2})
        self.assertEqual(cloud.make_matrix(spec | {"hard_deadline_seconds": 21600}), jobs)
        with self.assertRaises(ValueError):
            cloud.make_matrix(spec | {"hard_deadline_seconds": 21601})
        both = cloud.make_matrix(spec | {"backends": backends + ["tidehunter_nvme_tcp"]})
        self.assertEqual(len(both), 435)
        self.assertTrue(all("sealed_dataset" in j for j in both
                            if j["backend"] in cloud.NVME_BACKENDS))
        baseline_spec = spec | {"mode": "standard", "backends": [b for b in backends
                                if b not in cloud.NVME_BACKENDS]}
        old = cloud.make_matrix(baseline_spec | {"diagnostics": {"write_profile_limit": 256,
                                                                 "read_profile_limit": 256}})
        self.assertEqual(standard[:len(old)], old)

    def test_live_matrix_matches_distributed_primary_and_keeps_sealed_separate(self):
        backends = ["native", "tidehunter_tcp", "tidehunter_verbs_native",
                    "tidehunter_nvme_rdma", "tikv", "cassandra", "foundationdb",
                    "valkey_tcp", "valkey_rdma", "garnet", "kvrocks"]
        spec = self.spec(1) | {"mode": "standard-with-live-nvmeof", "backends": backends,
            "diagnostics": {"write_profile_limit": 256, "read_profile_limit": 256,
                            "write_timeline_limit": 8192},
            "hard_deadline_seconds": 14400}
        jobs = cloud.make_matrix(spec)
        self.assertEqual(len(jobs), 461)
        def primary(backend):
            return sorted((j["value_size"], j["profile"], j["concurrency"],
                           j["measurement_operations"], j["warmup_operations"],
                           json.dumps(j["mix"], sort_keys=True))
                          for j in jobs if j["backend"] == backend and j["cohort"] == "primary_rf1")
        self.assertEqual(primary("tidehunter_nvme_rdma"), primary("tidehunter_tcp"))
        live = [j for j in jobs if j.get("nvmeof_route") == "live"]
        sealed = [j for j in jobs if j.get("nvmeof_route") == "sealed_read"]
        self.assertEqual(len(live), 37)
        self.assertEqual(len(sealed), 26)
        self.assertTrue(all("sealed_dataset" not in j for j in live))
        self.assertTrue(all("sealed_dataset" in j for j in sealed))
        self.assertTrue(any(j["mix"]["delete"] for j in live))
        self.assertTrue(any(j["mix"]["put"] for j in live))
        self.assertTrue(any(j["mix"]["put_batch"] for j in live))
        self.assertEqual(len({j["case_id"] for j in jobs}), len(jobs))
        with self.assertRaisesRegex(ValueError, "461 cases"):
            cloud.make_matrix(spec | {"max_cases": 460})

    def test_spdk_live_twins_cover_every_live_rdma_case(self):
        spec = self.spec(1) | {"mode": "standard-with-live-nvmeof",
            "backends": ["native", "tidehunter_tcp", "tidehunter_nvme_rdma"],
            "spdk_comparison": True,
            "diagnostics": {"write_profile_limit": 256, "read_profile_limit": 256,
                            "write_timeline_limit": 8192}}
        jobs = cloud.make_matrix(spec)
        live = [j for j in jobs if j.get("nvmeof_route") == "live"
                and j["backend"] == "tidehunter_nvme_rdma"]
        kernel = [j for j in live if j.get("nvmeof_target", "kernel") == "kernel"]
        spdk = [j for j in live if j.get("nvmeof_target") == "spdk"]
        def workload(job):
            return {k: v for k, v in job.items() if k not in ("case_id", "nvmeof_target")}
        self.assertEqual(len(kernel), len(spdk))
        self.assertTrue(kernel)
        self.assertEqual([workload(j) for j in kernel], [workload(j) for j in spdk])
        self.assertTrue({"primary_rf1", "cold_os_cache", "coordinator_cache_configured",
                         "write_stage_diagnostics", "read_stage_diagnostics",
                         "write_timeline_diagnostics"} <= {j["cohort"] for j in spdk})
        self.assertTrue(all(cloud.comparison_backend(j) == "tidehunter_spdk_live_rdma" for j in spdk))
        self.assertTrue(any(cloud.comparison_backend(j) == "tidehunter_spdk_rdma" for j in jobs))
        self.assertEqual(len({j["case_id"] for j in jobs}), len(jobs))

    def test_nvme_diagnostic_identity_distinguishes_historical_and_sealed(self):
        job = {"backend": "tidehunter_nvme_rdma"}
        self.assertEqual(cloud.diagnostic_backend(job), "tidehunter_nvme_rdma")
        self.assertEqual(cloud.diagnostic_backend(job | {"nvmeof_route": "live"}),
                         "tidehunter_live_nvmeof")
        self.assertEqual(cloud.diagnostic_backend(job | {"nvmeof_route": "sealed_read"}),
                         "tidehunter_nvme_rdma_sealed_read")

    def test_live_measurement_requires_unmodified_boundary_counters_and_io(self):
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        end = zero | {"completed_read_ops": 2, "completed_read_bytes": 8192,
                      "completed_write_ops": 1, "completed_write_bytes": 4096}
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero}, "end": {"live_nvmeof": end},
                    "live_nvmeof": {"available": True, "counters": end.copy()}}
        job = {"mix": cloud.mix(read=50, put=50)}
        self.assertTrue(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["live_nvmeof"]["counters"]["completed_write_ops"] = 0
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["live_nvmeof"]["counters"]["completed_write_ops"] = 1
        evidence["end"]["live_nvmeof"]["payload_rpc_requests"] = 1
        evidence["live_nvmeof"]["counters"]["payload_rpc_requests"] = 1
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])

    def test_live_cache_hit_cohort_can_avoid_measurement_io_with_accounting(self):
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero, "coordinator": {"hits": 10}},
                    "end": {"live_nvmeof": zero, "coordinator": {"hits": 110}},
                    "live_nvmeof": {"available": True, "counters": zero},
                    "deltas": {"coordinator": {"available": True, "counters": {"hits": 100}}}}
        job = {"mix": cloud.mix(read=100), "coordinator_cache_bytes": 1024,
               "measurement_operations": 100}
        self.assertTrue(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])
        evidence["deltas"]["coordinator"]["counters"]["hits"] = 99
        self.assertFalse(cloud.qualify_live_nvme_measurement(evidence, job)["qualified"])

    def test_live_route_requires_actual_matching_kernel_namespace(self):
        zero = dict.fromkeys(cloud.LIVE_NVME_COUNTERS, 0)
        end = zero | {"completed_write_ops": 1, "completed_write_bytes": 4096}
        host = [1] * 16
        expected = {"transport": "rdma", "addresses": ["10.0.0.1"], "service": 4420,
                    "host_id": host, "host_nqn": "nqn.2014-08.org.nvmexpress:uuid:01010101-0101-0101-0101-010101010101"}
        route = {"transport": "rdma", "address": "10.0.0.1", "service": 4420,
                 "host_id": host, "host_nqn": expected["host_nqn"],
                 "namespace_identity": {"namespace_path": "/dev/nvme0n1",
                    "controller_instance": 0, "nqn": "nqn.2026-01.example:wal",
                    "uuid": [2] * 16, "nsid": 1, "bytes": 4096, "block_size": 4096}}
        evidence = {"measurement_attempted": True, "measurement_drained": True,
                    "start": {"live_nvmeof": zero}, "end": {"live_nvmeof": end | {"routes": [route]}},
                    "live_nvmeof": {"available": True, "counters": end}}
        job = {"backend": "tidehunter_nvme_rdma", "mix": cloud.mix(put=100)}
        self.assertTrue(cloud.qualify_live_nvme_route(evidence, job, expected)["qualified"])
        route["transport"] = "tcp"
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, job, expected)["qualified"])
        route["transport"] = "rdma"
        spdk_job = job | {"nvmeof_target": "spdk"}
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, spdk_job, expected)["qualified"])
        route["target_implementation"] = "spdk"
        route["namespace_identity"]["target_implementation"] = "spdk"
        route["spdk"] = {"rpc_socket": "/run/spdk.sock", "image_path": "/wal/file",
                         "bdev_name": "wal0", "pid": 12, "process_start_ticks": 34}
        route["writer"] = {"file_identity": {"path": "/wal/file", "device": 8, "inode": 9,
                                               "bytes": 4096, "block_size": 4096}}
        self.assertTrue(cloud.qualify_live_nvme_route(evidence, spdk_job, expected)["qualified"])
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, job, expected)["qualified"])
        route["writer"]["file_identity"]["inode"] = 0
        self.assertFalse(cloud.qualify_live_nvme_route(evidence, spdk_job, expected)["qualified"])

    def test_timeline_twins_have_matched_counts_and_no_legacy_profile(self):
        spec = self.nvmeof_spec() | {"mode": "standard-with-nvmeof",
                "backends": ["native", "tidehunter_tcp", "tidehunter_verbs_native", "tidehunter_nvme_rdma"],
                "diagnostics": {"write_profile_limit": 256, "write_timeline_limit": 8192}}
        jobs = cloud.make_matrix(spec)
        timeline = [j for j in jobs if j["cohort"] == "write_timeline_diagnostics"]
        self.assertEqual(len(timeline), 6)
        for job in timeline:
            self.assertEqual(job["diagnostics"], {"write_profile_limit": 0, "write_timeline_limit": 8192})
            candidates = [j for j in jobs if j["backend"] == job["backend"]
                          and j["value_size"] == job["value_size"]
                          and j["mix"] == job["mix"] and j["concurrency"] == job["concurrency"]
                          and j["cohort"] in ("primary_rf1", "native_write_regression")
                          and j["profile"] in ("write-concurrent", "write-c16-baseline", "write-batch32-c16-baseline")]
            self.assertTrue(any(j["measurement_operations"] == job["measurement_operations"]
                                and j["warmup_operations"] == job["warmup_operations"]
                                and j["keys"] == job["keys"] for j in candidates))
            config = cloud.make_run_config(job, Path("/tmp/case"), {}, "source")
            self.assertEqual(config["diagnostics"], job["diagnostics"])
        self.assertTrue(all("diagnostics" not in j for j in jobs if j["cohort"] == "primary_rf1"))
        for value in (-1, 65537, True, "8192"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud.make_matrix(spec | {"diagnostics": {"write_timeline_limit": value}})

    def test_portable_backends_preserve_standard_common_workloads(self):
        spec = self.spec(1)
        selected = cloud.make_matrix(spec | {"backends": ["valkey_tcp", "valkey_rdma",
                                                         "tidehunter_verbs", "tidehunter_verbs_native"]})
        workload_fields = ("repeat", "profile", "cohort", "value_size", "keys", "mix",
                           "concurrency", "measurement_operations", "warmup_operations",
                           "offered_ops_per_second", "distribution")
        common = lambda backend: sorted(
            cloud.analyze.canonical({k: job[k] for k in workload_fields})
            for job in selected if job["backend"] == backend and job["cohort"] == "primary_rf1")
        self.assertTrue(common("valkey_tcp"))
        self.assertEqual(common("valkey_tcp"), common("valkey_rdma"))
        self.assertEqual(common("valkey_tcp"), common("tidehunter_verbs"))
        self.assertEqual(common("valkey_tcp"), common("tidehunter_verbs_native"))


    def nvmeof_spec(self):
        return self.spec(1) | {
            "mode": "nvmeof-focused",
            "backends": ["native", "tidehunter_tcp", "tidehunter_nvme_tcp"],
        }

    def test_nvmeof_matrix_requires_controls_and_one_repetition(self):
        spec = self.nvmeof_spec()
        for backend in spec["backends"]:
            with self.subTest(missing=backend), self.assertRaises(ValueError):
                cloud.make_matrix(spec | {"backends": [b for b in spec["backends"] if b != backend]})
        with self.assertRaises(ValueError):
            cloud.make_matrix(spec | {"repeats": 2})
        with self.assertRaises(ValueError):
            cloud.make_matrix(spec | {"backends": spec["backends"] + ["valkey_tcp"]})
        jobs = cloud.make_matrix(spec | {"backends": spec["backends"] + ["tidehunter_efa_native"]})
        self.assertEqual({job["backend"] for job in jobs}, set(spec["backends"]) | {"tidehunter_efa_native"})
        self.assertEqual(len(jobs), 125)

    def test_portable_nvme_rdma_selection_keeps_logical_data_and_distinct_sources(self):
        for verbs in ("tidehunter_verbs", "tidehunter_verbs_native"):
            for extra in ([], ["tidehunter_nvme_tcp"]):
                backends = ["native", "tidehunter_tcp", verbs, "tidehunter_nvme_rdma"] + extra
                jobs = cloud.make_matrix(self.nvmeof_spec() | {"backends": backends})
                reads = [job for job in jobs if job["cohort"] == "sealed_read"]
                writes = [job for job in jobs if job["cohort"] == "native_write_regression"]
                self.assertEqual({job["backend"] for job in reads}, set(backends))
                self.assertEqual({job["backend"] for job in writes}, {"native", "tidehunter_tcp", verbs})
                for size in (64, 256, 1024, 8192, 65536):
                    block = [job for job in reads if job["value_size"] == size]
                    self.assertEqual(len({job["sealed_dataset"]["data_id"] for job in block}), 1)
                    sources = {backend: {job["sealed_dataset"]["source_id"] for job in block
                                         if job["backend"] == backend} for backend in backends}
                    self.assertTrue(all(len(source) == 1 for source in sources.values()))
                    self.assertEqual(sources["tidehunter_tcp"], sources["tidehunter_nvme_rdma"])
                    self.assertEqual(len({next(iter(sources[backend])) for backend in
                                          ("native", "tidehunter_tcp", verbs)}), 3)
                    if extra:
                        self.assertEqual(sources["tidehunter_tcp"], sources["tidehunter_nvme_tcp"])
                self.assertTrue(all(job["coordinator_cache_bytes"] == 0 for job in reads))
                self.assertTrue(all(job["mix"]["read"] + job["mix"]["get_many"] == 100 for job in reads))
        for backends in (["native", "tidehunter_tcp", "tidehunter_verbs"],
                         ["native", "tidehunter_nvme_rdma"], ["tidehunter_tcp", "tidehunter_nvme_rdma"],
                         ["native", "tidehunter_tcp", "tidehunter_nvme_rdma", "valkey_rdma"]):
            with self.subTest(backends=backends), self.assertRaises(ValueError):
                cloud.make_matrix(self.nvmeof_spec() | {"backends": backends})
        with self.assertRaises(ValueError):
            cloud.make_matrix(self.nvmeof_spec() | {"mode": "standard",
                "backends": ["native", "tidehunter_tcp", "tidehunter_nvme_rdma"]})


    def test_nvmeof_matrix_has_complete_read_shapes_and_independent_twins(self):
        spec = self.nvmeof_spec()
        jobs = cloud.make_matrix(spec)
        self.assertEqual(len(jobs), 90)
        self.assertEqual(len({job["case_id"] for job in jobs}), len(jobs))
        self.assertEqual({job["repeat"] for job in jobs}, {0})
        reads = [job for job in jobs if job["cohort"] == "sealed_read"]
        expected = set()
        for backend in spec["backends"]:
            for size in (64, 256, 1024, 8192, 65536):
                shapes = (("scalar", 1), ("scalar", 16), ("batch32", 1), ("batch32", 16)) \
                    if size in (1024, 8192) else (("scalar", 1),)
                for shape, concurrency in shapes:
                    for tier in ("first_touch", "repeat_read"):
                        expected.add((backend, size, shape, concurrency, tier, False))
                        if backend != "native" and (size, shape, concurrency) in (
                                (1024, "scalar", 1), (8192, "batch32", 16)):
                            expected.add((backend, size, shape, concurrency, tier, True))
        actual = set()
        twins = {}
        for job in reads:
            shape = "batch32" if job["mix"]["get_many"] else "scalar"
            profiled = job["sealed_dataset"]["profiled"]
            tier = job["sealed_dataset"]["tier"]
            key = (job["backend"], job["value_size"], shape, job["concurrency"], tier)
            actual.add((*key, profiled))
            twins.setdefault(key, []).append(job)
            self.assertEqual(job["mix"], cloud.mix(**{"get_many" if shape == "batch32" else "read": 100}))
            self.assertEqual(job["read_lanes"], 0 if job["backend"] == "native" else
                             (4 if job["concurrency"] == 16 else 1))
            self.assertEqual(job["warmup_operations"], 0)
            self.assertEqual(job["measurement_operations"], 2048 if shape == "batch32" else 8192)
            self.assertEqual(job["coordinator_cache_bytes"], 0)
            self.assertNotIn("owner_read_cache_bytes", job)
            self.assertEqual(job["measurement_key_sweep"], tier == "first_touch")
            self.assertEqual(job["drop_os_caches"], tier == "first_touch")
            self.assertEqual(job["evict_mapped_wal_before_measurement"],
                             tier == "first_touch" and job["backend"] not in cloud.NVME_BACKENDS)
            self.assertEqual(job["keys"], 65536 if job["value_size"] in (1024, 8192) else 8192)
        self.assertEqual(actual, expected)
        self.assertEqual(len(reads), len(expected))
        for key, pair in twins.items():
            self.assertEqual(len(pair), 2 if (*key, True) in expected else 1)
            baseline = next(job for job in pair if not job["sealed_dataset"]["profiled"])
            self.assertNotIn("diagnostics", baseline)
            if len(pair) == 2:
                diagnostic = next(job for job in pair if job["sealed_dataset"]["profiled"])
                self.assertNotEqual(baseline["case_id"], diagnostic["case_id"])
                self.assertEqual(baseline["sealed_dataset"]["source_id"], diagnostic["sealed_dataset"]["source_id"])
                self.assertEqual(diagnostic["diagnostics"]["read_profile_limit"], 256)

        writes = [job for job in jobs if job["cohort"] == "native_write_regression"]
        scalar = [job for job in writes if job["mix"] == cloud.mix(put=100)]
        batch = [job for job in writes if job["mix"] == cloud.mix(put_batch=100)]
        self.assertEqual(len(scalar), 12)
        self.assertEqual(len(batch), 4)
        self.assertEqual({(job["backend"], job["value_size"]) for job in scalar if "diagnostics" in job},
                         {("tidehunter_tcp", 1024), ("tidehunter_tcp", 8192)})
        self.assertEqual({(job["backend"], job["value_size"]) for job in scalar if "diagnostics" not in job},
                         {(backend, size) for backend in ("native", "tidehunter_tcp")
                          for size in (64, 256, 1024, 8192, 65536)})
        self.assertTrue(all("diagnostics" not in job for job in batch))
        self.assertEqual({(job["value_size"], job["measurement_operations"]) for job in batch},
                         {(1024, 2048), (8192, 2048)})
        self.assertTrue(all(job["measurement_operations"] == 4096 for job in scalar))
        self.assertEqual({job["backend"] for job in writes}, {"native", "tidehunter_tcp"})
        self.assertTrue(all(job["concurrency"] == 16 and "sealed_dataset" not in job for job in writes))
        self.assertLess(max(index for index, job in enumerate(jobs) if job["cohort"] == "sealed_read"),
                        min(index for index, job in enumerate(jobs) if job["cohort"] == "native_write_regression"))

    def test_nvmeof_sealed_source_identity_and_read_only_run_configs(self):
        spec = self.nvmeof_spec()
        jobs = [job for job in cloud.make_matrix(spec) if job["cohort"] == "sealed_read"]
        identities = {}
        for job in jobs:
            sealed = job["sealed_dataset"]
            expected = f"sealed-v2-seed{spec['seed']}-size{job['value_size']}-keys{job['keys']}"
            self.assertEqual(sealed["source_id"], expected + ("-native" if job["backend"] == "native" else "-tcp"))
            self.assertEqual(sealed["data_id"], expected)
            self.assertEqual(sealed["seed"], spec["seed"])
            self.assertEqual(sealed["page_cache_policy"],
                             "drop_at_measurement_gate" if job["drop_os_caches"] else "not_dropped")
            self.assertEqual(sealed["target_cache"], "buffered_file_page_cache" if job["backend"] == "tidehunter_nvme_tcp"
                             else "not_applicable")
            config = cloud.make_run_config(job, Path("/tmp/owned-case") / job["case_id"], {}, "source")
            self.assertFalse(config["phases"]["preload"])
            self.assertTrue(config["phases"]["verify_existing_dataset"])
            self.assertEqual(config["phases"]["evict_mapped_wal_before_measurement"],
                             job["evict_mapped_wal_before_measurement"])
            self.assertEqual(config["seed"], spec["seed"])
            self.assertEqual(config["dataset"]["keys"], job["keys"])
            self.assertEqual(config["phases"].get("measurement_key_sweep", False),
                             sealed["tier"] == "first_touch")
            self.assertEqual(config["dataset"]["value_size"], job["value_size"])
            identities.setdefault(job["value_size"], set()).add((sealed["data_id"], config["namespace"]))
        self.assertTrue(all(len(values) == 1 for values in identities.values()))
        self.assertEqual(len({next(iter(values))[0] for values in identities.values()}), 5)
        self.assertEqual(len({next(iter(values))[1] for values in identities.values()}), 5)

    def test_nvmeof_measurement_qualification_requires_complete_clean_interval(self):
        def evidence():
            def reader(count):
                return {"identity": {"transport": "tcp"}, "counters": {"available": True, "counters": {
                    "eligible_hits": count, "ineligible_fallbacks": 0,
                    "io_submissions": count, "io_completions": count,
                    "io_errors": 0, "io_timeouts": 0,
                }}}
            return {"nvmeof": {"available": True, "readers": {"0": reader(3), "1": reader(5)}},
                    "deltas": {"coordinator": {"available": True, "counters": {
                        "read_rpc_count": 0, "hits": 0}}}}
        targets = [{"target": {"exports": [{"target_receipt": {
            "target_cache": "buffered_file_page_cache", "target_cache_verified": True,
            "selected_transport": "tcp"}} for _ in range(2)]}}]
        def qualify(data, target_rows=targets):
            return cloud.qualify_nvme_measurement(data, 2, "tcp", target_rows)

        result = qualify(evidence())
        self.assertTrue(result["qualified"])
        self.assertEqual(result["reasons"], [])
        self.assertEqual(result["nvmeof_totals"], {
            "eligible_hits": 8, "ineligible_fallbacks": 0, "io_submissions": 8,
            "io_completions": 8, "io_errors": 0, "io_timeouts": 0})
        self.assertEqual(result["coordinator_read_rpc_count"], 0)
        self.assertEqual(result["coordinator_cache_hits"], 0)

        missing = evidence()
        del missing["nvmeof"]["readers"]["1"]
        result = qualify(missing)
        self.assertFalse(result["qualified"])
        self.assertIsNone(result["nvmeof_totals"])
        missing = evidence()
        del missing["nvmeof"]["readers"]["0"]["counters"]["counters"]["io_completions"]
        result = qualify(missing)
        self.assertFalse(result["qualified"])
        self.assertIsNone(result["nvmeof_totals"])
        missing = evidence()
        del missing["deltas"]["coordinator"]["counters"]["read_rpc_count"]
        result = qualify(missing)
        self.assertFalse(result["qualified"])
        self.assertIsNone(result["coordinator_read_rpc_count"])
        self.assertIsNone(result["coordinator_cache_hits"])

        for field, value in (("ineligible_fallbacks", 1), ("io_errors", 1),
                             ("io_timeouts", 1), ("io_completions", 2)):
            with self.subTest(field=field):
                invalid = evidence()
                invalid["nvmeof"]["readers"]["0"]["counters"]["counters"][field] = value
                self.assertFalse(qualify(invalid)["qualified"])
        for field in ("read_rpc_count", "hits"):
            with self.subTest(field=field):
                invalid = evidence()
                invalid["deltas"]["coordinator"]["counters"][field] = 1
                self.assertFalse(qualify(invalid)["qualified"])
        invalid = evidence()
        invalid["nvmeof"]["readers"]["0"]["identity"]["transport"] = None
        self.assertFalse(qualify(invalid)["qualified"])
        self.assertFalse(qualify(evidence(), [])["qualified"])
        self.assertFalse(qualify(evidence(), [{"target": {"exports": [
            {"target_receipt": {"target_cache": "buffered_file_page_cache",
                                "target_cache_verified": False, "selected_transport": "tcp"}}]}}])["qualified"])

    def test_short_campaign_keeps_existing_cases_and_prioritizes_controls(self):
        spec = self.spec(1)
        jobs = cloud.make_matrix(spec | {"mode": "read-tier-focused"})
        self.assertEqual(len(jobs), 142)
        self.assertEqual(len({j["case_id"] for j in jobs}), len(jobs))
        self.assertTrue(all(j["read_tier"]["name"] in
                            ("native_hot_wal_lru0_pagecache", "native_cold_os_lru0", "cold_device")
                            for j in jobs[:12]))
        selected = spec | {"mode": "read-tier-focused",
                           "backends": ["native", "tidehunter_tcp", "tidehunter_efa_native", "valkey_tcp"]}
        smaller = cloud.make_matrix(selected)
        self.assertEqual(len(smaller), 69)
        self.assertEqual({j["backend"] for j in smaller}, set(selected["backends"]))
        self.assertEqual(sum(j["profile"] == "write-concurrent" for j in smaller), 5)
        self.assertEqual(sum(j["profile"] == "read-many32" for j in smaller), 4)
        with self.assertRaises(ValueError):
            cloud.make_matrix(self.spec(2) | {"mode": "read-tier-focused"})
    def test_short_campaign_includes_separate_write_profiles_when_requested(self):
        spec = self.spec(1) | {"mode": "read-tier-focused",
                              "diagnostics": {"write_profile_limit": 256, "read_profile_limit": 256}}
        jobs = cloud.make_matrix(spec)
        self.assertEqual(len(jobs), 147)
        diagnostics = [j for j in jobs if j["cohort"] == "write_stage_diagnostics"]
        self.assertEqual({j["backend"] for j in diagnostics},
                         {"native"} | (set(spec["backends"]) & set(cloud.TRANSPORTS)))
        self.assertTrue(all(j["diagnostics"]["write_profile_limit"] == 256
                            and j["measurement_operations"] == 4096 for j in diagnostics))
        self.assertEqual(sum(j["cohort"] == "primary_rf1" and j["profile"] == "write-concurrent"
                             for j in jobs), 11)
        selected = spec | {"backends": ["native", "tidehunter_tcp", "tidehunter_efa_native", "valkey_tcp"]}
        self.assertEqual(len(cloud.make_matrix(selected)), 72)
        timeline_jobs = cloud.make_matrix(selected | {"diagnostics": {
            "write_profile_limit": 256, "read_profile_limit": 256, "write_timeline_limit": 8192}})
        timelines = [j for j in timeline_jobs if j["cohort"] == "write_timeline_diagnostics"]
        self.assertEqual(len(timeline_jobs), 74)
        self.assertEqual({j["backend"] for j in timelines}, {"tidehunter_tcp", "tidehunter_efa_native"})
        self.assertTrue(all(j["measurement_operations"] == 4096 and j["diagnostics"] ==
                            {"write_profile_limit": 0, "write_timeline_limit": 8192} for j in timelines))



    def test_focused_tiers_are_ordered_matched_and_bounded(self):
        jobs = cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
        names = list(dict.fromkeys(j["read_tier"]["name"] for j in jobs))
        self.assertEqual(names, ["remote", "coordinator_hit", "wal_warm",
                                 "native_hot_wal_lru0_pagecache", "cold_device",
                                 "native_cold_os_lru0", "bounded_mixed"])
        self.assertTrue(all("owner_read_cache_bytes" not in job and "resident_reads" not in job
                            for job in jobs))
        native = [j for j in jobs if j["backend"] == "native"]
        self.assertEqual(len(native), 10)
        self.assertTrue(all("diagnostics" not in j for j in native))
        remote = [j for j in jobs if j["backend"] != "native"]
        twins = {}
        for job in remote:
            twins.setdefault((job["read_tier"]["name"], job["read_tier"]["source_id"]), []).append(job)
        for first, second in twins.values():
            self.assertFalse(first["read_tier"]["profiled"])
            self.assertTrue(second["read_tier"]["profiled"])
            self.assertEqual(first["read_tier"]["source_id"], second["read_tier"]["source_id"])
            self.assertEqual(first["mix"], second["mix"])
            a = cloud.make_run_config(first, Path("/tmp/owned-case"), {}, "source")
            b = cloud.make_run_config(second, Path("/tmp/owned-case"), {}, "source")
            self.assertEqual(a["namespace"], b["namespace"])
            self.assertEqual(a["dataset"], b["dataset"])
            self.assertEqual(a["phases"], b["phases"])
            self.assertNotIn("resident_reads", first)
        contended = [job for job in jobs if "-c16-" in job["profile"]]
        self.assertTrue(contended)
        self.assertTrue(all(job["concurrency"] == 16 and job["read_lanes"] == (0 if job["backend"] == "native" else 4)
                            for job in contended))
        self.assertEqual({(job["value_size"], job["mix"]["get_many"]) for job in contended}, {(1024, 0), (8192, 100)})
        self.assertTrue(all(job["warmup_key_sweep"] and
                            job["warmup_operations"] * (32 if job["mix"]["get_many"] else 1) >= job["keys"]
                            for job in contended))
        with self.assertRaises(ValueError):
            cloud.make_matrix(self.spec(1) | {"mode": "read-tier", "backends": ["native"]})

    def test_focused_data_identity_is_shared_across_backends_and_profiles(self):
        jobs = cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
        selected = [job for job in jobs if job["value_size"] == 8192]
        self.assertEqual({job["backend"] for job in selected},
                         {"native"} | (set(self.spec(1)["backends"]) & set(cloud.TRANSPORTS)))
        self.assertGreater(len({job["read_tier"]["source_id"] for job in selected}), 4)
        self.assertEqual(len({job["read_tier"]["data_id"] for job in selected}), 1)
        configs = [cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source") for job in selected]
        self.assertEqual(len({config["namespace"] for config in configs}), 1)
        self.assertEqual(len({config["seed"] for config in configs}), 1)
        self.assertEqual(len({json.dumps(config["dataset"], sort_keys=True) for config in configs}), 1)
        self.assertGreater(len({json.dumps(config["workload"], sort_keys=True) for config in configs}), 1)
        other = next(job for job in jobs if job["value_size"] == 1024)
        self.assertNotEqual(other["read_tier"]["data_id"], selected[0]["read_tier"]["data_id"])

    def test_pure_cache_budgets_fit_working_set_and_fleet_bounds(self):
        jobs = cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
        pure = [j for j in jobs if j["read_tier"]["name"] == "coordinator_hit"]
        self.assertTrue(all(j["coordinator_cache_bytes"] >= 2 * j["keys"] * (j["value_size"] + 512)
                            for j in pure))
        large = next(j for j in pure if j["value_size"] == 65536)
        self.assertGreaterEqual(large["coordinator_cache_bytes"], 1024**3)
        self.assertGreaterEqual(large["coordinator_read_memory_allowance_bytes"],
                                large["coordinator_cache_bytes"] + 64 * 1024**2)
        mixed = [j for j in jobs if j["read_tier"]["name"] == "bounded_mixed"]
        self.assertTrue(all(j["coordinator_cache_bytes"] == 1024**2 for j in mixed))
        controller.validate_read_tier_memory(jobs, {"client_memory_mib": 16384, "server_memory_mib": 16384})
        with self.assertRaisesRegex(ValueError, "fleet memory budget"):
            controller.validate_read_tier_memory(jobs, {"client_memory_mib": 1024, "server_memory_mib": 16384})

    def test_focused_report_exposes_missing_and_partial_trace_capability(self):
        spec = self.spec(1) | {"mode": "read-tier"}
        jobs = cloud.make_matrix(spec)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "matrix.json").write_text(json.dumps({"spec": spec, "jobs": jobs}))
            report = cloud.report(root)
            self.assertFalse(report["trace_capability"]["identity_backend_available"])
            self.assertIn("probe not completed", report["trace_capability"]["gaps"][0])
            receipt = {"schema_version": 1, "identity_backend_available": False,
                       "hosts": [], "gaps": [{"host_index": 3, "gaps": ["BPF unavailable"]}]}
            (root / "trace-capability.json").write_text(json.dumps(receipt))
            self.assertEqual(cloud.report(root)["trace_capability"], receipt)

    def test_tier_qualification_never_infers_missing_or_cold_device_evidence(self):
        job = next(j for j in cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
                   if j["read_tier"]["name"] == "cold_device")
        proof = cloud.qualify_read_tier(job, None, {"status": "completed"})
        self.assertFalse(proof["qualified"])
        self.assertIsNone(proof["observed"]["wal_reads"])
        self.assertTrue(any("trace" in reason for reason in proof["reasons"]))

    def test_tier_plot_profiles_do_not_share_identity(self):
        jobs = cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
        identities = {(j["backend"], j["cohort"], j["profile"], j["value_size"], j["repeat"]) for j in jobs}
        self.assertEqual(len(identities), len(jobs))

    def test_cold_trace_loss_and_missing_counts_disqualify(self):
        job = next(j for j in cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
                   if j["read_tier"]["name"] == "cold_device")
        coord = {"available": True, "counters": {"hits": 0, "misses": 1, "read_rpc_count": 1}}
        owner_counts = {"wal_reads": 1, "wal_read_bytes": 256, "async_read_submissions": 1}
        def partitions(counts):
            return {"available": True, "partitions": {str(i): {"available": True, "counters": dict(counts)}
                                                       for i in range(5)}}
        evidence = {"measurement_drained": True, "deltas": {"coordinator": coord,
                    "owner_executor": partitions(owner_counts)}}
        trace = dict.fromkeys(("unmatched_completions", "unmatched_issues", "overlaps",
            "split_or_partial_completions", "requeues", "completion_errors", "malformed_events",
            "outstanding_overflow", "lost", "truncated"), 0)
        trace.update(available=True, qualified=False, read_issues=1, read_completions=1)
        receipt = {"status": "completed", "read_tier_effective": dict(job["read_tier"]["requested"]),
                   "cold_device_evidence": [{"available": False}, *[dict(trace) for _ in range(5)]]}
        proof = cloud.qualify_read_tier(job, evidence, receipt)
        self.assertFalse(proof["qualified"])
        self.assertEqual(proof["scope"], "measurement_counters_and_wider_collector_window")
        self.assertTrue(any("wider than measurement" in reason for reason in proof["reasons"]))
        receipt["cold_device_evidence"][2]["truncated"] = True
        self.assertFalse(cloud.qualify_read_tier(job, evidence, receipt)["qualified"])
        receipt["cold_device_evidence"][2] = {"available": False}
        self.assertFalse(cloud.qualify_read_tier(job, evidence, receipt)["qualified"])

    def test_warm_wal_does_not_claim_page_cache_residency(self):
        job = next(j for j in cloud.make_matrix(self.spec(1) | {"mode": "read-tier"})
                   if j["read_tier"]["name"] == "wal_warm")
        owner = {"available": True, "partitions": {
            str(i): {"available": True, "counters": {"wal_reads": 1}} for i in range(5)}}
        evidence = {"measurement_drained": True, "deltas": {
            "coordinator": {"available": True, "counters": {"hits": 0}},
            "owner_executor": owner}}
        receipt = {"status": "completed", "read_tier_effective": dict(job["read_tier"]["requested"])}
        proof = cloud.qualify_read_tier(job, evidence, receipt)
        self.assertFalse(proof["qualified"])
        self.assertTrue(any("page cache" in reason for reason in proof["reasons"]))
        del owner["partitions"]["0"]
        self.assertIsNone(cloud.qualify_read_tier(job, evidence, receipt)["observed"]["wal_reads"])

    def test_one_repeat_preserves_every_first_repeat_job_and_run_configuration(self):
        single = cloud.make_matrix(self.spec(1))
        historical = cloud.make_matrix(self.spec(3))
        self.assertEqual((len(single), len(historical)), (298, 894))
        first = [job for job in historical if job["repeat"] == 0]
        for job, old_job in zip(single, first):
            self.assertEqual({k: v for k, v in job.items() if k != "case_id"},
                             {k: v for k, v in old_job.items() if k != "case_id"})
            if job["cohort"] != "native_async_write":
                self.assertEqual(job["case_id"], old_job["case_id"])

    def test_write_diagnostics_are_a_distinct_cohort_without_reducing_any_work(self):
        normal = cloud.make_matrix(self.spec(1))
        diagnostic = cloud.make_matrix(self.spec(1) | {"diagnostics": {"write_profile_limit": 256}})
        self.assertEqual(len(diagnostic), len(normal) + 5)
        def identity(job):
            return job["backend"], job["repeat"], job["profile"], job["cohort"], job["value_size"]
        retained = {identity(job): job for job in diagnostic if job["cohort"] != "write_stage_diagnostics"}
        self.assertEqual(len(retained), len(normal))
        for before in normal:
            after = retained[identity(before)]
            self.assertEqual({key: value for key, value in before.items() if key != "case_id"},
                             {key: value for key, value in after.items() if key != "case_id"})
        selected = [job for job in diagnostic if job["cohort"] == "write_stage_diagnostics"]
        self.assertEqual({job["backend"] for job in selected},
                         {"native"} | (set(self.spec(1)["backends"]) & set(cloud.TRANSPORTS)))
        for after in selected:
            before = retained[(*identity(after)[:3], "primary_rf1", after["value_size"])]
            self.assertEqual(after["diagnostics"]["write_profile_limit"], 256)
            for field in ("mix", "concurrency", "keys", "measurement_operations", "warmup_operations",
                          "offered_ops_per_second", "distribution", "coordinator_cache_bytes",
                          "native_durability", "drop_os_caches"):
                self.assertEqual(before[field], after[field])


    def test_historical_fullset_qualification_rejects_fallback_and_missing_observations(self):
        job = {"cohort": "registered_ram_fullset_control", "warmup_key_sweep": True,
               "resident_reads": {"require_device_read": True}}
        resident = {name: 0 for name in ("message_read_requests", "grant_acquisitions",
            "grant_candidates", "rma_errors", "fallback_missing", "fallback_oversize",
            "fallback_capacity", "fallback_unsupported", "fallback_stale")}
        resident.update(rma_posts=1, rma_completions=1)
        owners = {"wal_reads": 0, "async_read_submissions": 0, "cache_hits": 0}
        def group(counters):
            return {"available": True, "partitions": {
                str(index): {"available": True, "counters": dict(counters)} for index in range(5)}}
        evidence = {"measurement_drained": True, "deltas": {
            "coordinator": {"available": True, "counters": {"hits": 0}},
            "client_resident": group(resident), "owner_executor": group(owners)}}
        self.assertTrue(cloud.qualify_fullset_read_path(job, evidence)["qualified"])
        evidence["deltas"]["client_resident"]["partitions"]["2"]["counters"]["message_read_requests"] = 1
        proof = cloud.qualify_fullset_read_path(job, evidence)
        self.assertFalse(proof["qualified"])
        self.assertEqual(proof["observed"]["message_read_requests"], 1)
        evidence["deltas"]["client_resident"]["partitions"].pop("2")
        proof = cloud.qualify_fullset_read_path(job, evidence)
        self.assertFalse(proof["qualified"])
        self.assertIsNone(proof["observed"]["message_read_requests"])


    def test_manual_read_diagnostics_are_propagated_for_mixed_cases(self):
        job = next(job for job in cloud.make_matrix(self.spec(1))
                   if job["backend"] == "tidehunter_tcp" and job["profile"] == "point-mixed")
        job["diagnostics"] = {"read_profile_limit": 7, "write_profile_limit": 0}
        self.assertEqual(cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source")["diagnostics"],
                         job["diagnostics"])

    def test_invalid_read_diagnostic_limits_are_rejected(self):
        for value in (True, -1, 65537, 0.5, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cloud.make_matrix(self.spec(1) | {"diagnostics": {"read_profile_limit": value}})

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
                expected.update((backend, "coordinator_cache_configured", "coordinator-cache", size)
                                for size in (256, 8192))
            if backend == "native":
                expected.update((backend, "native_pagecache", profile, size)
                                for profile in ("point-mixed", "read-many32") for size in spec["value_sizes"])
                expected.update((backend, "native_async_write", "write-concurrent", size)
                                for size in spec["value_sizes"])
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
                self.assertNotIn("owner_read_cache_bytes", job)
                self.assertEqual(job["native_durability"], "page_cache" if cohort in ("native_pagecache", "native_async_write") else "value_wal_barrier")
                self.assertEqual(job["distribution"], {"zipf": {"exponent": 0.99}} if profile == "zipf-read" else "uniform")
                self.assertNotIn("resident_reads", job)
                config = cloud.make_run_config(job, Path("/tmp/owned-case"), {}, "source", 180)
                if job["backend"] == "native":
                    options = config["backend"]["options"]
                    self.assertEqual(options["native_wal_geometry"], "native_default")
                    self.assertEqual(options["durability"], job["native_durability"])
                self.assertEqual(config["phases"], {"preload": True, "warmup_operations": warmup,
                    "aging_operations": 0, "measurement_operations": operations})
                self.assertFalse(config["qualification_only"])
                self.assertEqual(config["workload"]["batch_size"], 32)
                self.assertEqual(config["limits"]["max_operations"], job["keys"] + warmup + operations + 4096)
                self.assertEqual(config["limits"]["deadline_seconds"], 600 if cohort == "cold_os_cache" else 180)

    def test_native_async_reference_matches_primary_work_but_not_acknowledgement(self):
        jobs = cloud.make_matrix(self.spec(1))
        references = [job for job in jobs if job["cohort"] == "native_async_write"]
        self.assertEqual({job["value_size"] for job in references}, set(self.spec(1)["value_sizes"]))
        for reference in references:
            primary = next(job for job in jobs if job["backend"] == "native"
                           and job["cohort"] == "primary_rf1"
                           and job["profile"] == reference["profile"]
                           and job["value_size"] == reference["value_size"])
            for field in ("mix", "concurrency", "keys", "measurement_operations",
                          "warmup_operations", "distribution", "native_wal_geometry"):
                self.assertEqual(reference[field], primary[field])
            durable = cloud.make_run_config(primary, Path("/tmp/owned-case"), {}, "source")
            asynchronous = cloud.make_run_config(reference, Path("/tmp/owned-case"), {}, "source")
            self.assertEqual(durable["backend"]["options"]["durability"], "value_wal_barrier")
            self.assertEqual(asynchronous["backend"]["options"]["durability"], "page_cache")
            self.assertNotEqual(durable["namespace"], asynchronous["namespace"])
            with self.assertRaisesRegex(ValueError, "durability differs"):
                cloud.make_run_config(primary, Path("/tmp/owned-case"), {"durability": "page_cache"}, "source")
            with self.assertRaisesRegex(ValueError, "native_wal_geometry differs"):
                cloud.make_run_config(primary, Path("/tmp/owned-case"), {"native_wal_geometry": "small"}, "source")
            legacy = cloud.make_run_config(primary | {"native_wal_geometry": "small"},
                                           Path("/tmp/owned-case"), {}, "source")
            self.assertEqual(legacy["backend"]["options"]["native_wal_geometry"], "small")
            self.assertEqual(legacy["backend"]["options"]["durability"], "value_wal_barrier")

    def test_exact_case_bound_admits_all_jobs_but_never_truncates(self):
        for repeats, count in ((1, 298), (3, 894)):
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
        for name in ("garnet", "kvrocks"):
            jobs = [job for job in full_jobs if job["backend"] == name]
            self.assertEqual(len(jobs), 25)
            self.assertEqual(
                sorted(cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in jobs),
                sorted(cloud.analyze.canonical({k: v for k, v in job.items() if k not in ignored}) for job in valkey))
        expected = [job for job in full_jobs if job["backend"] == "valkey_tcp"]
        self.assertEqual(
            [cloud.analyze.canonical({k: v for k, v in job.items() if k != "case_id"}) for job in selected],
            [cloud.analyze.canonical({k: v for k, v in job.items() if k != "case_id"}) for job in expected])
        for invalid in ([], ["native", "native"], ["not-a-backend"]):
            with self.subTest(backends=invalid), self.assertRaises(ValueError):
                cloud.make_matrix(full | {"backends": invalid})
        self.assertEqual(len(cloud.make_matrix(self.spec(3) | {
            'diagnostics': {'write_profile_limit': 256}})), 909)

    def test_historical_frozen_jobs_report_without_current_completeness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            jobs = [job for job in cloud.make_matrix(self.spec(1))
                    if job["backend"] in cloud.LEGACY_BACKENDS]
            jobs.append({**jobs[0], "case_id": "historical-registered-value",
                         "cohort": "registered_ram_fullset_control"})
            frozen = {"schema_version": 1, "spec": self.spec(1) | {
                "backends": list(cloud.LEGACY_BACKENDS)}, "jobs": jobs}
            original = json.dumps(frozen)
            (root / "matrix.json").write_text(original)
            report = cloud.report(root)
            self.assertEqual(report["planned_cases"], len(jobs))
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

    def test_owner_evidence_is_integrity_bound_even_for_failed_cases(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            receipt["raw_artifacts_validated"] = False
            owner = directory / "owner-durability" / "node-0.durability.json"
            owner.parent.mkdir()
            owner.write_text('{"partition": 0}')
            facts = directory / "owner-provider-facts.json"
            facts.write_text('{"owners": []}')
            receipt["owner_durability_receipts"] = [{"path": str(owner.relative_to(directory)),
                "sha256": hashlib.sha256(owner.read_bytes()).hexdigest()}]
            receipt["owner_provider_facts"] = {"path": facts.name,
                "sha256": hashlib.sha256(facts.read_bytes()).hexdigest()}
            (directory / "receipt.json").write_text(json.dumps(receipt))
            result = cloud.summarize_case(directory)
            self.assertEqual(result["owner_durability_receipts"], [{"partition": 0}])
            self.assertEqual(result["owner_provider_facts"], {"owners": []})
            facts.write_text('{"owners": [1]}')
            with self.assertRaisesRegex(ValueError, "provider facts differ"):
                cloud.summarize_case(directory)
            facts.write_text('{"owners": []}')
            owner.write_text('{"partition": 1}')
            with self.assertRaisesRegex(ValueError, "durability receipt differs"):
                cloud.summarize_case(directory)

    def test_native_report_rejects_self_consistent_wrong_planned_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            receipt["job"].update(cohort="primary_rf1", native_durability="value_wal_barrier",
                                  native_wal_geometry="native_default")
            receipt_path = directory / "receipt.json"
            receipt_path.write_text(json.dumps(receipt))
            cloud.summarize_case(directory)
            for option, wrong in (("durability", "page_cache"), ("native_wal_geometry", "small")):
                with self.subTest(option=option):
                    config_path = directory / "config.json"
                    original = json.loads(config_path.read_text())
                    config = copy.deepcopy(original)
                    config["backend"]["options"][option] = wrong
                    config_path.write_text(json.dumps(config))
                    normalized_path = directory / "normalized-config.json"
                    normalized_path.write_text(json.dumps(config))
                    manifest_path = directory / "result/manifest.json"
                    manifest = json.loads(manifest_path.read_text())
                    manifest["run"]["config"] = config
                    manifest_path.write_text(json.dumps(manifest))
                    receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
                    receipt_path.write_text(json.dumps(receipt))
                    with self.assertRaisesRegex(ValueError, f"native {option} differs from the planned case"):
                        cloud.summarize_case(directory)
                    config_path.write_text(json.dumps(original))
                    normalized_path.write_text(json.dumps(original))
                    manifest["run"]["config"] = original
                    manifest_path.write_text(json.dumps(manifest))
                    receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
                    receipt_path.write_text(json.dumps(receipt))

    def test_nvme_report_requires_bound_target_and_measurement_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            receipt["job"]["backend"] = "tidehunter_nvme_tcp"
            config_path = directory / "config.json"
            config = json.loads(config_path.read_text())
            config["backend"]["options"]["nvmeof_readers"] = [{}]
            config_path.write_text(json.dumps(config))
            (directory / "normalized-config.json").write_text(json.dumps(config))
            manifest_path = directory / "result/manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["run"]["config"] = config
            manifest_path.write_text(json.dumps(manifest))
            receipt["artifacts_sha256"]["manifest.json"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            receipt_path = directory / "receipt.json"

            # A trusted failed partial remains readable, but cannot acquire a proof.
            receipt_path.write_text(json.dumps(receipt))
            partial = cloud.summarize_case(directory)["nvmeof_measurement_qualification"]
            self.assertFalse(partial["qualified"])
            self.assertIn("measurement qualification receipt unavailable", partial["reasons"])

            receipt["status"] = "completed"
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "lacks bound target evidence or measurement qualification"):
                cloud.summarize_case(directory)
            receipt["nvmeof_measurement_qualification"] = {"qualified": True}
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "lacks bound target evidence or measurement qualification"):
                cloud.summarize_case(directory)
            del receipt["nvmeof_measurement_qualification"]
            target_path = directory / "nvme-targets.json"
            target_path.write_text(json.dumps([{"target": {"exports": [{"target_receipt": {
                "target_cache": "buffered_file_page_cache", "target_cache_verified": True,
                "selected_transport": "tcp"}}]}}]))
            receipt["nvmeof_target_evidence"] = {"path": "nvme-targets.json",
                "sha256": hashlib.sha256(target_path.read_bytes()).hexdigest()}
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "lacks bound target evidence or measurement qualification"):
                cloud.summarize_case(directory)

            receipt["nvmeof_measurement_qualification"] = {"qualified": True}
            receipt["nvmeof_target_evidence"]["sha256"] = "0" * 64
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "target evidence differs from its integrity binding"):
                cloud.summarize_case(directory)

    def test_resource_interval_must_match_both_observed_boundary_counters(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory, receipt = self.failure_fixture(Path(temporary))
            before, after = json.loads(
                Path(__file__).with_name("resource-client-boundaries-0024.json").read_text())
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
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                **cloud.resource_accounting.coverage(resources)}
            (directory / "receipt.json").write_text(json.dumps(receipt))
            row = cloud.summarize_case(directory)["resource_evidence"]["observed"]["processes"][0]
            self.assertFalse(row["available"])
            self.assertTrue(row["core_available"])
            self.assertEqual(row["cpu_seconds"], .670104)
            self.assertEqual(row["memory_current_bytes"], 19963904)
            self.assertNotIn("io_read_bytes", row)
            receipt["resource_evidence"]["io_complete"] = True
            (directory / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "resource coverage differs"):
                cloud.summarize_case(directory)
            receipt["resource_evidence"]["io_complete"] = False
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

    def test_live_nvme_write_diagnostics_require_completed_consistent_io(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "result").mkdir()
            path = directory / "result/write-diagnostics.json"
            job = {"backend": "tidehunter_nvme_rdma", "nvmeof_route": "live",
                   "diagnostics": {"write_profile_limit": 2}}
            def evidence():
                return {"schema_version": 1, "backend": "tidehunter_live_nvmeof",
                        "phase": "measurement", "phase_attempted": True, "measurement_drained": True,
                        "runtime_enabled": True, "runtime_observation_mode": "live_nvmeof_phase_counters",
                        "selection": {"requested_limit": 2, "observed_samples": 3,
                                      "selection_policy": "measurement window", "phase_exclusions": ["warmup"]},
                        "clock_scope": "client-side", "overlap_semantics": "independent overlapping clocks",
                        "unsupported_stages": [], "error": None,
                        "profile": {"runtime_observation_mode": "live_nvmeof_phase_counters",
                                    "scope": "measurement_nvme_io_cumulative_delta", "requested_limit": 2,
                                    "selected_operations": 3,
                                    "finalization_error": None, "stages": {"live_nvmeof": {
                                        "completed_write_ops": 3, "completed_write_bytes": 12288,
                                        "errors": 0, "unknown_writes": 0,
                                        "queue_ns": 600, "queue_samples": 3,
                                        "cq_ns": 0, "cq_samples": 0}}}}
            value = evidence()
            path.write_text(json.dumps(value))
            admitted = cloud.admit_write_diagnostics(directory, job, completed=True)
            self.assertEqual(admitted["observed"], value)
            row = {"case_id": "case", "write_diagnostics": admitted}
            clocks = {clock["stage"]: clock for clock in cloud.write_diagnostic_summary(row)}
            self.assertEqual(clocks["queue"]["mean_ns"], 200)
            self.assertEqual(clocks["queue"]["scope"], "measurement_nvme_io_cumulative_delta")
            self.assertIsNone(clocks["cq"]["mean_ns"])
            self.assertIn("measurement_nvme_io_cumulative_delta", "\n".join(cloud.write_diagnostic_report([row])))
            bad = []
            value = evidence()
            value["runtime_enabled"] = False
            value["selection"]["observed_samples"] = 0
            value["profile"]["selected_operations"] = 0
            value["profile"]["stages"]["live_nvmeof"]["completed_write_ops"] = 0
            bad.append(value)
            value = evidence()
            value["profile"] = None
            bad.append(value)
            value = evidence()
            value["profile"]["selected_operations"] = 4
            bad.append(value)
            value = evidence()
            value["profile"]["stages"]["live_nvmeof"]["completed_write_ops"] = 4
            bad.append(value)
            value = evidence()
            value["profile"]["stages"]["live_nvmeof"]["errors"] = 1
            bad.append(value)
            value = evidence()
            value["error"] = "counter reset"
            bad.append(value)
            value = evidence()
            value["runtime_observation_mode"] = "dynamically_armed_remote_append_connection_profiles"
            bad.append(value)
            value = evidence()
            value["selection"]["requested_limit"] = True
            bad.append(value)
            value = evidence()
            value["phase"] = "warmup"
            bad.append(value)
            value = evidence()
            value["backend"] = "tidehunter_tcp"
            bad.append(value)
            value = evidence()
            del value["measurement_drained"]
            bad.append(value)
            for value in bad:
                with self.subTest(value=value):
                    path.write_text(json.dumps(value))
                    with self.assertRaises(ValueError):
                        cloud.admit_write_diagnostics(directory, job, completed=True)

    def test_write_stage_means_keep_distinct_populations(self):
        evidence = {"phase_attempted": True, "runtime_enabled": True, "error": None,
                    "selection": {"observed_samples": 99}, "profile": {"stages": {
                        "write_profile_client_exchange_ns": 1000,
                        "write_profile_client_requests": 2,
                        "write_profile_storage_persist_wal_file_ns": 900,
                        "write_profile_storage_file_persist_calls": 3},
                        "coordinator_interval": {"stages": {
                            "write_foreground_checkpoint_ns": 800,
                            "write_foreground_checkpoint_count": 4}}}}
        def summarize():
            return {item["stage"]: item for item in cloud.write_diagnostic_summary(
                {"write_diagnostics": {"observed": evidence}})}
        clocks = summarize()
        self.assertEqual(clocks["client_exchange"]["mean_ns"], 500)
        self.assertEqual(clocks["storage_persist_wal_file"]["mean_ns"], 300)
        self.assertEqual(clocks["foreground_admission_pressure"]["mean_ns"], 200)
        # A connection finalization error does not invalidate the independent boundary.
        evidence["error"] = "connection profile capture failed"
        clocks = summarize()
        self.assertIsNone(clocks["client_exchange"]["mean_ns"])
        self.assertEqual(clocks["foreground_admission_pressure"]["mean_ns"], 200)

    def test_write_summary_uses_pre_gate_boundary_without_relabeling_fallback(self):
        evidence = {"phase_attempted": True, "profile": {"coordinator_interval": {"stages": {
            "write_foreground_checkpoint_ns": 900, "write_foreground_checkpoint_count": 3}}},
            "pre_completion_gate": {"available": True, "stages": {
                "write_foreground_checkpoint_ns": 200, "write_foreground_checkpoint_count": 2}}}
        row = {"write_diagnostics": {"observed": evidence}}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["mean_ns"], 100)
        self.assertEqual(clock["scope"], "coordinator_arm_through_pre_completion_gate")
        evidence["pre_completion_gate"] = {"available": False, "reason": "missing"}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["mean_ns"], 300)
        self.assertEqual(clock["scope"], "coordinator_arm_through_post_gate")

    def test_incomplete_measurement_does_not_publish_boundary_means(self):
        evidence = {"phase_attempted": True, "measurement_drained": False,
                    "pre_completion_gate": {"available": True, "stages": {
                        "write_foreground_checkpoint_ns": 200,
                        "write_foreground_checkpoint_count": 2}}}
        clock = cloud.write_diagnostic_summary({"write_diagnostics": {"observed": evidence}})[0]
        self.assertFalse(clock["available"])
        self.assertIsNone(clock["mean_ns"])
        del evidence["measurement_drained"]
        self.assertEqual(cloud.write_diagnostic_summary({"write_diagnostics": {"observed": evidence}})[0]["mean_ns"], 100)

    def test_write_completion_wait_has_no_invented_call_mean(self):
        row = {"write_diagnostics": {"observed": {
            "phase_attempted": True, "pre_completion_gate": {"available": True,
                "stages": {"write_storage_wait_ns": 1234}}}}}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["sum_ns"], 1234)
        self.assertIsNone(clock["count"])
        self.assertIsNone(clock["mean_ns"])
        del row["write_diagnostics"]["observed"]["pre_completion_gate"]["stages"]["write_storage_wait_ns"]
        self.assertEqual(cloud.write_diagnostic_summary(row), [])

    def test_sampled_checkpoint_wait_uses_completed_controls_not_appends(self):
        evidence = {"phase_attempted": True, "runtime_enabled": True, "error": None,
                    "selection": {"observed_samples": 20}, "profile": {"stages": {
                        "write_profile_checkpoint_available": 1,
                        "write_profile_checkpoint_wait_ns": 600,
                        "write_profile_checkpoint_completed": 3,
                        "write_profile_checkpoint_requests": 4}}}
        row = {"write_diagnostics": {"observed": evidence}}
        clock = cloud.write_diagnostic_summary(row)[0]
        self.assertEqual(clock["count"], 3)
        self.assertEqual(clock["mean_ns"], 200)
        evidence["profile"]["stages"]["write_profile_checkpoint_completed"] = 0
        self.assertIsNone(cloud.write_diagnostic_summary(row)[0]["mean_ns"])
        evidence["profile"]["stages"]["write_profile_checkpoint_completed"] = 3
        del evidence["profile"]["stages"]["write_profile_checkpoint_available"]
        self.assertIsNone(cloud.write_diagnostic_summary(row)[0]["mean_ns"])

    def test_write_stage_missing_and_unavailable_is_not_zero(self):
        for total, count in ((None, 2), (0, None), (0, 0), (-1, 2), (1, True)):
            with self.subTest(total=total, count=count):
                row = {"write_diagnostics": {"observed": {
                    "phase_attempted": True, "runtime_enabled": True, "error": None,
                    "selection": {"observed_samples": 2},
                    "profile": {"stages": {"write_profile_client_exchange_ns": total,
                        "write_profile_client_requests": count}}}}}
                result = cloud.write_diagnostic_summary(row)[0]
                self.assertFalse(result["available"])
                self.assertIsNone(result["mean_ns"])
        for available in (None, False, True):
            row = {"write_diagnostics": {"observed": {"phase_attempted": True, "profile": {
                "coordinator_checkpoint_diagnostics_available": available,
                "coordinator_interval": {"stages": {
                    "write_checkpoint_diagnostic_root_adoption_ns": 24,
                    "write_checkpoint_diagnostic_root_adoption_count": 3}}}}}}
            result = cloud.write_diagnostic_summary(row)[0]
            self.assertEqual(result["mean_ns"], 8 if available else None)

    def test_read_diagnostics_admission_checks_runtime_and_integrity(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            (directory / "result").mkdir()
            job = {"backend": "tidehunter_tcp", "diagnostics": {"read_profile_limit": 16}}
            with self.assertRaisesRegex(ValueError, "artifact is missing"):
                cloud.admit_read_diagnostics(directory, job, completed=True)
            profile = {"schema_version": 1, "backend": "tidehunter_tcp", "phase": "measurement",
                       "phase_attempted": True, "runtime_enabled": True,
                       "runtime_observation_mode": "sampled_prefix",
                       "selection": {"requested_limit": 16, "selected_operations": 1,
                                     "completed_operations": 1, "selection_policy": "first",
                                     "phase_exclusions": ["warmup"]},
                       "clock_scope": "process monotonic", "overlap_semantics": "nested",
                       "unsupported_stages": [], "profile": {"stages": {
                           "server_async": {"available": True, "sum_ns": 10, "count": 2, "max_ns": 8,
                                            "unavailable_reason": None},
                           "server_nowait": {"available": False, "sum_ns": None, "count": 0,
                                             "max_ns": None, "unavailable_reason": "no samples"}},
                           "counters": {}, "owners": {"3": {"stages": {
                               "server_async": {"available": True, "sum_ns": 8, "count": 1,
                                                "max_ns": 8, "unavailable_reason": None}},
                               "counters": {}}}}, "error": None}
            path = directory / "result/read-diagnostics.json"
            path.write_text(json.dumps(profile))
            binding = cloud.admit_read_diagnostics(directory, job, completed=True)
            self.assertEqual(binding["observed"], profile)
            self.assertEqual(binding["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            text = "\n".join(cloud.read_diagnostic_report([{"case_id": "case", "job": {
                "cohort": "read_stage_diagnostics", "diagnostic_source_cohort": "owner_cache_configured"},
                "read_diagnostics": binding}]))
            self.assertIn("| case | owner_cache_configured | owner | aggregate | server_async | 2 | 10 | 5.000 | 8 |", text)
            self.assertIn("| case | owner_cache_configured | owner | aggregate | server_nowait | — | — | — | — |", text)
            self.assertIn("| case | owner_cache_configured | owner | 3 | server_async | 1 | 8 | 8.000 | 8 |", text)
            self.assertNotIn("p99", text)
            for change in ({"runtime_enabled": False}, {"error": "failed"}, {"phase": "warmup"}):
                path.write_text(json.dumps(profile | change))
                with self.assertRaises(ValueError):
                    cloud.admit_read_diagnostics(directory, job, completed=True)
            invalid = profile | {"selection": profile["selection"] | {"completed_operations": 2}}
            path.write_text(json.dumps(invalid))
            with self.assertRaises(ValueError):
                cloud.admit_read_diagnostics(directory, job, completed=True)

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
            # A timed-out client can finish writing after receipt admission failed.
            # Those late artifacts are preserved, not promoted into observations.
            late = directory / "result" / "measurement-read-evidence.json"
            late.write_text(json.dumps({
                "schema_version": 1, "scope": "measurement_boundary",
                "measurement_attempted": True, "measurement_drained": False,
                "start": None, "end": None, "deltas": {"available": False}}))
            original_receipt = (directory / "receipt.json").read_bytes()
            row = cloud.summarize_case(directory)
            self.assertNotIn("metrics", row)
            self.assertEqual(row["status"], "failed")
            self.assertEqual((directory / "receipt.json").read_bytes(), original_receipt)
            receipt["status"] = "completed"
            (directory / "receipt.json").write_text(json.dumps(receipt))
            with self.assertRaises(ValueError):
                cloud.summarize_case(directory)

    def test_common_operation_order_is_identical_across_backends(self):
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
                for job in block:
                    config = cloud.make_run_config(job, Path("/tmp/owned-case"), {}, None)
                    self.assertLessEqual(len(config["namespace"].encode()), 64)


if __name__ == "__main__":
    unittest.main()
