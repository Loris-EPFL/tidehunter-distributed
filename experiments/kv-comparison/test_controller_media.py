import unittest
import base64
import json
from pathlib import Path
import tempfile
from contextlib import ExitStack
from datetime import timedelta
from unittest.mock import MagicMock, patch

import cloud_controller as controller


class MediaEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.row = {"owner_index": 0, "major": 259, "minor": 1, "read_ops": 100,
                    "read_sectors": 0, "write_ops": 0, "write_sectors": 0}
    def test_diskstats_four_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "device").mkdir()
            (path / "device/model").write_text("local")
            with patch("cold_device_trace.resolve_device", return_value={"sysfs_path": str(path), "device": "259:1"}), \
                 patch.object(controller.physical_storage, "admitted_model", return_value=True):
                result = controller.media_device_snapshot(path, 0, "259 1 nvme0n1 10 0 20 0 30 0 40 0")
            self.assertEqual([result[k] for k in ("read_ops", "read_sectors", "write_ops", "write_sectors")],
                             [10, 20, 30, 40])


    def evidence(self, expected, after, reads=100, drained=True, history="normal"):
        return controller.qualify_media_evidence(
            "case", {"expected": expected, "history": history}, [self.row], after, reads, drained)

    def test_inclusive_thresholds(self):
        for expected, count in (("cold", 190), ("memory", 106)):
            result = self.evidence(expected, [self.row | {"read_ops": count}])
            self.assertEqual(result["status"], "qualified")
        self.assertEqual(self.evidence("cold", [self.row | {"read_ops": 189}])["status"], "unqualified")
        self.assertEqual(self.evidence("memory", [self.row | {"read_ops": 107}])["status"], "unqualified")
    def test_device_owner_cold_qualification_is_not_a_case_failure(self):
        job = next(job for job in controller.cloud_campaign.make_focused_matrix("device")
                   if job["case_kind"] == "media_read" and job["backend"] == "tidehunter_tcp")
        gate = {"gate_released": True, "completion_gate_observed": True,
                "os_caches_dropped": True, "mapped_wal_eviction": "completed_before_gate",
                "engine_caches_evicted": None}
        for count, expected in ((190, "qualified"), (189, "unqualified")):
            evidence = controller.qualify_media_evidence(
                job["case_id"], job["media"], [self.row], [self.row | {"read_ops": count}], 100, True)
            self.assertEqual(evidence["status"], expected)
            if expected == "unqualified":
                self.assertTrue(evidence["reason_codes"])
            controller.analyze._validate_focused_artifact(
                "media_evidence", evidence, job, {"measurement_evidence": gate})


    def test_missing_changed_decreasing_and_duplicate_counters(self):
        for rows in ([], [self.row | {"minor": 2}], [self.row | {"read_ops": 99}],
                     [self.row, self.row]):
            result = self.evidence("observe", rows)
            self.assertEqual(result["status"], "unqualified")
            self.assertIsNone(result["read_ops_per_success"])

    def test_zero_reads_undrained_and_history_are_unqualified(self):
        for reads, drained, history in ((0, True, "normal"), (100, False, "normal"),
                                        (100, True, "prewarm")):
            self.assertEqual(self.evidence("observe", [self.row], reads, drained, history)["status"], "unqualified")

    def test_history_snapshot_rejects_per_owner_reset(self):
        def boundary(values):
            return {"owner_executor": {"available": True, "partitions": {
                str(i): {"authority": {"history_loads": v}} for i, v in enumerate(values)}}}
        evidence = {"measurement_drained": True, "start": boundary([1, 2, 3, 4, 5]),
                    "end": boundary([2, 3, 4, 5, 6])}
        self.assertEqual(controller.owner_history_boundaries(evidence), (15, 20))
        evidence["end"] = boundary([0, 4, 5, 6, 7])
        self.assertEqual(controller.owner_history_boundaries(evidence), (None, None))

    def test_controller_matrix_dispatch(self):
        with patch.object(controller.cloud_campaign, "make_focused_matrix", return_value=["focused"]) as focused, \
             patch.object(controller.cloud_campaign, "make_matrix", return_value=["legacy"]) as legacy:
            self.assertEqual(controller.controller_matrix({"mode": "focused", "selection": "full"}), ["focused"])
            focused.assert_called_once_with("full")
            self.assertEqual(controller.controller_matrix({"mode": "standard"}), ["legacy"])
            legacy.assert_called_once_with({"mode": "standard"})

    def test_trace_unavailable_and_loss_are_not_forged(self):
        unavailable = controller.block_trace_evidence("case", "owner_put", None)
        self.assertFalse(unavailable["available"])
        self.assertIsNone(unavailable["lost_events"])
        row = {"available": True, "lost": 2, "device": {"device": "259:0"}, "truncated": False}
        evidence = controller.block_trace_evidence("case", "owner_put", [None] + [row] * 5)
        self.assertTrue(evidence["available"])
        self.assertFalse(evidence["qualified"])
        self.assertEqual(evidence["lost_events"], 10)
        self.assertEqual(evidence["files"], [])

    def test_phase_spans_record_success_and_failure(self):
        spans = []
        self.assertEqual(controller.record_phase_span(spans, "export_copy", lambda: 7), 7)
        def fail():
            raise RuntimeError("failed")
        with self.assertRaisesRegex(RuntimeError, "failed"):
            controller.record_phase_span(spans, "target_setup", fail)
        self.assertEqual([span["status"] for span in spans], ["completed", "failed"])
        self.assertTrue(all(span["monotonic_start_ns"] <= span["monotonic_end_ns"] for span in spans))

    def test_fio_summary_uses_raw_io_and_depth(self):
        job = {"case_id": "case", "fio": {"location": "local_file", "block_bytes": 4096, "queue_depth": 16}}
        raw = {"jobs": [{"read": {"total_ios": 5, "io_bytes": 20480, "runtime": 1,
                                   "clat_ns": {"percentile": {"50.000000": 1, "99.000000": 2, "99.900000": 3}}},
                         "write": {"total_ios": 0}, "elapsed": 10, "error": 0, "iodepth_level": {"16": 90.0}}]}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fio.json"
            path.write_text(json.dumps(raw))
            args = {"device_identity": "259:0", "namespace_uuid": "scratch", "region_bytes": 2**31,
                    "opened_read_only": True, "actual_direct": True, "quiescent": True,
                    "target_buffered_io": None}
            evidence = controller.fio_result_from_raw(job, path, **args)
            self.assertEqual(evidence["effective_depth"], 16)
            self.assertEqual(evidence["elapsed_seconds"], 0.001)
            self.assertEqual(controller.analyze._verify_fio_raw(path.read_bytes(), evidence), (True, True))
            self.assertEqual(evidence["write_ios"], 0)
            raw["jobs"][0]["write"]["total_ios"] = 1
            path.write_text(json.dumps(raw))
            with self.assertRaises(ValueError):
                controller.fio_result_from_raw(job, path, **args)

    def test_scratch_is_new_nonzero_and_identity_checked(self):
        with tempfile.TemporaryDirectory() as tmp:
            identity = controller.provision_fio_scratch(tmp, "case", 8192)
            path = Path(identity["path"])
            self.assertEqual(path.read_bytes(), bytes([0xA5]) * 8192)
            with self.assertRaises(FileExistsError):
                controller.provision_fio_scratch(tmp, "case", 8192)
            with self.assertRaises(controller.SafetyError):
                controller.remove_fio_scratch(tmp, "case", identity | {"inode": identity["inode"] + 1})
            controller.remove_fio_scratch(tmp, "case", identity)
            self.assertFalse(path.exists())

    def test_fio_command_has_only_read_work(self):
        fio = {"location": "local_file", "block_bytes": 4096, "queue_depth": 16,
               "runtime_seconds": 10, "direct": True, "pattern": "randread"}
        argv = controller.fio_read_command("/usr/bin/fio", "/scratch/case", fio, 2**31)
        for flag in ("--readonly", "--direct=1", "--numjobs=1", "--output-format=json"):
            self.assertIn(flag, argv)
        self.assertIn("--rw=randread", argv)
        self.assertNotIn("--rw=write", argv)
        with self.assertRaises(ValueError):
            controller.fio_read_command("/usr/bin/fio", "/scratch/case", fio | {"pattern": "write"}, 2**31)


    def test_fio_standalone_binds_raw_and_command(self):
        fio = {"location": "local_file", "block_bytes": 4096, "queue_depth": 1,
               "runtime_seconds": 10, "direct": True, "pattern": "randread"}
        job = {"case_kind": "fio_floor", "case_id": "case", "backend": "tidehunter_verbs_native", "fio": fio}
        raw = json.dumps({"jobs": [{"read": {"total_ios": 5, "io_bytes": 20480,
                    "clat_ns": {"percentile": {"50.000000": 1, "99.000000": 2, "99.900000": 3}}},
                    "write": {"total_ios": 0}, "elapsed": 10, "error": 0, "iodepth_level": {"1": 100}}]}).encode()
        with tempfile.TemporaryDirectory() as tmp:
            instance = controller.Controller.__new__(controller.Controller)
            instance.root = Path(tmp)
            instance.config = {"binary_blake2b512": "a" * 128}
            instance.active_tide = instance.active_nvme = instance.active_backend = instance.shared_tide = None
            instance.monitoring_attempted = False
            instance.monitoring_events = []
            instance.select_monitoring = MagicMock()
            instance.finish_monitoring = MagicMock()
            instance.stop_tide = MagicMock()
            instance.guard = MagicMock()
            instance.remaining = MagicMock(return_value=100)
            argv = controller.fio_read_command("/usr/bin/fio", "/owned/scratch", fio, 2**31)
            instance.request = MagicMock(return_value={"raw_base64": base64.b64encode(raw).decode(),
                "argv": argv, "device_identity": "259:0", "scratch": {"size": 2**31},
                "opened_read_only": True, "actual_direct": True})
            with patch.object(controller.cloud_campaign, "validate_focused_job"):
                receipt = instance.run_fio_case(job)
            self.assertEqual(receipt["status"], "completed")
            self.assertEqual((Path(tmp) / "cases/case/fio_raw.json").read_bytes(), raw)
            self.assertIn("fio_command", receipt)
            self.assertIn("fio_result", receipt)
            self.assertIn("phase_timing", receipt)

    def test_fio_missing_direct_fd_retains_bounded_output(self):
        def process(argv, stdout, stderr):
            stdout.write(b"prefix" + b"z" * 2000)
            stderr.write(b"tail-message")
            value = MagicMock()
            value.poll.return_value = 0
            value.returncode = 0
            return value
        with patch.object(controller.subprocess, "Popen", side_effect=process):
            with self.assertRaises(controller.FioFailure) as error:
                controller.run_readonly_fio(["fio"], 1, 2, 10)
        message = str(error.exception)
        self.assertIn("fio direct read-only target fd was not observed", message)
        self.assertIn("exit status 0", message)
        self.assertIn("tail-message", message)
        self.assertNotIn("z" * 1024, message)

    def test_fio_guest_failure_cleanup_and_safety(self):
        job = next(j for j in controller.cloud_campaign.make_focused_matrix("recommended")
                   if j["case_kind"] == "fio_floor" and j["fio"]["location"] == "local_file")
        request = {"index": 0, "action": "fio_scratch", "job": job,
                   "case_id": job["case_id"], "quiescent": True, "timeout": 60}
        for mode in ("failure", "cleanup_failure", "safety", "success"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as tmp, ExitStack() as stack:
                root = Path(tmp)
                config = {"fleet": {"data_root": str(root), "nodes": [
                    {"index": i, "instance_id": f"i-owner-{i}", "address": f"10.0.0.{i + 2}"}
                    for i in range(5)]}, "spec": {"mode": "focused"},
                          "deadline_utc": (controller.utc() + timedelta(minutes=5)).isoformat()}
                stack.enter_context(patch.object(controller, "verify_local_identity", return_value={}))
                stack.enter_context(patch.object(controller, "guarded", side_effect=lambda p, *a, **kw: Path(p)))
                stack.enter_context(patch.object(controller, "media_device_snapshot", return_value={"major": 1, "minor": 2}))
                original_provision = controller.provision_fio_scratch
                stack.enter_context(patch.object(controller, "provision_fio_scratch",
                    side_effect=lambda directory, case_id, deadline: original_provision(directory, case_id, 8192)))
                stack.enter_context(patch.object(controller.shutil, "which", return_value="/usr/bin/fio"))
                if mode in ("failure", "cleanup_failure"):
                    def failed_process(argv, stdout, stderr):
                        stderr.write(b"x" * 2200 + b"guest-fio-error" + bytes([255]))
                        stdout.write(b"fio-json-error")
                        process = MagicMock()
                        process.poll.return_value = 1
                        process.returncode = 1
                        return process
                    stack.enter_context(patch.object(controller.subprocess, "Popen", side_effect=failed_process))
                elif mode == "safety":
                    stack.enter_context(patch.object(controller, "run_readonly_fio",
                                                     side_effect=controller.SafetyError("unsafe write access")))
                else:
                    stack.enter_context(patch.object(controller, "run_readonly_fio",
                        return_value=(b"ok", {"read_only": True, "direct": True})))
                if mode == "cleanup_failure":
                    stack.enter_context(patch.object(controller, "remove_fio_scratch", side_effect=OSError("cleanup")))
                    with self.assertRaisesRegex(controller.InfrastructureError, "cleanup unconfirmed"):
                        controller.guest_operation(config, request, None)
                elif mode == "safety":
                    with self.assertRaisesRegex(controller.SafetyError, "unsafe write access"):
                        controller.guest_operation(config, request, None)
                    self.assertFalse((root / "fio-scratch" / (job["case_id"] + ".fio-scratch")).exists())
                else:
                    result = controller.guest_operation(config, request, None)
                    self.assertFalse((root / "fio-scratch" / (job["case_id"] + ".fio-scratch")).exists())
                    if mode == "failure":
                        self.assertIn("exit status 1", result["fio_failed"])
                        self.assertIn("guest-fio-error", result["fio_failed"])
                        self.assertIn(chr(0xfffd), result["fio_failed"])
                        self.assertIn("fio-json-error", result["fio_failed"])
                        self.assertNotIn("x" * 2100, result["fio_failed"])
                    else:
                        self.assertEqual(base64.b64decode(result["raw_base64"]), b"ok")

    def test_fio_host_failure_is_reported_and_not_fatal(self):
        import focused_analysis
        job = next(j for j in controller.cloud_campaign.make_focused_matrix("recommended")
                   if j["case_kind"] == "fio_floor" and j["fio"]["location"] == "local_file")
        with tempfile.TemporaryDirectory() as tmp:
            instance = controller.Controller.__new__(controller.Controller)
            instance.root = Path(tmp)
            instance.config = {"binary_blake2b512": "a" * 128}
            instance.active_tide = instance.active_nvme = instance.active_backend = instance.shared_tide = None
            instance.monitoring_attempted = False
            instance.monitoring_events = []
            instance.select_monitoring = MagicMock()
            instance.finish_monitoring = MagicMock()
            instance.stop_tide = MagicMock()
            instance.guard = MagicMock()
            instance.remaining = MagicMock(return_value=100)
            instance.request = MagicMock(return_value={"fio_failed": "exit status 1: guest-fio-error"})
            receipt = instance.run_fio_case(job)
            self.assertEqual(receipt["status"], "failed")
            self.assertIn("guest-fio-error", receipt["error"])
            (instance.root / "matrix.json").write_text(json.dumps({"jobs": [job], "spec": {}}))
            report = controller.cloud_campaign.report(instance.root)
            self.assertFalse(report["complete"])
            self.assertEqual(report["cases"][0]["case_id"], job["case_id"])
            self.assertEqual(report["cases"][0]["status"], "failed")
            admitted = json.loads((instance.root / "comparison.json").read_text())
            admitted["cases"][0]["focused_evidence"] = {"fio_result": {
                "read_ios": 10, "read_bytes": 40960, "elapsed_seconds": 1,
                "latency_ns": {"p50": 10, "p99": 20, "p999": 30}}}
            (instance.root / "comparison.json").write_text(json.dumps(admitted))
            summary = focused_analysis.generate(instance.root, instance.root, instance.root / "analysis")
            self.assertFalse(summary["complete"])
            self.assertEqual(summary["failed_cases"], 1)
            self.assertEqual(summary["tables"]["failed_cases"][0]["case_id"], job["case_id"])
            self.assertIsNone(summary["tables"]["fio_floor"][0]["iops"])
            instance.request.return_value = {"fio_failed": 1}
            with tempfile.TemporaryDirectory() as second:
                instance.root = Path(second)
                with self.assertRaisesRegex(controller.SafetyError, "invalid fio failure receipt"):
                    instance.run_fio_case(job)

    def test_standalone_trace_report_has_no_kv_manifest_or_samples(self):
        job = next(job for job in controller.cloud_campaign.make_focused_matrix("recommended")
                   if job["case_kind"] == "block_trace")
        with tempfile.TemporaryDirectory() as tmp:
            case = Path(tmp) / job["case_id"]
            result = case / "result"
            result.mkdir(parents=True)
            for name in ("manifest.json", "samples.csv", "summary.json", "metrics.prom"):
                (result / name).write_text("validated fixture")
            receipt = {"job": job, "status": "completed", "raw_artifacts_validated": True,
                       "validated_timing_rows_by_phase": {"measurement": 10},
                       "artifacts_sha256": {name: controller.digest(result / name) for name in
                                            ("manifest.json", "samples.csv", "summary.json", "metrics.prom")}}
            trace = controller.block_trace_evidence(job["case_id"], job["trace"]["scope"], None)
            for stem, data in (("block_trace_evidence", trace),
                               ("phase_timing", {"schema_version": 1, "case_id": job["case_id"], "spans": []})):
                path = case / (stem + ".json")
                controller.dump(path, data)
                receipt[stem] = {"path": path.name, "sha256": controller.digest(path)}
            controller.finalize_standalone_trace(case, receipt)
            receipt["artifacts_sha256"] = {name: controller.digest(result / name)
                                           for name in ("summary.json", "metrics.prom")}
            controller.dump(case / "receipt.json", receipt)
            summary = controller.cloud_campaign.summarize_case(case)
            self.assertEqual(summary["status"], "completed")
            self.assertFalse(summary["focused_evidence"]["block_trace_evidence"]["available"])
            self.assertFalse((result / "manifest.json").exists())
            self.assertFalse((result / "samples.csv").exists())
            self.assertEqual(receipt["trace_workload_validation"]["timing_rows_by_phase"]["measurement"], 10)
