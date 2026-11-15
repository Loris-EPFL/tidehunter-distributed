import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import analyze
import cloud_campaign


class FocusedEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.job = {"case_id": "case", "case_kind": "media_read",
                    "media": {"expected": "cold", "history": "normal"}}
        self.receipt = {"measurement_evidence": {"gate_released": True, "completion_gate_observed": True}}
        self.bind("phase_timing", {"schema_version": 1, "case_id": "case", "spans": []})

    def bind(self, name, value):
        raw = json.dumps(value).encode()
        path = self.root / (name + ".json")
        path.write_bytes(raw)
        self.receipt[name] = {"path": path.name, "sha256": hashlib.sha256(raw).hexdigest()}
        if name == "media_evidence":
            for side, counter in (("before", "read_ops_before"), ("end", "read_ops_after")):
                rows = value["owner_devices"]
                hosts = [{"diskstats": ""} for _ in range(1 + max((r["owner_index"] for r in rows), default=0) + 1)]
                for row in rows:
                    if row[counter] is not None:
                        hosts[row["owner_index"] + 1]["diskstats"] += (
                            f'{row["major"]} {row["minor"]} device {row[counter]} 0 0 0\n')
                snapshot = json.dumps({"hosts": hosts}).encode()
                filename = "measurement-" + side + ".json"
                (self.root / filename).write_bytes(snapshot)
                self.receipt["measurement_evidence"].update(
                    {side: filename, side + "_sha256": hashlib.sha256(snapshot).hexdigest()})

    def test_media_snapshot_mismatch_and_tamper(self):
        self.bind("media_evidence", self.media())
        end = self.root / self.receipt["measurement_evidence"]["end"]
        end.write_text(json.dumps({"hosts": [{}, {"diskstats": "8 1 other 190 0 0 0"}]}))
        self.receipt["measurement_evidence"]["end_sha256"] = hashlib.sha256(end.read_bytes()).hexdigest()
        with self.assertRaises(ValueError):
            self.admit()
        self.bind("media_evidence", self.media())
        end.write_text("{}")
        with self.assertRaises(ValueError):
            self.admit()

    def admit(self, completed=True):
        return analyze.admit_focused_artifacts(self.root, self.job, self.receipt, completed=completed)

    def media(self, delta=90, count=100):
        return {"schema_version": 1, "case_id": "case", "expected": "cold", "status": "qualified",
                "reason_codes": [], "measurement_drained": True,
                "owner_devices": [{"owner_index": 0, "major": 8, "minor": 0,
                                   "read_ops_before": 100, "read_ops_after": 100 + delta}],
                "successful_reads": count, "read_ops_per_success": delta / count,
                "history_loads_before": None, "history_loads_after": None,
                "history_prewarm_verified": None}
    def test_rest2_groups_preserve_read_media_and_write_device_admission(self):
        jobs = cloud_campaign.make_focused_matrix("nvmeof_reads_geometry_rest2")
        for job in jobs:
            with self.subTest(case=job["case_id"], group=job["group"]):
                self.job = job
                self.receipt = {"measurement_evidence": {
                    "gate_released": True, "completion_gate_observed": True}}
                self.bind("phase_timing", {"schema_version": 1, "case_id": job["case_id"], "spans": []})
                if job["group"] == "G24C":
                    with self.assertRaisesRegex(ValueError, "media_evidence"):
                        self.admit()
                    media = self.media(0, job["measurement_operations"])
                    media.update(case_id=job["case_id"], expected="memory")
                    self.bind("media_evidence", media)
                    self.assertEqual(self.admit()["media_evidence"]["status"], "qualified")
                elif job["group"] == "G24W":
                    row = {"owner_index": 0, "major": 259, "minor": 3, "read_ops": 0,
                           "read_sectors": 0, "write_ops": 1, "write_sectors": 8}
                    self.bind("device_counters", {"before": [row], "after": [{**row, "write_sectors": 16}]})
                    admitted = self.admit()
                    self.assertIn("device_counters", admitted)
                    self.assertNotIn("media_evidence", admitted)
                else:
                    self.assertEqual(job["group"], "G24N")
                    self.assertNotIn("media_evidence", self.admit())

    def test_live_read_requires_and_admits_media_evidence(self):
        self.job.update(case_kind="live_read")
        with self.assertRaisesRegex(ValueError, "completed case lacks requested media_evidence"):
            self.admit()
        self.bind("media_evidence", self.media())
        self.assertEqual(self.admit()["media_evidence"]["status"], "qualified")

    def test_put_admits_bound_device_counters(self):
        self.job = {"case_id": "case", "case_kind": "remote_put", "backend": "tidehunter_verbs_native",
                    "owner_count": 1, "device_counters": True}
        row = {"owner_index": 0, "major": 259, "minor": 3, "read_ops": 1,
               "read_sectors": 2, "write_ops": 3, "write_sectors": 4}
        self.bind("device_counters", {"before": [row], "after": [{**row, "write_sectors": 8}]})
        self.assertEqual(self.admit()["device_counters"]["after"][0]["write_sectors"], 8)

    def test_completed_device_artifacts_accept_read_and_put(self):
        jobs = cloud_campaign.make_focused_matrix("device")
        for kind in ("media_read", "remote_put"):
            self.job = next(job for job in jobs if job["case_kind"] == kind)
            self.receipt = {"measurement_evidence": {
                "gate_released": True, "completion_gate_observed": True,
                "os_caches_dropped": True, "mapped_wal_eviction": "completed_before_gate",
                "engine_caches_evicted": None}}
            self.bind("phase_timing", {"schema_version": 1, "case_id": self.job["case_id"], "spans": []})
            if kind == "media_read":
                media = self.media(89)
                media.update(case_id=self.job["case_id"], status="unqualified", reason_codes=["below_threshold"])
                self.bind("media_evidence", media)
            admitted = self.admit()
            self.assertIn("phase_timing", admitted)
            if kind == "media_read":
                self.assertEqual(admitted["media_evidence"]["status"], "unqualified")


    def test_media_rejects_inactive_owner_device(self):
        self.job["owner_count"] = 1
        value = self.media()
        self.bind("media_evidence", value)
        self.admit()
        value["owner_devices"][0]["owner_index"] = 1
        self.bind("media_evidence", value)
        with self.assertRaisesRegex(ValueError, "outside active owners"):
            self.admit()
        self.job["owner_count"] = 5
        self.admit()

    def test_media_thresholds_and_unavailable(self):
        value = self.media()
        self.bind("media_evidence", value)
        self.assertEqual(self.admit()["media_evidence"], value)
        value = self.media(89)
        self.bind("media_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()
        value.update(status="unqualified", reason_codes=["below_threshold"])
        self.bind("media_evidence", value)
        self.assertEqual(self.admit()["media_evidence"]["status"], "unqualified")
        self.job["media"]["expected"] = value["expected"] = "memory"
        value.update(status="qualified", reason_codes=[], read_ops_per_success=0.06)
        value["owner_devices"][0]["read_ops_after"] = 106
        self.bind("media_evidence", value)
        self.admit()
        value["owner_devices"][0]["read_ops_after"] = 107
        value["read_ops_per_success"] = .07
        self.bind("media_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()

    def test_unavailable_device_counter_is_not_zero(self):
        value = self.media()
        value["owner_devices"][0]["read_ops_after"] = None
        value.update(status="unqualified", reason_codes=["counter_unavailable"], read_ops_per_success=None)
        self.bind("media_evidence", value)
        self.assertIsNone(self.admit()["media_evidence"]["read_ops_per_success"])
        value["read_ops_per_success"] = 0
        self.bind("media_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()
        value.update(status="qualified", reason_codes=[], read_ops_per_success=None)
        self.bind("media_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()

    def test_tamper_missing_and_partial(self):
        with self.assertRaises(ValueError):
            self.admit()
        self.admit(completed=False)
        self.bind("media_evidence", self.media())
        (self.root / "media_evidence.json").write_text("{}")
        with self.assertRaises(ValueError):
            self.admit()
        self.receipt["media_evidence"]["path"] = "../outside"
        with self.assertRaises(ValueError):
            self.admit()

    def test_strict_numeric_and_identity(self):
        for field, bad in (("successful_reads", True), ("successful_reads", -1),
                           ("case_id", "other"), ("unexpected", 1)):
            value = self.media()
            value[field] = bad
            self.bind("media_evidence", value)
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.admit()
        value = self.media()
        value["owner_devices"].append(dict(value["owner_devices"][0]))
        self.bind("media_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()
        value = self.media()
        value["owner_devices"][0]["owner_index"] = 5
        self.bind("media_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()

    def test_gate_and_history_cannot_forge_qualification(self):
        self.bind("media_evidence", self.media())
        self.receipt["measurement_evidence"]["completion_gate_observed"] = False
        with self.assertRaises(ValueError):
            self.admit()
        self.receipt["measurement_evidence"]["completion_gate_observed"] = True
        self.job["media"]["history"] = "prewarm"
        with self.assertRaises(ValueError):
            self.admit()

    def test_phase_and_disabled_wait(self):
        self.job = {"case_id": "case", "case_kind": "write_wait_diagnostic",
                    "write_wait_diagnostics": {"enabled": False}}
        self.admit()
        self.bind("write_wait_metrics", {"schema_version": 1, "case_id": "case", "enabled": False, "windows": []})
        with self.assertRaises(ValueError):
            self.admit()
        del self.receipt["write_wait_metrics"]
        self.bind("phase_timing", {"schema_version": 1, "case_id": "case", "spans": [
            {"operation": "target_setup", "host_index": 0, "monotonic_start_ns": 2,
             "monotonic_end_ns": 1, "status": "completed"}]})
        with self.assertRaises(ValueError):
            self.admit()

    def test_interference_accounting(self):
        self.job = {"case_id": "case", "case_kind": "interference", "interference": {"read_offered_per_second": 1000}}
        stream = {"offered": 10, "admitted": 9, "completed": 9, "successful": 8, "rejected": 1, "failed": 0,
                  "first_dispatch_ns": 5, "last_completion_ns": 95}
        value = {"schema_version": 1, "case_id": "case", "read": dict(stream), "put": dict(stream),
                 "measurement_drained": True, "read_offered_per_second": 1000, "measurement_elapsed_ns": 100,
                 "window_start_ns": 0, "window_end_ns": 100}
        self.bind("interference_result", value)
        self.assertTrue(self.admit()["interference_result"]["qualification"]["qualified"])
        value["put"]["first_dispatch_ns"] = 6
        self.bind("interference_result", value)
        self.assertFalse(self.admit()["interference_result"]["qualification"]["qualified"])
        value["put"]["first_dispatch_ns"] = 5
        value["put"]["completed"] = 8
        self.bind("interference_result", value)
        with self.assertRaises(ValueError):
            self.admit()
        value["measurement_drained"] = False
        value["put"]["failed"] = 1
        self.bind("interference_result", value)
        self.admit()
        value["measurement_drained"] = True
        self.bind("interference_result", value)
        with self.assertRaises(ValueError):
            self.admit()

    def test_fio_dimensions_and_depth(self):
        self.job = {"case_id": "case", "case_kind": "fio_floor",
                    "fio": {"location": "local_file", "block_bytes": 4096, "queue_depth": 1}}
        self.bind("fio_command", ["fio", "--name=case", "--numjobs=1", "--readonly", "--direct=1", "--rw=randread", "--output-format=json"])
        value = {"schema_version": 1, "case_id": "case", "location": "local_file",
                 "device_identity": "dev", "namespace_uuid": "uuid", "block_bytes": 4096,
                 "requested_depth": 1, "effective_depth": None, "actual_direct": True,
                 "read_only": True, "elapsed_seconds": 10, "read_ios": 10,
                 "read_bytes": 40960, "errors": 0, "latency_ns": {"p50": 1, "p99": 2, "p999": 3},
                 "fio_sha256": "a" * 64, "raw_fio_path": "fio.json",
                 "region_bytes": 1024**3, "write_ios": 0, "opened_read_only": True,
                 "quiescent": True, "target_buffered_io": None}
        def raw_fio(depth, *, incomplete=False):
            raw = {"jobs": []} if incomplete else {"jobs": [{
                "read": {"total_ios": value["read_ios"], "io_bytes": value["read_bytes"],
                         "clat_ns": {"percentile": {"50.000000": 1, "99.000000": 2, "99.900000": 3}}},
                "write": {"total_ios": value["write_ios"]}, "error": value["errors"],
                "elapsed": value["elapsed_seconds"], "iodepth_level": {"1": depth}}]}
            encoded = json.dumps(raw).encode()
            (self.root / "fio.json").write_bytes(encoded)
            value["fio_sha256"] = hashlib.sha256(encoded).hexdigest()
            self.bind("fio_result", value)
        raw_fio(89)
        result = self.admit()["fio_result"]
        self.assertTrue(result["qualification"]["qualified"])
        self.assertFalse(result["qualification"]["queue_depth"]["qualified"])
        value["effective_depth"] = 1
        raw_fio(90)
        self.assertTrue(self.admit()["fio_result"]["qualification"]["queue_depth"]["qualified"])
        value.update(actual_direct=False, read_only=False, quiescent=False)
        self.bind("fio_result", value)
        self.assertFalse(self.admit()["fio_result"]["qualification"]["qualified"])
        value.update(actual_direct=True, read_only=True, quiescent=True)
        value.update(read_ios=0, read_bytes=0, elapsed_seconds=0)
        raw_fio(90)
        self.assertFalse(self.admit()["fio_result"]["qualification"]["qualified"])
        value.update(read_ios=10, read_bytes=40960, elapsed_seconds=10)
        raw_fio(90)
        value["read_ios"] = 11
        self.bind("fio_result", value)
        with self.assertRaises(ValueError):
            self.admit()
        value["read_ios"] = 10
        raw_fio(90, incomplete=True)
        self.assertFalse(self.admit()["fio_result"]["qualification"]["qualified"])
        (self.root / "fio.json").write_text("tampered")
        with self.assertRaises(ValueError):
            self.admit()
        raw_fio(90)
        self.bind("fio_command", ["fio", "--name=case", "--numjobs=1", "--direct=1", "--rw=randread", "--output-format=json"])
        with self.assertRaises(ValueError):
            self.admit()

    def test_trace_loss_and_wait_windows(self):
        self.job = {"case_id": "case", "case_kind": "block_trace", "trace": {"scope": "owner_put"}}
        value = {"schema_version": 1, "case_id": "case", "scope": "owner_put",
                 "available": False, "qualified": False, "device_identity": None,
                 "files": [], "lost_events": None, "truncated": False, "reasons": ["unavailable"]}
        self.bind("block_trace_evidence", value)
        self.admit()
        value.update(available=True, qualified=True, device_identity="dev", lost_events=1, reasons=[])
        self.bind("block_trace_evidence", value)
        with self.assertRaises(ValueError):
            self.admit()
        del self.receipt["block_trace_evidence"]
        self.job = {"case_id": "case", "case_kind": "write_wait_diagnostic",
                    "write_wait_diagnostics": {"enabled": True}}
        windows = [{"wait": name, "observations": 1, "depth_sum": 1, "depth_max": 1,
                    "wake_attempts": 1, "wake_coalesced": 0} for name in
                   ("owner_enqueue_to_dequeue", "admitted_to_append_enqueue", "harvest_to_durable")]
        self.bind("write_wait_metrics", {"schema_version": 1, "case_id": "case", "enabled": True, "windows": windows, "saturated": True, "complete": True})
        self.assertFalse(self.admit()["write_wait_metrics"]["qualification"]["qualified"])
        self.bind("write_wait_metrics", {"schema_version": 1, "case_id": "case", "enabled": True, "windows": windows, "saturated": False, "complete": False})
        qualification = self.admit()["write_wait_metrics"]["qualification"]
        self.assertTrue(qualification["depth"]["qualified"])
        self.assertFalse(qualification["wake"]["qualified"])
        windows[0]["wake_coalesced"] = 2
        self.bind("write_wait_metrics", {"schema_version": 1, "case_id": "case", "enabled": True, "windows": windows, "saturated": False, "complete": True})
        with self.assertRaises(ValueError):
            self.admit()

    def test_kernel_target_readback_requires_each_bound_export(self):
        job = {"nvmeof_target": "kernel", "target_io": "direct"}
        targets = [{"target": {"exports": [{"target_receipt": {
            "buffered_io": 0, "target_cache_verified": True}}]}}]
        self.assertTrue(analyze.qualify_target_buffered_io(job, targets, 1)["qualified"])
        policy = targets[0]["target"]["exports"][0]["target_receipt"]
        for value in (1, None, False):
            policy["buffered_io"] = value
            result = analyze.qualify_target_buffered_io(job, targets, 1)
            self.assertFalse(result["qualified"])
            self.assertEqual(result["reasons"], ["target_buffered_io_unverified"])
        media = {"status": "qualified", "reason_codes": []}
        verdict = analyze.qualify_media_target(media, result)
        self.assertEqual(verdict, {"status": "unqualified", "reason_codes": ["target_buffered_io_unverified"]})
        self.assertEqual(media["status"], "qualified")
        policy["buffered_io"] = 1
        job["target_io"] = "buffered"
        self.assertTrue(analyze.qualify_target_buffered_io(job, targets, 1)["qualified"])
        self.assertFalse(analyze.qualify_target_buffered_io(job, targets, 2)["qualified"])
        policy["target_cache_verified"] = False
        self.assertFalse(analyze.qualify_target_buffered_io(job, targets, 1)["qualified"])
        job.update(nvmeof_target="spdk", target_io="spdk_direct")
        self.assertIsNone(analyze.qualify_target_buffered_io(job, targets, 1))


if __name__ == "__main__":
    unittest.main()
