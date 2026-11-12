import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import spdk_measurement


class SpdkMeasurementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.start = []
        for index in range(5):
            self.start.append({"index": index, "complete": True, "target": {
                "kind": "spdk-target-stats", "target_implementation": "spdk",
                "campaign_id": "generation-" + str(index), "unit_name": "target-" + str(index),
                "pid": 100 + index, "process_start_ticks": 500, "binary_sha256": "a" * 64,
                "bdev_iostat": {"ticks": 1000, "tick_rate": 1000000,
                    "bdevs": [{"name": "wal", "bytes_read": 4096, "num_read_ops": 1,
                               "bytes_written": 0, "num_write_ops": 0, "read_latency_ticks": 10, "io_error": {}}]}}})
        self.end = copy.deepcopy(self.start)
        for index, row in enumerate(self.end):
            stats = row["target"]["bdev_iostat"]
            stats["ticks"] += 2000000
            stats["bdevs"][0].update(bytes_read=4096 * (index + 3), num_read_ops=index + 3,
                                      read_latency_ticks=10 + (index + 2) * (index + 1) * 20)

    def bind(self):
        binding = {}
        for boundary, rows in (("start", self.start), ("end", self.end)):
            path = self.directory / ("spdk-target-" + boundary + ".json")
            raw = json.dumps(rows).encode()
            path.write_bytes(raw)
            binding[boundary] = {"path": path.name, "sha256": hashlib.sha256(raw).hexdigest()}
        return binding

    def test_excludes_startup_reads_and_weights_completion_time_by_io_count(self):
        observed = spdk_measurement.admit(self.directory, self.bind())
        self.assertEqual(observed["read_ops"], 20)
        self.assertEqual(observed["read_bytes"], 20 * 4096)
        self.assertAlmostEqual(observed["mean_read_completion_us"], 70)
        self.assertEqual([row["read_ops"] for row in observed["owners"]], [2, 3, 4, 5, 6])

    def test_explicit_owner_counts(self):
        for count in (1, 5):
            with self.subTest(count=count):
                original_start, original_end = self.start, self.end
                self.start, self.end = original_start[:count], original_end[:count]
                result = spdk_measurement.admit(self.directory, self.bind(), owner_count=count)
                self.assertEqual([row["index"] for row in result["owners"]], list(range(count)))
                self.start, self.end = original_start, original_end

    def test_invalid_owner_count_is_rejected(self):
        binding = self.bind()
        for count in (True, False, 0, 6, "1", 1.0):
            with self.subTest(count=count), self.assertRaises(ValueError):
                spdk_measurement.admit(self.directory, binding, owner_count=count)
            with self.subTest(snapshot_count=count), self.assertRaises(ValueError):
                spdk_measurement._snapshot(self.directory, binding["start"], "start", owner_count=count)

    def test_boundary_owner_cardinality_and_indices(self):
        for boundary in ("start", "end"):
            for count in (1, 5):
                for mutation in ("missing", "extra", "duplicate", "noncontiguous"):
                    with self.subTest(boundary=boundary, count=count, mutation=mutation):
                        start, end = self.start, self.end
                        self.start, self.end = copy.deepcopy(start[:count]), copy.deepcopy(end[:count])
                        rows = getattr(self, boundary)
                        if mutation == "missing":
                            rows.pop()
                        elif mutation == "extra":
                            rows.append(copy.deepcopy(rows[0]))
                        elif mutation == "duplicate":
                            if count == 1:
                                rows.append(copy.deepcopy(rows[0]))
                            else:
                                rows[-1]["index"] = 0
                        else:
                            rows[-1]["index"] = count
                        with self.assertRaises(ValueError):
                            spdk_measurement.admit(self.directory, self.bind(), owner_count=count)
                        self.start, self.end = start, end

    def test_zero_reads_allowed_only_for_write_only_control(self):
        for start, end in zip(self.start, self.end):
            end["target"]["bdev_iostat"]["bdevs"] = copy.deepcopy(start["target"]["bdev_iostat"]["bdevs"])
        binding = self.bind()
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, binding)
        result = spdk_measurement.admit(self.directory, binding, allow_zero_reads=True)
        self.assertEqual(result["read_ops"], 0)
        self.assertIsNone(result["mean_read_completion_us"])
        self.end[0]["target"]["bdev_iostat"]["bdevs"][0]["num_write_ops"] = 1
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind(), allow_zero_reads=True)

    def test_process_restart_cannot_be_subtracted_as_one_interval(self):
        self.end[0]["target"]["process_start_ticks"] += 1
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind())

    def test_missing_owner_cannot_be_treated_as_zero_activity(self):
        self.end.pop()
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind())

    def test_counter_reset_cannot_become_negative_device_latency(self):
        self.end[0]["target"]["bdev_iostat"]["bdevs"][0]["read_latency_ticks"] = 0
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind())

    def test_sealed_read_target_must_not_include_writes(self):
        self.end[1]["target"]["bdev_iostat"]["bdevs"][0]["num_write_ops"] = 1
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind())

    def test_failed_io_is_rejected_even_when_successful_write_counter_is_zero(self):
        self.end[0]["target"]["bdev_iostat"]["bdevs"][0]["io_error"]["failed"] = 1
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind())

    def test_mixed_target_binaries_are_not_one_comparison_population(self):
        for rows in (self.start, self.end):
            rows[1]["target"]["binary_sha256"] = "b" * 64
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, self.bind())

    def test_modified_boundary_is_rejected_even_if_json_is_valid(self):
        binding = self.bind()
        (self.directory / binding["end"]["path"]).write_text(json.dumps(self.start))
        with self.assertRaises(ValueError):
            spdk_measurement.admit(self.directory, binding)


class LiveSpdkMeasurementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.start = []
        for index in range(5):
            daemon = {"pid": 100 + index, "process_start_ticks": 10, "executable": "/bin/nvmf_tgt",
                      "executable_device": 1, "executable_inode": 2, "binary_sha256": "a" * 64,
                      "rpc_socket": "/run/rpc", "rpc_device": 3, "rpc_inode": 4}
            self.start.append({"case_id": "case", "index": index, "target": {
                "daemon": daemon, "bdevs": [{"name": "wal"}], "subsystems": [],
                "iostat": {"ticks": 100, "tick_rate": 1000000, "bdevs": [
                    {"name": "wal", "bytes_read": 4096, "num_read_ops": 1,
                     "bytes_written": 4096, "num_write_ops": 1,
                     "read_latency_ticks": 10, "write_latency_ticks": 20}]}}})
        self.end = copy.deepcopy(self.start)
        for row in self.end:
            stats = row["target"]["iostat"]
            stats["ticks"] += 1000000
            stats["bdevs"][0].update(bytes_read=12288, num_read_ops=3, read_latency_ticks=50,
                                      bytes_written=8192, num_write_ops=2, write_latency_ticks=50)

    def bind(self):
        binding = {}
        for boundary, rows in (("start", self.start), ("end", self.end)):
            name = "spdk-target-" + boundary + ".json"
            raw = json.dumps(rows).encode()
            (self.directory / name).write_bytes(raw)
            binding[boundary] = {"path": name, "sha256": hashlib.sha256(raw).hexdigest()}
        return binding

    def test_live_read_write_and_missing_error_counters(self):
        result = spdk_measurement.admit_live(self.directory, self.bind())
        self.assertTrue(result["available"])
        self.assertEqual(result["kind"], "live")
        self.assertEqual((result["read_ops"], result["write_ops"]), (10, 5))
        self.assertEqual((result["read_bytes"], result["write_bytes"]), (40960, 20480))
        self.assertEqual(result["mean_read_completion_us"], 20)
        self.assertEqual(result["mean_write_completion_us"], 30)
        self.assertFalse(result["io_error_available"])
        self.assertNotIn("io_errors", result)

    def test_partial_preserves_owners_without_aggregate(self):
        self.end[2] = {"case_id": "case", "index": 2, "available": False, "reason": "RPC unavailable"}
        result = spdk_measurement.admit_live(self.directory, self.bind())
        self.assertFalse(result["available"])
        self.assertEqual(len(result["owners"]), 5)
        self.assertFalse(result["owners"][2]["available"])
        self.assertIn("RPC unavailable", result["owners"][2]["reason"])
        self.assertNotIn("read_ops", result)

    def test_rotation_and_process_restart_are_not_subtracted(self):
        self.end[0]["target"]["bdevs"][0]["name"] = "wal-next"
        self.end[0]["target"]["iostat"]["bdevs"][0]["name"] = "wal-next"
        self.end[1]["target"]["daemon"]["process_start_ticks"] += 1
        result = spdk_measurement.admit_live(self.directory, self.bind())
        self.assertFalse(result["owners"][0]["available"])
        self.assertFalse(result["owners"][1]["available"])
        self.assertTrue(result["owners"][2]["available"])

    def test_error_counters_are_reported_only_when_present_at_both_boundaries(self):
        for row in self.start:
            row["target"]["iostat"]["bdevs"][0]["io_error"] = {}
        for row in self.end:
            row["target"]["iostat"]["bdevs"][0]["io_error"] = {"failed": 1}
        result = spdk_measurement.admit_live(self.directory, self.bind())
        self.assertTrue(result["io_error_available"])
        self.assertEqual(result["io_errors"]["failed"], 5)

    def test_counter_reset_and_binding_tampering(self):
        self.end[0]["target"]["iostat"]["bdevs"][0]["bytes_written"] = 0
        binding = self.bind()
        self.assertFalse(spdk_measurement.admit_live(self.directory, binding)["owners"][0]["available"])
        (self.directory / binding["end"]["path"]).write_text("[]")
        with self.assertRaises(ValueError):
            spdk_measurement.admit_live(self.directory, binding)

    def test_reused_name_with_rotated_backing_inode_is_unavailable(self):
        fd = {"name": "wal", "available": True, "filename": "/wal", "device": 1,
              "inode": 2, "mode": 33152, "bytes": 4096, "descriptors": [{"fd": 9, "flags": 2}]}
        self.start[0]["target"]["backing_fds"] = [copy.deepcopy(fd)]
        self.end[0]["target"]["backing_fds"] = [copy.deepcopy(fd)]
        self.end[0]["target"]["backing_fds"][0]["inode"] += 1
        result = spdk_measurement.admit_live(self.directory, self.bind())
        self.assertFalse(result["owners"][0]["available"])
        self.assertIn("backing file identity changed", result["owners"][0]["reason"])

    def test_cross_case_rows_are_rejected(self):
        self.end[0]["case_id"] = "other"
        with self.assertRaises(ValueError):
            spdk_measurement.admit_live(self.directory, self.bind())
