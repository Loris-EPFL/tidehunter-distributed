"""Resource deltas must belong to one live owned cgroup, not inferred zeroes."""
import copy
from pathlib import Path
import tempfile
import unittest

import resource_accounting as resources


class ResourceAccountingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.group = self.root / "system.slice/owned.service"
        self.group.mkdir(parents=True)
        self.processes = [{"role": "client", "cgroup": "/system.slice/owned.service"}]
        self.write_counters(usage=100000, periods=3, throttled=1, throttle_us=1000, read=4096, write=8192)

    def write_counters(self, *, usage, periods, throttled, throttle_us, read, write):
        (self.group / "cpu.stat").write_text(
            f"usage_usec {usage}\nuser_usec 0\nsystem_usec 0\ncore_sched.force_idle_usec 0\n"
            f"nr_periods {periods}\nnr_throttled {throttled}\nthrottled_usec {throttle_us}\n")
        (self.group / "memory.current").write_text("1048576\n")
        (self.group / "memory.peak").write_text("2097152\n")
        (self.group / "io.stat").write_text(f"259:1 rbytes={read} wbytes={write} rios=1 wios=2\n")

    def capture(self, when):
        return resources.capture(-1, "boot-one", self.processes, cgroup_root=self.root, clock=lambda: when)

    def pair(self):
        before = self.capture(1_000_000_000)
        self.write_counters(usage=800000, periods=13, throttled=3, throttle_us=6000, read=12288, write=24576)
        after = self.capture(3_000_000_000)
        return before, after

    def test_measured_deltas_end_memory_and_host_local_elapsed(self):
        before, after = self.pair()
        row = resources.interval([before], [after])["processes"][0]
        self.assertTrue(row["available"])
        self.assertEqual(row["elapsed_seconds"], 2)
        self.assertEqual(row["cpu_seconds"], .7)
        self.assertEqual(row["cpu_throttled_seconds"], .005)
        self.assertEqual(row["cpu_periods"], 10)
        self.assertEqual(row["cpu_throttled_periods"], 2)
        self.assertEqual(row["io_read_bytes"], 8192)
        self.assertEqual(row["io_write_bytes"], 16384)
        self.assertEqual(row["memory_current_bytes"], 1048576)
        self.assertEqual(row["memory_peak_bytes"], 2097152)

    def test_reboot_replaced_cgroup_and_counter_reset_are_unavailable(self):
        before, after = self.pair()
        for mutate in (lambda f: f.update(boot_id="different-boot"),
                lambda f: f["processes"][0].update(inode=f["processes"][0]["inode"] + 1),
                lambda f: f["processes"][0]["cpu"].update(usage_usec=1),
                lambda f: f["processes"][0].update(monotonic_ns=1_000_000_000),
                lambda f: f["processes"][0]["io_devices"]["259:1"].update(rbytes=0),
                lambda f: f["processes"][0]["cpu"].update(nr_throttled=100)):
            broken = copy.deepcopy(after)
            mutate(broken)
            with self.subTest(mutator=mutate):
                row = resources.interval([before], [broken])["processes"][0]
                self.assertFalse(row["available"])
                self.assertEqual(set(row), {"host_index", "process_role", "available", "reason"})

    def test_missing_boundary_is_not_zero_cpu(self):
        before, _ = self.pair()
        row = resources.interval([before], [])["processes"][0]
        self.assertFalse(row["available"])
        self.assertNotIn("cpu_seconds", row)
        (self.group / "cpu.stat").unlink()
        unavailable = self.capture(3_000_000_000)
        self.assertFalse(unavailable["processes"][0]["available"])
        row = resources.interval([before], [unavailable])["processes"][0]
        self.assertFalse(row["available"])
        self.assertNotIn("cpu_seconds", row)

    def test_optional_fields_absent_stay_absent_and_real_zero_is_retained(self):
        (self.group / "memory.peak").unlink()
        (self.group / "io.stat").unlink()
        before, after = self.capture(1), self.capture(1_000_000_001)
        for frame in (before, after):
            frame["processes"][0]["cpu"].pop("throttled_usec")
        row = resources.interval([before], [after])["processes"][0]
        self.assertTrue(row["available"])
        self.assertEqual(row["cpu_seconds"], 0)
        self.assertEqual(row["cpu_periods"], 0)
        self.assertNotIn("cpu_throttled_seconds", row)
        self.assertNotIn("io_read_bytes", row)
        self.assertNotIn("memory_peak_bytes", row)

    def test_new_observed_io_device_starts_at_zero_but_disappearing_device_is_not_zero(self):
        before, after = self.pair()
        after["processes"][0]["io_devices"]["259:2"] = {"rbytes": 1024, "wbytes": 2048}
        row = resources.interval([before], [after])["processes"][0]
        self.assertEqual(row["io_read_bytes"], 8192 + 1024)
        after["processes"][0]["io_devices"].pop("259:1")
        row = resources.interval([before], [after])["processes"][0]
        self.assertFalse(row["available"])
        self.assertNotIn("io_read_bytes", row)

    def test_foreign_root_traversal_symlink_and_duplicate_roles_are_rejected(self):
        for group in ("/", "relative", "/system.slice/../owned.service"):
            with self.subTest(group=group), self.assertRaises(ValueError):
                resources.capture(-1, "boot", [{"role": "client", "cgroup": group}], cgroup_root=self.root)
        with tempfile.TemporaryDirectory() as other:
            (self.root / "foreign").symlink_to(other)
            with self.assertRaises(ValueError):
                resources.capture(-1, "boot", [{"role": "client", "cgroup": "/foreign"}], cgroup_root=self.root)
        with self.assertRaises(ValueError):
            resources.capture(-1, "boot", self.processes * 2, cgroup_root=self.root)

    def test_malformed_kernel_values_are_not_suppressed_as_zero(self):
        for text in ("usage_usec -1\n", "usage_usec 1\nusage_usec 2\n", "nr_periods 2\n"):
            (self.group / "cpu.stat").write_text(text)
            captured = self.capture(1)
            self.assertFalse(captured["processes"][0]["available"])
        with self.assertRaises(ValueError):
            resources.io_devices("259:1 rbytes=1 wbytes=2\n259:1 rbytes=3 wbytes=4\n")


if __name__ == "__main__":
    unittest.main()
