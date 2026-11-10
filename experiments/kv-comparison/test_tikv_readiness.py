import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import cloud_layout
import local


def regions():
    return {"count": 1, "regions": [{"id": 1, "start_key": "", "end_key": "",
            "epoch": {"conf_ver": 1, "version": 1},
            "peers": [{"id": 10, "store_id": 1}], "leader": {"id": 10, "store_id": 1}}]}


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class SteadyAdmission(unittest.TestCase):
    def test_operators_and_epoch_or_leader_changes_restart_window(self):
        tracker = local.TikvSteadyAdmission()
        original = regions()
        self.assertIsNone(tracker.observe(original, [], [], 0))
        with self.assertRaises(RuntimeError):
            tracker.observe(original, [], [{"region_id": 1}], 4)
        self.assertIsNone(tracker.observe(original, [], [], 5))
        changed = copy.deepcopy(original)
        changed["regions"][0]["epoch"]["version"] += 1
        self.assertIsNone(tracker.observe(changed, [], [], 9))
        changed["regions"][0]["leader"] = {"id": 11, "store_id": 2}
        self.assertIsNone(tracker.observe(changed, [], [], 13))
        proof = tracker.observe(changed, [], [], 18)
        self.assertEqual(proof["observed_stable_seconds"], 5)
        self.assertEqual(proof["first"]["regions"], changed)
        self.assertEqual(proof["last"]["regions"], changed)

    def test_incomplete_topology_and_bad_operator_response_fail_closed(self):
        tracker = local.TikvSteadyAdmission()
        broken = regions()
        del broken["regions"][0]["epoch"]
        for args in ((broken, [], []), (regions(), None, []), (regions(), [], {"error": "unavailable"})):
            with self.subTest(args=args), self.assertRaises(RuntimeError):
                tracker.observe(*args, 0)

    def test_local_readiness_requires_quiet_window_and_retains_proof(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            state = {"backend": "tikv", "directory": directory}
            def pd(path):
                return [{"region_id": 1}] if "kind=waiting" in path and clock.now < 2 else []
            with patch.object(local, "probe", return_value={"regions": regions()}), \
                 patch.object(local, "http", side_effect=pd), \
                 patch.object(local.time, "monotonic", side_effect=clock.monotonic), \
                 patch.object(local.time, "sleep", side_effect=clock.sleep):
                proof = local.ready(state, 10)
            self.assertEqual(clock.now, 7)
            self.assertEqual(proof["steady_admission"]["observed_stable_seconds"], 5)
            self.assertTrue((Path(directory) / "deployment.json").exists())

    def test_cloud_readiness_times_out_for_active_operator(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            config = {"backend": "tikv", "index": -1, "directory": directory}
            def pd(_fleet, path):
                return [{"region_id": 1}] if "kind=waiting" in path else []
            with patch.object(cloud_layout, "probe_once", return_value={"regions": regions()}), \
                 patch.object(cloud_layout, "http", side_effect=pd), \
                 patch.object(local.time, "monotonic", side_effect=clock.monotonic), \
                 patch.object(local.time, "sleep", side_effect=clock.sleep):
                with self.assertRaisesRegex(RuntimeError, "readiness deadline"):
                    cloud_layout.probe({}, config, timeout=3)
            self.assertFalse((Path(directory) / "deployment.json").exists())
            self.assertTrue((Path(directory) / "probe-attempt.json").exists())

    def test_cloud_readiness_restarts_after_probe_failure(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            config = {"backend": "tikv", "index": -1, "directory": directory}
            def probe(_fleet, _config):
                if clock.now == 3:
                    raise RuntimeError("PD unavailable")
                return {"regions": regions()}
            with patch.object(cloud_layout, "probe_once", side_effect=probe), \
                 patch.object(cloud_layout, "http", return_value=[]), \
                 patch.object(local.time, "monotonic", side_effect=clock.monotonic), \
                 patch.object(local.time, "sleep", side_effect=clock.sleep):
                result = cloud_layout.probe({}, config, timeout=12)
            self.assertEqual(clock.now, 9)
            self.assertEqual(result["status"], "ready")
            self.assertEqual(result["proof"]["steady_admission"]["observed_stable_seconds"], 5)
            self.assertTrue((Path(directory) / "deployment.json").exists())



if __name__ == "__main__":
    unittest.main()
