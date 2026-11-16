import argparse
import copy
import io
import json
from pathlib import Path
import subprocess
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


def owned_container(service, status="running", exit_code=0, oom=False):
    return {"Id": service, "Image": "sha256:pinned",
            "Config": {"Labels": {"com.docker.compose.service": service}},
            "HostConfig": {"Memory": (3 * 1024**3 if service == "tikv0" else 512 * 1024**2),
                           "NanoCpus": 2_000_000_000 if service == "tikv0" else 1_000_000_000},
            "State": {"Status": status, "Running": status == "running", "ExitCode": exit_code,
                      "OOMKilled": oom, "Error": ""}}


class LocalReadiness(unittest.TestCase):
    def test_tikv_cap_covers_measured_startup_with_margin_without_swap(self):
        measured_peak = 2_224_484_352
        quantum = 512 * 1024**2
        with_margin = (measured_peak * 5 + 3) // 4
        required = (with_margin + quantum - 1) // quantum * quantum
        self.assertEqual(required, 3 * 1024**3)
        for layout, expected_count in (("qualification", 1), ("distributed", 3)):
            spec, count = local.topology("tikv", layout, Path("/owned"), "owner")
            self.assertEqual(count, expected_count)
            self.assertEqual(set(spec["services"]), {"pd0", *(f"tikv{i}" for i in range(count))})
            self.assertEqual(spec["services"]["pd0"]["mem_limit"], "512m")
            for i in range(count):
                service = spec["services"][f"tikv{i}"]
                if layout == "qualification":
                    self.assertEqual(service["mem_limit"], "3g")
                    self.assertEqual(service["memswap_limit"], "3g")
                else:
                    self.assertEqual(service["mem_limit"], "1536m")
                    self.assertNotIn("memswap_limit", service)
                self.assertEqual(service["restart"], "no")
                self.assertEqual(service["ulimits"]["nofile"],
                                 {"soft": local.TIKV_NOFILE, "hard": local.TIKV_NOFILE})

    def test_exited_or_dead_pd_and_tikv_are_terminal_before_deadline(self):
        for service in ("pd0", "tikv0"):
            for status, code, oom in (("exited", 137, True), ("exited", 1, False), ("dead", 1, False)):
                with self.subTest(service=service, status=status, code=code), \
                     tempfile.TemporaryDirectory() as directory:
                    clock = Clock()
                    items = [owned_container(name, status, code, oom) if name == service else owned_container(name)
                             for name in ("pd0", "tikv0")]
                    state = {"backend": "tikv", "nodes": 1, "directory": directory}
                    with patch.object(local, "inspect_owned", return_value=items) as inspect, \
                         patch.object(local, "http") as http, \
                         patch.object(local.time, "monotonic", side_effect=clock.monotonic), \
                         patch.object(local.time, "sleep") as sleep:
                        with self.assertRaisesRegex(
                                RuntimeError, f"requested tikv readiness failed: requested service {service} "
                                              f"{status}: exit={code} oom={oom}"):
                            local.ready(state, 180)
                    inspect.assert_called_once_with(state)
                    http.assert_not_called()
                    sleep.assert_not_called()
                    self.assertFalse((Path(directory) / "deployment.json").exists())

    def test_absent_or_created_containers_can_reach_full_steady_admission(self):
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            state = {"backend": "tikv", "nodes": 1, "directory": directory}
            started = [owned_container("pd0"), owned_container("tikv0")]
            samples = [[], [started[0], owned_container("tikv0", "created")]]

            def inspect(_state):
                return samples.pop(0) if samples else started

            def pd(path):
                return {"/pd/api/v1/stores": {"stores": [{"store": {"state_name": "Up"}}]},
                        "/pd/api/v1/regions": regions(),
                        "/pd/api/v1/config/replicate": {"max-replicas": 1},
                        "/pd/api/v1/version": "8.5.4"}.get(path, [])

            with patch.object(local, "inspect_owned", side_effect=inspect), \
                 patch.object(local, "http", side_effect=pd), \
                 patch.object(local.urllib.request, "urlopen", side_effect=lambda *a, **k: io.StringIO("{}")), \
                 patch.object(local.time, "monotonic", side_effect=clock.monotonic), \
                 patch.object(local.time, "sleep", side_effect=clock.sleep):
                proof = local.ready(state, 10)
            self.assertEqual(clock.now, 7)
            self.assertEqual(proof["steady_admission"]["observed_stable_seconds"], 5)
            self.assertTrue((Path(directory) / "deployment.json").exists())

    def test_terminal_startup_failure_retains_state_and_both_log_streams(self):
        items = [owned_container("pd0"), owned_container("tikv0", "exited", 137, True)]

        def run(argv, **_kwargs):
            if argv[:3] == ["docker", "image", "inspect"]:
                return subprocess.CompletedProcess(argv, 0, json.dumps(
                    [{"Id": "sha256:pinned", "RepoDigests": ["pinned@sha256:test"]}]), "")
            self.assertEqual(argv[:4], ["docker", "logs", "--tail", "300"])
            return subprocess.CompletedProcess(argv, 0, "startup stdout\n", "startup stderr\n")

        with tempfile.TemporaryDirectory() as directory, \
             patch.object(local.platform, "system", return_value="Linux"), \
             patch.object(local.socket, "socket"), patch.object(local, "compose"), \
             patch.object(local, "inspect_owned", return_value=items), \
             patch.object(local.subprocess, "run", side_effect=run), \
             patch.object(local.time, "sleep") as sleep:
            root = Path(directory)
            args = argparse.Namespace(backend="tikv", layout="qualification", state=root, timeout=180)
            with self.assertRaisesRegex(RuntimeError, "tikv0 exited: exit=137 oom=True"):
                local.start(args)
            retained = json.loads((root / "failure/containers.json").read_text())
            self.assertEqual(retained[1]["state"], items[1]["State"])
            for service in ("pd0", "tikv0"):
                self.assertEqual((root / f"failure/{service}.log").read_text(), "startup stdout\nstartup stderr\n")
            config = (root / "failure/tikv.toml").read_text()
            self.assertIn('memory-usage-limit = "1GB"', config)
            self.assertIn('[storage.block-cache]\ncapacity = "256MB"', config)
            self.assertFalse((root / "deployment.json").exists())
            sleep.assert_not_called()


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
