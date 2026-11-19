#!/usr/bin/env python3
"""Offline checks for exact monitoring image pins and bounded guest caching."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import unittest
import subprocess
import tempfile
from unittest.mock import patch

import cloud_bootstrap as bootstrap

HERE = Path(__file__).resolve().parent


class MonitoringImageAdmissionTests(unittest.TestCase):
    def test_only_collector_host_needs_ui_images_and_all_pins_are_sealed(self):
        source = HERE.parents[1]
        node = bootstrap.monitor_image_plan(source, 0)
        coordinator = bootstrap.monitor_image_plan(source, 5)
        self.assertEqual(set(node), {"library/python", "prom/node-exporter"})
        self.assertEqual(set(coordinator), set(node) | {"prom/prometheus", "grafana/grafana"})
        for image in coordinator.values():
            self.assertRegex(image, r"@sha256:[a-f0-9]{64}$")
        for index in (-1, 6, True):
            with self.assertRaises(ValueError):
                bootstrap.monitor_image_plan(source, index)

    def test_mutable_tag_only_lock_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            lock = Path(tmp) / "experiments/kv-comparison/observability/images.lock.json"
            lock.parent.mkdir(parents=True)
            lock.write_text(json.dumps({"images": {
                "library/python": {"image": "library/python:latest"},
                "prom/node-exporter": {"image": "prom/node-exporter:latest"}}}))
            with self.assertRaises(ValueError):
                bootstrap.monitor_image_plan(tmp, 0)

    def test_image_cache_failures_remain_explicit_and_do_not_block_workload_activation(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        with patch.object(bootstrap.subprocess, "run", side_effect=subprocess.TimeoutExpired("docker pull", 60)):
            receipt = bootstrap.cache_monitor_images(HERE.parents[1], 0, future)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["images"], {})
        self.assertEqual(receipt["gaps"], ["library/python: TimeoutExpired", "prom/node-exporter: TimeoutExpired"])
        self.assertGreaterEqual(receipt["elapsed_seconds"], 0)

    def test_expired_deadline_never_attempts_registry_access(self):
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        with patch.object(bootstrap.subprocess, "run", side_effect=AssertionError("registry touched")):
            receipt = bootstrap.cache_monitor_images(HERE.parents[1], 0, past)
        self.assertFalse(receipt["complete"])
        self.assertEqual(receipt["gaps"], ["library/python: TimeoutError", "prom/node-exporter: TimeoutError"])

    def test_private_guest_uses_cache_without_contacting_registry_and_reports_missing_images(self):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        cached = json.dumps([{"Id": "sha256:" + "a" * 64, "RepoDigests": []}])
        for missing in (False, True):
            observations = [cached, subprocess.CalledProcessError(1, ["docker", "image", "inspect"])] if missing else [cached, cached]
            with self.subTest(missing=missing), \
                    patch.object(bootstrap.subprocess, "run", side_effect=AssertionError("registry contacted")), \
                    patch.object(bootstrap.subprocess, "check_output", side_effect=observations):
                receipt = bootstrap.cache_monitor_images(HERE.parents[1], 0, future, pull=False)
            self.assertEqual(receipt["complete"], not missing)
            self.assertEqual(set(receipt["images"]), {"library/python"} if missing else {"library/python", "prom/node-exporter"})
            self.assertEqual(receipt["gaps"], ["prom/node-exporter: CalledProcessError"] if missing else [])


if __name__ == "__main__":
    unittest.main()
