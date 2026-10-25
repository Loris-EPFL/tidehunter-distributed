#!/usr/bin/env python3
"""Pure activation-state validation: no AWS access or mocked publication success."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import unittest
import subprocess
import tempfile
from unittest.mock import patch

import cloud_bootstrap as bootstrap

HERE = Path(__file__).resolve().parent


def plan_fixture():
    run = "th-0123456789abcdef"
    return {"run_id": run, "account": "354775971805", "region": "us-east-2",
        "bucket": "tidehunter-ci-354775971805-us-east-1", "bucket_region": "us-east-1",
        "bundle_key": run + "/inputs/bundle.tar.gz", "bundle_sha256": "a" * 64,
        "bundle_bytes": 1024, "source_sha256": "b" * 64, "binary_sha256": "c" * 64,
        "deadline_utc": "2099-01-01T03:00:00Z", "ttl_seconds": 10800, "instance_type": "g6.8xlarge"}


def config_fixture(plan):
    run = plan["run_id"]
    stage = Path("/mnt/tidehunter") / run / "package"
    return {"account": plan["account"], "bucket": plan["bucket"], "bucket_region": plan["bucket_region"],
        "prefix": run, "source_revision": "d" * 40, "source_sha256": plan["source_sha256"],
        "binary_sha256": plan["binary_sha256"], "binary_blake2b512": "e" * 128,
        "repository_root": str(stage / "source"), "binary": str(stage / "runtime/bin/kv-research"),
        "fdb_library_dir": str(stage / "runtime/lib"), "deadline_utc": plan["deadline_utc"],
        "spec": json.loads((HERE / "cloud-campaign.json").read_text()),
        "fleet": {"schema_version": 1, "run_id": run, "region": plan["region"],
            "data_root": "/mnt/tidehunter-kv-comparison/" + run,
            "coordinator": {"instance_id": "i-00000000000000001", "address": "10.0.0.1"},
            "nodes": [{"index": i, "instance_id": f"i-{i + 2:017x}", "address": f"10.0.0.{i + 2}"} for i in range(5)],
            "server_cpus": 4, "server_memory_mib": 8192, "client_cpus": 4, "client_memory_mib": 8192,
            "replication_factor": 1}}


def pending_fixture(plan):
    return {"schema_version": 1, "state": "pending", "run_id": plan["run_id"], "source_sha256": plan["source_sha256"]}


def ready_fixture(plan, config):
    return {"schema_version": 1, "state": "ready", "run_id": plan["run_id"],
            "source_sha256": plan["source_sha256"], "config": config}


class ActivationStateTests(unittest.TestCase):
    def setUp(self):
        self.plan = plan_fixture()
        self.config = config_fixture(self.plan)

    def test_only_explicit_valid_pending_means_wait(self):
        record = pending_fixture(self.plan)
        self.assertIsNone(bootstrap.activation_config(self.plan, record))
        for value in (None, {}, [], "pending", self.config):
            with self.subTest(value=type(value).__name__), self.assertRaises(ValueError):
                bootstrap.activation_config(self.plan, value)


    def test_both_states_reject_stale_headers_unknown_states_and_extra_fields(self):
        mutations = [lambda r: r.update(schema_version=True), lambda r: r.update(schema_version=2),
            lambda r: r.update(schema_version=1.0),
            lambda r: r.update(schema_version="1"), lambda r: r.update(state="unknown"),
            lambda r: r.update(state=None), lambda r: r.pop("state"),
            lambda r: r.update(run_id="th-fedcba9876543210"), lambda r: r.update(source_sha256="f" * 64),
            lambda r: r.pop("source_sha256"), lambda r: r.update(unrecognized="not-admitted")]
        for state in ("pending", "ready"):
            original = pending_fixture(self.plan) if state == "pending" else ready_fixture(self.plan, self.config)
            for index, mutation in enumerate(mutations):
                broken = copy.deepcopy(original)
                mutation(broken)
                with self.subTest(state=state, mutation=index), self.assertRaises(ValueError):
                    bootstrap.activation_config(self.plan, broken)

    def test_pending_cannot_carry_config_and_ready_cannot_omit_it(self):
        pending = pending_fixture(self.plan)
        pending["config"] = None
        with self.assertRaises(ValueError):
            bootstrap.activation_config(self.plan, pending)
        ready = ready_fixture(self.plan, self.config)
        ready.pop("config")
        with self.assertRaises(ValueError):
            bootstrap.activation_config(self.plan, ready)
        for invalid in (None, [], "raw-config", {}):
            with self.subTest(config=invalid), self.assertRaises(ValueError):
                bootstrap.activation_config(self.plan, ready_fixture(self.plan, invalid))

    def test_ready_header_cannot_authorize_a_different_run_source_or_binary(self):
        alternate_plans = [{**self.plan, "source_sha256": "f" * 64},
                           {**self.plan, "binary_sha256": "f" * 64},
                           {**self.plan, "run_id": "th-fedcba9876543210"}]
        for alternative in alternate_plans:
            config = config_fixture(alternative)
            with self.subTest(run=alternative["run_id"], source=config["source_sha256"]), self.assertRaises(ValueError):
                bootstrap.activation_config(self.plan, ready_fixture(self.plan, config))
            with self.assertRaises(ValueError):
                bootstrap.activation_record(self.plan, config)

    def test_ready_binds_region_bucket_deadline_and_package_paths_to_plan(self):
        mutations = [lambda c: c["fleet"].update(region="us-west-2"),
            lambda c: c.update(account="000000000000"),
            lambda c: c.update(bucket="different-private-bucket"), lambda c: c.update(bucket_region="us-west-2"),
            lambda c: c.update(deadline_utc="2099-01-01T03:00:01Z"),
            lambda c: c.update(repository_root="/opt/foreign-package/source", binary="/opt/foreign-package/runtime/bin/kv-research",
                               fdb_library_dir="/opt/foreign-package/runtime/lib")]
        for index, mutation in enumerate(mutations):
            config = copy.deepcopy(self.config)
            mutation(config)
            with self.subTest(mutation=index), self.assertRaises(ValueError):
                bootstrap.activation_config(self.plan, ready_fixture(self.plan, config))

    def test_ready_uses_existing_strict_raw_config_schema(self):
        for field in ("binary_sha256", "fleet", "spec"):
            config = copy.deepcopy(self.config)
            config.pop(field)
            with self.subTest(missing=field), self.assertRaises(ValueError):
                bootstrap.activation_config(self.plan, ready_fixture(self.plan, config))
        config = copy.deepcopy(self.config)
        config["activation_state"] = "ready"
        with self.assertRaises(ValueError):
            bootstrap.activation_config(self.plan, ready_fixture(self.plan, config))

    def test_equal_utc_instants_are_not_compared_as_text(self):
        config = copy.deepcopy(self.config)
        config["deadline_utc"] = "2099-01-01T03:00:00+00:00"
        admitted = bootstrap.activation_config(self.plan, ready_fixture(self.plan, config))
        self.assertEqual(admitted["deadline_utc"], config["deadline_utc"])


class LaunchLifetimeTests(unittest.TestCase):
    def test_render_rejects_unreachable_or_expired_deadline_before_launch(self):
        now = datetime(2026, 10, 25, 6, 29, 19, tzinfo=timezone.utc)
        for seconds, ttl in ((9802.879533, 9775), (10801, 10800), (0, 10800), (-1, 10800)):
            plan = plan_fixture()
            plan.update(deadline_utc=(now + timedelta(seconds=seconds)).isoformat(), ttl_seconds=ttl)
            with self.subTest(remaining=seconds, ttl=ttl), patch.object(bootstrap, "datetime", wraps=datetime) as clock:
                clock.now.return_value = now
                with self.assertRaises(ValueError):
                    bootstrap.render_userdata(plan)


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


if __name__ == "__main__":
    unittest.main()
