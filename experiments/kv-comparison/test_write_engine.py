"""Exact write-engine arms and perpetual replay deployment contracts."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import campaign as runner
import cloud_campaign as campaign
import cloud_controller as controller
import cloud_package

SELECTION = "nvmeof_p9_write_engine"


class WriteEngineTests(unittest.TestCase):
    def test_exact_arms_and_cells(self):
        jobs = campaign.make_focused_matrix(SELECTION)
        self.assertEqual(len(jobs), 32)
        self.assertEqual(len({job["case_id"] for job in jobs}), 32)
        for arm, prewrite in (("file", False), ("prewritten", True)):
            cells = [job for job in jobs if job["arm"] == arm]
            self.assertEqual(len(cells), 16)
            puts = [job for job in cells if job["case_kind"] == "remote_put"]
            self.assertEqual({(j["value_size"], j["concurrency"], j["durability_tier"]) for j in puts},
                             {(size, concurrency, tier) for size in (512, 1024)
                              for concurrency in (16, 64, 256) for tier in ("native", "synchronous")})
            reads = [job for job in cells if job["case_kind"] == "live_read"]
            self.assertEqual([(j["value_size"], j["concurrency"], j["coordinator_cache_bytes"]) for j in reads],
                             [(1024, 16, 0), (1024, 64, 0)])
            replays = [job for job in cells if job["case_kind"] == "mainnet_replay"]
            self.assertEqual({j["durability_tier"] for j in replays}, {"native", "synchronous"})
            for job in cells:
                campaign.validate_focused_job(job)
                self.assertIs(job["owner_wal_prewrite"], prewrite)
                self.assertNotIn("owner_wal_backing", job)
                self.assertNotIn("storage_layout", job)
                self.assertNotIn("diagnostics", job)
                self.assertEqual((job["owner_count"], job["owner_topology"]), (5, "separate"))
        self.assertEqual(campaign.selection_owner_hosts(SELECTION), 5)
        for arms in (["file"], ["prewritten"], ["file", "prewritten"]):
            selected = controller.controller_matrix({"mode": "focused", "selection": SELECTION, "arms": arms})
            self.assertEqual(len(selected), 16 * len(arms))
            self.assertEqual({job["arm"] for job in selected}, set(arms))
        with self.assertRaises(controller.SafetyError):
            controller.controller_matrix({"mode": "focused", "selection": SELECTION, "arms": ["raw"]})

    def test_replay_config_and_result_counts(self):
        for job in campaign.make_focused_matrix(SELECTION):
            if job["case_kind"] != "mainnet_replay":
                continue
            options = {"path": "/tmp/perpetual-db", "external_remote_config": {"storage": {"database": [7] * 16}}}
            config = campaign.make_run_config(job, Path("/tmp/replay-case"), options, "a" * 40)
            self.assertEqual(config["backend"]["kind"], "tidehunter_tcp")
            self.assertEqual([node["db_id"] for node in config["backend"]["options"]["databases"]], [0])
            self.assertEqual(campaign.runtime_options(config)["durability_tier"], job["durability_tier"])
            self.assertEqual(config["workload"]["node_databases"], [0])
            self.assertEqual(config["workload"]["repetitions"], 4096)
            self.assertEqual(config["workload"]["minimum_measurement_seconds"], 15)
            self.assertEqual(config["execution"]["concurrency"], 16)
            self.assertEqual(runner.planned_timing_rows(config)["measurement"], 4096 * 510)
            manifest = Path(config["workload"]["manifest_path"])
            self.assertEqual(hashlib.sha256(manifest.read_bytes()).hexdigest(), config["workload"]["manifest_sha256"])
            summary = {"status": "completed", "phase_counts": {
                "scope": "archive-derivable node KV subset, node databases: perpetual",
                "counts": {"checkpoints": 4096, "store_gets": 4096 * 507, "overlay_demands": 4096 * 116,
                           "put_members": 4096 * 654, "write_calls": 4096 * 3, "transactions": 4096 * 45},
                "unknown_outcomes": 0, "unresolved_workers": 0, "profile": {"functional_qualification": True}}}
            campaign.admit_replay_summary(config, summary)
            summary["phase_counts"]["counts"]["put_members"] += 1
            with self.assertRaises(ValueError):
                campaign.admit_replay_summary(config, summary)
            self.assertEqual(controller.planned_write_batches_per_partition(job), 8192)
        for name in cloud_package.REPLAY_FIXTURE_FILES:
            self.assertIn(name, cloud_package.REQUIRED)
            self.assertTrue(cloud_package.allowed(name))
            self.assertTrue((Path(__file__).resolve().parents[2] / name).is_file())

    def test_replay_owner_preparation_uses_pinned_domain(self):
        job = next(j for j in campaign.make_focused_matrix(SELECTION) if j["case_kind"] == "mainnet_replay")
        with tempfile.TemporaryDirectory() as tmp:
            instance = SimpleNamespace(config={"fleet": {"data_root": tmp, "nodes": [
                {"index": i, "address": f"10.0.0.{i + 1}", "rdma_address": f"10.1.0.{i + 1}"} for i in range(5)],
                "server_cpus": 16, "server_memory_mib": 16384}, "binary": "/bin/kv-research"}, remaining=lambda: 100)
            with patch.object(controller, "platform_module", return_value=None), \
                 patch.object(controller, "live_nvme_preparation", return_value={}), \
                 patch.object(controller, "environment", return_value={}), \
                 patch.object(controller, "command", side_effect=RuntimeError("captured typed preparation")):
                with self.assertRaisesRegex(RuntimeError, "captured typed preparation"):
                    controller.Controller.prepare_tide(instance, job, Path(tmp))
            prep = json.loads((Path(tmp) / "prepare-input.json").read_text())
            self.assertEqual(prep["replay_node_db_id"], 0)
            self.assertEqual(len(prep["database"]), 16)
            self.assertEqual(prep["batch_limits"], {"max_operations": 519, "max_encoded_bytes": 500000})
            self.assertEqual(prep["durability_tier"], job["durability_tier"])

    def test_owner_launch_binds_and_validates_prewrite_boolean(self):
        for job in campaign.make_focused_matrix(SELECTION):
            launch = {"node": {"wal_prewrite": False}}
            controller.bind_owner_wal_prewrite(job, launch)
            if job["owner_wal_prewrite"]:
                self.assertIs(launch["node"]["wal_prewrite"], True)
            else:
                self.assertNotIn("wal_prewrite", launch["node"])
            invalid = copy.deepcopy(job)
            invalid["owner_wal_prewrite"] = 1
            with self.assertRaises(controller.SafetyError):
                controller.bind_owner_wal_prewrite(invalid, launch)
            config = {"spec": {"selection": SELECTION}, "fleet": {"data_root": "/tmp/engine", "nodes": [{}] * 5}}
            node = Path(config["fleet"]["data_root"]) / "cells" / job["case_id"] / "node-0"
            launch.update(ready_file=str(node / "ready"), stop_file=str(node / "stop"))
            launch["node"].update(root=str(node / "storage"), wal_prewrite=not job["owner_wal_prewrite"])
            with self.assertRaisesRegex(controller.SafetyError, "wal_prewrite differs"):
                controller.validate_launch(config, launch, job["case_id"], 0)


if __name__ == "__main__":
    unittest.main()
