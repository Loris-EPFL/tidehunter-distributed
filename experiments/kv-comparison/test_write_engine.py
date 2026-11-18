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
    def test_p13_keeps_controls_and_put_workload_shape(self):
        selection = "nvmeof_p13_write_engine"
        legacy = campaign.make_focused_matrix(SELECTION)
        jobs = campaign.make_focused_matrix(selection)
        self.assertEqual(len({job["case_id"] for job in jobs}), 50)
        for arm, prewrite in (("file", False), ("prewritten", True)):
            spec = {"mode": "focused", "selection": selection, "owner_hosts": 2, "arms": [arm]}
            selected = controller.controller_matrix(spec)
            self.assertEqual(selected, [job for job in jobs if job["arm"] == arm])
            self.assertEqual(len(selected), 25)
            self.assertTrue(all("checkpoint_pipeline_depth" not in job for job in selected[:-6]))
            self.assertEqual([(job["checkpoint_pipeline_depth"], job["durability_tier"]) for job in selected[-6:]],
                             [(depth, tier) for depth in (2, 4, 8) for tier in ("synchronous", "native")])
            puts = [job for job in selected if job["case_kind"] == "remote_put"]
            self.assertEqual(len(puts), 8)
            self.assertEqual({(job["concurrency"], job["durability_tier"]) for job in puts},
                             {(c, t) for c in (8, 16, 32, 64) for t in ("synchronous", "native")})
            replay = [job for job in selected if job["case_kind"] == "mainnet_replay"]
            self.assertEqual(len(replay), 16)
            self.assertEqual({(job["concurrency"], job["durability_tier"]) for job in replay},
                             {(n, t) for n in (4, 8, 16, 32, 64) for t in ("synchronous", "native")})
            self.assertEqual(sum(job["case_kind"] == "live_read" for job in selected), 1)
            for job in selected:
                self.assertIs(job["owner_wal_prewrite"], prewrite)
                self.assertEqual(campaign.job_owner_hosts(job), 2)
                if job in puts:
                    self.assertEqual(job["value_size"], 512)
                if job in replay:
                    source_c = 16
                else:
                    source_c = {8: 16, 32: 64}.get(job["concurrency"], job["concurrency"])
                source = next(j for j in legacy if j["arm"] == arm and j["case_kind"] == job["case_kind"]
                              and j["durability_tier"] == job["durability_tier"] and j["concurrency"] == source_c)
                expected = dict(source, case_id=job["case_id"], selection=selection,
                                concurrency=job["concurrency"], profile=job["profile"])
                if job in replay:
                    n = job["concurrency"]
                    deadline, estimate = {4: (1000, 480), 8: (800, 240)}.get(n, (600, 120))
                    depth = job.get("checkpoint_pipeline_depth")
                    expected["measurement_sizing"] = dict(
                        source["measurement_sizing"], case_deadline_seconds=deadline, expected_case_seconds=estimate)
                    profile = (source["profile"] if n == 16
                               else f"G35S-mainnet-perpetual-c{n}-separate-o2")
                    if depth is not None:
                        expected["checkpoint_pipeline_depth"] = depth
                        profile = f"G35S-mainnet-perpetual-c16-k{depth}-separate-o2"
                    self.assertEqual(job["profile"], profile)
                    options = {"path": "/tmp/perpetual-db",
                               "external_remote_config": {"storage": {"database": [7] * 16}}}
                    config = campaign.make_run_config(job, Path("/tmp/replay-case"), options, "a" * 40, deadline)
                    self.assertEqual(config["execution"]["concurrency"], n)
                    if depth is None:
                        self.assertNotIn("checkpoint_pipeline_depth", config["workload"])
                    else:
                        self.assertEqual(n, 16)
                        self.assertEqual(config["workload"]["checkpoint_pipeline_depth"], depth)
                        summary = {"status": "completed", "phase_counts": {
                            "scope": "archive-derivable node KV subset, node databases: perpetual",
                            "counts": {name: 4096 * count for name, count in
                                       campaign.PERPETUAL_REPLAY_PER_REPETITION.items()},
                            "unknown_outcomes": 0, "unresolved_workers": 0,
                            "profile": {"functional_qualification": True, "case_identity": {"K": depth},
                                        "pipeline": {"checkpoint_pipeline_depth": depth}}}}
                        campaign.admit_replay_summary(config, summary)
                        summary["phase_counts"]["profile"]["case_identity"]["K"] = 1
                        with self.assertRaisesRegex(ValueError, "pipeline depth differs"):
                            campaign.admit_replay_summary(config, summary)
                    self.assertGreaterEqual(config["execution"]["queue_capacity"], n)
                    self.assertEqual(config["limits"]["deadline_seconds"], deadline)
                    owner_seconds = controller.tide_owner_service_seconds(
                        {"case_deadline_seconds": 600}, job, owner_count=2, remaining=1800)
                    self.assertEqual(owner_seconds, deadline + 300)
                    self.assertEqual(runner.planned_timing_rows(config)["measurement"], 4096 * 510)
                self.assertEqual(job, expected)

    def test_p13_sample_archive_bound_is_separate_from_p9(self):
        selection = "nvmeof_p13_write_engine"
        self.assertNotIn(SELECTION, campaign.WRITE_ENGINE_ARCHIVE_LIMIT_BYTES)
        expected = 11_703_701_504
        with patch.dict(campaign.WRITE_ENGINE_ARCHIVE_LIMIT_BYTES, {selection: expected - 1}):
            with self.assertRaisesRegex(ValueError, "drop cells, not measurement seconds"):
                campaign._write_engine_matrix(selection)

    def test_exact_arms_and_cells(self):
        jobs = campaign.make_focused_matrix(SELECTION)
        self.assertEqual(len(jobs), 18)
        self.assertEqual(len({job["case_id"] for job in jobs}), 18)
        for arm, prewrite in (("file", False), ("prewritten", True)):
            cells = [job for job in jobs if job["arm"] == arm]
            self.assertEqual(len(cells), 9)
            puts = [job for job in cells if job["case_kind"] == "remote_put"]
            self.assertEqual({(j["value_size"], j["concurrency"], j["durability_tier"]) for j in puts},
                             {(size, concurrency, tier) for size in (512,)
                              for concurrency in (16, 64, 256) for tier in ("native", "synchronous")})
            reads = [job for job in cells if job["case_kind"] == "live_read"]
            self.assertEqual([(j["value_size"], j["concurrency"], j["coordinator_cache_bytes"]) for j in reads],
                             [(1024, 16, 0)])
            replays = [job for job in cells if job["case_kind"] == "mainnet_replay"]
            self.assertEqual({j["durability_tier"] for j in replays}, {"native", "synchronous"})
            for job in cells:
                campaign.validate_focused_job(job)
                self.assertIs(job["owner_wal_prewrite"], prewrite)
                self.assertNotIn("owner_wal_backing", job)
                self.assertNotIn("storage_layout", job)
                self.assertNotIn("diagnostics", job)
                self.assertEqual((job["owner_count"], job["owner_topology"]), (2, "separate"))
                self.assertEqual(campaign.active_owner_count(job), 2)
                self.assertEqual(campaign.job_owner_hosts(job), 2)
        self.assertEqual(campaign.selection_owner_hosts(SELECTION), 2)
        for arms in (["file"], ["prewritten"]):
            selected = controller.controller_matrix({"mode": "focused", "selection": SELECTION, "arms": arms})
            self.assertEqual(len(selected), 9)
            self.assertEqual({job["arm"] for job in selected}, set(arms))
        with self.assertRaises(controller.SafetyError):
            controller.controller_matrix({"mode": "focused", "selection": SELECTION, "arms": ["raw"]})
        with self.assertRaises(controller.SafetyError):
            controller.controller_matrix({"mode": "focused", "selection": SELECTION, "arms": ["file", "prewritten"]})

    def test_two_owner_topology_is_exactly_admitted(self):
        job = campaign.make_focused_matrix(SELECTION)[0]
        for mutation in ({"owner_count": 1}, {"owner_count": 5},
                         {"owner_topology": "colocated"}, {"selection": "unreviewed"}):
            changed = {**copy.deepcopy(job), **mutation}
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                campaign.active_owner_count(changed)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                campaign.job_owner_hosts(changed)

    def test_c256_only_reduction_and_archive_guard(self):
        sources = campaign._nvmeof_p9_small_put()
        jobs = campaign.make_focused_matrix(SELECTION)
        for job in jobs:
            if job["case_kind"] != "remote_put":
                continue
            source = next(s for s in sources if s["repeat"] == 0 and s["value_size"] == 512
                          and s["concurrency"] == job["concurrency"])
            divisor = 2 if job["concurrency"] == 256 else 1
            self.assertEqual(job["measurement_operations"], source["measurement_operations"] // divisor)
            self.assertEqual(job["warmup_operations"], source["warmup_operations"])
        oversized = copy.deepcopy(sources)
        for job in oversized:
            if job["value_size"] == 512 and job["concurrency"] == 16:
                job["measurement_sizing"]["expected_archive_bytes"] = 3_000_000_001
        with patch.object(campaign, "_nvmeof_p9_small_put", return_value=oversized):
            with self.assertRaisesRegex(ValueError, "archive exceeds3GB"):
                campaign._nvmeof_p9_write_engine()

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
            # The terminal case summary counts the same rows; replay configs carry no `phases`.
            self.assertNotIn("phases", config)
            self.assertEqual(campaign.planned_measurement_calls(config), 4096 * 510)
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
            self.assertEqual(controller.planned_write_batches_per_partition(job), 14336)
        for name in cloud_package.REPLAY_FIXTURE_FILES:
            self.assertIn(name, cloud_package.REQUIRED)
            self.assertTrue(cloud_package.allowed(name))
            self.assertTrue((Path(__file__).resolve().parents[2] / name).is_file())

    def test_case_namespace_is_never_added_to_replay_configs(self):
        # The typed replay config denies unknown fields; a namespace would fail admission.
        for job in campaign.make_focused_matrix(SELECTION):
            config = {}
            controller.apply_case_namespace(config, job, "bench-0000000000000000")
            if job["case_kind"] == "mainnet_replay":
                self.assertNotIn("key_namespace_hex", config)
            else:
                self.assertEqual(len(config["key_namespace_hex"]), 32)

    def test_replay_owner_preparation_uses_pinned_domain(self):
        job = next(j for j in campaign.make_focused_matrix(SELECTION) if j["case_kind"] == "mainnet_replay")
        with tempfile.TemporaryDirectory() as tmp:
            instance = SimpleNamespace(config={"fleet": {"data_root": tmp, "nodes": [
                {"index": i, "address": f"10.0.0.{i + 1}", "rdma_address": f"10.1.0.{i + 1}"} for i in range(2)],
                "server_cpus": 16, "server_memory_mib": 16384}, "binary": "/bin/kv-research"}, remaining=lambda: 100)
            with patch.object(controller, "platform_module", return_value=None), \
                 patch.object(controller, "live_nvme_preparation", return_value={}), \
                 patch.object(controller, "environment", return_value={}), \
                 patch.object(controller, "command", side_effect=RuntimeError("captured typed preparation")):
                with self.assertRaisesRegex(RuntimeError, "captured typed preparation"):
                    controller.Controller.prepare_tide(instance, job, Path(tmp))
            prep = json.loads((Path(tmp) / "prepare-input.json").read_text())
            self.assertEqual(len(prep["addresses"]), 2)
            self.assertEqual(prep["replay_node_db_id"], 0)
            self.assertEqual(len(prep["database"]), 16)
            self.assertEqual(prep["batch_limits"], {"max_operations": 519, "max_encoded_bytes": 500000})
            self.assertEqual(prep["durability_tier"], job["durability_tier"])

    def test_prepared_owner_launch_validates_for_both_arms(self):
        # Whatever the prepared launch carries (the preparer's explicit false, true, or
        # a legacy launch without the key), the loaded launch states the case's choice
        # as an explicit boolean before validate_launch runs.
        config = {"spec": {"selection": SELECTION}, "fleet": {"data_root": "/tmp/engine", "nodes": [{}] * 2}}
        arms = set()
        for job in campaign.make_focused_matrix(SELECTION):
            arms.add((job["arm"], job["owner_wal_prewrite"]))
            node = Path(config["fleet"]["data_root"]) / "cells" / job["case_id"] / "node-0"
            tls = {"transport": campaign.TRANSPORTS[job["backend"]], "ca": str(node / "tls" / "ca.der"),
                   "cert": str(node / "tls" / "tidehunter-node-0.der"),
                   "key": str(node / "tls" / "tidehunter-node-0.pk8")}
            for prepared_prewrite in (True, False, None):
                prepared = {"ready_file": str(node / "ready"), "stop_file": str(node / "stop"),
                            "listen": "10.0.0.1:9400", "node": {"root": str(node / "storage")}, "tls": tls}
                if prepared_prewrite is not None:
                    prepared["node"]["wal_prewrite"] = prepared_prewrite
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "launch.yaml"
                    path.write_text(json.dumps(prepared))
                    launch = controller.load_owner_launch(job, path)
                self.assertIs(launch["node"]["wal_prewrite"], job["owner_wal_prewrite"])
                with patch.object(controller, "guarded", side_effect=lambda path, *_args, **_kwargs: Path(path)), \
                     patch.object(controller, "logical_owner_hosts", return_value=[{"address": "10.0.0.1"}]):
                    controller.validate_launch(config, launch, job["case_id"], 0)
        self.assertEqual(arms, {("file", False), ("prewritten", True)})

    def test_owner_launch_binds_and_validates_prewrite_boolean(self):
        config = {"spec": {"selection": SELECTION}, "fleet": {"data_root": "/tmp/engine", "nodes": [{}] * 2}}
        for job in campaign.make_focused_matrix(SELECTION):
            for prepared in ({"wal_prewrite": True}, {"wal_prewrite": False}, {}):
                launch = {"node": dict(prepared)}
                controller.bind_owner_wal_prewrite(job, launch)
                self.assertIs(launch["node"]["wal_prewrite"], job["owner_wal_prewrite"])
            invalid = copy.deepcopy(job)
            invalid["owner_wal_prewrite"] = 1
            with self.assertRaises(controller.SafetyError):
                controller.bind_owner_wal_prewrite(invalid, launch)
            node = Path(config["fleet"]["data_root"]) / "cells" / job["case_id"] / "node-0"
            launch.update(ready_file=str(node / "ready"), stop_file=str(node / "stop"))
            launch["node"].update(root=str(node / "storage"), wal_prewrite=not job["owner_wal_prewrite"])
            with self.assertRaisesRegex(controller.SafetyError, "wal_prewrite differs"):
                controller.validate_launch(config, launch, job["case_id"], 0)
            # An absent key would mean "on" to the owner, so the file arm could never
            # be confirmed; any non-boolean is equally ambiguous.
            for ambiguous in (None, 0, "false"):
                if ambiguous is None:
                    launch["node"].pop("wal_prewrite")
                else:
                    launch["node"]["wal_prewrite"] = ambiguous
                with self.assertRaisesRegex(controller.SafetyError, "explicit boolean wal_prewrite"):
                    controller.validate_launch(config, launch, job["case_id"], 0)

    def test_unset_case_binds_explicit_legacy_false(self):
        launch = {"node": {"wal_prewrite": True}}
        controller.bind_owner_wal_prewrite({}, launch)
        self.assertIs(launch["node"]["wal_prewrite"], False)

    def test_historical_selection_owner_launches_carry_explicit_false(self):
        # Older reviewed selections never set owner_wal_prewrite; their owners must keep
        # the legacy behaviour even though an owner reads an absent key as on.
        for selection in ("nvmeof_m3_t1", "nvmeof_m3_t2_2", "nvmeof_m3_t2_5", "nvmeof_p9_small_put"):
            jobs = campaign.make_focused_matrix(selection)
            self.assertTrue(jobs)
            for job in jobs:
                self.assertNotIn("owner_wal_prewrite", job)
                hosts = campaign.job_owner_hosts(job)
                config = {"spec": {"selection": selection},
                          "fleet": {"data_root": "/tmp/engine", "nodes": [{}] * hosts}}
                node = Path(config["fleet"]["data_root"]) / "cells" / job["case_id"] / "node-0"
                transport = campaign.TRANSPORTS[job["backend"]]
                trusted = controller.is_trusted(controller.validate_transport(transport))
                tls = {"transport": transport}
                for field, name in (("ca", "ca.der"), ("cert", "tidehunter-node-0.der"),
                                    ("key", "tidehunter-node-0.pk8")):
                    tls[field] = "" if trusted else str(node / "tls" / name)
                for prepared_prewrite in (True, False, None):
                    prepared = {"ready_file": str(node / "ready"), "stop_file": str(node / "stop"),
                                "listen": "10.0.0.1:9400", "node": {"root": str(node / "storage")},
                                "tls": tls}
                    if prepared_prewrite is not None:
                        prepared["node"]["wal_prewrite"] = prepared_prewrite
                    with tempfile.TemporaryDirectory() as tmp:
                        path = Path(tmp) / "launch.yaml"
                        path.write_text(json.dumps(prepared))
                        launch = controller.load_owner_launch(job, path)
                    with self.subTest(selection=selection, case=job["case_id"], prepared=prepared_prewrite):
                        self.assertIs(launch["node"]["wal_prewrite"], False)
                        with patch.object(controller, "guarded",
                                          side_effect=lambda path, *_args, **_kwargs: Path(path)), \
                             patch.object(controller, "logical_owner_hosts",
                                          return_value=[{"address": "10.0.0.1"}]):
                            controller.validate_launch(config, launch, job["case_id"], 0)


if __name__ == "__main__":
    unittest.main()
