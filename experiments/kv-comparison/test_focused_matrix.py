import copy
import ast
import hashlib
import json
import math
import tempfile
from unittest import mock
import unittest
from collections import Counter
from pathlib import Path

import cloud_campaign as campaign
import cloud_layout


class FocusedMatrixTests(unittest.TestCase):
    def test_reads_rest2_excludes_mixed_without_changing_original_matrices(self):
        original = campaign.make_focused_matrix("nvmeof_reads_geometry_rest")
        jobs = campaign.make_focused_matrix("nvmeof_reads_geometry_rest2")
        sources = [j for j in original if j["group"] in ("G24C", "G24W", "G24N")]
        self.assertEqual(len(jobs), 18)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G24C": 6, "G24W": 8, "G24N": 4})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "c80e079b256fad0c765dc66fec387cdb7bac160c90aa0747ac184347cb5bbed4")
        self.assertEqual([j["case_id"] for j in jobs[:3]],
                         ["G24C-r0-96a4d5f39a2d0c9116a5", "G24C-r0-1ced882cfac5c1ff87ec",
                          "G24C-r0-3f5a4655a3f093b70a38"])
        for job, source in zip(jobs, sources):
            self.assertEqual({k: v for k, v in job.items() if k not in ("case_id", "selection")},
                             {k: v for k, v in source.items() if k not in ("case_id", "selection")})
            self.assertFalse(job["mix"]["read"] and job["mix"]["put"])
            self.assertEqual(job["selection"], "nvmeof_reads_geometry_rest2")

    def test_g24_cache_allowance_matches_dataset_cache_arms(self):
        for selection in ("nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "nvmeof_reads_geometry_rest2"):
            jobs = campaign.make_focused_matrix(selection)
            self.assertEqual(sum("coordinator_read_memory_allowance_bytes" in j for j in jobs), 2)
            for job in jobs:
                if "coordinator_read_memory_allowance_bytes" in job:
                    self.assertEqual(job["group"], "G24C")
                    self.assertGreaterEqual(job["coordinator_cache_bytes"], job["keys"] * job["value_size"])
                    self.assertEqual(job["coordinator_read_memory_allowance_bytes"],
                                     512 * 1024**2 + job["coordinator_cache_bytes"])

    def test_every_focused_key_sweep_matches_runtime_rule(self):
        for selection in campaign.FOCUSED_COUNTS:
            jobs = ([j for j in campaign.make_focused_matrix("unified") if j["group"] == "G11W"]
                    if selection == "ssd_write" else campaign.make_focused_matrix(selection))
            for job in jobs:
                for name, operations in (("warmup_key_sweep", "warmup_operations"),
                                         ("measurement_key_sweep", "measurement_operations")):
                    if not job.get(name):
                        continue
                    with self.subTest(selection=selection, case=job["case_id"], sweep=name):
                        mix = job["mix"]
                        self.assertTrue(all(mix[k] == 0 for k in ("put", "delete", "missing_read", "put_batch")))
                        # Mirror make_run_config's phases/mode without backend admission.
                        self.assertFalse(job.get("qualification_only", False))
                        preload = job.get("case_kind") == "hybrid_put" or "sealed_dataset" not in job
                        self.assertTrue(preload or "sealed_dataset" in job)
                        self.assertNotEqual(job.get("case_kind"), "interference")
                        self.assertIsNone(job["offered_ops_per_second"])
                        batch = 32  # make_run_config's workload.batch_size
                        keys_per_op = ((1 if mix["read"] > 0 else batch) if name == "warmup_key_sweep"
                                       else (batch if mix["get_many"] > 0 else 1))
                        visited = job[operations] * keys_per_op
                        self.assertLessEqual(visited, 2**64 - 1)
                        if name == "warmup_key_sweep":
                            self.assertGreaterEqual(visited, job["keys"])
                        else:
                            self.assertLessEqual(visited, job["keys"])

    def test_g24_generated_configs_match_runtime_bounds(self):
        for selection in ("nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "nvmeof_reads_geometry_rest2"):
            for job in campaign.make_focused_matrix(selection):
                with self.subTest(selection=selection, case=job["case_id"]):
                    config = campaign.make_run_config(job, "/tmp/g24/" + job["case_id"], {}, "a" * 40)
                    data, work, execution, phases, limits = (config[k] for k in
                        ("dataset", "workload", "execution", "phases", "limits"))
                    self.assertEqual(config["schema_version"], 1)
                    self.assertRegex(config["case_id"], r"^[A-Za-z0-9_-]{1,160}$")
                    self.assertRegex(config["namespace"], r"^[A-Za-z0-9_-]{1,64}$")
                    self.assertTrue(config["backend"]["kind"].strip())
                    self.assertTrue(0 < data["keys"] <= limits["max_keys"])
                    self.assertIn(data["value_size"], (64, 256, 1024, 8192, 65536))
                    self.assertTrue(math.isfinite(data["entropy"]) and 0 <= data["entropy"] <= 1)
                    self.assertLessEqual(data["keys"] * data["value_size"], limits["max_bytes"])
                    self.assertEqual(set(work["mix"]), set(campaign.MIX_FIELDS))
                    self.assertTrue(all(type(v) is int and 0 <= v <= 2**32 - 1 for v in work["mix"].values()))
                    self.assertGreater(sum(work["mix"].values()), 0)
                    self.assertTrue(2 <= work["batch_size"] <= 1024)
                    if isinstance(work["distribution"], dict):
                        exponent = work["distribution"]["zipf"]["exponent"]
                        self.assertTrue(math.isfinite(exponent) and 0 < exponent <= 2)
                    self.assertTrue(1 <= execution["request_timeout_ms"] <= 300000)
                    self.assertTrue(1 <= execution["concurrency"] <= 4096)
                    self.assertTrue(execution["concurrency"] <= execution["queue_capacity"] <= 65536)
                    self.assertEqual(execution["mode"], "closed_loop")
                    self.assertIsNone(execution["offered_ops_per_second"])
                    self.assertGreater(phases["measurement_operations"], 0)
                    total = phases["warmup_operations"] + phases["aging_operations"] + phases["measurement_operations"]
                    total += data["keys"] if phases["preload"] or phases.get("verify_existing_dataset") else 0
                    self.assertLessEqual(total, min(limits["max_operations"], 2**64 - 1))
                    for key in ("max_keys", "max_bytes", "max_operations"):
                        self.assertGreater(limits[key], 0)
                    self.assertTrue(1 <= limits["deadline_seconds"] <= 86400)
                    self.assertTrue(1 <= limits["shutdown_grace_seconds"] <= 600)
                    diagnostics = config.get("diagnostics", {})
                    self.assertTrue(all(type(v) is int and 0 <= v <= 65536 for v in diagnostics.values()))
                    if diagnostics.get("read_profile_limit") or diagnostics.get("write_timeline_limit"):
                        self.assertEqual(config["backend"]["kind"], "tidehunter")
                    self.assertFalse(diagnostics.get("write_timeline_limit") and diagnostics.get("write_profile_limit"))
                    if config["backend"]["kind"] == "tidehunter":
                        count = max(work["batch_size"], 2) if work["mix"]["put_batch"] else 2
                        self.assertLessEqual(count, 32)
                        self.assertLessEqual((data["value_size"] + 32 + 32) * count + 96, 320 * 1024)
                    markers = []
                    for key in ("measurement_gate", "measurement_completion_gate"):
                        gate = config[key]
                        self.assertTrue(1 <= gate["timeout_seconds"] <= 300)
                        markers.extend((gate["ready_file"], gate["release_file"]))
                    self.assertTrue(all(Path(p).is_absolute() for p in markers))
                    self.assertEqual(len(set(markers)), 4)
                    if job["group"] == "G24M":
                        self.assertFalse(job["warmup_key_sweep"])
                        self.assertTrue(phases["preload"])
                        self.assertEqual(phases["warmup_operations"], 65536)

    def test_every_focused_mix_has_exact_runtime_fields(self):
        for selection in campaign.FOCUSED_COUNTS:
            # ssd_write is an internal component of unified, not a selectable matrix.
            jobs = ([j for j in campaign.make_focused_matrix("unified") if j["group"] == "G11W"]
                    if selection == "ssd_write" else campaign.make_focused_matrix(selection))
            for job in jobs:
                self.assertEqual(set(job["mix"]), set(campaign.MIX_FIELDS),
                                 (selection, job["case_id"]))

    def test_reads_rest_exact_subset_and_pin(self):
        jobs = campaign.make_focused_matrix("nvmeof_reads_geometry_rest")
        parent = [j for j in campaign.make_focused_matrix("nvmeof_reads_geometry")
                  if j["group"] in ("G24C", "G24M", "G24W", "G24N")]
        self.assertEqual(len(jobs), 20)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "b5db99b28d0faf8386e5b885e25c55d50b74f732e9b220ef552bd6c90a761935")
        for actual, source in zip(jobs, parent):
            self.assertEqual({k: v for k, v in actual.items() if k not in ("selection", "case_id")},
                             {k: v for k, v in source.items() if k not in ("selection", "case_id")})
            self.assertEqual(actual["selection"], "nvmeof_reads_geometry_rest")
            campaign.active_owner_count(actual)
            config = campaign.make_run_config(actual, "/tmp/read-rest/" + actual["case_id"], {}, "a" * 40)
            self.assertEqual(set(config["workload"]["mix"]), set(campaign.MIX_FIELDS))

    def test_reads_geometry_controller_mapping_defaults(self):
        tree = ast.parse(Path(campaign.__file__).with_name("cloud_controller.py").read_text())
        fields = {}
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "get" and len(node.args) > 1
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[1], (ast.Dict, ast.List))):
                fields[node.args[0].value] = dict if isinstance(node.args[1], ast.Dict) else list
        for job in campaign.make_focused_matrix("nvmeof_reads_geometry"):
            for key, kind in fields.items():
                if key in job:
                    self.assertIsInstance(job[key], kind, (job["case_id"], key))

    def test_reads_geometry_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_reads_geometry")
        self.assertEqual(len(jobs), 74)
        self.assertEqual(len({j["case_id"] for j in jobs}), 74)
        self.assertEqual(Counter(j["group"] for j in jobs),
                         campaign._FOCUSED_GROUP_COUNTS["nvmeof_reads_geometry"])
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "c27b1598f34949d642d3cc518985a2f92f5c388e77e2742da9ce780ce8b41aea")
        for job in jobs:
            self.assertEqual(job["live_max_inflight_requests"], 128)
            if "live_leased_write_window" in job:
                self.assertIn(job["group"], ("G24M", "G24W"))
                self.assertEqual(job["live_leased_write_window"], 1)
            self.assertFalse(any(v is None and k != "offered_ops_per_second" for k, v in job.items()))
            if "diagnostics" in job:
                self.assertEqual(job["group"], "G24S")
                self.assertEqual(job["diagnostics"], {"read_profile_limit": 4096})
        base = [j for j in jobs if j["group"] == "G24R"]
        self.assertEqual({(j["value_size"], j["drop_os_caches"], j["concurrency"], j["repeat"]) for j in base},
                         {(s, c, n, r) for s in (1024, 65536) for c in (False, True)
                          for n in (1, 16, 64) for r in (0, 1)})


    def test_f7_exact_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_f7_highmem32")
        self.assertEqual(Counter(j["group"] for j in jobs),
                         {"G30R": 4, "G30S": 2, "G30W": 2})
        self.assertEqual(len({j["case_id"] for j in jobs}), 8)
        self.assertEqual({j["backend"] for j in jobs}, {"tidehunter_nvme_rdma"})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "25ef27bc334edc0d07a84061427893063e162f16f8f1a986ea99d758ca2c05dd")
        self.assertEqual([j["case_id"] for j in jobs[:3]],
                         ["G30R-r0-1c7afe9b88938c65b1a9", "G30R-r1-190f295420561313babd",
                          "G30R-r0-53c44157d53c72979377"])
        for job in jobs:
            campaign.validate_focused_job(job)
            expected = {"G30R": (600000, 120000), "G30S": (1500000, 300000),
                        "G30W": (350000, 70000)}[job["group"]]
            self.assertEqual((job["measurement_operations"], job["warmup_operations"]), expected)
            for field in ("measurement_operations", "warmup_operations", "keys"):
                changed = copy.deepcopy(job)
                changed[field] += 1
                with self.subTest(group=job["group"], field=field), self.assertRaises(ValueError):
                    campaign.validate_focused_job(changed)
            if job["group"] in ("G30R", "G30S"):
                self.assertEqual((job["read_lanes"], job["live_io_queues"], job["live_io_queue_size"]),
                                 (4, 4, 128))
                self.assertTrue(job["owner_trace"])
                self.assertFalse(job["drop_os_caches"])
                self.assertEqual(job["keys"], 65536)
                self.assertTrue(job["warmup_key_sweep"])
                self.assertGreaterEqual(job["warmup_operations"], job["keys"])
            if job["group"] == "G30W":
                self.assertNotIn("live_leased_write_window", job)
                self.assertEqual(job["durability_tier"], "native")

    def test_f7_preserves_source_fields_outside_its_reviewed_windows(self):
        reads = campaign.make_focused_matrix("nvmeof_reads_geometry")
        writes = campaign.make_focused_matrix("nvmeof_parallel_writes")
        changed = {"case_id", "selection", "group", "repeat", "owner_trace",
                   "warmup_operations", "measurement_operations"}
        for job in campaign.make_focused_matrix("nvmeof_f7_highmem32"):
            if job["group"] == "G30W":
                source = campaign._clone_source(writes, "G23W", "tidehunter_nvme_rdma",
                                                65536, 64, durability_tier="native",
                                                live_leased_write_window=None)
                self.assertEqual((source["warmup_operations"], source["measurement_operations"]), (512, 4096))
            else:
                source = campaign._clone_source(reads, "G24Q", "tidehunter_nvme_rdma",
                                                job["value_size"], job["concurrency"], operation="read",
                                                drop_os_caches=False, live_io_queues=4, live_io_queue_size=128)
                self.assertEqual((source["warmup_operations"], source["measurement_operations"]), (65536, 8192))
            with self.subTest(case=job["case_id"]):
                self.assertEqual({k: v for k, v in job.items() if k not in changed},
                                 {k: v for k, v in source.items() if k not in changed})
                legacy = copy.deepcopy(source)
                legacy["measurement_operations"] = job["measurement_operations"]
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(legacy)

    def test_new_fleet_value_sizes_match_binary_validator(self):
        # Matches benchmarks/kv-research/src/config.rs, the dataset.value_size check.
        allowed = {64, 256, 1024, 4096, 8192, 16384, 65536}
        for selection in ("nvmeof_f6b", "nvmeof_f7_highmem32", "nvmeof_f8"):
            for job in campaign.make_focused_matrix(selection):
                with self.subTest(selection=selection, case=job["case_id"]):
                    self.assertIn(job["value_size"], allowed)

    def test_f8_exact_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_f8")
        self.assertEqual(Counter(j["group"] for j in jobs), {"G32W": 16, "G32H": 8, "G32S": 4, "G32N": 8})
        self.assertEqual(len({j["case_id"] for j in jobs}), 36)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "30e7121a926003ebe18e691425db063c4093a942efdf5528913003720b9d5e1a")
        self.assertEqual([j["case_id"] for j in jobs[:3]],
                         ["G32W-r0-e95f3c61d30b1f960942", "G32W-r1-a40e4628708c8e71922a",
                          "G32W-r0-cc329f4b5dc477d314f6"])
        for job in jobs:
            campaign.validate_focused_job(job)
            if job["backend"] == "native":
                self.assertTrue(job["native_wal_sampling"])
                self.assertTrue(job["thread_cpu_sampling"])
                continue
            self.assertNotIn("live_leased_flush_min_frame_bytes", job)
            self.assertEqual(job.get("live_leased_sync"), "per_frame_fua" if "live_leased_sync" in job else None)
            self.assertEqual("diagnostics" in job, job["value_size"] == 65536 and job["repeat"] == 0)
            self.assertFalse(job["live_polled_writes"])
            self.assertEqual(job["durability_tier"], "synchronous")
            self.assertTrue(job["thread_cpu_sampling"])
        self.assertEqual(sum("live_leased_sync" not in j for j in jobs if j["backend"] != "native"), 14)
        self.assertEqual({(j["native_durability"], j["value_size"], j["concurrency"])
                          for j in jobs if j["group"] == "G32N"},
                         {(tier, size, c) for tier in ("page_cache", "value_wal_barrier")
                          for size in (1024, 65536) for c in (1, 16)})

    def test_f6b_exact_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_f6b")
        self.assertEqual(Counter(j["group"] for j in jobs),
                         {"G31W": 24, "G31H": 8, "G31S": 2, "G31X": 6, "G31G": 18})
        self.assertEqual(len({j["case_id"] for j in jobs}), 58)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "f5e680e6959f3a74369bae97c21354b6dc2911f0b6e15f048c6e7172e4c823de")
        self.assertEqual([j["case_id"] for j in jobs[:3]],
                         ["G31W-r0-f2536cc0481212c5c3ca", "G31W-r1-ac94a40973aeec772318",
                          "G31W-r0-a44cc304ce5b0799dbc5"])
        writes = [j for j in jobs if j["group"] == "G31W"]
        self.assertEqual({(j["value_size"], j["live_leased_sync"],
                           j.get("live_leased_flush_min_frame_bytes"), j["repeat"]) for j in writes},
                         {(s, mode, minimum, r) for s in (1024, 4096, 16384, 65536)
                          for mode, minimum in (("per_frame_fua", None), ("group_flush", None),
                                                ("group_flush", 32768)) for r in (0, 1)})
        for job in jobs:
            campaign.validate_focused_job(job)
            self.assertEqual(job["durability_tier"], "synchronous")
            self.assertFalse(job["live_polled_writes"])
            if job["group"] in ("G31W", "G31H", "G31S"):
                self.assertEqual("diagnostics" in job, job["value_size"] == 65536 and job["repeat"] == 0)
        reads = [j for j in jobs if j["group"] == "G31G"]
        self.assertEqual({(j["value_size"], j["live_io_queues"], j["live_io_queue_size"],
                           j["concurrency"], j["repeat"]) for j in reads},
                         {(s, q, d, c, r) for s in (1024, 65536) for q, d in ((1, 32), (4, 32), (4, 128))
                          for c in (16, 64) for r in range(2 if c == 16 else 1)})
        self.assertTrue(all(j["read_lanes"] == 4 and not j["drop_os_caches"] for j in reads))

    def test_f6_exact_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_f6")
        self.assertEqual(Counter(j["group"] for j in jobs),
                         {"G29W": 20, "G29X": 6, "G29R": 8, "G29G": 4, "G24M": 4})
        self.assertEqual(len({j["case_id"] for j in jobs}), 42)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "1186674f1706550f0faaac6d7852dcee711b43dba8c87b22be4dd071d768e8a7")
        self.assertEqual([j["case_id"] for j in jobs[:3]],
                         ["G29W-r0-f144fdde700a84ff476f", "G29W-r0-144fada772a94de658da",
                          "G29W-r0-ee6fb75b39a6e5b14665"])
        self.assertEqual(campaign.NVMEOF_F6_WRITE_LANES, [])
        for job in jobs:
            campaign.validate_focused_job(job)
            self.assertEqual(set(job["mix"]), set(campaign.MIX_FIELDS))
            self.assertEqual(job["durability_tier"], "synchronous")
            self.assertFalse(job["live_polled_writes"])
            self.assertFalse("diagnostics" in job and job["diagnostics"] is None)
            fields = {k: v for k, v in job.items() if k != "case_id"}
            identity = hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
            self.assertEqual(job["case_id"], f'{job["group"]}-r{job["repeat"]}-{identity}')
            if job["group"] == "G29W":
                self.assertEqual(bool(job.get("diagnostics")), job["value_size"] == 65536 and job["repeat"] == 0)
                self.assertIn(job["repeat"], (0,) if job["concurrency"] == 1 else (0, 1))
            if job["group"] == "G29R":
                self.assertEqual((job["read_lanes"], job["live_io_queues"], job["live_io_queue_size"]), (4, 4, 128))
                self.assertTrue(job["owner_trace"])
                self.assertFalse(job["drop_os_caches"])
            if job["group"] == "G24M":
                self.assertEqual(job["mix"]["missing_read"], 0)
                self.assertFalse(job["warmup_key_sweep"])
        writes = [j for j in jobs if j["group"] == "G29W"]
        self.assertEqual({(j["concurrency"], j["value_size"], j["live_leased_sync"], j["repeat"]) for j in writes},
                         {(c, s, sync, r) for c in (1, 16, 64) for s in (1024, 65536)
                          for sync in ("per_frame_fua", "group_flush") for r in ((0,) if c == 1 else (0, 1))})
        self.assertEqual({(j["concurrency"], j["live_pin_read_lanes"], j["repeat"])
                          for j in jobs if j["group"] == "G29R"},
                         {(c, p, r) for c in (16, 64) for p in (False, True) for r in (0, 1)})
        self.assertEqual({(j["value_size"], j["live_io_queues"], j["live_io_queue_size"])
                          for j in jobs if j["group"] == "G29G"},
                         {(s, q, d) for s in (1024, 65536) for q, d in ((1, 32), (4, 128))})
    def test_single_caller_exact_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_single_caller")
        self.assertEqual(len(jobs), 20)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G28W": 8, "G28C": 8, "G28N": 4})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "294b50957b21d6acafadcf1b682c71d1b424524a144ecd6c2c18e2b1cc40fd4e")
        self.assertTrue(all(j["group"] == "G28W" for j in jobs[:3]))
        for job in jobs:
            self.assertEqual(set(job["mix"]), set(campaign.MIX_FIELDS))
            self.assertFalse("diagnostics" in job and job["diagnostics"] is None)
            campaign.active_owner_count(job)
            with tempfile.TemporaryDirectory() as directory:
                campaign.make_run_config(job, directory, {}, "a" * 40)
            if job["group"] == "G28W":
                self.assertEqual(job["concurrency"], 1)
                self.assertEqual(job["read_lanes"], 1)
                self.assertEqual(job["diagnostics"], {"write_timeline_limit": 4096})
                self.assertNotIn("live_leased_write_window", job)

    def test_parallel_writes_exact_reviewed_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_parallel_writes")
        self.assertEqual(len(jobs), 44)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G23W": 32, "G23X": 6, "G23N": 6})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "64def2342711b6f17b65796ff907fcc6b390c9d7cee9e93ed85d8623c8e32aa4")
        writes = [j for j in jobs if j["group"] == "G23W"]
        self.assertEqual(sum(bool(j.get("diagnostics")) for j in writes), 6)
        # The controller reads `diagnostics` as a mapping; an explicit null stopped fleet F1.
        self.assertFalse(any("diagnostics" in j and not isinstance(j["diagnostics"], dict) for j in jobs))
        expected_writes = {
            (tier, size, 16, window, repeat)
            for tier in ("synchronous", "native") for size in (65536, 1024)
            for window in (1, 4, None) for repeat in (0, 1)
        } | {
            (tier, 65536, concurrency, window, 0)
            for tier in ("synchronous", "native") for concurrency in (1, 64)
            for window in (1, None)
        }
        self.assertEqual({
            (j["durability_tier"], j["value_size"], j["concurrency"],
             j.get("live_leased_write_window"), j["repeat"]) for j in writes
        }, expected_writes)
        for job in writes:
            self.assertEqual(bool(job.get("diagnostics")),
                             job["value_size"] == 65536 and job["concurrency"] == 16
                             and job["repeat"] == 0)
        natives = [j for j in jobs if j["group"] == "G23N"]
        self.assertEqual({
            (j["native_durability"], j["value_size"], j["concurrency"]) for j in natives
        }, {(tier, size, concurrency) for tier in ("value_wal_barrier", "page_cache")
            for size, concurrency in ((65536, 16), (1024, 16), (65536, 64))})
        for job in jobs:
            if job.get("live_leased_write_window") is None:
                self.assertNotIn("live_leased_write_window", job)
        for job in jobs:
            campaign.validate_focused_job(job)
            if job["backend"] != "native":
                self.assertEqual((job["nvmeof_route"], job["nvmeof_target"], job["target_io"]),
                                 ("live", "kernel", "buffered"))
                self.assertEqual((job["live_io_queues"], job["live_io_queue_size"], job["live_poll_queues"]), (1, 32, 0))
                self.assertTrue(job["live_lease_ahead"])
                self.assertEqual(job["live_export_mode"], "per_owner")
        crossings = [j for j in jobs if j["group"] == "G23X"]
        self.assertEqual(Counter(j["owner_wal_file_bytes"] for j in crossings), {268435456: 4, 10737418240: 2})
        self.assertEqual({
            (j["durability_tier"], j["owner_wal_file_bytes"],
             j.get("live_leased_write_window")) for j in crossings
        }, {(tier, file_bytes, window) for tier in ("synchronous", "native")
            for file_bytes, windows in ((268435456, (1, None)), (10737418240, (None,)))
            for window in windows})
        for job in crossings:
            self.assertEqual(job["measurement_operations"], 16384)
            self.assertEqual(job["diagnostics"], {"write_timeline_limit": 16384})
            self.assertTrue(job["owner_history_diagnostics"])
            self.assertTrue(job["owner_trace"])

    def test_crossing2_preserves_reviewed_jobs_and_pairs_contracts(self):
        old = campaign.make_focused_matrix("nvmeof_contracts_crossing")
        jobs = campaign.make_focused_matrix("nvmeof_contracts_crossing2")
        self.assertEqual(jobs[:31], old)
        self.assertEqual(json.dumps(jobs[:31], sort_keys=True), json.dumps(old, sort_keys=True))
        self.assertEqual(len(jobs), 32)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G21W": 12, "G21R": 8, "G21N": 10, "G22W": 2})
        self.assertEqual(jobs[-1]["case_id"], "G22W-r0-f5566538e6cc790734f0")
        self.assertEqual(jobs[-1]["durability_tier"], "native")
        expected = dict(old[-1], selection="nvmeof_contracts_crossing2", durability_tier="native",
                        case_id=jobs[-1]["case_id"])
        self.assertEqual(jobs[-1], expected)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "a51a42c7606fbc98afa436fca1695ce621cce20603f6497b9baa2b75a4af2ea9")
        campaign.validate_focused_job(jobs[-1])
        for changed in (dict(jobs[-1], durability_tier="synchronous"), dict(jobs[-1], owner_wal_file_bytes=True)):
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(changed)

    def test_crossing_preserves_contracts_and_exact_admission(self):
        old = campaign.make_focused_matrix("nvmeof_contracts")
        jobs = campaign.make_focused_matrix("nvmeof_contracts_crossing")
        self.assertEqual(jobs[:30], old)
        self.assertEqual(len(jobs), 31)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G21W": 12, "G21R": 8, "G21N": 10, "G22W": 1})
        job = jobs[-1]
        self.assertEqual(job["owner_wal_file_bytes"], 268435456)
        self.assertEqual(job["measurement_operations"], 16384)
        self.assertEqual(job["diagnostics"], {"write_timeline_limit": 16384})
        self.assertTrue(job["owner_history_diagnostics"])
        campaign.validate_focused_job(job)
        for value in (True, 0, 268435457, 134217728, 10737418241):
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(dict(job, owner_wal_file_bytes=value))

    def test_nvmeof_contracts_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_contracts")
        groups = {"G21W": 12, "G21R": 8, "G21N": 10}
        self.assertEqual(len(jobs), 30)
        self.assertEqual(Counter(job["group"] for job in jobs), groups)
        self.assertEqual(campaign.FOCUSED_COUNTS["nvmeof_contracts"], 30)
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_contracts"), 1)
        self.assertEqual({key: count for key, count in campaign.focused_group_counts(
            "nvmeof_contracts").items() if key.startswith("G21")}, groups)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "cb960b1bcddd75fe950d5648b79644b217f3243e1fa6e45b3b427c5964ea699a")
        self.assertEqual([job["case_id"] for job in jobs[:3]], [
            "G21N-r0-fefb35e6dd1c47cfbffc", "G21W-r0-a60b30cc439cf0b55118",
            "G21R-r0-1ceaaa5179c79dd670a2"])
        self.assertEqual(jobs[0]["native_durability"], "page_cache")
        self.assertEqual(jobs[1]["durability_tier"], "synchronous")
        self.assertTrue(jobs[2]["require_media_qualified"])
        self.assertEqual(jobs[2]["value_size"], 65536)
        self.assertEqual(sum(job["backend"] == "tidehunter_nvme_rdma"
                             and job["mix"]["put"] == 100
                             and job["durability_tier"] == "synchronous" for job in jobs), 6)
        sustained = [job for job in jobs if job["measurement_operations"] == 200000]
        self.assertEqual({campaign.durability_tier(job) for job in sustained}, {"native", "synchronous"})
        for job in sustained:
            config = campaign.make_run_config(job, "/tmp/contracts-case", {}, "test")
            self.assertEqual(config["backend"]["options"]["durability_tier"], job["durability_tier"])
        for job in jobs:
            campaign.validate_focused_job(job)
            self.assertEqual(job["repeat"], 0)
            if job["mix"]["read"]:
                expected_calls = (2048 if job["value_size"] == 1024 else 4096) if (
                    job.get("media", {}).get("expected") == "cold") else 8192
                self.assertEqual(job["measurement_operations"], expected_calls)
            else:
                self.assertIn(job["measurement_operations"], (4096, 200000))
            if job["backend"] != "native":
                self.assertEqual(job["history_persistence"], "wal_replay")
            if job.get("nvmeof_route") == "live":
                self.assertEqual(job["live_write_mode"], "leased")
                self.assertTrue(job["live_lease_ahead"])
                self.assertFalse(job["live_polled_writes"])
                self.assertEqual(job["live_poll_queues"], 0)
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(dict(jobs[1], durability_tier="native"))


    def test_nvmeof_first_put_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_first_put")
        sources = campaign.make_focused_matrix("nvmeof_path_fix")
        self.assertEqual(len(jobs), 12)
        groups = {"G20W": 9, "G20R": 1, "G20N": 2}
        self.assertEqual(Counter(job["group"] for job in jobs), groups)
        self.assertEqual(campaign.FOCUSED_COUNTS["nvmeof_first_put"], 12)
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_first_put"), 1)
        self.assertEqual({key: count for key, count in campaign.focused_group_counts(
            "nvmeof_first_put").items() if key.startswith("G20")}, groups)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "bc17406e8712b9ad7f49662554bcca316cff7c87774216c39c5cc42aebe1efdc")
        shape = lambda job: (job["group"], job["value_size"], job["concurrency"], job["repeat"],
                             bool(job.get("diagnostics")), job["measurement_operations"])
        self.assertEqual([shape(job) for job in jobs], [
            ("G20N", 1024, 1, 0, False, 4096),
            ("G20W", 1024, 1, 0, False, 4096),
            ("G20R", 65536, 1, 0, False, 4096),
            ("G20W", 1024, 1, 1, False, 4096),
            ("G20W", 1024, 16, 0, False, 4096),
            ("G20W", 1024, 16, 1, False, 4096),
            ("G20W", 65536, 16, 0, False, 4096),
            ("G20W", 65536, 16, 1, False, 4096),
            ("G20W", 1024, 1, 0, True, 4096),
            ("G20W", 65536, 16, 0, True, 4096),
            ("G20W", 1024, 16, 0, False, 200000),
            ("G20N", 65536, 16, 0, False, 4096)])
        def source_fields(job):
            return {key: value for key, value in job.items()
                    if key not in ("case_id", "selection", "group")}
        for job in jobs:
            self.assertTrue(any(source_fields(job) == source_fields(source) for source in sources))
            identity = hashlib.sha256(json.dumps(
                {key: value for key, value in job.items() if key != "case_id"},
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
            self.assertEqual(job["case_id"], f"{job['group']}-r{job['repeat']}-{identity}")
            campaign.validate_focused_job(job)
            if job["backend"] != "native":
                self.assertTrue(set(campaign.NVMEOF_PATH_LIVE_SETTING_KEYS) <= job.keys())
                self.assertEqual(job["history_persistence"], "wal_replay")
                self.assertTrue(job["live_lease_ahead"])
                self.assertFalse(job["live_polled_writes"])
        self.assertTrue(jobs[2]["require_media_qualified"])
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(dict(jobs[1], live_lease_ahead=False))

    def test_nvmeof_path_fix_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_path_fix")
        self.assertEqual(len(jobs), 29)
        self.assertEqual(Counter(job["group"] for job in jobs),
                         {"G19W": 15, "G19R": 5, "G19N": 8, "G19X": 1})
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_path_fix"), 1)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "a6c9f07e1d9b60c3145e261014250fb61e655517268c6755585100078303f9ae")
        self.assertEqual([job["group"] for job in jobs[:6]],
                         ["G19N", "G19W", "G19R", "G19R", "G19W", "G19W"])
        for job in jobs:
            fields = {key: value for key, value in job.items() if key != "case_id"}
            identity = hashlib.sha256(json.dumps(fields, sort_keys=True,
                                                 separators=(",", ":")).encode()).hexdigest()[:20]
            self.assertEqual(job["case_id"], f"{job['group']}-r{job['repeat']}-{identity}")
            if job.get("nvmeof_route") == "live":
                self.assertIn("live_lease_ahead", job)
                self.assertIn("live_polled_writes", job)

    def test_nvmeof_path_core_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_path_core")
        compact = campaign.make_focused_matrix("nvmeof_path_compact")
        self.assertEqual(campaign.NVMEOF_PATH_SELECTIONS,
                         ("nvmeof_path_compact", "nvmeof_path_core", "nvmeof_path_fix", "nvmeof_first_put", "nvmeof_contracts", "nvmeof_contracts_crossing", "nvmeof_contracts_crossing2", "nvmeof_parallel_writes", "nvmeof_single_caller", "nvmeof_f6", "nvmeof_f6b", "nvmeof_f8", "nvmeof_f7_highmem32", "nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "nvmeof_reads_geometry_rest2", *campaign.M3_SELECTIONS))
        self.assertEqual(len(jobs), 53)
        groups = {"G18N": 17, "G18W": 11, "G18R": 12, "G18M": 7, "G18B": 4, "G18X": 2}
        self.assertEqual(Counter(job["group"] for job in jobs), groups)
        self.assertEqual(campaign.FOCUSED_COUNTS["nvmeof_path_core"], 53)
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_path_core"), 1)
        self.assertEqual({key: value for key, value in campaign.focused_group_counts(
            "nvmeof_path_core").items() if key.startswith("G18")}, groups)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "f59942def5dc313b09c41dffa0df4d604f57e110f359dd12b2af75f5deea121c")
        def source_shape(job):
            return {key: value for key, value in job.items()
                    if key not in ("selection", "case_id")}
        shapes = [source_shape(job) for job in compact]
        self.assertEqual(source_shape(jobs[0]), shapes[0])
        self.assertTrue(jobs[1]["require_media_qualified"])
        self.assertEqual((jobs[2]["group"], jobs[2]["backend"], jobs[2]["mix"]["get_many"],
                          jobs[2]["media"]["expected"]), ("G18B", "native", 100, "cold"))
        # Moving the three priority jobs must preserve the order of every other job.
        positions = [shapes.index(source_shape(job)) for job in jobs[3:]]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(len({job["case_id"] for job in jobs}), 53)
        self.assertEqual(sum(job.get("require_media_qualified", False) for job in jobs), 1)
        self.assertTrue(jobs[1]["require_media_qualified"])
        for job in jobs:
            self.assertEqual(job["selection"], "nvmeof_path_core")
            self.assertIn(source_shape(job), shapes)
            self.assertNotIn(job["case_id"], {source["case_id"] for source in compact})
            identity = hashlib.sha256(json.dumps(
                {key: value for key, value in job.items() if key != "case_id"},
                sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:20]
            self.assertEqual(job["case_id"], f"{job['group']}-r{job['repeat']}-{identity}")
            self.assertTrue(set(campaign.NVMEOF_PATH_SETTING_KEYS[job["backend"]]) <= job.keys())
            self.assertEqual(campaign.active_owner_count(job), job.get("owner_count", 5))
            campaign.validate_focused_job(job)
        changed = dict(jobs[5], live_poll_queues=True)
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(changed)
        removed = next(source for source in compact if source_shape(source) not in
                       [source_shape(job) for job in jobs])
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(dict(removed, selection="nvmeof_path_core"))

    def test_nvmeof_path_core_retains_native_read_references(self):
        jobs = campaign.make_focused_matrix("nvmeof_path_core")
        reads = [job for job in jobs if job["group"] != "G18F"
                 and (job["mix"]["read"] or job["mix"]["get_many"])]
        def cell(job):
            return (job["mix"]["get_many"], job["value_size"], job["concurrency"],
                    job["repeat"], job["media"]["expected"], job["batch_size"], job["keys"])
        references = {cell(job) for job in reads if job["backend"] == "native"}
        cached = []
        for job in reads:
            if job["backend"] != "tidehunter_nvme_rdma":
                continue
            if job["coordinator_cache_bytes"]:
                cached.append(job)
            else:
                self.assertIn(cell(job), references, job["case_id"])
        self.assertEqual(len(cached), 1)

    def test_nvmeof_path_exact_matrix(self):
        jobs = campaign.make_focused_matrix("nvmeof_path_compact")
        self.assertEqual(len(jobs), 123)
        self.assertEqual(Counter(j["group"] for j in jobs),
                         {"G18N": 32, "G18W": 19, "G18R": 24, "G18M": 18, "G18B": 22, "G18X": 8})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "d830e7c32fd715a794328445a26c2d9e62a76188d5dbd12a9471712a90212772")
        self.assertEqual([j["group"] for j in jobs[:10]],
                         ["G18N", "G18W", "G18N", "G18M", "G18R", "G18R", "G18R", "G18R", "G18B", "G18X"])
        self.assertTrue(all(j["repeat"] == 0 for j in jobs[:10]))
        self.assertEqual(jobs[5]["value_size"], 65536)
        self.assertTrue(jobs[5]["require_media_qualified"])
        self.assertEqual(sum(j.get("require_media_qualified", False) for j in jobs), 1)
        self.assertEqual(jobs[6]["live_poll_queues"], 1)
        self.assertEqual((jobs[7]["concurrency"], jobs[7]["read_lanes"],
                          jobs[7]["live_io_queues"], jobs[7]["live_io_queue_size"]), (16, 4, 4, 128))
        order = ("G18N", "G18W", "G18M", "G18R", "G18B", "G18X")
        self.assertEqual(jobs[10:], sorted(jobs[10:], key=lambda j: order.index(j["group"])))
        for job in jobs:
            campaign.validate_focused_job(job)
            backend = job["backend"]
            keys = campaign.NVMEOF_PATH_SETTING_KEYS[backend]
            self.assertTrue(set(keys) <= job.keys())
            if backend != "native":
                self.assertEqual(job["history_persistence"], "wal_replay")
                self.assertEqual(job["owner_count"], 1)
            if backend == "tidehunter_nvme_rdma":
                self.assertEqual((job["live_write_mode"], job["live_export_mode"]), ("leased", "per_owner"))
                self.assertNotIn("sealed_dataset", job)
            if job["mix"]["put"] or job["mix"]["put_batch"]:
                self.assertTrue(job["device_counters"])
            if job["group"] == "G18B":
                self.assertEqual(job["batch_size"], 32)
            if job["group"] == "G18X":
                self.assertEqual(job["owner_wal_geometry"], "sui_harness")
                self.assertTrue(job["drop_os_caches"])
                self.assertTrue(job["evict_mapped_wal_before_measurement"])
            if job.get("distribution") == {"zipf": {"exponent": 0.99}}:
                self.assertEqual((job["keys"], job["coordinator_cache_bytes"], job["measurement_operations"]),
                                 (65536, 8388608, 8192))
            if backend == "native" and job["mix"]["read"]:
                self.assertEqual(job["keys"], 65536)
        for key, value in (("history_persistence", "on_disk"), ("live_export_mode", "on_demand"),
                           ("live_io_queues", 4), ("live_inline_reads", False)):
            changed = dict(jobs[5], **{key: value})
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(changed)
        changed = dict(jobs[5], live_io_queues=True)
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(changed)

    def test_nvmeof_path_read_populations_match_native_references(self):
        jobs = campaign.make_focused_matrix("nvmeof_path_compact")
        # Floors inherit a read mix but execute no benchmark read.
        reads = [j for j in jobs if j["group"] != "G18F"
                 and (j["mix"]["read"] or j["mix"]["get_many"])]
        self.assertTrue(all(j["keys"] == 65536 for j in reads))
        def cell(job):
            operation = "get_many" if job["mix"]["get_many"] else "read"
            return (operation, job["value_size"], job["concurrency"], job["repeat"],
                    job["media"]["expected"], job["batch_size"], job["keys"])
        references = {cell(j) for j in reads if j["backend"] == "native"}
        cache_cases = []
        for job in reads:
            if job["backend"] != "tidehunter_nvme_rdma":
                continue
            if job["coordinator_cache_bytes"]:
                # G18M(b), the Zipf coordinator-cache arm, has no native reference.
                self.assertEqual(job["group"], "G18M")
                self.assertEqual(job["distribution"], {"zipf": {"exponent": 0.99}})
                cache_cases.append(job)
                continue
            # Lane, polled, queue-geometry and G18X arms share the native cell.
            self.assertIn(cell(job), references, job["case_id"])
            if job["media"]["expected"] == "memory":
                self.assertEqual(job["warmup_operations"], 65536)
                self.assertTrue(job["warmup_key_sweep"])
                self.assertEqual(job["measurement_operations"], 8192)
                config = campaign.make_run_config(job, Path("/tmp/population-case"), {},
                                                  "revision", deadline_seconds=600)
                self.assertEqual(config["limits"]["deadline_seconds"], 600)
                self.assertLessEqual(job["keys"], config["limits"]["max_keys"])
                self.assertLessEqual(job["keys"] * job["value_size"], config["limits"]["max_bytes"])
                self.assertLessEqual(job["keys"] + job["warmup_operations"] + job["measurement_operations"],
                                     config["limits"]["max_operations"])
        self.assertEqual(len(cache_cases), 2)

    def test_live_route_consumers_accept_unsealed_population(self):
        import analyze
        import plot_cloud_comparison as plots
        jobs = campaign.make_focused_matrix('nvmeof_path_compact')
        for group in ('G18R', 'G18W'):
            job = next(j for j in jobs if j['group'] == group and j.get('nvmeof_route') == 'live')
            self.assertEqual(analyze.requested_nvmeof_route({'job': job}), 'live')
            def document(item):
                return {'schema_version': 1, 'planned_cases': 1, 'completed_cases': 0,
                        'cases': [{'case_id': item['case_id'], 'job': item, 'status': 'not_run', 'metrics': None}]}
            plots.Dataset(document(job))
            bad = dict(job, nvmeof_route='sealed_read')
            with self.assertRaises(ValueError):
                analyze.requested_nvmeof_route({'job': bad})
            with self.assertRaises(ValueError):
                plots.Dataset(document(bad))

    def test_owner_receipt_count_and_partition_binding(self):
        job = next(j for j in campaign.make_focused_matrix('nvmeof_path_core')
                   if j['backend'] == 'tidehunter_nvme_rdma')
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            receipt = {'job': job, 'status': 'completed'}
            with mock.patch.object(campaign.analyze, 'admit_focused_artifacts', return_value={}):
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'lacks owner durability'):
                    campaign.summarize_case(root)
                receipt['owner_durability_receipts'] = []
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'invalid owner durability'):
                    campaign.summarize_case(root)
                receipt['owner_durability_receipts'] = [{}, {}]
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'invalid owner durability'):
                    campaign.summarize_case(root)
                path = root / 'owner-durability/node-0.durability.json'
                path.parent.mkdir()
                data = json.dumps({'partition': 1}).encode()
                path.write_bytes(data)
                receipt['owner_durability_receipts'] = [{'path': str(path.relative_to(root)),
                    'sha256': hashlib.sha256(data).hexdigest()}]
                (root / 'receipt.json').write_text(json.dumps(receipt))
                with self.assertRaisesRegex(ValueError, 'partition differs'):
                    campaign.summarize_case(root)

    def test_all_jobs_fit_external_storage_batch_limit(self):
        # prepare_external_case_file rejects more than 1,000,000 batches per partition.
        backend_limit = 1_000_000
        for job in campaign.make_focused_matrix('nvmeof_path_compact'):
            bound = max(16384, job['keys'] + job['warmup_operations'] + job['measurement_operations'])
            self.assertLessEqual(bound, backend_limit, job['case_id'])

    def test_strict_admission(self):
        job = campaign.make_focused_matrix('nvmeof_path_core')[0]
        for change in ({'unexpected': 1}, {'repeat': True}, {'case_id': 'wrong'},
                       {'target_io': 'direct'}, {'case_kind': 'unknown'},
                       {'value_size': 1025}, {'execution': {'queue_capacity': 256, 'extra': 1}}):
            candidate = copy.deepcopy(job)
            candidate.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                campaign.validate_focused_job(candidate)
        for selection in ('unknown', None, 1):
            with self.assertRaises(ValueError):
                campaign.make_focused_matrix(selection)
