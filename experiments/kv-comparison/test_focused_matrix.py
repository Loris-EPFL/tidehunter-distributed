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
                         {"G30R": 4, "G30S": 2, "G30W": 2, "G30V": 2})
        self.assertEqual(len({j["case_id"] for j in jobs}), 10)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "3319cef697350c791476126a297d19f768ffb0690b6dd2fb4cef79978d9f6800")
        self.assertEqual([j["case_id"] for j in jobs[:3]],
                         ["G30R-r0-8ddc735431d9a8936fc6", "G30R-r1-da5e2c1b69d61312518a",
                          "G30R-r0-899d6aa918c7cb1fe633"])
        for job in jobs:
            campaign.validate_focused_job(job)
            if job["group"] in ("G30R", "G30S"):
                self.assertEqual((job["read_lanes"], job["live_io_queues"], job["live_io_queue_size"]),
                                 (4, 4, 128))
                self.assertTrue(job["owner_trace"])
                self.assertFalse(job["drop_os_caches"])
            if job["group"] == "G30W":
                self.assertNotIn("live_leased_write_window", job)
                self.assertEqual(job["durability_tier"], "native")

    def test_new_fleet_value_sizes_match_binary_validator(self):
        # Matches benchmarks/kv-research/src/config.rs, the dataset.value_size check.
        allowed = {64, 256, 1024, 4096, 8192, 16384, 65536}
        for selection in ("nvmeof_f6b", "nvmeof_f7_highmem32"):
            for job in campaign.make_focused_matrix(selection):
                with self.subTest(selection=selection, case=job["case_id"]):
                    self.assertIn(job["value_size"], allowed)

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
        self.assertEqual(jobs[:35], old)
        self.assertEqual(json.dumps(jobs[:35], sort_keys=True), json.dumps(old, sort_keys=True))
        self.assertEqual(len(jobs), 36)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G21W": 16, "G21R": 8, "G21N": 10, "G22W": 2})
        self.assertEqual(jobs[-1]["case_id"], "G22W-r0-f5566538e6cc790734f0")
        self.assertEqual(jobs[-1]["durability_tier"], "native")
        expected = dict(old[-1], selection="nvmeof_contracts_crossing2", durability_tier="native",
                        case_id=jobs[-1]["case_id"])
        self.assertEqual(jobs[-1], expected)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
                         "8dde34953df300c798b0a5605abf393d310bd6e321c3568c861cfdb27caab295")
        campaign.validate_focused_job(jobs[-1])
        for changed in (dict(jobs[-1], durability_tier="synchronous"), dict(jobs[-1], owner_wal_file_bytes=True)):
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(changed)

    def test_crossing_preserves_contracts_and_exact_admission(self):
        old = campaign.make_focused_matrix("nvmeof_contracts")
        jobs = campaign.make_focused_matrix("nvmeof_contracts_crossing")
        self.assertEqual(jobs[:34], old)
        self.assertEqual(len(jobs), 35)
        self.assertEqual(Counter(j["group"] for j in jobs), {"G21W": 16, "G21R": 8, "G21N": 10, "G22W": 1})
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
        groups = {"G21W": 16, "G21R": 8, "G21N": 10}
        self.assertEqual(len(jobs), 34)
        self.assertEqual(Counter(job["group"] for job in jobs), groups)
        self.assertEqual(campaign.FOCUSED_COUNTS["nvmeof_contracts"], 34)
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_contracts"), 1)
        self.assertEqual({key: count for key, count in campaign.focused_group_counts(
            "nvmeof_contracts").items() if key.startswith("G21")}, groups)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "cad4e58cc1e01a4977f7b7e3e0c87ed6ff00cfb15d498959472a511a8bab0208")
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

    def test_contracts_keep_all_earlier_matrix_hashes(self):
        expected = {
            "recommended": "e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d",
            "full": "04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c",
            "device": "8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43",
            "floors": "5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429",
            "stages": "646207fe6727aa37367b727759a4d46005ebd56a59f29ec49b13a690de24a724",
            "unified": "a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506",
            "append_floors": "646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40",
            "fair_native": "7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9",
            "parity": "ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6",
            "parity_compact": "4978687ab42ca839e2c864978fb1489854ae201d28f06facf89a283cdea8e0aa",
            "fair_five_owner": "479601db27b3a709e83944dcdfa9b77e7f8e4bacac9637a29e04327e09151946",
            "remote_rw_compact": "b2f2afc44afc1d066bedb27f819c5fa7789e000f96da0c7beb4c6f9359122620",
            "fabric_ab_compact": "8e12ba4352dc461680565b3bfaef5db705e702e03bce986323cc1e31ae9bde6e",
            "leased_ab_compact": "52029dc2047cecca4903b4240d7a84d8dcb402bfbfddcad827d605405f15c2dc",
            "nvmeof_path_compact": "8da5d9479b5fc470ff90c21211b3c3510a66d15bd3f03e520c9eefe38b7b2036",
            "nvmeof_path_core": "f286ce6c7255d21f98c9cef8b334b3d6540b1cc84005bc265e0f39751225ff55",
            "nvmeof_path_fix": "aa94a3842d196bd9cdb486b0c746e9939455b25d4d152e8127c1aba51f9ef606",
            "nvmeof_first_put": "bc17406e8712b9ad7f49662554bcca316cff7c87774216c39c5cc42aebe1efdc"}
        for selection, digest in expected.items():
            with self.subTest(selection=selection):
                self.assertEqual(hashlib.sha256(json.dumps(campaign.make_focused_matrix(selection),
                    sort_keys=True, separators=(",", ":")).encode()).hexdigest(), digest)

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
        self.assertEqual(len(jobs), 37)
        self.assertEqual(Counter(job["group"] for job in jobs),
                         {"G19W": 21, "G19R": 7, "G19N": 8, "G19X": 1})
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_path_fix"), 1)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "aa94a3842d196bd9cdb486b0c746e9939455b25d4d152e8127c1aba51f9ef606")
        self.assertEqual([job["group"] for job in jobs[:8]],
                         ["G19N", "G19W", "G19W", "G19R", "G19R", "G19R", "G19W", "G19W"])
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
                         ("nvmeof_path_compact", "nvmeof_path_core", "nvmeof_path_fix", "nvmeof_first_put", "nvmeof_contracts", "nvmeof_contracts_crossing", "nvmeof_contracts_crossing2", "nvmeof_parallel_writes", "nvmeof_single_caller", "nvmeof_f6", "nvmeof_f6b", "nvmeof_f7_highmem32", "nvmeof_reads_geometry", "nvmeof_reads_geometry_rest", "nvmeof_reads_geometry_rest2"))
        self.assertEqual(len(jobs), 66)
        groups = {"G18F": 4, "G18N": 17, "G18W": 19, "G18R": 12,
                  "G18M": 7, "G18B": 5, "G18X": 2}
        self.assertEqual(Counter(job["group"] for job in jobs), groups)
        self.assertEqual(campaign.FOCUSED_COUNTS["nvmeof_path_core"], 66)
        self.assertEqual(campaign.selection_owner_hosts("nvmeof_path_core"), 1)
        self.assertEqual({key: value for key, value in campaign.focused_group_counts(
            "nvmeof_path_core").items() if key.startswith("G18")}, groups)
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "f286ce6c7255d21f98c9cef8b334b3d6540b1cc84005bc265e0f39751225ff55")
        def source_shape(job):
            return {key: value for key, value in job.items()
                    if key not in ("selection", "case_id")}
        shapes = [source_shape(job) for job in compact]
        self.assertEqual(source_shape(jobs[0]), shapes[0])
        self.assertTrue(jobs[1]["require_media_qualified"])
        self.assertEqual((jobs[2]["group"], jobs[2]["backend"], jobs[2]["mix"]["get_many"],
                          jobs[2]["media"]["expected"]), ("G18B", "native", 100, "cold"))
        # Two compact canaries lead; the other ten follow the inserted batch canary.
        self.assertEqual([source_shape(job) for job in jobs[3:13]],
                         [shape for index, shape in enumerate(shapes[:12]) if index not in (0, 6)])
        # Moving the three priority jobs must preserve the order of every other job.
        positions = [shapes.index(source_shape(job)) for job in jobs[3:]]
        self.assertEqual(positions, sorted(positions))
        self.assertEqual(len({job["case_id"] for job in jobs}), 66)
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
        self.assertEqual(len(jobs), 157)
        self.assertEqual(Counter(j["group"] for j in jobs),
                         {"G18F": 10, "G18N": 32, "G18W": 35, "G18R": 24,
                          "G18M": 18, "G18B": 30, "G18X": 8})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True,
                         separators=(",", ":")).encode()).hexdigest(),
                         "8da5d9479b5fc470ff90c21211b3c3510a66d15bd3f03e520c9eefe38b7b2036")
        self.assertEqual([j["group"] for j in jobs[:12]],
                         ["G18N", "G18W", "G18W", "G18N", "G18M", "G18R",
                          "G18R", "G18R", "G18R", "G18B", "G18X", "G18F"])
        self.assertTrue(all(j["repeat"] == 0 for j in jobs[:12]))
        self.assertEqual([next(k for k, v in j["mix"].items() if v == 100) for j in jobs[:12]],
                         ["put", "put", "put", "read", "read", "read", "read",
                          "read", "read", "put_batch", "read", "read"])
        self.assertEqual(jobs[6]["value_size"], 65536)
        self.assertTrue(jobs[6]["require_media_qualified"])
        self.assertEqual(sum(j.get("require_media_qualified", False) for j in jobs), 1)
        self.assertEqual(jobs[7]["live_poll_queues"], 1)
        self.assertEqual((jobs[8]["concurrency"], jobs[8]["read_lanes"],
                          jobs[8]["live_io_queues"], jobs[8]["live_io_queue_size"]), (16, 4, 4, 128))
        self.assertEqual((jobs[11]["net"]["tool"], jobs[11]["net"]["mode"]), ("ib_read_lat", "polled"))
        order = ("G18F", "G18N", "G18W", "G18M", "G18R", "G18B", "G18X")
        self.assertEqual(jobs[12:], sorted(jobs[12:], key=lambda j: order.index(j["group"])))
        for job in jobs:
            campaign.validate_focused_job(job)
            backend = job["backend"]
            keys = campaign.NVMEOF_PATH_SETTING_KEYS[backend]
            self.assertTrue(set(keys) <= job.keys())
            if backend != "native":
                self.assertEqual(job["history_persistence"], "memory_only")
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

    def test_leased_compact_and_historical_hashes(self):
        pins = {
            'recommended': 'e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d',
            'full': '04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c',
            'device': '8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43',
            'floors': '5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429',
            'stages': '646207fe6727aa37367b727759a4d46005ebd56a59f29ec49b13a690de24a724',
            'unified': 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506',
            'append_floors': '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40',
            'fair_native': '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9',
            'parity': 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6',
            'parity_compact': '4978687ab42ca839e2c864978fb1489854ae201d28f06facf89a283cdea8e0aa',
            'fair_five_owner': '479601db27b3a709e83944dcdfa9b77e7f8e4bacac9637a29e04327e09151946',
            'remote_rw_compact': 'b2f2afc44afc1d066bedb27f819c5fa7789e000f96da0c7beb4c6f9359122620',
            'fabric_ab_compact': '8e12ba4352dc461680565b3bfaef5db705e702e03bce986323cc1e31ae9bde6e',
            'leased_ab_compact': '52029dc2047cecca4903b4240d7a84d8dcb402bfbfddcad827d605405f15c2dc',
            'nvmeof_path_compact': '8da5d9479b5fc470ff90c21211b3c3510a66d15bd3f03e520c9eefe38b7b2036',
        }
        for selection, pin in pins.items():
            data = json.dumps(campaign.make_focused_matrix(selection), sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(data).hexdigest(), pin, selection)
        jobs = campaign.make_focused_matrix('leased_ab_compact')
        self.assertEqual(len(jobs), 72)
        self.assertEqual(Counter(j['group'] for j in jobs), {'G17W': 50, 'G17R': 22})
        canaries = [j for j in jobs if j['repeat'] == 0 and j['value_size'] == 1024
                    and j['concurrency'] == 1 and not j.get('diagnostics')]
        self.assertEqual(jobs[:len(canaries)], canaries)
        for job in jobs:
            campaign.validate_focused_job(job)
            self.assertNotIn('sealed_dataset', job)
            if job['group'] == 'G17R':
                self.assertEqual(job['cohort'], 'primary_rf1')
                config = campaign.make_run_config(job, Path('/tmp/live-case'), {}, 'revision')
                self.assertTrue(config['phases']['preload'])
                self.assertTrue(config['phases']['warmup_key_sweep'])
                self.assertEqual(config['phases']['warmup_operations'], 4096)
                self.assertEqual(job['read_lanes'], 4 if job['concurrency'] == 16 else 1)
        source = next(j for j in jobs if j.get('live_write_mode') == 'leased')
        for field, value in [('durability_tier', 'bad'), ('live_write_mode', 'reserved'),
                             ('nvmeof_route', 'sealed_read'), ('value_size', 8192), ('owner_count', 5)]:
            changed = copy.deepcopy(source)
            changed[field] = value
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(changed)

    def test_live_route_consumers_accept_unsealed_population(self):
        import analyze
        import plot_cloud_comparison as plots
        jobs = campaign.make_focused_matrix('leased_ab_compact')
        for group in ('G17R', 'G17W'):
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

    def test_geometry_prefix_and_ids(self):
        full = campaign.make_focused_matrix('full')
        recommended = campaign.make_focused_matrix('recommended')
        self.assertEqual(len(full), 333)
        self.assertEqual(len(recommended), 279)
        self.assertEqual(recommended, full[:279])
        self.assertEqual(full, campaign.make_focused_matrix('full'))
        for selection, jobs in (('full', full), ('recommended', recommended)):
            counts = campaign.focused_group_counts(selection)
            for field in ('group', 'case_kind'):
                for name, count in Counter(j[field] for j in jobs).items():
                    self.assertEqual(counts[name], count)
        self.assertTrue(all(j['selection'] == 'recommended' for j in recommended))
        self.assertTrue(all(j['selection'] == 'full' for j in full[279:]))
        self.assertEqual(len({j['case_id'] for j in full}), len(full))
        old = [j for j in recommended if j['target_io'] != 'direct']
        digest = hashlib.sha256(json.dumps(old, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        self.assertEqual(digest, 'd95f90a4daffb2557e445bebe67436c0c782a131c31c49953e7a5decd542131e')
        self.assertEqual({j['backend'] for j in full}, {'native', 'tidehunter_tcp', 'tidehunter_verbs_native', 'tidehunter_nvme_rdma'})
        for job in full:
            self.assertIs(campaign.validate_focused_job(job), job)
    def test_device_matrix_and_configuration(self):
        jobs = campaign.make_focused_matrix('device')
        self.assertEqual(jobs, campaign._focused_jobs()[333:453])
        self.assertEqual(len(jobs), 120)
        digests = {
            'recommended': 'e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d',
            'full': '04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c',
            'device': '8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43',
        }
        for selection, expected in digests.items():
            data = json.dumps(campaign.make_focused_matrix(selection), sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected)
        self.assertEqual([job['case_kind'] for job in jobs[:7]],
                         ['media_read'] * 4 + ['remote_put'] * 3)
        for job in jobs:
            self.assertIs(campaign.validate_focused_job(job), job)
            config = campaign.make_run_config(job, Path('/tmp/device-case'), {}, 'revision')
            self.assertEqual(config['dataset']['keys'], job['keys'])
            self.assertFalse(config['phases'].get('measurement_key_sweep', False))
            expected_eviction = job['case_kind'] == 'media_read' and job['backend'] != 'tidehunter_nvme_rdma'
            self.assertEqual(config['phases'].get('evict_mapped_wal_before_measurement', False), expected_eviction)
            self.assertEqual(config['backend']['options']['durability_tier'], job['durability_tier'])
            if job['case_kind'] == 'remote_put':
                self.assertTrue(config['phases']['preload'])
                self.assertEqual(config['execution']['mode'], 'closed_loop')
        altered = copy.deepcopy(jobs[0])
        altered['keys'] += 1
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(altered)

    def test_floors_matrix_is_stable_and_valid(self):
        jobs = campaign.make_focused_matrix('floors')
        self.assertEqual(jobs, campaign._focused_jobs()[453:522])
        self.assertEqual(len(jobs), 69)
        self.assertEqual(Counter(j['group'] for j in jobs), {'G11N': 45, 'G11S': 24})
        self.assertEqual(Counter(j['case_kind'] for j in jobs), {'net_floor': 45, 'ssd_floor': 24})
        self.assertEqual(campaign.focused_group_counts('floors'),
                         {'G11N': 45, 'G11S': 24, 'net_floor': 45, 'ssd_floor': 24})
        self.assertEqual([j['repeat'] for j in jobs], [0] * 23 + [1] * 23 + [2] * 23)
        self.assertEqual([(j['net']['tool'], j['net']['mode'], j['net']['interface'])
                          for j in jobs[:3]],
                         [('ib_send_lat', 'polled', 'rdma0'), ('fi_pingpong', None, 'rdma0'),
                          ('tcp_rtt', 'block', 'eth0')])
        self.assertEqual(jobs[1]['net']['provider'], 'verbs;ofi_rxm')
        self.assertEqual([(j['ssd']['state'], j['ssd']['block_bytes'], j['ssd']['region_bytes'])
                          for j in jobs[3:5]], [('fresh', 4096, 67108864), ('aged', 4096, 67108864)])
        data = json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()
        self.assertEqual(hashlib.sha256(data).hexdigest(),
                         '5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429')
        for job in jobs:
            self.assertEqual(job['selection'], 'floors')
            if job.get('net', {}).get('tool') == 'fi_pingpong':
                self.assertEqual(job['net']['warmup'], 0)
                altered = copy.deepcopy(job)
                altered['net']['warmup'] = 500
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            if job['case_kind'] == 'ssd_floor':
                self.assertEqual(job['ssd']['runtime_seconds'], 6)
                altered = copy.deepcopy(job)
                altered['ssd']['runtime_seconds'] = 10
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            self.assertIs(campaign.validate_focused_job(job), job)
            key = 'net' if job['case_kind'] == 'net_floor' else 'ssd'
            altered = copy.deepcopy(job)
            altered[key]['unexpected'] = True
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        for job, key, field, value in ((jobs[0], 'net', 'iterations', True),
                                       (jobs[1], 'net', 'endpoint', 'msg'),
                                       (jobs[2], 'net', 'sizes', [8]),
                                       (jobs[3], 'ssd', 'age_seconds', 60)):
            altered = copy.deepcopy(job)
            altered[key][field] = value
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        altered = copy.deepcopy(campaign.make_focused_matrix('full')[0])
        altered['net'] = copy.deepcopy(jobs[0]['net'])
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(altered)


    def test_append_floors_and_frozen_unified_ids(self):
        jobs = campaign.make_focused_matrix('append_floors')
        tuples = [(j['ssd_write']['block_bytes'], j['ssd_write']['mode'],
                   j['repeat'], j['ssd_write']['host']) for j in jobs]
        expected = [(block, mode, repeat, host)
                    for block in (1024, 8192, 65536)
                    for mode in ('fdatasync', 'append_pwrite', 'append_mmap', 'append_mmap_fsync')
                    for repeat in (0, 1)
                    for host in (('client', 'owner0') if repeat == 0 else ('owner0', 'client'))]
        canary = (1024, 'append_mmap', 0, 'client')
        expected.remove(canary)
        self.assertEqual(tuples, [canary] + expected)
        self.assertEqual(len({j['case_id'] for j in jobs}), 48)
        self.assertEqual(campaign.focused_group_counts('append_floors'),
                         {'G13W': 48, 'ssd_write_floor': 48})
        for job in jobs:
            self.assertIs(campaign.validate_focused_job(job), job)
            spec = job['ssd_write']
            if spec['mode'] == 'fdatasync':
                self.assertEqual(spec['runtime_seconds'], 6)
                self.assertTrue(spec['preheat'])
            else:
                self.assertEqual((spec['warmup_operations'], spec['operations']), (512, 4096))
                self.assertNotIn('runtime_seconds', spec)
            invalid = copy.deepcopy(job)
            invalid['ssd_write']['host'] = 'other'
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(invalid)
        raw = campaign._focused_jobs()
        self.assertEqual(jobs, raw[723:771])
        self.assertEqual(hashlib.sha256(json.dumps(
            [j['case_id'] for j in jobs], separators=(',', ':')).encode()).hexdigest(),
            '7a8fb1f66a56485abe4c955362be76ce887df8a54a3518b218bc13fa95a04e5c')
        self.assertEqual(hashlib.sha256(json.dumps(
            jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
            '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40')
        ids = [j['case_id'] for j in campaign.make_focused_matrix('unified')]
        self.assertEqual(hashlib.sha256(json.dumps(ids, separators=(',', ':')).encode()).hexdigest(),
                         'e3d250e3da6874851db87ef137e59fa38a52a48eae12d7ebcdbbce37f788fe18')

    def test_fair_native_matrix(self):
        raw = campaign._focused_jobs()
        fair = campaign.make_focused_matrix('fair_native')
        append = campaign.make_focused_matrix('append_floors')
        parity = campaign.make_focused_matrix('parity')
        self.assertEqual(fair, raw[771:771 + len(fair)])
        self.assertEqual(parity, fair[:1] + append + fair[1:])
        self.assertEqual(len(fair), 168)
        self.assertEqual(len(parity), 216)
        self.assertEqual(len({j['case_id'] for j in raw}), len(raw))
        groups = {'G14N': 54, 'G14S': 36, 'G14O1': 36, 'G14O5': 36, 'G14P': 6}
        self.assertEqual(Counter(j['group'] for j in fair), groups)
        self.assertEqual(campaign.focused_group_counts('fair_native'),
                         {**groups, 'native_op': 92, 'remote_put': 76})
        self.assertEqual(campaign.focused_group_counts('parity'),
                         {'G13W': 48, **groups, 'ssd_write_floor': 48,
                          'native_op': 92, 'remote_put': 76})
        self.assertEqual((fair[0]['native_durability'], fair[0]['concurrency'], fair[0]['value_size']),
                         ('value_wal_group_barrier', 1, 1024))
        for selection, expected in (
                ('fair_native', '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9'),
                ('parity', 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6')):
            data = json.dumps(campaign.make_focused_matrix(selection),
                              sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected)
        for group, shards, concurrencies in (('G14N', 1, (1, 5, 16)), ('G14S', 5, (5, 16))):
            self.assertEqual({(j['native_shards'], j['native_durability'], j['value_size'],
                               j['concurrency'], j['repeat']) for j in fair if j['group'] == group},
                             {(shards, mode, size, concurrency, repeat)
                              for mode in ('page_cache', 'value_wal_barrier', 'value_wal_group_barrier')
                              for size in (1024, 8192, 65536)
                              for concurrency in concurrencies for repeat in (0, 1)})
        for group, owners in (('G14O1', 1), ('G14O5', 5)):
            self.assertEqual({(j['owner_count'], j['backend'], j['value_size'], j['concurrency'], j['repeat'])
                              for j in fair if j['group'] == group},
                             {(owners, backend, size, concurrency, repeat)
                              for backend in ('tidehunter_verbs_native', 'tidehunter_tcp')
                              for size in (1024, 8192, 65536)
                              for concurrency in (1, 5, 16) for repeat in (0, 1)})
        profiles = [j for j in fair if j['group'] == 'G14P']
        self.assertEqual({(j['backend'], j['concurrency'], j.get('owner_count'), j.get('native_shards'))
                          for j in profiles},
                         {('tidehunter_verbs_native', 1, 5, None),
                          ('tidehunter_tcp', 1, 5, None),
                          ('tidehunter_verbs_native', 16, 5, None),
                          ('tidehunter_verbs_native', 16, 1, None),
                          ('native', 16, None, 1), ('native', 1, None, 1)})
        for job in fair:
            self.assertEqual((job['selection'], job['profile'], job['cohort']),
                             ('fair_native', 'write-concurrent', 'primary_rf1'))
            self.assertEqual((job['keys'], job['measurement_operations'], job['warmup_operations']),
                             (4096, 4096, 512))
            self.assertEqual(job['mix']['put'], 100)
            self.assertNotIn('put_key_policy', job)
            if job['backend'] == 'native':
                self.assertEqual(job['native_wal_geometry'], 'native_default')
                self.assertEqual(job['native_durability'], job['durability_tier'])
            else:
                self.assertEqual(job['durability_tier'], 'synchronous')
            if job['group'] == 'G14P':
                self.assertEqual((job['value_size'], job['repeat']), (65536, 0))
                self.assertEqual(job['diagnostics'], {'write_profile_limit': 4096})
            self.assertIs(campaign.validate_focused_job(job), job)
            altered = copy.deepcopy(job)
            altered['measurement_operations'] += 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)

    def test_compact_selections_partition_parity(self):
        parity = campaign.make_focused_matrix('parity')
        compact = campaign.make_focused_matrix('parity_compact')
        five = campaign.make_focused_matrix('fair_five_owner')
        self.assertEqual((len(compact), len(five)), (177, 39))
        compact_ids = [job['case_id'] for job in compact]
        five_ids = [job['case_id'] for job in five]
        self.assertEqual(len(set(compact_ids + five_ids)), len(parity))
        self.assertEqual(set(compact_ids + five_ids), {job['case_id'] for job in parity})
        self.assertEqual(compact_ids, [j['case_id'] for j in parity
                                      if j['group'] in ('G13W', 'G14N', 'G14S', 'G14O1')
                                      or (j['group'] == 'G14P'
                                          and (j['backend'] == 'native' or j.get('owner_count') == 1))])
        self.assertEqual(five_ids, [j['case_id'] for j in parity if j['case_id'] not in set(compact_ids)])
        self.assertEqual(compact[0], parity[0])
        self.assertEqual(five[0]['group'], 'G14O5')
        self.assertEqual((campaign.selection_owner_hosts('parity_compact'),
                          campaign.selection_owner_hosts('fair_five_owner')), (1, 5))
        for job in compact:
            self.assertEqual(campaign.job_owner_hosts(job), 1)
        for job in five:
            self.assertEqual(campaign.job_owner_hosts(job), 5)
        for selection, digest in (
            ('parity_compact', '4978687ab42ca839e2c864978fb1489854ae201d28f06facf89a283cdea8e0aa'),
            ('fair_five_owner', '479601db27b3a709e83944dcdfa9b77e7f8e4bacac9637a29e04327e09151946'),
            ('unified', 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506'),
            ('append_floors', '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40'),
            ('fair_native', '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9'),
            ('parity', 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6')):
            encoded = json.dumps(campaign.make_focused_matrix(selection),
                                 sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(encoded).hexdigest(), digest)

    def test_fabric_compact_matrix_and_canaries(self):
        jobs = campaign.make_focused_matrix('fabric_ab_compact')
        self.assertEqual(len(jobs), 80)
        self.assertEqual(Counter(j['group'] for j in jobs), {'G16P': 40, 'G16C': 40})
        self.assertEqual(campaign.focused_group_counts('fabric_ab_compact'),
                         {'G16P': 40, 'G16C': 40, 'sealed_knee': 40, 'media_read': 40})
        self.assertEqual(campaign.selection_owner_hosts('fabric_ab_compact'), 1)
        self.assertTrue(all(campaign.job_owner_hosts(j) == 1 for j in jobs))
        self.assertEqual(len({j['case_id'] for j in jobs}), 80)
        data = json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()
        self.assertEqual(hashlib.sha256(data).hexdigest(),
                         '8e12ba4352dc461680565b3bfaef5db705e702e03bce986323cc1e31ae9bde6e')
        self.assertEqual(campaign.fabric_parity_violations(jobs), [])
        self.assertEqual([j['group'] for j in jobs[:10]], ['G16P'] * 5 + ['G16C'] * 5)
        self.assertTrue(all((j['value_size'], j['concurrency'], j['repeat']) == (1024, 1, 0)
                            for j in jobs[:10]))
        self.assertEqual([j['backend'] for j in jobs[10:13]],
                         ['tidehunter_nvme_rdma', 'tidehunter_nvme_rdma', 'tidehunter_nvme_tcp'])
        self.assertTrue(all((j['value_size'], j['concurrency'], j['repeat']) == (65536, 16, 0)
                            for j in jobs[10:15]))
        self.assertEqual([j['owner_polling'] for j in jobs[13:15]], ['busy', 'busy'])
        phases = [0 if j['group'] == 'G16P' else 2 if j['backend'] in campaign.NVME_BACKENDS else 1
                  for j in jobs[15:]]
        self.assertEqual(phases, sorted(phases))
        for group in ('G16P', 'G16C'):
            for size in (1024, 65536):
                for concurrency in (1, 16):
                    for nvme in ((False, True) if group == 'G16C' else (False,)):
                        blocks = [[(j['backend'], j.get('nvmeof_target'), j.get('owner_polling'))
                                   for j in jobs[15:] if j['group'] == group
                                   and (j['backend'] in campaign.NVME_BACKENDS) == nvme
                                   and (j['value_size'], j['concurrency'], j['repeat']) ==
                                       (size, concurrency, repeat)]
                                  for repeat in (0, 1)]
                        self.assertEqual(blocks[0], [arm for arm in reversed(blocks[1]) if arm in blocks[0]])
        for j in jobs:
            self.assertIs(campaign.validate_focused_job(j), j)
        for selection, digest in (
                ('unified', 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506'),
                ('append_floors', '646f9ff50949c4ecab44a4d77d2ad38e5b2700311cb5c1ce69e742ed521f4e40'),
                ('fair_native', '7a8e64eeb181b0f5040f3b02105d76def85823e87f591fea448d34bcb6a7f2d9'),
                ('parity', 'ee942bda8565ff1295814a61fb00040f6feedce9cd73304dea29dec095169ec6'),
                ('parity_compact', '4978687ab42ca839e2c864978fb1489854ae201d28f06facf89a283cdea8e0aa'),
                ('fair_five_owner', '479601db27b3a709e83944dcdfa9b77e7f8e4bacac9637a29e04327e09151946'),
                ('remote_rw_compact', 'b2f2afc44afc1d066bedb27f819c5fa7789e000f96da0c7beb4c6f9359122620')):
            encoded = json.dumps(campaign.make_focused_matrix(selection), sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(encoded).hexdigest(), digest)

    def test_fabric_parity_rejects_missing_and_changed_arms(self):
        jobs = campaign.make_focused_matrix('fabric_ab_compact')
        self.assertEqual(campaign.fabric_parity_violations(jobs), [])
        index = next(i for i, j in enumerate(jobs) if j['group'] == 'G16P'
                     and j['backend'] == 'tidehunter_tcp_trusted' and j['owner_polling'] == 'adaptive')
        for field, value in (('value_size', 65536), ('concurrency', 16), ('repeat', 1),
                             ('keys', 65536), ('measurement_operations', 2),
                             ('owner_polling', 'busy'), ('read_lanes', 4)):
            altered = copy.deepcopy(jobs)
            altered[index][field] = value
            with self.subTest(field=field):
                self.assertTrue(campaign.fabric_parity_violations(altered))
        self.assertTrue(campaign.fabric_parity_violations(jobs[:index] + jobs[index + 1:]))
        self.assertTrue(campaign.fabric_parity_violations(jobs + [copy.deepcopy(jobs[index])]))
        missing_cell = [j for j in jobs if not (j['group'] == 'G16P' and
                        (j['value_size'], j['concurrency'], j['repeat']) == (1024, 1, 0))]
        no_cold = [j for j in jobs if j['group'] != 'G16C']
        for partial in (missing_cell, no_cold):
            self.assertTrue(campaign.fabric_parity_violations(partial))
            self.assertEqual(campaign.fabric_parity_violations(partial, complete=False), [])
        for field, value in (('seed', 10), ('data_id', 'other'), ('instrumentation', 'profile')):
            altered = copy.deepcopy(jobs)
            altered[index]['sealed_dataset'][field] = value
            with self.subTest(dataset_field=field):
                self.assertTrue(campaign.fabric_parity_violations(altered))
        for index in (5, 6, 7, 8, 9):
            altered = copy.deepcopy(jobs)
            altered[index]['sealed_dataset']['page_cache_policy'] = 'wrong'
            self.assertTrue(campaign.fabric_parity_violations(altered))

    def test_fabric_allowed_arm_differences_and_cache_policies(self):
        jobs = campaign.make_focused_matrix('fabric_ab_compact')
        cell = [j for j in jobs if j['group'] == 'G16C'
                and (j['value_size'], j['concurrency'], j['repeat']) == (1024, 1, 0)]
        owner = next(j for j in cell if j['backend'] == 'tidehunter_tcp_trusted')
        verbs = next(j for j in cell if j['backend'] == 'tidehunter_verbs_native')
        nvmes = [j for j in cell if j['backend'] in campaign.NVME_BACKENDS]
        allowed = {'backend', 'owner_polling', 'nvmeof_route', 'nvmeof_target', 'target_io',
                   'evict_mapped_wal_before_measurement', 'case_id', 'profile', 'media', 'sealed_dataset'}
        dataset_allowed = {'source_id', 'directness', 'target_cache', 'mapped_wal_eviction',
                           'cold_claim', 'page_cache_policy'}
        for nvme in nvmes:
            differing = {key for key in owner.keys() | nvme.keys() if owner.get(key) != nvme.get(key)}
            self.assertFalse(differing - allowed)
            differences = {key for key in owner['sealed_dataset'].keys() | nvme['sealed_dataset'].keys()
                           if owner['sealed_dataset'].get(key) != nvme['sealed_dataset'].get(key)}
            self.assertFalse(differences - dataset_allowed)
            self.assertEqual(nvme['sealed_dataset']['page_cache_policy'], 'drop_at_measurement_gate')
            self.assertEqual({k: v for k, v in nvme['media'].items() if k != 'wal_eviction'},
                             {k: v for k, v in owner['media'].items() if k != 'wal_eviction'})
        self.assertEqual(owner['sealed_dataset']['page_cache_policy'],
                         'evict_mapped_wal_then_drop_at_measurement_gate')
        self.assertEqual(owner['sealed_dataset']['page_cache_policy'], verbs['sealed_dataset']['page_cache_policy'])
        altered = copy.deepcopy(jobs)
        altered[5]['profile'] = 'different'
        altered[5]['sealed_dataset']['source_id'] = 'other'
        altered[5]['sealed_dataset']['cold_claim'] = 'different text'
        self.assertEqual(campaign.fabric_parity_violations(altered), [])

    def test_fabric_does_not_change_default_backends_or_standard_matrix(self):
        self.assertNotIn('tidehunter_tcp_trusted', campaign.BACKENDS)
        self.assertNotIn('tidehunter_tcp_trusted', campaign.LEGACY_BACKENDS)
        self.assertIn('tidehunter_tcp_trusted', campaign.PORTABLE_BACKENDS)
        spec = json.loads(Path(campaign.__file__).with_name('cloud-campaign.json').read_text())
        jobs = campaign.make_matrix(spec)
        self.assertEqual(len(jobs), 315)
        self.assertNotIn('tidehunter_tcp_trusted', {j['backend'] for j in jobs})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                         '175607be6d0c1499461f1644d338dfdddf66167ce26d2a18b3263250b39df765')

    def test_focused_nvme_client_transport_and_standard_tcp_control(self):
        jobs = campaign.make_focused_matrix('fabric_ab_compact')
        for job in jobs[:10]:
            expected = (campaign.transport_for_case('tidehunter_verbs_native')
                        if job['backend'] in campaign.NVME_BACKENDS else campaign.TRANSPORTS[job['backend']])
            with self.subTest(case=job['case_id']):
                config = campaign.make_run_config(job, '/tmp/fabric-case', {}, 'revision')
                self.assertEqual(campaign.protocol_key(config['backend']['options']['transport']),
                                 campaign.protocol_key(expected))
        tcp = next(j for j in jobs if j['backend'] == 'tidehunter_nvme_tcp')
        with self.assertRaisesRegex(ValueError, 'runtime transport differs'):
            campaign.make_run_config(tcp, '/tmp/fabric-case', {'transport': {'kind': 'tcp'}}, 'revision')
        for route in ('sealed_read', 'live'):
            standard = copy.deepcopy(tcp)
            del standard['case_kind']
            standard['owner_count'] = 5
            standard['nvmeof_route'] = route
            config = campaign.make_run_config(standard, '/tmp/fabric-case', {}, 'revision')
            self.assertEqual(config['backend']['options']['transport'], {'kind': 'tcp'})

    def test_spdk_compact_owner_count_is_passed(self):
        compact = next(j for j in campaign.make_focused_matrix('fabric_ab_compact')
                       if j.get('nvmeof_target') == 'spdk')
        original = next(j for j in campaign.make_focused_matrix('device')
                        if j.get('nvmeof_target') == 'spdk')
        with mock.patch.object(campaign.spdk_measurement, 'admit', return_value={'ok': True}) as admit:
            self.assertEqual(campaign.admit_spdk_target_measurement('/tmp', compact,
                             {'spdk_target_measurement': {}}, completed=True), {'ok': True})
            admit.assert_called_with('/tmp', {}, allow_zero_reads=False, owner_count=1)
            campaign.admit_spdk_target_measurement('/tmp', original,
                {'spdk_target_measurement': {}}, completed=True)
            admit.assert_called_with('/tmp', {}, allow_zero_reads=False, owner_count=5)

    def test_remote_rw_compact_grid_and_frozen_reuse(self):
        jobs = campaign.make_focused_matrix('remote_rw_compact')
        self.assertEqual(len(jobs), 342)
        self.assertEqual(campaign.selection_owner_hosts('remote_rw_compact'), 1)
        self.assertEqual(Counter(j['group'] for j in jobs),
                         {'G13W': 48, 'G11N': 45, 'G11S': 24, 'G15F': 24,
                          'G14N': 54, 'G14O1': 36, 'G14P': 3, 'G15C': 54, 'G15M': 54})
        self.assertEqual(len({j['case_id'] for j in jobs}), len(jobs))
        self.assertTrue(all(campaign.job_owner_hosts(j) == 1 for j in jobs))
        self.assertEqual([(j['group'], j['backend']) for j in jobs[:5]],
                         [('G15C', 'native'), ('G15C', 'tidehunter_tcp'),
                          ('G15C', 'tidehunter_verbs_native'), ('G15M', 'native'),
                          ('G15M', 'tidehunter_verbs_native')])
        self.assertTrue(all((j['value_size'], j['concurrency'], j['repeat']) == (1024, 1, 0)
                            for j in jobs[:5]))
        encoded = json.dumps(jobs, sort_keys=True, separators=(',', ':')).encode()
        self.assertEqual(hashlib.sha256(encoded).hexdigest(),
                         'b2f2afc44afc1d066bedb27f819c5fa7789e000f96da0c7beb4c6f9359122620')
        frozen = campaign.make_focused_matrix('parity_compact') + campaign.make_focused_matrix('floors')
        expected = {j['case_id']: j for j in frozen if j['group'] in
                    ('G13W', 'G11N', 'G11S', 'G14N', 'G14O1', 'G14P')}
        reused = {j['case_id']: j for j in jobs if j['group'] in
                  ('G13W', 'G11N', 'G11S', 'G14N', 'G14O1', 'G14P')}
        self.assertEqual(reused, expected)
        floors = [j for j in jobs if j['group'] == 'G15F']
        actual = Counter(json.dumps({k: v for k, v in j['ssd'].items() if k != 'host'}, sort_keys=True)
                         for j in floors)
        original = Counter(json.dumps(j['ssd'], sort_keys=True) for j in frozen if j['group'] == 'G11S')
        self.assertEqual(actual, original)
        self.assertTrue(all(j['ssd']['host'] == 'client' for j in floors))
        self.assertEqual(campaign.parity_violations(jobs, complete=True), [])
        self.assertEqual(campaign.focused_group_counts('remote_rw_compact')['G15C'], 54)
        for job in (jobs[0], jobs[1], jobs[3], floors[0]):
            self.assertIs(campaign.validate_focused_job(job), job)
        head, rest = jobs[:46], jobs[46:]
        self.assertEqual((head[5]['group'], head[5]['backend'], head[5]['value_size'],
                          head[5]['concurrency'], head[5]['repeat']),
                         ('G15M', 'tidehunter_tcp', 1024, 1, 0))
        self.assertEqual(Counter(j['group'] for j in head),
                         {'G15C': 6, 'G15M': 6, 'G13W': 8, 'G11N': 15, 'G11S': 2,
                          'G15F': 2, 'G14N': 3, 'G14O1': 2, 'G14P': 2})
        self.assertTrue(all(j['repeat'] == 0 for j in head))
        self.assertTrue(all((j['value_size'], j['concurrency']) == (65536, 16)
                            for j in head[6:12]))
        self.assertEqual({(j['ssd_write']['mode'], j['ssd_write']['host']) for j in head
                          if j['group'] == 'G13W'},
                         {(mode, host) for mode in ('fdatasync', 'append_pwrite', 'append_mmap', 'append_mmap_fsync')
                          for host in ('client', 'owner0')})
        self.assertTrue(all(j['ssd_write']['block_bytes'] == 65536
                            for j in head if j['group'] == 'G13W'))
        for group in ('G11S', 'G15F'):
            cases = [j for j in head if j['group'] == group]
            self.assertEqual({j['ssd']['state'] for j in cases}, {'fresh', 'aged'})
            self.assertTrue(all((j['ssd']['block_bytes'], j['ssd']['region_bytes']) == (65536, 2147483648)
                                for j in cases))
        self.assertEqual(len({(j['net']['tool'], j['net']['mode'], j['net']['interface'],
                               j['net']['provider'], j['net']['endpoint'])
                              for j in head if j['group'] == 'G11N'}), 15)
        self.assertTrue(all((j['value_size'], j['concurrency']) == (65536, 16)
                            for j in head if j['group'] in ('G14N', 'G14O1', 'G14P')))
        self.assertEqual(campaign.parity_violations(list(reversed(jobs))), [])
        head_ids = {j['case_id'] for j in head}
        raw = campaign._focused_jobs()
        old_floors = ([j for j in campaign.make_focused_matrix('append_floors')]
                      + campaign.make_focused_matrix('floors')
                      + [j for j in raw if j['group'] == 'G15F'])
        self.assertEqual([j['case_id'] for j in rest if j['group'] in ('G13W', 'G11N', 'G11S', 'G15F')],
                         [j['case_id'] for j in old_floors if j['case_id'] not in head_ids])
        self.assertEqual([j['case_id'] for j in rest if j['group'] in ('G15C', 'G15M')],
                         [j['case_id'] for j in raw if j['group'] in ('G15C', 'G15M')
                          and j['case_id'] not in head_ids])
        for group in ('G15C', 'G15M'):
            for size in (1024, 8192, 65536):
                for concurrency in (1, 5, 16):
                    blocks = [[j['backend'] for j in rest if j['group'] == group
                               and (j['value_size'], j['concurrency'], j['repeat']) == (size, concurrency, repeat)]
                              for repeat in (0, 1)]
                    self.assertEqual(set(blocks[1]), {'native', 'tidehunter_tcp', 'tidehunter_verbs_native'})
                    self.assertEqual(blocks[0], [arm for arm in reversed(blocks[1]) if arm in blocks[0]])
        for size in (1024, 8192, 65536):
            for concurrency in (1, 5, 16):
                blocks = [[(j['backend'], j['durability_tier']) for j in rest
                           if j['group'] in ('G14N', 'G14O1')
                           and (j['value_size'], j['concurrency'], j['repeat']) == (size, concurrency, repeat)]
                          for repeat in (0, 1)]
                self.assertEqual(len(blocks[1]), 5)
                self.assertEqual(blocks[0], [arm for arm in reversed(blocks[1]) if arm in blocks[0]])

    def test_remote_rw_parity_rejects_mutations(self):
        jobs = campaign.make_focused_matrix('remote_rw_compact')
        index = next(i for i, j in enumerate(jobs) if j['group'] == 'G15C' and j['backend'] == 'native')
        for field, value in (('concurrency', 5), ('value_size', 8192), ('repeat', 1),
                             ('keys', 4096), ('measurement_operations', 4096), ('cohort', 'different')):
            altered = copy.deepcopy(jobs)
            altered[index][field] = value
            with self.subTest(field=field):
                self.assertTrue(campaign.parity_violations(altered))
        self.assertTrue(campaign.parity_violations(jobs[:index] + jobs[index + 1:]))
        for field, value in (('durability_tier', 'synchronous'), ('native_durability', 'page_cache')):
            altered = copy.deepcopy(jobs)
            altered[index][field] = value
            with self.subTest(field=field):
                self.assertTrue(campaign.parity_violations(altered, complete=True))
        write = next(i for i, j in enumerate(jobs) if j['group'] == 'G14N')
        altered = copy.deepcopy(jobs)
        altered[write]['native_durability'] = 'incorrect'
        self.assertTrue(campaign.parity_violations(altered, complete=True))
        missing_cell = [j for j in jobs if not (j['group'] == 'G15C' and
                        (j['value_size'], j['concurrency'], j['repeat']) == (1024, 1, 0))]
        no_reads = [j for j in jobs if j['group'] not in ('G15C', 'G15M')]
        no_writes = [j for j in jobs if j['group'] not in ('G14N', 'G14O1')]
        for partial in (missing_cell, no_reads, no_writes):
            self.assertTrue(campaign.parity_violations(partial))
            self.assertEqual(campaign.parity_violations(partial, complete=False), [])
        for field, value in (('seed', 17), ('page_cache_policy', 'not_dropped'),
                             ('mapped_wal_eviction', 'not_requested'), ('directness', 'other')):
            altered = copy.deepcopy(jobs)
            altered[index]['sealed_dataset'][field] = value
            with self.subTest(field=field):
                self.assertTrue(campaign.parity_violations(altered))

    def test_owner_host_requirements(self):
        self.assertIs(campaign.OWNER_HOST_COUNTS, cloud_layout.OWNER_HOST_COUNTS)
        floors = campaign.make_focused_matrix('append_floors')
        self.assertEqual({job['ssd_write']['mode'] for job in floors},
                         {'fdatasync', 'append_pwrite', 'append_mmap', 'append_mmap_fsync'})
        self.assertTrue(all(campaign.job_owner_hosts(job) == 1 for job in floors))
        self.assertEqual(campaign.job_owner_hosts({'backend': 'native'}), 1)
        self.assertEqual(campaign.job_owner_hosts({'backend': 'tidehunter_tcp', 'owner_count': 1}), 1)
        self.assertEqual(campaign.job_owner_hosts({'backend': 'tidehunter_tcp'}), 5)
        self.assertEqual(campaign.job_owner_hosts({'backend': 'tidehunter_tcp', 'owner_count': 5}), 5)
        for invalid in (True, 0, 2, '1', None):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                campaign.job_owner_hosts({'backend': 'tidehunter_tcp', 'owner_count': invalid})

    def test_fair_topology_identity_and_native_options(self):
        jobs = campaign.make_focused_matrix('fair_native')
        for field in ('native_shards', 'owner_count'):
            job = next(j for j in jobs if j.get(field) == 1)
            for invalid in (True, 1.0, '1', 5, None):
                altered = copy.deepcopy(job)
                altered[field] = invalid
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            altered = copy.deepcopy(job)
            del altered[field]
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        for shards in (1, 5):
            job = next(j for j in jobs if j.get('native_shards') == shards)
            config = campaign.make_run_config(job, '/tmp/case', {}, 'revision')
            self.assertEqual(config['backend']['options']['native_shards'], shards)
            for invalid in (True, float(shards), str(shards), 6 - shards):
                with self.assertRaises(ValueError):
                    campaign.make_run_config(job, '/tmp/case', {'native_shards': invalid}, 'revision')

    def test_active_owner_evidence_and_restrictions(self):
        self.assertEqual(campaign.active_partitions({}), {'0', '1', '2', '3', '4'})
        base = {'owner_count': 1, 'case_kind': 'remote_put',
                'backend': 'tidehunter_verbs_native', 'durability_tier': 'synchronous'}
        self.assertEqual(campaign.active_partitions(base), {'0'})
        for count in (True, 1.0, '1', 0, 2):
            with self.assertRaises(ValueError):
                campaign.active_owner_count({'owner_count': count})
        for field, value in (('sealed_dataset', {}), ('nvmeof_route', 'live'),
                             ('case_kind', 'interference'), ('case_kind', 'hybrid_put'),
                             ('case_kind', 'sealed_knee'), ('case_kind', 'ssd_floor'),
                             ('case_kind', 'ssd_write_floor'), ('case_kind', 'net_floor'),
                             ('case_kind', 'fio'), ('case_kind', 'unknown'),
                             ('backend', 'tidehunter_nvme_rdma'),
                             ('durability_tier', 'page_cache')):
            with self.subTest(field=field, value=value):
                invalid = {**base, field: value}
                with self.assertRaises(ValueError):
                    campaign.active_owner_count(invalid)
                with self.assertRaisesRegex(ValueError, 'one owner is restricted'):
                    campaign.validate_focused_job(invalid)
        job = {**base, 'cohort': 'registered_ram_fullset_control',
               'warmup_key_sweep': True, 'resident_reads': {'require_device_read': True}}
        counters = {name: 0 for name in ('message_read_requests', 'grant_acquisitions',
                    'grant_candidates', 'rma_errors', 'fallback_missing', 'fallback_oversize',
                    'fallback_capacity', 'fallback_unsupported', 'fallback_stale',
                    'wal_reads', 'async_read_submissions', 'cache_hits')}
        counters.update(rma_posts=1, rma_completions=1)
        row = {'available': True, 'counters': counters}
        evidence = {'measurement_drained': True, 'deltas': {
            'client_resident': {'available': True, 'partitions': {'0': row}},
            'owner_executor': {'available': True, 'partitions': {'0': row}},
            'coordinator': {'available': True, 'counters': {'hits': 0}}}}
        self.assertTrue(campaign.qualify_fullset_read_path(job, evidence)['qualified'])
        evidence['deltas']['owner_executor']['partitions']['1'] = row
        self.assertFalse(campaign.qualify_fullset_read_path(job, evidence)['qualified'])

    def test_one_owner_read_tier_partition_counters(self):
        job = {'owner_count': 1, 'case_kind': 'remote_put', 'backend': 'tidehunter_tcp',
               'durability_tier': 'synchronous', 'read_tier': {'name': 'wal_warm', 'requested': {}}}
        row = {'available': True, 'counters': {'wal_reads': 2, 'hits': 0}}
        boundary = {'available': True, 'partitions': {'0': {'authority': {'history_loads': 3}}}}
        end = {'available': True, 'partitions': {'0': {'authority': {'history_loads': 7}}}}
        evidence = {'measurement_drained': True, 'start': {'owner_executor': boundary},
                    'end': {'owner_executor': end}, 'deltas': {
                        'owner_executor': {'available': True, 'partitions': {'0': row}},
                        'coordinator': {'available': True, 'counters': {'hits': 0}}}}
        receipt = {'status': 'completed', 'read_tier_effective': {}}
        observed = campaign.qualify_read_tier(job, evidence, receipt)['observed']
        self.assertEqual((observed['wal_reads'], observed['authority_history_loads']), (2, 4))
        evidence['deltas']['owner_executor']['partitions']['1'] = row
        self.assertIsNone(campaign.qualify_read_tier(job, evidence, receipt)['observed']['wal_reads'])

    def test_owner_receipt_count_and_partition_binding(self):
        job = next(j for j in campaign.make_focused_matrix('fair_native')
                   if j.get('owner_count') == 1)
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

    def test_stages_and_unified_are_stable_and_valid(self):
        stages = campaign.make_focused_matrix('stages')
        unified = campaign.make_focused_matrix('unified')
        floors = campaign.make_focused_matrix('floors')
        self.assertEqual(unified[:183], stages)
        self.assertEqual(unified[183:252], floors)
        self.assertEqual(len(stages), 183)
        self.assertEqual(len(unified), 270)
        counts = {'G12N': 52, 'G12W': 60, 'G12H': 12, 'G12K': 12, 'G12D': 43, 'G12S': 4}
        self.assertEqual(Counter(j['group'] for j in stages), counts)
        self.assertEqual(Counter(j['group'] for j in unified), counts | {'G11N': 45, 'G11S': 24, 'G11W': 18})
        self.assertEqual([j['case_kind'] for j in stages[:11]],
                         ['native_op', 'native_media_read', 'remote_put', 'remote_put', 'remote_put',
                          'hybrid_put', 'hybrid_put', 'remote_put', 'remote_put', 'media_read', 'native_op'])
        self.assertEqual([next(k for k, v in j['mix'].items() if v) for j in stages[:8]],
                         ['put', 'read', 'put', 'put_batch', 'delete', 'put', 'put', 'put'])
        self.assertEqual([j['nvmeof_target'] for j in stages[5:7]], ['kernel', 'spdk'])
        self.assertEqual(stages[7]['put_key_policy'], 'new')
        digests = {
            'recommended': 'e033a68d931e9db33cacd5ac25ef43f0647bb3d919a1d7c7ac608d8c0414f08d',
            'full': '04dff9517aff741c01289d534e6a55f2f3b1d0e211b2f0ea9cb58c8b5bf6650c',
            'device': '8a0cf7c9ebf16552ef32d616aec14d6876d3f1a84eb904ee53e7a24cc28d9a43',
            'floors': '5fb477358bc57c14c8fad9b7e0958b93afb22ff4d1ba085204d44b1e74fcd429',
            'stages': '646207fe6727aa37367b727759a4d46005ebd56a59f29ec49b13a690de24a724',
            'unified': 'a73d7a070f8416f555aab6434e1c6893b4deca756e77036d0509001b23bc3506',
        }
        for selection, expected in digests.items():
            raw = json.dumps(campaign.make_focused_matrix(selection), sort_keys=True, separators=(',', ':')).encode()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), expected)
        for job in stages:
            self.assertIs(campaign.validate_focused_job(job), job)
            config = campaign.make_run_config(job, Path('/tmp/stages-case'), {}, 'revision')
            self.assertEqual(config['workload']['batch_size'], 32)
            if job['mix']['put_batch']:
                self.assertEqual(job['measurement_operations'], 1024)
            if 'put_key_policy' in job:
                self.assertEqual(config['workload']['put_key_policy'], 'new')
            if 'diagnostics' in job:
                self.assertEqual(config['diagnostics'], job['diagnostics'])
            if job['case_kind'] == 'hybrid_put':
                self.assertFalse(config['phases']['verify_existing_dataset'])
                self.assertTrue(config['phases']['preload'])
                self.assertNotIn('key_namespace_hex', config)
            for field in ('put_key_policy', 'diagnostics'):
                if field in job:
                    invalid = copy.deepcopy(job)
                    del invalid[field]
                    with self.assertRaises(ValueError):
                        campaign.validate_focused_job(invalid)
            altered = copy.deepcopy(job)
            execution, workload = config['execution'], config['workload']
            self.assertTrue(1 <= execution['concurrency'] <= execution['queue_capacity'] <= 65536)
            self.assertLessEqual(execution['concurrency'], 4096)
            self.assertTrue(2 <= workload['batch_size'] <= 1024)
            self.assertGreater(sum(workload['mix'].values()), 0)
            self.assertLessEqual(config['dataset']['keys'] * config['dataset']['value_size'],
                                 config['limits']['max_bytes'])
            if job['backend'] != 'native':
                self.assertTrue(1 <= job['read_lanes'] <= 8)
                # Default owner admission: 32 operations and 320 KiB encoded.
                batch_ops = workload['batch_size'] if workload['mix']['put_batch'] else 2
                self.assertLessEqual(batch_ops, 32)
                self.assertLessEqual((job['value_size'] + 32 + 32) * batch_ops + 96, 320 * 1024)
            diagnostics = config.get('diagnostics', {})
            self.assertTrue(all(0 <= limit <= 65536 for limit in diagnostics.values()))
            self.assertFalse(diagnostics.get('write_profile_limit') and diagnostics.get('write_timeline_limit'))
            if diagnostics.get('write_timeline_limit') or diagnostics.get('read_profile_limit'):
                self.assertEqual(config['backend']['kind'], 'tidehunter')
            if workload.get('put_key_policy') == 'new':
                self.assertTrue(workload['mix']['put'] or workload['mix']['put_batch'])
                self.assertFalse(any(workload['mix'][op] for op in ('read', 'get_many', 'missing_read', 'delete')))
                self.assertFalse(config['phases'].get('measurement_key_sweep', False))
            altered['concurrency'] += 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        for job in (j for j in stages if j['case_kind'] == 'native_media_read'):
            self.assertTrue(job['drop_os_caches'] and job['evict_mapped_wal_before_measurement'])
            self.assertEqual(job['key_order'], 'random_with_replacement')
            self.assertFalse(job['measurement_key_sweep'])
        for job in (j for j in stages if j['case_kind'] == 'hybrid_put'):
            self.assertEqual(job['read_lanes'], 4)
            self.assertEqual(job['nvmeof_route'], 'sealed_read')
            self.assertEqual(job['keys'], 4096)
        profiled_reads = [j for j in stages if j['group'] == 'G12D' and j['case_kind'] == 'media_read']
        self.assertEqual(len(profiled_reads), 22)
        self.assertTrue(all(j['measurement_operations'] <= j['diagnostics']['read_profile_limit']
                            for j in profiled_reads))
        c16 = [j for j in profiled_reads if j['concurrency'] == 16]
        self.assertEqual(len(c16), 6)
        self.assertEqual(Counter((j['backend'], j.get('nvmeof_target'), j['media']['expected'])
                                 for j in c16),
                         {('tidehunter_verbs_native', None, 'memory'): 1,
                          ('tidehunter_verbs_native', None, 'cold'): 1,
                          ('tidehunter_tcp', None, 'memory'): 1, ('tidehunter_tcp', None, 'cold'): 1,
                          ('tidehunter_nvme_rdma', 'kernel', 'cold'): 1,
                          ('tidehunter_nvme_rdma', 'spdk', 'cold'): 1})
        self.assertEqual(Counter(j['media']['expected'] for j in profiled_reads
                                 if j['backend'] == 'tidehunter_verbs'), {'memory': 1, 'cold': 1})
        for operation, size in (('put', 65536), ('put_batch', 8192), ('delete', 1024)):
            self.assertTrue(any(j['group'] == 'G12D' and j['backend'] == 'tidehunter_tcp'
                                and j['mix'][operation] == 100 and j['value_size'] == size
                                and j['concurrency'] == 1 and j['diagnostics'].get('write_timeline_limit') == 4096
                                for j in stages))
        writes = unified[252:]
        self.assertEqual(len(writes), 18)
        self.assertEqual((writes[0]['repeat'], writes[0]['ssd_write']['mode'],
                          writes[0]['ssd_write']['block_bytes']), (0, 'fdatasync', 1024))
        self.assertEqual({(j['repeat'], j['ssd_write']['mode'], j['ssd_write']['block_bytes']) for j in writes},
                         {(r, mode, size) for r in (0, 1) for mode in ('buffered', 'fdatasync', 'direct')
                          for size in (1024, 8192, 65536)})
        for job in writes:
            self.assertEqual(job['selection'], 'unified')
            self.assertIs(campaign.validate_focused_job(job), job)
            self.assertEqual(job['ssd_write']['region_bytes'], 536870912)
            self.assertEqual(job['ssd_write']['runtime_seconds'], 6)
            self.assertEqual(job['ssd_write']['queue_depth'], 1)
            self.assertEqual(job['ssd_write']['location'], 'local_file')
            self.assertIs(job['ssd_write']['preheat'], True)
            altered = copy.deepcopy(job)
            altered['ssd_write']['preheat'] = False
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
            altered = copy.deepcopy(job)
            altered['ssd_write']['unknown'] = 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)

    def test_all_jobs_fit_external_storage_batch_limit(self):
        # prepare_external_case_file rejects more than 1,000,000 batches per partition.
        backend_limit = 1_000_000
        for job in campaign.make_focused_matrix('full'):
            bound = max(16384, job['keys'] + job['warmup_operations'] + job['measurement_operations'])
            self.assertLessEqual(bound, backend_limit, job['case_id'])
        interference = [j for j in campaign.make_focused_matrix('full') if j['case_kind'] == 'interference']
        self.assertTrue(all(j['measurement_operations'] == 400000 for j in interference))

    def test_sealed_datasets_fit_the_preparer_and_the_hybrid_catalog_budget(self):
        # The sealed preparer (nvmeof_campaign.rs MAX_KEYS) rejects more than 65,536 keys. A hybrid reader
        # charges 4 KiB per catalog entry of its owner plus fixed budgets against a 448 MiB coordinator
        # allowance (cloud_controller.connect_nvme). Either limit aborts the campaign at preparation.
        mib = 1024 ** 2
        allowance = 512 * mib - 64 * mib
        for job in campaign.make_focused_matrix('full') + campaign.make_focused_matrix('device'):
            bound = max(16384, job['keys'] + job['warmup_operations'] + job['measurement_operations'])
            self.assertLessEqual(bound, 1_000_000, job['case_id'])
            if 'sealed_dataset' not in job:
                continue
            self.assertLessEqual(job['keys'], 65536, job['case_id'])
            if job['backend'] == 'tidehunter_nvme_rdma':
                entries = -(-job['keys'] // 5)
                charged = entries * 4096 + 223 + 32 * 4096 + 128 * 8192 + 4096
                reader = -(-charged // mib) * mib + 16 * mib + 128 * 2048 + 32 * 1024
                self.assertLessEqual(5 * reader, allowance, job['case_id'])

    def test_sources_are_repeat_isolated_and_contiguous(self):
        jobs = campaign.make_focused_matrix('full')
        sources = {}
        for index, job in enumerate(jobs):
            source = job.get('sealed_dataset')
            if source:
                sources.setdefault(source['source_id'], []).append((index, job['repeat']))
                self.assertIn('r' + str(job['repeat']), source['data_id'])
        for positions in sources.values():
            self.assertEqual(len({r for _, r in positions}), 1)
            self.assertEqual([i for i, _ in positions], list(range(positions[0][0], positions[-1][0] + 1)))

    def test_strict_admission(self):
        job = campaign.make_focused_matrix('full')[0]
        for change in ({'unexpected': 1}, {'repeat': True}, {'case_id': 'wrong'},
                       {'target_io': 'direct'}, {'case_kind': 'sui_trace_replay'},
                       {'value_size': 1025}, {'execution': {'queue_capacity': 256, 'extra': 1}}):
            candidate = copy.deepcopy(job)
            candidate.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                campaign.validate_focused_job(candidate)
        candidate = copy.deepcopy(job)
        candidate['sealed_dataset']['extra'] = 1
        with self.assertRaises(ValueError):
            campaign.validate_focused_job(candidate)
        for selection in ('unknown', None, 1):
            with self.assertRaises(ValueError):
                campaign.make_focused_matrix(selection)

    def test_routes_and_execution(self):
        jobs = campaign.make_focused_matrix('full')
        for job in jobs:
            config = campaign.make_run_config(job, Path('/tmp/focused-case'), {}, 'revision')
            self.assertEqual(config['case_id'], job['case_id'])
            if job['backend'] == 'tidehunter_nvme_rdma':
                self.assertEqual(job['nvmeof_route'], 'sealed_read')
                self.assertIn(job['target_io'], ('buffered', 'spdk_direct', 'direct'))
            if job['group'] == 'G7' and job['mix']['put']:
                self.assertNotEqual(job['backend'], 'tidehunter_nvme_rdma')
            if job['group'] == 'G7':
                self.assertEqual(config['case_id'], job['case_id'])
                self.assertGreaterEqual(config['execution']['queue_capacity'], job['concurrency'])
        hybrid = next(j for j in jobs if j['backend'] == 'tidehunter_nvme_rdma')
        config = campaign.make_run_config(hybrid, Path('/tmp/focused-case'), {}, 'revision')
        self.assertEqual(config['backend']['options']['transport'], campaign.transport_for_case('tidehunter_verbs_native'))
        interference = next(j for j in jobs if j["group"] == "G4")
        config = campaign.make_run_config(interference, Path('/tmp/focused-case'), {}, 'revision')
        self.assertEqual(config["execution"]["mode"], "interference")
        self.assertEqual(config["case_id"], interference["case_id"])
        self.assertEqual(config["interference"], interference["interference"])
        self.assertGreaterEqual(config["execution"]["queue_capacity"],
                                config["execution"]["concurrency"] + interference["interference"]["put_concurrency"])
        self.assertGreater(config["phases"]["measurement_operations"], 20000)

    def test_reviewed_kind_dimensions(self):
        jobs = campaign.make_focused_matrix('full')
        self.assertEqual({j['trace']['scope'] for j in jobs if j['group'] == 'G3'}, {'cold_owner_read', 'owner_put'})
        self.assertEqual({j['fio']['location'] for j in jobs if j['group'] == 'G2'}, {'local_file', 'nvmeof_namespace'})
        direct = [j for j in jobs if j['target_io'] == 'direct']
        self.assertEqual(len(direct), 9)
        for job in direct:
            self.assertEqual((job['group'], job['case_kind'], job['backend'], job['nvmeof_route'],
                              job['nvmeof_target'], job['media']['expected'], job['concurrency']),
                             ('G1', 'media_read', 'tidehunter_nvme_rdma', 'sealed_read', 'kernel', 'cold', 1))
            self.assertEqual(job['sealed_dataset']['directness'], 'initiator_O_DIRECT_target_kernel_buffered_io_0')
            self.assertEqual(job['sealed_dataset']['target_cache'], 'direct_kernel_buffered_io_0_no_target_pagecache')
            campaign.validate_focused_job(job)
            for field, value in (('group', 'G4'), ('group', 'G5'), ('group', 'G7'),
                                 ('nvmeof_route', 'live'), ('nvmeof_target', 'spdk'),
                                 ('mix', campaign.mix(put=100))):
                altered = copy.deepcopy(job)
                altered[field] = value
                with self.assertRaises(ValueError):
                    campaign.validate_focused_job(altered)
            altered = copy.deepcopy(job)
            altered['media']['expected'] = 'memory'
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(altered)
        self.assertEqual({j['write_wait_diagnostics']['enabled'] for j in jobs if j['group'] == 'G9'}, {False, True})
        prewarm = [j for j in jobs if j.get('media', {}).get('history') == 'prewarm']
        self.assertEqual(len(prewarm), 9)
        for job in prewarm:
            self.assertFalse(job['drop_os_caches'])
            self.assertTrue(job['evict_mapped_wal_before_measurement'])
            self.assertEqual(job['sealed_dataset']['mapped_wal_eviction'], 'before_measurement_gate')
            config = campaign.make_run_config(job, Path('/tmp/focused-case'), {}, 'revision')
            self.assertTrue(config['phases']['evict_mapped_wal_before_measurement'])
            self.assertEqual(config['phases']['warmup_operations'], job['keys'])
            self.assertTrue(config['phases']['warmup_key_sweep'])
        for job in jobs:
            if job['group'] == 'G4':
                self.assertEqual(job['interference'], {'read_offered_per_second': 1000, 'read_queue_capacity': 4096,
                    'put_concurrency': 16, 'duration_seconds': 20, 'key_policy': 'disjoint_read_put'})
