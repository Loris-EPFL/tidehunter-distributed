"""Reviewed five-owner small-put diagnostic matrix."""
import copy
import hashlib
import json
from pathlib import Path
import unittest

import cloud_campaign as campaign
import cloud_controller as controller


DIGEST = "e4c667a6080f8cc19efc3b35ba4ed7120d864a7d20c0eca12b998e753095f176"


class SmallPutMatrixTests(unittest.TestCase):
    def test_exact_cells_order_topology_and_trace_coverage(self):
        jobs = campaign.make_focused_matrix("nvmeof_p9_small_put")
        expected = [(size, concurrency, 0) for concurrency in (16, 64, 256)
                    for size in (512, 1024)] + [(1024, 256, 1)]
        self.assertEqual([(job["value_size"], job["concurrency"], job["repeat"])
                          for job in jobs], expected)
        self.assertEqual(len({job["case_id"] for job in jobs}), 7)
        self.assertEqual(campaign.focused_group_counts("nvmeof_p9_small_put"), {"G34W": 7, "remote_put": 7})
        self.assertEqual(hashlib.sha256(json.dumps(jobs, sort_keys=True, separators=(",", ":")).encode()).hexdigest(), DIGEST)
        for job in jobs:
            campaign.validate_focused_job(job)
            self.assertEqual((job["owner_count"], job["owner_topology"], job["backend"],
                              job["durability_tier"], job["coordinator_cache_bytes"]),
                             (5, "separate", "tidehunter_nvme_rdma", "synchronous", 0))
            self.assertEqual(campaign.job_owner_hosts(job), 5)
            self.assertEqual(job["mix"], campaign.mix(put=100))
            self.assertTrue(job["thread_cpu_sampling"])
            self.assertEqual(job["diagnostics"]["write_timeline_limit"], 2048)
            self.assertEqual(job["diagnostics"]["write_timeline_sample_every"] * 2048,
                             job["measurement_operations"])
            self.assertLessEqual(4096 + job["warmup_operations"] + job["measurement_operations"], 1_000_000)
            config = campaign.make_run_config(job, Path("/tmp/p9-small-put-config"), {}, "a" * 40)
            self.assertEqual(config["diagnostics"], job["diagnostics"])
            changed = copy.deepcopy(job)
            changed["diagnostics"]["write_timeline_sample_every"] += 1
            with self.assertRaises(ValueError):
                campaign.validate_focused_job(changed)

    def test_full_inventory_owner_admission_lookup_and_wrong_fleet_refusal(self):
        jobs = campaign.make_focused_matrix("nvmeof_p9_small_put")
        config = {"spec": {"mode": "focused", "selection": "nvmeof_p9_small_put", "owner_hosts": 5},
                  "fleet": {"nodes": [{"index": index} for index in range(5)]}}
        self.assertEqual(controller.controller_matrix(config["spec"]), jobs)
        for job in jobs:
            self.assertEqual(controller.case_owner_job(config, job["case_id"]), job)
        config["fleet"]["nodes"].pop()
        self.assertEqual(controller.case_owner_job(config, jobs[0]["case_id"]), {})


if __name__ == "__main__":
    unittest.main()
